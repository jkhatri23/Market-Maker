package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Log         LogConfig         `yaml:"log"`
	Engine      EngineConfig      `yaml:"engine"`
	Assets      []AssetConfig     `yaml:"assets"`
	Risk        RiskConfig        `yaml:"risk"`
	Paper       PaperConfig       `yaml:"paper"`
	Hyperliquid HyperliquidConfig `yaml:"hyperliquid"`
	Binance     BinanceConfig     `yaml:"binance"`
	PriceFeed   PriceFeedConfig   `yaml:"price_feed"`
	Storage     StorageConfig     `yaml:"storage"`
	Alerts      AlertsConfig      `yaml:"alerts"`
	Metrics     MetricsConfig     `yaml:"metrics"`
}

// EngineConfig wires which venue we quote on (the maker) and which we
// hedge against. Both must reference an enabled venue block. HedgeVenue
// is optional — empty string means single-venue MM (paper or naked HL).
type EngineConfig struct {
	MakerVenue string `yaml:"maker_venue"` // "paper" | "hyperliquid" | "binance"
	HedgeVenue string `yaml:"hedge_venue"` // "" | one of the above
}

type HyperliquidConfig struct {
	Enabled       bool   `yaml:"enabled"`
	BaseURL       string `yaml:"base_url"`
	WSURL         string `yaml:"ws_url"`
	PrivateKeyHex string `yaml:"private_key"`
	VaultAddress  string `yaml:"vault_address"`
	Mainnet       bool   `yaml:"mainnet"`
}

type BinanceConfig struct {
	Enabled    bool   `yaml:"enabled"`
	BaseURL    string `yaml:"base_url"`
	WSURL      string `yaml:"ws_url"`
	APIKey     string `yaml:"api_key"`
	APISecret  string `yaml:"api_secret"`
	QuoteAsset string `yaml:"quote_asset"` // "USDT"
	RecvWindow int    `yaml:"recv_window"`
}

type LogConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

type AssetConfig struct {
	Symbol              string        `yaml:"symbol"`
	Enabled             bool          `yaml:"enabled"`
	BaseQuantity        float64       `yaml:"base_quantity"`
	SpreadBps           float64       `yaml:"spread_bps"`
	RequoteThresholdBps float64       `yaml:"requote_threshold_bps"`
	MaxPosition         float64       `yaml:"max_position"`
	MaxLeverage         float64       `yaml:"max_leverage"`
	DepthLevels         int           `yaml:"depth_levels"`
	DepthAlpha          float64       `yaml:"depth_alpha"`
	DepthGamma          float64       `yaml:"depth_gamma"`
	SkewFactor          float64       `yaml:"skew_factor"`
	FundingWindow       time.Duration `yaml:"funding_window"`
}

type RiskConfig struct {
	DailyDrawdownHaltUSD   float64       `yaml:"daily_drawdown_halt_usd"`
	DislocationPct         float64       `yaml:"dislocation_pct"`
	DislocationWindow      time.Duration `yaml:"dislocation_window"`
	FillIntensityWindow    time.Duration `yaml:"fill_intensity_window"`
	FillIntensityThreshold int           `yaml:"fill_intensity_threshold"`
	ReconcileInterval      time.Duration `yaml:"reconcile_interval"`
}

type PaperConfig struct {
	InitialBalanceUSD float64 `yaml:"initial_balance_usd"`
}

type PriceFeedConfig struct {
	Pyth PythConfig `yaml:"pyth"`
}

type PythConfig struct {
	WSURL   string            `yaml:"ws_url"`
	FeedIDs map[string]string `yaml:"feed_ids"`
}

type StorageConfig struct {
	Postgres PostgresConfig `yaml:"postgres"`
}

type PostgresConfig struct {
	DSN string `yaml:"dsn"`
}

type AlertsConfig struct {
	Slack SlackConfig `yaml:"slack"`
}

type SlackConfig struct {
	Enabled                bool   `yaml:"enabled"`
	WebhookURL             string `yaml:"webhook_url"`
	NotifyOnFill           bool   `yaml:"notify_on_fill"`
	NotifyOnError          bool   `yaml:"notify_on_error"`
	NotifyOnCircuitBreaker bool   `yaml:"notify_on_circuit_breaker"`
}

type MetricsConfig struct {
	// Addr is the listen address for the Prometheus /metrics endpoint
	// (e.g. ":9090"). Empty disables the HTTP server.
	Addr string `yaml:"addr"`

	// SnapshotInterval controls how often the engine writes a PnL row to
	// storage and refreshes the position/NetPnL gauges. Defaults to 1m.
	SnapshotInterval time.Duration `yaml:"snapshot_interval"`
}

func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	expanded := os.ExpandEnv(string(raw))

	var cfg Config
	if err := yaml.Unmarshal([]byte(expanded), &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (c *Config) validate() error {
	if len(c.Assets) == 0 {
		return fmt.Errorf("no assets configured")
	}
	seen := map[string]bool{}
	for i, a := range c.Assets {
		if a.Symbol == "" {
			return fmt.Errorf("assets[%d]: missing symbol", i)
		}
		if seen[a.Symbol] {
			return fmt.Errorf("assets[%d]: duplicate symbol %q", i, a.Symbol)
		}
		seen[a.Symbol] = true
		if a.Enabled {
			if a.BaseQuantity <= 0 {
				return fmt.Errorf("asset %s: base_quantity must be > 0", a.Symbol)
			}
			if a.SpreadBps <= 0 {
				return fmt.Errorf("asset %s: spread_bps must be > 0", a.Symbol)
			}
			if a.MaxPosition <= 0 {
				return fmt.Errorf("asset %s: max_position must be > 0", a.Symbol)
			}
			if a.MaxLeverage <= 0 {
				return fmt.Errorf("asset %s: max_leverage must be > 0", a.Symbol)
			}
		}
	}
	if c.Risk.DailyDrawdownHaltUSD <= 0 {
		return fmt.Errorf("risk.daily_drawdown_halt_usd must be > 0")
	}
	if c.Engine.MakerVenue == "" {
		c.Engine.MakerVenue = "paper" // safe default
	}
	if err := c.checkVenue("engine.maker_venue", c.Engine.MakerVenue); err != nil {
		return err
	}
	if c.Engine.HedgeVenue != "" {
		if err := c.checkVenue("engine.hedge_venue", c.Engine.HedgeVenue); err != nil {
			return err
		}
		if c.Engine.HedgeVenue == c.Engine.MakerVenue {
			return fmt.Errorf("engine.hedge_venue must differ from maker_venue")
		}
	}
	return nil
}

func (c *Config) checkVenue(field, name string) error {
	switch name {
	case "paper":
		// Always available.
		return nil
	case "hyperliquid":
		if !c.Hyperliquid.Enabled {
			return fmt.Errorf("%s=%q but hyperliquid.enabled=false", field, name)
		}
		if c.Hyperliquid.PrivateKeyHex == "" {
			return fmt.Errorf("%s=%q requires hyperliquid.private_key", field, name)
		}
	case "binance":
		if !c.Binance.Enabled {
			return fmt.Errorf("%s=%q but binance.enabled=false", field, name)
		}
		if c.Binance.APIKey == "" || c.Binance.APISecret == "" {
			return fmt.Errorf("%s=%q requires binance.api_key + api_secret", field, name)
		}
	default:
		return fmt.Errorf("%s=%q unknown (want paper|hyperliquid|binance)", field, name)
	}
	return nil
}

func (c *Config) EnabledAssets() []AssetConfig {
	out := make([]AssetConfig, 0, len(c.Assets))
	for _, a := range c.Assets {
		if a.Enabled {
			out = append(out, a)
		}
	}
	return out
}
