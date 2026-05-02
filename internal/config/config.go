package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Log       LogConfig       `yaml:"log"`
	Assets    []AssetConfig   `yaml:"assets"`
	Risk      RiskConfig      `yaml:"risk"`
	Venues    VenuesConfig    `yaml:"venues"`
	PriceFeed PriceFeedConfig `yaml:"price_feed"`
	Storage   StorageConfig   `yaml:"storage"`
	Alerts    AlertsConfig    `yaml:"alerts"`
	Metrics   MetricsConfig   `yaml:"metrics"`
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

type VenuesConfig struct {
	Paper      PaperConfig      `yaml:"paper"`
	Polymarket PolymarketConfig `yaml:"polymarket"`
	Kalshi     KalshiConfig     `yaml:"kalshi"`
}

type PaperConfig struct {
	Enabled           bool    `yaml:"enabled"`
	InitialBalanceUSD float64 `yaml:"initial_balance_usd"`
}

type PolymarketConfig struct {
	Enabled       bool   `yaml:"enabled"`
	BaseURL       string `yaml:"base_url"`
	WSURL         string `yaml:"ws_url"`
	APIKey        string `yaml:"api_key"`
	APISecret     string `yaml:"api_secret"`
	APIPassphrase string `yaml:"api_passphrase"`
}

type KalshiConfig struct {
	Enabled        bool   `yaml:"enabled"`
	BaseURL        string `yaml:"base_url"`
	KeyID          string `yaml:"key_id"`
	PrivateKeyPath string `yaml:"private_key_path"`
}

type PriceFeedConfig struct {
	Pyth PythConfig `yaml:"pyth"`
}

type PythConfig struct {
	WSURL    string            `yaml:"ws_url"`
	FeedIDs  map[string]string `yaml:"feed_ids"`
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
