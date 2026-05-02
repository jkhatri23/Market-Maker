package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/jkhatri23/Market-Maker/internal/alerts"
	"github.com/jkhatri23/Market-Maker/internal/config"
	"github.com/jkhatri23/Market-Maker/internal/engine"
	"github.com/jkhatri23/Market-Maker/internal/exchange"
	"github.com/jkhatri23/Market-Maker/internal/exchanges/binance"
	"github.com/jkhatri23/Market-Maker/internal/exchanges/hyperliquid"
	"github.com/jkhatri23/Market-Maker/internal/exchanges/paper"
	"github.com/jkhatri23/Market-Maker/internal/hedger"
	"github.com/jkhatri23/Market-Maker/internal/log"
	"github.com/jkhatri23/Market-Maker/internal/metrics"
	"github.com/jkhatri23/Market-Maker/internal/pricefeed/pyth"
	"github.com/jkhatri23/Market-Maker/internal/risk"
	"github.com/jkhatri23/Market-Maker/internal/storage"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfgPath := flag.String("config", "config.yaml", "path to config file")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	logger, err := log.New(cfg.Log.Level, cfg.Log.Format)
	if err != nil {
		return fmt.Errorf("init logger: %w", err)
	}
	defer logger.Sync() //nolint:errcheck

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	var sink storage.Sink = storage.NoopSink{}
	if dsn := cfg.Storage.Postgres.DSN; dsn != "" {
		ps, err := storage.NewPostgresSink(ctx, dsn, logger)
		if err != nil {
			return fmt.Errorf("init postgres: %w", err)
		}
		defer ps.Close() //nolint:errcheck
		sink = ps
		logger.Info("postgres sink ready")
	} else {
		logger.Info("postgres sink disabled (no DSN)")
	}

	var notifier alerts.Notifier = alerts.NoopNotifier{}
	if cfg.Alerts.Slack.Enabled && cfg.Alerts.Slack.WebhookURL != "" {
		notifier = alerts.NewSlackNotifier(cfg.Alerts.Slack, logger)
		logger.Info("slack notifier ready")
	}

	m := metrics.New()
	riskMgr := risk.NewManager(cfg.Risk, 0, logger)
	riskMgr.SetHaltHook(func(reason string) {
		m.Halted.Set(1)
		_ = notifier.Notify(context.Background(), alerts.KindHalt, reason)
	})

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return m.Serve(gctx, cfg.Metrics.Addr) })

	if len(cfg.PriceFeed.Pyth.FeedIDs) == 0 {
		return fmt.Errorf("no Pyth feed IDs configured")
	}
	feed, err := pyth.New(cfg.PriceFeed.Pyth.WSURL, cfg.PriceFeed.Pyth.FeedIDs, logger)
	if err != nil {
		return fmt.Errorf("init pyth: %w", err)
	}
	g.Go(func() error { return feed.Run(gctx) })
	logger.Info("pyth price feed ready", zap.Any("assets", keys(cfg.PriceFeed.Pyth.FeedIDs)))

	maker, err := buildVenue(gctx, g, cfg, cfg.Engine.MakerVenue, feed, logger)
	if err != nil {
		return fmt.Errorf("init maker venue %q: %w", cfg.Engine.MakerVenue, err)
	}

	var hedge exchange.Exchange
	var hedgeImpl hedger.Hedger = hedger.NoopHedger{}
	if cfg.Engine.HedgeVenue != "" {
		hedge, err = buildVenue(gctx, g, cfg, cfg.Engine.HedgeVenue, feed, logger)
		if err != nil {
			return fmt.Errorf("init hedge venue %q: %w", cfg.Engine.HedgeVenue, err)
		}
		hedgeImpl = hedger.NewSimple(hedge, logger)
	}

	eng := engine.New(cfg, engine.Deps{
		Feed:     feed,
		Maker:    maker,
		Hedge:    hedge,
		Hedger:   hedgeImpl,
		Risk:     riskMgr,
		Sink:     sink,
		Notifier: notifier,
		Metrics:  m,
	}, logger)
	g.Go(func() error { return eng.Run(gctx) })

	enabled := cfg.EnabledAssets()
	syms := make([]string, 0, len(enabled))
	for _, a := range enabled {
		syms = append(syms, a.Symbol)
	}
	logger.Info("market-maker ready",
		zap.Strings("assets", syms),
		zap.String("maker_venue", cfg.Engine.MakerVenue),
		zap.String("hedge_venue", cfg.Engine.HedgeVenue),
		zap.String("metrics_addr", cfg.Metrics.Addr),
		zap.Float64("daily_drawdown_halt_usd", cfg.Risk.DailyDrawdownHaltUSD),
	)
	_ = notifier.Notify(ctx, alerts.KindStartup,
		fmt.Sprintf("market-maker up; assets=%v maker=%s hedge=%s",
			syms, cfg.Engine.MakerVenue, cfg.Engine.HedgeVenue))

	if err := g.Wait(); err != nil && !isShutdownErr(err) {
		return err
	}
	logger.Info("shutdown complete")
	return nil
}

// buildVenue constructs the named venue and registers any background
// loops (paper.Run) on the errgroup.
func buildVenue(
	ctx context.Context,
	g *errgroup.Group,
	cfg *config.Config,
	name string,
	feed *pyth.Client,
	logger *zap.Logger,
) (exchange.Exchange, error) {
	switch name {
	case "paper":
		specs := buildMarketSpecs(cfg)
		pe := paper.New("paper", feed, specs, cfg.Paper.InitialBalanceUSD, logger)
		g.Go(func() error { return pe.Run(ctx) })
		logger.Info("paper venue ready",
			zap.Float64("initial_balance_usd", cfg.Paper.InitialBalanceUSD))
		return pe, nil

	case "hyperliquid":
		hl, err := hyperliquid.New(hyperliquid.Config{
			BaseURL:       cfg.Hyperliquid.BaseURL,
			WSURL:         cfg.Hyperliquid.WSURL,
			PrivateKeyHex: cfg.Hyperliquid.PrivateKeyHex,
			VaultAddress:  cfg.Hyperliquid.VaultAddress,
			Mainnet:       cfg.Hyperliquid.Mainnet,
		}, logger)
		if err != nil {
			return nil, err
		}
		if err := hl.Bootstrap(ctx); err != nil {
			return nil, fmt.Errorf("hyperliquid bootstrap: %w", err)
		}
		logger.Info("hyperliquid venue ready", zap.Bool("mainnet", cfg.Hyperliquid.Mainnet))
		return hl, nil

	case "binance":
		bn, err := binance.New(binance.Config{
			BaseURL:    cfg.Binance.BaseURL,
			WSURL:      cfg.Binance.WSURL,
			APIKey:     cfg.Binance.APIKey,
			APISecret:  cfg.Binance.APISecret,
			QuoteAsset: cfg.Binance.QuoteAsset,
			RecvWindow: cfg.Binance.RecvWindow,
		}, logger)
		if err != nil {
			return nil, err
		}
		assets := make([]string, 0, len(cfg.EnabledAssets()))
		for _, a := range cfg.EnabledAssets() {
			assets = append(assets, a.Symbol)
		}
		if err := bn.Bootstrap(ctx, assets); err != nil {
			return nil, fmt.Errorf("binance bootstrap: %w", err)
		}
		logger.Info("binance venue ready",
			zap.String("quote_asset", cfg.Binance.QuoteAsset))
		return bn, nil
	}
	return nil, fmt.Errorf("unknown venue %q", name)
}

func isShutdownErr(err error) bool {
	return err == context.Canceled || err == context.DeadlineExceeded
}

func keys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// buildMarketSpecs returns sensible tick/lot defaults for the paper
// venue. Real venues (hyperliquid, binance) fetch their own specs from
// the exchange — this map is consulted only for paper.
func buildMarketSpecs(cfg *config.Config) map[string]exchange.MarketSpec {
	out := map[string]exchange.MarketSpec{}
	for _, ac := range cfg.EnabledAssets() {
		out[ac.Symbol] = defaultSpec(ac.Symbol)
	}
	return out
}

func defaultSpec(symbol string) exchange.MarketSpec {
	switch symbol {
	case "SOL":
		return exchange.MarketSpec{
			Instrument: "SOL", TickSize: 0.01, LotSize: 0.01, MinSize: 0.01, MaxLeverage: 10,
		}
	default:
		return exchange.MarketSpec{
			Instrument: symbol, TickSize: 0.01, LotSize: 1, MinSize: 1, MaxLeverage: 10,
		}
	}
}
