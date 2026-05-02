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
	"github.com/jkhatri23/Market-Maker/internal/exchanges/paper"
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

	// Storage: Postgres if a DSN is set, NoopSink otherwise.
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

	// Alerts: Slack if enabled and a webhook URL is set.
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
		return fmt.Errorf("no Pyth feed IDs configured (price_feed.pyth.feed_ids)")
	}
	feed, err := pyth.New(cfg.PriceFeed.Pyth.WSURL, cfg.PriceFeed.Pyth.FeedIDs, logger)
	if err != nil {
		return fmt.Errorf("init pyth: %w", err)
	}
	g.Go(func() error { return feed.Run(gctx) })
	logger.Info("pyth price feed ready", zap.Any("assets", keys(cfg.PriceFeed.Pyth.FeedIDs)))

	specs := buildMarketSpecs(cfg)
	venue := paper.New("paper", feed, specs, cfg.Paper.InitialBalanceUSD, logger)
	g.Go(func() error { return venue.Run(gctx) })
	logger.Info("paper venue ready",
		zap.Float64("initial_balance_usd", cfg.Paper.InitialBalanceUSD))

	eng := engine.New(cfg, engine.Deps{
		Feed:     feed,
		Venue:    venue,
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
		zap.String("metrics_addr", cfg.Metrics.Addr),
		zap.Float64("daily_drawdown_halt_usd", cfg.Risk.DailyDrawdownHaltUSD),
	)
	_ = notifier.Notify(ctx, alerts.KindStartup,
		fmt.Sprintf("market-maker up; assets=%v", syms))

	if err := g.Wait(); err != nil && !isShutdownErr(err) {
		return err
	}
	logger.Info("shutdown complete")
	return nil
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

// buildMarketSpecs returns sensible tick/lot defaults for paper trading.
// BTC and SOL get hand-tuned values; everything else falls back to a
// generic spec.
func buildMarketSpecs(cfg *config.Config) map[string]exchange.MarketSpec {
	out := map[string]exchange.MarketSpec{}
	for _, ac := range cfg.EnabledAssets() {
		out[ac.Symbol] = defaultSpec(ac.Symbol)
	}
	return out
}

func defaultSpec(symbol string) exchange.MarketSpec {
	switch symbol {
	case "BTC":
		return exchange.MarketSpec{
			Instrument: "BTC", TickSize: 0.5, LotSize: 0.001, MinSize: 0.001, MaxLeverage: 10,
		}
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
