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
	"github.com/jkhatri23/Market-Maker/internal/log"
	"github.com/jkhatri23/Market-Maker/internal/metrics"
	"github.com/jkhatri23/Market-Maker/internal/pricefeed"
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

	// Metrics + risk manager are always created.
	m := metrics.New()
	riskMgr := risk.NewManager(cfg.Risk, 0, logger)
	riskMgr.SetHaltHook(func(reason string) {
		m.Halted.Set(1)
		_ = notifier.Notify(context.Background(), alerts.KindHalt, reason)
	})

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return m.Serve(gctx, cfg.Metrics.Addr) })

	// Price feed: Pyth Hermes if any feed IDs are configured.
	var feed pricefeed.PriceFeed
	if len(cfg.PriceFeed.Pyth.FeedIDs) > 0 {
		client, err := pyth.New(cfg.PriceFeed.Pyth.WSURL, cfg.PriceFeed.Pyth.FeedIDs, logger)
		if err != nil {
			return fmt.Errorf("init pyth: %w", err)
		}
		feed = client
		g.Go(func() error { return client.Run(gctx) })

		// Phase 6 sanity log: print every received price for each enabled
		// asset. Phase 7 replaces this with engine.Run.
		for _, ac := range cfg.EnabledAssets() {
			ac := ac
			ch, err := client.Subscribe(ac.Symbol)
			if err != nil {
				logger.Warn("pyth subscribe", zap.String("asset", ac.Symbol), zap.Error(err))
				continue
			}
			g.Go(func() error {
				for {
					select {
					case <-gctx.Done():
						return gctx.Err()
					case p, ok := <-ch:
						if !ok {
							return nil
						}
						logger.Info("price",
							zap.String("asset", p.Asset),
							zap.Float64("price", p.Price),
							zap.Float64("conf", p.Confidence),
						)
					}
				}
			})
		}
		logger.Info("pyth price feed ready", zap.Any("assets", keys(cfg.PriceFeed.Pyth.FeedIDs)))
	} else {
		logger.Info("pyth price feed disabled (no feed_ids)")
	}
	_ = feed // wired into engine in Phase 7

	enabled := cfg.EnabledAssets()
	syms := make([]string, 0, len(enabled))
	for _, a := range enabled {
		syms = append(syms, a.Symbol)
	}
	logger.Info("perps-mm ready",
		zap.Strings("assets", syms),
		zap.Bool("polymarket_enabled", cfg.Venues.Polymarket.Enabled),
		zap.Bool("kalshi_enabled", cfg.Venues.Kalshi.Enabled),
		zap.String("metrics_addr", cfg.Metrics.Addr),
		zap.Float64("daily_drawdown_halt_usd", cfg.Risk.DailyDrawdownHaltUSD),
	)
	_ = notifier.Notify(ctx, alerts.KindStartup,
		fmt.Sprintf("perps-mm up; assets=%v", syms))

	if err := g.Wait(); err != nil && !isShutdownErr(err) {
		return err
	}
	logger.Info("shutdown complete")
	_ = sink
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
