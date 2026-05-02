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
	"github.com/jkhatri23/Market-Maker/internal/exchanges/kalshi"
	"github.com/jkhatri23/Market-Maker/internal/exchanges/paper"
	"github.com/jkhatri23/Market-Maker/internal/exchanges/polymarket"
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
		logger.Info("pyth price feed ready", zap.Any("assets", keys(cfg.PriceFeed.Pyth.FeedIDs)))
	} else {
		logger.Info("pyth price feed disabled (no feed_ids); engine will not start")
	}

	// Build venues map. Each enabled venue gets its own client + (if it
	// has its own loop) a goroutine on the errgroup.
	venues := map[string]exchange.Exchange{}
	specs := buildMarketSpecs(cfg)
	if cfg.Venues.Paper.Enabled && feed != nil {
		pe := paper.New("paper", feed, specs, cfg.Venues.Paper.InitialBalanceUSD, logger)
		venues["paper"] = pe
		g.Go(func() error { return pe.Run(gctx) })
		logger.Info("paper venue ready",
			zap.Float64("initial_balance_usd", cfg.Venues.Paper.InitialBalanceUSD))
	}
	if cfg.Venues.Polymarket.Enabled {
		pm, err := polymarket.New(cfg.Venues.Polymarket, logger)
		if err != nil {
			return fmt.Errorf("init polymarket: %w", err)
		}
		venues["polymarket"] = pm
		logger.Info("polymarket venue ready (endpoints stubbed)")
	}
	if cfg.Venues.Kalshi.Enabled {
		kc, err := kalshi.New(cfg.Venues.Kalshi, logger)
		if err != nil {
			return fmt.Errorf("init kalshi: %w", err)
		}
		venues["kalshi"] = kc
		logger.Info("kalshi venue ready (endpoints stubbed)")
	}

	// Start engine if we have a feed and at least one venue.
	if feed != nil && len(venues) > 0 {
		eng := engine.New(cfg, engine.Deps{
			Feed:     feed,
			Venues:   venues,
			Risk:     riskMgr,
			Sink:     sink,
			Notifier: notifier,
			Metrics:  m,
		}, logger)
		g.Go(func() error { return eng.Run(gctx) })
	} else {
		logger.Warn("engine not started (need a price feed and at least one venue)")
	}

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

// buildMarketSpecs returns reasonable defaults for paper trading. Real
// venue specs come from Exchange.GetMarketSpec when those endpoints land;
// for paper we hardcode sensible tick/lot for BTC + SOL at current
// prices (~$78K, ~$83).
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
