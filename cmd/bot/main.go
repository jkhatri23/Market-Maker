package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"

	"github.com/jkhatri23/Market-Maker/internal/config"
	"github.com/jkhatri23/Market-Maker/internal/log"
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

	enabled := cfg.EnabledAssets()
	syms := make([]string, 0, len(enabled))
	for _, a := range enabled {
		syms = append(syms, a.Symbol)
	}
	logger.Info("starting perps-mm",
		zap.Strings("assets", syms),
		zap.Bool("polymarket_enabled", cfg.Venues.Polymarket.Enabled),
		zap.Bool("kalshi_enabled", cfg.Venues.Kalshi.Enabled),
		zap.Float64("daily_drawdown_halt_usd", cfg.Risk.DailyDrawdownHaltUSD),
	)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	<-ctx.Done()
	logger.Info("shutdown signal received, exiting")
	return nil
}
