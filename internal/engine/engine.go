package engine

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/jkhatri23/Market-Maker/internal/config"
	"github.com/jkhatri23/Market-Maker/internal/exchange"
	"github.com/jkhatri23/Market-Maker/internal/pricefeed"
)

// Engine orchestrates one asset worker per enabled asset. Each worker
// owns its venue's order set for its instrument and reconciles whenever
// the reference price moves enough or the periodic safety tick fires.
//
// Phase 3 deliberately keeps inventory at zero (mid = reference price);
// Phase 4 wraps with Avellaneda-Stoikov skew + risk gates.
type Engine struct {
	cfg    *config.Config
	feed   pricefeed.PriceFeed
	venues map[string]exchange.Exchange
	logger *zap.Logger
}

func New(cfg *config.Config, feed pricefeed.PriceFeed, venues map[string]exchange.Exchange, logger *zap.Logger) *Engine {
	return &Engine{cfg: cfg, feed: feed, venues: venues, logger: logger}
}

// Run starts one goroutine per enabled asset and blocks until ctx ends or
// any worker returns an error.
func (e *Engine) Run(ctx context.Context) error {
	tradingVenue, ok := e.venues["polymarket"]
	if !ok {
		return errors.New("engine.Run: polymarket venue required (Phase 3 single-venue MM)")
	}

	g, gctx := errgroup.WithContext(ctx)
	for _, ac := range e.cfg.EnabledAssets() {
		ac := ac
		w, err := newAssetWorker(gctx, ac, tradingVenue, e.feed, e.cfg.Risk.ReconcileInterval, e.logger)
		if err != nil {
			return fmt.Errorf("init worker %s: %w", ac.Symbol, err)
		}
		g.Go(func() error { return w.Run(gctx) })
	}
	return g.Wait()
}

// assetWorker is one per (asset × venue). It owns:
//   - subscription to the reference price feed
//   - the reconciler that mirrors quotes to the venue
//   - the requote-threshold gate (don't churn on every tick)
type assetWorker struct {
	cfg               config.AssetConfig
	venue             exchange.Exchange
	feed              pricefeed.PriceFeed
	spec              exchange.MarketSpec
	rec               *Reconciler
	reconcileInterval time.Duration
	logger            *zap.Logger

	mu            sync.Mutex
	lastQuotedRef float64
	lastQuoteAt   time.Time
}

func newAssetWorker(
	ctx context.Context,
	ac config.AssetConfig,
	venue exchange.Exchange,
	feed pricefeed.PriceFeed,
	reconcileInterval time.Duration,
	logger *zap.Logger,
) (*assetWorker, error) {
	if reconcileInterval <= 0 {
		reconcileInterval = 30 * time.Second
	}
	spec, err := venue.GetMarketSpec(ctx, ac.Symbol)
	if err != nil {
		return nil, fmt.Errorf("get market spec: %w", err)
	}
	log := logger.With(
		zap.String("asset", ac.Symbol),
		zap.String("venue", venue.Name()),
	)
	return &assetWorker{
		cfg:               ac,
		venue:             venue,
		feed:              feed,
		spec:              spec,
		rec:               NewReconciler(venue, ac.Symbol, log),
		reconcileInterval: reconcileInterval,
		logger:            log,
	}, nil
}

func (w *assetWorker) Run(ctx context.Context) error {
	priceCh, err := w.feed.Subscribe(w.cfg.Symbol)
	if err != nil {
		return fmt.Errorf("subscribe price feed: %w", err)
	}

	tick := time.NewTicker(w.reconcileInterval)
	defer tick.Stop()

	w.logger.Info("asset worker started",
		zap.Float64("spread_bps", w.cfg.SpreadBps),
		zap.Float64("base_qty", w.cfg.BaseQuantity),
		zap.Float64("max_position", w.cfg.MaxPosition),
		zap.Duration("reconcile_interval", w.reconcileInterval),
	)

	for {
		select {
		case <-ctx.Done():
			w.shutdown()
			return ctx.Err()

		case p, ok := <-priceCh:
			if !ok {
				return errors.New("price feed channel closed")
			}
			if !w.shouldRequote(p.Price) {
				continue
			}
			if err := w.requote(ctx, p.Price); err != nil {
				w.logger.Error("requote failed", zap.Error(err))
			}

		case <-tick.C:
			ref, ok := w.lastRef()
			if !ok {
				continue
			}
			if err := w.requote(ctx, ref); err != nil {
				w.logger.Error("periodic reconcile failed", zap.Error(err))
			}
		}
	}
}

func (w *assetWorker) shouldRequote(newPrice float64) bool {
	w.mu.Lock()
	last := w.lastQuotedRef
	w.mu.Unlock()
	if last == 0 {
		return true
	}
	moveBps := math.Abs(newPrice-last) / last * 10_000
	return moveBps >= w.cfg.RequoteThresholdBps
}

func (w *assetWorker) requote(ctx context.Context, ref float64) error {
	quotes := Build(QuoteParams{
		Mid:                  ref,
		SpreadBps:            w.cfg.SpreadBps,
		BaseQuantity:         w.cfg.BaseQuantity,
		DepthLevels:          w.cfg.DepthLevels,
		DepthAlpha:           w.cfg.DepthAlpha,
		DepthGamma:           w.cfg.DepthGamma,
		TickSize:             w.spec.TickSize,
		LotSize:              w.spec.LotSize,
		MinSize:              w.spec.MinSize,
		MaxPositionContracts: w.cfg.MaxPosition,
	})
	res, err := w.rec.Reconcile(ctx, quotes)
	if err != nil {
		return err
	}
	w.mu.Lock()
	w.lastQuotedRef = ref
	w.lastQuoteAt = time.Now()
	w.mu.Unlock()
	w.logger.Debug("reconciled",
		zap.Float64("ref", ref),
		zap.Int("placed", res.Placed),
		zap.Int("canceled", res.Canceled),
		zap.Int("failed", res.Failed),
	)
	return nil
}

func (w *assetWorker) lastRef() (float64, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.lastQuotedRef == 0 {
		return 0, false
	}
	return w.lastQuotedRef, true
}

func (w *assetWorker) shutdown() {
	// Best-effort: cancel all open orders before exiting. Use a fresh
	// context with a short timeout because the parent ctx is already done.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := w.venue.CancelAllForInstrument(ctx, w.cfg.Symbol); err != nil {
		w.logger.Warn("shutdown cancel-all failed", zap.Error(err))
		return
	}
	w.logger.Info("shutdown: canceled all open orders")
}
