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

	"github.com/jkhatri23/Market-Maker/internal/alerts"
	"github.com/jkhatri23/Market-Maker/internal/config"
	"github.com/jkhatri23/Market-Maker/internal/exchange"
	"github.com/jkhatri23/Market-Maker/internal/metrics"
	"github.com/jkhatri23/Market-Maker/internal/pricefeed"
	"github.com/jkhatri23/Market-Maker/internal/risk"
	"github.com/jkhatri23/Market-Maker/internal/storage"
)

// Engine orchestrates one asset worker per enabled asset. Each worker
// reconciles whenever the reference price moves enough or the periodic
// safety tick fires.
type Engine struct {
	cfg      *config.Config
	feed     pricefeed.PriceFeed
	venue    exchange.Exchange
	risk     *risk.Manager
	sink     storage.Sink
	notifier alerts.Notifier
	metrics  *metrics.Metrics
	logger   *zap.Logger
}

type Deps struct {
	Feed     pricefeed.PriceFeed
	Venue    exchange.Exchange
	Risk     *risk.Manager
	Sink     storage.Sink
	Notifier alerts.Notifier
	Metrics  *metrics.Metrics
}

func New(cfg *config.Config, deps Deps, logger *zap.Logger) *Engine {
	if deps.Sink == nil {
		deps.Sink = storage.NoopSink{}
	}
	if deps.Notifier == nil {
		deps.Notifier = alerts.NoopNotifier{}
	}
	if deps.Metrics == nil {
		deps.Metrics = metrics.New()
	}
	e := &Engine{
		cfg:      cfg,
		feed:     deps.Feed,
		venue:    deps.Venue,
		risk:     deps.Risk,
		sink:     deps.Sink,
		notifier: deps.Notifier,
		metrics:  deps.Metrics,
		logger:   logger,
	}
	deps.Risk.SetHaltHook(func(reason string) {
		e.metrics.Halted.Set(1)
		_ = e.notifier.Notify(context.Background(), alerts.KindHalt, reason)
	})
	return e
}

// Run starts one goroutine per enabled asset, plus a fan-out goroutine
// that pumps fills into the risk manager. Blocks until ctx ends or any
// worker returns an error.
func (e *Engine) Run(ctx context.Context) error {
	if e.venue == nil {
		return errors.New("engine.Run: nil venue")
	}
	e.logger.Info("engine starting", zap.String("venue", e.venue.Name()))

	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error { return e.pumpFills(gctx) })

	for _, ac := range e.cfg.EnabledAssets() {
		ac := ac
		w, err := newAssetWorker(gctx, ac, e.venue, e.feed, e.risk, e.metrics, e.cfg.Risk.ReconcileInterval, e.logger)
		if err != nil {
			return fmt.Errorf("init worker %s: %w", ac.Symbol, err)
		}
		g.Go(func() error { return w.Run(gctx) })
	}

	g.Go(func() error { return e.snapshotLoop(gctx) })
	return g.Wait()
}

// snapshotLoop writes one PnLSnapshot row per asset to storage on every
// tick of MetricsConfig.SnapshotInterval. It also refreshes the
// position/NetPnL prom gauges. Default interval is 1 minute.
func (e *Engine) snapshotLoop(ctx context.Context) error {
	interval := e.cfg.Metrics.SnapshotInterval
	if interval <= 0 {
		interval = time.Minute
	}
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case now := <-tick.C:
			e.metrics.NetPnL.Set(e.risk.Book.NetPnL())
			for _, ac := range e.cfg.EnabledAssets() {
				snap := e.risk.Book.Snapshot(ac.Symbol)
				e.metrics.Position.WithLabelValues(ac.Symbol).Set(snap.Position)
				if err := e.sink.RecordPnLSnapshot(ctx, storage.PnLSnapshot{
					Timestamp:  now,
					Asset:      ac.Symbol,
					Position:   snap.Position,
					AvgEntry:   snap.AvgEntry,
					MarkPrice:  snap.MarkPrice,
					Realized:   snap.Realized,
					Unrealized: snap.Unrealized,
				}); err != nil {
					e.logger.Warn("pnl snapshot persist failed", zap.Error(err))
				}
			}
		}
	}
}

func (e *Engine) pumpFills(ctx context.Context) error {
	venueName := e.venue.Name()
	ch, err := e.venue.SubscribeFills(ctx)
	if err != nil {
		return fmt.Errorf("subscribe fills: %w", err)
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case f, ok := <-ch:
			if !ok {
				return errors.New("fills channel closed")
			}
			e.risk.ApplyFill(f)
			maker := "false"
			if f.IsMaker {
				maker = "true"
			}
			e.metrics.Fills.WithLabelValues(f.Instrument, venueName, string(f.Side), maker).Inc()
			if err := e.sink.RecordFill(ctx, venueName, f); err != nil {
				e.logger.Warn("fill persist failed", zap.Error(err))
			}
			_ = e.notifier.Notify(ctx, alerts.KindFill,
				fmt.Sprintf("%s %s %s %.6f @ %.2f", venueName, f.Instrument, f.Side, f.Size, f.Price))
			e.logger.Info("fill",
				zap.String("venue", venueName),
				zap.String("instrument", f.Instrument),
				zap.String("side", string(f.Side)),
				zap.Float64("price", f.Price),
				zap.Float64("size", f.Size),
				zap.Bool("maker", f.IsMaker),
			)
		}
	}
}

// assetWorker owns the requote loop for one asset:
//   - subscription to the reference price feed
//   - the reconciler that mirrors quotes to the venue
//   - the requote-threshold gate (don't churn on every tick)
type assetWorker struct {
	cfg               config.AssetConfig
	venue             exchange.Exchange
	feed              pricefeed.PriceFeed
	risk              *risk.Manager
	metrics           *metrics.Metrics
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
	riskMgr *risk.Manager,
	m *metrics.Metrics,
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
		risk:              riskMgr,
		metrics:           m,
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
			w.risk.ApplyMark(w.cfg.Symbol, p.Price)
			if !w.shouldRequote(p.Price) {
				continue
			}
			if err := w.requote(ctx); err != nil {
				w.logger.Error("requote failed", zap.Error(err))
			}

		case <-tick.C:
			if err := w.requote(ctx); err != nil {
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

// requote pulls the latest reference price + age, consults the risk
// manager, and reconciles the venue's order set accordingly. A deny
// verdict cancels everything for the asset; an allow verdict produces
// quotes shaped by the manager's skew/widen/pull instructions.
func (w *assetWorker) requote(ctx context.Context) error {
	ref, age, ok := w.feed.GetPrice(w.cfg.Symbol)
	if !ok {
		return nil // no price yet; nothing to do
	}

	v := w.risk.Check(w.cfg, ref, age)
	if !v.Allow {
		w.logger.Warn("risk deny", zap.String("reason", v.Reason))
		if err := w.venue.CancelAllForInstrument(ctx, w.cfg.Symbol); err != nil {
			return fmt.Errorf("cancel-all on deny: %w", err)
		}
		return nil
	}

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
		MidShiftBps:          v.MidShiftBps,
		ExtraBidBps:          v.ExtraBidBps,
		ExtraAskBps:          v.ExtraAskBps,
		PullBuys:             v.PullBuys,
		PullSells:            v.PullSells,
	})

	w.metrics.Requotes.WithLabelValues(w.cfg.Symbol, w.venue.Name()).Inc()
	res, err := w.rec.Reconcile(ctx, quotes)
	if err != nil {
		w.metrics.RequoteFailures.WithLabelValues(w.cfg.Symbol, w.venue.Name()).Inc()
		return err
	}
	for _, q := range quotes {
		w.metrics.OrdersPlaced.WithLabelValues(w.cfg.Symbol, w.venue.Name(), string(q.Side)).Inc()
	}
	w.mu.Lock()
	w.lastQuotedRef = ref
	w.lastQuoteAt = time.Now()
	w.mu.Unlock()
	w.logger.Debug("reconciled",
		zap.Float64("ref", ref),
		zap.Float64("mid_shift_bps", v.MidShiftBps),
		zap.Bool("pull_buys", v.PullBuys),
		zap.Bool("pull_sells", v.PullSells),
		zap.Int("placed", res.Placed),
		zap.Int("canceled", res.Canceled),
		zap.Int("failed", res.Failed),
	)
	return nil
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
