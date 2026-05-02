// Package paper is a fully-functional simulated venue. It implements
// exchange.Exchange against a price feed: orders rest in memory, and
// each price tick "fills" any resting orders the price has crossed.
//
// Use it to:
//   - Run engine.Run end-to-end against live Pyth prices with no real
//     venue (Phase 8 dry-run).
//   - Reproduce regression scenarios deterministically by replaying a
//     recorded price stream.
//
// Matching: a buy at B fills the first time the reference price ≤ B; a
// sell at A fills when price ≥ A. Fills happen at the order price, not
// the touch price (more conservative — closer to a real maker fill in a
// thin book where we'd rest at our quote and walk down with cancellations
// rather than taking a worse price).
package paper

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/jkhatri23/Market-Maker/internal/exchange"
	"github.com/jkhatri23/Market-Maker/internal/pricefeed"
)

type Exchange struct {
	name           string
	feed           pricefeed.PriceFeed
	specs          map[string]exchange.MarketSpec
	logger         *zap.Logger
	initialBalance float64

	mu        sync.Mutex
	orders    map[string]map[string]*exchange.Order // instrument → orderID → order
	positions map[string]*exchange.Position

	nextID  atomic.Int64
	fillsCh chan exchange.Fill
}

func New(name string, feed pricefeed.PriceFeed, specs map[string]exchange.MarketSpec, initialUSD float64, logger *zap.Logger) *Exchange {
	if specs == nil {
		specs = map[string]exchange.MarketSpec{}
	}
	return &Exchange{
		name:           name,
		feed:           feed,
		specs:          specs,
		logger:         logger,
		initialBalance: initialUSD,
		orders:         map[string]map[string]*exchange.Order{},
		positions:      map[string]*exchange.Position{},
		fillsCh:        make(chan exchange.Fill, 256),
	}
}

func (e *Exchange) Name() string { return e.name }

// Run subscribes to the price feed for every configured instrument and
// matches resting orders on each tick. Blocks until ctx ends.
func (e *Exchange) Run(ctx context.Context) error {
	g, gctx := errgroup.WithContext(ctx)
	for asset := range e.specs {
		asset := asset
		g.Go(func() error { return e.runAsset(gctx, asset) })
	}
	if err := g.Wait(); err != nil && err != context.Canceled {
		return err
	}
	return nil
}

func (e *Exchange) runAsset(ctx context.Context, asset string) error {
	ch, err := e.feed.Subscribe(asset)
	if err != nil {
		return fmt.Errorf("paper: subscribe %s: %w", asset, err)
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case upd, ok := <-ch:
			if !ok {
				return nil
			}
			e.matchOnPriceTick(upd)
		}
	}
}

func (e *Exchange) matchOnPriceTick(upd pricefeed.PriceUpdate) {
	e.mu.Lock()
	bucket, ok := e.orders[upd.Asset]
	if !ok {
		e.mu.Unlock()
		return
	}

	var fills []exchange.Fill
	for id, ord := range bucket {
		if !crosses(ord.Side, upd.Price, ord.Price) {
			continue
		}
		f := exchange.Fill{
			OrderID:       ord.ID,
			ClientOrderID: ord.ClientOrderID,
			Instrument:    ord.Instrument,
			Side:          ord.Side,
			Price:         ord.Price,
			Size:          ord.Remaining(),
			IsMaker:       true,
			Timestamp:     time.Now(),
		}
		fills = append(fills, f)
		delete(bucket, id)
		e.applyFillLocked(f)
	}
	e.mu.Unlock()

	for _, f := range fills {
		select {
		case e.fillsCh <- f:
		default:
			e.logger.Warn("paper: fills buffer full, dropping",
				zap.String("instrument", f.Instrument),
				zap.String("order_id", f.OrderID))
		}
	}
}

func crosses(side exchange.Side, refPrice, orderPrice float64) bool {
	switch side {
	case exchange.Buy:
		return refPrice <= orderPrice
	case exchange.Sell:
		return refPrice >= orderPrice
	}
	return false
}

func (e *Exchange) applyFillLocked(f exchange.Fill) {
	pos, ok := e.positions[f.Instrument]
	if !ok {
		pos = &exchange.Position{Instrument: f.Instrument}
		e.positions[f.Instrument] = pos
	}
	if f.Side == exchange.Buy {
		pos.NetSize += f.Size
	} else {
		pos.NetSize -= f.Size
	}
	pos.MarkPrice = f.Price
	pos.UpdatedAt = f.Timestamp
}

// ---------- exchange.Exchange ----------

func (e *Exchange) SubscribeBook(ctx context.Context, instrument string) (<-chan exchange.BookUpdate, error) {
	// Phase 7 paper: no synthetic book. The engine drives off PriceFeed,
	// not venue books, so this is intentionally inert.
	ch := make(chan exchange.BookUpdate)
	go func() {
		<-ctx.Done()
		close(ch)
	}()
	return ch, nil
}

func (e *Exchange) GetMarketSpec(_ context.Context, instrument string) (exchange.MarketSpec, error) {
	if s, ok := e.specs[instrument]; ok {
		return s, nil
	}
	return exchange.MarketSpec{}, fmt.Errorf("paper: no market spec for %s", instrument)
}

func (e *Exchange) GetFundingRate(_ context.Context, instrument string) (exchange.FundingRate, error) {
	return exchange.FundingRate{Instrument: instrument, UpdatedAt: time.Now()}, nil
}

func (e *Exchange) PlaceOrder(_ context.Context, req exchange.OrderRequest) (exchange.Order, error) {
	if _, ok := e.specs[req.Instrument]; !ok {
		return exchange.Order{}, fmt.Errorf("paper: unknown instrument %s", req.Instrument)
	}
	id := fmt.Sprintf("paper-%d", e.nextID.Add(1))
	now := time.Now()
	ord := &exchange.Order{
		ID:            id,
		ClientOrderID: req.ClientOrderID,
		Instrument:    req.Instrument,
		Side:          req.Side,
		Type:          req.Type,
		Price:         req.Price,
		Size:          req.Size,
		Status:        exchange.OrderOpen,
		PostOnly:      req.PostOnly,
		ReduceOnly:    req.ReduceOnly,
		PlacedAt:      now,
		UpdatedAt:     now,
	}

	e.mu.Lock()
	bucket, ok := e.orders[req.Instrument]
	if !ok {
		bucket = map[string]*exchange.Order{}
		e.orders[req.Instrument] = bucket
	}
	bucket[id] = ord
	e.mu.Unlock()

	return *ord, nil
}

func (e *Exchange) CancelOrder(_ context.Context, instrument, orderID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	bucket, ok := e.orders[instrument]
	if !ok {
		return nil
	}
	delete(bucket, orderID)
	return nil
}

func (e *Exchange) CancelAllForInstrument(_ context.Context, instrument string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.orders, instrument)
	return nil
}

func (e *Exchange) GetOpenOrders(_ context.Context, instrument string) ([]exchange.Order, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	bucket, ok := e.orders[instrument]
	if !ok {
		return nil, nil
	}
	out := make([]exchange.Order, 0, len(bucket))
	for _, o := range bucket {
		out = append(out, *o)
	}
	return out, nil
}

func (e *Exchange) SubscribeFills(_ context.Context) (<-chan exchange.Fill, error) {
	return e.fillsCh, nil
}

func (e *Exchange) GetPosition(_ context.Context, instrument string) (exchange.Position, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if p, ok := e.positions[instrument]; ok {
		return *p, nil
	}
	return exchange.Position{Instrument: instrument}, nil
}

func (e *Exchange) GetBalance(_ context.Context) (exchange.Balance, error) {
	return exchange.Balance{
		Currency:  "USD",
		Total:     e.initialBalance,
		Available: e.initialBalance,
		EquityUSD: e.initialBalance,
		UpdatedAt: time.Now(),
	}, nil
}
