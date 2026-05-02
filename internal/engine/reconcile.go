package engine

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/jkhatri23/Market-Maker/internal/exchange"
)

// Reconciler updates one venue's open orders for one instrument to match
// the desired quote set. Strategy: cancel-all then place-all. Trades one
// extra round-trip vs incremental diff for guaranteed correctness — at no
// point can the live set be a stale subset of the desired set.
type Reconciler struct {
	venue      exchange.Exchange
	instrument string
	logger     *zap.Logger
}

func NewReconciler(venue exchange.Exchange, instrument string, logger *zap.Logger) *Reconciler {
	return &Reconciler{venue: venue, instrument: instrument, logger: logger}
}

// ReconcileResult reports per-cycle counts. Useful for metrics + logs.
type ReconcileResult struct {
	Canceled int
	Placed   int
	Failed   int
}

func (r *Reconciler) Reconcile(ctx context.Context, desired []Quote) (ReconcileResult, error) {
	var res ReconcileResult

	// 1. Cancel everything currently open for this instrument.
	open, err := r.venue.GetOpenOrders(ctx, r.instrument)
	if err != nil {
		return res, fmt.Errorf("get open orders: %w", err)
	}
	res.Canceled = len(open)
	if len(open) > 0 {
		if err := r.venue.CancelAllForInstrument(ctx, r.instrument); err != nil {
			return res, fmt.Errorf("cancel all: %w", err)
		}
	}

	// 2. Place desired set. Per-order failures are logged but don't abort
	// the loop — the next reconcile cycle will try again.
	now := time.Now().UnixNano()
	for _, q := range desired {
		req := exchange.OrderRequest{
			Instrument:    r.instrument,
			Side:          q.Side,
			Type:          exchange.OrderTypeLimit,
			Price:         q.Price,
			Size:          q.Size,
			PostOnly:      true,
			ClientOrderID: fmt.Sprintf("%s-%s-L%d-%d", r.instrument, q.Side, q.Level, now),
		}
		if _, err := r.venue.PlaceOrder(ctx, req); err != nil {
			res.Failed++
			r.logger.Warn("place order failed",
				zap.String("instrument", r.instrument),
				zap.String("side", string(q.Side)),
				zap.Int("level", q.Level),
				zap.Float64("price", q.Price),
				zap.Float64("size", q.Size),
				zap.Error(err),
			)
			continue
		}
		res.Placed++
	}
	return res, nil
}
