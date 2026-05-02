// Package hedger flattens directional risk by mirroring maker-venue
// fills onto a separate hedge venue.
//
// Pattern: each fill on the maker venue (the thin-book / rebate venue
// where we quote) generates an immediate opposite-side market order on
// the hedge venue (the deep-book venue). Net position across both
// venues stays near zero; revenue = maker spread + maker rebates − hedge
// slippage − fees − funding.
package hedger

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/jkhatri23/Market-Maker/internal/exchange"
)

// Hedger is the interface engine uses. NoopHedger is used when no hedge
// venue is configured.
type Hedger interface {
	OnFill(ctx context.Context, f exchange.Fill) error
}

type NoopHedger struct{}

func (NoopHedger) OnFill(context.Context, exchange.Fill) error { return nil }

// Simple places one opposite-side market order per maker fill. No
// batching, no smart routing — fine for a single-asset bot. For
// multi-asset / high-fill-rate setups, batch fills within ~100ms before
// hedging.
type Simple struct {
	venue  exchange.Exchange
	logger *zap.Logger
}

func NewSimple(venue exchange.Exchange, logger *zap.Logger) *Simple {
	return &Simple{venue: venue, logger: logger}
}

func (s *Simple) OnFill(ctx context.Context, f exchange.Fill) error {
	if s.venue == nil {
		return nil
	}
	side := f.Side.Opposite()
	req := exchange.OrderRequest{
		Instrument:    f.Instrument,
		Side:          side,
		Type:          exchange.OrderTypeMarket,
		Size:          f.Size,
		ClientOrderID: fmt.Sprintf("hedge-%s-%d", f.Instrument, time.Now().UnixNano()),
	}
	s.logger.Info("hedging maker fill",
		zap.String("instrument", f.Instrument),
		zap.String("maker_side", string(f.Side)),
		zap.Float64("size", f.Size),
		zap.String("hedge_venue", s.venue.Name()),
	)
	if _, err := s.venue.PlaceOrder(ctx, req); err != nil {
		s.logger.Error("hedge order rejected",
			zap.String("instrument", f.Instrument),
			zap.Error(err))
		return err
	}
	return nil
}
