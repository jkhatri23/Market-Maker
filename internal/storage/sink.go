package storage

import (
	"context"

	"github.com/jkhatri23/Market-Maker/internal/exchange"
)

// Sink persists trading events. The engine writes; the sink stores or
// drops. Implementations: PostgresSink (real), NoopSink (storage disabled).
type Sink interface {
	RecordFill(ctx context.Context, venue string, f exchange.Fill) error
	RecordPnLSnapshot(ctx context.Context, snap PnLSnapshot) error
	RecordFundingRate(ctx context.Context, venue string, fr exchange.FundingRate) error
	Close() error
}

// NoopSink is the storage-disabled implementation. Used when no DSN is
// configured. All methods succeed and discard their input.
type NoopSink struct{}

func (NoopSink) RecordFill(context.Context, string, exchange.Fill) error  { return nil }
func (NoopSink) RecordPnLSnapshot(context.Context, PnLSnapshot) error      { return nil }
func (NoopSink) RecordFundingRate(context.Context, string, exchange.FundingRate) error {
	return nil
}
func (NoopSink) Close() error { return nil }
