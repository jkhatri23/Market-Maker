package exchange

import "context"

// Exchange is the only contract the engine depends on. Polymarket, Kalshi,
// and any future venue implement this. The engine never imports
// venue-specific types.
//
// Lifecycle: callers pass a context to subscriptions and long-running calls.
// Closing the context tears down WS connections and goroutines. Channels
// returned by Subscribe* close when the context ends or on unrecoverable
// error.
type Exchange interface {
	// Name returns a stable identifier ("polymarket", "kalshi"). Used in
	// logs, metrics labels, and the storage layer.
	Name() string

	// --- Market data ---

	// SubscribeBook delivers orderbook updates for one instrument. The first
	// message is a Snapshot=true full book; subsequent messages are deltas
	// (or fresh snapshots after detected gaps).
	SubscribeBook(ctx context.Context, instrument string) (<-chan BookUpdate, error)

	// GetMarketSpec returns tick / lot / min / max / leverage limits.
	// Called once at startup per instrument.
	GetMarketSpec(ctx context.Context, instrument string) (MarketSpec, error)

	// GetFundingRate returns the current funding rate, normalized per-hour.
	GetFundingRate(ctx context.Context, instrument string) (FundingRate, error)

	// --- Orders ---

	PlaceOrder(ctx context.Context, req OrderRequest) (Order, error)
	CancelOrder(ctx context.Context, instrument, orderID string) error

	// CancelAllForInstrument is the engine's primary kill action — single
	// call must remove every open order for an instrument so we never end
	// up in a partial-update state.
	CancelAllForInstrument(ctx context.Context, instrument string) error

	GetOpenOrders(ctx context.Context, instrument string) ([]Order, error)

	// SubscribeFills delivers private fill events for the authenticated
	// account across all instruments. The engine routes by Fill.Instrument.
	SubscribeFills(ctx context.Context) (<-chan Fill, error)

	// --- Account ---

	GetPosition(ctx context.Context, instrument string) (Position, error)
	GetBalance(ctx context.Context) (Balance, error)
}
