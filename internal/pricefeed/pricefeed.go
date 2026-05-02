package pricefeed

import (
	"context"
	"time"
)

// PriceUpdate is a single tick of the reference price for one asset.
// Confidence is the venue's published 1-sigma confidence interval (Pyth
// publishes this; for venue-bookbased feeds it can be 0).
type PriceUpdate struct {
	Asset      string
	Price      float64
	Confidence float64
	Timestamp  time.Time
}

// PriceFeed delivers reference prices from underlying markets. The engine
// uses these as the "true" mid and quotes spreads around them.
//
// Run blocks until ctx is canceled or an unrecoverable error occurs. It
// owns the WS connection and reconnect loop. Subscribe must be safe to
// call before or after Run.
type PriceFeed interface {
	Name() string

	Run(ctx context.Context) error

	// Subscribe returns a channel that receives every PriceUpdate for the
	// given asset. Multiple subscribers per asset are supported.
	Subscribe(asset string) (<-chan PriceUpdate, error)

	// GetPrice returns the most recent price for an asset and how stale it
	// is. ok=false means no price has been seen yet. Callers must check
	// staleness against their own freshness tolerance.
	GetPrice(asset string) (price float64, age time.Duration, ok bool)
}
