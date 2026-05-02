package risk

import (
	"math"
	"sync"

	"github.com/jkhatri23/Market-Maker/internal/exchange"
)

// Book tracks per-asset signed position, weighted-average entry price,
// realized PnL (cumulative since process start; Phase 5 wires day-rollover
// to Postgres), and last mark price for unrealized PnL.
//
// Single source of truth for both inventory (position) and PnL — keeps
// them atomic under one mutex and rules out drift between two trackers.
type Book struct {
	mu         sync.RWMutex
	positions  map[string]float64
	avgEntries map[string]float64
	realized   map[string]float64
	marks      map[string]float64
}

func NewBook() *Book {
	return &Book{
		positions:  map[string]float64{},
		avgEntries: map[string]float64{},
		realized:   map[string]float64{},
		marks:      map[string]float64{},
	}
}

// ApplyFill updates position and PnL for one venue fill.
//
//   - Opening from flat: avg_entry = fill_price.
//   - Extending the same direction: weighted-average new avg_entry.
//   - Reducing or closing same direction (no flip): realize PnL on the
//     closed portion at (fill_price − avg_entry) × signed_close_size.
//   - Flipping (signed_size > current pos in opposite dir): close all,
//     then open the residual at avg_entry = fill_price.
//
// Fees always reduce realized PnL.
func (b *Book) ApplyFill(asset string, side exchange.Side, price, size, fee float64) {
	b.mu.Lock()
	defer b.mu.Unlock()

	signed := size
	if side == exchange.Sell {
		signed = -size
	}
	pos := b.positions[asset]
	avg := b.avgEntries[asset]
	newPos := pos + signed

	switch {
	case pos == 0:
		avg = price
	case sameSign(pos, signed):
		avg = (avg*math.Abs(pos) + price*math.Abs(signed)) / math.Abs(newPos)
	case math.Abs(signed) <= math.Abs(pos):
		closed := math.Abs(signed)
		b.realized[asset] += (price - avg) * closed * sign(pos)
		// avg unchanged on partial close; if fully closed, avg becomes
		// stale but is reset on the next open-from-flat.
	default:
		closedAtFlip := math.Abs(pos)
		b.realized[asset] += (price - avg) * closedAtFlip * sign(pos)
		avg = price
	}

	b.positions[asset] = newPos
	b.avgEntries[asset] = avg
	b.realized[asset] -= fee
}

// ApplyMark updates the last-known mark price for unrealized PnL.
func (b *Book) ApplyMark(asset string, mark float64) {
	b.mu.Lock()
	b.marks[asset] = mark
	b.mu.Unlock()
}

func (b *Book) Position(asset string) float64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.positions[asset]
}

func (b *Book) Realized(asset string) float64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.realized[asset]
}

// NetPnL returns realized + unrealized across all assets.
func (b *Book) NetPnL() float64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	var total float64
	for asset, r := range b.realized {
		total += r
		if mark, ok := b.marks[asset]; ok {
			total += (mark - b.avgEntries[asset]) * b.positions[asset]
		}
	}
	return total
}

func sameSign(a, b float64) bool { return (a > 0 && b > 0) || (a < 0 && b < 0) }
func sign(x float64) float64 {
	if x > 0 {
		return 1
	}
	if x < 0 {
		return -1
	}
	return 0
}
