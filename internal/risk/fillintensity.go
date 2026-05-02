package risk

import (
	"sync"
	"time"

	"github.com/jkhatri23/Market-Maker/internal/exchange"
)

// fillIntensityGate counts recent fills per side. When more than
// `threshold` fills land on one side within `window`, that side is being
// swept — the engine widens that side to avoid further adverse selection.
type fillIntensityGate struct {
	window    time.Duration
	threshold int

	mu    sync.Mutex
	fills map[string][]fillEvent
	now   func() time.Time
}

type fillEvent struct {
	t    time.Time
	side exchange.Side
}

func newFillIntensityGate(window time.Duration, threshold int) *fillIntensityGate {
	return &fillIntensityGate{
		window:    window,
		threshold: threshold,
		fills:     map[string][]fillEvent{},
		now:       time.Now,
	}
}

func (g *fillIntensityGate) Record(asset string, side exchange.Side) {
	if g.window <= 0 {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.fills[asset] = append(g.fills[asset], fillEvent{t: g.now(), side: side})
}

// Counts returns (buyFills, sellFills) within the rolling window.
func (g *fillIntensityGate) Counts(asset string) (int, int) {
	if g.window <= 0 {
		return 0, 0
	}
	g.mu.Lock()
	defer g.mu.Unlock()

	cutoff := g.now().Add(-g.window)
	h := g.fills[asset]
	for len(h) > 0 && h[0].t.Before(cutoff) {
		h = h[1:]
	}
	g.fills[asset] = h

	var buy, sell int
	for _, f := range h {
		if f.side == exchange.Buy {
			buy++
		} else {
			sell++
		}
	}
	return buy, sell
}
