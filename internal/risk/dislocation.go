package risk

import (
	"sync"
	"time"
)

// dislocationGate trips when the reference price moves more than pct
// within window. On a trip we cancel quotes; the engine retries on the
// next price tick — so the "pause" is implicit (we won't trip again
// until the violent window ages out of history).
type dislocationGate struct {
	pct    float64
	window time.Duration

	mu      sync.Mutex
	history map[string][]priceTick
	now     func() time.Time // injectable for tests
}

type priceTick struct {
	t     time.Time
	price float64
}

func newDislocationGate(pct float64, window time.Duration) *dislocationGate {
	return &dislocationGate{
		pct:     pct,
		window:  window,
		history: map[string][]priceTick{},
		now:     time.Now,
	}
}

// Update records a new tick and returns true if the asset's max-min over
// the window exceeds pct.
func (g *dislocationGate) Update(asset string, price float64) bool {
	if g.pct <= 0 || g.window <= 0 {
		return false
	}
	g.mu.Lock()
	defer g.mu.Unlock()

	now := g.now()
	cutoff := now.Add(-g.window)
	h := g.history[asset]
	for len(h) > 0 && h[0].t.Before(cutoff) {
		h = h[1:]
	}
	h = append(h, priceTick{t: now, price: price})
	g.history[asset] = h

	if len(h) < 2 {
		return false
	}
	minP, maxP := h[0].price, h[0].price
	for _, e := range h {
		if e.price < minP {
			minP = e.price
		}
		if e.price > maxP {
			maxP = e.price
		}
	}
	if minP <= 0 {
		return false
	}
	return (maxP-minP)/minP > g.pct
}
