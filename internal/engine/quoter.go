package engine

import (
	"math"

	"github.com/jkhatri23/Market-Maker/internal/exchange"
)

// Quote is one desired order at one depth level on one side.
type Quote struct {
	Side  exchange.Side
	Price float64
	Size  float64
	Level int // 0 = best (closest to mid), 1 = next, ...
}

// QuoteParams is the full input to the pure quoting function. The engine
// builds it from config + market spec + reference price (and, in Phase 4,
// inventory).
type QuoteParams struct {
	Mid          float64
	SpreadBps    float64
	BaseQuantity float64

	DepthLevels int
	DepthAlpha  float64 // size growth per level
	DepthGamma  float64 // exponent on level index

	TickSize float64 // venue price increment
	LotSize  float64 // venue size increment
	MinSize  float64 // venue min order size; 0 = no min

	// MaxPositionContracts caps the sum of sizes on each side. If the depth
	// curve exceeds this, sizes are scaled down proportionally so we can't
	// exceed the position limit even if every order fills at once.
	MaxPositionContracts float64
}

// Build computes the desired set of bid/ask orders for the next reconcile
// cycle. Pure function — no side effects, deterministic given identical
// inputs. Phase 4 will wrap this with inventory skew (mid shift +
// asymmetric sizing).
func Build(p QuoteParams) []Quote {
	if p.DepthLevels <= 0 || p.Mid <= 0 || p.SpreadBps <= 0 || p.BaseQuantity <= 0 {
		return nil
	}

	halfSpread := p.Mid * p.SpreadBps / 10_000 / 2
	out := make([]Quote, 0, p.DepthLevels*2)

	for d := 0; d < p.DepthLevels; d++ {
		size := levelSize(p.BaseQuantity, p.DepthAlpha, p.DepthGamma, d)
		size = roundDownToLot(size, p.LotSize)
		if p.MinSize > 0 && size < p.MinSize {
			continue
		}
		if size <= 0 {
			continue
		}

		// Each level steps out by one base half-spread.
		dist := halfSpread * float64(1+d)

		bidPrice := roundDownToTick(p.Mid-dist, p.TickSize)
		askPrice := roundUpToTick(p.Mid+dist, p.TickSize)

		out = append(out,
			Quote{Side: exchange.Buy, Price: bidPrice, Size: size, Level: d},
			Quote{Side: exchange.Sell, Price: askPrice, Size: size, Level: d},
		)
	}

	if p.MaxPositionContracts > 0 {
		scaleToPositionCap(out, p.MaxPositionContracts, p.LotSize, p.MinSize)
	}
	return out
}

func levelSize(base, alpha, gamma float64, d int) float64 {
	if d == 0 {
		return base
	}
	return base * (1 + alpha*math.Pow(float64(d), gamma))
}

func scaleToPositionCap(quotes []Quote, cap, lot, min float64) {
	var bidSum, askSum float64
	for _, q := range quotes {
		if q.Side == exchange.Buy {
			bidSum += q.Size
		} else {
			askSum += q.Size
		}
	}
	worst := math.Max(bidSum, askSum)
	if worst <= cap {
		return
	}
	scale := cap / worst
	for i := range quotes {
		s := roundDownToLot(quotes[i].Size*scale, lot)
		if min > 0 && s < min {
			s = 0
		}
		quotes[i].Size = s
	}
}

func roundDownToTick(price, tick float64) float64 {
	if tick <= 0 {
		return price
	}
	return math.Floor(price/tick) * tick
}

func roundUpToTick(price, tick float64) float64 {
	if tick <= 0 {
		return price
	}
	return math.Ceil(price/tick) * tick
}

func roundDownToLot(size, lot float64) float64 {
	if lot <= 0 {
		return size
	}
	return math.Floor(size/lot) * lot
}
