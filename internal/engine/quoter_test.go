package engine

import (
	"math"
	"testing"

	"github.com/jkhatri23/Market-Maker/internal/exchange"
)

func TestLevelSize_FirstLevelIsBase(t *testing.T) {
	got := levelSize(2.0, 0.6, 1.5, 0)
	if got != 2.0 {
		t.Errorf("level 0 size = %v, want 2.0", got)
	}
}

func TestLevelSize_StrictlyIncreasing(t *testing.T) {
	prev := levelSize(1.0, 0.6, 1.5, 0)
	for d := 1; d < 5; d++ {
		s := levelSize(1.0, 0.6, 1.5, d)
		if s <= prev {
			t.Errorf("level %d size %v not greater than level %d size %v", d, s, d-1, prev)
		}
		prev = s
	}
}

func TestQuote_CountAndSymmetry(t *testing.T) {
	quotes := Build(QuoteParams{
		Mid:          50_000,
		SpreadBps:    50,
		BaseQuantity: 1,
		DepthLevels:  3,
		DepthAlpha:   0.6,
		DepthGamma:   1.5,
		TickSize:     0.01,
		LotSize:      0.001,
	})
	if len(quotes) != 6 {
		t.Fatalf("len(quotes) = %d, want 6 (3 levels × 2 sides)", len(quotes))
	}
	bids := side(quotes, exchange.Buy)
	asks := side(quotes, exchange.Sell)
	if len(bids) != 3 || len(asks) != 3 {
		t.Fatalf("bids=%d asks=%d, want 3 each", len(bids), len(asks))
	}
	for d := 0; d < 3; d++ {
		bidDist := 50_000 - bids[d].Price
		askDist := asks[d].Price - 50_000
		if math.Abs(bidDist-askDist) > 0.02 {
			t.Errorf("level %d asymmetric: bid_dist=%v ask_dist=%v", d, bidDist, askDist)
		}
	}
}

func TestQuote_TickAlignment(t *testing.T) {
	quotes := Build(QuoteParams{
		Mid:          50_000.123,
		SpreadBps:    100,
		BaseQuantity: 1,
		DepthLevels:  2,
		DepthAlpha:   0.6,
		DepthGamma:   1.5,
		TickSize:     0.5,
		LotSize:      0.001,
	})
	for _, q := range quotes {
		if !aligned(q.Price, 0.5) {
			t.Errorf("price %v not aligned to tick 0.5", q.Price)
		}
	}
}

func TestQuote_BidsBelowMidAsksAbove(t *testing.T) {
	quotes := Build(QuoteParams{
		Mid:          150,
		SpreadBps:    80,
		BaseQuantity: 5,
		DepthLevels:  3,
		DepthAlpha:   0.6,
		DepthGamma:   1.5,
		TickSize:     0.01,
		LotSize:      0.001,
	})
	for _, q := range quotes {
		if q.Side == exchange.Buy && q.Price >= 150 {
			t.Errorf("bid price %v >= mid 150", q.Price)
		}
		if q.Side == exchange.Sell && q.Price <= 150 {
			t.Errorf("ask price %v <= mid 150", q.Price)
		}
	}
}

func TestQuote_MaxPositionScaling(t *testing.T) {
	quotes := Build(QuoteParams{
		Mid:                  100,
		SpreadBps:            50,
		BaseQuantity:         5,
		DepthLevels:          3,
		DepthAlpha:           0.6,
		DepthGamma:           1.5,
		TickSize:             0.01,
		LotSize:              0.001,
		MaxPositionContracts: 10,
	})
	var bid, ask float64
	for _, q := range quotes {
		if q.Side == exchange.Buy {
			bid += q.Size
		} else {
			ask += q.Size
		}
	}
	if bid > 10.001 {
		t.Errorf("bid sum %v exceeds cap 10", bid)
	}
	if ask > 10.001 {
		t.Errorf("ask sum %v exceeds cap 10", ask)
	}
}

func TestQuote_ZeroInputsReturnNil(t *testing.T) {
	if got := Build(QuoteParams{}); got != nil {
		t.Errorf("expected nil, got %v", got)
	}
	if got := Build(QuoteParams{Mid: 100, SpreadBps: 50, BaseQuantity: 1}); got != nil {
		t.Errorf("expected nil with DepthLevels=0, got %v", got)
	}
}

func TestRoundTick(t *testing.T) {
	if got := roundDownToTick(50_000.7, 1); got != 50_000 {
		t.Errorf("roundDown 50000.7/1 = %v, want 50000", got)
	}
	if got := roundUpToTick(50_000.1, 1); got != 50_001 {
		t.Errorf("roundUp 50000.1/1 = %v, want 50001", got)
	}
	if got := roundDownToTick(50_000.7, 0); got != 50_000.7 {
		t.Errorf("zero tick should be no-op, got %v", got)
	}
}

func TestQuote_PullBuysOnlyAsks(t *testing.T) {
	quotes := Build(QuoteParams{
		Mid: 100, SpreadBps: 50, BaseQuantity: 1, DepthLevels: 3,
		DepthAlpha: 0.6, DepthGamma: 1.5, TickSize: 0.01, LotSize: 0.001,
		PullBuys: true,
	})
	for _, q := range quotes {
		if q.Side == exchange.Buy {
			t.Errorf("expected no bids when PullBuys=true, got %+v", q)
		}
	}
	if len(quotes) != 3 {
		t.Errorf("expected 3 asks, got %d", len(quotes))
	}
}

func TestQuote_PullSellsOnlyBids(t *testing.T) {
	quotes := Build(QuoteParams{
		Mid: 100, SpreadBps: 50, BaseQuantity: 1, DepthLevels: 3,
		DepthAlpha: 0.6, DepthGamma: 1.5, TickSize: 0.01, LotSize: 0.001,
		PullSells: true,
	})
	for _, q := range quotes {
		if q.Side == exchange.Sell {
			t.Errorf("expected no asks when PullSells=true, got %+v", q)
		}
	}
	if len(quotes) != 3 {
		t.Errorf("expected 3 bids, got %d", len(quotes))
	}
}

func TestQuote_MidShiftLowersBothSides(t *testing.T) {
	base := Build(QuoteParams{
		Mid: 100, SpreadBps: 100, BaseQuantity: 1, DepthLevels: 1,
		DepthAlpha: 0.6, DepthGamma: 1.5, TickSize: 0.001, LotSize: 0.001,
	})
	shifted := Build(QuoteParams{
		Mid: 100, SpreadBps: 100, BaseQuantity: 1, DepthLevels: 1,
		DepthAlpha: 0.6, DepthGamma: 1.5, TickSize: 0.001, LotSize: 0.001,
		MidShiftBps: -10, // shift mid down 10 bps = 0.1 on a $100 mid
	})
	bb, _ := pickOne(base, exchange.Buy)
	sb, _ := pickOne(shifted, exchange.Buy)
	ba, _ := pickOne(base, exchange.Sell)
	sa, _ := pickOne(shifted, exchange.Sell)
	if !(sb.Price < bb.Price) {
		t.Errorf("bid should drop with negative shift: base=%v shifted=%v", bb.Price, sb.Price)
	}
	if !(sa.Price < ba.Price) {
		t.Errorf("ask should drop with negative shift: base=%v shifted=%v", ba.Price, sa.Price)
	}
}

func TestQuote_ExtraBidBpsWidensBidsOnly(t *testing.T) {
	base := Build(QuoteParams{
		Mid: 100, SpreadBps: 50, BaseQuantity: 1, DepthLevels: 1,
		DepthAlpha: 0.6, DepthGamma: 1.5, TickSize: 0.001, LotSize: 0.001,
	})
	wide := Build(QuoteParams{
		Mid: 100, SpreadBps: 50, BaseQuantity: 1, DepthLevels: 1,
		DepthAlpha: 0.6, DepthGamma: 1.5, TickSize: 0.001, LotSize: 0.001,
		ExtraBidBps: 25,
	})
	bb, _ := pickOne(base, exchange.Buy)
	wb, _ := pickOne(wide, exchange.Buy)
	ba, _ := pickOne(base, exchange.Sell)
	wa, _ := pickOne(wide, exchange.Sell)
	if !(wb.Price < bb.Price) {
		t.Errorf("bid should move further from mid: base=%v wide=%v", bb.Price, wb.Price)
	}
	if math.Abs(wa.Price-ba.Price) > 0.001 {
		t.Errorf("ask should be unchanged: base=%v wide=%v", ba.Price, wa.Price)
	}
}

func pickOne(qs []Quote, s exchange.Side) (Quote, bool) {
	for _, q := range qs {
		if q.Side == s {
			return q, true
		}
	}
	return Quote{}, false
}

func side(qs []Quote, s exchange.Side) []Quote {
	out := make([]Quote, 0, len(qs))
	for _, q := range qs {
		if q.Side == s {
			out = append(out, q)
		}
	}
	return out
}

func aligned(price, tick float64) bool {
	r := math.Mod(price, tick)
	return r < 1e-9 || math.Abs(r-tick) < 1e-9
}
