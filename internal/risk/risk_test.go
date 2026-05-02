package risk

import (
	"math"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/jkhatri23/Market-Maker/internal/config"
	"github.com/jkhatri23/Market-Maker/internal/exchange"
)

// ---------- Skew ----------

func TestSkew_LongShiftsDown(t *testing.T) {
	got := Skew(SkewParams{NetPosition: 5, MaxPosition: 10, SpreadBps: 50, SkewFactor: 0.4})
	if got >= 0 {
		t.Errorf("expected negative shift when long, got %v", got)
	}
}

func TestSkew_FlatNoShift(t *testing.T) {
	if got := Skew(SkewParams{NetPosition: 0, MaxPosition: 10, SpreadBps: 50, SkewFactor: 0.4}); got != 0 {
		t.Errorf("expected 0 shift when flat, got %v", got)
	}
}

func TestSkew_SymmetricLongShort(t *testing.T) {
	long := Skew(SkewParams{NetPosition: 7, MaxPosition: 10, SpreadBps: 50, SkewFactor: 0.4})
	short := Skew(SkewParams{NetPosition: -7, MaxPosition: 10, SpreadBps: 50, SkewFactor: 0.4})
	if math.Abs(long+short) > 1e-9 {
		t.Errorf("long %v and short %v should be opposite", long, short)
	}
}

func TestSkew_ClampsAtMax(t *testing.T) {
	got := Skew(SkewParams{NetPosition: 100, MaxPosition: 10, SpreadBps: 50, SkewFactor: 0.4})
	want := -0.4 * 50.0 // ratio clamped to 1
	if math.Abs(got-want) > 1e-9 {
		t.Errorf("clamped shift = %v, want %v", got, want)
	}
}

// ---------- Book ----------

func TestBook_OpenAndClose(t *testing.T) {
	b := NewBook()
	b.ApplyFill("BTC", exchange.Buy, 50_000, 1, 0)
	if got := b.Position("BTC"); got != 1 {
		t.Errorf("position after buy = %v, want 1", got)
	}
	b.ApplyFill("BTC", exchange.Sell, 50_500, 1, 0)
	if got := b.Position("BTC"); got != 0 {
		t.Errorf("position after close = %v, want 0", got)
	}
	if got := b.Realized("BTC"); math.Abs(got-500) > 1e-6 {
		t.Errorf("realized = %v, want 500", got)
	}
}

func TestBook_WeightedAverageOnExtend(t *testing.T) {
	b := NewBook()
	b.ApplyFill("BTC", exchange.Buy, 50_000, 1, 0)
	b.ApplyFill("BTC", exchange.Buy, 51_000, 1, 0)
	// avg should be 50,500
	b.ApplyFill("BTC", exchange.Sell, 51_000, 2, 0)
	// realized = (51000 - 50500) * 2 = 1000
	if got := b.Realized("BTC"); math.Abs(got-1000) > 1e-6 {
		t.Errorf("realized = %v, want 1000", got)
	}
}

func TestBook_FlipPositionRealizesAndOpensNew(t *testing.T) {
	b := NewBook()
	b.ApplyFill("SOL", exchange.Buy, 100, 1, 0)
	b.ApplyFill("SOL", exchange.Sell, 110, 3, 0) // close 1 long @ +10, open 2 short @ 110
	// realized from close: (110 - 100) * 1 = 10
	if got := b.Realized("SOL"); math.Abs(got-10) > 1e-6 {
		t.Errorf("realized after flip = %v, want 10", got)
	}
	if got := b.Position("SOL"); got != -2 {
		t.Errorf("position after flip = %v, want -2", got)
	}
}

func TestBook_FeesReduceRealized(t *testing.T) {
	b := NewBook()
	b.ApplyFill("BTC", exchange.Buy, 50_000, 1, 5)
	b.ApplyFill("BTC", exchange.Sell, 50_500, 1, 5)
	// gross = 500, fees = 10
	if got := b.Realized("BTC"); math.Abs(got-490) > 1e-6 {
		t.Errorf("realized net of fees = %v, want 490", got)
	}
}

func TestBook_NetPnLIncludesUnrealized(t *testing.T) {
	b := NewBook()
	b.ApplyFill("BTC", exchange.Buy, 50_000, 1, 0)
	b.ApplyMark("BTC", 50_300)
	// unrealized = (50300 - 50000) * 1 = 300
	if got := b.NetPnL(); math.Abs(got-300) > 1e-6 {
		t.Errorf("NetPnL = %v, want 300", got)
	}
}

// ---------- Manager ----------

func newTestManager(t *testing.T) *Manager {
	t.Helper()
	cfg := config.RiskConfig{
		DailyDrawdownHaltUSD:   100,
		DislocationPct:         0.02,
		DislocationWindow:      30 * time.Second,
		FillIntensityWindow:    2 * time.Second,
		FillIntensityThreshold: 3,
	}
	return NewManager(cfg, 5*time.Second, zap.NewNop())
}

func defaultAsset() config.AssetConfig {
	return config.AssetConfig{
		Symbol: "BTC", Enabled: true, BaseQuantity: 1, SpreadBps: 50,
		MaxPosition: 5, MaxLeverage: 2.0, DepthLevels: 3, DepthAlpha: 0.6,
		DepthGamma: 1.5, SkewFactor: 0.4,
	}
}

func TestManager_AllowsWhenHealthy(t *testing.T) {
	m := newTestManager(t)
	v := m.Check(defaultAsset(), 50_000, time.Second)
	if !v.Allow {
		t.Errorf("expected Allow=true, got %+v", v)
	}
	if v.MidShiftBps != 0 {
		t.Errorf("flat position should mean zero skew, got %v", v.MidShiftBps)
	}
}

func TestManager_StaleFeedDenies(t *testing.T) {
	m := newTestManager(t)
	v := m.Check(defaultAsset(), 50_000, 30*time.Second)
	if v.Allow || v.Reason == "" {
		t.Errorf("expected deny on stale feed, got %+v", v)
	}
}

func TestManager_HaltSticky(t *testing.T) {
	m := newTestManager(t)
	m.Halt("test")
	v := m.Check(defaultAsset(), 50_000, time.Second)
	if v.Allow {
		t.Errorf("expected halted, got Allow=true")
	}
	m.Reset()
	v = m.Check(defaultAsset(), 50_000, time.Second)
	if !v.Allow {
		t.Errorf("expected allow after reset, got %+v", v)
	}
}

func TestManager_DrawdownTriggersStickyHalt(t *testing.T) {
	m := newTestManager(t)
	// Open BTC long, then mark down to bake unrealized loss > $100.
	m.ApplyFill(exchange.Fill{Instrument: "BTC", Side: exchange.Buy, Price: 50_000, Size: 1})
	m.ApplyMark("BTC", 49_800) // -$200 unrealized

	v := m.Check(defaultAsset(), 49_800, time.Second)
	if v.Allow {
		t.Errorf("expected drawdown halt, got Allow=true")
	}
	if _, halted := m.IsHalted(); !halted {
		t.Errorf("expected sticky halt after drawdown")
	}
}

func TestManager_PositionCapPullsBuys(t *testing.T) {
	m := newTestManager(t)
	m.ApplyFill(exchange.Fill{Instrument: "BTC", Side: exchange.Buy, Price: 50_000, Size: 5})
	v := m.Check(defaultAsset(), 50_000, time.Second)
	if !v.Allow {
		t.Errorf("expected allow, got %+v", v)
	}
	if !v.PullBuys {
		t.Errorf("expected PullBuys=true at +max position")
	}
	if v.PullSells {
		t.Errorf("expected PullSells=false at +max position")
	}
	if v.MidShiftBps >= 0 {
		t.Errorf("expected negative skew when long, got %v", v.MidShiftBps)
	}
}

func TestManager_DislocationDenies(t *testing.T) {
	m := newTestManager(t)
	a := defaultAsset()
	v := m.Check(a, 50_000, time.Second)
	if !v.Allow {
		t.Errorf("first tick should allow, got %+v", v)
	}
	v = m.Check(a, 51_500, time.Second) // 3% jump within window
	if v.Allow {
		t.Errorf("expected dislocation deny, got %+v", v)
	}
}

func TestManager_FillIntensityWidensOneSide(t *testing.T) {
	m := newTestManager(t)
	for i := 0; i < 4; i++ {
		m.ApplyFill(exchange.Fill{Instrument: "BTC", Side: exchange.Buy, Price: 50_000, Size: 0.1})
	}
	// 4 buy fills > threshold(3) → expect ExtraBidBps > 0
	a := defaultAsset()
	a.MaxPosition = 100 // avoid hitting position cap
	v := m.Check(a, 50_000, time.Second)
	if v.ExtraBidBps <= 0 {
		t.Errorf("expected ExtraBidBps > 0 after sweep, got %v", v.ExtraBidBps)
	}
	if v.ExtraAskBps != 0 {
		t.Errorf("expected ExtraAskBps == 0, got %v", v.ExtraAskBps)
	}
}
