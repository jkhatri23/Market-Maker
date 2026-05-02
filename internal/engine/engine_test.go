package engine

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/jkhatri23/Market-Maker/internal/config"
	"github.com/jkhatri23/Market-Maker/internal/exchange"
	"github.com/jkhatri23/Market-Maker/internal/risk"
)

type fillMockExchange struct {
	*mockExchange
	ch chan exchange.Fill
}

func (m *fillMockExchange) SubscribeFills(ctx context.Context) (<-chan exchange.Fill, error) {
	return m.ch, nil
}

type mockHedger struct {
	mu    sync.Mutex
	fills []exchange.Fill
}

func (m *mockHedger) OnFill(ctx context.Context, f exchange.Fill) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.fills = append(m.fills, f)
	return nil
}

func (m *mockHedger) getFills() []exchange.Fill {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.fills
}

func TestEngine_PumpFills(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	maker := &fillMockExchange{
		mockExchange: newMockExchange(),
		ch:           make(chan exchange.Fill, 10),
	}
	h := &mockHedger{}
	mgr := risk.NewManager(config.RiskConfig{DailyDrawdownHaltUSD: 1000}, 0, zap.NewNop())

	e := New(&config.Config{}, Deps{
		Maker:  maker,
		Hedger: h,
		Risk:   mgr,
	}, zap.NewNop())

	go func() {
		_ = e.pumpFills(ctx, maker, h)
	}()

	fill := exchange.Fill{
		Instrument: "SOL",
		Side:       exchange.Buy,
		Size:       1.0,
		Price:      100.0,
		IsMaker:    true,
	}

	maker.ch <- fill

	// Give it a moment to process.
	time.Sleep(100 * time.Millisecond)

	fills := h.getFills()
	if len(fills) != 1 {
		t.Fatalf("expected 1 fill hedged, got %d", len(fills))
	}

	if fills[0].Instrument != "SOL" {
		t.Errorf("expected SOL, got %s", fills[0].Instrument)
	}

	// Verify risk was applied.
	snap := mgr.Book.Snapshot("SOL")
	if snap.Position != 1.0 {
		t.Errorf("expected position 1.0, got %f", snap.Position)
	}
}
