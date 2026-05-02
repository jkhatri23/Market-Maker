package paper

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/jkhatri23/Market-Maker/internal/exchange"
	"github.com/jkhatri23/Market-Maker/internal/pricefeed"
)

// stubFeed is a minimal PriceFeed used to drive the matcher.
type stubFeed struct {
	mu   sync.Mutex
	chs  map[string]chan pricefeed.PriceUpdate
	last map[string]pricefeed.PriceUpdate
}

func newStubFeed() *stubFeed {
	return &stubFeed{
		chs:  map[string]chan pricefeed.PriceUpdate{},
		last: map[string]pricefeed.PriceUpdate{},
	}
}

func (s *stubFeed) Name() string { return "stub" }

func (s *stubFeed) Run(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }

func (s *stubFeed) Subscribe(asset string) (<-chan pricefeed.PriceUpdate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ch, ok := s.chs[asset]; ok {
		return ch, nil
	}
	ch := make(chan pricefeed.PriceUpdate, 16)
	s.chs[asset] = ch
	return ch, nil
}

func (s *stubFeed) GetPrice(asset string) (float64, time.Duration, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if p, ok := s.last[asset]; ok {
		return p.Price, time.Since(p.Timestamp), true
	}
	return 0, 0, false
}

func (s *stubFeed) push(asset string, price float64) {
	s.mu.Lock()
	upd := pricefeed.PriceUpdate{Asset: asset, Price: price, Timestamp: time.Now()}
	s.last[asset] = upd
	ch := s.chs[asset]
	s.mu.Unlock()
	if ch != nil {
		ch <- upd
	}
}

// ---------- Tests ----------

func newTestExchange(t *testing.T) (*Exchange, *stubFeed) {
	t.Helper()
	feed := newStubFeed()
	specs := map[string]exchange.MarketSpec{
		"BTC": {Instrument: "BTC", TickSize: 0.01, LotSize: 0.001, MinSize: 0.001},
		"SOL": {Instrument: "SOL", TickSize: 0.001, LotSize: 0.01, MinSize: 0.01},
	}
	pe := New("paper", feed, specs, 5_000, zap.NewNop())
	return pe, feed
}

func TestPaper_PlaceAndList(t *testing.T) {
	pe, _ := newTestExchange(t)
	ctx := context.Background()
	ord, err := pe.PlaceOrder(ctx, exchange.OrderRequest{
		Instrument: "BTC", Side: exchange.Buy, Type: exchange.OrderTypeLimit,
		Price: 49_000, Size: 1, PostOnly: true, ClientOrderID: "test-1",
	})
	if err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	if ord.ID == "" {
		t.Errorf("expected venue order ID")
	}
	if ord.Status != exchange.OrderOpen {
		t.Errorf("status = %v, want open", ord.Status)
	}

	open, err := pe.GetOpenOrders(ctx, "BTC")
	if err != nil {
		t.Fatalf("GetOpenOrders: %v", err)
	}
	if len(open) != 1 {
		t.Errorf("len(open) = %d, want 1", len(open))
	}
}

func TestPaper_CancelAllRemovesEverything(t *testing.T) {
	pe, _ := newTestExchange(t)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		_, _ = pe.PlaceOrder(ctx, exchange.OrderRequest{
			Instrument: "BTC", Side: exchange.Buy, Type: exchange.OrderTypeLimit,
			Price: 49_000 - float64(i), Size: 1,
		})
	}
	if err := pe.CancelAllForInstrument(ctx, "BTC"); err != nil {
		t.Fatalf("CancelAll: %v", err)
	}
	open, _ := pe.GetOpenOrders(ctx, "BTC")
	if len(open) != 0 {
		t.Errorf("expected 0 after cancel-all, got %d", len(open))
	}
}

func TestPaper_RejectsUnknownInstrument(t *testing.T) {
	pe, _ := newTestExchange(t)
	_, err := pe.PlaceOrder(context.Background(), exchange.OrderRequest{
		Instrument: "DOGE", Side: exchange.Buy, Price: 1, Size: 1,
	})
	if err == nil {
		t.Errorf("expected error on unknown instrument")
	}
}

func TestPaper_FillBidWhenPriceCrosses(t *testing.T) {
	pe, feed := newTestExchange(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fills, _ := pe.SubscribeFills(ctx)

	_, _ = pe.PlaceOrder(ctx, exchange.OrderRequest{
		Instrument: "BTC", Side: exchange.Buy, Type: exchange.OrderTypeLimit,
		Price: 49_500, Size: 1,
	})

	go func() { _ = pe.Run(ctx) }()
	time.Sleep(20 * time.Millisecond) // let runAsset subscribe

	// Push price NOT crossing — bid is at 49500, price stays at 50000.
	feed.push("BTC", 50_000)
	select {
	case f := <-fills:
		t.Fatalf("unexpected fill at 50000: %+v", f)
	case <-time.After(100 * time.Millisecond):
	}

	// Now price drops to 49,400 — below our 49,500 bid → fill.
	feed.push("BTC", 49_400)
	select {
	case f := <-fills:
		if f.Side != exchange.Buy {
			t.Errorf("side = %v, want buy", f.Side)
		}
		if f.Price != 49_500 {
			t.Errorf("fill price = %v, want 49500 (order price, not touch)", f.Price)
		}
		if !f.IsMaker {
			t.Errorf("expected maker fill")
		}
	case <-time.After(time.Second):
		t.Fatal("expected fill, got none")
	}

	// Position should reflect the fill.
	pos, _ := pe.GetPosition(ctx, "BTC")
	if pos.NetSize != 1 {
		t.Errorf("position = %v, want 1", pos.NetSize)
	}

	// Order should no longer be open.
	open, _ := pe.GetOpenOrders(ctx, "BTC")
	if len(open) != 0 {
		t.Errorf("expected 0 open, got %d", len(open))
	}
}

func TestPaper_FillAskWhenPriceCrosses(t *testing.T) {
	pe, feed := newTestExchange(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fills, _ := pe.SubscribeFills(ctx)
	_, _ = pe.PlaceOrder(ctx, exchange.OrderRequest{
		Instrument: "BTC", Side: exchange.Sell, Type: exchange.OrderTypeLimit,
		Price: 50_500, Size: 1,
	})
	go func() { _ = pe.Run(ctx) }()
	time.Sleep(20 * time.Millisecond)

	feed.push("BTC", 50_600)
	select {
	case f := <-fills:
		if f.Side != exchange.Sell {
			t.Errorf("side = %v, want sell", f.Side)
		}
		if f.Price != 50_500 {
			t.Errorf("fill price = %v, want 50500", f.Price)
		}
	case <-time.After(time.Second):
		t.Fatal("expected ask fill, got none")
	}

	pos, _ := pe.GetPosition(ctx, "BTC")
	if pos.NetSize != -1 {
		t.Errorf("position = %v, want -1", pos.NetSize)
	}
}

func TestPaper_GetBalance(t *testing.T) {
	pe, _ := newTestExchange(t)
	bal, err := pe.GetBalance(context.Background())
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if bal.Total != 5_000 {
		t.Errorf("balance = %v, want 5000", bal.Total)
	}
}
