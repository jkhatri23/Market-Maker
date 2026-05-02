package hedger

import (
	"context"
	"testing"

	"go.uber.org/zap"

	"github.com/jkhatri23/Market-Maker/internal/exchange"
)

type mockExchange struct {
	exchange.Exchange
	placed []exchange.OrderRequest
}

func (m *mockExchange) Name() string { return "mock" }

func (m *mockExchange) PlaceOrder(ctx context.Context, req exchange.OrderRequest) (exchange.Order, error) {
	m.placed = append(m.placed, req)
	return exchange.Order{ID: "123", Status: exchange.OrderFilled}, nil
}

func TestSimpleHedger_OnFill(t *testing.T) {
	mock := &mockExchange{}
	s := NewSimple(mock, zap.NewNop())

	fill := exchange.Fill{
		Instrument: "SOL",
		Side:       exchange.Buy,
		Size:       1.5,
		Price:      100.0,
		IsMaker:    true,
	}

	err := s.OnFill(context.Background(), fill)
	if err != nil {
		t.Fatalf("OnFill failed: %v", err)
	}

	if len(mock.placed) != 1 {
		t.Fatalf("expected 1 order placed, got %d", len(mock.placed))
	}

	req := mock.placed[0]
	if req.Instrument != "SOL" {
		t.Errorf("expected instrument SOL, got %s", req.Instrument)
	}
	if req.Side != exchange.Sell {
		t.Errorf("expected side SELL, got %s", req.Side)
	}
	if req.Size != 1.5 {
		t.Errorf("expected size 1.5, got %f", req.Size)
	}
	if req.Type != exchange.OrderTypeMarket {
		t.Errorf("expected type MARKET, got %s", req.Type)
	}
}
