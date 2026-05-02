package engine

import (
	"context"
	"errors"
	"testing"

	"go.uber.org/zap"

	"github.com/jkhatri23/Market-Maker/internal/exchange"
)

func TestReconcile_HappyPath(t *testing.T) {
	mock := newMockExchange()
	mock.openOrders = []exchange.Order{
		{ID: "old-1", Instrument: "BTC", Side: exchange.Buy, Price: 49_000, Size: 1},
		{ID: "old-2", Instrument: "BTC", Side: exchange.Sell, Price: 51_000, Size: 1},
	}
	r := NewReconciler(mock, "BTC", zap.NewNop())

	desired := []Quote{
		{Side: exchange.Buy, Price: 49_500, Size: 1, Level: 0},
		{Side: exchange.Sell, Price: 50_500, Size: 1, Level: 0},
	}
	res, err := r.Reconcile(context.Background(), desired)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Canceled != 2 {
		t.Errorf("Canceled = %d, want 2", res.Canceled)
	}
	if res.Placed != 2 {
		t.Errorf("Placed = %d, want 2", res.Placed)
	}
	if res.Failed != 0 {
		t.Errorf("Failed = %d, want 0", res.Failed)
	}
	if mock.cancelAllCalls != 1 {
		t.Errorf("cancelAllCalls = %d, want 1", mock.cancelAllCalls)
	}
	if len(mock.placedOrders) != 2 {
		t.Errorf("placedOrders = %d, want 2", len(mock.placedOrders))
	}
	for _, p := range mock.placedOrders {
		if !p.PostOnly {
			t.Errorf("expected PostOnly=true, got false on %+v", p)
		}
		if p.ClientOrderID == "" {
			t.Errorf("expected ClientOrderID set, got empty")
		}
	}
}

func TestReconcile_NoOpenOrdersSkipsCancelAll(t *testing.T) {
	mock := newMockExchange()
	r := NewReconciler(mock, "BTC", zap.NewNop())

	res, err := r.Reconcile(context.Background(), []Quote{
		{Side: exchange.Buy, Price: 100, Size: 1, Level: 0},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mock.cancelAllCalls != 0 {
		t.Errorf("expected no CancelAll when no open orders, got %d", mock.cancelAllCalls)
	}
	if res.Placed != 1 {
		t.Errorf("Placed = %d, want 1", res.Placed)
	}
}

func TestReconcile_PlaceFailureCounted(t *testing.T) {
	mock := newMockExchange()
	mock.placeError = errors.New("rate limit")
	r := NewReconciler(mock, "BTC", zap.NewNop())

	res, err := r.Reconcile(context.Background(), []Quote{
		{Side: exchange.Buy, Price: 100, Size: 1, Level: 0},
		{Side: exchange.Sell, Price: 110, Size: 1, Level: 0},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Placed != 0 {
		t.Errorf("Placed = %d, want 0", res.Placed)
	}
	if res.Failed != 2 {
		t.Errorf("Failed = %d, want 2", res.Failed)
	}
}

func TestReconcile_CancelAllErrorAborts(t *testing.T) {
	mock := newMockExchange()
	mock.openOrders = []exchange.Order{{ID: "x", Instrument: "BTC"}}
	mock.cancelAllError = errors.New("auth fail")
	r := NewReconciler(mock, "BTC", zap.NewNop())

	_, err := r.Reconcile(context.Background(), []Quote{
		{Side: exchange.Buy, Price: 100, Size: 1, Level: 0},
	})
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if len(mock.placedOrders) != 0 {
		t.Errorf("expected no places after cancel-all failure, got %d", len(mock.placedOrders))
	}
}
