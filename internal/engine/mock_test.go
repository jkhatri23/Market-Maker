package engine

import (
	"context"
	"fmt"

	"github.com/jkhatri23/Market-Maker/internal/exchange"
)

// mockExchange is a test double for the exchange.Exchange interface. Only
// the methods exercised by the engine tests are interesting; the rest are
// no-ops that return zero values or errors so the contract is satisfied.
type mockExchange struct {
	openOrders     []exchange.Order
	placedOrders   []exchange.OrderRequest
	cancelAllCalls int

	placeError     error
	cancelAllError error
	getOpenError   error
}

func newMockExchange() *mockExchange { return &mockExchange{} }

func (m *mockExchange) Name() string { return "mock" }

func (m *mockExchange) SubscribeBook(ctx context.Context, instrument string) (<-chan exchange.BookUpdate, error) {
	ch := make(chan exchange.BookUpdate)
	close(ch)
	return ch, nil
}

func (m *mockExchange) GetMarketSpec(ctx context.Context, instrument string) (exchange.MarketSpec, error) {
	return exchange.MarketSpec{Instrument: instrument, TickSize: 0.01, LotSize: 0.001, MinSize: 0.001}, nil
}

func (m *mockExchange) GetFundingRate(ctx context.Context, instrument string) (exchange.FundingRate, error) {
	return exchange.FundingRate{Instrument: instrument}, nil
}

func (m *mockExchange) PlaceOrder(ctx context.Context, req exchange.OrderRequest) (exchange.Order, error) {
	if m.placeError != nil {
		return exchange.Order{}, m.placeError
	}
	m.placedOrders = append(m.placedOrders, req)
	return exchange.Order{
		ID:            fmt.Sprintf("ord-%d", len(m.placedOrders)),
		ClientOrderID: req.ClientOrderID,
		Instrument:    req.Instrument,
		Side:          req.Side,
		Type:          req.Type,
		Price:         req.Price,
		Size:          req.Size,
		Status:        exchange.OrderOpen,
		PostOnly:      req.PostOnly,
	}, nil
}

func (m *mockExchange) CancelOrder(ctx context.Context, instrument, orderID string) error {
	return nil
}

func (m *mockExchange) CancelAllForInstrument(ctx context.Context, instrument string) error {
	m.cancelAllCalls++
	if m.cancelAllError != nil {
		return m.cancelAllError
	}
	m.openOrders = nil
	return nil
}

func (m *mockExchange) GetOpenOrders(ctx context.Context, instrument string) ([]exchange.Order, error) {
	if m.getOpenError != nil {
		return nil, m.getOpenError
	}
	return m.openOrders, nil
}

func (m *mockExchange) SubscribeFills(ctx context.Context) (<-chan exchange.Fill, error) {
	ch := make(chan exchange.Fill)
	close(ch)
	return ch, nil
}

func (m *mockExchange) GetPosition(ctx context.Context, instrument string) (exchange.Position, error) {
	return exchange.Position{Instrument: instrument}, nil
}

func (m *mockExchange) GetBalance(ctx context.Context) (exchange.Balance, error) {
	return exchange.Balance{}, nil
}
