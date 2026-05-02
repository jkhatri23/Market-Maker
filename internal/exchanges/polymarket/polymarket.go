package polymarket

import (
	"context"
	"errors"
	"net/http"
	"time"

	"go.uber.org/zap"

	"github.com/jkhatri23/Market-Maker/internal/config"
	"github.com/jkhatri23/Market-Maker/internal/exchange"
)

// errStub is returned by every endpoint until the perps API ships and we
// fill in the request shapes. Distinct error type so callers can branch.
var errStub = errors.New("polymarket: perps API endpoints not yet published — see internal/exchanges/polymarket/polymarket.go TODOs")

// Client is a stub Polymarket perps exchange client. Auth scaffolding is
// real (see auth.go); endpoint paths and body shapes are TODO.
type Client struct {
	cfg    config.PolymarketConfig
	http   *http.Client
	logger *zap.Logger
}

func New(cfg config.PolymarketConfig, logger *zap.Logger) (*Client, error) {
	if cfg.BaseURL == "" {
		return nil, errors.New("polymarket: base_url required")
	}
	return &Client{
		cfg:    cfg,
		http:   &http.Client{Timeout: 10 * time.Second},
		logger: logger,
	}, nil
}

func (c *Client) Name() string { return "polymarket" }

// SubscribeBook is the WS endpoint. TODO: dial cfg.WSURL, send subscribe
// message for the perps channel, parse depth deltas into BookUpdate.
func (c *Client) SubscribeBook(ctx context.Context, instrument string) (<-chan exchange.BookUpdate, error) {
	return nil, errStub
}

// GetMarketSpec is /markets/<instrument> (or similar). Returns tick, lot,
// min/max, max leverage, maintenance margin. TODO when docs publish.
func (c *Client) GetMarketSpec(ctx context.Context, instrument string) (exchange.MarketSpec, error) {
	return exchange.MarketSpec{}, errStub
}

// GetFundingRate exposes Polymarket's normalized per-hour funding rate.
// Polymarket's settlement window is 5min–8h depending on the contract;
// this method should normalize whatever windowed rate the API returns to
// per-hour before returning. TODO.
func (c *Client) GetFundingRate(ctx context.Context, instrument string) (exchange.FundingRate, error) {
	return exchange.FundingRate{}, errStub
}

// PlaceOrder is the order endpoint. CLOB equivalent is POST /order with
// an EIP-712 signed payload; perps may differ. TODO.
func (c *Client) PlaceOrder(ctx context.Context, req exchange.OrderRequest) (exchange.Order, error) {
	return exchange.Order{}, errStub
}

func (c *Client) CancelOrder(ctx context.Context, instrument, orderID string) error {
	return errStub
}

func (c *Client) CancelAllForInstrument(ctx context.Context, instrument string) error {
	return errStub
}

func (c *Client) GetOpenOrders(ctx context.Context, instrument string) ([]exchange.Order, error) {
	return nil, errStub
}

func (c *Client) SubscribeFills(ctx context.Context) (<-chan exchange.Fill, error) {
	return nil, errStub
}

func (c *Client) GetPosition(ctx context.Context, instrument string) (exchange.Position, error) {
	return exchange.Position{}, errStub
}

func (c *Client) GetBalance(ctx context.Context) (exchange.Balance, error) {
	return exchange.Balance{}, errStub
}
