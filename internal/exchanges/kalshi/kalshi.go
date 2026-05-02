package kalshi

import (
	"context"
	"crypto/rsa"
	"errors"
	"net/http"
	"time"

	"go.uber.org/zap"

	"github.com/jkhatri23/Market-Maker/internal/config"
	"github.com/jkhatri23/Market-Maker/internal/exchange"
)

var errStub = errors.New("kalshi: Timeless API endpoints not yet documented — see internal/exchanges/kalshi/kalshi.go TODOs")

type Client struct {
	cfg    config.KalshiConfig
	priv   *rsa.PrivateKey
	keyID  string
	http   *http.Client
	logger *zap.Logger
}

func New(cfg config.KalshiConfig, logger *zap.Logger) (*Client, error) {
	if cfg.BaseURL == "" {
		return nil, errors.New("kalshi: base_url required")
	}
	if cfg.PrivateKeyPath == "" {
		return nil, errors.New("kalshi: private_key_path required")
	}
	if cfg.KeyID == "" {
		return nil, errors.New("kalshi: key_id required")
	}
	priv, err := LoadPrivateKey(cfg.PrivateKeyPath)
	if err != nil {
		return nil, err
	}
	return &Client{
		cfg:    cfg,
		priv:   priv,
		keyID:  cfg.KeyID,
		http:   &http.Client{Timeout: 10 * time.Second},
		logger: logger,
	}, nil
}

func (c *Client) Name() string { return "kalshi" }

func (c *Client) SubscribeBook(ctx context.Context, instrument string) (<-chan exchange.BookUpdate, error) {
	return nil, errStub
}

func (c *Client) GetMarketSpec(ctx context.Context, instrument string) (exchange.MarketSpec, error) {
	return exchange.MarketSpec{}, errStub
}

func (c *Client) GetFundingRate(ctx context.Context, instrument string) (exchange.FundingRate, error) {
	return exchange.FundingRate{}, errStub
}

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
