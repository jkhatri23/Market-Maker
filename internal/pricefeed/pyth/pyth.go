// Package pyth implements pricefeed.PriceFeed against the Pyth Hermes
// WebSocket endpoint (wss://hermes.pyth.network/ws).
//
// The Hermes message format is not formally versioned; this client targets
// the current `price_update` envelope:
//
//	{
//	  "type": "price_update",
//	  "price_feed": {
//	    "id": "<hex>",
//	    "price": { "price": "<int>", "conf": "<int>", "expo": <int>, "publish_time": <unix> }
//	  }
//	}
//
// If the live shape diverges, only handleUpdate needs to change — the
// reconnect/subscribe machinery is shape-agnostic.
package pyth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"

	"github.com/jkhatri23/Market-Maker/internal/pricefeed"
)

type Client struct {
	wsURL     string
	feedIDs   map[string]string // asset → hex id (no 0x prefix)
	idToAsset map[string]string // reverse lookup
	logger    *zap.Logger

	mu   sync.RWMutex
	last map[string]pricefeed.PriceUpdate
	subs map[string][]chan pricefeed.PriceUpdate
}

func New(wsURL string, feedIDs map[string]string, logger *zap.Logger) (*Client, error) {
	if wsURL == "" {
		return nil, errors.New("pyth: ws_url required")
	}
	if len(feedIDs) == 0 {
		return nil, errors.New("pyth: at least one feed id required")
	}

	normalized := make(map[string]string, len(feedIDs))
	idToAsset := make(map[string]string, len(feedIDs))
	for asset, id := range feedIDs {
		key := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(id)), "0x")
		if key == "" {
			return nil, fmt.Errorf("pyth: empty feed id for asset %q", asset)
		}
		normalized[asset] = key
		idToAsset[key] = asset
	}
	return &Client{
		wsURL:     wsURL,
		feedIDs:   normalized,
		idToAsset: idToAsset,
		logger:    logger,
		last:      map[string]pricefeed.PriceUpdate{},
		subs:      map[string][]chan pricefeed.PriceUpdate{},
	}, nil
}

func (c *Client) Name() string { return "pyth" }

func (c *Client) Subscribe(asset string) (<-chan pricefeed.PriceUpdate, error) {
	if _, ok := c.feedIDs[asset]; !ok {
		return nil, fmt.Errorf("pyth: no feed id configured for %q", asset)
	}
	ch := make(chan pricefeed.PriceUpdate, 16)
	c.mu.Lock()
	c.subs[asset] = append(c.subs[asset], ch)
	c.mu.Unlock()
	return ch, nil
}

// GetPrice returns the most recent price + age (now − publish_time, NOT
// receive_time — staleness reflects the upstream publisher, which is
// what the engine cares about for the kill-switch).
func (c *Client) GetPrice(asset string) (float64, time.Duration, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	p, ok := c.last[asset]
	if !ok {
		return 0, 0, false
	}
	return p.Price, time.Since(p.Timestamp), true
}

// Run owns the WS connection and reconnects with capped exponential
// backoff. Returns when ctx is canceled.
func (c *Client) Run(ctx context.Context) error {
	backoff := time.Second
	const maxBackoff = 30 * time.Second
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := c.runOnce(ctx)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		c.logger.Warn("pyth ws disconnected; reconnecting",
			zap.Error(err), zap.Duration("backoff", backoff))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < maxBackoff {
			backoff *= 2
		}
	}
}

func (c *Client) runOnce(ctx context.Context) error {
	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}
	conn, _, err := dialer.DialContext(ctx, c.wsURL, nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	ids := make([]string, 0, len(c.feedIDs))
	for _, id := range c.feedIDs {
		ids = append(ids, id)
	}
	if err := conn.WriteJSON(map[string]any{
		"type": "subscribe",
		"ids":  ids,
	}); err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}
	c.logger.Info("pyth ws connected", zap.Int("feeds", len(ids)))

	const readTimeout = 60 * time.Second
	if err := conn.SetReadDeadline(time.Now().Add(readTimeout)); err != nil {
		return err
	}
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(readTimeout))
	})

	// Tear the connection down when ctx ends — gorilla's ReadMessage
	// blocks otherwise.
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-done:
		}
	}()

	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}
		if err := conn.SetReadDeadline(time.Now().Add(readTimeout)); err != nil {
			return err
		}
		c.dispatch(raw)
	}
}

type wsMessage struct {
	Type      string  `json:"type"`
	PriceFeed wsFeed  `json:"price_feed"`
}

type wsFeed struct {
	ID    string  `json:"id"`
	Price wsPrice `json:"price"`
}

type wsPrice struct {
	Price       string `json:"price"`
	Conf        string `json:"conf"`
	Expo        int    `json:"expo"`
	PublishTime int64  `json:"publish_time"`
}

func (c *Client) dispatch(raw []byte) {
	var msg wsMessage
	if err := json.Unmarshal(raw, &msg); err != nil {
		c.logger.Debug("pyth: drop unparseable message", zap.Error(err))
		return
	}
	if msg.Type != "price_update" {
		return
	}
	c.handleUpdate(msg.PriceFeed)
}

func (c *Client) handleUpdate(f wsFeed) {
	id := strings.TrimPrefix(strings.ToLower(f.ID), "0x")
	asset, ok := c.idToAsset[id]
	if !ok {
		return
	}

	rawPrice, err := strconv.ParseInt(f.Price.Price, 10, 64)
	if err != nil {
		c.logger.Debug("pyth: bad price", zap.String("asset", asset), zap.Error(err))
		return
	}
	rawConf, _ := strconv.ParseInt(f.Price.Conf, 10, 64)

	scale := math.Pow10(f.Price.Expo) // expo is typically negative, e.g. -8
	upd := pricefeed.PriceUpdate{
		Asset:      asset,
		Price:      float64(rawPrice) * scale,
		Confidence: float64(rawConf) * math.Abs(scale),
		Timestamp:  time.Unix(f.Price.PublishTime, 0),
	}

	c.mu.Lock()
	c.last[asset] = upd
	subs := append([]chan pricefeed.PriceUpdate(nil), c.subs[asset]...)
	c.mu.Unlock()

	// Drop on slow consumer rather than block the WS reader — stale prices
	// are worse than dropped ones for an MM bot.
	for _, ch := range subs {
		select {
		case ch <- upd:
		default:
		}
	}
}
