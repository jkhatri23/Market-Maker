package binance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"

	"github.com/jkhatri23/Market-Maker/internal/exchange"
)

type Config struct {
	BaseURL    string // https://fapi.binance.com
	WSURL      string // wss://fstream.binance.com/ws
	APIKey     string
	APISecret  string
	QuoteAsset string // "USDT" by default
	RecvWindow int    // ms; 5000 default
}

// Client implements exchange.Exchange against USD-M Futures.
type Client struct {
	cfg    Config
	http   *http.Client
	logger *zap.Logger

	mu      sync.RWMutex
	symbols map[string]symbolMeta // engine asset → exchange info
	fillsCh chan exchange.Fill
	wsOnce  sync.Once
}

type symbolMeta struct {
	Symbol      string  // SOLUSDT
	BaseAsset   string  // SOL
	TickSize    float64
	StepSize    float64
	MinQty      float64
	MaxLeverage float64
}

func New(cfg Config, logger *zap.Logger) (*Client, error) {
	if cfg.BaseURL == "" {
		cfg.BaseURL = "https://fapi.binance.com"
	}
	if cfg.WSURL == "" {
		cfg.WSURL = "wss://fstream.binance.com/ws"
	}
	if cfg.QuoteAsset == "" {
		cfg.QuoteAsset = "USDT"
	}
	if cfg.RecvWindow == 0 {
		cfg.RecvWindow = 5000
	}
	if cfg.APIKey == "" || cfg.APISecret == "" {
		return nil, errors.New("binance: api_key and api_secret required")
	}
	return &Client{
		cfg:     cfg,
		http:    &http.Client{Timeout: 10 * time.Second},
		logger:  logger,
		symbols: map[string]symbolMeta{},
		fillsCh: make(chan exchange.Fill, 256),
	}, nil
}

func (c *Client) Name() string { return "binance" }

func (c *Client) symbolFor(asset string) string { return asset + c.cfg.QuoteAsset }

// Bootstrap fetches /fapi/v1/exchangeInfo for the assets we care about.
// Called from GetMarketSpec lazily.
func (c *Client) Bootstrap(ctx context.Context, assets []string) error {
	if len(assets) == 0 {
		return nil
	}
	var resp struct {
		Symbols []struct {
			Symbol     string `json:"symbol"`
			BaseAsset  string `json:"baseAsset"`
			QuoteAsset string `json:"quoteAsset"`
			Filters    []json.RawMessage `json:"filters"`
		} `json:"symbols"`
	}
	if err := c.publicGET(ctx, "/fapi/v1/exchangeInfo", nil, &resp); err != nil {
		return err
	}
	want := map[string]string{}
	for _, a := range assets {
		want[c.symbolFor(a)] = a
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, s := range resp.Symbols {
		asset, ok := want[s.Symbol]
		if !ok {
			continue
		}
		m := symbolMeta{Symbol: s.Symbol, BaseAsset: s.BaseAsset}
		for _, raw := range s.Filters {
			var f struct {
				Type     string `json:"filterType"`
				TickSize string `json:"tickSize"`
				StepSize string `json:"stepSize"`
				MinQty   string `json:"minQty"`
			}
			_ = json.Unmarshal(raw, &f)
			switch f.Type {
			case "PRICE_FILTER":
				m.TickSize, _ = strconv.ParseFloat(f.TickSize, 64)
			case "LOT_SIZE":
				m.StepSize, _ = strconv.ParseFloat(f.StepSize, 64)
				m.MinQty, _ = strconv.ParseFloat(f.MinQty, 64)
			}
		}
		c.symbols[asset] = m
	}
	for asset := range want {
		if _, ok := c.symbols[want[asset]]; !ok {
			c.logger.Warn("binance: asset not found in exchangeInfo", zap.String("symbol", asset))
		}
	}
	return nil
}

func (c *Client) lookup(asset string) (symbolMeta, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	m, ok := c.symbols[asset]
	if !ok {
		return symbolMeta{}, fmt.Errorf("binance: bootstrap not run for %q", asset)
	}
	return m, nil
}

// ---------- Market data ----------

func (c *Client) SubscribeBook(ctx context.Context, instrument string) (<-chan exchange.BookUpdate, error) {
	ch := make(chan exchange.BookUpdate)
	go func() { <-ctx.Done(); close(ch) }()
	return ch, nil
}

func (c *Client) GetMarketSpec(ctx context.Context, instrument string) (exchange.MarketSpec, error) {
	if _, err := c.lookup(instrument); err != nil {
		if err := c.Bootstrap(ctx, []string{instrument}); err != nil {
			return exchange.MarketSpec{}, err
		}
	}
	m, err := c.lookup(instrument)
	if err != nil {
		return exchange.MarketSpec{}, err
	}
	return exchange.MarketSpec{
		Instrument: instrument,
		TickSize:   m.TickSize,
		LotSize:    m.StepSize,
		MinSize:    m.MinQty,
		MaxLeverage: 0, // returned per-symbol elsewhere; 0 means "unknown"
	}, nil
}

func (c *Client) GetFundingRate(ctx context.Context, instrument string) (exchange.FundingRate, error) {
	m, err := c.lookup(instrument)
	if err != nil {
		return exchange.FundingRate{}, err
	}
	var resp struct {
		LastFundingRate string `json:"lastFundingRate"`
		NextFundingTime int64  `json:"nextFundingTime"`
	}
	q := url.Values{"symbol": []string{m.Symbol}}
	if err := c.publicGET(ctx, "/fapi/v1/premiumIndex", q, &resp); err != nil {
		return exchange.FundingRate{}, err
	}
	rate, _ := strconv.ParseFloat(resp.LastFundingRate, 64)
	// Binance funding is paid every 8 hours → per-hour rate / 8.
	return exchange.FundingRate{
		Instrument:     instrument,
		RatePerHour:    rate / 8,
		NextSettlement: time.UnixMilli(resp.NextFundingTime),
		WindowDuration: 8 * time.Hour,
		UpdatedAt:      time.Now(),
	}, nil
}

// ---------- Orders ----------

func (c *Client) PlaceOrder(ctx context.Context, req exchange.OrderRequest) (exchange.Order, error) {
	m, err := c.lookup(req.Instrument)
	if err != nil {
		return exchange.Order{}, err
	}
	q := url.Values{}
	q.Set("symbol", m.Symbol)
	q.Set("side", strings.ToUpper(string(req.Side)))
	q.Set("quantity", trimZeros(strconv.FormatFloat(req.Size, 'f', -1, 64)))
	if req.ClientOrderID != "" {
		q.Set("newClientOrderId", req.ClientOrderID)
	}
	if req.ReduceOnly {
		q.Set("reduceOnly", "true")
	}
	switch req.Type {
	case exchange.OrderTypeMarket:
		q.Set("type", "MARKET")
	default:
		q.Set("type", "LIMIT")
		q.Set("price", trimZeros(strconv.FormatFloat(req.Price, 'f', -1, 64)))
		if req.PostOnly {
			q.Set("timeInForce", "GTX")
		} else {
			q.Set("timeInForce", "GTC")
		}
	}

	var resp struct {
		OrderID       int64  `json:"orderId"`
		ClientOrderID string `json:"clientOrderId"`
		Status        string `json:"status"`
		Symbol        string `json:"symbol"`
		Price         string `json:"price"`
		OrigQty       string `json:"origQty"`
	}
	if err := c.signedPOST(ctx, "/fapi/v1/order", q, &resp); err != nil {
		return exchange.Order{}, err
	}
	now := time.Now()
	return exchange.Order{
		ID:            strconv.FormatInt(resp.OrderID, 10),
		ClientOrderID: resp.ClientOrderID,
		Instrument:    req.Instrument,
		Side:          req.Side,
		Type:          req.Type,
		Price:         req.Price,
		Size:          req.Size,
		Status:        binanceStatus(resp.Status),
		PostOnly:      req.PostOnly,
		ReduceOnly:    req.ReduceOnly,
		PlacedAt:      now,
		UpdatedAt:     now,
	}, nil
}

func binanceStatus(s string) exchange.OrderStatus {
	switch s {
	case "NEW":
		return exchange.OrderOpen
	case "PARTIALLY_FILLED":
		return exchange.OrderPartial
	case "FILLED":
		return exchange.OrderFilled
	case "CANCELED", "EXPIRED":
		return exchange.OrderCanceled
	case "REJECTED":
		return exchange.OrderRejected
	default:
		return exchange.OrderOpen
	}
}

func (c *Client) CancelOrder(ctx context.Context, instrument, orderID string) error {
	m, err := c.lookup(instrument)
	if err != nil {
		return err
	}
	q := url.Values{}
	q.Set("symbol", m.Symbol)
	q.Set("orderId", orderID)
	return c.signedDELETE(ctx, "/fapi/v1/order", q, nil)
}

func (c *Client) CancelAllForInstrument(ctx context.Context, instrument string) error {
	m, err := c.lookup(instrument)
	if err != nil {
		return err
	}
	q := url.Values{"symbol": []string{m.Symbol}}
	return c.signedDELETE(ctx, "/fapi/v1/allOpenOrders", q, nil)
}

func (c *Client) GetOpenOrders(ctx context.Context, instrument string) ([]exchange.Order, error) {
	m, err := c.lookup(instrument)
	if err != nil {
		return nil, err
	}
	q := url.Values{"symbol": []string{m.Symbol}}
	var resp []struct {
		OrderID       int64  `json:"orderId"`
		ClientOrderID string `json:"clientOrderId"`
		Symbol        string `json:"symbol"`
		Side          string `json:"side"`
		Type          string `json:"type"`
		Price         string `json:"price"`
		OrigQty       string `json:"origQty"`
		ExecutedQty   string `json:"executedQty"`
		Status        string `json:"status"`
		Time          int64  `json:"time"`
		ReduceOnly    bool   `json:"reduceOnly"`
	}
	if err := c.signedGET(ctx, "/fapi/v1/openOrders", q, &resp); err != nil {
		return nil, err
	}
	out := make([]exchange.Order, 0, len(resp))
	for _, o := range resp {
		side := exchange.Buy
		if o.Side == "SELL" {
			side = exchange.Sell
		}
		px, _ := strconv.ParseFloat(o.Price, 64)
		sz, _ := strconv.ParseFloat(o.OrigQty, 64)
		filled, _ := strconv.ParseFloat(o.ExecutedQty, 64)
		out = append(out, exchange.Order{
			ID:            strconv.FormatInt(o.OrderID, 10),
			ClientOrderID: o.ClientOrderID,
			Instrument:    instrument,
			Side:          side,
			Type:          exchange.OrderTypeLimit,
			Price:         px,
			Size:          sz,
			Filled:        filled,
			Status:        binanceStatus(o.Status),
			ReduceOnly:    o.ReduceOnly,
			PlacedAt:      time.UnixMilli(o.Time),
		})
	}
	return out, nil
}

func (c *Client) SubscribeFills(ctx context.Context) (<-chan exchange.Fill, error) {
	c.wsOnce.Do(func() { go c.runUserStream(ctx) })
	return c.fillsCh, nil
}

func (c *Client) GetPosition(ctx context.Context, instrument string) (exchange.Position, error) {
	m, err := c.lookup(instrument)
	if err != nil {
		return exchange.Position{}, err
	}
	q := url.Values{"symbol": []string{m.Symbol}}
	var resp []struct {
		Symbol           string `json:"symbol"`
		PositionAmt      string `json:"positionAmt"`
		EntryPrice       string `json:"entryPrice"`
		MarkPrice        string `json:"markPrice"`
		UnrealizedProfit string `json:"unRealizedProfit"`
		LiquidationPrice string `json:"liquidationPrice"`
	}
	if err := c.signedGET(ctx, "/fapi/v2/positionRisk", q, &resp); err != nil {
		return exchange.Position{}, err
	}
	if len(resp) == 0 {
		return exchange.Position{Instrument: instrument}, nil
	}
	p := resp[0]
	szi, _ := strconv.ParseFloat(p.PositionAmt, 64)
	entry, _ := strconv.ParseFloat(p.EntryPrice, 64)
	mark, _ := strconv.ParseFloat(p.MarkPrice, 64)
	upnl, _ := strconv.ParseFloat(p.UnrealizedProfit, 64)
	liq, _ := strconv.ParseFloat(p.LiquidationPrice, 64)
	return exchange.Position{
		Instrument:       instrument,
		NetSize:          szi,
		EntryPrice:       entry,
		MarkPrice:        mark,
		UnrealizedPnL:    upnl,
		LiquidationPrice: liq,
		UpdatedAt:        time.Now(),
	}, nil
}

func (c *Client) GetBalance(ctx context.Context) (exchange.Balance, error) {
	var resp []struct {
		Asset            string `json:"asset"`
		Balance          string `json:"balance"`
		AvailableBalance string `json:"availableBalance"`
	}
	if err := c.signedGET(ctx, "/fapi/v2/balance", nil, &resp); err != nil {
		return exchange.Balance{}, err
	}
	for _, b := range resp {
		if b.Asset != c.cfg.QuoteAsset {
			continue
		}
		total, _ := strconv.ParseFloat(b.Balance, 64)
		avail, _ := strconv.ParseFloat(b.AvailableBalance, 64)
		return exchange.Balance{
			Currency:  b.Asset,
			Total:     total,
			Available: avail,
			EquityUSD: total,
			UpdatedAt: time.Now(),
		}, nil
	}
	return exchange.Balance{Currency: c.cfg.QuoteAsset}, nil
}

// ---------- Transport ----------

func (c *Client) publicGET(ctx context.Context, path string, q url.Values, out any) error {
	full := c.cfg.BaseURL + path
	if len(q) > 0 {
		full += "?" + q.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, full, nil)
	if err != nil {
		return err
	}
	return c.do(req, out)
}

func (c *Client) signedGET(ctx context.Context, path string, q url.Values, out any) error {
	return c.signed(ctx, http.MethodGet, path, q, out)
}

func (c *Client) signedPOST(ctx context.Context, path string, q url.Values, out any) error {
	return c.signed(ctx, http.MethodPost, path, q, out)
}

func (c *Client) signedDELETE(ctx context.Context, path string, q url.Values, out any) error {
	return c.signed(ctx, http.MethodDelete, path, q, out)
}

func (c *Client) signed(ctx context.Context, method, path string, q url.Values, out any) error {
	if q == nil {
		q = url.Values{}
	}
	q.Set("recvWindow", strconv.Itoa(c.cfg.RecvWindow))
	q.Set("timestamp", strconv.FormatInt(time.Now().UnixMilli(), 10))
	payload := q.Encode()
	sig, err := Sign(c.cfg.APISecret, payload)
	if err != nil {
		return err
	}
	full := c.cfg.BaseURL + path + "?" + payload + "&signature=" + sig
	req, err := http.NewRequestWithContext(ctx, method, full, nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-MBX-APIKEY", c.cfg.APIKey)
	return c.do(req, out)
}

func (c *Client) do(req *http.Request, out any) error {
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("binance %s %s: %d %s", req.Method, req.URL.Path, resp.StatusCode, string(body))
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(body, out)
}

// ---------- User Data Stream (private fills) ----------

func (c *Client) runUserStream(ctx context.Context) {
	backoff := time.Second
	for {
		if err := ctx.Err(); err != nil {
			return
		}
		err := c.runUserStreamOnce(ctx)
		if err == nil {
			return
		}
		c.logger.Warn("binance user stream disconnected", zap.Error(err), zap.Duration("backoff", backoff))
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

func (c *Client) runUserStreamOnce(ctx context.Context) error {
	var keyResp struct {
		ListenKey string `json:"listenKey"`
	}
	if err := c.signedPOST(ctx, "/fapi/v1/listenKey", nil, &keyResp); err != nil {
		return fmt.Errorf("create listenKey: %w", err)
	}
	c.logger.Info("binance listenKey created")

	conn, _, err := websocket.DefaultDialer.DialContext(ctx, c.cfg.WSURL+"/"+keyResp.ListenKey, nil)
	if err != nil {
		return fmt.Errorf("ws dial: %w", err)
	}
	defer conn.Close()

	// Refresh listenKey every 30 minutes; Binance expires it after 60.
	ka := time.NewTicker(30 * time.Minute)
	defer ka.Stop()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-ka.C:
				if err := c.signedPOST(ctx, "/fapi/v1/listenKey", nil, nil); err != nil {
					c.logger.Warn("listenKey refresh failed", zap.Error(err))
				}
			}
		}
	}()
	go func() { <-ctx.Done(); _ = conn.Close() }()

	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		c.dispatchUserStream(raw)
	}
}

func (c *Client) dispatchUserStream(raw []byte) {
	var env struct {
		EventType string          `json:"e"`
		EventTime int64           `json:"E"`
		O         json.RawMessage `json:"o"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		return
	}
	if env.EventType != "ORDER_TRADE_UPDATE" {
		return
	}
	var o struct {
		Symbol        string `json:"s"`
		ClientOrderID string `json:"c"`
		Side          string `json:"S"`
		ExecType      string `json:"x"`  // "TRADE" for fills
		Status        string `json:"X"`
		OrderID       int64  `json:"i"`
		LastPrice     string `json:"L"`
		LastQty       string `json:"l"`
		Commission    string `json:"n"`
		CommissionAsset string `json:"N"`
		TradeTime     int64  `json:"T"`
		IsMaker       bool   `json:"m"`
	}
	if err := json.Unmarshal(env.O, &o); err != nil {
		return
	}
	if o.ExecType != "TRADE" {
		return
	}
	side := exchange.Buy
	if o.Side == "SELL" {
		side = exchange.Sell
	}
	px, _ := strconv.ParseFloat(o.LastPrice, 64)
	sz, _ := strconv.ParseFloat(o.LastQty, 64)
	fee, _ := strconv.ParseFloat(o.Commission, 64)
	asset := strings.TrimSuffix(o.Symbol, c.cfg.QuoteAsset)
	select {
	case c.fillsCh <- exchange.Fill{
		OrderID:       strconv.FormatInt(o.OrderID, 10),
		ClientOrderID: o.ClientOrderID,
		Instrument:    asset,
		Side:          side,
		Price:         px,
		Size:          sz,
		Fee:           fee,
		FeeCurrency:   o.CommissionAsset,
		IsMaker:       o.IsMaker,
		Timestamp:     time.UnixMilli(o.TradeTime),
	}:
	default:
		c.logger.Warn("binance: fills buffer full, dropping", zap.String("symbol", o.Symbol))
	}
}

func trimZeros(s string) string {
	if !strings.Contains(s, ".") {
		return s
	}
	s = strings.TrimRight(s, "0")
	s = strings.TrimRight(s, ".")
	if s == "" {
		return "0"
	}
	return s
}
