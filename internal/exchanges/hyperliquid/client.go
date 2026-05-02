package hyperliquid

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	secp "github.com/decred/dcrd/dcrec/secp256k1/v4"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"

	"github.com/jkhatri23/Market-Maker/internal/exchange"
)

// Config bundles the Hyperliquid connection + auth parameters.
type Config struct {
	BaseURL       string // e.g. https://api.hyperliquid.xyz (mainnet) or testnet equivalent
	WSURL         string // e.g. wss://api.hyperliquid.xyz/ws
	PrivateKeyHex string // 32-byte hex; with or without 0x
	VaultAddress  string // optional; empty = direct account
	Mainnet       bool   // selects the EIP-712 source byte ("a" vs "b")
}

// Client implements exchange.Exchange.
type Client struct {
	cfg     Config
	priv    *secp.PrivateKey
	address string
	http    *http.Client
	logger  *zap.Logger

	mu       sync.RWMutex
	universe map[string]assetMeta // name → meta
	fillsCh  chan exchange.Fill
	wsOnce   sync.Once
}

type assetMeta struct {
	Index       int
	SzDecimals  int
	MaxLeverage float64
}

// New constructs a Client and validates the private key. The asset
// universe is fetched lazily on first use (or via Bootstrap).
func New(cfg Config, logger *zap.Logger) (*Client, error) {
	if cfg.BaseURL == "" {
		cfg.BaseURL = "https://api.hyperliquid.xyz"
	}
	if cfg.WSURL == "" {
		cfg.WSURL = "wss://api.hyperliquid.xyz/ws"
	}
	priv, err := LoadPrivateKeyHex(cfg.PrivateKeyHex)
	if err != nil {
		return nil, fmt.Errorf("hyperliquid: load key: %w", err)
	}
	return &Client{
		cfg:     cfg,
		priv:    priv,
		address: AddressFromKey(priv),
		http:    &http.Client{Timeout: 10 * time.Second},
		logger:  logger,
		fillsCh: make(chan exchange.Fill, 256),
	}, nil
}

func (c *Client) Name() string { return "hyperliquid" }

// Bootstrap fetches /info {"type":"meta"} and caches the asset universe.
// Called automatically on first GetMarketSpec / PlaceOrder if not yet
// loaded.
func (c *Client) Bootstrap(ctx context.Context) error {
	c.mu.RLock()
	loaded := c.universe != nil
	c.mu.RUnlock()
	if loaded {
		return nil
	}
	var resp struct {
		Universe []struct {
			Name        string  `json:"name"`
			SzDecimals  int     `json:"szDecimals"`
			MaxLeverage float64 `json:"maxLeverage"`
		} `json:"universe"`
	}
	if err := c.postInfo(ctx, map[string]any{"type": "meta"}, &resp); err != nil {
		return fmt.Errorf("fetch meta: %w", err)
	}
	universe := make(map[string]assetMeta, len(resp.Universe))
	for i, u := range resp.Universe {
		universe[u.Name] = assetMeta{Index: i, SzDecimals: u.SzDecimals, MaxLeverage: u.MaxLeverage}
	}
	c.mu.Lock()
	c.universe = universe
	c.mu.Unlock()
	c.logger.Info("hyperliquid universe loaded", zap.Int("assets", len(universe)))
	return nil
}

func (c *Client) lookup(asset string) (assetMeta, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	m, ok := c.universe[asset]
	if !ok {
		return assetMeta{}, fmt.Errorf("hyperliquid: unknown asset %q (universe loaded? %v)", asset, c.universe != nil)
	}
	return m, nil
}

// ---------- Market data ----------

func (c *Client) SubscribeBook(ctx context.Context, instrument string) (<-chan exchange.BookUpdate, error) {
	// Engine drives off PriceFeed; book sub is unused. Inert channel.
	ch := make(chan exchange.BookUpdate)
	go func() { <-ctx.Done(); close(ch) }()
	return ch, nil
}

// GetMarketSpec returns Hyperliquid's tick/lot rules for the asset.
//
// Hyperliquid's price rule: max 5 significant figures AND max
// (6 - szDecimals) decimal places — whichever is more restrictive. We
// approximate at the asset's nominal price using the current /allMids
// quote and pick the larger of the two ticks.
func (c *Client) GetMarketSpec(ctx context.Context, instrument string) (exchange.MarketSpec, error) {
	if err := c.Bootstrap(ctx); err != nil {
		return exchange.MarketSpec{}, err
	}
	m, err := c.lookup(instrument)
	if err != nil {
		return exchange.MarketSpec{}, err
	}
	mid, err := c.midPrice(ctx, instrument)
	if err != nil {
		return exchange.MarketSpec{}, err
	}
	tick := computeTick(mid, m.SzDecimals)
	lot := math.Pow10(-m.SzDecimals)
	return exchange.MarketSpec{
		Instrument:  instrument,
		TickSize:    tick,
		LotSize:     lot,
		MinSize:     lot,
		MaxLeverage: m.MaxLeverage,
	}, nil
}

func (c *Client) midPrice(ctx context.Context, instrument string) (float64, error) {
	var resp map[string]string
	if err := c.postInfo(ctx, map[string]any{"type": "allMids"}, &resp); err != nil {
		return 0, err
	}
	s, ok := resp[instrument]
	if !ok {
		return 0, fmt.Errorf("no mid for %q", instrument)
	}
	return strconv.ParseFloat(s, 64)
}

func computeTick(price float64, szDecimals int) float64 {
	if price <= 0 {
		return math.Pow10(-(6 - szDecimals))
	}
	mag := int(math.Floor(math.Log10(price)))
	tickSig := math.Pow10(mag - 4)         // 5 sig figs
	tickDec := math.Pow10(-(6 - szDecimals))
	if tickSig > tickDec {
		return tickSig
	}
	return tickDec
}

func (c *Client) GetFundingRate(ctx context.Context, instrument string) (exchange.FundingRate, error) {
	// Hyperliquid funding accrues hourly. /info {"type":"metaAndAssetCtxs"}
	// returns ctx[i].funding (per-hour rate as a string).
	var resp [2]json.RawMessage
	if err := c.postInfo(ctx, map[string]any{"type": "metaAndAssetCtxs"}, &resp); err != nil {
		return exchange.FundingRate{}, err
	}
	var ctxs []struct {
		Funding string `json:"funding"`
	}
	if err := json.Unmarshal(resp[1], &ctxs); err != nil {
		return exchange.FundingRate{}, err
	}
	m, err := c.lookup(instrument)
	if err != nil {
		return exchange.FundingRate{}, err
	}
	if m.Index >= len(ctxs) {
		return exchange.FundingRate{}, fmt.Errorf("ctxs out of range")
	}
	rate, _ := strconv.ParseFloat(ctxs[m.Index].Funding, 64)
	return exchange.FundingRate{
		Instrument:     instrument,
		RatePerHour:    rate,
		WindowDuration: time.Hour,
		UpdatedAt:      time.Now(),
	}, nil
}

// ---------- Orders ----------

func (c *Client) PlaceOrder(ctx context.Context, req exchange.OrderRequest) (exchange.Order, error) {
	if err := c.Bootstrap(ctx); err != nil {
		return exchange.Order{}, err
	}
	m, err := c.lookup(req.Instrument)
	if err != nil {
		return exchange.Order{}, err
	}
	tif := "Gtc"
	if req.PostOnly {
		tif = "Alo"
	}
	if req.Type == exchange.OrderTypeMarket {
		tif = "Ioc"
	}
	priceStr := formatHLPrice(req.Price, m.SzDecimals)
	sizeStr := formatHLSize(req.Size, m.SzDecimals)

	action := orderAction{
		Type: "order",
		Orders: []orderWire{{
			A: m.Index,
			B: req.Side == exchange.Buy,
			P: priceStr,
			S: sizeStr,
			R: req.ReduceOnly,
			T: orderTypeWire{Limit: limitTypeWire{Tif: tif}},
		}},
		Grouping: "na",
	}

	var resp struct {
		Status   string `json:"status"`
		Response struct {
			Type string `json:"type"`
			Data struct {
				Statuses []json.RawMessage `json:"statuses"`
			} `json:"data"`
		} `json:"response"`
	}
	if err := c.postExchange(ctx, action, &resp); err != nil {
		return exchange.Order{}, err
	}
	if resp.Status != "ok" {
		return exchange.Order{}, fmt.Errorf("place order: status=%s", resp.Status)
	}
	if len(resp.Response.Data.Statuses) == 0 {
		return exchange.Order{}, errors.New("place order: empty statuses")
	}
	st := resp.Response.Data.Statuses[0]
	var errBody struct{ Error string `json:"error"` }
	if err := json.Unmarshal(st, &errBody); err == nil && errBody.Error != "" {
		return exchange.Order{}, fmt.Errorf("place rejected: %s", errBody.Error)
	}
	var resting struct {
		Resting struct {
			Oid int64 `json:"oid"`
		} `json:"resting"`
		Filled struct {
			Oid int64 `json:"oid"`
		} `json:"filled"`
	}
	if err := json.Unmarshal(st, &resting); err != nil {
		return exchange.Order{}, fmt.Errorf("decode status: %w (raw=%s)", err, st)
	}
	id := resting.Resting.Oid
	status := exchange.OrderOpen
	if id == 0 {
		id = resting.Filled.Oid
		status = exchange.OrderFilled
	}
	now := time.Now()
	return exchange.Order{
		ID:            strconv.FormatInt(id, 10),
		ClientOrderID: req.ClientOrderID,
		Instrument:    req.Instrument,
		Side:          req.Side,
		Type:          req.Type,
		Price:         req.Price,
		Size:          req.Size,
		Status:        status,
		PostOnly:      req.PostOnly,
		ReduceOnly:    req.ReduceOnly,
		PlacedAt:      now,
		UpdatedAt:     now,
	}, nil
}

func (c *Client) CancelOrder(ctx context.Context, instrument, orderID string) error {
	m, err := c.lookup(instrument)
	if err != nil {
		return err
	}
	oid, err := strconv.ParseInt(orderID, 10, 64)
	if err != nil {
		return fmt.Errorf("parse oid: %w", err)
	}
	action := cancelAction{
		Type:    "cancel",
		Cancels: []cancelWire{{A: m.Index, O: oid}},
	}
	var resp struct {
		Status   string          `json:"status"`
		Response json.RawMessage `json:"response"`
	}
	if err := c.postExchange(ctx, action, &resp); err != nil {
		return err
	}
	if resp.Status != "ok" {
		return fmt.Errorf("cancel: status=%s body=%s", resp.Status, resp.Response)
	}
	return nil
}

// CancelAllForInstrument loops through open orders since Hyperliquid has
// no native "cancel all by symbol" endpoint. Batches into one signed
// cancel action so it's still a single round-trip.
func (c *Client) CancelAllForInstrument(ctx context.Context, instrument string) error {
	open, err := c.GetOpenOrders(ctx, instrument)
	if err != nil {
		return fmt.Errorf("list for cancel-all: %w", err)
	}
	if len(open) == 0 {
		return nil
	}
	m, err := c.lookup(instrument)
	if err != nil {
		return err
	}
	cancels := make([]cancelWire, 0, len(open))
	for _, o := range open {
		oid, err := strconv.ParseInt(o.ID, 10, 64)
		if err != nil {
			continue
		}
		cancels = append(cancels, cancelWire{A: m.Index, O: oid})
	}
	action := cancelAction{Type: "cancel", Cancels: cancels}
	var resp struct {
		Status string `json:"status"`
	}
	if err := c.postExchange(ctx, action, &resp); err != nil {
		return err
	}
	if resp.Status != "ok" {
		return fmt.Errorf("cancel-all: status=%s", resp.Status)
	}
	return nil
}

func (c *Client) GetOpenOrders(ctx context.Context, instrument string) ([]exchange.Order, error) {
	var resp []struct {
		Coin      string `json:"coin"`
		Side      string `json:"side"` // "B" or "A"
		LimitPx   string `json:"limitPx"`
		Sz        string `json:"sz"`
		Oid       int64  `json:"oid"`
		Timestamp int64  `json:"timestamp"`
	}
	if err := c.postInfo(ctx, map[string]any{"type": "openOrders", "user": c.address}, &resp); err != nil {
		return nil, err
	}
	out := make([]exchange.Order, 0, len(resp))
	for _, o := range resp {
		if o.Coin != instrument {
			continue
		}
		side := exchange.Buy
		if o.Side == "A" {
			side = exchange.Sell
		}
		px, _ := strconv.ParseFloat(o.LimitPx, 64)
		sz, _ := strconv.ParseFloat(o.Sz, 64)
		out = append(out, exchange.Order{
			ID:         strconv.FormatInt(o.Oid, 10),
			Instrument: o.Coin,
			Side:       side,
			Type:       exchange.OrderTypeLimit,
			Price:      px,
			Size:       sz,
			Status:     exchange.OrderOpen,
			PlacedAt:   time.UnixMilli(o.Timestamp),
		})
	}
	return out, nil
}

func (c *Client) SubscribeFills(ctx context.Context) (<-chan exchange.Fill, error) {
	c.wsOnce.Do(func() {
		go c.runWS(ctx)
	})
	return c.fillsCh, nil
}

func (c *Client) GetPosition(ctx context.Context, instrument string) (exchange.Position, error) {
	var resp struct {
		AssetPositions []struct {
			Position struct {
				Coin           string `json:"coin"`
				Szi            string `json:"szi"`
				EntryPx        string `json:"entryPx"`
				UnrealizedPnl  string `json:"unrealizedPnl"`
				PositionValue  string `json:"positionValue"`
				MarginUsed     string `json:"marginUsed"`
				LiquidationPx  string `json:"liquidationPx"`
			} `json:"position"`
		} `json:"assetPositions"`
	}
	if err := c.postInfo(ctx, map[string]any{"type": "clearinghouseState", "user": c.address}, &resp); err != nil {
		return exchange.Position{}, err
	}
	for _, ap := range resp.AssetPositions {
		if ap.Position.Coin != instrument {
			continue
		}
		szi, _ := strconv.ParseFloat(ap.Position.Szi, 64)
		entry, _ := strconv.ParseFloat(ap.Position.EntryPx, 64)
		upnl, _ := strconv.ParseFloat(ap.Position.UnrealizedPnl, 64)
		margin, _ := strconv.ParseFloat(ap.Position.MarginUsed, 64)
		liq, _ := strconv.ParseFloat(ap.Position.LiquidationPx, 64)
		return exchange.Position{
			Instrument:       instrument,
			NetSize:          szi,
			EntryPrice:       entry,
			UnrealizedPnL:    upnl,
			MarginUsed:       margin,
			LiquidationPrice: liq,
			UpdatedAt:        time.Now(),
		}, nil
	}
	return exchange.Position{Instrument: instrument}, nil
}

func (c *Client) GetBalance(ctx context.Context) (exchange.Balance, error) {
	var resp struct {
		MarginSummary struct {
			AccountValue   string `json:"accountValue"`
			TotalMarginUsed string `json:"totalMarginUsed"`
		} `json:"marginSummary"`
		Withdrawable string `json:"withdrawable"`
	}
	if err := c.postInfo(ctx, map[string]any{"type": "clearinghouseState", "user": c.address}, &resp); err != nil {
		return exchange.Balance{}, err
	}
	total, _ := strconv.ParseFloat(resp.MarginSummary.AccountValue, 64)
	used, _ := strconv.ParseFloat(resp.MarginSummary.TotalMarginUsed, 64)
	avail, _ := strconv.ParseFloat(resp.Withdrawable, 64)
	return exchange.Balance{
		Currency:   "USDC",
		Total:      total,
		Available:  avail,
		MarginUsed: used,
		EquityUSD:  total,
		UpdatedAt:  time.Now(),
	}, nil
}

// ---------- Transport ----------

func (c *Client) postInfo(ctx context.Context, body any, out any) error {
	return c.postJSON(ctx, c.cfg.BaseURL+"/info", body, out)
}

func (c *Client) postExchange(ctx context.Context, action any, out any) error {
	nonce := time.Now().UnixMilli()
	sig, err := SignAction(c.priv, action, nonce, c.cfg.VaultAddress, c.cfg.Mainnet)
	if err != nil {
		return fmt.Errorf("sign: %w", err)
	}
	envelope := map[string]any{
		"action":    action,
		"nonce":     nonce,
		"signature": sig,
	}
	if c.cfg.VaultAddress != "" {
		envelope["vaultAddress"] = c.cfg.VaultAddress
	}
	return c.postJSON(ctx, c.cfg.BaseURL+"/exchange", envelope, out)
}

func (c *Client) postJSON(ctx context.Context, url string, body any, out any) error {
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("hyperliquid %s: %d %s", url, resp.StatusCode, string(respBody))
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(respBody, out)
}

// ---------- WebSocket (private fills) ----------

func (c *Client) runWS(ctx context.Context) {
	backoff := time.Second
	for {
		if err := ctx.Err(); err != nil {
			return
		}
		err := c.runWSOnce(ctx)
		if err == nil {
			return
		}
		c.logger.Warn("hyperliquid ws disconnected", zap.Error(err), zap.Duration("backoff", backoff))
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

func (c *Client) runWSOnce(ctx context.Context) error {
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, c.cfg.WSURL, nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	if err := conn.WriteJSON(map[string]any{
		"method": "subscribe",
		"subscription": map[string]any{
			"type": "userFills",
			"user": c.address,
		},
	}); err != nil {
		return err
	}

	go func() { <-ctx.Done(); _ = conn.Close() }()

	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		c.dispatchWS(raw)
	}
}

func (c *Client) dispatchWS(raw []byte) {
	var env struct {
		Channel string          `json:"channel"`
		Data    json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		return
	}
	if env.Channel != "userFills" {
		return
	}
	var data struct {
		IsSnapshot bool `json:"isSnapshot"`
		Fills      []struct {
			Coin    string `json:"coin"`
			Px      string `json:"px"`
			Sz      string `json:"sz"`
			Side    string `json:"side"`
			Time    int64  `json:"time"`
			Oid     int64  `json:"oid"`
			Cloid   string `json:"cloid"`
			Fee     string `json:"fee"`
			FeeToken string `json:"feeToken"`
			Crossed bool   `json:"crossed"`
		} `json:"fills"`
	}
	if err := json.Unmarshal(env.Data, &data); err != nil {
		return
	}
	if data.IsSnapshot {
		return // skip historical replay; only deliver new fills
	}
	for _, f := range data.Fills {
		side := exchange.Buy
		if f.Side == "A" {
			side = exchange.Sell
		}
		px, _ := strconv.ParseFloat(f.Px, 64)
		sz, _ := strconv.ParseFloat(f.Sz, 64)
		fee, _ := strconv.ParseFloat(f.Fee, 64)
		select {
		case c.fillsCh <- exchange.Fill{
			OrderID:       strconv.FormatInt(f.Oid, 10),
			ClientOrderID: f.Cloid,
			Instrument:    f.Coin,
			Side:          side,
			Price:         px,
			Size:          sz,
			Fee:           fee,
			FeeCurrency:   f.FeeToken,
			IsMaker:       !f.Crossed,
			Timestamp:     time.UnixMilli(f.Time),
		}:
		default:
			c.logger.Warn("hyperliquid: fills buffer full, dropping",
				zap.String("coin", f.Coin), zap.Int64("oid", f.Oid))
		}
	}
}

// ---------- Helpers ----------

// formatHLPrice respects Hyperliquid's "5 sig figs, ≤6-szDecimals
// decimals" rule.
func formatHLPrice(price float64, szDecimals int) string {
	maxDec := 6 - szDecimals
	if maxDec < 0 {
		maxDec = 0
	}
	// Truncate to max decimals, then trim to ≤5 sig figs.
	s := strconv.FormatFloat(price, 'f', maxDec, 64)
	return trimSigFigs(s, 5)
}

func formatHLSize(size float64, szDecimals int) string {
	return strconv.FormatFloat(size, 'f', szDecimals, 64)
}

// trimSigFigs returns s rounded to at most n significant figures (in
// string form, no rounding). Hyperliquid rejects more than 5 sig figs.
func trimSigFigs(s string, n int) string {
	// Find first non-zero digit
	sign := ""
	if strings.HasPrefix(s, "-") {
		sign = "-"
		s = s[1:]
	}
	i := 0
	for i < len(s) && (s[i] == '0' || s[i] == '.') {
		i++
	}
	if i >= len(s) {
		return sign + s
	}
	count := 0
	out := []byte(s)
	for j := i; j < len(out); j++ {
		if out[j] == '.' {
			continue
		}
		if count >= n {
			out[j] = '0'
		}
		count++
	}
	// Trim trailing zeros after decimal, but keep the integer part.
	str := string(out)
	if dot := strings.Index(str, "."); dot >= 0 {
		str = strings.TrimRight(str, "0")
		str = strings.TrimRight(str, ".")
	}
	if str == "" {
		str = "0"
	}
	return sign + str
}
