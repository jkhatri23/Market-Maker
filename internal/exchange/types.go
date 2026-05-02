package exchange

import "time"

// Prices and sizes are float64 throughout. Tick / lot alignment is the
// responsibility of the exchange client (the only layer that knows the
// venue's MarketSpec). The engine treats float64 as the canonical type.

type Side string

const (
	Buy  Side = "buy"
	Sell Side = "sell"
)

func (s Side) Opposite() Side {
	if s == Buy {
		return Sell
	}
	return Buy
}

type OrderType string

const (
	OrderTypeLimit  OrderType = "limit"
	OrderTypeMarket OrderType = "market"
)

type OrderStatus string

const (
	OrderOpen     OrderStatus = "open"
	OrderPartial  OrderStatus = "partial"
	OrderFilled   OrderStatus = "filled"
	OrderCanceled OrderStatus = "canceled"
	OrderRejected OrderStatus = "rejected"
)

type Level struct {
	Price float64
	Size  float64
}

// BookUpdate carries either a full snapshot (Snapshot=true) or a delta.
// Bids are sorted high-to-low, Asks low-to-high. SequenceID lets the
// engine detect gaps and request a fresh snapshot.
type BookUpdate struct {
	Instrument string
	Timestamp  time.Time
	Bids       []Level
	Asks       []Level
	SequenceID uint64
	Snapshot   bool
}

func (b BookUpdate) BestBid() (Level, bool) {
	if len(b.Bids) == 0 {
		return Level{}, false
	}
	return b.Bids[0], true
}

func (b BookUpdate) BestAsk() (Level, bool) {
	if len(b.Asks) == 0 {
		return Level{}, false
	}
	return b.Asks[0], true
}

func (b BookUpdate) Mid() (float64, bool) {
	bid, okB := b.BestBid()
	ask, okA := b.BestAsk()
	if !okB || !okA {
		return 0, false
	}
	return (bid.Price + ask.Price) / 2, true
}

type OrderRequest struct {
	Instrument    string
	Side          Side
	Type          OrderType
	Price         float64 // ignored for market orders
	Size          float64
	PostOnly      bool
	ReduceOnly    bool
	ClientOrderID string
}

type Order struct {
	ID            string
	ClientOrderID string
	Instrument    string
	Side          Side
	Type          OrderType
	Price         float64
	Size          float64
	Filled        float64
	Status        OrderStatus
	PostOnly      bool
	ReduceOnly    bool
	PlacedAt      time.Time
	UpdatedAt     time.Time
	Reason        string // populated on Rejected/Canceled when the venue gives one
}

func (o Order) Remaining() float64 {
	r := o.Size - o.Filled
	if r < 0 {
		return 0
	}
	return r
}

func (o Order) IsTerminal() bool {
	return o.Status == OrderFilled || o.Status == OrderCanceled || o.Status == OrderRejected
}

type Fill struct {
	OrderID       string
	ClientOrderID string
	Instrument    string
	Side          Side
	Price         float64
	Size          float64
	Fee           float64
	FeeCurrency   string
	IsMaker       bool
	Timestamp     time.Time
}

// Position uses signed NetSize: positive=long, negative=short, zero=flat.
type Position struct {
	Instrument       string
	NetSize          float64
	EntryPrice       float64
	MarkPrice        float64
	UnrealizedPnL    float64
	RealizedPnL      float64
	MarginUsed       float64
	LiquidationPrice float64
	UpdatedAt        time.Time
}

func (p Position) IsFlat() bool { return p.NetSize == 0 }
func (p Position) IsLong() bool { return p.NetSize > 0 }

type Balance struct {
	Currency   string
	Total      float64
	Available  float64
	MarginUsed float64
	EquityUSD  float64
	UpdatedAt  time.Time
}

// FundingRate is normalized to per-hour to compare across venues with
// different settlement windows (Polymarket: 5m..8h, Kalshi: TBD).
type FundingRate struct {
	Instrument     string
	RatePerHour    float64
	NextSettlement time.Time
	WindowDuration time.Duration
	UpdatedAt      time.Time
}

type MarketSpec struct {
	Instrument             string
	TickSize               float64
	LotSize                float64
	MinSize                float64
	MaxSize                float64
	MaxLeverage            float64
	MaintenanceMarginRatio float64
}
