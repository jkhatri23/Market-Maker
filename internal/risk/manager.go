package risk

import (
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/jkhatri23/Market-Maker/internal/config"
	"github.com/jkhatri23/Market-Maker/internal/exchange"
)

// Verdict is the per-asset risk decision the engine consults before each
// reconcile. It tells the quoter whether to quote at all, which side to
// pull, how much to shift the mid, and how much extra spread to add per
// side.
type Verdict struct {
	Allow          bool
	PullBuys       bool
	PullSells      bool
	MidShiftBps    float64
	ExtraBidBps    float64
	ExtraAskBps    float64
	Reason         string // populated when Allow=false; otherwise empty
}

// Manager combines all gates: hard halt, stale-feed kill, daily-drawdown
// halt, dislocation breaker, position-cap side pull, A-S inventory skew,
// fill-intensity widening.
//
// The Book (positions + PnL), dislocation history, and fill-intensity
// history are all owned here so callers don't have to wire them
// individually.
type Manager struct {
	cfg     config.RiskConfig
	Book    *Book
	disloc  *dislocationGate
	fills   *fillIntensityGate
	maxAge  time.Duration
	logger  *zap.Logger

	haltMu     sync.RWMutex
	halted     bool
	haltReason string
}

// NewManager wires the risk gates from config. maxFeedAge defaults to 5s
// when zero — used by the stale-feed gate.
func NewManager(cfg config.RiskConfig, maxFeedAge time.Duration, logger *zap.Logger) *Manager {
	if maxFeedAge <= 0 {
		maxFeedAge = 5 * time.Second
	}
	return &Manager{
		cfg:    cfg,
		Book:   NewBook(),
		disloc: newDislocationGate(cfg.DislocationPct, cfg.DislocationWindow),
		fills:  newFillIntensityGate(cfg.FillIntensityWindow, cfg.FillIntensityThreshold),
		maxAge: maxFeedAge,
		logger: logger,
	}
}

// ApplyFill is the single entry point for fill events: updates positions,
// PnL, and the fill-intensity window.
func (m *Manager) ApplyFill(f exchange.Fill) {
	m.Book.ApplyFill(f.Instrument, f.Side, f.Price, f.Size, f.Fee)
	m.fills.Record(f.Instrument, f.Side)
}

// ApplyMark updates the mark price for unrealized PnL on one asset.
func (m *Manager) ApplyMark(asset string, mark float64) {
	m.Book.ApplyMark(asset, mark)
}

// Halt is sticky — once set, all future Check calls return Allow=false
// until Reset is called. Used for daily-drawdown breach (manual restart
// required) and for the operator panic button.
func (m *Manager) Halt(reason string) {
	m.haltMu.Lock()
	defer m.haltMu.Unlock()
	if m.halted {
		return
	}
	m.halted = true
	m.haltReason = reason
	m.logger.Error("risk manager halted", zap.String("reason", reason))
}

func (m *Manager) Reset() {
	m.haltMu.Lock()
	defer m.haltMu.Unlock()
	m.halted = false
	m.haltReason = ""
	m.logger.Warn("risk manager reset (halt cleared)")
}

func (m *Manager) IsHalted() (string, bool) {
	m.haltMu.RLock()
	defer m.haltMu.RUnlock()
	return m.haltReason, m.halted
}

// Check is the engine's per-cycle gate. Order matters: hard kills first
// (halted, stale feed, drawdown, dislocation), then quote-shape modifiers
// (side pull, skew, fill-intensity widen).
func (m *Manager) Check(ac config.AssetConfig, ref float64, refAge time.Duration) Verdict {
	if reason, halted := m.IsHalted(); halted {
		return Verdict{Reason: "halted: " + reason}
	}
	if refAge > m.maxAge {
		return Verdict{Reason: fmt.Sprintf("stale price feed: age=%s", refAge)}
	}

	if m.cfg.DailyDrawdownHaltUSD > 0 {
		if pnl := m.Book.NetPnL(); pnl < -m.cfg.DailyDrawdownHaltUSD {
			m.Halt(fmt.Sprintf("daily drawdown breached: pnl=$%.2f limit=-$%.2f",
				pnl, m.cfg.DailyDrawdownHaltUSD))
			return Verdict{Reason: "daily drawdown breached"}
		}
	}

	if m.disloc.Update(ac.Symbol, ref) {
		return Verdict{Reason: fmt.Sprintf("price dislocation > %.2f%% in %s",
			m.cfg.DislocationPct*100, m.cfg.DislocationWindow)}
	}

	pos := m.Book.Position(ac.Symbol)
	v := Verdict{Allow: true}
	if ac.MaxPosition > 0 {
		if pos >= ac.MaxPosition {
			v.PullBuys = true
		}
		if pos <= -ac.MaxPosition {
			v.PullSells = true
		}
	}

	v.MidShiftBps = Skew(SkewParams{
		NetPosition: pos,
		MaxPosition: ac.MaxPosition,
		SpreadBps:   ac.SpreadBps,
		SkewFactor:  ac.SkewFactor,
	})

	if m.cfg.FillIntensityThreshold > 0 {
		buy, sell := m.fills.Counts(ac.Symbol)
		if buy > m.cfg.FillIntensityThreshold {
			// Widen the bid: lots of buy-fills mean sellers are aggressing
			// into our bids — price likely moving down, pull bids back.
			v.ExtraBidBps = ac.SpreadBps * 0.25
		}
		if sell > m.cfg.FillIntensityThreshold {
			v.ExtraAskBps = ac.SpreadBps * 0.25
		}
	}

	return v
}
