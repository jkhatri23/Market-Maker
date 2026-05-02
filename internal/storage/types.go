package storage

import "time"

// PnLSnapshot is one row of position+PnL state captured periodically by
// the engine. Used for offline PnL analysis and the daily-rollover anchor.
type PnLSnapshot struct {
	Timestamp  time.Time
	Asset      string
	Position   float64
	AvgEntry   float64
	MarkPrice  float64
	Realized   float64
	Unrealized float64
}
