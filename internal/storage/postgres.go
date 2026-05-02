package storage

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"

	"github.com/jkhatri23/Market-Maker/internal/exchange"
)

// PostgresSink writes events synchronously via a connection pool. Volume
// is low enough (a few fills/min, snapshots/min) that we don't need
// batching yet; if write latency ever shows up in profiling we add a
// buffered channel here without touching callers.
type PostgresSink struct {
	pool   *pgxpool.Pool
	logger *zap.Logger
}

func NewPostgresSink(ctx context.Context, dsn string, logger *zap.Logger) (*PostgresSink, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("pgxpool.New: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres ping: %w", err)
	}
	return &PostgresSink{pool: pool, logger: logger}, nil
}

func (s *PostgresSink) RecordFill(ctx context.Context, venue string, f exchange.Fill) error {
	const q = `
INSERT INTO fills (ts, venue, instrument, side, price, size, fee, fee_currency, is_maker, order_id, client_order_id)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`
	_, err := s.pool.Exec(ctx, q,
		f.Timestamp, venue, f.Instrument, string(f.Side),
		f.Price, f.Size, f.Fee, f.FeeCurrency, f.IsMaker,
		f.OrderID, f.ClientOrderID,
	)
	return err
}

func (s *PostgresSink) RecordPnLSnapshot(ctx context.Context, snap PnLSnapshot) error {
	const q = `
INSERT INTO pnl_snapshots (ts, asset, position, avg_entry, mark_price, realized, unrealized)
VALUES ($1, $2, $3, $4, $5, $6, $7)`
	_, err := s.pool.Exec(ctx, q,
		snap.Timestamp, snap.Asset, snap.Position, snap.AvgEntry,
		snap.MarkPrice, snap.Realized, snap.Unrealized,
	)
	return err
}

func (s *PostgresSink) RecordFundingRate(ctx context.Context, venue string, fr exchange.FundingRate) error {
	const q = `
INSERT INTO funding_history (ts, venue, instrument, rate_per_hour, next_settlement, window_seconds)
VALUES ($1, $2, $3, $4, $5, $6)`
	_, err := s.pool.Exec(ctx, q,
		fr.UpdatedAt, venue, fr.Instrument, fr.RatePerHour, fr.NextSettlement,
		int(fr.WindowDuration.Seconds()),
	)
	return err
}

func (s *PostgresSink) Close() error {
	s.pool.Close()
	return nil
}
