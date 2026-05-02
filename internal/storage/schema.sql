-- perps-mm schema. Run once against a fresh database:
--   psql "$POSTGRES_DSN" -f internal/storage/schema.sql
-- Idempotent: safe to re-run.

CREATE TABLE IF NOT EXISTS fills (
    id              BIGSERIAL PRIMARY KEY,
    ts              TIMESTAMPTZ NOT NULL,
    venue           TEXT        NOT NULL,
    instrument      TEXT        NOT NULL,
    side            TEXT        NOT NULL,
    price           DOUBLE PRECISION NOT NULL,
    size            DOUBLE PRECISION NOT NULL,
    fee             DOUBLE PRECISION NOT NULL DEFAULT 0,
    fee_currency    TEXT,
    is_maker        BOOLEAN     NOT NULL DEFAULT FALSE,
    order_id        TEXT,
    client_order_id TEXT
);
CREATE INDEX IF NOT EXISTS idx_fills_ts             ON fills (ts DESC);
CREATE INDEX IF NOT EXISTS idx_fills_instrument_ts  ON fills (instrument, ts DESC);

CREATE TABLE IF NOT EXISTS pnl_snapshots (
    id          BIGSERIAL PRIMARY KEY,
    ts          TIMESTAMPTZ NOT NULL,
    asset       TEXT        NOT NULL,
    position    DOUBLE PRECISION NOT NULL,
    avg_entry   DOUBLE PRECISION,
    mark_price  DOUBLE PRECISION,
    realized    DOUBLE PRECISION NOT NULL,
    unrealized  DOUBLE PRECISION NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_pnl_snapshots_ts          ON pnl_snapshots (ts DESC);
CREATE INDEX IF NOT EXISTS idx_pnl_snapshots_asset_ts    ON pnl_snapshots (asset, ts DESC);

CREATE TABLE IF NOT EXISTS funding_history (
    id              BIGSERIAL PRIMARY KEY,
    ts              TIMESTAMPTZ NOT NULL,
    venue           TEXT        NOT NULL,
    instrument      TEXT        NOT NULL,
    rate_per_hour   DOUBLE PRECISION NOT NULL,
    next_settlement TIMESTAMPTZ,
    window_seconds  INTEGER
);
CREATE INDEX IF NOT EXISTS idx_funding_history_ts            ON funding_history (ts DESC);
CREATE INDEX IF NOT EXISTS idx_funding_history_instrument_ts ON funding_history (venue, instrument, ts DESC);
