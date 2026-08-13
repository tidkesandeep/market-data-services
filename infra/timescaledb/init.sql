-- Market Data Services — TimescaleDB schema

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Reference data
CREATE TABLE IF NOT EXISTS symbols (
    symbol          TEXT PRIMARY KEY,
    exchange        TEXT NOT NULL,
    asset_class     TEXT NOT NULL DEFAULT 'equity',
    currency        TEXT NOT NULL DEFAULT 'USD',
    isin            TEXT,
    cusip           TEXT,
    figi            TEXT,
    active          BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS trading_calendars (
    exchange        TEXT NOT NULL,
    trade_date      DATE NOT NULL,
    is_open         BOOLEAN NOT NULL,
    open_time       TIME,
    close_time      TIME,
    PRIMARY KEY (exchange, trade_date)
);

-- Level 1 / trades / quotes (hypertables)
CREATE TABLE IF NOT EXISTS trades (
    time            TIMESTAMPTZ NOT NULL,
    symbol          TEXT NOT NULL,
    exchange        TEXT NOT NULL,
    price           NUMERIC(18, 8) NOT NULL,
    size            NUMERIC(18, 4) NOT NULL,
    trade_id        TEXT,
    side            TEXT,
    source          TEXT NOT NULL DEFAULT 'simulator'
);

SELECT create_hypertable('trades', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_trades_symbol_time ON trades (symbol, time DESC);

CREATE TABLE IF NOT EXISTS quotes (
    time            TIMESTAMPTZ NOT NULL,
    symbol          TEXT NOT NULL,
    exchange        TEXT NOT NULL,
    bid_price       NUMERIC(18, 8),
    bid_size        NUMERIC(18, 4),
    ask_price       NUMERIC(18, 8),
    ask_size        NUMERIC(18, 4),
    source          TEXT NOT NULL DEFAULT 'simulator'
);

SELECT create_hypertable('quotes', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_quotes_symbol_time ON quotes (symbol, time DESC);

-- Level 2 order book snapshots
CREATE TABLE IF NOT EXISTS order_book_snapshots (
    time            TIMESTAMPTZ NOT NULL,
    symbol          TEXT NOT NULL,
    exchange        TEXT NOT NULL,
    side            TEXT NOT NULL,
    price           NUMERIC(18, 8) NOT NULL,
    size            NUMERIC(18, 4) NOT NULL,
    level           INT NOT NULL,
    source          TEXT NOT NULL DEFAULT 'simulator'
);

SELECT create_hypertable('order_book_snapshots', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_ob_symbol_time ON order_book_snapshots (symbol, time DESC);

-- Index values
CREATE TABLE IF NOT EXISTS index_values (
    time            TIMESTAMPTZ NOT NULL,
    index_symbol    TEXT NOT NULL,
    value           NUMERIC(18, 8) NOT NULL,
    change_pct      NUMERIC(10, 6),
    source          TEXT NOT NULL DEFAULT 'simulator'
);

SELECT create_hypertable('index_values', 'time', if_not_exists => TRUE);

-- Corporate actions
CREATE TABLE IF NOT EXISTS corporate_actions (
    id              SERIAL PRIMARY KEY,
    symbol          TEXT NOT NULL,
    action_type     TEXT NOT NULL,
    ex_date         DATE NOT NULL,
    record_date     DATE,
    pay_date        DATE,
    ratio           NUMERIC(18, 8),
    amount          NUMERIC(18, 8),
    currency        TEXT,
    description     TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_corp_actions_symbol ON corporate_actions (symbol, ex_date DESC);

-- Data quality / reconciliation audit
CREATE TABLE IF NOT EXISTS data_quality_events (
    time            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    symbol          TEXT,
    event_type      TEXT NOT NULL,
    severity        TEXT NOT NULL,
    message         TEXT NOT NULL,
    metadata        JSONB
);

SELECT create_hypertable('data_quality_events', 'time', if_not_exists => TRUE);

-- Anomaly detection results
CREATE TABLE IF NOT EXISTS anomalies (
    time            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    symbol          TEXT NOT NULL,
    metric          TEXT NOT NULL,
    observed_value  NUMERIC(18, 8) NOT NULL,
    expected_value  NUMERIC(18, 8),
    z_score         NUMERIC(10, 4),
    model_version   TEXT NOT NULL DEFAULT 'zscore-v1'
);

SELECT create_hypertable('anomalies', 'time', if_not_exists => TRUE);

-- Reconciliation audit trail
CREATE TABLE IF NOT EXISTS reconciliation_runs (
    time            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    symbol          TEXT NOT NULL,
    metric          TEXT NOT NULL,
    observed_count  INT NOT NULL,
    expected_min    INT NOT NULL,
    gap             INT NOT NULL,
    status          TEXT NOT NULL
);

SELECT create_hypertable('reconciliation_runs', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_recon_symbol_time ON reconciliation_runs (symbol, time DESC);

-- Seed reference symbols
INSERT INTO symbols (symbol, exchange, asset_class, currency, isin) VALUES
    ('AAPL', 'NASDAQ', 'equity', 'USD', 'US0378331005'),
    ('MSFT', 'NASDAQ', 'equity', 'USD', 'US5949181045'),
    ('GOOGL', 'NASDAQ', 'equity', 'USD', 'US02079K3059'),
    ('AMZN', 'NASDAQ', 'equity', 'USD', 'US0231351067'),
    ('TSLA', 'NASDAQ', 'equity', 'USD', 'US88160R1014'),
    ('SPX', 'CBOE', 'index', 'USD', NULL)
ON CONFLICT (symbol) DO NOTHING;
