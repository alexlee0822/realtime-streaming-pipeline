-- Clickstream curated warehouse schema
CREATE SCHEMA IF NOT EXISTS curated;

CREATE TABLE IF NOT EXISTS curated.events_cleaned (
    event_id        TEXT PRIMARY KEY,
    event_type      TEXT NOT NULL,
    event_ts        TIMESTAMPTZ NOT NULL,
    user_id         TEXT NOT NULL,
    session_id      TEXT NOT NULL,
    page            TEXT NOT NULL,
    referrer        TEXT,
    device          TEXT,
    country         TEXT,
    utm_source      TEXT,
    latency_ms      INTEGER,
    amount          NUMERIC(12, 2) DEFAULT 0,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_events_cleaned_ts ON curated.events_cleaned (event_ts DESC);
CREATE INDEX IF NOT EXISTS idx_events_cleaned_type ON curated.events_cleaned (event_type);
CREATE INDEX IF NOT EXISTS idx_events_cleaned_page ON curated.events_cleaned (page);

CREATE TABLE IF NOT EXISTS curated.page_metrics_1m (
    window_start    TIMESTAMPTZ NOT NULL,
    window_end      TIMESTAMPTZ NOT NULL,
    page            TEXT NOT NULL,
    event_type      TEXT NOT NULL,
    event_count     BIGINT NOT NULL,
    unique_users    BIGINT NOT NULL,
    unique_sessions BIGINT NOT NULL,
    avg_latency_ms  DOUBLE PRECISION,
    total_amount    NUMERIC(14, 2) NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_page_metrics_window
    ON curated.page_metrics_1m (window_start DESC);

CREATE TABLE IF NOT EXISTS curated.funnel_1m (
    window_start    TIMESTAMPTZ NOT NULL,
    window_end      TIMESTAMPTZ NOT NULL,
    page_views      BIGINT NOT NULL,
    clicks          BIGINT NOT NULL,
    add_to_carts    BIGINT NOT NULL,
    purchases       BIGINT NOT NULL,
    revenue         NUMERIC(14, 2) NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_funnel_window
    ON curated.funnel_1m (window_start DESC);

CREATE TABLE IF NOT EXISTS curated.device_country_1m (
    window_start    TIMESTAMPTZ NOT NULL,
    window_end      TIMESTAMPTZ NOT NULL,
    device          TEXT NOT NULL,
    country         TEXT NOT NULL,
    event_count     BIGINT NOT NULL,
    purchases       BIGINT NOT NULL,
    revenue         NUMERIC(14, 2) NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_device_country_window
    ON curated.device_country_1m (window_start DESC);

CREATE OR REPLACE VIEW curated.v_events_per_minute AS
SELECT
    date_trunc('minute', event_ts) AS minute,
    event_type,
    COUNT(*) AS events
FROM curated.events_cleaned
GROUP BY 1, 2
ORDER BY 1 DESC;
