-- Enable timescale extension (already bundled in the image, just needs enabling)
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Consumer lag samples (hypertable)
CREATE TABLE consumer_lag_samples (
    time            TIMESTAMPTZ NOT NULL,
    consumer_group  TEXT NOT NULL,
    topic           TEXT NOT NULL,
    partition       INT NOT NULL,
    current_offset  BIGINT NOT NULL,
    end_offset      BIGINT NOT NULL,
    lag             BIGINT NOT NULL
);
SELECT create_hypertable('consumer_lag_samples', 'time');
CREATE INDEX ON consumer_lag_samples (consumer_group, topic, partition, time DESC);

-- End-to-end latency samples (hypertable)
CREATE TABLE end_to_end_latency_samples (
    time            TIMESTAMPTZ NOT NULL,
    event_type      TEXT NOT NULL,
    trace_id        TEXT NOT NULL,
    origin_ts       TIMESTAMPTZ NOT NULL,
    completion_ts   TIMESTAMPTZ NOT NULL,
    latency_ms      BIGINT NOT NULL
);
SELECT create_hypertable('end_to_end_latency_samples', 'time');
CREATE INDEX ON end_to_end_latency_samples (event_type, time DESC);

-- Service graph edges (regular table)
CREATE TABLE service_graph_edges (
    id              SERIAL PRIMARY KEY,
    producer_client TEXT NOT NULL,
    topic           TEXT NOT NULL,
    consumer_group  TEXT NOT NULL,
    discovered_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (producer_client, topic, consumer_group)
);

-- DLQ events (regular table)
CREATE TABLE dlq_events (
    id              SERIAL PRIMARY KEY,
    source_group    TEXT NOT NULL,
    error_hash      TEXT NOT NULL,
    error_type      TEXT NOT NULL,
    count           BIGINT NOT NULL DEFAULT 1,
    oldest_ts       TIMESTAMPTZ NOT NULL,
    UNIQUE (source_group, error_hash)
);

-- SLA definitions (regular table)
CREATE TABLE sla_definitions (
    id                  SERIAL PRIMARY KEY,
    event_type          TEXT NOT NULL UNIQUE,
    max_latency_ms      BIGINT NOT NULL,
    alert_threshold_pct NUMERIC NOT NULL DEFAULT 80
);