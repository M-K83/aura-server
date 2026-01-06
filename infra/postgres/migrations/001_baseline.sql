-- 001_baseline.sql
-- Aura Core baseline: meta schema + run logging table + core domains

BEGIN;

-- Meta schema for pipeline bookkeeping
CREATE SCHEMA IF NOT EXISTS aura_meta;

CREATE TABLE IF NOT EXISTS aura_meta.ingest_runs (
  run_id           BIGSERIAL PRIMARY KEY,
  service          TEXT NOT NULL,
  source           TEXT,
  status           TEXT NOT NULL CHECK (status IN ('started','success','failed')),
  started_at_utc   TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at_utc  TIMESTAMPTZ,
  rows_fetched     INTEGER,
  rows_inserted    INTEGER,
  rows_updated     INTEGER,
  max_ts_utc       TIMESTAMPTZ,
  error_message    TEXT
);

-- Domain schemas (empty for now; tables will come in later migrations)
CREATE SCHEMA IF NOT EXISTS aura_weather;
CREATE SCHEMA IF NOT EXISTS aura_fitness;
CREATE SCHEMA IF NOT EXISTS aura_finance;
CREATE SCHEMA IF NOT EXISTS aura_sports;

COMMIT;
