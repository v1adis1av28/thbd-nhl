-- ODS NHL. Файл выполняется при первом старте postgres-ods
-- и повторно из DAG, поэтому все операторы идемпотентны.

CREATE SCHEMA IF NOT EXISTS ods;

CREATE TABLE IF NOT EXISTS ods.nhl_standings (
    standings_date date NOT NULL,
    team_abbrev text NOT NULL,
    team_name text NOT NULL,
    conference_abbrev text NOT NULL,
    conference_name text NOT NULL,
    division_abbrev text NOT NULL,
    division_name text NOT NULL,
    games_played integer,
    wins integer,
    losses integer,
    ot_losses integer,
    points integer,
    goal_differential integer,
    raw_payload jsonb NOT NULL,
    source_url text NOT NULL,
    batch_id text NOT NULL,
    ingested_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (standings_date, team_abbrev)
);

CREATE INDEX IF NOT EXISTS nhl_standings_division_idx
    ON ods.nhl_standings (standings_date, division_name, points DESC);

CREATE TABLE IF NOT EXISTS ods.ingest_log (
    id bigserial PRIMARY KEY,
    batch_id text NOT NULL,
    dag_id text NOT NULL,
    storage text NOT NULL,
    entity text NOT NULL,
    records_loaded integer NOT NULL,
    payload_bytes bigint NOT NULL,
    duration_ms integer NOT NULL,
    details jsonb,
    loaded_at timestamptz NOT NULL DEFAULT now()
);
