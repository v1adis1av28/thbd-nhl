-- DDS: снежинка клуба (дивизион → конференция) и звезда фактов.
-- Скрипт пересоздаёт только схему dds. Слой ods не трогает.
-- Применяется в общей транзакции сборки измерений и фактов.

CREATE SCHEMA IF NOT EXISTS dds;

DROP VIEW IF EXISTS dds.game_goal_summary;
DROP TABLE IF EXISTS dds.load_manifest;
DROP TABLE IF EXISTS dds.dq_result CASCADE;
DROP TABLE IF EXISTS dds.fact_goal CASCADE;
DROP TABLE IF EXISTS dds.fact_game CASCADE;
DROP TABLE IF EXISTS dds.fact_standings CASCADE;
DROP TABLE IF EXISTS dds.dim_player CASCADE;
DROP TABLE IF EXISTS dds.dim_team CASCADE;
DROP TABLE IF EXISTS dds.dim_division CASCADE;
DROP TABLE IF EXISTS dds.dim_conference CASCADE;
DROP TABLE IF EXISTS dds.dim_date CASCADE;

CREATE TABLE dds.dim_date (
    date_sk integer PRIMARY KEY,
    full_date date NOT NULL UNIQUE,
    calendar_year integer NOT NULL,
    calendar_month integer NOT NULL,
    calendar_day integer NOT NULL,
    week_day integer NOT NULL
);

CREATE TABLE dds.dim_conference (
    conference_sk integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    conference_abbrev text NOT NULL UNIQUE,
    conference_name text NOT NULL
);

CREATE TABLE dds.dim_division (
    division_sk integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    division_abbrev text NOT NULL UNIQUE,
    division_name text NOT NULL,
    conference_sk integer NOT NULL REFERENCES dds.dim_conference (conference_sk)
);

CREATE TABLE dds.dim_team (
    team_sk integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    team_abbrev text NOT NULL,
    team_name text NOT NULL,
    division_sk integer REFERENCES dds.dim_division (division_sk),
    valid_from date NOT NULL,
    valid_to date,
    is_current boolean NOT NULL,
    UNIQUE (team_abbrev, valid_from)
);

CREATE TABLE dds.dim_player (
    player_sk integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    player_id bigint NOT NULL,
    player_name text,
    team_sk integer REFERENCES dds.dim_team (team_sk),
    valid_from date NOT NULL,
    valid_to date,
    is_current boolean NOT NULL,
    UNIQUE (player_id, valid_from)
);

CREATE TABLE dds.fact_game (
    game_id text PRIMARY KEY,
    date_sk integer NOT NULL REFERENCES dds.dim_date (date_sk),
    home_team_sk integer REFERENCES dds.dim_team (team_sk),
    away_team_sk integer REFERENCES dds.dim_team (team_sk),
    season integer,
    venue text,
    home_score integer,
    away_score integer,
    goal_count integer,
    play_count integer
);

CREATE TABLE dds.fact_standings (
    standings_date date NOT NULL,
    team_sk integer NOT NULL REFERENCES dds.dim_team (team_sk),
    games_played integer,
    wins integer,
    losses integer,
    ot_losses integer,
    points integer,
    goal_differential integer,
    PRIMARY KEY (standings_date, team_sk)
);

CREATE TABLE dds.fact_goal (
    goal_sk bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    game_id text NOT NULL REFERENCES dds.fact_game (game_id),
    event_id integer NOT NULL,
    scorer_player_sk integer REFERENCES dds.dim_player (player_sk),
    team_sk integer REFERENCES dds.dim_team (team_sk),
    assist1_player_sk integer REFERENCES dds.dim_player (player_sk),
    assist2_player_sk integer REFERENCES dds.dim_player (player_sk),
    period integer,
    period_type text,
    time_in_period text,
    x integer,
    y integer,
    shot_type text,
    home_score integer,
    away_score integer,
    UNIQUE (game_id, event_id)
);

CREATE TABLE dds.dq_result (
    check_name text PRIMARY KEY,
    severity text NOT NULL,
    passed boolean NOT NULL,
    expected text,
    actual text,
    details text,
    checked_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX dim_team_abbrev_idx ON dds.dim_team (team_abbrev, valid_from);
CREATE INDEX dim_player_id_idx ON dds.dim_player (player_id, valid_from);
CREATE INDEX fact_goal_scorer_idx ON dds.fact_goal (scorer_player_sk);

CREATE TABLE dds.load_manifest (
    snapshot_id text PRIMARY KEY,
    games integer NOT NULL,
    goals integer NOT NULL,
    standings integer NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX dim_team_current_unique ON dds.dim_team(team_abbrev) WHERE is_current;
CREATE UNIQUE INDEX dim_player_current_unique ON dds.dim_player(player_id) WHERE is_current;
CREATE VIEW dds.game_goal_summary AS
SELECT g.game_id, g.goal_count AS source_goals, count(f.goal_sk) AS normalized_goals
FROM dds.fact_game g LEFT JOIN dds.fact_goal f USING (game_id)
GROUP BY g.game_id, g.goal_count;
