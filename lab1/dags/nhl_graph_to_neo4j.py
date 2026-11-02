"""ODS: граф NHL в Neo4j.

Узлы: Conference, Division, Team, Game, Player.
Рёбра: IN_CONFERENCE, IN_DIVISION, PLAYED_HOME, PLAYED_AWAY, PLAYS_FOR, APPEARED_IN.
Составы — все 32 клуба. Матчи — несколько последних недель расписания.
Выходы на лёд — box score не более чем NHL_GRAPH_BOXSCORES завершённых игр.
Сырьё, из которого собран граф, лежит в MinIO и перечитывается на шаге загрузки.
"""

import logging
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "plugins"))

from airflow.sdk import dag, task

from thbd.graph import load_graph
from thbd.nhl import fetch_boxscore, fetch_roster, fetch_schedule_weeks, fetch_standings
from thbd.settings import current_batch_id, elapsed_ms, load_settings, utc_now_iso
from thbd.stores import RawLanding, ensure_ods_schema, envelope, log_ingest
from thbd.transform import flatten_standing, game_header, iter_appearances, iter_roster, select_games

log = logging.getLogger(__name__)

DAG_ID = "nhl_graph_to_neo4j"


@dag(
    dag_id=DAG_ID,
    description="Граф клубов, матчей и игроков NHL в Neo4j",
    schedule="0 14 * * *",
    start_date=datetime(2026, 10, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["nhl", "ods", "lab1", "neo4j"],
    default_args={"owner": "thbd", "retries": 2, "retry_delay": timedelta(minutes=1)},
)
def nhl_graph_to_neo4j():
    @task
    def extract_and_land() -> dict:
        settings = load_settings()
        ensure_ods_schema(settings)
        started = time.perf_counter()
        batch_id = current_batch_id()
        fetched_at = utc_now_iso()
        landing = RawLanding(settings)
        total_bytes = 0

        standings, standings_url = fetch_standings()
        standings_key = f"nhl/graph/standings/batch={batch_id}.json"
        total_bytes += landing.put_json(
            standings_key,
            envelope(standings, source_url=standings_url, batch_id=batch_id, fetched_at=fetched_at),
        )

        schedule_keys = []
        for index, (week, week_url) in enumerate(fetch_schedule_weeks(settings.max_weeks)):
            week_date = ((week.get("gameWeek") or [{}])[0].get("date")) or f"week-{index}"
            key = f"nhl/graph/schedule/week={week_date}/batch={batch_id}.json"
            total_bytes += landing.put_json(
                key,
                envelope(week, source_url=week_url, batch_id=batch_id, fetched_at=fetched_at),
            )
            schedule_keys.append(key)

        abbrevs = []
        for row in standings.get("standings") or []:
            flat = flatten_standing(row)
            abbrevs.append(flat["team_abbrev"])
        roster_keys = []
        for abbrev in abbrevs:
            roster, roster_url = fetch_roster(abbrev)
            key = f"nhl/graph/roster/team={abbrev}/batch={batch_id}.json"
            total_bytes += landing.put_json(
                key,
                envelope(roster, source_url=roster_url, batch_id=batch_id, fetched_at=fetched_at),
            )
            roster_keys.append(key)
            log.info("roster %s", abbrev)

        weeks = [landing.get_json(key)["payload"] for key in schedule_keys]
        off_games = select_games(weeks, states={"OFF"}, limit=settings.graph_boxscores, newest_first=True)
        boxscore_keys = []
        for game in off_games:
            game_id = str(game["id"])
            boxscore, box_url = fetch_boxscore(game_id)
            key = f"nhl/graph/boxscore/game={game_id}/batch={batch_id}.json"
            total_bytes += landing.put_json(
                key,
                envelope(boxscore, source_url=box_url, batch_id=batch_id, fetched_at=fetched_at),
            )
            boxscore_keys.append(key)
            log.info("boxscore %s", game_id)

        log_ingest(
            settings,
            batch_id=batch_id,
            dag_id=DAG_ID,
            storage="minio",
            entity="nhl_graph_raw",
            records_loaded=1 + len(schedule_keys) + len(roster_keys) + len(boxscore_keys),
            payload_bytes=total_bytes,
            duration_ms=elapsed_ms(started),
            details={"teams": len(abbrevs), "boxscores": len(boxscore_keys)},
        )
        return {
            "batch_id": batch_id,
            "standings_key": standings_key,
            "schedule_keys": schedule_keys,
            "roster_keys": roster_keys,
            "boxscore_keys": boxscore_keys,
        }

    @task
    def load_neo4j(landed: dict) -> dict:
        settings = load_settings()
        started = time.perf_counter()
        landing = RawLanding(settings)
        standings_payload = landing.get_json(landed["standings_key"])["payload"]
        teams = [flatten_standing(row) for row in standings_payload.get("standings") or []]

        weeks = [landing.get_json(key)["payload"] for key in landed["schedule_keys"]]
        games = [game_header(game) for game in select_games(weeks)]
        games = [game for game in games if game["home_abbrev"] and game["away_abbrev"]]

        players = []
        for key in landed["roster_keys"]:
            body = landing.get_json(key)
            team_abbrev = key.split("team=")[1].split("/")[0]
            players.extend(iter_roster(body["payload"], team_abbrev))

        appearances = []
        for key in landed["boxscore_keys"]:
            boxscore = landing.get_json(key)["payload"]
            appearances.extend(iter_appearances(boxscore))

        counts = load_graph(
            settings,
            teams=teams,
            games=games,
            players=players,
            appearances=appearances,
        )
        log_ingest(
            settings,
            batch_id=landed["batch_id"],
            dag_id=DAG_ID,
            storage="neo4j",
            entity="nhl_graph",
            records_loaded=sum(counts.values()),
            payload_bytes=0,
            duration_ms=elapsed_ms(started),
            details=counts,
        )
        log.info("graph load %s", counts)
        return counts

    load_neo4j(extract_and_land())


nhl_graph_to_neo4j()
