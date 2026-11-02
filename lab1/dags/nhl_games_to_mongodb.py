"""ODS: протоколы матчей NHL в MongoDB.

Берём последние завершённые игры (gameState=OFF) за NHL_MAX_WEEKS недель,
не больше NHL_MAX_GAMES. В документ кладётся неизменённый box score и
play-by-play: массив событий удобно фильтровать без нормализации.
Сырой пакет параллельно пишется в MinIO.
Уже загруженный game_id пропускается, пока NHL_REFRESH не равен 1.
"""

import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "plugins"))

from airflow.sdk import dag, task

from thbd.nhl import completed_games, fetch_boxscore, fetch_play_by_play
from thbd.settings import current_batch_id, elapsed_ms, load_settings, utc_now_iso
from thbd.stores import (
    RawLanding,
    ensure_ods_schema,
    envelope,
    log_ingest,
    stored_game_ids,
    upsert_games,
)
from thbd.transform import count_goals, game_header

log = logging.getLogger(__name__)

DAG_ID = "nhl_games_to_mongodb"


def _document(schedule_game: dict, boxscore: dict, play_by_play: dict, batch_id: str) -> dict:
    header = game_header(boxscore or schedule_game)
    plays = play_by_play.get("plays") or []
    return {
        "_id": header["game_id"],
        "game_id": header["game_id"],
        "game_date": header["game_date"],
        "season": header["season"],
        "state": header["state"],
        "venue": header["venue"],
        "home": {"abbrev": header["home_abbrev"], "score": header["home_score"]},
        "away": {"abbrev": header["away_abbrev"], "score": header["away_score"]},
        "goal_count": count_goals(plays),
        "play_count": len(plays),
        "plays": plays,
        "raw_boxscore": boxscore,
        "raw_play_by_play": play_by_play,
        "batch_id": batch_id,
        "ingested_at": utc_now_iso(),
    }


@dag(
    dag_id=DAG_ID,
    description="Протоколы NHL: сырьё в MinIO, документ матча в MongoDB",
    schedule="30 12 * * *",
    start_date=datetime(2026, 10, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["nhl", "ods", "lab1", "mongodb"],
    default_args={
        "owner": "thbd",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(hours=2),
    },
)
def nhl_games_to_mongodb():
    @task
    def extract_and_land() -> dict:
        settings = load_settings()
        ensure_ods_schema(settings)
        started = time.perf_counter()
        games, _weeks = completed_games(settings.max_weeks, settings.max_games)
        if not games:
            raise RuntimeError(
                "NHL не вернул завершённых матчей. Увеличьте NHL_MAX_WEEKS или проверьте API."
            )
        batch_id = current_batch_id()
        landing = RawLanding(settings)
        refresh = os.environ.get("NHL_REFRESH", "0") == "1"
        known = set() if refresh else stored_game_ids(settings)
        keys = []
        total_bytes = 0
        skipped = 0
        for game in games:
            game_id = str(game["id"])
            if game_id in known:
                skipped += 1
                continue
            boxscore, box_url = fetch_boxscore(game_id)
            play_by_play, pbp_url = fetch_play_by_play(game_id)
            body = envelope(
                {"schedule": game, "boxscore": boxscore, "play_by_play": play_by_play},
                source_url=[box_url, pbp_url],
                batch_id=batch_id,
                fetched_at=utc_now_iso(),
            )
            key = f"nhl/game/game={game_id}/batch={batch_id}.json"
            total_bytes += landing.put_json(key, body)
            landing.put_json(f"nhl/game/game={game_id}/latest.json", body)
            keys.append(key)
            if len(keys) % 25 == 0:
                log.info("landed %s new games, skipped %s", len(keys), skipped)
        log.info("new games %s, already stored %s", len(keys), skipped)
        log_ingest(
            settings,
            batch_id=batch_id,
            dag_id=DAG_ID,
            storage="minio",
            entity="nhl_game",
            records_loaded=len(keys),
            payload_bytes=total_bytes,
            duration_ms=elapsed_ms(started),
            details={"games": len(keys), "skipped": skipped},
        )
        return {"keys": keys, "batch_id": batch_id, "skipped": skipped}

    @task
    def load_mongodb(landed: dict) -> int:
        if not landed["keys"]:
            log.info("new games 0, skipped %s", landed.get("skipped", 0))
            return 0
        settings = load_settings()
        ensure_ods_schema(settings)
        started = time.perf_counter()
        landing = RawLanding(settings)
        documents = []
        for key in landed["keys"]:
            body = landing.get_json(key)
            payload = body["payload"]
            documents.append(
                _document(payload["schedule"], payload["boxscore"], payload["play_by_play"], landed["batch_id"])
            )
        loaded = upsert_games(settings, documents)
        log_ingest(
            settings,
            batch_id=landed["batch_id"],
            dag_id=DAG_ID,
            storage="mongodb",
            entity="nhl_game",
            records_loaded=loaded,
            payload_bytes=0,
            duration_ms=elapsed_ms(started),
            details={"keys": len(landed["keys"])},
        )
        log.info("upserted %s game documents", loaded)
        return loaded

    load_mongodb(extract_and_land())


nhl_games_to_mongodb()
