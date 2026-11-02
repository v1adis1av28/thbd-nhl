"""ODS: турнирная таблица NHL в PostgreSQL.

Источник: https://api-web.nhle.com/v1/standings/{date}
Сырой JSON остаётся в MinIO, строковая проекция — в ods.nhl_standings.
Кроме снимка «сейчас» пишутся недельные срезы за NHL_STANDINGS_WEEKS.
Повторный запуск обновляет строку по ключу (дата, команда).
"""

import logging
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "plugins"))

from airflow.sdk import dag, task

from thbd.nhl import fetch_schedule_weeks, fetch_standings
from thbd.settings import current_batch_id, elapsed_ms, load_settings, utc_now_iso
from thbd.stores import RawLanding, ensure_ods_schema, envelope, log_ingest, upsert_standings
from thbd.transform import flatten_standing, weekly_anchor_dates

log = logging.getLogger(__name__)

DAG_ID = "nhl_standings_to_postgres"


@dag(
    dag_id=DAG_ID,
    description="Таблица NHL: сырьё в MinIO, проекция в PostgreSQL",
    schedule="0 12 * * *",
    start_date=datetime(2026, 10, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["nhl", "ods", "lab1", "postgres"],
    default_args={"owner": "thbd", "retries": 2, "retry_delay": timedelta(minutes=1)},
)
def nhl_standings_to_postgres():
    @task
    def extract_and_land() -> dict:
        settings = load_settings()
        ensure_ods_schema(settings)
        started = time.perf_counter()
        batch_id = current_batch_id()
        today = datetime.now(timezone.utc).date().isoformat()
        week_payloads = [payload for payload, _url in fetch_schedule_weeks(settings.standings_weeks)]
        dates = weekly_anchor_dates(week_payloads, today, settings.standings_weeks)
        landing = RawLanding(settings)
        keys = []
        total_bytes = 0
        seen_dates: set[str] = set()
        snapshots = [fetch_standings()] + [fetch_standings(day) for day in dates]
        for payload, source_url in snapshots:
            raw_rows = payload.get("standings") or []
            standings_date = (raw_rows[0].get("date") if raw_rows else None) or "unknown"
            if standings_date in seen_dates:
                continue
            seen_dates.add(standings_date)
            body = envelope(payload, source_url=source_url, batch_id=batch_id, fetched_at=utc_now_iso())
            key = f"nhl/standings/dt={standings_date}/batch={batch_id}.json"
            total_bytes += landing.put_json(key, body)
            keys.append(key)
            log.info("landed standings %s", standings_date)
        if keys:
            landing.put_json("nhl/standings/latest.json", landing.get_json(keys[0]))
        log_ingest(
            settings,
            batch_id=batch_id,
            dag_id=DAG_ID,
            storage="minio",
            entity="nhl_standings",
            records_loaded=len(seen_dates) * 32,
            payload_bytes=total_bytes,
            duration_ms=elapsed_ms(started),
            details={"snapshots": len(keys)},
        )
        return {"keys": keys, "batch_id": batch_id}

    @task
    def load_postgres(landed: dict) -> int:
        settings = load_settings()
        ensure_ods_schema(settings)
        started = time.perf_counter()
        landing = RawLanding(settings)
        loaded = 0
        for key in landed["keys"]:
            body = landing.get_json(key)
            raw_rows = body["payload"].get("standings") or []
            flat_rows = [flatten_standing(row) for row in raw_rows]
            raw_by_abbrev = {flat["team_abbrev"]: raw for flat, raw in zip(flat_rows, raw_rows)}
            source_url = (body.get("source_urls") or ["https://api-web.nhle.com/v1/standings"])[0]
            loaded += upsert_standings(
                settings,
                flat_rows,
                raw_by_abbrev,
                source_url=source_url,
                batch_id=landed["batch_id"],
            )
        log_ingest(
            settings,
            batch_id=landed["batch_id"],
            dag_id=DAG_ID,
            storage="postgres",
            entity="nhl_standings",
            records_loaded=loaded,
            payload_bytes=0,
            duration_ms=elapsed_ms(started),
            details={"snapshots": len(landed["keys"])},
        )
        log.info("upserted %s standings rows", loaded)
        return loaded

    load_postgres(extract_and_land())


nhl_standings_to_postgres()
