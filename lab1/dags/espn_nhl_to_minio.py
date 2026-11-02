"""ODS: табло и новости NHL с ESPN в MinIO.

Второй источник, другая форма: лента событий и статьи, а не статистическая таблица.
Объектное хранилище здесь и есть приёмник. Второй таск перечитывает сырьё
и кладёт короткий manifest, чтобы на защите было что открыть глазами.
"""

import logging
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "plugins"))

from airflow.sdk import dag, task

from thbd.espn import NEWS_URL, SCOREBOARD_URL, fetch_news, fetch_scoreboard
from thbd.settings import current_batch_id, elapsed_ms, load_settings, utc_now_iso
from thbd.stores import RawLanding, ensure_ods_schema, envelope, log_ingest
from thbd.transform import summarize_espn_news, summarize_espn_scoreboard

log = logging.getLogger(__name__)

DAG_ID = "espn_nhl_to_minio"


@dag(
    dag_id=DAG_ID,
    description="ESPN NHL: табло и новости целиком в MinIO",
    schedule="0 13 * * *",
    start_date=datetime(2026, 10, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["nhl", "espn", "ods", "lab1", "minio"],
    default_args={"owner": "thbd", "retries": 2, "retry_delay": timedelta(minutes=1)},
)
def espn_nhl_to_minio():
    @task
    def extract_and_land() -> dict:
        settings = load_settings()
        ensure_ods_schema(settings)
        started = time.perf_counter()
        scoreboard, scoreboard_url = fetch_scoreboard()
        news, news_url = fetch_news()
        batch_id = current_batch_id()
        fetched_at = utc_now_iso()
        day = (scoreboard.get("day") or {}).get("date") or fetched_at[:10]
        landing = RawLanding(settings)
        scoreboard_key = f"espn/scoreboard/dt={day}/batch={batch_id}.json"
        news_key = f"espn/news/dt={day}/batch={batch_id}.json"
        scoreboard_bytes = landing.put_json(
            scoreboard_key,
            envelope(scoreboard, source_url=scoreboard_url, batch_id=batch_id, fetched_at=fetched_at),
        )
        news_bytes = landing.put_json(
            news_key,
            envelope(news, source_url=news_url, batch_id=batch_id, fetched_at=fetched_at),
        )
        landing.put_json("espn/scoreboard/latest.json", landing.get_json(scoreboard_key))
        landing.put_json("espn/news/latest.json", landing.get_json(news_key))
        total = scoreboard_bytes + news_bytes
        log_ingest(
            settings,
            batch_id=batch_id,
            dag_id=DAG_ID,
            storage="minio",
            entity="espn_nhl",
            records_loaded=len(scoreboard.get("events") or []) + len(news.get("articles") or []),
            payload_bytes=total,
            duration_ms=elapsed_ms(started),
            details={"scoreboard_url": SCOREBOARD_URL, "news_url": NEWS_URL},
        )
        log.info("landed espn scoreboard %s and news %s", scoreboard_key, news_key)
        return {
            "scoreboard_key": scoreboard_key,
            "news_key": news_key,
            "batch_id": batch_id,
            "day": day,
        }

    @task
    def publish_manifest(landed: dict) -> dict:
        settings = load_settings()
        started = time.perf_counter()
        landing = RawLanding(settings)
        scoreboard = landing.get_json(landed["scoreboard_key"])["payload"]
        news = landing.get_json(landed["news_key"])["payload"]
        events = summarize_espn_scoreboard(scoreboard)
        articles = summarize_espn_news(news)
        manifest = {
            "batch_id": landed["batch_id"],
            "day": landed["day"],
            "event_count": len(events),
            "article_count": len(articles),
            "events": events,
            "articles": articles,
        }
        key = f"espn/manifest/dt={landed['day']}/batch={landed['batch_id']}.json"
        nbytes = landing.put_json(key, manifest)
        landing.put_json("espn/manifest/latest.json", manifest)
        log_ingest(
            settings,
            batch_id=landed["batch_id"],
            dag_id=DAG_ID,
            storage="minio",
            entity="espn_manifest",
            records_loaded=len(events) + len(articles),
            payload_bytes=nbytes,
            duration_ms=elapsed_ms(started),
            details={"key": key},
        )
        log.info("manifest %s events=%s articles=%s", key, len(events), len(articles))
        return {"events": len(events), "articles": len(articles), "key": key}

    publish_manifest(extract_and_land())


espn_nhl_to_minio()
