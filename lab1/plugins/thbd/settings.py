"""Общие настройки и небольшие помощники, одинаковые для всех DAG."""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    minio_bucket: str
    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str
    mongo_uri: str
    mongo_db: str
    neo4j_uri: str
    neo4j_user: str
    neo4j_password: str
    max_games: int
    max_weeks: int
    graph_boxscores: int
    standings_weeks: int
    sql_path: str


def load_settings() -> Settings:
    return Settings(
        minio_endpoint=os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
        minio_access_key=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        minio_secret_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
        minio_bucket=os.environ.get("MINIO_BUCKET", "nhl-raw"),
        postgres_host=os.environ.get("POSTGRES_ODS_HOST", "postgres-ods"),
        postgres_port=int(os.environ.get("POSTGRES_ODS_PORT", "5432")),
        postgres_db=os.environ.get("POSTGRES_ODS_DB", "nhl_ods"),
        postgres_user=os.environ.get("POSTGRES_ODS_USER", "nhl"),
        postgres_password=os.environ.get("POSTGRES_ODS_PASSWORD", "nhlods"),
        mongo_uri=os.environ.get(
            "MONGO_URI",
            "mongodb://nhl:nhlods@mongodb:27017/nhl_ods?authSource=admin",
        ),
        mongo_db=os.environ.get("MONGO_DB", "nhl_ods"),
        neo4j_uri=os.environ.get("NEO4J_URI", "bolt://neo4j:7687"),
        neo4j_user=os.environ.get("NEO4J_USER", "neo4j"),
        neo4j_password=os.environ.get("NEO4J_PASSWORD", "nhlgraph"),
        max_games=int(os.environ.get("NHL_MAX_GAMES", "12")),
        max_weeks=int(os.environ.get("NHL_MAX_WEEKS", "4")),
        graph_boxscores=int(os.environ.get("NHL_GRAPH_BOXSCORES", "8")),
        standings_weeks=int(os.environ.get("NHL_STANDINGS_WEEKS", "16")),
        sql_path=os.environ.get("ODS_SQL_PATH", "/opt/airflow/sql/001_ods.sql"),
    )


def current_batch_id() -> str:
    """Идентификатор пакета. У ручного запуска в Airflow 3 logical_date пустой."""
    from airflow.sdk import get_current_context

    context = get_current_context()
    logical = context.get("logical_date")
    if hasattr(logical, "strftime"):
        return logical.strftime("%Y%m%dT%H%M%S")
    run_id = context.get("run_id")
    dag_run = context.get("dag_run")
    if run_id is None and dag_run is not None:
        run_id = getattr(dag_run, "run_id", None)
    if isinstance(run_id, str) and run_id:
        return "".join(ch for ch in run_id if ch.isalnum())[:40]
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def elapsed_ms(started: float) -> int:
    return int((time.perf_counter() - started) * 1000)


def sql_statements(path: str | Path) -> list[str]:
    lines = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        if line.strip().startswith("--"):
            continue
        lines.append(line)
    return [part.strip() for part in "\n".join(lines).split(";") if part.strip()]
