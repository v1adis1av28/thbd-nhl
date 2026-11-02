"""Клиенты четырёх хранилищ ODS.

Подключения продублированы переменными AIRFLOW_CONN_* и видны в UI Airflow.
Код ходит в базы напрямую: у MinIO нужен path-style, у Neo4j нет смысла
протаскивать URI через общий разборщик, а параметры те же, что в .env.
"""

from __future__ import annotations

import json
from contextlib import contextmanager

import boto3
import psycopg2
from botocore.client import Config
from neo4j import GraphDatabase
from psycopg2.extras import Json, execute_values
from pymongo import MongoClient, ReplaceOne

from thbd.settings import Settings, sql_statements


def s3_client(settings: Settings):
    return boto3.client(
        "s3",
        endpoint_url=settings.minio_endpoint,
        aws_access_key_id=settings.minio_access_key,
        aws_secret_access_key=settings.minio_secret_key,
        region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


class RawLanding:
    """Неизменяемый архив оригинальных ответов. Ключ содержит batch_id."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self.client = s3_client(settings)
        self.bucket = settings.minio_bucket
        self._ensure_bucket()

    def _ensure_bucket(self) -> None:
        names = {item["Name"] for item in self.client.list_buckets().get("Buckets", [])}
        if self.bucket not in names:
            self.client.create_bucket(Bucket=self.bucket)

    def put_json(self, key: str, payload: dict) -> int:
        body = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
        self.client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=body,
            ContentType="application/json",
        )
        return len(body)

    def get_json(self, key: str) -> dict:
        obj = self.client.get_object(Bucket=self.bucket, Key=key)
        return json.loads(obj["Body"].read())


def envelope(payload: dict, *, source_url: str | list[str], batch_id: str, fetched_at: str) -> dict:
    urls = [source_url] if isinstance(source_url, str) else list(source_url)
    return {
        "source_urls": urls,
        "fetched_at": fetched_at,
        "batch_id": batch_id,
        "payload": payload,
    }


@contextmanager
def pg_cursor(settings: Settings):
    connection = psycopg2.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        dbname=settings.postgres_db,
        user=settings.postgres_user,
        password=settings.postgres_password,
        connect_timeout=8,
    )
    try:
        with connection.cursor() as cursor:
            yield cursor
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def ensure_ods_schema(settings: Settings) -> None:
    with pg_cursor(settings) as cursor:
        for statement in sql_statements(settings.sql_path):
            cursor.execute(statement)


def log_ingest(
    settings: Settings,
    *,
    batch_id: str,
    dag_id: str,
    storage: str,
    entity: str,
    records_loaded: int,
    payload_bytes: int,
    duration_ms: int,
    details: dict | None = None,
) -> None:
    with pg_cursor(settings) as cursor:
        cursor.execute(
            """
            INSERT INTO ods.ingest_log (
                batch_id, dag_id, storage, entity, records_loaded,
                payload_bytes, duration_ms, details
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                batch_id,
                dag_id,
                storage,
                entity,
                records_loaded,
                payload_bytes,
                duration_ms,
                Json(details) if details is not None else None,
            ),
        )


def upsert_standings(
    settings: Settings,
    rows: list[dict],
    raw_by_abbrev: dict[str, dict],
    *,
    source_url: str,
    batch_id: str,
) -> int:
    values = []
    for row in rows:
        raw = raw_by_abbrev[row["team_abbrev"]]
        values.append(
            (
                row["standings_date"],
                row["team_abbrev"],
                row["team_name"],
                row["conference_abbrev"],
                row["conference_name"],
                row["division_abbrev"],
                row["division_name"],
                row["games_played"],
                row["wins"],
                row["losses"],
                row["ot_losses"],
                row["points"],
                row["goal_differential"],
                Json(raw),
                source_url,
                batch_id,
            )
        )
    sql = """
        INSERT INTO ods.nhl_standings (
            standings_date, team_abbrev, team_name, conference_abbrev, conference_name,
            division_abbrev, division_name, games_played, wins, losses, ot_losses,
            points, goal_differential, raw_payload, source_url, batch_id
        ) VALUES %s
        ON CONFLICT (standings_date, team_abbrev) DO UPDATE SET
            team_name = EXCLUDED.team_name,
            conference_abbrev = EXCLUDED.conference_abbrev,
            conference_name = EXCLUDED.conference_name,
            division_abbrev = EXCLUDED.division_abbrev,
            division_name = EXCLUDED.division_name,
            games_played = EXCLUDED.games_played,
            wins = EXCLUDED.wins,
            losses = EXCLUDED.losses,
            ot_losses = EXCLUDED.ot_losses,
            points = EXCLUDED.points,
            goal_differential = EXCLUDED.goal_differential,
            raw_payload = EXCLUDED.raw_payload,
            source_url = EXCLUDED.source_url,
            batch_id = EXCLUDED.batch_id,
            ingested_at = now()
    """
    with pg_cursor(settings) as cursor:
        execute_values(cursor, sql, values, page_size=100)
    return len(values)


def mongo_db(settings: Settings):
    client = MongoClient(settings.mongo_uri, serverSelectionTimeoutMS=8000)
    return client, client[settings.mongo_db]


def stored_game_ids(settings: Settings) -> set[str]:
    client, database = mongo_db(settings)
    try:
        return {str(value) for value in database["games"].distinct("game_id")}
    finally:
        client.close()


def upsert_games(settings: Settings, documents: list[dict]) -> int:
    if not documents:
        return 0
    client, database = mongo_db(settings)
    try:
        collection = database["games"]
        operations = [
            ReplaceOne({"_id": document["_id"]}, document, upsert=True) for document in documents
        ]
        collection.bulk_write(operations, ordered=False)
        collection.create_index("game_date")
        collection.create_index("home.abbrev")
        collection.create_index("away.abbrev")
        collection.create_index("plays.typeDescKey")
    finally:
        client.close()
    return len(documents)
