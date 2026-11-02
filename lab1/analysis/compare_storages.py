"""Один и тот же снимок таблицы NHL — запись и запрос в четырёх хранилищах.

Запуск из контейнера scheduler, где уже есть клиенты и сеть до баз:

    docker compose exec -T airflow-scheduler python /opt/airflow/analysis/compare_storages.py

Скрипт не трогает ods.nhl_standings и граф Team. Замер идёт в отдельных
объектах bench.*, коллекции bench_standings, ключе bench/standings.json
и метке BenchTeam. Медиана пяти прогонов пишется в analysis/output.
"""

from __future__ import annotations

import json
import sys
import time
from collections import Counter
from pathlib import Path

sys.path.insert(0, "/opt/airflow/plugins")
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "plugins"))

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import psycopg2
from neo4j import GraphDatabase
from psycopg2.extras import execute_values
from pymongo import MongoClient

from thbd.nhl import fetch_standings
from thbd.settings import load_settings
from thbd.stores import s3_client
from thbd.transform import flatten_standing

REPEATS = 5
STORES = ("PostgreSQL", "MongoDB", "MinIO", "Neo4j")
OUTPUT = Path(__file__).resolve().parent / "output"


def median(values: list[float]) -> float:
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2


def ms(started: float) -> float:
    return (time.perf_counter() - started) * 1000


def prepare_postgres(settings, division: str):
    connection = psycopg2.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        dbname=settings.postgres_db,
        user=settings.postgres_user,
        password=settings.postgres_password,
        connect_timeout=8,
    )
    with connection.cursor() as cursor:
        cursor.execute("CREATE SCHEMA IF NOT EXISTS bench")
        cursor.execute("DROP TABLE IF EXISTS bench.standings")
        cursor.execute(
            """
            CREATE TABLE bench.standings (
                team_abbrev text PRIMARY KEY,
                team_name text NOT NULL,
                division_name text NOT NULL,
                points integer
            )
            """
        )
    connection.commit()
    return connection, division


def bench_postgres(connection, rows, division: str) -> tuple[float, float, int]:
    with connection.cursor() as cursor:
        cursor.execute("TRUNCATE bench.standings")
    connection.commit()
    values = [(row["team_abbrev"], row["team_name"], row["division_name"], row["points"]) for row in rows]
    started = time.perf_counter()
    with connection.cursor() as cursor:
        execute_values(
            cursor,
            "INSERT INTO bench.standings (team_abbrev, team_name, division_name, points) VALUES %s",
            values,
        )
    connection.commit()
    write_ms = ms(started)
    started = time.perf_counter()
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT team_abbrev, points
            FROM bench.standings
            WHERE division_name = %s
            ORDER BY points DESC, team_abbrev
            """,
            (division,),
        )
        found = cursor.fetchall()
    return write_ms, ms(started), len(found)


def bench_mongo(database, rows, division: str) -> tuple[float, float, int]:
    collection = database["bench_standings"]
    collection.delete_many({})
    docs = [
        {
            "team_abbrev": row["team_abbrev"],
            "team_name": row["team_name"],
            "division_name": row["division_name"],
            "points": row["points"],
        }
        for row in rows
    ]
    started = time.perf_counter()
    collection.insert_many(docs)
    write_ms = ms(started)
    started = time.perf_counter()
    found = list(
        collection.find({"division_name": division}, {"_id": 0, "team_abbrev": 1, "points": 1}).sort(
            [("points", -1), ("team_abbrev", 1)]
        )
    )
    return write_ms, ms(started), len(found)


def bench_minio(client, bucket: str, rows, division: str) -> tuple[float, float, int]:
    body = json.dumps(rows, ensure_ascii=False).encode("utf-8")
    started = time.perf_counter()
    client.put_object(Bucket=bucket, Key="bench/standings.json", Body=body, ContentType="application/json")
    write_ms = ms(started)
    started = time.perf_counter()
    raw = client.get_object(Bucket=bucket, Key="bench/standings.json")["Body"].read()
    loaded = json.loads(raw)
    found = [row for row in loaded if row["division_name"] == division]
    found.sort(key=lambda row: (-(row["points"] or 0), row["team_abbrev"]))
    return write_ms, ms(started), len(found)


def bench_neo4j(session, rows, division: str) -> tuple[float, float, int]:
    session.run("MATCH (t:BenchTeam) DETACH DELETE t").consume()
    payload = [
        {
            "team_abbrev": row["team_abbrev"],
            "team_name": row["team_name"],
            "division_name": row["division_name"],
            "points": row["points"],
        }
        for row in rows
    ]
    started = time.perf_counter()
    session.run(
        """
        UNWIND $rows AS row
        CREATE (t:BenchTeam {
            abbrev: row.team_abbrev,
            name: row.team_name,
            division_name: row.division_name,
            points: row.points
        })
        """,
        rows=payload,
    ).consume()
    write_ms = ms(started)
    started = time.perf_counter()
    result = session.run(
        """
        MATCH (t:BenchTeam {division_name: $division})
        RETURN t.abbrev AS abbrev, t.points AS points
        ORDER BY points DESC, abbrev
        """,
        division=division,
    )
    found = list(result)
    return write_ms, ms(started), len(found)


def read_ingest(settings) -> list[dict]:
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
            cursor.execute(
                """
                SELECT dag_id, storage, entity, records_loaded, payload_bytes, duration_ms
                FROM ods.ingest_log
                ORDER BY id
                """
            )
            columns = ["dag_id", "storage", "entity", "records_loaded", "payload_bytes", "duration_ms"]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
    except psycopg2.Error:
        connection.rollback()
        return []
    finally:
        connection.close()


def save_grouped_chart(path: Path, title: str, ylabel: str, series: dict[str, list[float]]) -> None:
    fig, ax = plt.subplots(figsize=(8.2, 4.6))
    labels = list(series.keys())
    width = 0.36
    positions = list(range(len(labels)))
    write_values = [series[label][0] for label in labels]
    query_values = [series[label][1] for label in labels]
    ax.bar([pos - width / 2 for pos in positions], write_values, width, label="Запись")
    ax.bar([pos + width / 2 for pos in positions], query_values, width, label="Запрос дивизиона")
    ax.set_xticks(positions)
    ax.set_xticklabels(labels)
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.legend()
    fig.tight_layout()
    fig.savefig(path, dpi=140)
    plt.close(fig)


def save_ingest_chart(path: Path, rows: list[dict]) -> None:
    labels = [f"{row['storage']}:{row['entity']}" for row in rows]
    values = [row["payload_bytes"] / 1024 for row in rows]
    fig, ax = plt.subplots(figsize=(8.2, 4.8))
    ax.barh(labels, values)
    ax.set_xlabel("Килобайты сырого пакета")
    ax.set_title("Сколько байт пришло в ODS за прогон DAG")
    fig.tight_layout()
    fig.savefig(path, dpi=140)
    plt.close(fig)


def main() -> None:
    settings = load_settings()
    payload, source_url = fetch_standings()
    rows = [flatten_standing(row) for row in payload.get("standings") or []]
    if not rows:
        raise RuntimeError("Пустая таблица NHL, сравнивать нечего")
    division = Counter(row["division_name"] for row in rows).most_common(1)[0][0]
    expected = sum(1 for row in rows if row["division_name"] == division)

    timings = {name: {"write": [], "query": []} for name in STORES}
    connection, _ = prepare_postgres(settings, division)
    mongo_client = MongoClient(settings.mongo_uri, serverSelectionTimeoutMS=8000)
    database = mongo_client[settings.mongo_db]
    minio = s3_client(settings)
    names = {item["Name"] for item in minio.list_buckets().get("Buckets", [])}
    if settings.minio_bucket not in names:
        minio.create_bucket(Bucket=settings.minio_bucket)
    driver = GraphDatabase.driver(
        settings.neo4j_uri,
        auth=(settings.neo4j_user, settings.neo4j_password),
        connection_timeout=8,
    )
    try:
        for _ in range(REPEATS):
            write_ms, query_ms, found = bench_postgres(connection, rows, division)
            if found != expected:
                raise RuntimeError(f"PostgreSQL вернул {found} клубов дивизиона, ожидалось {expected}")
            timings["PostgreSQL"]["write"].append(write_ms)
            timings["PostgreSQL"]["query"].append(query_ms)

            write_ms, query_ms, found = bench_mongo(database, rows, division)
            if found != expected:
                raise RuntimeError(f"MongoDB вернул {found} клубов дивизиона, ожидалось {expected}")
            timings["MongoDB"]["write"].append(write_ms)
            timings["MongoDB"]["query"].append(query_ms)

            write_ms, query_ms, found = bench_minio(minio, settings.minio_bucket, rows, division)
            if found != expected:
                raise RuntimeError(f"MinIO вернул {found} клубов дивизиона, ожидалось {expected}")
            timings["MinIO"]["write"].append(write_ms)
            timings["MinIO"]["query"].append(query_ms)

            with driver.session() as session:
                write_ms, query_ms, found = bench_neo4j(session, rows, division)
            if found != expected:
                raise RuntimeError(f"Neo4j вернул {found} клубов дивизиона, ожидалось {expected}")
            timings["Neo4j"]["write"].append(write_ms)
            timings["Neo4j"]["query"].append(query_ms)
    finally:
        connection.close()
        mongo_client.close()
        driver.close()

    summary = {
        name: {
            "write_median_ms": round(median(timings[name]["write"]), 2),
            "query_median_ms": round(median(timings[name]["query"]), 2),
            "write_runs_ms": [round(value, 2) for value in timings[name]["write"]],
            "query_runs_ms": [round(value, 2) for value in timings[name]["query"]],
        }
        for name in STORES
    }
    ingest_rows = read_ingest(settings)
    OUTPUT.mkdir(parents=True, exist_ok=True)
    result = {
        "source_url": source_url,
        "teams": len(rows),
        "division": division,
        "division_teams": expected,
        "repeats": REPEATS,
        "stores": summary,
        "ingest_log": ingest_rows,
    }
    (OUTPUT / "results.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    save_grouped_chart(
        OUTPUT / "benchmark_latency.png",
        f"Медиана {REPEATS} прогонов, дивизион {division}, клубов в снимке: {len(rows)}",
        "Миллисекунды",
        {name: [summary[name]["write_median_ms"], summary[name]["query_median_ms"]] for name in STORES},
    )
    if ingest_rows:
        save_ingest_chart(OUTPUT / "ingest_bytes.png", ingest_rows)

    lines = [
        "# Замеры хранилищ",
        "",
        f"Источник снимка: `{source_url}`.",
        f"Клубов в снимке: {len(rows)}. Дивизион для запроса: {division} ({expected} клубов).",
        f"Повторов: {REPEATS}. В таблицу попала медиана.",
        "",
        "Вопрос один и тот же: записать снимок и выбрать клубы дивизиона, отсортировав по очкам.",
        "MinIO отвечает фильтром в Python после чтения объекта. Отдельного языка запросов у бакета нет.",
        "Метка Neo4j в замере — `BenchTeam`. Боевые узлы `Team` этот скрипт не удаляет.",
        "",
        "| Хранилище | Запись, мс | Запрос, мс |",
        "|---|---:|---:|",
    ]
    for name in STORES:
        lines.append(
            f"| {name} | {summary[name]['write_median_ms']} | {summary[name]['query_median_ms']} |"
        )
    lines.extend(["", "## Журнал загрузок DAG", ""])
    if not ingest_rows:
        lines.append("Таблица `ods.ingest_log` пуста. Сначала запустите четыре DAG, затем этот скрипт ещё раз.")
    else:
        lines.append("| DAG | Хранилище | Сущность | Записей | Байт | Мс |")
        lines.append("|---|---|---|---:|---:|---:|")
        for row in ingest_rows:
            lines.append(
                f"| {row['dag_id']} | {row['storage']} | {row['entity']} | {row['records_loaded']} | {row['payload_bytes']} | {row['duration_ms']} |"
            )
    lines.append("")
    (OUTPUT / "results.md").write_text("\n".join(lines), encoding="utf-8")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
