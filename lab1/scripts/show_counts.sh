#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

echo "== PostgreSQL: строки таблицы =="
docker compose exec -T postgres-ods psql -U nhl -d nhl_ods -c \
  "SELECT count(*) AS standings_rows FROM ods.nhl_standings;"

echo "== MongoDB: документы матчей =="
docker compose exec -T mongodb mongosh --quiet \
  -u nhl -p nhlods --authenticationDatabase admin nhl_ods \
  --eval 'print("games", db.games.countDocuments())'

echo "== Neo4j: узлы =="
docker compose exec -T neo4j cypher-shell -u neo4j -p nhlgraph \
  "MATCH (n) RETURN labels(n)[0] AS label, count(*) AS nodes ORDER BY label"

echo "== MinIO: бакет nhl-raw =="
docker compose exec -T airflow-scheduler python /opt/airflow/scripts/list_bucket.py
