#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

dags=(
  nhl_standings_to_postgres
  nhl_games_to_mongodb
  espn_nhl_to_minio
  nhl_graph_to_neo4j
)

for dag in "${dags[@]}"; do
  docker compose exec -T airflow-scheduler airflow dags unpause "$dag"
  docker compose exec -T airflow-scheduler airflow dags trigger "$dag"
  echo "triggered $dag"
done
