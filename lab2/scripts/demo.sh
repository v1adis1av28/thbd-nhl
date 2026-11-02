#!/usr/bin/env bash
# Единая точка входа для последовательной защиты. Запускается из любой папки.
set -euo pipefail
cd "$(dirname "$0")/../../lab1"
compose=(docker compose -f docker-compose.yaml -f ../lab2/compose.yaml)
case "${1:-help}" in
  up) "${compose[@]}" up -d ;;
  run)
    # Выполнение последовательно: последняя попытка upstream обязана быть успешной.
    for dag in lab2_ods_to_dds lab2_dds_quality; do
      "${compose[@]}" exec -T airflow-scheduler python /opt/airflow/lab2/scripts/run_dag.py "$dag"
    done ;;
  results)
    "${compose[@]}" exec -T airflow-scheduler python /opt/airflow/lab2/scripts/export_results.py ;;
  *) echo 'Использование: bash lab2/scripts/demo.sh up|run|results' ;;
esac
