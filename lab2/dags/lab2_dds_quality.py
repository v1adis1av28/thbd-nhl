"""Проверки качества DDS. Ждёт DAG фактов."""

import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, "/opt/airflow/plugins")
sys.path.insert(0, "/opt/airflow/lab2")

from airflow.sdk import dag, task
try:
    from airflow.sensors.python import PythonSensor
except ImportError:
    from airflow.providers.standard.sensors.python import PythonSensor

from processing.upstream import upstream_succeeded
from processing.warehouse import collect_quality
from thbd.settings import load_settings
from thbd.stores import pg_cursor


@dag(
    dag_id="lab2_dds_quality",
    description="ЛР2: сверка объёмов и поиск аномалий",
    schedule=None,
    start_date=datetime(2026, 10, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["lab2", "dds", "quality", "nhl"],
    default_args={"owner": "thbd", "retries": 1, "retry_delay": timedelta(minutes=2)},
)
def lab2_dds_quality():
    @task
    def run_checks() -> list[dict]:
        settings = load_settings()
        with pg_cursor(settings) as cursor:
            cursor.execute("SELECT games, goals, standings FROM dds.load_manifest")
            source = cursor.fetchone()
            if source is None:
                raise RuntimeError("Нет зафиксированного входного снимка DDS")
            cursor.execute("DELETE FROM dds.dq_result")
            checks = collect_quality(cursor, *source)
        failed = [check["check_name"] for check in checks if check["severity"] == "error" and not check["passed"]]
        if failed:
            raise RuntimeError("Проверки качества не прошли: " + ", ".join(failed))
        return checks

    wait_facts = PythonSensor(
        task_id="wait_lab2_ods_to_dds",
        python_callable=upstream_succeeded,
        op_kwargs={"dag_id": "lab2_ods_to_dds"},
        mode="reschedule",
        poke_interval=30,
        timeout=60 * 60,
    )
    wait_facts >> run_checks()


lab2_dds_quality()
