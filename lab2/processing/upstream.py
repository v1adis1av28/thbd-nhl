"""Сенсор ждёт успех последнего запуска DAG независимо от logical_date.

В Airflow 3 задача не может читать метаданные через ORM. Успех внешнего
DAG смотрим через REST API того же Airflow.
"""

from __future__ import annotations

import os

import requests


def upstream_succeeded(dag_id: str) -> bool:
    base = os.environ.get("AIRFLOW_API_BASE", "http://airflow-apiserver:8080")
    username = os.environ.get("_AIRFLOW_WWW_USER_USERNAME", "airflow")
    password = os.environ.get("_AIRFLOW_WWW_USER_PASSWORD", "airflow")
    token_response = requests.post(
        f"{base}/auth/token",
        json={"username": username, "password": password},
        timeout=30,
    )
    token_response.raise_for_status()
    token = token_response.json()["access_token"]
    runs_response = requests.get(
        f"{base}/api/v2/dags/{dag_id}/dagRuns",
        params={"limit": 1, "order_by": "-run_after"},
        headers={"Authorization": f"Bearer {token}"},
        timeout=30,
    )
    runs_response.raise_for_status()
    runs = runs_response.json().get("dag_runs") or []
    return bool(runs and runs[0]["state"] == "success")
