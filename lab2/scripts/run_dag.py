"""Запустить DAG через Airflow REST и ждать именно созданный run_id."""
import os
import sys
import time
import requests
base = 'http://airflow-apiserver:8080'
dag = sys.argv[1]
session = requests.Session()
response = session.post(base + '/auth/token', json={
    'username': os.getenv('_AIRFLOW_WWW_USER_USERNAME', 'airflow'),
    'password': os.getenv('_AIRFLOW_WWW_USER_PASSWORD', 'airflow')}, timeout=30)
response.raise_for_status()
session.headers['Authorization'] = 'Bearer ' + response.json()['access_token']
response = session.patch(f'{base}/api/v2/dags/{dag}', json={'is_paused': False}, timeout=30)
response.raise_for_status()
response = session.post(f'{base}/api/v2/dags/{dag}/dagRuns', json={'logical_date': None}, timeout=30)
response.raise_for_status()
run_id = response.json()['dag_run_id']
print(dag, run_id, flush=True)
for attempt in range(240):
    response = session.get(f'{base}/api/v2/dags/{dag}/dagRuns/{run_id}', timeout=30)
    response.raise_for_status()
    state = response.json()['state']
    if state == 'success':
        print('SUCCESS', dag, run_id, flush=True)
        break
    if state == 'failed':
        raise SystemExit(f'FAILED: {dag} {run_id}; см. логи Airflow')
    time.sleep(5)
else:
    raise SystemExit('Timeout: DAG не завершился за 20 минут')
