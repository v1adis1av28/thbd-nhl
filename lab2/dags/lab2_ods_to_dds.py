"""ЛР2: согласованный снимок ODS → измерения SCD2 и факты DDS."""
import sys
from datetime import datetime, timezone, timedelta
sys.path[:0] = ['/opt/airflow/plugins', '/opt/airflow/lab2']
from airflow.sdk import dag, task
from airflow.providers.standard.sensors.python import PythonSensor
from processing.upstream import upstream_succeeded
from processing.warehouse import rebuild

@dag(dag_id='lab2_ods_to_dds', schedule=None,
     start_date=datetime(2026, 10, 1, tzinfo=timezone.utc), catchup=False,
     max_active_runs=1, tags=['lab2', 'dds', 'nhl'],
     default_args={'owner': 'thbd', 'retries': 1, 'retry_delay': timedelta(seconds=30),
                   'execution_timeout': timedelta(minutes=30)})
def pipeline():
    waits = [PythonSensor(task_id='wait_' + name,
             python_callable=upstream_succeeded, op_kwargs={'dag_id': name},
             mode='reschedule', poke_interval=30, timeout=3600)
             for name in ['nhl_standings_to_postgres', 'nhl_games_to_mongodb']]
    @task
    def transform_and_load():
        return rebuild()
    waits >> transform_and_load()
pipeline()
