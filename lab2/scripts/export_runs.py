import requests
import json
session = requests.Session()
base = 'http://airflow-apiserver:8080'
response = session.post(base+'/auth/token',json={'username':'airflow','password':'airflow'},timeout=30)
response.raise_for_status()
session.headers['Authorization']='Bearer '+response.json()['access_token']
result = {}
for dag in ['lab2_ods_to_dds','lab2_dds_quality']:
    response = session.get(f'{base}/api/v2/dags/{dag}/dagRuns',params={'limit':2,'order_by':'-run_after'},timeout=30)
    response.raise_for_status()
    result[dag]=[{key:run.get(key) for key in ['dag_run_id','state','start_date','end_date']} for run in response.json()['dag_runs']]
print(json.dumps(result,ensure_ascii=False,indent=2))
