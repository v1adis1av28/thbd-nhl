"""Негативный интеграционный тест DQ с откатом тестовой ошибки."""
import sys
sys.path[:0] = ['/opt/airflow/plugins', '/opt/airflow/lab2']
from thbd.settings import load_settings
from thbd.stores import pg_cursor
from processing.warehouse import collect_quality
with pg_cursor(load_settings()) as cursor:
    cursor.execute('SELECT games,goals,standings FROM dds.load_manifest')
    source = cursor.fetchone()
    cursor.execute('SELECT count(*) FROM ods.nhl_standings')
    before = cursor.fetchone()[0]
    cursor.execute('SAVEPOINT quality_test')
    cursor.execute('UPDATE dds.fact_standings SET points=-1 WHERE ctid=(SELECT ctid FROM dds.fact_standings LIMIT 1)')
    cursor.execute('DELETE FROM dds.dq_result')
    checks = collect_quality(cursor,*source)
    assert any(c['check_name']=='standings_points_non_negative' and not c['passed'] for c in checks)
    cursor.execute('ROLLBACK TO SAVEPOINT quality_test')
    cursor.execute('SELECT count(*) FROM dds.fact_standings WHERE points<0')
    assert cursor.fetchone()[0] == 0
    cursor.execute('SELECT count(*) FROM ods.nhl_standings')
    assert cursor.fetchone()[0] == before
    cursor.execute("SELECT count(*) FROM dds.dq_result WHERE severity='error' AND NOT passed")
    assert cursor.fetchone()[0] == 0
print('PASS: negative points detected, test corruption rolled back, original DQ restored, ODS unchanged')
