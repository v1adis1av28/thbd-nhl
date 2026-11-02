"""Экспорт фактических результатов, вывод в stdout. Host сохраняет их в results/."""
import sys
import json
from datetime import datetime, timezone
sys.path[:0] = ['/opt/airflow/plugins', '/opt/airflow/lab2']
from thbd.settings import load_settings
from thbd.stores import pg_cursor
result = {'captured_at': datetime.now(timezone.utc).isoformat()}
with pg_cursor(load_settings()) as cursor:
    result['counts'] = {}
    for table in ['dim_date','dim_conference','dim_division','dim_team','dim_player','fact_game','fact_goal','fact_standings']:
        cursor.execute(f'SELECT count(*) FROM dds.{table}')
        result['counts'][table] = cursor.fetchone()[0]
    for name, query in {
        'quality': 'SELECT check_name,severity,passed,expected,actual FROM dds.dq_result ORDER BY check_name',
        'manifest': 'SELECT * FROM dds.load_manifest',
        'player_history': '''SELECT p.player_id,p.player_name,t.team_abbrev,p.valid_from,p.valid_to,p.is_current
             FROM dds.dim_player p LEFT JOIN dds.dim_team t USING(team_sk)
             WHERE player_id IN (SELECT player_id FROM dds.dim_player GROUP BY player_id HAVING count(*)>1)
             ORDER BY player_id,valid_from LIMIT 12''',
        'goals_sample': '''SELECT g.game_id,g.event_id,g.time_in_period,g.x,g.y,p.player_name
             FROM dds.fact_goal g LEFT JOIN dds.dim_player p ON p.player_sk=g.scorer_player_sk
             ORDER BY g.game_id,g.event_id LIMIT 5''',
    }.items():
        cursor.execute(query)
        columns = [d[0] for d in cursor.description]
        result[name] = [dict(zip(columns,row)) for row in cursor.fetchall()]
print(json.dumps(result,ensure_ascii=False,indent=2,default=str))
