-- Что показать на защите ЛР2. База nhl_ods, схема dds.

-- Текущий дивизион клуба. Снежинка: команда → дивизион → конференция.
SELECT t.team_abbrev, t.team_name, d.division_name, c.conference_name, t.valid_from
FROM dds.dim_team t
LEFT JOIN dds.dim_division d ON d.division_sk = t.division_sk
LEFT JOIN dds.dim_conference c ON c.conference_sk = d.conference_sk
WHERE t.is_current
ORDER BY c.conference_name, d.division_name, t.team_abbrev;

-- Версии SCD2, если дивизион или имя менялись.
SELECT team_abbrev, team_name, valid_from, valid_to, is_current
FROM dds.dim_team
WHERE team_abbrev IN (
    SELECT team_abbrev FROM dds.dim_team GROUP BY team_abbrev HAVING count(*) > 1
)
ORDER BY team_abbrev, valid_from;

-- Голы матча с автором.
SELECT g.game_id, g.time_in_period, g.x, g.y, g.shot_type, p.player_name
FROM dds.fact_goal g
LEFT JOIN dds.dim_player p ON p.player_sk = g.scorer_player_sk
ORDER BY g.game_id, g.event_id
LIMIT 20;

-- Проверки качества.
SELECT check_name, severity, passed, expected, actual, details
FROM dds.dq_result
ORDER BY severity, check_name;

-- SCD игроков: реальные наблюдаемые смены команды.
SELECT p.player_id, p.player_name, t.team_abbrev, p.valid_from, p.valid_to, p.is_current
FROM dds.dim_player p JOIN dds.dim_team t USING(team_sk)
WHERE player_id IN (SELECT player_id FROM dds.dim_player GROUP BY player_id HAVING count(*) > 1)
ORDER BY player_id, valid_from LIMIT 20;

-- Снимок входных объёмов и сверка каждого матча.
SELECT * FROM dds.load_manifest;
SELECT * FROM dds.game_goal_summary WHERE source_goals <> normalized_goals;

-- Объёмы основных фактов.
SELECT 'games' AS entity, count(*) FROM dds.fact_game
UNION ALL SELECT 'goals', count(*) FROM dds.fact_goal
UNION ALL SELECT 'standings', count(*) FROM dds.fact_standings;
