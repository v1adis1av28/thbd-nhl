-- Снимок таблицы на последнюю загруженную дату.
SELECT team_abbrev,
       team_name,
       division_name,
       games_played,
       wins,
       losses,
       ot_losses,
       points,
       goal_differential
FROM ods.nhl_standings
WHERE standings_date = (SELECT max(standings_date) FROM ods.nhl_standings)
ORDER BY points DESC, goal_differential DESC;

-- Metropolitan на ту же дату: вопрос, который потом гоняется по всем четырём базам.
SELECT team_abbrev, points, goal_differential
FROM ods.nhl_standings
WHERE standings_date = (SELECT max(standings_date) FROM ods.nhl_standings)
  AND division_name = 'Metropolitan'
ORDER BY points DESC;

-- Сколько байт и времени заняла каждая загрузка.
SELECT dag_id, storage, entity, records_loaded, payload_bytes, duration_ms, loaded_at
FROM ods.ingest_log
ORDER BY loaded_at DESC;
