// Клубы дивизиона и их очки. Та же формулировка, что и SQL-демо.
MATCH (t:Team)-[:IN_DIVISION]->(d:Division {name: 'Metropolitan'})
RETURN t.abbrev AS team, t.points AS points, t.goal_differential AS goal_differential
ORDER BY points DESC;

// Соперники клуба и счёт. Обход в один шаг, без соединения таблиц.
MATCH (team:Team {abbrev: 'TOR'})-[:PLAYED_HOME|PLAYED_AWAY]->(game:Game)<-[:PLAYED_HOME|PLAYED_AWAY]-(opponent:Team)
WHERE opponent.abbrev <> team.abbrev
RETURN opponent.abbrev AS opponent,
       game.game_date AS game_date,
       game.home_score AS home_score,
       game.away_score AS away_score,
       game.state AS state
ORDER BY game_date DESC;

// Партнёры по матчам: игроки, которые выходили на лёд в тех же играх.
MATCH (player:Player)-[:APPEARED_IN]->(game:Game)<-[:APPEARED_IN]-(teammate:Player)
WHERE player.name CONTAINS 'Matthews'
  AND teammate.player_id <> player.player_id
RETURN teammate.name AS teammate, count(game) AS games_together
ORDER BY games_together DESC
LIMIT 10;
