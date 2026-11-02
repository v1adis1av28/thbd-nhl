"""Загрузка NHL в Neo4j. Идентификаторы матчей всегда строки, иначе MERGE двоит узлы."""

from __future__ import annotations

from neo4j import GraphDatabase

from thbd.settings import Settings

CONSTRAINTS = (
    "CREATE CONSTRAINT team_abbrev IF NOT EXISTS FOR (t:Team) REQUIRE t.abbrev IS UNIQUE",
    "CREATE CONSTRAINT division_abbrev IF NOT EXISTS FOR (d:Division) REQUIRE d.abbrev IS UNIQUE",
    "CREATE CONSTRAINT conference_abbrev IF NOT EXISTS FOR (c:Conference) REQUIRE c.abbrev IS UNIQUE",
    "CREATE CONSTRAINT game_id IF NOT EXISTS FOR (g:Game) REQUIRE g.game_id IS UNIQUE",
    "CREATE CONSTRAINT player_id IF NOT EXISTS FOR (p:Player) REQUIRE p.player_id IS UNIQUE",
)

MERGE_TEAMS = """
UNWIND $rows AS row
MERGE (c:Conference {abbrev: row.conference_abbrev})
SET c.name = row.conference_name
MERGE (d:Division {abbrev: row.division_abbrev})
SET d.name = row.division_name
MERGE (d)-[:IN_CONFERENCE]->(c)
MERGE (t:Team {abbrev: row.team_abbrev})
SET t.name = row.team_name,
    t.points = row.points,
    t.wins = row.wins,
    t.losses = row.losses,
    t.ot_losses = row.ot_losses,
    t.goal_differential = row.goal_differential,
    t.standings_date = row.standings_date
MERGE (t)-[:IN_DIVISION]->(d)
"""

MERGE_GAMES = """
UNWIND $rows AS row
MERGE (g:Game {game_id: row.game_id})
SET g.game_date = row.game_date,
    g.start_time_utc = row.start_time_utc,
    g.state = row.state,
    g.season = row.season,
    g.home_score = row.home_score,
    g.away_score = row.away_score,
    g.venue = row.venue
MERGE (home:Team {abbrev: row.home_abbrev})
MERGE (away:Team {abbrev: row.away_abbrev})
MERGE (home)-[:PLAYED_HOME]->(g)
MERGE (away)-[:PLAYED_AWAY]->(g)
"""

MERGE_ROSTER = """
UNWIND $rows AS row
MERGE (p:Player {player_id: row.player_id})
SET p.name = row.name,
    p.position = row.position,
    p.group = row.group,
    p.sweater = row.sweater
WITH p, row
OPTIONAL MATCH (p)-[old:PLAYS_FOR]->(:Team)
DELETE old
WITH p, row
MATCH (t:Team {abbrev: row.team_abbrev})
MERGE (p)-[:PLAYS_FOR]->(t)
"""

MERGE_APPEARANCES = """
UNWIND $rows AS row
MERGE (p:Player {player_id: row.player_id})
SET p.name = coalesce(p.name, row.name),
    p.position = coalesce(p.position, row.position)
WITH p, row
MATCH (g:Game {game_id: row.game_id})
MERGE (p)-[a:APPEARED_IN]->(g)
SET a.group = row.group,
    a.goals = row.goals,
    a.assists = row.assists,
    a.points = row.points,
    a.sog = row.sog,
    a.toi = row.toi,
    a.saves = row.saves,
    a.shots_against = row.shots_against
"""


def _session(settings: Settings):
    driver = GraphDatabase.driver(
        settings.neo4j_uri,
        auth=(settings.neo4j_user, settings.neo4j_password),
        connection_timeout=8,
    )
    return driver


def _run(session, query: str, **parameters) -> None:
    session.run(query, **parameters).consume()


def load_graph(
    settings: Settings,
    *,
    teams: list[dict],
    games: list[dict],
    players: list[dict],
    appearances: list[dict],
) -> dict[str, int]:
    driver = _session(settings)
    try:
        with driver.session() as session:
            for statement in CONSTRAINTS:
                _run(session, statement)
            if teams:
                _run(session, MERGE_TEAMS, rows=teams)
            if games:
                _run(session, MERGE_GAMES, rows=games)
            if players:
                _run(session, MERGE_ROSTER, rows=players)
            if appearances:
                _run(session, MERGE_APPEARANCES, rows=appearances)
    finally:
        driver.close()
    return {
        "teams": len(teams),
        "games": len(games),
        "players": len(players),
        "appearances": len(appearances),
    }
