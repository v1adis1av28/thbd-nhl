"""Сборка DDS из ODS. Схему ods эта загрузка не меняет."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

from psycopg2.extras import execute_values

from processing.goals import goals_from_plays, team_by_id
from processing.quality import anomaly_flags, compare_counts
from processing.scd import build_scd2, sk_on_date

DDL_PATH = Path(__file__).resolve().parents[1] / "sql" / "001_dds.sql"


def _as_date(value) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)[:10]


def _date_sk(value: str) -> int:
    return int(value.replace("-", ""))


def _int_or_none(value):
    if value is None:
        return None
    return int(value)


def _localized(value):
    if isinstance(value, dict):
        return value.get("default")
    return value


def apply_ddl(cursor) -> None:
    lines = []
    for line in DDL_PATH.read_text(encoding="utf-8").splitlines():
        if line.strip().startswith("--"):
            continue
        lines.append(line)
    for statement in "\n".join(lines).split(";"):
        if statement.strip():
            cursor.execute(statement)


def _fetch_standings(cursor) -> list[dict]:
    cursor.execute(
        """
        SELECT standings_date::text, team_abbrev, team_name, conference_abbrev,
               conference_name, division_abbrev, division_name, games_played,
               wins, losses, ot_losses, points, goal_differential
        FROM ods.nhl_standings
        ORDER BY standings_date, team_abbrev
        """
    )
    columns = [item[0] for item in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _fetch_games(database) -> list[dict]:
    cursor = database["games"].find(
        {},
        {
            "_id": 0,
            "game_id": 1,
            "game_date": 1,
            "season": 1,
            "venue": 1,
            "home": 1,
            "away": 1,
            "goal_count": 1,
            "play_count": 1,
            "plays": 1,
            "raw_boxscore.homeTeam": 1,
            "raw_boxscore.awayTeam": 1,
            "raw_boxscore.playerByGameStats": 1,
        },
    )
    return list(cursor.sort("game_id", 1))


def appearances_from_game(game: dict) -> list[dict]:
    box = game.get("raw_boxscore") or {}
    rows = []
    for side in ("homeTeam", "awayTeam"):
        team = box.get(side) or {}
        abbrev = _localized(team.get("abbrev"))
        groups = (box.get("playerByGameStats") or {}).get(side) or {}
        for group in ("forwards", "defense", "goalies"):
            for player in groups.get(group) or []:
                if player.get("playerId") is None or not abbrev:
                    continue
                rows.append(
                    {
                        "player_id": int(player["playerId"]),
                        "player_name": _localized(player.get("name")),
                        "team_abbrev": abbrev,
                        "game_date": _as_date(game.get("game_date")),
                    }
                )
    return rows


def load_dimensions(cursor, games: list[dict], standings: list[dict] | None = None) -> dict[str, int]:
    standings = _fetch_standings(cursor) if standings is None else standings
    if not standings:
        raise RuntimeError("ods.nhl_standings пуст. Сначала нужен DAG таблицы из ЛР1.")

    conferences = {}
    divisions = {}
    for row in standings:
        conferences[row["conference_abbrev"]] = row["conference_name"]
        divisions[row["division_abbrev"]] = (
            row["division_name"],
            row["conference_abbrev"],
        )
    execute_values(
        cursor,
        "INSERT INTO dds.dim_conference (conference_abbrev, conference_name) VALUES %s",
        list(conferences.items()),
    )
    cursor.execute("SELECT conference_abbrev, conference_sk FROM dds.dim_conference")
    conference_sk = dict(cursor.fetchall())
    execute_values(
        cursor,
        """
        INSERT INTO dds.dim_division (division_abbrev, division_name, conference_sk)
        VALUES %s
        """,
        [
            (abbrev, name, conference_sk[conference])
            for abbrev, (name, conference) in divisions.items()
        ],
    )
    cursor.execute("SELECT division_abbrev, division_sk FROM dds.dim_division")
    division_sk = dict(cursor.fetchall())

    team_versions = build_scd2(
        standings,
        "team_abbrev",
        ["team_name", "division_abbrev", "conference_abbrev"],
        "standings_date",
    )
    earliest_game: dict[str, str] = {}
    for game in games:
        game_day = _as_date(game.get("game_date")) if game.get("game_date") else None
        if not game_day:
            continue
        for side in ("home", "away"):
            abbrev = _localized((game.get(side) or {}).get("abbrev"))
            if not abbrev:
                continue
            current = earliest_game.get(abbrev)
            if current is None or game_day < current:
                earliest_game[abbrev] = game_day
    first_version: dict[str, dict] = {}
    for version in team_versions:
        abbrev = version["team_abbrev"]
        previous = first_version.get(abbrev)
        if previous is None or version["valid_from"] < previous["valid_from"]:
            first_version[abbrev] = version
    for abbrev, version in first_version.items():
        earlier = earliest_game.get(abbrev)
        if earlier and earlier < version["valid_from"]:
            version["valid_from"] = earlier
    known = {version["team_abbrev"] for version in team_versions}
    for game in games:
        for side in ("home", "away"):
            abbrev = _localized((game.get(side) or {}).get("abbrev"))
            if not abbrev or abbrev in known:
                continue
            known.add(abbrev)
            team_versions.append(
                {
                    "team_abbrev": abbrev,
                    "team_name": abbrev,
                    "division_abbrev": None,
                    "valid_from": earliest_game[abbrev],
                    "valid_to": None,
                    "is_current": True,
                }
            )
    execute_values(
        cursor,
        """
        INSERT INTO dds.dim_team (
            team_abbrev, team_name, division_sk, valid_from, valid_to, is_current
        ) VALUES %s
        """,
        [
            (
                version["team_abbrev"],
                version.get("team_name") or version["team_abbrev"],
                division_sk.get(version.get("division_abbrev")),
                version["valid_from"],
                version["valid_to"],
                version["is_current"],
            )
            for version in team_versions
        ],
    )
    cursor.execute(
        """
        SELECT team_sk, team_abbrev, valid_from::text, valid_to::text
        FROM dds.dim_team
        """
    )
    teams_by_abbrev: dict[str, list[dict]] = {}
    for team_sk, abbrev, valid_from, valid_to in cursor.fetchall():
        teams_by_abbrev.setdefault(abbrev, []).append(
            {"team_sk": team_sk, "valid_from": valid_from, "valid_to": valid_to}
        )

    dates = {row["standings_date"] for row in standings}
    dates.update(_as_date(game.get("game_date")) for game in games if game.get("game_date"))
    execute_values(
        cursor,
        """
        INSERT INTO dds.dim_date (
            date_sk, full_date, calendar_year, calendar_month, calendar_day, week_day
        ) VALUES %s
        """,
        [
            (
                _date_sk(day),
                day,
                int(day[:4]),
                int(day[5:7]),
                int(day[8:10]),
                date.fromisoformat(day).isoweekday(),
            )
            for day in sorted(dates)
        ],
    )

    appearances = []
    seen_appearance = set()
    for game in games:
        for row in appearances_from_game(game):
            key = (row["player_id"], row["game_date"])
            if key in seen_appearance:
                continue
            seen_appearance.add(key)
            row["team_sk"] = sk_on_date(teams_by_abbrev.get(row["team_abbrev"], []), row["game_date"], "team_sk")
            appearances.append(row)
    player_versions = build_scd2(appearances, "player_id", ["team_sk"], "game_date")
    execute_values(
        cursor,
        """
        INSERT INTO dds.dim_player (
            player_id, player_name, team_sk, valid_from, valid_to, is_current
        ) VALUES %s
        """,
        [
            (
                version["player_id"],
                version.get("player_name"),
                version["team_sk"],
                version["valid_from"],
                version["valid_to"],
                version["is_current"],
            )
            for version in player_versions
        ],
    )
    return {
        "conferences": len(conferences),
        "divisions": len(divisions),
        "team_versions": len(team_versions),
        "player_versions": len(player_versions),
        "dates": len(dates),
        "standings_rows": len(standings),
        "games": len(games),
    }


def load_facts(cursor, games: list[dict], standings: list[dict] | None = None) -> dict[str, int]:
    cursor.execute("SELECT count(*) FROM dds.dim_team")
    if cursor.fetchone()[0] == 0:
        raise RuntimeError("dds.dim_team пуст. Сначала нужен DAG измерений.")

    cursor.execute(
        """
        SELECT team_sk, team_abbrev, valid_from::text, valid_to::text
        FROM dds.dim_team
        """
    )
    teams_by_abbrev: dict[str, list[dict]] = {}
    for team_sk, abbrev, valid_from, valid_to in cursor.fetchall():
        teams_by_abbrev.setdefault(abbrev, []).append(
            {"team_sk": team_sk, "valid_from": valid_from, "valid_to": valid_to}
        )
    cursor.execute(
        """
        SELECT player_sk, player_id, valid_from::text, valid_to::text
        FROM dds.dim_player
        """
    )
    players_by_id: dict[int, list[dict]] = {}
    for player_sk, player_id, valid_from, valid_to in cursor.fetchall():
        players_by_id.setdefault(int(player_id), []).append(
            {"player_sk": player_sk, "valid_from": valid_from, "valid_to": valid_to}
        )

    game_rows = []
    goal_rows = []
    seen_goals = set()
    for game in games:
        game_date = _as_date(game.get("game_date"))
        home = _localized((game.get("home") or {}).get("abbrev"))
        away = _localized((game.get("away") or {}).get("abbrev"))
        game_rows.append(
            (
                str(game["game_id"]),
                _date_sk(game_date),
                sk_on_date(teams_by_abbrev.get(home, []), game_date, "team_sk"),
                sk_on_date(teams_by_abbrev.get(away, []), game_date, "team_sk"),
                game.get("season"),
                _localized(game.get("venue")),
                (game.get("home") or {}).get("score"),
                (game.get("away") or {}).get("score"),
                game.get("goal_count"),
                game.get("play_count"),
            )
        )
        teams = team_by_id(game.get("raw_boxscore"))
        for goal in goals_from_plays(game.get("plays") or [], str(game["game_id"]), teams):
            key = (goal["game_id"], goal["event_id"])
            if key in seen_goals:
                continue
            seen_goals.add(key)
            scorer = goal.get("scorer_id")
            goal_rows.append(
                (
                    goal["game_id"],
                    goal["event_id"],
                    sk_on_date(players_by_id.get(int(scorer), []), game_date, "player_sk") if scorer else None,
                    sk_on_date(teams_by_abbrev.get(goal.get("owner_team_abbrev"), []), game_date, "team_sk"),
                    sk_on_date(players_by_id.get(int(goal["assist1_id"]), []), game_date, "player_sk") if goal.get("assist1_id") else None,
                    sk_on_date(players_by_id.get(int(goal["assist2_id"]), []), game_date, "player_sk") if goal.get("assist2_id") else None,
                    goal.get("period"),
                    goal.get("period_type"),
                    goal.get("time_in_period"),
                    _int_or_none(goal.get("x")),
                    _int_or_none(goal.get("y")),
                    goal.get("shot_type"),
                    goal.get("home_score"),
                    goal.get("away_score"),
                )
            )

    execute_values(
        cursor,
        """
        INSERT INTO dds.fact_game (
            game_id, date_sk, home_team_sk, away_team_sk, season, venue,
            home_score, away_score, goal_count, play_count
        ) VALUES %s
        """,
        game_rows,
        page_size=500,
    )
    standings = _fetch_standings(cursor) if standings is None else standings
    standing_rows = []
    for row in standings:
        team_sk = sk_on_date(teams_by_abbrev.get(row["team_abbrev"], []), row["standings_date"], "team_sk")
        if team_sk is None:
            continue
        standing_rows.append(
            (
                row["standings_date"],
                team_sk,
                row["games_played"],
                row["wins"],
                row["losses"],
                row["ot_losses"],
                row["points"],
                row["goal_differential"],
            )
        )
    execute_values(
        cursor,
        """
        INSERT INTO dds.fact_standings (
            standings_date, team_sk, games_played, wins, losses, ot_losses,
            points, goal_differential
        ) VALUES %s
        """,
        standing_rows,
        page_size=500,
    )
    execute_values(
        cursor,
        """
        INSERT INTO dds.fact_goal (
            game_id, event_id, scorer_player_sk, team_sk,
            assist1_player_sk, assist2_player_sk, period, period_type, time_in_period,
            x, y, shot_type, home_score, away_score
        ) VALUES %s
        """,
        goal_rows,
        page_size=500,
    )
    return {
        "games": len(game_rows),
        "standings": len(standing_rows),
        "goals": len(goal_rows),
    }


def collect_quality(cursor, mongo_games: int, source_goals: int, source_standings: int) -> list[dict]:
    cursor.execute("SELECT count(*) FROM dds.fact_standings")
    fact_standings = cursor.fetchone()[0]
    cursor.execute("SELECT count(*) FROM dds.fact_game")
    fact_games = cursor.fetchone()[0]
    cursor.execute("SELECT count(*) FROM dds.fact_goal")
    fact_goals = cursor.fetchone()[0]
    cursor.execute(
        """
        SELECT count(*) FROM (
            SELECT team_abbrev
            FROM dds.dim_team
            WHERE is_current
            GROUP BY team_abbrev
            HAVING count(*) > 1
        ) extra
        """
    )
    duplicate_current = cursor.fetchone()[0]
    checks = [
        compare_counts("standings_volume", source_standings, fact_standings),
        compare_counts("games_volume", mongo_games, fact_games),
        compare_counts("goals_volume", source_goals, fact_goals),
        {
            "check_name": "one_current_team_version",
            "severity": "error",
            "passed": duplicate_current == 0,
            "expected": "0",
            "actual": str(duplicate_current),
            "details": "у клуба больше одной текущей SCD-версии",
        },
    ]
    cursor.execute(
        """
        SELECT x, y FROM dds.fact_goal
        """
    )
    goals = [{"x": row[0], "y": row[1]} for row in cursor.fetchall()]
    cursor.execute("SELECT points, wins, games_played FROM dds.fact_standings")
    standings = [
        {"points": row[0], "wins": row[1], "games_played": row[2]} for row in cursor.fetchall()
    ]
    checks.extend(anomaly_flags(goals, standings))
    validations = {
        "per_game_goals_reconciled": "SELECT count(*) FROM dds.game_goal_summary WHERE source_goals <> normalized_goals",
        "game_teams_resolved": "SELECT count(*) FROM dds.fact_game WHERE home_team_sk IS NULL OR away_team_sk IS NULL",
        "player_team_resolved": "SELECT count(*) FROM dds.dim_player WHERE team_sk IS NULL",
        "goal_scorer_resolved": "SELECT count(*) FROM dds.fact_goal WHERE scorer_player_sk IS NULL",
        "game_scores_nonnegative": "SELECT count(*) FROM dds.fact_game WHERE home_score < 0 OR away_score < 0",
        "player_scd_intervals": "SELECT count(*) FROM dds.dim_player a JOIN dds.dim_player b ON a.player_id=b.player_id AND a.player_sk<b.player_sk WHERE a.valid_from < coalesce(b.valid_to,'infinity'::date) AND b.valid_from < coalesce(a.valid_to,'infinity'::date)",
        "team_scd_intervals": "SELECT count(*) FROM dds.dim_team a JOIN dds.dim_team b ON a.team_abbrev=b.team_abbrev AND a.team_sk<b.team_sk WHERE a.valid_from < coalesce(b.valid_to,'infinity'::date) AND b.valid_from < coalesce(a.valid_to,'infinity'::date)",
    }
    for name, query in validations.items():
        cursor.execute(query)
        checks.append(compare_counts(name, 0, cursor.fetchone()[0]))

    execute_values(
        cursor,
        """
        INSERT INTO dds.dq_result (check_name, severity, passed, expected, actual, details)
        VALUES %s
        """,
        [
            (
                check["check_name"],
                check["severity"],
                check["passed"],
                check["expected"],
                check["actual"],
                check["details"],
            )
            for check in checks
        ],
    )
    return checks


def rebuild() -> dict:
    """Полный детерминированный пересчёт из одного входного снимка.

    SCD-история восстанавливается из всей доступной истории ODS.
    DDL и загрузка атомарны, при ошибке PostgreSQL откатывает весь DDS.
    """
    from thbd.settings import load_settings
    from thbd.stores import mongo_db, pg_cursor
    from uuid import uuid4
    settings = load_settings()
    client, database = mongo_db(settings)
    try:
        games = _fetch_games(database)
    finally:
        client.close()
    if not games:
        raise RuntimeError("ODS MongoDB пуст")
    with pg_cursor(settings) as cursor:
        cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        cursor.execute("SELECT pg_advisory_xact_lock(23002)")
        standings = _fetch_standings(cursor)
        apply_ddl(cursor)
        dimensions = load_dimensions(cursor, games, standings)
        facts = load_facts(cursor, games, standings)
        source_goals = len({(str(g['game_id']), int(p['eventId']))
                            for g in games for p in (g.get('plays') or [])
                            if p.get('typeDescKey') == 'goal' and p.get('eventId') is not None})
        snapshot_id = str(uuid4())
        cursor.execute("INSERT INTO dds.load_manifest(snapshot_id,games,goals,standings) VALUES (%s,%s,%s,%s)",
                       (snapshot_id, len(games), source_goals, len(standings)))
    return {'snapshot_id': snapshot_id, 'dimensions': dimensions, 'facts': facts}
