"""Чистые преобразования ответов NHL и ESPN. Без сети и без баз."""

from __future__ import annotations


def localized(value):
    """NHL отдаёт часть полей строкой, а часть — словарём {'default': ...}."""
    if isinstance(value, dict):
        return value.get("default")
    return value


def flatten_standing(row: dict) -> dict:
    abbrev = localized(row.get("teamAbbrev"))
    name = localized(row.get("teamName"))
    if not abbrev or not name:
        raise ValueError(f"standing row without team identity: {row.get('teamAbbrev')!r}")
    return {
        "standings_date": row["date"],
        "team_abbrev": abbrev,
        "team_name": name,
        "conference_abbrev": row.get("conferenceAbbrev"),
        "conference_name": row.get("conferenceName"),
        "division_abbrev": row.get("divisionAbbrev"),
        "division_name": row.get("divisionName"),
        "games_played": row.get("gamesPlayed"),
        "wins": row.get("wins"),
        "losses": row.get("losses"),
        "ot_losses": row.get("otLosses"),
        "points": row.get("points"),
        "goal_differential": row.get("goalDifferential"),
    }


def game_header(game: dict) -> dict:
    home = game.get("homeTeam") or {}
    away = game.get("awayTeam") or {}
    start = game.get("startTimeUTC") or ""
    venue = game.get("venue")
    return {
        "game_id": str(game["id"]),
        "season": game.get("season"),
        "game_date": (game.get("gameDate") or start)[:10],
        "start_time_utc": start or None,
        "state": game.get("gameState"),
        "home_abbrev": localized(home.get("abbrev")),
        "away_abbrev": localized(away.get("abbrev")),
        "home_score": home.get("score"),
        "away_score": away.get("score"),
        "venue": localized(venue) if isinstance(venue, dict) else venue,
    }


def weekly_anchor_dates(weeks: list[dict], today: str, limit: int) -> list[str]:
    """Последний уже наступивший день каждой недели расписания.

    Снимок таблицы на этот день хранит положение клубов на конец недели,
    а не только «сейчас».
    """
    anchors: list[str] = []
    seen: set[str] = set()
    for week in weeks:
        past = [
            day.get("date")
            for day in (week.get("gameWeek") or [])
            if day.get("date") and day["date"] <= today
        ]
        if not past:
            continue
        anchor = past[-1]
        if anchor in seen:
            continue
        seen.add(anchor)
        anchors.append(anchor)
        if len(anchors) >= limit:
            break
    return anchors


def select_games(
    weeks: list[dict],
    *,
    states: set[str] | None = None,
    limit: int | None = None,
    newest_first: bool = False,
) -> list[dict]:
    found: list[dict] = []
    seen: set = set()
    for week in weeks:
        for day in week.get("gameWeek") or []:
            for game in day.get("games") or []:
                if states is not None and game.get("gameState") not in states:
                    continue
                game_id = game.get("id")
                if game_id in seen:
                    continue
                seen.add(game_id)
                found.append(game)
    found.sort(
        key=lambda game: (game.get("startTimeUTC") or "", game.get("id") or 0),
        reverse=newest_first,
    )
    if limit is None:
        return found
    return found[:limit]


def flatten_roster_player(player: dict, team_abbrev: str, group: str) -> dict:
    first = localized(player.get("firstName")) or ""
    last = localized(player.get("lastName")) or ""
    return {
        "player_id": int(player["id"]),
        "name": f"{first} {last}".strip(),
        "position": player.get("positionCode"),
        "sweater": player.get("sweaterNumber"),
        "team_abbrev": team_abbrev,
        "group": group,
    }


def iter_roster(payload: dict, team_abbrev: str):
    for group in ("forwards", "defensemen", "goalies"):
        for player in payload.get(group) or []:
            if player.get("id") is None:
                continue
            yield flatten_roster_player(player, team_abbrev, group)


def flatten_appearance(player: dict, group: str, game_id: str) -> dict:
    if player.get("playerId") is None:
        raise ValueError("boxscore player without playerId")
    return {
        "game_id": str(game_id),
        "player_id": int(player["playerId"]),
        "name": localized(player.get("name")),
        "position": player.get("position"),
        "group": group,
        "goals": player.get("goals"),
        "assists": player.get("assists"),
        "points": player.get("points"),
        "sog": player.get("sog"),
        "toi": player.get("toi"),
        "saves": player.get("saves"),
        "shots_against": player.get("shotsAgainst"),
    }


def iter_appearances(boxscore: dict):
    game_id = str(boxscore["id"])
    stats = boxscore.get("playerByGameStats") or {}
    for side in ("homeTeam", "awayTeam"):
        groups = stats.get(side) or {}
        for group in ("forwards", "defense", "goalies"):
            for player in groups.get(group) or []:
                if player.get("playerId") is None:
                    continue
                yield flatten_appearance(player, group, game_id)


def count_goals(plays: list) -> int:
    return sum(1 for play in plays if play.get("typeDescKey") == "goal")


def summarize_espn_scoreboard(payload: dict) -> list[dict]:
    events = []
    for event in payload.get("events") or []:
        status = ((event.get("status") or {}).get("type") or {}).get("name")
        events.append(
            {
                "id": event.get("id"),
                "name": event.get("name"),
                "date": event.get("date"),
                "status": status,
            }
        )
    return events


def summarize_espn_news(payload: dict) -> list[dict]:
    articles = []
    for article in payload.get("articles") or []:
        articles.append(
            {
                "id": article.get("id"),
                "headline": article.get("headline"),
                "published": article.get("published"),
            }
        )
    return articles
