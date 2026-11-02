"""Разворот play-by-play в факты голов. Это семя слоя DDS для ЛР2."""

from __future__ import annotations


def team_by_id(boxscore: dict | None) -> dict[int, str]:
    mapping: dict[int, str] = {}
    for side in ("homeTeam", "awayTeam"):
        team = (boxscore or {}).get(side) or {}
        abbrev = team.get("abbrev")
        if isinstance(abbrev, dict):
            abbrev = abbrev.get("default")
        if team.get("id") is None or not abbrev:
            continue
        mapping[int(team["id"])] = abbrev
    return mapping


def goals_from_plays(plays: list, game_id: str, teams: dict[int, str] | None = None) -> list[dict]:
    rows = []
    for play in plays or []:
        if play.get("typeDescKey") != "goal":
            continue
        details = play.get("details") or {}
        period = play.get("periodDescriptor") or {}
        event_id = play.get("eventId")
        if event_id is None:
            continue
        owner_id = details.get("eventOwnerTeamId")
        owner = None
        if teams and owner_id is not None:
            owner = teams.get(int(owner_id))
        rows.append(
            {
                "game_id": str(game_id),
                "event_id": int(event_id),
                "owner_team_abbrev": owner,
                "period": period.get("number"),
                "period_type": period.get("periodType"),
                "time_in_period": play.get("timeInPeriod"),
                "scorer_id": details.get("scoringPlayerId"),
                "assist1_id": details.get("assist1PlayerId"),
                "assist2_id": details.get("assist2PlayerId"),
                "x": details.get("xCoord"),
                "y": details.get("yCoord"),
                "shot_type": details.get("shotType"),
                "home_score": details.get("homeScore"),
                "away_score": details.get("awayScore"),
            }
        )
    return rows
