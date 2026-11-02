"""Чтение NHL Web API. Эндпоинты публичные, ключ не нужен."""

from __future__ import annotations

from thbd.client import get_json
from thbd.transform import select_games

BASE = "https://api-web.nhle.com"


def fetch_standings(date: str | None = None) -> tuple[dict, str]:
    url = f"{BASE}/v1/standings/now" if not date else f"{BASE}/v1/standings/{date}"
    return get_json(url)


def fetch_schedule_weeks(max_weeks: int) -> list[tuple[dict, str]]:
    url = f"{BASE}/v1/schedule/now"
    seen: set[str] = set()
    weeks: list[tuple[dict, str]] = []
    for _ in range(max_weeks):
        if url in seen:
            break
        seen.add(url)
        payload, final_url = get_json(url)
        weeks.append((payload, final_url))
        previous = payload.get("previousStartDate")
        if not previous:
            break
        url = f"{BASE}/v1/schedule/{previous}"
    return weeks


def fetch_boxscore(game_id: str) -> tuple[dict, str]:
    return get_json(f"{BASE}/v1/gamecenter/{game_id}/boxscore")


def fetch_play_by_play(game_id: str) -> tuple[dict, str]:
    return get_json(f"{BASE}/v1/gamecenter/{game_id}/play-by-play")


def fetch_roster(team_abbrev: str) -> tuple[dict, str]:
    return get_json(f"{BASE}/v1/roster/{team_abbrev}/current")


def completed_games(max_weeks: int, max_games: int) -> tuple[list[dict], list[tuple[dict, str]]]:
    weeks = fetch_schedule_weeks(max_weeks)
    games = select_games(
        [payload for payload, _url in weeks],
        states={"OFF"},
        limit=max_games,
        newest_first=True,
    )
    return games, weeks
