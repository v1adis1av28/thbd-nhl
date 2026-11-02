"""ESPN site API, лига NHL. Тоже без ключа. Это второй независимый источник."""

from __future__ import annotations

from thbd.client import get_json

BASE = "https://site.api.espn.com"
SCOREBOARD_URL = f"{BASE}/apis/site/v2/sports/hockey/nhl/scoreboard"
NEWS_URL = f"{BASE}/apis/site/v2/sports/hockey/nhl/news"


def fetch_scoreboard() -> tuple[dict, str]:
    return get_json(SCOREBOARD_URL)


def fetch_news() -> tuple[dict, str]:
    return get_json(NEWS_URL)
