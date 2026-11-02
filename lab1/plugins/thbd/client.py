"""HTTP-клиент к открытым JSON API. Пауза и повтор — чтобы не долбить источник.

Файл называется client.py, а не http.py: Airflow кладёт каталог плагина в sys.path,
и модуль http затирает стандартную библиотеку.
"""

from __future__ import annotations

import time

import requests

HEADERS = {
    "User-Agent": "thbd-lab1/1.0 (university ETL)",
    "Accept": "application/json",
}


def get_json(url: str, *, pause_seconds: float = 0.25, attempts: int = 5) -> tuple[dict, str]:
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        time.sleep(pause_seconds)
        try:
            response = requests.get(url, headers=HEADERS, timeout=45)
            if response.status_code == 429:
                last_error = requests.HTTPError(f"429 for {url}")
                time.sleep(5 * attempt)
                continue
            response.raise_for_status()
            payload = response.json()
        except (requests.RequestException, ValueError) as exc:
            last_error = exc
            time.sleep(attempt)
            continue
        if not isinstance(payload, dict):
            raise RuntimeError(f"{url} вернул {type(payload).__name__}, ожидался объект JSON")
        return payload, response.url
    raise RuntimeError(f"GET {url} не удался за {attempts} попытки") from last_error
