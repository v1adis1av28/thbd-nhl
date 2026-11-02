"""Проверки качества DDS. Ошибка роняет DAG, предупреждение остаётся в таблице."""

from __future__ import annotations

RINK_X = 100
RINK_Y = 42


def compare_counts(name: str, left: int, right: int) -> dict:
    return {
        "check_name": name,
        "severity": "error",
        "passed": left == right,
        "expected": str(left),
        "actual": str(right),
        "details": "объём до трансформации и после",
    }


def anomaly_flags(goals: list[dict], standings: list[dict]) -> list[dict]:
    checks = []
    missing_coord = sum(1 for goal in goals if goal.get("x") is None or goal.get("y") is None)
    off_rink = sum(
        1
        for goal in goals
        if goal.get("x") is not None
        and goal.get("y") is not None
        and (abs(int(goal["x"])) > RINK_X or abs(int(goal["y"])) > RINK_Y)
    )
    negative_points = sum(1 for row in standings if (row.get("points") or 0) < 0)
    wins_over_games = sum(
        1
        for row in standings
        if row.get("wins") is not None
        and row.get("games_played") is not None
        and row["wins"] > row["games_played"]
    )
    checks.append(
        {
            "check_name": "goal_coordinates_present",
            "severity": "warning",
            "passed": missing_coord == 0,
            "expected": "0",
            "actual": str(missing_coord),
            "details": "голы без координат броска",
        }
    )
    checks.append(
        {
            "check_name": "goal_coordinates_on_rink",
            "severity": "error",
            "passed": off_rink == 0,
            "expected": "0",
            "actual": str(off_rink),
            "details": f"координаты вне диапазона x ±{RINK_X}, y ±{RINK_Y}",
        }
    )
    checks.append(
        {
            "check_name": "standings_points_non_negative",
            "severity": "error",
            "passed": negative_points == 0,
            "expected": "0",
            "actual": str(negative_points),
            "details": "отрицательные очки в таблице",
        }
    )
    checks.append(
        {
            "check_name": "wins_not_above_games_played",
            "severity": "error",
            "passed": wins_over_games == 0,
            "expected": "0",
            "actual": str(wins_over_games),
            "details": "побед больше, чем сыгранных матчей",
        }
    )
    return checks
