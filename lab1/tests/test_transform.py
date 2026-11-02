import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "plugins"))

from thbd.transform import (
    count_goals,
    flatten_standing,
    localized,
    select_games,
    summarize_espn_news,
    summarize_espn_scoreboard,
    weekly_anchor_dates,
)


class TransformTest(unittest.TestCase):
    def test_localized_reads_default_and_leaves_strings(self):
        self.assertEqual(localized({"default": "CAR"}), "CAR")
        self.assertEqual(localized("TOR"), "TOR")
        self.assertIsNone(localized(None))

    def test_flatten_standing_promotes_nested_name(self):
        row = flatten_standing(
            {
                "date": "2026-10-23",
                "teamAbbrev": {"default": "CAR"},
                "teamName": {"default": "Carolina Hurricanes"},
                "conferenceAbbrev": "E",
                "conferenceName": "Eastern",
                "divisionAbbrev": "M",
                "divisionName": "Metropolitan",
                "gamesPlayed": 12,
                "wins": 8,
                "losses": 2,
                "otLosses": 2,
                "points": 18,
                "goalDifferential": 14,
            }
        )
        self.assertEqual(row["team_abbrev"], "CAR")
        self.assertEqual(row["points"], 18)
        self.assertEqual(row["division_name"], "Metropolitan")

    def test_select_games_keeps_latest_finals_only(self):
        weeks = [
            {
                "gameWeek": [
                    {
                        "games": [
                            {"id": 1, "gameState": "OFF", "startTimeUTC": "2026-10-15T23:00:00Z"},
                            {"id": 2, "gameState": "FUT", "startTimeUTC": "2026-10-24T00:00:00Z"},
                            {"id": 1, "gameState": "OFF", "startTimeUTC": "2026-10-15T23:00:00Z"},
                        ]
                    }
                ]
            },
            {
                "gameWeek": [
                    {"games": [{"id": 3, "gameState": "OFF", "startTimeUTC": "2026-10-20T23:00:00Z"}]}
                ]
            },
        ]
        selected = select_games(weeks, states={"OFF"}, limit=2, newest_first=True)
        self.assertEqual([game["id"] for game in selected], [3, 1])

    def test_weekly_anchor_picks_last_past_day(self):
        weeks = [
            {"gameWeek": [{"date": "2026-10-20"}, {"date": "2026-10-26"}]},
            {"gameWeek": [{"date": "2026-10-13"}, {"date": "2026-10-19"}]},
        ]
        self.assertEqual(weekly_anchor_dates(weeks, "2026-10-24", 10), ["2026-10-20", "2026-10-19"])

    def test_count_goals_and_espn_summaries(self):
        self.assertEqual(count_goals([{"typeDescKey": "goal"}, {"typeDescKey": "hit"}]), 1)
        events = summarize_espn_scoreboard(
            {"events": [{"id": "1", "name": "A at B", "date": "2026-10-24T00:00Z", "status": {"type": {"name": "STATUS_SCHEDULED"}}}]}
        )
        self.assertEqual(events[0]["status"], "STATUS_SCHEDULED")
        articles = summarize_espn_news({"articles": [{"id": 9, "headline": "Deal", "published": "2026-10-23"}]})
        self.assertEqual(articles[0]["headline"], "Deal")


if __name__ == "__main__":
    unittest.main()
