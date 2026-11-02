import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from processing.goals import goals_from_plays


class GoalsTest(unittest.TestCase):
    def test_keeps_only_goals_with_coordinates(self):
        rows = goals_from_plays(
            [
                {"typeDescKey": "hit", "eventId": 1},
                {
                    "typeDescKey": "goal",
                    "eventId": 93,
                    "timeInPeriod": "02:24",
                    "periodDescriptor": {"number": 1, "periodType": "REG"},
                    "details": {
                        "xCoord": -80,
                        "yCoord": -5,
                        "shotType": "wrist",
                        "scoringPlayerId": 8479318,
                        "homeScore": 1,
                        "awayScore": 0,
                    },
                },
            ],
            "2026020108",
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["scorer_id"], 8479318)
        self.assertEqual(rows[0]["x"], -80)
        self.assertEqual(rows[0]["game_id"], "2026020108")


if __name__ == "__main__":
    unittest.main()
