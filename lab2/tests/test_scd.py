import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from processing.quality import anomaly_flags, compare_counts
from processing.scd import build_scd2, sk_on_date


class ScdTest(unittest.TestCase):
    def test_same_attributes_stay_one_version(self):
        versions = build_scd2(
            [
                {"team_abbrev": "TOR", "division": "A", "name": "Toronto", "standings_date": "2026-01-01"},
                {"team_abbrev": "TOR", "division": "A", "name": "Toronto", "standings_date": "2026-02-01"},
                {"team_abbrev": "TOR", "division": "M", "name": "Toronto", "standings_date": "2026-03-01"},
            ],
            "team_abbrev",
            ["division", "name"],
            "standings_date",
        )
        self.assertEqual(len(versions), 2)
        self.assertEqual(versions[0]["valid_to"], "2026-03-01")
        self.assertFalse(versions[0]["is_current"])
        self.assertTrue(versions[1]["is_current"])
        self.assertIsNone(versions[1]["valid_to"])

    def test_sk_uses_half_open_interval(self):
        versions = [
            {"team_sk": 1, "valid_from": "2026-01-01", "valid_to": "2026-03-01"},
            {"team_sk": 2, "valid_from": "2026-03-01", "valid_to": None},
        ]
        self.assertEqual(sk_on_date(versions, "2026-02-01", "team_sk"), 1)
        self.assertEqual(sk_on_date(versions, "2026-03-01", "team_sk"), 2)


class QualityTest(unittest.TestCase):
    def test_volume_mismatch_fails_and_off_rink_is_error(self):
        volume = compare_counts("games_volume", 10, 9)
        self.assertFalse(volume["passed"])
        flags = anomaly_flags([{"x": 140, "y": 0}, {"x": None, "y": None}], [{"points": 10, "wins": 3, "games_played": 4}])
        by_name = {item["check_name"]: item for item in flags}
        self.assertFalse(by_name["goal_coordinates_on_rink"]["passed"])
        self.assertEqual(by_name["goal_coordinates_present"]["severity"], "warning")
        self.assertTrue(by_name["standings_points_non_negative"]["passed"])


if __name__ == "__main__":
    unittest.main()
