from __future__ import annotations

import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from server_coverage import CoverageError, evaluate, parse_changed_lines, parse_profile, snapshot


PROFILE = """mode: atomic
github.com/YouToco/vane/server/api/a.go:1.1,2.2 2 1
github.com/YouToco/vane/server/api/a.go:3.1,3.2 1 0
"""


class ServerCoveragePolicyTest(unittest.TestCase):
    def test_snapshot_and_changed_line_floor(self) -> None:
        profile = parse_profile(PROFILE)
        baseline = snapshot(profile)
        changed = parse_changed_lines("+++ b/server/api/a.go\n@@ -0,0 +1,3 @@\n")
        failures, covered, total = evaluate(profile, changed, baseline)
        self.assertEqual((covered, total), (2, 3))
        self.assertIn("changed-line coverage", "\n".join(failures))

    def test_regression_floor_and_missing_touched_file_fail(self) -> None:
        profile = parse_profile(PROFILE)
        baseline = snapshot(profile)
        baseline["total"] += 0.51
        baseline["files"]["server/missing.go"] = 100
        changed = {"server/missing.go": {1}}
        failures, _, _ = evaluate(profile, changed, baseline)
        self.assertIn("overall coverage", "\n".join(failures))
        self.assertIn("missing touched-file coverage", "\n".join(failures))

    def test_profile_parser_rejects_zero_or_malformed_input(self) -> None:
        with self.assertRaises(CoverageError):
            parse_profile("mode: atomic\n")
        with self.assertRaises(CoverageError):
            parse_profile("mode: atomic\nnot a record\n")

    def test_exact_eighty_percent_passes(self) -> None:
        profile = parse_profile(
            "mode: atomic\n"
            "github.com/YouToco/vane/server/task/a.go:1.1,4.2 4 1\n"
            "github.com/YouToco/vane/server/task/a.go:5.1,5.2 1 0\n"
        )
        baseline = snapshot(profile)
        changed = {"server/task/a.go": {1, 2, 3, 4, 5}}
        failures, covered, total = evaluate(profile, changed, baseline)
        self.assertEqual((covered, total), (4, 5))
        self.assertEqual(failures, [])


if __name__ == "__main__":
    unittest.main()
