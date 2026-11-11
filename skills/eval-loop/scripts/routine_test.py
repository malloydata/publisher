#!/usr/bin/env python3
"""Tests for routine.py: the order of steps, and what stops it."""
import contextlib
import io
import pathlib
import sys
import unittest
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import routine  # noqa: E402


class Order(unittest.TestCase):
    def go(self, codes: dict, record=True):
        ran = []

        def step(name, *args):
            ran.append(name)
            return codes.get(name, 0)
        with mock.patch.object(routine, "step", side_effect=step), \
                mock.patch("run_index.record", return_value={"run": "r", "path": "p"} if record else None), \
                mock.patch("run_index.set_runs", return_value=[]), \
                mock.patch("config.load"), contextlib.redirect_stdout(io.StringIO()):
            code = routine.main(["--set", "s", "--label", "n"])
        return code, ran

    def test_every_step_in_order(self):
        code, ran = self.go({})
        self.assertEqual((code, ran), (0, ["check", "verify", "run", "diagnose", "package",
                                           "query-replay", "history"]))

    def test_a_failed_step_stops_everything_after_it(self):
        code, ran = self.go({"verify": 1})
        self.assertEqual((code, ran), (1, ["check", "verify"]))

    def test_a_blocked_replay_is_reported_and_history_still_builds(self):
        code, ran = self.go({"query-replay": 1})
        self.assertEqual((code, ran[-1]), (0, "history"))


if __name__ == "__main__":
    unittest.main()
