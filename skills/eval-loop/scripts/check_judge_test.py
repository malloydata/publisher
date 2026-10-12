#!/usr/bin/env python3
"""The exit code of the judge check, where a green 0/0 used to hide.

`check_judge.py` skips a fixture whose `goldenRevision` pin is stale, and
`verify_goldens.py --refresh` bumps every drifted case's revision at once. So
one refresh could skip every fixture, and the gate printed `0/0 fixtures
reproduce` and exited 0 having never called the judge. Reproduced by review;
the `--repeat < 1` guard exists to stop the same failure by another road."""
import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import check_judge as cj  # noqa: E402


class GateExit(unittest.TestCase):
    def test_every_fixture_skipped_is_a_failure_not_a_pass(self):
        self.assertEqual(cj.gate_exit(12, rows=[], fails=[], unresolved=[]), 1)

    def test_no_fixtures_at_all_is_not_this_failure(self):
        # The no-fixture-file case returns 0 earlier with its own message; an
        # empty fixture list here must not be reported as a skipped gate.
        self.assertEqual(cj.gate_exit(0, rows=[], fails=[], unresolved=[]), 0)

    def test_judged_and_agreeing_passes(self):
        self.assertEqual(cj.gate_exit(2, rows=[{}, {}], fails=[], unresolved=[]), 0)

    def test_a_regression_fails(self):
        self.assertEqual(cj.gate_exit(2, rows=[{}, {}], fails=[{}], unresolved=[]), 1)

    def test_an_unresolved_fixture_fails(self):
        self.assertEqual(cj.gate_exit(2, rows=[{}], fails=[], unresolved=[({}, "gone")]), 1)



class FixtureAttempt(unittest.TestCase):
    """A fixture's givens reach the judge with its query."""

    def test_the_final_givens_are_shown_with_the_final_query(self):
        att = cj.fixture_attempt({
            "answer_text": "West is 5.",
            "final_query": "run: s -> { aggregate: n }",
            "queries": ["run: s -> { aggregate: n }"],
            "final_givens": {"region": "West"}})
        self.assertEqual(att["final_givens"], {"region": "West"})
        self.assertEqual(
            cj.rb.queries_for_judge(att),
            '[1] run: s -> { aggregate: n }\n    givens: {"region": "West"}')

if __name__ == "__main__":
    unittest.main()
