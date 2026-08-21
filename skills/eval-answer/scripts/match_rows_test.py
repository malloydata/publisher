#!/usr/bin/env python3
import os
import tempfile
import unittest

from match_rows import grade, graded_match


def _csv(content: str) -> str:
    f = tempfile.NamedTemporaryFile(
        "w", suffix=".csv", delete=False, encoding="utf-8"
    )
    f.write(content)
    f.close()
    return f.name


class MatchRowsTests(unittest.TestCase):
    def test_exact_match(self):
        gold = _csv("category,revenue\nshoes,100.5\nhats,20\n")
        pred = _csv("cat,rev\nhats,20\nshoes,100.5\n")
        out = grade(gold, pred)
        self.assertEqual(out["f1"], 1.0)
        self.assertTrue(out["ex_strict"])
        self.assertIsNone(out["failure_bucket"])

    def test_wrong_value_scores_low_not_error(self):
        gold = _csv("category,revenue\nshoes,100.5\n")
        pred = _csv("category,revenue\nshoes,999\n")
        out = grade(gold, pred)
        self.assertLess(out["f1"], 1.0)
        self.assertFalse(out["ex_strict"])

    def test_missing_gold_file_is_a_harness_error(self):
        # A path typo must raise (exit 1 in main), never score the model 0.
        pred = _csv("a\n1\n")
        with self.assertRaises(RuntimeError):
            graded_match("/nonexistent/gold.csv", pred)

    def test_missing_pred_file_is_a_harness_error(self):
        gold = _csv("a\n1\n")
        with self.assertRaises(RuntimeError):
            graded_match(gold, "/nonexistent/pred.csv")

    def test_empty_vs_empty_is_a_strict_match(self):
        # Header-only on both sides: the prediction reproduced the (empty)
        # golden exactly. strict must equal (f1 == 1) in both directions.
        gold = _csv("a\n")
        pred = _csv("a\n")
        out = graded_match(gold, pred)
        self.assertEqual(out["f1"], 1.0)
        self.assertTrue(out["strict"])

    def test_empty_gold_nonempty_pred_is_not_strict(self):
        gold = _csv("a\n")
        pred = _csv("a\n1\n")
        out = graded_match(gold, pred)
        self.assertFalse(out["strict"])
        self.assertLess(out["f1"], 1.0)

    def test_execution_error_content_scores_zero_exit_ok(self):
        gold = _csv("a\n1\n")
        pred = _csv("Error: relation does not exist\n")
        out = grade(gold, pred)
        self.assertEqual(out["f1"], 0.0)
        self.assertIn("no_result", out["failure_bucket"])

    def test_numeric_tolerance(self):
        gold = _csv("v\n169.14900\n")
        pred = _csv("v\n169.148949\n")
        out = graded_match(gold, pred)
        self.assertEqual(out["f1"], 1.0)

    def test_known_limit_sorted_numeric_cross_match(self):
        # Documented limitation, pinned so a silent behavior change is
        # noticed: numeric cells sort per row, so (2020, 5) matches (5, 2020).
        # The judge, not this script, owns column identity.
        gold = _csv("year,n\n2020,5\n")
        pred = _csv("year,n\n5,2020\n")
        out = graded_match(gold, pred)
        self.assertEqual(out["matched"], 1)


if __name__ == "__main__":
    os.chdir(os.path.dirname(__file__))
    unittest.main()
