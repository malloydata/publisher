#!/usr/bin/env python3
"""Tests for attempts.py: the file of what was tried, and what a reader is shown."""
import pathlib
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent.parent
                       / "eval-answer" / "scripts"))
import attempts  # noqa: E402
import ledger  # noqa: E402

DIFF = """--- a/m.malloy
+++ b/m.malloy
@@ -10,3 +10,4 @@ source: orders
   measure: n is count()
-  measure: net is sum(amount)
+  #(doc) Net sales. Excludes returned lines.
+  measure: net is sum(amount) { where: status != 'Returned' }
"""


def attempt(outcome, targets, what="edit", diff=None, lesson=None):
    return attempts.row(what=what, outcome=outcome, baseline="b", candidates=["a1", "a2"],
                        targets=targets, failed={} if outcome == "accepted" else
                        {"no regression": "keep in a1"},
                        effect={"targets": len(targets), "targetsFixed": 0, "regressions": []},
                        diff=diff, lesson=lesson)


class File(unittest.TestCase):
    def setUp(self):
        self.set = pathlib.Path(tempfile.mkdtemp())
        (self.set / "set.json").write_text("{}")
        (self.set / "cases.jsonl").write_text('{"qid": "a"}\n')

    def tearDown(self):
        shutil.rmtree(self.set, ignore_errors=True)

    def test_lines_append_and_read_back_oldest_first(self):
        attempts.append(self.set, attempt("rejected", ["a"], "first"))
        attempts.append(self.set, attempt("accepted", ["a"], "second"))
        self.assertEqual([r["what"] for r in attempts.read(self.set)], ["first", "second"])

    def test_a_missing_file_is_no_attempts_and_a_bad_line_is_skipped(self):
        self.assertEqual(attempts.read(self.set), [])
        attempts.append(self.set, attempt("accepted", ["a"]))
        with attempts.path(self.set).open("a") as f:
            f.write("<<<<<<< HEAD\n")
        attempts.append(self.set, attempt("rejected", ["a"]))
        self.assertEqual(len(attempts.read(self.set)), 2)

    def test_a_new_line_does_not_move_the_dataset_sha(self):
        before = ledger.dataset_sha(self.set)
        attempts.append(self.set, attempt("rejected", ["a"]))
        self.assertEqual(ledger.dataset_sha(self.set), before)


class SameEdit(unittest.TestCase):
    def test_the_hash_is_of_what_changed_not_where(self):
        moved = DIFF.replace("@@ -10,3 +10,4 @@", "@@ -40,3 +40,4 @@").replace(
            "   measure: n is count()", "   measure: other is count()")
        self.assertEqual(attempts.patch_sha(DIFF), attempts.patch_sha(moved))
        self.assertNotEqual(attempts.patch_sha(DIFF),
                            attempts.patch_sha(DIFF.replace("Excludes", "Includes")))

    def test_no_diff_has_no_hash(self):
        for d in (None, "", "   \n", "--- a/x\n+++ b/x\n"):
            self.assertIsNone(attempts.patch_sha(d))

    def test_only_a_rejected_attempt_counts_as_tried_and_failed(self):
        rows = [attempt("accepted", ["a"], diff=DIFF)]
        self.assertIsNone(attempts.same_edit(rows, DIFF))
        rows.append(attempt("rejected", ["a"], "again", diff=DIFF))
        self.assertEqual(attempts.same_edit(rows, DIFF)["what"], "again")
        self.assertIsNone(attempts.same_edit(rows, DIFF.replace("Excludes", "Includes")))


class WhatAReaderIsShown(unittest.TestCase):
    def test_attempts_at_these_cases_come_first_and_the_list_is_short(self):
        rows = [attempt("rejected", ["z"], f"other {i}") for i in range(10)]
        rows.insert(3, attempt("rejected", ["a", "b"], "mine"))
        got = attempts.relevant(rows, ["a"], limit=4)
        self.assertEqual((got[0]["what"], len(got)), ("mine", 4))
        self.assertEqual(got[1]["what"], "other 9")              # then the newest

    def test_the_brief_leads_with_the_outcome_and_why(self):
        text = attempts.brief([attempt("rejected", ["a"], "doc net", DIFF, "net excludes returns")])
        self.assertIn("REJECTED", text.splitlines()[0])
        self.assertIn("rejected because: no regression: keep in a1", text)
        self.assertIn("lesson: net excludes returns", text)
        self.assertIn("+  #(doc) Net sales.", text)
        self.assertEqual(attempts.brief([]), "(none recorded for this set)")


if __name__ == "__main__":
    unittest.main()
