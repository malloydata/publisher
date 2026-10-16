#!/usr/bin/env python3
"""Tests for agreement. Run in place: python3 agreement_test.py"""
from __future__ import annotations

import json
import pathlib
import sys
import tempfile
import unittest

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import agreement as ag  # noqa: E402


def run(**cases):
    return {q: {"verdict": v, "submitted": s, "packages": p}
            for q, (v, s, p) in cases.items()}


class Agreement(unittest.TestCase):
    def test_a_case_that_passed_and_failed_is_split(self):
        rows = ag.agreement({
            "a1": run(q=("match", True, ["samples/ecommerce"])),
            "a2": run(q=("no_match", False, []))})
        r = rows[0]
        self.assertEqual((r["pass"], r["fail"]), (1, 1))
        self.assertTrue(r["split"])
        self.assertFalse(r["agrees"])
        self.assertEqual(r["no_query"], 1)

    def test_one_decided_run_does_not_claim_agreement(self):
        # One draw agrees with itself, which says nothing.
        rows = ag.agreement({"a1": run(q=("match", True, []))})
        self.assertFalse(rows[0]["agrees"])

    def test_two_fails_agree(self):
        rows = ag.agreement({"a1": run(q=("no_match", True, [])),
                             "a2": run(q=("no_match", True, []))})
        self.assertTrue(rows[0]["agrees"])

    def test_a_null_verdict_is_undecided_not_neither(self):
        # Folding it into neither would read as a hedge the judge never made.
        rows = ag.agreement({"a1": run(q=(None, True, [])),
                             "a2": run(q=("needs_human", True, []))})
        self.assertEqual((rows[0]["undecided"], rows[0]["neither"]), (1, 1))

    def test_packages_are_counted_across_runs(self):
        rows = ag.agreement({
            "a1": run(q=("match", True, ["samples/storefront"])),
            "a2": run(q=("match", True, ["samples/ecommerce"])),
            "a3": run(q=("match", True, ["samples/storefront"]))})
        self.assertEqual(rows[0]["packages"],
                         {"samples/storefront": 2, "samples/ecommerce": 1})

    def test_a_case_missing_from_some_runs_counts_only_its_own(self):
        rows = {r["qid"]: r for r in ag.agreement({
            "a1": run(q=("match", True, []), r=("match", True, [])),
            "a2": run(q=("match", True, []))})}
        self.assertEqual((rows["q"]["runs"], rows["r"]["runs"]), (2, 1))


class Report(unittest.TestCase):
    def test_it_names_split_and_multi_package_cases(self):
        rows = ag.agreement({
            "a1": run(q=("match", True, ["samples/ecommerce"])),
            "a2": run(q=("no_match", True, ["samples/storefront"]))})
        body = "\n".join(ag.report(rows, 2))
        self.assertIn("both passed and failed across runs: q", body)
        self.assertIn("more than one package: q", body)
        self.assertIn("SPLIT", body)


class ReadRun(unittest.TestCase):
    def test_it_reads_attempts_and_scores_from_the_ledger(self):
        with tempfile.TemporaryDirectory() as d:
            p = pathlib.Path(d)
            (p / "events.jsonl").write_text("\n".join(json.dumps(e) for e in [
                {"kind": "attempt", "qid": "q", "sample": None,
                 "phase": "baseline", "submitted": True,
                 "queriedPackages": ["samples/ecommerce"]},
                {"kind": "tool_call", "qid": "q", "tool": "execute_query"},
                {"kind": "score", "qid": "q", "verdict": "match"}]) + "\n")
            got = ag.read_run(p)
        self.assertEqual(got, {"q": {"verdict": "match", "submitted": True,
                                     "packages": ["samples/ecommerce"]}})


if __name__ == "__main__":
    unittest.main(verbosity=1)
