#!/usr/bin/env python3
"""Tests for perf_findings.py: one issue per source, the code each state gets,
and a re-run that replaces rather than duplicates."""
import json
import pathlib
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import perf_findings as pf  # noqa: E402

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent.parent / "eval-answer" / "scripts"))
import ledger  # noqa: E402


def row(qid, source, ms, slow, served=("none",)):
    return {"qid": qid, "source": source, "median_ms": ms, "slow": slow,
            "served_from": list(served), "error": None}


class Findings(unittest.TestCase):
    def setUp(self):
        self.run = pathlib.Path(tempfile.mkdtemp())
        (self.run / "run.json").write_text(json.dumps({
            "phase": "query-replay", "budgetMs": 100, "packageVersion": {"id": "1.0.0"},
            "materialization": {"persistSources": ["orders"], "refusedSources": ["patients"],
                                "bound": False}}))
        rows = [row("a", "orders", 900, True), row("b", "patients", 300, True),
                row("c", "items", 500, True), row("d", "items", 150, True),
                row("e", "items", 20, False), row("f", "events", 30, False, ["live_fallback"])]
        (self.run / "results.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows))

    def tearDown(self):
        shutil.rmtree(self.run, ignore_errors=True)

    def test_each_source_gets_one_issue_with_the_code_its_state_calls_for(self):
        got = {i["issue_id"]: (i["primary_code"], i["qids"]) for i in pf.findings(self.run)}
        self.assertEqual(got, {
            "perf-orders": ("PERSIST-NOT-BUILT", ["a"]),
            "perf-patients": ("PERSIST-REFUSED", ["b"]),
            "perf-items": ("SLOW-QUERY", ["c", "d"]),
            "perf-fallback-events": ("LIVE-FALLBACK", ["f"]),
        })
        issue = next(i for i in pf.findings(self.run) if i["issue_id"] == "perf-items")
        self.assertEqual((issue["component"], issue["owner"]), ("performance", "model"))
        self.assertEqual(issue["evidence"]["medianMs"], {"c": 500, "d": 150})

    def test_an_unbuilt_persist_is_reported_when_nothing_is_slow(self):
        (self.run / "results.jsonl").write_text(json.dumps(row("a", "orders", 40, False)) + "\n")
        got = [(i["primary_code"], i["qids"]) for i in pf.findings(self.run)]
        self.assertEqual(got, [("PERSIST-NOT-BUILT", ["a"])])

    def test_the_codes_are_the_skills_codes(self):
        skill = (pathlib.Path(pf.__file__).resolve().parent.parent / "SKILL.md").read_text()
        for code in ("PERSIST-REFUSED", "PERSIST-NOT-BUILT", "SLOW-QUERY", "LIVE-FALLBACK"):
            self.assertIn(f"| `{code}` |", skill)

    def test_a_rerun_replaces_its_own_issues(self):
        pf.write(self.run)
        pf.write(self.run)
        issues = [e for e in ledger.read_jsonl(self.run / "events.jsonl") if e["kind"] == "issue"]
        self.assertEqual(len(issues), 4)


class HotSource(unittest.TestCase):
    """With no budget set nothing is slow, and the costliest source is still named."""

    def setUp(self):
        self.run = pathlib.Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.run, ignore_errors=True)

    def findings(self, rows, persisted=()):
        (self.run / "run.json").write_text(json.dumps({
            "phase": "query-replay", "budgetMs": None,
            "materialization": {"persistSources": list(persisted), "bound": True}}))
        (self.run / "results.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows))
        return {i["issue_id"]: i for i in pf.findings(self.run)}

    def test_the_source_most_of_the_time_goes_to_is_named_by_how_often_its_queries_ran(self):
        # `items` is slower per query; `orders` ran forty times.
        rows = [{**row("a", "orders", 10, False), "count": 40}, row("b", "items", 90, False)]
        got = self.findings(rows)
        self.assertEqual(list(got), ["perf-hot-orders"])
        i = got["perf-hot-orders"]
        self.assertEqual((i["primary_code"], i["qids"], i["evidence"]["weightedMs"]),
                         ("HOT-SOURCE", ["a"], 400.0))
        self.assertIn("82%", i["diagnosis"])

    def test_a_source_that_is_already_persisted_is_not(self):
        rows = [{**row("a", "orders", 10, False), "count": 40}, row("b", "items", 90, False)]
        self.assertEqual(self.findings(rows, persisted=["orders"]), {})

    def test_time_spread_thin_across_sources_names_none(self):
        rows = [row(str(n), f"s{n}", 10, False) for n in range(5)]      # 20% each
        self.assertEqual(self.findings(rows), {})

    def test_a_query_whose_source_cannot_be_read_is_left_out(self):
        self.assertEqual(self.findings([row("a", None, 500, False)]), {})


if __name__ == "__main__":
    unittest.main()
