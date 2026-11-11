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

    def test_the_codes_are_the_skills_codes(self):
        skill = (pathlib.Path(pf.__file__).resolve().parent.parent / "SKILL.md").read_text()
        for code in ("PERSIST-REFUSED", "PERSIST-NOT-BUILT", "SLOW-QUERY", "LIVE-FALLBACK"):
            self.assertIn(f"| `{code}` |", skill)

    def test_a_rerun_replaces_its_own_issues(self):
        pf.write(self.run)
        pf.write(self.run)
        issues = [e for e in ledger.read_jsonl(self.run / "events.jsonl") if e["kind"] == "issue"]
        self.assertEqual(len(issues), 4)


if __name__ == "__main__":
    unittest.main()
