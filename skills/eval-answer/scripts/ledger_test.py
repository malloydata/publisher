#!/usr/bin/env python3
"""Tests for ledger.py, the event contract. Stdlib only: python ledger_test.py"""
from __future__ import annotations

import json
import os
import pathlib
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import ledger  # noqa: E402


ATTEMPT = {"qid": "q1", "sample": 0, "phase": "baseline", "submitted": True,
           "final_query": "run: order_items -> by_month", "answer_text": "42",
           "transcriptPath": "artifacts/q1/answerer.jsonl"}

RUN = {"runId": "set-baseline-01", "target": "local", "answererModel": "m",
       "phase": "baseline", "started": "2026-09-03T00:00:00Z"}


class Contaminated(unittest.TestCase):
    """`contaminated` is `bool or "unknown"` (ledger-schema.md:162, :216).

    The drift this pins: run_baseline wrote the strings "true"/"false", which
    `bool()` reads as True in BOTH cases. A consumer taking the documented type
    at face value marks every clean attempt contaminated, and contamination
    nulls the verdict, so the run silently voids.
    """

    def test_bools_are_accepted_on_write(self):
        for v in (True, False):
            e = ledger.event("attempt", **ATTEMPT, contaminated=v)
            self.assertIs(e["contaminated"], v)

    def test_unknown_is_accepted_on_write(self):
        e = ledger.event("attempt", **ATTEMPT, contaminated="unknown")
        self.assertEqual(e["contaminated"], "unknown")

    def test_the_legacy_strings_are_rejected_on_write(self):
        for v in ("true", "false"):
            with self.assertRaises(ValueError) as cm:
                ledger.event("attempt", **ATTEMPT, contaminated=v)
            self.assertIn("expected bool", str(cm.exception))

    def test_score_events_are_held_to_the_same_type(self):
        score = {"qid": "q1", "sample": 0, "phase": "baseline",
                 "verdict": "match", "reason": "ok"}
        self.assertIs(
            ledger.event("score", **score, contaminated=False)["contaminated"],
            False)
        with self.assertRaises(ValueError):
            ledger.event("score", **score, contaminated="false")

    def test_is_contaminated_reads_both_shapes(self):
        # The bug in one assertion: "false" must NOT read as contaminated.
        self.assertFalse(ledger.is_contaminated({"contaminated": "false"}))
        self.assertTrue(ledger.is_contaminated({"contaminated": "true"}))
        self.assertFalse(ledger.is_contaminated({"contaminated": False}))
        self.assertTrue(ledger.is_contaminated({"contaminated": True}))

    def test_unknown_is_not_clean(self):
        # No host log means contamination could not be ruled out.
        self.assertTrue(ledger.is_contaminated({"contaminated": "unknown"}))

    def test_absent_is_clean(self):
        self.assertFalse(ledger.is_contaminated({}))


class StoredRuns(unittest.TestCase):
    def _run_dir(self, events: list[dict], cfg: dict | None = None):
        d = pathlib.Path(tempfile.mkdtemp())
        (d / "run.json").write_text(json.dumps(cfg if cfg is not None else RUN))
        (d / "events.jsonl").write_text(
            "".join(json.dumps(e) + "\n" for e in events))
        return d

    def test_a_legacy_string_on_disk_warns_rather_than_erroring(self):
        # The ledger is append-only: runs written before the fix must still
        # validate, or every historical run becomes unreadable.
        d = self._run_dir([{"kind": "attempt", **ATTEMPT,
                            "contaminated": "false"}])
        errors, warnings = ledger.validate_run(d)
        self.assertEqual(errors, [])
        self.assertTrue(any("legacy shape" in w for w in warnings), warnings)

    def test_a_bool_on_disk_is_clean(self):
        d = self._run_dir([{"kind": "attempt", **ATTEMPT,
                            "contaminated": False}])
        errors, warnings = ledger.validate_run(d)
        self.assertEqual(errors, [])
        self.assertFalse([w for w in warnings if "contaminated" in w])

    def test_a_missing_required_field_is_still_an_error(self):
        partial = {k: v for k, v in ATTEMPT.items() if k != "transcriptPath"}
        d = self._run_dir([{"kind": "attempt", **partial}])
        errors, _ = ledger.validate_run(d)
        self.assertTrue(any("transcriptPath" in e for e in errors), errors)

    def test_two_scores_for_one_attempt_is_an_error(self):
        score = {"kind": "score", "qid": "q1", "sample": 0,
                 "phase": "baseline", "verdict": "match", "reason": "ok"}
        d = self._run_dir([score, dict(score)])
        errors, _ = ledger.validate_run(d)
        self.assertTrue(any("second score" in e for e in errors), errors)


class TheUnderReportFloor(unittest.TestCase):
    """The floor consults two fields and used to be gated on a third.

    `mcp_tool_uses` was bound and conjoined into the condition without ever
    being compared, and it is OPTIONAL on an attempt event -- so the check
    written to catch an under-reporting answerer skipped every attempt that
    omitted it. It was applied to exactly the attempts carrying a field it
    never read.
    """

    def _run_dir(self, attempt: dict):
        d = pathlib.Path(tempfile.mkdtemp())
        (d / "run.json").write_text(json.dumps(RUN))
        (d / "events.jsonl").write_text(
            json.dumps({"kind": "attempt", **ATTEMPT, **attempt}) + "\n")
        return d

    def _floor_warnings(self, attempt: dict):
        _, warnings = ledger.validate_run(self._run_dir(attempt))
        return [w for w in warnings if "reported_calls" in w]

    def test_it_fires_without_mcp_tool_uses(self):
        # The regression: this attempt under-reports and carries no
        # `mcp_tool_uses`, which used to be enough to skip the floor entirely.
        self.assertTrue(self._floor_warnings(
            {"reported_calls": 9, "host_tool_uses": 2}))

    def test_it_still_fires_with_mcp_tool_uses(self):
        self.assertTrue(self._floor_warnings(
            {"reported_calls": 9, "host_tool_uses": 2, "mcp_tool_uses": 1}))

    def test_a_consistent_attempt_is_clean(self):
        self.assertEqual(self._floor_warnings(
            {"reported_calls": 2, "host_tool_uses": 5}), [])

    def test_an_attempt_reporting_neither_number_is_not_judged(self):
        # Absent is not "zero calls"; there is nothing to compare.
        self.assertEqual(self._floor_warnings({}), [])


class WriteContract(unittest.TestCase):
    def test_an_unknown_field_raises_on_write(self):
        with self.assertRaises(ValueError) as cm:
            ledger.event("attempt", **ATTEMPT, notAField=1)
        self.assertIn("unknown field", str(cm.exception))

    def test_an_unknown_kind_raises(self):
        with self.assertRaises(ValueError):
            ledger.event("not_a_kind", qid="q1")

    def test_a_bad_verdict_raises(self):
        with self.assertRaises(ValueError):
            ledger.event("score", qid="q1", sample=0, phase="baseline",
                         verdict="probably", reason="r")

    def test_a_tool_call_records_the_filters_it_ran_under(self):
        """`query`, `modelPath` and `filterParams` are one fact about one call.

        A re-execution replays the query; the file and the filter values decide
        what it means. Sent under another call's `report_id` it returns real
        rows for the wrong population and nothing says so -- unlike the wrong
        file, which at least errors. The field has to be writable for that
        pairing to be auditable from the ledger at all.
        """
        e = ledger.event("tool_call", qid="q1", sample=0, phase="baseline",
                         tool="execute_query", query="run: x -> y",
                         modelPath="a.malloy",
                         filterParams={"report_id": "123"})
        self.assertEqual(e["filterParams"], {"report_id": "123"})

    def test_run_config_requires_identity(self):
        with self.assertRaises(ValueError) as cm:
            ledger.run_config(**{k: v for k, v in RUN.items()
                                 if k != "answererModel"})
        self.assertIn("answererModel", str(cm.exception))


if __name__ == "__main__":
    unittest.main()
