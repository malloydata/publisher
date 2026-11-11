#!/usr/bin/env python3
"""Tests for targets_from_run.py."""
import contextlib
import io
import json
import pathlib
import sys
import tempfile
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import targets_from_run as tfr  # noqa: E402


def call(qid, targets, sample=None, error=None):
    return {"kind": "tool_call", "tool": "get_context", "qid": qid, "sample": sample,
            "search_targets": targets, "error": error}


REV = {"target_type": "measure", "search_text": "revenue"}
CAT = {"target_type": "dimension", "search_text": "category"}
SRC = {"target_type": "source", "search_text": None}


class SentTargets(unittest.TestCase):
    def test_distinct_targets_in_first_seen_order_and_a_bare_one_has_no_text(self):
        events = [call("q", [REV, SRC]), call("q", [CAT, REV]),
                  {"kind": "tool_call", "tool": "execute_query", "qid": "q"}]
        self.assertEqual(tfr.sent_targets(events, "q"),
                         [REV, {"target_type": "source"}, CAT])

    def test_only_the_first_attempt_is_read(self):
        events = [call("q", [CAT], sample=1), call("q", [REV], sample=0)]
        self.assertEqual(tfr.sent_targets(events, "q"), [REV])

    def test_an_errored_call_is_not_copied(self):
        self.assertEqual(tfr.sent_targets([call("q", [REV], error="500")], "q"), [])

    def test_example_values_are_kept(self):
        t = {"target_type": "dimensional_value", "search_text": "status",
             "example_values": ["Cancelled"]}
        self.assertEqual(tfr.sent_targets([call("q", [t])], "q"), [t])


class Write(unittest.TestCase):
    def setUp(self):
        self.d = pathlib.Path(tempfile.mkdtemp())
        (self.d / "set").mkdir()
        (self.d / "run").mkdir()
        cases = [{"qid": "a", "question": "?"},
                 {"qid": "b", "question": "?", "searchTargets": [CAT]},
                 {"qid": "c", "question": "?"}]
        (self.d / "set" / "cases.jsonl").write_text("".join(json.dumps(c) + "\n" for c in cases))
        events = [call("a", [REV]), call("b", [REV])]
        (self.d / "run" / "events.jsonl").write_text("".join(json.dumps(e) + "\n" for e in events))

    def run_main(self, *extra):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = tfr.main(["--run", str(self.d / "run"), "--set", str(self.d / "set"), *extra])
        cases = [json.loads(x) for x in (self.d / "set" / "cases.jsonl").read_text().splitlines()]
        return code, {c["qid"]: c.get("searchTargets") for c in cases}, err.getvalue()

    def test_without_write_nothing_changes(self):
        code, got, _ = self.run_main()
        self.assertEqual((code, got), (0, {"a": None, "b": [CAT], "c": None}))

    def test_write_fills_an_empty_case_and_leaves_a_keyed_one(self):
        code, got, err = self.run_main("--write")
        self.assertEqual((code, got), (0, {"a": [REV], "b": [CAT], "c": None}))
        self.assertIn("b: already has searchTargets", err)
        self.assertIn("c: the run holds no get_context call", err)
        self.assertIn("Bump datasetVersion", err)

    def test_replace_overwrites(self):
        _, got, _ = self.run_main("--write", "--replace")
        self.assertEqual(got["b"], [REV])


if __name__ == "__main__":
    unittest.main()
