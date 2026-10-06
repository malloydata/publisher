#!/usr/bin/env python3
"""Tests for hosted_query. Run in place: python3 hosted_query_test.py

Covers reading rows out of the transport agent's transcript, which decides
correctness without a network: both result shapes, a cut page, a host error,
and prose that must never be read as data.
"""
from __future__ import annotations

import json
import pathlib
import sys
import unittest

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent.parent / "eval-answer" / "scripts"))

import hosted_query as hq  # noqa: E402


def rows_of(events):
    return hq.result_from_transcript(events)[0]


class RowsFromTranscript(unittest.TestCase):
    @staticmethod
    def transcript(text):
        return [{"type": "user", "message": {"content": [
            {"type": "tool_result", "tool_use_id": "t",
             "content": [{"type": "text", "text": text}]}]}}]

    def test_compact_rows_arrive_as_a_json_string_in_result(self):
        rows = [{"request_id": "r1"}]
        got = rows_of(
            self.transcript(json.dumps({"result": json.dumps(rows)})))
        self.assertEqual(got, rows)

    def test_rows_already_parsed_are_taken_as_they_are(self):
        rows = [{"request_id": "r1"}]
        got = rows_of(
            self.transcript(json.dumps({"result": rows})))
        self.assertEqual(got, rows)

    def test_the_publisher_resource_preamble_is_understood_too(self):
        rows = [{"request_id": "r1"}]
        body = json.dumps({"result": rows})
        got = rows_of(self.transcript(
            f"[Resource from publisher at x] {body}"))
        self.assertEqual(got, rows)

    def test_credible_columnar_rows_are_zipped_with_their_columns(self):
        # Credible's execute_query names each column once and sends rows by
        # position. Read as `result`, it yields nothing, and every case would
        # come back as an empty turn.
        body = {"_format": "columnar-v1",
                "rows": {"columns": ["request_id", "tool"],
                         "rows": [["r1", "t1"], ["r2", "t2"]]},
                "_limit_hit": False}
        got, cut, _err = hq.result_from_transcript(self.transcript(json.dumps(body)))
        self.assertEqual(got, [{"request_id": "r1", "tool": "t1"},
                               {"request_id": "r2", "tool": "t2"}])
        self.assertFalse(cut)

    def test_a_cut_page_says_so(self):
        body = {"_format": "columnar-v1",
                "rows": {"columns": ["a"], "rows": [[1]]}, "_limit_hit": True}
        _rows, cut, _err = hq.result_from_transcript(self.transcript(json.dumps(body)))
        self.assertTrue(cut)

    def test_a_deferred_tool_reference_before_the_result_is_skipped(self):
        # The CLI may load the tool first; that result block carries no rows.
        events = [{"type": "user", "message": {"content": [
            {"type": "tool_result", "tool_use_id": "t0",
             "content": [{"type": "tool_reference", "tool_name": "x"}]}]}}]
        events += self.transcript(json.dumps({"result": [{"a": 1}]}))
        self.assertEqual(rows_of(events), [{"a": 1}])

    def test_prose_alone_yields_nothing(self):
        # The agent is a transport. Nothing it SAYS is data, so a reply with no
        # tool result is a miss, not a parse.
        got = rows_of([{"type": "assistant", "message": {
            "content": [{"type": "text", "text": "I found 3 rows: r1 r2 r3"}]}}])
        self.assertEqual(got, [])

    def test_an_unreadable_result_is_a_miss_not_a_crash(self):
        self.assertEqual(rows_of(self.transcript("{oops")), [])

    def test_a_host_error_is_an_error_not_an_empty_result(self):
        # A rejected query and a query that returned nothing mean opposite
        # things to a golden check.
        events = [{"type": "user", "message": {"content": [
            {"type": "tool_result", "tool_use_id": "t", "is_error": True,
             "content": [{"type": "text", "text": "403 forbidden"}]}]}}]
        rows, _cut, err = hq.result_from_transcript(events)
        self.assertEqual(rows, [])
        self.assertIn("403", err)


if __name__ == "__main__":
    unittest.main(verbosity=1)
