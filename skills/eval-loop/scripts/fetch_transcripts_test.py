#!/usr/bin/env python3
"""Tests for fetch_transcripts. Run in place: python3 fetch_transcripts_test.py

Covers the parts that decide correctness without a network: the query it sends,
the escaping of an opaque id, and reading rows out of a transcript rather than
out of an agent's prose.
"""
from __future__ import annotations

import json
import pathlib
import sys
import unittest

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent.parent / "eval-answer" / "scripts"))

import fetch_transcripts as ft  # noqa: E402


class Query(unittest.TestCase):
    def test_it_filters_on_the_session_and_orders_oldest_first(self):
        q = ft.rows_query("get_context_calls", "abc-123", 50)
        self.assertIn("where: session_id = 'abc-123'", q)
        self.assertIn("order_by: `timestamp` asc", q)
        self.assertIn("limit: 50", q)

    def test_the_limit_is_coerced_to_an_integer(self):
        # It reaches the query as a literal, so a string here would be a hole.
        self.assertIn("limit: 10", ft.rows_query("s", "x", "10"))

    def test_a_quote_in_the_id_is_escaped_not_stripped(self):
        # Stripping produces a query for a DIFFERENT session that returns rows
        # and looks like a success.
        q = ft.rows_query("s", "a'b", 1)
        self.assertIn("'a\\'b'", q)

    def test_a_backslash_in_the_id_is_escaped_first(self):
        self.assertIn("'a\\\\b'", ft.rows_query("s", "a\\b", 1))


class RowsFromTranscript(unittest.TestCase):
    @staticmethod
    def transcript(text):
        return [{"type": "user", "message": {"content": [
            {"type": "tool_result", "tool_use_id": "t",
             "content": [{"type": "text", "text": text}]}]}}]

    def test_compact_rows_arrive_as_a_json_string_in_result(self):
        rows = [{"request_id": "r1"}]
        got = ft.rows_from_transcript(
            self.transcript(json.dumps({"result": json.dumps(rows)})))
        self.assertEqual(got, rows)

    def test_rows_already_parsed_are_taken_as_they_are(self):
        rows = [{"request_id": "r1"}]
        got = ft.rows_from_transcript(
            self.transcript(json.dumps({"result": rows})))
        self.assertEqual(got, rows)

    def test_the_publisher_resource_preamble_is_understood_too(self):
        rows = [{"request_id": "r1"}]
        body = json.dumps({"result": rows})
        got = ft.rows_from_transcript(self.transcript(
            f"[Resource from publisher at x] {body}"))
        self.assertEqual(got, rows)

    def test_prose_alone_yields_nothing(self):
        # The agent is a transport. Nothing it SAYS is data, so a reply with no
        # tool result is a miss, not a parse.
        got = ft.rows_from_transcript([{"type": "assistant", "message": {
            "content": [{"type": "text", "text": "I found 3 rows: r1 r2 r3"}]}}])
        self.assertEqual(got, [])

    def test_an_unreadable_result_is_a_miss_not_a_crash(self):
        self.assertEqual(ft.rows_from_transcript(self.transcript("{oops")), [])


class Normalise(unittest.TestCase):
    def test_columns_are_renamed_to_the_builders_keys(self):
        got = ft.normalise([{"request_id": "r", "response_body": {"a": 1}}],
                           ft.GET_CONTEXT_COLUMNS)
        self.assertEqual(got, [{"request_id": "r", "response": {"a": 1}}])

    def test_a_column_the_host_lacks_is_absent_not_none(self):
        # `log_transcript` distinguishes "not recorded" from "recorded as
        # nothing", and a None would erase that.
        got = ft.normalise([{"request_id": "r", "input_tokens": None}],
                           ft.TURN_COLUMNS)
        self.assertEqual(got, [{"request_id": "r"}])
        self.assertNotIn("input_tokens", got[0])


class TierDowngrade(unittest.TestCase):
    """A T2 fetch that came back with no prose did not get T2."""

    def setUp(self):
        self.calls = []

    def fake(self, rows_by_source):
        def _fetch(a, source, session_id):
            self.calls.append(source)
            return rows_by_source.get(source, [])
        return _fetch

    def args(self, tier):
        import argparse
        return argparse.Namespace(
            tier=tier, source_get_context="gc", source_execute="ex",
            source_messages="msg", source_turns="turn")

    def run_with(self, tier, rows):
        original = ft.fetch_rows
        ft.fetch_rows = self.fake(rows)
        try:
            return ft.fetch_session(self.args(tier), "s1")
        finally:
            ft.fetch_rows = original

    def test_t2_with_no_message_rows_is_written_as_t1(self):
        # Claiming answer_captured over an empty answer is the exact failure
        # the tier exists to prevent: the judge would score a real agent as
        # having said nothing.
        events, counts = self.run_with("T2", {"gc": [{"request_id": "r"}]})
        self.assertEqual(counts["tier"], "T1")
        self.assertFalse(events[0]["answer_captured"])

    def test_t2_with_prose_stays_t2(self):
        events, counts = self.run_with("T2", {
            "gc": [{"request_id": "r"}],
            "msg": [{"role": "assistant", "text": "42", "seq": 1,
                     "turn_started_ms": 1, "chunk": 0}]})
        self.assertEqual(counts["tier"], "T2")
        self.assertTrue(events[0]["answer_captured"])

    def test_t1_never_asks_the_host_for_the_prose_tables(self):
        # Asking for a table the host has not shipped returns an error per
        # case, which reads like a broken fetch rather than a chosen tier.
        self.run_with("T1", {"gc": [{"request_id": "r"}]})
        self.assertEqual(self.calls, ["gc", "ex"])


class ScopeParsing(unittest.TestCase):
    def args(self, scope):
        return ["--set", "s", "--out", "o", "--mcp-url", "u",
                "--logs-scope", scope]

    def test_a_two_part_scope_is_refused_rather_than_guessed(self):
        with self.assertRaises(SystemExit):
            ft.main(self.args("org/pkg"))

    def test_an_empty_segment_is_refused(self):
        with self.assertRaises(SystemExit):
            ft.main(self.args("org//pkg"))


if __name__ == "__main__":
    unittest.main(verbosity=1)
