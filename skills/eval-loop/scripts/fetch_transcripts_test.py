#!/usr/bin/env python3
"""Tests for fetch_transcripts. Run in place: python3 fetch_transcripts_test.py

Covers the parts that decide correctness without a network: the query it sends,
the escaping of an opaque id, reading rows out of a transcript rather than out
of an agent's prose, and assembling one turn from several logged sources.
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
    def test_it_filters_on_every_id_and_orders_oldest_first(self):
        q = ft.rows_query("agent_tool_calls", "request_id", ["a1", "b2"], 50)
        self.assertIn("where: request_id = 'a1' | 'b2'", q)
        self.assertIn("order_by: `timestamp` asc", q)
        self.assertIn("limit: 50", q)

    def test_the_limit_is_coerced_to_an_integer(self):
        # It reaches the query as a literal, so a string here would be a hole.
        self.assertIn("limit: 10", ft.rows_query("s", "f", ["x"], "10"))

    def test_a_quote_in_the_id_is_escaped_not_stripped(self):
        # Stripping produces a query for a DIFFERENT turn that returns rows
        # and looks like a success.
        q = ft.rows_query("s", "f", ["a'b"], 1)
        self.assertIn("'a\\'b'", q)

    def test_a_backslash_in_the_id_is_escaped_first(self):
        self.assertIn("'a\\\\b'", ft.rows_query("s", "f", ["a\\b"], 1))


class Normalise(unittest.TestCase):
    def test_the_published_column_names_are_read(self):
        got = ft.normalise([{"request_id": "r", "arguments": "{}",
                             "error_message": "bad"}], ft.TOOL_COLUMNS)
        self.assertEqual(got, [{"request_id": "r", "request_payload": "{}",
                                "error": "bad"}])

    def test_the_older_column_names_are_still_read(self):
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


class RetrievalBody(unittest.TestCase):
    def test_the_http_envelope_is_removed(self):
        logged = json.dumps({"status_code": "200",
                             "response_body": {"sources": []}})
        self.assertEqual(ft.retrieval_body(logged), {"sources": []})

    def test_a_bare_body_is_passed_through(self):
        self.assertEqual(ft.retrieval_body({"sources": []}), {"sources": []})


def gc_call(ts, targets, session="s1", error=None):
    row = {"session_id": session, "timestamp": ts,
           "tool": "mcp__credible__get_context",
           "request_payload": json.dumps({"search_targets": targets})}
    if error:
        row["error"] = error
    return row


def gc_logged(ts, targets, body, session="s1"):
    return {"session_id": session, "timestamp": ts,
            "request_payload": json.dumps({"search_targets": targets}),
            "response": json.dumps({"response_body": body})}


T1 = [{"target_type": "measure", "search_text": "revenue"}]
T2 = [{"target_type": "dimension", "search_text": "brand"}]


class PairRetrieval(unittest.TestCase):
    def test_a_call_gets_the_response_that_searched_for_the_same_targets(self):
        calls = [gc_call("2026-10-02T14:09:52.217Z", T1)]
        logged = [gc_logged("2026-10-02T14:09:52.080Z", T2, {"x": 2}),
                  gc_logged("2026-10-02T14:09:52.080Z", T1, {"x": 1})]
        self.assertEqual(ft.pair_retrieval(calls, logged), 0)
        self.assertEqual(calls[0]["response"], {"x": 1})

    def test_a_response_outside_the_window_is_another_turns(self):
        calls = [gc_call("2026-10-02T14:09:52Z", T1)]
        logged = [gc_logged("2026-10-02T14:19:52Z", T1, {"x": 1})]
        self.assertEqual(ft.pair_retrieval(calls, logged), 1)
        self.assertNotIn("response", calls[0])

    def test_a_response_from_another_session_is_never_taken(self):
        calls = [gc_call("2026-10-02T14:09:52Z", T1)]
        logged = [gc_logged("2026-10-02T14:09:52Z", T1, {"x": 1}, session="s2")]
        self.assertEqual(ft.pair_retrieval(calls, logged), 1)

    def test_two_identical_searches_take_one_response_each_in_order(self):
        calls = [gc_call("2026-10-02T14:09:50Z", T1),
                 gc_call("2026-10-02T14:09:53Z", T1)]
        logged = [gc_logged("2026-10-02T14:09:50Z", T1, {"x": 1}),
                  gc_logged("2026-10-02T14:09:53Z", T1, {"x": 2})]
        ft.pair_retrieval(calls, logged)
        self.assertEqual([c["response"] for c in calls], [{"x": 1}, {"x": 2}])

    def test_a_failed_call_gets_no_response_and_is_not_counted_unpaired(self):
        # The service may never have seen it; attaching a neighbour's results
        # would credit a failed search with a success.
        calls = [gc_call("2026-10-02T14:09:52Z", T1, error="tool_error")]
        logged = [gc_logged("2026-10-02T14:09:52Z", T1, {"x": 1})]
        self.assertEqual(ft.pair_retrieval(calls, logged), 0)
        self.assertNotIn("response", calls[0])


class SplitToolCalls(unittest.TestCase):
    def test_tools_are_told_apart_by_suffix_whatever_the_server_name(self):
        rows = [{"tool": "mcp__credible__get_context", "outcome": "ok"},
                {"tool": "mcp__credible_analysis__execute_query", "outcome": "ok"},
                {"tool": "mcp__analysis_tools__suggest_prompts", "outcome": "ok"}]
        gc, ex = ft.split_tool_calls(rows)
        self.assertEqual(len(gc), 1)
        self.assertEqual(len(ex), 1)

    def test_a_failed_call_without_a_message_carries_its_outcome_as_error(self):
        # Otherwise pick_final_query treats a rejected query as answered and
        # hands it to the judge.
        _gc, ex = ft.split_tool_calls([
            {"tool": "x__execute_query", "outcome": "compile_error"}])
        self.assertEqual(ex[0]["error"], "compile_error")

    def test_a_successful_call_carries_no_error(self):
        _gc, ex = ft.split_tool_calls([
            {"tool": "x__execute_query", "outcome": "ok", "error": "stale"}])
        self.assertNotIn("error", ex[0])


class BuildCase(unittest.TestCase):
    def build(self, tier="T2", tools=(), retrieval=(), messages=(), names=True):
        return ft.build_case("r1", tier, tools=[dict(t) for t in tools],
                             retrieval=list(retrieval),
                             messages=[dict(m) for m in messages], turns=[],
                             session_id="s1", tool_log_names_tools=names)

    def test_t2_with_no_prose_is_written_as_t1(self):
        # Claiming answer_captured over an empty answer is the exact failure
        # the tier exists to prevent: the judge would score a real agent as
        # having said nothing.
        events, counts = self.build(tools=[gc_call("2026-01-01T00:00:00Z", T1)])
        self.assertEqual(counts["tier"], "T1")
        self.assertFalse(events[0]["answer_captured"])

    def test_replies_with_no_role_column_are_the_agents(self):
        # Credible's reply table holds only the agent's replies and has no
        # role column; dropping them would downgrade every turn to T1.
        events, counts = self.build(messages=[
            {"request_id": "r1", "seq": 1, "chunk": 0, "text": "$1,234"}])
        self.assertEqual(counts["tier"], "T2")
        texts = [c["text"] for e in events if e.get("type") == "assistant"
                 for c in e["message"]["content"] if c.get("type") == "text"]
        self.assertEqual(texts, ["$1,234"])

    def test_a_refusal_with_no_calls_does_not_borrow_the_sessions_searches(self):
        # A turn that called nothing has no tool rows. The retrieval rows in
        # the batch belong to OTHER turns of the session and must stay out.
        events, counts = self.build(
            retrieval=[gc_logged("2026-01-01T00:00:00Z", T1, {"x": 1})],
            messages=[{"seq": 1, "chunk": 0, "text": "I can't help with that."}])
        self.assertEqual(counts["get_context"], 0)
        self.assertFalse(any(e.get("type") == "assistant" and any(
            c.get("type") == "tool_use" for c in e["message"]["content"])
            for e in events))

    def test_the_older_shape_takes_retrieval_rows_as_the_calls(self):
        _events, counts = self.build(
            tier="T1", names=False,
            retrieval=[gc_logged("2026-01-01T00:00:00Z", T1, {"x": 1})])
        self.assertEqual(counts["get_context"], 1)


class FetchAll(unittest.TestCase):
    """The batch asks each source once, and only for what the tier claims."""

    def setUp(self):
        self.calls = []

    def run_with(self, tier, unit="turn", turns=None):
        import argparse

        def fake(a, source, field, ids):
            self.calls.append((source, field, tuple(ids)))
            if source == "turn":
                return turns or [{"request_id": "r1", "session_id": "s1"}]
            return []

        a = argparse.Namespace(tier=tier, unit=unit, source_turns="turn",
                               source_execute="tools", source_get_context="gc",
                               source_messages="msg")
        original = ft.fetch_rows
        ft.fetch_rows = fake
        try:
            return ft.fetch_all(a, ["r1"])
        finally:
            ft.fetch_rows = original

    def test_t1_never_asks_the_host_for_the_prose_table(self):
        # Asking for a table the host has not shipped returns an error, which
        # reads like a broken fetch rather than a chosen tier.
        self.run_with("T1")
        self.assertNotIn("msg", [c[0] for c in self.calls])

    def test_turn_mode_finds_retrieval_rows_by_the_turns_session(self):
        self.run_with("T1")
        self.assertIn(("gc", "session_id", ("s1",)), self.calls)
        self.assertIn(("tools", "request_id", ("r1",)), self.calls)


class RepeatedAnswers(unittest.TestCase):
    """A list in --source-map writes one ordinary run directory per repeat."""

    def test_each_position_is_its_own_run_from_one_fetch(self):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            root = pathlib.Path(d)
            (root / "set").mkdir()
            (root / "set" / "cases.jsonl").write_text("\n".join(json.dumps(c)
                for c in [{"qid": "q1", "question": "?", "source": "x"},
                          {"qid": "q2", "question": "?", "source": "y"}]) + "\n")
            (root / "map.json").write_text(json.dumps(
                {"q1": ["t1", "t2", "t3"], "q2": "t4"}))
            fetched = []

            def fake(a, ids):
                fetched.append(list(ids))
                return {"turns": [{"request_id": i, "session_id": "s"}
                                  for i in ids],
                        "tools": [], "retrieval": [],
                        "messages": [{"request_id": i, "seq": 1, "chunk": 0,
                                      "text": f"answer {i}"} for i in ids]}

            original = ft.fetch_all
            ft.fetch_all = fake
            try:
                ft.main(["--set", str(root / "set"), "--out", str(root / "out"),
                         "--source-map", str(root / "map.json"), "--tier", "T2",
                         "--mcp-url", "u", "--logs-scope", "o/e/p"])
            finally:
                ft.fetch_all = original

            self.assertEqual(fetched, [["t1", "t2", "t3", "t4"]])
            got = sorted(str(p.relative_to(root / "out"))
                         for p in (root / "out").rglob("answerer.jsonl"))
            self.assertEqual(got, [
                "attempt-1/artifacts/q1/answerer.jsonl",
                "attempt-1/artifacts/q2/answerer.jsonl",
                "attempt-2/artifacts/q1/answerer.jsonl",
                "attempt-3/artifacts/q1/answerer.jsonl"])
            text = (root / "out" / "attempt-2" / "artifacts" / "q1" /
                    "answerer.jsonl").read_text()
            self.assertIn("answer t2", text)


def search(rid, ts, prompt=None, who="u@x.com", session=None, org="o"):
    return {"request_id": rid, "timestamp": ts, "user_email": who,
            "organization_id": org, "user_prompt": prompt,
            "session_id": session, "request_payload": "{}"}


def query(rid, ts, who="u@x.com", session=None, org="o"):
    return {"request_id": rid, "timestamp": ts, "user_email": who,
            "organization_id": org, "session_id": session,
            "request_payload": "{}", "outcome": "ok"}


class WindowRows(unittest.TestCase):
    """One third-party question's calls, reconstructed from a person's rows."""

    def rows(self, searches, queries, anchor=None):
        anchor = anchor or searches[0]
        s, q = ft.window_rows(anchor, searches, queries, gap_minutes=30,
                              max_minutes=120)
        return [r["request_id"] for r in s], [r["request_id"] for r in q]

    def test_it_takes_the_persons_calls_after_the_question(self):
        s, q = self.rows(
            [search("a", "2026-10-01T10:00:00Z", "total sales last quarter"),
             search("b", "2026-10-01T10:00:20Z")],
            [query("q1", "2026-10-01T10:00:40Z")])
        self.assertEqual((s, q), (["a", "b"], ["q1"]))

    def test_a_search_for_a_different_question_starts_the_next_case(self):
        s, q = self.rows(
            [search("a", "2026-10-01T10:00:00Z", "total sales last quarter"),
             search("b", "2026-10-01T10:02:00Z", "top carriers")],
            [query("q1", "2026-10-01T10:01:00Z"),
             query("q2", "2026-10-01T10:03:00Z")])
        self.assertEqual((s, q), (["a"], ["q1"]))

    def test_a_long_gap_ends_the_window(self):
        s, q = self.rows(
            [search("a", "2026-10-01T10:00:00Z", "p")],
            [query("q1", "2026-10-01T10:10:00Z"),
             query("q2", "2026-10-01T11:00:00Z")])
        self.assertEqual(q, ["q1"])

    def test_in_app_rows_and_other_people_are_never_taken(self):
        # In-app rows carry a session and have their own unit; another
        # person's query in the same minute is not this person's work.
        s, q = self.rows(
            [search("a", "2026-10-01T10:00:00Z", "p")],
            [query("q1", "2026-10-01T10:00:10Z", session="sess"),
             query("q2", "2026-10-01T10:00:20Z", who="v@x.com"),
             query("q3", "2026-10-01T10:00:30Z")])
        self.assertEqual(q, ["q3"])

    def test_rows_before_the_question_are_not_its_answer(self):
        s, q = self.rows(
            [search("a", "2026-10-01T10:00:00Z", "p")],
            [query("q0", "2026-10-01T09:59:00Z")])
        self.assertEqual(q, [])

    def test_the_anchor_is_kept_even_if_the_range_query_missed_it(self):
        anchor = search("a", "2026-10-01T10:00:00Z", "p")
        s, _q = self.rows([], [], anchor=anchor)
        self.assertEqual(s, ["a"])


class WindowQuery(unittest.TestCase):
    def test_it_bounds_by_person_and_time_and_escapes_emails(self):
        q = ft.window_query("query_executions", ["a'b@x.com"],
                            "2026-10-01 10:00:00", "2026-10-01 12:00:00",
                            "surface = 'mcp'", 100)
        self.assertIn("user_email = 'a\\'b@x.com'", q)
        self.assertIn("`timestamp` >= @2026-10-01 10:00:00", q)
        self.assertIn("and (surface = 'mcp')", q)


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
