#!/usr/bin/env python3
"""Tests for log_transcript. Run in place: python3 log_transcript_test.py"""
from __future__ import annotations

import argparse
import json
import pathlib
import shutil
import sys
import tempfile
import unittest

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent.parent / "eval-answer" / "scripts"))

import log_transcript as lt  # noqa: E402
import run_baseline as rb  # noqa: E402


class AssistantText(unittest.TestCase):
    def test_chunks_of_one_message_rejoin_with_no_separator(self):
        # A host caps a chunk at a byte limit, so the split lands mid-word.
        # Joining with anything at all corrupts the text.
        rows = [{"role": "assistant", "turn_started_ms": 1, "seq": 1,
                 "chunk": 0, "text": "the answer is fo"},
                {"role": "assistant", "turn_started_ms": 1, "seq": 1,
                 "chunk": 1, "text": "rty two"}]
        self.assertEqual(lt.assistant_text(rows), "the answer is forty two")

    def test_separate_messages_are_blank_line_separated(self):
        rows = [{"role": "assistant", "turn_started_ms": 1, "seq": 1,
                 "chunk": 0, "text": "one"},
                {"role": "assistant", "turn_started_ms": 1, "seq": 2,
                 "chunk": 0, "text": "two"}]
        self.assertEqual(lt.assistant_text(rows), "one\n\ntwo")

    def test_the_user_prompt_is_not_the_agents_answer(self):
        rows = [{"role": "user", "turn_started_ms": 1, "seq": 0,
                 "chunk": 0, "text": "how many orders?"},
                {"role": "assistant", "turn_started_ms": 1, "seq": 1,
                 "chunk": 0, "text": "42"}]
        self.assertEqual(lt.assistant_text(rows), "42")

    def test_rows_out_of_order_are_sorted_not_trusted(self):
        rows = [{"role": "assistant", "turn_started_ms": 2, "seq": 1,
                 "chunk": 0, "text": "second"},
                {"role": "assistant", "turn_started_ms": 1, "seq": 1,
                 "chunk": 0, "text": "first"}]
        self.assertEqual(lt.assistant_text(rows), "first\n\nsecond")


class Provenance(unittest.TestCase):
    def test_t1_admits_it_captured_no_answer_and_had_no_host_log(self):
        prov = lt.build_transcript(tier="T1")[0]
        self.assertEqual(prov["type"], "provenance")
        self.assertFalse(prov["answer_captured"])
        self.assertFalse(prov["host_log"])

    def test_t2_captured_the_answer_but_still_has_no_host_log(self):
        prov = lt.build_transcript(tier="T2")[0]
        self.assertTrue(prov["answer_captured"])
        self.assertFalse(prov["host_log"])

    def test_the_tier_decides_not_whether_text_happened_to_be_empty(self):
        # A T2 session in which the agent said nothing is a RESULT about the
        # agent. Flipping answer_captured off because the text is empty would
        # relabel it as a gap in the logs and drop it out of the pass rate.
        prov = lt.build_transcript(tier="T2", messages=[])[0]
        self.assertTrue(prov["answer_captured"])


class Payloads(unittest.TestCase):
    def test_a_payload_logged_as_json_text_is_parsed(self):
        ev = lt.build_transcript(get_context=[
            {"request_id": "r1", "timestamp": "t",
             "request_payload": json.dumps({"search_targets": [{"x": 1}]})}])
        self.assertEqual(ev[1]["message"]["content"][0]["input"],
                         {"search_targets": [{"x": 1}]})

    def test_an_unreadable_payload_costs_its_call_and_not_the_session(self):
        ev = lt.build_transcript(get_context=[
            {"request_id": "r1", "timestamp": "t",
             "request_payload": "{not json"}])
        self.assertEqual(ev[1]["message"]["content"][0]["input"], {})
        # The session still produced a transcript.
        self.assertEqual(ev[-1]["type"], "result")

    def test_calls_are_ordered_by_when_the_host_observed_them(self):
        ev = lt.build_transcript(
            get_context=[{"request_id": "b", "timestamp": "2020-01-02"}],
            execute=[{"request_id": "a", "timestamp": "2020-01-01"}])
        names = [c["message"]["content"][0]["name"] for c in ev
                 if c.get("type") == "assistant"
                 and c["message"]["content"][0].get("type") == "tool_use"]
        self.assertEqual(names, [lt.EXECUTE_QUERY, lt.GET_CONTEXT])


class ResultEvent(unittest.TestCase):
    def test_token_counts_sum_across_the_sessions_turns(self):
        ev = lt.build_transcript(turns=[
            {"input_tokens": 10, "output_tokens": 1, "round_trips": 2},
            {"input_tokens": 5, "output_tokens": 2, "round_trips": 3}])
        res = ev[-1]
        self.assertEqual(res["usage"]["input_tokens"], 15)
        self.assertEqual(res["num_turns"], 5)

    def test_a_field_no_turn_recorded_is_null_not_zero(self):
        # Zero is a measurement; null is the absence of one. A run reporting
        # 0 tokens for a real session reads as a free answer.
        ev = lt.build_transcript(turns=[{"input_tokens": 10}])
        self.assertIsNone(ev[-1]["usage"]["output_tokens"])

    def test_a_turn_the_host_recorded_as_disconnected_is_an_error(self):
        ev = lt.build_transcript(turns=[{"outcome": "disconnected"}])
        self.assertTrue(ev[-1]["is_error"])
        self.assertEqual(ev[-1]["subtype"], "disconnected")

    def test_a_completed_session_carries_no_error(self):
        ev = lt.build_transcript(turns=[{"outcome": "completed"}])
        self.assertFalse(ev[-1].get("is_error"))


class DerivesThroughTheRealParser(unittest.TestCase):
    """The point of the whole exercise: `derive_attempt` reads what we build."""

    def setUp(self):
        self.tmp = pathlib.Path(tempfile.mkdtemp())
        self.a = argparse.Namespace(target="local", hosted_tools=(),
                                    set_dir=self.tmp / "set",
                                    answerer_skills=[])

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def derive(self, events):
        return rb.derive_attempt(events, {"qid": "q"}, self.a, self.tmp, None)

    def test_a_t1_session_yields_tool_calls_and_admits_no_answer(self):
        events = lt.build_transcript(
            tier="T1",
            get_context=[{"request_id": "r1", "timestamp": "2020-01-01",
                          "request_payload": {
                              "search_targets": [
                                  {"target_type": "measure",
                                   "search_text": "revenue"}],
                              "scopes": [{"environment": "e",
                                          "package": "p"}]},
                          "response": {"sources": [
                              {"source_info": {"resource_id": {
                                  "source": "orders"}},
                               "entities": [{"name": "total_sales",
                                             "entity_type": "measure",
                                             "relevance": 0.9}]}]}}],
            execute=[{"request_id": "r2", "timestamp": "2020-01-02",
                      "request_payload": {"query": "run: orders -> by_month",
                                          "modelPath": "m.malloy"}}])
        att = self.derive(events)

        self.assertFalse(att["answer_captured"])
        self.assertFalse(att["host_log"])
        self.assertEqual(att["n_get_context"], 1)
        self.assertEqual(att["n_execute"], 1)
        self.assertEqual(att["final_query"], "run: orders -> by_month")
        self.assertEqual(att["final_model_path"], "m.malloy")

    def test_the_retrieval_response_is_read_by_the_existing_payload_parser(self):
        # mcp_payload already knows the hosted source-centric shape. This pins
        # that we hand it that shape unmodified, because re-encoding it here
        # would be a second place for that knowledge to live.
        events = lt.build_transcript(
            get_context=[{"request_id": "r1", "timestamp": "t",
                          "request_payload": {"search_targets": [
                              {"target_type": "measure",
                               "search_text": "revenue"}]},
                          "response": {"retrieval": "semantic", "sources": [
                              {"source_info": {"resource_id": {
                                  "source": "orders"}},
                               "entities": [{"name": "total_sales",
                                             "entity_type": "measure"}]}]}}])
        call = self.derive(events)["calls"][0]
        self.assertEqual(call["retrieval_mode"], "semantic")
        self.assertIn("measure:orders:total_sales",
                      call["rankedSummary"]["entityIds"])
        self.assertEqual(call["targets"], ["measure: revenue"])
        self.assertEqual(call["target_shapes"],
                         [{"type": "measure", "has_text": True}])

    def test_a_bare_target_survives_into_target_shapes(self):
        # The bare-target rate is the whole "enumerates instead of searching"
        # argument, and `targets` drops a target carrying no text.
        events = lt.build_transcript(
            get_context=[{"request_id": "r1", "timestamp": "t",
                          "request_payload": {"search_targets": [
                              {"target_type": "view"}]}}])
        call = self.derive(events)["calls"][0]
        self.assertEqual(call["target_shapes"],
                         [{"type": "view", "has_text": False}])

    def test_a_t2_session_carries_the_answer_through(self):
        events = lt.build_transcript(
            tier="T2",
            execute=[{"request_id": "r1", "timestamp": "t",
                      "request_payload": {"query": "run: orders -> n"}}],
            messages=[{"role": "assistant", "turn_started_ms": 1, "seq": 1,
                       "chunk": 0, "text": "There were 42 orders."}])
        att = self.derive(events)
        self.assertTrue(att["answer_captured"])
        self.assertEqual(att["answer_text"], "There were 42 orders.")

    def test_a_named_view_call_renders_as_the_malloy_it_stands_for(self):
        events = lt.build_transcript(execute=[
            {"request_id": "r1", "timestamp": "t",
             "request_payload": {"sourceName": "orders",
                                 "queryName": "by_month"}}])
        self.assertEqual(self.derive(events)["final_query"],
                         "run: orders -> by_month")


class RoundTrip(unittest.TestCase):
    """A spawned transcript, projected down to log rows and rebuilt, derives
    the same tool calls. This is the test that proves the two paths converge,
    and it needs no network and no spend."""

    def setUp(self):
        self.tmp = pathlib.Path(tempfile.mkdtemp())
        self.a = argparse.Namespace(target="local", hosted_tools=(),
                                    set_dir=self.tmp / "set",
                                    answerer_skills=[])

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    SPAWNED = [
        {"type": "assistant", "message": {"content": [
            {"type": "tool_use", "id": "a1",
             "name": "mcp__publisher__get_context",
             "input": {"search_targets": [{"target_type": "measure",
                                           "search_text": "revenue"}],
                       "scopes": [{"environment": "e", "package": "p"}]}}]}},
        {"type": "user", "message": {"content": [
            {"type": "tool_result", "tool_use_id": "a1", "content": [
                {"type": "text", "text": json.dumps({
                    "retrieval": "semantic",
                    "sources": [{"source_info": {"resource_id": {
                        "source": "orders"}},
                        "entities": [{"name": "total_sales",
                                      "entity_type": "measure"}]}]})}]}]}},
        {"type": "assistant", "message": {"content": [
            {"type": "tool_use", "id": "a2",
             "name": "mcp__publisher__execute_query",
             "input": {"query": "run: orders -> by_month",
                       "modelPath": "m.malloy"}}]}},
        {"type": "user", "message": {"content": [
            {"type": "tool_result", "tool_use_id": "a2",
             "content": [{"type": "text", "text": "{}"}]}]}},
        {"type": "assistant", "message": {"content": [
            {"type": "text", "text": "42 orders."}]}},
        {"type": "result", "usage": {}, "num_turns": 3},
    ]

    @staticmethod
    def project(events):
        """The spawned transcript as the columns a host would have logged."""
        gc, ex, pending = [], [], {}
        for e in events:
            if e.get("type") == "assistant":
                for c in e["message"]["content"]:
                    if c.get("type") == "tool_use":
                        pending[c["id"]] = c
            elif e.get("type") == "user":
                for c in e["message"]["content"]:
                    if c.get("type") != "tool_result":
                        continue
                    use = pending[c["tool_use_id"]]
                    body = c["content"][0]["text"]
                    row = {"request_id": use["id"], "timestamp": use["id"],
                           "request_payload": json.dumps(use["input"]),
                           "response": json.loads(body)}
                    (gc if use["name"].endswith("get_context")
                     else ex).append(row)
        return gc, ex

    def test_the_rebuilt_transcript_derives_the_same_tool_calls(self):
        spawned = rb.derive_attempt(self.SPAWNED, {"qid": "q"}, self.a,
                                    self.tmp / "s", None)
        gc, ex = self.project(self.SPAWNED)
        rebuilt = rb.derive_attempt(
            lt.build_transcript(get_context=gc, execute=ex, tier="T1"),
            {"qid": "q"}, self.a, self.tmp / "r", None)

        self.assertEqual(spawned["calls"], rebuilt["calls"])
        self.assertEqual(spawned["final_query"], rebuilt["final_query"])
        self.assertEqual(spawned["final_model_path"],
                         rebuilt["final_model_path"])
        self.assertEqual(spawned["n_get_context"], rebuilt["n_get_context"])
        self.assertEqual(spawned["n_execute"], rebuilt["n_execute"])

    def test_and_differs_exactly_where_the_source_is_poorer(self):
        # The honest part: what T1 loses is the prose and the contamination
        # check, and nothing else. If this ever starts failing on another
        # field, that field silently became unmeasurable on a logged run.
        spawned = rb.derive_attempt(self.SPAWNED, {"qid": "q"}, self.a,
                                    self.tmp / "s", None)
        gc, ex = self.project(self.SPAWNED)
        rebuilt = rb.derive_attempt(
            lt.build_transcript(get_context=gc, execute=ex, tier="T1"),
            {"qid": "q"}, self.a, self.tmp / "r", None)

        differ = {k for k in spawned
                  if k != "transcriptPath" and spawned[k] != rebuilt[k]}
        # `breaches` differs because a T1 source has no host log to check, so
        # the isolation checks do not run and contamination is recorded as
        # "unknown" rather than as clean. `num_turns` because this fixture
        # passes no turn rows. Everything else is identical, and that is the
        # claim: what a logged run loses is the prose and the contamination
        # check, not the tool calls.
        self.assertEqual(differ, {"answer_text", "answer_captured",
                                  "host_log", "num_turns", "breaches"})


class Writing(unittest.TestCase):
    def test_one_json_object_per_line(self):
        tmp = pathlib.Path(tempfile.mkdtemp())
        try:
            path = tmp / "a" / "answerer.jsonl"
            lt.write_transcript(lt.build_transcript(tier="T1"), path)
            lines = path.read_text().splitlines()
            self.assertTrue(all(json.loads(l) for l in lines))
            self.assertEqual(json.loads(lines[0])["type"], "provenance")
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    unittest.main(verbosity=1)
