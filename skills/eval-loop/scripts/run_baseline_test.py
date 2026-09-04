#!/usr/bin/env python3
"""Tests for run_baseline's recording decisions: which query an attempt is
credited with, which retriever answered, what was actually re-executed, and
when an attempt is unjudgeable."""
import argparse
import json
import pathlib
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import run_baseline as rb  # noqa: E402


class NamedQuery(unittest.TestCase):
    def test_view_on_a_source(self):
        self.assertEqual(
            rb.named_query({"sourceName": "order_items", "queryName": "yoy"}),
            "run: order_items -> yoy")

    def test_model_level_query_has_no_source(self):
        self.assertEqual(rb.named_query({"queryName": "top_categories"}),
                         "run: top_categories")

    def test_snake_case_keys_are_the_same_call(self):
        self.assertEqual(
            rb.named_query({"source_name": "s", "query_name": "v"}),
            "run: s -> v")

    def test_ad_hoc_call_names_nothing(self):
        self.assertIsNone(rb.named_query({"query": "run: x -> y"}))


class FinalQuery(unittest.TestCase):
    def setUp(self):
        self.qs = ["run: a -> answer", "run: a -> probe"]
        self.calls = [
            {"tool": "execute_query", "query": "run: a -> answer", "error": None},
            {"tool": "execute_query", "query": "run: a -> probe", "error": None},
        ]

    def test_the_answer_names_its_query(self):
        text = "The total is 4744743.45.\n\n```malloy\nrun: a -> answer\n```"
        self.assertEqual(rb.pick_final_query(self.qs, self.calls, text),
                         ("run: a -> answer", "declared"))

    def test_a_fenced_block_it_never_ran_is_not_credited(self):
        text = "```malloy\nrun: a -> invented\n```"
        q, how = rb.pick_final_query(self.qs, self.calls, text)
        self.assertEqual(how, "last_ok")

    def test_whitespace_does_not_defeat_the_match(self):
        text = "```\nrun:   a\n  -> answer\n```"
        self.assertEqual(rb.pick_final_query(["run: a -> answer"],
                                             self.calls[:1], text)[1],
                         "declared")

    def test_the_last_error_is_skipped(self):
        calls = [self.calls[0],
                 {"tool": "execute_query", "query": "run: a -> probe",
                  "error": "no such field"}]
        self.assertEqual(rb.pick_final_query(self.qs, calls, "no fence"),
                         ("run: a -> answer", "last_ok"))

    def test_with_no_results_it_falls_back_to_the_last_query(self):
        self.assertEqual(rb.pick_final_query(self.qs, [], "no fence"),
                         ("run: a -> probe", "last"))

    def test_no_queries_at_all(self):
        self.assertEqual(rb.pick_final_query([], [], ""), (None, None))


class Retrieval(unittest.TestCase):
    def att(self, *modes):
        return {"calls": [{"tool": "get_context", "retrieval_mode": m}
                          for m in modes]}

    def test_all_semantic(self):
        mode, tally = rb.retrieval_summary([self.att("semantic", "semantic")])
        self.assertEqual(mode, "semantic")
        self.assertEqual(tally["semantic"], 2)

    def test_a_provider_that_fell_over_partway_is_mixed(self):
        self.assertEqual(
            rb.retrieval_summary([self.att("semantic", "lexical")])[0], "mixed")

    def test_no_provider_is_unreported_not_lexical(self):
        # An unrecorded retriever is not evidence that it was lexical.
        self.assertEqual(rb.retrieval_summary([self.att(None)])[0],
                         "unreported")

    def test_some_calls_unreported_is_mixed_not_semantic(self):
        self.assertEqual(
            rb.retrieval_summary([self.att("semantic", None)])[0], "mixed")

    def test_execute_query_is_not_retrieval(self):
        att = {"calls": [{"tool": "execute_query", "retrieval_mode": None}]}
        self.assertEqual(rb.retrieval_summary([att]),
                         ("unreported", {"semantic": 0, "lexical": 0,
                                         "unreported": 0}))


class ReExecution(unittest.TestCase):
    def setUp(self):
        self.tmp = pathlib.Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def pred(self, qid: str, query, rendered: str) -> None:
        d = self.tmp / qid
        d.mkdir(parents=True)
        (d / "prediction.json").write_text(
            json.dumps({"query": query, "rendered": rendered}))

    def test_counts_separate_a_failure_from_an_absent_query(self):
        self.pred("ok1", "run: a", "| total |\n| 1 |")
        self.pred("bad", "run: b", "(re-execution failed: no such field)")
        self.pred("none", None, "(the answerer ran no query, so there is "
                                "nothing to re-execute)")
        self.pred("skip", "run: d", "(not re-executed: the server is not "
                                    "serving the model)")
        got = rb.reexecution_summary(self.tmp,
                                     ["ok1", "bad", "none", "skip", "absent"])
        self.assertEqual(got, {"attempted": 2, "ok": 1, "failed": 1,
                               "noQuery": 1})


class JudgeGate(unittest.TestCase):
    """The gate decides what is unjudgeable, and it is not `submitted`."""

    def setUp(self):
        self.tmp = pathlib.Path(tempfile.mkdtemp())
        # rebuild without rejudge returns before any model call, so reaching
        # `no_saved_verdict` proves the attempt got past the gate.
        self.a = argparse.Namespace(rebuild=True, rejudge=False)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def judge(self, att):
        return rb.run_judge({"qid": "q", "question": "?", "golden": {}},
                            att, self.a, self.tmp, "", "", False)

    def test_neither_text_nor_query_is_not_submitted(self):
        v = self.judge({"answer_text": "", "submitted": False})
        self.assertEqual(v["reason"], "not_submitted")
        self.assertIsNone(v["verdict"])

    def test_prose_with_no_query_is_still_judged(self):
        # A confident refusal on an answerable case has to be scorable, or the
        # answerable-sounds-unanswerable cases measure nothing.
        v = self.judge({"answer_text": "I cannot answer that.",
                        "submitted": False})
        self.assertEqual(v["reason"], "no_saved_verdict")

    def test_a_query_with_no_prose_is_still_judged(self):
        v = self.judge({"answer_text": "", "submitted": True})
        self.assertEqual(v["reason"], "no_saved_verdict")


if __name__ == "__main__":
    unittest.main()
