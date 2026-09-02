#!/usr/bin/env python3
"""Tests for score_retrieval. Stdlib only: python score_retrieval_test.py"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from score_retrieval import (  # noqa: E402
    attribute, main, score_case, summarise,
)

M_SALES = "measure:order_items:total_sales"
M_COUNT = "measure:order_items:order_count"
D_STATUS = "dimension:order_items:status"
V_TOP = "view:order_items:top_categories"


def case(qid="q", coverage="covered", required=(M_SALES,), acceptable=()):
    return {"qid": qid, "coverage": coverage,
            "expectedEntities": {"required": list(required),
                                 "acceptable": list(acceptable) or list(required)}}


def calls(*entity_groups, qid="q", sample=None, phase="baseline"):
    return [{"kind": "tool_call", "tool": "get_context", "qid": qid,
             "sample": sample, "phase": phase,
             "rankedSummary": {"entityIds": list(g)}} for g in entity_groups]


KEY = ("q", None, "baseline")

M_SALES_ALIAS = "measure:orders:total_sales"     # same type + name, sibling source
M_REVENUE = "measure:order_items:revenue"


def calls_with_docs(entities, tokens, qid="q"):
    return [{"kind": "tool_call", "tool": "get_context", "qid": qid,
             "sample": None, "phase": "baseline",
             "rankedSummary": {"entityIds": list(entities),
                               "docTokens": list(tokens)}}]


class Routes(unittest.TestCase):
    """How an entity may reach the answerer. Only `missing` is a retrieval miss;
    the route is recorded so the strict (ranked-only) count stays recoverable."""

    def test_same_name_under_a_sibling_source_is_delivered_as_alias(self):
        r = score_case(case(required=[M_SALES]), calls([M_SALES_ALIAS]), KEY, "match")
        self.assertEqual(r["recall"], 1.0)
        self.assertEqual(r["delivery"][M_SALES], "alias")
        self.assertEqual(r["n_ranked"], 1)

    def test_named_in_a_returned_sources_docs_is_delivered_but_not_ranked(self):
        r = score_case(case(required=[M_SALES]),
                       calls_with_docs([D_STATUS], ["total_sales", "status"]),
                       KEY, "match")
        self.assertEqual(r["recall"], 1.0)
        self.assertEqual(r["delivery"][M_SALES], "in_docs")
        self.assertEqual(r["n_ranked"], 0)

    def test_a_different_type_with_the_same_name_is_not_an_alias(self):
        r = score_case(case(required=[M_SALES]),
                       calls(["dimension:order_items:total_sales"]), KEY, "no_match")
        self.assertEqual(r["delivery"][M_SALES], "missing")
        self.assertEqual(r["recall"], 0.0)


class AnyOf(unittest.TestCase):
    """A case the model can answer through more than one route names them as a
    group; any one member satisfies it. Naming only one scored the other route
    as a retrieval miss and sent diagnosis after retrieval for a failure that
    was never retrieval's."""

    def group_case(self):
        return {"qid": "q", "coverage": "covered",
                "expectedEntities": {"required": [D_STATUS],
                                     "requiredAnyOf": [[M_SALES, M_REVENUE]]}}

    def test_either_member_satisfies_the_group(self):
        for got in ([D_STATUS, M_SALES], [D_STATUS, M_REVENUE]):
            r = score_case(self.group_case(), calls(got), KEY, "match")
            self.assertEqual(r["recall"], 1.0, got)
            self.assertEqual(r["n_required"], 2)

    def test_an_unmet_group_is_one_miss_named_with_both_routes(self):
        r = score_case(self.group_case(), calls([D_STATUS]), KEY, "no_match")
        self.assertEqual(r["recall"], 0.5)
        self.assertEqual(r["missing"], [f"{M_REVENUE} | {M_SALES}"]
                         if f"{M_REVENUE} | {M_SALES}" in r["missing"]
                         else [f"{M_SALES} | {M_REVENUE}"])
        self.assertEqual(r["where_to_fix"], "retrieval ranking")

    def test_full_delivery_with_a_wrong_answer_is_construction(self):
        r = score_case(self.group_case(), calls([D_STATUS, M_REVENUE]), KEY, "no_match")
        self.assertEqual(r["where_to_fix"], "query construction")


class Recall(unittest.TestCase):
    def test_everything_needed_was_returned(self):
        r = score_case(case(required=[M_SALES, D_STATUS]),
                       calls([M_SALES, D_STATUS]), KEY, "no_match")
        self.assertEqual(r["recall"], 1.0)
        self.assertEqual(r["missing"], [])

    def test_a_missing_entity_lowers_recall_and_is_named(self):
        r = score_case(case(required=[M_SALES, D_STATUS]),
                       calls([M_SALES]), KEY, "no_match")
        self.assertEqual(r["recall"], 0.5)
        self.assertEqual(r["missing"], [D_STATUS])

    def test_entities_pool_across_calls_because_the_agent_saw_them_all(self):
        r = score_case(case(required=[M_SALES, D_STATUS]),
                       calls([M_SALES], [D_STATUS]), KEY, "no_match")
        self.assertEqual(r["recall"], 1.0)
        self.assertEqual(r["n_get_context"], 2)

    def test_only_this_attempts_calls_count(self):
        events = calls([M_SALES]) + calls([D_STATUS], sample=2)
        r = score_case(case(required=[M_SALES, D_STATUS]), events, KEY, "no_match")
        self.assertEqual(r["recall"], 0.5, "sample 2's call leaked into sample 1")

    def test_execute_query_calls_are_not_retrieval(self):
        events = calls([M_SALES]) + [
            {"kind": "tool_call", "tool": "execute_query", "qid": "q",
             "sample": None, "phase": "baseline",
             "rankedSummary": {"entityIds": [D_STATUS]}}]
        r = score_case(case(required=[M_SALES, D_STATUS]), events, KEY, "no_match")
        self.assertEqual(r["recall"], 0.5)


class Precision(unittest.TestCase):
    def test_an_acceptable_alternate_is_not_noise(self):
        c = case(required=[M_SALES], acceptable=[M_SALES, M_COUNT])
        r = score_case(c, calls([M_SALES, M_COUNT]), KEY, "match")
        self.assertEqual(r["precision"], 1.0)
        self.assertEqual(r["noise"], [])

    def test_an_unrelated_entity_is_noise(self):
        c = case(required=[M_SALES], acceptable=[M_SALES])
        r = score_case(c, calls([M_SALES, V_TOP]), KEY, "match")
        self.assertEqual(r["precision"], 0.5)
        self.assertEqual(r["noise"], [V_TOP])

    def test_required_is_always_acceptable_even_if_unlisted(self):
        c = {"qid": "q", "coverage": "covered",
             "expectedEntities": {"required": [M_SALES], "acceptable": []}}
        r = score_case(c, calls([M_SALES]), KEY, "match")
        self.assertEqual(r["precision"], 1.0)

    def test_returning_nothing_leaves_precision_undefined_not_zero(self):
        r = score_case(case(), calls([]), KEY, "no_match")
        self.assertIsNone(r["precision"])
        self.assertEqual(r["recall"], 0.0)


class Attribution(unittest.TestCase):
    def test_full_recall_and_a_wrong_answer_blames_construction(self):
        r = score_case(case(), calls([M_SALES]), KEY, "no_match")
        self.assertEqual((r["component"], r["owner"]),
                         ("construction", "agent-skill"))

    def test_a_missed_entity_that_exists_blames_retrieval(self):
        r = score_case(case(coverage="covered"), calls([]), KEY, "no_match")
        self.assertEqual((r["component"], r["owner"]),
                         ("get_context/retrieval", "retrieval"))

    def test_a_missed_entity_that_does_not_exist_blames_the_model(self):
        r = score_case(case(coverage="derivable"), calls([]), KEY, "no_match")
        self.assertEqual((r["component"], r["owner"]), ("get_context/model", "model"))

    def test_a_passing_attempt_is_attributed_to_nobody(self):
        for verdict in ("match", "near_match"):
            r = score_case(case(), calls([]), KEY, verdict)
            self.assertEqual(r["where_to_fix"], "",
                             f"{verdict} should not be a failure")

    def test_an_unscored_verdict_is_attributed_to_nobody(self):
        # needs_human and null are neither passes nor failures. Attributing them
        # would inflate whichever bucket they landed in.
        for verdict in (None, "needs_human"):
            r = score_case(case(), calls([]), KEY, verdict)
            self.assertEqual(r["where_to_fix"], "")
            self.assertFalse(r["failed"])

    def test_absent_coverage_is_excluded_from_recall_but_still_attributed(self):
        c = {"qid": "q", "coverage": "absent",
             "expectedEntities": {"required": [], "acceptable": []}}
        r = score_case(c, calls([M_SALES, D_STATUS]), KEY, "no_match")
        self.assertIsNone(r["recall"], "retrieval cannot fail with nothing to find")
        self.assertIsNone(r["precision"])
        self.assertEqual(r["where_to_fix"], "refusal behaviour")
        self.assertEqual((r["component"], r["owner"]),
                         ("construction", "agent-skill"))

    def test_a_passing_refusal_is_not_attributed(self):
        c = {"qid": "q", "coverage": "absent",
             "expectedEntities": {"required": [], "acceptable": []}}
        r = score_case(c, calls([]), KEY, "match")
        self.assertEqual(r["where_to_fix"], "")


class Summary(unittest.TestCase):
    def test_absent_cases_stay_out_of_the_means(self):
        rows = [
            score_case(case(qid="a"), calls([M_SALES], qid="a"),
                       ("a", None, "baseline"), "match"),
            score_case({"qid": "b", "coverage": "absent",
                        "expectedEntities": {"required": [], "acceptable": []}},
                       [], ("b", None, "baseline"), "match"),
        ]
        s = summarise(rows)
        self.assertEqual(s["attempts"], 2)
        self.assertEqual(s["retrieval_scored"], 1)
        self.assertEqual(s["mean_recall"], 1.0)

    def test_every_failure_is_attributed_somewhere(self):
        # The regression this exists for: absent-coverage failures counted in the
        # score table and appeared under no heading in the attribution table, so
        # 30 failures showed as 29 attributed.
        absent = {"qid": "d", "coverage": "absent",
                  "expectedEntities": {"required": [], "acceptable": []}}
        rows = [
            score_case(case(qid="a"), calls([M_SALES], qid="a"),
                       ("a", None, "baseline"), "no_match"),
            score_case(case(qid="b"), calls([], qid="b"),
                       ("b", None, "baseline"), "no_match"),
            score_case(case(qid="c", coverage="derivable"), calls([], qid="c"),
                       ("c", None, "baseline"), "no_match"),
            score_case(absent, calls([], qid="d"),
                       ("d", None, "baseline"), "no_match"),
            score_case(case(qid="e"), calls([M_SALES], qid="e"),
                       ("e", None, "baseline"), "match"),
            score_case(case(qid="f"), calls([], qid="f"),
                       ("f", None, "baseline"), "needs_human"),
        ]
        s = summarise(rows)
        self.assertEqual(s["failures"], 4)
        self.assertEqual(s["attributed"], s["failures"],
                         "a failure fell through attribute()")
        self.assertEqual(s["failures_by_where_to_fix"], {
            "query construction": 1, "retrieval ranking": 1,
            "model coverage": 1, "refusal behaviour": 1})


class EndToEnd(unittest.TestCase):
    def test_the_cli_reads_a_ledger_and_attributes_each_attempt(self):
        events = [
            {"kind": "attempt", "qid": "a", "sample": None, "phase": "baseline"},
            {"kind": "attempt", "qid": "b", "sample": None, "phase": "baseline"},
            {"kind": "tool_call", "tool": "get_context", "qid": "a", "sample": None,
             "phase": "baseline", "rankedSummary": {"entityIds": [M_SALES]}},
            {"kind": "tool_call", "tool": "get_context", "qid": "b", "sample": None,
             "phase": "baseline", "rankedSummary": {"entityIds": []}},
            {"kind": "score", "qid": "a", "sample": None, "phase": "baseline",
             "verdict": "no_match"},
            {"kind": "score", "qid": "b", "sample": None, "phase": "baseline",
             "verdict": "no_match"},
        ]
        cases = [case(qid="a"), case(qid="b", coverage="derivable")]
        with tempfile.TemporaryDirectory() as d:
            ep, cp = os.path.join(d, "e.jsonl"), os.path.join(d, "c.jsonl")
            for path, rows in ((ep, events), (cp, cases)):
                with open(path, "w") as fh:
                    for r in rows:
                        fh.write(json.dumps(r) + "\n")
            out = subprocess.run(
                [sys.executable, os.path.join(os.path.dirname(__file__),
                                              "score_retrieval.py"),
                 "--events", ep, "--cases", cp, "--json"],
                capture_output=True, text=True, check=True).stdout
        rows = [json.loads(l) for l in out.splitlines()]
        by = {r["qid"]: r for r in rows}
        self.assertEqual(by["a"]["owner"], "agent-skill",
                         "had everything and still failed")
        self.assertEqual(by["b"]["owner"], "model",
                         "nothing to retrieve, so not retrieval's fault")


if __name__ == "__main__":
    unittest.main(verbosity=2)
