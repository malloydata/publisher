#!/usr/bin/env python3
"""Tests for score_targets. Stdlib only: python score_targets_test.py

The fixtures use the storefront example's entity ids, so a reader can check a
pairing against examples/storefront/storefront.malloy.
"""
from __future__ import annotations

import io
import json
import os
import sys
import tempfile
import unittest
from contextlib import redirect_stdout

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import score_targets  # noqa: E402
from score_targets import score, score_attempt, summarise, words  # noqa: E402

SALES = "measure:order_items:total_sales"
CATEGORY = "dimension:order_items:category"
P_CATEGORY = "dimension:products:category"
BRAND = "dimension:order_items:brand"
ORDERS = "measure:order_items:order_count"
SRC = "source:order_items:order_items"
KEY = ("q", None, "baseline")


def case(*targets):
    return {"qid": "q", "searchTargets": list(targets)}


def target(kind, text, required=(), any_of=(), acceptable=(), values=None):
    t = {"target_type": kind, "expectedEntities": {
        "required": list(required),
        "requiredAnyOf": [list(g) for g in any_of],
        "acceptable": list(acceptable)}}
    if text is not None:
        t["search_text"] = text
    if values is not None:
        t["example_values"] = list(values)
    return t


def hit(eid, *texts):
    """One hit. `texts` are the search texts the server attributed it to;
    none at all means no attribution (a source card, or a lexical run)."""
    return {"entity_id": eid,
            "matched_targets": [{"search_text": t, "relevance": 0.8}
                                for t in texts] if texts else None}


def call(sent, hits, tokens=()):
    return {"kind": "tool_call", "tool": "get_context", "qid": "q",
            "sample": None, "phase": "baseline",
            "search_targets": [dict(zip(("target_type", "search_text"), s))
                               for s in sent],
            "rankedSummary": {"entityIds": [h["entity_id"] for h in hits],
                              "hits": hits, "docTokens": list(tokens)}}


class Words(unittest.TestCase):
    def test_plurals_and_stop_words_fall_away(self):
        self.assertEqual(words("the categories of products"),
                         {"category", "product"})

    def test_a_concept_word_like_total_stays(self):
        self.assertIn("total", words("total sales"))


class Decomposition(unittest.TestCase):
    """Question 1: did the agent ask for the right things?"""

    def test_a_shared_word_and_the_same_type_covers_a_golden_target(self):
        r = score_attempt(case(target("measure", "revenue", [SALES])),
                          [call([("measure", "total revenue")],
                                [hit(SALES, "total revenue")])], KEY, "match")
        t = r["targets"][0]
        self.assertTrue(t["covered"])
        self.assertEqual(t["covered_by"][0]["route"], "words")

    def test_a_synonym_is_caught_by_what_it_returned(self):
        # "sales" shares no word with "revenue"; it returned the golden's
        # entity, so it searched for the same thing.
        r = score_attempt(case(target("measure", "revenue", [SALES])),
                          [call([("measure", "sales")], [hit(SALES, "sales")])],
                          KEY, "match")
        self.assertEqual(r["targets"][0]["covered_by"][0]["route"], "entities")

    def test_the_right_words_under_the_wrong_type_is_a_type_mismatch(self):
        r = score_attempt(case(target("dimension", "category", [CATEGORY])),
                          [call([("measure", "sales by category")],
                                [hit(SALES, "sales by category")])], KEY, "match")
        t = r["targets"][0]
        self.assertFalse(t["covered"])
        self.assertEqual(t["wrong_type"][0]["target_type"], "measure")
        self.assertEqual(r["n_wrong_type"], 1)
        # A mismatched target is accounted for, so it is not also an extra.
        self.assertEqual(r["extras"], [])

    def test_a_target_matching_no_golden_one_is_an_extra(self):
        r = score_attempt(case(target("measure", "revenue", [SALES])),
                          [call([("measure", "revenue"), ("source", "orders")],
                                [hit(SALES, "revenue"), hit(SRC)])], KEY, "match")
        self.assertEqual([(x["target_type"], x["search_text"])
                          for x in r["extras"]], [("source", "orders")])

    def test_the_same_extra_sent_twice_is_counted_once(self):
        calls = [call([("source", "orders")], [hit(SRC)]),
                 call([("source", "orders")], [hit(SRC)])]
        r = score_attempt(case(target("measure", "revenue", [SALES])), calls,
                          KEY, None)
        self.assertEqual(len(r["extras"]), 1)

    def test_a_golden_example_value_the_agent_left_out_is_named(self):
        g = target("dimension", "product category", [CATEGORY],
                   values=["Outerwear"])
        sent = call([("dimension", "category")], [hit(CATEGORY, "category")])
        self.assertEqual(score_attempt(case(g), [sent], KEY, "match")
                         ["targets"][0]["values_missing"], ["Outerwear"])
        sent["search_targets"][0]["example_values"] = ["outerwear"]
        self.assertEqual(score_attempt(case(g), [sent], KEY, "match")
                         ["targets"][0]["values_missing"], [])


class Retrieval(unittest.TestCase):
    """Question 2: did the search return the right entities?"""

    def test_recall_counts_only_what_the_paired_target_returned(self):
        # The measure target returned nothing useful; the category arrived
        # under a different target. Per-target recall says the measure
        # search failed, and `recall_anywhere` says the entity arrived anyway.
        g = case(target("measure", "revenue", [SALES]),
                 target("dimension", "category", [CATEGORY]))
        sent = call([("measure", "revenue"), ("dimension", "category")],
                    [hit(ORDERS, "revenue"), hit(CATEGORY, "category"),
                     hit(SALES, "category")])
        r = score_attempt(g, [sent], KEY, "no_match")
        m, d = r["targets"]
        self.assertEqual(m["recall"], 0.0)
        self.assertEqual(m["missing"], [SALES])
        self.assertEqual(m["recall_anywhere"], 1.0)
        self.assertEqual(d["recall"], 1.0)
        self.assertEqual(r["recall"], 1.0)

    def test_an_any_of_group_is_satisfied_by_one_member(self):
        g = case(target("dimension", "category",
                        any_of=[(CATEGORY, P_CATEGORY)]))
        r = score_attempt(g, [call([("dimension", "category")],
                                   [hit(P_CATEGORY, "category")])], KEY, "match")
        self.assertEqual(r["targets"][0]["recall"], 1.0)
        self.assertEqual(r["targets"][0]["precision"], 1.0)

    def test_precision_counts_acceptable_ids_as_signal(self):
        g = case(target("dimension", "category", [CATEGORY],
                        acceptable=[P_CATEGORY]))
        r = score_attempt(g, [call([("dimension", "category")],
                                   [hit(CATEGORY, "category"),
                                    hit(P_CATEGORY, "category"),
                                    hit(BRAND, "category")])], KEY, "match")
        self.assertAlmostEqual(r["targets"][0]["precision"], 2 / 3)

    def test_a_target_never_asked_for_has_no_retrieval_number(self):
        g = case(target("dimension", "brand", [BRAND]))
        r = score_attempt(g, [call([("measure", "revenue")],
                                   [hit(SALES, "revenue")])], KEY, "no_match")
        t = r["targets"][0]
        self.assertIsNone(t["recall"])
        self.assertIsNone(t["precision"])
        self.assertEqual(t["recall_anywhere"], 0.0)

    def test_with_no_attribution_a_target_is_credited_by_kind(self):
        # A lexical run publishes no matched_targets. The dimension target
        # gets every dimension the call returned, and none of its measures.
        g = case(target("dimension", "category", [CATEGORY]))
        r = score_attempt(g, [call([("dimension", "category"),
                                    ("measure", "revenue")],
                                   [hit(CATEGORY), hit(SALES)])], KEY, "match")
        t = r["targets"][0]
        self.assertEqual(t["attribution"], "kind")
        self.assertEqual(t["n_returned"], 1)
        self.assertEqual(t["recall"], 1.0)

    def test_a_source_target_is_credited_with_the_source_cards(self):
        g = case(target("source", "order items", [SRC]))
        r = score_attempt(g, [call([("source", "order items"),
                                    ("measure", "revenue")],
                                   [hit(SRC), hit(SALES, "revenue")])],
                          KEY, "match")
        self.assertEqual(r["targets"][0]["recall"], 1.0)
        self.assertEqual(r["targets"][0]["precision"], 1.0)


class OlderRuns(unittest.TestCase):
    def test_a_run_without_search_targets_is_read_from_targets(self):
        c = call([], [hit(SALES, "revenue")])
        del c["search_targets"]
        c["targets"] = ["measure: revenue"]
        c["target_shapes"] = [{"type": "measure", "has_text": True},
                              {"type": "source", "has_text": False}]
        r = score_attempt(case(target("measure", "revenue", [SALES])), [c],
                          KEY, "match")
        self.assertEqual(r["request_field"], "targets")
        self.assertTrue(r["targets"][0]["covered"])
        self.assertEqual([x["target_type"] for x in r["extras"]], ["source"])


class TheAnswer(unittest.TestCase):
    """Question 3, unchanged: the judge's verdict, split by decomposition."""

    def test_the_summary_splits_passes_by_whether_every_target_was_asked(self):
        g = case(target("measure", "revenue", [SALES]),
                 target("dimension", "category", [CATEGORY]))
        good = score_attempt(g, [call([("measure", "revenue"),
                                       ("dimension", "category")],
                                      [hit(SALES, "revenue"),
                                       hit(CATEGORY, "category")])],
                             ("q", 1, "baseline"), "match")
        bad = score_attempt(g, [call([("measure", "revenue")],
                                     [hit(SALES, "revenue")])],
                            ("q", 2, "baseline"), "no_match")
        s = summarise([good, bad])
        self.assertEqual((s["golden_targets"], s["covered"], s["never_asked"]),
                         (4, 3, 1))
        self.assertEqual((s["passed"], s["decided"]), (1, 2))
        self.assertEqual((s["passed_full_cover"], s["decided_full_cover"]),
                         (1, 1))
        self.assertEqual((s["passed_part_cover"], s["decided_part_cover"]),
                         (0, 1))

    def test_near_match_is_not_decided(self):
        r = score_attempt(case(target("measure", "revenue", [SALES])), [],
                          KEY, "near_match")
        self.assertEqual(summarise([r])["decided"], 0)


class EndToEnd(unittest.TestCase):
    def test_a_case_without_search_targets_is_skipped(self):
        events = [{"kind": "attempt", "qid": "old", "sample": None,
                   "phase": "baseline"}]
        self.assertEqual(score(events, {"old": {"qid": "old"}}), [])

    def test_the_cli_reads_a_ledger_and_prints_all_three_parts(self):
        g = case(target("measure", "revenue", [SALES]))
        events = [{"kind": "attempt", "qid": "q", "sample": None,
                   "phase": "baseline"},
                  call([("measure", "revenue")], [hit(SALES, "revenue")]),
                  {"kind": "score", "qid": "q", "sample": None,
                   "phase": "baseline", "verdict": "match"}]
        with tempfile.TemporaryDirectory() as d:
            ev, cs = os.path.join(d, "events.jsonl"), os.path.join(d, "cases.jsonl")
            with open(ev, "w") as fh:
                fh.write("".join(json.dumps(e) + "\n" for e in events))
            with open(cs, "w") as fh:
                fh.write(json.dumps(g) + "\n")
            out = io.StringIO()
            with redirect_stdout(out):
                self.assertEqual(score_targets.main(
                    ["--events", ev, "--cases", cs]), 0)
        text = out.getvalue()
        self.assertIn("1. asked?     1 of 1 golden targets asked for", text)
        self.assertIn("2. returned?  over the 1 asked targets with a key: "
                      "recall 100%, precision 100%", text)
        self.assertIn("3. correct?   1 of 1 decided", text)


if __name__ == "__main__":
    unittest.main()
