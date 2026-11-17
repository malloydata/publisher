#!/usr/bin/env python3
"""Tests for target_outcomes.py: which entities a target returned, and the four per-target measures."""
import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import target_outcomes as to  # noqa: E402


def hit(eid, *matches, relevance=None):
    """A returned entity. `matches` are (search_text, relevance) pairs; none means unattributed."""
    return {"entity_id": eid, "relevance": relevance,
            "matched_targets": [{"search_text": t, "relevance": r} for t, r in matches] if matches else None}


def call(*hits):
    return {"rankedSummary": {"hits": list(hits), "entityIds": [h["entity_id"] for h in hits]}}


def target(kind, text, **exp):
    return {"target_type": kind, "search_text": text, "expectedEntities": exp}


class Ranked(unittest.TestCase):
    def test_a_target_gets_only_the_hits_that_name_its_text_and_are_of_its_kind(self):
        c = call(hit("measure:o:sales", ("revenue", 0.9)), hit("measure:o:margin", ("margin", 0.9)),
                 hit("dimension:o:revenue_band", ("revenue", 0.8)))
        got, rule = to.ranked(target("measure", "revenue"), c)
        self.assertEqual(([e for e, _ in got], rule), (["measure:o:sales"], "server"))

    def test_the_best_first_order_is_the_targets_own_relevance(self):
        c = call(hit("measure:o:a", ("revenue", 0.6)), hit("measure:o:b", ("revenue", 0.94)))
        self.assertEqual([e for e, _ in to.ranked(target("measure", "revenue"), c)[0]],
                         ["measure:o:b", "measure:o:a"])

    def test_a_value_target_is_answered_by_a_dimension(self):
        c = call(hit("dimension:o:status", ("cancelled", 0.94)))
        self.assertEqual([e for e, _ in to.ranked(target("dimensional_value", "cancelled"), c)[0]],
                         ["dimension:o:status"])

    def test_an_attributed_source_card_belongs_only_to_the_source_target_it_names(self):
        c = call(hit("source:o:o", ("orders", 0.9)), hit("source:c:c", ("customers", 0.9)),
                 hit("measure:o:sales", ("revenue", 0.9)))
        self.assertEqual([e for e, _ in to.ranked(target("source", "orders"), c)[0]], ["source:o:o"])

    def test_an_unattributed_card_on_an_attributed_response_goes_to_every_source_target(self):
        c = call(hit("source:o:o"), hit("measure:o:sales", ("revenue", 0.9)))
        self.assertEqual([e for e, _ in to.ranked(target("source", "orders"), c)[0]], ["source:o:o"])

    def test_with_no_attribution_a_target_gets_every_entity_of_its_kind(self):
        c = call(hit("measure:o:sales"), hit("measure:o:margin"), hit("dimension:o:state"))
        got, rule = to.ranked(target("measure", "revenue"), c)
        self.assertEqual((len(got), rule), (2, "kind"))


class Outcome(unittest.TestCase):
    def test_recall_counts_each_required_group_and_only_exact_ids_from_the_targets_own_hits(self):
        t = target("dimension", "customer", required=["dimension:c:id"],
                   requiredAnyOf=[["dimension:c:name", "dimension:o:c.name"]])
        c = call(hit("dimension:o:c.name", ("customer", 0.9)), hit("dimension:c:id", ("other", 0.9)))
        row = to.outcome(1, t, c)
        self.assertEqual((row["n_required"], row["n_found"], row["n_found_anywhere"]), (2, 1, 2))
        self.assertEqual(row["missing"], ["dimension:c:id"])

    def test_acceptable_fields_are_not_noise(self):
        t = target("measure", "revenue", required=["measure:o:sales"], acceptable=["measure:o:gross"])
        c = call(hit("measure:o:sales", ("revenue", 0.9)), hit("measure:o:gross", ("revenue", 0.8)),
                 hit("measure:o:count", ("revenue", 0.7)))
        row = to.outcome(1, t, c)
        self.assertEqual((row["n_ok"], row["n_returned"], row["noise"]), (2, 3, ["measure:o:count"]))

    def test_best_pick_is_right_when_any_entity_tied_at_the_top_is_required(self):
        t = target("measure", "revenue", required=["measure:o:sales"])
        tied = call(hit("measure:o:count", ("revenue", 0.94)), hit("measure:o:sales", ("revenue", 0.94)))
        below = call(hit("measure:o:count", ("revenue", 0.94)), hit("measure:o:sales", ("revenue", 0.9)))
        self.assertTrue(to.outcome(1, t, tied)["best_pick_ok"])
        self.assertFalse(to.outcome(1, t, below)["best_pick_ok"])

    def test_a_required_target_that_returned_nothing_has_a_wrong_best_pick(self):
        row = to.outcome(1, target("measure", "revenue", required=["measure:o:sales"]), call())
        self.assertEqual((row["best_pick_ok"], row["recall"], row["precision"]), (False, 0.0, None))

    def test_an_absent_target_is_right_when_nothing_or_only_the_closest_field_came_back(self):
        t = target("measure", "net revenue", absent=True, closest=["measure:o:gross"])
        self.assertTrue(to.outcome(1, t, call())["not_found_ok"])
        self.assertTrue(to.outcome(1, t, call(hit("measure:o:gross", ("net revenue", 0.6))))["not_found_ok"])
        wrong = to.outcome(1, t, call(hit("measure:o:count", ("net revenue", 0.6))))
        self.assertEqual((wrong["not_found_ok"], wrong["n_ok"], wrong["best_pick_ok"]), (False, 0, None))

    def test_a_target_with_no_label_is_counted_and_not_scored(self):
        row = to.outcome(1, {"target_type": "measure", "search_text": "x"},
                         call(hit("measure:o:a", ("x", 0.9))))
        self.assertEqual((row["labelled"], row["n_ok"], row["not_found_ok"]), (False, None, None))


class Pool(unittest.TestCase):
    def test_ratios_of_sums_not_means_of_ratios(self):
        big = target("measure", "revenue", required=["measure:o:sales"])
        small = target("dimension", "state", required=["dimension:c:state"])
        gone = target("measure", "tax", absent=True)
        c = call(hit("measure:o:sales", ("revenue", 0.94)),
                 *[hit(f"measure:o:m{i}", ("revenue", 0.5)) for i in range(9)],
                 hit("dimension:c:state", ("state", 0.94)))
        rows = [to.outcome(i, t, c) for i, t in enumerate([big, small, gone, {"target_type": "view"}], 1)]
        p = to.pool(rows)
        self.assertEqual((p["targets"], p["labelled"], p["required_answers"], p["returned"]), (4, 3, 2, 11))
        self.assertEqual(p["recall"], 1.0)
        self.assertAlmostEqual(p["precision"], 2 / 11)
        self.assertEqual((p["best_pick"], p["best_pick_targets"]), (1.0, 2))
        self.assertEqual((p["not_found"], p["not_found_targets"]), (1.0, 1))

    def test_nothing_labelled_pools_to_none(self):
        p = to.pool([])
        self.assertEqual((p["recall"], p["precision"], p["best_pick"], p["not_found"]), (None,) * 4)


if __name__ == "__main__":
    unittest.main()
