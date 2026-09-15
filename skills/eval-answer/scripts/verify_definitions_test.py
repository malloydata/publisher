#!/usr/bin/env python3
"""Tests for verify_definitions.py. Stdlib only: python3 verify_definitions_test.py

The thing these guard is not "does it compute a number" but "does it ever claim
to have validated something it did not". Every `unchecked` path below is a case
where saying `agrees` would be the tool committing the error it exists to find.
"""
from __future__ import annotations

import argparse
import json
import os
import pathlib
import shutil
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import verify_definitions as vd  # noqa: E402

MODEL = """\
source: order_items is duckdb.table('data/order_items.parquet') extend {
  join_one: inventory_items is inventory_items on inventory_item_id = inventory_items.id
  measure:
    total_sales is sale_price.sum()
    order_item_count is count()
    total_sales_2022 is total_sales { where: year(created_at) = 2022 }
    total_cost_through_join is inventory_items.cost.sum()
    worst_margin is inventory_items.cost.min()
  dimension:
    order_year is year(created_at)
}
"""


def write(text: str) -> pathlib.Path:
    d = pathlib.Path(tempfile.mkdtemp())
    p = d / "m.malloy"
    p.write_text(text)
    return p


def by_name(recs):
    return {r["name"]: r for r in recs}


class TheLedgerItBuilds(unittest.TestCase):
    def setUp(self):
        self.model = write(MODEL)
        self.recs = by_name(vd.records(self.model, recursive=False))

    def tearDown(self):
        shutil.rmtree(self.model.parent, ignore_errors=True)

    def test_a_measure_is_source_qualified(self):
        # The flat `dict[name]` this replaced resolved a same-named field in
        # two sources to whichever file was read first.
        self.assertEqual(self.recs["total_sales"]["entityId"],
                         "measure:order_items:total_sales")

    def test_a_definition_records_what_it_builds_on(self):
        self.assertIn("measure:order_items:total_sales",
                      self.recs["total_sales_2022"]["depends"])

    def test_a_leaf_definition_depends_on_nothing_defined(self):
        self.assertEqual(self.recs["total_sales"]["depends"], [])

    def test_views_and_joins_are_not_checkable_definitions(self):
        # A view composes measures that are checked on their own; a join is
        # structure rather than a value.
        self.assertNotIn("inventory_items", self.recs)


class TheShaChainInvalidatesDownstream(unittest.TestCase):
    """A measure whose own text never moved is still stale when something it
    builds on does. A flat hash of the one line reports it as current, which is
    how a ledger goes quietly wrong."""

    def shas(self, text):
        p = write(text)
        try:
            return {r["name"]: r["exprSha"] for r in vd.records(p, False)}
        finally:
            shutil.rmtree(p.parent, ignore_errors=True)

    def test_editing_a_dependency_moves_its_dependant(self):
        before = self.shas(MODEL)
        after = self.shas(MODEL.replace("total_sales is sale_price.sum()",
                                        "total_sales is sale_price.sum() * 2"))
        self.assertNotEqual(before["total_sales"], after["total_sales"])
        # The point: this one's own text is byte-identical across the two.
        self.assertNotEqual(before["total_sales_2022"], after["total_sales_2022"])

    def test_an_unrelated_definition_is_not_disturbed(self):
        before = self.shas(MODEL)
        after = self.shas(MODEL.replace("total_sales is sale_price.sum()",
                                        "total_sales is sale_price.sum() * 2"))
        self.assertEqual(before["order_item_count"], after["order_item_count"])


class WhatItRefusesToCheck(unittest.TestCase):
    def setUp(self):
        self.model = write(MODEL)
        self.recs = by_name(vd.records(self.model, recursive=False))

    def tearDown(self):
        shutil.rmtree(self.model.parent, ignore_errors=True)

    def test_a_join_crossing_measure_is_held_back_for_a_raw_check(self):
        # Fanout inflates the measure and the control expression equally, so a
        # within-model comparison stays green on a genuinely broken join.
        r = self.recs["total_cost_through_join"]
        self.assertEqual(r["check"]["kind"], "raw")
        self.assertIn("inventory_items", r["needs"])

    def test_a_method_call_on_a_column_is_not_a_join(self):
        # The regression: matching any `word.word` read `sale_price.sum()` as a
        # join hop and held back the measure twelve of the set's cases use.
        self.assertEqual(self.recs["total_sales"]["check"]["kind"], "within_model")

    def test_a_fanout_safe_aggregate_across_a_join_is_still_checkable(self):
        # MIN survives uniform duplication, so fanout cannot move it.
        self.assertEqual(self.recs["worst_margin"]["check"]["kind"], "within_model")

    def test_a_multi_line_definition_is_unreadable_not_checked(self):
        p = write("source: s is t extend {\n  dimension:\n    full_name is concat(\n")
        try:
            r = by_name(vd.records(p, False))["full_name"]
            self.assertEqual(r["check"]["kind"], "unreadable")
            self.assertIn("more than one line", r["needs"])
        finally:
            shutil.rmtree(p.parent, ignore_errors=True)

    def test_incomplete_spots_every_unbalanced_delimiter(self):
        for expr in ("concat(", "x { where: y", "arr[0"):
            self.assertTrue(vd.incomplete(expr), expr)
        for expr in ("sale_price.sum()", "count() { where: s != 'C' }"):
            self.assertFalse(vd.incomplete(expr), expr)


class RunningOneCheck(unittest.TestCase):
    def ns(self):
        return argparse.Namespace(publisher="http://t", environment="e",
                                  package="p", model_path="m.malloy")

    def rec(self, **kw):
        base = {"kind": "measure", "source": "order_items", "name": "total_sales",
                "expr": "sale_price.sum()", "check": {"kind": "within_model"},
                "needs": None}
        base.update(kw)
        return base

    def check(self, rows, err=None, **kw):
        with mock.patch.object(vd, "try_query", lambda *a, **k: (rows, err)):
            return vd.run_check(self.rec(**kw), self.ns())

    def test_matching_values_agree(self):
        v, _ = self.check([{"stated": 42.0, "control": 42.0}])
        self.assertEqual(v, "agrees")

    def test_a_different_value_disagrees_and_names_both(self):
        v, detail = self.check([{"stated": 84.0, "control": 42.0}])
        self.assertEqual(v, "disagrees")
        self.assertIn("84", detail)
        self.assertIn("42", detail)

    def test_float_association_is_not_a_disagreement(self):
        # Two aggregation orders differ in the last places; verify_goldens
        # tolerates the same.
        v, _ = self.check([{"stated": 5788744.332576918,
                            "control": 5788744.3325769575}])
        self.assertEqual(v, "agrees")

    def test_a_failed_query_is_unchecked_not_a_disagreement(self):
        # A check that could not run is not a check that found a defect.
        v, detail = self.check([], err="HTTP 400: nope")
        self.assertEqual(v, "unchecked")
        self.assertIn("query failed", detail)

    def test_no_rows_is_unchecked(self):
        v, _ = self.check([])
        self.assertEqual(v, "unchecked")

    def test_a_raw_check_is_never_run_here(self):
        v, _ = self.check([{"stated": 1, "control": 2}],
                          check={"kind": "raw"}, needs="crosses a join")
        self.assertEqual(v, "unchecked")

    def test_a_dimension_compares_every_sampled_row(self):
        rows = [{"stated": i, "control": i} for i in range(5)]
        rows[3]["control"] = 99
        v, detail = self.check(rows, kind="dimension", name="order_year",
                               expr="year(created_at)")
        self.assertEqual(v, "disagrees")
        self.assertIn("row 4", detail)

    def test_an_all_null_slice_is_unchecked_not_agreed(self):
        # Every sampled row null on both sides proves nothing about the
        # definition; calling it a pass is the over-claim this file guards.
        v, detail = self.check([{"stated": None, "control": None}] * 3,
                               kind="dimension")
        self.assertEqual(v, "unchecked")
        self.assertIn("null", detail)

    def test_a_passing_dimension_records_the_slice_it_used(self):
        rec = self.rec(kind="dimension")
        with mock.patch.object(vd, "try_query",
                               lambda *a, **k: ([{"stated": 1, "control": 1}] * 7, None)):
            v, _ = vd.run_check(rec, self.ns())
        self.assertEqual(v, "agrees")
        self.assertIn("slice", rec["check"])


class TheQueryItBuilds(unittest.TestCase):
    def test_a_measure_aggregates(self):
        q = vd.within_model_query({"kind": "measure", "source": "s",
                                   "name": "m", "expr": "x.sum()"})
        self.assertIn("aggregate:", q)
        self.assertNotIn("limit:", q)

    def test_a_dimension_selects_a_slice(self):
        # `aggregate:` rejects a scalar outright ("Cannot use a scalar field in
        # an aggregate"), which is how this was found.
        q = vd.within_model_query({"kind": "dimension", "source": "s",
                                   "name": "d", "expr": "year(t)"})
        self.assertIn("select:", q)
        self.assertIn(f"limit: {vd.DIM_SLICE}", q)

    def test_both_sides_come_from_one_query(self):
        # Two queries could see different rows, and the difference would read
        # as a bad definition rather than a moving population.
        q = vd.within_model_query({"kind": "measure", "source": "s",
                                   "name": "m", "expr": "x.sum()"})
        self.assertEqual(q.count("run:"), 1)


class TheExitContract(unittest.TestCase):
    """Matches verify_goldens: 0 clean, 1 a finding, 3 could not run."""

    def setUp(self):
        self.model = write(MODEL)

    def tearDown(self):
        shutil.rmtree(self.model.parent, ignore_errors=True)

    def run_main(self, rows, extra=()):
        argv = ["--model", str(self.model), "--publisher", "http://t",
                "--package", "p", "--model-path", "m.malloy", "--quiet", *extra]
        with mock.patch.object(vd, "try_query", lambda *a, **k: (rows, None)):
            return vd.main(argv)

    def test_a_disagreement_exits_1(self):
        self.assertEqual(self.run_main([{"stated": 1, "control": 2}]), 1)

    def test_agreement_exits_0(self):
        self.assertEqual(self.run_main([{"stated": 1, "control": 1}]), 0)

    def test_building_without_a_server_is_could_not_run_not_a_pass(self):
        # The whole point of 3: a ledger built and never checked must not read
        # as validation.
        self.assertEqual(vd.main(["--model", str(self.model), "--quiet"]),
                         vd.CANNOT_RUN)

    def test_a_missing_model_is_could_not_run(self):
        self.assertEqual(vd.main(["--model", "/nope/absent.malloy", "--quiet"]),
                         vd.CANNOT_RUN)

    def test_a_publisher_without_an_address_is_could_not_run(self):
        self.assertEqual(vd.main(["--model", str(self.model),
                                  "--publisher", "http://t", "--quiet"]),
                         vd.CANNOT_RUN)

    def test_the_ledger_is_written_as_jsonl(self):
        out = self.model.parent / "led.jsonl"
        self.run_main([{"stated": 1, "control": 1}], extra=["--out", str(out)])
        rows = [json.loads(l) for l in out.read_text().splitlines() if l.strip()]
        self.assertTrue(rows)
        self.assertTrue(all("entityId" in r and "exprSha" in r for r in rows))


class TheSummary(unittest.TestCase):
    def test_it_says_how_many_are_not_validated_by_this_run(self):
        recs = [{"verdict": "agrees", "check": {"kind": "within_model"}},
                {"verdict": "unchecked", "check": {"kind": "raw"}}]
        text = " ".join(vd.summarise(recs))
        self.assertIn("1 agrees", text)
        self.assertIn("NOT validated", text)


if __name__ == "__main__":
    unittest.main(verbosity=1)
