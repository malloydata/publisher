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


TWO_SOURCES = """\
source: source_a is duckdb.table('data/a.parquet') extend {
  measure:
    total_sales is sale_price.sum()
}
source: source_b is duckdb.table('data/b.parquet') extend {
  measure:
    total_sales is price.sum()
    doubled is total_sales * 2
    borrowed is source_a.total_sales * 3
}
"""


class DependenciesResolveInTheSourceThatOwnsThem(unittest.TestCase):
    """A bare word names a field of the DECLARING source.

    Resolving it against a flat `dict[name]` linked `source_b.doubled` to
    `source_a.total_sales` on a model where the two sources both declare the
    name -- and, worse, on one where only the OTHER source declares it, giving
    a dependant a dependency its own source does not have. Either way the sha
    chain hashes the wrong expression, so `stale_ids` marks the wrong rows.
    """

    def setUp(self):
        self.model = write(TWO_SOURCES)
        self.recs = {r["entityId"]: r
                     for r in vd.records(self.model, recursive=False)}

    def tearDown(self):
        shutil.rmtree(self.model.parent, ignore_errors=True)

    def test_a_bare_word_resolves_to_its_own_sources_field(self):
        self.assertEqual(self.recs["measure:source_b:doubled"]["depends"],
                         ["measure:source_b:total_sales"])

    def test_a_dotted_path_resolves_in_the_source_it_names(self):
        self.assertEqual(self.recs["measure:source_b:borrowed"]["depends"],
                         ["measure:source_a:total_sales"])

    def test_a_name_only_the_other_source_declares_is_not_a_dependency(self):
        p = write(
            "source: source_a is duckdb.table('data/a.parquet') extend {\n"
            "  measure:\n    total_sales is sale_price.sum()\n}\n"
            "source: source_b is duckdb.table('data/b.parquet') extend {\n"
            "  dimension:\n    doubled is total_sales * 2\n}\n")
        try:
            recs = {r["entityId"]: r for r in vd.records(p, recursive=False)}
            self.assertEqual(recs["dimension:source_b:doubled"]["depends"], [])
        finally:
            shutil.rmtree(p.parent, ignore_errors=True)

    def test_a_join_alias_resolves_to_the_source_it_targets(self):
        """`join_one: destination is airports` declares `destination`, so
        `destination.airport_count` is a field of `airports`.

        Scoping bare words to the declaring source is right; treating a join
        ALIAS as if it were a source name is not, and it dropped the edge
        outright. Measured on faa: `flights.destination_count` came back
        depending on nothing, so an edit to `airports.airport_count` would not
        have moved its sha. The join is a dependency too, because its `on`
        clause decides which rows the field sees.
        """
        p = write(
            "source: airports is duckdb.table('data/a.parquet') extend {\n"
            "  measure:\n    airport_count is count()\n}\n"
            "source: flights is duckdb.table('data/f.parquet') extend {\n"
            "  join_one: destination is airports with destination_code\n"
            "  measure:\n    destination_count is destination.airport_count\n}\n")
        try:
            recs = {r["entityId"]: r for r in vd.records(p, recursive=False)}
            self.assertEqual(
                recs["measure:flights:destination_count"]["depends"],
                ["measure:airports:airport_count", "join:flights:destination"])
        finally:
            shutil.rmtree(p.parent, ignore_errors=True)

    def test_the_two_same_named_measures_hash_apart(self):
        a = self.recs["measure:source_a:total_sales"]["exprSha"]
        b = self.recs["measure:source_b:total_sales"]["exprSha"]
        self.assertNotEqual(a, b)


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

    def test_ledger_mode_reads_merges_checks_and_writes_back(self):
        led = self.model.parent / "led.jsonl"
        vd.main(["--model", str(self.model), "--out", str(led), "--quiet"])
        rows = [json.loads(l) for l in led.read_text().splitlines()]
        for r in rows:
            if r["name"] == "total_cost_through_join":
                r["check"]["query"] = "run: x -> { aggregate: control is 1 }"
        led.write_text("".join(json.dumps(r) + "\n" for r in rows))
        with mock.patch.object(vd, "try_query",
                               lambda *a, **k: ([{"stated": 1, "control": 1}], None)):
            code = vd.main(["--model", str(self.model), "--ledger", str(led),
                            "--publisher", "http://t", "--package", "p",
                            "--model-path", "m.malloy", "--quiet"])
        self.assertEqual(code, 0)
        back = {r["name"]: r for r in
                (json.loads(l) for l in led.read_text().splitlines())}
        self.assertEqual(back["total_cost_through_join"]["check"]["query"],
                         "run: x -> { aggregate: control is 1 }")
        self.assertEqual(back["total_cost_through_join"]["verdict"], "agrees")

    def test_a_missing_ledger_is_could_not_run(self):
        self.assertEqual(vd.main(["--model", str(self.model),
                                  "--ledger", "/nope/led.jsonl", "--quiet"]),
                         vd.CANNOT_RUN)

    def test_the_ledger_is_written_as_jsonl(self):
        out = self.model.parent / "led.jsonl"
        self.run_main([{"stated": 1, "control": 1}], extra=["--out", str(out)])
        rows = [json.loads(l) for l in out.read_text().splitlines() if l.strip()]
        self.assertTrue(rows)
        self.assertTrue(all("entityId" in r and "exprSha" in r for r in rows))


class WhatTheScoreRestsOn(unittest.TestCase):
    """The composition rule, per case: a golden is trustworthy if it was derived
    independently, OR if every definition it tests has been validated."""

    def ledger(self, **verdicts):
        return {eid: {"entityId": eid, "verdict": v, "exprSha": "s"}
                for eid, v in verdicts.items()}

    def case(self, ids=("measure:s:m",), **golden):
        g = {"status": "verified", "kind": "scalar", "value": 1}
        g.update(golden)
        return {"qid": "q", "golden": g,
                "expectedEntities": {"required": list(ids)}}

    def setUp(self):
        self.set_dir = pathlib.Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.set_dir, ignore_errors=True)

    def basis(self, case, ledger=None, stale=()):
        return vd.case_basis(case, ledger or {}, set(stale), self.set_dir)

    def test_a_second_derivation_makes_a_key_independent(self):
        # It was established twice, in differently shaped ways, so no single
        # model bug could have certified it.
        c = self.case()
        c["golden"]["verification"] = {"primaryAxis": "a", "variesAxis": "b"}
        self.assertEqual(self.basis(c), "independent")

    def test_free_text_verifiedby_is_not_the_signal(self):
        # The regression: matching `verifiedBy` by prefix classified 34 goldens
        # of the ecommerce set as unchecked, every one of which reads "authored
        # and re-derived against ecommerce-truth". A well-founded set reading as
        # unvalidated is the same over-claim, pointed the other way.
        c = self.case(verifiedBy="authored and re-derived against ecommerce-truth")
        self.assertEqual(self.basis(c, self.ledger()), "unchecked")
        c["golden"]["verification"] = {"primaryAxis": "a", "variesAxis": "b"}
        self.assertEqual(self.basis(c, self.ledger()), "independent")

    def test_a_value_free_golden_is_independent(self):
        # Nothing to re-derive, so nothing a model bug could get wrong.
        for kind in ("criteria", "unanswerable"):
            self.assertEqual(self.basis(self.case(kind=kind)), "independent")

    def test_validated_definitions_carry_a_model_derived_key(self):
        self.assertEqual(
            self.basis(self.case(verifiedBy="something else"),
                       self.ledger(**{"measure:s:m": "agrees"})),
            "definitions")

    def test_one_unchecked_definition_makes_the_whole_case_unchecked(self):
        # The weakest link decides. A case is only as established as the least
        # established thing it depends on.
        led = self.ledger(**{"measure:s:m": "agrees", "measure:s:n": "unchecked"})
        self.assertEqual(
            self.basis(self.case(ids=("measure:s:m", "measure:s:n")), led),
            "unchecked")

    def test_a_definition_missing_from_the_ledger_is_unchecked(self):
        self.assertEqual(self.basis(self.case(), self.ledger()), "unchecked")

    def test_a_stale_row_does_not_count_as_validated(self):
        # It agreed once, against a definition that has since moved.
        led = self.ledger(**{"measure:s:m": "agrees"})
        self.assertEqual(self.basis(self.case(), led, stale=("measure:s:m",)),
                         "unchecked")

    def test_a_disagreeing_definition_outranks_everything(self):
        led = self.ledger(**{"measure:s:m": "disagrees"})
        self.assertEqual(self.basis(self.case(), led), "disagrees")

    def test_a_case_naming_no_entities_is_unchecked_not_validated(self):
        # Nothing was established, so it must not read as though something was.
        c = {"qid": "q", "golden": {"kind": "scalar", "value": 1}}
        self.assertEqual(self.basis(c, self.ledger()), "unchecked")

    def test_an_anyof_group_is_satisfied_by_either_member(self):
        # The regression: the groups were flattened and every member demanded,
        # so a case answerable through `startYear` OR the model's alias for it
        # was held unvalidated because only the alias is a checkable definition.
        c = {"qid": "q", "golden": {"kind": "scalar", "value": 1},
             "expectedEntities": {"requiredAnyOf": [["dimension:s:raw",
                                                     "dimension:s:alias"]]}}
        led = {"dimension:s:alias": {"verdict": "agrees", "exprSha": "x"}}
        self.assertEqual(vd.case_basis(c, led, set(), self.set_dir), "definitions")

    def test_a_passthrough_column_counts_as_validated(self):
        # A raw column exposed as-is has no expression that could be wrong.
        # Absent from the ledger it blocked a case forever; recorded as
        # `no_definition` it is what it is.
        c = {"qid": "q", "golden": {"kind": "scalar", "value": 1},
             "expectedEntities": {"required": ["dimension:s:runtimeMinutes"]}}
        led = {"dimension:s:runtimeMinutes": {"verdict": "no_definition",
                                              "exprSha": None}}
        self.assertEqual(vd.case_basis(c, led, set(), self.set_dir), "definitions")

    def test_an_anyof_group_with_no_validated_member_is_unchecked(self):
        c = {"qid": "q", "golden": {"kind": "scalar", "value": 1},
             "expectedEntities": {"requiredAnyOf": [["dimension:s:a",
                                                     "dimension:s:b"]]}}
        self.assertEqual(vd.case_basis(c, {}, set(), self.set_dir), "unchecked")

    def test_requiredanyof_members_count_as_tested(self):
        c = {"qid": "q", "golden": {"kind": "scalar", "value": 1},
             "expectedEntities": {"requiredAnyOf": [["measure:s:a", "measure:s:b"]]}}
        self.assertEqual(vd.tested_ids(c), ["measure:s:a", "measure:s:b"])

    def test_a_disagreeing_definition_is_named_even_under_an_independent_key(self):
        # The golden was derived from raw tables and is trustworthy; the model's
        # own total_sales still contradicts its docs. Both are true, and the
        # second is the finding the run exists to surface.
        c = self.case()
        c["golden"]["verification"] = {"primaryAxis": "a", "variesAxis": "b"}
        ev = vd.evidence_basis([c], self.ledger(**{"measure:s:m": "disagrees"}),
                               set(), self.set_dir)
        self.assertEqual(ev["counts"], {"independent": 1})
        self.assertEqual(ev["disagreeing"], ["measure:s:m"])

    def test_the_basis_totals_cover_every_case(self):
        cases = [self.case(kind="criteria"), self.case(), self.case()]
        ev = vd.evidence_basis(cases, self.ledger(), set(), self.set_dir)
        self.assertEqual(sum(ev["counts"].values()), len(cases))


class StalenessIsAHashComparison(unittest.TestCase):
    def setUp(self):
        self.model = write(MODEL)

    def tearDown(self):
        shutil.rmtree(self.model.parent, ignore_errors=True)

    def test_an_unmoved_definition_is_not_stale(self):
        led = {r["entityId"]: r for r in vd.records(self.model, False)}
        self.assertEqual(vd.stale_ids(led, self.model), set())

    def test_editing_a_dependency_makes_its_dependant_stale_too(self):
        led = {r["entityId"]: r for r in vd.records(self.model, False)}
        self.model.write_text(MODEL.replace("total_sales is sale_price.sum()",
                                            "total_sales is sale_price.sum() * 2"))
        stale = vd.stale_ids(led, self.model)
        self.assertIn("measure:order_items:total_sales", stale)
        # Its own text never changed; the sha chain is what catches it.
        self.assertIn("measure:order_items:total_sales_2022", stale)
        self.assertNotIn("measure:order_items:order_item_count", stale)

    def test_a_removed_definition_is_stale(self):
        led = {"measure:order_items:gone": {"entityId": "measure:order_items:gone",
                                            "exprSha": "x"}}
        self.assertEqual(vd.stale_ids(led, self.model),
                         {"measure:order_items:gone"})

    def test_no_model_means_nothing_can_be_called_stale(self):
        self.assertEqual(vd.stale_ids({"a": {"exprSha": "x"}}, None), set())


class AuthoredControls(unittest.TestCase):
    """A raw check is a control query a person wrote into the ledger, re-run and
    compared here. Re-deriving the definition's own expression over raw tables
    computes the same quantity and agrees; what catches a wrong population is
    the person who read the docs and wrote the population down."""

    def ns(self, truth=None):
        return argparse.Namespace(
            publisher="http://m", environment="e", package="p",
            model_path="m.malloy", truth_publisher=truth, truth_environment=None,
            truth_package="truth", truth_model="truth.malloy")

    def rec(self, **check):
        c = {"kind": "raw"}
        c.update(check)
        return {"kind": "measure", "source": "order_items",
                "name": "total_gross_margin",
                "expr": "sale_price.sum() - inventory_items.cost.sum()",
                "check": c, "needs": "reaches through inventory_items"}

    def exercise(self, rec, stated, control, truth=None):
        calls = []

        def fake(base, env, pkg, model, q, **k):
            calls.append(base)
            if "stated is" in q:
                return [{"stated": stated}], None
            return [{"control": control}], None
        with mock.patch.object(vd, "try_query", fake):
            v, d = vd.run_check(rec, self.ns(truth))
        return v, d, calls

    def test_no_authored_query_stays_unchecked(self):
        v, d, _ = self.exercise(self.rec(), 1, 1)
        self.assertEqual(v, "unchecked")
        self.assertIn("check.query", d)

    def test_an_agreeing_control_agrees(self):
        v, _, _ = self.exercise(self.rec(query="run: x -> { aggregate: control is y }"),
                           5.0, 5.0)
        self.assertEqual(v, "agrees")

    def test_a_disagreeing_control_disagrees_and_carries_the_note(self):
        # The cogs shape: the model includes cancelled lines its own status doc
        # says are not sales. Nothing mechanical sees that; the person who read
        # the doc did, and wrote it down.
        rec = self.rec(query="run: x -> { aggregate: control is z }",
                       note="excludes Cancelled, per the status doc")
        v, d, _ = self.exercise(rec, 6002288.38, 5788744.33)
        self.assertEqual(v, "disagrees")
        self.assertIn("status doc", d)

    def test_against_truth_routes_only_the_control_to_the_truth_server(self):
        rec = self.rec(query="run: raw -> { aggregate: control is c }",
                       against="truth")
        v, _, calls = self.exercise(rec, 1, 1, truth="http://t")
        self.assertEqual(v, "agrees")
        # stated came from the model server, the control from the truth server
        self.assertEqual(sorted(set(calls)), ["http://m", "http://t"])

    def test_against_truth_without_a_truth_server_is_unchecked(self):
        rec = self.rec(query="run: raw -> {}", against="truth")
        v, d, _ = self.exercise(rec, 1, 1)
        self.assertEqual(v, "unchecked")
        self.assertIn("--truth-publisher", d)

    def test_an_unknown_target_is_unchecked(self):
        v, d, _ = self.exercise(self.rec(query="run: x -> {}", against="warehouse"), 1, 1)
        self.assertEqual(v, "unchecked")
        self.assertIn("warehouse", d)

    def test_a_control_without_a_control_column_is_unchecked_not_a_crash(self):
        def fake(base, env, pkg, model, q, **k):
            return ([{"stated": 1}] if "stated is" in q else [{"wrong": 1}]), None
        with mock.patch.object(vd, "try_query", fake):
            v, d = vd.run_check(self.rec(query="run: x -> {}"), self.ns())
        self.assertEqual(v, "unchecked")
        self.assertIn("`control`", d)

    def test_a_dimension_is_not_a_target_for_an_authored_control(self):
        rec = self.rec(query="run: x -> {}")
        rec["kind"] = "dimension"
        v, _, _ = self.exercise(rec, 1, 1)
        self.assertEqual(v, "unchecked")

    def test_an_authored_control_supersedes_the_within_model_check(self):
        # total_sales is sale_price.sum() agrees with itself while including the
        # cancelled lines its own docs exclude. The within-model check cannot see
        # that; a person's population assertion can, and it wins.
        rec = self.rec(kind="within_model",
                       query="run: order_items -> { aggregate: control is "
                             "sale_price.sum() { where: status != 'Cancelled' } }",
                       note="excludes Cancelled, per the status doc")
        rec["name"], rec["expr"] = "total_sales", "sale_price.sum()"
        v, d, _ = self.exercise(rec, 12566292.88, 12119909.43)
        self.assertEqual(v, "disagrees")
        self.assertIn("authored control", d)

    def test_a_failing_control_query_is_unchecked_not_a_disagreement(self):
        def fake(base, env, pkg, model, q, **k):
            if "stated is" in q:
                return [{"stated": 1}], None
            return [], "HTTP 400: nope"
        with mock.patch.object(vd, "try_query", fake):
            v, d = vd.run_check(self.rec(query="run: x -> {}"), self.ns())
        self.assertEqual(v, "unchecked")
        self.assertIn("control query failed", d)


class TheLedgerKeepsWhatAPersonWrote(unittest.TestCase):
    """`--ledger` rebuilds from the model and must not lose an authored control,
    and must not inherit a verdict from a definition that may have moved."""

    def setUp(self):
        self.model = write(MODEL)

    def tearDown(self):
        shutil.rmtree(self.model.parent, ignore_errors=True)

    def test_a_rebuild_carries_authored_fields_forward_and_recomputes_the_rest(self):
        eid = "measure:order_items:total_cost_through_join"
        prior = {r["entityId"]: r for r in vd.records(self.model, False)}
        prior[eid]["check"]["query"] = "run: x -> { aggregate: control is 1 }"
        prior[eid]["check"]["note"] = "why"
        prior[eid]["cause"] = "CONVENTION"
        prior[eid]["verdict"] = "agrees"          # must NOT survive
        second = {r["entityId"]: r
                  for r in vd.records(self.model, False, existing=prior)}
        self.assertEqual(second[eid]["check"]["query"],
                         "run: x -> { aggregate: control is 1 }")
        self.assertEqual(second[eid]["check"]["note"], "why")
        self.assertEqual(second[eid]["cause"], "CONVENTION")
        self.assertEqual(second[eid]["verdict"], "unchecked")

    def test_a_definition_the_model_no_longer_declares_is_dropped(self):
        gone = "measure:order_items:gone"
        prior = {gone: {"entityId": gone, "check": {"query": "q"}}}
        ids = {r["entityId"] for r in vd.records(self.model, False, existing=prior)}
        self.assertNotIn(gone, ids)


class TheSummary(unittest.TestCase):
    def test_it_says_how_many_are_not_validated_by_this_run(self):
        recs = [{"verdict": "agrees", "check": {"kind": "within_model"}},
                {"verdict": "unchecked", "check": {"kind": "raw"}}]
        text = " ".join(vd.summarise(recs))
        self.assertIn("1 agrees", text)
        self.assertIn("NOT validated", text)


if __name__ == "__main__":
    unittest.main(verbosity=1)
