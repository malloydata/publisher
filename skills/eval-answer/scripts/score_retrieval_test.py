#!/usr/bin/env python3
"""Tests for score_retrieval. Stdlib only: python score_retrieval_test.py"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest

import score_retrieval

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from score_retrieval import (  # noqa: E402
    MEASURED_GAPS, MEASURED_OK, attribute, cascade, coverage_report_summary,
    load_coverage_report, main, score_case, summarise,
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


def calls_with_targets(entities, targets, qid="q"):
    return [{"kind": "tool_call", "tool": "get_context", "qid": qid,
             "sample": None, "phase": "baseline", "targets": list(targets),
             "rankedSummary": {"entityIds": list(entities)}}]


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
        # No targets recorded at all, so nothing proves the agent asked for a
        # measure; the honest read is the ownerless one.
        self.assertEqual(r["where_to_fix"], "never asked")

    def test_full_delivery_with_a_wrong_answer_is_delivered_wrong(self):
        r = score_case(self.group_case(), calls([D_STATUS, M_REVENUE]), KEY, "no_match")
        self.assertEqual(r["where_to_fix"], "delivered, wrong")

    def test_a_satisfied_group_does_not_make_its_unused_route_unasked(self):
        """The route not taken is not a miss, so its KIND is not unasked-for.

        Collecting every undelivered member regardless of whether its group
        was satisfied let a satisfied group contribute a phantom kind, which
        `attribute` read as NEVER-ASKED. A case whose only real miss was a
        dimension the agent DID search for came back owned by agent-skill,
        "no search asked for a join at all" -- charging the skills team for a
        route the case never needed.
        """
        c = {"qid": "q", "coverage": "covered",
             "expectedEntities": {
                 "required": [D_STATUS],
                 "requiredAnyOf": [[M_SALES, "join:order_items:users"]]}}
        # The agent asked for exactly the kinds it needed, never for a join,
        # and the group is satisfied through its measure route.
        events = calls_with_targets([M_SALES],
                                    ["dimension: order status",
                                     "measure: total sales"])
        r = score_case(c, events, KEY, "no_match")
        self.assertEqual(r["missing"], [D_STATUS])
        self.assertEqual(r["recall"], 0.5)
        self.assertEqual(r["where_to_fix"], "not retrieved")
        self.assertEqual(r["owner"], "undecided")
        self.assertNotIn("join", r["why"])


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
    def test_full_recall_and_a_wrong_answer_names_no_owner(self):
        # Everything arrived. eval-diagnose attributes construction only after
        # sufficiency -- WRONG-PICK is the model's if the docs did not
        # distinguish the candidates -- so charging the agent here filed
        # documentation gaps as skills bugs.
        r = score_case(case(), calls([M_SALES]), KEY, "no_match")
        self.assertEqual((r["component"], r["owner"]),
                         ("construction", "undecided"))
        self.assertEqual(r["where_to_fix"], "delivered, wrong")

    def test_a_miss_after_a_search_of_the_right_kind_names_no_owner(self):
        # The entity exists and a measure search was issued, and it still did
        # not come back. Docs or search wording; eval-diagnose separates
        # NOT-RETURNED from QUESTION-VOCAB. The run must not pick.
        r = score_case(case(coverage="covered"),
                       calls_with_targets([], ["measure: total sales"]),
                       KEY, "no_match")
        self.assertEqual(r["where_to_fix"], "not retrieved")
        self.assertEqual(r["owner"], "undecided")

    def test_a_miss_with_no_search_of_that_kind_is_the_agents(self):
        # The regression, and it is from a real run: the agent searched only
        # `source:` and `dimension:` for "how many titles were released in
        # 2019?", so the measure could not come back, and a documented measure
        # was blamed on its docs. eval-diagnose calls this NEVER-ASKED.
        r = score_case(case(coverage="covered"),
                       calls_with_targets([], ["source: titles",
                                               "dimension: release year"]),
                       KEY, "no_match")
        self.assertEqual(r["where_to_fix"], "never asked")
        self.assertEqual(r["owner"], "agent-skill")
        self.assertIn("measure", r["why"])

    def test_the_kinds_the_agent_searched_are_recorded(self):
        # So a reader can judge the search instead of taking the label's word,
        # and so a set can be surveyed for the vocabulary its questions need.
        r = score_case(case(), calls_with_targets([M_SALES],
                                                  ["measure: total sales"]),
                       KEY, "match")
        self.assertEqual(r["asked_kinds"], ["measure"])

    def test_typed_targets_are_read_as_well_as_prefixed_strings(self):
        r = score_case(case(coverage="covered"),
                       calls_with_targets([], [{"target_type": "measure",
                                                "search_text": "sales"}]),
                       KEY, "no_match")
        self.assertEqual(r["where_to_fix"], "not retrieved")

    def test_a_missed_entity_that_does_not_exist_blames_the_model(self):
        # `derivable` is a MEASURED label: someone looked and found nothing to
        # surface. That stays the model's.
        r = score_case(case(coverage="derivable"), calls([]), KEY, "no_match")
        self.assertEqual((r["component"], r["owner"]), ("get_context/model", "model"))

    def test_a_missed_entity_with_no_coverage_label_blames_nobody_yet(self):
        # The bug: with no authored label, `case.get("coverage", "unknown")`
        # fell through to MODEL with "coverage is unknown, so the entity does
        # not exist" -- a model gap asserted on no evidence. A set that arrives
        # as bare questions has no labels, so every retrieval failure in it was
        # attributed to the model.
        c = {"qid": "q", "expectedEntities": {"required": [M_SALES]}}
        r = score_case(c, calls([]), KEY, "no_match")
        self.assertEqual(r["where_to_fix"], "coverage not measured")
        self.assertEqual(r["owner"], "unknown")
        self.assertNotEqual(r["component"], "get_context/model")
        self.assertTrue(r["failed"], "still a failure; only the owner is undecided")

    def test_an_explicit_unknown_label_is_also_unmeasured(self):
        r = score_case(case(coverage="unknown"), calls([]), KEY, "no_match")
        self.assertEqual(r["where_to_fix"], "coverage not measured")

    def test_a_miss_is_attributed_even_when_the_answer_was_right(self):
        # An answer that came out right WITHOUT a required entity was right by
        # another route -- usually the agent rebuilding the model's own measure
        # inline, which holds only while the measure is trivial. Attributing
        # the miss anyway is what makes it visible: measured on one run, 4 of 5
        # undelivered entities sat on passing cases.
        for verdict in ("match", "near_match"):
            r = score_case(case(), calls([]), KEY, verdict)
            self.assertTrue(r["where_to_fix"],
                            f"{verdict} with an undelivered entity must attribute")
            # The pass rate is untouched: the answer is still not a failure.
            self.assertFalse(r["failed"])
            self.assertEqual(r["verdict"], verdict)

    def test_a_pass_that_received_everything_has_nothing_to_attribute(self):
        r = score_case(case(), calls([M_SALES]), KEY, "match")
        self.assertEqual(r["recall"], 1.0)
        self.assertEqual(r["where_to_fix"], "")

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


class MeasuredCoverage(unittest.TestCase):
    """check_coverage.py's verdict reaches attribution, and beats the label.

    The `--out` report it writes was read by nothing in the repo. The authored
    `coverage` label is a standing hand judgement about the question; the
    verdict is a measurement against this build, which is what an attribution
    is about.
    """

    def test_a_measured_ok_with_a_miss_is_a_retrieval_rung_finding(self):
        r = score_case(case(coverage="derivable"), calls([]), KEY, "no_match",
                       measured="ok")
        self.assertIn(r["where_to_fix"], ("not retrieved", "never asked"))
        self.assertEqual(r["coverage_source"], "measured")

    def test_a_measured_gap_blames_the_model_and_names_the_code(self):
        r = score_case(case(coverage="covered"), calls([]), KEY, "no_match",
                       measured="NO-DISAMBIG")
        self.assertEqual(r["where_to_fix"], "model coverage")
        self.assertIn("NO-DISAMBIG", r["why"])

    def test_measurement_beats_the_authored_label(self):
        # Label says covered (retrieval's fault); measurement says the model
        # has no representing entity (model's fault). The measurement wins.
        r = score_case(case(coverage="covered"), calls([]), KEY, "no_match",
                       measured="COVERAGE")
        self.assertEqual(r["owner"], "model")

    def test_an_undecided_measurement_falls_back_to_the_label(self):
        r = score_case(case(coverage="covered"), calls([]), KEY, "no_match",
                       measured=None)
        self.assertEqual(r["coverage_source"], "authored")
        self.assertIn(r["where_to_fix"], ("not retrieved", "never asked"))

    def test_no_label_and_no_measurement_charges_nobody(self):
        c = {"qid": "q", "expectedEntities": {"required": [M_SALES]}}
        r = score_case(c, calls([]), KEY, "no_match")
        self.assertEqual(r["coverage_source"], "none")
        self.assertEqual(r["where_to_fix"], "coverage not measured")

    def test_gap_vocabulary_matches_check_coverage(self):
        # score_retrieval stays stdlib-only and does not import check_coverage,
        # so this is the one place the two files are held to the same codes.
        import check_coverage as cc
        for v in cc.FAIL_VERDICTS:
            self.assertIn(v, MEASURED_GAPS, v)
        self.assertEqual(cc.OK, MEASURED_OK)

    def test_load_coverage_report_keys_verdicts_by_qid(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "cov.json")
            with open(path, "w") as fh:
                json.dump({"version": "0.0.58", "cases_detail": [
                    {"qid": "a", "verdict": "ok"},
                    {"qid": "b", "verdict": None}]}, fh)
            self.assertEqual(load_coverage_report(path), {"a": "ok", "b": None})

    def test_coverage_report_summary_records_what_run_json_needs(self):
        # The file, the version, the judge, and decided-of-cases. Not the
        # percentage: 4 of 49 decided is a sample size, not a coverage number.
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "cov.json")
            with open(path, "w") as fh:
                json.dump({"version": "0.0.58", "agentModel": "sonnet",
                           "cases": 49, "decided": 45, "ok": 22,
                           "coverage": 0.489, "cases_detail": []}, fh)
            got = coverage_report_summary(path)
        self.assertEqual(got, {"path": path, "version": "0.0.58",
                               "agentModel": "sonnet", "decided": 45,
                               "cases": 49})


class Cascade(unittest.TestCase):
    """Each metric conditions the next, so the report is a funnel, and the rungs
    must sum to the rows or a case has fallen between them."""

    def rows(self):
        bare = {"qid": "q", "expectedEntities": {"required": [M_SALES]}}
        return [
            score_case(case(coverage="derivable"), calls([]), KEY, "no_match"),
            score_case(bare, calls([]), KEY, "no_match"),
            score_case(case(), calls([]), KEY, "no_match"),
            score_case(case(), calls([M_SALES]), KEY, "no_match"),
            score_case(case(), calls([M_SALES]), KEY, "match"),
            score_case(case(), calls([M_SALES]), KEY, "needs_human"),
        ]

    # `passed_not_covered` and `passed_not_retrieved` OVERLAY the rungs: they
    # re-count rows already counted by "not covered" and "not retrieved". They
    # are not rungs and must never be added to them.
    OVERLAY = ("total", "passed_not_covered", "passed_not_retrieved")

    def test_every_row_lands_on_exactly_one_rung(self):
        c = cascade(self.rows())
        self.assertEqual(c["total"], 6)
        self.assertEqual(sum(v for k, v in c.items()
                             if k not in self.OVERLAY), 6)

    def test_the_rungs_never_revise_the_pass_rate(self):
        """The rule a hand-written report broke, on a run that was 10 of 10.

        Rendered as a shrinking funnel -- 5 of 10 covered, 3 of those 5
        retrieved, 3 of those 3 correct -- the last rung reads as "3 of 10
        succeeded" on a run where every answer was right. The three numbers
        that reconcile it are `delivered, right` plus the two overlays, and a
        report that drops them cannot be checked. So pin the identity.
        """
        rows = [
            # passes that stop on the coverage rung
            score_case(case(coverage="derivable"), calls([M_SALES]), KEY, "match"),
            score_case(case(coverage="absent"), calls([M_SALES]), KEY, "match"),
            # a pass that stops on the retrieval rung
            score_case(case(required=(M_SALES, M_COUNT)), calls([M_SALES]),
                       KEY, "match"),
            # a pass that goes all the way
            score_case(case(), calls([M_SALES]), KEY, "match"),
            # and a genuine failure, which the last rung DOES own
            score_case(case(), calls([M_SALES]), KEY, "no_match"),
        ]
        c = cascade(rows)
        passes = (c["delivered, right"] + c["passed_not_covered"]
                  + c["passed_not_retrieved"])
        self.assertEqual(passes, 4, "four of the five rows matched")
        self.assertEqual(c["delivered, right"], 1)
        self.assertNotEqual(c["delivered, right"], passes,
                            "the last rung alone must not be read as the score")

    def test_the_rungs(self):
        c = cascade(self.rows())
        self.assertEqual(c["not covered"], 1)
        self.assertEqual(c["unmeasured"], 1)
        self.assertEqual(c["not retrieved"], 1)
        self.assertEqual(c["delivered, wrong"], 1)
        self.assertEqual(c["delivered, right"], 1)
        self.assertEqual(c["not scored"], 1)

    def test_a_pass_on_an_earlier_rung_is_counted_there(self):
        # A real run: every case matched, yet the last rung read 6 because two
        # passed despite a coverage gap and incomplete retrieval. 6 is exactly
        # the number a reader mistakes for the pass rate.
        rows = [
            score_case(case(coverage="derivable"), calls([M_SALES]), KEY, "match"),
            score_case(case(required=(M_SALES, M_COUNT)), calls([M_SALES]),
                       KEY, "match"),
            score_case(case(), calls([M_SALES]), KEY, "match"),
        ]
        c = cascade(rows)
        self.assertEqual(c["passed_not_covered"], 1)
        self.assertEqual(c["passed_not_retrieved"], 1)
        self.assertEqual(c["delivered, right"], 1)
        # and the three still sum
        self.assertEqual(c["not covered"] + c["not retrieved"]
                         + c["delivered, right"], 3)

    def test_a_measured_ok_counts_as_covered(self):
        c = cascade([score_case(case(coverage="derivable"), calls([M_SALES]),
                                KEY, "match", measured="ok")])
        self.assertEqual(c["delivered, right"], 1)

    def test_no_rows_is_an_empty_funnel_not_a_crash(self):
        self.assertEqual(cascade([])["total"], 0)


class LabelsMatchTheRunPackage(unittest.TestCase):
    """The run package's Malloy model counts failures by filtering on these
    labels as string literals. A label renamed here and not there makes that
    measure read zero forever, silently: the exact unearned number this script
    exists to prevent. So the two are held equal, and a new label must come with
    a measure."""

    def test_the_template_filters_on_exactly_the_labels_emitted(self):
        import re
        from score_retrieval import (DELIVERED, MODEL, NEVER_ASKED,
                                     NOT_RETURNED, REFUSAL, UNMEASURED)
        here = os.path.dirname(os.path.abspath(__file__))
        tpl = os.path.join(here, "..", "..", "eval-loop", "templates",
                           "eval-run-package", "eval_run.malloy")
        with open(tpl) as fh:
            literals = set(re.findall(r"where_to_fix = '([^']+)'", fh.read()))
        emitted = {t[2] for t in (DELIVERED, MODEL, NEVER_ASKED, NOT_RETURNED,
                                  REFUSAL, UNMEASURED)}
        self.assertEqual(literals, emitted)

    def package_file(self, *parts):
        here = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(here, "..", "..", "eval-loop", "templates",
                               "eval-run-package", *parts)) as fh:
            return fh.read()

    def emitted(self):
        from score_retrieval import (DELIVERED, MODEL, NEVER_ASKED,
                                     NOT_RETURNED, REFUSAL, UNMEASURED)
        return {t[2] for t in (DELIVERED, MODEL, NEVER_ASKED, NOT_RETURNED,
                               REFUSAL, UNMEASURED)}

    def test_the_notebook_legend_names_every_label(self):
        """The legend is what a reader consults to interpret the column, so a
        label missing from it is worse than no legend. It named `documentation`
        -- a value that never appears -- and omitted `never asked` entirely."""
        text = self.package_file("eval_run.malloynb")
        for label in self.emitted():
            self.assertIn(f"**{label}**", text, label)

    def test_the_dashboard_tooltip_names_every_label(self):
        text = self.package_file("public", "app.js")
        start = text.index("Where a failure would have to be fixed")
        tooltip = text[start:start + 700]
        for label in self.emitted():
            self.assertIn(label, tooltip, label)

    def test_no_retired_label_survives_in_the_package(self):
        # Each of these was a real value once; each now matches nothing.
        for f in (("eval_run.malloy",), ("eval_run.malloynb",),
                  ("public", "app.js"), ("README.md",)):
            text = self.package_file(*f)
            for retired in ("query construction", "retrieval ranking"):
                self.assertNotIn(retired, text, f"{f}: {retired}")


class Summary(unittest.TestCase):
    def test_an_unmeasured_failure_is_counted_not_dropped(self):
        # "Every failure is attributed somewhere" must hold for this bucket too,
        # or a bare-question set's failures vanish from the where-to-fix totals.
        c = {"qid": "q", "expectedEntities": {"required": [M_SALES]}}
        s = summarise([score_case(c, calls([]), KEY, "no_match")])
        self.assertEqual(s["failures"], 1)
        self.assertEqual(s["failures_by_where_to_fix"],
                         {"coverage not measured": 1})

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
            "delivered, wrong": 1, "never asked": 1,
            "model coverage": 1, "refusal behaviour": 1})


class TotalRetrievalFailure(unittest.TestCase):
    """Every scored attempt returned zero entities.

    `score_case` leaves `precision` None for an empty `got_set`, so the mean of
    an all-None column is None -- while `mean_recall` is still a float, because
    it is taken over every scored row. Guarding only recall let this run reach
    `100 * None`, and it is the run whose report matters most.
    """

    def test_summarise_gives_a_recall_but_no_precision(self):
        rows = [score_case(case(qid="a"), calls(qid="a"), ("a", None, "baseline"),
                           "no_match")]
        s = summarise(rows)
        self.assertEqual(s["retrieval_scored"], 1)
        self.assertIsNotNone(s["mean_recall"])
        self.assertIsNone(s["mean_precision"])

    def test_the_cli_reports_rather_than_crashing(self):
        events = [
            {"kind": "attempt", "qid": "a", "sample": None, "phase": "baseline"},
            {"kind": "tool_call", "tool": "get_context", "qid": "a",
             "sample": None, "phase": "baseline",
             "rankedSummary": {"entityIds": []}},
            {"kind": "score", "qid": "a", "sample": None, "phase": "baseline",
             "verdict": "no_match"},
        ]
        with tempfile.TemporaryDirectory() as d:
            ep, cp = os.path.join(d, "e.jsonl"), os.path.join(d, "c.jsonl")
            for path, rows in ((ep, events), (cp, [case(qid="a")])):
                with open(path, "w") as fh:
                    for r in rows:
                        fh.write(json.dumps(r) + "\n")
            p = subprocess.run(
                [sys.executable, os.path.join(os.path.dirname(__file__),
                                              "score_retrieval.py"),
                 "--events", ep, "--cases", cp],
                capture_output=True, text=True)
        self.assertEqual(p.returncode, 0, p.stderr)
        self.assertNotIn("TypeError", p.stderr)
        self.assertIn("mean precision n/a", p.stdout)


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
        self.assertEqual(by["a"]["owner"], "undecided",
                         "had everything and still failed")
        self.assertEqual(by["b"]["owner"], "model",
                         "nothing to retrieve, so not retrieval's fault")


class TargetKinds(unittest.TestCase):
    """What a search target can return, and who is blamed when it cannot."""

    def test_kinds_by_target_matches_the_server(self):
        # This module is stdlib-only and cannot import the TypeScript, so the
        # map is mirrored and pinned here. Wrong in either direction
        # misattributes: too narrow blames the agent for not asking when it
        # did, too wide reads a real never-asked as a retrieval failure.
        here = os.path.dirname(os.path.abspath(__file__))
        src = os.path.join(here, "..", "..", "..", "packages", "server", "src",
                           "mcp", "tools", "get_context_tool.ts")
        with open(src, encoding="utf-8") as fh:
            text = fh.read()
        block = text.split("const KINDS_BY_TARGET")[1].split("};")[0]
        for target, kinds in score_retrieval.KINDS_BY_TARGET.items():
            self.assertIn(f"{target}:", block,
                          f"{target} is not in the server's map")
            line = next(l for l in block.splitlines()
                        if l.strip().startswith(f"{target}:"))
            for k in kinds:
                self.assertIn(f'"{k}"', line,
                              f"server's {target} does not select {k}")
            if not kinds:
                self.assertIn("[]", line, f"{target} should select nothing")

    def test_a_bare_target_still_counts_as_having_asked(self):
        # A target carrying no `search_text` enumerates its type. `targets`
        # drops it (there is no term to record), so reading only that scored an
        # agent who enumerated every measure as never having asked for one.
        ev = [{"kind": "tool_call", "tool": "get_context", "qid": "q",
               "sample": None, "phase": "baseline",
               "rankedSummary": {"entityIds": []},
               "targets": ["dimension: carrier"],
               "target_shapes": [{"type": "dimension", "has_text": True},
                                 {"type": "measure", "has_text": False}]}]
        _, _, _, asked = score_retrieval.retrieved(ev, KEY)
        self.assertIn("measure", asked)

    def test_a_view_target_covers_a_named_query(self):
        ev = [{"kind": "tool_call", "tool": "get_context", "qid": "q",
               "sample": None, "phase": "baseline",
               "rankedSummary": {"entityIds": []},
               "target_shapes": [{"type": "view", "has_text": True}]}]
        _, _, _, asked = score_retrieval.retrieved(ev, KEY)
        self.assertEqual(asked, {"view", "query"})

    def test_a_legacy_run_without_shapes_still_reads_its_targets(self):
        ev = [{"kind": "tool_call", "tool": "get_context", "qid": "q",
               "sample": None, "phase": "baseline",
               "rankedSummary": {"entityIds": []},
               "targets": ["measure: flight count", "dimension: carrier"]}]
        _, _, _, asked = score_retrieval.retrieved(ev, KEY)
        self.assertEqual(asked, {"measure", "dimension"})

    def test_a_measure_missed_with_no_measure_target_is_never_asked(self):
        # The deterministic case, and the one this whole distinction exists
        # for: `target_type` is a hard filter on the server, so no amount of
        # documentation could have delivered it.
        comp, owner, where, why = score_retrieval.attribute(
            recall=0.0, coverage="covered", passed=False,
            missing_kinds={"measure"}, asked_kinds={"source", "dimension"})
        self.assertEqual(owner, "agent-skill")
        self.assertIn("never asked", where)

    def test_a_measure_missed_despite_a_measure_target_is_not_never_asked(self):
        # A measure target existed but described a different concept. That is a
        # judgement about phrasing, not a mechanical miss, so it goes to
        # diagnose rather than being charged to the agent here.
        comp, owner, where, why = score_retrieval.attribute(
            recall=0.0, coverage="covered", passed=False,
            missing_kinds={"measure"}, asked_kinds={"measure", "dimension"})
        self.assertNotIn("never asked", where)
        self.assertEqual(owner, "undecided")


if __name__ == "__main__":
    unittest.main(verbosity=2)
