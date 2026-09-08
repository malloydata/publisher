#!/usr/bin/env python3
"""Tests for the set-name lint: ids and vetoes that name nothing in the model
under test. The rest of verify_goldens needs a live Publisher and is exercised
by running it."""
import json
import pathlib
import shutil
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from verify_goldens import (  # noqa: E402
    model_text, unknown_name_findings, verify)

MODEL = """
source: order_items is duckdb.table('data/order_items.parquet') extend {
  measure: total_sales is sale_price.sum()
  dimension: is_returned is status = 'Returned'
}
"""


def case(qid="q1", required=None, any_of=None, acceptable=None, must_not_use=None):
    return {"qid": qid,
            "expectedEntities": {k: v for k, v in
                                 (("required", required), ("requiredAnyOf", any_of),
                                  ("acceptable", acceptable)) if v},
            "golden": {"mustNotUse": must_not_use} if must_not_use else {}}


class RequiredIds(unittest.TestCase):
    def test_an_id_the_model_has_is_no_finding(self):
        f = unknown_name_findings(
            [case(required=["measure:order_items:total_sales"])], MODEL)
        self.assertEqual(f, [])

    def test_an_id_from_another_package_fails(self):
        # The real bug: five ids named after a sibling package scored as
        # retrieval misses on every run for two days.
        f = unknown_name_findings(
            [case(required=["measure:attribution_creative_linear:creative_name"])],
            MODEL)
        self.assertEqual(len(f), 1)
        self.assertFalse(f[0].startswith("review "))
        self.assertIn("creative_name", f[0])

    def test_a_right_field_under_a_wrong_source_still_fails(self):
        # The field name often survives a rename that the source name does not.
        f = unknown_name_findings(
            [case(required=["measure:attribution_creative_linear:total_sales"])],
            MODEL)
        self.assertEqual(len(f), 1)

    def test_a_two_part_id_is_checked_on_its_name(self):
        self.assertEqual(unknown_name_findings([case(required=["measure:total_sales"])],
                                               MODEL), [])
        self.assertEqual(len(unknown_name_findings(
            [case(required=["measure:no_such_measure"])], MODEL)), 1)

    def test_an_id_with_no_kind_prefix_is_a_finding(self):
        # `all(parts[1:])` is True on an empty slice, so a one-part id passed
        # the check written to catch exactly this shape, while the well-formed
        # `measure:x:no_such_field` was caught. Scoring compares whole ids, so a
        # prefix-less one can never match however real the field is.
        for bad in ("ecommerce.no_such_field", "no_such_field", ""):
            with self.subTest(bad=bad):
                f = unknown_name_findings([case(required=[bad])], MODEL)
                self.assertEqual(len(f), 1)
                self.assertFalse(f[0].startswith("review "))
                self.assertIn("kind:", f[0])

    def test_a_bare_name_the_model_does_have_is_still_a_finding(self):
        # `total_sales` IS in the model, and the id is still unusable: it is
        # the prefix that is missing, not the field. Checking the name alone
        # would let this one through and it would score as a miss every run.
        f = unknown_name_findings([case(required=["total_sales"])], MODEL)
        self.assertEqual(len(f), 1)
        self.assertIn("kind:", f[0])


class RequiredAnyOf(unittest.TestCase):
    def test_a_group_passes_when_one_id_resolves(self):
        # This is the repair for a set scored against two package versions, so
        # it must not be flagged.
        f = unknown_name_findings([case(any_of=[[
            "measure:attribution_creative_linear:creative_name",
            "measure:order_items:total_sales"]])], MODEL)
        self.assertEqual(f, [])

    def test_a_group_where_none_resolves_fails(self):
        f = unknown_name_findings([case(any_of=[[
            "measure:gone:one", "measure:gone:two"]])], MODEL)
        self.assertEqual(len(f), 1)
        self.assertFalse(f[0].startswith("review "))

    def test_a_malformed_entry_does_not_rescue_a_dead_group(self):
        # Worse here than in `required`: a group passes when ANY member
        # resolves, so one prefix-less id used to pass a group whose every
        # well-formed id names nothing.
        f = unknown_name_findings([case(any_of=[[
            "measure:gone:one", "no_such_field"]])], MODEL)
        self.assertEqual(len(f), 1)
        self.assertFalse(f[0].startswith("review "))


class ReviewedNotFailed(unittest.TestCase):
    def test_an_unknown_acceptable_id_is_only_reviewed(self):
        f = unknown_name_findings([case(acceptable=["measure:gone:x"])], MODEL)
        self.assertEqual(len(f), 1)
        self.assertTrue(f[0].startswith("review "))

    def test_a_dead_veto_is_only_reviewed(self):
        # It cannot move a number; it just protects nothing. And a model that
        # passes a raw column through without naming it would trip this.
        f = unknown_name_findings([case(must_not_use=["no_such_field"])], MODEL)
        self.assertEqual(len(f), 1)
        self.assertTrue(f[0].startswith("review "))

    def test_a_live_veto_is_no_finding(self):
        self.assertEqual(
            unknown_name_findings([case(must_not_use=["sale_price"])], MODEL), [])

    def test_prose_must_not_use_is_never_checked(self):
        # It names no field, so there is nothing to look for.
        self.assertEqual(unknown_name_findings(
            [case(must_not_use=["weekly_active_users as a cumulative series"])],
            MODEL), [])


class NoModel(unittest.TestCase):
    def test_without_model_text_the_lint_says_nothing(self):
        # A platform target has no local model text. Silence is right; claiming
        # every id is unknown would be worse than not checking.
        self.assertEqual(unknown_name_findings([case(required=["measure:a:b"])], ""), [])


class ModelText(unittest.TestCase):
    def setUp(self):
        self.tmp = pathlib.Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_a_directory_is_walked_recursively(self):
        (self.tmp / "dashboards").mkdir()
        (self.tmp / "a.malloy").write_text("source: one is x")
        (self.tmp / "dashboards" / "b.malloy").write_text("source: two is y")
        text = model_text(self.tmp)
        self.assertIn("one", text)
        self.assertIn("two", text)

    def test_a_missing_path_is_empty_not_an_error(self):
        self.assertEqual(model_text(self.tmp / "nope"), "")
        self.assertEqual(model_text(None), "")


class ExitCodes(unittest.TestCase):
    """The three-way signal improve.py's acceptance gate reads.

    An uncaught traceback exits 1 by default, and 1 is the code meaning "a
    golden drifted" -- so a missing cases.jsonl used to send someone to settle a
    golden that was fine. Anything unanticipated must land outside {0, 1}.
    """

    SCRIPT = pathlib.Path(__file__).resolve().parent / "verify_goldens.py"

    def setUp(self):
        self.tmp = pathlib.Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def run_it(self, *args):
        return subprocess.run(
            [sys.executable, str(self.SCRIPT), *args],
            capture_output=True, text=True, timeout=120)

    def test_a_crash_exits_3_not_1(self):
        # set.json present, cases.jsonl absent: the read that used to raise
        # FileNotFoundError straight through Python's default exit status.
        (self.tmp / "set.json").write_text('{"truthPackage": "x"}')
        p = self.run_it("--set", str(self.tmp),
                        "--publisher", "http://127.0.0.1:9")
        self.assertEqual(p.returncode, 3, p.stderr[-400:])
        self.assertIn("could not run", p.stderr)
        self.assertIn("says NOTHING about the goldens", p.stderr)

    def test_a_usage_error_still_exits_2(self):
        self.assertEqual(self.run_it().returncode, 2)

    def audit_set(self, required: str) -> pathlib.Path:
        """A set with no truthPackage and one entity id to audit."""
        (self.tmp / "set.json").write_text('{"name": "probe"}')
        (self.tmp / "cases.jsonl").write_text(json.dumps(
            {"qid": "q1", "expectedEntities": {"required": [required]}}) + "\n")
        model = self.tmp / "m.malloy"
        model.write_text(MODEL)
        return model

    def test_a_set_with_no_truth_package_exits_3_not_0(self):
        # 0 claimed "every golden re-derived, no findings" about a run that
        # re-derived nothing, and improve.py recorded it as `clean`. No
        # --publisher here on purpose: nothing is contacted.
        model = self.audit_set("measure:order_items:total_sales")
        p = self.run_it("--set", str(self.tmp), "--model", str(model))
        self.assertEqual(p.returncode, 3, p.stdout[-400:])
        self.assertIn("truthPackage", p.stdout)
        self.assertIn("do not read it as a pass", p.stderr)

    def test_the_audits_still_run_without_a_truth_package(self):
        # The whole point. Four checks need no server, including the set-name
        # lint, and the early return skipped all of them on exactly the set
        # whose names nobody had verified.
        model = self.audit_set("measure:other_package:creative_name")
        p = self.run_it("--set", str(self.tmp), "--model", str(model))
        self.assertIn("creative_name", p.stdout)

    def test_a_finding_without_a_truth_package_exits_1_not_3(self):
        # A finding outranks a skip: 3 tells the caller there is nothing here
        # to read, and a caller obeying that would discard the one fact this
        # run produced.
        model = self.audit_set("measure:other_package:creative_name")
        p = self.run_it("--set", str(self.tmp), "--model", str(model))
        self.assertEqual(p.returncode, 1, p.stdout[-400:])


class SkipShape(unittest.TestCase):
    """run_baseline.py calls verify() in process and never sees an exit code.

    It reads `tally` and `findings` on both paths, so one return shape has to
    carry both -- two shapes is what let the skip branch drop the findings.
    """

    def setUp(self):
        self.tmp = pathlib.Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_the_skip_shape_carries_a_tally_and_the_audit_findings(self):
        (self.tmp / "set.json").write_text('{"name": "probe"}')
        (self.tmp / "cases.jsonl").write_text(json.dumps(
            {"qid": "q1",
             "expectedEntities": {"required": ["measure:other_package:x"]}}) + "\n")
        model = self.tmp / "m.malloy"
        model.write_text(MODEL)
        # An unroutable publisher: an empty tally is also the proof that no
        # request went out, since a contacted-and-failed one tallies `error`.
        r = verify(self.tmp, "http://127.0.0.1:9", "samples",
                   model=model, quiet=True)
        self.assertTrue(r["skipped"])
        self.assertEqual(r["tally"], {})
        self.assertEqual(r["drifted"], 0)
        self.assertTrue([f for f in r["findings"]
                         if not f.startswith("review ")])

    def test_a_normal_run_reports_skipped_as_none(self):
        (self.tmp / "set.json").write_text('{"truthPackage": "truth"}')
        (self.tmp / "cases.jsonl").write_text("")
        r = verify(self.tmp, "http://127.0.0.1:9", "samples", quiet=True)
        self.assertIsNone(r["skipped"])


if __name__ == "__main__":
    unittest.main()
