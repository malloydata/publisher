#!/usr/bin/env python3
"""Tests for the set-name lint: ids and vetoes that name nothing in the model
under test. The rest of verify_goldens needs a live Publisher and is exercised
by running it."""
import pathlib
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from verify_goldens import (  # noqa: E402
    model_text, unknown_name_findings)

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


if __name__ == "__main__":
    unittest.main()
