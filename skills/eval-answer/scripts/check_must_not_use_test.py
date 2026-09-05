#!/usr/bin/env python3
"""Tests for check_must_not_use. Entries are taken from the ecommerce set, so a
change that breaks a real golden's veto fails here."""
import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from check_must_not_use import candidate, check, judge_note, strip_noise  # noqa: E402


class Candidate(unittest.TestCase):
    def test_plain_identifier(self):
        self.assertEqual(candidate("shipped_at"), "shipped_at")

    def test_call_suffix_names_the_field_not_the_aggregate(self):
        self.assertEqual(
            candidate("products.retail_price.avg() through the product join"),
            "products.retail_price")

    def test_path_before_through(self):
        self.assertEqual(candidate("product.cost through the order_items join"),
                         "product.cost")

    def test_prose_is_not_a_candidate(self):
        for entry in ("an average of per-SKU prices",
                      "status = 'Cancelled' as an abandonment proxy",
                      "a line-item denominator",
                      "1207 + 1701 as a sum",
                      "November 2022 as the answer without naming the truncation"):
            self.assertIsNone(candidate(entry), entry)


class Check(unittest.TestCase):
    def test_forbidden_field_in_a_filter_is_a_hit(self):
        r = check(["shipped_at", "delivered_at"],
                  "run: order_items -> { where: year(shipped_at) = 2022 }")
        self.assertEqual(r["hits"], ["shipped_at"])
        self.assertEqual(r["unchecked"], ["delivered_at"])

    def test_a_longer_name_is_not_the_forbidden_one(self):
        r = check(["total_sales_2021"],
                  "run: x -> { aggregate: total_sales_2021_adj }")
        self.assertEqual(r["hits"], [])

    def test_exact_measure_name_is_a_hit(self):
        r = check(["total_sales_2021"], "run: x -> { aggregate: total_sales_2021 }")
        self.assertEqual(r["hits"], ["total_sales_2021"])

    def test_full_path_is_a_hit(self):
        r = check(["product.cost through the order_items join"],
                  "run: order_items -> { aggregate: c is product.cost.sum() }")
        self.assertEqual(len(r["hits"]), 1)
        self.assertEqual(r["leaf_hits"], [])

    def test_bare_leaf_is_reported_but_never_vetoes(self):
        r = check(["product.cost through the order_items join"],
                  "run: inventory_items -> { aggregate: c is cost.sum() }")
        self.assertEqual(r["hits"], [])
        self.assertEqual(len(r["leaf_hits"]), 1)

    def test_prose_goes_to_the_judge(self):
        r = check(["an average of per-SKU prices"], "run: x -> { aggregate: y }")
        self.assertEqual(r["hits"], [])
        self.assertEqual(r["unchecked"], ["an average of per-SKU prices"])

    def test_a_name_in_a_comment_does_not_veto(self):
        r = check(["shipped_at"],
                  "run: x -> {\n  -- deliberately not shipped_at\n"
                  "  where: year(created_at) = 2022\n}")
        self.assertEqual(r["hits"], [])

    def test_a_name_inside_a_string_literal_does_not_veto(self):
        r = check(["shipped_at"], "run: x -> { where: label = 'shipped_at' }")
        self.assertEqual(r["hits"], [])

    def test_no_query_checks_nothing(self):
        r = check(["shipped_at"], None)
        self.assertEqual(r["hits"], [])
        self.assertEqual(r["unchecked"], ["shipped_at"])

    def test_no_entries_is_empty(self):
        r = check(None, "run: x -> { aggregate: y }")
        self.assertEqual(r, {"hits": [], "leaf_hits": [], "unchecked": [],
                             "checked": []})


class Noise(unittest.TestCase):
    def test_block_comment_and_quotes_are_blanked(self):
        self.assertNotIn("shipped_at",
                         strip_noise("/* shipped_at */ where: x = 'shipped_at'"))


class JudgeNote(unittest.TestCase):
    def test_note_carries_prose_and_leaf_suspicions_only(self):
        r = check(["an average of per-SKU prices",
                   "product.cost through the order_items join",
                   "shipped_at"],
                  "run: x -> { where: year(shipped_at) = 2022, "
                  "aggregate: c is cost.sum() }")
        note = judge_note(r)
        self.assertIn("an average of per-SKU prices", note)
        self.assertIn("bare `cost`", note)
        # The vetoed one is applied to the verdict, not argued to the judge.
        self.assertNotIn("shipped_at", note)


if __name__ == "__main__":
    unittest.main()
