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
        self.assertEqual(candidate("products.retail_price.avg()"),
                         "products.retail_price")

    def test_a_dotted_path_is_a_candidate(self):
        self.assertEqual(candidate("product.cost"), "product.cost")

    def test_a_connective_makes_it_prose_however_it_starts(self):
        # These name a WAY of using the field, not a ban on the field. Vetoing
        # the head fails correct answers: the first cost a real run a false
        # no_match, and the next two name fields nearly every correct answer to
        # their ecommerce case must use.
        for entry in ("weekly_active_users as a cumulative series",
                      "total_sales as the answer",
                      "sale_price.avg() as spend per customer",
                      "product.cost through the order_items join",
                      "products.retail_price.avg() through the product join",
                      "inventory_items.sold_at as a count of units actually sold"):
            self.assertIsNone(candidate(entry), entry)

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
        r = check(["product.cost"],
                  "run: order_items -> { aggregate: c is product.cost.sum() }")
        self.assertEqual(len(r["hits"]), 1)
        self.assertEqual(r["leaf_hits"], [])

    def test_bare_leaf_is_reported_but_never_vetoes(self):
        r = check(["product.cost"],
                  "run: inventory_items -> { aggregate: c is cost.sum() }")
        self.assertEqual(r["hits"], [])
        self.assertEqual(len(r["leaf_hits"]), 1)

    def test_a_path_reached_through_a_join_is_still_a_hit(self):
        # Reaching a joined field from the fact source is the ordinary shape in
        # Malloy, and one hop used to walk out of the veto entirely: no hit, no
        # leaf_hit, and a judge line indistinguishable from prose -- on the case
        # that is MOST certainly a violation. `test_full_path_is_a_hit` passes
        # only because its fixture puts the path at the root of the expression.
        r = check(["products.retail_price"],
                  "run: order_items -> { aggregate: p is "
                  "order_items.products.retail_price.sum() }")
        self.assertEqual(r["hits"], ["products.retail_price"])
        self.assertEqual(r["unchecked"], [])

    def test_a_bare_name_veto_fires_on_a_qualified_use(self):
        # The commoner half of the same hole: the ecommerce set's live vetoes
        # are bare names (`shipped_at`, `delivered_at`, `total_sales_2021`) and
        # a query almost always writes them qualified. writing-rubrics.md says
        # a bare name means the field must not appear "at all".
        r = check(["shipped_at"],
                  "run: order_items -> { group_by: order_items.shipped_at }")
        self.assertEqual(r["hits"], ["shipped_at"])

    def test_a_longer_source_name_is_not_the_forbidden_path(self):
        # The regression the old lookbehind was protecting, kept.
        r = check(["products.retail_price"],
                  "run: x -> { group_by: x_products.retail_price }")
        self.assertEqual(r["hits"], [])

    def test_a_longer_field_name_is_not_the_forbidden_path(self):
        r = check(["products.retail_price"],
                  "run: x -> { group_by: products.retail_price_adj }")
        self.assertEqual(r["hits"], [])

    def test_an_entry_the_script_could_not_decide_is_not_listed_as_checked(self):
        # It goes to the judge as prose; saying it was checked as well claimed
        # a decision the script did not make.
        r = check(["products.retail_price"],
                  "run: x -> { group_by: other.cost }")
        self.assertEqual(r["unchecked"], ["products.retail_price"])
        self.assertEqual(r["checked"], [])

    def test_a_use_objection_never_vetoes_the_field_it_names(self):
        # The regression this file exists to hold: the answer showed the field
        # as an extra column and its series was correct.
        r = check(["weekly_active_users as a cumulative series"],
                  "run: reach -> { group_by: week; aggregate: weekly_active_users, "
                  "cumulative is weekly_active_users.sum() }")
        self.assertEqual(r["hits"], [])
        self.assertEqual(r["unchecked"],
                         ["weekly_active_users as a cumulative series"])

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

    def test_a_comment_marker_inside_a_string_does_not_eat_the_line(self):
        # The regression: stripping comments before strings read the `//` in a
        # URL literal as a comment start and blanked the rest of the line, so
        # every forbidden name after it silently escaped the veto.
        q = ("run: x -> { where: link = 'https://example.com', "
             "aggregate: c is shipped_at.sum() }")
        self.assertIn("shipped_at", strip_noise(q))
        self.assertEqual(check(["shipped_at"], q)["hits"], ["shipped_at"])

    def test_a_double_hyphen_inside_a_string_does_not_eat_the_line(self):
        q = ("run: x -> { where: r = '2024-01--2024-06', "
             "aggregate: c is total_sales }")
        self.assertEqual(check(["total_sales"], q)["hits"], ["total_sales"])

    def test_a_quote_inside_a_comment_still_blanks_the_comment(self):
        # The mirror bug the one-pass scan also has to avoid: fixing the above
        # by stripping strings first would leave this comment's text live.
        q = "run: x -> { -- avoided 'shipped_at' per the note\n  c is count() }"
        self.assertNotIn("shipped_at", strip_noise(q))
        self.assertEqual(check(["shipped_at"], q)["hits"], [])


class JudgeNote(unittest.TestCase):
    def test_note_carries_prose_and_leaf_suspicions_only(self):
        r = check(["an average of per-SKU prices",
                   "product.cost",
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
