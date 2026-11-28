#!/usr/bin/env python3
"""Tests for golden_leak.py: an edit that writes a case's answer into the model."""
import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import golden_leak as gl  # noqa: E402


def diff(*added: str, removed: tuple[str, ...] = ()) -> str:
    return "\n".join(["--- a/m.malloy", "+++ b/m.malloy", "@@ -1 +1 @@",
                      *("-" + r for r in removed), *("+" + a for a in added)]) + "\n"


KEYS = {"top-cat": [{"category": "Outerwear", "revenue": 464861.37}],
        "rate": 0.5023, "count": 12, "year": {"year": 2025, "orders": 11204}}


class Values(unittest.TestCase):
    def test_a_keys_number_in_an_added_doc_is_found_however_it_is_written(self):
        for written in ("464861.37", "464,861", "$464,861.37", "about 464861 in revenue"):
            with self.subTest(written=written):
                hits = gl.check(diff(f"  #(doc) Outerwear leads at {written}"), KEYS)
                self.assertEqual([(h["qid"], h["kind"]) for h in hits], [("top-cat", "value")])

    def test_a_different_number_is_not(self):
        # 464,900 would be a leak: it is the key rounded to four digits.
        self.assertEqual(gl.check(diff("  #(doc) revenue was 471,200 last year"), KEYS), [])
        self.assertEqual(gl.check(diff("  #(doc) about 46,486 units"), KEYS), [])

    def test_short_numbers_are_not_checked(self):
        # A year, a small count and a rate appear in honest docs.
        self.assertEqual(gl.check(diff("  #(doc) 12 regions, since 2025, rate 0.5023"), KEYS), [])

    def test_a_five_digit_count_in_the_key_is_found(self):
        hits = gl.check(diff("  #(doc) there were 11,204 orders"), KEYS)
        self.assertEqual([h["qid"] for h in hits], ["year"])

    def test_only_added_lines_are_read(self):
        d = diff("  #(doc) Net sales", removed=("  #(doc) was 464,861",))
        self.assertEqual(gl.check(d, KEYS), [])
        self.assertEqual(gl.check("", KEYS), [])

    def test_a_text_value_is_not_a_leak(self):
        self.assertEqual(gl.check(diff("  #(doc) categories such as Outerwear"), KEYS), [])

    def test_an_identifier_holding_digits_is_not_a_number(self):
        self.assertEqual(gl.check(diff("  dimension: sku_464861 is id", "  x is v464861"), KEYS), [])


class AcceptedAnswers(unittest.TestCase):
    """The storefront-tour signup-cohort case: a key of 488970.54 and a rubric
    that accepts a rounded figure, a range, and a second basis."""
    GOLDEN = {"value": {"cohort_revenue": 488970.53999999806},
              "rubric": "Correct if it gives about $488,971 (accept "
                        "$487,500-$490,500). Also correct if it gives about "
                        "$445,319 (accept $444,000-$446,700). Wrong if it gives "
                        "the $512,300 figure.",
              "mustState": "about $488,971 counting every line, or about $445,319"}

    def hits(self, line):
        g = self.GOLDEN
        return gl.check(diff(f"  #(doc) {line}"), {"cohort": g["value"]}, {},
                        {"cohort": gl.accepted_text(g)})

    def test_rounded_and_suffixed_forms_of_the_key_are_found(self):
        for written in ("$489,000", "489K", "$0.49M", "$489.0k", "490,000"):
            with self.subTest(written=written):
                self.assertEqual([(h["qid"], h["found"]) for h in self.hits(
                    f"the 2025 cohort brought in {written}")],
                    [("cohort", written.lstrip("$"))])

    def test_the_rubrics_second_basis_is_found(self):
        self.assertEqual(len(self.hits("excluding returns it is $445.3K")), 1)
        self.assertEqual(len(self.hits("excluding returns it is 445,000")), 1)

    def test_a_figure_the_rubric_rejects_or_a_round_number_is_not(self):
        for line in ("the wrong basis gives $512,300", "about $1M in 2025",
                     "12 regions since 2025", "about $47K per month"):
            with self.subTest(line=line):
                self.assertEqual(self.hits(line), [])


class Queries(unittest.TestCase):
    Q = ("run: order_items -> { group_by: products.category; "
         "aggregate: total_sales; order_by: total_sales desc; limit: 1 }")

    def test_a_golden_query_pasted_whole_is_found_across_reformatting(self):
        pasted = self.Q.replace("; ", "\n+    ")
        hits = gl.check(diff("  // example:", "  " + pasted), {}, {"top-cat": [self.Q]})
        self.assertEqual([(h["qid"], h["kind"]) for h in hits], [("top-cat", "query")])

    def test_a_fragment_or_a_short_query_is_not(self):
        self.assertEqual(gl.check(diff("  aggregate: total_sales"), {}, {"top-cat": [self.Q]}), [])
        self.assertEqual(gl.check(diff("  run: a -> b"), {}, {"q": ["run: a -> b"]}), [])


if __name__ == "__main__":
    unittest.main()
