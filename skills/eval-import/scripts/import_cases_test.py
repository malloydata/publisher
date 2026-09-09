#!/usr/bin/env python3
"""Tests for the import checks: what an imported key may claim, and whether a
question has been edited since it was sealed."""
import json
import pathlib
import sys
import tempfile
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import import_cases as ic  # noqa: E402


def case(**over):
    base = {"qid": "q1", "question": "How many orders shipped late?",
            "split": "dev"}
    base.update(over)
    return base


def findings(c):
    return ic.check_case(c, "cases.jsonl:1")[0]


def review(c):
    return ic.check_case(c, "cases.jsonl:1")[1]


class TheSeal(unittest.TestCase):
    def test_a_matching_stamp_is_silent(self):
        q = "How many orders shipped late?"
        self.assertEqual(
            findings(case(questionSha=ic.sha256_text(q))), [])

    def test_an_edited_question_is_a_finding(self):
        # The audit this exists for narrowed four questions to match what the
        # answerer kept doing, three of them with the key untouched.
        stamped = ic.sha256_text("the frequency distribution")
        got = findings(case(question="the reach frequency distribution",
                            questionSha=stamped))
        self.assertEqual(len(got), 1)
        self.assertIn("does not match its `questionSha`", got[0])

    def test_no_stamp_is_not_a_finding(self):
        # --stamp writes it. A set mid-import is not a broken set.
        self.assertEqual(findings(case()), [])

    def test_stamping_never_overwrites(self):
        with tempfile.TemporaryDirectory() as d:
            path = pathlib.Path(d) / "cases.jsonl"
            cases = [case(qid="a", questionSha="pinned-by-conversion"),
                     case(qid="b")]
            path.write_text("".join(json.dumps(c) + "\n" for c in cases))
            self.assertEqual(ic.stamp_cases(path, cases), 1)
            self.assertEqual(cases[0]["questionSha"], "pinned-by-conversion")
            self.assertEqual(cases[1]["questionSha"],
                             ic.sha256_text(cases[1]["question"]))


class WhatAnImportedKeyMayClaim(unittest.TestCase):
    def test_a_verified_value_needs_to_say_what_verified_it(self):
        got = findings(case(golden={"status": "verified", "kind": "scalar",
                                    "value": 4200000}))
        self.assertEqual(len(got), 1)
        self.assertIn("Nothing an import can do makes a value verified", got[0])

    def test_a_value_of_zero_is_still_a_value(self):
        # `if golden.get("value")` would have let 0 through as no key at all.
        got = findings(case(golden={"status": "verified", "kind": "scalar",
                                    "value": 0}))
        self.assertEqual(len(got), 1)

    def test_criteria_are_verified_on_arrival(self):
        self.assertEqual(findings(case(golden={
            "status": "verified", "kind": "criteria",
            "verifiedBy": "authored_criteria",
            "rubric": "Breaks the total out by region."})), [])

    def test_criteria_holding_a_number_must_be_split(self):
        got = findings(case(golden={
            "status": "verified", "kind": "criteria", "value": 4200000,
            "rubric": "Should be about 4.2M, broken out by region."}))
        self.assertTrue(any("split it" in f for f in got))

    def test_criteria_with_no_clauses_has_no_key(self):
        got = findings(case(golden={"status": "verified", "kind": "criteria",
                                    "verifiedBy": "authored_criteria"}))
        self.assertTrue(any("golden.rubric" in f for f in got))

    def test_an_authored_query_claim_needs_the_query(self):
        got = findings(case(golden={"status": "provisional", "kind": "scalar",
                                    "value": 12, "verifiedBy": "authored_query"}))
        self.assertEqual(len(got), 1)
        self.assertIn("canonicalQuery", got[0])

    def test_a_provisional_value_is_the_normal_import(self):
        self.assertEqual(findings(case(golden={
            "status": "provisional", "kind": "scalar", "value": 12,
            "canonicalQuery": "run: orders -> late_count",
            "verifiedBy": "authored_query"})), [])

    def test_an_unknown_status_is_a_finding(self):
        got = findings(case(golden={"status": "trusted", "kind": "scalar",
                                    "value": 1}))
        self.assertTrue(any("golden.status" in f for f in got))


class WhatIsReportedNotFailed(unittest.TestCase):
    def test_a_question_with_no_golden_is_a_case(self):
        self.assertEqual(findings(case()), [])
        self.assertTrue(any("no golden" in r for r in review(case())))

    def test_guessed_required_entities_are_flagged(self):
        c = case(golden={"status": "provisional", "kind": "scalar", "value": 1},
                 expectedEntities={"required": ["field:orders:late_count"]})
        self.assertEqual(findings(c), [])
        self.assertTrue(any("retrieval miss" in r for r in review(c)))


class RequiredFields(unittest.TestCase):
    def test_a_missing_split_says_why_it_matters(self):
        got = findings(case(split=None))
        self.assertTrue(any("not a holdout" in f for f in got))

    def test_a_blank_question_is_not_a_case(self):
        got = findings(case(question="   "))
        self.assertTrue(any("no `question`" in f for f in got))


class Counting(unittest.TestCase):
    def test_unparseable_lines_are_counted_not_swallowed(self):
        with tempfile.TemporaryDirectory() as d:
            path = pathlib.Path(d) / "cases.jsonl"
            path.write_text(json.dumps(case()) + "\n{oops\n"
                            + json.dumps(case(qid="q2")) + "\n")
            cases, f = ic.read_cases(path)
            self.assertEqual(len(cases), 2)
            self.assertEqual(len(f), 1)
            self.assertIn("cases.jsonl:2", f[0])

    def test_the_summary_leads_with_the_scorable_count(self):
        cases = [
            case(qid="a", golden={"status": "verified", "kind": "criteria",
                                  "rubric": "x"}),
            case(qid="b", golden={"status": "provisional", "kind": "scalar",
                                  "value": 1, "canonicalQuery": "run: x"}),
            case(qid="c", golden={"status": "provisional", "kind": "scalar",
                                  "value": 2}),
            case(qid="d"),
        ]
        out = ic.summarize(cases, lines=4)
        self.assertEqual(out[0], "4 cases from 4 lines")
        self.assertIn("1 scorable now", out[1])
        self.assertIn("2 provisional (1 with their query, 1 numbers only)", out[2])
        self.assertIn("1 no golden", out[3])

    def test_a_dropped_line_shows_in_the_headline(self):
        out = ic.summarize([case()], lines=50)
        self.assertEqual(out[0], "1 cases from 50 lines")


if __name__ == "__main__":
    unittest.main()
