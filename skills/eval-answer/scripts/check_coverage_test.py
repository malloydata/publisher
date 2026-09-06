#!/usr/bin/env python3
"""Tests for the coverage measurement.

No model is spawned here: these pin the parts that decide whether a reply is
believed and how the score is computed. The judgement itself is checked by
`check_coverage.py --self-check`, which costs one call and is not run in CI."""
import json
import pathlib
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import check_coverage as cc  # noqa: E402


class Vocabulary(unittest.TestCase):
    """The verdicts are eval-diagnose's codes, not a second vocabulary."""

    def test_every_failing_verdict_is_a_real_cause_code(self):
        codes = cc.diagnose_codes()
        for v in cc.FAIL_VERDICTS:
            self.assertIn(v, codes, v)

    def test_ok_is_not_a_cause_code(self):
        # `ok` means there is nothing to diagnose, so it is deliberately not in
        # that table and must not be looked for there.
        self.assertNotIn(cc.OK, cc.diagnose_codes())

    def test_a_missing_table_fails_loudly(self):
        tmp = pathlib.Path(tempfile.mkdtemp())
        try:
            with self.assertRaises(SystemExit):
                cc.diagnose_codes(tmp)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


class ParseReply(unittest.TestCase):
    def setUp(self):
        self.allowed = cc.verdicts()

    def parse(self, obj, **over):
        body = {"why": "because", "verdict": "COVERAGE", "entities": []}
        body.update(obj)
        return cc.parse_reply(json.dumps(body), self.allowed, **over) \
            if over else cc.parse_reply(json.dumps(body), self.allowed)

    def test_a_failing_verdict_is_taken(self):
        r = self.parse({"verdict": "CONVENTION", "why": "no denominator"})
        self.assertEqual(r["verdict"], "CONVENTION")
        self.assertEqual(r["why"], "no denominator")

    def test_ok_needs_a_named_entity(self):
        # `ok` is the only verdict a reader can check against the model, and it
        # is only checkable if it says which entity expresses the answer.
        r = self.parse({"verdict": "ok", "entities": []})
        self.assertIsNone(r["verdict"])
        self.assertIn("without naming an entity", r["why"])

    def test_ok_with_an_entity_is_taken(self):
        r = self.parse({"verdict": "ok", "entities": ["measure:x:total"]})
        self.assertEqual(r["verdict"], "ok")
        self.assertEqual(r["entities"], ["measure:x:total"])

    def test_an_invented_verdict_is_dropped(self):
        r = self.parse({"verdict": "PARTIAL"})
        self.assertIsNone(r["verdict"])
        self.assertIn("PARTIAL", r["why"])

    def test_prose_around_the_json_is_tolerated(self):
        r = cc.parse_reply(
            'Here is my answer:\n{"why": "w", "verdict": "AMBIGUOUS", '
            '"entities": []}\nHope that helps.', self.allowed)
        self.assertEqual(r["verdict"], "AMBIGUOUS")

    def test_no_json_at_all(self):
        r = cc.parse_reply("I could not decide.", self.allowed)
        self.assertIsNone(r["verdict"])

    def test_unparseable_json(self):
        r = cc.parse_reply('{"verdict": "ok",,}', self.allowed)
        self.assertIsNone(r["verdict"])

    def test_a_non_list_entities_field_does_not_crash(self):
        r = cc.parse_reply('{"why": "w", "verdict": "COVERAGE", '
                           '"entities": "measure:x"}', self.allowed)
        self.assertEqual(r["verdict"], "COVERAGE")
        self.assertEqual(r["entities"], [])


class Summary(unittest.TestCase):
    def rows(self, *verdicts):
        return [{"qid": f"q{i}", "verdict": v, "why": "", "entities": []}
                for i, v in enumerate(verdicts)]

    def test_coverage_is_over_decided_cases_only(self):
        # An undecided case is not evidence either way; counting it as a gap
        # would make a flaky judgement look like a model regression.
        s = cc.summarise(self.rows("ok", "ok", "COVERAGE", None))
        self.assertEqual((s["cases"], s["decided"], s["ok"]), (4, 3, 2))
        self.assertAlmostEqual(s["coverage"], 2 / 3)

    def test_every_verdict_is_counted(self):
        s = cc.summarise(self.rows("ok", "CONVENTION", "CONVENTION", None))
        self.assertEqual(s["by_verdict"],
                         {"ok": 1, "CONVENTION": 2, "undecided": 1})

    def test_nothing_decided_gives_no_percentage(self):
        s = cc.summarise(self.rows(None, None))
        self.assertIsNone(s["coverage"])

    def test_no_rows_at_all(self):
        s = cc.summarise([])
        self.assertIsNone(s["coverage"])
        self.assertEqual(s["decided"], 0)


class Report(unittest.TestCase):
    def test_the_line_carries_the_version_and_the_counts(self):
        rows = [{"qid": "q1", "verdict": "ok", "why": "measure:x", "entities": []},
                {"qid": "q2", "verdict": "CONVENTION", "why": "no denominator",
                 "entities": []}]
        text = cc.report(rows, cc.summarise(rows), "0.0.58")
        self.assertIn("coverage 50%", text)
        self.assertIn("version 0.0.58", text)
        self.assertIn("CONVENTION 1", text)
        self.assertIn("no denominator", text)

    def test_an_undecided_case_is_visible_not_hidden(self):
        rows = [{"qid": "q1", "verdict": None, "why": "no reply", "entities": []}]
        text = cc.report(rows, cc.summarise(rows), None)
        self.assertIn("undecided", text)
        self.assertIn("coverage n/a", text)


class Fixture(unittest.TestCase):
    """The fixture must stay a real test of expressibility."""

    def test_it_expects_a_non_ok_verdict(self):
        self.assertNotIn(cc.OK, cc.FIXTURE_EXPECTED)
        for v in cc.FIXTURE_EXPECTED:
            self.assertIn(v, cc.FAIL_VERDICTS)

    def test_the_model_names_the_numerator_but_no_reach_total(self):
        # This is what makes it a test: a checker that matches field names
        # against the question finds the numerator and says `ok`.
        self.assertIn("first_contact_resolutions", cc.FIXTURE_MODEL)
        self.assertIn("is_actionable", cc.FIXTURE_MODEL)
        self.assertIn("answered_ticket_count", cc.FIXTURE_MODEL)
        self.assertNotIn("first_contact_resolution_rate", cc.FIXTURE_MODEL)
        self.assertNotIn("first_contact_resolution_rate", cc.FIXTURE_MODEL)

    def test_the_question_asks_for_a_share(self):
        self.assertTrue(any(w in cc.FIXTURE_CASE["question"].lower()
                            for w in ("rate", "share")))


class Prompt(unittest.TestCase):
    def test_it_forbids_answering_and_names_the_ratio_rule(self):
        self.assertIn("not answering the question", cc.PROMPT)
        self.assertIn("Finding the numerator is not coverage", cc.PROMPT)

    def test_it_offers_exactly_the_allowed_verdicts(self):
        for v in cc.verdicts():
            self.assertIn(v, cc.PROMPT)

    def test_it_takes_the_fields_judge_case_formats(self):
        import re
        self.assertEqual(set(re.findall(r"\{(\w+)\}", cc.PROMPT)),
                         {"model", "question", "concepts"})

    def test_it_asks_for_the_enumeration_the_verdict_rests_on(self):
        self.assertIn("list every candidate in the model", cc.PROMPT.lower())
        self.assertIn("quantities", cc.PROMPT)
        self.assertIn("resolved_by", cc.PROMPT)


class UnresolvedCandidates(unittest.TestCase):
    """An `ok` that contradicts its own enumeration is not taken.

    This is the rule the fixture holds: a first run of it came back `ok`
    because the agent matched "answered ticket population" to a measure labelled "Universe
    Estimate" and stopped looking, while a second denominator was equally
    available and gave a materially different number."""

    def reply(self, **over):
        body = {"quantities": {"denominator": ["answered_ticket_count",
                                               "unfiltered first_contact_resolutions"]},
                "resolved_by": None, "why": "maps directly", "verdict": "ok",
                "entities": ["measure:x:first_contact_resolutions"]}
        body.update(over)
        return cc.parse_reply(json.dumps(body), cc.verdicts())

    def test_ok_with_two_unresolved_candidates_becomes_no_disambig(self):
        r = self.reply()
        self.assertEqual(r["verdict"], "NO-DISAMBIG")
        self.assertIn("several candidates", r["why"])

    def test_ok_stands_when_the_model_resolves_it(self):
        r = self.reply(resolved_by="the measure doc names this denominator")
        self.assertEqual(r["verdict"], "ok")

    def test_ok_stands_when_each_quantity_has_one_candidate(self):
        r = self.reply(quantities={"denominator": ["answered_ticket_count"]})
        self.assertEqual(r["verdict"], "ok")

    def test_a_failing_verdict_is_never_upgraded(self):
        r = self.reply(verdict="COVERAGE")
        self.assertEqual(r["verdict"], "COVERAGE")

    def test_the_enumeration_is_kept_on_the_record(self):
        r = self.reply(resolved_by="a doc")
        self.assertIn("denominator", r["quantities"])
class Majority(unittest.TestCase):
    """A case on the line is arguable, not whichever way it fell."""

    def rows(self, *verdicts):
        return [{"qid": "q", "verdict": v, "why": f"w{i}", "entities": [],
                 "quantities": {}, "resolved_by": None}
                for i, v in enumerate(verdicts)]

    def test_the_majority_wins(self):
        r = cc.majority(self.rows("CONVENTION", "ok", "CONVENTION"))
        self.assertEqual(r["verdict"], "CONVENTION")
        self.assertEqual(r["samples"], ["CONVENTION", "ok", "CONVENTION"])

    def test_a_tie_goes_to_the_gap_not_to_ok(self):
        # Scoring a half-and-half case as covered is the optimistic direction
        # this measurement must not drift in.
        self.assertEqual(cc.majority(self.rows("ok", "CONVENTION"))["verdict"],
                         "CONVENTION")

    def test_agreement_is_reported_as_stable(self):
        self.assertTrue(cc.majority(self.rows("ok", "ok"))["stable"])
        self.assertFalse(cc.majority(self.rows("ok", "COVERAGE"))["stable"])

    def test_the_kept_row_matches_the_winning_verdict(self):
        # The `why` a reader sees has to be the reasoning for the verdict
        # reported, not for a sample that lost.
        r = cc.majority(self.rows("ok", "COVERAGE", "COVERAGE"))
        self.assertEqual(r["verdict"], "COVERAGE")
        self.assertIn(r["why"], ("w1", "w2"))

    def test_undecided_samples_do_not_win_by_default(self):
        r = cc.majority(self.rows(None, "COVERAGE", "COVERAGE"))
        self.assertEqual(r["verdict"], "COVERAGE")

    def test_all_undecided_stays_undecided(self):
        self.assertIsNone(cc.majority(self.rows(None, None))["verdict"])


if __name__ == "__main__":
    unittest.main()
