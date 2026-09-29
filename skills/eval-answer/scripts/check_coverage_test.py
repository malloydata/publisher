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
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import check_coverage as cc  # noqa: E402


class Vocabulary(unittest.TestCase):
    """The verdicts are eval-diagnose's codes, not a second vocabulary."""

    def test_every_failing_verdict_is_a_real_cause_code(self):
        codes = cc.diagnose_codes()
        for v in cc.FAIL_VERDICTS:
            self.assertIn(v, codes, v)

    def test_modelled_is_not_a_cause_code(self):
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
        body = {"why": "because", "verdict": "MISSING", "entities": []}
        body.update(obj)
        return cc.parse_reply(json.dumps(body), self.allowed, **over) \
            if over else cc.parse_reply(json.dumps(body), self.allowed)

    def test_a_failing_verdict_is_taken(self):
        r = self.parse({"verdict": "RULE_UNWRITTEN", "why": "no denominator"})
        self.assertEqual(r["verdict"], "RULE_UNWRITTEN")
        self.assertEqual(r["why"], "no denominator")

    def test_modelled_needs_a_named_entity(self):
        # `ok` is the only verdict a reader can check against the model, and it
        # is only checkable if it says which entity expresses the answer.
        r = self.parse({"verdict": "MODELLED", "entities": []})
        self.assertIsNone(r["verdict"])
        self.assertIn("without naming an entity", r["why"])

    def test_modelled_with_an_entity_is_taken(self):
        r = self.parse({"verdict": "MODELLED", "entities": ["measure:x:total"]})
        self.assertEqual(r["verdict"], "MODELLED")
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
        r = cc.parse_reply('{"verdict": "MODELLED",,}', self.allowed)
        self.assertIsNone(r["verdict"])

    def test_a_non_list_entities_field_does_not_crash(self):
        r = cc.parse_reply('{"why": "w", "verdict": "MISSING", '
                           '"entities": "measure:x"}', self.allowed)
        self.assertEqual(r["verdict"], "MISSING")
        self.assertEqual(r["entities"], [])

    def test_quoted_model_text_does_not_swallow_the_verdict(self):
        # The regression this replaced `\{.*\}` for: the prompt hands the agent
        # the model source, so a reply that quotes one `extend { ... }` block
        # used to match from THAT brace to the last one and read as unparseable.
        r = cc.parse_reply(
            'The model says `source: x is t extend { measure: c is count() }`, '
            'so:\n{"why": "w", "verdict": "MISSING", "entities": []}',
            self.allowed)
        self.assertEqual(r["verdict"], "MISSING")

    def test_the_object_carrying_a_verdict_wins_over_earlier_json(self):
        r = cc.parse_reply(
            'Shape I will use: {"why": "...", "entities": []}\n'
            '{"why": "real", "verdict": "AMBIGUOUS", "entities": []}',
            self.allowed)
        self.assertEqual(r["verdict"], "AMBIGUOUS")
        self.assertEqual(r["why"], "real")


class Summary(unittest.TestCase):
    def rows(self, *verdicts):
        return [{"qid": f"q{i}", "verdict": v, "why": "", "entities": []}
                for i, v in enumerate(verdicts)]

    def test_coverage_is_over_decided_cases_only(self):
        # An undecided case is not evidence either way; counting it as a gap
        # would make a flaky judgement look like a model regression.
        s = cc.summarise(self.rows("MODELLED", "MODELLED", "MISSING", None))
        self.assertEqual((s["cases"], s["decided"], s["ok"]), (4, 3, 2))
        self.assertAlmostEqual(s["coverage"], 2 / 3)

    def test_every_verdict_is_counted(self):
        s = cc.summarise(self.rows("MODELLED", "RULE_UNWRITTEN", "RULE_UNWRITTEN", None))
        self.assertEqual(s["by_verdict"],
                         {"MODELLED": 1, "RULE_UNWRITTEN": 2, "undecided": 1})

    def test_nothing_decided_gives_no_percentage(self):
        s = cc.summarise(self.rows(None, None))
        self.assertIsNone(s["coverage"])

    def test_no_rows_at_all(self):
        s = cc.summarise([])
        self.assertIsNone(s["coverage"])
        self.assertEqual(s["decided"], 0)


class Report(unittest.TestCase):
    def test_the_line_carries_the_version_and_the_counts(self):
        rows = [{"qid": "q1", "verdict": "MODELLED", "why": "measure:x", "entities": []},
                {"qid": "q2", "verdict": "RULE_UNWRITTEN", "why": "no denominator",
                 "entities": []}]
        text = cc.report(rows, cc.summarise(rows), "0.0.58")
        self.assertIn("coverage 50%", text)
        self.assertIn("version 0.0.58", text)
        self.assertIn("RULE_UNWRITTEN 1", text)
        self.assertIn("no denominator", text)

    def test_an_undecided_case_is_visible_not_hidden(self):
        rows = [{"qid": "q1", "verdict": None, "why": "no reply", "entities": []}]
        text = cc.report(rows, cc.summarise(rows), None)
        self.assertIn("undecided", text)
        self.assertIn("coverage n/a", text)


class Fixture(unittest.TestCase):
    """The fixture must stay a real test of expressibility."""

    def test_it_expects_a_non_modelled_verdict(self):
        self.assertNotIn(cc.OK, cc.FIXTURE_EXPECTED)
        for v in cc.FIXTURE_EXPECTED:
            self.assertIn(v, cc.FAIL_VERDICTS)

    def test_the_model_names_the_numerator(self):
        # This is what makes it a test: a checker that matches field names
        # against the question finds the numerator and says `ok`.
        self.assertIn("first_contact_resolutions", cc.FIXTURE_MODEL)

    def test_the_model_names_no_denominator_for_the_question(self):
        # Three candidate populations, none of them named as THE denominator
        # of first-contact resolution, and nothing saying which is meant.
        for candidate in ("ticket_count", "answered_ticket_count",
                          "is_actionable"):
            with self.subTest(candidate=candidate):
                self.assertIn(candidate, cc.FIXTURE_MODEL)
        for named_rate in ("first_contact_resolution_rate", "fcr_rate",
                           "first_contact_rate"):
            with self.subTest(named_rate=named_rate):
                self.assertNotIn(named_rate, cc.FIXTURE_MODEL)

    def test_a_label_resembles_the_question_while_measuring_something_else(self):
        # Rule 1 of the prompt: a question's words resembling a field's label
        # is not the model resolving anything. `resolution_rate` is the trap a
        # name-matching checker grabs, and its own doc says it is not this.
        self.assertIn("resolution_rate", cc.FIXTURE_MODEL)
        self.assertIn("NOT first-contact resolution", cc.FIXTURE_MODEL)

    def test_the_question_asks_for_a_ratio(self):
        q = cc.FIXTURE_CASE["question"].lower()
        self.assertTrue(any(w in q for w in ("rate", "share", "percentage")),
                        f"the fixture question must ask for a ratio: {q!r}")

    def test_the_fixture_carries_no_customer_model(self):
        # It ships in a public repo. The shape is what is under test, so there
        # is never a reason for the excerpt to be someone's real model.
        blob = cc.FIXTURE_MODEL + repr(cc.FIXTURE_CASE)
        import re as _re
        self.assertIsNone(
            _re.search(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
                       r"[0-9a-f]{4}-[0-9a-f]{12}", blob),
            "the fixture must not carry a real report or resource id")


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
                         {"model", "surface", "conventions", "question", "concepts"})

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
                "resolved_by": None, "why": "maps directly", "verdict": "MODELLED",
                "entities": ["measure:x:first_contact_resolutions"]}
        body.update(over)
        return cc.parse_reply(json.dumps(body), cc.verdicts())

    def test_modelled_with_two_unresolved_candidates_becomes_ambiguous(self):
        r = self.reply()
        self.assertEqual(r["verdict"], "AMBIGUOUS")
        self.assertIn("several candidates", r["why"])

    def test_modelled_stands_when_the_model_resolves_it(self):
        r = self.reply(resolved_by="the measure doc names this denominator")
        self.assertEqual(r["verdict"], "MODELLED")

    def test_modelled_stands_when_each_quantity_has_one_candidate(self):
        r = self.reply(quantities={"denominator": ["answered_ticket_count"]})
        self.assertEqual(r["verdict"], "MODELLED")

    def test_a_failing_verdict_is_never_upgraded(self):
        r = self.reply(verdict="MISSING")
        self.assertEqual(r["verdict"], "MISSING")

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
        r = cc.majority(self.rows("RULE_UNWRITTEN", "MODELLED", "RULE_UNWRITTEN"))
        self.assertEqual(r["verdict"], "RULE_UNWRITTEN")
        self.assertEqual(r["samples"], ["RULE_UNWRITTEN", "MODELLED", "RULE_UNWRITTEN"])

    def test_a_tie_goes_to_the_gap_not_to_ok(self):
        # Scoring a half-and-half case as covered is the optimistic direction
        # this measurement must not drift in.
        self.assertEqual(cc.majority(self.rows("MODELLED", "RULE_UNWRITTEN"))["verdict"],
                         "RULE_UNWRITTEN")

    def test_agreement_is_reported_as_stable(self):
        self.assertTrue(cc.majority(self.rows("MODELLED", "MODELLED"))["stable"])
        self.assertFalse(cc.majority(self.rows("MODELLED", "MISSING"))["stable"])

    def test_the_kept_row_matches_the_winning_verdict(self):
        # The `why` a reader sees has to be the reasoning for the verdict
        # reported, not for a sample that lost.
        r = cc.majority(self.rows("MODELLED", "MISSING", "MISSING"))
        self.assertEqual(r["verdict"], "MISSING")
        self.assertIn(r["why"], ("w1", "w2"))

    def test_undecided_samples_do_not_win_by_default(self):
        r = cc.majority(self.rows(None, "MISSING", "MISSING"))
        self.assertEqual(r["verdict"], "MISSING")

    def test_all_undecided_stays_undecided(self):
        self.assertIsNone(cc.majority(self.rows(None, None))["verdict"])

    def test_two_different_gaps_tied_is_undecided_not_alphabetical(self):
        # `sorted()` used to hand this to CONVENTION purely because it sorts
        # before COVERAGE. Which gap it is was never decided, so the case is
        # undecided and leaves the denominator rather than reporting a coin flip.
        r = cc.majority(self.rows("RULE_UNWRITTEN", "MISSING", "MODELLED"))
        self.assertIsNone(r["verdict"])
        self.assertIn("disagreed on which gap", r["why"])
        self.assertFalse(r["stable"])

    def test_one_gap_still_beats_a_tied_ok(self):
        # The tie-to-the-gap rule above must survive the disagreement rule.
        self.assertEqual(
            cc.majority(self.rows("MISSING", "MODELLED"))["verdict"], "MISSING")


class CompareLabels(unittest.TestCase):
    """The join against the set's authored `coverage` field.

    Written because the disagreement was found by hand, once, and the finding
    that came out of it was the opposite of the first reading: the label was
    the stale side more often than the verdict was. So the join has to report
    disagreements without taking a side, and it must not be read as a score.
    """

    def cases(self):
        return [
            {"qid": "a", "coverage": "covered", "coverageNote": "total_sales"},
            {"qid": "b", "coverage": "derivable",
             "coverageNote": "no rate measure"},
            {"qid": "c", "coverage": "absent", "coverageNote": "no source"},
            {"qid": "d", "coverage": "covered", "coverageNote": "order_count"},
        ]

    def rows(self, *verdicts):
        return [{"qid": q, "verdict": v, "why": "w"}
                for q, v in zip("abcd", verdicts)]

    def test_covered_matches_ok_and_gaps_match_gaps(self):
        c = cc.compare_labels(self.rows("MODELLED", "RULE_UNWRITTEN", "MISSING", "MODELLED"),
                              self.cases())
        self.assertEqual((c["compared"], c["agree"]), (4, 4))
        self.assertEqual(c["disagree"], [])

    def test_a_label_claiming_a_gap_the_model_can_express_is_flagged(self):
        # The shape that turned out to be the common one: the model gained a
        # measure and the standing label was never revisited.
        c = cc.compare_labels(self.rows("MODELLED", "MODELLED", "MISSING", "MODELLED"),
                              self.cases())
        self.assertEqual(c["agree"], 3)
        (d,) = c["disagree"]
        self.assertEqual(d["qid"], "b")
        self.assertIn("label says gap", d["shape"])
        # The note travels with it, because triage starts by reading the note
        # against the model.
        self.assertEqual(d["coverageNote"], "no rate measure")

    def test_a_label_claiming_covered_where_the_model_has_a_gap_is_flagged(self):
        c = cc.compare_labels(self.rows("RULE_UNWRITTEN", "RULE_UNWRITTEN", "MISSING",
                                        "MODELLED"), self.cases())
        (d,) = c["disagree"]
        self.assertEqual(d["qid"], "a")
        self.assertIn("label says covered", d["shape"])

    def test_undecided_is_not_a_disagreement(self):
        # An undecided case is not evidence about the label either way, the
        # same reason `summarise` keeps it out of the denominator.
        c = cc.compare_labels(self.rows(None, "RULE_UNWRITTEN", "MISSING", "MODELLED"),
                              self.cases())
        self.assertEqual((c["compared"], c["agree"], c["disagree"]), (3, 3, []))

    def test_an_unlabelled_set_compares_nothing_rather_than_scoring_zero(self):
        # A set with no `coverage` field must not read as total disagreement.
        c = cc.compare_labels(self.rows("MODELLED", "MODELLED", "MODELLED", "MODELLED"),
                              [{"qid": q} for q in "abcd"])
        self.assertEqual((c["compared"], c["agree"]), (0, 0))
        self.assertIn("nothing to compare", cc.label_report(c))

    def test_the_report_never_calls_a_disagreement_a_checker_error(self):
        c = cc.compare_labels(self.rows("MODELLED", "MODELLED", "MISSING", "MODELLED"),
                              self.cases())
        text = cc.label_report(c)
        self.assertIn("EITHER side can be the wrong one", text)
        self.assertNotIn("accuracy", text.lower())


class NothingDecided(unittest.TestCase):
    def test_no_decided_case_is_not_zero_percent_coverage(self):
        s = cc.summarise([{"qid": "a", "verdict": None, "why": "too large"}])
        self.assertIsNone(s["coverage"])
        self.assertEqual(s["decided"], 0)



class TheComparisonSurvivesTheArtifactWrite(unittest.TestCase):
    """Reproduced by review: `--compare-labels --out` died on a tuple dict key
    after every model call had been paid for. The artifact path was untested."""

    def test_serialisable_round_trips_and_keeps_the_counts(self):
        cases = [{"qid": "a", "coverage": "covered"},
                 {"qid": "b", "coverage": "absent"}]
        rows = [{"qid": "a", "verdict": cc.OK, "why": ""},
                {"qid": "b", "verdict": "MISSING", "why": ""}]
        cmp = cc.compare_labels(rows, cases)
        text = json.dumps({"labelComparison": cc.serialisable(cmp)})
        back = json.loads(text)["labelComparison"]
        self.assertEqual(sum(m["n"] for m in back["matrix"]), 2)
        self.assertEqual(back["compared"], 2)
        # The console report still reads the tuple-keyed original.
        self.assertIn("covered", cc.label_report(cmp))

class ModelIdentity(unittest.TestCase):
    """Coverage is sold as a per-version trend, and a trend needs each point
    tied to the bytes behind it. A `--model <dir>` run stamped `version: null`
    and named no path, so two runs reading 38% and 62% could not afterwards be
    told apart -- the numbers became unciteable."""

    SRC = (pathlib.Path(__file__).resolve().parent / "check_coverage.py").read_text()

    def test_the_report_carries_the_model_source_and_sha(self):
        self.assertIn('"modelSource": model_source', self.SRC)
        self.assertIn('"modelSha256": model_sha', self.SRC)

    def test_the_sha_is_over_the_text_the_agent_was_shown(self):
        self.assertIn("hashlib.sha256(model.encode()).hexdigest()", self.SRC)

    def test_a_local_run_names_the_resolved_path(self):
        self.assertIn("str(a.model_path.resolve()) if a.model_path", self.SRC)

    def test_a_served_run_names_the_publisher_and_package(self):
        self.assertIn('f"{a.publisher} {a.environment}/{a.package}"', self.SRC)



class CompiledSurface(unittest.TestCase):
    """`compiled_surface()` lists the fields a source exposes without declaring
    them. That the judge is SHOWN the list is pinned by `MainWiring`.

    A Malloy source picks up every column of its table, so retail_price and
    signup_date appear nowhere in the .malloy and are fully queryable. Shown
    only source text, the judge called both absent and returned MISSING (then
    named COVERAGE) on two questions the model answers correctly -- two of
    three false gaps on one measured set.
    """

    def test_it_lists_fields_per_source(self):
        with mock.patch.object(cc, "compiled_entities", return_value={
                "products": {"source:products", "dimension:retail_price",
                             "dimension:cost"},
                "customers": {"source:customers", "dimension:signup_date"}}):
            out = cc.compiled_surface("http://p", "env", "pkg")
        self.assertIn("products: dimension:cost, dimension:retail_price", out)
        self.assertIn("customers: dimension:signup_date", out)
        # The source marker is not a field and would only add noise.
        self.assertNotIn("source:products", out)

    def test_an_unreadable_model_is_empty_not_a_guess(self):
        with mock.patch.object(cc, "compiled_entities", return_value=None):
            self.assertEqual(cc.compiled_surface("http://p", "env", "pkg"), "")


class ConventionsInThePrompt(unittest.TestCase):
    """The prompt states what the question's words mean to the business.

    Without it the judge substitutes its own reading of an ambiguous word --
    "customers" as anyone with an order line -- and passes a question the model
    demonstrably cannot answer, which is the failure this check exists to find.
    """

    def test_the_prompt_tells_the_judge_not_to_substitute_its_own_reading(self):
        self.assertIn("not a reading you may substitute", cc.PROMPT)
        self.assertIn("RULE_UNWRITTEN", cc.PROMPT)

    def test_the_prompt_tells_the_judge_conventions_are_scoped(self):
        """Applied to everything, they turn every question into a gap.

        Measured: conventions stated as bare rules were read as defaults, and a
        question about the peak revenue MONTH came back RULE_UNWRITTEN because
        the set defines "net" somewhere. Nine of twelve cases became gaps and
        the measurement stopped discriminating. Whether a judge obeys this
        is a property of the judge, which no test here can reach."""
        self.assertIn("definitions of TERMS, not defaults", cc.PROMPT)
        self.assertIn("ignore the ones it", cc.PROMPT)

    def test_the_prompt_carries_no_retired_code(self):
        """A prompt naming both vocabularies teaches the judge neither."""
        for retired in ("`ok`", "COVERAGE", "NO-DISAMBIG", "CONVENTION"):
            self.assertNotIn(retired, cc.PROMPT, f"{retired} is retired")

    def test_no_skill_teaches_a_retired_code(self):
        """The same, for every SKILL.md served to an agent.

        The rename once stopped at the prompt, and four skill files went on
        defining `COVERAGE`, `NO-DISAMBIG` and `CONVENTION` as the codes to
        use. The mapping from old to new lives in eval-answer's
        reference/ledger-schema.md, which is not a SKILL.md.
        """
        found = []
        for f in sorted(cc.SKILLS_ROOT.glob("*/SKILL.md")):
            for n, line in enumerate(f.read_text().splitlines(), 1):
                for retired in ("`COVERAGE`", "`NO-DISAMBIG`", "`CONVENTION`"):
                    if retired in line:
                        found.append(f"{f.parent.name}/SKILL.md:{n} {retired}")
        self.assertEqual(found, [])

    def test_the_prompt_says_undocumented_is_not_absent(self):
        self.assertIn("undocumented, not absent", cc.PROMPT)


class MainWiring(unittest.TestCase):
    """`main()` end to end, with the REST reads and the judge stubbed.

    The pure functions are guarded above. These are what fail if `main()` stops
    fetching the compiled surface, stops reading the set's conventions, or
    `judge_case` stops putting either in the prompt -- each of which once
    passed the whole suite.
    """

    SURFACE = {"products": {"source:products", "dimension:retail_price"}}
    CONVENTION = ('"Summer" means 25 May to 15 September. Applies to '
                  'questions that ask about summer.')

    def run_main(self, replies, *, conventions=None, surface=SURFACE,
                 surface_raises=None, extra=()):
        """Run `main()` over a two-case set. `replies` maps qid -> the judge's
        JSON. Returns (the prompts sent, the --out report, stderr)."""
        d = pathlib.Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, d)
        meta = {"name": "t"}
        if conventions is not None:
            meta["conventions"] = conventions
        (d / "set.json").write_text(json.dumps(meta))
        (d / "cases.jsonl").write_text("".join(
            json.dumps({"qid": q, "question": f"question {q}?"}) + "\n"
            for q in replies))
        prompts = {}

        def fake_cli(cmd, **kw):
            prompt = cmd[2]
            qid = next(q for q in replies if f"question {q}?" in prompt)
            prompts[qid] = prompt
            return [{"type": "assistant"}], json.dumps(replies[qid]), "", 1, 0.1

        entities = mock.Mock(side_effect=surface_raises, return_value=surface)
        err = []
        with mock.patch.object(cc, "rest_model_text",
                               return_value="source: products is t"), \
             mock.patch.object(cc, "compiled_entities", entities), \
             mock.patch.object(cc, "run_cli", side_effect=fake_cli), \
             mock.patch("builtins.print",
                        lambda *a, **k: err.append(" ".join(map(str, a)))
                        if k.get("file") is sys.stderr else None):
            cc.main(["--set", str(d), "--publisher", "http://p",
                     "--package", "pkg", "--parallel", "1",
                     "--out", str(d / "out.json"), *extra])
        return prompts, json.loads((d / "out.json").read_text()), "\n".join(err)

    OK_REPLY = {"verdict": "MODELLED", "why": "w",
                "entities": ["products.retail_price"]}

    def test_the_judge_is_shown_the_compiled_surface(self):
        prompts, out, _ = self.run_main({"a": self.OK_REPLY})
        self.assertIn("products: dimension:retail_price", prompts["a"])
        self.assertEqual(out["compiledSurface"], "read")

    def test_the_judge_is_shown_the_sets_conventions(self):
        prompts, out, _ = self.run_main({"a": self.OK_REPLY},
                                        conventions=[self.CONVENTION])
        self.assertIn(f"- {self.CONVENTION}", prompts["a"])
        self.assertEqual(out["conventions"], [self.CONVENTION])

    def test_a_failed_surface_read_is_recorded_in_the_report(self):
        """In --publisher mode a REST failure used to read as a normal run."""
        prompts, out, err = self.run_main(
            {"a": self.OK_REPLY}, surface_raises=OSError("connection refused"))
        self.assertEqual(out["compiledSurface"], "failed: connection refused")
        self.assertIn("(unavailable)", prompts["a"])
        self.assertIn("no compiled field list", err)

    def test_an_unscoped_convention_is_warned_about(self):
        _, _, err = self.run_main({"a": self.OK_REPLY},
                                  conventions=['"Net" excludes returns.'])
        self.assertIn("conventions[0] states no scope", err)
        _, _, err = self.run_main({"a": self.OK_REPLY},
                                  conventions=[self.CONVENTION])
        self.assertNotIn("states no scope", err)

    def test_underspecified_is_a_decided_gap_not_an_undecided_case(self):
        """Out of FAIL_VERDICTS it would parse as undecided and silently leave
        the denominator."""
        _, out, _ = self.run_main({
            "a": self.OK_REPLY,
            "b": {"verdict": "UNDERSPECIFIED", "why": "which adjustment?",
                  "entities": []}})
        self.assertEqual((out["decided"], out["ok"]), (2, 1))
        self.assertEqual(out["by_verdict"], {"MODELLED": 1, "UNDERSPECIFIED": 1})

    def test_rule_kind_is_recorded_and_an_unqualified_one_is_unstated(self):
        _, out, _ = self.run_main({
            "a": {"verdict": "RULE_UNWRITTEN", "why": "w", "entities": [],
                  "rule_kind": "arbitrary"},
            "b": {"verdict": "RULE_UNWRITTEN", "why": "w", "entities": []}})
        kinds = {r["qid"]: r["rule_kind"] for r in out["cases_detail"]}
        self.assertEqual(kinds, {"a": "arbitrary", "b": "unstated"})


class SetConventions(unittest.TestCase):
    def conv(self, meta):
        d = pathlib.Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, d)
        if meta is not None:
            (d / "set.json").write_text(meta if isinstance(meta, str)
                                        else json.dumps(meta))
        return cc.set_conventions(d)

    def test_shapes(self):
        self.assertEqual(self.conv({"conventions": ["a", "", 3, "b"]}), ["a", "b"])
        self.assertEqual(self.conv({"conventions": "one"}), ["one"])
        self.assertEqual(self.conv({"conventions": {"not": "a list"}}), [])
        self.assertEqual(self.conv({}), [])
        self.assertEqual(self.conv("not json"), [])
        self.assertEqual(self.conv(None), [])


if __name__ == "__main__":
    unittest.main()
