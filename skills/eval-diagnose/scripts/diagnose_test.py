#!/usr/bin/env python3
"""Tests for salvaging a per-case diagnosis out of a cluster-shaped reply.

Both prompts used to end with the same sentence pointing at the same reference
file, and that file defines two objects. 11 of 28 per-case replies in one run
came back as the clustering object: owner, component, codes and a root cause,
all real, in the wrong envelope, and every one of them discarded by a validator
looking for the per-case keys. About $12 of that run's $20.

The prompts now name their section, which is the actual fix. Salvage is the
belt: it lifts what is present and, deliberately, does not invent `probes`.
That last part is the one worth a test, because fabricating the record that
something was checked is how a shape error becomes a false claim."""
import pathlib
import sys
import unittest

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import diagnose  # noqa: E402

CLUSTER_REPLY = {
    "clusters": [{
        "cluster_id": "index-values-not-whole",
        "qids": ["q1", "q2"],
        "owner": "model",
        "component": "model-definition",
        "codes": ["RULE_UNWRITTEN", "AMBIGUOUS"],
        "rootCause": "the index measure returns long decimals",
        "evidence": "28 other places use render tags",
        "confidence": "high",
    }],
    "reasoning": "considered merging with the rounding cluster",
}


class SalvageClusterShape(unittest.TestCase):
    def test_lifts_the_fields_that_are_present(self):
        out = diagnose.salvage_cluster_shape(CLUSTER_REPLY)
        self.assertEqual(out["owner"], "model")
        self.assertEqual(out["component"], "model-definition")
        self.assertEqual(out["primary_code"], "RULE_UNWRITTEN")
        self.assertEqual(out["contributing_codes"], ["AMBIGUOUS"])
        self.assertEqual(out["diagnosis"], "the index measure returns long decimals")

    def test_never_synthesises_probes(self):
        # The field that must not be fabricated: it is the record that
        # something was checked, and the cluster shape has no probe records.
        out = diagnose.salvage_cluster_shape(CLUSTER_REPLY)
        self.assertEqual(out["probes"], [])

    def test_marks_sufficiency_unknown(self):
        # A warning carried forward, not a gate. Nothing refuses to act on it;
        # it reaches the improve step, whose prompt tells the agent to probe
        # the claim before editing on it.
        out = diagnose.salvage_cluster_shape(CLUSTER_REPLY)
        self.assertEqual(out["sufficiency"], "unknown")

    def test_a_salvaged_object_still_fails_validation_on_probes(self):
        # Salvage must not launder a diagnosis into looking probed. The
        # vocabulary checks pass; "no probes recorded" must still fire.
        codes = diagnose.skill_codes()
        self.assertIn("RULE_UNWRITTEN", codes,
                      "the skill's own code table should define "
                      "RULE_UNWRITTEN; if it was renamed this test needs the "
                      "new name")
        out = diagnose.salvage_cluster_shape(CLUSTER_REPLY)
        bad = diagnose.validate(out, codes)
        self.assertEqual(bad, ["no probes recorded"],
                         "the vocabulary fields should all pass after salvage, "
                         "and the missing probe record should be the only "
                         "finding left")

    def test_says_it_was_salvaged(self):
        out = diagnose.salvage_cluster_shape(CLUSTER_REPLY)
        self.assertIn("clustering shape", out["_salvaged"])

    def test_records_when_more_than_one_cluster_was_offered(self):
        # Several clusters means the agent answered about the whole run from
        # one case's evidence; that must not read as a clean single cause.
        reply = {"clusters": [CLUSTER_REPLY["clusters"][0],
                              {"cluster_id": "other", "owner": "retrieval"}]}
        out = diagnose.salvage_cluster_shape(reply)
        self.assertIn("2 clusters", out["_salvagedFrom"])

    def test_a_per_case_reply_is_left_alone(self):
        self.assertIsNone(diagnose.salvage_cluster_shape(
            {"probes": [{"why": "w", "query": "q", "result": "r"}],
             "primary_code": "RULE_UNWRITTEN", "owner": "model"}))

    def test_an_empty_or_unparseable_reply_is_not_invented_into_a_diagnosis(self):
        for empty in ({}, {"clusters": []}, {"clusters": "not a list"},
                      {"clusters": ["a string, not an object"]}):
            self.assertIsNone(diagnose.salvage_cluster_shape(empty), empty)

    def test_a_cluster_with_no_codes_yields_no_primary_code(self):
        # Better an object the validator rejects for a missing code than one
        # carrying a code nobody chose.
        out = diagnose.salvage_cluster_shape(
            {"clusters": [{"owner": "model", "component": "model-definition"}]})
        self.assertNotIn("primary_code", out)


class ClusterIdNamesTheDefect(unittest.TestCase):
    """The contract now says an id names the defect, never the remedy.

    Pinned as text because the id is produced by an agent, not by this script,
    so the rule can only be carried in the prompt and the contract. If either
    stops saying it, the next run starts prescribing fixes again."""

    def test_the_rule_is_in_the_clustering_prompt(self):
        self.assertIn("names the DEFECT", diagnose.CLUSTER_PROMPT)
        for word in ("missing", "should", "add", "fix"):
            self.assertIn(word, diagnose.CLUSTER_PROMPT)

    def test_both_prompts_name_their_own_contract_section(self):
        # The ambiguous pointer that caused the 11 of 28. Each prompt must name
        # its section AND say the other object belongs to someone else.
        self.assertIn("## Per case", diagnose.DIAGNOSE_PROMPT)
        self.assertIn("## Per run", diagnose.DIAGNOSE_PROMPT)
        self.assertIn("## Per run, clustering", diagnose.CLUSTER_PROMPT)
        self.assertIn("## Per case", diagnose.CLUSTER_PROMPT)


class SufficiencyReachesImprove(unittest.TestCase):
    """The cluster's `sufficiency` is worst case, and improve.py reads it.

    It used to be `members[0]`, and it stopped at the diagnose step. Both were
    fine while nothing consumed it; now the improve prompt does, so a cluster
    whose first member happened to be probed must not read as probed when a
    later one was not."""

    @staticmethod
    def worst(*values):
        # The function cluster assembly calls, not a copy of it: a test that
        # re-implemented the ordering agreed with the code by construction and
        # could not have caught the crash below.
        return diagnose.worst(values, diagnose.SUFFICIENCY, "unknown")

    def test_one_unprobed_member_makes_the_cluster_unprobed(self):
        self.assertEqual(self.worst("sufficient", "unknown"), "unknown")
        self.assertEqual(self.worst("sufficient", "insufficient"),
                         "insufficient")

    def test_all_probed_stays_sufficient(self):
        self.assertEqual(self.worst("sufficient", "sufficient"), "sufficient")

    def test_cluster_assembly_uses_that_ordering_not_the_first_member(self):
        src = (pathlib.Path(diagnose.__file__)).read_text()
        self.assertIn("sufficiency=worst(", src)
        self.assertNotIn('sufficiency=members[0]', src)


class OffVocabularyValuesDoNotLoseTheRun(unittest.TestCase):
    """`validate()` says these fields can come back wrong; the aggregate has to
    survive one that did.

    `good` is filtered on `error`, not on `_invalid`, so a per-case agent
    emitting `sufficiency: "partial"` reaches cluster assembly. A bare
    `[...].index` raised `ValueError` there -- after every per-case call and the
    clustering call were paid for, and before `replace_events` and
    `diagnoses.jsonl` were written, so the whole diagnose spend was lost to a
    traceback."""

    def test_an_off_vocabulary_sufficiency_does_not_raise(self):
        self.assertEqual(
            diagnose.worst(["sufficient", "partial"], diagnose.SUFFICIENCY,
                           "unknown"),
            "unknown")

    def test_an_off_vocabulary_severity_does_not_raise(self):
        self.assertEqual(
            diagnose.worst(["low", "catastrophic"], diagnose.SEVERITY, "low"),
            "low")

    def test_an_unreadable_severity_is_not_evidence_of_a_high_one(self):
        # Read as the worst value it could be, an unreadable severity would
        # escalate a backlog item on a typo. It takes the same `low` the
        # missing-value default already took.
        self.assertEqual(
            diagnose.worst(["catastrophic"], diagnose.SEVERITY, "low"), "low")
        self.assertEqual(
            diagnose.worst(["medium", "catastrophic"], diagnose.SEVERITY,
                           "low"),
            "medium")

    def test_a_missing_value_still_reads_as_the_fallback(self):
        self.assertEqual(
            diagnose.worst([None, "sufficient"], diagnose.SUFFICIENCY,
                           "unknown"),
            "unknown")
        self.assertEqual(
            diagnose.worst([None, "high"], diagnose.SEVERITY, "low"), "high")

    def test_no_members_reads_as_the_fallback_rather_than_raising(self):
        self.assertEqual(
            diagnose.worst([], diagnose.SUFFICIENCY, "unknown"), "unknown")

    def test_the_result_is_always_in_vocabulary(self):
        # What lands in the ledger. Ranking an off-vocabulary value last and
        # then RETURNING it would have written "partial" into an issue event.
        self.assertIn(
            diagnose.worst(["partial"], diagnose.SUFFICIENCY, "unknown"),
            diagnose.SUFFICIENCY)

    def test_improve_passes_sufficiency_to_the_editing_agent(self):
        # The whole point: the value has to reach the step that acts on it.
        improve_py = (pathlib.Path(diagnose.__file__).parent.parent.parent
                      / "eval-improve" / "scripts" / "improve.py").read_text()
        self.assertIn('"sufficiency"', improve_py)
        self.assertIn("READ THE CLUSTER'S `sufficiency` BEFORE YOU EDIT",
                      improve_py)


class BehaviourStats(unittest.TestCase):
    """The measurements a behavioural root cause gets stated in.

    Diagnosis reads failures only, so a behaviour common to the whole run looks
    causal from inside it. On one measured run the largest cluster was built on
    "substitutes broad enumeration for targeted retrieval", and the enumeration
    rate was 25% of targets in the failures against 26% in the passes.

    THE FIXTURES HERE ARE THE SHAPES THE HARNESS ACTUALLY WRITES. An earlier
    version of this class invented a `{"target_type", "search_text"}` dict that
    no writer emits, and the code passed those tests while reading
    `targetsWithoutSearchText: 0` on every real attempt -- because `targets`
    holds `"<type>: <text>"` STRINGS and a target with no text is dropped
    before the ledger sees it.
    """

    def events(self, qid, tool_call, *, verdict="match", turns=9):
        return [
            {"kind": "attempt", "qid": qid, "n_get_context": 2, "n_execute": 5,
             "n_execute_errors": 1, "num_turns": turns,
             "skills_invoked": ["malloy-analysis"]},
            {"kind": "score", "qid": qid, "verdict": verdict},
            {"kind": "tool_call", "qid": qid, "tool": "get_context",
             **tool_call},
            {"kind": "tool_call", "qid": qid, "tool": "execute_query"},
        ]

    def shapes(self, *pairs):
        return {"target_shapes": [{"type": t, "has_text": h} for t, h in pairs]}

    def test_a_bare_target_is_counted_from_target_shapes(self):
        s = diagnose.behaviour_stats("q1", self.events("q1", self.shapes(
            ("dimension", False), ("measure", True), ("view", False))))
        self.assertEqual(s["searchTargets"], 3)
        self.assertEqual(s["targetsWithoutSearchText"], 2)
        self.assertTrue(s["targetsMeasured"])

    def test_it_records_which_target_types_were_asked_for(self):
        # A type-classification error is the most common agent-call defect, and
        # a falsifier that cannot reach it is not much of a falsifier -- which
        # a clustering agent said, in those terms, about the first version.
        s = diagnose.behaviour_stats("q1", self.events("q1", self.shapes(
            ("measure", True), ("dimension", True), ("dimension", False))))
        self.assertEqual(s["targetTypes"], {"measure": 1, "dimension": 2})

    def test_a_legacy_run_reads_types_but_abstains_on_the_bare_count(self):
        # Runs written before `target_shapes` existed. The types survive in the
        # `"<type>: <text>"` strings; the bare count does not, and reporting it
        # as 0 would be a falsifier quietly asserting the opposite of the truth.
        s = diagnose.behaviour_stats("q1", self.events(
            "q1", {"targets": ["measure: total revenue", "source: flights"]}))
        self.assertEqual(s["targetTypes"], {"measure": 1, "source": 1})
        self.assertIsNone(s["targetsWithoutSearchText"])
        self.assertFalse(s["targetsMeasured"])

    def test_the_two_shapes_do_not_get_mixed(self):
        # If both are present the measured one wins; the legacy strings are a
        # lossy view of the same call.
        s = diagnose.behaviour_stats("q1", self.events("q1", {
            "targets": ["measure: total revenue"],
            **self.shapes(("measure", True), ("dimension", False))}))
        self.assertEqual(s["searchTargets"], 2)
        self.assertEqual(s["targetsWithoutSearchText"], 1)

    def test_it_carries_the_verdict_so_the_groups_are_comparable(self):
        s = diagnose.behaviour_stats("q1", self.events("q1", self.shapes(),
                                                       verdict="match"))
        self.assertEqual(s["verdict"], "match")
        self.assertEqual(s["skillsInvoked"], ["malloy-analysis"])
        self.assertEqual(s["numTurns"], 9)

    def test_only_this_case_is_measured(self):
        ev = self.events("q1", self.shapes(("dimension", True))) + \
            self.events("q2", self.shapes(("a", True), ("b", True)))
        self.assertEqual(
            diagnose.behaviour_stats("q1", ev)["searchTargets"], 1)

    def test_an_attempt_with_no_calls_is_still_a_row(self):
        s = diagnose.behaviour_stats("q9", [])
        self.assertEqual(s["qid"], "q9")
        self.assertEqual(s["searchTargets"], 0)

    def test_the_clustering_prompt_asks_for_the_falsification(self):
        self.assertIn("{controls}", diagnose.CLUSTER_PROMPT)


class ControlsBlock(unittest.TestCase):
    """The CONTROLS section sizes itself, because the instruction that is right
    for twenty passing cases is wrong for one and meaningless for none."""

    def block(self, n):
        return diagnose.controls_block(
            [{"qid": f"q{i}", "verdict": "match"} for i in range(n)])

    def test_a_run_where_everything_failed_says_there_is_nothing_to_compare(self):
        # 10 of 10 failing is a real shape, and asking the agent to compare
        # rates against an empty list invites it to read [] as evidence.
        b = self.block(0)
        self.assertIn("CONTROLS: none", b)
        self.assertIn("unfalsified", b)
        self.assertIn("Do NOT read the absence of controls", b)
        # A model-fact cluster is not affected by having no controls, and the
        # block has to say so or every cluster gets downgraded.
        self.assertIn("unaffected", b)

    def test_one_or_two_controls_is_not_a_rate(self):
        for n in (1, 2):
            with self.subTest(n=n):
                b = self.block(n)
                self.assertIn("is NOT a rate", b)
                self.assertIn("Do not compute a percentage", b)

    def test_three_or_more_gets_the_real_comparison(self):
        b = self.block(3)
        self.assertIn("similar rate in the passes", b)
        self.assertNotIn("is NOT a rate", b)

    def test_the_prompt_renders_at_every_size(self):
        # The failure this guards: an unrendered {controls} shipping to the
        # agent, or a KeyError at the one moment the run cannot be redone.
        for n in (0, 1, 2, 3, 20):
            with self.subTest(n=n):
                out = diagnose.CLUSTER_PROMPT.format(
                    issues="[]", controls=self.block(n))
                self.assertNotIn("{controls}", out)
                self.assertNotIn("{issues}", out)

    def test_every_size_carries_the_rows_it_has(self):
        self.assertIn('"qid": "q0"', self.block(1))
        self.assertNotIn('"qid"', self.block(0))


class RetrievalMissOnAPassingCase(unittest.TestCase):
    """A correct answer that never received a required entity is a finding.

    It was silently dropped in three places: `attribute()` short-circuited on
    `passed`, `summarise()` dropped the empty label, and `diagnose.py` routed
    `match` to a control group. Measured on one run, 3 of 4 findings sat on
    passing cases and produced nothing.
    """

    def case(self, qid="q", required=("measure:m:total",)):
        return {"qid": qid, "coverage": "covered",
                "expectedEntities": {"required": list(required)}}

    def events(self, qid, returned, targets):
        return [
            {"kind": "attempt", "qid": qid, "sample": None, "phase": "baseline"},
            {"kind": "score", "qid": qid, "sample": None, "phase": "baseline",
             "verdict": "match", "reason": "ok"},
            {"kind": "tool_call", "qid": qid, "sample": None, "phase": "baseline",
             "tool": "get_context",
             "rankedSummary": {"entityIds": list(returned)},
             "target_shapes": [{"type": t, "has_text": True} for t in targets]},
        ]

    def test_a_pass_that_missed_an_entity_yields_a_finding(self):
        ev = self.events("q", [], ["source", "dimension"])
        row = diagnose.retrieval_finding(self.case(), ev,
                                         ("q", None, "baseline"), "match")
        self.assertIsNotNone(row)
        # Deterministic: it needed a measure and sent no measure target.
        self.assertIn("never asked", row["where_to_fix"])
        # And the answer is still not a failure.
        self.assertFalse(row["failed"])

    def test_a_pass_that_received_everything_yields_nothing(self):
        ev = self.events("q", ["measure:m:total"], ["measure"])
        self.assertIsNone(diagnose.retrieval_finding(
            self.case(), ev, ("q", None, "baseline"), "match"))

    def test_it_calls_the_real_scorer_rather_than_restating_the_rule(self):
        # Two definitions of "a miss" would drift, and the run summary and the
        # diagnosis would then disagree about what happened.
        src = pathlib.Path(diagnose.__file__).read_text()
        self.assertIn("score_retrieval.score_case", src)

    def test_the_prompt_says_the_answer_was_correct(self):
        # Without it the diagnoser reads the evidence as a wrong answer and
        # goes looking for a number that is not wrong.
        src = pathlib.Path(diagnose.__file__).read_text()
        self.assertIn("THIS ANSWER WAS CORRECT", src)
        self.assertIn("answered_correctly", src)

    def test_there_is_an_opt_out(self):
        src = pathlib.Path(diagnose.__file__).read_text()
        self.assertIn("--no-retrieval-misses", src)

    def test_the_coverage_denominator_excludes_them(self):
        # They are passes. Counting a diagnosed pass against a non-passing
        # denominator printed "2 of 1 non-passing case(s) diagnosed (200%)".
        src = pathlib.Path(diagnose.__file__).read_text()
        self.assertIn("good_failures", src)


class SelectingWhatToDiagnose(unittest.TestCase):
    """Every scored case lands in exactly one bucket, and narrowing what gets
    diagnosed does not change what the run failed."""

    def cases(self, n, split=None):
        return {f"q{i}": {"qid": f"q{i}", "coverage": "covered",
                          **({"split": split} if split else {})}
                for i in range(n)}

    def scores(self, n, verdict="no_match"):
        return [{"kind": "score", "qid": f"q{i}", "sample": None,
                 "phase": "baseline", "verdict": verdict, "reason": "r"}
                for i in range(n)]

    def select(self, events, cases, **kw):
        """(failed, passed, retrieval_only, excluded) -- the four buckets most
        of these assert on. `excluded_passes` has its own tests below."""
        return diagnose.select_cases(events, cases, ("no_match",), **kw)[:4]

    def test_limit_records_what_it_dropped_as_an_exclusion(self):
        # Truncating `failed` in place moved numerator and denominator
        # together: 23 failures with --limit 5 printed "5 of 5 (100%)".
        failed, _, _, excluded = self.select(
            self.scores(23), self.cases(23), limit=5)
        self.assertEqual(len(failed), 5)
        not_passing = len(failed) + sum(len(v) for v in excluded.values())
        self.assertEqual(not_passing, 23)
        self.assertEqual(len(excluded["beyond --limit 5"]), 18)

    def test_limit_zero_selects_nothing_rather_than_everything(self):
        """`if limit:` read 0 as "no limit" and diagnosed all 23, one billable
        agent each -- failing OPEN on the one axis where that costs money.
        0 means zero; unlimited is spelled by omitting the flag."""
        failed, _, _, excluded = self.select(
            self.scores(23), self.cases(23), limit=0)
        self.assertEqual(failed, [])
        self.assertEqual(len(excluded["beyond --limit 0"]), 23)

    def test_omitting_the_limit_still_means_unlimited(self):
        failed, _, _, excluded = self.select(self.scores(23), self.cases(23))
        self.assertEqual(len(failed), 23)
        self.assertEqual(excluded, {})

    def test_a_holdout_PASS_is_not_a_non_passing_case(self):
        """The invariant, for the branch that still broke it.

        The holdout test sits above the verdict checks on purpose, so the
        split cannot depend on the score. That sent a holdout case that
        PASSED into `excluded`, which `not_passing` sums: a real run with one
        failure and four holdout passes printed "1 of 5 non-passing case(s)
        diagnosed (20%)" when it had diagnosed the only failure there was.
        """
        events = self.scores(1, verdict="no_match") + [
            {"kind": "score", "qid": f"h{i}", "sample": None,
             "phase": "baseline", "verdict": "match", "reason": "r"}
            for i in range(4)]
        cases = dict(self.cases(1))
        for i in range(4):
            cases[f"h{i}"] = {"qid": f"h{i}", "split": "holdout"}
        failed, _, _, excluded, excluded_passes = diagnose.select_cases(
            events, cases, ("no_match",))
        self.assertEqual(failed, ["q0"])
        not_passing = len(failed) + sum(len(v) for v in excluded.values())
        self.assertEqual(not_passing, 1, "four holdout PASSES inflated it to 5")
        self.assertEqual(
            len(excluded_passes["holdout, withheld from diagnosis"]), 4)

    def test_a_holdout_FAILURE_is_still_a_non_passing_case(self):
        events = [{"kind": "score", "qid": "h0", "sample": None,
                   "phase": "baseline", "verdict": "no_match", "reason": "r"}]
        cases = {"h0": {"qid": "h0", "split": "holdout"}}
        failed, _, _, excluded, excluded_passes = diagnose.select_cases(
            events, cases, ("no_match",))
        self.assertEqual(failed, [])
        self.assertEqual(excluded_passes, {})
        self.assertEqual(
            len(excluded["holdout, withheld from diagnosis"]), 1,
            "a holdout failure is still withheld, and still non-passing")

    def test_only_records_what_it_dropped_as_an_exclusion(self):
        failed, _, _, excluded = self.select(
            self.scores(4), self.cases(4), only="q0,q1")
        self.assertEqual(sorted(failed), ["q0", "q1"])
        self.assertEqual(
            len(failed) + sum(len(v) for v in excluded.values()), 4)

    def test_an_unnarrowed_run_excludes_nothing_for_narrowing(self):
        failed, _, _, excluded = self.select(self.scores(3), self.cases(3))
        self.assertEqual(len(failed), 3)
        self.assertEqual(excluded, {})

    def test_a_passing_holdout_with_a_retrieval_miss_stays_withheld(self):
        """The leak: `verdict == "match"` was tested BEFORE the split, so a
        holdout case that answered correctly with an incomplete retrieval went
        into `retrieval_only` and on to a diagnosis call on every run, with no
        flag able to prevent it. Holdout exists so improve has something
        diagnosis never saw."""
        cases = {"q0": {"qid": "q0", "coverage": "covered", "split": "holdout",
                        "expectedEntities": {"required": ["measure:m:total"]}}}
        events = [
            {"kind": "score", "qid": "q0", "sample": None, "phase": "baseline",
             "verdict": "match", "reason": "ok"},
            {"kind": "tool_call", "qid": "q0", "sample": None,
             "phase": "baseline", "tool": "get_context",
             "rankedSummary": {"entityIds": []},
             "target_shapes": [{"type": "dimension", "has_text": True}]},
        ]
        (failed, passed, retrieval_only, excluded,
         excluded_passes) = diagnose.select_cases(events, cases, ("no_match",))
        self.assertEqual(retrieval_only, [])
        self.assertEqual(passed, [])
        self.assertEqual(failed, [])
        # Withheld, which is what this test is about -- and in the PASSES
        # bucket, because it matched and `not_passing` sums the other one.
        self.assertIn("q0",
                      excluded_passes["holdout, withheld from diagnosis"])
        self.assertEqual(excluded, {})

    def test_include_holdout_lets_that_same_case_through(self):
        cases = {"q0": {"qid": "q0", "coverage": "covered", "split": "holdout",
                        "expectedEntities": {"required": ["measure:m:total"]}}}
        events = [
            {"kind": "score", "qid": "q0", "sample": None, "phase": "baseline",
             "verdict": "match", "reason": "ok"},
            {"kind": "tool_call", "qid": "q0", "sample": None,
             "phase": "baseline", "tool": "get_context",
             "rankedSummary": {"entityIds": []},
             "target_shapes": [{"type": "dimension", "has_text": True}]},
        ]
        _, passed, retrieval_only, _ = self.select(
            events, cases, include_holdout=True)
        self.assertEqual(passed, ["q0"])
        self.assertEqual(retrieval_only, ["q0"])

    def passing_with_a_miss(self):
        """Three cases that all PASSED, two of them on an incomplete
        retrieval."""
        cases, ev = {}, []
        for q in ("q1", "q2", "q3"):
            cases[q] = {"qid": q, "coverage": "covered",
                        "expectedEntities": {"required": ["measure:m:total"]}}
            got = [] if q in ("q1", "q2") else ["measure:m:total"]
            ev += [{"kind": "score", "qid": q, "sample": None,
                    "phase": "baseline", "verdict": "match", "reason": "ok"},
                   {"kind": "tool_call", "qid": q, "sample": None,
                    "phase": "baseline", "tool": "get_context",
                    "rankedSummary": {"entityIds": got},
                    "target_shapes": [{"type": "measure", "has_text": True}]}]
        return cases, ev

    def test_a_pass_kept_out_of_diagnosis_is_not_a_non_passing_case(self):
        """`--no-retrieval-misses` used to push these into `excluded`, which
        `not_passing` sums, so a run where every case passed printed
        "coverage: 0 of 2 non-passing case(s) diagnosed (0%)"."""
        cases, ev = self.passing_with_a_miss()
        f, p, r, excluded, excluded_passes = diagnose.select_cases(
            ev, cases, ("no_match",), no_retrieval_misses=True)
        self.assertEqual(f, [])
        self.assertEqual(sorted(p), ["q1", "q2", "q3"])
        self.assertEqual(excluded, {})
        not_passing = len(f) + sum(len(v) for v in excluded.values())
        self.assertEqual(not_passing, 0)
        # Still reported, just not as a failure.
        self.assertEqual(
            excluded_passes["passed with a retrieval miss "
                            "(--no-retrieval-misses)"], ["q1", "q2"])

    def test_without_the_flag_those_passes_are_diagnosed(self):
        cases, ev = self.passing_with_a_miss()
        f, p, r, excluded, excluded_passes = diagnose.select_cases(
            ev, cases, ("no_match",))
        self.assertEqual(sorted(r), ["q1", "q2"])
        self.assertEqual(excluded_passes, {})
        self.assertEqual(len(f) + sum(len(v) for v in excluded.values()), 0)

    def test_contamination_still_outranks_the_split(self):
        cases = self.cases(1, split="holdout")
        events = [{"kind": "score", "qid": "q0", "sample": None,
                   "phase": "baseline", "verdict": "no_match", "reason": "r",
                   "contaminated": True}]
        _, _, _, excluded = self.select(events, cases)
        self.assertIn("contaminated", excluded)
        self.assertNotIn("holdout, withheld from diagnosis", excluded)

class MatchedPair(unittest.TestCase):
    """A case that passed in another arm is the richest evidence in the run,
    and it was the one thing the diagnoser never saw.

    diagnose.py reads one run. A flip lives across two, so a case that passed
    once and failed once was diagnosed from the failing side alone -- with the
    matched pair, same question and same model, sitting unread in the other
    run directory.
    """

    def evidence(self, passed_elsewhere=None):
        events = [
            {"kind": "attempt", "qid": "q1",
             "final_query": "run: wrong -> { x }", "answer_text": "2 rows"},
            {"kind": "score", "qid": "q1", "verdict": "no_match",
             "reason": "expected 443"},
        ]
        return diagnose.evidence_for("q1", {"question": "how many?"}, events,
                                     passed_elsewhere)

    def test_absent_by_default(self):
        self.assertIsNone(self.evidence()["passedInAnotherArm"])

    def test_the_other_arms_query_reaches_the_agent(self):
        e = self.evidence({"arm": "aa-2", "verdict": "match",
                           "finalQuery": "run: right -> { y }"})
        self.assertEqual(e["passedInAnotherArm"]["finalQuery"],
                         "run: right -> { y }")
        self.assertEqual(e["passedInAnotherArm"]["arm"], "aa-2")

    def test_this_arms_query_is_still_there_to_diff_against(self):
        e = self.evidence({"arm": "aa-2", "finalQuery": "run: right -> { y }"})
        self.assertIn("run: wrong -> { x }", e["queriesRun"])

    def test_the_prompt_tells_the_agent_what_to_do_with_it(self):
        self.assertIn("passedInAnotherArm", diagnose.DIAGNOSE_PROMPT)
        self.assertIn("matched pair", diagnose.DIAGNOSE_PROMPT)

    def test_the_prompt_says_a_flip_is_not_noise(self):
        # The doctrine change, pinned: a flip is a model-quality finding.
        self.assertIn("not noise", diagnose.DIAGNOSE_PROMPT)

if __name__ == "__main__":
    unittest.main()
