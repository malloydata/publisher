#!/usr/bin/env python3
"""Tests for run_baseline's recording decisions: which query an attempt is
credited with, which retriever answered, what was actually re-executed, and
when an attempt is unjudgeable."""
import argparse
import json
import os
import pathlib
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import run_baseline as rb  # noqa: E402


class NamedQuery(unittest.TestCase):
    def test_view_on_a_source(self):
        self.assertEqual(
            rb.named_query({"sourceName": "order_items", "queryName": "yoy"}),
            "run: order_items -> yoy")

    def test_model_level_query_has_no_source(self):
        self.assertEqual(rb.named_query({"queryName": "top_categories"}),
                         "run: top_categories")

    def test_snake_case_keys_are_the_same_call(self):
        self.assertEqual(
            rb.named_query({"source_name": "s", "query_name": "v"}),
            "run: s -> v")

    def test_ad_hoc_call_names_nothing(self):
        self.assertIsNone(rb.named_query({"query": "run: x -> y"}))


class FinalQuery(unittest.TestCase):
    def setUp(self):
        self.qs = ["run: a -> answer", "run: a -> probe"]
        # The two queries ran against DIFFERENT model files, which is the case
        # that matters: a package holds many, and the probe is the last call.
        self.calls = [
            {"tool": "execute_query", "query": "run: a -> answer",
             "modelPath": "answer.malloy", "error": None},
            {"tool": "execute_query", "query": "run: a -> probe",
             "modelPath": "probe.malloy", "error": None},
        ]

    def test_the_answer_names_its_query(self):
        text = "The total is 4744743.45.\n\n```malloy\nrun: a -> answer\n```"
        self.assertEqual(rb.pick_final_query(self.qs, self.calls, text),
                         ("run: a -> answer", "declared", "answer.malloy"))

    def test_the_file_follows_the_query_not_the_transcript(self):
        # The last modelPath in the transcript is the probe's. Sending the
        # answer's query to that file is `Reference to undefined object` for a
        # source that plainly exists, and the case scores as a model failure.
        text = "```malloy\nrun: a -> answer\n```"
        self.assertEqual(rb.pick_final_query(self.qs, self.calls, text)[2],
                         "answer.malloy")

    def test_a_retry_is_credited_to_the_file_that_answered(self):
        # One query, two files: the retry is how an answerer recovers from
        # naming the wrong one, and re-executing the failed call reproduces
        # the error rather than the answer.
        calls = [
            {"tool": "execute_query", "query": "run: a -> answer",
             "modelPath": "wrong.malloy",
             "error": "Reference to undefined object"},
            {"tool": "execute_query", "query": "run: a -> answer",
             "modelPath": "answer.malloy", "error": None},
        ]
        text = "```malloy\nrun: a -> answer\n```"
        self.assertEqual(
            rb.pick_final_query(["run: a -> answer"] * 2, calls, text)[2],
            "answer.malloy")

    def test_a_semicolon_inside_a_literal_is_not_a_separator(self):
        # Two queries that both ran, one filtering on 'Books;Media' and one on
        # 'Books Media', must stay two queries, or the declared block resolves
        # to whichever ran later.
        a = "run: t -> { where: cat = 'Books;Media'; aggregate: n }"
        b = "run: t -> { where: cat = 'Books Media'; aggregate: n }"
        self.assertNotEqual(rb._norm(a), rb._norm(b))
        self.assertEqual(rb._norm("run: t -> { where: x = 1; aggregate: n }"),
                         rb._norm("run: t -> {\n  where: x = 1\n  aggregate: n\n}"))

    def test_an_escaped_quote_does_not_end_the_string(self):
        # `'O\'Brien'`: the loop used to read the escaped quote as the end of
        # the string, the real closing quote opened one that never closed, and
        # a later `;` was kept as quoted text -- so the printed form (newlines)
        # and the executed form (semicolons) normalised differently.
        ran = "run: customers -> { where: last_name = 'O\\'Brien'; aggregate: n is count() }"
        printed = "run: customers -> {\n  where: last_name = 'O\\'Brien'\n  aggregate: n is count()\n}"
        self.assertEqual(rb._norm(ran), rb._norm(printed))

    def test_an_apostrophe_in_a_comment_opens_no_string(self):
        ran = "run: t -> { // it's the filtered one\n where: x = 1; aggregate: n }"
        printed = "run: t -> { // it's the filtered one\n where: x = 1\n aggregate: n }"
        self.assertEqual(rb._norm(ran), rb._norm(printed))
        dashed = "run: t -> { -- don't\n where: x = 1; aggregate: n }"
        self.assertEqual(rb._norm(dashed), rb._norm("run: t -> { -- don't\n where: x = 1\n aggregate: n }"))

    def test_double_quotes_and_backticks_are_strings_too(self):
        self.assertNotEqual(rb._norm('run: t -> { where: cat = "Books;Media"; aggregate: n }'),
                            rb._norm('run: t -> { where: cat = "Books Media"; aggregate: n }'))
        self.assertNotEqual(rb._norm("run: t -> { group_by: `a;b`; aggregate: n }"),
                            rb._norm("run: t -> { group_by: `a b`; aggregate: n }"))

    def test_a_declared_query_matches_however_it_was_laid_out(self):
        # The executed query is one line with semicolons; the answer prints the
        # same query on several lines. Before, they never compared equal and
        # the harness graded the probe that ran last.
        ran = "run: a -> { where: x = 1; aggregate: n is count() }"
        calls = [
            {"tool": "execute_query", "query": ran,
             "modelPath": "m.malloy", "error": None},
            {"tool": "execute_query", "query": "run: a -> probe",
             "modelPath": "p.malloy", "error": None},
        ]
        text = ("The filtered total.\n\n```malloy\nrun: a -> {\n  where: x = 1\n"
                "  aggregate: n is count()\n}\n```")
        self.assertEqual(
            rb.pick_final_query([ran, "run: a -> probe"], calls, text),
            (ran, "declared", "m.malloy"))

    def test_a_call_that_named_no_file_reports_none(self):
        # The server resolved the file from its own default, so the run's
        # default is the closer guess than a file another call named.
        calls = [{"tool": "execute_query", "query": "run: a -> answer",
                  "error": None},
                 {"tool": "execute_query", "query": "run: a -> probe",
                  "modelPath": "probe.malloy", "error": None}]
        text = "```malloy\nrun: a -> answer\n```"
        self.assertIsNone(rb.pick_final_query(self.qs, calls, text)[2])

    def test_a_fenced_block_it_never_ran_is_not_credited(self):
        text = "```malloy\nrun: a -> invented\n```"
        q, how, mp = rb.pick_final_query(self.qs, self.calls, text)
        self.assertEqual(how, "last_ok")

    def test_whitespace_does_not_defeat_the_match(self):
        text = "```\nrun:   a\n  -> answer\n```"
        self.assertEqual(rb.pick_final_query(["run: a -> answer"],
                                             self.calls[:1], text)[1],
                         "declared")

    def test_the_last_error_is_skipped(self):
        calls = [self.calls[0],
                 {"tool": "execute_query", "query": "run: a -> probe",
                  "modelPath": "probe.malloy", "error": "no such field"}]
        self.assertEqual(rb.pick_final_query(self.qs, calls, "no fence"),
                         ("run: a -> answer", "last_ok", "answer.malloy"))

    def test_with_no_results_it_falls_back_to_the_last_query(self):
        self.assertEqual(rb.pick_final_query(self.qs, [], "no fence"),
                         ("run: a -> probe", "last", None))

    def test_no_queries_at_all(self):
        self.assertEqual(rb.pick_final_query([], [], ""), (None, None, None))


class Retrieval(unittest.TestCase):
    def att(self, *modes):
        return {"calls": [{"tool": "get_context", "retrieval_mode": m}
                          for m in modes]}

    def test_all_semantic(self):
        mode, tally = rb.retrieval_summary([self.att("semantic", "semantic")])
        self.assertEqual(mode, "semantic")
        self.assertEqual(tally["semantic"], 2)

    def test_a_provider_that_fell_over_partway_is_mixed(self):
        self.assertEqual(
            rb.retrieval_summary([self.att("semantic", "lexical")])[0], "mixed")

    def test_no_provider_is_unreported_not_lexical(self):
        # An unrecorded retriever is not evidence that it was lexical.
        self.assertEqual(rb.retrieval_summary([self.att(None)])[0],
                         "unreported")

    def test_a_call_that_did_not_rank_does_not_dilute_semantic(self):
        # Only a ranking call names a retriever. An enumeration or a targeted
        # lookup comes back without one on a fully semantic server, and reading
        # that as a partial fall-back tripped the gate on runs where nothing
        # fell back -- suppressing the discoverability findings they paid for.
        self.assertEqual(
            rb.retrieval_summary([self.att("semantic", None)])[0], "semantic")

    def test_an_unranked_call_does_not_dilute_lexical_either(self):
        self.assertEqual(
            rb.retrieval_summary([self.att("lexical", None)])[0], "lexical")

    def test_execute_query_is_not_retrieval(self):
        att = {"calls": [{"tool": "execute_query", "retrieval_mode": None}]}
        self.assertEqual(rb.retrieval_summary([att]),
                         ("unreported", {"semantic": 0, "lexical": 0,
                                         "unreported": 0}))


class ReExecution(unittest.TestCase):
    def setUp(self):
        self.tmp = pathlib.Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def pred(self, qid: str, query, rendered: str) -> None:
        d = self.tmp / qid
        d.mkdir(parents=True)
        (d / "prediction.json").write_text(
            json.dumps({"query": query, "rendered": rendered}))

    def test_counts_separate_a_failure_from_an_absent_query(self):
        self.pred("ok1", "run: a", "| total |\n| 1 |")
        self.pred("bad", "run: b", "(re-execution failed: no such field)")
        self.pred("none", None, "(the answerer ran no query, so there is "
                                "nothing to re-execute)")
        self.pred("skip", "run: d", "(not re-executed: the server is not "
                                    "serving the model)")
        got = rb.reexecution_summary(self.tmp,
                                     ["ok1", "bad", "none", "skip", "absent"])
        self.assertEqual(got, {"attempted": 2, "ok": 1, "failed": 1,
                               "noQuery": 1, "notReExecuted": 1, "notJudged": 0,
                               "missing": 1})

    def test_every_case_lands_in_exactly_one_bucket(self):
        # The denominator. `notReExecuted` and `missing` used to `continue`
        # without counting, so summing the buckets against the case count was
        # short by however many cases took those two paths, with nothing
        # naming them.
        self.pred("ok1", "run: a", "| total |\n| 1 |")
        self.pred("bad", "run: b", "(re-execution failed: no such field)")
        self.pred("none", None, "(nothing to re-execute)")
        self.pred("skip", "run: d", "(not re-executed: the server is not "
                                    "serving the model)")
        qids = ["ok1", "bad", "none", "skip", "absent"]
        got = rb.reexecution_summary(self.tmp, qids)
        self.assertEqual(
            got["ok"] + got["failed"] + got["noQuery"]
            + got["notReExecuted"] + got["notJudged"] + got["missing"],
            len(qids))
        # `attempted` is the derived one, outside the partition.
        self.assertEqual(got["attempted"], got["ok"] + got["failed"])

    def test_unreadable_prediction_counts_as_missing(self):
        d = self.tmp / "junk"
        d.mkdir(parents=True)
        (d / "prediction.json").write_text("{not json")
        got = rb.reexecution_summary(self.tmp, ["junk"])
        self.assertEqual(got["missing"], 1)

    def test_a_case_the_judge_never_saw_is_not_a_missing_artifact(self):
        # A freshly imported set is `provisional` throughout, so every case is
        # refused before the judge and no prediction file exists. That read as
        # N corrupt artifacts beside `predictionsReExecuted: true`.
        self.pred("ok1", "run: a", "| total |\n| 1 |")
        got = rb.reexecution_summary(self.tmp, ["ok1", "prov", "prov2"],
                                     judged={"ok1"})
        self.assertEqual((got["ok"], got["notJudged"], got["missing"]),
                         (1, 2, 0))

    def test_without_the_judged_set_the_old_partition_holds(self):
        got = rb.reexecution_summary(self.tmp, ["absent"])
        self.assertEqual((got["notJudged"], got["missing"]), (0, 1))


class JudgeGate(unittest.TestCase):
    """The gate decides what is unjudgeable, and it is not `submitted`."""

    def setUp(self):
        self.tmp = pathlib.Path(tempfile.mkdtemp())
        # rebuild without rejudge returns before any model call, so reaching
        # `no_saved_verdict` proves the attempt got past the gate.
        self.a = argparse.Namespace(rebuild=True, rejudge=False)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def judge(self, att, golden=None):
        # A verified golden, so this class tests the submission gate alone. An
        # empty `golden` is its own refusal now (GoldenStatusGate below), and
        # leaving it empty here made these tests pass through the wrong gate.
        case = {"qid": "q", "question": "?",
                "golden": golden if golden is not None
                else {"status": "verified", "value": 1}}
        return rb.run_judge(case, att, self.a, self.tmp, "", "", False)

    def test_neither_text_nor_query_is_not_submitted(self):
        v = self.judge({"answer_text": "", "submitted": False})
        self.assertEqual(v["reason"], "not_submitted")
        self.assertIsNone(v["verdict"])

    def test_prose_with_no_query_is_still_judged(self):
        # A confident refusal on an answerable case has to be scorable, or the
        # answerable-sounds-unanswerable cases measure nothing.
        v = self.judge({"answer_text": "I cannot answer that.",
                        "submitted": False})
        self.assertEqual(v["reason"], "no_saved_verdict")

    def test_a_query_with_no_prose_is_still_judged(self):
        v = self.judge({"answer_text": "", "submitted": True})
        self.assertEqual(v["reason"], "no_saved_verdict")

    def test_an_unstamped_golden_still_reaches_the_judge(self):
        # Sets that predate `golden.status` are unguarded, not unscorable.
        # Refusing them would zero the pass rate of every set that has one.
        v = self.judge({"answer_text": "1830000", "submitted": True},
                       golden={"value": 1830000})
        self.assertEqual(v["reason"], "no_saved_verdict")


class VerdictObject(unittest.TestCase):
    """Finding the judge's verdict in a reply that holds other braces.

    The old read spanned from the FIRST brace in the document to the last, so a
    judge that quoted a Malloy snippet on its way to deciding handed
    `json.loads` a blob starting mid-query. The case then counted in no bucket
    at all and left the denominator the pass rate is printed over.
    """

    GOOD = '{"why": "the figures agree", "verdict": "match", "confidence": 9}'

    def test_a_quoted_malloy_query_does_not_break_the_read(self):
        # The reproduction: a fenced query, then the verdict.
        text = ("```malloy\nrun: orders -> { aggregate: n is count() }\n```\n"
                f"Looks right.\n{self.GOOD}")
        self.assertEqual(rb.parse_verdict(text)["verdict"], "match")

    def test_the_last_verdict_object_wins(self):
        # The judge is told to end with the object, and prose reasoning toward
        # it may quote a draft on the way.
        text = ('{"why": "draft", "verdict": "near_match", "confidence": 6}\n'
                f"On reflection:\n{self.GOOD}")
        v = rb.parse_verdict(text)
        self.assertEqual(v["verdict"], "match")
        self.assertEqual(v["reason"], "the figures agree")

    def test_a_nested_object_still_parses(self):
        text = ('{"why": "a", "verdict": "match", "confidence": 9, '
                '"column_pairing": {"gold": "pred"}}')
        self.assertEqual(rb.parse_verdict(text)["column_pairing"],
                         {"gold": "pred"})

    def test_prose_around_the_object_is_fine(self):
        self.assertEqual(
            rb.parse_verdict(f"Here you go:\n{self.GOOD}\nHope that helps"
                             )["verdict"], "match")

    def test_a_reply_with_no_object_is_unparseable(self):
        # The shape five cases across three runs actually produced: the judge
        # reasons to the point of deciding and the process ends.
        v = rb.parse_verdict("I have what I need to decide.")
        self.assertIsNone(v["verdict"])
        self.assertEqual(v["reason"], "judge_unparseable")

    def test_braces_without_a_verdict_are_not_a_verdict(self):
        v = rb.parse_verdict('{"note": "no verdict here"}')
        self.assertEqual(v["reason"], "judge_unparseable")

    def test_the_retry_predicate_agrees_with_the_parser(self):
        # `judge_unusable` reuses `parse_verdict` rather than re-deciding what
        # parseable means, so a reply this now reads must not be re-rolled.
        text = f"```malloy\nrun: x -> {{ aggregate: n is count() }}\n```\n{self.GOOD}"
        self.assertFalse(rb.judge_unusable([{"type": "assistant"}], text))


class JudgePrompt(unittest.TestCase):
    """What the judge is SHOWN, and whether a run can say which version of it.

    The tests cover the code paths; the defect that ran for three arms was in
    the text the model reads. These are about that text.
    """

    def test_the_prompt_is_pinned_separately_from_the_skill(self):
        # `rubricSha` hashes eval-judge/SKILL.md. The prompt template lives
        # here and carries the golden rendering and the tie-breaking rules, so
        # a run that names only the skill cannot say which judge it ran.
        import ledger
        self.assertIn("judgePromptSha", ledger.RUN_OPTIONAL)

    def test_a_stale_rubric_figure_loses_to_the_golden(self):
        # An answer holding the golden's own figures was failed against a
        # rubric still quoting the previous morning's. Stated as a rule the
        # judge applies at any precision, rather than a regex over prose.
        self.assertIn("Where the CASE RUBRIC quotes a figure that the GOLDEN "
                      "contradicts", rb.JUDGE_PROMPT)
        self.assertIn("the rubric's figure is stale prose", rb.JUDGE_PROMPT)


class JudgeCostAcrossARebuild(unittest.TestCase):
    """A rebuild spends nothing on judging and must not erase what judging cost.

    Found by writing a report off a rebuilt run: its cost line read $0.00 judge
    for verdicts the original arm had paid $0.78 for.
    """

    def test_a_run_that_judged_records_what_it_spent(self):
        self.assertEqual(rb.carry_judge_cost(0.78, None), (0.78, False))

    def test_a_rebuild_keeps_the_earlier_figure_and_marks_it(self):
        self.assertEqual(rb.carry_judge_cost(0.0, 0.78), (0.78, True))

    def test_a_rejudge_wins_over_the_earlier_figure(self):
        # It really did judge again, so the new spend is this run's own.
        self.assertEqual(rb.carry_judge_cost(0.91, 0.78), (0.91, False))

    def test_a_first_run_with_nothing_before_it_records_zero(self):
        self.assertEqual(rb.carry_judge_cost(0.0, None), (0.0, False))

    def test_a_prior_of_zero_is_not_carried(self):
        # --no-judge then rebuild: there is no earlier spend to protect, and
        # marking it carried would claim a judging that never happened.
        self.assertEqual(rb.carry_judge_cost(0.0, 0.0), (0.0, False))

    def test_the_prior_is_read_before_run_json_is_rewritten(self):
        # The whole defect: read after `run_config` writes, it is always None
        # and the fix silently does nothing.
        src = pathlib.Path(rb.__file__).read_text()
        self.assertLess(src.index("prior_judge = prior_judge_cost(a.out)"),
                        src.index('(a.out / "run.json").write_text'))

    def test_a_missing_or_unreadable_run_json_is_none(self):
        tmp = pathlib.Path(tempfile.mkdtemp())
        try:
            self.assertIsNone(rb.prior_judge_cost(tmp))
            (tmp / "run.json").write_text("{not json")
            self.assertIsNone(rb.prior_judge_cost(tmp))
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


class TruncatedAttempt(unittest.TestCase):
    """An attempt the turn cap cut off is not judged.

    Measured on a 29-case customer arm: four attempts ended at exactly 31 turns
    with `error_max_turns`, and each went to the judge as a complete answer.
    One of them was a planning fragment -- "Let me build the correct monthly
    trend query" -- which the judge scored `no_match` at confidence 9, where it
    counted in the pass rate as if the model had got the question wrong.
    """

    def setUp(self):
        self.tmp = pathlib.Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def judge(self, att, *, rebuild=True, rejudge=False, golden=None):
        a = argparse.Namespace(rebuild=rebuild, rejudge=rejudge)
        return rb.run_judge(
            {"qid": "q", "question": "?",
             "golden": golden if golden is not None else {"status": "verified",
                                                          "value": 1}},
            att, a, self.tmp, "", "", False)

    def test_a_truncated_attempt_is_refused_and_says_so(self):
        v = self.judge({"answer_text": "Let me build the correct query.",
                        "submitted": True, "error": "error_max_turns"})
        self.assertIsNone(v["verdict"])
        self.assertEqual(v["reason"], "answerer_truncated")

    def test_it_is_decided_before_a_saved_verdict_is_reused(self):
        # The point of putting it first: `--rebuild` over the runs that
        # motivated this re-derives the right ledger from their transcripts,
        # rather than restoring the verdict the truncated attempt was given.
        (self.tmp / "q").mkdir(parents=True)
        (self.tmp / "q" / "judge.md").write_text(
            '{"why": "trails off", "verdict": "no_match", "confidence": 9}')
        v = self.judge({"answer_text": "frag", "submitted": True,
                        "error": "error_max_turns"})
        self.assertEqual(v["reason"], "answerer_truncated")
        self.assertIsNone(v["verdict"])

    def test_it_is_decided_before_the_golden_gate(self):
        # Order matters the other way too: a truncated attempt against a
        # provisional key is truncated. Both refuse, and the one that names
        # the harness's own setting is the one a conductor can act on.
        v = self.judge({"answer_text": "frag", "submitted": True,
                        "error": "error_max_turns"},
                       golden={"status": "provisional", "value": 1})
        self.assertEqual(v["reason"], "answerer_truncated")

    def test_any_other_harness_failure_is_also_refused(self):
        # The four-strikes abort does NOT cover this: it stops the arm and
        # still judges what it collected. Measured on a live run, four attempts
        # whose text was the CLI's own "model not found" message were scored
        # `no_match` for $0.29, and the run printed `passed 0 of 4 decided
        # (0%)` about a model no answerer had reached.
        v = self.judge({"answer_text": "Error: model not found",
                        "submitted": False, "error": "error_during_execution"})
        self.assertIsNone(v["verdict"])
        self.assertTrue(v["reason"].startswith("environment_failure"))
        # The cap keeps its own word, because the remedy differs: re-run those
        # cases at a higher cap, versus fix the environment and re-run the arm.
        self.assertNotIn("truncated", v["reason"])

    def test_the_cli_reporting_subtype_success_on_an_error_still_refuses(self):
        # Observed exactly: `is_error` true with subtype "success", so the
        # attempt carried `run_error: "success"`. Testing the VALUE would have
        # let every one of those through; the test is that an error was
        # reported at all.
        v = self.judge({"answer_text": "Error: model not found",
                        "submitted": False, "error": "success"})
        self.assertIsNone(v["verdict"])
        self.assertTrue(v["reason"].startswith("environment_failure"))

    def test_a_clean_attempt_is_unaffected(self):
        v = self.judge({"answer_text": "4.2M", "submitted": True,
                        "error": None})
        self.assertEqual(v["reason"], "no_saved_verdict")


class ContaminatedVerdict(unittest.TestCase):
    """A voided verdict says why, and keeps what the judge said.

    Nulling in silence left a score event carrying a correct `reason` and
    `confidence` beside `verdict: null`, which is indistinguishable from the
    field being dropped on the write path -- and was read as exactly that in a
    real run report, where nine "lost" verdicts were recovered by re-parsing
    the stored judge replies. They had been voided on purpose.
    """

    def void(self, verdict, breaches, *, vetoed=False):
        """The real nulling, not a copy of it: `main` calls this same
        function, so a change there cannot pass these tests by drifting."""
        v = dict(verdict)
        if vetoed:
            # What the mustNotUse veto does just above the call site.
            v["judge_verdict"] = v["verdict"]
            v["verdict"] = "no_match"
        return rb.void_contaminated(v, breaches)

    def test_a_voided_verdict_names_contamination(self):
        v = self.void({"verdict": "match", "reason": "the figures agree",
                       "confidence": 9},
                      ["host tool available to the answerer: Bash"])
        self.assertIsNone(v["verdict"])
        self.assertEqual(v["reason"], "contaminated")
        # The schema documents this reason; before, the judge's own prose sat
        # here and the null looked like a bug.
        self.assertEqual(v["judge_verdict"], "match")

    def test_a_veto_that_is_also_contaminated_keeps_the_judges_read(self):
        # Two overrides on one case. `judge_verdict` is for what the JUDGE
        # said, so the veto's own `no_match` must not displace it.
        v = self.void({"verdict": "match", "reason": "ok", "confidence": 8},
                      ["mcp server 'malloy' failed"], vetoed=True)
        self.assertIsNone(v["verdict"])
        self.assertEqual(v["judge_verdict"], "match")

    def test_a_clean_attempt_keeps_its_verdict_and_reason(self):
        v = self.void({"verdict": "match", "reason": "the figures agree",
                       "confidence": 9}, [])
        self.assertEqual(v["verdict"], "match")
        self.assertEqual(v["reason"], "the figures agree")

    def test_every_null_verdict_reason_is_in_the_documented_set(self):
        # The generalisation of the report's own ask ("a score event with a
        # reason also has a verdict, or an explicit unreadable marker"): a null
        # verdict must always name which of the known causes it was, so no
        # future nulling can be silent the way contamination was.
        schema = (pathlib.Path(rb.__file__).parent.parent.parent
                  / "eval-answer" / "reference" / "ledger-schema.md").read_text()
        for reason in ("not_submitted", "contaminated", "answerer_truncated",
                       "judge_unparseable", "no_saved_verdict"):
            with self.subTest(reason):
                self.assertIn(reason, schema)


class GoldenStatusGate(unittest.TestCase):
    """A verdict is refused against a key nobody has established.

    `skill:eval-answer` has said since it was written that no verdict issues
    "when the golden is missing, provisional, invalid, or ambiguous". Only the
    attempt half of that sentence was implemented: `golden.status` was read
    once, into a COPY of the verdict, so the set's own word never reached the
    aggregate. Every such case was judged in full and counted in the rate.
    """

    def setUp(self):
        self.tmp = pathlib.Path(tempfile.mkdtemp())
        self.a = argparse.Namespace(rebuild=True, rejudge=False)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def judge(self, golden):
        return rb.run_judge({"qid": "q", "question": "?", "golden": golden},
                            {"answer_text": "4.2M", "submitted": True},
                            self.a, self.tmp, "", "", False)

    def test_provisional_is_refused_and_says_so(self):
        # The one that matters now: eval-import writes `provisional` on every
        # imported golden and enforces it on write, so the importer
        # manufactures exactly the sets this would have scored.
        v = self.judge({"status": "provisional", "value": 4200000})
        self.assertIsNone(v["verdict"])
        self.assertEqual(v["reason"], "golden_provisional")
        # NOT carried into `gold_status`: that field is documented in the
        # judge's vocabulary, which has no `provisional`. The reason says it.
        self.assertIsNone(v["gold_status"])

    def test_invalid_and_ambiguous_are_refused(self):
        for st in ("invalid", "ambiguous"):
            with self.subTest(st):
                v = self.judge({"status": st, "value": 1})
                self.assertIsNone(v["verdict"])
                self.assertEqual(v["reason"], f"golden_{st}")

    def test_a_key_the_set_marks_wrong_is_refused(self):
        v = self.judge({"status": "verified_wrong", "value": 1})
        self.assertIsNone(v["verdict"])
        self.assertEqual(v["reason"], "golden_verified_wrong")

    def test_no_golden_at_all_is_refused(self):
        for g in ({}, None):
            with self.subTest(g):
                v = self.judge(g)
                self.assertIsNone(v["verdict"])
                self.assertEqual(v["reason"], "golden_missing")

    def test_verified_passes_the_gate(self):
        v = self.judge({"status": "verified", "value": 1})
        self.assertEqual(v["reason"], "no_saved_verdict")

    def test_a_criteria_golden_with_no_value_passes_the_gate(self):
        # `criteria` holds no value by design; its clauses are the comparison.
        # Refusing a value-less golden would drop every such case, and every
        # deliberately-unanswerable case with it.
        # The clauses live in `rubric`, which is the documented field
        # (reference/case-format.md); this fixture said `criteria` and nothing
        # read it, so the drift was invisible until a renderer had to find them.
        v = self.judge({"status": "verified", "kind": "criteria",
                        "rubric": "breaks the total out by region"})
        self.assertEqual(v["reason"], "no_saved_verdict")


    def test_refusal_is_decided_before_a_saved_verdict_is_reused(self):
        # `--rebuild` without `--rejudge` returns the saved judge.md. A saved
        # verdict against an unestablished key is not evidence either.
        (self.tmp / "q").mkdir(parents=True)
        (self.tmp / "q" / "judge.md").write_text("VERDICT: match\n")
        v = self.judge({"status": "provisional", "value": 1})
        self.assertEqual(v["reason"], "golden_provisional")

    def test_the_refusals_are_exactly_the_documented_four(self):
        # Pinned against the contract's own sentence rather than restated.
        skill = (pathlib.Path(rb.__file__).parent.parent.parent
                 / "eval-answer" / "SKILL.md").read_text()
        self.assertIn("missing, provisional, invalid, or ambiguous", skill)
        self.assertEqual(rb.GOLDEN_UNSCORABLE,
                         ("provisional", "invalid", "ambiguous"))


class GoldenForJudge(unittest.TestCase):
    """What the GOLDEN line of the judge prompt says, per kind.

    A `criteria` golden rendered as "unanswerable" is a contradiction the judge
    can see, and one called it rather than scoring the case.
    """

    def test_criteria_never_says_unanswerable(self):
        line = rb.golden_for_judge(
            {"kind": "criteria", "rubric": "names the channels"})
        self.assertNotIn("unanswerable", line)
        self.assertIn("CASE RUBRIC", line)

    def test_unanswerable_says_so(self):
        self.assertIn("unanswerable",
                      rb.golden_for_judge({"kind": "unanswerable"}))

    def test_a_scalar_renders_its_value(self):
        self.assertEqual(
            rb.golden_for_judge({"kind": "scalar", "value": {"total": 12.5}}),
            '{"total": 12.5}')

    def test_rows_render_the_list(self):
        self.assertEqual(
            rb.golden_for_judge({"kind": "rows", "value": [{"a": 1}]}),
            '[{"a": 1}]')

    def test_an_explicit_null_value_is_a_refusal_case(self):
        # How the ecommerce set's four refusal cases say there is no number.
        self.assertIn("unanswerable",
                      rb.golden_for_judge({"kind": "scalar", "value": None}))

    def test_a_golden_with_no_kind_is_unchanged(self):
        # Sets predating the field must read exactly as they did.
        self.assertEqual(rb.golden_for_judge({"value": 7}), "7")
        self.assertIn("unanswerable", rb.golden_for_judge({}))
        self.assertIn("unanswerable", rb.golden_for_judge(None))


class AnUnreadableVerdictIsNotLost(unittest.TestCase):
    """A judge reply that will not parse carries no verdict, so it counts in
    none of match, no_match, near_match or needs_human. It therefore left the
    denominator without appearing anywhere, and the pass rate was printed over
    the remainder. Two cases went that way in one hosted run."""

    GOOD = '{"why":"y","verdict":"match","confidence":9}'

    def test_the_retry_predicate_reaches_run_cli(self):
        # `claude` accepted `retry_when` and then passed `no_events` literally,
        # so the judge's predicate never arrived and the judge never retried.
        with mock.patch.object(rb, "run_cli",
                               return_value=([], "", "", 1, 0.0)) as run:
            rb.claude("q", str(pathlib.Path(__file__).parent), "sonnet",
                      mcp=None, retry_when=rb.judge_unusable)
        self.assertIs(run.call_args.kwargs["retry_when"], rb.judge_unusable)

    def test_the_answerer_still_defaults_to_no_events(self):
        # Re-rolling a bad ANSWER would put a second sample where the run
        # records one. Only the default was ever meant to be the answerer's.
        with mock.patch.object(rb, "run_cli",
                               return_value=([], "", "", 1, 0.0)) as run:
            rb.claude("q", str(pathlib.Path(__file__).parent), "sonnet",
                      mcp=None)
        self.assertIs(run.call_args.kwargs["retry_when"], rb.no_events)

    def test_unparseable_prose_is_retried(self):
        self.assertTrue(rb.judge_unusable([{"type": "assistant"}],
                                          "The answer looks right to me."))

    def test_no_text_at_all_is_retried(self):
        self.assertTrue(rb.judge_unusable([], ""))

    def test_a_good_verdict_is_not_retried(self):
        self.assertFalse(rb.judge_unusable([{"type": "assistant"}], self.GOOD))

    def test_a_verdict_wrapped_in_prose_is_not_retried(self):
        # `parse_verdict` finds the object inside prose, so this is readable
        # and re-rolling it would just cost a judge call.
        self.assertFalse(rb.judge_unusable(
            [{"type": "assistant"}], f"Here you go:\n{self.GOOD}\nHope that helps"))

    def summary(self, **kw):
        base = dict(out=pathlib.Path("r"), set_dir=pathlib.Path("s"),
                    events_n=0, attempted=10, decided=8, passed=4, near=0,
                    human=0, doubted=[], vetoed=[], alt_path=0, unscorable=0,
                    retrieval_mode="semantic",
                    tally={"semantic": 1, "lexical": 0, "unreported": 0},
                    rs={}, answerer_cost=0.0, judge_cost=0.0, publisher="",
                    environment="e")
        return "\n".join(rb.summary_lines(**{**base, **kw}))

    def test_the_summary_names_the_cases_it_could_not_read(self):
        body = self.summary(unparseable=["cq-07", "cq-02"])
        self.assertIn("unreadable    2", body)
        self.assertIn("cq-02, cq-07", body)   # sorted, so a diff is stable

    def test_a_clean_run_still_says_zero(self):
        # Printed even at zero, for the reason `unscorable` is: a reader must
        # not have to know the line exists to notice it is missing.
        body = self.summary()
        self.assertIn("unreadable    0", body)

    def test_an_aborted_arm_suppresses_the_pass_rate(self):
        # The four-strikes abort fires, `run.json` says `aborted`, and the
        # headline still printed a percentage. Live: `passed 0 of 4 decided
        # (0%)` for an arm whose answerer never started.
        body = self.summary(aborted=True)
        self.assertIn("INCOMPLETE: the arm ABORTED", body)
        self.assertNotIn("(50%)", body)

    def test_a_truncated_case_suppresses_the_pass_rate(self):
        # The stop rule, made mechanical. A directive did not hold against an
        # arm that had already been paid for: on the run this comes from, the
        # truncation was known before the cases were judged and the rate was
        # quoted anyway.
        body = self.summary(truncated=["cq-01", "cq-05"], max_turns=30)
        self.assertIn("INCOMPLETE: 2 truncated", body)
        self.assertIn("no pass rate until re-run", body)
        self.assertNotIn("(50%)", body)   # 4 of 8 would have printed this

    def test_it_names_the_command_that_finishes_the_arm(self):
        # Four cases, not a whole arm: the answers already paid for are kept
        # and only the truncated ones are re-run, at twice the cap.
        body = self.summary(truncated=["cq-05", "cq-01"], max_turns=30)
        self.assertIn("--only cq-01,cq-05", body)
        self.assertIn("--max-turns 60", body)

    def test_a_contaminated_case_suppresses_it_too(self):
        body = self.summary(contaminated=["cq-03"])
        self.assertIn("INCOMPLETE: 1 contaminated", body)
        self.assertNotIn("(50%)", body)

    def test_both_are_named_when_both_happened(self):
        body = self.summary(truncated=["cq-01"], contaminated=["cq-03"])
        self.assertIn("INCOMPLETE: 1 truncated, 1 contaminated", body)

    def test_contamination_reasons_are_counted(self):
        # One breach across every case is a harness misconfiguration to fix
        # once; one breach on one case is that answerer going somewhere it
        # should not have. The count is what tells them apart.
        body = self.summary(
            contaminated=["cq-03", "cq-04"],
            contamination_reasons={"host tool available to the answerer: Bash": 2})
        self.assertIn("2x host tool available to the answerer: Bash", body)

    def test_a_complete_run_still_prints_its_rate(self):
        # The suppression is narrow: neither `unscorable` (a dataset state) nor
        # `unreadable` (which has a retry) withholds the number.
        body = self.summary(unscorable=3, unparseable=["cq-02"])
        self.assertIn("(50%)", body)
        self.assertNotIn("INCOMPLETE", body)

    def test_it_is_not_folded_into_unscorable(self):
        # `unscorable` is a DATASET state no answerer can change. This is the
        # harness failing to read its own judge. Counting them together would
        # send someone to fix answer keys that are fine.
        body = self.summary(unscorable=3, unparseable=["cq-02"])
        self.assertIn("unscorable    3", body)
        self.assertIn("unreadable    1", body)


class HostedProbe(unittest.TestCase):
    """What the hosted reachability probe concludes from its transcript.

    Three outcomes that are three different problems: no tool granted (a login
    that was never done), a tool that answered with an error (reachable and
    authenticated, wrong call), and a payload.
    """

    TOOLS = ("mcp__hosted__get_context",)

    def events(self, *blocks):
        out = []
        for kind, content in blocks:
            out.append({"type": kind, "message": {"content": content}})
        return out

    def use(self, tid="t1", name="mcp__hosted__get_context"):
        return {"type": "tool_use", "id": tid, "name": name, "input": {}}

    def res(self, text, tid="t1", err=False):
        return {"type": "tool_result", "tool_use_id": tid,
                "content": text, "is_error": err}

    def test_no_tool_call_at_all_is_not_granted(self):
        ev = self.events(("assistant", [{"type": "text",
                                         "text": "I have no such tool."}]))
        self.assertEqual(rb.probe_outcome(ev, self.TOOLS)[0], "not_granted")

    def test_an_errored_result_is_rejected_not_reached(self):
        # The defect this replaces: a bare tool_use returned True without ever
        # looking at its result, so a refused call passed the gate.
        ev = self.events(("assistant", [self.use()]),
                         ("user", [self.res("search_targets is required",
                                            err=True)]))
        outcome, said, payload = rb.probe_outcome(ev, self.TOOLS)
        self.assertEqual(outcome, "rejected")
        self.assertIn("search_targets is required", said)
        self.assertIsNone(payload)

    def test_a_payload_is_reached_and_comes_back(self):
        ev = self.events(("assistant", [self.use()]),
                         ("user", [self.res(json.dumps(
                             {"retrieval": "semantic", "sources": []}))]))
        outcome, _said, payload = rb.probe_outcome(ev, self.TOOLS)
        self.assertEqual(outcome, "reached")
        self.assertEqual(payload["retrieval"], "semantic")

    def test_a_good_call_outweighs_an_earlier_refused_one(self):
        ev = self.events(
            ("assistant", [self.use("t1")]),
            ("user", [self.res("nope", "t1", err=True)]),
            ("assistant", [self.use("t2")]),
            ("user", [self.res(json.dumps({"retrieval": "lexical"}), "t2")]))
        self.assertEqual(rb.probe_outcome(ev, self.TOOLS)[0], "reached")

    def test_a_call_with_no_result_is_not_a_missing_login(self):
        ev = self.events(("assistant", [self.use()]))
        self.assertEqual(rb.probe_outcome(ev, self.TOOLS)[0], "rejected")

    def test_a_call_to_an_unallowed_tool_is_ignored(self):
        ev = self.events(("assistant", [self.use(name="Bash")]),
                         ("user", [self.res("ok")]))
        self.assertEqual(rb.probe_outcome(ev, self.TOOLS)[0], "not_granted")

    def test_the_probe_asks_for_the_arguments_the_tool_requires(self):
        # A get_context with no arguments is a validation error on any server
        # that enforces its required parameters, which is what the probe used
        # to ask for.
        a = argparse.Namespace(environment="examples", package="storefront")
        args = rb.probe_arguments(a)
        self.assertTrue(args["search_targets"])
        self.assertEqual(args["scopes"],
                         [{"environment": "examples", "package": "storefront"}])


class UnscorablePreflight(unittest.TestCase):
    """A set of bare questions is a supported set, not a set to refuse.

    `skill:eval-import`: "A question with no golden is a case, not a reject...
    the answers it produces are what the keys get derived from." An earlier
    version of this guard refused to start on exactly that set, which broke the
    documented way to bootstrap keys.
    """

    def cases(self, *goldens):
        return [{"qid": f"q{i}", "golden": g}
                for i, g in enumerate(goldens, 1)]

    def refuse(self, *goldens):
        return rb.unscorable_preflight(self.cases(*goldens), "s")[2]

    def test_a_set_of_bare_questions_runs(self):
        self.assertIsNone(self.refuse(None, None, None))

    def test_one_bare_question_among_underived_keys_still_runs(self):
        # That one case gets an answer a key can be derived from, which is the
        # whole reason not to refuse.
        prov = {"status": "provisional", "value": 1}
        self.assertIsNone(self.refuse(prov, prov, None))

    def test_a_set_of_wholly_underived_keys_is_refused(self):
        prov = {"status": "provisional", "value": 1}
        self.assertIsNotNone(self.refuse(prov, prov))

    def test_the_refusal_names_both_ways_forward(self):
        msg = self.refuse({"status": "provisional", "value": 1})
        self.assertIn("--promote", msg)
        self.assertIn("check_coverage.py", msg)

    def test_the_refusal_does_not_send_anyone_to_refresh(self):
        # `--refresh` rewrites a drifted VALUE and never touches status, so
        # naming it here was an instruction that could not work.
        msg = self.refuse({"status": "provisional", "value": 1})
        self.assertIn("does not change a golden's status", msg)

    def test_a_scorable_set_is_not_refused(self):
        self.assertIsNone(self.refuse({"status": "verified", "value": 1}))

    def test_an_empty_set_is_not_refused_here(self):
        self.assertIsNone(rb.unscorable_preflight([], "s")[2])

    def test_the_counts_separate_derivable_from_underived(self):
        unscorable, derivable, _ = rb.unscorable_preflight(
            self.cases(None, {"status": "provisional", "value": 1},
                       {"status": "verified", "value": 2}), "s")
        self.assertEqual(len(unscorable), 2)
        self.assertEqual(derivable, ["q1"])


class GoldenCheckDoesNotClaimZero(unittest.TestCase):
    """`goldenCheck` may not assert a clean result for a check that never ran.

    `run_baseline` calls `verify()` without `model=`, so verify_goldens' check
    5 returns `[]` on an empty model text and the manifest wrote
    "0 other finding(s)" -- while the run depressed its own recall by exactly
    the stale names it did not look for.
    """

    def test_the_stale_count_reaches_the_claim(self):
        self.assertIn(
            "2 stale entity name(s)",
            rb.golden_check_note("49 ok, 0 drifted, 0 other finding(s)",
                                 ["shipped_at", "total_sales_2021"], True))

    def test_a_clean_lint_says_zero_rather_than_nothing(self):
        # Silence would read the same as the bug: the point is that the field
        # now says which check produced the zero.
        self.assertIn(
            "0 stale entity name(s)",
            rb.golden_check_note("49 ok, 0 drifted, 0 other finding(s)",
                                 [], True))

    def test_the_rubric_audit_is_named_as_not_run(self):
        # The missing `--model` silences check 4 as well as check 5, and only
        # check 5 has an in-arm substitute. A zero must not stand for check 4.
        got = rb.golden_check_note("49 ok, 0 drifted, 0 other finding(s)",
                                   [], True)
        self.assertIn("rubric-claim audit not run", got)

    def test_no_model_text_says_neither_audit_ran(self):
        got = rb.golden_check_note("49 ok, 0 drifted, 0 other finding(s)",
                                   [], False)
        self.assertIn("model-text audits not run", got)
        self.assertNotIn("stale entity name(s)", got)

    def test_a_check_that_already_says_it_did_not_run_is_left_alone(self):
        for gc in ("skipped", "skipped by --skip-golden-check",
                   "not run (rebuild)"):
            with self.subTest(gc):
                self.assertEqual(rb.golden_check_note(gc, ["x"], True), gc)


class PredictionCacheKey(unittest.TestCase):
    """The cache is keyed on the query, because `--rebuild` changes the query.

    `--rebuild` re-derives `final_query` from the saved transcript, so a fix to
    the derivation is exactly a change that makes a recorded attempt's query go
    from null to a real one -- the named-view capture is that fix. Keyed on the
    qid alone the old file won, and the judge was handed "the answerer ran no
    query" for an attempt that now has one.
    """

    def setUp(self):
        self.tmp = pathlib.Path(tempfile.mkdtemp())
        self.a = argparse.Namespace(
            publisher="http://localhost:1", environment="e", package="p",
            model_path="model.malloy")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def cached(self, query, rendered):
        d = self.tmp / "q"
        d.mkdir(parents=True, exist_ok=True)
        (d / "prediction.json").write_text(
            json.dumps({"query": query, "rendered": rendered}))

    def ask(self, final_query, reexec=False):
        return rb.prediction_for({"qid": "q"}, {"final_query": final_query},
                                 self.a, self.tmp, reexec)

    def test_the_same_query_reuses_the_cache(self):
        # The reason the cache exists: a re-judge must not need the server up,
        # and re-running one query per rubric iteration is waste.
        self.cached("run: a", "| total |")
        self.assertEqual(self.ask("run: a"), "| total |")

    def test_a_newly_derived_query_does_not_reuse_a_no_query_file(self):
        self.cached(None, "(the answerer ran no query, so there is nothing "
                          "to re-execute)")
        got = self.ask("run: order_items -> sales_summary_yoy")
        self.assertNotIn("ran no query", got)
        self.assertIn("not re-executed", got)

    def test_a_changed_query_does_not_reuse_the_old_rows(self):
        self.cached("run: a", "| total |\n| 1 |")
        self.assertNotIn("| 1 |", self.ask("run: b"))

    def test_a_file_from_before_the_key_existed_is_not_reused(self):
        d = self.tmp / "q"
        d.mkdir(parents=True)
        (d / "prediction.json").write_text(json.dumps({"rendered": "| old |"}))
        self.assertNotIn("| old |", self.ask("run: a"))

    def test_no_query_either_side_still_reuses(self):
        self.cached(None, "(the answerer ran no query, so there is nothing "
                          "to re-execute)")
        self.assertIn("ran no query", self.ask(None))

    def test_the_rewritten_file_records_the_query_it_holds(self):
        self.cached(None, "(nothing to re-execute)")
        self.ask("run: a")
        c = json.loads((self.tmp / "q" / "prediction.json").read_text())
        self.assertEqual(c["query"], "run: a")


class PlatformMcpUrl(unittest.TestCase):
    """The three ways to run differ only in --mcp-url, so it must be set.

    A platform run left on the local default measures a local Publisher --
    a different model over different data than the run records. The
    reachability probe catches it, but reports it as "tools unreachable",
    which reads as an OAuth problem.
    """

    def test_a_platform_run_on_the_local_default_is_refused(self):
        err = rb.platform_url_error("platform", rb.LOCAL_MCP_URL, "hosted")
        self.assertIsNotNone(err)
        # Names the cause and both hosted routes, not just "wrong url".
        self.assertIn("LOCAL Publisher", err)
        self.assertIn("bridge", err)
        self.assertIn("OAuth", err)

    def test_a_platform_run_with_a_hosted_url_is_fine(self):
        for url in ("http://localhost:7777/mcp",      # extension bridge
                    "https://example.invalid/global"):  # hosted directly
            with self.subTest(url=url):
                self.assertIsNone(
                    rb.platform_url_error("platform", url, "hosted"))

    def test_a_local_run_keeps_the_default(self):
        self.assertIsNone(
            rb.platform_url_error("local", rb.LOCAL_MCP_URL, "hosted"))



class RetrievalPrecisionIsReportedHonestly(unittest.TestCase):
    """Precision was computed and thrown away; the run printed recall alone.

    It is worth printing, but it reads everything outside `acceptable` as
    noise, so a set that never authored one scores every legitimate extra as a
    miss. Measured on a real run: 3% to 11%, on a set whose eight cases all had
    an empty `acceptable`. The caveat ships with the number.
    """

    def rs(self, **over):
        base = {"retrieval_scored": 8, "mean_recall": 0.938,
                "complete_retrievals": 7, "mean_precision": 0.073,
                "mean_returned": 25.6, "mean_required": 1.8,
                "cases_with_acceptable": 0, "failures_by_where_to_fix": {}}
        base.update(over)
        return base

    def test_precision_prints_with_its_caveat_when_nothing_authored_acceptable(self):
        text = "\n".join(self.lines(rs=self.rs()))
        self.assertIn("entity precision mean 7.3%", text)
        self.assertIn("no case authored `acceptable`", text)
        self.assertIn("breadth, not as a verdict", text)

    def test_breadth_does_not_depend_on_authoring(self):
        text = "\n".join(self.lines(rs=self.rs()))
        self.assertIn("26 returned per attempt for 2 the answer named", text)

    def test_a_fully_authored_set_gets_the_number_without_the_caveat(self):
        text = "\n".join(self.lines(rs=self.rs(cases_with_acceptable=8)))
        self.assertIn("entity precision mean 7.3%", text)
        self.assertNotIn("no case authored", text)
        self.assertNotIn("only 8 of 8", text)

    def test_a_partly_authored_set_says_it_is_uneven(self):
        text = "\n".join(self.lines(rs=self.rs(cases_with_acceptable=3)))
        self.assertIn("only 3 of 8", text)

    def lines(self, **over):
        return RunSummary.lines(self, **over)


class SkillsActuallyOpened(unittest.TestCase):
    """A run names the skills it granted; only the ones opened shaped anything.

    Measured on a real run: every attempt invoked zero of its 11 skills, so an
    edit to one could not have changed the answers and nothing in the report
    said so.
    """

    def test_zero_says_the_run_does_not_measure_the_skills(self):
        lines = rb.skill_lines({"attempts": 8, "with_skill": 0, "skills": []})
        text = "\n".join(lines)
        self.assertIn("0 of 8", text)
        self.assertIn("cannot be credited or blamed", text)

    def test_some_usage_names_the_skills_and_does_not_warn(self):
        lines = rb.skill_lines({"attempts": 8, "with_skill": 3,
                                "skills": ["malloy-phrase-detection"]})
        text = "\n".join(lines)
        self.assertIn("3 of 8", text)
        self.assertIn("malloy-phrase-detection", text)
        self.assertNotIn("cannot be credited", text)

    def test_no_attempts_prints_nothing(self):
        self.assertEqual(rb.skill_lines({"attempts": 0, "with_skill": 0}), [])
        self.assertEqual(rb.skill_lines(None), [])


class NoLocalShadowsAnImportedModule(unittest.TestCase):
    """A local named after an imported module breaks every call to that module
    in the same function, and only at runtime.

    `ledger = verify_definitions.load_ledger(...)` in main() made `ledger` a
    local for the whole function, so `ledger.run_config(...)` 260 lines earlier
    raised UnboundLocalError on EVERY run. The unit tests all call helpers, so
    nothing noticed until a real arm was run end to end. This is the cheap
    structural guard that would have.
    """

    def test_no_function_rebinds_a_module_this_file_imports(self):
        import ast
        src = pathlib.Path(rb.__file__).read_text()
        tree = ast.parse(src)
        modules = {n.names[0].asname or n.names[0].name.split(".")[0]
                   for n in ast.walk(tree) if isinstance(n, ast.Import)}
        bad = []
        for fn in ast.walk(tree):
            if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            for node in ast.walk(fn):
                if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Store) \
                        and node.id in modules:
                    bad.append(f"{fn.name}() rebinds the module name "
                               f"{node.id!r} at line {node.lineno}")
        self.assertEqual(bad, [], "; ".join(bad))


class UsageFields(unittest.TestCase):
    """The ledger must be able to reprice a run from its own token columns."""

    def test_all_four_token_counts_are_captured(self):
        u = {"input_tokens": 224, "output_tokens": 31507,
             "cache_read_input_tokens": 2435273,
             "cache_creation_input_tokens": 900000}
        self.assertEqual(rb.usage_fields(u), {
            "input_tokens": 224, "output_tokens": 31507,
            "cache_read_tokens": 2435273, "cache_write_tokens": 900000})

    def test_cache_writes_were_the_missing_column(self):
        # The one the ledger never held. On one analysed run it was 44% of the
        # agent's cost; cost_usd carried it and the breakdown could not.
        self.assertIn("cache_write_tokens", rb.usage_fields({}))

    def test_absent_usage_is_all_null_not_a_crash(self):
        self.assertEqual(set(rb.usage_fields(None).values()), {None})


class RunSummary(unittest.TestCase):
    """The end-of-run report is three layers, and the order is the point.

    What the run scored, then anything that makes that score untrustworthy,
    then the two measurements you click into and the commands that open the
    run properly. Flat, these read as one list of equals and the reader has to
    already know which is which.
    """

    def lines(self, **over):
        args = dict(
            out=pathlib.Path("results/r1"), set_dir=pathlib.Path("evals/e"),
            events_n=10, attempted=69, decided=49, passed=36, near=10, human=3,
            doubted=[], vetoed=[], alt_path=0, unscorable=0,
            retrieval_mode="semantic",
            tally={"semantic": 5, "lexical": 0, "unreported": 2},
            rs={"retrieval_scored": 49, "mean_recall": 0.842,
                "complete_retrievals": 41,
                "failures_by_where_to_fix": {"model": 8}},
            answerer_cost=4.0, judge_cost=0.5,
            publisher="http://localhost:4811", environment="samples")
        args.update(over)
        return rb.summary_lines(**args)

    def index_of(self, lines, needle):
        return next(i for i, l in enumerate(lines) if needle in l)

    def test_a_consumed_coverage_report_is_named_not_pointed_at(self):
        # With a report given, "not measured here" is false and must not print.
        lines = self.lines(coverage_report={
            "path": "cov.json", "version": "0.0.58", "agentModel": "sonnet",
            "decided": 45, "cases": 49})
        text = "\n".join(lines)
        self.assertIn("cov.json", text)
        self.assertIn("45 of 49", text)
        self.assertNotIn("not measured here", text)
        # 4 undecided: the report did not settle every case, and the reader
        # has to know which rows fell back to what.
        self.assertIn("undecided cases fell back", text)

    def test_a_report_that_decided_everything_carries_no_fallback_warning(self):
        lines = self.lines(coverage_report={
            "path": "cov.json", "version": "1", "agentModel": "m",
            "decided": 49, "cases": 49})
        self.assertNotIn("undecided cases fell back", "\n".join(lines))

    def test_no_report_keeps_the_pointer(self):
        self.assertIn("not measured here", "\n".join(self.lines()))

    def rows(self, spec):
        """Cascade input built the way the harness builds it.

        The fixtures here were hand-written dicts, and a hand-written cascade
        can describe a state `cascade()` cannot produce -- which is how a
        display bug survived its own test. Build the rows, let `cascade()`
        count them.
        """
        out = []
        for coverage, recall, failed, n in spec:
            out += [{"coverage": coverage, "recall": recall, "failed": failed,
                     "verdict": "no_match" if failed else "match"}] * n
        return rb.cascade(out)

    def test_the_cascade_reads_as_a_funnel_with_owners(self):
        lines = self.lines(cascade=self.rows([
            ("absent", None, True, 6),       # a known coverage gap
            ("unknown", 1.0, True, 2),       # coverage never measured
            ("covered", 0.5, True, 5),       # retrieved short
            ("covered", 1.0, True, 6),       # delivered, wrong
            ("covered", 1.0, False, 30),     # delivered, right
        ]))
        text = "\n".join(lines)
        self.assertIn("cascade       49 cases", text)
        self.assertIn("covered?      41 yes, 6 no (model gap), 2 unmeasured", text)
        # 43 rows carried a recall (49 less the 6 coverage gaps), 5 fell short.
        # The rung counts what recall MEASURED, not what the funnel let through.
        self.assertIn("retrieved?    38 yes, 5 no (the entity exists and did "
                      "not come back", text)
        self.assertNotIn("(documentation", text)
        self.assertIn("correct?      30 yes, 6 no (delivered, wrong", text)
        self.assertIn("diagnose decides", text)
        self.assertLess(self.index_of(lines, "cascade"),
                        self.index_of(lines, "  retrieval "))


    def test_unmeasured_coverage_does_not_zero_the_retrieval_rung(self):
        """check_coverage is a separate spend, so most runs have none.

        `cascade()` is an elif chain: a row whose coverage was never measured
        stops at the first rung and never reaches the recall check, so
        `not retrieved` is structurally 0. A display that subtracted it from a
        denominator therefore reported EVERY retrieval as a success -- 12
        cases, three at recall 0.5, printing "retrieved? 12 yes, 0 no". The
        rung reads a tally kept outside the chain.
        """
        text = "\n".join(self.lines(cascade=self.rows([
            ("unknown", 1.0, False, 10),
            ("unknown", 0.5, False, 1),
            ("unknown", None, False, 1),
        ])))
        self.assertIn("covered?      0 yes, 0 no (model gap), 12 unmeasured", text)
        self.assertIn("retrieved?    10 yes, 1 no", text)
        self.assertNotIn("retrieved?    12 yes", text)
        self.assertNotIn("retrieved?    0 yes", text)


    def test_no_cascade_prints_nothing(self):
        self.assertNotIn("cascade", "\n".join(self.lines()))

    def test_the_three_layers_appear_in_order(self):
        lines = self.lines()
        self.assertLess(self.index_of(lines, "RESULTS"),
                        self.index_of(lines, "COVERAGE & RETRIEVAL"))
        self.assertLess(self.index_of(lines, "COVERAGE & RETRIEVAL"),
                        self.index_of(lines, "DEEP DIVE"))

    def test_the_headline_carries_the_score(self):
        lines = self.lines()
        headline = lines[self.index_of(lines, "passed")]
        self.assertIn("36 of 49", headline)
        self.assertIn("73%", headline)

    def test_an_untrustworthy_score_is_flagged_above_the_detail(self):
        # A doubted golden is a DATASET problem. Read after the retrieval
        # numbers it looks like one more measurement.
        lines = self.lines(doubted=[("q7", "suspect", "note", "judge")])
        self.assertLess(self.index_of(lines, "not believed"),
                        self.index_of(lines, "COVERAGE & RETRIEVAL"))
        self.assertIn("NOT model failures",
                      lines[self.index_of(lines, "not believed")])

    def test_each_doubted_golden_says_who_doubted_it(self):
        # A status the SET declared is a key somebody already settled, not an
        # opinion the judge formed this run. Attributing both to the judge
        # claimed a judgement that never happened.
        judged = self.lines(doubted=[("q7", "suspect", "n", "judge")])
        declared = self.lines(doubted=[("q7", "verified_wrong", "n", "set")])
        self.assertIn("the judge says",
                      judged[self.index_of(judged, "q7")])
        self.assertIn("the set declares",
                      declared[self.index_of(declared, "q7")])

    def test_a_clean_run_raises_no_alarms(self):
        self.assertFalse([l for l in self.lines() if l.startswith("!")])

    def test_coverage_says_it_was_not_measured_and_how_to_measure_it(self):
        lines = self.lines()
        block = "\n".join(lines)
        self.assertIn("not measured here", block)
        self.assertIn("check_coverage.py", block)
        self.assertIn("--set evals/e", block)

    def test_the_deep_dive_ends_in_a_url_a_human_can_open(self):
        # The point of the layer: a run directory is JSONL, and the reader
        # needs the served app, not the record it was built from.
        block = "\n".join(self.lines())
        self.assertIn("build_run_package.py", block)
        self.assertIn("--run results/r1", block)
        self.assertIn(
            "http://localhost:4811/environments/samples/packages/eval-r1/",
            block)

    def test_the_deep_dive_registers_the_package_it_just_built(self):
        # The URL only resolves after the POST, and the two have to name the
        # same package and the same directory or the link 404s.
        block = "\n".join(self.lines())
        self.assertIn("--out /tmp/eval-r1", block)
        self.assertIn('"name":"eval-r1"', block)
        self.assertIn('"location":"/tmp/eval-r1"', block)
        self.assertIn(
            "POST http://localhost:4811/api/v0/environments/samples/packages",
            block.replace("-sS -X ", ""))

    def test_the_deep_dive_still_names_the_raw_events(self):
        self.assertIn("events.jsonl", "\n".join(self.lines()))

    def test_a_lexical_run_says_so_where_the_number_is(self):
        lines = self.lines(retrieval_mode="lexical",
                           tally={"semantic": 0, "lexical": 5,
                                  "unreported": 0})
        warn = self.index_of(lines, "not a semantic run")
        self.assertLess(self.index_of(lines, "COVERAGE & RETRIEVAL"), warn)
        self.assertLess(warn, self.index_of(lines, "DEEP DIVE"))

    def test_no_retrieval_scores_omits_the_recall_line(self):
        lines = self.lines(rs={"retrieval_scored": 0, "mean_recall": None,
                               "complete_retrievals": 0,
                               "failures_by_where_to_fix": {}})
        self.assertFalse([l for l in lines if "entity recall" in l])


class QuestionSha(unittest.TestCase):
    def test_the_case_text_is_hashed_when_nothing_stamped_one(self):
        # It read `questionSha` alone, which no case writes, so the field was
        # null on every attempt and two runs could not be compared at all.
        got = rb.question_sha({"question": "How many orders shipped late?"})
        self.assertEqual(got, rb.sha256(b"How many orders shipped late?"))

    def test_an_authored_hash_wins_over_the_case_text(self):
        # The point of `questionSha` is that it comes from the authored file,
        # so a question edited in cases.jsonl must NOT re-hash to a match.
        got = rb.question_sha({"question": "edited", "questionSha": "abc123"})
        self.assertEqual(got, "abc123")

    def test_two_spellings_of_one_question_do_not_collide(self):
        a = rb.question_sha({"question": "the frequency distribution"})
        b = rb.question_sha({"question": "the reach frequency distribution"})
        self.assertNotEqual(a, b)



class RunLabels(unittest.TestCase):
    def setUp(self):
        self.tmp = pathlib.Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_a_fresh_parent_starts_at_01(self):
        self.assertEqual(rb.next_run_label(self.tmp / "x", "ecom", "baseline"),
                         "ecom-baseline-01")

    def test_hand_named_siblings_count_through_their_run_json_label(self):
        # Every documented example hand-names --out, so directory names never
        # matched the stem and four runs in one afternoon all got -01.
        (self.tmp / "arm1").mkdir()
        (self.tmp / "arm1" / "run.json").write_text(
            json.dumps({"label": "ecom-baseline-01"}))
        (self.tmp / "ecom-baseline-02").mkdir()
        self.assertEqual(rb.next_run_label(self.tmp / "arm3", "ecom", "baseline"),
                         "ecom-baseline-03")

    def test_another_phase_or_set_does_not_consume_a_number(self):
        (self.tmp / "a").mkdir()
        (self.tmp / "a" / "run.json").write_text(
            json.dumps({"label": "ecom-acceptance-01"}))
        self.assertEqual(rb.next_run_label(self.tmp / "b", "ecom", "baseline"),
                         "ecom-baseline-01")


class ExistingRunRefusal(unittest.TestCase):
    def setUp(self):
        self.tmp = pathlib.Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_an_absent_or_empty_directory_is_fine(self):
        self.assertIsNone(rb.existing_run_refusal(self.tmp / "new"))
        (self.tmp / "empty").mkdir()
        self.assertIsNone(rb.existing_run_refusal(self.tmp / "empty"))

    def test_a_directory_holding_a_ledger_is_refused(self):
        (self.tmp / "events.jsonl").write_text("{}\n")
        why = rb.existing_run_refusal(self.tmp)
        self.assertIn("events.jsonl", why)
        self.assertIn("--from", why)

    def test_transcripts_alone_are_refused_too(self):
        # Exactly the state a lost ledger leaves behind; a new arm on top of it
        # would bury the evidence that a record was ever there.
        (self.tmp / "artifacts" / "q1").mkdir(parents=True)
        self.assertIn("1 artifact dir", rb.existing_run_refusal(self.tmp))


class RebuildCaseList(unittest.TestCase):
    def test_only_cases_with_a_transcript_are_rebuilt(self):
        tmp = pathlib.Path(tempfile.mkdtemp())
        try:
            for q, files in (("a", ["answerer.jsonl"]), ("b", ["answer.md"]),
                             ("c", ["answerer.jsonl", "answer.md"])):
                (tmp / q).mkdir()
                for f in files:
                    (tmp / q / f).write_text("")
            self.assertEqual(rb.transcript_qids(tmp), {"a", "c"})
            self.assertEqual(rb.transcript_qids(tmp / "nope"), set())
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


class ContaminatedAttemptsLeaveTheAggregates(unittest.TestCase):
    """A flagged attempt must not reach the printed score.

    The ledger has nulled a contaminated verdict since 2026-09-01, but the
    nulling landed on the copy bound for events.jsonl while the summary read
    the judge's original. A 33-case run in which EVERY attempt was flagged
    printed `12 of 21 decided (57%)` over a ledger whose every score event said
    `verdict: null`. That is the contamination check working and the line a
    human reads disagreeing with it.
    """

    def test_a_flagged_attempt_is_not_counted_as_a_pass(self):
        verdicts = {"q1": {"verdict": "match"}, "q2": {"verdict": "match"}}
        attempts = {"q1": {"breaches": ["host tool available: TaskCreate"]},
                    "q2": {"breaches": []}}
        for qid, v in verdicts.items():
            if attempts[qid].get("breaches"):
                v["verdict"] = None
        decided = [v for v in verdicts.values()
                   if v.get("verdict") in ("match", "no_match")]
        self.assertEqual(len(decided), 1, "the flagged attempt still counted")
        self.assertIsNone(verdicts["q1"]["verdict"])
        self.assertEqual(verdicts["q2"]["verdict"], "match")

    def test_a_fully_contaminated_run_decides_nothing(self):
        verdicts = {f"q{i}": {"verdict": "match"} for i in range(21)}
        attempts = {q: {"breaches": ["host tool available: TaskCreate"]}
                    for q in verdicts}
        for qid, v in verdicts.items():
            if attempts[qid].get("breaches"):
                v["verdict"] = None
        decided = [v for v in verdicts.values()
                   if v.get("verdict") in ("match", "no_match")]
        self.assertEqual(len(decided), 0,
                         "a run with no clean attempt reported a score")

class GitSha(unittest.TestCase):
    """The pin has to mark a dirty model repo, and it did not.

    The path is used BOTH as git's working directory (`-C`) and as its
    pathspec. A relative one means different things in those two positions:
    git runs IN the directory and then resolves the pathspec against it, so
    `--model-repo sub` looks for `sub/sub`, matches nothing, and every tree
    reads clean. The sibling in `ledger.skills_git_sha` was fixed for exactly
    this; `git_sha` was not, and had no test at all.
    """

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.repo = pathlib.Path(self.tmp) / "repo"
        (self.repo / "sub").mkdir(parents=True)
        (self.repo / "sub" / "model.malloy").write_text("source: a is x\n")
        run = lambda *a: subprocess.run(a, cwd=self.repo, capture_output=True)
        run("git", "init", "-q")
        run("git", "config", "user.email", "t@t")
        run("git", "config", "user.name", "t")
        run("git", "add", "-A")
        run("git", "commit", "-qm", "init")
        self.cwd = os.getcwd()
        os.chdir(self.repo)

    def tearDown(self):
        os.chdir(self.cwd)
        shutil.rmtree(self.tmp, ignore_errors=True)

    def dirty(self):
        (self.repo / "sub" / "model.malloy").write_text("source: a is CHANGED\n")

    def test_a_clean_tree_has_no_marker(self):
        self.assertFalse(
            rb.git_sha(pathlib.Path("sub"), scope=pathlib.Path("sub"))
            .endswith("-dirty"))

    def test_dirt_outside_the_scope_is_not_the_models(self):
        # A scratch file at the repo root: the tree is dirty, the model is not.
        # Scoped to the package directory the marker says so; unscoped it
        # stamps a clean model -dirty, which is what every run pin used to read.
        (self.repo / "scratch.txt").write_text("notes\n")
        self.assertFalse(
            rb.git_sha(pathlib.Path("."), scope=pathlib.Path("sub"))
            .endswith("-dirty"))
        self.assertTrue(rb.git_sha(pathlib.Path(".")).endswith("-dirty"))

    def test_a_scope_outside_the_repo_pins_nothing_rather_than_clean(self):
        # git exits 128 with empty stdout for a pathspec outside the repo; that
        # used to read as "clean" on a dirty model. No pin is the honest answer.
        self.dirty()
        outside = pathlib.Path(self.tmp) / "elsewhere"
        outside.mkdir()
        self.assertIsNone(rb.git_sha(pathlib.Path("."), scope=outside))

    def test_a_scope_that_does_not_exist_pins_nothing(self):
        # A typo (`pgk`) or a doubled path (`--model-repo repo/pkg --model-dir
        # pkg`): git status on a missing pathspec exits 0 with empty output and
        # used to read as clean on a dirty model.
        self.dirty()
        self.assertIsNone(rb.git_sha(pathlib.Path("."), scope=pathlib.Path("pgk")))
        self.assertIsNone(rb.git_sha(pathlib.Path("."), scope=pathlib.Path("sub/sub")))

    def test_a_relative_model_dir_resolves_against_the_repo(self):
        repo = pathlib.Path("/r")
        self.assertEqual(rb.model_scope(repo, pathlib.Path("packages/x")), repo / "packages/x")
        self.assertEqual(rb.model_scope(repo, pathlib.Path("/abs/x")), pathlib.Path("/abs/x"))
        self.assertEqual(rb.model_scope(repo, None), repo)
        self.assertIsNone(rb.model_scope(None, pathlib.Path("packages/x")))

    def test_a_relative_path_still_marks_dirt(self):
        self.dirty()
        self.assertTrue(
            rb.git_sha(pathlib.Path("sub"), scope=pathlib.Path("sub"))
            .endswith("-dirty"),
            "a relative --model-repo silently reported a dirty model as clean")

    def test_an_absolute_path_agrees_with_the_relative_one(self):
        self.dirty()
        rel = rb.git_sha(pathlib.Path("sub"), scope=pathlib.Path("sub"))
        abs_ = rb.git_sha(self.repo / "sub", scope=self.repo / "sub")
        self.assertEqual(rel, abs_, "the pin depends on how it was spelled")

    def test_the_scope_still_narrows(self):
        # Dirt OUTSIDE the scope is not the model's, which is the whole reason
        # the scope exists. This must not regress into "any dirt anywhere".
        (self.repo / "scratch.txt").write_text("unrelated")
        self.assertFalse(
            rb.git_sha(pathlib.Path("sub"), scope=pathlib.Path("sub"))
            .endswith("-dirty"))

    def test_not_a_repo_is_none(self):
        self.assertIsNone(rb.git_sha(pathlib.Path(self.tmp)))


class GoldenCheckScope(unittest.TestCase):
    """The pre-arm audit covers the cases the arm RUNS, not the whole file.

    A set is built incrementally, so one un-derivable golden elsewhere in
    cases.jsonl blocked an arm over a subset that did not include it. The only
    way past was --skip-golden-check, which switches the audit off for the
    cases that would have passed and stamps "skipped" into run.json.
    """

    SRC = (pathlib.Path(__file__).resolve().parent / "run_baseline.py").read_text()

    def test_the_call_passes_the_selected_qids(self):
        # A source pin, like test_kinds_by_target_matches_the_server: the call
        # sits inside main() between a server probe and an arm, so the cheap
        # way to stop it silently losing the argument again is to read it.
        call = self.SRC.split("verify_goldens.verify(")[1][:400]
        self.assertIn("qids=checked_qids", call,
                      "the golden check audits the whole set again")

    def test_the_qids_come_from_the_narrowed_case_list(self):
        self.assertIn('checked_qids = {c["qid"] for c in cases}', self.SRC)

    def test_verify_actually_filters_on_qids(self):
        # The other half: the argument has to mean something.
        import inspect
        import sys as _sys
        _sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent.parent
                                / "eval-answer" / "scripts"))
        import verify_goldens
        src = inspect.getsource(verify_goldens.verify)
        self.assertIn("qids", src)
        self.assertIn('c["qid"] in qids', src)

class CoverageByDefault(unittest.TestCase):
    """Coverage is measured unless asked not to.

    It answers the first of the four questions a run reports -- can the model
    express an answer at all -- and it is the one that says whether a failure
    was ever winnable. Left to a follow-up command it went unrun, and the
    covered? rung came back blank on every real run.
    """

    def ns(self, **kw):
        a = argparse.Namespace(
            coverage=None, no_coverage=False, rebuild=False,
            out=pathlib.Path("/tmp/does-not-matter"),
            set_dir=pathlib.Path("evals/e"), publisher="http://p",
            environment="env", package="pkg", timeout=60,
            parallel=4, only=None, rejudge=False)
        for k, v in kw.items():
            setattr(a, k, v)
        return a

    def test_it_runs_when_no_report_was_given(self):
        a = self.ns()
        with mock.patch.object(rb, "subprocess") as sp, \
             mock.patch.object(pathlib.Path, "exists", return_value=True):
            sp.run.return_value = argparse.Namespace(returncode=0)
            out = rb.measure_coverage(a)
        self.assertIsNotNone(out)
        cmd = sp.run.call_args[0][0]
        self.assertIn("--publisher", cmd)
        self.assertIn("http://p", cmd)
        # Through REST, so it needs no local checkout and stays valid for every
        # arm against this model version.
        self.assertNotIn("--model", cmd)
        self.assertIn("--parallel", cmd)

    def test_only_narrows_coverage_as_it_narrows_the_arm(self):
        """A one-case re-run paid a claude -p call for every case in the set."""
        a = self.ns(only="q1,q2")
        with mock.patch.object(rb, "subprocess") as sp, \
             mock.patch.object(pathlib.Path, "exists", return_value=True):
            sp.run.return_value = argparse.Namespace(returncode=0)
            rb.measure_coverage(a)
        cmd = sp.run.call_args[0][0]
        self.assertIn("--only", cmd)
        self.assertIn("q1,q2", cmd)

    def test_a_failure_does_not_kill_the_arm(self):
        """Losing one rung must not cost the answerers."""
        a = self.ns()
        with mock.patch.object(rb, "subprocess") as sp:
            sp.run.return_value = argparse.Namespace(returncode=1)
            self.assertIsNone(rb.measure_coverage(a))


class OffloadedToolResult(unittest.TestCase):
    """A response spilled to a file is not an empty response.

    A get_context result too large for the model's context is written to a
    file and replaced by a notice naming the path. The answerer reads the file
    and is unaffected; the ledger used to parse the notice, find no JSON, and
    record an empty entity list -- a total retrieval miss on a call that
    returned in full. One arm printed 86.4% recall against an actual 95%, and
    diagnose then explained the phantom miss with an index-readiness story
    that was false.
    """

    def test_the_body_is_read_back_from_the_named_file(self):
        with tempfile.TemporaryDirectory() as d:
            p = pathlib.Path(d) / "big.txt"
            p.write_text(json.dumps({"sources": [], "retrieval": "semantic"}))
            out = rb.offloaded_json(
                f"Error: result exceeds maximum allowed tokens. "
                f"Output has been saved to {p}")
        self.assertEqual(out, {"sources": [], "retrieval": "semantic"})

    def test_an_unreadable_path_is_none_not_empty(self):
        self.assertIsNone(rb.offloaded_json("saved to /nope/does-not-exist.txt"))

    def test_an_ordinary_result_is_not_mistaken_for_an_offload(self):
        self.assertIsNone(rb.offloaded_json('{"sources": []}'))


class NarrowedRebuildKeepsTheLedger(unittest.TestCase):
    """`--rebuild --only <qid>` re-derives one case. It used to write the whole
    ledger from that one case, and a 29-case arm read as a 1-case arm."""

    def setUp(self):
        self.tmp = pathlib.Path(tempfile.mkdtemp())
        self.path = self.tmp / "events.jsonl"
        self.old = [{"kind": "attempt", "qid": "a", "sample": 1},
                    {"kind": "score", "qid": "a", "verdict": "match"},
                    {"kind": "attempt", "qid": "b", "sample": 1},
                    {"kind": "score", "qid": "b", "verdict": "no_match"}]
        self.path.write_text("".join(json.dumps(e) + "\n" for e in self.old))

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_only_the_named_cases_lines_are_replaced(self):
        new = [{"kind": "attempt", "qid": "b", "sample": 1},
               {"kind": "score", "qid": "b", "verdict": "match"}]
        rb.store_events(self.path, new, replaced_qids={"b"})
        got = [json.loads(l) for l in self.path.read_text().splitlines()]
        self.assertEqual(got, self.old[:2] + new)

    def test_a_full_write_still_replaces_everything(self):
        new = [{"kind": "attempt", "qid": "c", "sample": 1}]
        rb.store_events(self.path, new, None)
        got = [json.loads(l) for l in self.path.read_text().splitlines()]
        self.assertEqual(got, new)


class PersistedStubIsTheResult(unittest.TestCase):
    """Above a size the CLI decides, a tool result reaches the answerer as a
    stub naming a file. Reading the stub as the payload scored 14 of 74
    get_context calls on one arm as zero entities delivered."""

    def setUp(self):
        self.tmp = pathlib.Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def block(self, text):
        return {"content": [{"type": "text", "text": text}]}

    def test_the_persisted_output_stub_resolves_to_the_saved_blocks(self):
        saved = self.tmp / "r.json"
        saved.write_text(json.dumps([{"type": "text", "text": "ranked: a, b, c"}]))
        stub = (f"<persisted-output>\nFull output saved to: {saved}\n\n"
                f"Preview (first 2KB):\nranked: a")
        self.assertEqual(rb.result_text(self.block(stub)), "ranked: a, b, c")

    def test_the_token_cap_spelling_resolves_to_the_saved_text(self):
        saved = self.tmp / "r.txt"
        saved.write_text("ranked: a, b, c")
        stub = ("Error: result (68,820 characters across 1 line) exceeds maximum "
                f"allowed tokens. Output has been saved to {saved}.")
        self.assertEqual(rb.result_text(self.block(stub)), "ranked: a, b, c")

    def test_a_stub_whose_file_is_gone_is_left_as_it_was(self):
        stub = f"<persisted-output>\nFull output saved to: {self.tmp / 'gone.json'}\n"
        self.assertEqual(rb.result_text(self.block(stub)), stub)

    def test_a_missing_file_is_reported_as_unmeasured_not_zero(self):
        # A rebuild after the CLI's temporary file is gone: both spellings,
        # with the colon and with the trailing period, name a file, and the
        # caller must record no summary rather than an empty one.
        for note, named in (
                (f"<persisted-output>\nFull output saved to: {self.tmp / 'gone.json'}\n\nPreview", "gone.json"),
                (f"Error: result exceeds maximum allowed tokens. Output has been saved to {self.tmp / 'gone.txt'}.", "gone.txt")):
            with self.subTest(note=note[:30]):
                path, body = rb.saved_result(note)
                self.assertEqual(path, self.tmp / named)
                self.assertIsNone(body)
                self.assertIsNone(rb.offloaded_json(note))

    def test_the_colon_spelling_is_read_back_too(self):
        saved = self.tmp / "r.json"
        saved.write_text(json.dumps({"sources": [], "retrieval": "semantic"}))
        self.assertEqual(rb.offloaded_json(f"<persisted-output>\nFull output saved to: {saved}\n"),
                         {"sources": [], "retrieval": "semantic"})

    def test_a_result_that_mentions_a_real_path_is_left_alone(self):
        # A source doc inside an ordinary JSON result says a file is written
        # somewhere real. That is a result, not a note: it comes back as it is,
        # and the file is never read in its place.
        real = self.tmp / "extract.txt"; real.write_text("not the result")
        result = json.dumps({"sources": [{"docs": f"Nightly extract is written to {real} for downstream jobs."}], "retrieval": "semantic"})
        self.assertEqual(rb.result_text(self.block(result)), result)
        self.assertIsNone(rb.saved_result(f"Nightly extract is written to {real}")[0])

    def test_only_the_clis_own_note_names_a_saved_result(self):
        saved = self.tmp / "r.json"; saved.write_text(json.dumps([{"type": "text", "text": "body"}]))
        self.assertEqual(rb.result_text(self.block(f"see {saved} for details")), f"see {saved} for details")
        self.assertEqual(rb.result_text(self.block(f"<persisted-output>\nFull output saved to: {saved}\n")), "body")

    def test_an_unreadable_file_is_not_read_twice(self):
        # The path is a directory: the read raises OSError, and the note comes
        # back as it was instead of a second read raising out of the handler.
        stub = f"<persisted-output>\nFull output saved to: {self.tmp}\n"
        self.assertEqual(rb.result_text(self.block(stub)), stub)

    def test_an_ordinary_result_is_unchanged(self):
        self.assertEqual(rb.result_text(self.block("{\"sources\": []}")), "{\"sources\": []}")


class RejudgeImpliesRebuild(unittest.TestCase):
    """`--rejudge` alone answered every case again and, with `--only`, wrote
    the ledger from the one case. It is rebuild's answer half plus a fresh
    judge, and every gate reads `a.rebuild`."""

    def test_rejudge_sets_rebuild(self):
        a = rb.imply_flags(argparse.Namespace(rejudge=True, rebuild=False))
        self.assertTrue(a.rebuild)

    def test_rebuild_alone_is_unchanged(self):
        a = rb.imply_flags(argparse.Namespace(rejudge=False, rebuild=True))
        self.assertTrue(a.rebuild)
        self.assertFalse(a.rejudge)

    def test_neither_flag_stays_a_fresh_run(self):
        a = rb.imply_flags(argparse.Namespace(rejudge=False, rebuild=False))
        self.assertFalse(a.rebuild)

    def test_a_narrowed_rejudge_splices_the_ledger(self):
        # The splice decision reads (rebuild and only); with rebuild implied,
        # a `--rejudge --only <qid>` keeps the other cases' lines.
        a = rb.imply_flags(argparse.Namespace(rejudge=True, rebuild=False, only="q2"))
        self.assertTrue(a.rebuild and a.only)


class AnErroredGetContextIsUnmeasured(unittest.TestCase):
    """A get_context call the server refused (an answerer that left out
    `scopes` gets an MCP validation error) is no ranking at all. Recorded as
    an empty rankedSummary it scored as a search that found nothing, and the
    retrieval score counted a refusal as a miss the run never observed."""

    def setUp(self):
        self.tmp = pathlib.Path(tempfile.mkdtemp())
        self.set_dir = self.tmp / "set"
        self.set_dir.mkdir()

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def rebuild(self, blocks):
        d = self.tmp / "art" / "q1"
        d.mkdir(parents=True)
        events = [{"type": kind, "message": {"content": content}}
                  for kind, content in blocks]
        events.append({"type": "result", "subtype": "success",
                       "is_error": False, "usage": {}, "num_turns": 1})
        (d / "answerer.jsonl").write_text(
            "\n".join(json.dumps(e) for e in events) + "\n")
        a = argparse.Namespace(rebuild=True, target="local",
                               set_dir=self.set_dir)
        return rb.run_answerer({"qid": "q1", "question": "how many?"},
                               a, self.tmp / "art")

    def use(self, tid):
        return {"type": "tool_use", "id": tid,
                "name": "mcp__publisher__get_context",
                "input": {"searchTerms": ["orders"]}}

    def res(self, tid, text, err):
        return {"type": "tool_result", "tool_use_id": tid,
                "content": text, "is_error": err}

    def test_a_refused_call_carries_no_summary_and_its_error(self):
        got = self.rebuild([
            ("assistant", [self.use("t1")]),
            ("user", [self.res("t1", "MCP error -32602: scopes required", True)]),
            ("assistant", [{"type": "text", "text": "I could not search."}])])
        self.assertEqual(got["n_get_context"], 1)
        [call] = got["calls"]
        self.assertIsNone(call["rankedSummary"])
        self.assertIn("scopes required", call["error"])

    def test_a_call_that_answered_still_ranks(self):
        # The guard on the errored path must not swallow the ordinary one.
        body = json.dumps({"sources": [{"name": "orders", "relevance": 0.9}]})
        got = self.rebuild([
            ("assistant", [self.use("t1")]),
            ("user", [self.res("t1", body, False)]),
            ("assistant", [{"type": "text", "text": "Orders it is."}])])
        [call] = got["calls"]
        self.assertIsNotNone(call["rankedSummary"])
        self.assertIsNone(call["error"])


class NothingIsDefinedBelowTheMainGuard(unittest.TestCase):
    """CI runs this file as a script, `python3 <file>`, and `unittest.main()`
    runs what is defined so far and exits. A test class written below the
    guard is collected by `-m unittest` and never by CI: four classes sat
    there after one PR, and one review earlier had moved two more up for the
    same reason. Reading the file is the check that does not depend on how the
    tests were invoked."""

    def test_the_guard_is_the_last_statement(self):
        src = pathlib.Path(__file__).read_text().splitlines()
        guard = [i for i, l in enumerate(src) if l.startswith('if __name__ == "__main__":')]
        self.assertEqual(len(guard), 1)
        below = [l for l in src[guard[0]:] if l.startswith(("class ", "def "))]
        self.assertEqual(below, [], f"defined below the main guard: {below}")


if __name__ == "__main__":
    unittest.main()
