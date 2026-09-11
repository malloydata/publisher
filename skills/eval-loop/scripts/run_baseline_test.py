#!/usr/bin/env python3
"""Tests for run_baseline's recording decisions: which query an attempt is
credited with, which retriever answered, what was actually re-executed, and
when an attempt is unjudgeable."""
import argparse
import json
import pathlib
import shutil
import sys
import tempfile
import unittest

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
                               "noQuery": 1, "notReExecuted": 1, "missing": 1})

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
            + got["notReExecuted"] + got["missing"],
            len(qids))
        # `attempted` is the derived one, outside the partition.
        self.assertEqual(got["attempted"], got["ok"] + got["failed"])

    def test_unreadable_prediction_counts_as_missing(self):
        d = self.tmp / "junk"
        d.mkdir(parents=True)
        (d / "prediction.json").write_text("{not json")
        got = rb.reexecution_summary(self.tmp, ["junk"])
        self.assertEqual(got["missing"], 1)


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
        self.assertEqual(v["gold_status"], "provisional")

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
        v = self.judge({"status": "verified", "kind": "criteria",
                        "criteria": ["breaks the total out by region"]})
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


class GoldenCheckDoesNotClaimZero(unittest.TestCase):
    """`goldenCheck` may not assert a clean result for a check that never ran.

    `run_baseline` calls `verify()` without `model=`, so verify_goldens' check
    5 returns `[]` on an empty model text and the manifest wrote
    "0 other finding(s)" -- while the run depressed its own recall by exactly
    the stale names it did not look for.
    """

    def test_the_stale_count_reaches_the_claim(self):
        self.assertEqual(
            rb.golden_check_note("49 ok, 0 drifted, 0 other finding(s)",
                                 ["shipped_at", "total_sales_2021"], True),
            "49 ok, 0 drifted, 0 other finding(s), 2 stale entity name(s)")

    def test_a_clean_lint_says_zero_rather_than_nothing(self):
        # Silence would read the same as the bug: the point is that the field
        # now says which check produced the zero.
        self.assertTrue(
            rb.golden_check_note("49 ok, 0 drifted, 0 other finding(s)",
                                 [], True).endswith("0 stale entity name(s)"))

    def test_no_model_text_says_the_lint_did_not_run(self):
        got = rb.golden_check_note("49 ok, 0 drifted, 0 other finding(s)",
                                   [], False)
        self.assertIn("entity-name lint not run", got)
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
        lines = self.lines(doubted=[("q7", "suspect", "note")])
        self.assertLess(self.index_of(lines, "does not believe"),
                        self.index_of(lines, "COVERAGE & RETRIEVAL"))
        self.assertIn("NOT model failures",
                      lines[self.index_of(lines, "does not believe")])

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


if __name__ == "__main__":
    unittest.main()