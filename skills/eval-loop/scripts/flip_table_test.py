#!/usr/bin/env python3
"""Tests for flip_table's gates: the retriever pair, the stable near_match
list, and the calibration block a band has to be quoted against."""
import contextlib
import io
import json
import pathlib
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import flip_table as ft  # noqa: E402


def write_run(root: pathlib.Path, name: str, scores: dict[str, str],
              **cfg) -> pathlib.Path:
    d = root / name
    d.mkdir(parents=True)
    base = {"runId": name, "target": "publisher", "answererModel": "sonnet",
            "phase": "baseline", "started": "2026-09-03T00:00:00Z",
            "datasetVersion": 13, "judgeVersion": 4, "judgeModel": "sonnet",
            "retrievalMode": "semantic"}
    base.update(cfg)
    (d / "run.json").write_text(json.dumps(base))
    lines = [json.dumps({"kind": "score", "qid": q, "sample": None,
                         "phase": "baseline", "verdict": v, "reason": ""})
             for q, v in scores.items()]
    lines += [json.dumps({"kind": "attempt", "qid": q, "sample": None,
                          "phase": "baseline", "cost_usd": 0.1,
                          "submitted": True, "final_query": "run: x",
                          "answer_text": "a", "transcriptPath": "t"})
              for q in scores]
    (d / "events.jsonl").write_text("\n".join(lines) + "\n")
    return d


class Gate(unittest.TestCase):
    def test_matching_semantic_passes(self):
        self.assertEqual(ft.retrieval_gate({"retrievalMode": "semantic"},
                                           {"retrievalMode": "semantic"},
                                           "a", "b", False), 0)

    def test_differing_modes_fail(self):
        self.assertEqual(ft.retrieval_gate({"retrievalMode": "semantic"},
                                           {"retrievalMode": "lexical"},
                                           "a", "b", False), 2)

    def test_mixed_on_one_side_fails_even_when_equal(self):
        self.assertEqual(ft.retrieval_gate({"retrievalMode": "mixed"},
                                           {"retrievalMode": "mixed"},
                                           "a", "b", False), 2)

    def test_two_mixed_arms_are_refused_for_the_right_reason(self):
        # Refusing is right -- a mixed arm is not a measurement whatever the
        # other arm did -- but the message said "retrieval differs: a mixed,
        # b mixed", which reads as a contradiction. The reason is the
        # actionable part.
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            code = ft.retrieval_gate({"retrievalMode": "mixed"},
                                     {"retrievalMode": "mixed"},
                                     "a", "b", False)
        self.assertEqual(code, 2)
        self.assertIn("both arms changed retriever mid-run", buf.getvalue())
        # The embedding provider is not the fix here: it answered, and
        # changed its mind mid-run.
        self.assertNotIn("Fix the embedding provider", buf.getvalue())
        self.assertNotIn("retrieval differs", buf.getvalue())

    def test_a_genuine_difference_still_says_so(self):
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            ft.retrieval_gate({"retrievalMode": "mixed"},
                              {"retrievalMode": "semantic"}, "a", "b", False)
        self.assertIn("retrieval differs", buf.getvalue())

    def test_override_reports_instead_of_failing(self):
        self.assertEqual(ft.retrieval_gate({"retrievalMode": "semantic"},
                                           {"retrievalMode": "lexical"},
                                           "a", "b", True), 0)

    def test_both_lexical_is_allowed_and_said_so(self):
        self.assertEqual(ft.retrieval_gate({"retrievalMode": "lexical"},
                                           {"retrievalMode": "lexical"},
                                           "a", "b", False), 0)

    def test_unrecorded_mode_is_reported_not_refused(self):
        # Refusing every run written before the harness recorded this would
        # make the gate unusable rather than safe.
        self.assertEqual(ft.retrieval_gate({}, {}, "a", "b", False), 0)


class Main(unittest.TestCase):
    def setUp(self):
        self.tmp = pathlib.Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def run_main(self, *args: str) -> int:
        argv = sys.argv
        sys.argv = ["flip_table.py", *args]
        try:
            return ft.main()
        finally:
            sys.argv = argv

    def test_an_aa_pair_exits_zero(self):
        s = {"q1": "match", "q2": "no_match"}
        a = write_run(self.tmp, "aa-1", s)
        b = write_run(self.tmp, "aa-2", s)
        self.assertEqual(self.run_main("--a", str(a), "--b", str(b)), 0)

    def test_a_retrieval_mismatch_exits_two(self):
        s = {"q1": "match"}
        a = write_run(self.tmp, "r-1", s)
        b = write_run(self.tmp, "r-2", s, retrievalMode="lexical")
        self.assertEqual(self.run_main("--a", str(a), "--b", str(b)), 2)

    def test_case_set_mismatch_still_exits_one(self):
        a = write_run(self.tmp, "c-1", {"q1": "match"})
        b = write_run(self.tmp, "c-2", {"q2": "match"})
        self.assertEqual(self.run_main("--a", str(a), "--b", str(b)), 1)


class Calibration(unittest.TestCase):
    def test_block_names_every_pin_and_the_flip_count(self):
        cfg = {"datasetVersion": 13, "judgeVersion": 4, "rubricSha": "abc",
               "answererModel": "sonnet", "judgeModel": "sonnet",
               "answererManifest": "analysis-plugin", "datasetSha": "d1",
               "retrievalMode": "semantic"}
        block = ft.calibration_block(cfg, cfg, "aa-1", "aa-2", 45, 46, 47, 2,
                                     ["q9"])
        for pin in ft.COMPARABLE:
            self.assertIn(pin, block)
        self.assertIn("Flips: 2", block)
        self.assertIn("47 cases", block)
        self.assertIn("q9", block)

    def test_a_differing_pin_shows_both_values(self):
        block = ft.calibration_block({"judgeModel": "sonnet"},
                                     {"judgeModel": "opus"},
                                     "a", "b", 1, 1, 1, 0, [])
        self.assertIn("sonnet / opus", block)


class CompletenessNote(unittest.TestCase):
    """The asymmetry between two arms' unscored cases, reported not refused.

    This replaced a refusal that was based on a false claim: that a case one
    arm excluded "reads as a flip". It cannot. A flip needs a real pass or fail
    on BOTH sides, so a null verdict on either lands in `unscored` and counts
    toward no flip. Refusing a 99-versus-100 pair discarded 99 good
    comparisons to avoid a distortion that was not there.
    """

    def verdicts(self, **qids):
        return {q: {"verdict": v, "passed": p, "outcome": "x", "reason": "",
                    "confidence": None}
                for q, (v, p) in qids.items()}

    def test_an_excluded_case_cannot_become_a_flip(self):
        # The claim the refusal rested on, tested directly against the pairing
        # rule the report uses.
        A = self.verdicts(q1=("match", True), q2=(None, None))
        B = self.verdicts(q1=("match", True), q2=("no_match", False))
        shared = sorted(A)
        a_only = [q for q in shared if A[q]["passed"] and B[q]["passed"] is False]
        b_only = [q for q in shared if A[q]["passed"] is False and B[q]["passed"]]
        unscored = [q for q in shared
                    if A[q]["passed"] is None or B[q]["passed"] is None]
        self.assertEqual(a_only + b_only, [])
        self.assertEqual(unscored, ["q2"])

    def test_it_never_refuses(self):
        A = self.verdicts(q1=("match", True), q2=(None, None))
        B = self.verdicts(q1=("match", True), q2=("no_match", False))
        self.assertEqual(
            ft.completeness_note({"status": "incomplete", "truncated": ["q2"]},
                                 {"status": "complete"}, "a", "b", A, B), 0)

    def test_two_complete_arms_say_nothing(self):
        A = B = self.verdicts(q1=("match", True))
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            ft.completeness_note({}, {}, "a", "b", A, B)
        self.assertEqual(out.getvalue(), "")

    def test_it_names_the_cases_and_why(self):
        A = self.verdicts(q1=("match", True), q2=(None, None))
        B = self.verdicts(q1=("match", True), q2=("no_match", False))
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            ft.completeness_note({"truncated": ["q2"]}, {}, "arm-a", "arm-b",
                                 A, B)
        body = out.getvalue()
        self.assertIn("arm-a left 1 case(s) unscored", body)
        self.assertIn("1 truncated", body)
        self.assertIn("q2", body)
        # The caveat that is actually true: the dropped cases are the long ones.
        self.assertIn("BECAUSE it ran long", body)

    def test_the_answerer_model_is_already_a_pin(self):
        # Not this note's job, and worth pinning so nobody adds a second
        # mechanism: an arm on one model against another is already reported
        # as a differing pin. That was the largest uncontrolled variable in the
        # run this came from.
        self.assertIn("answererModel", ft.COMPARABLE)


class Outcome(unittest.TestCase):
    def test_near_match_is_neither(self):
        self.assertEqual(ft.outcome("near_match"), "neither")

    def test_an_unknown_verdict_is_never_a_fail(self):
        self.assertEqual(ft.outcome("invented"), "neither")

class CountsTowardScore(unittest.TestCase):
    """One rule, three readers, because two of them had drifted.

    `run_baseline.py` dropped a `verified_wrong` golden before counting and
    `eval_run.malloy` did not, while the package's own doc comment on
    `gold_status` said it did. On a real 13-case run with one such golden the
    printed rate read 91.67% and the notebook's `pass_rate` read 92.31%, off
    the same rows, with nothing to say which was right.
    """

    def test_a_verified_wrong_golden_leaves_the_aggregates(self):
        self.assertFalse(ft.counts_toward_score("verified_wrong"))

    def test_every_other_status_counts(self):
        for s in ("verified", "verified_benign", "suspect", "provisional",
                  "ambiguous", "invalid"):
            with self.subTest(status=s):
                self.assertTrue(ft.counts_toward_score(s))

    def test_an_absent_status_counts(self):
        # A run predating gold_status must not silently lose every case.
        self.assertTrue(ft.counts_toward_score(None))

    def test_run_baseline_reads_this_rule_rather_than_restating_it(self):
        src = (pathlib.Path(__file__).resolve().parent
               / "run_baseline.py").read_text()
        # The AGGREGATE site reads the helper. The other `verified_wrong`
        # test in that file is a different question -- whether a case is
        # scorable at all, from the golden's own status, before an attempt --
        # and is correctly separate.
        self.assertIn('counts_toward_score(v.get("gold_status"))', src)
        self.assertNotIn('v.get("gold_status") == "verified_wrong"', src)

    def test_the_flattener_writes_it_as_a_column(self):
        src = (pathlib.Path(__file__).resolve().parent
               / "build_run_package.py").read_text()
        self.assertIn('"counts": counts_toward_score(', src)
        self.assertIn('"counts", "reason"', src)

    def test_the_malloy_measures_filter_on_that_column(self):
        malloy = (pathlib.Path(__file__).resolve().parent.parent
                  / "templates" / "eval-run-package" / "eval_run.malloy").read_text()
        for m in ("confident_count", "passed", "failed", "near_matches",
                  "needs_human"):
            line = next(l for l in malloy.splitlines()
                        if l.strip().startswith(f"{m} is count()"))
            self.assertIn("counts = 'true'", line, f"{m} does not exclude it")
        # Written as a string because flatten() serialises a bool that way.
        self.assertIn("public: counts", malloy)

if __name__ == "__main__":
    unittest.main()
