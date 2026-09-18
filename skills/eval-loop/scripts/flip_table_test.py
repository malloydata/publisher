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


class CompletenessGate(unittest.TestCase):
    """A flip table over an arm that did not finish is not a flip table.

    `run_baseline.py` withholds an incomplete run's own pass rate. The same
    claim made twice has to withhold too: the cases that arm excluded are
    missing on one side and present on the other, so each one reads here as a
    flip that some change caused.
    """

    COMPLETE = {"status": "complete"}

    def gate(self, ca, cb, allow=False):
        return ft.completeness_gate(ca, cb, "a", "b", allow)

    def test_two_complete_arms_pass(self):
        self.assertEqual(self.gate(self.COMPLETE, self.COMPLETE), 0)

    def test_an_incomplete_arm_refuses(self):
        self.assertEqual(
            self.gate({"status": "incomplete", "truncated": ["q1"]},
                      self.COMPLETE), 2)

    def test_an_aborted_arm_refuses(self):
        self.assertEqual(self.gate(self.COMPLETE, {"status": "aborted"}), 2)

    def test_the_flag_reports_anyway(self):
        self.assertEqual(
            self.gate({"status": "incomplete", "contaminated": ["q1"]},
                      self.COMPLETE, allow=True), 0)

    def test_a_run_predating_the_field_is_not_refused(self):
        # `status` is absent on older runs. Refusing every historical pair
        # would make the gate unusable rather than safe, which is the rule
        # `retrieval_gate` already follows for an unrecorded mode.
        self.assertEqual(self.gate({}, {}), 0)

    def test_the_answerer_model_is_already_a_pin(self):
        # Not this gate's job, and worth pinning so nobody adds a second
        # mechanism for it: an arm on Sonnet against one on Opus is refused by
        # COMPARABLE. That was the largest uncontrolled variable in the run
        # this gate comes from.
        self.assertIn("answererModel", ft.COMPARABLE)


class Outcome(unittest.TestCase):
    def test_near_match_is_neither(self):
        self.assertEqual(ft.outcome("near_match"), "neither")

    def test_an_unknown_verdict_is_never_a_fail(self):
        self.assertEqual(ft.outcome("invented"), "neither")


if __name__ == "__main__":
    unittest.main()
