#!/usr/bin/env python3
"""Tests for flip_table's gates: the retriever pair, the stable near_match
list, and the calibration block a band has to be quoted against."""
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


class Outcome(unittest.TestCase):
    def test_near_match_is_neither(self):
        self.assertEqual(ft.outcome("near_match"), "neither")

    def test_an_unknown_verdict_is_never_a_fail(self):
        self.assertEqual(ft.outcome("invented"), "neither")


if __name__ == "__main__":
    unittest.main()
