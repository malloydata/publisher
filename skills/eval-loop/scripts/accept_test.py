#!/usr/bin/env python3
"""Tests for accept.py: each rule it checks, failing for the reason it names."""
import json
import pathlib
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent.parent
                       / "eval-answer" / "scripts"))
import accept  # noqa: E402
import config  # noqa: E402


def run(d: pathlib.Path, verdicts: dict, **pins) -> pathlib.Path:
    d.mkdir(parents=True)
    (d / "run.json").write_text(json.dumps({"status": "complete", "judgeModel": "sonnet",
                                            "modelGitSha": d.name, **pins}))
    ev = []
    for q, v in verdicts.items():
        ev += [{"kind": "attempt", "qid": q, "question_sha": "s"},
               {"kind": "score", "qid": q, "verdict": v}]
    (d / "events.jsonl").write_text("".join(json.dumps(e) + "\n" for e in ev))
    return d


class Rules(unittest.TestCase):
    def setUp(self):
        self.root = pathlib.Path(tempfile.mkdtemp())
        self.set = self.root / "set"
        self.set.mkdir()
        (self.set / "set.json").write_text('{"name": "s"}')
        (self.set / "cases.jsonl").write_text("".join(json.dumps(c) + "\n" for c in (
            {"qid": "fix", "split": "dev"}, {"qid": "keep", "split": "dev"},
            {"qid": "held", "split": "holdout"})))
        (self.set / "eval.toml").write_text(f'[paths]\nworkdir = "{self.root / "w"}"\n')
        self.cfg = config.load(self.set)
        self.runs = self.root / "w" / "runs"
        self.base = run(self.runs / "base", {"fix": "no_match", "keep": "match", "held": "match"})

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def check(self, *cands, **kw):
        return accept.check(self.cfg, self.base, list(cands), kw.get("targets"), None)

    def test_a_fix_that_holds_twice_on_both_splits_is_accepted(self):
        good = {"fix": "match", "keep": "match", "held": "match"}
        r = self.check(run(self.runs / "a", good), run(self.runs / "b", good))
        self.assertTrue(r["accepted"], r["rules"])
        self.assertEqual(r["targets"], ["fix"])

    def test_one_run_is_not_twice(self):
        r = self.check(run(self.runs / "a", {"fix": "match", "keep": "match", "held": "match"}))
        self.assertFalse(r["rules"]["twice"]["ok"])

    def test_a_regression_is_named(self):
        bad = {"fix": "match", "keep": "no_match", "held": "match"}
        r = self.check(run(self.runs / "a", bad), run(self.runs / "b", bad))
        self.assertFalse(r["accepted"])
        self.assertIn("keep in a", r["rules"]["no regression"]["detail"])

    def test_no_holdout_case_answered_fails_both_splits(self):
        dev = {"fix": "match", "keep": "match"}
        r = self.check(run(self.runs / "a", dev), run(self.runs / "b", dev))
        self.assertFalse(r["rules"]["both splits"]["ok"])

    def test_a_moved_judge_is_not_one_change(self):
        good = {"fix": "match", "keep": "match", "held": "match"}
        r = self.check(run(self.runs / "a", good, judgeModel="opus"),
                       run(self.runs / "b", good, judgeModel="opus"))
        self.assertIn("judgeModel", r["rules"]["one change"]["detail"])
        self.assertFalse(r["accepted"])

    def test_a_set_edit_alone_does_not_fail_one_change(self):
        good = {"fix": "match", "keep": "match", "held": "match"}
        r = self.check(run(self.runs / "a", good, datasetSha="new"),
                       run(self.runs / "b", good, datasetSha="new"))
        self.assertTrue(r["rules"]["one change"]["ok"])

    def test_a_doubted_golden_halts_it(self):
        good = {"fix": "match", "keep": "match", "held": "match"}
        r = self.check(run(self.runs / "a", good, doubtedGoldens=[{"qid": "held"}]),
                       run(self.runs / "b", good))
        self.assertEqual(r["doubtedGoldens"], ["held"])
        self.assertFalse(r["accepted"])


if __name__ == "__main__":
    unittest.main()
