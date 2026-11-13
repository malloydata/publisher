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


class OneRunIsNotTwo(unittest.TestCase):
    """The refusals `main` makes before any rule is checked, and their exit code."""

    def setUp(self):
        Rules.setUp(self)

    def tearDown(self):
        Rules.tearDown(self)

    def main(self, *argv):
        return accept.main(["--set", str(self.set), "--baseline", "base", *argv])

    def test_the_same_run_named_twice_cannot_be_checked(self):
        run(self.runs / "a", {"fix": "match", "keep": "match", "held": "match"})
        self.assertEqual(self.main("--run", "a", "--run", "a"), 3)

    def test_the_baseline_is_not_a_candidate(self):
        run(self.runs / "a", {"fix": "match", "keep": "match", "held": "match"})
        self.assertEqual(self.main("--run", "a", "--run", "base"), 3)

    def test_a_missing_run_exits_3_not_1(self):
        self.assertEqual(self.main("--run", "nope", "--run", "nope2"), 3)

    def test_two_runs_that_hold_exit_0(self):
        good = {"fix": "match", "keep": "match", "held": "match"}
        run(self.runs / "a", good), run(self.runs / "b", good)
        self.assertEqual(self.main("--run", "a", "--run", "b"), 0)


class RunSettings(unittest.TestCase):
    def setUp(self):
        Rules.setUp(self)
        shutil.rmtree(self.base)
        self.good = {"fix": "match", "keep": "match", "held": "match"}
        self.base = run(self.runs / "base", {"fix": "no_match", "keep": "match", "held": "match"},
                        maxTurns=30, judgePromptSha="p1")

    def tearDown(self):
        Rules.tearDown(self)

    def other(self, **pins):
        r = accept.check(self.cfg, self.base, [run(self.runs / "a", self.good, **pins),
                                               run(self.runs / "b", self.good, **pins)], None, None)
        return r["rules"]["one change"]

    def test_a_different_turn_cap_is_a_second_change(self):
        rule = self.other(maxTurns=40, judgePromptSha="p1")
        self.assertFalse(rule["ok"])
        self.assertIn("maxTurns", rule["detail"])

    def test_a_different_judge_prompt_is_a_second_change(self):
        self.assertIn("judgePromptSha", self.other(maxTurns=30, judgePromptSha="p2")["detail"])

    def test_a_setting_one_side_never_recorded_is_not_counted(self):
        self.assertTrue(self.other(judgePromptSha="p1")["ok"])

    def test_a_package_skill_edit_is_the_model_not_a_second_change(self):
        self.assertTrue(self.other(maxTurns=30, judgePromptSha="p1", packageSkillsSha="new")["ok"])


if __name__ == "__main__":
    unittest.main()
