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


class Noise(unittest.TestCase):
    """A second run of the baseline says which cases flip with nothing changed."""

    def setUp(self):
        Rules.setUp(self)
        shutil.rmtree(self.base)
        pins = dict(maxTurns=30, started="2026-11-01")
        # `flaky` fails here and passes in the second run; `wobble` the reverse.
        self.base = run(self.runs / "base", {"fix": "no_match", "keep": "match", "held": "match",
                                             "flaky": "no_match", "wobble": "match"},
                        modelGitSha="m0", **pins)
        self.aa = run(self.runs / "base2", {"fix": "no_match", "keep": "match", "held": "match",
                                            "flaky": "match", "wobble": "no_match"},
                      modelGitSha="m0", **{**pins, "started": "2026-11-02"})
        with (self.set / "cases.jsonl").open("a") as f:
            f.write('{"qid": "flaky", "split": "dev"}\n{"qid": "wobble", "split": "dev"}\n')

    def tearDown(self):
        Rules.tearDown(self)

    def cand(self, name, **verdicts):
        v = {"fix": "match", "keep": "match", "held": "match", "flaky": "match",
             "wobble": "match", **verdicts}
        return run(self.runs / name, v, maxTurns=30)

    def check(self, *cands, aa="yes", targets=None):
        return accept.check(self.cfg, self.base, list(cands), targets, None,
                            self.aa if aa == "yes" else None)

    def test_the_cases_the_two_runs_disagree_on_are_unstable(self):
        r = self.check(self.cand("a"), self.cand("b"))
        self.assertEqual(r["unstable"], ["flaky", "wobble"])

    def test_an_unstable_failure_is_not_a_default_target(self):
        r = self.check(self.cand("a"), self.cand("b"))
        self.assertEqual((r["targets"], r["unstableTargets"]), (["fix"], ["flaky"]))
        self.assertTrue(r["accepted"], r["rules"])

    def test_a_regression_on_an_unstable_case_does_not_fail_the_edit(self):
        r = self.check(self.cand("a", wobble="no_match"), self.cand("b"))
        self.assertTrue(r["rules"]["no regression"]["ok"])
        self.assertEqual(r["noise"], ["wobble in a"])

    def test_a_regression_on_a_stable_case_still_does(self):
        r = self.check(self.cand("a", keep="no_match"), self.cand("b"))
        self.assertFalse(r["rules"]["no regression"]["ok"])

    def test_with_no_second_run_every_case_counts(self):
        r = self.check(self.cand("a", wobble="no_match"), self.cand("b"), aa=None)
        self.assertFalse(r["rules"]["no regression"]["ok"])
        self.assertEqual((r["aa"], r["targets"]), (None, ["fix", "flaky"]))

    def test_an_edit_whose_only_target_is_unstable_shows_nothing(self):
        r = self.check(self.cand("a"), self.cand("b"), targets=["flaky"])
        self.assertFalse(r["rules"]["targets fixed"]["ok"])
        self.assertIn("unstable", r["rules"]["targets fixed"]["detail"])

    def test_the_second_run_is_found_by_its_pins_and_an_edited_model_is_not_one(self):
        self.assertEqual(accept.find_aa(self.cfg, self.base).name, "base2")
        self.cand("a")                                  # another model: modelGitSha "a"
        self.assertEqual(accept.find_aa(self.cfg, self.base).name, "base2")
        shutil.rmtree(self.aa)
        self.assertIsNone(accept.find_aa(self.cfg, self.base))

    def test_the_same_served_content_at_another_commit_is_still_a_second_run(self):
        pv = {"id": "1.0.0", "contentHash": "h1"}
        a = {"modelGitSha": "c1", "packageVersion": pv, "maxTurns": 30}
        self.assertTrue(accept.same_measurement(a, {**a, "modelGitSha": "c2"}))
        self.assertFalse(accept.same_measurement(
            a, {**a, "modelGitSha": "c2", "packageVersion": {"id": "1.0.0", "contentHash": "h2"}}))
        # with no content hash the commit is all there is to go on
        bare = {"modelGitSha": "c1", "maxTurns": 30}
        self.assertFalse(accept.same_measurement(bare, {**bare, "modelGitSha": "c2"}))
        self.assertFalse(accept.same_measurement(a, {**a, "maxTurns": 40}))

    def test_a_run_with_other_pins_named_as_the_second_run_cannot_be_checked(self):
        self.cand("a"), self.cand("b")
        code = accept.main(["--set", str(self.set), "--baseline", "base", "--run", "a",
                            "--run", "b", "--aa", "a", "--no-record"])
        self.assertEqual(code, 3)


class TestSplitAndRecord(unittest.TestCase):
    def setUp(self):
        Rules.setUp(self)

    def tearDown(self):
        Rules.tearDown(self)

    def test_a_test_case_decides_nothing(self):
        with (self.set / "cases.jsonl").open("a") as f:
            f.write('{"qid": "final", "split": "test"}\n')
        shutil.rmtree(self.base)
        self.base = run(self.runs / "base", {"fix": "no_match", "keep": "match", "held": "match",
                                             "final": "match"})
        # `final` regresses in both candidates, and it must not block: a rule
        # that read it would be tuning the edit against the reported number.
        bad = {"fix": "match", "keep": "match", "held": "match", "final": "no_match"}
        r = accept.check(self.cfg, self.base, [run(self.runs / "a", bad),
                                               run(self.runs / "b", bad)], None, None)
        self.assertTrue(r["accepted"], r["rules"])
        self.assertEqual(r["testCasesLeftOut"], 1)

    def go(self, verdicts, *extra):
        run(self.runs / "a", verdicts), run(self.runs / "b", verdicts)
        code = accept.main(["--set", str(self.set), "--baseline", "base", "--run", "a",
                            "--run", "b", "--aa", "none", "--what", "doc net_sales", *extra])
        rows = [json.loads(x) for x in (self.set / "attempts.jsonl").read_text().splitlines()] \
            if (self.set / "attempts.jsonl").exists() else []
        return code, rows

    def test_an_accepted_edit_is_recorded_with_its_patch_and_lesson(self):
        patch = self.root / "e.patch"
        patch.write_text("--- a/m.malloy\n+++ b/m.malloy\n@@ -1 +1 @@\n-  x\n+  #(doc) net\n")
        code, rows = self.go({"fix": "match", "keep": "match", "held": "match"},
                             "--patch", str(patch), "--issue", "r:c1", "--lesson", "net excludes returns")
        self.assertEqual(code, 0)
        r = rows[0]
        self.assertEqual((r["outcome"], r["what"], r["targets"], r["issueIds"], r["lesson"]),
                         ("accepted", "doc net_sales", ["fix"], ["r:c1"], "net excludes returns"))
        self.assertEqual(r["effect"]["targetsFixed"], 1)
        self.assertTrue(r["patchSha"] and "+  #(doc) net" in r["patch"])

    def test_a_rejected_edit_is_recorded_with_the_rule_it_failed(self):
        code, rows = self.go({"fix": "match", "keep": "no_match", "held": "match"})
        self.assertEqual((code, rows[0]["outcome"]), (1, "rejected"))
        self.assertIn("no regression", rows[0]["failedRules"])
        self.assertEqual(rows[0]["effect"]["regressions"], ["keep in a", "keep in b"])

    def test_no_record_writes_nothing(self):
        _, rows = self.go({"fix": "match", "keep": "match", "held": "match"}, "--no-record")
        self.assertEqual(rows, [])


if __name__ == "__main__":
    unittest.main()
