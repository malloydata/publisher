#!/usr/bin/env python3
"""Tests for improve_mode.py: what an edit is re-asked on, and what is left on the branch."""
import argparse
import json
import pathlib
import shutil
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent.parent
                       / "eval-answer" / "scripts"))
import attempts  # noqa: E402
import config  # noqa: E402
import improve_mode as im  # noqa: E402


def sh(cwd, *args):
    return subprocess.run(["git", "-C", str(cwd), *args], capture_output=True, text=True,
                          check=True).stdout.strip()


class FakeRoutine:
    """A routine whose steps are functions, so no server or agent is needed."""

    def __init__(self, cfg, label, on):
        self.cfg, self.label, self.on, self.ran = cfg, label, on, []
        self.s = ["--set", str(cfg.set_dir)]
        self.run_dir = cfg.workdir() / "runs" / label
        self.a = argparse.Namespace(max_turns="40", parallel="4", max_edits=1, max_cost_usd=None)
        self.result = {}

    def do(self, name, *args):
        self.ran.append((name, args))
        return self.on.get(name.split()[0], lambda *a: 0)(*args) or 0


class Mode(unittest.TestCase):
    def setUp(self):
        self.root = pathlib.Path(tempfile.mkdtemp()).resolve()
        self.repo = self.root / "repo"
        (self.repo / "pkg").mkdir(parents=True)
        (self.repo / "evals").mkdir()
        (self.repo / "pkg" / "m.malloy").write_text("source: orders is t\n")
        (self.repo / "evals" / "set.json").write_text('{"name": "s"}')
        (self.repo / "evals" / "cases.jsonl").write_text("".join(json.dumps(c) + "\n" for c in (
            {"qid": "fix", "split": "dev", "tags": ["returns"]},
            {"qid": "near", "split": "dev", "tags": ["returns", "dates"]},
            {"qid": "far", "split": "dev", "tags": ["geo"]},
            {"qid": "held", "split": "holdout", "tags": ["geo"]},
            {"qid": "final", "split": "test", "tags": ["returns"]})))
        (self.repo / "evals" / "eval.toml").write_text(
            f'[paths]\nworkdir = "{self.root / "w"}"\n[model]\npackage = "p"\n'
            f'environment = "e"\nrepo = "../pkg"\n')
        sh(self.repo, "init", "-q", "-b", "main")
        sh(self.repo, "add", ".")
        sh(self.repo, "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-qm", "base")
        self.cfg = config.load(self.repo / "evals")
        self.run = self.root / "w" / "runs" / "night"
        self.run.mkdir(parents=True)
        self.issue([{"id": "night:c1", "qids": ["fix"], "owner": "model"}])

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def issue(self, issues):
        ev = []
        for i in issues:
            ev += [{"kind": "issue", "issue_id": i["id"], "qids": i["qids"], "owner": i["owner"],
                    "component": i.get("component")},
                   {"kind": "issue_status", "issue_id": i["id"], "status": i.get("status", "open")}]
        (self.run / "events.jsonl").write_text("".join(json.dumps(e) + "\n" for e in ev))

    def steps(self, accepted=True, edit=True, improve_exit=0):
        """What each step would have done: the agent's edit, and accept's record."""
        def improve(*args):
            d = self.run / "artifacts" / "clusters" / "night:c1"
            d.mkdir(parents=True, exist_ok=True)
            (d / "result.json").write_text(json.dumps(
                {"edit": "say what net sales excludes"} if edit else {"disagreement": "the key is wrong"}))
            (d / "edit.patch").write_text("+  #(doc) net\n")
            if edit:
                (self.repo / "pkg" / "m.malloy").write_text("#(doc) net\nsource: orders is t\n")
            return improve_exit

        def accept(*args):
            out = self.root / "w" / "acceptances"
            out.mkdir(parents=True, exist_ok=True)
            (out / "1.json").write_text(json.dumps({
                "accepted": accepted, "baseline": "night", "candidates": ["a", "b"],
                "replay": None, "targets": ["fix"],
                "rules": {"no regression": {"ok": accepted, "detail": "none" if accepted else "far in a"}}}))
            attempts.append(self.cfg.set_dir, attempts.row(
                what="say what net sales excludes", outcome="accepted" if accepted else "rejected",
                baseline="night", candidates=["a", "b"], targets=["fix"],
                failed={} if accepted else {"no regression": "far in a"}, effect={}))
            return 0 if accepted else 1
        return {"improve": improve, "accept": accept}

    def go(self, cap=None, spent=None, **kw):
        R = FakeRoutine(self.cfg, "night", self.steps(**kw))
        R.a.max_cost_usd = cap
        if spent is not None:
            R.result = {"number": {"costUsd": {"answerer": spent, "judge": None}}}
        return im.improve_quality(R), R

    def test_each_edit_says_what_it_cost_the_agent_and_its_two_runs(self):
        on = self.steps()
        improve = on["improve"]

        def priced(*args):
            code = improve(*args)
            d = self.run / "artifacts" / "clusters" / "night:c1"
            (d / "result.json").write_text(json.dumps(
                {"edit": "say what net sales excludes", "cost_usd": 0.5}))
            return code

        def run(*args):
            label = args[args.index("--label") + 1]
            d = self.root / "w" / "runs" / label
            d.mkdir(parents=True, exist_ok=True)
            (d / "run.json").write_text(json.dumps({"answererCostUsd": 0.75, "judgeCostUsd": 0.25}))
            return 0
        R = FakeRoutine(self.cfg, "night", {**on, "improve": priced, "run": run})
        r = im.improve_quality(R)
        self.assertEqual((r["edits"][0]["costUsd"], r["costUsd"]), (2.5, 2.5))

    def test_a_night_already_at_the_cap_tries_no_edit_and_says_why(self):
        r, R = self.go(cap=2, spent=2.5)
        self.assertEqual((r["edits"], r["branch"]), ([], None))
        self.assertIn("stopped before edit 1 of 1: $2.50 spent, at or over --max-cost-usd 2",
                      r["capped"])
        self.assertNotIn("improve", [n.split()[0] for n, _ in R.ran])
        self.assertEqual(sh(self.repo, "branch", "--list", "eval/night"), "")

    # ------------------------------------------------------------- the cases

    def test_an_edit_is_re_asked_on_its_targets_holdout_and_tag_neighbours_never_test(self):
        self.assertEqual(im.candidate_cases(self.cfg.set_dir, ["fix"]), ["fix", "near", "held"])

    def test_only_open_clusters_the_models_repository_owns_largest_first(self):
        self.issue([{"id": "a", "qids": ["x"], "owner": "model"},
                    {"id": "b", "qids": ["x", "y"], "owner": "package-skill"},
                    {"id": "c", "qids": ["x", "y", "z"], "owner": "retrieval"},
                    {"id": "d", "qids": ["x", "y", "z"], "owner": "model", "component": "performance"},
                    {"id": "e", "qids": ["x", "y", "z"], "owner": "model", "status": "fixed"}])
        self.assertEqual([i["issue_id"] for i in im.open_issues(self.run)], ["b", "a"])

    # ------------------------------------------------------------ the branch

    def test_an_accepted_edit_is_one_commit_on_the_branch_with_its_attempt(self):
        r, R = self.go(accepted=True)
        self.assertEqual((r["branch"], r["accepted"], r["edits"][0]["outcome"]),
                         ("eval/night", 1, "accepted"))
        self.assertEqual(sh(self.repo, "rev-parse", "--abbrev-ref", "HEAD"), "main")
        self.assertEqual(sh(self.repo, "log", "--format=%s", "main..eval/night"),
                         "checkpoint: say what net sales excludes")
        changed = sh(self.repo, "diff", "--name-only", "main", "eval/night").splitlines()
        self.assertEqual(sorted(changed), ["evals/attempts.jsonl", "pkg/m.malloy"])
        self.assertEqual(sh(self.repo, "status", "--porcelain"), "")
        run = next(a for n, a in R.ran if n.startswith("run "))
        self.assertEqual(run[run.index("--only") + 1], "fix,near,held")
        self.assertEqual(run[run.index("--experiment") + 1], "night-c1")
        acc = next(a for n, a in R.ran if n.startswith("accept"))
        self.assertEqual((acc[acc.index("--baseline") + 1], acc[acc.index("--targets") + 1]),
                         ("night", "fix"))
        self.assertEqual([n.split()[0] for n, _ in R.ran],
                         ["improve", "experiment", "run", "run", "stop", "accept"])

    def test_a_rejected_edit_is_taken_off_and_its_attempt_is_kept(self):
        r, _ = self.go(accepted=False)
        self.assertEqual((r["accepted"], r["edits"][0]["outcome"]), (0, "rejected"))
        self.assertIn("no regression: far in a", r["edits"][0]["why"])
        self.assertEqual(sh(self.repo, "diff", "--name-only", "main", "eval/night"),
                         "evals/attempts.jsonl")
        self.assertEqual(sh(self.repo, "show", "eval/night:pkg/m.malloy"), "source: orders is t")
        self.assertEqual(sh(self.repo, "status", "--porcelain"), "")

    def test_no_edit_leaves_no_branch(self):
        r, R = self.go(edit=False)
        self.assertEqual((r["branch"], r["edits"][0]["outcome"]), (None, "skipped"))
        self.assertIn("the key is wrong", r["edits"][0]["why"])
        self.assertEqual(sh(self.repo, "branch", "--list", "eval/night"), "")
        self.assertEqual([n.split()[0] for n, _ in R.ran], ["improve"])

    def test_an_edit_the_improve_step_stopped_is_dropped_and_not_measured(self):
        r, R = self.go(improve_exit=2)
        self.assertEqual(r["edits"][0]["outcome"], "skipped")
        self.assertIn("check of the answer key failed", r["edits"][0]["why"])
        self.assertEqual(sh(self.repo, "status", "--porcelain"), "")
        self.assertEqual(sh(self.repo, "show", "HEAD:pkg/m.malloy"), "source: orders is t")
        self.assertNotIn("run", [n.split()[0] for n, _ in R.ran])

    def test_why_an_edit_was_not_measured_names_the_most_specific_reason(self):
        f = im.stopped_because
        self.assertIsNone(f(0, {}, True))
        self.assertIn("doubts the answer key on q1, so it made no edit",
                      f(2, {"goldenSuspect": [{"qid": "q1"}], "disagreement": "both bases"}, False))
        self.assertIn("states a case's answer (q2)", f(2, {"goldenLeak": [{"qid": "q2"}]}, True))
        self.assertIn("same as one rejected on 2026-11-01",
                      f(2, {"duplicateOf": {"at": "2026-11-01"}}, True))
        self.assertIn("made no edit: not a model fault", f(0, {"disagreement": "not a model fault"}, False))
        self.assertIn("failed (exit 1)", f(1, {}, False))

    def test_uncommitted_work_anywhere_in_the_repository_means_no_edit_is_tried(self):
        (self.repo / "evals" / "notes.md").write_text("mine\n")
        r, R = self.go()
        self.assertEqual((r["edits"], R.ran), ([], []))
        self.assertIn("uncommitted changes", r["why"])
        self.assertEqual((self.repo / "evals" / "notes.md").read_text(), "mine\n")

    def test_nothing_to_fix_says_so(self):
        self.issue([{"id": "c", "qids": ["x"], "owner": "retrieval"}])
        r, _ = self.go()
        self.assertIn("nothing the model's repository owns", r["why"])


class Base(unittest.TestCase):
    """The temporary repository and set both modes are tested in."""
    setUp, tearDown, issue = Mode.setUp, Mode.tearDown, Mode.issue


class Speed(Base):
    def setUp(self):
        super().setUp()
        ev = [{"kind": "issue", "issue_id": "perf-hot-orders", "qids": ["a"], "owner": "model",
               "component": "performance", "primary_code": "HOT-SOURCE",
               "evidence": {"source": "orders"}},
              {"kind": "issue", "issue_id": "perf-items", "qids": ["b"], "owner": "model",
               "component": "performance", "primary_code": "SLOW-QUERY",
               "evidence": {"source": "items"}},
              {"kind": "issue", "issue_id": "perf-events", "qids": ["c"], "owner": "model",
               "component": "performance", "primary_code": "PERSIST-NOT-BUILT",
               "evidence": {"source": "events"}}]
        (self.run / "events.jsonl").write_text("".join(json.dumps(e) + "\n" for e in ev))
        (self.run / "summary.json").write_text(json.dumps({"by_source": {
            "orders": {"weighted_ms": 400.0, "total_ms": 10.0},
            "items": {"weighted_ms": 90.0, "total_ms": 90.0},
            "events": {"weighted_ms": 900.0}}}))

    def recorded(self):
        """The attempts committed on the branch the routine left."""
        text = sh(self.repo, "show", "eval/night:evals/attempts.jsonl")
        return [json.loads(x) for x in text.splitlines()]

    def compare(self, **kw):
        return {"compare": {"rows_changed": [], "broke": [], "slower": [],
                            "faster": [{"qid": "a"}], "total_ratio": 0.4,
                            "total_before_ms": 400.0, "total_after_ms": 160.0, **kw},
                "build": {"tables": 1, "build_ms": 30}}

    def test_the_costliest_source_an_edit_can_help_is_first(self):
        # `events` costs most and needs a build, not an edit.
        self.assertEqual([i["issue_id"] for i in im.speed_issues(self.run)],
                         ["perf-hot-orders", "perf-items"])

    def test_a_speed_up_is_two_replays_that_agree(self):
        ok = self.compare()
        self.assertEqual(im.speed_verdict([ok, ok], 0.2), (True, {}))
        self.assertIn("twice", im.speed_verdict([ok], 0.2)[1])
        for bad, rule in ((self.compare(rows_changed=["a"]), "rows held (replay 2)"),
                          (self.compare(slower=[{"qid": "b"}]), "nothing slower (replay 2)"),
                          (self.compare(total_ratio=0.9), "faster (replay 2)"),
                          (self.compare(faster=[]), "faster (replay 2)"),
                          ({"build": None}, "compared (replay 2)")):
            accepted, failed = im.speed_verdict([ok, bad], 0.2)
            self.assertFalse(accepted)
            self.assertIn(rule, failed)

    def go(self, summary):
        def improve(*args):
            d = self.run / "artifacts" / "clusters" / "perf-hot-orders"
            d.mkdir(parents=True, exist_ok=True)
            (d / "result.json").write_text(json.dumps({"edit": "persist orders_by_month"}))
            (d / "edit.patch").write_text("+#@ persist name=\"orders_by_month\"\n")
            (self.repo / "pkg" / "m.malloy").write_text("#@ persist\nsource: orders is t\n")
            return 0

        def replay(*args):
            d = self.root / "w" / "runs" / args[args.index("--label") + 1]
            d.mkdir(parents=True)
            (d / "summary.json").write_text(json.dumps(summary))
            (d / "run.json").write_text(json.dumps({"latencyBand": 0.2}))
            return 1 if summary["compare"]["rows_changed"] else 0
        R = FakeRoutine(self.cfg, "night", {"improve": improve, "query-replay": replay})
        return im.improve_speed(R), R

    def test_an_accepted_speed_edit_is_built_replayed_twice_and_kept_with_its_cost(self):
        r, R = self.go(self.compare())
        self.assertEqual((r["accepted"], r["branch"]), (1, "eval/night"))
        self.assertEqual([n.split()[0] for n, _ in R.ran],
                         ["improve", "experiment", "query-replay", "query-replay", "stop"])
        self.assertIn("--unattended", R.ran[0][1])
        replay = R.ran[2][1]
        self.assertEqual((replay[replay.index("--from") + 1], replay[replay.index("--baseline") + 1],
                          replay[replay.index("--experiment") + 1]), ("night", "night", "night-c1"))
        body = sh(self.repo, "log", "-1", "--format=%B", "eval/night")
        self.assertIn("total 400.0 -> 160.0 ms", body)
        self.assertIn("It builds 1 table(s) in 30 ms on each refresh", body)
        row = self.recorded()[-1]
        self.assertEqual((row["outcome"], row["issueIds"], row["effect"]["totalAfterMs"]),
                         ("accepted", ["perf-hot-orders"], 160.0))

    def test_a_speed_edit_that_changes_rows_is_taken_off_and_recorded(self):
        r, _ = self.go(self.compare(rows_changed=["a"]))
        self.assertEqual(r["edits"][0]["outcome"], "rejected")
        self.assertIn("rows held", r["edits"][0]["why"])
        self.assertEqual(sh(self.repo, "show", "eval/night:pkg/m.malloy"), "source: orders is t")
        self.assertEqual(self.recorded()[-1]["outcome"], "rejected")
        self.assertEqual(sh(self.repo, "status", "--porcelain"), "")
        # the line is on the branch, for the pull request, and not left behind here
        self.assertEqual(attempts.read(self.cfg.set_dir), [])


if __name__ == "__main__":
    unittest.main()
