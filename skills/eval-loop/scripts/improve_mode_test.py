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
        self.a = argparse.Namespace(max_turns="40", parallel="4", max_edits=1)

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

    def go(self, **kw):
        R = FakeRoutine(self.cfg, "night", self.steps(**kw))
        return im.improve_quality(R), R

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
        self.assertIn("state an answer", r["edits"][0]["why"])
        self.assertEqual(sh(self.repo, "status", "--porcelain"), "")
        self.assertEqual(sh(self.repo, "show", "HEAD:pkg/m.malloy"), "source: orders is t")
        self.assertNotIn("run", [n.split()[0] for n, _ in R.ran])

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


if __name__ == "__main__":
    unittest.main()
