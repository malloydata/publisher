#!/usr/bin/env python3
"""The improve half of a routine: turn a diagnosis into a branch. Stdlib only.

  python3 eval.py routine --kind quality --mode improve --set <set-dir>

Run after the routine has its number. For each diagnosed cluster the model's
repository owns (largest first, `--max-edits` of them, default 1):

  1. improve     one agent makes one smallest edit, probing it as an experiment
                 beside the main package (improve.py --experiment)
  2. commit      the edit, on a new branch eval/<label> in the model's repository
  3. experiment  that commit, served beside the main package
  4. run, twice  the cases the edit aimed at, every holdout case, and every
                 case that shares a tag with a target
  5. accept      the acceptance rules, against tonight's run (accept.py)
  6. keep or drop   an accepted edit stays on the branch, with the acceptance in
                 its commit message. A rejected one is taken off again.
                 Either way the attempt is recorded in <set>/attempts.jsonl and
                 that line is committed, so the next night reads it.

It ends on the branch it found the repository on. What it leaves is the branch
eval/<label>: nothing is published, pushed or merged. Opening a pull request
from that branch is for whoever, or whatever, runs the routine.

It needs the model's repository to be a git checkout with nothing uncommitted
anywhere in it: it commits there, and takes a rejected edit off with a reset.
With uncommitted work it tries no edit and says so.
"""
from __future__ import annotations

import json
import pathlib
import subprocess
import sys
from typing import Any

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent.parent / "eval-answer" / "scripts"))
sys.path.insert(0, str(HERE))

from ledger import read_jsonl  # noqa: E402

IDENTITY = ("-c", "user.name=eval-loop", "-c", "user.email=eval-loop@localhost")


def git(cwd: pathlib.Path, *args: str, check: bool = False) -> subprocess.CompletedProcess:
    return subprocess.run(["git", "-C", str(cwd), *args], capture_output=True, text=True,
                          check=check)


def commit(repo: pathlib.Path, message: str, *args: str) -> bool:
    """Commit as whoever git is configured for, or as `eval-loop` when nobody is:
    an agent's fresh checkout often has no identity, and a commit that fails
    for that reason loses the edit."""
    who = () if git(repo, "config", "user.email").stdout.strip() else IDENTITY
    return git(repo, *who, "commit", "-q", *args, "-m", message).returncode == 0


def open_issues(run: pathlib.Path) -> list[dict[str, Any]]:
    """The clusters an edit to the model's repository can close, largest first.
    Speed issues are the other routine's."""
    events = read_jsonl(run / "events.jsonl") if (run / "events.jsonl").is_file() else []
    status = {e["issue_id"]: e.get("status") for e in events if e.get("kind") == "issue_status"}
    got = [e for e in events if e.get("kind") == "issue"
           and e.get("owner") in ("model", "package-skill")
           and e.get("component") != "performance" and status.get(e["issue_id"]) == "open"]
    return sorted(got, key=lambda i: (-len(i.get("qids") or []), i["issue_id"]))


def candidate_cases(set_dir: pathlib.Path, targets: list[str]) -> list[str]:
    """The cases an edit is re-asked on: its targets, every holdout case, and
    every case that shares a tag with a target.

    Not the whole set, which would double the night's cost per edit. The
    neighbours by tag are where a doc or a rename is most likely to break
    something. Never a `test` case: no decision is made on those.
    """
    cases = read_jsonl(set_dir / "cases.jsonl")
    want = set(targets)
    tags = {t for c in cases if c.get("qid") in want for t in c.get("tags") or []}
    return [c["qid"] for c in cases if c.get("split") != "test" and (
        c["qid"] in want or c.get("split") == "holdout" or tags & set(c.get("tags") or []))]


def newest_acceptance(workdir: pathlib.Path) -> dict[str, Any]:
    files = sorted((workdir / "acceptances").glob("*.json"))
    return json.loads(files[-1].read_text()) if files else {}


def stopped_because(code: int, result: dict[str, Any], edited: bool) -> str | None:
    """Why an improve step's result is not put to the acceptance check, or None
    when it is. The most specific reason first: a reader acts differently on a
    doubted answer key than on an edit that repeats a rejected one."""
    doubted = [g.get("qid") for g in result.get("goldenSuspect") or [] if g.get("qid")]
    said = (result.get("disagreement") or "")[:300]
    if doubted:
        return (f"the agent doubts the answer key on {', '.join(doubted)}, so "
                + ("its edit was not measured" if edited else "it made no edit")
                + ". Settle each through the golden side door first"
                + (f". It said: {said}" if said else ""))
    if result.get("goldenLeak"):
        return ("the edit states a case's answer ("
                + ", ".join(sorted({x["qid"] for x in result["goldenLeak"]})) + "), so it was not measured")
    if result.get("duplicateOf"):
        return (f"the edit is the same as one rejected on {result['duplicateOf'].get('at')}, "
                f"so it was not measured again")
    if code == 2:
        return ("the improve step stopped it: its check of the answer key failed or could "
                "not run. Read the improve step's output")
    if code != 0:
        return f"the improve step failed (exit {code})"
    if not edited:
        return f"the agent made no edit: {said or result.get('agentError') or 'it gave no reason'}"
    return None


def one_edit(R: Any, repo: pathlib.Path, issue: dict[str, Any], k: int) -> dict[str, Any]:
    """Improve, commit, measure, accept, keep or drop. Returns what happened."""
    import accept
    import attempts
    cfg, s, label = R.cfg, R.s, R.label
    iid, targets = issue["issue_id"], list(issue.get("qids") or [])
    out: dict[str, Any] = {"issue": iid, "targets": targets, "outcome": None}
    name = f"{label}-c{k}"
    cluster = R.run_dir / "artifacts" / "clusters" / iid.replace("/", "_")

    code = R.do(f"improve {iid}", "improve", *s, "--label", label, "--only", iid,
                "--experiment", f"{label}-i{k}", "--no-isolate", "--force")
    result = json.loads((cluster / "result.json").read_text()) if (
        cluster / "result.json").exists() else {}
    out["what"] = what = (result.get("edit") or f"close {iid}")[:200]
    dirty = git(repo, "status", "--porcelain", "--", ".").stdout.strip()
    stop = stopped_because(code, result, bool(dirty))
    if stop:
        git(repo, "checkout", "--", ".")
        git(repo, "clean", "-fdq", "--", ".")
        return {**out, "outcome": "skipped", "why": stop}

    git(repo, "add", "-A", "--", ".")
    if not commit(repo, f"candidate: {what}"):
        git(repo, "reset", "-q", "--hard")
        return {**out, "outcome": "skipped", "why": "the edit could not be committed"}

    only = ",".join(candidate_cases(cfg.set_dir, targets))
    runs = [f"{name}-1", f"{name}-2"]
    failed = None
    if R.do(f"experiment {name}", "experiment", "start", name, *s) != 0:
        failed = "the edit could not be served as an experiment"
    else:
        for r in runs:
            if failed is None and R.do(f"run {r}", "run", *s, "--experiment", name, "--only", only,
                                       "--label", r, "--no-share", "--recover",
                                       "--max-turns", R.a.max_turns,
                                       "--parallel", R.a.parallel) != 0:
                failed = f"the run {r} did not finish"
        R.do(f"stop {name}", "experiment", "stop", name, *s)
    before = len(attempts.read(cfg.set_dir))
    if failed is None:
        code = R.do(f"accept {iid}", "accept", *s, "--baseline", label,
                    "--run", runs[0], "--run", runs[1], "--targets", ",".join(targets),
                    "--issue", iid, "--patch", str(cluster / "edit.patch"), "--what", what)
        if code not in (0, 1):
            failed = f"the acceptance check could not run (exit {code})"
    out["runs"] = runs
    recorded = attempts.read(cfg.set_dir)[before:]
    # Keep the line accept wrote, whatever happens to the edit: a rejected
    # attempt nobody can read is one somebody pays to reject again.
    line = attempts.path(cfg.set_dir)
    kept = line.read_text() if line.exists() else None
    r = newest_acceptance(cfg.workdir()) if failed is None else {}
    if failed is None and r.get("accepted"):
        git(repo, "add", "-A", "--", str(line))
        commit(repo, accept.commit_message(r, what), "--amend")
        return {**out, "outcome": "accepted", "commit": git(repo, "rev-parse", "HEAD").stdout.strip()}
    git(repo, "reset", "-q", "--hard", "HEAD~1")
    if kept is not None and recorded:
        line.write_text(kept)
        git(repo, "add", "-A", "--", str(line))
        commit(repo, f"eval: record a rejected attempt at {iid}\n\n{what}")
    why = failed or "; ".join(f"{k}: {v['detail']}" for k, v in (r.get("rules") or {}).items()
                              if not v["ok"]) or "not accepted"
    return {**out, "outcome": "rejected" if failed is None else "skipped", "why": why}


def improve_quality(R: Any) -> dict[str, Any]:
    """Every edit this routine tries, on a branch. Returns the routine's `improve` record."""
    repo = pathlib.Path(R.cfg.need(None, "model", "repo"))
    if git(repo, "rev-parse", "--is-inside-work-tree").stdout.strip() != "true":
        return {"edits": [], "why": f"{repo} is not a git checkout, so there is no branch "
                                    f"to leave an edit on"}
    # The whole repository, not only the package: a rejected edit is taken off
    # with a hard reset, which would take any other uncommitted work with it.
    top = pathlib.Path(git(repo, "rev-parse", "--show-toplevel").stdout.strip())
    dirty = git(top, "status", "--porcelain").stdout.strip()
    if dirty:
        return {"edits": [], "why": f"the repository at {top} has uncommitted changes "
                                    f"({len(dirty.splitlines())} path(s)). Commit or stash them: "
                                    f"an edit is committed there and a rejected one is reset away"}
    issues = open_issues(R.run_dir)[:R.a.max_edits]
    if not issues:
        return {"edits": [], "why": "the diagnosis found nothing the model's repository owns"}
    start = git(repo, "rev-parse", "--abbrev-ref", "HEAD").stdout.strip()
    if start == "HEAD":
        start = git(repo, "rev-parse", "HEAD").stdout.strip()
    base = git(repo, "rev-parse", "HEAD").stdout.strip()
    branch = f"eval/{R.label}"
    if git(repo, "checkout", "-q", "-b", branch).returncode != 0:
        return {"edits": [], "why": f"the branch {branch} could not be made; it may already exist"}
    edits: list[dict[str, Any]] = []
    try:
        for k, issue in enumerate(issues, 1):
            edits.append(one_edit(R, repo, issue, k))
    finally:
        moved = git(repo, "rev-parse", "HEAD").stdout.strip() != base
        git(repo, "checkout", "-q", start)
        if not moved:
            git(repo, "branch", "-q", "-D", branch)
    return {"branch": branch if moved else None, "base": base, "edits": edits,
            "accepted": sum(e["outcome"] == "accepted" for e in edits),
            "measured": sum(e["outcome"] in ("accepted", "rejected") for e in edits)}
