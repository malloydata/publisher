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


def make_edit(R: Any, repo: pathlib.Path, issue: dict[str, Any], k: int,
              extra: tuple[str, ...] = ()) -> tuple[dict[str, Any], pathlib.Path, bool]:
    """Run the improve step for one cluster and commit what it wrote.

    Returns (what happened so far, the cluster's artifact directory, whether a
    candidate commit now sits on the branch). With no commit, the tree is back
    as it was and `outcome` says why.
    """
    label = R.label
    iid, targets = issue["issue_id"], list(issue.get("qids") or [])
    out: dict[str, Any] = {"issue": iid, "targets": targets, "outcome": None}
    cluster = R.run_dir / "artifacts" / "clusters" / iid.replace("/", "_")
    code = R.do(f"improve {iid}", "improve", *R.s, "--label", label, "--only", iid,
                "--experiment", f"{label}-i{k}", "--no-isolate", "--force", *extra)
    result = json.loads((cluster / "result.json").read_text()) if (
        cluster / "result.json").exists() else {}
    out["what"] = (result.get("edit") or f"close {iid}")[:200]
    dirty = git(repo, "status", "--porcelain", "--", ".").stdout.strip()
    stop = stopped_because(code, result, bool(dirty))
    if stop:
        git(repo, "checkout", "--", ".")
        git(repo, "clean", "-fdq", "--", ".")
        return {**out, "outcome": "skipped", "why": stop}, cluster, False
    git(repo, "add", "-A", "--", ".")
    if not commit(repo, f"candidate: {out['what']}"):
        git(repo, "reset", "-q", "--hard")
        return {**out, "outcome": "skipped", "why": "the edit could not be committed"}, cluster, False
    return out, cluster, True


def keep_or_drop(R: Any, repo: pathlib.Path, out: dict[str, Any], accepted: bool,
                 message: str, why: str | None, measured: bool, before: int) -> dict[str, Any]:
    """Settle the candidate commit: amend it into the checkpoint, or take it off.

    Either way the line the acceptance wrote to attempts.jsonl is committed: a
    rejected attempt nobody can read is one somebody pays to reject again.
    `before` is how many attempts the file held before this one.
    """
    import attempts
    line = attempts.path(R.cfg.set_dir)
    recorded = len(attempts.read(R.cfg.set_dir)) > before
    kept = line.read_text() if line.exists() else None
    if accepted:
        git(repo, "add", "-A", "--", str(line))
        commit(repo, message, "--amend")
        return {**out, "outcome": "accepted",
                "commit": git(repo, "rev-parse", "HEAD").stdout.strip()}
    git(repo, "reset", "-q", "--hard", "HEAD~1")
    if kept is not None and recorded:
        line.write_text(kept)
        git(repo, "add", "-A", "--", str(line))
        commit(repo, f"eval: record a rejected attempt at {out['issue']}\n\n{out['what']}")
    return {**out, "outcome": "rejected" if measured else "skipped", "why": why or "not accepted"}


def one_edit(R: Any, repo: pathlib.Path, issue: dict[str, Any], k: int) -> dict[str, Any]:
    """Improve, commit, measure, accept, keep or drop. Returns what happened."""
    import accept
    import attempts
    cfg, s, label = R.cfg, R.s, R.label
    out, cluster, committed = make_edit(R, repo, issue, k)
    if not committed:
        return out
    iid, targets, what = out["issue"], out["targets"], out["what"]
    name = f"{label}-c{k}"
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
    r = newest_acceptance(cfg.workdir()) if failed is None else {}
    why = failed or "; ".join(f"{k}: {v['detail']}" for k, v in (r.get("rules") or {}).items()
                              if not v["ok"])
    return keep_or_drop(R, repo, out, failed is None and bool(r.get("accepted")),
                        accept.commit_message(r, what) if r.get("accepted") else "",
                        why, failed is None, before)


# ------------------------------------------------------------------- speed

NO_EDIT_CODES = ("PERSIST-NOT-BUILT", "LIVE-FALLBACK")


def speed_issues(run: pathlib.Path) -> list[dict[str, Any]]:
    """The speed issues an edit can close, the source that costs most first.

    Ranked on the replay itself, before anything is built: a source's cost is
    each of its queries' time by how often it ran. `PERSIST-NOT-BUILT` and
    `LIVE-FALLBACK` need a build or a look at the server, not an edit.
    """
    events = read_jsonl(run / "events.jsonl") if (run / "events.jsonl").is_file() else []
    status = {e["issue_id"]: e.get("status") for e in events if e.get("kind") == "issue_status"}
    try:
        by_source = json.loads((run / "summary.json").read_text()).get("by_source") or {}
    except (OSError, ValueError):
        by_source = {}

    def cost(i: dict[str, Any]) -> float:
        s = by_source.get((i.get("evidence") or {}).get("source")) or {}
        return s.get("weighted_ms") or s.get("total_ms") or 0.0
    got = [e for e in events if e.get("kind") == "issue" and e.get("component") == "performance"
           and e.get("owner") == "model" and e.get("primary_code") not in NO_EDIT_CODES
           and status.get(e["issue_id"], "open") == "open"]
    return sorted(got, key=lambda i: (-cost(i), i["issue_id"]))


def speed_verdict(summaries: list[dict[str, Any]], band: float) -> tuple[bool, dict[str, str]]:
    """Is the edit a speed-up? (accepted, the rules it failed).

    Every replay of the edit must say the same thing: no stored answer's rows
    changed and none now errors, no query is slower, at least one is faster,
    and the total is faster by more than the band. Two replays, because one
    replay's total moves by itself: an unchanged package measured 11 to 24%
    apart on a loaded machine.
    """
    failed: dict[str, str] = {}
    if len(summaries) < 2:
        failed["twice"] = f"{len(summaries)} replay(s) of the edit"
    for n, s in enumerate(summaries, 1):
        c = s.get("compare")
        if not c:
            failed[f"compared (replay {n})"] = "the replay was not compared with the baseline"
            continue
        if c.get("rows_changed") or c.get("broke"):
            failed[f"rows held (replay {n})"] = (
                f"{len(c.get('rows_changed') or [])} changed, {len(c.get('broke') or [])} now error")
        if c.get("slower"):
            failed[f"nothing slower (replay {n})"] = ", ".join(e["qid"] for e in c["slower"])
        ratio = c.get("total_ratio")
        if not c.get("faster") or ratio is None or ratio > 1 - band:
            failed[f"faster (replay {n})"] = (
                f"{len(c.get('faster') or [])} quer(ies) faster, total x{ratio} "
                f"(needs under x{round(1 - band, 3)})")
    return not failed, failed


def one_speed_edit(R: Any, repo: pathlib.Path, issue: dict[str, Any], k: int) -> dict[str, Any]:
    """Improve, commit, build, replay twice, decide, keep or drop."""
    import attempts
    cfg, s, label = R.cfg, R.s, R.label
    # Nobody is here to approve a persist. The edit is measured and left on a
    # branch, and merging the branch is the approval.
    out, cluster, committed = make_edit(R, repo, issue, k, ("--unattended",))
    if not committed:
        return out
    iid, what = out["issue"], out["what"]
    name = f"{label}-c{k}"
    replays = [f"{name}-1", f"{name}-2"]
    failed = None
    if R.do(f"experiment {name}", "experiment", "start", name, *s) != 0:
        failed = "the edit could not be served and built as an experiment"
    else:
        for r in replays:
            # Exit 1 is a replay that ran and found changed rows: a result.
            if failed is None and R.do(f"query-replay {r}", "query-replay", *s, "--experiment", name,
                                       "--from", label, "--baseline", label, "--label", r,
                                       "--no-share") not in (0, 1):
                failed = f"the replay {r} did not run"
        R.do(f"stop {name}", "experiment", "stop", name, *s)
    out["runs"] = replays
    before = len(attempts.read(cfg.set_dir))
    accepted, rules, effect = False, {}, {}
    if failed is None:
        runs = cfg.workdir() / "runs"
        got = [json.loads((runs / r / "summary.json").read_text())
               for r in replays if (runs / r / "summary.json").exists()]
        band = json.loads((runs / replays[0] / "run.json").read_text()).get("latencyBand") or 0.2 \
            if (runs / replays[0] / "run.json").exists() else 0.2
        accepted, rules = speed_verdict(got, band)
        last = (got[-1].get("compare") or {}) if got else {}
        effect = {"totalBeforeMs": last.get("total_before_ms"),
                  "totalAfterMs": last.get("total_after_ms"),
                  "buildBefore": last.get("build_before"),
                  "buildAfter": got[-1].get("build") if got else None}
        diff = (cluster / "edit.patch").read_text() if (cluster / "edit.patch").exists() else None
        attempts.append(cfg.set_dir, attempts.row(
            what=what, outcome="accepted" if accepted else "rejected", baseline=label,
            candidates=replays, targets=out["targets"], issue_ids=[iid], failed=rules,
            effect=effect, diff=diff))
    out["effect"] = effect
    build = (effect.get("buildAfter") or {})
    message = "\n".join([
        f"checkpoint: {what}", "",
        f"A speed edit, accepted on two query replays against {label}: no stored answer's "
        f"rows changed, no query slower, total {effect.get('totalBeforeMs')} -> "
        f"{effect.get('totalAfterMs')} ms.",
        f"It builds {build.get('tables')} table(s) in {build.get('build_ms')} ms on each refresh."
        if build else "It builds no table.",
        f"Replays: {', '.join(replays)}."])
    why = failed or "; ".join(f"{k}: {v}" for k, v in rules.items())
    return keep_or_drop(R, repo, out, accepted, message, why, failed is None, before)


def improve_quality(R: Any) -> dict[str, Any]:
    """Every edit a quality routine tries, on a branch. Returns its `improve` record."""
    return on_a_branch(R, open_issues(R.run_dir), one_edit,
                       "the diagnosis found nothing the model's repository owns")


def improve_speed(R: Any) -> dict[str, Any]:
    """Every edit a speed routine tries, on a branch. Returns its `improve` record."""
    return on_a_branch(R, speed_issues(R.run_dir), one_speed_edit,
                       "the replay found no source an edit would speed up")


def repo_not_ready(repo: pathlib.Path) -> str | None:
    """Why improve mode cannot leave edits in `repo`, or None."""
    if git(repo, "rev-parse", "--is-inside-work-tree").stdout.strip() != "true":
        return f"{repo} is not a git checkout, so there is no branch to leave an edit on"
    # The whole repository, not only the package: a rejected edit is taken off
    # with a hard reset, which would take any other uncommitted work with it.
    top = pathlib.Path(git(repo, "rev-parse", "--show-toplevel").stdout.strip())
    dirty = git(top, "status", "--porcelain").stdout.strip()
    if dirty:
        return (f"the repository at {top} has uncommitted changes "
                f"({len(dirty.splitlines())} path(s)). Commit or stash them: "
                f"an edit is committed there and a rejected one is reset away")
    return None


def on_a_branch(R: Any, found: list[dict[str, Any]], try_one: Any, nothing: str) -> dict[str, Any]:
    """Try an edit for each of `found` on the branch eval/<label>, and end on
    the branch the repository was on."""
    repo = pathlib.Path(R.cfg.need(None, "model", "repo"))
    # Asked again here, not only before the run: the repository can change
    # while the run is answering.
    why = repo_not_ready(repo)
    if why:
        return {"edits": [], "why": why}
    issues = found[:R.a.max_edits]
    if not issues:
        return {"edits": [], "why": nothing}
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
            edits.append(try_one(R, repo, issue, k))
    finally:
        moved = git(repo, "rev-parse", "HEAD").stdout.strip() != base
        git(repo, "checkout", "-q", start)
        if not moved:
            git(repo, "branch", "-q", "-D", branch)
    return {"branch": branch if moved else None, "base": base, "edits": edits,
            "accepted": sum(e["outcome"] == "accepted" for e in edits),
            "measured": sum(e["outcome"] in ("accepted", "rejected") for e in edits)}
