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

import config  # noqa: E402
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


# Tags every production case carries, which say nothing about what an edit
# might break: sharing one does not make a case a neighbour.
BROAD_TAGS = {"production", "failure", "control", "mixed", "follow-up"}


def candidate_cases(set_dir: pathlib.Path, targets: list[str]) -> list[str]:
    """The cases an edit is re-asked on: its targets, every holdout case, every
    `control` case, and every case that shares a specific tag with a target.

    Not the whole set, which would double the night's cost per edit. The
    neighbours by tag are where a doc or a rename is most likely to break
    something, and the controls are the questions production gets right, which
    an edit must not break. Never a `test` case: no decision is made on those.

    A set with neither holdout nor control cases, such as questions a person
    wrote before launch, has nothing else that would catch an edit breaking an
    unrelated question, so every case but `test` is re-asked.
    """
    cases = read_jsonl(set_dir / "cases.jsonl")
    if not any(c.get("split") == "holdout" or "control" in (c.get("tags") or []) for c in cases):
        return [c["qid"] for c in cases if c.get("split") != "test"]
    want = set(targets)
    tags = {t for c in cases if c.get("qid") in want for t in c.get("tags") or []} - BROAD_TAGS
    return [c["qid"] for c in cases if c.get("split") != "test" and (
        c["qid"] in want or c.get("split") == "holdout" or "control" in (c.get("tags") or [])
        or tags & set(c.get("tags") or []))]


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


TABLE_ANNOTATIONS = ("#@ persist", "#@ preaggregate", "#(materialize)", "#(rollup)")


def storage_breach(diff: str, allowed: str) -> str | None:
    """Why an edit puts a table where `[performance] table_storage` forbids,
    or None. A table annotation with `storage=` lives in a storage destination
    (Credible); one without it lives in the source's own warehouse."""
    for line in diff.splitlines():
        if not line.startswith("+") or line.startswith("+++"):
            continue
        text = line[1:].strip()
        if not text.startswith(TABLE_ANNOTATIONS):
            continue
        stored = "storage=" in text.replace(" ", "")
        if allowed == "warehouse" and stored:
            return (f"the edit builds a table in Credible storage ({text}), and table_storage "
                    f"is \"warehouse\": tables stay in the source's own warehouse")
        if allowed == "credible" and not stored:
            return (f"the edit builds a table in the warehouse ({text}), and table_storage "
                    f"is \"credible\": tables go to Credible storage (storage=)")
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
    breach = storage_breach(git(repo, "diff", "--cached", "-U0", "--", ".").stdout,
                            config.table_storage(R.cfg))
    if breach:
        git(repo, "reset", "-q", "--hard")
        git(repo, "clean", "-fdq", "--", ".")
        return {**out, "outcome": "skipped", "why": breach}, cluster, False
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
    # A set version keeps its attempts in the optimization package, outside
    # the model's repository: nothing to commit, and nothing a reset can lose.
    top = pathlib.Path(git(repo, "rev-parse", "--show-toplevel").stdout.strip() or repo)
    in_repo = line.resolve().is_relative_to(top.resolve())
    kept = line.read_text() if line.exists() and in_repo else None
    if accepted:
        if in_repo:
            git(repo, "add", "-A", "--", str(line))
        commit(repo, message, "--amend")
        return {**out, "outcome": "accepted",
                "commit": git(repo, "rev-parse", "HEAD").stdout.strip()}
    git(repo, "reset", "-q", "--hard", "HEAD~1")
    if in_repo and kept is not None and recorded:
        line.write_text(kept)
        git(repo, "add", "-A", "--", str(line))
        commit(repo, f"eval: record a rejected attempt at {out['issue']}\n\n{out['what']}")
    return {**out, "outcome": "rejected" if measured else "skipped", "why": why or "not accepted"}


def diagnose_failed_targets(R: Any, name: str, runs: list[str], targets: list[str]) -> None:
    """Diagnose each target a re-run got wrong, against the edited model, so
    `accept` can tell the failure the edit aimed at coming back from a
    different mistake on the same question."""
    from flip_table import verdicts
    for r in runs:
        d = R.cfg.workdir() / "runs" / r
        if not (d / "events.jsonl").is_file():
            continue
        v = verdicts(d)
        bad = [q for q in targets if q in v and v[q]["passed"] is False]
        if bad:
            R.do(f"diagnose {r}", "diagnose", *R.s, "--experiment", name, "--label", r,
                 "--only", ",".join(bad), "--verdicts", "no_match,near_match", "--no-effort")


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
        if failed is None:
            diagnose_failed_targets(R, name, runs, targets)
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
    if r:
        out["acceptance"] = {k: r.get(k) for k in ("targetsFixed", "failedAnotherWay", "regressions", "noise",
                                                    "unstable", "doubtedGoldens")}
        out["acceptance"]["rules"] = {k: v.get("ok") for k, v in (r.get("rules") or {}).items()}
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


def target_answers(run: pathlib.Path, targets: list[str]) -> dict[str, dict[str, Any]]:
    """What answered each targeted query in one replay, from its path report:
    `{qid: {"kind", "skipped"}}`. `kind` is None when the server sent no path
    report, and the query is absent when the replay did not time it."""
    path = run / "results.jsonl"
    rows = read_jsonl(path) if path.is_file() else []
    want = set(targets)
    return {r["qid"]: {"kind": (r.get("served_by") or {}).get("kind"),
                       "skipped": r.get("skipped") or []}
            for r in rows if r.get("qid") in want and not r.get("error")}


def still_live(answers: dict[str, dict[str, Any]]) -> list[str]:
    """The targeted queries the path report says no table answered."""
    return sorted(q for q, a in answers.items() if a["kind"] == "live")


def why_live(answers: dict[str, dict[str, Any]], verdicts: list[dict[str, Any]] | None = None) -> str:
    """The server's own reason a targeted query was answered live: a table that
    does not serve at all, else the first skip on the first live query."""
    for v in verdicts or []:
        if v.get("verdict") not in (None, "serves", "awaiting_build"):
            return f"{v.get('source')} does not serve: {v.get('message') or v.get('reason')}"
    for q in still_live(answers):
        for sk in answers[q]["skipped"]:
            return f"{q}: {sk.get('reason')}" + (f" ({sk['detail']})" if sk.get("detail") else "")
    return "no table answered them, and the path report gave no reason"


def speed_verdict(summaries: list[dict[str, Any]], band: float,
                  answers: list[dict[str, dict[str, Any]]] | None = None) -> tuple[bool, dict[str, str]]:
    """Is the edit a speed-up? (accepted, the rules it failed).

    Every replay of the edit must say the same thing: no stored answer's rows
    changed and none now errors, no query is slower, at least one is faster,
    and the total is faster by more than the band. Two replays, because one
    replay's total moves by itself: an unchanged package measured 11 to 24%
    apart on a loaded machine.

    With `answers` (one `target_answers` per replay), the speed-up must come
    from tables, in every replay: at least one targeted query is answered by
    a table, and every targeted query counted faster is one a table answered.
    Faster alone is not proof: the noise band can pass a change whose table
    never answered. A target that stays live is allowed, since "nothing
    slower" already holds it: a hot source's targets are all its queries, and
    some (a distinct count, say) no rollup can answer, so requiring every one
    rejected an edit that made the rest 20% faster with the same rows.
    """
    failed: dict[str, str] = {}
    if len(summaries) < 2:
        failed["twice"] = f"{len(summaries)} replay(s) of the edit"
    for n, a in enumerate(answers or [], 1):
        live = still_live(a)
        c = (summaries[n - 1].get("compare") or {}) if n <= len(summaries) else {}
        faster_live = sorted({e["qid"] for e in c.get("faster") or []} & set(live))
        if a and len(live) == len(a):
            failed[f"answered by a table (replay {n})"] = (
                f"no targeted query was answered by a table; {why_live(a)}")
        elif faster_live:
            failed[f"answered by a table (replay {n})"] = (
                f"{', '.join(faster_live)} counted faster but still answered live, so the "
                f"gain is noise; {why_live({q: a[q] for q in faster_live})}")
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
    runs = cfg.workdir() / "runs"
    failed, early = None, None
    if R.do(f"experiment {name}", "experiment", "start", name, *s) != 0:
        failed = "the edit could not be served and built as an experiment"
    else:
        for r in replays:
            # Exit 1 is a replay that ran and found changed rows: a result.
            if failed is None and early is None and R.do(
                    f"query-replay {r}", "query-replay", *s, "--experiment", name,
                    "--from", label, "--baseline", label, "--label", r, "--no-share") not in (0, 1):
                failed = f"the replay {r} did not run"
            if failed is None and early is None and r == replays[0]:
                # A table that never answers a targeted query cannot be the
                # speed-up, whatever the clock says: stop before the second
                # replay, and record the server's reason.
                a = target_answers(runs / r, out["targets"])
                if a and len(still_live(a)) == len(a):
                    meta = runs / r / "run.json"
                    mat = (json.loads(meta.read_text()).get("materialization") or {}) \
                        if meta.exists() else {}
                    early = (f"no targeted query was answered by a table; "
                             f"{why_live(a, mat.get('serveVerdicts'))}")
        R.do(f"stop {name}", "experiment", "stop", name, *s)
    out["runs"] = replays if early is None else replays[:1]
    before = len(attempts.read(cfg.set_dir))
    accepted, rules, effect = False, {}, {}
    if failed is None:
        got = [json.loads((runs / r / "summary.json").read_text())
               for r in out["runs"] if (runs / r / "summary.json").exists()]
        band = json.loads((runs / replays[0] / "run.json").read_text()).get("latencyBand") or 0.2 \
            if (runs / replays[0] / "run.json").exists() else 0.2
        if early:
            rules = {"answered by a table (replay 1)": early}
        else:
            accepted, rules = speed_verdict(
                got, band, [target_answers(runs / r, out["targets"]) for r in replays])
        last = (got[-1].get("compare") or {}) if got else {}
        effect = {"totalBeforeMs": last.get("total_before_ms"),
                  "totalAfterMs": last.get("total_after_ms"),
                  "buildBefore": last.get("build_before"),
                  "buildAfter": got[-1].get("build") if got else None}
        diff = (cluster / "edit.patch").read_text() if (cluster / "edit.patch").exists() else None
        attempts.append(cfg.set_dir, attempts.row(
            what=what, outcome="accepted" if accepted else "rejected", baseline=label,
            candidates=out["runs"], targets=out["targets"], issue_ids=[iid], failed=rules,
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


def edit_cost(R: Any, issue: dict[str, Any], runs: list[str]) -> float:
    """What one edit cost in model calls: the improving agent, then each
    candidate run's answering and judging. A replay costs nothing."""
    total = 0.0
    cluster = R.run_dir / "artifacts" / "clusters" / issue["issue_id"].replace("/", "_")
    try:
        total += json.loads((cluster / "result.json").read_text()).get("cost_usd") or 0
    except (OSError, ValueError):
        pass
    for r in runs:
        try:
            m = json.loads((R.cfg.workdir() / "runs" / r / "run.json").read_text())
        except (OSError, ValueError):
            continue
        total += (m.get("answererCostUsd") or 0) + (m.get("judgeCostUsd") or 0)
    return round(total, 4)


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
    # The cap covers the night, not only the run: checked before each edit,
    # so the last edit can run past it by at most what one edit costs.
    cap = getattr(R.a, "max_cost_usd", None)
    spent = sum(v or 0 for v in ((R.result.get("number") or {}).get("costUsd") or {}).values())
    capped = None
    stop_after = getattr(R.a, "max_rejected", None)
    in_a_row = 0
    try:
        for k, issue in enumerate(issues, 1):
            if cap is not None and spent >= cap:
                capped = (f"stopped before edit {k} of {len(issues)}: ${spent:.2f} spent, at or "
                          f"over --max-cost-usd {cap:g}")
                break
            if stop_after and in_a_row >= stop_after:
                capped = (f"stopped before edit {k} of {len(issues)}: the last {in_a_row} "
                          f"edits were not accepted (--max-rejected {stop_after})")
                break
            e = try_one(R, repo, issue, k)
            e["costUsd"] = edit_cost(R, issue, e.get("runs") or [])
            spent += e["costUsd"]
            edits.append(e)
            in_a_row = 0 if e["outcome"] == "accepted" else in_a_row + 1
    finally:
        moved = git(repo, "rev-parse", "HEAD").stdout.strip() != base
        git(repo, "checkout", "-q", start)
        if not moved:
            git(repo, "branch", "-q", "-D", branch)
    return {"branch": branch if moved else None, "base": base, "edits": edits,
            "accepted": sum(e["outcome"] == "accepted" for e in edits),
            "measured": sum(e["outcome"] in ("accepted", "rejected") for e in edits),
            "costUsd": round(sum(e["costUsd"] for e in edits), 4),
            **({"capped": capped} if capped else {})}


# ------------------------------------------------------------------ the PR

def for_a_person(run: pathlib.Path, set_dir: pathlib.Path,
                 imp: dict[str, Any]) -> list[str]:
    """What only a person can settle, one line each: keys nobody verified,
    verdicts the judge would not decide, keys the judge or the improving agent
    doubted, and failures diagnosed as the eval's own (dataset)."""
    cases = {c["qid"]: c for c in read_jsonl(set_dir / "cases.jsonl")} if (
        set_dir / "cases.jsonl").is_file() else {}
    events = read_jsonl(run / "events.jsonl") if (run / "events.jsonl").is_file() else []
    out = []
    for q, c in cases.items():
        st = (c.get("golden") or {}).get("status")
        if st and st != "verified":
            out.append(f"`{q}`: the key is `{st}`, not verified. \"{c['question']}\"")
    for e in events:
        if e.get("kind") == "score" and e.get("verdict") == "needs_human":
            out.append(f"`{e['qid']}`: the judge would not decide: {(e.get('reason') or '')[:200]}")
        elif e.get("kind") == "score" and e.get("gold_status") in ("suspect", "verified_wrong"):
            out.append(f"`{e['qid']}`: the judge doubts the key ({e['gold_status']}): "
                       f"{(e.get('gold_note') or '')[:200]}")
        elif e.get("kind") == "issue" and e.get("owner") == "dataset" or (
                e.get("kind") == "issue" and e.get("component") == "dataset"):
            out.append(f"`{', '.join(e.get('qids') or [])}`: diagnosed as the eval's own problem "
                       f"({e.get('primary_code')}): {(e.get('diagnosis') or '')[:200]}")
    for e in imp.get("edits") or []:
        for g in (e.get("acceptance") or {}).get("doubtedGoldens") or []:
            out.append(f"`{g}`: the acceptance check set this key aside as doubted")
        if e.get("outcome") == "skipped" and "answer key" in (e.get("why") or ""):
            out.append(f"{e['issue']}: {e['why'][:200]}")
    return list(dict.fromkeys(out))


def pr_body(R: Any, imp: dict[str, Any]) -> str:
    """The pull request body for the branch: why (production failures, with how
    often they were asked), what changed, the evidence, and what a person must
    settle. Plain Markdown, no answer values."""
    set_dir = R.cfg.set_dir
    meta = R.cfg.set_meta
    cases = {c["qid"]: c for c in read_jsonl(set_dir / "cases.jsonl")} if (
        set_dir / "cases.jsonl").is_file() else {}
    n = R.result.get("number") or {}
    lines = [f"Eval routine `{R.label}` on set `{R.cfg.set_name}`"
             + (f", version `{meta['version']}`" if meta.get("version") else "")
             + f" ({set_dir}).", ""]
    accepted = [e for e in imp.get("edits") or [] if e["outcome"] == "accepted"]
    lines += ["## What changed", ""]
    for e in accepted:
        lines.append(f"- {e['what']}")
    if not accepted:
        lines.append("- No edit was accepted.")
    lines += ["", "## Why: production failures it closes" if accepted
              else "## The production failures the edits aimed at", ""]
    for e in accepted or [e for e in imp.get("edits") or [] if e.get("targets")]:
        for q in e.get("targets") or []:
            c = cases.get(q) or {}
            sf = c.get("sourceFields") or {}
            codes = [t.split(":", 1)[1] for t in c.get("tags") or [] if t.startswith("failure:")]
            lines.append(f"- \"{c.get('question', q)}\": asked {sf.get('asked', '?')} time(s), "
                         f"judged wrong {sf.get('judgedWrong', '?')}"
                         + (f" ({', '.join(codes)})" if codes else "")
                         + (f"; key from {sf['keyFrom']}" if sf.get("keyFrom") else ""))
    lines += ["", "## Evidence", "",
              f"- Baseline run `{R.label}`: {n.get('passed')} of {n.get('decided')} decided cases passed."]
    controls = [q for q, c in cases.items() if "control" in (c.get("tags") or [])]
    for e in imp.get("edits") or []:
        acc = e.get("acceptance") or {}
        rules = acc.get("rules") or {}
        lines.append(f"- `{e['issue']}`: {e['outcome']}"
                     + (f", runs {', '.join(f'`{r}`' for r in e.get('runs') or [])}" if e.get("runs") else "")
                     + (f"; targets fixed: {len(acc.get('targetsFixed') or [])} of {len(e.get('targets') or [])}"
                        if acc else "")
                     + (f"; regressions: {len(acc.get('regressions') or [])}" if acc else "")
                     + (f"; controls re-asked: {len(controls)}" if controls else "")
                     + (f"; rules: {', '.join(k + (' ok' if v else ' FAILED') for k, v in rules.items())}"
                        if rules else "")
                     + (f". Why not: {e['why'][:300]}" if e.get("why") and e["outcome"] != "accepted" else ""))
    if imp.get("capped"):
        lines.append(f"- {imp['capped']}")
    cost = (n.get("costUsd") or {})
    lines.append(f"- Cost: run ${sum(v or 0 for v in cost.values()):.2f}, edits ${imp.get('costUsd') or 0:.2f}.")
    people = for_a_person(R.run_dir, set_dir, imp)
    lines += ["", "## For a person to settle", ""]
    lines += [f"- {x}" for x in people] or ["- Nothing."]
    lines += ["", "Every edit tried, accepted or not, is in the set's attempts file "
              f"(`{__import__('attempts').path(set_dir)}`)."]
    return "\n".join(lines) + "\n"
