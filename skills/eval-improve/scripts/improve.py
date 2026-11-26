#!/usr/bin/env python3
"""Spawn one modeling agent per diagnosed cluster to edit the model. Stdlib only.

  python improve.py --run <workdir>/runs/<label> --set <set-dir> --watch-mode
  # the model directory comes from [model] repo in the set's eval.toml

One agent per fixable cluster (`owner: model` or `owner: package-skill`), each
holding `skill:eval-improve`, each producing at most one smallest edit with
probe receipts. Appends one `candidate` event per cluster to the run's
`events.jsonl`.

THIS SCRIPT CONTAINS NO MODELING DOCTRINE

The edit-tier ranking, the expert test, the DISAGREEMENT rule, the warning that
a false `primary_key` compiles and silently corrupts every aggregate -- all of
that is in `skill:eval-improve` and the `malloy-gotchas-modeling` skill, and
none of it is repeated here. Both are installed into the agent's workspace and
loaded by the CLI -- eval-improve by name, malloy-gotchas-modeling with the rest
of the `modeling` manifest group -- instead of being hand-pasted at a guessed
path. What is left in this file is process: pick the clusters, isolate the
tree, capture the diff, write the event.

THIS SCRIPT DOES NOT ACCEPT ANYTHING

The skill's second hard boundary is that an improver never accepts its own edit,
and the reason is specific: an improver verifying its own fix writes the query
it already knows, which proves the fix is expressible, not that the next blind
answerer will find it. So this stops at `candidate`. The acceptance check -- re-answering
with fresh agents that never saw the diagnosis, then comparing against the noise
band -- belongs to `skill:eval-loop`, and the command is printed at the end.

ISOLATION BETWEEN CLUSTERS

Each cluster is a separate hypothesis and must be separately reviewable, so the
model directory is returned to its starting state between agents and each edit
is kept as a patch. Without that, cluster 3's agent inherits clusters 1 and 2's
edits, every probe it runs measures their combined effect, and the candidate
events describe diffs that never existed alone.

MAKING AN EDIT VISIBLE TO THE SERVER

Publisher serves a package outside a watched environment as a published
version under `publisher_data/<env>/<pkg>/<version>/`, which is immutable:
editing the repo and reloading changes nothing, and the verification probe
quietly tests the old model. `--watch-mode` says Publisher was started with
`--watch-env` (`eval.py serve model --set <set> --watch-env <env>`), which symlinks
local-dir packages in place and makes edits live; then the helper is just a
reload. Without it the helper syncs the edit onto the served copy first, which
works only on a server with versioning off. Either way a package the server
serves as a published version is refused before any agent starts (the server
is asked, in both modes): nothing synced there is served, the sync would
delete it, and a reload of it answers 200 without reading the edit. Either way
the agent gets one generated script rather than a general shell.

`--experiment <name>` is the third way, and the one that works on a server
that publishes versions, which is the default: the agent's edits are served as
the experiment `<package>.<name>` beside the main package (experiment.py), and
the helper publishes the working tree as that experiment again after each
edit. The agent probes the experiment, the main package and its record are
never touched, and no watch mode or restart is needed. The routine's improve
mode runs this way.
"""
from __future__ import annotations

import argparse
import json
import pathlib
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.request
from typing import Any

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent.parent
                       / "eval-loop" / "scripts"))
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent.parent
                       / "eval-answer" / "scripts"))
from agent_harness import default_manifest, manifest_skills, skills_roots, spawn_agent  # noqa: E402
import config  # noqa: E402
import ledger  # noqa: E402
from ledger import read_jsonl  # noqa: E402
import golden_rows  # noqa: E402
import attempts  # noqa: E402
import golden_leak  # noqa: E402

SKILLS_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
EXPERIMENT_PY = SKILLS_ROOT / "eval-loop" / "scripts" / "experiment.py"
IMPROVE_TOOLS = ("mcp__publisher__get_context",
                 "mcp__publisher__execute_query",
                 "mcp__publisher__compile_model",
                 "mcp__publisher__reload_package",
                 "mcp__publisher__get_skill",
                 "mcp__publisher__get_status",
                 "Read", "Edit", "Write", "Grep", "Glob",
                 "Bash(bash ./sync_and_reload.sh)")

# REST, not MCP: an MCP reply escapes its inner JSON. The sha is read from the
# package's serving state after the reload (the reload response itself carries
# it only while the server inlines the serving fields). A failed reload prints
# its error and stops, so the previous load's sha is never shown for it.
RELOAD = """reload_body=$(mktemp)
reload_code=$(curl -s -m 60 -o "$reload_body" -w "%{{http_code}}" "{rest_url}/api/v0/environments/{environment}/packages/{package}?reload=true" || echo 000)
if [ "$reload_code" != "200" ]; then
  echo "reload failed (HTTP $reload_code):"; cat "$reload_body"; echo; rm -f "$reload_body"; exit 1
fi
rm -f "$reload_body"
python3 -c '
import json, urllib.request
base = "{rest_url}/api/v0"
def get(path):
    try:
        with urllib.request.urlopen(base + path, timeout=30) as r:
            return json.load(r)
    except Exception:
        return {{}}
sha = get("/environments/{environment}/packages/{package}/status").get("sourceContentSha")
if not sha:  # a server from before that route: the package`s /status entry
    for e in get("/status").get("environments") or []:
        for p in (e.get("packages") or []) if e.get("name") == "{environment}" else []:
            if p.get("name") == "{package}" and p.get("sourceContentSha"):
                sha = p["sourceContentSha"]
if sha:
    print("\\"sourceContentSha\\":\\"%s\\"" % sha)
'"""

SYNC_SCRIPT = """#!/bin/bash
set -euo pipefail
{sync}{reload}
echo "reloaded; if sourceContentSha did not change, the edit did not reach the server"
"""


# With --experiment: publish the working tree as the experiment again. Its last
# line is the same `"sourceContentSha":"..."` the reload prints, so the
# agent's instruction to watch that value holds in both modes.
EXPERIMENT_SYNC = """{python} {experiment_py} sync {name} --set {set_dir} --dir {model_dir} --no-build
"""

SYNC_HELPER = "sync_and_reload.sh"

# For a run nobody is watching. The skill tells a speed edit to wait for the
# user's yes, because a persisted table costs a build on every refresh. Here
# the yes comes later and from the right place: the edit is measured and left
# on a branch, and a person merges it or does not.
UNATTENDED = """
NOBODY IS HERE TO ASK

This run is unattended. Do not wait for approval and do not ask a question:
nobody will answer. Where the skill says to propose an edit and wait for the
user, make the edit instead. It is not published: it is measured as an
experiment beside the main package and left on a branch, and a person approves
it by merging that branch. If you would not make the edit without asking,
make none and say why in `disagreement`.
"""


def served_version(rest_url: str | None, environment: str,
                   package: str) -> str | None:
    """The version the model server serves this package as, or None.

    The server's own answer: a published version's resource carries its
    `versionId`, and a watch-mounted or copied package has none. A version
    is immutable, so neither the sync nor watch mode reaches it. None also
    when the server cannot be asked; the folder check below still stands.
    """
    if not rest_url:
        return None
    url = (f"{rest_url.rstrip('/')}/api/v0/environments/{environment}"
           f"/packages/{package}")
    try:
        with urllib.request.urlopen(url, timeout=30) as r:
            return json.load(r).get("versionId") or None
    except Exception:
        return None


def served_as_versions(served: pathlib.Path) -> bool:
    """Whether Publisher serves this package folder as published versions.

    A version lives in `<pkg>/<version>/`, so the folder holds no
    publisher.json of its own. A copy (versioning off) does, and a watch
    mount is a symlink to the source.
    """
    return (served.is_dir() and not served.is_symlink()
            and not (served / "publisher.json").exists())


def git(args: list[str], cwd: pathlib.Path) -> str:
    return subprocess.run(["git", *args], cwd=cwd, capture_output=True,
                          text=True).stdout


def candidate_diff(model_dir: pathlib.Path, extra: tuple[str, ...] = ()) -> str:
    """The agent's edit: tracked changes plus new files, minus our own helper.

    `git diff` alone omits untracked files, so a new skills/ file left an empty
    patch that `git clean` then deleted. `--no-index` diffs each against
    /dev/null without touching the index.
    """
    out = git(["diff", *extra, "--", "."], model_dir)
    new = git(["ls-files", "--others", "--exclude-standard", "--", "."],
              model_dir).splitlines()
    for f in new:
        if f != SYNC_HELPER:
            out += git(["diff", "--no-index", *extra, "--", "/dev/null", f],
                       model_dir)
    return out


# Thin on purpose: the cluster, the cases, the one operational fact the skill
# cannot know (how to reach the server), and the output shape. Everything about
# how to choose an edit is in the loaded skill.
IMPROVE_PROMPT = """Apply the eval-improve skill to close ONE diagnosed cluster.

Environment: {environment}
Package: {package}
Model directory: {model_dir} -- your working tree, and your cwd. Edit here.

THE CLUSTER
{cluster}

THE CASES IN IT
{cases}

WHAT WAS ALREADY TRIED ON THIS SET

Each entry is an edit that was put to the acceptance check, newest first,
those aimed at your cases before the rest. A REJECTED edit failed for the
reason given: do not make it again, and do not make a reworded version of it.
If you believe a rejected edit was right and the rejection was noise, say so
in `disagreement` and make no edit. An ACCEPTED edit is already in the model.

{prior}

MAKING YOUR EDIT REACH THE SERVER

After each edit run `bash ./sync_and_reload.sh`. It prints `sourceContentSha`.
If that value does not change, your edit did NOT reach the server and every
probe you run afterwards is testing the old model. This covers a package skill
under skills/ as well as a .malloy file: both are part of the hash.

One edit for the cluster's shared root cause, not one per case.
{unattended}
READ THE CLUSTER'S `sufficiency` BEFORE YOU EDIT

It reports whether the diagnosis was PROBED, worst case across the cluster's
cases. `sufficient` means someone checked. `insufficient` and `unknown` mean
the diagnosis is a lead and not a finding: `unknown` in particular is what a
diagnosis carries when it was recovered from a malformed reply and holds no
probe records at all. On either of those, probe the claim yourself before you
change anything, and if your probe does not reproduce what the diagnosis
asserts, say so and make no edit. An edit resting on a claim nobody verified is
the failure this field exists to prevent.

WHEN YOU ARE DONE

Give the report block, then emit the JSON object as the LAST thing in your
reply, with nothing after it. Both shapes, and the golden re-derivation you run
BEFORE either, are defined in `reference/output-contract.md` of the eval-improve
skill -- read that file; a shape invented here is dropped.

A reply that ends in prose is a lost result. The edit still happened and the
receipts -- probes, edit tier, golden-suspect list -- are gone, so the
acceptance check has nothing to weigh.
"""


def model_under_test(a: argparse.Namespace) -> pathlib.Path | None:
    """The edited model for the verifier's `--model`: `<package>.malloy`, else
    the model directory when it holds any .malloy file, else None.

    Named by the configured package, not `a.package`: with `--experiment` that
    is `<package>.<name>`, which no file is called. The served copy is skipped
    for an experiment too, because it is the main package, which the agent
    never edits.
    """
    name = getattr(a, "model_package", None) or a.package
    served = (a.server_root / "publisher_data" / a.environment / name
              if a.server_root and not getattr(a, "experiment", None) else None)
    for cand in (served, a.model_dir):
        if cand and (cand / f"{name}.malloy").exists():
            return cand / f"{name}.malloy"
    # The verifier takes a package directory as well as a file.
    if a.model_dir and a.model_dir.is_dir() and any(a.model_dir.rglob("*.malloy")):
        return a.model_dir
    return None


def verify_goldens(a: argparse.Namespace, d: pathlib.Path,
                   diff: str) -> dict[str, Any]:
    """Run golden verification against the edited model."""
    # The verifier is the SHARED one in eval-answer/scripts, not a per-set copy.
    # Probing `set_dir` meant no set ever had one, so this returned
    # `{"ran": False}` on every call -- an acceptance check reporting "not
    # applicable" rather than failing, which is indistinguishable from a
    # legitimate skip. The improve loop then proceeded believing the goldens had
    # survived the edit when nothing had looked. A set may still override with
    # its own copy; that takes precedence.
    script = a.set_dir / "verify_goldens.py"
    if not script.exists():
        script = (pathlib.Path(__file__).resolve().parent.parent.parent
                  / "eval-answer" / "scripts" / "verify_goldens.py")
    # `ran: False` covers two opposite situations, so `couldNotRun` separates
    # them: a legitimate skip (no edit, so there is nothing to invalidate) may
    # proceed, while a check that SHOULD have happened and did not must block.
    # Without the split, a missing verifier or a dead truth Publisher read as
    # "not applicable" and the cluster sailed through the acceptance gate.
    if not script.exists():
        return {"ran": False, "couldNotRun": True,
                "why": f"no verify_goldens.py at {script}"}
    if not diff.strip():
        return {"ran": False, "why": "no edit to invalidate anything"}

    model = model_under_test(a)
    # Without `--model` the verifier skips its stale-rubric and name checks and
    # still exits 0, so a clean result would claim checks that never ran.
    if model is None:
        return {"ran": False, "couldNotRun": True,
                "why": f"no .malloy file in {a.model_dir}, so the stale-rubric and "
                       f"name checks cannot run against the edited model"}
    # `--set` is required by the verifier and was never passed, so argparse
    # exited 2 on every call: `clean` was false whenever there was a diff, and
    # the acceptance check below reported BLOCKED for every cluster that made an
    # edit. The gate this step is built around had therefore never once passed,
    # and its failure was indistinguishable from a golden the edit really did
    # invalidate. `--environment` too: the verifier's own default is `samples`,
    # which silently verified against the wrong environment on any other set.
    # The TRUTH server's environment, which is not the model server's. It is a
    # separate server and names its environments however it likes, so passing
    # `--environment` here 404'd every case ("Environment 'local' could not be
    # resolved to a path"), the audit exited non-zero, and the acceptance
    # check read that as "this edit may have invalidated a golden" and BLOCKED
    # every cluster. No edit could ever be accepted. `run_baseline.py` grew
    # `--truth-environment` for exactly this; this caller had not.
    cmd = [sys.executable, str(script.resolve()),
           "--set", str(a.set_dir.resolve()),
           "--environment",
           getattr(a, "truth_environment", None) or a.environment]
    # Appended only when set: passing None here is a TypeError inside
    # subprocess.run, and the two excepts below catch OSError and a timeout,
    # not that -- so it would have crashed improve.py AFTER the model edit,
    # losing the receipts. Omitted, the verifier skips the value check and
    # exits 3, which `couldNotRun` already reads correctly.
    if a.truth_publisher:
        cmd += ["--publisher", a.truth_publisher]
    # getattr: older callers and the tests build this Namespace by hand.
    if getattr(a, "definitions", None):
        cmd += ["--definitions", str(a.definitions)]
    cmd += ["--model", str(model)]
    # The isolation guard needs to know what is under test; without it the
    # guard reads `set.json`'s `targetPackage`, which nothing writes.
    if a.package:
        cmd += ["--target-package", a.package]
    try:
        # No cwd: `--set` is absolute and the verifier resolves `--cases` and
        # the gold artifacts under it, so nothing here is relative any more.
        # Running in the set dir also crashed outright when the path did not
        # exist, and it crashed AFTER the model edit, losing the receipts.
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    except subprocess.TimeoutExpired:
        return {"ran": False, "couldNotRun": True,
                "why": "verify_goldens timed out"}
    except OSError as exc:
        return {"ran": False, "couldNotRun": True,
                "why": f"could not run verify_goldens: {exc}"}
    (d / "verify_goldens.txt").write_text(p.stdout + p.stderr)
    # 0 clean, 1 a golden the edit may have invalidated, anything else the
    # verifier failing to run at all (3 is its own "could not run"; see its
    # EXIT CODES block). The third is not evidence about the goldens, and
    # reporting it as one sends someone to settle a golden that is fine. It is a
    # harness failure, says so, and still blocks: a check that did not happen is
    # not a check that passed.
    if p.returncode not in (0, 1):
        return {"ran": False, "couldNotRun": True,
                "why": f"verify_goldens could not run (exit {p.returncode}); "
                       f"see artifacts/clusters/*/verify_goldens.txt",
                "model": str(model),
                "tail": (p.stderr or p.stdout or "").strip().splitlines()[-25:]}
    out = {"ran": True, "clean": p.returncode == 0,
           "model": str(model),
           "tail": (p.stdout or p.stderr or "").strip().splitlines()[-25:]}
    if p.returncode == 1:
        # The question is whether THIS edit broke a golden. A finding the set
        # already had before the edit (a set name the model text never
        # mentions, say) is not that, and blocking on it blocked every edit on
        # such a set: the shipped storefront tour could never accept one.
        before = unedited_failures(a, cmd, model)
        if before is not None:
            new_ones = sorted(hard_failures(p.stdout) - before)
            out["preExisting"] = sorted(hard_failures(p.stdout) & before)
            out["clean"] = not new_ones
            if out["preExisting"]:
                (d / "verify_goldens.txt").write_text(
                    p.stdout + p.stderr + "\n\nAlso failing on the unedited model, so not "
                    "caused by this edit:\n" + "\n".join(f"  {f}" for f in out["preExisting"]) + "\n")
    return out


def hard_failures(text: str) -> set[str]:
    """The verifier's failing lines: each drifted or erroring golden, and each
    finding it lists under "finding(s) besides value drift"."""
    out: set[str] = set()
    in_findings = False
    for line in text.splitlines():
        if line.startswith(("  DIFF", "  ERROR")):
            out.add(" ".join(line.split()))
        if line.rstrip().endswith("finding(s) besides value drift:"):
            in_findings = True
            continue
        if in_findings:
            if not line.startswith("  "):
                in_findings = False
            else:
                out.add(line.strip())
    return out


def unedited_failures(a: argparse.Namespace, cmd: list[str],
                      model: pathlib.Path) -> set[str] | None:
    """The verifier's failures on the model as committed, before the edit, or
    None when that cannot be read (not a git checkout): then the edited
    result stands on its own, as before."""
    if not a.model_dir:
        return None
    root = pathlib.Path(a.model_dir)
    try:
        prefix = subprocess.run(["git", "rev-parse", "--show-prefix"], cwd=root,
                                capture_output=True, text=True, check=True).stdout.strip()
        tmp = pathlib.Path(tempfile.mkdtemp(prefix="unedited-"))
        tar = subprocess.run(["git", "archive", f"HEAD:{prefix}" if prefix else "HEAD"],
                             cwd=root, capture_output=True, check=True).stdout
        subprocess.run(["tar", "-x", "-C", str(tmp)], input=tar, check=True)
    except (OSError, subprocess.CalledProcessError):
        return None
    try:
        same = tmp / model.name if model.is_file() else tmp
        if not same.exists():
            return None
        i = cmd.index("--model")
        p = subprocess.run(cmd[:i + 1] + [str(same)] + cmd[i + 2:],
                           capture_output=True, text=True, timeout=600)
    except (OSError, subprocess.TimeoutExpired, ValueError):
        return None
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    return hard_failures(p.stdout) if p.returncode in (0, 1) else None


def golden_keys(cases: dict[str, Any], set_dir: pathlib.Path | None
                ) -> tuple[dict[str, Any], dict[str, list[str]], dict[str, str]]:
    """Every case's key, its golden queries and the answers its rubric and
    mustState accept, for the leak check. A key whose rows cannot be read is
    left out: there is nothing to look for."""
    keys: dict[str, Any] = {}
    queries: dict[str, list[str]] = {}
    accepted: dict[str, str] = {}
    for qid, c in cases.items():
        g = c.get("golden") or {}
        try:
            keys[qid] = golden_rows.key_value(g, set_dir, qid)
        except golden_rows.GoldenRowsError:
            pass
        # `canonicalQuery` is the field the case format defines; there is no
        # `golden.query`, so reading that left every primary query unchecked.
        qs = [g.get("canonicalQuery"), (g.get("verification") or {}).get("verifyQuery")]
        queries[qid] = [q for q in qs if isinstance(q, str)]
        accepted[qid] = golden_leak.accepted_text(g)
    return keys, queries, accepted


# `served_by`, `skipped` and `shape` come from each query's path report: what
# answered it, why existing tables did not (GRAIN_TOO_COARSE, MISSING_FIELD,
# with the server's sentence), and the grouped-by, filtered-by and measures a
# table must hold to answer it. They are what picks a rollup's grain.
REPLAY_FIELDS = ("qid", "source", "query", "givens", "modelPath", "count",
                 "median_ms", "error", "served_by", "skipped", "shape")


def cluster_members(issue: dict[str, Any], cases: dict[str, Any],
                    set_dir: pathlib.Path | None,
                    run: pathlib.Path | None = None) -> list[dict[str, Any]]:
    """What the improver is shown about each case in the cluster.

    A speed issue gets the replayed queries and their timings, read from the
    replay's results, and nothing from the case: a replay times every split,
    so its qids include test and holdout cases whose keys the improver must
    never see, and a speed edit needs the query, not the answer.

    Any other issue gets each case's question, key and rubric. The key comes
    through `golden_rows`, so a rows golden kept in `golden.path` reaches the
    improver as its rows rather than as no key. A `test` case is skipped:
    diagnose never puts one in a cluster, and this keeps its key out even if
    an issue names one.
    """
    if issue.get("component") == "performance":
        want = set(issue.get("qids", []))
        rows = (ledger.read_jsonl(run / "results.jsonl")
                if run and (run / "results.jsonl").is_file() else [])
        return [{k: r.get(k) for k in REPLAY_FIELDS}
                for r in rows if r.get("qid") in want]
    out = []
    for q in issue.get("qids", []):
        if (cases.get(q) or {}).get("split") == "test":
            continue
        g = (cases.get(q) or {}).get("golden") or {}
        out.append({"qid": q, "question": (cases.get(q) or {}).get("question"),
                    "golden": golden_rows.key_value_or_note(g, set_dir, q),
                    "rubric": g.get("rubric")})
    return out


def improve_cluster(issue: dict[str, Any], cases: dict[str, Any],
                    a: argparse.Namespace, art: pathlib.Path) -> dict[str, Any]:
    cid = issue["issue_id"]
    d = art / "clusters" / cid.replace("/", "_")
    out = d / "result.json"
    if out.exists() and not a.force:
        return {**json.loads(out.read_text()), "_cached": True}
    d.mkdir(parents=True, exist_ok=True)

    reload_cmd = RELOAD.format(rest_url=a.rest_url.rstrip("/"),
                               environment=a.environment,
                               package=a.package)
    sync = ""
    if a.experiment:
        import shlex
        reload_cmd = EXPERIMENT_SYNC.format(
            python=shlex.quote(sys.executable),
            experiment_py=shlex.quote(str(EXPERIMENT_PY)), name=shlex.quote(a.experiment),
            set_dir=shlex.quote(str(a.set_dir.resolve())),
            model_dir=shlex.quote(str(a.model_dir.resolve())))
    elif not a.watch_mode:
        served = (a.server_root / "publisher_data" / a.environment / a.package
                  if a.server_root else None)
        if served:
            sync = (f'rsync -a --delete --exclude .git '
                    f'"{a.model_dir.resolve()}/" "{served.resolve()}/"\n')
    helper = a.model_dir / SYNC_HELPER
    helper.write_text(SYNC_SCRIPT.format(sync=sync, reload=reload_cmd))
    helper.chmod(0o755)

    members = cluster_members(issue, cases, a.set_dir, getattr(a, "run", None))

    r = spawn_agent(
        IMPROVE_PROMPT.format(
            environment=a.environment, package=a.package,
            model_dir=a.model_dir.resolve(),
            # `sufficiency` travels with the cluster because it says whether
            # the diagnosis was PROBED. It used to stop at the diagnose step,
            # so an unprobed diagnosis arrived here indistinguishable from one
            # backed by evidence. This skill already requires a probe receipt
            # for every factual claim, so the honest fix is to tell the agent
            # what it is holding rather than to silently drop the cluster.
            cluster=json.dumps({k: issue.get(k) for k in
                                ("issue_id", "component", "primary_code",
                                 "contributing_codes", "owner", "severity",
                                 "sufficiency", "diagnosis", "evidence")},
                               indent=2),
            cases=json.dumps(members, indent=2)[:8000],
            unattended=UNATTENDED if getattr(a, "unattended", False) else "",
            prior=attempts.brief(attempts.relevant(a.prior, issue.get("qids", [])))[:6000],
            cases_file=(a.set_dir / "cases.jsonl").resolve()),
        skills=["eval-improve", *a.role_skills], skills_root=a.roots,
        model=a.model,
        mcp_url=a.mcp_url, tools=IMPROVE_TOOLS, cwd=a.model_dir,
        turns=a.max_turns, timeout=a.timeout, retries=a.retries,
        save_transcript=d / "improver.jsonl")
    (d / "improver.md").write_text(r.text)

    diff = candidate_diff(a.model_dir)
    (d / "edit.patch").write_text(diff)
    stat = candidate_diff(a.model_dir, ("--stat",)).strip()

    # While the edit is still in the tree: the mechanical half of Step 4. Only
    # catches drift and rubric sentences that contradict the model -- a golden
    # whose stored value silently moved is invisible here, which is why the
    # agent is asked for goldenSuspect as well.
    audit = verify_goldens(a, d, diff)
    # Two checks a script can make on the diff itself, before anything is
    # re-answered. An edit that states a case's answer passes that case for
    # nothing, and one that was already rejected would cost two runs to
    # reject again.
    leak = golden_leak.check(diff, a.keys, a.golden_queries, a.accepted)
    again = attempts.same_edit(a.prior, diff)

    helper.unlink(missing_ok=True)
    if a.isolate:
        # Back to the starting state, so the next cluster's agent measures its
        # own edit rather than the accumulation.
        subprocess.run(["git", "checkout", "--", "."], cwd=a.model_dir,
                       capture_output=True)
        subprocess.run(["git", "clean", "-fd", "--", "."], cwd=a.model_dir,
                       capture_output=True)

    res = {"issue_id": cid, "qids": issue.get("qids", []),
           "diffStat": stat, "diffEmpty": not diff.strip(),
           "goldenAudit": audit, "goldenLeak": leak,
           "duplicateOf": ({"at": again.get("at"), "what": again.get("what"),
                            "failedRules": again.get("failedRules")} if again else None),
           "patchSha": attempts.patch_sha(diff),
           "patchPath": str((d / "edit.patch").relative_to(art.parent)),
           "cost_usd": r.cost_usd, "wall_seconds": r.wall_seconds,
           "attempts": r.attempts, "agentError": r.error,
           **(r.json or {})}
    out.write_text(json.dumps(res, indent=2))
    return res


def main(argv: list[str] | None = None) -> int:
    """`run`, then take down the experiment it started, however it ended."""
    started: dict[str, Any] = {}
    try:
        return run(argv, started)
    finally:
        if started:
            import contextlib
            import io
            import experiment
            with contextlib.redirect_stdout(io.StringIO()):
                experiment.stop(started["cfg"], started["name"])


def run(argv: list[str] | None, started: dict[str, Any]) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", required=True, type=pathlib.Path)
    ap.add_argument("--set", dest="set_dir", required=True, type=pathlib.Path)
    ap.add_argument("--model-dir", type=pathlib.Path, default=None,
                    help="the model's git checkout. Default: [model] repo in "
                         "eval.toml")
    ap.add_argument("--server-root", type=pathlib.Path, default=None,
                    help="Publisher SERVER_ROOT; only needed without watch "
                         "mode. Each accepted edit is rsynced onto the copy "
                         "this root serves and reloaded, which works only "
                         "with versioning off: a root serving published "
                         "versions is refused. Default: the one "
                         "`serve.py --role model` uses, when it exists")
    ap.add_argument("--watch-mode", action="store_true",
                    help="Publisher was started with --watch-env, so edits are "
                         "live and the helper only reloads")
    ap.add_argument("--experiment", default=None, metavar="NAME",
                    help="serve the agent's edits as the experiment "
                         "<package>.<NAME> beside the main package, and have "
                         "the agent probe that. Works on a server that "
                         "publishes versions, where --watch-mode and the "
                         "sync do not, and leaves the main package alone")
    ap.add_argument("--unattended", action="store_true",
                    help="tell the agent nobody will answer a question or approve "
                         "an edit: it makes the edit, which is then measured and "
                         "left on a branch for a person to merge")
    ap.add_argument("--model", default="opus", help="the modeling agent")
    ap.add_argument("--environment", default=None)
    ap.add_argument("--package", default=None)
    ap.add_argument("--mcp-url", default=None)
    ap.add_argument("--rest-url", default=None,
                    help="REST base; the reload receipt is read from here, "
                         "because the MCP reply double-encodes its JSON. "
                         "Default: the [model] server in the set's eval.toml")
    # No built-in port: a fixed default once named the server holding the
    # model under test, which cannot verify its own goldens. The fallback is
    # the [truth] section of the set's eval.toml. With neither, the value
    # check does not happen, the verifier exits 3, and the acceptance gate
    # blocks with "did not run".
    ap.add_argument("--truth-publisher", default=None,
                    help="the Publisher serving the TRUTH package, for the "
                         "golden re-derivation after an edit. The model under "
                         "test cannot verify its own goldens, which is the "
                         "whole point of the second server. Default: the "
                         "[truth] server in the set's eval.toml. With neither, "
                         "the value check does not run and the acceptance "
                         "check blocks")
    ap.add_argument("--truth-environment", default=None,
                    help="the environment name on the TRUTH server, when it "
                         "differs from --environment. That server is separate "
                         "and names its environments independently; the wrong "
                         "name 404s every case and the acceptance check blocks "
                         "every cluster. Default: [truth] environment in the "
                         "set's eval.toml. Same flag as run_baseline.py")
    ap.add_argument("--definitions", type=pathlib.Path, default=None,
                    help="a definition ledger (verify_definitions.py). Passed to "
                         "the golden audit so a set with no truth package can "
                         "pass the acceptance check when every tested definition "
                         "is validated")
    ap.add_argument("--max-turns", type=int, default=60)
    ap.add_argument("--timeout", type=int, default=1800)
    ap.add_argument("--retries", type=int, default=1)
    ap.add_argument("--only", default=None, help="comma-separated issue_ids")
    ap.add_argument("--limit", type=int, default=None,
                    help="how many to process; each one spawns a real agent. Omit for no limit. 0 means zero, not unlimited.")
    ap.add_argument("--force", action="store_true",
                    help="redo clusters that already have a result")
    ap.add_argument("--no-isolate", dest="isolate", action="store_false",
                    help="keep each edit in the tree instead of reverting")
    ap.add_argument("--manifest", default=None,
                    help="shipped manifest whose skills the improving agent "
                         "loads, on top of skill:eval-improve. This is a "
                         "modeling task and the edit ladder's top rungs are "
                         "documentation edits, so the modeling manifest's "
                         "skills are the doctrine the edit is supposed to "
                         "follow")
    ap.add_argument("--skills-root", default=None,
                    help="checkout holding skills/ and manifests/ for the role "
                         "skills (a Publisher checkout); this checkout still "
                         "supplies the eval-* skills. Also EVAL_SKILLS_ROOT")
    ap.add_argument("--no-role-skills", action="store_true",
                    help="load only skill:eval-improve and its transitive "
                         "closure, as runs before 2026-09-01 did. That closure "
                         "is the eval-* skills alone, so the modeling doctrine "
                         "the edit ladder assumes -- malloy-gotchas-modeling "
                         "above all -- is NOT loaded")
    a = ap.parse_args(argv)
    cfg = config.load(a.set_dir)
    a.model_dir = cfg.need(a.model_dir, "model", "repo", "--model-dir")
    a.environment = cfg.need(a.environment, "model", "environment",
                             "--environment")
    a.package = cfg.need(a.package, "model", "package", "--package")
    a.mcp_url = a.mcp_url or cfg.model_mcp_url()
    a.rest_url = a.rest_url or cfg.model_publisher()
    a.truth_publisher = a.truth_publisher or cfg.truth_publisher()
    a.truth_environment = a.truth_environment or cfg.get("truth", "environment")
    a.skills_root = a.skills_root or cfg.get("paths", "skills_root")
    if not a.server_root and not a.watch_mode and cfg.server_root("model").exists():
        a.server_root = cfg.server_root("model")
    a.roots = skills_roots(a.skills_root)
    repo = a.roots[0].parent if a.skills_root else SKILLS_ROOT.parent
    if not a.manifest:
        a.manifest = default_manifest("modeling", repo)
    a.role_skills = ([] if a.no_role_skills
                     else manifest_skills(a.manifest, repo))

    if not git(["rev-parse", "--git-dir"], a.model_dir).strip():
        raise SystemExit(f"{a.model_dir} is not a git repo; isolation and the "
                         f"candidate diff both depend on it")
    dirty = git(["status", "--porcelain", "--", "."], a.model_dir).strip()
    if dirty:
        raise SystemExit(f"{a.model_dir} has uncommitted changes; commit or "
                         f"stash first so each candidate diff is only its own "
                         f"edit:\n{dirty}")
    if a.experiment:
        # The agent edits the working tree and probes the experiment, so what
        # the main package is served as does not matter: nothing writes to it.
        import experiment
        import publish
        experiment.sync(cfg, a.experiment, a.model_dir, build=False)
        started.update(cfg=cfg, name=a.experiment)
        publish.warm(cfg, a.environment, publish.experiment_package(a.package, a.experiment))
        a.model_package = a.package
        a.package = publish.experiment_package(a.package, a.experiment)
        print(f"edits are served as {a.environment}/{a.package}; the main package is untouched")
    elif not a.watch_mode and not a.server_root:
        print("warning: no --watch-mode and no --server-root, so the agent "
              "cannot sync its edit into Publisher's snapshot; every probe it "
              "runs will test the unedited model")
    # Asked in both modes: --watch-mode is the caller's claim, and a server
    # started with --watch-env over a preserved root still serves a version.
    version = None if a.experiment else served_version(a.rest_url, a.environment, a.package)
    served = (a.server_root / "publisher_data" / a.environment / a.package
              if a.server_root and not a.watch_mode and not a.experiment else None)
    if version or (served is not None and served_as_versions(served)):
        what = (f"the model server serves {a.package} as published version "
                f"{version}" if version else f"{served} holds published versions")
        why = ("so watch mode is not in effect for it" if a.watch_mode else
               "no edit synced there is ever served, and the sync would "
               "delete them")
        raise SystemExit(
            f"{what}, which is immutable: {why}. Fix: restart the model "
            f"server in watch mode, `eval.py serve model --set {a.set_dir} "
            f"--watch-env {a.environment} --reinit`, then pass --watch-mode. Or pass "
            f"--experiment <name>, which needs neither")

    events = read_jsonl(a.run / "events.jsonl")
    cases = {c["qid"]: c for c in read_jsonl(a.set_dir / "cases.jsonl")}
    a.prior = attempts.read(a.set_dir)
    a.keys, a.golden_queries, a.accepted = golden_keys(cases, a.set_dir)

    status: dict[str, str] = {}
    for e in events:
        if e.get("kind") == "issue_status":
            status[e["issue_id"]] = e["status"]
    issues = [e for e in events if e.get("kind") == "issue"
              and e.get("owner") in ("model", "package-skill")
              # An issue with no status event is open: the speed findings of
              # a query replay are written without one.
              and status.get(e["issue_id"], "open") == "open"]
    if a.only:
        want = {x.strip() for x in a.only.split(",")}
        issues = [i for i in issues if i["issue_id"] in want]
    issues.sort(key=lambda i: -len(i.get("qids", [])))
    # `is not None`: see diagnose.py's select_cases. 0 means zero issues,
    # not every issue -- each one spawns a real agent that edits the model.
    if a.limit is not None:
        issues = issues[:a.limit]
    if not issues:
        print("no open model- or package-skill-owned issues; run diagnose.py "
              "first, or every cluster is owned by retrieval, agent-skill, "
              "or dataset")
        return 0

    art = a.run / "artifacts"
    art.mkdir(parents=True, exist_ok=True)
    print(f"{len(issues)} model/package-skill clusters, agent {a.model}, "
          f"{'isolated' if a.isolate else 'cumulative'}, "
          f"{'served as an experiment' if a.experiment else 'watch mode' if a.watch_mode else 'snapshot sync'}")

    results = []
    for n, issue in enumerate(issues, 1):
        qids = issue.get("qids", [])
        print(f"\n[{n}/{len(issues)}] {issue['issue_id']}  {len(qids)} cases")
        print(f"  {(issue.get('diagnosis') or '')[:100]}")
        r = improve_cluster(issue, cases, a, art)
        results.append(r)
        if r.get("_cached"):
            print("  (cached; --force to redo)")
        if r.get("diffEmpty"):
            print(f"  no edit: "
                  f"{(r.get('disagreement') or r.get('edit') or r.get('agentError') or '?')[:90]}")
        else:
            print(f"  {str(r.get('edit'))[:90]}")
            print(f"  tier {r.get('editTier')}, {len(r.get('probes') or [])} "
                  f"probes, compiled={r.get('compiled')}, "
                  f"synced={r.get('syncedShaChanged')}")
            print(f"  {r['diffStat']}")
            aud = r.get("goldenAudit") or {}
            if aud.get("ran") and not aud.get("clean"):
                print("  ! golden verification FAILED against the edited model:")
                for line in (aud.get("tail") or [])[-6:]:
                    print(f"      {line}")
            elif aud.get("couldNotRun"):
                # Said out loud rather than left in the ledger. This used to
                # print nothing at all, so a verifier that never ran looked
                # exactly like one that passed.
                print(f"  ! golden verification DID NOT RUN, so nothing checked "
                      f"this edit: {aud.get('why')}")
                for line in (aud.get("tail") or [])[-6:]:
                    print(f"      {line}")
            for g in (r.get("goldenSuspect") or []):
                print(f"  ! golden_suspect {g.get('qid')} via {g.get('entity')}: "
                      f"{g.get('stored')} -> {g.get('rederived')}")
            for x in (r.get("goldenLeak") or []):
                print(f"  ! the edit states {x['qid']}'s answer ({x['kind']}): {x['found']}")
            if r.get("duplicateOf"):
                dup = r["duplicateOf"]
                print(f"  ! this same edit was REJECTED on {dup.get('at')} ({dup.get('what')}): "
                      + "; ".join(f"{k}: {v}" for k, v in (dup.get("failedRules") or {}).items()))

    # Keyed by issue_id so a re-run replaces its own candidates instead of
    # appending a second set. This script may be re-invoked after a crash.
    ev_path = a.run / "events.jsonl"
    done = {r["issue_id"] for r in results}
    new = [ledger.event(
        "candidate", issue_ids=[r["issue_id"]], qids=r["qids"],
        files=r.get("files") or [],
        diffSummary=r.get("diffStat") or "(no change)",
        probes=r.get("probes") or [], edit=r.get("edit"),
        editTier=r.get("editTier"), disagreement=r.get("disagreement"),
        compiled=r.get("compiled"), patchPath=r.get("patchPath"),
        meaningChanged=r.get("meaningChanged") or [],
        goldenSuspect=r.get("goldenSuspect") or [],
        goldenAudit=r.get("goldenAudit") or {},
        goldenLeak=r.get("goldenLeak") or [],
        duplicateOf=r.get("duplicateOf"),
        patchSha=r.get("patchSha"),
        improvedBy=a.model,
        at=ledger.now(),
    ) for r in results]
    ledger.replace_events(
        ev_path,
        keep=lambda e: (e.get("kind") != "candidate"
                        or not (set(e.get("issue_ids") or []) & done)),
        new=new)
    ledger.update_run(a.run, improverModel=a.model,
                      improverManifest=a.manifest if a.role_skills else None)

    edited = [r for r in results if not r.get("diffEmpty")]
    spend = sum(r.get("cost_usd") or 0 for r in results)
    print(f"\n{len(edited)}/{len(results)} clusters produced an edit; "
          f"{len(new)} candidate events in {ev_path}; cost ${spend:.2f}")

    # A rerun against a golden this edit invalidated measures nothing, so the
    # acceptance check does not get to start until a human settles each one. Reported, not
    # repaired: an improver editing its own answer key removes the only
    # independent check on the edit.
    # `couldNotRun` blocks alongside a real finding. The two are different facts
    # -- one is evidence about a golden, the other is the absence of evidence --
    # and the report below keeps them apart, but neither is a pass. Letting an
    # unrun check through was the same false-green the exit-code split fixed one
    # layer down.
    blocked = [(r, r.get("goldenSuspect") or [],
                (r.get("goldenAudit") or {}))
               for r in results]
    blocked = [(r, g, aud) for r, g, aud in blocked
               if g or (aud.get("ran") and not aud.get("clean"))
               or aud.get("couldNotRun") or r.get("goldenLeak") or r.get("duplicateOf")]
    if blocked:
        print(f"\nACCEPTANCE CHECK BLOCKED: {len(blocked)} cluster(s) either may "
              f"have invalidated a golden, were never checked, state a case's "
              f"answer, or repeat a rejected edit. Settle each "
              f"through the golden side door in skill:eval-loop, and re-run any "
              f"verification that failed to start, before re-answering.")
        for r, g, aud in blocked:
            print(f"  {r['issue_id']}")
            for x in r.get("goldenLeak") or []:
                print(f"    the edit states {x['qid']}'s answer ({x['kind']}: {x['found']}). "
                      f"Re-asking that case would pass it for nothing. Take the value out "
                      f"of the edit, or confirm it is a constant of the business and not "
                      f"the answer")
            if r.get("duplicateOf"):
                print(f"    the same edit was rejected on {r['duplicateOf'].get('at')}; "
                      f"read why in {attempts.path(a.set_dir)} before paying to reject it again")
            if aud.get("ran") and not aud.get("clean"):
                print(f"    verify_goldens failed; see "
                      f"artifacts/clusters/*/verify_goldens.txt")
            elif aud.get("couldNotRun"):
                print(f"    verify_goldens DID NOT RUN ({aud.get('why')}); this "
                      f"cluster is unverified, not clean")
            for x in g:
                print(f"    {x.get('qid')}: {x.get('entity')} "
                      f"{x.get('stored')} -> {x.get('rederived')}")
        return 2
    if a.isolate and edited:
        print(f"\nEach edit is a patch under {art}/clusters/<issue_id>/"
              "edit.patch and the tree is clean. Apply the one to check:")
        print(f"  git -C {a.model_dir} apply <patch>")
    if edited:
        targets = ",".join(q for r in edited for q in r["qids"])
        print("\nTHIS IS NOT ACCEPTED YET. The acceptance check belongs to eval-loop: "
              "re-answer with fresh agents that never saw the diagnosis, then")
        print(f"  python ../eval-loop/scripts/flip_table.py \\\n"
              f"      --a <baseline-run> --b <post-edit-run> \\\n"
              f"      --targets {targets[:120]}{'...' if len(targets) > 120 else ''} \\\n"
              f"      --noise-band <from your A/A run>")
    return 0


if __name__ == "__main__":
    sys.exit(main())
