#!/usr/bin/env python3
"""Spawn one modeling agent per diagnosed cluster to edit the model. Stdlib only.

  python improve.py --run results/2026-08-30-sonnet --set evals/ecommerce \
      --model-dir ../malloy-samples/ecommerce --watch-mode

One agent per `owner: model` cluster, each holding `skill:eval-improve`, each
producing at most one smallest edit with probe receipts. Appends one `candidate`
event per cluster to the run's `events.jsonl`.

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

Publisher normally serves a snapshot copy under `publisher_data/<env>/<pkg>/`,
so editing the repo and reloading recompiles the unchanged copy: the reload
succeeds, nothing changes, and the verification probe quietly tests the old
model. `--watch-mode` says Publisher was started with `--watch-env`, which
symlinks local-dir packages in place and makes edits live; then the helper is
just a reload. Without it the helper syncs first. Either way the agent gets one
generated script rather than a general shell.
"""
from __future__ import annotations

import argparse
import json
import pathlib
import subprocess
import sys
import time
from typing import Any

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent.parent
                       / "eval-loop" / "scripts"))
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent.parent
                       / "eval-answer" / "scripts"))
from agent_harness import default_manifest, manifest_skills, skills_roots, spawn_agent  # noqa: E402
import ledger  # noqa: E402
from ledger import read_jsonl  # noqa: E402

SKILLS_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
IMPROVE_TOOLS = ("mcp__publisher__get_context",
                 "mcp__publisher__execute_query",
                 "mcp__publisher__compile_model",
                 "mcp__publisher__reload_package",
                 "Read", "Edit", "Write", "Grep", "Glob",
                 "Bash(bash ./sync_and_reload.sh)")

RELOAD = """curl -s -m 60 -X POST "{mcp_url}" \\
  -H "Content-Type: application/json" \\
  -H "Accept: application/json, text/event-stream" \\
  -d '{{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{{"name":"reload_package","arguments":{{"environmentName":"{environment}","packageName":"{package}"}}}}}}' \\
  | grep -o '"sourceContentSha":"[^"]*"' | head -1"""

SYNC_SCRIPT = """#!/bin/bash
set -euo pipefail
{sync}{reload}
echo "reloaded; if sourceContentSha did not change, the edit did not reach the server"
"""


def git(args: list[str], cwd: pathlib.Path) -> str:
    return subprocess.run(["git", *args], cwd=cwd, capture_output=True,
                          text=True).stdout


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

MAKING YOUR EDIT REACH THE SERVER

After each edit run `bash ./sync_and_reload.sh`. It prints `sourceContentSha`.
If that value does not change, your edit did NOT reach the server and every
probe you run afterwards is testing the old model.

One edit for the cluster's shared root cause, not one per case.

WHEN YOU ARE DONE

Give the report block, then emit the JSON object as the LAST thing in your
reply, with nothing after it. Both shapes, and the golden re-derivation you run
BEFORE either, are defined in `reference/output-contract.md` of the eval-improve
skill -- read that file; a shape invented here is dropped.

A reply that ends in prose is a lost result. The edit still happened and the
receipts -- probes, edit tier, golden-suspect list -- are gone, so the
acceptance check has nothing to weigh.
"""


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
    if not script.exists():
        return {"ran": False, "why": f"no verify_goldens.py at {script}"}
    if not diff.strip():
        return {"ran": False, "why": "no edit to invalidate anything"}

    model = None
    for cand in (a.server_root / "publisher_data" / a.environment / a.package
                 if a.server_root else None, a.model_dir):
        if cand and (cand / f"{a.package}.malloy").exists():
            model = cand / f"{a.package}.malloy"
            break
    cmd = [sys.executable, str(script.resolve())]
    if model:
        cmd += ["--model", str(model)]
    try:
        p = subprocess.run(cmd, cwd=a.set_dir, capture_output=True, text=True,
                           timeout=600)
    except subprocess.TimeoutExpired:
        return {"ran": False, "why": "verify_goldens timed out"}
    (d / "verify_goldens.txt").write_text(p.stdout + p.stderr)
    return {"ran": True, "clean": p.returncode == 0,
            "model": str(model) if model else None,
            "tail": (p.stdout or p.stderr or "").strip().splitlines()[-25:]}


def improve_cluster(issue: dict[str, Any], cases: dict[str, Any],
                    a: argparse.Namespace, art: pathlib.Path) -> dict[str, Any]:
    cid = issue["issue_id"]
    d = art / "clusters" / cid.replace("/", "_")
    out = d / "result.json"
    if out.exists() and not a.force:
        return {**json.loads(out.read_text()), "_cached": True}
    d.mkdir(parents=True, exist_ok=True)

    reload_cmd = RELOAD.format(mcp_url=a.mcp_url, environment=a.environment,
                               package=a.package)
    sync = ""
    if not a.watch_mode:
        served = (a.server_root / "publisher_data" / a.environment / a.package
                  if a.server_root else None)
        if served:
            sync = (f'rsync -a --delete --exclude .git '
                    f'"{a.model_dir.resolve()}/" "{served.resolve()}/"\n')
    helper = a.model_dir / "sync_and_reload.sh"
    helper.write_text(SYNC_SCRIPT.format(sync=sync, reload=reload_cmd))
    helper.chmod(0o755)

    members = [{"qid": q, "question": (cases.get(q) or {}).get("question"),
                "golden": ((cases.get(q) or {}).get("golden") or {}).get("value"),
                "rubric": ((cases.get(q) or {}).get("golden") or {}).get("rubric")}
               for q in issue.get("qids", [])]

    r = spawn_agent(
        IMPROVE_PROMPT.format(
            environment=a.environment, package=a.package,
            model_dir=a.model_dir.resolve(),
            cluster=json.dumps({k: issue.get(k) for k in
                                ("issue_id", "component", "primary_code",
                                 "contributing_codes", "owner", "severity",
                                 "diagnosis", "evidence")}, indent=2),
            cases=json.dumps(members, indent=2)[:8000],
            cases_file=(a.set_dir / "cases.jsonl").resolve()),
        skills=["eval-improve", *a.role_skills], skills_root=a.roots,
        model=a.model,
        mcp_url=a.mcp_url, tools=IMPROVE_TOOLS, cwd=a.model_dir,
        turns=a.max_turns, timeout=a.timeout, retries=a.retries,
        save_transcript=d / "improver.jsonl")
    (d / "improver.md").write_text(r.text)

    diff = git(["diff", "--", "."], a.model_dir)
    (d / "edit.patch").write_text(diff)
    stat = git(["diff", "--stat", "--", "."], a.model_dir).strip()

    # While the edit is still in the tree: the mechanical half of Step 4. Only
    # catches drift and rubric sentences that contradict the model -- a golden
    # whose stored value silently moved is invisible here, which is why the
    # agent is asked for goldenSuspect as well.
    audit = verify_goldens(a, d, diff)

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
           "goldenAudit": audit,
           "patchPath": str((d / "edit.patch").relative_to(art.parent)),
           "cost_usd": r.cost_usd, "wall_seconds": r.wall_seconds,
           "attempts": r.attempts, "agentError": r.error,
           **(r.json or {})}
    out.write_text(json.dumps(res, indent=2))
    return res


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", required=True, type=pathlib.Path)
    ap.add_argument("--set", dest="set_dir", required=True, type=pathlib.Path)
    ap.add_argument("--model-dir", required=True, type=pathlib.Path)
    ap.add_argument("--server-root", type=pathlib.Path, default=None,
                    help="Publisher SERVER_ROOT; only needed without watch mode")
    ap.add_argument("--watch-mode", action="store_true",
                    help="Publisher was started with --watch-env, so edits are "
                         "live and the helper only reloads")
    ap.add_argument("--model", default="opus", help="the modeling agent")
    ap.add_argument("--environment", default="samples")
    ap.add_argument("--package", default="ecommerce")
    ap.add_argument("--mcp-url", default="http://localhost:4040/mcp")
    ap.add_argument("--max-turns", type=int, default=60)
    ap.add_argument("--timeout", type=int, default=1800)
    ap.add_argument("--retries", type=int, default=1)
    ap.add_argument("--only", default=None, help="comma-separated issue_ids")
    ap.add_argument("--limit", type=int, default=None)
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
    if not a.watch_mode and not a.server_root:
        print("warning: no --watch-mode and no --server-root, so the agent "
              "cannot sync its edit into Publisher's snapshot; every probe it "
              "runs will test the unedited model")

    events = read_jsonl(a.run / "events.jsonl")
    cases = {c["qid"]: c for c in read_jsonl(a.set_dir / "cases.jsonl")}

    status: dict[str, str] = {}
    for e in events:
        if e.get("kind") == "issue_status":
            status[e["issue_id"]] = e["status"]
    issues = [e for e in events if e.get("kind") == "issue"
              and e.get("owner") == "model"
              and status.get(e["issue_id"]) == "open"]
    if a.only:
        want = {x.strip() for x in a.only.split(",")}
        issues = [i for i in issues if i["issue_id"] in want]
    issues.sort(key=lambda i: -len(i.get("qids", [])))
    if a.limit:
        issues = issues[:a.limit]
    if not issues:
        print("no open model-owned issues; run diagnose.py first, or every "
              "cluster is owned by retrieval, agent-skill, or dataset")
        return 0

    art = a.run / "artifacts"
    art.mkdir(parents=True, exist_ok=True)
    print(f"{len(issues)} model-owned clusters, agent {a.model}, "
          f"{'isolated' if a.isolate else 'cumulative'}, "
          f"{'watch mode' if a.watch_mode else 'snapshot sync'}")

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
            for g in (r.get("goldenSuspect") or []):
                print(f"  ! golden_suspect {g.get('qid')} via {g.get('entity')}: "
                      f"{g.get('stored')} -> {g.get('rederived')}")

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
    blocked = [(r, r.get("goldenSuspect") or [],
                (r.get("goldenAudit") or {}))
               for r in results]
    blocked = [(r, g, aud) for r, g, aud in blocked
               if g or (aud.get("ran") and not aud.get("clean"))]
    if blocked:
        print(f"\nACCEPTANCE CHECK BLOCKED: {len(blocked)} cluster(s) may have invalidated a "
              f"golden. Settle each through the golden side door in "
              f"skill:eval-loop before re-answering.")
        for r, g, aud in blocked:
            print(f"  {r['issue_id']}")
            if aud.get("ran") and not aud.get("clean"):
                print(f"    verify_goldens failed; see "
                      f"artifacts/clusters/*/verify_goldens.txt")
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
