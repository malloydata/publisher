#!/usr/bin/env python3
"""Diagnose a run's failures with agents, in two tiers. Stdlib only.

  python diagnose.py --run results/2026-08-30-sonnet --set evals/ecommerce \
      --model-dir ../malloy-samples/ecommerce

Tier 1 spawns one agent per failed case. Tier 2 spawns one agent over all of
tier 1's diagnoses to cluster them. Both load `skill:eval-diagnose` natively and
append to the run's `events.jsonl`.

WHAT THIS SCRIPT DOES NOT DO

It does not diagnose. Every judgement -- which ladder station, which cause code,
who owns it, what clusters with what -- belongs to `skill:eval-diagnose`, and
this file deliberately contains none of it. An earlier version pasted the skill
into a prompt and then restated its rules in Python around it; the two copies
drift, and the Python copy wins by accident because it is the one that runs.

So the split is: Python assembles evidence and writes events, the skill decides.
The one exception is `validate()`, which checks the agent stayed inside the
skill's vocabulary -- and even that parses the codes out of `SKILL.md` rather
than hardcoding them, so the skill stays the single source of truth and editing
it cannot silently invalidate the checker.

TWO TIERS, AND WHY NOT ONE

Diagnosis is per-case and deep: it wants the model source, the full call log for
that attempt, and freedom to probe with new queries before committing to a code.
Clustering is per-run and global: it must see every diagnosis at once to notice
that six cases share one undocumented convention. Different context shapes, and
one agent doing both either truncates the per-case evidence or clusters from
summaries of summaries. Tier 1 also parallelises; tier 2 cannot.

WHY THE AGENT MAY READ THE MODEL

The answerer in `run_baseline.py` is isolated because a case is only evidence
about the model if the answer came through the model. Diagnosis measures
nothing, so the same isolation would only blind it. It gets Read over the model
directory and the Publisher MCP tools because the skill requires probing --
"search a distinctive phrase from the entity's own doc", "probe the claim before
writing the issue" -- and a diagnosis that cannot run a query is guessing at
exactly the point the skill says not to. It still may not edit.
"""
from __future__ import annotations

import argparse
import concurrent.futures as futures
import json
import pathlib
import re
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
DIAGNOSE_TOOLS = ("mcp__publisher__malloy_getContext",
                  "mcp__publisher__malloy_executeQuery",
                  "mcp__publisher__malloy_compile",
                  "Read", "Grep", "Glob")
# The platform target: a hosted MCP server. Its NAME has to match the one
# run_baseline.py's answerer used, because that name is the OAuth cache key --
# probe through a differently-named server and this agent finds no token.
HOSTED_DIAGNOSE_TOOLS = ("get_context", "execute_query", "search_malloy_docs")


def hosted_diagnose_tools(server: str) -> tuple[str, ...]:
    return tuple(f"mcp__{server}__{t}" for t in HOSTED_DIAGNOSE_TOOLS) + (
        "Read", "Grep", "Glob")
NO_EDITS = ("Edit", "Write", "NotebookEdit")

# Codes are written in the skill's tables as a backticked SHOUTY-KEBAB token in
# the first column. Parsed, never copied: a hardcoded list silently stops
# matching the day someone edits the skill, and then the checker passes
# vocabulary the skill no longer defines.
CODE_IN_TABLE = re.compile(r"^\|\s*`([A-Z][A-Z-]+)`\s*\|", re.M)
COMPONENTS = ("dataset", "agent-call", "get_context/model",
              "get_context/retrieval", "construction", "model-definition")
OWNERS = ("model", "retrieval", "agent-skill", "dataset")
SUFFICIENCY = ("sufficient", "insufficient", "unknown")




def skill_codes() -> set[str]:
    text = (SKILLS_ROOT / "eval-diagnose" / "SKILL.md").read_text()
    codes = set(CODE_IN_TABLE.findall(text))
    if not codes:
        raise SystemExit("parsed zero cause codes from eval-diagnose/SKILL.md; "
                         "the table format changed and the checker would "
                         "silently accept anything")
    return codes


def evidence_for(qid: str, case: dict[str, Any],
                 events: list[dict[str, Any]]) -> dict[str, Any]:
    """Everything the ladder's Step 1 asks for: asked, returned, used.

    Assembled here rather than letting the agent read the ledger, for two
    reasons: `cases.jsonl` carries the goldens for every OTHER case, and a
    diagnosing agent that browses it has seen answers it will later be asked
    to judge the model on.
    """
    attempt = next((e for e in events
                    if e.get("kind") == "attempt" and e.get("qid") == qid), {})
    score = next((e for e in events
                  if e.get("kind") == "score" and e.get("qid") == qid), {})
    calls = [e for e in events
             if e.get("kind") == "tool_call" and e.get("qid") == qid]

    asked = []
    for c in calls:
        if c.get("tool") != "get_context":
            continue
        summary = c.get("rankedSummary") or {}
        asked.append({"targets": c.get("targets"),
                      "returnedInRankOrder": summary.get("entityIds") or [],
                      "resultCount": summary.get("resultCount"),
                      "error": c.get("error")})

    golden = case.get("golden") or {}
    return {
        "qid": qid,
        "question": case.get("question"),
        "golden": {"kind": golden.get("kind"), "value": golden.get("value"),
                   "rubric": golden.get("rubric"),
                   "status": golden.get("status")},
        "coverage": case.get("coverage") or golden.get("coverage"),
        "expectedEntities": case.get("expectedEntities"),
        "modelQuery": case.get("modelQuery"),
        "verdict": score.get("verdict"),
        "judgeReasoning": score.get("reason"),
        "judgeConfidence": score.get("confidence"),
        "answerText": (attempt.get("answer_text") or "")[:4000],
        "queriesRun": attempt.get("queries") or
        ([attempt["final_query"]] if attempt.get("final_query") else []),
        "getContextCalls": asked,
        "nGetContext": attempt.get("n_get_context"),
        "nExecute": attempt.get("n_execute"),
        "nExecuteErrors": attempt.get("n_execute_errors"),
        "runError": attempt.get("run_error"),
        "retrievalMode": attempt.get("retrieval_mode"),
    }


# Thin on purpose. The skill says how to diagnose; this says what to look at and
# what shape to answer in. Anything here that explains the ladder is a second
# copy of the skill, so there is nothing here that explains the ladder.
DIAGNOSE_PROMPT = """Apply the eval-diagnose skill to ONE failed case.

Environment: {environment}
Package: {package}

The model source is the current directory; read it. The answerer could not.
You have the {tools_name} MCP tools, and the skill requires probing before you
commit to a code -- use them. You may not edit anything.{scope_line}

EVIDENCE FROM THE RUN
{evidence}

In `getContextCalls`, `targets` is what the agent searched for and
`returnedInRankOrder` is what came back, in rank order.

Output ONLY a JSON object as the last thing in your reply:

{{"probes": [{{"why": "the claim this checks", "query": "query or search text",
             "result": "what came back, briefly"}}],
  "reasoning": "how the ladder resolved: what you ruled out, and why",
  "component": "one of the six",
  "primary_code": "one code, verbatim from the skill",
  "contributing_codes": ["zero or more, verbatim"],
  "owner": "model | retrieval | agent-skill | dataset",
  "sufficiency": "sufficient | insufficient | unknown",
  "severity": "high | medium | low",
  "confidence": "high | medium | low",
  "diagnosis": "the suspected entity, file, or root cause, in one or two sentences",
  "sharedWith": "a short phrase naming what other cases would share this cause"}}

`probes` must be non-empty: it is the record that you checked rather than
assumed. `reasoning` precedes the codes because the codes must follow from it."""


def diagnose_one(qid: str, case: dict[str, Any], events: list[dict[str, Any]],
                 a: argparse.Namespace, art: pathlib.Path) -> dict[str, Any]:
    d = art / qid
    out = d / "diagnosis.json"
    if out.exists() and not a.force:
        return {**json.loads(out.read_text()), "qid": qid, "_cached": True}

    platform = a.target == "platform"
    scope_line = ""
    if platform and a.scope:
        env, pkg = a.scope.split("/", 1)
        scope_line = (f"\nThe run under diagnosis was scoped to environment "
                      f'"{env}", package "{pkg}". Pass scopes=[{{"environment": '
                      f'"{env}", "package": "{pkg}"}}] on get_context and '
                      f'environment="{env}", package="{pkg}" on execute_query, '
                      f"so your probes hit the same model the answerer did.")
    r = spawn_agent(
        DIAGNOSE_PROMPT.format(
            environment=a.environment, package=a.package,
            tools_name="the hosted platform" if platform else "Publisher",
            scope_line=scope_line,
            evidence=json.dumps(evidence_for(qid, case, events),
                                indent=2)[:14000]),
        skills=["eval-diagnose", *a.role_skills], skills_root=a.roots,
        model=a.model,
        mcp_url=a.mcp_url,
        tools=hosted_diagnose_tools(a.hosted_mcp_server) if platform
        else DIAGNOSE_TOOLS,
        blocked=NO_EDITS,
        cwd=a.model_dir, turns=a.max_turns, timeout=a.timeout,
        retries=a.retries, save_transcript=d / "diagnosis.jsonl",
        mcp_server=a.hosted_mcp_server if platform else "publisher")

    d.mkdir(parents=True, exist_ok=True)
    (d / "diagnosis.md").write_text(r.text)
    if r.json is None:
        return {"qid": qid, "error": r.error or "unparseable",
                "cost_usd": r.cost_usd}
    obj = {**r.json, "qid": qid, "cost_usd": r.cost_usd,
           "wall_seconds": r.wall_seconds, "attempts": r.attempts}
    out.write_text(json.dumps(obj, indent=2))
    return obj


def validate(obj: dict[str, Any], codes: set[str]) -> list[str]:
    """Did the agent stay in the skill's vocabulary? Not whether it was right."""
    bad = []
    if obj.get("component") not in COMPONENTS:
        bad.append(f"component={obj.get('component')!r}")
    if obj.get("owner") not in OWNERS:
        bad.append(f"owner={obj.get('owner')!r}")
    if obj.get("sufficiency") not in SUFFICIENCY:
        bad.append(f"sufficiency={obj.get('sufficiency')!r}")
    if obj.get("primary_code") not in codes:
        bad.append(f"primary_code={obj.get('primary_code')!r}")
    if not obj.get("probes"):
        bad.append("no probes recorded")
    return bad


CLUSTER_PROMPT = """Apply Step 5 of the eval-diagnose skill across a whole run.

These are the per-case diagnoses from one run. Cluster them.

DIAGNOSED ISSUES
{issues}

Output ONLY a JSON object as the last thing in your reply:

{{"clusters": [
  {{"cluster_id": "short-kebab-slug",
    "qids": ["every case in this cluster"],
    "owner": "model | retrieval | agent-skill | dataset",
    "component": "the shared component",
    "codes": ["the primary codes present"],
    "rootCause": "one or two sentences: the ONE thing explaining all of them",
    "evidence": "why these belong together, and what would prove it wrong",
    "confidence": "high | medium | low"}}
 ],
 "reasoning": "what you considered merging and chose not to, and why"}}

Order clusters by the number of qids, descending. Every diagnosed case must
appear in exactly one cluster; a case that shares a cause with nothing else is
a cluster of one."""


def cluster(issues: list[dict[str, Any]], a: argparse.Namespace,
            out: pathlib.Path) -> dict[str, Any]:
    keep = ("qid", "component", "primary_code", "contributing_codes", "owner",
            "sufficiency", "severity", "diagnosis", "sharedWith")
    compact = [{k: v for k, v in i.items() if k in keep} for i in issues]
    r = spawn_agent(
        CLUSTER_PROMPT.format(issues=json.dumps(compact, indent=2)),
        skills=["eval-diagnose", *a.role_skills], skills_root=a.roots,
        model=a.cluster_model,
        mcp_url=None, blocked=NO_EDITS, turns=8, timeout=a.timeout,
        retries=a.retries, save_transcript=out / "clustering.jsonl")
    (out / "clustering.md").write_text(r.text)
    res = r.json or {"clusters": [], "reasoning": r.error or "unparseable"}
    res["_cost_usd"] = r.cost_usd
    return res


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", required=True, type=pathlib.Path)
    ap.add_argument("--set", dest="set_dir", required=True, type=pathlib.Path)
    ap.add_argument("--model-dir", type=pathlib.Path, default=None,
                    help="the Malloy package under test; the agent's cwd")
    ap.add_argument("--model", default="sonnet")
    ap.add_argument("--cluster-model", default="opus")
    ap.add_argument("--environment", default="samples")
    ap.add_argument("--package", default="ecommerce")
    ap.add_argument("--mcp-url", default="http://localhost:4040/mcp")
    ap.add_argument("--parallel", type=int, default=4)
    ap.add_argument("--max-turns", type=int, default=40)
    ap.add_argument("--timeout", type=int, default=900)
    ap.add_argument("--retries", type=int, default=2)
    ap.add_argument("--only", default=None, help="comma-separated qids")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--no-cluster", action="store_true")
    ap.add_argument("--target", choices=("local", "platform"), default="local",
                    help="platform: probe through a hosted MCP server "
                         "(cached OAuth) instead of a local Publisher")
    ap.add_argument("--hosted-mcp-server", default="hosted",
                    help="platform only: the MCP server name. Must match the "
                         "one the run's answerer used -- it is the OAuth cache "
                         "key and the `mcp__<server>__<tool>` prefix")
    ap.add_argument("--scope", default=None, metavar="ENV/PACKAGE",
                    help="platform only: the package the run was scoped to, "
                         "so probes hit the same model")
    ap.add_argument("--force", action="store_true",
                    help="re-diagnose cases that already have a diagnosis")
    ap.add_argument("--manifest", default=None,
                    help="shipped manifest whose skills the diagnosing agents "
                         "load, on top of skill:eval-diagnose. Modeling by "
                         "default: deciding that a doc is ambiguous or a "
                         "measure is missing needs the vocabulary of a good "
                         "model, and without it diagnosis is biased away from "
                         "model-owned causes. Deliberately NOT the answerer's "
                         "manifest -- a diagnoser fluent in the answerer's own "
                         "playbook over-attributes to query construction")
    ap.add_argument("--skills-root", default=None,
                    help="checkout holding skills/ and manifests/ for the role "
                         "skills (a Publisher checkout); this checkout still "
                         "supplies the eval-* skills. Also EVAL_SKILLS_ROOT")
    ap.add_argument("--no-role-skills", action="store_true",
                    help="load only skill:eval-diagnose, as runs before "
                         "2026-09-01 did")
    a = ap.parse_args(argv)
    a.roots = skills_roots(a.skills_root)
    repo = a.roots[0].parent if a.skills_root else SKILLS_ROOT.parent
    if not a.manifest:
        a.manifest = default_manifest("modeling", repo)
    a.role_skills = ([] if a.no_role_skills
                     else manifest_skills(a.manifest, repo))

    events = read_jsonl(a.run / "events.jsonl")
    cases = {c["qid"]: c for c in read_jsonl(a.set_dir / "cases.jsonl")}
    codes = skill_codes()

    # Dev failures only. A holdout case the improve step never saw is the only
    # thing that makes the gate mean anything, and a diagnosis describes the fix.
    failed = []
    for e in events:
        if e.get("kind") != "score" or e.get("verdict") != "no_match":
            continue
        case = cases.get(e["qid"])
        if case is None or case.get("split") == "holdout":
            continue
        if e.get("contaminated") == "true":
            continue
        failed.append(e["qid"])
    failed = list(dict.fromkeys(failed))

    if a.only:
        want = {q.strip() for q in a.only.split(",")}
        failed = [q for q in failed if q in want]
    if a.limit:
        failed = failed[:a.limit]
    if not failed:
        print("no diagnosable failures in this run")
        return 0

    art = a.run / "artifacts"
    art.mkdir(parents=True, exist_ok=True)
    print(f"tier 1: {len(failed)} failed dev cases, {a.model}, "
          f"{a.parallel} at a time  ({len(codes)} codes in the skill)")

    issues: list[dict[str, Any]] = []
    with futures.ThreadPoolExecutor(max_workers=a.parallel) as ex:
        fut = {ex.submit(diagnose_one, q, cases[q], events, a, art): q
               for q in failed}
        for i, f in enumerate(futures.as_completed(fut), 1):
            q = fut[f]
            try:
                obj = f.result()
            except Exception as exc:
                obj = {"qid": q, "error": f"{type(exc).__name__}: {exc}"[:200]}
            issues.append(obj)
            if obj.get("error"):
                print(f"  [{i}/{len(failed)}] ! {q} {obj['error']}", flush=True)
            else:
                obj["_invalid"] = validate(obj, codes)
                tag = "=" if obj.get("_cached") else ("!" if obj["_invalid"] else ".")
                print(f"  [{i}/{len(failed)}] {tag} {q} "
                      f"{obj.get('component')}/{obj.get('primary_code')} "
                      f"-> {obj.get('owner')}"
                      + (f"  [{'; '.join(obj['_invalid'])}]"
                         if obj["_invalid"] else ""), flush=True)

    good = [i for i in issues if not i.get("error")]
    clusters: dict[str, Any] = {"clusters": []}
    if good and not a.no_cluster:
        print(f"\ntier 2: clustering {len(good)} diagnoses with {a.cluster_model}")
        clusters = cluster(good, a, a.run)
        for c in clusters.get("clusters", []):
            print(f"  {len(c.get('qids', [])):>2} cases  {c.get('cluster_id')} "
                  f"({c.get('owner')})  {(c.get('rootCause') or '')[:66]}")

    # One issue per cluster: the cluster is the unit of work, so a per-case
    # issue would enter the same root cause into the backlog six times.
    # `issue_id` is derived from the run and the cluster, never a timestamp, so
    # re-running this script overwrites its own issues instead of duplicating
    # them -- which matters because it appends to a file it may append to again.
    by_qid = {i["qid"]: i for i in good}
    new: list[dict[str, Any]] = []
    for n, c in enumerate(clusters.get("clusters", []), 1):
        qids = [q for q in c.get("qids", []) if q in by_qid]
        if not qids:
            continue
        members = [by_qid[q] for q in qids]
        codes_seen = [m.get("primary_code") for m in members]
        issue_id = f"{a.run.name}:{c.get('cluster_id') or f'cluster-{n}'}"
        new.append(ledger.event(
            "issue", issue_id=issue_id, qids=qids,
            primary_code=max(set(codes_seen), key=codes_seen.count),
            contributing_codes=sorted(
                {x for m in members
                 for x in (m.get("contributing_codes") or [])}
                | set(codes_seen)),
            component=c.get("component"), owner=c.get("owner"),
            severity=max((m.get("severity") or "low" for m in members),
                         key=["low", "medium", "high"].index),
            confidence=c.get("confidence") or "medium",
            sufficiency=members[0].get("sufficiency") or "unknown",
            traceIds=[], diagnosis=c.get("rootCause"),
            evidence=c.get("evidence"),
            diagnosedBy=a.model, clusteredBy=a.cluster_model,
        ))
        new.append(ledger.event("issue_status", issue_id=issue_id,
                                status="open", at=ledger.now()))

    ledger.replace_events(
        a.run / "events.jsonl",
        keep=lambda e: (e.get("kind") not in ("issue", "issue_status")
                        or not str(e.get("issue_id", ""))
                        .startswith(f"{a.run.name}:")),
        new=new)
    ledger.update_run(a.run, diagnoserModel=a.model,
                      diagnoserManifest=a.manifest if a.role_skills else None)
    (a.run / "diagnoses.jsonl").write_text(
        "".join(json.dumps(i) + "\n" for i in issues))

    # The agent's clusters are the clusters of record, in the file the package
    # builder reads. Until 2026-09-02 only cluster_failures.py wrote this file,
    # so the browsable package always carried the mechanical grouping -- which
    # on both the ecommerce and VideoAmp runs charged everything to retrieval
    # while the diagnosis beside it said otherwise.
    where = {"model": "model coverage", "retrieval": "retrieval ranking",
             "agent-skill": "query construction", "dataset": "dataset"}
    lever = {"model": "model", "retrieval": "retrieval", "agent-skill": "skill",
             "dataset": "dataset"}
    with (a.run / "clusters.jsonl").open("w") as fh:
        for n, c in enumerate(clusters.get("clusters", []), 1):
            qids = [q for q in c.get("qids", []) if q in by_qid]
            if not qids:
                continue
            fh.write(json.dumps({
                "clusterId": c.get("cluster_id") or f"cluster-{n}",
                "label": c.get("rootCause") or c.get("cluster_id"),
                "whereToFix": where.get(c.get("owner"), c.get("owner")),
                "component": c.get("component"), "owner": c.get("owner"),
                "lever": lever.get(c.get("owner")),
                "qids": qids,
                "evidence": c.get("evidence"),
                "codes": c.get("codes") or [],
                "proposedEdit": "", "confidence": c.get("confidence"),
                "source": "diagnose",
            }) + "\n")
    ev_path = a.run / "events.jsonl"

    invalid = [i for i in good if i.get("_invalid")]
    spend = sum(i.get("cost_usd") or 0 for i in issues) + \
        (clusters.get("_cost_usd") or 0)
    print(f"\n{len(good)}/{len(failed)} diagnosed, "
          f"{len(clusters.get('clusters', []))} clusters, "
          f"{len(new) // 2} issues in {ev_path}")
    if invalid:
        print(f"{len(invalid)} broke the skill's vocabulary "
              f"(see `_invalid` in diagnoses.jsonl)")
    actionable = sum(len(c.get("qids", []))
                     for c in clusters.get("clusters", [])
                     if c.get("owner") == "model")
    print(f"{actionable} cases sit behind a model-owned cluster "
          f"(the only ones eval-improve may touch)")
    print(f"cost ${spend:.2f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
