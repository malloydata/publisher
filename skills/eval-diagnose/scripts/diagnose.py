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
from collections.abc import Iterable
from typing import Any

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent.parent
                       / "eval-loop" / "scripts"))
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent.parent
                       / "eval-answer" / "scripts"))
from agent_harness import (NO_EDITS, NO_SHELL, default_manifest,  # noqa: E402
                           manifest_skills, skills_roots, spawn_agent)
import ledger  # noqa: E402
import cluster_failures  # noqa: E402
import score_retrieval  # noqa: E402
from ledger import read_jsonl  # noqa: E402

SKILLS_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
DIAGNOSE_TOOLS = ("mcp__publisher__get_context",
                  "mcp__publisher__execute_query",
                  "mcp__publisher__compile_model",
                  "Read", "Grep", "Glob")
# The platform target: a hosted MCP server. Its NAME has to match the one
# run_baseline.py's answerer used, because that name is the OAuth cache key --
# probe through a differently-named server and this agent finds no token.
HOSTED_DIAGNOSE_TOOLS = ("get_context", "execute_query", "search_malloy_docs")


def hosted_diagnose_tools(server: str) -> tuple[str, ...]:
    return tuple(f"mcp__{server}__{t}" for t in HOSTED_DIAGNOSE_TOOLS) + (
        "Read", "Grep", "Glob")
# `NO_EDITS` and `NO_SHELL` come from agent_harness; neither agent here
# writes or runs anything, and blocking the edit tools while leaving
# `Bash` granted only looks like a fence.
READ_ONLY = (*NO_EDITS, *NO_SHELL)

# Codes are written in the skill's tables as a backticked SHOUTY-KEBAB token in
# the first column. Parsed, never copied: a hardcoded list silently stops
# matching the day someone edits the skill, and then the checker passes
# vocabulary the skill no longer defines.
# `A-Z_-`: RULE_UNWRITTEN carries an underscore, and a class of code the
# regex cannot see is a code the validator silently rejects.
CODE_IN_TABLE = re.compile(r"^\|\s*`([A-Z][A-Z_-]+)`\s*\|", re.M)
COMPONENTS = ("dataset", "agent-call", "get_context/model",
              "get_context/retrieval", "construction", "model-definition")
OWNERS = ("model", "retrieval", "agent-skill", "dataset")
SUFFICIENCY = ("sufficient", "insufficient", "unknown")
SEVERITY = ("low", "medium", "high")


def worst(values: Iterable[str | None], vocab: tuple[str, ...],
          fallback: str) -> str:
    """The last-listed value present, with anything off-vocabulary read as
    `fallback`.

    `validate()` says outright that these fields can come back wrong -- it
    appends them to `bad` -- but `bad` only sets `_invalid`, and the clustering
    input is filtered on `error`, so an off-vocabulary value reaches the
    aggregate. A bare `[...].index` then raises, after every per-case call AND
    the clustering call have been paid for and before a single event is
    written, so one agent typing "partial" costs the whole diagnose run.

    The fallback is per field rather than "treat it as the worst". An
    unreadable `sufficiency` must not read as probed, and `unknown` is that.
    An unreadable `severity` is not evidence of a high one, so it takes the
    same `low` the missing-value default already takes.
    """
    ranked = [v if v in vocab else fallback for v in values]
    return max(ranked, key=vocab.index) if ranked else fallback


def skill_codes() -> set[str]:
    text = (SKILLS_ROOT / "eval-diagnose" / "SKILL.md").read_text()
    codes = set(CODE_IN_TABLE.findall(text))
    if not codes:
        raise SystemExit("parsed zero cause codes from eval-diagnose/SKILL.md; "
                         "the table format changed and the checker would "
                         "silently accept anything")
    return codes


def evidence_for(qid: str, case: dict[str, Any],
                 events: list[dict[str, Any]],
                 passed_elsewhere: dict[str, Any] | None = None) -> dict[str, Any]:
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
                      # Without this a narrow scope is invisible, and a miss it
                      # fully explains reads as a retrieval failure. One
                      # diagnosis asserted "a single unscoped get_context call"
                      # about a call scoped to one source, and charged the miss
                      # to retrieval on that premise.
                      "scopes": c.get("scopes"),
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
        # The matched pair, when another arm of the SAME model answered this
        # question correctly. Same question, same model, one right answer and
        # one wrong one: the diff between the two queries isolates the cause,
        # and it is the cheapest evidence in the whole run. Absent unless
        # --compare-run named an arm.
        "passedInAnotherArm": passed_elsewhere,
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

In `getContextCalls`, `targets` is what the agent searched for, `scopes` is
the scope it searched UNDER, and `returnedInRankOrder` is what came back.

**If `passedInAnotherArm` is present, start there.** Another run of the SAME
model answered this question correctly. That is a matched pair: same question,
same model, one right answer and one wrong one, and the diff between the two
queries usually names the cause outright. Read `finalQuery` there against
`queriesRun` here and say what the passing arm did differently.

A case that flips is not noise to be averaged away. It is a case where the
agent found two paths and the model did not make one of them obviously right,
which is a MODEL-quality finding: the fix is usually a doc or a name that
makes the wrong path unattractive, not a change to the agent. On one real set
five flips over 28 cases read as an 18% churn rate and turned out to be one
question asked two ways, where the wrong idiom returned 2 rows against the
right one's 443. If both arms ran the SAME query, the flip is downstream of it
-- the judge, the rubric, or non-determinism in the data -- and that is a
different finding, worth saying plainly.

**Read `scopes` before you blame retrieval for anything.** A call carrying a
`source` in its scope is pinned to that source and cannot return an entity from
another one, however well documented that entity is. A miss under a narrow
scope is the agent's scoping -- `agent-call` -- not `get_context/retrieval`.
Not hypothetical: a diagnosis asserted "a single unscoped get_context call"
about a call scoped to `source: flights`, and charged a missing `airports`
field to retrieval on that premise. The scope was in the request all along and
was missing from the evidence; it is there now, so calling a scoped call
unscoped is a checkable error.

Emit the object defined under `## Per case` in `reference/output-contract.md`
of the eval-diagnose skill as the LAST thing in your reply. Read that file; it
is the contract a script parses, and a shape invented here is dropped.

That file defines TWO objects. Yours is the per-case one, with `probes`,
`component`, `owner`, `sufficiency` and `primary_code` at the top level. The
clustering object under `## Per run` has a `clusters` array and belongs to a
different job and a different agent; emitting it here is dropped, however good
the analysis inside it is.
"""


def diagnose_one(qid: str, case: dict[str, Any], events: list[dict[str, Any]],
                 a: argparse.Namespace, art: pathlib.Path, *,
                 answered_correctly: bool = False,
                 passed_elsewhere: dict[str, Any] | None = None) -> dict[str, Any]:
    d = art / qid
    out = d / "diagnosis.json"
    if out.exists() and not a.force:
        return {**json.loads(out.read_text()), "qid": qid, "_cached": True}

    # Said up front, because the evidence would otherwise read as a wrong
    # answer and the diagnosis would go looking for one. The finding here is
    # narrower: a required entity never reached the answerer, and the answer
    # was right anyway -- by another route, which is worth naming.
    correct_line = ("\nTHIS ANSWER WAS CORRECT. Diagnose the RETRIEVAL miss "
                    "only: a required entity never reached the answerer and "
                    "the answer was right without it. Say by what route it "
                    "was right (a rebuilt measure, a sibling field, the "
                    "source docs), and do NOT look for a wrong number. An "
                    "answer that is right without the model's own entity is "
                    "right for now, not right by design.\n"
                    if answered_correctly else "")
    platform = a.target == "platform"
    scope_line = ""
    if platform and a.scope:
        env, pkg = a.scope.split("/", 1)
        ver = getattr(a, "scope_version", None)
        vscope = f', "version": "{ver}"' if ver else ""
        vquery = f' and version="{ver}"' if ver else ""
        scope_line = (f"\nThe run under diagnosis was scoped to environment "
                      f'"{env}", package "{pkg}"'
                      + (f", version \"{ver}\"" if ver else "")
                      + f'. Pass scopes=[{{"environment": '
                      f'"{env}", "package": "{pkg}"{vscope}}}] on get_context and '
                      f'environment="{env}", package="{pkg}"{vquery} on execute_query, '
                      f"so your probes hit the same model the answerer did.")
        if ver:
            scope_line += (f" Do not omit the version: without it you probe "
                           f"whatever is latest, and a finding about a model "
                           f"the run never measured is not a finding.")
    r = spawn_agent(
        DIAGNOSE_PROMPT.format(
            environment=a.environment, package=a.package,
            tools_name="the hosted platform" if platform else "Publisher",
            scope_line=scope_line + correct_line,
            evidence=json.dumps(evidence_for(qid, case, events,
                                             passed_elsewhere),
                                indent=2)[:14000]),
        skills=["eval-diagnose", *a.role_skills], skills_root=a.roots,
        model=a.model,
        mcp_url=a.mcp_url,
        tools=hosted_diagnose_tools(a.hosted_mcp_server) if platform
        else DIAGNOSE_TOOLS,
        blocked=READ_ONLY,
        cwd=a.model_dir, turns=a.max_turns, timeout=a.timeout,
        retries=a.retries, save_transcript=d / "diagnosis.jsonl",
        mcp_server=a.hosted_mcp_server if platform else "publisher")

    d.mkdir(parents=True, exist_ok=True)
    (d / "diagnosis.md").write_text(r.text)
    if r.json is None:
        return {"qid": qid, "error": r.error or "unparseable",
                "cost_usd": r.cost_usd}
    body = r.json
    lifted = salvage_cluster_shape(body)
    if lifted is not None:
        body = {**body, **lifted}
    obj = {**body, "qid": qid, "cost_usd": r.cost_usd,
           "wall_seconds": r.wall_seconds, "attempts": r.attempts}
    out.write_text(json.dumps(obj, indent=2))
    return obj


def salvage_cluster_shape(obj: dict[str, Any]) -> dict[str, Any] | None:
    """Lift a per-case diagnosis out of a reply written in the CLUSTER shape.

    The two prompts used to end with the same sentence pointing at the same
    reference file, and that file defines two objects. 11 of 28 per-case
    replies in one run came back as the clustering object: real analysis, with
    owner, component, codes and a root cause, in the wrong envelope. `validate`
    looks for the per-case keys, finds none, and the whole reply is discarded.
    At roughly $0.70 a case that was about $12 of the run's $20 thrown away.

    So when the violation is systematic rather than random, salvage beats
    discard. Only the fields ACTUALLY PRESENT are lifted; nothing is invented
    to satisfy the validator, because a fabricated field turns a shape error
    into a false claim. `probes` is the field that must never be fabricated:
    it is the record that something was checked, and the clustering shape has
    no structured probe records. So it stays empty, `sufficiency` reads
    `unknown`, and `validate` still reports "no probes recorded".

    Be precise about what that buys, because it is easy to overstate.
    `sufficiency: unknown` is a WARNING carried forward, not a gate: nothing
    refuses to act on it. `good` in main() filters on `error`, not on
    `_invalid`, so a salvaged diagnosis still reaches clustering -- which is
    the point, since without salvage it reached clustering with no owner, no
    component and no code, and grouped on nothing. From there the cluster's
    `sufficiency` is aggregated worst-case and travels to the improve step,
    whose prompt tells the agent to probe a claim itself before editing on it.
    Enforcement lives there, in an instruction, not in this function.

    Returns None when the reply is not cluster-shaped, so a genuinely
    unparseable or empty reply is left exactly as it was.
    """
    clusters = obj.get("clusters")
    if not isinstance(clusters, list) or not clusters:
        return None
    first = next((c for c in clusters if isinstance(c, dict)), None)
    if first is None:
        return None
    codes = [c for c in (first.get("codes") or []) if isinstance(c, str)]
    out: dict[str, Any] = {
        "probes": [],
        "sufficiency": "unknown",
        "reasoning": obj.get("reasoning") or first.get("evidence") or "",
        "diagnosis": first.get("rootCause") or "",
        "_salvaged": "reply used the clustering shape; per-case fields lifted, "
                     "probes not synthesised",
    }
    for src, dst in (("owner", "owner"), ("component", "component"),
                     ("confidence", "confidence"), ("severity", "severity")):
        if first.get(src) is not None:
            out[dst] = first[src]
    if codes:
        out["primary_code"] = codes[0]
        out["contributing_codes"] = codes[1:]
    # More than one cluster means the agent answered about the whole run from
    # one case's evidence. Keep the count so that is visible rather than
    # reading as a clean single-cause diagnosis.
    if len(clusters) > 1:
        out["_salvagedFrom"] = f"{len(clusters)} clusters; first used"
    return out


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


def retrieval_finding(case: dict[str, Any], events: list[dict[str, Any]],
                      key: tuple, verdict: str | None) -> dict[str, Any] | None:
    """The retrieval attribution for one case, when there is one to report.

    Calls the real scorer rather than restating its rule: `where_to_fix` is
    non-empty exactly when a required entity did not reach the answerer, and
    `score_retrieval` owns that decision for the run summary too, so the two
    cannot drift into disagreeing about what counts as a miss.
    """
    row = score_retrieval.score_case(case, events, key, verdict)
    return row if row.get("where_to_fix") else None


def behaviour_stats(qid: str, events: list[dict[str, Any]]) -> dict[str, Any]:
    """How an attempt CONDUCTED itself, for one case, in comparable numbers.

    The measures a behavioural root cause gets stated in: how much retrieval it
    did, how much of that retrieval named what it wanted, how many queries it
    ran, how many skills it opened, how long it took. Cheap -- it reads events
    already on disk and calls nothing.
    """
    attempt = next((e for e in events
                    if e.get("kind") == "attempt" and e.get("qid") == qid), {})
    calls = [e for e in events if e.get("kind") == "tool_call"
             and e.get("qid") == qid and e.get("tool") == "get_context"]
    # `target_shapes` when the run recorded it, `targets` otherwise. They are
    # not interchangeable and the difference is the whole point: `targets` is
    # the terms SEARCHED FOR, and a target carrying no `search_text` is dropped
    # before it is written, so counting bare targets from it always yields
    # zero. Measured on a real 10-case run, every attempt read
    # `targetsWithoutSearchText: 0` while the argument the field exists to test
    # is precisely about how often that count is high.
    shapes = [t for c in calls for t in (c.get("target_shapes") or [])]
    if shapes:
        by_type: dict[str, int] = {}
        for t in shapes:
            by_type[t.get("type") or "?"] = by_type.get(t.get("type") or "?", 0) + 1
        n_targets = len(shapes)
        bare = sum(1 for t in shapes if not t.get("has_text"))
        measured = True
    else:
        # A run written before `target_shapes` existed. The types are readable
        # from the `"<type>: <text>"` strings, but the bare count is NOT
        # recoverable, so it is reported as unknown rather than as zero -- a
        # falsifier that silently reads zero is worse than one that abstains.
        targets = [t for c in calls for t in (c.get("targets") or [])]
        by_type = {}
        for t in targets:
            kind = (t.split(":", 1)[0].strip()
                    if isinstance(t, str) and ":" in t else "?")
            by_type[kind] = by_type.get(kind, 0) + 1
        n_targets, bare, measured = len(targets), None, False

    return {
        "qid": qid,
        "verdict": next((e.get("verdict") for e in events
                         if e.get("kind") == "score" and e.get("qid") == qid),
                        None),
        "targetTypes": by_type,
        "targetsMeasured": measured,
        "nGetContext": attempt.get("n_get_context"),
        "nExecute": attempt.get("n_execute"),
        "nExecuteErrors": attempt.get("n_execute_errors"),
        "searchTargets": n_targets,
        "targetsWithoutSearchText": bare,
        "skillsInvoked": attempt.get("skills_invoked") or [],
        "numTurns": attempt.get("num_turns"),
    }


def controls_block(controls: list[dict[str, Any]]) -> str:
    """The CONTROLS section of the clustering prompt, sized to what exists.

    Three cases, because the instruction that is right for twenty passing cases
    is wrong for one and meaningless for none:

    - **None.** Every case in the run failed, so there is nothing to compare
      against. Asking the agent to compare rates against an empty list invites
      it to read `[]` as evidence of absence. Say plainly that no behavioural
      cluster can be falsified here.
    - **One or two.** Not a rate. Two passing cases cannot establish that a
      behaviour is rarer in passes, and treating them as if they could is how a
      cluster gets confirmed by a coin flip.
    - **Three or more.** The real comparison.
    """
    n = len(controls)
    if not n:
        return ("CONTROLS: none. Every scored case in this run failed.\n\n"
                "There is nothing to compare a behaviour against, so a cluster\n"
                "whose root cause is a BEHAVIOUR -- how much the agent\n"
                "retrieved, how it phrased its targets, how many queries it ran\n"
                "-- cannot be falsified from this run. Mark every such cluster\n"
                "`unfalsified` and say why. Do NOT read the absence of controls\n"
                "as evidence that the behaviour is causal. A cluster resting on\n"
                "a MODEL fact -- a missing entity, a wrong measure, an\n"
                "undocumented convention -- is unaffected: those are checked\n"
                "against the model, not against other cases.")
    head = (f"CONTROLS: the same measurements on the {n} case(s) that PASSED "
            f"this run\n\n" + json.dumps(controls, indent=2) + "\n\n")
    if n < 3:
        return head + (
            f"{n} passing case(s) is NOT a rate. It is enough to notice that a\n"
            "behaviour you called causal also appears in a passing case, which\n"
            "is worth saying; it is not enough to establish that the behaviour\n"
            "is rarer in passes. Do not compute a percentage from it. If the\n"
            "behaviour appears here too, mark the cluster `contributing` rather\n"
            "than `primary`; if it does not, the cluster stays weakly supported\n"
            "and say so.")
    return head + (
        "These are the falsifier for any cluster whose root cause is a\n"
        "BEHAVIOUR -- how much the agent retrieved, how it phrased its targets,\n"
        "how many queries it ran, which skills it opened. Diagnosis only ever\n"
        "looks at failures, so a behaviour common to both looks causal here and\n"
        "is not.\n\n"
        "Before you claim a behaviour explains a cluster, compare it against\n"
        "these rows. If it occurs at a similar rate in the passes, say so and\n"
        "mark that cluster `contributing` rather than `primary`; a cluster\n"
        "whose behaviour does not separate the two groups must not be routed to\n"
        "a skill or model edit as the root cause.")


CLUSTER_PROMPT = """Apply Step 5 of the eval-diagnose skill across a whole run.

These are the per-case diagnoses from one run. Cluster them.

DIAGNOSED ISSUES
{issues}

{controls}

Emit the object defined under `## Per run, clustering` in
`reference/output-contract.md` of the eval-diagnose skill as the LAST thing in
your reply. Read that file; it is the contract a script parses, and a shape
invented here is dropped.

That file defines TWO objects. Yours is the clustering one, a `clusters` array
with a `reasoning` string beside it. The per-case object under `## Per case`
belongs to the agent that diagnosed each case individually; that work is
already done and is your input, not your output.

Each `cluster_id` names the DEFECT, never a remedy. `index-values-not-whole`
names what is wrong; `index-measures-missing-rounding` names a fix, and picks
it before anyone has weighed the alternatives. An id containing `missing`,
`should`, `add`, `fix` or `use` is a prescription, and choosing the remedy is
the improve step's job, not yours.
"""


def cluster(issues: list[dict[str, Any]], a: argparse.Namespace,
            out: pathlib.Path,
            controls: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    keep = ("qid", "component", "primary_code", "contributing_codes", "owner",
            "sufficiency", "severity", "diagnosis", "sharedWith")
    compact = [{k: v for k, v in i.items() if k in keep} for i in issues]
    r = spawn_agent(
        CLUSTER_PROMPT.format(issues=json.dumps(compact, indent=2),
                              controls=controls_block(controls or [])),
        skills=["eval-diagnose", *a.role_skills], skills_root=a.roots,
        model=a.cluster_model,
        # 14, not 8. The output contract moved into
        # reference/output-contract.md, and FETCHING it costs turns before the
        # agent has written a word. At 8 the whole reply was "I'll read the
        # output contract now." -- text, so no retry fired, and the run
        # recorded zero clusters. A turn budget tuned for a pasted prompt is
        # too tight the moment the prompt stops carrying everything.
        mcp_url=None, blocked=READ_ONLY, turns=14, timeout=a.timeout,
        retries=a.retries, save_transcript=out / "clustering.jsonl")
    (out / "clustering.md").write_text(r.text)
    res = r.json or {"clusters": [], "reasoning": r.error or "unparseable"}
    res["_cost_usd"] = r.cost_usd
    return res


def select_cases(events: list[dict[str, Any]],
                 cases: dict[str, dict[str, Any]],
                 want_verdicts: set[str] | tuple[str, ...],
                 *, include_holdout: bool = False,
                 no_retrieval_misses: bool = False,
                 only: str | None = None,
                 limit: int | None = None) -> tuple[
                     list[str], list[str], list[str], dict[str, list[str]],
                     dict[str, list[str]]]:
    """Sort every scored case into diagnose / passed / excluded.

    Pure, so the three accounting rules it enforces are pinned by tests rather
    than only by reading a run: a case lands in exactly ONE bucket; a case the
    run did not pass stays counted as non-passing even when `--only` or
    `--limit` keeps it out of this diagnosis; and a case that PASSED never
    reaches the non-passing denominator, however it was excluded.

    Returns (failed, passed, retrieval_only, excluded, excluded_passes).
    `excluded` holds only non-passing cases, so `not_passing` can sum it;
    `excluded_passes` holds passes kept out of diagnosis, reported separately.
    """
    # Account for EVERY scored case, not just the ones that get diagnosed.
    # A run that diagnosed 8 of 18 failures reported six clusters as though
    # they covered the failures; they covered 44% of them, and nothing said so.
    # Each case lands in exactly one bucket, so the buckets sum to the scored
    # cases and a reader can see what the clusters are silent about.
    failed, passed, retrieval_only = [], [], []
    excluded: dict[str, list[str]] = {}
    # Passes kept out of diagnosis. Separate from `excluded` because
    # `not_passing` sums that one and a pass is not a non-passing case.
    excluded_passes: dict[str, list[str]] = {}

    def exclude(why: str, *qids: str) -> None:
        excluded.setdefault(why, []).extend(qids)

    for e in events:
        if e.get("kind") != "score":
            continue
        qid, verdict = e["qid"], e.get("verdict")
        case = cases.get(qid)
        if case is None:
            exclude("not in the case file", qid)
        elif ledger.is_contaminated(e):
            # Ahead of the holdout and verdict checks: a contaminated attempt
            # is not evidence either way, so it is excluded for that reason
            # whatever split it is on.
            exclude("contaminated", qid)
        elif verdict is None:
            # No verdict to explain: an unestablished key, a truncated
            # attempt, or a judge reply that could not be read. The `reason`
            # says which, and none of them is a model failure.
            exclude(f"unscored ({e.get('reason') or 'no reason recorded'})",
                    qid)
        elif case.get("split") == "holdout" and not include_holdout:
            # Ahead of the verdict checks, beside contamination and for the
            # same reason: what split a case is on does not depend on how it
            # scored. Below the `match` branch, a holdout case that answered
            # correctly WITH a retrieval miss went straight into
            # `retrieval_only` and on to a diagnosis call, because the holdout
            # test was on a branch it never reached -- so the split leaked on
            # every run and no flag could stop it.
            #
            # Which bucket still depends on how it scored, because
            # `not_passing` sums `excluded` and a pass is not a non-passing
            # case. Withholding a holdout PASS through `exclude()` printed
            # "1 of 5 non-passing case(s) diagnosed (20%)" on a run with one
            # failure and four holdout passes -- the same arithmetic the
            # retrieval-miss branch below was already fixed for.
            if verdict == "match":
                excluded_passes.setdefault(
                    "holdout, withheld from diagnosis", []).append(qid)
            else:
                exclude("holdout, withheld from diagnosis", qid)
        elif verdict == "match":
            passed.append(qid)
            # A correct answer can still rest on a retrieval miss, and that is
            # a finding: the answer was right by another route, which on the
            # run this comes from meant the agent rebuilding the model's own
            # measure inline. It held while the measure was `count()` and
            # failed the moment one carried a grain rule. "It worked anyway" is
            # not a reason to leave the gap, so the case is diagnosed -- with
            # its correctness recorded, so nobody reads the issue as a wrong
            # number.
            key = (qid, e.get("sample"), e.get("phase"))
            if retrieval_finding(case, events, key, verdict):
                retrieval_only.append(qid)
        elif verdict not in want_verdicts:
            exclude(f"{verdict}, not in --verdicts", qid)
        else:
            failed.append(qid)
    failed = list(dict.fromkeys(failed))
    retrieval_only = [q for q in dict.fromkeys(retrieval_only)
                      if q not in failed]
    if no_retrieval_misses:
        if retrieval_only:
            # NOT `exclude()`: these cases PASSED. `excluded` is the account of
            # what the clusters are silent about among cases the run did not
            # pass, and `not_passing` sums it -- so putting a pass in there
            # printed "coverage: 0 of 2 non-passing case(s) diagnosed (0%)" on
            # a run where every case passed. Reported on its own line instead,
            # which is also what `retrieval_only` gets when the flag is off.
            excluded_passes["passed with a retrieval miss "
                            "(--no-retrieval-misses)"] = list(retrieval_only)
        retrieval_only = []

    # `--only` and `--limit` narrow what gets DIAGNOSED; they do not change
    # what the run failed. The cases they drop are recorded as exclusions so
    # the coverage denominator below still counts every non-passing case:
    # truncating `failed` in place moved numerator and denominator together
    # and printed "coverage: 5 of 5 non-passing case(s) diagnosed (100%)" on a
    # run with 23 failures and --limit 5, which is the exact silence the
    # coverage line was added to break.
    if only:
        want = {q.strip() for q in only.split(",")}
        dropped = [q for q in failed if q not in want]
        if dropped:
            exclude("not named in --only", *dropped)
        failed = [q for q in failed if q in want]
        retrieval_only = [q for q in retrieval_only if q in want]
    # `is not None`, not truthiness: `--limit 0` used to read as "no limit" and
    # diagnose EVERY failure, one billable agent each -- failing open, on the
    # one axis where failing open costs money. 0 now means zero cases, which is
    # what it says. Unlimited is the default, spelled by omitting the flag.
    if limit is not None:
        if failed[limit:]:
            exclude(f"beyond --limit {limit}", *failed[limit:])
        failed = failed[:limit]
        retrieval_only = retrieval_only[:max(0, limit - len(failed))]
    return failed, passed, retrieval_only, excluded, excluded_passes


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", required=True, type=pathlib.Path)
    ap.add_argument("--set", dest="set_dir", required=True, type=pathlib.Path)
    ap.add_argument("--model-dir", type=pathlib.Path, default=None,
                    help="the Malloy package under test; the agent's cwd")
    # The split is cheap-on-per-case, expensive-on-clustering, and one measured
    # run says it is the wrong way round: every failure in it came from per-case
    # work (wrong output shape, a probe at the wrong grain) while the clustering
    # output was sound even when downgraded to the cheaper model. Left as it is
    # on purpose. Three of the four causes were wording, now fixed, so the
    # controlled test is to re-run the same cases with the same models and see
    # what remains; only a probe that still goes wrong with the rule in front of
    # it justifies paying more on every future run.
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
    ap.add_argument("--verdicts", default="no_match",
                    help="comma-separated verdicts to diagnose. A near_match "
                         "that is STABLE across two arms is a coverage "
                         "finding, not judge noise, and repairing the rubric "
                         "will not close it -- take the stable list from "
                         "flip_table.py and pass --verdicts near_match. Never "
                         "diagnose a one-armed near_match; that is noise.")
    ap.add_argument("--no-retrieval-misses", action="store_true",
                    help="do not diagnose a case that answered correctly but "
                         "never received a required entity. Those are real "
                         "findings -- the answer was right by another route -- "
                         "so this is an opt-out for a run that only wants "
                         "answer failures, not a default.")
    ap.add_argument("--include-holdout", action="store_true",
                    help="diagnose holdout cases too. Holdout is normally "
                         "withheld so the acceptance check keeps something "
                         "the improve step never saw -- but a MEASURE-ONLY "
                         "run never reaches improve, so it is holding them "
                         "back from nothing. Refused when the run already "
                         "carries a candidate edit.")
    ap.add_argument("--compare-run", type=pathlib.Path, default=None,
                    help="another arm of the SAME model. A case this run "
                         "failed and that arm PASSED is a matched pair -- same "
                         "question, same model, one right answer and one wrong "
                         "one -- and the diff between the two queries usually "
                         "names the cause outright. Without it a flipped case "
                         "is diagnosed from one side only, which is the "
                         "richest evidence in the run going unread")
    ap.add_argument("--limit", type=int, default=None,
                    help="how many to process; each one spawns a real agent. Omit for no limit. 0 means zero, not unlimited.")
    ap.add_argument("--no-cluster", action="store_true")
    ap.add_argument("--target", choices=("local", "platform"), default="local",
                    help="platform: probe through a hosted MCP server "
                         "(cached OAuth) instead of a local Publisher")
    ap.add_argument("--hosted-mcp-server", default="hosted",
                    help="platform only: the MCP server name. Must match the "
                         "one the run's answerer used -- it is the OAuth cache "
                         "key and the `mcp__<server>__<tool>` prefix")
    ap.add_argument("--scope", default=None, metavar="ENV/PACKAGE[@VERSION]",
                    help="platform only: the package the run was scoped to, "
                         "so probes hit the same model. Append @VERSION to pin "
                         "the published version too; without one, probes hit "
                         "whatever is latest, which is not what the run "
                         "measured. Defaults to the run's own scope.")
    ap.add_argument("--target-version", default=None, metavar="VERSION",
                    help="platform only: the published version to probe. An "
                         "alternative to @VERSION on --scope, and it wins if "
                         "both are given. Defaults to the run's targetVersion.")
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
                         "playbook over-attributes to how the query was built")
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

    # A diagnosis of version X whose probes hit latest describes a model no one
    # measured. Take the scope and version from the run unless told otherwise,
    # so the default is "probe what was measured" rather than "probe latest".
    run_meta: dict[str, Any] = {}
    rj = a.run / "run.json"
    if rj.exists():
        try:
            run_meta = json.loads(rj.read_text())
        except json.JSONDecodeError:
            run_meta = {}
    if not a.scope and run_meta.get("scope"):
        a.scope = run_meta["scope"]
    a.scope_version = None
    if a.scope and "@" in a.scope:
        a.scope, a.scope_version = a.scope.rsplit("@", 1)
    if a.target_version:
        a.scope_version = a.target_version
    elif not a.scope_version and run_meta.get("targetVersion"):
        a.scope_version = run_meta["targetVersion"]
    if a.target == "platform" and a.scope and not a.scope_version:
        print("  ! no published version pinned: probes will hit latest, which "
              "may not be the version this run measured. Pass --target-version "
              "or --scope ENV/PACKAGE@VERSION.")

    events = read_jsonl(a.run / "events.jsonl")
    cases = {c["qid"]: c for c in read_jsonl(a.set_dir / "cases.jsonl")}
    codes = skill_codes()

    # Dev failures only. A holdout case the improve step never saw is the only
    # thing that makes the acceptance check mean anything, and a diagnosis describes the fix.
    want_verdicts = {v.strip() for v in a.verdicts.split(",") if v.strip()}
    unknown = want_verdicts - {"no_match", "near_match", "needs_human"}
    if unknown:
        raise SystemExit(f"--verdicts: {', '.join(sorted(unknown))} is not a "
                         f"diagnosable verdict (no_match, near_match, "
                         f"needs_human)")
    # Holdout exists so the acceptance check has something the improve step
    # never saw. A run that will never reach improve is holding it back from
    # nothing -- but the harness cannot take that on trust, so it checks: a run
    # already carrying a `candidate` has an edit in flight, and diagnosing its
    # holdout would burn the only split that can still falsify that edit.
    if a.include_holdout:
        spent = [e for e in events if e.get("kind") == "candidate"]
        if spent:
            raise SystemExit(
                f"--include-holdout: this run already carries "
                f"{len(spent)} candidate edit(s), so its holdout is the only "
                f"thing left that can falsify them. Diagnose holdout only on a "
                f"run that will not reach improve.")
        print("  --include-holdout: holdout cases WILL be diagnosed. This run "
              "must not go on to an improve step.")

    failed, passed, retrieval_only, excluded, excluded_passes = select_cases(
        events, cases, want_verdicts,
        include_holdout=a.include_holdout,
        no_retrieval_misses=a.no_retrieval_misses,
        only=a.only, limit=a.limit)
    # Diagnosed together, because the question asked of both is the same one:
    # why did the model not deliver what the answer needed. They are told
    # apart on the way in, so the prompt can say the answer was right, and on
    # the way out, so a reader never mistakes one for a wrong number.
    to_diagnose = failed + retrieval_only
    if not to_diagnose:
        print("no diagnosable failures in this run")
        return 0

    art = a.run / "artifacts"
    art.mkdir(parents=True, exist_ok=True)
    extra = (f" + {len(retrieval_only)} that answered correctly without a "
             f"required entity" if retrieval_only else "")
    print(f"tier 1: {len(failed)} failed dev cases{extra}, {a.model}, "
          f"{a.parallel} at a time  ({len(codes)} codes in the skill)")

    # The matched pairs, if another arm was named. A case this run failed and
    # that arm passed hands the diagnosing agent the one comparison it can
    # never make from a single run: same question, same model, two outcomes.
    other_arm: dict[str, dict[str, Any]] = {}
    if a.compare_run:
        try:
            import flip_table
            theirs = flip_table.verdicts(a.compare_run)
        except Exception as exc:                                # noqa: BLE001
            raise SystemExit(f"--compare-run: could not read "
                             f"{a.compare_run}: {exc}")
        label = (json.loads((a.compare_run / "run.json").read_text())
                 .get("label") if (a.compare_run / "run.json").exists()
                 else a.compare_run.name)
        for q in to_diagnose:
            v = theirs.get(q)
            if v and v.get("passed"):
                other_arm[q] = {"arm": label, "verdict": v["verdict"],
                                "finalQuery": v.get("final_query")}
        print(f"  --compare-run {label}: {len(other_arm)} of "
              f"{len(to_diagnose)} case(s) passed there, so they are "
              f"diagnosed as matched pairs")

    issues: list[dict[str, Any]] = []
    with futures.ThreadPoolExecutor(max_workers=a.parallel) as ex:
        fut = {ex.submit(diagnose_one, q, cases[q], events, a, art,
                         answered_correctly=q in retrieval_only,
                         passed_elsewhere=other_arm.get(q)): q
               for q in to_diagnose}
        for i, f in enumerate(futures.as_completed(fut), 1):
            q = fut[f]
            try:
                obj = f.result()
            except Exception as exc:
                obj = {"qid": q, "error": f"{type(exc).__name__}: {exc}"[:200]}
            issues.append(obj)
            if obj.get("error"):
                print(f"  [{i}/{len(to_diagnose)}] ! {q} {obj['error']}",
                      flush=True)
            else:
                obj["_invalid"] = validate(obj, codes)
                tag = "=" if obj.get("_cached") else ("!" if obj["_invalid"] else ".")
                mark = " (answered correctly)" if q in retrieval_only else ""
                print(f"  [{i}/{len(to_diagnose)}] {tag} {q}{mark} "
                      f"{obj.get('component')}/{obj.get('primary_code')} "
                      f"-> {obj.get('owner')}"
                      + (f"  [{'; '.join(obj['_invalid'])}]"
                         if obj["_invalid"] else ""), flush=True)

    good = [i for i in issues if not i.get("error")]
    clusters: dict[str, Any] = {"clusters": []}
    if good and not a.no_cluster:
        # The control group. Diagnosis only ever reads failures, so any
        # behaviour common to the whole run looks causal from inside it. On one
        # measured run the largest cluster was built on "substitutes broad
        # enumeration for targeted retrieval", and the enumeration rate was 25%
        # of targets in the failures against 26% in the passes -- identical.
        # What actually separated them was volume, which question difficulty
        # explains at least as well as call style. So the passes go to the
        # clustering agent as a falsifier, measured the same way.
        controls = [behaviour_stats(q, events) for q in passed]
        how = (f"{len(controls)} passing case(s) as controls"
               if len(controls) >= 3 else
               f"only {len(controls)} passing case(s): too few to establish a "
               f"rate, so behavioural clusters stay weakly supported"
               if controls else
               "NO passing cases in this run, so no behavioural cluster can be "
               "falsified here")
        print(f"\ntier 2: clustering {len(good)} diagnoses with "
              f"{a.cluster_model} ({how})")
        clusters = cluster(good, a, a.run, controls=controls)
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
            severity=worst((m.get("severity") for m in members),
                           SEVERITY, "low"),
            confidence=c.get("confidence") or "medium",
            # Worst case across the members, the way `severity` above already
            # aggregates, not `members[0]`. This value now travels to the
            # improve step, so a cluster whose first member happened to be
            # probed must not read as probed when a later one was not: a
            # cluster is `sufficient` only when every member is.
            sufficiency=worst((m.get("sufficiency") for m in members),
                              SUFFICIENCY, "unknown"),
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
    # Imported, not spelled: this file and `cluster_failures.py` both write
    # `clusters.jsonl`, and when `score_retrieval` renamed the labels these two
    # maps kept the old ones -- so the package's `where_to_fix` column carried
    # "query construction" from here and "delivered, wrong" from the scorer,
    # under one name, with the column's own doc matching neither.
    where = cluster_failures.WHERE_BY_OWNER
    lever = cluster_failures.LEVER_BY_OWNER
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
    print(f"\n{len(good)}/{len(to_diagnose)} diagnosed, "
          f"{len(clusters.get('clusters', []))} clusters, "
          f"{len(new) // 2} issues in {ev_path}")
    if retrieval_only:
        # Counted apart from the failures on purpose. These answers were
        # RIGHT; what is wrong is that the model did not deliver what they
        # needed and they were right by another route. Folding them into a
        # failure count would misreport the run.
        print(f"  of those, {len(retrieval_only)} answered correctly and are "
              f"diagnosed for a retrieval miss only: "
              f"{', '.join(sorted(retrieval_only))}")
    # What the clusters are silent about. A run that diagnosed 8 of 18
    # non-passing cases reported its six clusters as though they covered the
    # failures, and the exclusions -- five holdout, two near_match, one
    # unparseable diagnoser reply, one lost verdict -- were each individually
    # correct and never added up anywhere. The denominator is every case the
    # run did NOT pass, because that is the number a reader has in mind.
    # `retrieval_only` cases are PASSES and stay out of this denominator: the
    # account answers "what did the clusters not cover, of what did not pass".
    not_passing = len(failed) + sum(len(v) for v in excluded.values())
    # Numerator and denominator must describe the same population. `good` now
    # includes cases that PASSED and were diagnosed for a retrieval miss, and
    # counting those against a non-passing denominator printed "2 of 1 (200%)".
    # They are reported on their own line above instead.
    good_failures = [i for i in good if i.get("qid") in set(failed)]
    if not_passing:
        print(f"\ncoverage: {len(good_failures)} of {not_passing} "
              f"non-passing case(s) diagnosed "
              f"({100 * len(good_failures) / not_passing:.0f}%)")
        for why, qids in sorted(excluded.items(),
                                key=lambda kv: (-len(kv[1]), kv[0])):
            print(f"  {len(qids):>3} {why}: {', '.join(sorted(qids))}")
        undiagnosed = len(failed) - len(good_failures)
        if undiagnosed:
            print(f"  {undiagnosed:>3} selected but not diagnosed (the "
                  f"diagnoser errored or its reply did not parse)")
    # Below the coverage block and never inside its denominator: these PASSED.
    # Reported all the same, because "2 passing cases carried a retrieval miss
    # and you told me not to look at them" is a thing a reader should see.
    for why, qids in sorted(excluded_passes.items()):
        print(f"\n{len(qids)} passing case(s) kept out of diagnosis -- {why}: "
              f"{', '.join(sorted(qids))}")
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
