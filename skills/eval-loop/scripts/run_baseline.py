#!/usr/bin/env python3
"""Run an eval arm headlessly and write a conformant ledger. Stdlib only.

  python run_baseline.py --set evals/ecommerce --out results/2026-08-30-sonnet \
      --model sonnet --label sonnet-baseline \
      --environment samples --package ecommerce

Each case gets one fresh `claude -p` answerer holding the Publisher MCP tools and
nothing else, then one fresh judge that sees the golden and the answer but never
the model. Both phases append to `events.jsonl` per
`skills/eval-answer/reference/ledger-schema.md`.

SKILLS, AND WHY ISOLATION HAD TO CHANGE (2026-09-01)

The answerer loads the skills the product ships -- `--answerer-manifest`,
default `analysis-app` -- because otherwise the run measures a bare model
holding two MCP tools, not the agent we sell, and no skills A/B is possible.
Every run before this date accidentally measured the bare model.

That forced the isolation model to change, because `--restricted` and skill
loading are mutually exclusive: --restricted ignores the project settings that
load `.claude/skills/`. It was also doing two jobs at once, and dropping it
restored the FULL host toolset -- Bash, Task and the rest -- which is a worse
leak than the one it was preventing. So confinement is now three things:

  --setting-sources project   loads the workspace's skills
  --disallowedTools           names every host tool, Bash and Task included
  cwd is a workspace holding ONLY the skills and an MCP config; the eval set is
                              never passed as an --add-dir, so Read cannot
                              reach a golden
  --strict-mcp-config         ignores every MCP server but the one named here

Read stays available, unlike before, because a SKILL.md that points at a
reference file is a dead end without it.

A flag list is a claim, not a guarantee, so every attempt reads the CLI's own
`init` event back and records any host tool it was granted but should not have
had, plus any MCP server that failed to connect. Both mark the attempt
contaminated: an answer that may not have come through the model is not
evidence about the model, and a refusal caused by a dead server is not a fact
about the model either. `check_contamination.py` still runs over the tool log.

Do NOT add `--bare`. It looks right for an answerer, and it disables keychain
auth, so every attempt returns "Not logged in" as a successful zero-token run.

WHAT IS CAPTURED, AND WHY IT MATTERS

`--output-format stream-json` exposes each tool call's INPUT as well as its
result. That closes the gap in the ledger schema, where `tool_call` records
`rankedSummary` (what came back) with no field for what was asked, leaving "the
agent searched for the wrong thing" indistinguishable from "retrieval ranked the
right thing poorly". Here the get_context `targets` are recorded beside the
entities they returned, so the two are separable.

Token counts and cost come off the result event and land on the attempt. The
schema counts calls but not tokens, and the product claim is about both.
"""
from __future__ import annotations

import argparse
import concurrent.futures as futures
import hashlib
import json
import os
import pathlib
import re
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

ANSWER_TOOLS = ("mcp__publisher__malloy_getContext",
                "mcp__publisher__malloy_executeQuery",
                "mcp__publisher__malloy_compile")
# The platform target: a hosted MCP server exposing the same two operations
# under its own names. The CLI addresses a tool as `mcp__<server>__<tool>`, so
# both halves are configuration -- `--hosted-mcp-server` names the server (which
# is also the OAuth cache key, so it must match what the user authenticated) and
# `--hosted-tools` names the bare tools. Nothing here is specific to one vendor.
#
# Grant every tool the hosted surface ships, not just the two: a run that
# withholds one measures a narrower product than users get, and an un-granted
# tool surfaces mid-attempt as a permission-prompt error. Add a host's own
# extras (its product-doc search, say) with --hosted-tools.
#
# The answerer must authenticate once interactively (`claude` + /mcp) so the
# OAuth token is cached before a headless run.
HOSTED_TOOLS_DEFAULT = ("get_context", "execute_query", "search_malloy_docs")


def hosted_tools(server: str, bare: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(f"mcp__{server}__{t}" for t in bare)
# `--restricted` used to do two jobs at once: hide the settings that load
# skills, AND strip the host toolset down to nothing. Turning it off to get
# skills restored the FULL default toolset -- Bash, Task, ToolSearch and the
# rest -- which was verified on 2026-09-01 by reading an answerer transcript's
# init event. So the deny-list now has to name them: an answerer holding Bash
# can reach the eval set on disk no matter what its cwd is, and one holding
# Task can spawn a sub-agent that is confined by nothing at all.
BLOCKED_TOOLS = ("Edit", "Write", "NotebookEdit", "WebFetch", "WebSearch",
                 "Bash", "Task", "ToolSearch", "Monitor", "SendMessage",
                 "ListAgents", "CronCreate", "CronDelete", "CronList",
                 "RemoteTrigger", "PushNotification", "ScheduleWakeup",
                 "Workflow", "DesignSync", "EnterWorktree", "ExitWorktree",
                 "TaskOutput", "TaskStop", "ShareOnboardingGuide",
                 "ReportFindings",
                 # Granted whenever the MCP server advertises resources (the
                 # hosted server does); seen 2026-09-01 on the first platform
                 # smoke, where they alone flagged the attempt contaminated.
                 "ListMcpResourcesTool", "ReadMcpResourceDirTool",
                 "ReadMcpResourceTool")

# get_context and executeQuery results arrive as "[Resource from publisher at
# <uri>] {json}". Nothing else in the payload is machine-readable.
RESOURCE = re.compile(r"\[Resource from publisher at [^\]]+\]\s*(\{.*)", re.S)

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent.parent
                       / "eval-answer" / "scripts"))
import ledger  # noqa: E402
from ledger import read_jsonl  # noqa: E402
from mcp_payload import doc_tokens, entity_ids, search_terms  # noqa: E402
from publisher_rest import package_identity, served_model_path, try_query  # noqa: E402
from score_retrieval import score_case, summarise  # noqa: E402
from check_contamination import check as path_check  # noqa: E402
import verify_goldens  # noqa: E402

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from agent_harness import (build_workspace, default_manifest,  # noqa: E402
                           manifest_skills, no_events, no_text,
                           run_cli, skills_roots)

SKILLS_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
REPO_ROOT = SKILLS_ROOT.parent

# What the judge is allowed to know. Grain, fan-out and query-shape knowledge
# so it can tell a wrong answer from a wrong answer key -- and nothing that
# teaches it how the answerer was told to work.
# The judge is a SKILL, installed into its workspace like every other agent's
# doctrine, not 20KB pasted into the prompt. Two reasons beyond consistency: the
# prompt stops having to define everything, and a judge that needs to understand
# a Malloy query can reach for the skills beside it instead of being handed a
# transcription of them.
JUDGE_SKILLS = ("eval-judge", "malloy-analysis-pitfalls", "malloy-gotchas-queries")


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def git_sha(path: pathlib.Path, scope: pathlib.Path | None = None) -> str | None:
    """HEAD of the repo containing `path`, with a dirty marker. None if not a repo.

    `-dirty` is decided over `scope` (default: the whole tree). Every run pin
    so far ended `-dirty` because the repo had unrelated uncommitted files --
    a scratch notebook, a run directory -- which says nothing about whether the
    MODEL bytes match the commit. Scoped to the package directory the answerer
    was served from, the marker means what a reader takes it to mean.
    """
    try:
        d = path if path.is_dir() else path.parent
        head = subprocess.run(["git", "-C", str(d), "rev-parse", "HEAD"],
                              capture_output=True, text=True, timeout=10)
        if head.returncode != 0:
            return None
        cmd = ["git", "-C", str(d), "status", "--porcelain"]
        if scope is not None:
            cmd += ["--", str(scope)]
        dirty = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        return head.stdout.strip() + ("-dirty" if dirty.stdout.strip() else "")
    except Exception:  # noqa: BLE001
        return None


# --- Run naming --------------------------------------------------------------
#
# A run's label is what every comparison, flip table and report prints, so a set
# of hand-typed names -- base, rejudged, rejudged2, r3, post1, post2 -- becomes
# unreadable within one afternoon and cannot be sorted, grouped or matched to an
# arm. Runs are named for what they ARE, from fields the run already carries:
#
#     <set>-<phase>-<nn>        ecommerce-baseline-01, ecommerce-baseline-02
#                               ecommerce-blind_gate-01
#
# `phase` is the ledger's own vocabulary (baseline / loop / blind_gate / canary
# / final), so the A/A pair is two runs of the SAME phase and the post-edit arms
# are two runs of `blind_gate`. The sequence number is assigned by looking at
# what already exists beside this run, so a second arm cannot silently overwrite
# the first or be given a name that sorts before it.
#
# `--label` still overrides, for the rare run that needs a human name.


def next_run_label(out: pathlib.Path, set_name: str, phase: str) -> str:
    """`<set>-<phase>-<nn>`, with nn the next free number beside `out`."""
    stem = f"{set_name}-{phase}"
    siblings = out.parent.glob(f"{stem}-*") if out.parent.exists() else []
    used = set()
    for s in siblings:
        tail = s.name[len(stem) + 1:]
        if tail.isdigit():
            used.add(int(tail))
    n = 1
    while n in used:
        n += 1
    return f"{stem}-{n:02d}"


# Tools only the open-source Publisher exposes. A shared skill refers to an MCP
# tool by its BARE name (`get_context`, `execute_query`) precisely so it reads
# correctly on any host; a skill naming these is a host/router skill written for
# Publisher, and on a hosted target it teaches the answerer tools it does not
# have. That does not error -- the answerer simply reads instructions for a
# different surface -- so it has to be said out loud or the run quietly measures
# a different system than the one it names.
PUBLISHER_ONLY_TOOLS = ("malloy_getContext", "malloy_executeQuery",
                        "malloy_compile", "malloy_reloadPackage")


def publisher_only_skills(names: list[str],
                          roots: list[pathlib.Path]) -> list[str]:
    """Which of `names` name a Publisher-only tool in their SKILL.md."""
    out = []
    for name in names:
        for r in roots:
            f = r / "skills" / name / "SKILL.md" if (r / "skills").is_dir() else r / name / "SKILL.md"
            if f.exists():
                text = f.read_text()
                if any(tool in text for tool in PUBLISHER_ONLY_TOOLS):
                    out.append(name)
                break
    return out


def judge_pins(judge_md: pathlib.Path) -> tuple[str, str | None]:
    """(JUDGE_VERSION, content sha) declared by the judge file itself.

    Parsed rather than hardcoded: a judge version that lives in two places is a
    judge version that will disagree with itself, and every verdict is stamped
    with it.
    """
    if not judge_md.exists():
        return "0", None
    text = judge_md.read_text()
    m = re.search(r"^JUDGE_VERSION:\s*(\S+)", text, re.M)
    return (m.group(1) if m else "0"), sha256(text.encode())


# --- The model the answerer actually queried -------------------------------
#
# A judge that cannot read the model cannot tell a wrong answer from a rubric
# describing a model that no longer exists. That is not hypothetical: a fix to
# `lifetime_orders` left two rubrics asserting the old definition, and the judge
# went on failing correct answers because the rubric's claim was the only
# account of the model it had.
#
# The model to show it is the one the SERVER was serving, which on a snapshot
# host is not the working tree. So resolve it through the Publisher's own
# project location and snapshot it into the run directory, where `--rejudge` can
# still find it after the served copy has moved on.


def format_rows(rows: list[dict], limit: int = 60) -> str:
    if not rows:
        return "(the query returned no rows)"
    head = rows[:limit]
    cols = list(head[0].keys())
    out = [" | ".join(cols), "-+-".join("-" * len(c) for c in cols)]
    out += [" | ".join("" if r.get(c) is None else str(r.get(c)) for c in cols)
            for r in head]
    if len(rows) > limit:
        out.append(f"... {len(rows) - limit} more rows (of {len(rows)} total)")
    return "\n".join(out)


def resource_json(text: str) -> dict[str, Any] | None:
    """The machine-readable part of a tool result. Publisher wraps it in a
    '[Resource from publisher at ...]' preamble; a hosted MCP may return
    the JSON body directly, so a bare parse is the fallback."""
    m = RESOURCE.search(text or "")
    if m:
        try:
            return json.loads(m.group(1))
        except json.JSONDecodeError:
            return None
    s = (text or "").strip()
    if s.startswith("{"):
        try:
            parsed = json.loads(s)
            return parsed if isinstance(parsed, dict) else None
        except json.JSONDecodeError:
            return None
    return None


# What an answerer is allowed to hold, checked against what the host actually
# granted. A deny-list alone cannot hold this line: the host ships new tools,
# and each one would arrive silently enabled. The CLI announces its real
# toolset in the `init` event, so the run reads it back and treats anything
# unexpected as a breach of isolation rather than trusting the flags took.
ANSWERER_ALLOWED = {"Read", "Skill", "Glob", "Grep", "TodoWrite",
                    "AskUserQuestion", "ExitPlanMode", "EnterPlanMode"}


def isolation_breaches(events: list[dict[str, Any]],
                       mcp_tools: tuple[str, ...]) -> list[str]:
    """Host tools the answerer held that it should not have, plus any MCP
    server that failed. Both make an attempt unusable as evidence: the first
    because the answer may not have come through the model, the second because
    a refusal caused by a dead server is not a fact about the model."""
    init = next((e for e in events
                 if e.get("type") == "system" and e.get("subtype") == "init"),
                None)
    if init is None:
        return ["no init event: cannot verify what the answerer held"]
    granted = set(init.get("tools") or [])
    allowed = ANSWERER_ALLOWED | set(mcp_tools) | {
        t.split("__")[-1] for t in mcp_tools}
    unexpected = sorted(t for t in granted
                        if t not in allowed and not t.startswith("mcp__"))
    out = [f"host tool available to the answerer: {t}" for t in unexpected]
    out += [f"mcp server {s.get('name')!r} {s.get('status')}"
            for s in (init.get("mcp_servers") or [])
            if s.get("status") != "connected"]
    return out


def path_breaches(events: list[dict[str, Any]],
                  set_dir: pathlib.Path) -> list[str]:
    """Host tool uses that touched the eval set or a gold path.

    `isolation_breaches` checks what the answerer was GRANTED; this checks
    what it DID. A skills-loaded answerer legitimately holds Read/Glob/Grep,
    so the grant check alone would pass an answerer that grepped the gold
    file. `check_contamination.check` has carried the path rules since the
    loop was written and had no caller until 2026-09-01.
    """
    uses = []
    for e in events:
        if e.get("type") != "assistant":
            continue
        for c in e["message"].get("content") or []:
            if c.get("type") == "tool_use":
                uses.append({"name": c["name"], "input": c.get("input") or {}})
    # Absolute, always: passed `--set .`, a relative path becomes the needle "."
    # and matches every tool input, voiding every verdict in the run.
    r = path_check({"tool_uses": uses}, eval_paths=[str(set_dir.resolve())])
    return list(r["reasons"])


def result_text(block: dict[str, Any]) -> str:
    c = block.get("content")
    if isinstance(c, str):
        return c
    if isinstance(c, list):
        return "\n".join(x.get("text", "") for x in c if isinstance(x, dict))
    return ""


def claude(prompt: str, cwd: str, model: str, *, mcp: str | None,
           tools: tuple[str, ...] = (), turns: int = 30,
           effort: str | None = None, timeout: int = 900,
           skills: bool = True, retry: int = 2,
           backoff: float = 20.0,
           retry_when: Any = no_events) -> list[dict[str, Any]]:
    """One `claude -p` run. With `skills=True` the workspace's
    `.claude/skills/` is loaded; see ISOLATION in the module docstring for how
    the answerer is confined once `--restricted` is off."""
    cmd = ["claude", "-p", prompt, "--model", model,
           "--output-format", "stream-json", "--verbose",
           "--max-turns", str(turns)]
    # `--restricted` and skill loading are mutually exclusive: --restricted
    # ignores user, project and local settings, and project settings are what
    # load `.claude/skills/`. With skills on, isolation comes from the
    # deny-list plus a cwd holding nothing but the skills and an MCP config.
    cmd += ["--setting-sources", "project"] if skills else ["--restricted"]
    # Read stays available only when skills are loaded, because a SKILL.md that
    # points at a reference file is a dead end without it. It reaches the
    # workspace and nothing else: the eval set is never passed as an --add-dir.
    blocked = BLOCKED_TOOLS if skills else (*BLOCKED_TOOLS, "Read")
    cmd += ["--disallowedTools", *blocked]
    if effort:
        cmd += ["--effort", effort]
    if mcp:
        cmd += ["--strict-mcp-config", "--mcp-config", mcp]
    if tools:
        cmd += ["--allowedTools", *tools]
    # The subprocess, the stream-json parse and the retry are shared with
    # `agent_harness.spawn_agent`; only the retry predicate differs. `no_events`
    # retries a dead process or a rate limit and does NOT retry an attempt that
    # came back with events but no text -- that is a real failed answer, and
    # re-rolling it would put a second sample where the run records one.
    events, _text, stderr, _attempts, _wall = run_cli(
        cmd, cwd=cwd, timeout=timeout, retry_when=no_events,
        retries=retry, backoff=backoff)
    if not events:
        subtype = "timeout" if "timeout after" in (stderr or "") else "no_output"
        events = [{"type": "result", "subtype": subtype, "is_error": True,
                   "stderr": (stderr or "")[-500:]}]
    return events


ANSWER_PROMPT = """You are answering a business question against a published Malloy semantic model.

Environment: {environment}
Package: {package}

Question: {question}

Use malloy_getContext to find the entities you need, then malloy_executeQuery to
run a Malloy query against the model. Base the answer only on what the model
returns. If the model genuinely cannot answer the question, say so plainly and
name the specific data that is missing rather than substituting a proxy.

Finish with the number or rows you found, and the final Malloy query you ran."""

PLATFORM_PROMPT = """You are answering a business question against a published Malloy semantic model served by a hosted platform.

Organization: {environment}
Workspace: {package}
{scope_line}
Question: {question}

Use get_context to find the entities you need, then execute_query to run a
Malloy query against the model. Base the answer only on what the model
returns. If the model genuinely cannot answer the question, say so plainly and
name the specific data that is missing rather than substituting a proxy.

Finish with the number or rows you found, and the final Malloy query you ran."""

# A workspace can serve many packages -- a personal workspace serves every
# package the user can read -- and the same environment often holds an old and
# a new package with near-identical entities. Unscoped, the run also measures
# whether retrieval picks the right package, which is a different question
# from whether the model answers. So the answerer is told the one package the
# run is about and asked to pass it as an explicit scope on every call.
SCOPE_LINE = """
This run is about one package only: environment "{env}", package "{pkg}".
Pass scopes=[{{"environment": "{env}", "package": "{pkg}"}}] on every
get_context call, and environment="{env}", package="{pkg}" on every
execute_query call. Do not query any other package, even if get_context
suggests one.
"""


def server_alive(a: argparse.Namespace) -> bool:
    """Is the thing the answerer is about to talk to actually up?

    A dead Publisher returns an empty body that the answerer reads as a bad
    answer and the judge scores as a miss (Publisher's own friction log, item
    7; the VideoAmp run's server died mid-session and a raw probe caught it,
    not the harness). So every attempt asks first. Local: the REST root.
    Platform: any HTTP answer from the MCP URL -- it says 405 to a bare GET,
    which is fine; refusal or timeout is not.
    """
    url = (a.mcp_url if a.target == "platform"
           else a.publisher.rstrip("/") + "/api/v0/projects")
    try:
        urllib.request.urlopen(urllib.request.Request(url, method="GET"),
                               timeout=8)
        return True
    except urllib.error.HTTPError as e:
        return e.code < 500
    except Exception:  # noqa: BLE001 -- refused, reset, timed out
        return False


def wait_alive(a: argparse.Namespace, tries: int = 3, pause: float = 10.0) -> bool:
    for i in range(tries):
        if server_alive(a):
            return True
        if i < tries - 1:
            time.sleep(pause)
    return False


def run_answerer(case: dict[str, Any], a: argparse.Namespace,
                 art: pathlib.Path) -> dict[str, Any]:
    qid = case["qid"]
    d = art / qid
    d.mkdir(parents=True, exist_ok=True)

    if a.rebuild:
        # Re-derive the ledger from a transcript already on disk. The parser is
        # the part of this pipeline most likely to be wrong -- get_context has
        # several response shapes, and reading one of them as "no entities"
        # scores a good attempt at zero recall. Fixing that should cost nothing,
        # so parsing is replayable independently of answering.
        path = d / "answerer.jsonl"
        if not path.exists():
            raise FileNotFoundError(f"no saved transcript for {qid}")
        events = read_jsonl(path)
        elapsed = None
    else:
        if not wait_alive(a):
            # No spawn: a refusal produced by a dead server is not evidence
            # about the model, and it would still cost a model call.
            return {"qid": qid, "submitted": False, "final_query": None,
                    "answer_text": "", "calls": [], "error": "server_dead",
                    "n_get_context": 0, "n_execute": 0, "n_execute_errors": 0,
                    "host_tool_uses": 0, "transcriptPath": None,
                    "breaches": ["server unreachable before the attempt"]}
        platform = a.target == "platform"
        server = a.hosted_mcp_server if platform else "publisher"
        # The workspace holds the answerer's skills and nothing else, and it is
        # the cwd, so `Read` reaches the doctrine and never the eval set.
        if a.answerer_skills:
            work = str(build_workspace(a.answerer_skills, a.roots,
                                       mcp_url=None, prefix=f"ans-{qid}-"))
        else:
            work = tempfile.mkdtemp(prefix=f"ans-{qid}-")
        mcp = os.path.join(work, "mcp.json")
        with open(mcp, "w") as fh:
            json.dump({"mcpServers": {server: {"type": "http",
                                               "url": a.mcp_url}}}, fh)
        scope_line = ""
        if platform and a.scope:
            env, pkg = a.scope.split("/", 1)
            scope_line = SCOPE_LINE.format(env=env, pkg=pkg)
        prompt = (PLATFORM_PROMPT if platform else ANSWER_PROMPT).format(
            environment=a.environment, package=a.package,
            question=case["question"], scope_line=scope_line)
        t0 = time.time()
        events = claude(
            prompt, work, a.model, mcp=mcp,
            tools=a.hosted_tools if platform else ANSWER_TOOLS,
            turns=a.max_turns, effort=a.effort, timeout=a.timeout,
            skills=bool(a.answerer_skills))
        elapsed = round(time.time() - t0, 1)
        (d / "answerer.jsonl").write_text(
            "".join(json.dumps(e) + "\n" for e in events))
        shutil.rmtree(work, ignore_errors=True)

    calls, answer, queries = [], [], []
    model_paths = []
    n_get, n_exec, n_err, host_tools = 0, 0, 0, 0
    foreign_skills: list[str] = []
    pending: dict[str, dict[str, Any]] = {}

    for e in events:
        if e.get("type") == "assistant":
            for c in e["message"].get("content") or []:
                if c.get("type") == "text" and c["text"].strip():
                    answer.append(c["text"])
                elif c.get("type") == "tool_use":
                    name = c["name"]
                    # Both surfaces: Publisher's malloy_getContext/
                    # malloy_executeQuery and a hosted server's get_context/
                    # execute_query.
                    if (name.endswith("malloy_getContext")
                            or name.endswith("__get_context")):
                        n_get += 1
                        pending[c["id"]] = {"tool": "get_context",
                                            "targets": search_terms(c["input"])}
                    elif (name.endswith("malloy_executeQuery")
                            or name.endswith("__execute_query")):
                        n_exec += 1
                        q = c["input"].get("query")
                        if q:
                            queries.append(q)
                            # A package can hold many model files, and a source
                            # does not resolve outside the file that declares
                            # it. Re-executing every query against a single
                            # --model-path fails for every case whose source
                            # lives in another file, which then reads as a
                            # model failure rather than a harness one.
                            model_paths.append(c["input"].get("modelPath")
                                               or c["input"].get("model_path"))
                        pending[c["id"]] = {"tool": "execute_query",
                                            "targets": None}
                    else:
                        host_tools += 1
                        # The CLI ships ~17 skills of its own (batch, loop,
                        # code-review, dataviz ...) that no flag removes from
                        # the answerer's list -- verified 2026-09-02 against a
                        # scratch CLAUDE_CONFIG_DIR, which still listed them.
                        # They cannot be hidden, so an answerer that INVOKES
                        # one has left the product's doctrine, and that is
                        # recorded as a breach.
                        if name == "Skill":
                            sk = (c.get("input") or {}).get("skill")
                            if sk and sk not in (a.answerer_skills or []):
                                foreign_skills.append(sk)
        elif e.get("type") == "user":
            for c in e["message"].get("content") or []:
                if c.get("type") != "tool_result":
                    continue
                info = pending.pop(c.get("tool_use_id"), None)
                if info is None:
                    continue
                text = result_text(c)
                failed = bool(c.get("is_error"))
                payload = resource_json(text)
                if info["tool"] == "execute_query":
                    if failed or payload is None:
                        n_err += 1
                    calls.append({**info, "error": text[:300] if failed else None,
                                  "rankedSummary": None})
                else:
                    ids = entity_ids(payload or {})
                    calls.append({**info, "error": text[:300] if failed else None,
                                  "rankedSummary": {
                                      "entityIds": ids,
                                      "ranks": list(range(1, len(ids) + 1)),
                                      "resultCount": len(ids),
                                      # identifiers named in the returned
                                      # sources' docs: an entity there has
                                      # reached the answerer without being
                                      # a ranked entity (score_retrieval)
                                      "docTokens": doc_tokens(payload or {})}})

    res = next((e for e in reversed(events) if e.get("type") == "result"), {})
    usage = res.get("usage") or {}
    text = "\n\n".join(answer).strip()
    (d / "answer.md").write_text(text)

    return {
        "qid": qid,
        "submitted": bool(queries),
        # Every query it ran, in order. Taking only the last one mis-scored a
        # correct answer: the answerer computed the result, then ran a small
        # follow-up probe to sanity-check the date range, and the judge -- told
        # the answer must be supported by the final query -- graded the probe.
        "queries": queries,
        "final_query": queries[-1] if queries else None,
        "final_model_path": next((p for p in reversed(model_paths) if p), None),
        "answer_text": text,
        "n_get_context": n_get,
        "n_execute": n_exec,
        "n_execute_errors": n_err,
        "host_tool_uses": host_tools,
        "input_tokens": usage.get("input_tokens"),
        "output_tokens": usage.get("output_tokens"),
        "cache_read_tokens": usage.get("cache_read_input_tokens"),
        "cost_usd": res.get("total_cost_usd"),
        "num_turns": res.get("num_turns"),
        "wall_seconds": elapsed,
        "error": res.get("subtype") if res.get("is_error") else None,
        "calls": calls,
        "breaches": isolation_breaches(
            events, a.hosted_tools if a.target == "platform" else ANSWER_TOOLS)
        + path_breaches(events, a.set_dir)
        + [f"invoked a skill outside the manifest: {sk}"
           for sk in sorted(set(foreign_skills))],
        "transcriptPath": str((d / "answerer.jsonl").relative_to(art.parent)),
    }


JUDGE_PROMPT = """/eval-judge

Follow `skill:eval-judge`. It defines every verdict, the anchors,
the output shape and the `gold_status` judgement. Read it before you
emit anything; reach for the other skills in your workspace if a
Malloy query needs understanding.

---

You are judging ONE attempt. Return ONLY a JSON object, no prose around it,
with the keys in exactly this order:

{{"why": "one or two sentences of reasoning",
  "verdict": "match|near_match|no_match|needs_human",
  "confidence": 1-10, "column_pairing": null,
  "gold_status": "verified|verified_benign|suspect|verified_wrong",
  "gold_note": null}}

`why` comes first because the verdict must follow from the reasoning. Before you
emit `verdict`, re-read what you wrote in `why`: if it says the value and the
filter agree with the golden, the verdict is `match`, not `no_match`.

`gold_status` is a SEPARATE judgement from the verdict, and it does not soften
it: score against the golden as written, then say whether you believe the golden.
It is `verified` unless you can point at something specific, and `gold_note` has
to be concrete enough for someone else to check.

QUESTION: {question}

GOLDEN ({kind}): {golden}

CASE RUBRIC: {rubric_note}

THE ANSWER UNDER JUDGEMENT:
{answer}

QUERIES THE ANSWERER RAN, in order:
{query}

RE-EXECUTED RESULT of the answerer's final query, run against the same model it
queried:
{prediction}

MODEL THE ANSWERER QUERIED (source, including docs):
{model}

The queries and the re-executed rows are how you VERIFY the answer; they are not
a bar the answer has to clear.

- Where the re-executed rows and the answer's prose disagree about a number, the
  ROWS are the prediction. Prose is a report of them, and reports drift.
- An answerer that reads a value out of returned rows, or computes one from them
  (a running total, a ratio, a month-over-month change), has answered
  legitimately. Deriving in your head is not a defect and is not by itself
  grounds for no_match. Do not require that a single query return the stated
  figure, and do not penalise an answer for the number of queries it took.
- Whether the answer must SAY that it derived the metric is the case rubric's
  call, not yours. Apply that only where the rubric asks for it.
- The model source is there so you can check the rubric against it. A rubric is
  a claim about the model written at some past moment; where it asserts a
  definition the model contradicts, the model is what the answerer actually had.
  Say so in `why` rather than failing an answer that matches the model."""


# Phrases that only appear when the judge has talked itself into agreeing with
# the answer. If one shows up under a no_match, the verdict contradicts its own
# reasoning and is not safe to count either way.
AGREEMENT = (
    r"matching the golden exactly",
    r"matches the golden exactly",
    r"exactly matches the golden",
    r"agrees with the golden exactly",
    r"identical to the golden",
    r"correct filter and value",
)


def contradicts(reason: str, verdict: str | None) -> bool:
    if verdict != "no_match" or not reason:
        return False
    low = reason.lower()
    return any(re.search(p, low) for p in AGREEMENT) and \
        not re.search(r"\b(but|however|except|although)\b", low)


def parse_verdict(text: str) -> dict[str, Any]:
    m = re.search(r"\{.*\}", text, re.S)
    if not m:
        return {"verdict": None, "reason": "judge_unparseable", "confidence": None}
    try:
        v = json.loads(m.group(0))
    except json.JSONDecodeError:
        return {"verdict": None, "reason": "judge_unparseable", "confidence": None}
    verdict, conf = v.get("verdict"), v.get("confidence")
    reason = v.get("why", "")
    if isinstance(conf, int) and conf <= 5:
        verdict = "needs_human"
    if contradicts(reason, verdict):
        # Observed at confidence 9: "the answer's final query does apply
        # where: status = 'Complete' and reports 11865343.56, matching the
        # golden exactly with the correct filter and value" -> no_match. A
        # verdict its own reasoning refutes must not be counted as a failure;
        # doing so invents work for whoever is told to fix it.
        verdict = "needs_human"
        reason = f"[verdict contradicted its own reasoning] {reason}"
    # Only the four defined values survive. A judge inventing a fifth would
    # otherwise write it straight into the ledger and nothing downstream would
    # match on it.
    gs = v.get("gold_status")
    if gs not in ("verified", "verified_benign", "suspect", "verified_wrong"):
        gs = None
    return {"verdict": verdict, "reason": reason, "confidence": conf,
            "column_pairing": v.get("column_pairing"),
            "gold_status": gs, "gold_note": v.get("gold_note")}


def prediction_for(case: dict[str, Any], att: dict[str, Any],
                   a: argparse.Namespace, art: pathlib.Path,
                   reexec: bool) -> str:
    """Re-execute the answerer's final query so the judge sees rows, not prose.

    Cached per attempt: a re-judge must not depend on the server still being up,
    and re-running the same query for every rubric iteration is waste.
    """
    cache = art / case["qid"] / "prediction.json"
    if cache.exists():
        c = json.loads(cache.read_text())
        return c.get("rendered") or "(no prediction)"

    q = att.get("final_query")
    if not q:
        rendered = "(the answerer ran no query, so there is nothing to re-execute)"
    elif not reexec:
        # Rows from a model other than the one that produced the answer are worse
        # than no rows: they look authoritative and describe a different world.
        rendered = ("(not re-executed: the server is not serving the model this "
                    "run was answered against. Judge from the answer and the "
                    "queries, and lower your confidence accordingly.)")
    else:
        # The answerer's own modelPath first: its query was written against
        # that file. Then the case's, then the run default.
        mp = (att.get("final_model_path") or case.get("modelPath")
              or a.model_path)
        rows, err = try_query(a.publisher, a.environment, a.package, mp, q)
        rendered = (f"(re-execution failed: {err})" if err else format_rows(rows))
    cache.parent.mkdir(parents=True, exist_ok=True)
    cache.write_text(json.dumps({"query": q, "rendered": rendered}, indent=2))
    return rendered


_MODEL_SRC_CACHE: dict[str, str] = {}


def case_model_src(a, case: dict[str, Any], default: str) -> str:
    """The model file THIS case's entities live in.

    A package holds many model files and a source does not resolve outside the
    one that declares it. Handing every judge the run's single --model-path
    makes it report a perfectly good golden as `suspect` ("no such field in the
    model") whenever the case belongs to another file.
    """
    mp = case.get("modelPath")
    if not mp or mp == a.model_path:
        return default
    if mp not in _MODEL_SRC_CACHE:
        served = served_model_path(a.publisher, a.environment, a.package, mp)
        _MODEL_SRC_CACHE[mp] = served.read_text() if served else default
    return _MODEL_SRC_CACHE[mp]


def run_judge(case: dict[str, Any], att: dict[str, Any], a: argparse.Namespace,
              art: pathlib.Path, rubric: str, model_src: str,
              reexec: bool) -> dict[str, Any]:
    g = case.get("golden") or {}
    if not att["answer_text"]:
        return {"verdict": None, "reason": "not_submitted", "confidence": None}

    if a.rebuild and not a.rejudge:
        saved = art / case["qid"] / "judge.md"
        if saved.exists():
            return parse_verdict(saved.read_text())
        return {"verdict": None, "reason": "no_saved_verdict", "confidence": None}

    value = g.get("value")
    # Nothing here is truncated to a length shorter than the thing itself needs.
    # Rubrics were cut at 2000 characters, and because authors write the
    # CORRECT/WRONG part first and the accepted-divergence clauses last, the cut
    # removed the permissive half of four rubrics -- turning a rubric that said
    # "this alternate reading is fine" into one that appeared not to.
    prompt = JUDGE_PROMPT.format(
        rubric=rubric, question=case["question"], kind=g.get("kind"),
        golden=json.dumps(value) if value is not None
        else "(unanswerable: the model cannot answer this)",
        rubric_note=(g.get("rubric") or "none"),
        answer=att["answer_text"],
        query="\n\n".join(f"[{i}] {q}" for i, q in
                          enumerate(att.get("queries") or [], 1)) or "(none)",
        prediction=prediction_for(case, att, a, art, reexec),
        model=model_src or "(model source unavailable)")

    # The judge gets a DELIBERATELY NARROWER set than the answerer: enough
    # grain and fan-out knowledge to notice that a golden is wrong, and not the
    # answerer's own playbook. Handing the judge the manifest the answerer ran
    # would make it share the answerer's priors on exactly the questions where
    # independence is the point -- the two already share a model family, and
    # that is one correlation too many.
    if a.judge_skills:
        work = str(build_workspace(a.judge_skills, a.roots, mcp_url=None,
                                   prefix="judge-"))
    else:
        work = tempfile.mkdtemp(prefix="judge-")
    # `no_text` rather than the answerer's `no_events`: a judge that emitted
    # events but no verdict text parses as needs_human, which drops the case
    # from every aggregate. The judge is instrumentation, so a retry can only
    # help -- the measurement-integrity argument against retrying applies to
    # the answerer alone.
    # Six turns, not three: the judge now LOADS skill:eval-judge rather than
    # being handed it, and the load costs a turn before it has read a word of
    # the rubric. Three left it emitting a verdict with the skill still
    # unopened on a bad day.
    events = claude(prompt, work, a.judge_model, mcp=None, turns=6,
                    timeout=300, skills=bool(a.judge_skills),
                    retry_when=no_text)
    shutil.rmtree(work, ignore_errors=True)

    text = ""
    for e in events:
        if e.get("type") == "assistant":
            for c in e["message"].get("content") or []:
                if c.get("type") == "text":
                    text += c["text"]
    (art / case["qid"] / "judge.md").write_text(text)
    res = next((e for e in reversed(events) if e.get("type") == "result"), {})
    return {**parse_verdict(text), "judge_cost_usd": res.get("total_cost_usd")}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--set", dest="set_dir", required=True, type=pathlib.Path)
    ap.add_argument("--out", required=True, type=pathlib.Path)
    ap.add_argument("--model", default="sonnet")
    ap.add_argument("--judge-model", default="sonnet")
    ap.add_argument("--label", default=None)
    ap.add_argument("--effort", default=None)
    ap.add_argument("--target", choices=("local", "platform"), default="local",
                    help="local: a local Publisher serving working files. "
                         "platform: a hosted MCP server over a PUBLISHED "
                         "version -- the deployed retrieval engine and its "
                         "indexes; requires --target-version, and a cached "
                         "OAuth login for the MCP URL (run `claude`, /mcp, "
                         "authenticate once)")
    ap.add_argument("--hosted-mcp-server", default="hosted",
                    help="platform only: the MCP server name, which is both "
                         "the OAuth cache key and the `mcp__<server>__<tool>` "
                         "prefix. Must match the name the answerer "
                         "authenticated under")
    ap.add_argument("--hosted-tools", default=",".join(HOSTED_TOOLS_DEFAULT),
                    help="platform only: comma-separated BARE tool names the "
                         "hosted server exposes; the server prefix is added. "
                         f"Default: {','.join(HOSTED_TOOLS_DEFAULT)}")
    ap.add_argument("--target-version", default=None,
                    help="platform only: the published package version the "
                         "workspace serves. Required, because a platform run "
                         "pins the version it queried, not a local snapshot")
    ap.add_argument("--scope", default=None, metavar="ENV/PACKAGE",
                    help="platform only: the one package the answerer is told "
                         "to scope every call to, as environment/package. A "
                         "workspace can serve many packages (a personal one "
                         "serves every package the user can read); unscoped, "
                         "the run also measures package selection")
    ap.add_argument("--environment", default="samples",
                    help="local: the Publisher environment. "
                         "platform: the hosted ORGANIZATION")
    ap.add_argument("--package", default="ecommerce",
                    help="local: the served package. "
                         "platform: the hosted WORKSPACE")
    ap.add_argument("--mcp-url", default="http://localhost:4040/mcp")
    ap.add_argument("--publisher", default="http://localhost:4811",
                    help="Publisher REST base, used to re-execute the answerer's "
                         "final query so the judge sees rows rather than prose")
    ap.add_argument("--model-path", default=None,
                    help="model within the package; defaults to set.json targetModelPath")
    ap.add_argument("--skills-root", default=None,
                    help="a checkout holding skills/ and manifests/ to load the "
                         "answerer's and judge's doctrine from -- a Publisher "
                         "checkout, for the open-source skills. Searched before "
                         "this checkout, which still supplies the eval-* skills. "
                         "Also EVAL_SKILLS_ROOT. Recorded as skillsRoot; "
                         "skillsVersion pins ITS git sha and harnessVersion this one's")
    ap.add_argument("--answerer-manifest", default=None,
                    help="the shipped manifest whose skills the answerer "
                         "loads, so the run measures the product rather than "
                         "a bare model. Default: analysis-app where that "
                         "exists (a private skills repo), else publisher-local#analysis "
                         "(a Publisher checkout). name#group takes one group")
    ap.add_argument("--no-answerer-skills", action="store_true",
                    help="run the answerer with no skills at all. This is a "
                         "DIFFERENT measurement -- the semantic model alone, "
                         "not the product -- and every run before 2026-09-01 "
                         "was accidentally this. Recorded in run.json so the "
                         "two can never be compared by mistake")
    ap.add_argument("--truth-environment", default=None,
                    help="the environment name on the TRUTH server, when it "
                         "differs from --environment. A truth package is "
                         "usually served on its own server, which the answerer "
                         "has no route to, and that server names its "
                         "environments independently")
    ap.add_argument("--truth-publisher", default=None,
                    help="the Publisher serving the set's truthPackage, for the "
                         "pre-run golden check. Defaults to --publisher on a "
                         "local target; on a platform target the check is "
                         "skipped unless this is given")
    ap.add_argument("--skip-golden-check", action="store_true",
                    help="start even if goldens do not re-derive. The run is "
                         "then measuring against numbers nobody can reproduce, "
                         "and run.json records that you said so")
    ap.add_argument("--parallel", type=int, default=4)
    ap.add_argument("--max-turns", type=int, default=30)
    ap.add_argument("--timeout", type=int, default=900)
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--only", default=None, help="comma-separated qids")
    ap.add_argument("--phase", default="baseline")
    ap.add_argument("--no-judge", action="store_true")
    ap.add_argument("--rebuild", action="store_true",
                    help="re-derive events.jsonl from saved transcripts, "
                         "calling no model")
    ap.add_argument("--from", dest="from_run", type=pathlib.Path, default=None,
                    help="re-score into a NEW run directory: copy this run's "
                         "transcripts and pins to --out, then --rebuild "
                         "--rejudge them. The manual dance this replaces (cp -R, "
                         "delete events.jsonl, find -name judge.md -delete) was "
                         "done three times on one set")
    ap.add_argument("--rejudge", action="store_true",
                    help="with --rebuild: reuse the answers but score them "
                         "again, for a judge or rubric change")
    a = ap.parse_args(argv)
    # Resolved once here rather than per attempt: the answerer's granted tool
    # list is part of what a run measured, so it must not vary within a run.
    a.hosted_tools = hosted_tools(
        a.hosted_mcp_server,
        tuple(s.strip() for s in a.hosted_tools.split(",") if s.strip()))

    if a.from_run:
        # A re-score is a new run that shares the old run's answers: same
        # transcripts, fresh judge, new run.json (new judge/rubric/dataset
        # pins), and the old verdicts left untouched where they were.
        if a.out.exists() and any(a.out.iterdir()):
            raise SystemExit(f"--from: {a.out} exists and is not empty")
        src_art = a.from_run / "artifacts"
        if not src_art.exists():
            raise SystemExit(f"--from: {src_art} not found")
        (a.out / "artifacts").mkdir(parents=True)
        for d in src_art.iterdir():
            if d.is_dir():
                (a.out / "artifacts" / d.name).mkdir()
                for f in ("answerer.jsonl", "answer.md"):
                    if (d / f).exists():
                        shutil.copy(d / f, a.out / "artifacts" / d.name / f)
        if (a.from_run / "model.malloy").exists():
            shutil.copy(a.from_run / "model.malloy", a.out / "model.malloy")
        a.rebuild = a.rejudge = True
        print(f"re-scoring {a.from_run.name} into {a.out.name}: answers copied, "
              f"judge and predictions fresh")

    # Resolved once, before any spawn: a missing manifest or a dangling skill
    # must stop the run, not produce 49 attempts that quietly held no doctrine.
    a.roots = skills_roots(a.skills_root)
    ext_repo = a.roots[0].parent if a.skills_root else None
    if not a.answerer_manifest:
        a.answerer_manifest = default_manifest("analysis", ext_repo or REPO_ROOT)
    a.answerer_skills = ([] if a.no_answerer_skills
                         else manifest_skills(a.answerer_manifest,
                                              ext_repo or REPO_ROOT))
    a.judge_skills = list(JUDGE_SKILLS)
    if a.target == "platform" and a.answerer_skills:
        mismatched = publisher_only_skills(a.answerer_skills, a.roots)
        if mismatched:
            print(f"  ! {len(mismatched)} answerer skill(s) name Publisher-only "
                  f"tools on a PLATFORM run: {', '.join(mismatched)}")
            print(f"    The answerer holds mcp__{a.hosted_mcp_server}__* tools "
                  f"and will be told to call malloy_* ones. Point "
                  f"--answerer-manifest at a manifest written for this host, or "
                  f"--skills-root at the checkout that ships it.")
    print(f"answerer skills: "
          + (f"{a.answerer_manifest} ({len(a.answerer_skills)} skills)"
             if a.answerer_skills else "NONE -- measuring the model, not the product"))

    cases = read_jsonl(a.set_dir / "cases.jsonl")
    if a.only:
        want = {q.strip() for q in a.only.split(",")}
        cases = [c for c in cases if c["qid"] in want]
    if a.limit:
        cases = cases[:a.limit]
    if not cases:
        raise SystemExit("no cases selected")

    # Goldens must re-derive from the truth package before a dollar is spent:
    # 8 of 16 ecommerce goldens once drifted in four hours and every verdict
    # on them was noise. The check needs the truth server; on a platform
    # target there may not be one, and then it is skipped and said so.
    golden_check = "skipped"
    truth = a.truth_publisher or (a.publisher if a.target != "platform" else None)
    if a.rebuild:
        golden_check = "not run (rebuild)"
    elif truth and not a.skip_golden_check:
        print("checking goldens against the truth package")
        # The truth server is a SEPARATE server and names its environments
        # however it likes; assuming it reuses the model server's environment
        # name made the check report "nothing to re-derive against" on a truth
        # package that was serving correctly two ports away.
        r = verify_goldens.verify(a.set_dir, truth,
                                  a.truth_environment or a.environment,
                                  quiet=True)
        if r.get("skipped"):
            golden_check = r["skipped"]
            print(f"  {golden_check}")
        else:
            hard = [f for f in r["findings"] if not f.startswith("review ")]
            golden_check = (f"{r['tally'].get('ok', 0)} ok, {r['drifted']} drifted, "
                            f"{len(hard)} other finding(s)")
            print(f"  {golden_check}")
            if r["drifted"] or hard:
                for f in r["findings"]:
                    if not f.startswith("review "):
                        print(f"    {f}")
                raise SystemExit(
                    "goldens do not re-derive; fix or refresh them "
                    "(verify_goldens.py --refresh) or pass --skip-golden-check "
                    "to run anyway and have run.json say so")
    elif a.skip_golden_check:
        golden_check = "skipped by --skip-golden-check"
    else:
        print("  ! no truth server for a golden check on a platform target "
              "(--truth-publisher); goldens are taken as they stand")

    served_identity = package_identity(a.publisher, a.environment, a.package)
    label = a.label or next_run_label(a.out, a.set_dir.name, a.phase)
    art = a.out / "artifacts"
    art.mkdir(parents=True, exist_ok=True)

    rubric_path = (pathlib.Path(__file__).parent.parent.parent
                   / "eval-judge" / "SKILL.md")
    # Not pasted any more -- read only to PIN it. A verdict still has to say
    # which judge produced it, and the skill's own content is that identity.
    rubric = ""
    JUDGE_VERSION, RUBRIC_SHA = judge_pins(rubric_path)

    set_meta = {}
    if (a.set_dir / "set.json").exists():
        set_meta = json.loads((a.set_dir / "set.json").read_text())
    if not a.model_path:
        a.model_path = set_meta.get("targetModelPath") or "model.malloy"

    # Pin the model by CONTENT, in the run directory. `modelGitSha` was null on
    # every run this harness had produced, and a git sha would not have been
    # enough anyway: a snapshot host serves a copy, so the bytes answered
    # against are often a dirty working tree that no commit names. The snapshot
    # is what makes a re-judge months later still describe the right model.
    #
    # A platform target has no local file to snapshot: the pin is the PUBLISHED
    # version (--target-version), and the judge grades without re-executed rows
    # or model source -- a weaker verdict, stamped on the run so nobody reads a
    # platform score as if it had the local judge's evidence.
    if a.target == "platform":
        if a.scope and "/" not in a.scope:
            raise SystemExit("--scope must be environment/package")
        if not a.target_version:
            raise SystemExit("--target platform requires --target-version "
                             "(the published version the workspace serves)")
        model_src, pinned_sha, reexec, served = "", None, False, None
        print("  ! platform target: predictions are not re-executed and the "
              "judge does not see the model source; verdicts rest on the "
              "answer text and the golden alone")
    else:
        snap = a.out / "model.malloy"
        served = served_model_path(a.publisher, a.environment, a.package,
                                   a.model_path)
        served_sha = sha256(served.read_bytes()) if served else None
        if snap.exists():
            model_src, pinned_sha = snap.read_text(), sha256(snap.read_bytes())
        elif served:
            shutil.copyfile(served, snap)
            model_src, pinned_sha = snap.read_text(), served_sha
        else:
            model_src, pinned_sha = "", None

        reexec = bool(served_sha and pinned_sha and served_sha == pinned_sha)
        if not reexec:
            why = ("the Publisher is serving different bytes than this run is "
                   f"pinned to (served {str(served_sha)[:12]}, pinned "
                   f"{str(pinned_sha)[:12]})"
                   if served_sha and pinned_sha else
                   "the served model could not be located")
            print(f"  ! not re-executing predictions: {why}")

    # Lint the set's expected entities against the model text when there is
    # one: a name that appears nowhere in the served source is a stale set,
    # not a retrieval miss, and both VideoAmp platform runs carried five of
    # them (the set was written against a later package) which read as misses
    # until someone checked by hand. No model text on a platform target, so
    # the lint is skipped there and the report has to say so.
    if model_src:
        names = {e.split(":")[-1] for c in cases
                 for g in (c.get("expectedEntities") or {}).get("required", [])
                 for e in [g]} | {e.split(":")[-1] for c in cases
                                  for grp in (c.get("expectedEntities") or {}).get("requiredAnyOf", [])
                                  for e in grp}
        stale = sorted(n for n in names
                       if n and not re.search(r"(?<![A-Za-z0-9_])" + re.escape(n)
                                              + r"(?![A-Za-z0-9_])", model_src))
        if stale:
            print(f"  ! {len(stale)} expected entity name(s) appear nowhere in the "
                  f"served model: {', '.join(stale[:8])}{' ...' if len(stale) > 8 else ''}"
                  f" -- a stale set, not a retrieval miss; fix expectedEntities")
    elif a.target == "platform":
        print("  ! expected entities not linted against the model (platform target "
              "serves no model text)")

    (a.out / "run.json").write_text(json.dumps(ledger.run_config(
        runId=a.out.name, label=label, target=a.target,
        targetVersion=a.target_version,
        scope=a.scope if a.target == "platform" else None,
        answererModel=a.model, judgeModel=a.judge_model,
        effort=a.effort, phase=a.phase,
        started=ledger.now(),
        judgeVersion=JUDGE_VERSION, rubricSha=RUBRIC_SHA,
        datasetVersion=set_meta.get("datasetVersion"),
        # Content hash of the set, beside the human-readable version. A golden
        # repair moves this without anyone remembering to bump anything.
        datasetSha=ledger.dataset_sha(a.set_dir),
        # What the SERVER says it served, taken rather than recomputed.
        # packageSha covers every model path in the package, so an edit to an
        # imported file moves it where a sha of the one --model-path does not.
        packageSha=served_identity.get("sourceContentSha"),
        servedRevision=served_identity.get("servedRevision"),
        environment=a.environment, package=a.package,
        modelPath=a.model_path, modelSha=pinned_sha,
        # Pinned to the served package's tree when there is one: the dirty
        # marker then answers "do the model bytes match HEAD", not "is anything
        # in the repo uncommitted". A platform target has no local tree.
        modelGitSha=(git_sha(served.parent, scope=served.parent)
                     if a.target != "platform" and served else None),
        skillsVersion=ledger.skills_git_sha(a.roots[0]),
        skillsRoot=str(a.roots[0]),
        harnessVersion=ledger.skills_git_sha(),
        answererManifest=a.answerer_manifest if a.answerer_skills else None,
        answererSkills=a.answerer_skills or [],
        judgeSkills=a.judge_skills or [],
        mcpUrl=a.mcp_url, publisher=a.publisher,
        predictionsReExecuted=reexec,
        goldenCheck=golden_check,
    ), indent=2))

    if a.rebuild:
        print(f"re-deriving {len(cases)} cases from saved transcripts")
    else:
        print(f"{len(cases)} cases, answerer {a.model}, judge {a.judge_model}, "
              f"{a.parallel} at a time")

    attempts: dict[str, dict[str, Any]] = {}
    # Four strikes: four consecutive attempts that errored or found the
    # server dead abort the run instead of spending the rest of the budget
    # on attempts nobody will trust. eval-loop mandates this; the harness
    # had no such check until 2026-09-02.
    strikes, aborted = 0, False
    with futures.ThreadPoolExecutor(max_workers=a.parallel) as ex:
        fut = {ex.submit(run_answerer, c, a, art): c for c in cases}
        for i, f in enumerate(futures.as_completed(fut), 1):
            c = fut[f]
            try:
                att = f.result()
            except futures.CancelledError:
                continue
            except Exception as e:
                att = {"qid": c["qid"], "submitted": False, "final_query": None,
                       "answer_text": "", "calls": [], "error": str(e)[:200],
                       "n_get_context": 0, "n_execute": 0, "n_execute_errors": 0,
                       "host_tool_uses": 0, "transcriptPath": None}
            attempts[c["qid"]] = att
            dead = att.get("error") == "server_dead" or any(
                "mcp server" in b or "unreachable" in b for b in att.get("breaches") or [])
            strikes = strikes + 1 if (att.get("error") or dead) else 0
            flag = "!" if att.get("error") else ("." if att["submitted"] else "?")
            print(f"  [{i}/{len(cases)}] {flag} {c['qid']} "
                  f"({att.get('n_get_context',0)}gc/{att.get('n_execute',0)}eq "
                  f"{att.get('wall_seconds','?')}s)", flush=True)
            if strikes >= 4 and not aborted:
                aborted = True
                ex.shutdown(wait=False, cancel_futures=True)
                print("  !! four consecutive failed or dead attempts -- aborting "
                      "the run; the remaining cases were not attempted",
                      flush=True)
                break
    if aborted:
        cases = [c for c in cases if c["qid"] in attempts]

    verdicts: dict[str, dict[str, Any]] = {}
    if not a.no_judge:
        print("judging")
        with futures.ThreadPoolExecutor(max_workers=a.parallel) as ex:
            fut = {ex.submit(run_judge, c, attempts[c["qid"]], a, art, rubric,
                             case_model_src(a, c, model_src), reexec): c
                   for c in cases}
            for i, f in enumerate(futures.as_completed(fut), 1):
                c = fut[f]
                try:
                    verdicts[c["qid"]] = f.result()
                except Exception as e:
                    verdicts[c["qid"]] = {"verdict": None,
                                          "reason": f"judge_error: {e}"[:200],
                                          "confidence": None}
                print(f"  [{i}/{len(cases)}] {c['qid']} "
                      f"{verdicts[c['qid']].get('verdict')}", flush=True)

    events: list[dict[str, Any]] = []
    for c in cases:
        qid = c["qid"]
        att = attempts[qid]
        base = {"qid": qid, "sample": None, "phase": a.phase}
        events.append(ledger.event("attempt", **base,
                      question_sha=c.get("questionSha"),
                      submitted=att["submitted"],
                      final_query=att["final_query"],
                      # The revision that actually answered. Documented
                      # as "package revision actually queried" and left
                      # None until now, so nothing could tell an attempt
                      # answered before a reload from one answered after.
                      servedRevision=served_identity.get("servedRevision"),
                      n_get_context=att["n_get_context"],
                      n_execute=att["n_execute"],
                      n_execute_errors=att["n_execute_errors"],
                      host_tool_uses=att["host_tool_uses"],
                      reported_calls=att["n_get_context"] + att["n_execute"],
                      contaminated=("true" if att.get("breaches") else "false"),
                      contamination_reasons=att.get("breaches") or [],
                      input_tokens=att.get("input_tokens"),
                      output_tokens=att.get("output_tokens"),
                      cache_read_tokens=att.get("cache_read_tokens"),
                      cost_usd=att.get("cost_usd"),
                      num_turns=att.get("num_turns"),
                      wall_seconds=att.get("wall_seconds"),
                      answer_text=att.get("answer_text"),
                      run_error=att.get("error"),
                      transcriptPath=att["transcriptPath"]))
        for call in att["calls"]:
            events.append(ledger.event("tool_call", **base, **call,
                                       traceId=None))
        v = verdicts.get(qid)
        if v is not None:
            sc = {k: x for k, x in v.items() if k != "judge_cost_usd"}
            # The judge's read when it has one, the case's standing status
            # when it does not.
            sc["gold_status"] = (sc.get("gold_status")
                                 or (c.get("golden") or {}).get("status"))
            # The schema: a score copies the attempt's contamination flag and
            # a contaminated attempt carries no verdict. This was hardcoded
            # "false" until 2026-09-01, so a flagged attempt could still pass.
            tainted = bool(att.get("breaches"))
            if tainted:
                sc["verdict"] = None
            events.append(ledger.event("score", **base, **sc,
                          judge_version=JUDGE_VERSION,
                          rubric_sha=RUBRIC_SHA,
                          golden_revision=c.get("goldenRevision"),
                          contaminated="true" if tainted else "false",
                          artifactPath=f"artifacts/{qid}/judge.md"))

    ledger.write_events(a.out / "events.jsonl", events)

    # near_match is neither a pass nor a fail: see flip_table.py. Reported on
    # its own line, because a set whose near_match count is climbing has rubrics
    # going vague, and folding it into either column hides that.
    # A demonstrably wrong key is not evidence about the model either way, so it
    # leaves the aggregates entirely rather than counting as a failure.
    wrong_gold = {q for q, v in verdicts.items()
                  if v.get("gold_status") == "verified_wrong"}
    scored = {q: v for q, v in verdicts.items() if q not in wrong_gold}

    ok = sum(1 for v in scored.values() if v.get("verdict") == "match")
    near = sum(1 for v in scored.values() if v.get("verdict") == "near_match")
    conf = sum(1 for v in scored.values() if v.get("verdict") in
               ("match", "no_match"))
    human = sum(1 for v in scored.values() if v.get("verdict") == "needs_human")
    cost = sum(a.get("cost_usd") or 0 for a in attempts.values())
    print(f"\n{a.out}/events.jsonl  ({len(events)} events)")
    print(f"attempted {len(cases)}, decided {conf}, "
          f"passed {ok}" + (f" ({100*ok/conf:.0f}%)" if conf else ""))
    print(f"neither: {near} near_match, {human} needs_human")

    # Printed rather than left in the ledger: a doubted answer key sends the
    # next agent to fix a model that is already right, and the whole point of
    # asking the judge was to catch that before anyone acts on the run.
    doubted = sorted((q, v.get("gold_status"), (v.get("gold_note") or "")[:150])
                     for q, v in verdicts.items()
                     if v.get("gold_status") in ("suspect", "verified_wrong"))
    if doubted:
        print(f"\n! {len(doubted)} golden(s) the judge does not believe. These are "
              f"dataset issues, NOT model failures:")
        for q, st, note in doubted:
            print(f"    {st:15s} {q}\n      {note}")
        print("  Route via the golden side door in skill:eval-loop before improving.")

    # Retrieval was always recorded and never read: the entity IDs go onto every
    # tool_call event, but scoring them only happened in build_run_package, so a
    # run you never packaged had no attribution at all. That is the half of the
    # verdict that says WHERE to fix a failure, so it belongs in the run summary.
    retr = [score_case(c, events, (c["qid"], None, a.phase),
                       verdicts.get(c["qid"], {}).get("verdict"))
            for c in cases]
    rs = summarise(retr)
    if rs["retrieval_scored"]:
        print(f"\nper-question entity recall: mean {100 * rs['mean_recall']:.1f}%, "
              f"complete on {rs['complete_retrievals']} of "
              f"{rs['retrieval_scored']} scored")
        # Recall below 1.0 on a PASSING case means the required list named one
        # path to an answer the agent reached by another. That is an expectation
        # defect, not a retrieval miss, and it is why mean recall is a weaker
        # number than the attribution below.
        alt = sum(1 for r in retr
                  if r["recall"] is not None and r["recall"] < 1.0
                  and not r["failed"] and r["verdict"] is not None)
        if alt:
            print(f"  {alt} passing case(s) answered without every required "
                  f"entity -- check whether `required` over-specifies the path")
    if rs["failures_by_where_to_fix"]:
        print("where to fix: " + ", ".join(
            f"{k} {v}" for k, v in sorted(rs["failures_by_where_to_fix"].items())))
    judge_cost = sum((v.get("judge_cost_usd") or 0) for v in verdicts.values())
    # Recorded, not only printed. The next command is usually diagnose, and a
    # doubted key sends a modelling agent to fix a model that is already right --
    # the most expensive wrong turn this loop can take. A warning that exists
    # only as console text is one scrollback away from being missed, so the run
    # itself carries the list and the conductor can read it from run.json.
    ledger.update_run(a.out, answererCostUsd=round(cost, 4),
                      judgeCostUsd=round(judge_cost, 4),
                      doubtedGoldens=[{"qid": q, "gold_status": st,
                                       "gold_note": note}
                                      for q, st, note in doubted],
                      status="aborted" if aborted else "complete")
    print(f"answerer cost ${cost:.2f}" + (f", judge ${judge_cost:.2f}" if judge_cost else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())
