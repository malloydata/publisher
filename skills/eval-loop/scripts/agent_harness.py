#!/usr/bin/env python3
"""Spawn one Claude CLI holding real skills and MCP tools. Stdlib only.

Shared by the eval scripts, following the cross-skill import pattern already in
`run_baseline.py` (which imports `mcp_payload` by `sys.path` insert).

WHY SKILLS ARE INSTALLED, NOT PASTED

`reference/judge.md` says of itself "This file IS the judge prompt", so the judge
is pasted and that is its design. `eval-diagnose` and `eval-improve` are skills:
they live in `skills/`, they are listed in the manifests, and they refer to each
other as `skill:eval-improve`, `skill:eval-diagnose`. Those references are
meant to RESOLVE. Pasting a skill's text into a prompt leaves every one of
them dangling, and it duplicates doctrine that then drifts from the file
everybody else reads.

So the workspace gets a real `.claude/skills/` and the CLI is told to load it.
Verified 2026-08-30: with `--setting-sources project` an agent asked for the
diagnose ladder returns all six components in order and answers a
buried-entity question with `LOW-RANK|model`; without the flag the same agent
replies that no skill named eval-diagnose is available. The flag is what does
it, and `--restricted` is its opposite -- it "ignores user, project and local
settings files", so it cannot be combined with skill loading. That is why the
answerer in `run_baseline.py` keeps `--restricted` (it is isolated on purpose,
and it holds no skills) while diagnose and improve do not.

THE CLOSURE

Installing only the named skill is not enough: `eval-improve` says "the issue
this requires" is `skill:eval-diagnose`, and if that is absent the instruction
is a dead end the agent cannot act on. So references are followed transitively.
The graph is cyclic -- eval-diagnose and eval-improve name each other -- hence
the seen-set.

A group's skills only `skill:`-reference inside their own group (enforced by
`packages/skills/src/manifest.spec.ts`), so this closure never leaves the group.
Doctrine from another role -- `malloy-gotchas-modeling` for an improver,
`malloy-analysis` for an answerer -- is supplied by passing that manifest
group's skills in `names`, not by reference-following.

A skill named but not present on disk is reported rather than skipped. Silently
omitting it reproduces the exact failure this module exists to remove.
"""
from __future__ import annotations

import json
import os
import pathlib
import re
import shutil
import subprocess
import tempfile
import time
from dataclasses import dataclass, field
from typing import Any, Iterable

SKILL_REF = re.compile(r"skill:([a-z0-9][a-z0-9-]*)")

# Never available to a spawned agent regardless of caller: an eval agent that
# can reach the network can reach the question's answer.
ALWAYS_BLOCKED = ("WebFetch", "WebSearch")


HERE_SKILLS = pathlib.Path(__file__).resolve().parent.parent.parent
HERE_REPO = HERE_SKILLS.parent


def skills_roots(external: str | pathlib.Path | None = None) -> list[pathlib.Path]:
    """Where skills are looked up, in order.

    The doctrine the agents load can come from a checkout other than this one
    -- Publisher ships the malloy-* skills and, since publisher#1088, the same
    manifest shape -- so `--skills-root` (or EVAL_SKILLS_ROOT) names it and it
    is searched FIRST. This checkout is always searched too, because the
    eval-* skills themselves live only here. A run records which root the
    doctrine came from (skillsRoot / skillsVersion) beside the harness's own
    version, so a Publisher-skills run and an agent-skills run never share a
    pin by accident.
    """
    ext = external or os.environ.get("EVAL_SKILLS_ROOT")
    roots: list[pathlib.Path] = []
    if ext:
        e = pathlib.Path(ext).expanduser().resolve()
        # Accept either the repo root (holding skills/ and manifests/) or the
        # skills directory itself.
        e = e / "skills" if (e / "skills").is_dir() and not (e / "SKILL.md").exists() \
            and not any((e / n / "SKILL.md").exists() for n in ("malloy-analysis",)) else e
        if not e.is_dir():
            raise FileNotFoundError(f"--skills-root {ext}: {e} is not a directory")
        roots.append(e)
    roots.append(HERE_SKILLS)
    return roots


def find_skill(name: str, roots: Iterable[pathlib.Path]) -> pathlib.Path | None:
    # The eval-* skills are the harness's own doctrine, versioned with these
    # scripts, and always come from this checkout -- an external root may carry
    # an older port of them (publisher#1032 does) that would otherwise shadow
    # the version the scripts were written against.
    if name.startswith("eval-") and (HERE_SKILLS / name / "SKILL.md").exists():
        return HERE_SKILLS / name
    for r in roots:
        if (r / name / "SKILL.md").exists():
            return r / name
    return None


# The same role under the two manifest layouts: Credible's private repo has one
# manifest per surface; Publisher has one manifest with a group per role.
ROLE_MANIFESTS = {
    "analysis": ("analysis-app", "publisher-local#analysis"),
    "modeling": ("modeling-app", "publisher-local#modeling"),
}


def default_manifest(role: str, repo_root: pathlib.Path) -> str:
    """The first manifest for `role` that exists under repo_root/manifests.

    Lets the same scripts run unchanged from either checkout: in agent-skills
    the answerer defaults to analysis-app, in a Publisher checkout to
    publisher-local#analysis. A caller who passes --answerer-manifest
    explicitly bypasses this.
    """
    for cand in ROLE_MANIFESTS[role]:
        if (repo_root / "manifests" / f"{cand.split('#')[0]}.json").exists():
            return cand
    return ROLE_MANIFESTS[role][0]


def manifest_skills(manifest: str, repo_root: pathlib.Path) -> list[str]:
    """The skills a shipped manifest loads, read from the manifest itself.

    The answerer has to hold the same skills the product loads, or the run
    measures a different system than the one we ship. The only honest source
    for "the same" is the manifest the product ships: a list hardcoded here
    would drift the day someone adds a skill to the manifest, and the run would
    go on reporting a manifest name it no longer matches.
    """
    # `name#group` takes one named group of the manifest instead of the whole
    # set: Publisher ships one manifest for everything it has (publisher-local)
    # with `analysis` and `modeling` groups naming what each role loads, where
    # Credible's private manifests are one per role.
    manifest, _, group = manifest.partition("#")
    path = repo_root / "manifests" / f"{manifest}.json"
    if not path.exists():
        available = sorted(p.stem for p in
                           (repo_root / "manifests").glob("*.json"))
        raise FileNotFoundError(
            f"no manifest {manifest!r} in {repo_root / 'manifests'}. "
            f"Available: {', '.join(available)}")
    data = json.loads(path.read_text())
    if group:
        groups = data.get("groups") or {}
        if group not in groups:
            raise FileNotFoundError(
                f"manifest {manifest!r} has no group {group!r}. "
                f"Available: {', '.join(sorted(groups)) or 'none'}")
        return list(groups[group])
    return (list(data.get("auto_discovered") or [])
            + list(data.get("supporting") or []))


@dataclass
class AgentResult:
    text: str = ""
    json: dict[str, Any] | None = None
    events: list[dict[str, Any]] = field(default_factory=list)
    cost_usd: float | None = None
    num_turns: int | None = None
    input_tokens: int | None = None
    output_tokens: int | None = None
    wall_seconds: float | None = None
    attempts: int = 1
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.error is None and bool(self.text)


def resolve_closure(names: Iterable[str],
                    skills_root: pathlib.Path | Iterable[pathlib.Path],
                    depth: int = 1) -> tuple[list[str], list[str]]:
    """Skills reachable from `names` by `skill:` reference, to `depth` hops.

    Returns (present, missing). The graph is cyclic -- eval-diagnose and
    eval-improve name each other -- so `seen` is the guard.

    Depth defaults to 1 because the full transitive closure is not what anyone
    wants: the role skills handed in from a manifest group cite each other
    freely, so following one of them reaches the whole library. Claude Code
    puts every installed skill's name and description in the prompt, so that
    is both noise and a real hazard -- an agent told to diagnose should not be
    choosing between eval-diagnose and malloy-analyze.
    One hop covers what the named skill actually instructs the agent to follow,
    which is the reason to install anything beyond the skill itself.
    """
    roots = ([skills_root] if isinstance(skills_root, pathlib.Path)
             else list(skills_root))
    missing: list[str] = []
    seen: set[str] = set()
    frontier = list(dict.fromkeys(names))
    for hop in range(depth + 1):
        nxt: list[str] = []
        for name in frontier:
            if name in seen or name in missing:
                continue
            found = find_skill(name, roots)
            if found is None:
                missing.append(name)
                continue
            seen.add(name)
            if hop < depth:
                nxt.extend(SKILL_REF.findall((found / "SKILL.md").read_text()))
        frontier = nxt
    return sorted(seen), sorted(missing)


def build_workspace(skills: Iterable[str],
                    skills_root: pathlib.Path | Iterable[pathlib.Path],
                    mcp_url: str | None, prefix: str = "agent-",
                    depth: int = 1,
                    mcp_server: str = "publisher") -> pathlib.Path:
    """A scratch cwd holding `.claude/skills/` and an MCP config.

    Symlinked rather than copied so an edit to a skill takes effect on the next
    spawn, and so a long run cannot be reading a stale copy of the doctrine it
    reports having applied.
    """
    work = pathlib.Path(tempfile.mkdtemp(prefix=prefix))
    dest = work / ".claude" / "skills"
    dest.mkdir(parents=True)
    present, missing = resolve_closure(skills, skills_root, depth)
    if missing:
        raise FileNotFoundError(
            f"skills named but not found under {skills_root}: "
            f"{', '.join(missing)}. A dangling skill: reference is an "
            f"instruction the agent cannot follow, so this is fatal rather "
            f"than a warning.")
    roots = ([skills_root] if isinstance(skills_root, pathlib.Path)
             else list(skills_root))
    for name in present:
        (dest / name).symlink_to(find_skill(name, roots))
    if mcp_url:
        (work / "mcp.json").write_text(json.dumps(
            {"mcpServers": {mcp_server: {"type": "http", "url": mcp_url}}}))
    return work


def last_json_object(text: str) -> dict[str, Any] | None:
    """The last balanced {...} in the text; agents narrate before the payload."""
    depth, start, best = 0, None, None
    for i, ch in enumerate(text):
        if ch == "{":
            if depth == 0:
                start = i
            depth += 1
        elif ch == "}":
            if depth == 0:
                continue
            depth -= 1
            if depth == 0 and start is not None:
                best = text[start:i + 1]
    if best is None:
        return None
    try:
        obj = json.loads(best)
    except json.JSONDecodeError:
        return None
    return obj if isinstance(obj, dict) else None


def spawn_agent(prompt: str, *, skills: Iterable[str],
                skills_root: pathlib.Path, model: str,
                mcp_url: str | None = None,
                tools: Iterable[str] = (),
                blocked: Iterable[str] = (),
                add_dirs: Iterable[pathlib.Path] = (),
                cwd: pathlib.Path | None = None,
                turns: int = 40, timeout: int = 900,
                retries: int = 2, backoff: float = 20.0,
                depth: int = 1,
                save_transcript: pathlib.Path | None = None,
                mcp_server: str = "publisher") -> AgentResult:
    """Run one agent to completion, retrying an empty result.

    An empty stdout is not a considered answer -- overnight it is nearly always
    a transient rate limit or a killed process. Recording it as an unparseable
    reply turns a retryable blip into a permanent hole in the run, so it is
    retried with backoff and the attempt count is reported.
    """
    # The MCP server's NAME is part of the OAuth cache key and of every tool
    # name (mcp__<server>__<tool>), so a hosted target has to say what it is.
    work = build_workspace(skills, skills_root, mcp_url, depth=depth,
                           mcp_server=mcp_server)
    run_cwd = str(cwd) if cwd else str(work)
    # When cwd is elsewhere (e.g. the model repo) the skills still have to be
    # found, so point the CLI's project settings at the scratch workspace.
    env = {**os.environ, "CLAUDE_PROJECT_DIR": str(work)}

    cmd = ["claude", "-p", prompt, "--model", model,
           "--output-format", "stream-json", "--verbose",
           "--max-turns", str(turns),
           "--setting-sources", "project"]
    blocked_all = tuple(dict.fromkeys((*ALWAYS_BLOCKED, *blocked)))
    if blocked_all:
        cmd += ["--disallowedTools", *blocked_all]
    if mcp_url:
        cmd += ["--strict-mcp-config", "--mcp-config", str(work / "mcp.json")]
    if tools:
        cmd += ["--allowedTools", *tools]
    for d in add_dirs:
        cmd += ["--add-dir", str(d)]
    # The skills live in the scratch dir, so it must be readable even when the
    # agent is working somewhere else.
    if cwd:
        cmd += ["--add-dir", str(work)]

    res = AgentResult()
    for attempt in range(1, retries + 2):
        res.attempts = attempt
        t0 = time.time()
        try:
            p = subprocess.run(cmd, cwd=run_cwd, capture_output=True, text=True,
                               timeout=timeout, env=env)
            stdout, stderr = p.stdout, p.stderr
        except subprocess.TimeoutExpired:
            stdout, stderr = "", f"timeout after {timeout}s"
        res.wall_seconds = round(time.time() - t0, 1)

        events, text = [], ""
        for line in stdout.splitlines():
            line = line.strip()
            if not line.startswith("{"):
                continue
            try:
                e = json.loads(line)
            except json.JSONDecodeError:
                continue
            events.append(e)
            if e.get("type") == "assistant":
                for c in e["message"].get("content") or []:
                    if c.get("type") == "text":
                        text += c["text"]

        if text.strip():
            result = next((e for e in reversed(events)
                           if e.get("type") == "result"), {})
            usage = result.get("usage") or {}
            res.text = text
            res.events = events
            res.json = last_json_object(text)
            res.cost_usd = result.get("total_cost_usd")
            res.num_turns = result.get("num_turns")
            res.input_tokens = usage.get("input_tokens")
            res.output_tokens = usage.get("output_tokens")
            res.error = None
            break

        res.error = (stderr or "empty reply")[-300:]
        res.events = events
        if attempt <= retries:
            time.sleep(backoff * attempt)

    if save_transcript is not None:
        save_transcript.parent.mkdir(parents=True, exist_ok=True)
        save_transcript.write_text(
            "".join(json.dumps(e) + "\n" for e in res.events))
    shutil.rmtree(work, ignore_errors=True)
    return res
