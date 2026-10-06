#!/usr/bin/env python3
"""Run one Malloy query against a hosted package behind OAuth. Stdlib only.

    rows, cut, error = hosted_query.query(target, package, model_path, malloy)

Two steps of the loop need rows from a hosted platform, and neither can get
them from a plain script: `fetch_transcripts.py` reads a host's request logs,
and `verify_goldens.py` re-derives a golden from a truth package published on
the host. `run_baseline.mcp_call` is a raw urllib POST and says so in its own
`AuthRequired` docstring: it "carries no token, reads no credential store, and
there is nothing a CLI login can do for it". `claude -p` is the only client in
this tree that carries the credential, so this module is the one place that
spawns it for data.

THE AGENT IS A TRANSPORT, NOT A JUDGE

It is told to make one `execute_query` call and nothing else, it holds no other
tool, and the ROWS ARE READ OUT OF ITS TRANSCRIPT rather than out of its prose
-- the same `resource_json` the answerer parser uses. Nothing it says is
trusted or parsed. An LLM in the data path would be a place for the data to
change, and this keeps it out of one.

Each call costs about $0.10 and ten seconds, so callers batch: one query for
many ids, not one per id.
"""
from __future__ import annotations

import json
import pathlib
import sys
from dataclasses import dataclass
from typing import Any

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from agent_harness import run_cli  # noqa: E402
from run_baseline import resource_json, result_text  # noqa: E402

# One call, nothing else. The agent holds no other tool, so there is no second
# thing it could do with the credential it is being handed.
PROMPT = """Call the `execute_query` tool exactly once, with these
arguments and no others. Do not explain, summarise, or reformat the result.
Do not call any other tool. Reply with the single word DONE.

{arguments}
"""


@dataclass
class Target:
    """Where a hosted query goes and whose credential carries it.

    `server` is both the `mcp__<server>__` prefix and the OAuth cache key, so
    it must be the name the operator authenticated under.
    """
    mcp_url: str
    server: str
    organization: str
    environment: str
    workdir: pathlib.Path
    model: str = "sonnet"
    timeout: int = 300


def payload_rows(payload: dict[str, Any]) -> tuple[list[dict[str, Any]], bool] | None:
    """Rows out of one `execute_query` result body, and whether a limit cut it.

    Two shapes arrive. Publisher returns `{"result": [...]}` (sometimes with
    the list JSON-encoded). Credible returns `columnar-v1`: column names once,
    then positional rows, plus `_limit_hit`.
    """
    if payload.get("_format") == "columnar-v1":
        block = payload.get("rows") or {}
        cols = block.get("columns") or []
        rows = [dict(zip(cols, r)) for r in block.get("rows") or []]
        return rows, bool(payload.get("_limit_hit"))
    rows = payload.get("result")
    if isinstance(rows, str):
        try:
            rows = json.loads(rows)
        except json.JSONDecodeError:
            return None
    if isinstance(rows, list):
        return rows, False
    return None


def result_from_transcript(events: list[dict[str, Any]]
                           ) -> tuple[list[dict[str, Any]], bool, str | None]:
    """(rows, cut, error) from the transport agent's transcript.

    Read from tool results, never from prose. A result the host marked as an
    error is returned as the error, so a query the host REJECTED is not read
    as a query that returned nothing -- the two mean opposite things to a
    golden check. ([], False, None) means the call never happened.
    """
    for e in events:
        if e.get("type") != "user":
            continue
        for c in e["message"].get("content") or []:
            if c.get("type") != "tool_result":
                continue
            text = result_text(c)
            if c.get("is_error"):
                return [], False, (text or "the host returned an error")[:500]
            payload = resource_json(text)
            if payload is None:
                continue
            got = payload_rows(payload)
            if got is not None:
                return got[0], got[1], None
    return [], False, None


def query(target: Target, package: str, model_path: str, malloy: str, *,
          version: str | None = None
          ) -> tuple[list[dict[str, Any]], bool, str | None]:
    """One Malloy query against `package`. Returns (rows, cut, error)."""
    args: dict[str, Any] = {"organization": target.organization,
                            "environment": target.environment,
                            "package": package, "model_path": model_path,
                            "query": malloy}
    if version:
        args["version"] = version
    tool = f"mcp__{target.server}__execute_query"
    cfg = target.workdir / ".hosted-mcp.json"
    cfg.parent.mkdir(parents=True, exist_ok=True)
    cfg.write_text(json.dumps({"mcpServers": {target.server: {
        "type": "http", "url": target.mcp_url}}}))
    events, _text, stderr, _n, _secs = run_cli(
        ["claude", "-p", PROMPT.format(arguments=json.dumps(args, indent=2)),
         "--model", target.model, "--output-format", "stream-json",
         "--verbose", "--max-turns", "3", "--strict-mcp-config",
         "--mcp-config", str(cfg), "--allowedTools", tool],
        cwd=str(target.workdir), timeout=target.timeout)
    rows, cut, err = result_from_transcript(events)
    if err is None and not rows and not events:
        err = (f"no transcript from claude -p ({stderr.strip()[:200]}). Is "
               f"`{target.server}` authenticated? `claude`, then /mcp")
    return rows, cut, err
