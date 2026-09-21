#!/usr/bin/env python3
"""Rebuild answerer transcripts for sessions that ran somewhere else. Stdlib only.

  python3 fetch_transcripts.py --set evals/<set> --out <runDir> \
      --mcp-url <scoped hosted url> --hosted-mcp-server <name> \
      --logs-scope <org>/<environment>/<package> --tier T2

Reads the set's cases, takes each case's `source` as the id of the session that
produced it (the id `skill:eval-import` tells a log scrape to keep), fetches
that session's logged calls, and writes `artifacts/<qid>/answerer.jsonl`. Then:

  python3 run_baseline.py --set <set> --out <runDir> --rebuild ...

That second step is the existing one, unchanged. This script writes no ledger
events and makes no scoring decision; `run_baseline --rebuild` derives the run
from the transcripts exactly as it would from spawned ones.

WHY IT SPAWNS AN AGENT TO READ A DATABASE

`run_baseline.mcp_call` is a raw urllib POST and says so in its own
`AuthRequired` docstring: it "carries no token, reads no credential store, and
there is nothing a CLI login can do for it". A hosted endpoint behind OAuth is
therefore unreachable from a plain script, and `claude -p` is the only client
in this tree that carries the credential.

So the agent is a transport, not a judge. It is told to make one
`execute_query` call and nothing else, and the ROWS ARE READ OUT OF ITS
TRANSCRIPT rather than out of its prose -- the same `resource_json` the
answerer parser uses. Nothing it says is trusted or parsed. An LLM in the data
path would be a place for the data to change, and this keeps it out of one.

HOST SPECIFICS ARE ARGUMENTS, NOT CODE

No vendor's table names appear here. `--logs-scope` names the package, and
`--source-*` name the sources within it, defaulting to the shape the access
design describes. A second host needs different flags, not a different file.
`log_transcript.py` holds the row-to-transcript rules and no host knowledge at
all.
"""
from __future__ import annotations

import argparse
import json
import pathlib
import sys
from typing import Any

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent.parent / "eval-answer" / "scripts"))

import log_transcript as lt  # noqa: E402
from agent_harness import run_cli  # noqa: E402
from run_baseline import resource_json, result_text  # noqa: E402

# One call, nothing else. The agent holds no other tool, so there is no second
# thing it could do with the credential it is being handed.
FETCH_PROMPT = """Call the `execute_query` tool exactly once, with these
arguments and no others. Do not explain, summarise, or reformat the result.
Do not call any other tool. Reply with the single word DONE.

{arguments}
"""


def rows_query(source: str, session_id: str, limit: int) -> str:
    """The Malloy for one session's rows from one logged source.

    `select:` rather than an aggregate because every column is wanted verbatim;
    the caller normalises. Ordered so a truncated page is the OLDEST part of
    the session rather than an arbitrary slice, which is the half a reader can
    still reason about.
    """
    return (f"run: {source} -> {{\n"
            f"  where: session_id = '{_literal(session_id)}'\n"
            f"  select: *\n"
            f"  order_by: `timestamp` asc\n"
            f"  limit: {int(limit)}\n"
            f"}}")


def _literal(value: str) -> str:
    """A Malloy string literal body.

    Single quotes and backslashes are escaped rather than stripped: a session
    id is an opaque token from a host, and silently rewriting one produces a
    query for a DIFFERENT session that returns rows and looks like a success.
    """
    return value.replace("\\", "\\\\").replace("'", "\\'")


def rows_from_transcript(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """The rows an `execute_query` tool result carried.

    Read from the transcript, never from the agent's prose. Returns [] when the
    call did not happen or returned nothing readable, which the caller reports
    as a miss rather than as an empty session.
    """
    for e in events:
        if e.get("type") != "user":
            continue
        for c in e["message"].get("content") or []:
            if c.get("type") != "tool_result":
                continue
            payload = resource_json(result_text(c))
            if payload is None:
                continue
            rows = payload.get("result") if isinstance(payload, dict) else None
            if isinstance(rows, str):
                try:
                    rows = json.loads(rows)
                except json.JSONDecodeError:
                    continue
            if isinstance(rows, list):
                return rows
    return []


def fetch_rows(a: argparse.Namespace, source: str,
               session_id: str) -> list[dict[str, Any]]:
    """One source's rows for one session, over the hosted MCP."""
    env, pkg = a.logs_environment, a.logs_package
    arguments = json.dumps({
        "organization": a.logs_organization,
        "environment": env,
        "package": pkg,
        "model_path": a.logs_model_path,
        "query": rows_query(source, session_id, a.row_limit),
    }, indent=2)
    tool = f"mcp__{a.hosted_mcp_server}__execute_query"
    mcp = {"mcpServers": {a.hosted_mcp_server: {"type": "http",
                                                "url": a.mcp_url}}}
    cfg = a.out / ".fetch-mcp.json"
    cfg.parent.mkdir(parents=True, exist_ok=True)
    cfg.write_text(json.dumps(mcp))
    events, _text, stderr, _n, _secs = run_cli(
        ["claude", "-p", FETCH_PROMPT.format(arguments=arguments),
         "--model", a.model, "--output-format", "stream-json", "--verbose",
         "--max-turns", "3", "--strict-mcp-config",
         "--mcp-config", str(cfg), "--allowedTools", tool],
        cwd=str(a.out), timeout=a.timeout)
    rows = rows_from_transcript(events)
    if not rows and stderr.strip():
        print(f"    ! {source}: no rows ({stderr.strip()[:120]})")
    return rows


def normalise(rows: list[dict[str, Any]], mapping: dict[str, str]
              ) -> list[dict[str, Any]]:
    """Host columns renamed to the keys `log_transcript` takes.

    A column the host does not have is ABSENT from the result, not None: the
    builder distinguishes "not recorded" from "recorded as nothing", and
    filling every gap with None would erase that.
    """
    out = []
    for r in rows:
        row = {k: r[src] for k, src in mapping.items()
               if src in r and r[src] is not None}
        out.append(row)
    return out


GET_CONTEXT_COLUMNS = {"request_id": "request_id", "session_id": "session_id",
                       "timestamp": "timestamp",
                       "request_payload": "request_payload",
                       "response": "response_body"}
# `error` is mapped from the response body because that is the only thing hosts
# tend to capture for this tool: a compile error is logged, a successful result
# is not. Which is exactly the asymmetry the final-query choice needs, since it
# picks the last call the server ANSWERED.
EXECUTE_COLUMNS = {"request_id": "request_id", "session_id": "session_id",
                   "timestamp": "timestamp",
                   "request_payload": "request_payload",
                   "error": "response_body"}
MESSAGE_COLUMNS = {"request_id": "request_id",
                   "turn_started_ms": "turn_started_ms", "seq": "seq",
                   "chunk": "chunk", "role": "role", "text": "text"}
TURN_COLUMNS = {"request_id": "request_id", "input_tokens": "input_tokens",
                "output_tokens": "output_tokens",
                "cache_read_tokens": "cache_read_input_tokens",
                "cache_write_tokens": "cache_creation_input_tokens",
                "round_trips": "round_trips", "outcome": "outcome"}


def fetch_session(a: argparse.Namespace, session_id: str
                  ) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """One session as a transcript, plus what each source contributed."""
    gc = normalise(fetch_rows(a, a.source_get_context, session_id),
                   GET_CONTEXT_COLUMNS)
    ex = normalise(fetch_rows(a, a.source_execute, session_id),
                   EXECUTE_COLUMNS)
    msgs: list[dict[str, Any]] = []
    turns: list[dict[str, Any]] = []
    # Only at a tier that claims them. Asking a host for a table the design
    # has not shipped yet returns an error per case, which reads in the log
    # like a broken fetch rather than a tier the operator chose.
    if a.tier in ("T2", "T3"):
        msgs = normalise(fetch_rows(a, a.source_messages, session_id),
                         MESSAGE_COLUMNS)
        turns = normalise(fetch_rows(a, a.source_turns, session_id),
                          TURN_COLUMNS)
    # A T2 fetch that came back with no prose did not get T2. Either the host
    # does not serve that table yet or this session has none, and in both
    # cases claiming `answer_captured: true` over an empty answer is the exact
    # failure this tier exists to prevent: the judge would compare "" against
    # the golden and score a real agent as having said nothing. Downgrade the
    # SESSION rather than the run, so a set where only some sessions kept
    # their prose still scores the ones that did.
    tier = a.tier
    if tier == "T2" and not msgs:
        tier = "T1"
    counts = {"get_context": len(gc), "execute": len(ex),
              "messages": len(msgs), "turns": len(turns), "tier": tier}
    return lt.build_transcript(get_context=gc, execute=ex, messages=msgs,
                               turns=turns, session_id=session_id,
                               tier=tier), counts


def read_jsonl(path: pathlib.Path) -> list[dict[str, Any]]:
    return [json.loads(l) for l in path.read_text().splitlines() if l.strip()]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--set", dest="set_dir", type=pathlib.Path, required=True)
    ap.add_argument("--out", type=pathlib.Path, required=True,
                    help="the run directory; transcripts land under its artifacts/")
    ap.add_argument("--only", default=None, help="comma-separated qids")
    ap.add_argument("--tier", choices=("T1", "T2"), default="T1",
                    help="T1 tool calls only; T2 also fetches the agent's "
                         "prose. A T1 run scores retrieval and takes no "
                         "verdict, so it has no pass rate to quote")
    ap.add_argument("--mcp-url", required=True)
    ap.add_argument("--hosted-mcp-server", default="credible",
                    help="also the OAuth cache key, so it must match the name "
                         "you authenticated under")
    ap.add_argument("--logs-scope", required=True,
                    help="<organization>/<environment>/<package>")
    ap.add_argument("--logs-model-path", default="logs.malloy")
    ap.add_argument("--source-get-context", default="get_context_calls")
    ap.add_argument("--source-execute", default="tool_calls")
    ap.add_argument("--source-messages", default="agent_messages")
    ap.add_argument("--source-turns", default="agent_turns")
    ap.add_argument("--row-limit", type=int, default=500)
    ap.add_argument("--model", default="sonnet")
    ap.add_argument("--timeout", type=int, default=300)
    a = ap.parse_args(argv)

    parts = a.logs_scope.split("/")
    if len(parts) != 3 or not all(p.strip() for p in parts):
        raise SystemExit("--logs-scope must be <organization>/<environment>/"
                         f"<package>, got {a.logs_scope!r}")
    a.logs_organization, a.logs_environment, a.logs_package = [p.strip() for p in parts]

    cases = read_jsonl(a.set_dir / "cases.jsonl")
    if a.only:
        want = {q.strip() for q in a.only.split(",")}
        cases = [c for c in cases if c["qid"] in want]

    # A case with no `source` names no session, so there is nothing to fetch
    # for it. Named rather than skipped: a set half of which was authored by
    # hand would otherwise produce a run that silently covered half the cases.
    unsourced = [c["qid"] for c in cases if not c.get("source")]
    cases = [c for c in cases if c.get("source")]
    if unsourced:
        print(f"  {len(unsourced)} case(s) carry no `source` session id and "
              f"were skipped: {', '.join(unsourced[:6])}")
    if not cases:
        raise SystemExit("no case carries a `source` session id, so there is "
                         "nothing to fetch. skill:eval-import keeps the "
                         "session or request id there on a log scrape.")

    art = a.out / "artifacts"
    fetched, empty, downgraded = 0, [], []
    for c in cases:
        qid, session = c["qid"], str(c["source"])
        print(f"  {qid}  <- {session}")
        events, counts = fetch_session(a, session)
        lt.write_transcript(events, art / qid / "answerer.jsonl")
        if not (counts["get_context"] or counts["execute"]):
            empty.append(qid)
        else:
            fetched += 1
        if counts["tier"] != a.tier:
            downgraded.append(qid)
        print(f"    {counts['get_context']} get_context, "
              f"{counts['execute']} execute, {counts['messages']} message "
              f"rows, {counts['turns']} turn(s)"
              + (f"  [{counts['tier']}]" if counts["tier"] != a.tier else ""))

    print(f"\n{fetched} of {len(cases)} session(s) had logged calls -> "
          f"{art}")
    if empty:
        # An empty session is not a bad answer. It means the id matched no
        # rows, which is a fetch problem, and scoring it would record an agent
        # that did nothing.
        print(f"! {len(empty)} session(s) returned no calls at all: "
              f"{', '.join(empty[:6])}. Check the id and the window before "
              f"scoring them; an empty transcript is not a wrong answer.")
    if downgraded:
        print(f"! {len(downgraded)} session(s) asked for {a.tier} and kept no "
              f"prose, so they were written as T1 and will take no verdict: "
              f"{', '.join(downgraded[:6])}. If that is all of them, the host "
              f"is not serving --source-messages ({a.source_messages}).")
    print(f"\nNext: python3 run_baseline.py --set {a.set_dir} --out {a.out} "
          f"--rebuild --target platform ...")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
