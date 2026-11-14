#!/usr/bin/env python3
"""Rebuild an answerer transcript from a host's request-log rows. Stdlib only.

WHY A TRANSCRIPT AND NOT A NEW LEDGER WRITER

`run_baseline.py --rebuild` already derives an entire run from
`artifacts/<qid>/answerer.jsonl` while calling no model and no server, and
`derive_attempt` is pure over that file. So the cheapest honest way to score a
session that ran somewhere else is to put it in that file's shape and let the
existing parser do the rest. No second parser, no second ledger writer, and no
risk of the two disagreeing about what a `tool_call` is.

That shape is the Claude CLI's `--output-format stream-json`. For a first-party
app driven by the Claude Agent SDK this is not even a translation: the stored
session document IS that JSONL. For everything else -- a machine integration, a
third-party MCP client -- there was never a transcript, and what we build here
is a faithful record of the calls the host logged and an explicit admission
about what it did not.

WHAT THE PROVENANCE LINE IS FOR

The first event is `{"type": "provenance", ...}`, which nothing the CLI emits
carries. It states what the SOURCE could record, not what the agent did:

  answer_captured  false when the host logged no assistant prose. The agent may
                   have answered perfectly. `run_baseline` turns this into
                   `verdict: null, reason: no_answer_captured` rather than
                   letting the judge compare "" against the golden and call the
                   model wrong.
  host_log         false when there was no host-side tool log, so a Read of a
                   gold CSV could not have been seen. Becomes
                   `contaminated: "unknown"`, which is the ledger's own word
                   for it.

Saying this in the transcript rather than in a side file is what keeps
`derive_attempt` pure and what makes a `--rebuild` a year from now derive the
same ledger.

WHAT THE ROWS LOOK LIKE

Deliberately generic. The caller normalises its host's columns into these dicts
first, so no vendor's table names reach this module and a second host needs a
new caller rather than a new branch here.

  get_context : {request_id, session_id, timestamp, request_payload, response}
  execute     : {request_id, session_id, timestamp, request_payload, error}
  message     : {request_id, turn_started_ms, seq, chunk, role, text}
  turn        : {request_id, input_tokens, output_tokens, cache_read_tokens,
                 cache_write_tokens, round_trips, cost_usd, outcome}

`request_payload` is the tool's own arguments, as the caller sent them, and
`response` is the retrieval response body verbatim. Both are passed through
untouched: `mcp_payload` already reads the hosted `get_context` shape, and
re-encoding here would be a second place for that knowledge to live.

`error` is the one field worth arguing for. Hosts commonly log a query's
COMPILE ERROR while logging nothing for a successful one, and that asymmetry is
enough: it is precisely the failures that have to be known, because the final
query is chosen as the last call the server answered. Pass it whenever the host
has it.
"""
from __future__ import annotations

import json
from typing import Any, Iterable

PROVENANCE = "provenance"

# The tool names `run_baseline` matches on. It accepts any name ENDING in these,
# so the server segment is free; `publisher` keeps a rebuilt transcript reading
# like a local one rather than inventing a vendor prefix that means nothing to
# the parser.
GET_CONTEXT = "mcp__publisher__get_context"
EXECUTE_QUERY = "mcp__publisher__execute_query"


def _as_dict(value: Any) -> dict[str, Any]:
    """A payload column that may arrive as JSON text or as a parsed object.

    Returns {} for anything unreadable rather than raising. A single malformed
    row should cost that row's call, not the whole session: the alternative is
    a fetch that dies on one truncated payload and reports nothing.
    """
    if isinstance(value, dict):
        return value
    if isinstance(value, (str, bytes)):
        try:
            parsed = json.loads(value)
        except (json.JSONDecodeError, UnicodeDecodeError):
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _sort_key(row: dict[str, Any]) -> tuple:
    """Order calls within a session.

    `observed_ms` when the host records it, else the timestamp. Ties keep input
    order, which is what `sorted` guarantees, so two calls logged in the same
    millisecond stay in the order the host returned them rather than swapping
    between fetches.
    """
    return (row.get("observed_ms") if row.get("observed_ms") is not None
            else row.get("timestamp") or "",)


def _tool_use(call_id: str, name: str, payload: dict[str, Any]) -> dict[str, Any]:
    return {"type": "assistant", "message": {"content": [
        {"type": "tool_use", "id": call_id, "name": name, "input": payload}]}}


def _tool_result(call_id: str, body: Any,
                 error: str | None = None) -> dict[str, Any]:
    """One tool result. `error` marks the call as having FAILED.

    Whether a call succeeded is not cosmetic. `pick_final_query` chooses the
    attempt's final query as the last one the server ANSWERED, so a transcript
    in which every call looks successful hands the judge whatever ran last --
    which, for an agent that made a syntax error and then fixed it, is the
    BROKEN query. Measured on a real 8-case arm replayed without per-call
    outcomes: 4 of 8 attempts changed their final query, and one was handed a
    query whose aggregate list was semicolon-separated and had errored.

    So a host that logs an error for a call must pass it here, and one that
    logs nothing at all gets `final_query_source: last` and should read it as
    the warning it already is.
    """
    # Serialised as the bare JSON body a hosted MCP returns. `resource_json`
    # already accepts that form alongside Publisher's "[Resource from ...]"
    # preamble, so no wrapper is invented here.
    text = (error if error is not None
            else body if isinstance(body, str) else json.dumps(body or {}))
    block: dict[str, Any] = {"type": "tool_result", "tool_use_id": call_id,
                             "content": [{"type": "text", "text": text}]}
    if error is not None:
        block["is_error"] = True
    return {"type": "user", "message": {"content": [block]}}


def assistant_text(messages: Iterable[dict[str, Any]]) -> str:
    """The agent's visible prose for a session, in the order it was shown.

    Chunked rows are reassembled: a host that splits a long message caps each
    chunk at a byte limit, so a chunk boundary is not a sentence boundary and
    joining with anything but the empty string corrupts the text. Turns and
    messages are joined with a blank line, matching how `run_baseline` joins an
    answerer's own text blocks.
    """
    rows = [m for m in messages if (m.get("role") or "") == "assistant"]
    rows.sort(key=lambda m: (m.get("turn_started_ms") or 0,
                             m.get("seq") or 0, m.get("chunk") or 0))
    out: list[str] = []
    for (_turn, _seq), group in _grouped(rows):
        joined = "".join(r.get("text") or "" for r in group)
        if joined.strip():
            out.append(joined)
    return "\n\n".join(out)


def _grouped(rows: list[dict[str, Any]]):
    """Chunks regrouped into the messages they were split from."""
    current: tuple | None = None
    batch: list[dict[str, Any]] = []
    for r in rows:
        key = (r.get("turn_started_ms") or 0, r.get("seq") or 0)
        if key != current and batch:
            yield current, batch
            batch = []
        current, batch = key, batch + [r]
    if batch:
        yield current, batch


def build_transcript(*, get_context: Iterable[dict[str, Any]] = (),
                     execute: Iterable[dict[str, Any]] = (),
                     messages: Iterable[dict[str, Any]] = (),
                     turns: Iterable[dict[str, Any]] = (),
                     session_id: str | None = None,
                     tier: str = "T1") -> list[dict[str, Any]]:
    """One session's rows as a stream-json transcript.

    The order is: provenance, then every tool call in the order the host
    observed them, then the agent's prose, then one result event carrying the
    session's totals. Prose last because `run_baseline` takes the answer as the
    concatenation of the assistant text blocks and the final query as the last
    query the server answered; a transcript that interleaved them would change
    neither, and this order is the one a reader can follow.
    """
    calls = ([dict(r, _tool=GET_CONTEXT) for r in get_context]
             + [dict(r, _tool=EXECUTE_QUERY) for r in execute])
    calls.sort(key=_sort_key)

    text = assistant_text(messages)
    events: list[dict[str, Any]] = [{
        "type": PROVENANCE,
        "source": "logs",
        "tier": tier,
        "session_id": session_id,
        # Both are facts about the SOURCE. `answer_captured` is whether any
        # prose was recorded at all, which is not the same as whether this
        # session happened to produce some, so it is driven by the tier the
        # caller established and not by whether `text` came back empty.
        "answer_captured": tier in ("T2", "T3"),
        "host_log": tier == "T3",
    }]

    for i, row in enumerate(calls, 1):
        call_id = f"{row.get('request_id') or 'req'}-{i}"
        events.append(_tool_use(call_id, row["_tool"],
                                _as_dict(row.get("request_payload"))))
        events.append(_tool_result(call_id, row.get("response"),
                                   row.get("error")))

    if text:
        events.append({"type": "assistant", "message": {
            "content": [{"type": "text", "text": text}]}})

    events.append(_result_event(turns))
    return events


def _result_event(turns: Iterable[dict[str, Any]]) -> dict[str, Any]:
    """The session's totals, in the shape `usage_fields` already reads.

    Summed across turns, because a session is many turns and the ledger's
    attempt is one row. A host that records none of this leaves the counts
    null rather than zero: zero is a measurement and null is the absence of
    one, and a run that reported 0 tokens for a real session would be read as
    a free answer.
    """
    rows = list(turns)
    if not rows:
        return {"type": "result", "usage": {}, "num_turns": None,
                "total_cost_usd": None}

    def total(field: str) -> int | None:
        seen = [r.get(field) for r in rows if r.get(field) is not None]
        return sum(seen) if seen else None

    # `outcome` is the host's word for how the turn ended. Anything but a clean
    # completion is an environment fact, and `run_baseline` refuses to judge an
    # attempt carrying `error`, which is the right answer here too: a session
    # the host recorded as disconnected has an answer that stops mid-thought.
    bad = next((r.get("outcome") for r in rows
                if r.get("outcome") not in (None, "completed")), None)

    return {"type": "result",
            "usage": {"input_tokens": total("input_tokens"),
                      "output_tokens": total("output_tokens"),
                      "cache_read_input_tokens": total("cache_read_tokens"),
                      "cache_creation_input_tokens": total("cache_write_tokens")},
            "num_turns": total("round_trips"),
            "total_cost_usd": total("cost_usd"),
            "is_error": bad is not None,
            "subtype": bad}


def write_transcript(events: list[dict[str, Any]], path) -> None:
    """One JSON object per line, the format `--rebuild` reads."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(e) + "\n" for e in events))
