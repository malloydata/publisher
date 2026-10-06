#!/usr/bin/env python3
"""Rebuild answerer transcripts for agent turns that ran somewhere else. Stdlib only.

  python3 fetch_transcripts.py --set evals/<set> --out <runDir> \
      --mcp-url <scoped hosted url> --hosted-mcp-server <name> \
      --logs-scope <org>/<environment>/<package> --tier T2

Reads the set's cases, takes each case's `source` as the id of the turn that
produced it (the request id `skill:eval-import` tells a log scrape to keep),
fetches that turn's logged calls and prose, and writes
`artifacts/<qid>/answerer.jsonl`. Then:

  python3 run_baseline.py --set <set> --out <runDir> --rebuild ...

That second step is the existing one, unchanged. This script writes no ledger
events and makes no scoring decision; `run_baseline --rebuild` derives the run
from the transcripts exactly as it would from spawned ones.

A TURN, NOT A SESSION

A case is one question, and one question is one turn. A session is a whole
conversation: measured on real in-app traffic, one session held a refusal, a
second question and a third, so fetching "the session" for a case hands the
judge three answers to grade as one. `--unit session` keeps the session
behaviour for a host that logs no turn id.

`--unit window` is for a third-party client (the claude.ai connector, Claude
Desktop) that sends no session or turn id at all. A case's id is then the
logged `get_context` request that carried the person's question, and its calls
are that person's searches and MCP queries after it, until a gap, a search for a
different question, or a cap. That is a reconstruction, not a record, and the
replay guide says what it can claim. No prose is logged for these clients, so a
window case is always T1.

ONE LOGGED ANSWER PER RUN, SO `--source-map` SAYS WHICH

Real traffic repeats itself: one question in a 96-prompt pull was asked seven
times. A run holds one attempt per case, so each logged answer to the same
question is its own run. `--source-map` is a JSON object of qid -> turn id, or
qid -> [turn ids]. With lists, one fetch writes `<out>/attempt-1/`,
`<out>/attempt-2/` ... (the Nth id of every case that has one), each an
ordinary run directory, and `agreement.py --runs <out>/attempt-*` reads them
back together. Without a map, each case's own `source` is used.

HOW IT READS A DATABASE BEHIND OAUTH

Through `hosted_query.py`, which spawns `claude -p` as a transport and reads
rows out of its transcript, never its prose. Each query costs about $0.10, so
every case is fetched in ONE query per source rather than one per case per
source: four spawns for a set, not four per case.

HOST SPECIFICS ARE ARGUMENTS, NOT CODE

`--logs-scope` names the package and `--source-*` name the sources within it.
The defaults are the published Credible request-log package; the column maps
below also accept the older names the access design used. A second host needs
different flags, not a different file. `log_transcript.py` holds the
row-to-transcript rules and no host knowledge at all.
"""
from __future__ import annotations

import argparse
import datetime
import json
import pathlib
import sys
from typing import Any

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent.parent / "eval-answer" / "scripts"))

import hosted_query  # noqa: E402
import log_transcript as lt  # noqa: E402

# A logged get_context call and the retrieval service's own row for it are
# written by two services, so their timestamps differ. Measured on real turns:
# 0.14s apart. Wide enough for clock skew, narrow enough that two searches in
# one turn with identical targets are still told apart by order.
PAIRING_WINDOW_SECONDS = 5.0


def rows_query(source: str, field: str, ids: list[str], limit: int) -> str:
    """The Malloy for every listed id's rows from one logged source.

    `select:` rather than an aggregate because every column is wanted verbatim;
    the caller normalises. Ordered so a truncated page is the OLDEST part of
    the window rather than an arbitrary slice, which is the half a reader can
    still reason about.
    """
    alternatives = " | ".join(f"'{_literal(i)}'" for i in ids)
    return (f"run: {source} -> {{\n"
            f"  where: {field} = {alternatives}\n"
            f"  select: *\n"
            f"  order_by: `timestamp` asc\n"
            f"  limit: {int(limit)}\n"
            f"}}")


def _literal(value: str) -> str:
    """A Malloy string literal body.

    Single quotes and backslashes are escaped rather than stripped: an id is an
    opaque token from a host, and silently rewriting one produces a query for a
    DIFFERENT turn that returns rows and looks like a success.
    """
    return value.replace("\\", "\\\\").replace("'", "\\'")


def window_query(source: str, emails: list[str], start: str, end: str,
                 extra: str | None, limit: int) -> str:
    """The Malloy for every listed person's rows in one time range.

    Bounded by time as well as by person, because these tables are large and a
    person-only filter scans their whole history.
    """
    who = " | ".join(f"'{_literal(e)}'" for e in emails)
    cond = (f"user_email = {who} and `timestamp` >= @{start} "
            f"and `timestamp` <= @{end}")
    if extra:
        cond += f" and ({extra})"
    return (f"run: {source} -> {{\n"
            f"  where: {cond}\n"
            f"  select: *\n"
            f"  order_by: `timestamp` asc\n"
            f"  limit: {int(limit)}\n"
            f"}}")


def fetch_rows(a: argparse.Namespace, source: str, field: str,
               ids: list[str]) -> list[dict[str, Any]]:
    """One source's rows for every listed id, over the hosted MCP."""
    if not ids:
        return []
    return run_query(a, source, rows_query(source, field, ids, a.row_limit))


def run_query(a: argparse.Namespace, source: str,
              query: str) -> list[dict[str, Any]]:
    """One Malloy query against the logs package, through `hosted_query`."""
    target = hosted_query.Target(
        mcp_url=a.mcp_url, server=a.hosted_mcp_server,
        organization=a.logs_organization, environment=a.logs_environment,
        workdir=a.out, model=a.model, timeout=a.timeout)
    rows, cut, err = hosted_query.query(target, a.logs_package,
                                        a.logs_model_path, query)
    if cut:
        # A silently truncated page makes an agent look decisive: its later
        # calls vanish and the earlier ones read as the whole turn.
        print(f"    ! {source}: hit the {a.row_limit}-row limit, so the newest "
              f"rows are missing. Raise --row-limit or fetch fewer cases.")
    if err:
        print(f"    ! {source}: {err[:200]}")
    return rows


# Each builder key maps to the host columns that can carry it, first present
# wins. The first name is the published Credible request-log package; later
# ones are the names the access design used before it shipped.
GET_CONTEXT_COLUMNS = {"request_id": ("request_id",),
                       "session_id": ("session_id",),
                       "user_email": ("user_email",),
                       "organization_id": ("organization_id",),
                       "user_prompt": ("user_prompt",),
                       "timestamp": ("timestamp",),
                       "request_payload": ("request_json", "request_payload"),
                       "response": ("response_json", "response_body")}
TOOL_COLUMNS = {"request_id": ("request_id",), "session_id": ("session_id",),
                "user_email": ("user_email",),
                "organization_id": ("organization_id",),
                "timestamp": ("timestamp",), "tool": ("tool",),
                "request_payload": ("arguments", "request_json",
                                    "request_payload"),
                "outcome": ("outcome",),
                "error": ("error_message", "response_body")}
MESSAGE_COLUMNS = {"request_id": ("request_id",),
                   "turn_started_ms": ("turn_started_ms",), "seq": ("seq",),
                   "chunk": ("chunk",), "role": ("role",), "text": ("text",)}
TURN_COLUMNS = {"request_id": ("request_id",), "session_id": ("session_id",),
                "timestamp": ("timestamp",),
                "duration_seconds": ("duration_seconds",),
                "input_tokens": ("input_tokens",),
                "output_tokens": ("output_tokens",),
                "cache_read_tokens": ("cache_read_input_tokens",),
                "cache_write_tokens": ("cache_creation_input_tokens",),
                "round_trips": ("round_trips",), "outcome": ("outcome",)}


def normalise(rows: list[dict[str, Any]],
              mapping: dict[str, tuple[str, ...]]) -> list[dict[str, Any]]:
    """Host columns renamed to the keys `log_transcript` takes.

    A column the host does not have is ABSENT from the result, not None: the
    builder distinguishes "not recorded" from "recorded as nothing", and
    filling every gap with None would erase that.
    """
    out = []
    for r in rows:
        row = {}
        for key, sources in mapping.items():
            src = next((s for s in sources if r.get(s) is not None), None)
            if src is not None:
                row[key] = r[src]
        out.append(row)
    return out


def retrieval_body(value: Any) -> Any:
    """The retrieval response body out of a logged get_context response.

    Credible logs the whole HTTP response (`status_code`, timing, and the body
    under `response_body`); an older shape logged the body alone. `mcp_payload`
    reads the body, so the envelope is removed here and nowhere else.
    """
    parsed = lt._as_dict(value) if not isinstance(value, dict) else value
    if "response_body" in parsed:
        return parsed["response_body"]
    return value


def _seconds(ts: Any) -> float | None:
    if not isinstance(ts, str) or not ts:
        return None
    try:
        return datetime.datetime.fromisoformat(
            ts.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def _targets(payload: Any) -> Any:
    return lt._as_dict(payload).get("search_targets")


def pair_retrieval(calls: list[dict[str, Any]],
                   responses: list[dict[str, Any]]) -> int:
    """Attach the retrieval service's response to each agent get_context call.

    The agent's tool log is the authority on WHICH calls a turn made, in what
    order, and whether they failed. The retrieval service's log is the only
    place the ranked results live, under its own request id. A response is
    attached when it is in the same session, within the window, and searched
    for the same targets; each response is used once. A failed call gets none,
    because the service may never have seen it.

    Returns how many successful calls found no response. Those calls still
    count as calls; their retrieval is unmeasured, not empty.
    """
    used: set[int] = set()
    unmatched = 0
    for call in calls:
        if call.get("error") is not None:
            continue
        t = _seconds(call.get("timestamp"))
        want = _targets(call.get("request_payload"))
        hit = None
        for i, r in enumerate(responses):
            if i in used or r.get("session_id") != call.get("session_id"):
                continue
            rt = _seconds(r.get("timestamp"))
            if t is None or rt is None or abs(rt - t) > PAIRING_WINDOW_SECONDS:
                continue
            if _targets(r.get("request_payload")) != want:
                continue
            hit = i
            break
        if hit is None:
            unmatched += 1
            continue
        used.add(hit)
        call["response"] = retrieval_body(responses[hit].get("response"))
    return unmatched


def split_tool_calls(rows: list[dict[str, Any]]
                     ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """An agent tool log split into get_context calls and execute_query calls.

    Matched on the tool name's suffix, because the server segment varies with
    the client (`mcp__credible__`, `mcp__credible_analysis__`). Other tools the
    agent called are not Malloy calls and are left out. A row that names no
    tool at all comes from a source that logs only executions, as the older
    shape did, and is taken as one.

    A call that did not end `ok` carries its error, so `pick_final_query` does
    not hand the judge a query the server rejected.
    """
    gc, ex = [], []
    for r in rows:
        tool = r.get("tool")
        outcome = r.get("outcome")
        if outcome is not None and outcome != "ok" and r.get("error") is None:
            r["error"] = outcome
        elif outcome == "ok":
            r.pop("error", None)
        if tool is None or tool.endswith("__execute_query"):
            ex.append(r)
        elif tool.endswith("__get_context"):
            gc.append(r)
    return gc, ex


def build_case(case_id: str, tier: str, *, tools: list[dict[str, Any]],
               retrieval: list[dict[str, Any]], messages: list[dict[str, Any]],
               turns: list[dict[str, Any]], session_id: str | None,
               tool_log_names_tools: bool = True
               ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """One case's rows as a transcript, plus what each source contributed.

    Pure: every row is already fetched and already belongs to this case.

    `tool_log_names_tools` is a fact about the SOURCE, decided once over the
    whole batch, not per case: a turn that called no tool at all (a refusal)
    has no rows to read it from, and guessing it per case would treat every
    search elsewhere in that session as this turn's.
    """
    gc, ex = split_tool_calls(tools)
    if tool_log_names_tools:
        unmatched = pair_retrieval(gc, retrieval)
    else:
        # A tool log with no tool column logs executions only, so the
        # retrieval rows ARE the get_context calls, as in the older shape.
        # Only meaningful per session: that shape has no turn id.
        gc = [dict(r, response=retrieval_body(r.get("response")))
              for r in retrieval]
        unmatched = 0
    # A messages source with no role column holds the agent's replies and
    # nothing else, so an absent role is the assistant's.
    for m in messages:
        m.setdefault("role", "assistant")
    # A T2 fetch that came back with no prose did not get T2. Either the host
    # does not serve that table yet or this turn has none, and in both cases
    # claiming `answer_captured: true` over an empty answer is the exact
    # failure this tier exists to prevent: the judge would compare "" against
    # the golden and score a real agent as having said nothing. Downgrade the
    # CASE rather than the run, so a set where only some turns kept their
    # prose still scores the ones that did.
    if tier == "T2" and not any((m.get("text") or "").strip() for m in messages):
        tier = "T1"
    counts = {"get_context": len(gc), "execute": len(ex),
              "messages": len(messages), "turns": len(turns), "tier": tier,
              "unpaired_retrieval": unmatched}
    return lt.build_transcript(get_context=gc, execute=ex, messages=messages,
                               turns=turns, session_id=session_id,
                               tier=tier), counts


def fetch_all(a: argparse.Namespace, ids: list[str]
              ) -> dict[str, list[dict[str, Any]]]:
    """Every source's rows for every id: one query per source.

    In turn mode the turn rows are always fetched, even at T1, because they
    carry the session id that the retrieval service's rows are found by.
    """
    field = "request_id" if a.unit == "turn" else "session_id"
    turns = normalise(fetch_rows(a, a.source_turns, field, ids), TURN_COLUMNS) \
        if (a.unit == "turn" or a.tier in ("T2", "T3")) else []
    tools = normalise(fetch_rows(a, a.source_execute, field, ids), TOOL_COLUMNS)
    if a.unit == "turn":
        sessions = sorted({t["session_id"] for t in turns
                           if t.get("session_id")})
    else:
        sessions = ids
    retrieval = normalise(
        fetch_rows(a, a.source_get_context, "session_id", sessions),
        GET_CONTEXT_COLUMNS)
    messages: list[dict[str, Any]] = []
    # Only at a tier that claims them. Asking a host for a table it has not
    # shipped returns an error, which reads in the log like a broken fetch
    # rather than a tier the operator chose.
    if a.tier in ("T2", "T3"):
        messages = normalise(fetch_rows(a, a.source_messages, field, ids),
                             MESSAGE_COLUMNS)
    return {"turns": turns, "tools": tools, "retrieval": retrieval,
            "messages": messages}


def rows_for(a: argparse.Namespace, data: dict[str, list[dict[str, Any]]],
             case_id: str) -> dict[str, Any]:
    """The slice of the batch that belongs to one case."""
    key = "request_id" if a.unit == "turn" else "session_id"
    turns = [r for r in data["turns"] if r.get(key) == case_id]
    session = (turns[0].get("session_id") if a.unit == "turn" and turns
               else case_id if a.unit == "session" else None)
    return {"tools": [dict(r) for r in data["tools"] if r.get(key) == case_id],
            "retrieval": [r for r in data["retrieval"]
                          if r.get("session_id") == session],
            "messages": [dict(r) for r in data["messages"]
                         if r.get(key) == case_id],
            "turns": turns, "session_id": session}


def _no_session(row: dict[str, Any]) -> bool:
    return not row.get("session_id")


def window_rows(anchor: dict[str, Any], searches: list[dict[str, Any]],
                queries: list[dict[str, Any]], *, gap_minutes: float,
                max_minutes: float) -> tuple[list[dict[str, Any]],
                                             list[dict[str, Any]]]:
    """The searches and queries one question's window holds. Pure.

    Starts at the anchor search and walks forward through the same person's
    session-less rows in the same organization, in time order. It stops at the
    first of: a gap longer than `gap_minutes` since the last kept row, a row
    past `max_minutes` from the anchor, or a search that carried a DIFFERENT
    non-empty question, which is the next case's anchor. The anchor itself is
    the first search.

    A row with a session id came from the in-app agent and is never taken: that
    traffic has its own turn id and its own unit.
    """
    t0 = _seconds(anchor.get("timestamp"))
    if t0 is None:
        return [], []
    me, org = anchor.get("user_email"), anchor.get("organization_id")
    prompt = (anchor.get("user_prompt") or "").strip()
    # The anchor is the window's first search even if the range query missed
    # it: it is the row the case is named after.
    if not any(r.get("request_id") == anchor.get("request_id")
               for r in searches):
        searches = [anchor] + list(searches)
    pool = [("s", r) for r in searches] + [("q", r) for r in queries]
    pool = [(k, r) for k, r in pool
            if r.get("user_email") == me and _no_session(r)
            and (org is None or r.get("organization_id") in (None, org))
            and (_seconds(r.get("timestamp")) or -1) >= t0]
    pool.sort(key=lambda kr: (_seconds(kr[1].get("timestamp")),
                              kr[1].get("request_id") != anchor.get("request_id")))
    kept_s, kept_q, last = [], [], t0
    for kind, r in pool:
        t = _seconds(r.get("timestamp"))
        if t - t0 > max_minutes * 60 or t - last > gap_minutes * 60:
            break
        other = (r.get("user_prompt") or "").strip()
        if kind == "s" and r.get("request_id") != anchor.get("request_id") \
                and other and other != prompt:
            break
        (kept_s if kind == "s" else kept_q).append(r)
        last = t
    return kept_s, kept_q


def _malloy_ts(seconds: float) -> str:
    return datetime.datetime.fromtimestamp(
        seconds, datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def fetch_windows(a: argparse.Namespace, ids: list[str]
                  ) -> dict[str, tuple[list[dict[str, Any]],
                                       list[dict[str, Any]]]]:
    """anchor id -> (searches, queries), from three queries in all."""
    anchors = normalise(fetch_rows(a, a.source_get_context, "request_id", ids),
                        GET_CONTEXT_COLUMNS)
    times = [s for s in (_seconds(r.get("timestamp")) for r in anchors)
             if s is not None]
    emails = sorted({r["user_email"] for r in anchors if r.get("user_email")})
    if not times or not emails:
        return {}
    start = _malloy_ts(min(times) - 1)
    end = _malloy_ts(max(times) + a.window_max_minutes * 60 + 1)
    searches = normalise(run_query(a, a.source_get_context, window_query(
        a.source_get_context, emails, start, end, None, a.row_limit)),
        GET_CONTEXT_COLUMNS)
    surface = (f"surface = '{_literal(a.window_surface)}'"
               if a.window_surface else None)
    queries = normalise(run_query(a, a.source_queries, window_query(
        a.source_queries, emails, start, end, surface, a.row_limit)),
        TOOL_COLUMNS)
    out = {}
    for anchor in anchors:
        out[anchor["request_id"]] = window_rows(
            anchor, searches, queries, gap_minutes=a.window_gap_minutes,
            max_minutes=a.window_max_minutes)
    return out


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
    ap.add_argument("--unit", choices=("turn", "session", "window"),
                    default="turn",
                    help="what a case's id names: one agent turn (its request "
                         "id), a whole session for a host with no turn id, or "
                         "the search that opened a third-party client's "
                         "window (always T1)")
    ap.add_argument("--source-queries", default="query_executions",
                    help="--unit window: the source holding MCP query runs")
    ap.add_argument("--window-surface", default="mcp",
                    help="--unit window: keep only query rows on this surface; "
                         "'' keeps all")
    ap.add_argument("--window-gap-minutes", type=float, default=30.0)
    ap.add_argument("--window-max-minutes", type=float, default=120.0)
    ap.add_argument("--source-map", type=pathlib.Path, default=None,
                    help="JSON object qid -> id or [ids], overriding each "
                         "case's `source`. A list writes one run directory "
                         "per position under --out (attempt-1, attempt-2 ...)")
    ap.add_argument("--mcp-url", required=True)
    ap.add_argument("--hosted-mcp-server", default="credible",
                    help="also the OAuth cache key, so it must match the name "
                         "you authenticated under")
    ap.add_argument("--logs-scope", required=True,
                    help="<organization>/<environment>/<package>")
    ap.add_argument("--logs-model-path", default="request_logs.malloy")
    ap.add_argument("--source-get-context", default="get_context_calls")
    ap.add_argument("--source-execute", default="agent_tool_calls")
    ap.add_argument("--source-messages", default="agent_replies")
    ap.add_argument("--source-turns", default="conversations")
    ap.add_argument("--row-limit", type=int, default=5000)
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

    overrides: dict[str, list[str]] = {}
    repeated = False
    if a.source_map:
        raw = json.loads(a.source_map.read_text())
        repeated = any(isinstance(v, list) for v in raw.values())
        overrides = {q: [str(x) for x in (v if isinstance(v, list) else [v])]
                     for q, v in raw.items()}
        if a.only is None:
            # A map names the cases this run replays. Cases it leaves out have
            # no logged answer for this run, not their default one.
            cases = [c for c in cases if c["qid"] in overrides]

    def ids_of(c: dict[str, Any]) -> list[str]:
        if c["qid"] in overrides:
            return overrides[c["qid"]]
        return [str(c["source"])] if c.get("source") else []

    # A case with no id names no turn, so there is nothing to fetch for it.
    # Named rather than skipped: a set half of which was authored by hand
    # would otherwise produce a run that silently covered half the cases.
    unsourced = [c["qid"] for c in cases if not ids_of(c)]
    cases = [c for c in cases if ids_of(c)]
    if unsourced:
        print(f"  {len(unsourced)} case(s) carry no `source` id and were "
              f"skipped: {', '.join(unsourced[:6])}")
    if not cases:
        raise SystemExit("no case carries a `source` id, so there is nothing "
                         "to fetch. skill:eval-import keeps the request id "
                         "there on a log scrape.")

    ids = sorted({i for c in cases for i in ids_of(c)})
    print(f"  fetching {len(ids)} {a.unit}(s) from {a.logs_scope}")
    if a.unit == "window" and a.tier != "T1":
        # These clients log no prose. Asking for T2 would downgrade every case
        # and read as a broken fetch; say it once instead.
        print(f"  --unit window logs no prose, so this is a T1 fetch, not "
              f"{a.tier}: retrieval only, no verdict")
        a.tier = "T1"
    windows = fetch_windows(a, ids) if a.unit == "window" else {}
    data = fetch_all(a, ids) if a.unit != "window" else {}
    depth = max(len(ids_of(c)) for c in cases)
    # (run directory, case, id): one run per position when the map holds
    # lists, so every repeat is scored by the unchanged run_baseline.
    work = [((a.out / f"attempt-{k + 1}") if repeated else a.out, c,
             ids_of(c)[k])
            for k in range(depth) for c in cases if k < len(ids_of(c))]

    names_tools = any(r.get("tool") for r in data.get("tools", []))
    if a.unit == "turn" and data["tools"] and not names_tools:
        raise SystemExit(f"--source-execute ({a.source_execute}) has no tool "
                         "column, so it cannot say which of a turn's calls "
                         "were searches. Use --unit session for that shape.")

    fetched, empty, downgraded, unpaired = 0, [], [], 0
    for run_dir, c, cid in work:
        qid = c["qid"]
        art = run_dir / "artifacts"
        if a.unit == "window":
            # The searches ARE the calls (no agent log names them), and the
            # query rows name no tool, so both take the older-shape path.
            searches, queries = windows.get(cid, ([], []))
            events, counts = build_case(cid, a.tier, tools=[dict(q) for q in queries],
                                        retrieval=searches, messages=[],
                                        turns=[], session_id=None,
                                        tool_log_names_tools=False)
        else:
            part = rows_for(a, data, cid)
            events, counts = build_case(cid, a.tier, tools=part["tools"],
                                        retrieval=part["retrieval"],
                                        messages=part["messages"],
                                        turns=part["turns"],
                                        session_id=part["session_id"],
                                        tool_log_names_tools=names_tools)
        lt.write_transcript(events, art / qid / "answerer.jsonl")
        where = f"{run_dir.name}/" if repeated else ""
        if not (counts["get_context"] or counts["execute"] or counts["messages"]):
            empty.append(f"{where}{qid}")
        else:
            fetched += 1
        if counts["tier"] != a.tier:
            downgraded.append(f"{where}{qid}")
        unpaired += counts["unpaired_retrieval"]
        print(f"  {where}{qid}  <- {cid}: {counts['get_context']} get_context, "
              f"{counts['execute']} execute, {counts['messages']} message "
              f"rows, {counts['turns']} turn(s)"
              + (f"  [{counts['tier']}]" if counts["tier"] != a.tier else ""))

    print(f"\n{fetched} of {len(work)} logged answer(s) had activity -> "
          f"{a.out}")
    if empty:
        # An empty turn is not a bad answer. It means the id matched no rows,
        # which is a fetch problem, and scoring it would record an agent that
        # did nothing.
        print(f"! {len(empty)} case(s) returned nothing at all: "
              f"{', '.join(empty[:6])}. Check the id and the window before "
              f"scoring them; an empty transcript is not a wrong answer.")
    if downgraded:
        print(f"! {len(downgraded)} case(s) asked for {a.tier} and kept no "
              f"prose, so they were written as T1 and will take no verdict: "
              f"{', '.join(downgraded[:6])}. If that is all of them, the host "
              f"is not serving --source-messages ({a.source_messages}).")
    if unpaired:
        print(f"! {unpaired} get_context call(s) found no retrieval response "
              f"in {a.source_get_context}; their retrieval is unmeasured, "
              f"not empty.")
    runs = sorted({w[0] for w in work})
    print("\nNext, score each run:")
    for r in runs:
        print(f"  python3 run_baseline.py --set {a.set_dir} --out {r} "
              f"--rebuild --rejudge --target platform ...")
    if repeated:
        print(f"then: python3 agreement.py --runs {a.out}/attempt-*")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
