#!/usr/bin/env python3
"""Turn query logs into a workload file a query replay can time. Stdlib only.

  python3 workload_from_logs.py --package storefront --publisher-log server.log > workload.jsonl
  python3 workload_from_logs.py --package storefront --rows executions.jsonl > workload.jsonl
  python3 eval.py query-replay --set <set-dir> --queries workload.jsonl --label perf-01

Step 2 of the optimization loop (reference/optimization-loop.md), for speed:
the queries people actually ran, each with how often it ran, in the format
`query-replay --queries` reads (one JSON object per line: `query`, `count`,
`modelPath`, `givens`).

Two inputs:

- **`--publisher-log`**: a Publisher server log at debug level
  (`PUBLISHER_LOGGING_LEVEL=debug`). Each query request is logged with its
  body (`payload`: the query text or the named query, and the givens) and its
  model path in the URL. Only at debug level: at info the body is left out,
  and the access record, which is logged at info, never holds the query text.
- **`--rows`**: a JSONL file of executed queries from anywhere else, such as
  Credible's request logs (the `query_executions` source of the logs
  package). Each line needs `query` (or `queryName` with an optional
  `sourceName`), `modelPath` and `package`, and may carry `givens`,
  `givenKeys` (given names without values) and `count`.

A query that names givens without their values cannot be replayed as it ran:
it is left out and counted, never guessed. Failed queries (status not 2xx)
are left out. Identical queries (same text, model and givens) are counted
once with their total. A summary goes to stderr.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from typing import Any, Iterable

ANSI = re.compile(r"\x1b\[[0-9;]*m")
QUERY_LINE = re.compile(
    r"POST /api/v0/environments/(?P<env>[^/\s]+)/packages/(?P<pkg>[^/\s]+)"
    r"/models/(?P<model>\S+?)/query\s+(?P<json>\{.*\})\s*$")


def query_text(payload: dict[str, Any]) -> str | None:
    """The Malloy to replay: ad-hoc text as sent, a named query or view rebuilt."""
    if isinstance(payload.get("query"), str) and payload["query"].strip():
        return payload["query"]
    name = payload.get("queryName")
    if not isinstance(name, str) or not name:
        return None
    src = payload.get("sourceName")
    return f"run: {src} -> {name}" if isinstance(src, str) and src else f"run: {name}"


def from_publisher_log(lines: Iterable[str], package: str) -> Iterable[dict[str, Any]]:
    for raw in lines:
        m = QUERY_LINE.search(ANSI.sub("", raw).rstrip("\n"))
        if not m or m["pkg"] != package:
            continue
        try:
            rec = json.loads(m["json"])
        except ValueError:
            continue
        payload = rec.get("payload")
        if not isinstance(payload, dict):
            yield {"skip": "no request body (the log is not at debug level)"}
            continue
        if not 200 <= int(rec.get("statusCode") or 0) < 300:
            yield {"skip": "the query failed"}
            continue
        yield {"query": query_text(payload), "modelPath": m["model"],
               "givens": payload.get("givens") or None, "count": 1}


def from_rows(lines: Iterable[str], package: str) -> Iterable[dict[str, Any]]:
    for raw in lines:
        if not raw.strip():
            continue
        r = json.loads(raw)
        if r.get("package") != package:
            continue
        if r.get("givenKeys") and not r.get("givens"):
            yield {"skip": "names givens without their values"}
            continue
        yield {"query": query_text(r), "modelPath": r.get("modelPath"),
               "givens": r.get("givens") or None, "count": int(r.get("count") or 1)}


def workload(items: Iterable[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """(one line per distinct query with its count, most frequent first;
    how many of each kind were left out)."""
    merged: dict[tuple[str, str, str], dict[str, Any]] = {}
    skipped: dict[str, int] = {}
    for it in items:
        if "skip" in it:
            skipped[it["skip"]] = skipped.get(it["skip"], 0) + 1
            continue
        if not it.get("query") or not it.get("modelPath"):
            skipped["no query text or model path"] = skipped.get("no query text or model path", 0) + 1
            continue
        key = (it["query"], it["modelPath"], json.dumps(it.get("givens"), sort_keys=True))
        if key in merged:
            merged[key]["count"] += it["count"]
        else:
            merged[key] = {k: v for k, v in it.items() if v is not None}
    out = sorted(merged.values(), key=lambda w: (-w["count"], w["query"]))
    return out, skipped


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--package", required=True, help="the package whose queries to keep")
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--publisher-log", help="a Publisher server log at debug level")
    src.add_argument("--rows", help="a JSONL file of executed queries")
    a = ap.parse_args(argv)
    path = a.publisher_log or a.rows
    with open(path, encoding="utf-8", errors="replace") as f:
        items = (from_publisher_log(f, a.package) if a.publisher_log else from_rows(f, a.package))
        lines, skipped = workload(items)
    for w in lines:
        print(json.dumps(w))
    total = sum(w["count"] for w in lines)
    print(f"{len(lines)} distinct queries ({total} runs) on {a.package}"
          + "".join(f"; left out {n}: {why}" for why, n in sorted(skipped.items())), file=sys.stderr)
    if not lines and skipped.get("no request body (the log is not at debug level)"):
        print("Fix: restart Publisher with PUBLISHER_LOGGING_LEVEL=debug for the capture window",
              file=sys.stderr)
    return 0 if lines else 1


if __name__ == "__main__":
    sys.exit(main())
