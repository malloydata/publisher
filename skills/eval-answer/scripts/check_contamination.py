#!/usr/bin/env python3
"""Score-time contamination check. Stdlib only.

An attempt is contaminated if the host-side tool-use log touched an eval path,
gold artifact, or the model file under test, or if the answerer reported more
MCP calls than the host observed as tool uses.

  python check_contamination.py --log tool_uses.json [options]

All path matching is PLAIN SUBSTRING containment against the flattened tool
input, never glob expansion. --gold-paths and --eval-paths EXTEND the built-in
defaults rather than replacing them, so passing extra paths can only widen the
check.

Exit 0 always when the check itself ran. Exit 1 if the log cannot be read.
A contaminated attempt is data for the grader, not a script failure.

The log is a JSON object:

  {
    "toolUses": [
      {"name": "Read", "input": {"path": "..."}},
      {"name": "Shell", "input": {"command": "..."}},
      {"name": "malloy_getContext", "input": {}}
    ],
    "reportedCalls": 4
  }

`reportedCalls` may also be passed as --reported-calls.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any


DEFAULT_GOLD_PATHS = (
    "evals/",
    "results/gold/",
    "gold/",
    "cases.jsonl",
    "intents.jsonl",
    "set.json",
    "events.jsonl",
    "judge-regressions",
)
DEFAULT_EVAL_PATHS = (
    "publisher.db",
    "mcp_traces",
)
HOST_FILE_TOOLS = (
    "read",
    "write",
    "edit",
    "grep",
    "glob",
    "shell",
    "bash",
)


def _tool_name(entry: dict[str, Any]) -> str:
    return str(entry.get("name") or entry.get("tool") or "")


def _is_host_file_tool(name: str) -> bool:
    n = name.lower()
    return any(n == t or n.endswith(f"_{t}") or n.endswith(f"-{t}") for t in HOST_FILE_TOOLS)


def _is_mcp_transport(haystack: str) -> bool:
    """A shell command that is itself an MCP call (curl JSON-RPC to /mcp).

    The modelPath argument on an MCP execute_query is required, not a file
    read, and that stays true when the MCP call rides over Shell instead of
    a native tool. Gold and eval-path needles still apply to these entries;
    only the model-path needle is exempted.
    """
    return "tools/call" in haystack and "/mcp" in haystack


def _flatten_input(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, (int, float, bool)):
        return str(value)
    if isinstance(value, dict):
        return " ".join(_flatten_input(v) for v in value.values())
    if isinstance(value, list):
        return " ".join(_flatten_input(v) for v in value)
    return str(value)


def _haystack(entry: dict[str, Any]) -> str:
    name = str(entry.get("name") or entry.get("tool") or "")
    raw = entry.get("input") or entry.get("arguments") or entry.get("args") or ""
    return f"{name} {_flatten_input(raw)}".lower()


def _matches(haystack: str, needles: list[str]) -> list[str]:
    hits = []
    for needle in needles:
        n = needle.lower().strip()
        if not n:
            continue
        if n in haystack:
            hits.append(needle)
    return hits


def check(
    log: dict[str, Any],
    *,
    gold_paths: list[str] | None = None,
    eval_paths: list[str] | None = None,
    model_path: str | None = None,
    reported_calls: int | None = None,
) -> dict[str, Any]:
    uses = log.get("toolUses") or log.get("tool_uses") or []
    if not isinstance(uses, list):
        raise ValueError("toolUses must be a list")

    # Custom paths extend the defaults; they never replace them. Replacing
    # once turned the gold check into a no-op when a caller passed glob
    # patterns that substring-match nothing.
    needles = list(DEFAULT_GOLD_PATHS) + list(gold_paths or ())
    needles.extend(list(DEFAULT_EVAL_PATHS) + list(eval_paths or ()))
    # A needle like "." or "/" matches everything and voids every verdict in a
    # run -- which is what happened the first time a caller passed `--set .`.
    # Substring matching needs a needle that could only mean the eval set.
    short = [n for n in needles if len(n.strip("./")) < 3]
    if short:
        raise ValueError(f"contamination path(s) too short to be meaningful: "
                         f"{short!r}; pass an absolute path")
    model_needles = []
    if model_path:
        model_needles = [os.path.normpath(model_path), os.path.basename(model_path)]

    reasons: list[str] = []
    for i, entry in enumerate(uses):
        if not isinstance(entry, dict):
            continue
        name = _tool_name(entry) or f"use[{i}]"
        hay = _haystack(entry)
        hits = _matches(hay, needles)
        # modelPath on malloy_executeQuery is required, not a file read --
        # including when the MCP call rides over Shell as a curl JSON-RPC.
        if model_needles and _is_host_file_tool(name) and not _is_mcp_transport(hay):
            hits.extend(_matches(hay, model_needles))
        if hits:
            reasons.append(f"{name} touched {hits[0]}")

    host_uses = len(uses)
    reported = reported_calls
    if reported is None:
        raw = log.get("reportedCalls", log.get("reported_calls"))
        if raw is not None:
            reported = int(raw)
    if reported is not None and reported > host_uses:
        reasons.append(
            f"reportedCalls {reported} > host tool uses {host_uses}"
        )

    return {
        "contaminated": bool(reasons),
        "reasons": reasons,
        "host_tool_uses": host_uses,
        "reported_calls": reported,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--log", required=True, help="host tool-use JSON")
    ap.add_argument(
        "--gold-paths",
        nargs="*",
        default=None,
        help="extra gold substrings; extends the defaults, substring match only",
    )
    ap.add_argument(
        "--eval-paths",
        nargs="*",
        default=None,
        help="extra eval-ledger substrings; extends the defaults",
    )
    ap.add_argument("--model-path", default=None)
    ap.add_argument("--reported-calls", type=int, default=None)
    args = ap.parse_args()
    try:
        with open(args.log) as f:
            log = json.load(f)
        out = check(
            log,
            gold_paths=args.gold_paths,
            eval_paths=args.eval_paths,
            model_path=args.model_path,
            reported_calls=args.reported_calls,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"harness error: {type(exc).__name__}: {exc}", file=sys.stderr)
        sys.exit(1)
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
