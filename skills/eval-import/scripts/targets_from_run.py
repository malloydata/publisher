#!/usr/bin/env python3
"""Copy the get_context searches a run actually sent onto its cases, as
`searchTargets`. Stdlib only.

  python3 targets_from_run.py --run <run-dir> --set <set-dir>            # print
  python3 targets_from_run.py --run <run-dir> --set <set-dir> --write    # and save

The usual source is a run rebuilt from a host's request logs
(`skill:eval-loop`, `reference/replaying-a-logged-session.md`). Its
`tool_call` events hold the targets a real agent sent, in request order. Copied
onto the case, they let `retrieval_replay.py` resend that exact request after a
retrieval change, with no agent and no judge.

What is copied: each distinct `{target_type, search_text, example_values}` from
the case's `get_context` calls, first seen first. A target sent twice is copied
once. What is NOT copied: a key. A copied target has no `expectedEntities`, so
the replay scores it against the case's own `expectedEntities` only, until
someone keys each target (`skill:eval-import`, "Proposing search targets").

These are what one agent asked, not what a good agent asks. Treat them as the
request to hold fixed while retrieval changes, and review them before they
become the golden decomposition `score_targets.py` grades an agent against.

A case that already has `searchTargets` is left alone unless `--replace`.
Writing changes `datasetSha`, so bump `datasetVersion` in set.json after it.
"""
from __future__ import annotations

import argparse
import json
import pathlib
import sys
from typing import Any

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent.parent / "eval-answer" / "scripts"))
sys.path.insert(0, str(HERE))

from import_cases import search_target_findings  # noqa: E402
from ledger import read_jsonl  # noqa: E402


def sent_targets(events: list[dict[str, Any]], qid: str) -> list[dict[str, Any]]:
    """The distinct targets the case's get_context calls sent, in first-seen order.

    Only the first attempt at the case is read (the lowest `sample`), so a run
    with repeats does not merge two agents' searches into one request.
    """
    calls = [e for e in events if e.get("kind") == "tool_call"
             and e.get("tool") == "get_context" and e.get("qid") == qid
             and not e.get("error")]
    if not calls:
        return []
    first = min((e.get("sample") is not None, e.get("sample") or 0) for e in calls)
    calls = [e for e in calls
             if (e.get("sample") is not None, e.get("sample") or 0) == first]
    out: list[dict[str, Any]] = []
    seen = set()
    for call in calls:
        for t in call.get("search_targets") or []:
            if not isinstance(t, dict) or not t.get("target_type"):
                continue
            row: dict[str, Any] = {"target_type": t["target_type"]}
            if t.get("search_text"):
                row["search_text"] = t["search_text"]
            if t.get("example_values"):
                row["example_values"] = list(t["example_values"])
            sig = json.dumps(row, sort_keys=True)
            if sig not in seen:
                seen.add(sig)
                out.append(row)
    return out


def propose(cases: list[dict[str, Any]], events: list[dict[str, Any]],
            replace: bool) -> tuple[dict[str, list[dict[str, Any]]], list[str]]:
    """The targets to write per qid, and a line for each case left alone."""
    proposed: dict[str, list[dict[str, Any]]] = {}
    notes: list[str] = []
    for c in cases:
        qid = c.get("qid")
        targets = sent_targets(events, qid)
        if not targets:
            notes.append(f"{qid}: the run holds no get_context call with targets for it")
        elif c.get("searchTargets") and not replace:
            notes.append(f"{qid}: already has searchTargets. Pass --replace to overwrite them")
        else:
            proposed[qid] = targets
    return proposed, notes


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--run", type=pathlib.Path, required=True,
                    help="a run directory holding events.jsonl")
    ap.add_argument("--set", dest="set_dir", type=pathlib.Path, required=True,
                    help="the set directory holding cases.jsonl")
    ap.add_argument("--write", action="store_true", help="save to cases.jsonl")
    ap.add_argument("--replace", action="store_true",
                    help="overwrite a case's existing searchTargets")
    a = ap.parse_args(argv)

    events_path, cases_path = a.run / "events.jsonl", a.set_dir / "cases.jsonl"
    for p in (events_path, cases_path):
        if not p.is_file():
            raise SystemExit(f"{p} does not exist.")
    cases = read_jsonl(cases_path)
    proposed, notes = propose(cases, read_jsonl(events_path), a.replace)
    for qid, targets in proposed.items():
        print(json.dumps({"qid": qid, "searchTargets": targets}))
    for n in notes:
        print(f"  skipped {n}", file=sys.stderr)
    if not a.write or not proposed:
        return 0

    problems = []
    for c in cases:
        if c.get("qid") in proposed:
            c["searchTargets"] = proposed[c["qid"]]
            problems += search_target_findings(c, cases_path.name, c["qid"])
    if problems:
        raise SystemExit("not written, the copied targets do not validate:\n  "
                         + "\n  ".join(problems))
    cases_path.write_text("".join(json.dumps(c, ensure_ascii=False) + "\n" for c in cases))
    print(f"wrote searchTargets on {len(proposed)} case(s) in {cases_path}. "
          "Bump datasetVersion in set.json: datasetSha changed.", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
