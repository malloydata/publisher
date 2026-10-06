#!/usr/bin/env python3
"""Read several runs of the same questions and report, per case, how much they agree.

    python3 agreement.py --runs <runDir>/attempt-*

`flip_table.py` compares two runs. This reads any number, and it exists for one
situation: the same question asked many times in production, each logged answer
replayed as its own run. Measured on a real 96-prompt pull, one question was
asked seven times and drew four refusals and three different answers from three
packages. A single run of that case is one draw from that spread; the spread is
the finding.

For each case it reports how many runs answered it, the verdicts by outcome
(pass / fail / neither / undecided, with the ONE classification flip_table
exports), how many of those answers ran no query, and which packages answered.
It decides nothing: a case whose runs disagree is not a failure, it is a case
whose single verdict could not be quoted.

Exits 0. A run directory with no ledger is named and skipped.
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

from flip_table import outcome  # noqa: E402
from ledger import read_jsonl  # noqa: E402


def read_run(run: pathlib.Path) -> dict[str, dict[str, Any]]:
    """qid -> {verdict, submitted, packages} for one run."""
    events = read_jsonl(run / "events.jsonl")
    out: dict[str, dict[str, Any]] = {}
    for e in events:
        q = e.get("qid")
        if not q:
            continue
        row = out.setdefault(q, {"verdict": None, "submitted": None,
                                 "packages": []})
        if e.get("kind") == "attempt":
            row["submitted"] = bool(e.get("submitted"))
            row["packages"] = list(e.get("queriedPackages") or [])
        elif e.get("kind") == "score":
            row["verdict"] = e.get("verdict")
    return out


def agreement(runs: dict[str, dict[str, dict[str, Any]]]) -> list[dict[str, Any]]:
    """One row per case across every run that holds it. Pure."""
    by_case: dict[str, list[tuple[str, dict[str, Any]]]] = {}
    for name, cases in runs.items():
        for qid, row in cases.items():
            by_case.setdefault(qid, []).append((name, row))
    rows = []
    for qid in sorted(by_case):
        draws = by_case[qid]
        counts = {"pass": 0, "fail": 0, "neither": 0, "undecided": 0}
        packages: dict[str, int] = {}
        no_query = 0
        for _name, r in draws:
            # A null verdict is not "neither": nothing was judged, and folding
            # it in would read as a hedge the judge never made.
            counts["undecided" if r["verdict"] is None
                   else outcome(r["verdict"])] += 1
            if r["submitted"] is False:
                no_query += 1
            for p in r["packages"]:
                packages[p] = packages.get(p, 0) + 1
        decided = counts["pass"] + counts["fail"]
        rows.append({
            "qid": qid, "runs": len(draws), **counts,
            "no_query": no_query, "packages": packages,
            # Agreement is only claimable over decided runs. One decided run
            # agrees with itself, which says nothing, so it is not "agrees".
            "agrees": (decided >= 2 and (counts["pass"] == 0
                                         or counts["fail"] == 0)),
            "split": counts["pass"] > 0 and counts["fail"] > 0,
            "verdicts": {name: r["verdict"] for name, r in draws}})
    return rows


def report(rows: list[dict[str, Any]], n_runs: int) -> list[str]:
    lines = [f"AGREEMENT  {len(rows)} case(s) across {n_runs} run(s)", ""]
    w = max([len(r["qid"]) for r in rows] + [4])
    lines.append(f"  {'case':<{w}}  runs  pass fail neither undecided  "
                 f"no-query  packages")
    for r in rows:
        pk = ", ".join(f"{k} ({n})" for k, n in sorted(
            r["packages"].items(), key=lambda kv: (-kv[1], kv[0]))) or "-"
        flag = "  SPLIT" if r["split"] else ""
        lines.append(f"  {r['qid']:<{w}}  {r['runs']:>4}  {r['pass']:>4} "
                     f"{r['fail']:>4} {r['neither']:>7} {r['undecided']:>9}  "
                     f"{r['no_query']:>8}  {pk}{flag}")
    split = [r["qid"] for r in rows if r["split"]]
    multi = [r["qid"] for r in rows if len(r["packages"]) > 1]
    lines.append("")
    if split:
        lines.append(f"! {len(split)} case(s) both passed and failed across "
                     f"runs: {', '.join(split)}. No single verdict for these "
                     f"is quotable; the pass share is the measurement.")
    if multi:
        lines.append(f"! {len(multi)} case(s) were answered from more than one "
                     f"package: {', '.join(multi)}. Which answer a person got "
                     f"depended on which package the agent picked.")
    if not split and not multi:
        lines.append("  every case agrees with itself across its decided runs")
    return lines


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--runs", nargs="+", type=pathlib.Path, required=True,
                    help="run directories holding the same cases")
    ap.add_argument("--json", type=pathlib.Path, default=None,
                    help="also write the rows here")
    a = ap.parse_args(argv)

    runs: dict[str, dict[str, dict[str, Any]]] = {}
    for run in a.runs:
        if not run.is_dir():
            # A glob such as attempt-* also matches the logs beside the runs.
            continue
        if not (run / "events.jsonl").exists():
            print(f"  skipped {run}: no events.jsonl (not scored yet?)")
            continue
        runs[run.name] = read_run(run)
    rows = agreement(runs)
    for line in report(rows, len(runs)):
        print(line)
    if a.json:
        a.json.write_text(json.dumps(rows, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
