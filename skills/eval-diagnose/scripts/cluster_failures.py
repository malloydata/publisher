#!/usr/bin/env python3
"""Group a run's failures by shared root cause. Stdlib only.

  python cluster_failures.py --run results/<run> --set evals/ecommerce
  python cluster_failures.py --run results/<run> --set evals/ecommerce --json

Writes `clusters-mechanical.jsonl` into the run directory: a CANDIDATE grouping
by retrieval outcome, for a first look and as input to diagnosis. The clusters
of record are the diagnose agent's, which `diagnose.py` writes to
`clusters.jsonl`; this script never writes that file, because on two sets the
mechanical grouping charged every failure to retrieval while the diagnosis said
otherwise, and the package builder read this one.

WHAT THIS DECIDES AND WHAT IT DOES NOT

A per-case list of failures is not a work plan. Eight failures are rarely eight
problems -- more often two or three, each showing up in several cases -- and the
edit worth making is the one that clears a group. So the unit here is the
cluster, ordered by how many cases it would fix.

The grouping is mechanical and deliberately conservative: cases join a cluster
only on evidence in the ledger -- the same missing entity, the same undocumented
concept, the same where-to-fix with the same lever. That keeps clustering
reproducible and reviewable.

`proposedEdit` is left empty on purpose. What to change about a model is a
judgement about intent, and a script that guesses it produces confident
nonsense that reviewers then have to disprove. eval-improve fills it in, and
`--json` prints the clusters in the shape it expects. A cluster with no proposed
edit is an honest open question; a cluster with a fabricated one is worse than
nothing.
"""
from __future__ import annotations

import argparse
import collections
import json
import pathlib
import sys
from typing import Any

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent.parent
                       / "eval-answer" / "scripts"))
from score_retrieval import load, score_case  # noqa: E402

# Which artifact a cluster's fix would land in. eval-improve needs this to know
# what kind of edit it is even proposing.
LEVER_BY_WHERE = {
    "query construction": "skill",
    "retrieval ranking": "ranking",
    "model coverage": "model",
    "refusal behaviour": "skill",
}


def cluster_key(row: dict[str, Any], case: dict[str, Any]) -> tuple[str, str]:
    """(key, human label) for the group this failure belongs in.

    Ordered most specific first. A shared missing entity is the strongest
    evidence two failures are one problem; falling back to where-to-fix alone
    yields a loose group, which is still better than eight singletons.
    """
    where = row["where_to_fix"]
    missing = row.get("missing") or []

    if where == "model coverage":
        note = (case.get("coverageNote") or "").strip()
        if note:
            # Notes are already phrased as an absence ("no measure; ..."), so
            # prefixing another "no" reads as a double negative.
            lead = "" if note.lower().startswith(("no ", "not ", "missing")) \
                else "no "
            return (f"model:{note[:60]}", f"the model has {lead}{note[:80]}")
        return ("model:unspecified", "the model does not cover what was asked")

    if where == "retrieval ranking" and missing:
        # One entity that several questions needed and none received.
        return (f"retrieval:{missing[0]}", f"{missing[0]} is never returned")

    if where == "refusal behaviour":
        return ("refusal", "answered a question the model cannot answer")

    if where == "query construction":
        tags = [t for t in (case.get("tags") or [])
                if t in ("window", "time-series", "ratio", "join", "distinct",
                         "cohort", "cumulative", "rank")]
        if tags:
            return (f"construction:{tags[0]}",
                    f"queries involving {tags[0]} are built wrong")
        return ("construction:other", "the query was built wrong")

    return (f"other:{where}", where or "unattributed")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", required=True, type=pathlib.Path)
    ap.add_argument("--set", dest="set_dir", required=True, type=pathlib.Path)
    ap.add_argument("--json", action="store_true",
                    help="print clusters as JSON for eval-improve")
    a = ap.parse_args(argv)

    cases, attempts = load(a.run / "events.jsonl", a.set_dir / "cases.jsonl")
    rows = []
    for k, att in sorted(attempts.items()):
        case = cases.get(k[0])
        if case:
            rows.append((score_case(case, att["calls"], k, att["verdict"]), case))

    failures = [(r, c) for r, c in rows if r["failed"]]
    groups: dict[str, dict[str, Any]] = collections.OrderedDict()
    for r, c in failures:
        key, label = cluster_key(r, c)
        g = groups.setdefault(key, {
            "clusterId": key, "label": label,
            "whereToFix": r["where_to_fix"], "component": r["component"],
            "owner": r["owner"], "lever": LEVER_BY_WHERE.get(r["where_to_fix"]),
            "qids": [], "evidence": [], "proposedEdit": "", "confidence": None})
        g["qids"].append(r["qid"])
        why = (c.get("golden") or {}).get("rubric") or ""
        g["evidence"].append({
            "qid": r["qid"], "question": c.get("question"),
            "verdict": r["verdict"], "missing": r.get("missing") or [],
            "recall": r["recall"], "rubric": why[:200]})

    ordered = sorted(groups.values(), key=lambda g: (-len(g["qids"]),
                                                     g["clusterId"]))
    with (a.run / "clusters-mechanical.jsonl").open("w") as fh:
        for g in ordered:
            fh.write(json.dumps({**g, "source": "mechanical"}) + "\n")

    if a.json:
        print(json.dumps(ordered, indent=2))
        return 0

    print(f"{len(failures)} failures in {len(ordered)} clusters\n")
    for g in ordered:
        print(f"[{len(g['qids'])}] {g['label']}")
        print(f"     fix in: {g['whereToFix']}  (lever: {g['lever']})")
        for e in g["evidence"]:
            miss = f"  missing {', '.join(e['missing'])}" if e["missing"] else ""
            print(f"     - {e['qid']}: {e['question'][:70]}{miss}")
        print()
    print(f"{a.run}/clusters-mechanical.jsonl")
    print("A candidate grouping by retrieval outcome. The clusters of record are "
          "the diagnose agent's (diagnose.py -> clusters.jsonl).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
