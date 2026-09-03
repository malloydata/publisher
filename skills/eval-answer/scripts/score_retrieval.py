#!/usr/bin/env python3
"""Score retrieval per case and attribute the failure. Stdlib only.

  python score_retrieval.py --events events.jsonl --cases cases.jsonl [--json]

An engine-side retrieval skill, which does not ship here, measures the ENGINE:
fixed search terms replayed and judged, answerer-independent, for A/B-ing a
retrieval change.

This is the customer's question. For a case with a known answer, did the agent RECEIVE
the entities the answer needs? It is mechanical, exact, and per-case, and it is
what makes a wrong answer attributable:

  recall 1.0 and the answer is wrong -> retrieval delivered everything, so the
  failure is construction, and no embedding or ranking work will fix it.

  recall below 1.0 -> the agent never had the entity. Coverage then says whose
  problem that is: `covered` means the entity was there and search missed it;
  `derivable` or `absent` means there was nothing to surface. (An earlier
  version of this text said "no query-writing skill would have saved it"; that
  was false whenever the set named one route and the model offered another,
  which is what `requiredAnyOf` below exists to express.)

Those two look identical in an answer score and have opposite owners. Components
and owners match eval-diagnose's taxonomy so the output drops into `issue` events
without translation.

INPUTS

`--cases` supplies `expectedEntities` per qid:

  {"qid": "...", "coverage": "covered|derivable|absent",
   "expectedEntities": {"required": ["measure:order_items:total_sales"],
                        "requiredAnyOf": [["measure:a:x", "measure:b:y"]],
                        "acceptable": [...]}}

`required` lists entities the answer cannot be produced without. `requiredAnyOf`
lists GROUPS, each satisfied by any one member: the case can be answered through
either route, and naming only one would score the other as a retrieval miss --
which then steers diagnosis to "retrieval ranking" for a failure that was never
retrieval's. Every `required` entity is a group of one.

WHAT COUNTS AS DELIVERED

An entity reached the answerer if it was returned as a ranked entity under its
exact id (`exact`); as a ranked entity of the same type and name under a sibling
source (`alias` -- the set names one source and the model has several); or by
name inside a returned source's own documentation (`in_docs`), which is text the
answerer reads and acts on. Only `missing` is a retrieval miss. `delivery` on the
row records the route per entity, so the strict count (ranked only) is still
recoverable.

`--events` is the run ledger. Read here: `tool_call` (for
`rankedSummary.entityIds`), `attempt` (for `submitted`), and `score` (for
`verdict`). Everything else is ignored.

WHAT COUNTS

Recall is over `required`. Precision counts anything outside `acceptable` as
noise, so a defensible alternate reading is not punished -- a model offering two
honest readings of "revenue" should surface both, and choosing between them is a
construction decision.

Entities are pooled across every `get_context` call in the attempt, because the
agent gets to see all of them. A case that needed three calls to find everything
has recall 1.0 and a call count worth looking at separately.

`coverage: absent` cases are excluded from both metrics rather than scored as
all-noise. Retrieval cannot fail when there is nothing to find, and the proxy
entities such a case attracts are expected rather than wrong.
"""
from __future__ import annotations

import argparse
import json
import sys
from typing import Any

# eval-diagnose's taxonomy. Kept as literals so a rename there fails loudly here
# rather than silently mislabelling every issue this script emits. The third
# element is the human label -- "owner" reads like a person, and the value people
# actually want from this column is where to go and fix it.
CONSTRUCTION = ("construction", "agent-skill", "query construction")
RETRIEVAL = ("get_context/retrieval", "retrieval", "retrieval ranking")
MODEL = ("get_context/model", "model", "model coverage")
# A case whose coverage is `absent` should have been declined. Answering it is an
# answerer failure, so it shares construction's component and owner, but it is
# worth its own label: the fix is refusal behaviour, not query-writing.
REFUSAL = ("construction", "agent-skill", "refusal behaviour")
UNATTRIBUTED = ("", "", "")

PASSING = {"match", "near_match"}
# Verdicts the acceptance check counts as neither a pass nor a failure.
UNSCORED = (None, "", "needs_human")


def read_jsonl(path: str) -> list[dict[str, Any]]:
    out = []
    with open(path) as fh:
        for n, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError as e:
                raise SystemExit(f"{path}:{n}: {e}")
    return out


def attempt_key(e: dict[str, Any]) -> tuple:
    """Identify an attempt. `sample` is required-but-nullable in the schema."""
    return (e.get("qid"), e.get("sample"), e.get("phase"))


def retrieved(events: list[dict[str, Any]],
              key: tuple) -> tuple[list[str], int, set[str]]:
    """Entities pooled over the attempt's get_context calls, the call count,
    and every identifier named in the returned sources' documentation."""
    seen: dict[str, None] = {}
    tokens: set[str] = set()
    calls = 0
    for e in events:
        if e.get("kind") != "tool_call" or e.get("tool") != "get_context":
            continue
        if attempt_key(e) != key:
            continue
        calls += 1
        rs = e.get("rankedSummary") or {}
        for eid in rs.get("entityIds") or []:
            seen.setdefault(eid, None)
        tokens.update(rs.get("docTokens") or [])
    return list(seen), calls, tokens


def split_entity(eid: str) -> tuple[str, str, str]:
    parts = (eid or "").split(":")
    if len(parts) >= 3:
        return parts[0], parts[1], ":".join(parts[2:])
    if len(parts) == 2:
        return parts[0], "", parts[1]
    return "", "", eid or ""


def delivery(eid: str, returned: set[str], doc_tokens: set[str]) -> str:
    """How one required entity reached the answerer: exact, alias, in_docs, missing."""
    if eid in returned:
        return "exact"
    kind, _, name = split_entity(eid)
    for r in returned:
        rk, _, rn = split_entity(r)
        if rk == kind and rn == name:
            return "alias"
    if name and name in doc_tokens:
        return "in_docs"
    return "missing"


def groups(exp: dict[str, Any]) -> list[list[str]]:
    """Required groups: each `required` entity alone, plus every `requiredAnyOf` group."""
    out = [[r] for r in (exp.get("required") or [])]
    for g in exp.get("requiredAnyOf") or []:
        if isinstance(g, list) and g:
            out.append(list(g))
    return out


def attribute(recall: float | None, coverage: str, passed: bool | None) -> tuple:
    """Where to fix this outcome. Returns (component, owner, label, why).

    EVERY failure gets attributed. An earlier version returned nothing when
    recall was null, which silently dropped the `absent`-coverage cases: they
    counted as failures in the score table and appeared under no heading here,
    so the two never summed to the same number.
    """
    if passed:
        return (*UNATTRIBUTED, "passed")
    if passed is None:
        # needs_human or unscorable. Neither a pass nor a failure, so it must not
        # be attributed -- doing so would inflate whichever bucket it landed in.
        return (*UNATTRIBUTED, "not scored")
    if recall is None:
        # No required entities, which in this set means coverage is `absent`.
        # The case failed, so the answerer produced something for a question the
        # model cannot answer.
        return (*REFUSAL,
                "the model cannot answer this and the answerer did not decline")
    if recall >= 1.0:
        return (*CONSTRUCTION,
                "retrieval delivered every required entity; the failure is in the query")
    if coverage == "covered":
        return (*RETRIEVAL,
                "the entity exists in the model and was not returned")
    return (*MODEL,
            f"nothing to return: coverage is {coverage}, so the entity does not exist")


def score_case(case: dict[str, Any], events: list[dict[str, Any]],
               key: tuple, verdict: str | None) -> dict[str, Any]:
    coverage = case.get("coverage", "unknown")
    exp = case.get("expectedEntities") or {}
    req_groups = groups(exp)
    required = {e for g in req_groups for e in g}
    acceptable = set(exp.get("acceptable") or []) | required
    got, calls, tokens = retrieved(events, key)
    got_set = set(got)
    route = {e: delivery(e, got_set, tokens) for e in sorted(required)}
    delivered = {e for e, r in route.items() if r != "missing"}

    # needs_human is neither a pass nor a failure, exactly like a null verdict:
    # the acceptance check excludes both, so counting one as failed would put it in an
    # attribution bucket it has not earned.
    passed = None if verdict in UNSCORED else verdict in PASSING

    if coverage == "absent" or not req_groups:
        recall = precision = None
        missing: list[str] = []
        noise: list[str] = []
    else:
        satisfied = [g for g in req_groups if any(e in delivered for e in g)]
        recall = len(satisfied) / len(req_groups)
        precision = (len(got_set & acceptable) / len(got_set)) if got_set else None
        # A missing group is written as its members joined by " | ", so a
        # two-route group reads as one unmet need rather than two.
        missing = sorted(" | ".join(g) for g in req_groups
                         if not any(e in delivered for e in g))
        noise = sorted(got_set - acceptable)

    component, owner, where_to_fix, why = attribute(recall, coverage, passed)
    return {
        "qid": case["qid"],
        "sample": key[1],
        "phase": key[2],
        "coverage": coverage,
        "verdict": verdict,
        "failed": passed is False,
        "recall": recall,
        "precision": precision,
        "n_required": len(req_groups),
        "n_returned": len(got_set),
        "delivery": route,
        "n_ranked": sum(1 for r in route.values() if r in ("exact", "alias")),
        "n_get_context": calls,
        "missing": missing,
        "noise": noise,
        "component": component,
        "owner": owner,
        "where_to_fix": where_to_fix,
        "why": why,
    }


def summarise(rows: list[dict[str, Any]]) -> dict[str, Any]:
    scored = [r for r in rows if r["recall"] is not None]
    placed: dict[str, int] = {}
    for r in rows:
        if r["where_to_fix"]:
            placed[r["where_to_fix"]] = placed.get(r["where_to_fix"], 0) + 1
    failures = sum(1 for r in rows if r["failed"])
    mean = lambda xs: (sum(xs) / len(xs)) if xs else None
    return {
        "attempts": len(rows),
        "failures": failures,
        # Must equal `failures`. If it does not, some failure fell through
        # attribute() and the two tables on any report built from this disagree.
        "attributed": sum(placed.values()),
        "retrieval_scored": len(scored),
        "mean_recall": mean([r["recall"] for r in scored]),
        "mean_precision": mean([r["precision"] for r in scored
                                if r["precision"] is not None]),
        "complete_retrievals": sum(1 for r in scored if r["recall"] >= 1.0),
        "failures_by_where_to_fix": placed,
    }


def load(events_path, cases_path) -> tuple[dict, dict]:
    """(cases by qid, attempts by key) for anything scoring a run.

    Shared so that a consumer cannot quietly disagree with this file about which
    verdict belongs to which attempt.
    """
    events = read_jsonl(events_path)
    cases = {c["qid"]: c for c in read_jsonl(cases_path)}
    verdicts = {attempt_key(e): e.get("verdict")
                for e in events if e.get("kind") == "score"}
    attempts: dict[tuple, dict] = {}
    for e in events:
        if e.get("kind") != "attempt":
            continue
        k = attempt_key(e)
        attempts[k] = {"attempt": e, "verdict": verdicts.get(k),
                       "calls": [t for t in events
                                 if t.get("kind") == "tool_call"
                                 and attempt_key(t) == k]}
    return cases, attempts


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", required=True)
    ap.add_argument("--cases", required=True)
    ap.add_argument("--json", action="store_true", help="emit rows as JSONL")
    a = ap.parse_args(argv)

    events = read_jsonl(a.events)
    cases = {c["qid"]: c for c in read_jsonl(a.cases)}

    verdicts = {attempt_key(e): e.get("verdict")
                for e in events if e.get("kind") == "score"}

    rows = []
    for e in events:
        if e.get("kind") != "attempt":
            continue
        case = cases.get(e.get("qid"))
        if case is None:
            print(f"warning: no case for qid {e.get('qid')!r}", file=sys.stderr)
            continue
        key = attempt_key(e)
        rows.append(score_case(case, events, key, verdicts.get(key)))

    rows.sort(key=lambda r: (r["qid"], str(r["sample"])))
    if a.json:
        for r in rows:
            print(json.dumps(r))
        return 0

    pct = lambda x: "  -  " if x is None else f"{100 * x:5.1f}"
    print(f"{'qid':34s} {'cov':10s} {'verdict':11s} {'rec':>5s} {'prec':>5s}  "
          f"where to fix")
    for r in rows:
        print(f"{r['qid']:34s} {r['coverage']:10s} {str(r['verdict']):11s} "
              f"{pct(r['recall'])} {pct(r['precision'])}  "
              f"{r['where_to_fix'] or '-'}")
        if r["missing"]:
            print(f"{'':34s} missing: {', '.join(r['missing'])}")

    s = summarise(rows)
    print()
    print(f"attempts {s['attempts']}, failures {s['failures']}, "
          f"retrieval scored {s['retrieval_scored']}")
    if s["mean_recall"] is not None:
        print(f"complete retrieval {s['complete_retrievals']} of "
              f"{s['retrieval_scored']} (every required entity returned); "
              f"mean recall {100 * s['mean_recall']:.1f}%, "
              f"mean precision {100 * s['mean_precision']:.1f}%")
    if s["failures_by_where_to_fix"]:
        print("where to fix: " + ", ".join(
            f"{k} {v}" for k, v in sorted(s["failures_by_where_to_fix"].items())))
    if s["attributed"] != s["failures"]:
        print(f"WARNING: {s['failures']} failures but {s['attributed']} "
              f"attributed; these must tie", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
