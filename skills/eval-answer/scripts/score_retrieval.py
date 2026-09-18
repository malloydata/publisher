#!/usr/bin/env python3
"""Score retrieval per case and attribute the failure. Stdlib only.

  python score_retrieval.py --events events.jsonl --cases cases.jsonl [--json]

An engine-side retrieval skill, which does not ship here, measures the ENGINE:
fixed search terms replayed and judged, answerer-independent, for A/B-ing a
retrieval change.

This is the customer's question. For a case with a known answer, did the agent RECEIVE
the entities the answer needs? It is mechanical, exact, and per-case, and it is
what makes a wrong answer attributable:

  recall 1.0 and the answer is wrong -> retrieval delivered everything, so no
  embedding or ranking work will fix it. Whether the agent misused what it had
  or the docs never said how to use it is eval-diagnose's call, sufficiency
  first, so the row reads "delivered, wrong" and names no owner.

  recall below 1.0 -> the agent never had the entity. Coverage then says whose
  problem that is: `covered` means the entity was there and search missed it,
  which from the customer's side is a documentation finding -- the retrieval
  algorithm is fixed, semantic search over doc strings, so an entity that exists
  and does not come back is one whose docs do not say what people ask
  (eval-diagnose NOT-RETURNED, owner model). `derivable` or `absent` means there
  was nothing to surface. (An earlier version of this text said "no
  query-writing skill would have saved it"; that was false whenever the set
  named one route and the model offered another, which is what `requiredAnyOf`
  below exists to express.) No label at all means nobody has measured it, and
  the row says so instead of asserting a model gap.

Those look identical in an answer score and have different owners. Components
and owners match eval-diagnose's taxonomy so the output drops into `issue` events
without translation; a label that named the engine ("retrieval ranking") did
not, and was corrected.

INPUTS

`--cases` supplies `expectedEntities` per qid:

  {"qid": "...", "coverage": "covered|derivable|absent",
   "expectedEntities": {"required": ["measure:order_items:total_sales"],
                        "requiredAnyOf": [["measure:a:x", "measure:b:y"]],
                        "acceptable": [...]}}

`required` lists entities the answer cannot be produced without. `requiredAnyOf`
lists GROUPS, each satisfied by any one member: the case can be answered through
either route, and naming only one would score the other as a retrieval miss --
which then steers the fix to the docs for an entity whose docs were never the
problem. Every `required` entity is a group of one.

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

`--coverage` is optional: a `check_coverage.py --out` report. Its per-case
verdict is a measurement against this build and beats the case's authored
`coverage` label; `coverage_source` on each row says which one attribution
used, and `none` means neither existed and nobody gets charged.

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
# Delivered everything and still wrong. eval-diagnose attributes construction
# "only after sufficiency": first establish that the docs said enough to use the
# entity correctly. WRONG-PICK is the model's if the docs did not distinguish the
# candidates, SCOPE is the model's if the rule was undocumented, CONVENTION is
# the model's ("expose a named measure"). So this row names no owner. An earlier
# version charged every such case to the agent, which is how a documentation gap
# gets filed as a skills bug and never fixed.
DELIVERED = ("construction", "undecided", "delivered, wrong")
# Covered, and not returned. eval-diagnose's default code for that is
# NOT-RETURNED under get_context/model, owner model: "labels, docs, synonyms,
# index". Its RETRIEVAL code (owner retrieval) exists, but needs a rare-token
# proof -- a distinctive phrase from the entity's own doc retrieves it and
# ordinary phrasing does not -- and is never assigned mechanically here. From
# the customer's side the retrieval algorithm is fixed, semantic search over doc
# strings, so an entity that exists and does not come back is one whose docs do
# not say what people ask. This label used to read "retrieval ranking", which
# named the engine and sent the fix to the wrong team.
# Asked for the right KIND of thing, and it still did not come back. Two live
# causes and the run cannot separate them: the docs do not describe the entity
# the way this question phrases it, or the search vocabulary was off. Both are
# real, they have different owners, and eval-diagnose decides between them
# (NOT-RETURNED / LOW-RANK are the model's; QUESTION-VOCAB and VAGUE are the
# agent's). An earlier version asserted `documentation` here outright, which
# was provably wrong on the first real run.
NOT_RETURNED = ("get_context", "undecided", "not retrieved")
# Never issued a search for that kind of entity at all. eval-diagnose's
# NEVER-ASKED / WRONG-TYPE-OR-SCOPE, owner agent-skill. Mechanical and certain:
# a measure cannot be returned by a search that asked only for dimensions.
NEVER_ASKED = ("get_context/agent-call", "agent-skill", "never asked")
MODEL = ("get_context/model", "model", "model coverage")
# A case whose coverage is `absent` should have been declined. Answering it is
# the answerer's, and documentation cannot help when there is nothing to
# document; its own label because the fix is refusal behaviour, not query-writing.
REFUSAL = ("construction", "agent-skill", "refusal behaviour")
UNATTRIBUTED = ("", "", "")
# Coverage values that were MEASURED and found nothing to surface. Only these
# may send a retrieval miss to the model: the authored labels `derivable` and
# `absent`, and check_coverage.py's four gap verdicts, which are eval-diagnose's
# codes. The four must match `check_coverage.FAIL_VERDICTS`; the test pins that,
# because this file stays stdlib-only and does not import it.
MEASURED_GAPS = ("derivable", "absent",
                 "COVERAGE", "AMBIGUOUS", "NO-DISAMBIG", "CONVENTION")
# check_coverage.py's "a correct answer is expressible": the measured `covered`.
MEASURED_OK = "ok"
# A failure that is retrieval's or the model's, and nothing measured which. Its
# own bucket, because the alternative was worse: with no authored `coverage`
# label the case fell through to MODEL with the words "coverage is unknown, so
# the entity does not exist" -- a model gap asserted on no evidence, on exactly
# the sets that arrive as bare questions with no labels. `unknown` as the
# conservative reading is eval-diagnose's own convention for `sufficiency`; it
# is not a new owner, it is the absence of one.
UNMEASURED = ("get_context", "unknown", "coverage not measured")

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


# Which entity KINDS each search target type can return. Mirrors
# `KINDS_BY_TARGET` in the server's get_context tool, and `test_kinds_by_target_
# matches_the_server` pins it against that file, because this module stays
# stdlib-only and cannot import TypeScript. Getting this wrong in either
# direction misattributes a miss: too narrow and the agent is blamed for not
# asking when it did, too wide and a real never-asked reads as a retrieval
# failure.
#
# `target_type` is a HARD FILTER on the server, which is what makes a miss of
# this shape mechanical rather than a judgement: no amount of documentation can
# deliver a measure to a search that asked only for dimensions.
KINDS_BY_TARGET = {
    "source": {"source"},
    "dimension": {"dimension"},
    "measure": {"measure"},
    # A model-level named query is a pre-built analysis, which is what the
    # published shape says a `view` target is for.
    "view": {"view", "query"},
    "join": {"join"},
    # Publisher indexes no dimensional values, so this target selects nothing.
    "dimensional_value": set(),
}


def retrieved(events: list[dict[str, Any]],
              key: tuple) -> tuple[list[str], int, set[str], set[str]]:
    """Entities pooled over the attempt's get_context calls, the call count,
    every identifier named in the returned sources' documentation, and the
    entity KINDS the agent actually searched for.

    The kinds matter: a measure cannot come back from a search that asked only
    for a source and a dimension, and that is the agent's miss rather than the
    model's. `targets` on the tool_call records what was asked, phrased
    `"measure: count of titles"`, so the distinction is mechanical.
    """
    seen: dict[str, None] = {}
    tokens: set[str] = set()
    asked: set[str] = set()
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
        # `target_shapes` when the run recorded it: it keeps EVERY target,
        # including one carrying no `search_text`, which `targets` drops
        # because there is no term to record. Reading only `targets` scored an
        # agent that enumerated all measures as never having asked for one.
        shapes = e.get("target_shapes")
        if shapes:
            for t in shapes:
                if isinstance(t, dict) and t.get("type"):
                    asked.add(str(t["type"]).strip().lower())
        else:
            for t in e.get("targets") or []:
                if isinstance(t, str) and ":" in t:
                    asked.add(t.split(":", 1)[0].strip().lower())
                elif isinstance(t, dict) and t.get("target_type"):
                    asked.add(str(t["target_type"]).strip().lower())
    # Expand target types to the kinds they can actually return, so a `view`
    # target counts as having asked for a model-level named query.
    reachable = set().union(*(KINDS_BY_TARGET.get(a, {a}) for a in asked)) \
        if asked else set()
    return list(seen), calls, tokens, reachable


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


def attribute(recall: float | None, coverage: str, passed: bool | None,
              missing_kinds: set[str] | None = None,
              asked_kinds: set[str] | None = None) -> tuple:
    """Where to fix this outcome. Returns (component, owner, label, why).

    EVERY failure gets attributed. An earlier version returned nothing when
    recall was null, which silently dropped the `absent`-coverage cases: they
    counted as failures in the score table and appeared under no heading here,
    so the two never summed to the same number.
    """
    # NO short-circuit on `passed`. This used to open with
    # `if passed: return (*UNATTRIBUTED, "passed")`, before recall was looked
    # at -- so a case that answered correctly while never receiving a required
    # entity was attributed to nobody, `summarise()` dropped it because the
    # empty label is falsy, and no issue could ever be raised for it. Measured
    # on one run: 4 of 5 undelivered entities were on passing cases and
    # produced zero findings.
    #
    # "It worked anyway" is not a reason to leave the gap. The answer being
    # right is recorded on the row (`failed`, `verdict`) and reported
    # separately, so attributing the miss does not move the pass rate.
    if passed is None:
        # needs_human or unscorable. Neither a pass nor a failure, so it must not
        # be attributed -- doing so would inflate whichever bucket it landed in.
        return (*UNATTRIBUTED, "not scored")
    if recall is None:
        # No required entities, which in this set means coverage is `absent`.
        if passed:
            # It declined a question the model cannot answer, which is the
            # right behaviour and the pass. Nothing to attribute.
            return (*UNATTRIBUTED, "declined an unanswerable question")
        return (*REFUSAL,
                "the model cannot answer this and the answerer did not decline")
    if recall >= 1.0:
        if passed:
            # Everything arrived and the answer was right. The only shape with
            # genuinely nothing to report.
            return (*UNATTRIBUTED, "delivered and correct")
        return (*DELIVERED,
                "retrieval delivered every required entity; the failure is in how "
                "it was used, or in docs that never said how to use it. Diagnose "
                "decides which, sufficiency first")
    # Below here recall is < 1.0: a required entity did not reach the answerer.
    # That is attributed whether or not the answer came out right, because an
    # answer that was right WITHOUT the entity was right by another route --
    # most often the agent rebuilding the model's own measure inline, which is
    # correct only while the measure is trivial.
    if coverage in ("covered", MEASURED_OK):
        unasked = sorted((missing_kinds or set()) - (asked_kinds or set()))
        if unasked:
            return (*NEVER_ASKED,
                    f"the entity exists and no search asked for a "
                    f"{'/'.join(unasked)} at all, so nothing of that kind could "
                    f"come back (eval-diagnose NEVER-ASKED)")
        return (*NOT_RETURNED,
                "the entity exists, a search of the right kind was issued, and it "
                "did not come back. The docs may not say what this question asks, "
                "or the search wording may be off; eval-diagnose separates "
                "NOT-RETURNED from QUESTION-VOCAB. Assumes a semantic run")
    if coverage in MEASURED_GAPS:
        return (*MODEL,
                f"nothing to return: coverage is {coverage}, so the entity does not exist")
    return (*UNMEASURED,
            "recall below 1.0 and coverage was never measured for this case; run "
            "check_coverage.py before calling this a model gap or a retrieval miss")


def load_coverage_report(path: str) -> dict[str, str | None]:
    """`qid -> verdict` from a `check_coverage.py --out` report.

    That report used to be written and read by nothing. This is the read. A
    case whose measurement was undecided carries None and falls back to whatever
    else the case has, and `coverage_source` on the row says which won.
    """
    with open(path) as fh:
        data = json.load(fh)
    return {r["qid"]: r.get("verdict") for r in data.get("cases_detail", [])}


def coverage_report_summary(path: str) -> dict[str, Any]:
    """What run.json records about a coverage report it consumed.

    Enough to say which measurement charged the failures: the file, the model
    version it was stamped with, the judge that decided it, and how much of the
    set it actually decided. `decided` of `cases` matters more than the
    percentage: a report that decided 4 of 49 is not a coverage number, it is a
    sample size.
    """
    with open(path) as fh:
        d = json.load(fh)
    return {"path": path, "version": d.get("version"),
            "agentModel": d.get("agentModel"),
            "decided": d.get("decided"), "cases": d.get("cases")}


def score_case(case: dict[str, Any], events: list[dict[str, Any]],
               key: tuple, verdict: str | None,
               measured: str | None = None) -> dict[str, Any]:
    # A measured verdict beats the authored label. The label is a standing hand
    # judgement about the question; the verdict is a measurement against THIS
    # build, which is the thing an attribution is about. Neither present reads
    # "unknown", which attribute() refuses to charge to anyone.
    if measured is not None:
        coverage, coverage_source = measured, "measured"
    elif case.get("coverage") is not None:
        coverage, coverage_source = case["coverage"], "authored"
    else:
        coverage, coverage_source = "unknown", "none"
    exp = case.get("expectedEntities") or {}
    req_groups = groups(exp)
    required = {e for g in req_groups for e in g}
    authored_acceptable = set(exp.get("acceptable") or [])
    acceptable = authored_acceptable | required
    got, calls, tokens, asked = retrieved(events, key)
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

    missing_kinds = {split_entity(e)[0] for g in req_groups for e in g
                     if e not in delivered}
    component, owner, where_to_fix, why = attribute(
        recall, coverage, passed, missing_kinds, asked)
    return {
        "qid": case["qid"],
        "sample": key[1],
        "phase": key[2],
        "coverage": coverage,
        "coverage_source": coverage_source,
        "verdict": verdict,
        "failed": passed is False,
        "recall": recall,
        "precision": precision,
        "n_required": len(req_groups),
        "n_returned": len(got_set),
        # Precision reads everything outside `acceptable` as noise, so a set
        # that never authored one scores every legitimate extra as a miss. The
        # flag travels with the row so a reader knows which they are looking at.
        "has_acceptable": bool(authored_acceptable),
        "delivery": route,
        "n_ranked": sum(1 for r in route.values() if r in ("exact", "alias")),
        "n_get_context": calls,
        # What the agent asked for, so a reader can judge the search rather than
        # take the label's word for it -- and so a set can be surveyed for the
        # vocabulary its questions actually need.
        "asked_kinds": sorted(asked),
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
        # How much get_context handed back, and how much of it the answer
        # needed. Unlike precision these do not depend on `acceptable` being
        # authored, so they say something about retrieval on any set.
        "mean_returned": mean([r["n_returned"] for r in scored]),
        "mean_required": mean([r["n_required"] for r in scored]),
        "cases_with_acceptable": sum(1 for r in rows if r.get("has_acceptable")),
        "failures_by_where_to_fix": placed,
    }


def cascade(rows: list[dict[str, Any]]) -> dict[str, int]:
    """The three metrics as a funnel, because each one conditions the next.

    A case the model cannot express has no meaningful retrieval or answer result;
    a case whose entity never arrived has no meaningful answer result. Reported
    as three independent percentages those read as three unrelated problems.
    Reported as a cascade, each failure sits at the one rung that owns it, and
    the rungs sum to the row count so a reader can check the arithmetic.
    """
    c = {"total": len(rows), "not covered": 0, "unmeasured": 0,
         "no entities named": 0, "not retrieved": 0, "delivered, wrong": 0,
         "delivered, right": 0, "not scored": 0,
         # Passes that stop on an earlier rung. Without these the funnel looks
         # like it contradicts the pass rate: a run where every case matched
         # read "correct? 6 yes" because two passed despite a coverage gap and
         # incomplete retrieval, and 6 is exactly the number a reader would
         # mistake for the score.
         "passed_not_covered": 0, "passed_not_retrieved": 0}
    for r in rows:
        cov = r["coverage"]
        passed = not r["failed"] and r["verdict"] not in UNSCORED
        if cov in MEASURED_GAPS:
            c["not covered"] += 1
            c["passed_not_covered"] += passed
        elif cov not in ("covered", MEASURED_OK):
            c["unmeasured"] += 1
        elif r["recall"] is None:
            c["no entities named"] += 1
        elif r["recall"] < 1.0:
            c["not retrieved"] += 1
            c["passed_not_retrieved"] += passed
        elif r["verdict"] in UNSCORED:
            c["not scored"] += 1
        elif r["failed"]:
            c["delivered, wrong"] += 1
        else:
            c["delivered, right"] += 1
    return c


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
    ap.add_argument("--coverage", default=None,
                    help="a check_coverage.py --out report; its per-case verdict "
                         "beats the authored `coverage` label")
    a = ap.parse_args(argv)

    events = read_jsonl(a.events)
    cases = {c["qid"]: c for c in read_jsonl(a.cases)}
    measured = load_coverage_report(a.coverage) if a.coverage else {}

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
        rows.append(score_case(case, events, key, verdicts.get(key),
                               measured.get(e.get("qid"))))

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
    # Each mean is guarded on its own. `mean_precision` is None whenever every
    # scored attempt returned zero entities (score_case leaves precision None
    # for an empty `got_set`), which is total retrieval failure -- the run whose
    # report matters most. Testing only `mean_recall`, which is a float whenever
    # anything scored at all, let that run reach `100 * None`.
    meanpct = lambda x: "n/a" if x is None else f"{100 * x:.1f}%"
    if s["mean_recall"] is not None:
        print(f"complete retrieval {s['complete_retrievals']} of "
              f"{s['retrieval_scored']} (every required entity returned); "
              f"mean recall {meanpct(s['mean_recall'])}, "
              f"mean precision {meanpct(s['mean_precision'])}")
    if s["failures_by_where_to_fix"]:
        print("where to fix: " + ", ".join(
            f"{k} {v}" for k, v in sorted(s["failures_by_where_to_fix"].items())))
    if s["attributed"] != s["failures"]:
        print(f"WARNING: {s['failures']} failures but {s['attributed']} "
              f"attributed; these must tie", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
