#!/usr/bin/env python3
"""Validate a model's DEFINITIONS once, so answer keys need not be re-derived per case.

WHY

The loop's standing rule is that a golden must never be derived through the
model under test, because a model bug would certify its own golden. That rule
is sound and it does not scale: on a model with real transformation, deriving
every key independently means reimplementing the model, and you end up with two
implementations and no reason to trust either.

This is the other half. A definition is checked ONCE, against the layer directly
beneath it; every case that depends on it inherits the result. Work then scales
with the number of definitions rather than the number of cases, and a deep model
is CHEAPER per case than a shallow one, because deep chains share their lower
layers. Measured on a 548-definition customer model: the twenty deepest measures
close over 812 definition-checks without reuse and 116 with it.

WHAT IT CHECKS, AND WHAT IT CANNOT

`within_model`, the only kind this script runs today: ask the model for the
measure and for the expression it CLAIMS to be, in one query, and compare.

    run: order_items -> {
      aggregate: stated is total_sales, control is sale_price.sum()
    }

That catches a measure that is not what it says -- a filter nobody mentioned, a
renamed column, the wrong aggregate. It does NOT catch a bad source: a join that
fans out inflates both sides equally, and the comparison stays green. Measures
that cross a join therefore record `unchecked` with `needs: raw`, never `agrees`.
Reporting them as validated would be this tool committing the exact error it
exists to find.

`raw` is an AUTHORED control. The plan for this file once said raw checks would
"catch a definition wrong about the business, like the cogs case". They cannot:
re-deriving the definition's own expression over the base tables computes the
same quantity, cancelled lines and all, and agrees. What caught the cogs case
was a person who read the `status` doc and wrote down what the population
should be. So a raw check is a control query the conductor authors into the
ledger record -- `check.query`, a `run:` returning one row with a `control`
column -- and this file re-runs and compares it every time, against the model
by default (`check.against: model`) or against a truth package of raw tables
(`against: truth`, which needs --truth-publisher). A control may be authored on
any measure, not only a raw one, and it supersedes the within-model check: that
check can say a measure is what its expression says, a person can say what its
population should be. A raw record with no authored query stays `unchecked`,
and `needs` says so. That is "validate what
someone asserted", not "find business-wrong definitions unaided", and the
difference is the whole point.

Build once, then maintain. `--out` writes a fresh ledger. `--ledger` reads an
existing one, keeps every field a person wrote (`check.query`, `check.against`,
`check.note`, `cause`), re-parses the model, re-runs every check, and writes it
back, so an authored control survives every rebuild and a definition that moved
gets a fresh verdict rather than a stale `agrees`.

EXIT CODES, matching `verify_goldens.py`

    0  every check that could run ran, and agreed
    1  a definition disagrees with its own stated expression
    3  could not run: no model, no server, or nothing checkable

3 is load-bearing. A caller must treat anything outside {0, 1} as "did not run"
and must never read it as a pass.

USAGE

    python3 verify_definitions.py --model <file-or-dir> --out <ledger.jsonl>
    python3 verify_definitions.py --model m.malloy --publisher http://localhost:4811 \\
        --environment samples --package ecommerce --out evals/definitions/ecommerce.jsonl
    # after authoring check.query on the raw records:
    python3 verify_definitions.py --model m.malloy --ledger evals/definitions/ecommerce.jsonl \\
        --publisher http://localhost:4811 --package ecommerce --model-path ecommerce.malloy
"""
from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import re
import sys
import traceback
from typing import Any

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from mcp_payload import entity_id                                  # noqa: E402
from publisher_rest import try_query                               # noqa: E402
from verify_goldens import has_second_derivation, parse_definitions  # noqa: E402

CANNOT_RUN = 3

# Kinds whose correctness is a question at all. A `view` composes measures that
# are checked on their own, and a `join` is structure rather than a value.
CHECKABLE_KINDS = ("measure", "dimension")

WORD = re.compile(r"[A-Za-z_]\w*")
# Aggregates that survive uniform duplication, so fanout does not move them.
# `verify_goldens`' own fanout note lists the same four.
FANOUT_SAFE = ("avg(", "stddev(", "min(", "max(")
# What a person writes into a record and a rebuild must never lose. Everything
# else on the record is recomputed from the model.
AUTHORED_CHECK_FIELDS = ("query", "against", "note")
AUTHORED_FIELDS = ("cause",)
# Rows sampled for a dimension comparison. A wrong dimension shows on a slice,
# and the slice is recorded so nobody reads it as a whole-table proof.
DIM_SLICE = 200


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def expr_sha(expr: str, dep_shas: list[str]) -> str:
    """Content hash of a definition AND everything beneath it.

    A Merkle chain, not a flat hash of the one line. A measure whose own text
    never changed is still stale when a definition it builds on moves, and a
    flat hash reports it as current -- which is how a ledger goes quietly wrong.
    `packageSha` exists but is whole-package, so it invalidates everything on
    any edit and can distinguish nothing.
    """
    return sha256_text(expr + "|" + "|".join(sorted(dep_shas)))


def incomplete(expr: str) -> bool:
    """Whether the captured expression is only the first line of a longer one.

    `parse_definitions` reads line by line, so `full_name is concat(` comes back
    truncated at the paren. Interpolating that into a check query builds
    something that cannot compile, and the failure would read as "the server
    rejected it" rather than "this tool never had the whole definition".
    Unbalanced delimiters are the cheap, reliable tell.
    """
    return (expr.count("(") != expr.count(")")
            or expr.count("{") != expr.count("}")
            or expr.count("[") != expr.count("]"))


def needs_raw(expr: str, joins: set[str]) -> str | None:
    """Why a within-model check would be blind here, or None if it would not.

    The within-model check compares a measure against its own stated expression
    THROUGH the model, so anything that corrupts both sides equally is invisible
    to it. A join is the case that matters: fanout multiplies the stated measure
    and the control expression identically.

    Detection needs the join NAMES, not a dot. `sale_price.sum()` is a method
    call on a column in this very source and is perfectly checkable;
    `inventory_items.cost.sum()` traverses a join and is not. A first pass
    matched any `word.word` and held back `total_sales` -- the measure twelve
    of the set's cases depend on -- for a join it does not cross.
    """
    hops = {m.split(".", 1)[0] for m in re.findall(r"\b\w+\.\w+", expr)}
    crossed = sorted(hops & joins)
    if crossed and not any(f in expr.lower() for f in FANOUT_SAFE):
        return (f"reaches through {', '.join(crossed)}, so fanout would inflate "
                f"both sides equally")
    return None


def records(model: pathlib.Path, recursive: bool,
            existing: dict[str, dict[str, Any]] | None = None
            ) -> list[dict[str, Any]]:
    """The ledger's rows, built from the model text and linked by `depends`.

    `existing` is a prior ledger keyed by entityId. Fields a person wrote on it
    are carried onto the rebuilt row; nothing else is, so a verdict is never
    inherited from a definition that has since moved. A row whose definition
    the model no longer declares is dropped, authored fields and all -- a
    control for a measure that no longer exists is not a validated measure.
    """
    parsed = parse_definitions(model, recursive=recursive)
    by_name: dict[str, dict[str, Any]] = {}
    for r in parsed:
        by_name.setdefault(r["name"], r)
    # Anything an expression can traverse INTO: a declared join, or a source
    # name used as one. Both make the within-model comparison fanout-blind.
    joins = ({r["name"] for r in parsed if r["kind"] == "join"}
             | {r["source"] for r in parsed if r["source"]})

    def deps_of(rec: dict[str, Any]) -> list[str]:
        return sorted({w for w in WORD.findall(rec["expr"])
                       if w in by_name and w != rec["name"]})

    # Depth-first so a dependency's sha exists before its dependants'.
    shas: dict[str, str] = {}

    def sha_of(name: str, stack: frozenset = frozenset()) -> str:
        if name in shas:
            return shas[name]
        if name in stack:                       # cycle: hash the text alone
            return sha256_text(by_name[name]["expr"])
        rec = by_name[name]
        v = expr_sha(rec["expr"],
                     [sha_of(d, stack | {name}) for d in deps_of(rec)])
        shas[name] = v
        return v

    out = []
    for r in parsed:
        if r["kind"] not in CHECKABLE_KINDS:
            continue
        deps = deps_of(r)
        why = ("spans more than one line, so only its first line was read"
               if incomplete(r["expr"])
               else needs_raw(r["expr"], joins - {r["source"]}))
        out.append({
            "entityId": entity_id(r["kind"], r["source"], r["name"]),
            "kind": r["kind"],
            "source": r["source"],
            "name": r["name"],
            "expr": r["expr"],
            "exprSha": sha_of(r["name"]),
            "depends": [entity_id(by_name[d]["kind"], by_name[d]["source"], d)
                        for d in deps],
            "check": {"kind": "unreadable" if incomplete(r["expr"])
                              else "raw" if why else "within_model"},
            "verdict": "unchecked",
            "needs": why,
            "file": r["file"],
            "line": r["line"],
        })
    if existing:
        for rec in out:
            prior = existing.get(rec["entityId"]) or {}
            for k in AUTHORED_CHECK_FIELDS:
                if k in (prior.get("check") or {}):
                    rec["check"][k] = prior["check"][k]
            for k in AUTHORED_FIELDS:
                if k in prior:
                    rec[k] = prior[k]
    return out


def stated_query(rec: dict[str, Any]) -> str:
    """The measure's own value through the model, on its own. A control query
    is the other side, written by a person, and may run elsewhere."""
    return (f"run: {rec['source']} -> {{\n"
            f"  aggregate: stated is {rec['name']}\n"
            f"}}")


def within_model_query(rec: dict[str, Any]) -> str:
    """One query asking for the entity and its own stated expression together.

    Both in one query against one source, so they see identical rows: a
    difference is the definition, not the population.

    A measure aggregates; a dimension is scalar and `aggregate:` rejects it
    outright ("Cannot use a scalar field in an aggregate"), so a dimension is
    compared row by row over a slice instead. The slice is the point rather
    than a shortcut: a wrong dimension differs on the first rows that reach it.
    """
    if rec["kind"] == "measure":
        return (f"run: {rec['source']} -> {{\n"
                f"  aggregate:\n"
                f"    stated is {rec['name']}\n"
                f"    control is {rec['expr']}\n"
                f"}}")
    return (f"run: {rec['source']} -> {{\n"
            f"  select:\n"
            f"    stated is {rec['name']}\n"
            f"    control is {rec['expr']}\n"
            f"  limit: {DIM_SLICE}\n"
            f"}}")


def close_enough(a: Any, b: Any) -> bool:
    """Floats from two aggregation orders differ in the last places; that is
    association, not disagreement. `verify_goldens` tolerates the same."""
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        if a == b:
            return True
        scale = max(abs(a), abs(b), 1.0)
        return abs(a - b) / scale < 1e-9
    return a == b


def run_authored(rec: dict[str, Any], a: argparse.Namespace) -> tuple[str, str]:
    """(verdict, detail) for a raw record: the measure through the model against
    the control a person wrote. Two queries, deliberately -- the control is
    supposed to differ in population, and may run on another server."""
    q = (rec["check"].get("query") or "").strip()
    if not q:
        return "unchecked", ((rec.get("needs") or "reaches through a join")
                             + "; no authored control yet (check.query)")
    if rec["kind"] != "measure":
        return "unchecked", ("an authored control compares one aggregate; this "
                             "is a dimension")
    against = rec["check"].get("against") or "model"
    if against == "model":
        target = (a.publisher, a.environment, a.package, a.model_path)
    elif against == "truth":
        if not getattr(a, "truth_publisher", None):
            return "unchecked", ("check.against is truth but no --truth-publisher "
                                 "was given, so the control could not run")
        target = (a.truth_publisher, a.truth_environment or a.environment,
                  a.truth_package, a.truth_model)
    else:
        return "unchecked", f"check.against is {against!r}; expected model or truth"
    stated_rows, err = try_query(a.publisher, a.environment, a.package,
                                 a.model_path, stated_query(rec))
    if err:
        return "unchecked", f"stated query failed: {err[:120]}"
    control_rows, err = try_query(*target, q)
    if err:
        return "unchecked", f"control query failed: {err[:120]}"
    if not stated_rows or not control_rows:
        return "unchecked", "a side returned no rows"
    if "control" not in control_rows[0]:
        return "unchecked", ("the control query must return a column named "
                             "`control`; got " + ", ".join(control_rows[0]))
    stated, control = stated_rows[0].get("stated"), control_rows[0]["control"]
    rec["check"]["against"] = against
    if close_enough(stated, control):
        return "agrees", (f"stated={stated} control={control} (authored control, "
                          f"against {against})")
    return "disagrees", (f"stated={stated} but the authored control gives "
                         f"{control} (against {against}) -- "
                         f"{rec['check'].get('note') or 'see check.query'}")


def run_check(rec: dict[str, Any], a: argparse.Namespace) -> tuple[str, str]:
    """(verdict, detail) for one definition.

    An authored control wins on ANY measure record, not only a raw one. The
    within-model check can only say a measure is what its expression says; a
    person can say what its population should be, and `total_sales is
    sale_price.sum()` -- which agrees with itself while including the cancelled
    lines its own docs exclude -- is exactly the case where the second question
    matters more than the first.
    """
    if (rec["check"].get("query") or "").strip() and rec["kind"] == "measure":
        return run_authored(rec, a)
    if rec["check"]["kind"] == "raw":
        return run_authored(rec, a)
    if rec["check"]["kind"] != "within_model":
        return "unchecked", rec["needs"] or "not a within-model check"
    if not rec["source"]:
        return "unchecked", "no enclosing source, so there is nothing to query"
    q = within_model_query(rec)
    rows, err = try_query(a.publisher, a.environment, a.package, a.model_path, q)
    if err:
        return "unchecked", f"query failed: {err[:120]}"
    if not rows:
        return "unchecked", "query returned no rows"
    for n, row in enumerate(rows, 1):
        stated, control = row.get("stated"), row.get("control")
        if stated is None and control is None:
            continue
        if not close_enough(stated, control):
            where = f" (row {n} of {len(rows)})" if len(rows) > 1 else ""
            return "disagrees", (f"stated={stated} but its own expression gives "
                                 f"{control}{where} -- {rec['name']} is not "
                                 f"{rec['expr']}")
    if all(r.get("stated") is None and r.get("control") is None for r in rows):
        return "unchecked", "every sampled row was null on both sides"
    if len(rows) > 1:
        rec["check"]["slice"] = f"first {len(rows)} rows"
        return "agrees", f"{len(rows)} rows match"
    return "agrees", f"stated={rows[0].get('stated')} control={rows[0].get('control')}"


def verify(model: pathlib.Path, a: argparse.Namespace,
           existing: dict[str, dict[str, Any]] | None = None) -> dict[str, Any]:
    recs = records(model, a.recursive, existing)
    if not recs:
        return {"records": [], "findings": [], "cannotRun":
                "no measure or dimension definitions found in the model"}
    findings: list[str] = []
    if a.publisher:
        for r in recs:
            verdict, detail = run_check(r, a)
            r["verdict"], r["detail"] = verdict, detail
            if verdict == "disagrees":
                findings.append(f"{r['entityId']}: {detail}")
            if not a.quiet:
                print(f"  {verdict.upper():10s} {r['entityId']:52s} {detail[:70]}")
    return {"records": recs, "findings": findings, "cannotRun": None}


def summarise(recs: list[dict[str, Any]]) -> list[str]:
    tally: dict[str, int] = {}
    for r in recs:
        tally[r["verdict"]] = tally.get(r["verdict"], 0) + 1
    raw = [r for r in recs if r["check"]["kind"] == "raw"]
    authored = sum(1 for r in raw if (r["check"].get("query") or "").strip())
    lines = [f"{len(recs)} definition(s): "
             + ", ".join(f"{n} {k}" for k, n in sorted(tally.items()))]
    if raw:
        lines.append(f"{len(raw)} reach through a join: {authored} carry an "
                     f"authored control, {len(raw) - authored} do not and are "
                     f"NOT validated by this run")
    return lines


# ------------------------------------------------- reading a ledger back

# Kinds that hold no value, so there is nothing a model bug could get wrong.
VALUE_FREE_KINDS = ("criteria", "unanswerable")


def load_ledger(path: pathlib.Path | None) -> dict[str, dict[str, Any]]:
    """`entityId -> record`, or empty when there is no ledger."""
    if not path or not path.exists():
        return {}
    out = {}
    for line in path.read_text().splitlines():
        if line.strip():
            r = json.loads(line)
            out[r["entityId"]] = r
    return out


def stale_ids(ledger: dict[str, dict[str, Any]], model: pathlib.Path | None,
              recursive: bool = False) -> set[str]:
    """Ledger rows whose definition has moved since they were checked.

    Pure hash comparison, no queries, so this is cheap enough to run before
    every arm. An id the model no longer declares counts as stale too: a
    definition that was renamed away is not a validated definition.
    """
    if not model:
        return set()
    current = {r["entityId"]: r["exprSha"] for r in records(model, recursive)}
    return {eid for eid, rec in ledger.items()
            if current.get(eid) != rec.get("exprSha")}


def tested_ids(case: dict[str, Any]) -> list[str]:
    """The definitions a case's answer depends on.

    Read from `expectedEntities`, which already names entities in the same
    `kind:source:name` form the ledger keys on, rather than from a new
    hand-maintained field. A wrong id here is the failure mode that cost a real
    set two days, so this reuses a link the set already maintains and that
    `verify_goldens` check 5 already audits against the model.
    """
    exp = case.get("expectedEntities") or {}
    out = list(exp.get("required") or [])
    for group in exp.get("requiredAnyOf") or []:
        out += list(group or [])
    return out


def case_basis(case: dict[str, Any], ledger: dict[str, dict[str, Any]],
               stale: set[str], set_dir: pathlib.Path | None = None) -> str:
    """What this case's verdict rests on: independent, definitions, unchecked
    or disagrees.

    The composition rule, applied per case: a golden is trustworthy if it was
    derived independently, OR if every definition it tests has been validated.
    Anything else is `unchecked` -- a statement about the EVIDENCE, not about
    the answer, and never to be read as a failing case.

    Independence is read from `golden.verification` (or `gold/<qid>.json`) via
    `verify_goldens.has_second_derivation`, which is the structured record of
    "two differently shaped derivations agree". NOT from `verifiedBy`: that is
    free text, and a first pass matched it by prefix and classified 34 goldens
    of the ecommerce set as unchecked when every one of them says "authored and
    re-derived against ecommerce-truth". A well-founded set reading as
    unvalidated is the same over-claim as an unfounded one reading as validated,
    pointed the other way.
    """
    g = case.get("golden") or {}
    if g.get("kind") in VALUE_FREE_KINDS:
        return "independent"
    if set_dir is not None and has_second_derivation(case, set_dir):
        return "independent"
    ids = tested_ids(case)
    if not ids:
        return "unchecked"
    worst = "definitions"
    for eid in ids:
        rec = ledger.get(eid)
        if rec is None or eid in stale or rec.get("verdict") == "unchecked":
            worst = "unchecked"
        elif rec.get("verdict") == "disagrees":
            return "disagrees"
    return worst


def evidence_basis(cases: list[dict[str, Any]],
                   ledger: dict[str, dict[str, Any]],
                   stale: set[str],
                   set_dir: pathlib.Path | None = None) -> dict[str, Any]:
    """Counts per basis, plus the ids worth naming. Pure, so tests can pin it."""
    counts: dict[str, int] = {}
    disagreeing: set[str] = set()
    for c in cases:
        b = case_basis(c, ledger, stale, set_dir)
        counts[b] = counts.get(b, 0) + 1
        # Collected for EVERY case, not only those whose basis is "disagrees".
        # A golden derived from raw tables is trustworthy however wrong the
        # model's own definition is -- and that the definition is wrong is
        # exactly the finding the run exists to surface. On the ecommerce set
        # every golden is independent, `total_sales` disagrees with its own
        # docs once a control is authored, and without this the run would have
        # said nothing about it.
        disagreeing |= {e for e in tested_ids(c)
                        if (ledger.get(e) or {}).get("verdict") == "disagrees"}
    return {"counts": counts, "disagreeing": sorted(disagreeing),
            "stale": sorted(stale), "ledgerSize": len(ledger)}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--model", required=True, help="a .malloy file or a package directory")
    ap.add_argument("--out", default=None, help="write a fresh ledger here (JSONL)")
    ap.add_argument("--ledger", default=None,
                    help="an existing ledger to read, re-check and write back, "
                         "keeping every field a person authored on it")
    ap.add_argument("--truth-publisher", dest="truth_publisher", default=None,
                    help="server for controls with check.against: truth")
    ap.add_argument("--truth-environment", dest="truth_environment", default=None)
    ap.add_argument("--truth-package", dest="truth_package", default=None)
    ap.add_argument("--truth-model", dest="truth_model", default="truth.malloy")
    ap.add_argument("--publisher", default=None,
                    help="server to run the checks against; without it the "
                         "ledger is built and nothing is checked")
    ap.add_argument("--environment", default="samples")
    ap.add_argument("--package", default=None)
    ap.add_argument("--model-path", dest="model_path", default=None,
                    help="model path within the package, for the query endpoint")
    ap.add_argument("--recursive", action="store_true",
                    help="read .malloy files in subdirectories too")
    ap.add_argument("--quiet", action="store_true")
    a = ap.parse_args(argv)

    model = pathlib.Path(a.model)
    if not model.exists():
        print(f"--model {a.model} does not exist", file=sys.stderr)
        return CANNOT_RUN
    if a.publisher and not (a.package and a.model_path):
        print("--publisher needs --package and --model-path to address a query",
              file=sys.stderr)
        return CANNOT_RUN
    if a.truth_publisher and not a.truth_package:
        print("--truth-publisher needs --truth-package", file=sys.stderr)
        return CANNOT_RUN
    existing = None
    if a.ledger:
        led = pathlib.Path(a.ledger)
        if not led.exists():
            print(f"--ledger {a.ledger} does not exist; build one with --out first",
                  file=sys.stderr)
            return CANNOT_RUN
        existing = load_ledger(led)

    r = verify(model, a, existing)
    if r["cannotRun"]:
        print(r["cannotRun"], file=sys.stderr)
        return CANNOT_RUN

    # --ledger writes back where it read from unless --out says otherwise.
    if a.out or a.ledger:
        out = pathlib.Path(a.out or a.ledger)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text("".join(json.dumps(x) + "\n" for x in r["records"]))
        if not a.quiet:
            print(f"\nwrote {len(r['records'])} record(s) to {out}")

    if not a.quiet:
        for line in summarise(r["records"]):
            print(f"  {line}")

    if r["findings"]:
        print(f"\n{len(r['findings'])} definition(s) disagree with their own "
              f"expression:", file=sys.stderr)
        for f in r["findings"]:
            print(f"  {f}", file=sys.stderr)
        return 1

    if not a.publisher:
        print("\nNo --publisher, so nothing was checked: this built the ledger "
              "and validated nothing. Do not read it as a pass.", file=sys.stderr)
        return CANNOT_RUN
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except SystemExit:
        raise
    except Exception:
        traceback.print_exc()
        print("\nverify_definitions could not run, so this says NOTHING about the "
              "definitions. Fix the error above; do not read it as a pass.",
              file=sys.stderr)
        sys.exit(CANNOT_RUN)
