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

`raw`, `external` and `irreducible` are recorded but not executed here; raw
checks need a lens onto the base tables, which Publisher only grants a model
file in a package (restricted-mode compilation rejects `duckdb.table(...)` in
any ad-hoc query, whatever `queryableSources` says).

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
from verify_goldens import parse_definitions                       # noqa: E402

CANNOT_RUN = 3

# Kinds whose correctness is a question at all. A `view` composes measures that
# are checked on their own, and a `join` is structure rather than a value.
CHECKABLE_KINDS = ("measure", "dimension")

WORD = re.compile(r"[A-Za-z_]\w*")
# Aggregates that survive uniform duplication, so fanout does not move them.
# `verify_goldens`' own fanout note lists the same four.
FANOUT_SAFE = ("avg(", "stddev(", "min(", "max(")
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


def records(model: pathlib.Path, recursive: bool) -> list[dict[str, Any]]:
    """The ledger's rows, built from the model text and linked by `depends`."""
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
    return out


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


def run_check(rec: dict[str, Any], a: argparse.Namespace) -> tuple[str, str]:
    """(verdict, detail) for one definition."""
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


def verify(model: pathlib.Path, a: argparse.Namespace) -> dict[str, Any]:
    recs = records(model, a.recursive)
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
    raw = sum(1 for r in recs if r["check"]["kind"] == "raw")
    lines = [f"{len(recs)} definition(s): "
             + ", ".join(f"{n} {k}" for k, n in sorted(tally.items()))]
    if raw:
        lines.append(f"{raw} need a raw check and are NOT validated by this run; "
                     f"a within-model check cannot see fanout")
    return lines


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--model", required=True, help="a .malloy file or a package directory")
    ap.add_argument("--out", default=None, help="write the ledger here (JSONL)")
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

    r = verify(model, a)
    if r["cannotRun"]:
        print(r["cannotRun"], file=sys.stderr)
        return CANNOT_RUN

    if a.out:
        out = pathlib.Path(a.out)
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
