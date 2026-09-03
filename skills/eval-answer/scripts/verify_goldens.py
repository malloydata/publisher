#!/usr/bin/env python3
"""Re-derive every golden from the truth package and report what no longer holds.

  python3 verify_goldens.py --set evals/ecommerce --publisher http://localhost:4811
  python3 verify_goldens.py --set ... --qid ecom_profit          # one case
  python3 verify_goldens.py --set ... --refresh                  # rewrite drifted values

A golden that cannot be re-derived is not a golden, it is a number somebody
typed once. This runs whenever the data changes, a golden is repaired, or the
truth model is edited -- and `run_baseline.py` runs it before every arm and
refuses to start on a drifted set, because 8 of 16 ecommerce goldens drifted in
four hours once and every verdict on them was noise.

Promoted from the ecommerce set's own copy on 2026-09-02, because every set was
about to get one. The set supplies what differs: `set.json` names the
`truthPackage`, and an optional `truthTableRewrite` (see below).

WHAT IT CHECKS, AND WHAT EACH CATCHES

1. value      -- the golden equals what its `canonicalQuery` returns from the
                 TRUTH package (semantics-free sources over the raw data, never
                 the model under test). Catches drift: data moved, truth model
                 edited, golden typed wrong.

2. rubric numbers -- every figure the rubric's ACCEPTING clause quotes appears
                 in the golden rows or their column sums. Would have caught the
                 authoring defect that cost an arm on the VideoAmp set: a rubric
                 that said "122,935,709.72 (group 31681)" while its own executed
                 rows held a different number, so the judge -- correctly --
                 reported the golden as suspect. Reported as REVIEW items, not
                 failures: rubrics legitimately quote intermediates (a numerator,
                 a row count) that no golden row holds, so a hit here is a
                 prompt to read the rubric against its rows, not proof of error.

3. verification axis -- when a golden carries a second derivation (`gold/<qid>.json`
                 with `verifyRows`, or `golden.verification`), it should NAME the
                 axis the verification query varies (`verification.variesAxis`,
                 beside `primaryAxis`), and it is flagged when that axis is the
                 one the primary query already varies. Review items. Two
                 derivations that both vary conversion group and share a
                 time-slice blind spot agree with each other and are both wrong;
                 that is how a 3x-too-high golden passed a two-derivation check.

4. stale rubric claims -- a rubric that asserts `X is count(...)` about the model
                 is compared with the model's own definition of X. Catches the
                 rubric that goes stale when the model is fixed.

What it deliberately does NOT do: score an answer. The oracle for an answer is
the judge (`skill:eval-judge`); scripted row comparison fails correct answers
over an extra column and passes wrong ones whose numbers coincide.

TABLE REWRITE

A canonical query may be written as `duckdb.table('data/x.parquet')` so it
reads as raw-data provenance; Publisher's query endpoint rejects that form. If
`set.json` has `"truthTableRewrite": true`, such references are rewritten to the
bare table stem, which the truth model is expected to bind to the same file.
"""
from __future__ import annotations

import argparse
import json
import pathlib
import re
import sys
from typing import Any

from publisher_rest import try_query    # the one direct path to a Publisher

_TABLE_REF = re.compile(r"""duckdb\.table\(\s*['"](?:\.\./)?data/(\w+)\.\w+['"]\s*\)""")


def rewrite_table_refs(malloy: str) -> str:
    return _TABLE_REF.sub(lambda m: m.group(1), malloy)


def close_enough(want: Any, got: Any, places: int | None) -> bool:
    if isinstance(want, (int, float)) and isinstance(got, (int, float)):
        # Compare at the precision the golden is stated to, not float exactness.
        tol = 10 ** -places / 2 if places is not None else max(abs(want) * 1e-9, 1e-9)
        return abs(float(want) - float(got)) <= max(tol, 0.011)
    return want == got


# ---------------------------------------------------------------- 1. value

def check_value(case: dict[str, Any], a: argparse.Namespace
                ) -> tuple[str, str, list[dict[str, Any]] | None]:
    """(status, detail, fresh rows). Status: ok / diff / error / skipped."""
    g = case.get("golden") or {}
    q = g.get("canonicalQuery")
    if g.get("kind") == "unanswerable":
        return "skipped", "unanswerable by design: the pass is a refusal", None
    if not q:
        return "error", "no canonicalQuery: this golden cannot be re-derived", None
    if a.rewrite:
        q = rewrite_table_refs(q)
    # try_query, not query: one bad golden reports and the sweep continues.
    rows, err = try_query(a.publisher, a.environment, a.truth_package,
                          a.truth_model, q)
    if err:
        return "error", err[:160], None

    want = g.get("value")
    places = want.get("round") if isinstance(want, dict) else None

    if g.get("kind") == "rows":
        if not isinstance(want, list):
            return "error", "kind=rows but value is not a list", rows
        if len(rows) < len(want):
            return "diff", f"golden has {len(want)} rows, query returned {len(rows)}", rows
        for i, wrow in enumerate(want):
            for k, v in wrow.items():
                if k not in rows[i]:
                    continue  # presentation-only key (rank, label)
                if not close_enough(v, rows[i][k], places):
                    return "diff", f"row {i} {k}: golden {v}, query {rows[i][k]}", rows
        return "ok", f"{len(want)} rows", rows

    if not rows:
        return "diff", "query returned no rows", rows
    got = rows[0]
    scalars = {k: v for k, v in (want or {}).items() if k not in ("currency", "round")}
    for k, v in scalars.items():
        if k not in got:
            return "diff", f"golden names {k!r}, query returned {sorted(got)}", rows
        if not close_enough(v, got[k], places):
            return "diff", f"{k}: golden {v}, query {got[k]}", rows
    return "ok", ", ".join(f"{k}={v}" for k, v in scalars.items()), rows


# ---------------------------------------------------------------- 2. rubric numbers

_NUM = re.compile(r"(?<![\w.])(\d{1,3}(?:,\d{3})+(?:\.\d+)?|\d+\.\d{2,}|\d{4,})(?![\w])")


def golden_numbers(value: Any) -> list[float]:
    out: list[float] = []

    def walk(v: Any) -> None:
        if isinstance(v, bool):
            return
        if isinstance(v, (int, float)):
            out.append(float(v))
        elif isinstance(v, dict):
            for k, x in v.items():
                if k not in ("round",):
                    walk(x)
        elif isinstance(v, list):
            for x in v:
                walk(x)

    walk(value)
    return out


_WRONG_MARK = re.compile(r"(?i)\b(close but wrong|wrong|incorrect|trap|reject)\b")


def rubric_number_findings(case: dict[str, Any]) -> list[str]:
    """Figures the rubric asserts as RIGHT that appear nowhere in the golden.

    Only the rubric's accepting clause is read -- the text before its first
    "wrong" / "close but wrong" / "trap" marker -- because the rejecting half
    quotes numbers that are supposed to be absent. Only figures specific
    enough to be a quoted result are checked (five or more significant
    digits, or a decimal with two-plus places on a value over 100); years and
    small counts are excluded by construction. A figure matches if it is a
    golden value, or the sum of a golden column -- rubrics legitimately quote
    "177,340,447.81 across the three groups".
    """
    g = case.get("golden") or {}
    rubric = g.get("rubric") or ""
    if not rubric or g.get("kind") == "unanswerable":
        return []
    m = _WRONG_MARK.search(rubric)
    accepting = rubric[:m.start()] if m else rubric
    have = golden_numbers(g.get("value"))
    val = g.get("value")
    if isinstance(val, list):
        cols: dict[str, float] = {}
        for row in val:
            for k, x in (row or {}).items():
                if isinstance(x, (int, float)) and not isinstance(x, bool):
                    cols[k] = cols.get(k, 0.0) + float(x)
        have += list(cols.values())
    text_values = {str(v) for v in _walk_strings(val)}
    bad = []
    for tok in _NUM.findall(accepting):
        if tok in text_values:
            continue
        digits = tok.replace(",", "").replace(".", "").lstrip("0")
        decimals = len(tok.split(".")[1]) if "." in tok else 0
        n = float(tok.replace(",", ""))
        if decimals == 0 and (len(digits) < 5 or 1900 <= n <= 2100):
            continue
        if decimals and n < 100 and len(digits) < 5:
            continue
        tol = max(0.5 * 10 ** -decimals, 0.5 if decimals == 0 else 0)
        if not any(abs(n - h) <= tol or (h and abs(n - h) / abs(h) < 5e-4) for h in have):
            bad.append(tok)
    return [f"review {case['qid']}: rubric asserts {t} as right; not in the golden "
            f"rows or their column sums" for t in bad]


def _walk_strings(v: Any):
    if isinstance(v, dict):
        for x in v.values():
            yield from _walk_strings(x)
    elif isinstance(v, list):
        for x in v:
            yield from _walk_strings(x)
    elif isinstance(v, (str, int)) and not isinstance(v, bool):
        yield v


# ---------------------------------------------------------------- 3. verification axis

def axis_findings(case: dict[str, Any], set_dir: pathlib.Path) -> list[str]:
    g = case.get("golden") or {}
    if g.get("kind") == "unanswerable":
        return []
    ver = g.get("verification") or {}
    side = set_dir / "gold" / f"{case['qid']}.json"
    has_second = bool(ver) or (side.exists() and "verifyRows" in json.loads(side.read_text()))
    if not has_second:
        return []
    axis = ver.get("variesAxis")
    primary = ver.get("primaryAxis")
    if not axis:
        return [f"review {case['qid']}: has a second derivation but does not name "
                f"the axis it varies (golden.verification.variesAxis)"]
    if primary and axis == primary:
        return [f"review {case['qid']}: verification varies {axis!r}, the same axis "
                f"as the primary query -- the two share every other blind spot"]
    return []


# ---------------------------------------------------------------- 4. stale rubric claims

DEFINES = re.compile(r"^\s*(\w+)\s+is\s+(.+?)\s*$", re.M)
ASSERTS = re.compile(r"(\w+)\s+is\s+(?:defined\s+as\s+)?(count\([^)]*\)|sum\([^)]*\))")


def model_definitions(model_path: pathlib.Path | None) -> dict[str, str]:
    if not model_path or not model_path.exists():
        return {}
    out: dict[str, str] = {}
    files = [model_path] if model_path.is_file() else sorted(model_path.glob("*.malloy"))
    for f in files:
        for name, expr in DEFINES.findall(f.read_text()):
            out.setdefault(name, expr.split("#")[0].strip())
    return out


def stale_rubric_claims(cases: list[dict[str, Any]], defs: dict[str, str]) -> list[str]:
    bad = []
    for case in cases:
        rubric = ((case.get("golden") or {}).get("rubric") or "")
        for name, claimed in ASSERTS.findall(rubric):
            actual = defs.get(name)
            if actual is None:
                continue
            if claimed.replace(" ", "") not in actual.replace(" ", ""):
                bad.append(f"{case['qid']}: rubric says {name} is {claimed}, "
                           f"model says {name} is {actual}")
    return bad


# ---------------------------------------------------------------- driver

def verify(set_dir: pathlib.Path, publisher: str, environment: str,
           *, qids: set[str] | None = None, model: pathlib.Path | None = None,
           refresh: bool = False, cases_file: str = "cases.jsonl",
           quiet: bool = False) -> dict[str, Any]:
    """Run every check. Returns a summary; `drifted` is the count that should
    stop a run."""
    meta = json.loads((set_dir / "set.json").read_text()) if (set_dir / "set.json").exists() else {}
    a = argparse.Namespace(
        publisher=publisher, environment=environment,
        truth_package=meta.get("truthPackage"),
        truth_model=meta.get("truthModel", "truth.malloy"),
        rewrite=bool(meta.get("truthTableRewrite", False)))
    if not a.truth_package:
        return {"skipped": "set.json names no truthPackage; nothing to re-derive against",
                "drifted": 0, "findings": []}

    path = set_dir / cases_file
    lines = path.read_text().splitlines()
    cases = [json.loads(l) for l in lines if l.strip()]
    chosen = [c for c in cases if not qids or c["qid"] in qids]

    tally: dict[str, int] = {}
    findings: list[str] = []
    refreshed: list[str] = []
    for c in chosen:
        status, detail, rows = check_value(c, a)
        tally[status] = tally.get(status, 0) + 1
        if not quiet:
            print(f"  {status.upper():7s} {c['qid']:34s} {detail}")
        if status == "diff":
            findings.append(f"{c['qid']}: {detail}")
            if refresh and rows is not None:
                g = c["golden"]
                if g.get("kind") == "rows":
                    keep = list((g.get("value") or [{}])[0].keys()) or None
                    g["value"] = [{k: r.get(k) for k in keep} if keep else r
                                  for r in rows[:len(g.get("value") or rows)]]
                else:
                    want = g.get("value") or {}
                    g["value"] = {**want, **{k: rows[0].get(k) for k in want
                                             if k not in ("currency", "round")
                                             and k in rows[0]}}
                g["verifiedBy"] = "verify_goldens.py --refresh"
                c["goldenRevision"] = int(c.get("goldenRevision") or 1) + 1
                refreshed.append(c["qid"])
        findings += rubric_number_findings(c)
        findings += axis_findings(c, set_dir)
    findings += stale_rubric_claims(chosen, model_definitions(model))

    if refreshed:
        by_qid = {c["qid"]: c for c in cases}
        out = []
        for l in lines:
            if not l.strip():
                continue
            q = json.loads(l)["qid"]
            out.append(json.dumps(by_qid[q]) if q in refreshed else l)
        path.write_text("\n".join(out) + "\n")
        if not quiet:
            print(f"\n  refreshed {len(refreshed)} golden(s): {', '.join(refreshed)} "
                  f"-- goldenRevision bumped; bump set.json datasetVersion and note "
                  f"that runs before it are not comparable")

    drifted = tally.get("diff", 0) + tally.get("error", 0)
    if not quiet:
        print("\n" + "  ".join(f"{k}={v}" for k, v in sorted(tally.items())))
        other = [f for f in findings if not any(f.startswith(x + ": ") and
                 (" golden " in f or "query returned" in f) for x in (c["qid"] for c in chosen))]
        hard = [f for f in other if not f.startswith("review ")]
        soft = [f for f in other if f.startswith("review ")]
        if hard:
            print(f"\n{len(hard)} finding(s) besides value drift:")
            for f in hard:
                print(f"  {f}")
        if soft:
            print(f"\n{len(soft)} rubric figure(s) to review (not failures):")
            for f in soft[:20]:
                print(f"  {f[len('review '):]}")
            if len(soft) > 20:
                print(f"  ... and {len(soft) - 20} more")
    return {"tally": tally, "drifted": drifted, "findings": findings,
            "refreshed": refreshed}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--set", dest="set_dir", required=True, type=pathlib.Path)
    ap.add_argument("--publisher", default="http://localhost:4811",
                    help="the Publisher serving the TRUTH package")
    ap.add_argument("--environment", default="samples")
    ap.add_argument("--qid", action="append", help="verify only these cases")
    ap.add_argument("--cases", default="cases.jsonl",
                    help="case file to verify, relative to the set dir")
    ap.add_argument("--model", type=pathlib.Path, default=None,
                    help="the model under test (file or package dir), for the "
                         "stale-rubric audit")
    ap.add_argument("--refresh", action="store_true",
                    help="rewrite each drifted golden's value from the fresh rows "
                         "and bump its goldenRevision. For drift, not for a wrong "
                         "canonical query -- read the diff first")
    args = ap.parse_args()

    r = verify(args.set_dir, args.publisher, args.environment,
               qids=set(args.qid) if args.qid else None, model=args.model,
               refresh=args.refresh, cases_file=args.cases)
    if r.get("skipped"):
        print(r["skipped"])
        return 0
    # Drift and findings both fail: a golden that cannot be re-derived is a
    # broken set, and a rubric quoting a number its rows do not hold fails the
    # same way -- as wrong verdicts, silently.
    hard = [f for f in r["findings"] if not f.startswith("review ")]
    return 1 if r["drifted"] or hard else 0


if __name__ == "__main__":
    sys.exit(main())
