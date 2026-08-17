#!/usr/bin/env python3
"""Deterministic row-matching oracle for eval-answer. Stdlib only.

This decides WHETHER an answer is right. Nothing here is a judgment call, and no
agent should ever re-implement it: an LLM asked to compare result rows called 20 of
33 wrong answers correct (61% false-positive) and issued a confident false-negative
on a perfect answer. Row matching is code. Judgment starts at *why* it failed, which
is a different skill.

  python match_rows.py --gold GOLD.csv --pred PRED.csv [--status ok] [--json]

Exit code is 0 whether or not the answer is correct -- a wrong answer is data, not a
harness failure. Exit 1 means the comparison itself could not be performed.

WHY ROWS ARE SPLIT INTO A TEXT KEY AND A NUMERIC VECTOR
-------------------------------------------------------
Rounding floats to N significant digits and comparing the strings puts a hard
boundary in the middle of the value space: 169.14900 and 169.148949 are the same
number to within 6e-7 (float32 noise on a 4-byte column) but straddle a 7-digit
boundary and compare unequal. Any fixed digit count has this problem somewhere. So
rows are split into an EXACT text key and a numeric vector compared under relative
tolerance: text is matched exactly (grouping on it is safe), and numbers never pass
through a rounding boundary at all.

This is also why a sub-ULP float difference cannot fail a correct answer -- a real
concern raised during a live run, tested, and found unfounded. Do not re-raise it.
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from collections import Counter, defaultdict

# Significant digits used only for canonical *text* rendering of numerics. Actual
# numeric comparison uses REL_TOL/ABS_TOL below, not this.
SIG_DIGITS = 7
REL_TOL = 1e-6
ABS_TOL = 1e-9

# Spellings an engine or driver may emit for SQL NULL, and for booleans. Verified safe
# to collapse: across 995 gold CSVs (88.8M cells) no cell is literally NULL/None/NaN/
# N/A or true/false, so normalizing these cannot make two distinct values collide.
_NULL_WORDS = {"null", "none", "nan", "n/a", "<na>", "nat"}
_BOOL_WORDS = {"true": "1", "false": "0"}

_ERROR_MARKERS = ("Error:", "ERROR:", "Traceback (most recent call last)")


def _canon(val) -> str:
    """Normalize a cell: strip quotes/whitespace/%/thousands commas, fold NULL and
    boolean spellings, render numerics to SIG_DIGITS. Empty/NULL -> "" (not matched)."""
    if val is None or str(val).strip() == "":
        return ""
    s = str(val).replace('"', "").replace("'", "").strip()
    if s.lower() in _NULL_WORDS:
        return ""
    if s.lower() in _BOOL_WORDS:
        return _BOOL_WORDS[s.lower()]
    if s.endswith("%"):
        s = s[:-1].strip()
    s = s.replace(",", "")
    try:
        f = float(s)
    except (ValueError, TypeError):
        return s
    if f == 0:  # avoid "-0" / "0e+00" spellings
        return "0"
    return f"{f:.{SIG_DIGITS}g}"


def _canon_cell(val):
    """Like _canon, but numerics stay floats. Returns "" | float | str."""
    s = _canon(val)
    if s == "":
        return ""
    try:
        return float(s)
    except ValueError:
        return s


def read_rows(path: str):
    """Data rows of a result CSV (row 0 is a header and is dropped).

    Returns (rows, None) or (None, error). A file holding an execution error is not
    an empty result -- an empty result carries evidence about the data, an error
    carries none, and conflating them makes the error path actively worse.
    """
    try:
        with open(path, newline="", encoding="utf-8", errors="replace") as f:
            head = f.read(2048)
            if any(m in head for m in _ERROR_MARKERS):
                return None, "file contains execution error"
            f.seek(0)
            rows = [r for r in csv.reader(f)]
    except OSError as exc:
        return None, f"unreadable: {exc}"
    if not rows:
        return [], None
    return rows[1:], None  # drop the header row


def _split_row(values: list) -> tuple[tuple, tuple]:
    """(exact text key, sorted numeric vector) for a row; empty cells dropped."""
    texts, nums = [], []
    for v in values:
        c = _canon_cell(v)
        if c == "":
            continue
        (nums if isinstance(c, float) else texts).append(c)
    return tuple(sorted(texts)), tuple(sorted(nums))


def _vectors_close(a: tuple, b: tuple) -> bool:
    return len(a) == len(b) and all(
        math.isclose(x, y, rel_tol=REL_TOL, abs_tol=ABS_TOL) for x, y in zip(a, b))


def matched_rows(gold: list, pred: list) -> int:
    """How many gold rows pair with a DISTINCT pred row (text exact, numbers tolerant)."""
    gb, pb = defaultdict(list), defaultdict(list)
    for r in gold:
        k, n = _split_row(r)
        gb[k].append(n)
    for r in pred:
        k, n = _split_row(r)
        pb[k].append(n)
    matched = 0
    for key, gnums in gb.items():
        pnums = pb.get(key)
        if not pnums:
            continue
        used = [False] * len(pnums)
        for gv in sorted(gnums):
            for i, pv in enumerate(sorted(pnums)):
                if not used[i] and _vectors_close(gv, pv):
                    used[i] = True
                    matched += 1
                    break
    return matched


def graded_match(gold_path: str, pred_path: str) -> dict:
    """Row-level precision / recall / F1.

    F1, not binary correctness, is the signal to optimize. Binary strict is just the
    F1 == 1 case, but the partial score keeps a near-miss (115 of 117 rows)
    distinguishable from garbage -- a fairer number, a far better learning signal,
    and roughly 4x cheaper in sample size for the same statistical power.
    """
    g, ge = read_rows(gold_path)
    p, pe = read_rows(pred_path)
    if ge or pe or g is None or p is None:
        return {"precision": 0.0, "recall": 0.0, "f1": 0.0, "strict": False,
                "n_gold": 0, "n_pred": 0, "matched": 0}
    m = matched_rows(g, p)
    prec = m / len(p) if p else 0.0
    rec = m / len(g) if g else 0.0
    f1 = (2 * prec * rec / (prec + rec)) if (prec + rec) else 0.0
    return {"precision": round(prec, 4), "recall": round(rec, 4), "f1": round(f1, 4),
            "strict": bool(len(g) == len(p) == m), "n_gold": len(g), "n_pred": len(p),
            "matched": m}


def _signatures(rows: list, fold_case: bool = False) -> Counter:
    out = []
    for r in rows:
        vals = [_canon(v) for v in r]
        vals = [(v.lower() if fold_case else v) for v in vals if v != ""]
        out.append(tuple(sorted(vals)))
    return Counter(out)


def _rows_are_supersets(gold: list, pred: list) -> bool:
    """Every gold row's values appear within a distinct pred row (pred carries extra
    columns but no wrong values)."""
    remaining = [Counter(v for v in (_canon(x) for x in r) if v != "") for r in pred]
    for grow in gold:
        want = Counter(v for v in (_canon(x) for x in grow) if v != "")
        for i, have in enumerate(remaining):
            if not (want - have):
                remaining.pop(i)
                break
        else:
            return False
    return True


def attribute_failure(gold_path: str, pred_path: str, pred_status: str = "ok") -> str:
    """Bucket a non-strict result.

    Format-only differences are named separately from wrong values, so score lost to
    representation stays visible and routes to adjudication instead of being silently
    counted as a wrong answer. An answer carrying all gold values plus one extra
    column scored 0.0 in an earlier run and alone accounted for ~36% of a phantom
    null result.
    """
    if pred_status != "ok":
        return "no_result (query error / empty)"
    g, _ = read_rows(gold_path)
    p, _ = read_rows(pred_path)
    if g is None or p is None:
        return "unreadable_csv"
    if len(p) == 0:
        return "empty_result"
    if len(g) == len(p):
        if _signatures(g, fold_case=True) == _signatures(p, fold_case=True):
            return "format_mismatch (text casing only)"
        if _rows_are_supersets(g, p):
            return "format_mismatch (extra columns, gold values all present)"
    if len(p) > 2 * max(len(g), 1):
        return "over_returns_rows (>2x gold)"
    if len(p) != len(g):
        if _rows_are_supersets(g, p):
            return "over_returns_rows (gold rows all present)"
        return "row_count_mismatch"
    return "value_mismatch (same row count)"


def column_agreement(gold_path: str, pred_path: str) -> dict | None:
    """Which of the target's columns the answer reproduced, and which it did not.

    The single most informative diagnostic available for free, and it was learned the
    hard way: on one answer the RANGE column matched exactly while avg and variance
    did not. Identical extremes with a different mean means the FORMULAS were right
    and the ROW SET was wrong -- a population/filter/join-scope gap, not an arithmetic
    one. Matching is by value multiset, so column order and naming don't matter.
    """
    try:
        with open(gold_path, newline="", encoding="utf-8", errors="replace") as f:
            grows = list(csv.reader(f))
        with open(pred_path, newline="", encoding="utf-8", errors="replace") as f:
            prows = list(csv.reader(f))
    except OSError:
        return None
    if len(grows) < 2 or len(prows) < 2:
        return None
    ghdr, gdata = grows[0], grows[1:]
    phdr, pdata = prows[0], prows[1:]
    matched, missing = [], []
    pred_cols = [Counter(_canon_cell(r[j]) for r in pdata if j < len(r))
                 for j in range(len(phdr))]
    for i, gname in enumerate(ghdr):
        gcol = Counter(_canon_cell(r[i]) for r in gdata if i < len(r))
        (matched if any(gcol == pc for pc in pred_cols) else missing).append(gname)
    out = {"target_columns_reproduced": matched, "target_columns_wrong": missing}
    if matched and missing:
        out["hint"] = (
            f"{len(matched)} of {len(ghdr)} target columns reproduced exactly "
            f"({', '.join(matched)}) while {', '.join(missing)} did not. If the "
            f"reproduced ones are extremes (min/max/range/count) and the wrong ones "
            f"are means or sums, the formulas were right and the ROW SET differed -- "
            f"look for a population, filter-scope or join-scope gap, not an "
            f"arithmetic one.")
    return out


def grade(gold_path: str, pred_path: str, status: str = "ok") -> dict:
    """Everything eval-answer needs about one answer, in one call."""
    res = graded_match(gold_path, pred_path)
    out = dict(res)
    out["ex_strict"] = res["strict"]
    if not res["strict"]:
        out["failure_bucket"] = attribute_failure(gold_path, pred_path, status)
        ca = column_agreement(gold_path, pred_path)
        if ca:
            out["column_agreement"] = ca
    else:
        out["failure_bucket"] = None
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--gold", required=True, help="reference answer CSV")
    ap.add_argument("--pred", required=True, help="the answer under test, as CSV")
    ap.add_argument("--status", default="ok",
                    help="'ok', or anything else if the query failed to execute")
    ap.add_argument("--json", action="store_true", help="emit the full record")
    a = ap.parse_args()
    try:
        out = grade(a.gold, a.pred, a.status)
    except Exception as exc:  # comparison itself failed -- that IS a harness error
        print(f"harness error: {type(exc).__name__}: {exc}", file=sys.stderr)
        sys.exit(1)
    if a.json:
        print(json.dumps(out, indent=2))
    else:
        print(f"f1={out['f1']} strict={out['ex_strict']} "
              f"matched={out['matched']}/{out['n_gold']} pred_rows={out['n_pred']} "
              f"bucket={out['failure_bucket']}")


if __name__ == "__main__":
    main()
