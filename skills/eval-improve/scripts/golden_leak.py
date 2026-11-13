#!/usr/bin/env python3
"""Does an edit to the model carry a case's answer? Stdlib only.

An improve edit is checked by re-asking the cases it aimed at. If the edit
wrote a case's expected value, or its golden query, into a doc or a package
skill, the answerer reads the answer off the model and the case passes without
the model having got any better. The score goes up and means nothing, and the
next reader of that doc is told a number that will be stale by next month.

This looks at the lines an edit ADDS, and reports:

  value   a number in an added line that equals a number in some case's key,
          to five significant digits. Thousands separators are ignored, so a
          key of 464861.37 is found as 464,861. Shorter numbers are not
          checked: a year, a count of 12 or a 50% rate appear in honest docs.
  query   a case's golden query, or its verifying query, pasted whole.

Every case of the set is checked, not only the cluster's: an edit for one
cluster that states another case's answer leaks just the same. A text value
is not checked. "Outerwear" in a doc is a fact about the data, and telling a
category name from an answer needs a reader.

A report is a reason for a person to look, not proof. The number may be a
real constant of the business. That is the person's call: `improve.py` stops
the acceptance check until it is made.
"""
from __future__ import annotations

import math
import re
from typing import Any

NUMBER = re.compile(r"(?<![\w.])-?\d[\d,]*(?:\.\d+)?(?![\w])")
MIN_DIGITS = 5
MIN_QUERY = 60


def significant_digits(text: str) -> int:
    digits = re.sub(r"[^0-9]", "", text).lstrip("0")
    return len(digits)


def numbers_in(value: Any) -> list[float]:
    """Every number in a key worth looking for: five significant digits or more."""
    out: list[float] = []
    if isinstance(value, bool) or value is None:
        return out
    if isinstance(value, (int, float)):
        if math.isfinite(value) and significant_digits(repr(value)) >= MIN_DIGITS:
            out.append(float(value))
    elif isinstance(value, str):
        for m in NUMBER.finditer(value):
            if significant_digits(m.group()) >= MIN_DIGITS:
                out.append(float(m.group().replace(",", "")))
    elif isinstance(value, dict):
        for v in value.values():
            out += numbers_in(v)
    elif isinstance(value, (list, tuple)):
        for v in value:
            out += numbers_in(v)
    return out


def added_lines(diff: str) -> list[str]:
    return [l[1:] for l in diff.splitlines() if l.startswith("+") and not l.startswith("+++")]


def squash(text: str) -> str:
    """Text as one line of words. A semicolon and a line break both separate
    Malloy statements, so a query pasted one statement per line is the same
    query."""
    return " ".join(text.replace(";", " ").split())


def same(a: float, b: float) -> bool:
    """Equal to five significant digits: 464861.37 and 464861, not 464900."""
    if a == b:
        return True
    scale = max(abs(a), abs(b))
    return scale > 0 and abs(a - b) / scale < 0.5 * 10 ** -(MIN_DIGITS - 1)


def check(diff: str, keys: dict[str, Any], queries: dict[str, list[str]] | None = None
          ) -> list[dict[str, Any]]:
    """The leaks in `diff`. `keys` is qid -> the case's key (a value or rows);
    `queries` is qid -> its golden and verifying queries."""
    added = added_lines(diff or "")
    if not added:
        return []
    found: list[dict[str, Any]] = []
    wanted = {q: numbers_in(k) for q, k in keys.items()}
    for line in added:
        for m in NUMBER.finditer(line):
            if significant_digits(m.group()) < MIN_DIGITS:
                continue
            n = float(m.group().replace(",", ""))
            for qid, nums in wanted.items():
                if any(same(n, k) for k in nums):
                    hit = {"qid": qid, "kind": "value", "found": m.group(),
                           "line": line.strip()[:200]}
                    if hit not in found:
                        found.append(hit)
    text = squash("\n".join(added))
    for qid, qs in (queries or {}).items():
        for q in qs:
            q = squash(q or "")
            if len(q) >= MIN_QUERY and q in text:
                found.append({"qid": qid, "kind": "query", "found": q[:120], "line": None})
    return found
