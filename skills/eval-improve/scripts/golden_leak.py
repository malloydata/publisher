#!/usr/bin/env python3
"""Does an edit to the model carry a case's answer? Stdlib only.

An improve edit is checked by re-asking the cases it aimed at. If the edit
wrote a case's expected value, or its golden query, into a doc or a package
skill, the answerer reads the answer off the model and the case passes without
the model having got any better. The score goes up and means nothing, and the
next reader of that doc is told a number that will be stale by next month.

This looks at the lines an edit ADDS, and reports:

  value   a figure in an added line that states a number some case accepts:
          a number in its key (or a column sum of a rows key), a figure in
          its rubric's accepting clause or its mustState, or a value inside a
          range the rubric accepts ("accept $487,500-$490,500"). A figure
          matches at the precision it is written, so for a key of 488970.54
          "$489,000", "489K" and "$0.49M" all match. What counts as a figure,
          and what matches, is `verify_goldens.quoted_figures` and
          `figure_matches`, the rubric-figure check's own definition: years,
          small counts and short rates are not figures, because they appear
          in honest docs.
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

import pathlib
import re
import sys
from typing import Any

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent.parent
                       / "eval-answer" / "scripts"))
from verify_goldens import (  # noqa: E402
    Figure, _walk_strings, accepting_clause, figure_matches, key_figures,
    quoted_figures)

MIN_QUERY = 60
# Two figures joined like this in an accepting clause are a range it accepts.
_RANGE_JOIN = re.compile(r"\s*(?:-|\u2013|\u2014|to)\s*\$?\s*")


def accepted_text(golden: dict[str, Any] | None) -> str:
    """The prose of a golden that states accepted answers: the rubric's
    accepting clause and the mustState, each cut at its first rejecting
    marker so a trap figure is not read as an answer."""
    g = golden or {}
    return "\n".join(accepting_clause(t) for t in (g.get("rubric"), g.get("mustState"))
                     if isinstance(t, str) and t)


def targets(key: Any, text: str = "") -> tuple[list[float], list[tuple[float, float]]]:
    """(values, ranges) one case accepts as its answer."""
    values = key_figures(key)
    values += [f.value for s in _walk_strings(key) if isinstance(s, str)
               for f in quoted_figures(s)]
    figs = quoted_figures(text)
    values += [f.value for f in figs]
    ranges = [(min(a.value, b.value), max(a.value, b.value))
              for a, b in zip(figs, figs[1:])
              if _RANGE_JOIN.fullmatch(text[a.end:b.start])]
    return values, ranges


def leaks(f: Figure, values: list[float], ranges: list[tuple[float, float]]) -> bool:
    return (figure_matches(f, values)
            or any(lo - f.tol <= f.value <= hi + f.tol for lo, hi in ranges))


def added_lines(diff: str) -> list[str]:
    return [l[1:] for l in diff.splitlines() if l.startswith("+") and not l.startswith("+++")]


def squash(text: str) -> str:
    """Text as one line of words. A semicolon and a line break both separate
    Malloy statements, so a query pasted one statement per line is the same
    query."""
    return " ".join(text.replace(";", " ").split())


def check(diff: str, keys: dict[str, Any], queries: dict[str, list[str]] | None = None,
          accepted: dict[str, str] | None = None) -> list[dict[str, Any]]:
    """The leaks in `diff`. `keys` is qid -> the case's key (a value or rows);
    `queries` is qid -> its golden and verifying queries; `accepted` is qid ->
    `accepted_text(golden)`, the answers its rubric and mustState accept."""
    added = added_lines(diff or "")
    if not added:
        return []
    found: list[dict[str, Any]] = []
    accepted = accepted or {}
    wanted = {q: targets(keys.get(q), accepted.get(q, ""))
              for q in {*keys, *accepted}}
    for line in added:
        for f in quoted_figures(line):
            for qid, (values, ranges) in wanted.items():
                if leaks(f, values, ranges):
                    hit = {"qid": qid, "kind": "value", "found": f.text,
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
