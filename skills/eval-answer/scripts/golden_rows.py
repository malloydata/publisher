#!/usr/bin/env python3
"""Read a `rows` golden, wherever it keeps its rows.

A rows golden holds its key in `golden.value` (a list of row objects) or, for a
long one, in a CSV named by `golden.path`, relative to the set folder. Code
that read `value` alone treated a path-held golden as having no key: the judge
was told the question was unanswerable, and the golden audit reported an error.

`value` wins when both exist. A `path` that leaves the set folder, or names a
file that is not there, raises `GoldenRowsError` with the case and the path.
"""
from __future__ import annotations

import csv
import json
import pathlib
import re
from typing import Any

# The most rows a judge prompt carries. A golden past this is cut, and the
# prompt says so, rather than letting one case fill the context.
ROWS_CAP = 500

_INT = re.compile(r"-?(0|[1-9]\d*)")
_FLOAT = re.compile(r"-?(0|[1-9]\d*)?\.\d+([eE][-+]?\d+)?")


class GoldenRowsError(ValueError):
    """A rows golden's `path` cannot be read; the message names case and path."""


def _cell(text: str | None) -> Any:
    """A CSV cell as a value: numbers as numbers, empty as null.

    Only canonical numerals convert, so an identifier such as `007` stays text.
    """
    if text is None or text == "":
        return None
    if _INT.fullmatch(text):
        return int(text)
    if _FLOAT.fullmatch(text):
        return float(text)
    return text


def load_rows(golden: dict[str, Any] | None, set_dir: pathlib.Path | None,
              qid: str) -> list[dict[str, Any]] | None:
    """The golden's rows, all of them, or None when it holds none.

    `value` is used as it stands when it is a list. Otherwise `path` is read
    (CSV, or a JSON list of row objects).
    """
    g = golden or {}
    if isinstance(g.get("value"), list):
        return g["value"]
    rel = g.get("path")
    if g.get("value") is not None or not rel:
        return None
    if set_dir is None:
        raise GoldenRowsError(
            f"{qid}: golden.path {rel!r} cannot be read without the set folder")
    root = pathlib.Path(set_dir).resolve()
    target = (root / rel).resolve()
    if not target.is_relative_to(root):
        raise GoldenRowsError(
            f"{qid}: golden.path {rel!r} is outside the set folder {root}; "
            f"keep the file under the set and give its path relative to it")
    if not target.is_file():
        raise GoldenRowsError(
            f"{qid}: golden.path {rel!r} does not exist under {root}")
    try:
        if target.suffix.lower() == ".json":
            rows = json.loads(target.read_text())
            if not (isinstance(rows, list)
                    and all(isinstance(r, dict) for r in rows)):
                raise GoldenRowsError(
                    f"{qid}: golden.path {rel!r} is JSON but not a list of "
                    f"row objects")
            return rows
        with target.open(newline="", encoding="utf-8-sig") as fh:
            return [{k: _cell(v) for k, v in row.items()}
                    for row in csv.DictReader(fh)]
    except (OSError, ValueError, csv.Error) as exc:
        if isinstance(exc, GoldenRowsError):
            raise
        raise GoldenRowsError(
            f"{qid}: golden.path {rel!r} could not be read: {exc}") from exc


def render(rows: list[dict[str, Any]], total: int | None = None) -> str:
    """The rows as the judge sees them, cut at ROWS_CAP with the cut stated."""
    total = len(rows) if total is None else total
    shown = rows[:ROWS_CAP]
    text = json.dumps(shown)
    if total > len(shown):
        text += (f" (first {len(shown)} of {total} rows; the rest are not "
                 f"shown)")
    return text
