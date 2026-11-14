#!/usr/bin/env python3
"""Read a `rows` golden, wherever it keeps its rows.

A rows golden holds its key in `golden.value` (a list of row objects) or, for a
long one, in a CSV named by `golden.path`, relative to the set folder. Code
that read `value` alone treated a path-held golden as having no key: the judge
was told the question was unanswerable, and the golden audit reported an error.

`path` names a CSV with a header row, or a JSON array of row objects. `value`
wins when both exist. A `path` that leaves the set folder, names a file that is
not there, or holds no header or no data rows raises `GoldenRowsError` with the
case and the path.
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
    `true` and `false` (lower case, as JSON and Publisher write them) become
    booleans, so they compare equal to what a query returns.
    """
    if text is None or text == "":
        return None
    if text == "true":
        return True
    if text == "false":
        return False
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
        else:
            rows = _read_csv(target, rel, qid)
    except (OSError, ValueError, csv.Error) as exc:
        if isinstance(exc, GoldenRowsError):
            raise
        raise GoldenRowsError(
            f"{qid}: golden.path {rel!r} could not be read: {exc}") from exc
    # A truncated export reads as zero rows, and a judge shown an empty key
    # fails every correct answer. A key that really is "no rows" belongs in
    # `value: []`, where nobody can mistake it for a lost file.
    if not rows:
        raise GoldenRowsError(
            f"{qid}: golden.path {rel!r} has no data rows; re-export it, or "
            f"write `value: []` on the golden if the answer is no rows")
    return rows


def _read_csv(target: pathlib.Path, rel: str, qid: str
              ) -> list[dict[str, Any]]:
    """The CSV's rows, keyed by its header.

    `csv.reader`, not `DictReader`: DictReader skips a blank line, and DuckDB
    writes a NULL in a one-column file as exactly that, so the row dropped. In
    a one-column file a blank line is one empty value. In a wider file it
    carries no value at all and is skipped, as before.
    """
    with target.open(newline="", encoding="utf-8-sig") as fh:
        reader = csv.reader(fh)
        header = next(reader, None)
        if not header:
            raise GoldenRowsError(
                f"{qid}: golden.path {rel!r} has no header row; the first "
                f"line must name the columns")
        out = []
        for row in reader:
            if not row:
                if len(header) > 1:
                    continue
                row = [""]
            if len(row) != len(header):
                raise GoldenRowsError(
                    f"{qid}: golden.path {rel!r} line {reader.line_num} "
                    f"has a different number of fields than the header "
                    f"({len(header)}); fix the row")
            out.append({k: _cell(v) for k, v in zip(header, row)})
        return out


def key_value(golden: dict[str, Any] | None, set_dir: pathlib.Path | None,
              qid: str) -> Any:
    """The golden's key as a reader should see it: its rows when they live in
    `path`, otherwise `value` as it stands. Raises `GoldenRowsError`."""
    rows = load_rows(golden, set_dir, qid)
    return rows if rows is not None else (golden or {}).get("value")


def key_value_or_note(golden: dict[str, Any] | None,
                      set_dir: pathlib.Path | None, qid: str) -> Any:
    """`key_value`, with an unreadable `path` given as a note in its place.

    For a prompt that shows the key to an agent: a note that says the file is
    broken is evidence, while a missing key reads as a keyless case.
    """
    try:
        return key_value(golden, set_dir, qid)
    except GoldenRowsError as exc:
        return f"(the golden's rows could not be read: {exc})"


def render(rows: list[dict[str, Any]], total: int | None = None) -> str:
    """The rows as the judge sees them, cut at ROWS_CAP with the cut stated."""
    total = len(rows) if total is None else total
    shown = rows[:ROWS_CAP]
    text = json.dumps(shown)
    if total > len(shown):
        text += (f" (first {len(shown)} of {total} rows; the rest are not "
                 f"shown)")
    return text
