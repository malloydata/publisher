#!/usr/bin/env python3
"""What was tried on a set, and how it went. Stdlib only.

  <set-dir>/attempts.jsonl      one line per edit put to the acceptance check

A run records what one night measured. This file records what was TRIED across
all of them: the edit, the cases it aimed at, whether it was accepted, and why
not when it was not. `eval.py accept` appends a line every time it checks an
edit, accepted or rejected. `improve.py` shows the lines that bear on a cluster
to the agent editing for it, so an edit that was already tried and rejected is
not proposed again, and one that worked on a neighbouring cluster is seen.

It lives with the set, in the model's repository, for three reasons. It is
small and curated, unlike a run. It has to reach whoever improves the model
next, on any machine, and the repository already does that. And the answerer
cannot read it: the set directory is off limits to it, and a read is recorded
as a breach of isolation. It is not part of `datasetSha`, so a new line voids
no stored run.

A line holds no golden and no answer. `lesson` is one sentence about the MODEL
("returns are excluded from net sales by a filter, not a measure"), never the
value a case expects.
"""
from __future__ import annotations

import datetime
import hashlib
import json
import pathlib
from typing import Any

FILE = "attempts.jsonl"
PATCH_KEPT = 20_000       # characters of the diff kept on the line


def path(set_dir: pathlib.Path) -> pathlib.Path:
    return set_dir / FILE


def read(set_dir: pathlib.Path) -> list[dict[str, Any]]:
    """Every attempt, oldest first. A line that does not parse is skipped: the
    file is edited by merges, and one bad line must not hide the rest."""
    out = []
    try:
        lines = path(set_dir).read_text().splitlines()
    except OSError:
        return out
    for line in lines:
        try:
            row = json.loads(line)
        except ValueError:
            continue
        if isinstance(row, dict):
            out.append(row)
    return out


def append(set_dir: pathlib.Path, row: dict[str, Any]) -> pathlib.Path:
    p = path(set_dir)
    with p.open("a") as f:
        f.write(json.dumps(row) + "\n")
    return p


def patch_sha(diff: str | None) -> str | None:
    """A hash of what a diff CHANGES: its added and removed lines, trimmed.

    Not of the diff text, which carries line numbers and context that move
    when anything above the edit moves. Two diffs that add and remove the same
    lines are the same edit for the purpose of "was this tried before".
    """
    if not diff or not diff.strip():
        return None
    changed = [l[0] + l[1:].strip() for l in diff.splitlines()
               if l[:1] in "+-" and not l.startswith(("+++", "---")) and l[1:].strip()]
    if not changed:
        return None
    return hashlib.sha256("\n".join(changed).encode()).hexdigest()[:16]


def row(*, what: str, outcome: str, baseline: str, candidates: list[str],
        targets: list[str], failed: dict[str, str], effect: dict[str, Any],
        aa: str | None = None, issue_ids: list[str] | None = None,
        diff: str | None = None, lesson: str | None = None) -> dict[str, Any]:
    """One attempt, in the shape every reader of the file expects."""
    return {
        "at": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
        "what": what, "outcome": outcome, "issueIds": issue_ids or [],
        "targets": targets, "baseline": baseline, "aa": aa, "candidates": candidates,
        "failedRules": failed, "effect": effect,
        "patchSha": patch_sha(diff),
        "patch": (diff or "")[:PATCH_KEPT] or None,
        "lesson": lesson,
    }


def same_edit(rows: list[dict[str, Any]], diff: str | None) -> dict[str, Any] | None:
    """The newest REJECTED attempt that made this same edit, or None."""
    sha = patch_sha(diff)
    if sha is None:
        return None
    hits = [r for r in rows if r.get("patchSha") == sha and r.get("outcome") == "rejected"]
    return hits[-1] if hits else None


def relevant(rows: list[dict[str, Any]], qids: list[str], limit: int = 6) -> list[dict[str, Any]]:
    """The attempts that bear on these cases: the ones that aimed at any of
    them, newest first, then the newest others, up to `limit`.

    A short list on purpose. An agent shown every past attempt reads none of
    them; shown the few about its own cases, it reads those.
    """
    want = set(qids)
    newest = list(reversed(rows))
    hits = [r for r in newest if want & set(r.get("targets") or [])]
    rest = [r for r in newest if r not in hits]
    return (hits + rest)[:limit]


def brief(rows: list[dict[str, Any]]) -> str:
    """The attempts as an agent reads them: outcome first, the diff last."""
    if not rows:
        return "(none recorded for this set)"
    out = []
    for r in rows:
        why = "; ".join(f"{k}: {v}" for k, v in (r.get("failedRules") or {}).items())
        eff = r.get("effect") or {}
        out.append("\n".join(filter(None, [
            f"- {str(r.get('outcome')).upper()} {r.get('at')}: {r.get('what')}",
            f"  aimed at: {', '.join(r.get('targets') or []) or '(none named)'}",
            f"  fixed {eff.get('targetsFixed')} of {eff.get('targets')} target(s)"
            + (f", regressed {', '.join(eff['regressions'])}" if eff.get("regressions") else ""),
            f"  rejected because: {why}" if why else None,
            f"  lesson: {r['lesson']}" if r.get("lesson") else None,
            ("  the edit:\n" + "\n".join("    " + l for l in r["patch"].splitlines()[:40]))
            if r.get("patch") else None])))
    return "\n".join(out)
