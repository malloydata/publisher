#!/usr/bin/env python3
"""Named groups of a set's cases, and the one place `--slice` and `--tag`
turn into qids. Stdlib only.

  python3 slices.py --set <set-dir>                     # list slices and their size
  python3 slices.py --set <set-dir> --slice near-twins  # print the qids
  python3 slices.py --set <set-dir> --tag join --tag rank

A slice lives in the set's `slices.json`, beside `cases.jsonl`:

  {"near-twins": {"tags": ["near-twin-sources"]},
   "smoke":      {"qids": ["q1", "q7", "q12"]}}

A slice holds every case carrying ANY of its `tags`, plus every qid in its
`qids`. `--tag` alone does the same for tags named on the command line.

`slices.json` is NOT part of `datasetSha` (`ledger.dataset_sha` hashes
`set.json` and `cases.jsonl`). Adding or editing a slice therefore never voids
a stored run or a calibrated noise band. Every run that used a slice records it
in run.json as `slice`: its name, its definition, and the qids it resolved to
on the day, so a later edit to the file cannot change what an old run meant.

Every script that runs a subset of a set (`run_baseline.py`,
`retrieval_replay.py`, `diagnose.py`, `flip_table.py`) calls `resolve` and
feeds the qids into the filter it already had (`--only` or `--targets`).
"""
from __future__ import annotations

import argparse
import json
import pathlib
import sys
from typing import Any

FILE = "slices.json"


def load(set_dir: pathlib.Path) -> dict[str, dict[str, Any]]:
    """The set's slices, or {} when it has no slices.json."""
    path = set_dir / FILE
    if not path.is_file():
        return {}
    try:
        data = json.loads(path.read_text())
    except json.JSONDecodeError as e:
        raise SystemExit(f"{path} is not JSON: {e}") from None
    problems = []
    if not isinstance(data, dict):
        raise SystemExit(f"{path}: expected an object of named slices, got "
                         f"{type(data).__name__}. Fix: {{\"smoke\": {{\"qids\": [\"q1\"]}}}}")
    for name, spec in data.items():
        if not isinstance(spec, dict) or not (spec.get("tags") or spec.get("qids")):
            problems.append(f"{name}: expected {{\"tags\": [...]}} and/or {{\"qids\": [...]}}, "
                            f"got {json.dumps(spec)}")
            continue
        for key in ("tags", "qids"):
            v = spec.get(key)
            if v is not None and not (isinstance(v, list) and all(isinstance(x, str) for x in v)):
                problems.append(f"{name}.{key}: expected a list of strings, got {json.dumps(v)}")
        extra = sorted(set(spec) - {"tags", "qids", "description"})
        if extra:
            problems.append(f"{name}: unknown key(s) {', '.join(extra)}; "
                            "a slice takes tags, qids and description")
    if problems:
        raise SystemExit(f"{path}: " + "; ".join(problems))
    return data


def select(cases: list[dict[str, Any]], tags: list[str] | None = None,
           qids: list[str] | None = None) -> list[str]:
    """The qids of `cases` carrying any of `tags`, plus `qids`, in file order."""
    want_tags, want_qids = set(tags or []), set(qids or [])
    return [c["qid"] for c in cases
            if c.get("qid") in want_qids or want_tags & set(c.get("tags") or [])]


def resolve(set_dir: pathlib.Path, cases: list[dict[str, Any]],
            slice_name: str | None = None,
            tags: list[str] | None = None) -> dict[str, Any] | None:
    """`{"name", "tags", "qids"}` for a `--slice` and/or `--tag`, or None for neither.

    Refuses, naming the fix: an unknown slice name, a slice naming a qid the
    set does not hold, and a selection that holds no case.
    """
    if not slice_name and not tags:
        return None
    want_tags, want_qids = list(tags or []), []
    if slice_name:
        defined = load(set_dir)
        if slice_name not in defined:
            names = ", ".join(sorted(defined)) or "none: the set has no slices.json"
            raise SystemExit(f"--slice {slice_name!r} is not in {set_dir / FILE}. "
                             f"Slices: {names}.")
        spec = defined[slice_name]
        want_tags += [t for t in spec.get("tags") or [] if t not in want_tags]
        want_qids = list(spec.get("qids") or [])
        unknown = sorted(set(want_qids) - {c.get("qid") for c in cases})
        if unknown:
            raise SystemExit(f"slice {slice_name!r} names qid(s) not in cases.jsonl: "
                             f"{', '.join(unknown)}. Fix: remove them from {FILE}.")
    qids = select(cases, want_tags, want_qids)
    if not qids:
        what = f"slice {slice_name!r}" if slice_name else f"--tag {', '.join(want_tags)}"
        raise SystemExit(f"{what} selects no case in {set_dir / 'cases.jsonl'}.")
    return {"name": slice_name, "tags": want_tags, "qids": qids}


def add_arguments(ap: argparse.ArgumentParser) -> None:
    ap.add_argument("--slice", default=None, metavar="NAME",
                    help=f"run only the cases of this slice from the set's {FILE}")
    ap.add_argument("--tag", action="append", default=None, metavar="TAG",
                    help="run only cases carrying this tag (repeatable; any of them)")


def narrow(a: argparse.Namespace, set_dir: pathlib.Path,
           cases: list[dict[str, Any]], only_attr: str = "only",
           joined: bool = True) -> dict[str, Any] | None:
    """Resolve `a.slice`/`a.tag` and write the qids into the script's own filter.

    `joined` is True for a comma-separated `--only` string, False for a list.
    Refuses `--slice`/`--tag` together with `--only`: two filters on one run
    leave nobody sure which cases ran.
    """
    picked = resolve(set_dir, cases, getattr(a, "slice", None), getattr(a, "tag", None))
    if picked is None:
        return None
    if getattr(a, only_attr, None):
        raise SystemExit(f"--{only_attr.replace('_', '-')} and --slice/--tag both choose cases. "
                         "Fix: pass one of them.")
    setattr(a, only_attr, ",".join(picked["qids"]) if joined else list(picked["qids"]))
    return picked


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--set", dest="set_dir", type=pathlib.Path, required=True)
    add_arguments(ap)
    a = ap.parse_args(argv)
    cases_path = a.set_dir / "cases.jsonl"
    if not cases_path.is_file():
        raise SystemExit(f"{cases_path} does not exist.")
    cases = [json.loads(x) for x in cases_path.read_text().splitlines() if x.strip()]
    picked = resolve(a.set_dir, cases, a.slice, a.tag)
    if picked:
        print("\n".join(picked["qids"]))
        return 0
    defined = load(a.set_dir)
    if not defined:
        print(f"{a.set_dir / FILE}: no slices defined")
    for name, spec in defined.items():
        n = len(select(cases, spec.get("tags"), spec.get("qids")))
        print(f"{name}: {n} case(s)  {spec.get('description') or ''}".rstrip())
    tags: dict[str, int] = {}
    for c in cases:
        for t in c.get("tags") or []:
            tags[t] = tags.get(t, 0) + 1
    if tags:
        print("tags: " + ", ".join(f"{t} ({n})" for t, n in sorted(tags.items())))
    return 0


if __name__ == "__main__":
    sys.exit(main())
