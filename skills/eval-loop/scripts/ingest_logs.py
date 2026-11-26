#!/usr/bin/env python3
"""Add new production log rows to the history model's tables. Stdlib only.

  python3 eval.py ingest --set <set-dir> --turns turns.jsonl --queries queries.jsonl
  python3 eval.py ingest --set <set-dir> --builds builds.jsonl --versions versions.jsonl

Step H1 of the optimization loop (reference/optimization-loop.md): production
data in, nothing judged or derived. Each input is a JSONL file of rows pulled
from wherever the host keeps its logs; a host-specific skill does the pulling.
Each row is appended to one CSV under the optimization package's `data/`
(optimization_package.py says where it is and owns every table's columns):

  prod_turns.csv         one question and its answer (a turn, not a session)
  prod_transcripts.csv   the transcript of a turn, when the row carries one
  prod_queries.csv       one query run in production
  prod_table_builds.csv  one build of one materialized table
  model_versions.csv     one published version of the served package

The same CSVs are what a hosted run loads into the managed database, so a
local run and a nightly one write the same columns.

THREE RULES

- **A row is added once.** Each table has a key (`turn_id`, `execution_id`,
  `build_id`, package + version). A key already stored is skipped, so a rerun,
  or a pull that overlaps the last one, adds nothing twice.
- **Only the served package's traffic.** Rows for another package are left
  out.
- **Never the loop's own traffic.** Eval runs and replays query the served
  package too. A row from an environment in `[optimization] exclude_environments`,
  or whose `origin` is `eval` or `replay`, is left out and counted. Without
  this the loop would judge its own questions as production.

`data/watermarks.json` records the newest timestamp stored per table, which
is where the next pull starts. A summary of what was added and left out goes
to stdout.
"""
from __future__ import annotations

import argparse
import csv
import json
import pathlib
import sys
from typing import Any, Iterable

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent.parent / "eval-answer" / "scripts"))
sys.path.insert(0, str(HERE))

import config  # noqa: E402
from optimization_package import APPENDED, append, install_for  # noqa: E402

TABLES = APPENDED
INPUT_TABLE = {"turns": "prod_turns", "queries": "prod_queries", "builds": "prod_table_builds",
               "versions": "model_versions"}
# Other names hosts use for a column, mapped to the history model's.
ALIASES = {"query": "query_text", "timestamp": "at", "request_id": "turn_id",
           "trace_id": "execution_id", "durationMs": "duration_ms", "packageVersion": "model_version",
           "version_id": "model_version"}
LOOP_ORIGINS = ("eval", "replay")
WATERMARKS = "watermarks.json"


def text(v: Any) -> Any:
    """Nested values as JSON text, so a column holds one type."""
    return json.dumps(v, ensure_ascii=False, sort_keys=True) if isinstance(v, (list, dict)) else v


def normalise(raw: dict[str, Any]) -> dict[str, Any]:
    """The row under the history model's names. A host name fills a column only
    when the row does not already carry it under ours."""
    out = {k: v for k, v in raw.items() if k not in ALIASES}
    for k, v in raw.items():
        if k in ALIASES and out.get(ALIASES[k]) in (None, ""):
            out[ALIASES[k]] = v
    return out


def stored_keys(path: pathlib.Path, key: tuple[str, ...]) -> set[tuple[str, ...]]:
    if not path.is_file():
        return set()
    with path.open(newline="", encoding="utf-8") as f:
        return {tuple(r.get(k) or "" for k in key) for r in csv.DictReader(f)}


def why_left_out(row: dict[str, Any], table: str, package: str,
                 excluded: set[str]) -> str | None:
    key = TABLES[table][0]
    if any(row.get(k) in (None, "") for k in key):
        return f"no {' + '.join(key)}"
    if row.get("package") != package:
        return "another package"
    if row.get("environment") in excluded:
        return "the loop's own environment"
    if row.get("origin") in LOOP_ORIGINS:
        return f"origin {row['origin']}"
    return None


def ingest(data: pathlib.Path, table: str, rows: Iterable[dict[str, Any]], package: str,
           excluded: set[str]) -> dict[str, Any]:
    """Append the new rows of one table. Returns what was added and left out."""
    key, ts, cols = TABLES[table]
    data.mkdir(parents=True, exist_ok=True)
    path = data / f"{table}.csv"
    seen = stored_keys(path, key)
    new, left_out, transcripts = [], {}, []
    for raw in rows:
        row = normalise(raw)
        why = why_left_out(row, table, package, excluded)
        if why is None and tuple(str(row.get(k)) for k in key) in seen:
            why = "already stored"
        if why:
            left_out[why] = left_out.get(why, 0) + 1
            continue
        seen.add(tuple(str(row.get(k)) for k in key))
        new.append({c: text(row.get(c)) for c in cols})
        if table == "prod_turns" and row.get("transcript"):
            transcripts.append({"turn_id": row["turn_id"], "transcript": text(row["transcript"])})
    append(path, cols, new)
    if transcripts:
        append(data / "prod_transcripts.csv", TABLES["prod_transcripts"][2], transcripts)
    newest = max((r[ts] for r in new if ts and r.get(ts)), default=None)
    return {"table": table, "added": len(new), "transcripts": len(transcripts),
            "left_out": left_out, "newest": newest}


def update_watermarks(data: pathlib.Path, results: list[dict[str, Any]]) -> dict[str, str]:
    path = data / WATERMARKS
    marks = json.loads(path.read_text()) if path.is_file() else {}
    for r in results:
        if r["newest"] and r["newest"] > marks.get(r["table"], ""):
            marks[r["table"]] = r["newest"]
    path.write_text(json.dumps(marks, indent=1, sort_keys=True) + "\n")
    return marks


def read_jsonl(path: pathlib.Path) -> Iterable[dict[str, Any]]:
    with path.open(encoding="utf-8") as f:
        for n, line in enumerate(f, 1):
            if line.strip():
                try:
                    yield json.loads(line)
                except ValueError as e:
                    raise SystemExit(f"{path}:{n}: not a JSON object ({e}). Fix: one row per line.")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--set", dest="set_dir", required=True)
    for name in INPUT_TABLE:
        ap.add_argument(f"--{name}", type=pathlib.Path, help=f"a JSONL file of {name} rows")
    a = ap.parse_args(argv)
    inputs = {n: getattr(a, n) for n in INPUT_TABLE if getattr(a, n)}
    if not inputs:
        raise SystemExit("Nothing to ingest. Fix: pass --turns, --queries, --builds or --versions.")
    cfg = config.load(pathlib.Path(a.set_dir))
    package = cfg.get("model", "package")
    if not package:
        raise SystemExit("Missing eval.toml key 'model.package': the served package whose "
                         "traffic to keep. Fix: [model] package = \"storefront\"")
    excluded = set(cfg.get("optimization", "exclude_environments") or [])
    data = install_for(cfg)
    results = [ingest(data, INPUT_TABLE[n], read_jsonl(p), package, excluded)
               for n, p in inputs.items()]
    marks = update_watermarks(data, results)
    for r in results:
        out = "; ".join(f"{n} {why}" for why, n in sorted(r["left_out"].items()))
        extra = f" (+{r['transcripts']} transcripts)" if r["transcripts"] else ""
        print(f"{r['table']}: added {r['added']}{extra}" + (f"; left out {out}" if out else ""))
    if not excluded:
        print("note: [optimization] exclude_environments is unset, so only rows tagged origin "
              "eval/replay are treated as the loop's own traffic")
    print(f"next pull starts after: {json.dumps(marks)}")
    print(data)
    return 0


if __name__ == "__main__":
    sys.exit(main())
