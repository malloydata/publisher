#!/usr/bin/env python3
"""Rebuild a run directory from its exported tables. Stdlib only.

  python3 eval.py import-run --set <set-dir> --from <dir>

The inverse of `eval.py export`. `--from` is a directory holding the tables of
ONE run, each as `<table>.csv` (what export writes), or as `<table>.json` (a
list of row objects) or `<table>.jsonl` (one row object per line), which is
what a query against a database the tables were loaded into returns. A file
named for the loaded table (`eval_runs__baseline_01.csv`) is found too.

Three tables are needed and three columns of them are the run itself:

  eval_runs.run_json    run.json, as written
  eval_events.event     each line of events.jsonl, as written, in `seq` order
  eval_files.text       every other record file, as written, by `path`

`eval_transcripts`, when given, puts each case's transcript, answer and judge
write-up back under artifacts/<qid>/. The other tables are views of these and
are not read.

The run is written to <workdir>/runs/<run_id>, where `accept`, `versions`,
`history` and `--baseline record` look. What comes back is the run's record:
run.json and events.jsonl are byte for byte what was exported, so a store
that already holds the run sees the same run. Not carried: a replay's rows/
(the first 500 rows of each query), and anything under artifacts/ but the
three files above. A run that is already here with the same record is left
alone; one with a different record under the same name is refused.
"""
from __future__ import annotations

import argparse
import csv
import json
import pathlib
import sys
import tempfile
from typing import Any

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent.parent / "eval-answer" / "scripts"))
sys.path.insert(0, str(HERE))

import config  # noqa: E402
import run_store  # noqa: E402

NEEDED = ("eval_runs", "eval_events")
ARTIFACTS = (("transcript", "answerer.jsonl"), ("answer", "answer.md"), ("judge", "judge.md"))


def table_file(src: pathlib.Path, table: str) -> pathlib.Path | None:
    """`<table>.<ext>`, or the one file named `<table>__<run>.<ext>`."""
    for ext in ("csv", "json", "jsonl"):
        if (src / f"{table}.{ext}").is_file():
            return src / f"{table}.{ext}"
    named = sorted(p for ext in ("csv", "json", "jsonl") for p in src.glob(f"{table}__*.{ext}"))
    if len(named) > 1:
        raise SystemExit(f"{src} holds {len(named)} files for {table} "
                         f"({', '.join(p.name for p in named)}). Fix: --from takes the tables "
                         f"of one run; put each run's files in a directory of their own")
    return named[0] if named else None


def rows(path: pathlib.Path) -> list[dict[str, Any]]:
    """The rows of one table file. A CSV field that is empty reads as None."""
    if path.suffix == ".json":
        data = json.loads(path.read_text())
        if not isinstance(data, list):
            raise SystemExit(f"Invalid {path.name}: expected a JSON list of row objects, got "
                             f"{type(data).__name__}. Fix: write the query's rows as a list")
        return data
    if path.suffix == ".jsonl":
        return [json.loads(l) for l in path.read_text().splitlines() if l.strip()]
    csv.field_size_limit(sys.maxsize)        # a transcript is one field
    with path.open(newline="") as f:
        return [{k: (v if v != "" else None) for k, v in r.items()} for r in csv.DictReader(f)]


def one_run(table: str, got: list[dict[str, Any]], run_id: str | None) -> list[dict[str, Any]]:
    ids = sorted({str(r.get("run_id")) for r in got})
    if run_id is None and len(ids) > 1:
        raise SystemExit(f"{table} holds rows of {len(ids)} runs ({', '.join(ids[:5])}). "
                         f"Fix: name the one to rebuild with --run-id")
    return [r for r in got if run_id is None or str(r.get("run_id")) == run_id]


def safe_name(name: Any, what: str) -> str:
    """A file or case name that stays inside the run directory."""
    s = str(name or "")
    if not s or s in (".", "..") or "/" in s or "\\" in s or "\0" in s:
        raise SystemExit(f"Invalid {what} {name!r}: expected a plain name with no path "
                         f"separator. The tables are not as export wrote them")
    return s


def build(src: pathlib.Path, dest: pathlib.Path, run_id: str | None) -> str:
    """Write the run under `dest` (a new, empty directory). Returns its run id."""
    found = {t: table_file(src, t) for t in (*NEEDED, "eval_files", "eval_transcripts")}
    missing = [t for t in NEEDED if found[t] is None]
    if missing:
        raise SystemExit(f"{src} has no {' or '.join(missing)} table. Fix: --from needs "
                         f"eval_runs and eval_events as .csv, .json or .jsonl; export writes "
                         f"both, and a query for them selects every column")
    runs = one_run("eval_runs", rows(found["eval_runs"]), run_id)
    if len(runs) != 1:
        raise SystemExit(f"eval_runs holds {len(runs)} rows for "
                         f"{run_id or 'this run'}, expected 1")
    run_id = safe_name(runs[0].get("run_id"), "run_id")
    text = runs[0].get("run_json")
    try:
        json.loads(text or "")
    except ValueError:
        raise SystemExit("Invalid eval_runs.run_json: expected the text of run.json. Fix: "
                         "select run_json as text, not cast or re-formatted") from None
    (dest / "run.json").write_text(text)
    events = one_run("eval_events", rows(found["eval_events"]), run_id)
    events.sort(key=lambda r: int(float(r["seq"])))
    (dest / "events.jsonl").write_text("".join(r["event"] + "\n" for r in events))
    if found["eval_files"]:
        for r in one_run("eval_files", rows(found["eval_files"]), run_id):
            name = safe_name(r.get("path"), "eval_files.path")
            if name not in ("run.json", "events.jsonl"):
                (dest / name).write_text(r.get("text") or "")
    if found["eval_transcripts"]:
        for r in one_run("eval_transcripts", rows(found["eval_transcripts"]), run_id):
            d = dest / "artifacts" / safe_name(r.get("qid"), "eval_transcripts.qid")
            for column, name in ARTIFACTS:
                if r.get(column):
                    d.mkdir(parents=True, exist_ok=True)
                    (d / name).write_text(r[column])
    return run_id


def import_run(cfg: config.Config, src: pathlib.Path, run_id: str | None = None) -> str:
    """Rebuild the run into the set's workdir. Returns what happened, in words."""
    if not src.is_dir():
        raise SystemExit(f"{src} is not a directory. Fix: --from <the directory holding the "
                         f"run's table files>")
    runs = cfg.workdir() / "runs"
    runs.mkdir(parents=True, exist_ok=True)
    tmp = pathlib.Path(tempfile.mkdtemp(prefix=".import-", dir=runs))
    try:
        run_id = build(src, tmp, run_id)
        dest = runs / run_id
        if dest.exists():
            if run_store.content_sha(dest) == run_store.content_sha(tmp):
                return f"{run_id} is already here"
            raise SystemExit(f"{dest} already holds a different run named {run_id}. A run "
                             f"never changes once shared, so one of the two is not the run "
                             f"that was exported. Fix: move the local one aside, then import")
        tmp.rename(dest)             # whole or not at all
        return f"imported {run_id} to {dest}"
    finally:
        if tmp.exists():
            import shutil
            shutil.rmtree(tmp, ignore_errors=True)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--set", dest="set_dir", required=True, type=pathlib.Path)
    ap.add_argument("--from", dest="src", required=True, type=pathlib.Path,
                    help="a directory holding one run's tables as .csv, .json or .jsonl")
    ap.add_argument("--run-id", default=None,
                    help="the run to rebuild, when the files hold rows of several")
    a = ap.parse_args(argv)
    print(import_run(config.load(a.set_dir), a.src, a.run_id))
    return 0


if __name__ == "__main__":
    sys.exit(main())
