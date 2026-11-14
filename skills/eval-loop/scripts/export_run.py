#!/usr/bin/env python3
"""Turn one run directory into flat files, one per table, ready to load into a
database a team can query. Stdlib only.

  python3 export_run.py --run <run-dir> --set <set-dir> [--out <dir>]
  # or: python3 eval.py export --set <set-dir> --label baseline-01

Runs do not go in git. This is how a run leaves the laptop instead: seven CSV
files in `<run>/export/` (or `--out`), each row carrying `run_id`, plus a
`manifest.json` naming each file and the table name to load it under. It calls nothing remote. Where the files go is the caller's choice:
`reference/sharing-runs.md` lists the options, and a host can ship its own
upload step that reads `manifest.json`.

  eval_runs         one row: the run's pins and its headline score
  eval_attempts     one row per attempt: verdict, golden revision, cost, tags
  eval_events       one row per ledger event, the event itself as JSON text
  eval_transcripts  one row per attempt: the answerer transcript, answer and
                    judge write-up as text
  eval_cases        one row per case of the set as it was run, keyed by
                    `dataset_sha`: the question, tags, split and golden
  eval_query_timings  one row per query of a query replay (`query_replay.py`):
                    median and max time, where it was served from, the rows'
                    hash; empty for any other kind of run
  eval_files        one row per other file of the run's record, as text:
                    summary.json and results.jsonl of a replay, the diagnosis,
                    the coverage report, the model snapshot

The tables are also the run itself. `eval_runs.run_json`, `eval_events.event`
and `eval_files.text` hold run.json, each line of events.jsonl and each record
file exactly as written, so `import_run.py` rebuilds the run directory from
them, byte for byte, wherever the tables were loaded.

Every row of a table has the same columns, in a fixed order, and a nested value
(a list, an object) is written as JSON text. CSV and not JSON lines, because a
loader infers types from each file alone: a column that is null in every row of
one run reads as JSON from a JSON-lines file, and DuckDB then refuses to union
it with the same column holding values from another run. From CSV it reads as
VARCHAR, which unions with anything. So a model reading several runs unions by
name and CASTS each column it computes on (`TRY_CAST(passed AS BOOLEAN)`):
measured on DuckDB 1.5.1.

`--summary` writes one file instead, `eval_summary.csv`, into
`<run>/export-summary/`: the run's one row of `eval_runs` without `run_json`
and the slice's case ids, plus the pass rate on each split. It holds the
numbers and the pins and nothing else: no question, no golden, no answer, no
query, no case id. That is the file that may be loaded where the agent under
test can read, when there is nowhere else: a database in the package's own
environment. It cannot be imported back into a run.

THE FILES HOLD THE ANSWER KEY. `eval_cases` carries every golden, and a
transcript holds an answer. Load them only where the agent being evaluated
cannot read them: a database is commonly readable from everything in its
environment, so that means an environment of its own.
"""
from __future__ import annotations

import argparse
import csv
import json
import pathlib
import re
import sys
from typing import Any

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent.parent / "eval-answer" / "scripts"))
sys.path.insert(0, str(HERE))

import ledger  # noqa: E402
from ledger import read_jsonl  # noqa: E402
from run_index import runs as index_runs  # noqa: E402

TABLES = ("eval_runs", "eval_attempts", "eval_events", "eval_transcripts", "eval_cases",
          "eval_query_timings", "eval_files")
# The files of a run that are its record, beside run.json and events.jsonl,
# which have columns of their own. Only what sits at the top of the run
# directory: artifacts/ is in eval_transcripts, and rows/ (a replay's first 500
# rows per query), export/ and voided/ are not carried.
NOT_A_RECORD_FILE = ("run.json", "events.jsonl")
# A table name a loader accepts: a lowercase letter or _ first, then lowercase
# letters, digits or _, at most 63 characters. The run id is appended to keep
# one run's tables apart from another's until the loader can append to one.
NAME = re.compile(r"[^a-z0-9_]+")


def table_name(table: str, run_id: str) -> str:
    """`eval_attempts__baseline_01`: the table plus the run id, made loadable."""
    suffix = NAME.sub("_", run_id.lower()).strip("_") or "run"
    return f"{table}__{suffix}"[:63].rstrip("_")


def as_text(v: Any) -> Any:
    """Nested values as JSON text, so every run's column has one type."""
    return json.dumps(v, ensure_ascii=False, sort_keys=True) if isinstance(v, (list, dict)) else v


def read_text(p: pathlib.Path) -> str | None:
    try:
        return p.read_text()
    except (OSError, UnicodeDecodeError):
        return None


def rows_runs(run: pathlib.Path, meta: dict[str, Any], index_row: dict[str, Any]) -> list[dict]:
    pv = meta.get("packageVersion") or {}
    sl = meta.get("slice") or {}
    return [{
        "run_id": run.name, "set_name": meta.get("setName"), "label": meta.get("label"),
        "kind": "full" if index_row.get("full") else index_row.get("kind"),
        "phase": meta.get("phase"), "status": meta.get("status"), "started": meta.get("started"),
        "target": meta.get("target"), "environment": meta.get("environment"),
        "package": meta.get("package"), "version_id": pv.get("id"),
        "content_hash": pv.get("contentHash"), "base_version": pv.get("baseVersion"),
        "dataset_version": meta.get("datasetVersion"), "dataset_sha": meta.get("datasetSha"),
        "slice_name": sl.get("name"), "slice_qids": as_text(sl.get("qids")),
        "answerer_model": meta.get("answererModel"), "judge_model": meta.get("judgeModel"),
        "judge_version": meta.get("judgeVersion"), "rubric_sha": meta.get("rubricSha"),
        "judge_prompt_sha": meta.get("judgePromptSha"),
        "retrieval_mode": meta.get("retrievalMode"), "model_git_sha": meta.get("modelGitSha"),
        "cases": index_row.get("cases"), "passed": index_row.get("passed"),
        "decided": index_row.get("decided"), "recall_exact": index_row.get("recall_exact"),
        "answerer_cost_usd": meta.get("answererCostUsd"), "judge_cost_usd": meta.get("judgeCostUsd"),
        "p50_ms": index_row.get("p50_ms"), "total_ms": index_row.get("total_ms"),
        "build_ms": index_row.get("build_ms"),
        "build_scanned_bytes": index_row.get("build_scanned_bytes"),
        # The file as written, not a re-serialization: import_run.py writes
        # this text back, and a store tells two copies of a run apart by their
        # bytes.
        "run_json": (run / "run.json").read_text(),
    }]


ATTEMPT_COLUMNS = ("run_id", "qid", "sample", "phase", "question_sha", "golden_revision",
                   "verdict", "passed", "confidence", "tags", "split", "submitted",
                   "final_query", "n_get_context", "n_execute", "n_execute_errors",
                   "contaminated", "answer_captured", "cost_usd", "num_turns",
                   "wall_seconds", "recall", "recall_exact", "precision", "error")


def rows_attempts(run: pathlib.Path, events: list[dict], cases: dict[str, dict]) -> list[dict]:
    out = []
    phase = json.loads((run / "run.json").read_text()).get("phase")
    if phase == "query-replay":                              # timings, not attempts
        return out
    if (run / "results.jsonl").is_file():                    # a retrieval replay
        for r in read_jsonl(run / "results.jsonl"):
            c = cases.get(r.get("qid"), {})
            row = {k: None for k in ATTEMPT_COLUMNS}
            row.update(run_id=run.name, qid=r.get("qid"), phase="replay",
                       question_sha=c.get("questionSha"), tags=as_text(c.get("tags") or []),
                       split=c.get("split"), recall=r.get("recall"),
                       recall_exact=r.get("recall_exact"), precision=r.get("precision"),
                       error=r.get("error"))
            out.append(row)
        return out
    scores = {(e.get("qid"), e.get("sample")): e for e in events if e.get("kind") == "score"}
    for e in events:
        if e.get("kind") != "attempt":
            continue
        s = scores.get((e.get("qid"), e.get("sample")), {})
        c = cases.get(e.get("qid"), {})
        v = s.get("verdict")
        row = {k: None for k in ATTEMPT_COLUMNS}
        row.update(
            run_id=run.name, qid=e.get("qid"), sample=e.get("sample"), phase=e.get("phase"),
            question_sha=e.get("question_sha"), golden_revision=s.get("golden_revision"),
            verdict=v, passed=True if v == "match" else False if v == "no_match" else None,
            confidence=s.get("confidence"), tags=as_text(c.get("tags") or []),
            split=c.get("split"), submitted=e.get("submitted"),
            final_query=e.get("final_query"), n_get_context=e.get("n_get_context"),
            n_execute=e.get("n_execute"), n_execute_errors=e.get("n_execute_errors"),
            contaminated=as_text(e.get("contaminated")), answer_captured=e.get("answer_captured"),
            cost_usd=e.get("cost_usd"), num_turns=e.get("num_turns"),
            wall_seconds=e.get("wall_seconds"), error=e.get("run_error"))
        out.append(row)
    return out


def rows_events(run: pathlib.Path, events: list[dict]) -> list[dict]:
    """One row per ledger line, `event` being the line as written."""
    f = run / "events.jsonl"
    lines = [l for l in f.read_text().splitlines() if l.strip()] if f.is_file() else []
    return [{"run_id": run.name, "seq": i, "kind": e.get("kind"), "qid": e.get("qid"),
             "sample": e.get("sample"), "event": line}
            for i, (e, line) in enumerate(zip(events, lines))]


def rows_files(run: pathlib.Path) -> list[dict]:
    out = []
    for f in sorted(run.iterdir()):
        if f.is_file() and f.name not in NOT_A_RECORD_FILE and not f.name.startswith("."):
            text = read_text(f)
            if text is not None:
                out.append({"run_id": run.name, "path": f.name, "text": text})
    return out


def rows_transcripts(run: pathlib.Path, events: list[dict]) -> list[dict]:
    art = run / "artifacts"
    qids = sorted({e["qid"] for e in events if e.get("kind") == "attempt"}
                  | ({d.name for d in art.iterdir() if d.is_dir()} if art.is_dir() else set()))
    out = []
    for q in qids:
        d = art / q
        row = {"run_id": run.name, "qid": q,
               "transcript": read_text(d / "answerer.jsonl"),
               "answer": read_text(d / "answer.md"), "judge": read_text(d / "judge.md")}
        if any(row[k] for k in ("transcript", "answer", "judge")):
            out.append(row)
    return out


TIMING_COLUMNS = ("run_id", "qid", "tags", "source", "model_path", "query", "givens",
                  "median_ms", "min_ms", "max_ms", "round_trip_ms", "timing", "served_from",
                  "query_cost_bytes", "rows_sha", "n_rows", "unstable_rows", "slow", "error",
                  "count")


def rows_timings(run: pathlib.Path, meta: dict[str, Any]) -> list[dict]:
    if meta.get("phase") != "query-replay" or not (run / "results.jsonl").is_file():
        return []
    out = []
    for r in read_jsonl(run / "results.jsonl"):
        row = {k: r.get(k) for k in TIMING_COLUMNS if k in r}
        row.update(run_id=run.name, model_path=r.get("modelPath"),
                   tags=as_text(r.get("tags") or []), givens=as_text(r.get("givens")),
                   served_from=as_text(r.get("served_from")))
        out.append(row)
    return out


def rows_cases(run_id: str, dataset_sha: str | None, cases: list[dict]) -> list[dict]:
    return [{"run_id": run_id, "dataset_sha": dataset_sha, "qid": c.get("qid"),
             "question": c.get("question"), "question_sha": c.get("questionSha"),
             "tags": as_text(c.get("tags") or []), "split": c.get("split"),
             "golden_revision": c.get("goldenRevision"), "golden": as_text(c.get("golden")),
             "case": as_text(c)} for c in cases]


# What the summary leaves out of the eval_runs row, and why: run.json carries
# the judge's notes on doubted keys, and a slice's qids name cases.
NOT_IN_SUMMARY = ("run_json", "slice_qids")
SUMMARY_EXTRA = ("dev_passed", "dev_decided", "holdout_passed", "holdout_decided",
                 "test_passed", "test_decided", "diagnose_cost_usd", "weighted_ms")


def rows_summary(run: pathlib.Path, meta: dict[str, Any], index_row: dict[str, Any],
                 cases: dict[str, dict]) -> list[dict]:
    """The run's numbers and pins, and nothing that names a case or an answer."""
    row = {k: v for k, v in rows_runs(run, meta, index_row)[0].items() if k not in NOT_IN_SUMMARY}
    from flip_table import verdicts
    by: dict[str, list[int]] = {}
    if (run / "events.jsonl").is_file() and meta.get("phase") != "query-replay":
        for q, v in verdicts(run).items():
            if v["passed"] is not None:
                s = by.setdefault((cases.get(q) or {}).get("split") or "dev", [0, 0])
                s[0] += bool(v["passed"])
                s[1] += 1
    for split in ("dev", "holdout", "test"):
        p, d = by.get(split, (None, None))
        row[f"{split}_passed"], row[f"{split}_decided"] = p, d
    row["diagnose_cost_usd"] = meta.get("diagnoseCostUsd")
    try:
        row["weighted_ms"] = json.loads((run / "summary.json").read_text()).get("weighted_ms")
    except (OSError, ValueError):
        row["weighted_ms"] = None
    return [row]


def export_summary(run: pathlib.Path, set_dir: pathlib.Path, out: pathlib.Path) -> dict[str, Any]:
    """Write eval_summary.csv and manifest.json; return the manifest."""
    meta = json.loads((run / "run.json").read_text())
    case_list = read_jsonl(set_dir / "cases.jsonl")
    index_row = next((r for r in index_runs(run.parent.parent, len(case_list))
                      if r["run"] == run.name), {})
    rows = rows_summary(run, meta, index_row, {c.get("qid"): c for c in case_list})
    out.mkdir(parents=True, exist_ok=True)
    f = out / "eval_summary.csv"
    write_csv(f, rows, SUMMARY_COLUMNS)
    manifest = {"runId": run.name, "setName": meta.get("setName"),
                "packageVersion": meta.get("packageVersion"), "summaryOnly": True,
                "tables": {"eval_summary": {"file": f.name, "rows": 1, "bytes": f.stat().st_size,
                                            "name": table_name("eval_summary", run.name)}}}
    (out / "manifest.json").write_text(json.dumps(manifest, indent=1) + "\n")
    return manifest


def export_summary_all(runs_dir: pathlib.Path, set_dir: pathlib.Path,
                       out: pathlib.Path) -> dict[str, Any]:
    """One eval_summary.csv with a row per run under `runs_dir`, oldest first.

    A database table cannot have rows added to it, so the history of the
    number is this one file, written whole each time and loaded over the last.
    """
    case_list = read_jsonl(set_dir / "cases.jsonl")
    cases = {c.get("qid"): c for c in case_list}
    index = {r["run"]: r for r in index_runs(runs_dir.parent, len(case_list))}
    rows = []
    for run in sorted(p for p in runs_dir.iterdir() if (p / "run.json").is_file()):
        meta = json.loads((run / "run.json").read_text())
        rows += rows_summary(run, meta, index.get(run.name, {}), cases)
    rows.sort(key=lambda r: (r.get("started") or "", r.get("run_id") or ""))
    out.mkdir(parents=True, exist_ok=True)
    f = out / "eval_summary.csv"
    write_csv(f, rows, SUMMARY_COLUMNS)
    manifest = {"setName": set_dir.name, "summaryOnly": True, "runs": [r["run_id"] for r in rows],
                "tables": {"eval_summary": {"file": f.name, "rows": len(rows),
                                            "bytes": f.stat().st_size, "name": "eval_summary"}}}
    (out / "manifest.json").write_text(json.dumps(manifest, indent=1) + "\n")
    return manifest


COLUMNS = {
    "eval_runs": ("run_id", "set_name", "label", "kind", "phase", "status", "started", "target",
                  "environment", "package", "version_id", "content_hash", "base_version",
                  "dataset_version", "dataset_sha", "slice_name", "slice_qids", "answerer_model",
                  "judge_model", "judge_version", "rubric_sha", "judge_prompt_sha",
                  "retrieval_mode", "model_git_sha", "cases", "passed", "decided",
                  "recall_exact", "answerer_cost_usd", "judge_cost_usd", "p50_ms", "total_ms",
                  "build_ms", "build_scanned_bytes", "run_json"),
    "eval_attempts": ATTEMPT_COLUMNS,
    "eval_events": ("run_id", "seq", "kind", "qid", "sample", "event"),
    "eval_transcripts": ("run_id", "qid", "transcript", "answer", "judge"),
    "eval_cases": ("run_id", "dataset_sha", "qid", "question", "question_sha", "tags", "split",
                   "golden_revision", "golden", "case"),
    "eval_query_timings": TIMING_COLUMNS,
    "eval_files": ("run_id", "path", "text"),
}


SUMMARY_COLUMNS = tuple(c for c in COLUMNS["eval_runs"] if c not in NOT_IN_SUMMARY) + SUMMARY_EXTRA


def write_csv(path: pathlib.Path, rows: list[dict[str, Any]], columns: tuple[str, ...]) -> None:
    """A header, then one line per row in `columns` order. Text is quoted; a
    number or boolean is bare; None is an empty field, which a loader reads as
    NULL."""
    extra = sorted({k for r in rows for k in r} - set(columns))
    if extra:
        raise ValueError(f"{path.name}: columns not in its schema: {', '.join(extra)}")
    with path.open("w", newline="") as f:
        w = csv.writer(f, quoting=csv.QUOTE_NONNUMERIC)
        w.writerow(columns)
        for r in rows:
            w.writerow(["" if r.get(c) is None else as_text(r.get(c)) for c in columns])


def export(run: pathlib.Path, set_dir: pathlib.Path, out: pathlib.Path) -> dict[str, Any]:
    """Write the seven files and manifest.json; return the manifest."""
    meta = json.loads((run / "run.json").read_text())
    events = read_jsonl(run / "events.jsonl") if (run / "events.jsonl").is_file() else []
    case_list = read_jsonl(set_dir / "cases.jsonl")
    cases = {c.get("qid"): c for c in case_list}
    n_cases = len(case_list)
    index_row = next((r for r in index_runs(run.parent.parent, n_cases) if r["run"] == run.name), {})
    sha = ledger.dataset_sha(set_dir)
    if meta.get("datasetSha") and sha != meta["datasetSha"]:
        raise SystemExit(
            f"the set at {set_dir} is not the one this run scored: datasetSha {sha} on disk, "
            f"{meta['datasetSha']} in run.json. Fix: check out the set's commit the run used, "
            f"so eval_cases holds the cases the verdicts were about.")
    tables = {
        "eval_runs": rows_runs(run, meta, index_row),
        "eval_attempts": rows_attempts(run, events, cases),
        "eval_events": rows_events(run, events),
        "eval_transcripts": rows_transcripts(run, events),
        "eval_cases": rows_cases(run.name, sha, case_list),
        "eval_query_timings": rows_timings(run, meta),
        "eval_files": rows_files(run),
    }
    out.mkdir(parents=True, exist_ok=True)
    manifest: dict[str, Any] = {"runId": run.name, "setName": meta.get("setName"),
                                "packageVersion": meta.get("packageVersion"),
                                "datasetSha": sha, "tables": {}}
    for t in TABLES:
        f = out / f"{t}.csv"
        write_csv(f, tables[t], COLUMNS[t])
        manifest["tables"][t] = {"file": f.name, "rows": len(tables[t]),
                                 "name": table_name(t, run.name), "bytes": f.stat().st_size}
    (out / "manifest.json").write_text(json.dumps(manifest, indent=1) + "\n")
    return manifest


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--run", type=pathlib.Path, default=None, help="the run directory")
    ap.add_argument("--all", dest="every", action="store_true",
                    help="with --summary: one eval_summary.csv holding a row per run of the "
                         "set, to load over the last one. Default out: <workdir>/export-summary")
    ap.add_argument("--set", dest="set_dir", type=pathlib.Path, required=True)
    ap.add_argument("--out", type=pathlib.Path, default=None, help="default: <run>/export")
    ap.add_argument("--summary", action="store_true",
                    help="write only eval_summary.csv: the run's numbers and pins, with no "
                         "question, golden, answer, query or case id. Default out: "
                         "<run>/export-summary")
    a = ap.parse_args(argv)
    if a.every:
        if not a.summary:
            raise SystemExit("--all exports summaries only. Fix: add --summary, or export one "
                             "run in full with --run.")
        import config
        workdir = config.load(a.set_dir).workdir()
        out = a.out or workdir / "export-summary"
        m = export_summary_all(workdir / "runs", a.set_dir, out)
        info = m["tables"]["eval_summary"]
        print(f"{info['rows']:>6} rows  {info['bytes']:>9} bytes  {out / info['file']}  -> {info['name']}")
        print("This file holds each run's numbers and no case, key, answer or query.")
        return 0
    if a.run is None:
        raise SystemExit("Name the run. Fix: --run <run directory>, or --summary --all.")
    if not (a.run / "run.json").is_file():
        raise SystemExit(f"{a.run} holds no run.json. Fix: --run must be a run directory.")
    if a.summary:
        out = a.out or a.run / "export-summary"
        m = export_summary(a.run, a.set_dir, out)
        info = m["tables"]["eval_summary"]
        print(f"     1 row   {info['bytes']:>9} bytes  {out / info['file']}  -> {info['name']}")
        print("This file holds the run's numbers and no case, key, answer or query.")
        return 0
    m = export(a.run, a.set_dir, a.out or a.run / "export")
    out = a.out or a.run / "export"
    for t, info in m["tables"].items():
        print(f"{info['rows']:>6} rows  {info['bytes']:>9} bytes  {out / info['file']}  -> {info['name']}")
    print(f"manifest: {out / 'manifest.json'}")
    print("These files hold the answer key and the transcripts. Load them only where the "
          "agent under test cannot read them (reference/sharing-runs.md).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
