#!/usr/bin/env python3
"""Turn one or more query replays into a Malloy package you can serve. Stdlib only.

  python3 eval.py package --set <set-dir> --label perf-02            # one replay
  python3 build_run_package.py --set <set-dir> \
      --run <workdir>/runs/perf-01 --run <workdir>/runs/perf-02     # compared

`eval.py package` comes here when every run it is given is a query replay
(`query_replay.py`, phase `query-replay`). It writes three CSVs, a model over
them (`speed_run.malloy`), a report (`speed_report.malloy`) and a data app
(`public/`), from templates/speed-run-package.

The FIRST replay given is the baseline, and each later one is compared with it
by `query_replay.compare`, the rule the replay itself prints. The package
records each comparison's verdict rather than re-deciding it in Malloy, so the
app, the report and the terminal cannot disagree about what was faster.

It is registered where an answer-run package is, on the truth server: a replay
holds the query behind every passing answer, which is most of an answer key.
"""
from __future__ import annotations

import json
import pathlib
import shutil
import sys
from typing import Any

HERE = pathlib.Path(__file__).resolve().parent
TEMPLATE = HERE.parent / "templates" / "speed-run-package"
sys.path.insert(0, str(HERE.parent.parent / "eval-answer" / "scripts"))
sys.path.insert(0, str(HERE))

from build_run_package import read_json, read_jsonl, stamp_version, write_csv  # noqa: E402
from query_replay import PHASE, compare  # noqa: E402
from run_index import WORKING_COPY, version_label  # noqa: E402

RUN_COLUMNS = ["run_id", "version", "started", "from_run", "repeats", "budget_ms",
               "latency_band", "p50_ms", "p95_ms", "total_ms", "queries", "errors",
               "persist_sources", "bound", "materialization_id"]
TIMING_COLUMNS = ["run_id", "qid", "source_name", "tags", "median_ms", "min_ms", "max_ms",
                  "slow", "error", "served_from", "rows_sha", "unstable_rows", "query"]
COMPARISON_COLUMNS = ["baseline_run", "run_id", "qid", "source_name", "verdict",
                      "before_ms", "after_ms", "ratio"]


def is_replay(run: pathlib.Path) -> bool:
    return read_json(run / "run.json").get("phase") == PHASE


def run_row(run: pathlib.Path) -> dict[str, Any]:
    meta, s = read_json(run / "run.json"), read_json(run / "summary.json")
    mat = meta.get("materialization") or {}
    return {"run_id": run.name, "version": version_label(meta) or WORKING_COPY,
            "started": meta.get("started"), "from_run": meta.get("fromRun"),
            "repeats": meta.get("repeats"), "budget_ms": meta.get("budgetMs"),
            "latency_band": meta.get("latencyBand"), "p50_ms": s.get("p50_ms"),
            "p95_ms": s.get("p95_ms"), "total_ms": s.get("total_ms"),
            "queries": s.get("queries"), "errors": s.get("errors"),
            "persist_sources": mat.get("persistSources") or [],
            "bound": mat.get("bound"),
            "materialization_id": (mat.get("latestMaterialization") or {}).get("id")}


def comparison_rows(base: pathlib.Path, run: pathlib.Path) -> list[dict[str, Any]]:
    """One row per query the two replays share, with the verdict `compare` gave it."""
    A = read_jsonl(base / "results.jsonl")
    B = read_jsonl(run / "results.jsonl")
    band = read_json(run / "run.json").get("latencyBand") or 0.2
    c = compare(A, B, band)
    verdict: dict[str, str] = {}
    for k in ("rows_changed", "broke", "fixed", "query_changed", "unstable"):
        verdict.update({q: k for q in c[k]})
    for k in ("faster", "slower"):
        verdict.update({e["qid"]: k for e in c[k]})
    a = {r["qid"]: r for r in A}
    out = []
    for r in B:
        q = r["qid"]
        if q not in a or (a[q].get("error") and r.get("error")):
            continue
        before, after = a[q].get("median_ms"), r.get("median_ms")
        out.append({"baseline_run": base.name, "run_id": run.name, "qid": q,
                    "source_name": r.get("source"), "verdict": verdict.get(q, "within_band"),
                    "before_ms": before, "after_ms": after,
                    "ratio": round(after / before, 3) if before and after is not None else None})
    return out


def build(run_dirs: list[pathlib.Path], out: pathlib.Path) -> dict[str, int]:
    data = out / "data"
    data.mkdir(parents=True, exist_ok=True)
    runs = [run_row(r) for r in run_dirs]
    # `source` is a Malloy keyword, so the column is source_name.
    timings = [{**t, "run_id": r.name, "source_name": t.get("source")} for r in run_dirs
               for t in read_jsonl(r / "results.jsonl")]
    comparisons = [row for r in run_dirs[1:] for row in comparison_rows(run_dirs[0], r)]
    write_csv(data / "runs.csv", runs, RUN_COLUMNS)
    write_csv(data / "timings.csv", timings, TIMING_COLUMNS)
    write_csv(data / "comparisons.csv", comparisons, COMPARISON_COLUMNS)
    for name in ("publisher.json", "speed_run.malloy", "speed_report.malloy"):
        shutil.copy(TEMPLATE / name, out / name)
    stamp_version(out / "publisher.json")   # a rebuild publishes as a new version
    shutil.copytree(TEMPLATE / "public", out / "public", dirs_exist_ok=True)
    return {"replays": len(runs), "timings": len(timings), "comparisons": len(comparisons)}
