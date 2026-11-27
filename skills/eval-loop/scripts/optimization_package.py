#!/usr/bin/env python3
"""The optimization package: its tables, and laying it down. Stdlib only.

One package beside each served package (reference/optimization-loop.md),
at `[optimization] package_dir`, default `<workdir>/packages/optimization-<set>`:

  history.malloy          model 1: production (ingest_logs.py, judge_sample.py)
  optimization.malloy     model 2: what the loop tried (build_history.py, predictions.py)
  public/history/         data app 1
  public/optimization/    data app 2
  data/<table>.csv        every table below

It is never inside the served package and is served where the answerer
cannot reach it, because it holds verdicts. Judging sets and their runs live
in the workdir, not here: a run's model snapshot inside a package stops it
loading.

This module is the one definition of each table's columns. Two kinds:

- APPENDED (production and predictions): rows are added once, by key, and
  never rewritten. A hosted run loads the same CSVs.
- REBUILT (the eval-run views): rewritten from the runs on every
  `eval.py history`, so two people building from the same runs get the same
  tables.
"""
from __future__ import annotations

import csv
import pathlib
from typing import Any

HERE = pathlib.Path(__file__).resolve().parent
TEMPLATE = HERE.parent / "templates" / "optimization-package"

# table -> (key columns, timestamp column, columns)
APPENDED: dict[str, tuple[tuple[str, ...], str, tuple[str, ...]]] = {
    # Model 1, written by ingest_logs.py.
    "prod_turns": (("turn_id",), "at", (
        "turn_id", "session_id", "at", "environment", "package", "model_version", "client",
        "question", "n_get_context", "n_execute", "n_errors", "tokens", "cost_usd", "kind")),
    "prod_transcripts": (("turn_id",), "", ("turn_id", "transcript")),
    "prod_queries": (("execution_id",), "at", (
        "execution_id", "at", "environment", "package", "model_version", "model_path", "source",
        "query_text", "origin", "turn_id", "served_by", "table_id", "duration_ms", "bytes",
        "status", "engine", "bytes_scanned")),
    "prod_table_builds": (("build_id",), "started_at", (
        "build_id", "package", "model_version", "table_id", "source", "kind", "grain",
        "started_at", "build_ms", "rows", "bytes", "status")),
    "model_versions": (("package", "version"), "published_at", (
        "package", "version", "commit", "published_at")),
    # Model 1, written by judge_sample.py collect.
    "prod_judgments": (("turn_id", "metric_version"), "judged_at", (
        "turn_id", "judged_at", "run_id", "judge_version", "metric_version", "sampled_because",
        "verdict", "right", "failure_code", "component", "owner", "note", "basis",
        "question_group", "judged_turn", "user_signal", "depends_on", "key_kind",
        "retrieval_recall")),
    # Model 2, written by predictions.py.
    "predictions": (("prediction_id",), "made_at", (
        "prediction_id", "made_at", "loop", "made_from", "candidate_commit", "pr_url", "metric",
        "metric_version", "baseline", "predicted", "low", "high", "method", "min_n",
        "check_after")),
    "outcomes": (("prediction_id",), "checked_at", (
        "prediction_id", "checked_at", "version", "measured", "n", "inside_range", "gap", "note")),
}

# Model 2's eval-run views, written by build_history.py.
REBUILT: dict[str, tuple[str, ...]] = {
    "runs": ("run", "kind", "version", "experiment", "started", "slice", "full", "passed",
             "decided", "cases", "p50_ms", "total_ms", "slow", "dataset_version", "status"),
    "versions": ("version", "run", "passed", "decided", "vs", "fixed", "broke", "perf_run",
                 "p50_ms", "total_ms", "faster", "slower", "rows_changed", "total_ratio"),
    "tag_scores": ("run", "version", "experiment", "started", "tag", "passed", "decided"),
    "routines": ("label", "kind", "mode", "started", "finished", "exit", "reason", "run",
                 "passed", "decided", "total_ms", "cost_usd", "keys_verified", "edits_tried",
                 "edits_accepted", "branch"),
}

# Written from eval.toml [performance] at every install, so the price the
# models use is the one the config states: one row, overwritten.
PRICE_COLUMNS = ("usd_per_compute_hour", "usd_per_gb_month", "usd_per_tb_scanned",
                 "max_query_ms", "max_staleness_hours", "credible_usd_per_compute_hour",
                 "credible_usd_per_gb_month")


def metric_version(run_meta: dict[str, Any]) -> str | None:
    """Which quality metric a run's verdicts are: its judge and rubric. A
    production judgment and an eval run judged the same way get the same
    string, so a prediction is checked only against verdicts of its kind."""
    judge, rubric = run_meta.get("judgeVersion"), (run_meta.get("rubricSha") or "")[:12]
    return ":".join(str(x) for x in (judge, rubric) if x) or None


def upgrade(path: pathlib.Path, cols: tuple[str, ...]) -> None:
    """Bring an existing CSV's header up to the table's columns.

    A table that gained columns keeps every stored row, with the new columns
    empty: the file is rewritten once under the new header. Without this a
    row appended under an older header lands its values in the wrong columns.
    A column that was dropped or renamed is refused: the stored values would
    have nowhere to go."""
    if not path.is_file():
        return
    with path.open(newline="", encoding="utf-8") as f:
        have = tuple(next(csv.reader(f), []))
    if have == tuple(cols):
        return
    gone = [c for c in have if c not in cols]
    if gone:
        raise SystemExit(f"Invalid {path.name}: it holds column(s) {', '.join(gone)} the table no "
                         f"longer defines (expected {', '.join(cols)}). Fix: move the file aside, "
                         f"or map those columns to the new names by hand")
    rows = read(path)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(cols))
        w.writeheader()
        w.writerows(rows)


def append(path: pathlib.Path, cols: tuple[str, ...], rows: list[dict[str, Any]]) -> None:
    """Add rows to a table's CSV, writing the header when the file is new and
    upgrading it when the table has gained columns since."""
    path.parent.mkdir(parents=True, exist_ok=True)
    upgrade(path, cols)
    fresh = not path.is_file()
    if not rows and not fresh:
        return
    with path.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(cols))
        if fresh:
            w.writeheader()
        w.writerows(rows)


def read(path: pathlib.Path) -> list[dict[str, str]]:
    if not path.is_file():
        return []
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def install(package: pathlib.Path, prices: dict[str, Any]) -> pathlib.Path:
    """Lay the package down: the template's models, apps and publisher.json,
    refreshed each time; a header-only CSV for every table not yet written, so
    both models compile before their first row; the price card. Rows already
    in `data/` are never touched. Returns the data directory."""
    for src in TEMPLATE.rglob("*"):
        if src.is_file() and "__pycache__" not in src.parts:
            dst = package / src.relative_to(TEMPLATE)
            dst.parent.mkdir(parents=True, exist_ok=True)
            dst.write_bytes(src.read_bytes())
    data = package / "data"
    for table, (_, _, cols) in APPENDED.items():
        append(data / f"{table}.csv", cols, [])
    for table, cols in REBUILT.items():
        append(data / f"{table}.csv", cols, [])
    with (data / "price_card.csv").open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(PRICE_COLUMNS))
        w.writeheader()
        w.writerow({c: prices.get(c) for c in PRICE_COLUMNS})
    return data


def install_for(cfg: Any) -> pathlib.Path:
    """install() at the set's optimization package, with its prices."""
    return install(cfg.optimization_dir(),
                   {c: cfg.get("performance", c) for c in PRICE_COLUMNS})
