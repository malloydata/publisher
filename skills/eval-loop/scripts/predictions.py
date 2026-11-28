#!/usr/bin/env python3
"""Write down what a change should do in production, and later check it. Stdlib only.

  python3 eval.py predict  --set <set-dir> --quality-run fix-01 --commit <sha> [--pr <url>]
  python3 eval.py predict  --set <set-dir> --simulation sim-perf-01 --commit <sha> [--option <name>]
  python3 eval.py outcomes --set <set-dir>

Steps O5 and O7 of the optimization loop (reference/optimization-loop.md). An
eval set and a simulation are only useful if what they predict happens in
production. So before a change goes to a person, `predict` appends what it
should do to the optimization package's `predictions` table. After the change
is published and production has had time to show it, `outcomes` measures the
same number from the history tables and appends the comparison. `baseline` is
production's value for the newest published version when the prediction is made. Model 2 and
its app read both.

WHAT IS PREDICTED

- From a quality run (the candidate's run on the eval set): `quality_rate`,
  its pass rate over decided cases, with a 90% Wilson interval. Its
  `metric_version` is the run's judge and rubric, the same string a
  production judgment by that judge carries.
- From a simulation (simulate.py): `mean_query_ms` and `over_limit_share`
  over the workload, each query weighted by how often it ran, for the chosen
  option. The range is the `[performance] latency_band`, the noise a replay
  already treats as no change.

HOW AN OUTCOME IS MATCHED

By commit. The prediction names the candidate's commit. `outcomes` looks for
a published version with that commit in `model_versions`. A person may edit a
pull request before merging it, and then no published version carries the
predicted commit: once the prediction is due, that is recorded as the
outcome, with no number, because nothing published was the thing predicted.

An outcome is written only when the prediction is due (`check_after`) and
production has at least `min_n` decided turns or queries on that version.
Before then it is skipped and the reason printed. Each prediction gets one
outcome, never rewritten.
"""
from __future__ import annotations

import argparse
import datetime
import hashlib
import json
import math
import pathlib
import sys
from typing import Any

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent.parent / "eval-answer" / "scripts"))
sys.path.insert(0, str(HERE))

import config  # noqa: E402
from ledger import read_jsonl  # noqa: E402
from optimization_package import APPENDED, append, install_for, metric_version, read  # noqa: E402

Z90 = 1.645


def wilson(passed: int, n: int, z: float = Z90) -> tuple[float, float]:
    """The interval a pass rate of passed/n is consistent with."""
    if n == 0:
        return 0.0, 1.0
    p = passed / n
    d = 1 + z * z / n
    centre = (p + z * z / (2 * n)) / d
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return round(max(0.0, centre - half), 4), round(min(1.0, centre + half), 4)


def now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def row(loop: str, made_from: str, commit: str, pr: str | None, metric: str, mversion: str | None,
        baseline: float | None, predicted: float, low: float, high: float, method: str,
        min_n: int, days: int, at: datetime.datetime) -> dict[str, Any]:
    pid = hashlib.sha256(f"{made_from}|{commit}|{metric}".encode()).hexdigest()[:16]
    return {"prediction_id": pid, "made_at": at.isoformat(timespec="seconds"), "loop": loop,
            "made_from": made_from, "candidate_commit": commit, "pr_url": pr, "metric": metric,
            "metric_version": mversion, "baseline": baseline, "predicted": predicted, "low": low,
            "high": high, "method": method, "min_n": min_n,
            "check_after": (at + datetime.timedelta(days=days)).isoformat(timespec="seconds")}


def from_quality_run(run: pathlib.Path) -> dict[str, Any]:
    """The candidate's pass rate on the eval set, as a prediction of production's."""
    meta = json.loads((run / "run.json").read_text())
    verdicts = [e.get("verdict") for e in read_jsonl(run / "events.jsonl") if e.get("kind") == "score"]
    passed = sum(1 for v in verdicts if v == "match")
    decided = sum(1 for v in verdicts if v in ("match", "no_match"))
    if not decided:
        raise SystemExit(f"{run.name} has no decided case, so it predicts nothing. "
                         f"Fix: name a run with judged cases.")
    low, high = wilson(passed, decided)
    return {"metric": "quality_rate", "metric_version": metric_version(meta),
            "predicted": round(passed / decided, 4), "low": low, "high": high,
            "method": f"pass rate on {decided} decided cases of {run.name}; 90% Wilson interval"}


def weighted(preds: list[dict[str, Any]], limit_ms: float | None) -> tuple[float, float | None]:
    runs = sum(p.get("count") or 1 for p in preds)
    mean = sum((p.get("count") or 1) * p["predicted_ms"] for p in preds) / runs
    over = (sum(p.get("count") or 1 for p in preds if p["predicted_ms"] > limit_ms) / runs
            if limit_ms else None)
    return round(mean, 1), (round(over, 4) if over is not None else None)


def from_simulation(sim: dict[str, Any], option: str | None, band: float,
                    limit_ms: float | None) -> list[dict[str, Any]]:
    """The chosen option's predicted mean query time and share over the limit."""
    opts = [o for o in sim["options"] if o["option"] != "baseline"]
    chosen = next((o for o in opts if o["option"] == option), None) if option else \
        next((o for o in opts if o["feasible"]), None)
    if not chosen:
        raise SystemExit(f"No option {option or 'that is feasible'} in {sim['label']}. Fix: "
                         f"--option one of {', '.join(o['option'] for o in opts) or '(none)'}")
    preds = [p for p in sim["predictions"] if p["option"] == chosen["option"]]
    mean, over = weighted(preds, limit_ms)
    how = f"option {chosen['option']} of {sim['label']} ({sim['tier']}); range is latency_band {band:g}"
    out = [{"metric": "mean_query_ms", "metric_version": "duration_ms", "predicted": mean,
            "low": round(mean * (1 - band), 1), "high": round(mean * (1 + band), 1), "method": how}]
    if over is not None:
        out.append({"metric": "over_limit_share", "metric_version": f"over {limit_ms:g} ms",
                    "predicted": over, "low": round(over * (1 - band), 4),
                    "high": round(min(1.0, over * (1 + band)), 4), "method": how})
    return out


def measure(metric: str, mversion: str | None, version: str, data: pathlib.Path,
            limit_ms: float | None) -> tuple[float | None, int]:
    """Production's value of one metric for one published version, and its n."""
    if metric == "quality_rate":
        turns = {t["turn_id"] for t in read(data / "prod_turns.csv") if t["model_version"] == version}
        js = [j for j in read(data / "prod_judgments.csv")
              if j["turn_id"] in turns and j["metric_version"] == (mversion or "")
              and j["right"] in ("True", "False")]
        n = len(js)
        return (round(sum(j["right"] == "True" for j in js) / n, 4) if n else None), n
    qs = [float(q["duration_ms"]) for q in read(data / "prod_queries.csv")
          if q["model_version"] == version and q["duration_ms"] and q["status"] in ("", "ok")]
    if not qs:
        return None, 0
    if metric == "mean_query_ms":
        return round(sum(qs) / len(qs), 1), len(qs)
    if metric == "over_limit_share" and limit_ms:
        return round(sum(ms > limit_ms for ms in qs) / len(qs), 4), len(qs)
    return None, len(qs)


def outcome(p: dict[str, str], versions: list[dict[str, str]], data: pathlib.Path,
            limit_ms: float | None, at: datetime.datetime) -> tuple[dict[str, Any] | None, str]:
    """The outcome row for one prediction, or (None, why it is not due)."""
    if datetime.datetime.fromisoformat(p["check_after"]) > at:
        return None, f"due {p['check_after'][:10]}"
    commit = p["candidate_commit"]
    match = [v for v in versions if v["commit"] and commit
             and (v["commit"].startswith(commit) or commit.startswith(v["commit"]))]
    base = {"prediction_id": p["prediction_id"], "checked_at": at.isoformat(timespec="seconds")}
    if not match:
        return {**base, "version": None, "measured": None, "n": 0, "inside_range": None,
                "gap": None, "note": "no published version has this commit: the change was "
                                     "edited before merging, or not merged"}, "recorded"
    version = match[0]["version"]
    measured, n = measure(p["metric"], p["metric_version"] or None, version, data, limit_ms)
    if measured is None or n < int(p["min_n"] or 1):
        return None, f"version {version} has {n} of the {p['min_n']} needed"
    low, high, pred = float(p["low"]), float(p["high"]), float(p["predicted"])
    return {**base, "version": version, "measured": measured, "n": n,
            "inside_range": low <= measured <= high, "gap": round(measured - pred, 4),
            "note": None}, "recorded"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("verb", choices=("predict", "outcomes"))
    ap.add_argument("--set", dest="set_dir", required=True)
    src = ap.add_mutually_exclusive_group()
    src.add_argument("--quality-run", help="the candidate's run on the eval set")
    src.add_argument("--simulation", help="a simulation label (simulate.py)")
    ap.add_argument("--option", help="with --simulation: the option chosen (default: best feasible)")
    ap.add_argument("--commit", help="the candidate's commit, as it goes to review")
    ap.add_argument("--pr", help="the pull request's address")
    ap.add_argument("--check-after-days", type=int, default=7)
    ap.add_argument("--min-n", type=int, default=None,
                    help="production turns (quality, default 20) or queries (speed, default 100)")
    a = ap.parse_args(argv)
    cfg = config.load(pathlib.Path(a.set_dir))
    data = install_for(cfg)
    limit = cfg.get("performance", "max_query_ms")
    at = now()
    cols = APPENDED["predictions"][2]
    if a.verb == "predict":
        if not a.commit or not (a.quality_run or a.simulation):
            raise SystemExit("predict needs --commit and one of --quality-run or --simulation")
        if a.quality_run:
            got = [from_quality_run(cfg.workdir() / "runs" / a.quality_run)]
            loop, made_from, min_n = "quality", a.quality_run, a.min_n or 20
        else:
            sim = json.loads((cfg.workdir() / "simulations" / a.simulation / "options.json").read_text())
            got = from_simulation(sim, a.option, cfg.get("performance", "latency_band"), limit)
            loop, made_from, min_n = "speed", a.simulation, a.min_n or 100
        have = {r["prediction_id"] for r in read(data / "predictions.csv")}
        live = max(read(data / "model_versions.csv"), key=lambda v: v["published_at"], default=None)
        base = {g["metric"]: (measure(g["metric"], g["metric_version"], live["version"], data, limit)[0]
                              if live else None) for g in got}
        rows = [row(loop, made_from, a.commit, a.pr, g["metric"], g["metric_version"], base[g["metric"]],
                    g["predicted"], g["low"], g["high"], g["method"], min_n,
                    a.check_after_days, at) for g in got]
        new = [r for r in rows if r["prediction_id"] not in have]
        append(data / "predictions.csv", cols, new)
        for r in rows:
            print(f"{r['metric']}: {r['predicted']} (range {r['low']} to {r['high']}; "
                  f"production now {r['baseline'] if r['baseline'] is not None else 'unmeasured'}), "
                  f"check after {r['check_after'][:10]}"
                  + ("" if r in new else " [already recorded]"))
        return 0
    done = {r["prediction_id"] for r in read(data / "outcomes.csv")}
    versions = read(data / "model_versions.csv")
    new, waiting = [], []
    for p in read(data / "predictions.csv"):
        if p["prediction_id"] in done:
            continue
        o, why = outcome(p, versions, data, limit, at)
        (new.append(o) if o else waiting.append(f"{p['metric']} from {p['made_from']}: {why}"))
    append(data / "outcomes.csv", APPENDED["outcomes"][2], new)
    for o in new:
        said = ("no number" if o["measured"] is None else
                f"{o['measured']} on {o['version']} (n {o['n']}), "
                f"{'inside' if o['inside_range'] else 'OUTSIDE'} the predicted range, gap {o['gap']}")
        print(f"{o['prediction_id']}: {said}" + (f"; {o['note']}" if o["note"] else ""))
    for w in waiting:
        print(f"not yet: {w}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
