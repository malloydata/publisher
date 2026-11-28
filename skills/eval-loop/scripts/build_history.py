#!/usr/bin/env python3
"""Every run of one set, as model 2 of the optimization package. Stdlib only.

  python3 eval.py history --set <set-dir>

Reads the runs in the set's workdir (including any pulled from the store,
run_store.py) and the routines' result files, and rewrites four of the
optimization package's tables (runs, versions, tag_scores, routines), which
`optimization.malloy`, `optimization_report.malloy` and the app at
`public/optimization/` read. Then registers the package on the truth server,
as a run package is, because it names the questions' tags and verdicts.

The package is the one optimization_package.py lays down, so the production
history ingest_logs.py appends sits beside it and is never touched here.
These four tables are a view, rebuilt from the runs every time and never
edited, so two people building from the same runs get the same page. The version rows come from `run_index.versions`, the same code
`eval.py versions` prints, so the page and the terminal cannot disagree.
"""
from __future__ import annotations

import argparse
import pathlib
import sys
from typing import Any

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent.parent / "eval-answer" / "scripts"))
sys.path.insert(0, str(HERE))

import config  # noqa: E402
import run_index  # noqa: E402
from build_run_package import register, stamp_version, write_csv  # noqa: E402
from flip_table import verdicts  # noqa: E402
from ledger import read_jsonl  # noqa: E402
from optimization_package import REBUILT, install_for  # noqa: E402

RUN_COLUMNS = list(REBUILT["runs"])
VERSION_COLUMNS = list(REBUILT["versions"])
TAG_COLUMNS = list(REBUILT["tag_scores"])
ROUTINE_COLUMNS = list(REBUILT["routines"])


def routine_rows(workdir: pathlib.Path) -> list[dict[str, Any]]:
    """One row per routine result file: what the routine did, in one line."""
    import json
    out = []
    for f in sorted((workdir / "routines").glob("*.json")):
        try:
            r = json.loads(f.read_text())
        except (OSError, ValueError):
            continue
        n, imp = r.get("number") or {}, r.get("improve") or {}
        cost = n.get("costUsd") or {}
        edits = imp.get("edits") or []
        out.append({"label": r.get("label"), "kind": r.get("kind"), "mode": r.get("mode"),
                    "started": r.get("started"), "finished": r.get("finished"),
                    "exit": r.get("exit"), "reason": r.get("reason"), "run": r.get("run"),
                    "passed": n.get("passed"), "decided": n.get("decided"),
                    "total_ms": n.get("totalMs"),
                    # The run, its judging and diagnosis, and every edit an improve tried.
                    "cost_usd": (round(sum(v or 0 for v in cost.values())
                                       + (imp.get("costUsd") or 0), 4) if cost or imp else None),
                    "keys_verified": r.get("keysVerified", True if r.get("kind") == "quality" else None),
                    "edits_tried": len(edits) if imp else None,
                    "edits_accepted": sum(e.get("outcome") == "accepted" for e in edits) if imp else None,
                    "branch": imp.get("branch")})
    out.sort(key=lambda x: x["started"] or "")
    return out


def version_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for v in run_index.versions(rows):
        perf, speed, flips = v.get("perf") or {}, v.get("speed") or {}, v.get("flips") or {}
        out.append({"version": v["version"], "run": v.get("run"), "passed": v.get("passed"),
                    "decided": v.get("decided"), "vs": v.get("vs"), "fixed": flips.get("fixed"),
                    "broke": flips.get("broke"), "perf_run": perf.get("run"),
                    "p50_ms": perf.get("p50_ms"), "total_ms": perf.get("total_ms"),
                    "faster": speed.get("faster"), "slower": speed.get("slower"),
                    "rows_changed": speed.get("rows_changed"),
                    "total_ratio": speed.get("total_ratio")})
    return out


def tag_rows(rows: list[dict[str, Any]], set_dir: pathlib.Path) -> list[dict[str, Any]]:
    tags = {c["qid"]: c.get("tags") or [] for c in read_jsonl(set_dir / "cases.jsonl")}
    out = []
    for r in rows:
        if r["kind"] not in ("run", "slice") or r.get("status") != "complete":
            continue
        tally: dict[str, list[int]] = {}
        for qid, v in verdicts(pathlib.Path(r["path"])).items():
            if v["passed"] is None:
                continue
            for t in tags.get(qid, []):
                t2 = tally.setdefault(t, [0, 0])
                t2[0] += int(bool(v["passed"]))
                t2[1] += 1
        out += [{"run": r["run"], "version": r.get("version") or run_index.WORKING_COPY,
                 "experiment": r.get("experiment"), "started": r.get("started"), "tag": t,
                 "passed": p, "decided": d} for t, (p, d) in sorted(tally.items())]
    return out


def build(cfg: config.Config) -> dict[str, int]:
    rows = run_index.set_runs(cfg)
    out = cfg.optimization_dir()
    data = install_for(cfg)
    runs = [{**r, "version": r.get("version") or run_index.WORKING_COPY,
             "dataset_version": r.get("datasetVersion")} for r in rows]
    versions, tags = version_rows(rows), tag_rows(rows, cfg.set_dir)
    write_csv(data / "runs.csv", runs, RUN_COLUMNS)
    write_csv(data / "versions.csv", versions, VERSION_COLUMNS)
    write_csv(data / "tag_scores.csv", tags, TAG_COLUMNS)
    routines = routine_rows(cfg.workdir())
    write_csv(data / "routines.csv", routines, ROUTINE_COLUMNS)
    stamp_version(out / "publisher.json")   # a rebuild publishes as a new version
    return {"runs": len(runs), "versions": len(versions), "tag rows": len(tags),
            "routines": len(routines)}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--set", dest="set_dir", required=True, type=pathlib.Path)
    ap.add_argument("--no-register", action="store_true")
    a = ap.parse_args(argv)
    cfg = config.load(a.set_dir)
    out = cfg.optimization_dir()
    counts = build(cfg)
    print(out)
    print("  " + ", ".join(f"{v} {k}" for k, v in counts.items()))
    base, env = cfg.truth_publisher(), cfg.get("truth", "environment")
    if not base or a.no_register:
        print(f"  not registered{'' if base else ': the set has no truth server'}. "
              f"Serve {out} on a Publisher the answerer cannot reach.")
        return 0
    failed = register(base, env, out)
    if failed:
        print(f"  ! could not register on {base}: {failed}", file=sys.stderr)
        return 1
    print(f"  eval runs:  {base}/environments/{env}/packages/{out.name}/optimization/")
    print(f"  production: {base}/environments/{env}/packages/{out.name}/history/")
    print(f"  report:     {base}/{env}/{out.name}/optimization_report")
    return 0


if __name__ == "__main__":
    sys.exit(main())
