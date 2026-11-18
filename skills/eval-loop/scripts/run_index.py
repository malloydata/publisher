#!/usr/bin/env python3
"""The runs a set has, and how each package version scored. Stdlib only.

  python3 run_index.py runs     --set <set-dir> [--json]
  python3 run_index.py versions --set <set-dir> [--publisher URL] [--json]
  # or: python3 eval.py runs --set <set-dir>;  python3 eval.py versions --set <set-dir>

Read straight from the run directories under the set's workdir
(`<workdir>/runs/*/run.json` and `events.jsonl`), never from a separate index:
a second record of the same facts would drift from the runs it describes.

`runs` lists one row per run: its label, kind, slice, package version, set
version, and score. A run's KIND is:

  replay   a retrieval replay (no answerer); its score is exact recall
  perf     a query replay (no answerer): passing queries re-run and timed;
           its score is the median query time
  slice    an answer run limited with --slice or --tag
  run      any other answer run; `full` when it scored every case of the set

`versions` lists one row per package version that has a scored answer run,
highest version first. Per version: whether it is `latest` or archived (from
the server's `GET …/packages/{pkg}/versions` when `--publisher` reaches it),
its most recent full run, that run's pass count and set version, and the
flips against the previous version's most recent full run. Flips are counted
only over cases both runs scored with the same `question_sha` and
`golden_revision`, the rule `flip_table.py` uses. A run with no published
version (`packageVersion.id` null) is a working copy. It is keyed by the
`version` its `publisher.json` named (`1.2.0 (unpublished)`), so a server run
without package versioning still shows one row per version the author bumped
to; a run that recorded no version at all appears as `working copy`.

Each version row also carries its most recent query replay: the median and
total query time, and against the previous version's replay, how many queries
got faster or slower and how many changed rows (`query_replay.compare`).
"""
from __future__ import annotations

import argparse
import json
import pathlib
import sys
from typing import Any

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent.parent / "eval-answer" / "scripts"))
sys.path.insert(0, str(HERE))

import config  # noqa: E402
from flip_table import incomparable, verdicts  # noqa: E402
from ledger import read_jsonl  # noqa: E402
from publisher_rest import package_versions  # noqa: E402

WORKING_COPY = "working copy"
PERF_PHASE = "query-replay"
UNPUBLISHED = "(unpublished)"


def version_label(meta: dict[str, Any]) -> str | None:
    """The version a run measured: its published id, else the publisher.json
    version of the working copy it ran on (`1.2.0 (unpublished)`), else None."""
    pv = meta.get("packageVersion") or {}
    return ((pv.get("id") if pv else meta.get("targetVersion"))
            or (f"{pv['baseVersion']} {UNPUBLISHED}" if pv.get("baseVersion") else None))


def version_key(v: str) -> tuple:
    """Semver order for display, highest first after reverse. A pre-release
    sorts below its release; anything unparseable sorts last."""
    core, _, pre = v.split(" ")[0].partition("-")
    try:
        nums = tuple(int(x) for x in core.split("+")[0].split("."))
    except ValueError:
        return ((-1,), 0, v)
    return (nums, 0 if pre else 1, pre)


def score_of(run: pathlib.Path) -> tuple[int, int]:
    """(passed, decided) over the run's verdicts, as `flip_table` reads them."""
    v = verdicts(run) if (run / "events.jsonl").is_file() else {}
    decided = [r for r in v.values() if r["passed"] is not None]
    return sum(1 for r in decided if r["passed"]), len(decided)


def experiment_of(package: str | None, main: str | None) -> str | None:
    """The experiment a run measured: None for the set's own package. An
    experiment is served as `<package>.<name>` (experiment.py); any other
    package than the set's is reported by its whole name."""
    if not package or not main or package == main:
        return None
    return package[len(main) + 1:] if package.startswith(main + ".") else package


def attempted(run: pathlib.Path) -> set[str]:
    """The cases a run answered."""
    return ({e["qid"] for e in read_jsonl(run / "events.jsonl") if e.get("kind") == "attempt"}
            if (run / "events.jsonl").is_file() else set())


def not_a_record(run: pathlib.Path, meta: dict[str, Any], n_cases: int | None,
                 main_package: str | None) -> str | None:
    """Why a run is not one the team compares against, or None when it is.

    The one rule: the store's `after_run` shares by it and `record` picks
    `--baseline record` by it, so a run is never shared as a record and then
    passed over, or the reverse. A record is a complete run of the main line
    (no experiment) over the whole set: every case for an answer run, and for
    a query replay the stored queries, all of them, with no stored answer's
    rows changed. A replay whose rows changed would otherwise become the
    baseline the next replay compares with, and the change would read as
    settled.
    """
    if meta.get("status") != "complete":
        return f"it is {meta.get('status')!r}, not complete"
    if experiment_of(meta.get("package"), main_package):
        return f"it measured an experiment ({meta.get('package')})"
    if meta.get("slice"):
        return "it covered a slice of the cases"
    if meta.get("phase") == PERF_PHASE:
        if meta.get("mode") == "workload":
            return "it timed a workload file, not the stored queries"
        try:
            c = json.loads((run / "summary.json").read_text()).get("compare") or {}
        except (OSError, ValueError):
            c = {}
        if c.get("rows_changed") or c.get("broke"):
            return (f"{len(c.get('rows_changed') or [])} stored answer(s) changed rows and "
                    f"{len(c.get('broke') or [])} now error against {c.get('baseline')}. If the "
                    f"change is meant, replay with no --baseline to start a new record")
        return None
    if not n_cases or len(attempted(run)) != n_cases:
        return "it did not answer every case of the set"
    return None


def why_not_record(cfg: config.Config, run: pathlib.Path) -> str | None:
    """`not_a_record` for a run of this set, read from its directory."""
    try:
        meta = json.loads((run / "run.json").read_text())
    except (OSError, ValueError):
        return "it has no readable run.json"
    return not_a_record(run, meta, set_size(cfg), main_line(cfg))


def set_size(cfg: config.Config) -> int | None:
    cases = cfg.set_dir / "cases.jsonl"
    return len(read_jsonl(cases)) if cases.is_file() else None


def main_line(cfg: config.Config) -> str | None:
    return cfg.get("model", "package") or cfg.set_meta.get("targetPackage")


def run_dirs(runs_dir: pathlib.Path) -> list[pathlib.Path]:
    """The run directories under `runs_dir`. A name starting with `.` is a copy
    still in progress (a push or a pull writes there, then renames), never a run."""
    return sorted(rj.parent for rj in runs_dir.glob("*/run.json")
                  if not rj.parent.name.startswith("."))


def runs(workdir: pathlib.Path, n_cases: int | None = None,
         main_package: str | None = None) -> list[dict[str, Any]]:
    """One row per run directory that holds a run.json, oldest first."""
    out = []
    for run in run_dirs(workdir / "runs"):
        try:
            meta = json.loads((run / "run.json").read_text())
        except (OSError, ValueError):
            continue
        pv = meta.get("packageVersion") or {}
        row: dict[str, Any] = {
            "run": run.name, "path": str(run), "started": meta.get("started"),
            "status": meta.get("status"), "phase": meta.get("phase"),
            "slice": (meta.get("slice") or {}).get("name")
            or ("tags " + ",".join((meta.get("slice") or {}).get("tags") or [])
                if meta.get("slice") else None),
            "version": version_label(meta),
            "contentHash": pv.get("contentHash"),
            "baseVersion": pv.get("baseVersion"),
            "package": meta.get("package"),
            "experiment": experiment_of(meta.get("package"), main_package),
            "datasetVersion": meta.get("datasetVersion"),
            "datasetSha": meta.get("datasetSha"),
            "answererModel": meta.get("answererModel"),
            "judgeModel": meta.get("judgeModel"),
        }
        why = not_a_record(run, meta, n_cases, main_package)
        row["record"], row["notRecord"] = why is None, why
        if meta.get("phase") == PERF_PHASE:
            row["kind"] = "perf"
            summary = {}
            try:
                summary = json.loads((run / "summary.json").read_text())
            except (OSError, ValueError):
                pass
            row["p50_ms"] = summary.get("p50_ms")
            row["total_ms"] = summary.get("total_ms")
            row["slow"] = len(summary.get("slow") or [])
            row["cases"] = summary.get("queries")
            row["errors"] = summary.get("errors")
            build = summary.get("build") or {}
            row["build_ms"] = build.get("build_ms")
            row["build_scanned_bytes"] = build.get("scanned_bytes")
        elif meta.get("phase") == "replay" or meta.get("mode") == "retrieval":
            row["kind"] = "replay"
            summary = {}
            try:
                summary = json.loads((run / "summary.json").read_text())
            except (OSError, ValueError):
                pass
            row["recall_exact"] = summary.get("recall_exact")
            row["cases"] = summary.get("cases")
        else:
            row["kind"] = "slice" if meta.get("slice") else "run"
            row["passed"], row["decided"] = score_of(run)
            answered = attempted(run)
            row["cases"] = len(answered)
            row["full"] = bool(n_cases) and row["kind"] == "run" and len(answered) == n_cases
        out.append(row)
    out.sort(key=lambda r: (r.get("started") or "", r["run"]))
    return out


def set_runs(cfg: config.Config) -> list[dict[str, Any]]:
    """Every run in the set's workdir, with `full` decided against the set's cases."""
    return runs(cfg.workdir(), set_size(cfg), main_line(cfg))


def record(rows: list[dict[str, Any]], kind: str = "run") -> dict[str, Any] | None:
    """The run `--baseline record` names: the newest of `kind` ("run" or
    "perf") that `not_a_record` passes.

    Not matched on datasetSha: a set edit that touched no case (a convention,
    a description) changes the sha, and a comparison already sets aside each
    case whose question or key changed.
    """
    fits = [r for r in rows if r.get("record") and r["kind"] == kind]
    return max(fits, key=lambda r: (r.get("started") or "", r["run"]), default=None)


def flips(a: pathlib.Path, b: pathlib.Path) -> dict[str, int]:
    """Pass/fail flips from run `a` to run `b` over the cases both can be compared on."""
    A, B = verdicts(a), verdicts(b)
    moved = incomparable(A, B)
    shared = [q for q in A if q in B and q not in moved]
    fixed = sum(1 for q in shared if A[q]["passed"] is False and B[q]["passed"])
    broke = sum(1 for q in shared if A[q]["passed"] and B[q]["passed"] is False)
    return {"compared": len(shared), "fixed": fixed, "broke": broke, "set_aside": len(moved)}


def versions(rows: list[dict[str, Any]],
             served: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    """One row per package version with an answer run, highest first."""
    state = {v.get("id"): v for v in served or [] if isinstance(v, dict)}
    # An experiment is served beside the package, not as one of its versions.
    rows = [r for r in rows if not r.get("experiment")]
    answer_runs = [r for r in rows if r["kind"] == "run" and r.get("status") == "complete"]
    perf: dict[str, dict[str, Any]] = {}
    for r in rows:                  # oldest first, so the newest replay wins
        # A replay in which no query ran says nothing about its version's speed.
        if (r["kind"] == "perf" and r.get("status") == "complete" and r["version"]
                and (r.get("errors") or 0) < (r.get("cases") or 0)):
            perf[r["version"]] = r
    by_version: dict[str, list[dict[str, Any]]] = {}
    for r in answer_runs:
        by_version.setdefault(r["version"] or WORKING_COPY, []).append(r)
    names = sorted((v for v in by_version if v != WORKING_COPY), key=version_key, reverse=True)
    names += [v for v in state if v not in by_version and v]
    names += [v for v in perf if v not in by_version]
    names = sorted(set(names), key=version_key, reverse=True)
    if WORKING_COPY in by_version:
        names.append(WORKING_COPY)
    out = []
    for v in names:
        rs = by_version.get(v, [])
        # Prefer a full run; else the largest. Newest wins a tie.
        best = max(rs, key=lambda r: (r.get("full", False), r.get("cases") or 0,
                                      r.get("started") or ""), default=None)
        s = state.get(v, {})
        out.append({
            "version": v,
            "latest": s.get("latest"),
            "archived": s.get("archiveStatus") == "archive" if s else None,
            "run": best["run"] if best else None,
            "path": best["path"] if best else None,
            "full": best.get("full") if best else None,
            "passed": best.get("passed") if best else None,
            "decided": best.get("decided") if best else None,
            "datasetVersion": best.get("datasetVersion") if best else None,
            "runs": len(rs),
            "perf": perf.get(v),
        })
    published = [r for r in out if r["version"] != WORKING_COPY and r["path"]]
    for newer, older in zip(published, published[1:]):
        newer["vs"] = older["version"]
        newer["flips"] = flips(pathlib.Path(older["path"]), pathlib.Path(newer["path"]))
    timed = [r for r in out if r.get("perf")]
    for newer, older in zip(timed, timed[1:]):
        newer["speed"] = speed(pathlib.Path(older["perf"]["path"]),
                               pathlib.Path(newer["perf"]["path"]))
        newer["speed_vs"] = older["version"]
    return out


def speed(a: pathlib.Path, b: pathlib.Path) -> dict[str, Any]:
    """Query-by-query comparison of two query replays, as `query_replay.compare` makes it."""
    from query_replay import compare
    band = json.loads((b / "run.json").read_text()).get("latencyBand") or 0.2
    c = compare(read_jsonl(a / "results.jsonl"), read_jsonl(b / "results.jsonl"), band)
    return {"faster": len(c["faster"]), "slower": len(c["slower"]),
            "rows_changed": len(c["rows_changed"]) + len(c["broke"]),
            "compared": c["compared"], "total_ratio": c["total_ratio"]}


def fmt_runs(rows: list[dict[str, Any]]) -> list[str]:
    lines = [f"{'run':<34} {'kind':<7} {'version':<14} {'set':>4}  score"]
    for r in rows:
        kind = "full" if r.get("full") else r["kind"]
        if r["kind"] == "perf":
            score = (f"p50 {r['p50_ms']} ms, total {r['total_ms']} ms"
                     + (f", {r['slow']} slow" if r.get("slow") else ""))
        elif r["kind"] == "replay":
            rec = r.get("recall_exact")
            score = f"recall {rec:.2f}" if isinstance(rec, (int, float)) else "recall -"
        else:
            score = f"{r.get('passed')}/{r.get('decided')} passed"
        if r.get("status") not in (None, "complete"):
            score += f" ({r['status']})"
        if r.get("slice"):
            score += f"  slice {r['slice']}"
        lines.append(f"{r['run'][:34]:<34} {kind:<7} {str(r['version'] or WORKING_COPY)[:14]:<14} "
                     f"{str(r.get('datasetVersion') or '-'):>4}  {score}")
    return lines


def fmt_versions(rows: list[dict[str, Any]]) -> list[str]:
    lines = [f"{'version':<20} {'state':<9} {'run':<30} {'set':>4}  score / flips"]
    for r in rows:
        state = ("latest" if r["latest"] else "archived" if r["archived"]
                 else "" if r["latest"] is None else "-")
        if not r["run"]:
            lines.append(f"{r['version'][:20]:<20} {state:<9} {'(no answer run)':<30}")
            lines += fmt_speed(r)
            continue
        score = f"{r['passed']}/{r['decided']} passed" + ("" if r["full"] else " (not full)")
        if r.get("flips"):
            f = r["flips"]
            score += (f"; vs {r['vs']}: {f['fixed']} fixed, {f['broke']} broke"
                      f" over {f['compared']}"
                      + (f", {f['set_aside']} set aside" if f["set_aside"] else ""))
        lines.append(f"{r['version'][:20]:<20} {state:<9} {r['run'][:30]:<30} "
                     f"{str(r.get('datasetVersion') or '-'):>4}  {score}")
        lines += fmt_speed(r)
    return lines


def fmt_speed(r: dict[str, Any]) -> list[str]:
    """The version's query replay, on the line under its score."""
    p = r.get("perf")
    if not p:
        return []
    text = f"{'':<14} {'':<9} {p['run'][:30]:<30} {'':>4}  p50 {p['p50_ms']} ms, total {p['total_ms']} ms"
    s = r.get("speed")
    if s:
        text += (f"; vs {r['speed_vs']}: {s['faster']} faster, {s['slower']} slower"
                 + (f", {s['rows_changed']} ROWS CHANGED" if s["rows_changed"] else "")
                 + (f", total x{s['total_ratio']}" if s["total_ratio"] else ""))
    return [text]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("view", choices=("runs", "versions"))
    ap.add_argument("--set", dest="set_dir", type=pathlib.Path, required=True)
    ap.add_argument("--publisher", default=None,
                    help="REST URL to read each version's latest/archived state from; "
                         "default: [model] in eval.toml")
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args(argv)
    cfg = config.load(a.set_dir)
    rows = set_runs(cfg)
    if a.view == "runs":
        print(json.dumps(rows, indent=1) if a.json else "\n".join(fmt_runs(rows)))
        return 0
    served = []
    env, pkg = cfg.get("model", "environment"), cfg.get("model", "package")
    if env and pkg:
        served = package_versions(a.publisher or cfg.model_publisher(), env, pkg)
    vrows = versions(rows, served)
    print(json.dumps(vrows, indent=1) if a.json else "\n".join(fmt_versions(vrows)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
