#!/usr/bin/env python3
"""Predict what a table would do for the workload, before building it. Stdlib only.

  python3 eval.py simulate --set <set-dir> --from perf-01                    # options from the replay
  python3 eval.py simulate --set <set-dir> --from perf-01 --candidate <dir>  # one edited package, dry-run
  python3 eval.py simulate --set <set-dir> --check sim-01 --against perf-02  # prediction vs measurement

Step 4 of the optimization loop (reference/optimization-loop.md), for speed.
It reads a query replay (`--from`), whose results hold each query's time, how
often it ran, what answered it and its shape (the path report's grouped-by,
filtered-by and measures), and the speed objective in eval.toml's
[performance] section.

**Options from the replay** (no `--candidate`). For each source, it proposes
`#(rollup)` grains: the grain of each query still answered live, and the union
of them. A rollup at grain G holding measures M answers a query on the same
source when every field the query groups or filters by is in G and every
measure it uses is in M. That is the router's rule, so which queries a table
answers is close to exact; the router may still refuse a measure that cannot
be merged (a distinct count, an average), which only a dry run or a build
shows. The time of a query a table answers is not known before a build:
`table_read_ms` stands in for it. The build is assumed to cost about the
slowest live query on the same source, and to run often enough to keep the
data within `max_staleness_hours` (hourly when unset). Every assumption is
written into the output.

**A candidate** (`--candidate <package-dir>`): the edited package is published
as a dry run (`POST …/packages?dryRun=true`), which builds nothing. Its build
plan names each table's grain and measures, and its serve verdicts say
whether each would serve. A table whose verdict is `live` answers nothing.

**A check** (`--check <sim-label> --against <replay>`): step 7. Each predicted
query is compared with what the replay measured: which table answered it, and
its time. The gaps are what to correct: `table_read_ms`, the build estimate,
or the coverage rule.

Writes `<workdir>/simulations/<label>/`: `options.json` (the options, every
prediction and the assumptions), `options.csv` and `predictions.csv` (for the
options app), and prints the options against the objective.
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import pathlib
import sys
import urllib.error
import urllib.request
from typing import Any

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent.parent / "eval-answer" / "scripts"))
sys.path.insert(0, str(HERE))

import config  # noqa: E402
import ledger  # noqa: E402

OBJECTIVES = ("weighted_ms", "p95_ms")
MAX_GRAIN = 6
OPTION_COLUMNS = ["option", "kind", "source", "grain", "measures", "tier", "covered",
                  "objective_before", "objective_after", "saved_ms_per_day", "build_ms",
                  "refreshes_per_day", "build_ms_per_day", "net_ms_per_day", "staleness_hours",
                  "usd_per_day", "feasible", "why_not"]
PREDICTION_COLUMNS = ["option", "qid", "source", "count", "before_ms", "predicted_ms", "covered",
                      "why_not"]


# ------------------------------------------------------------------ inputs

def read_replay(run: pathlib.Path) -> list[dict[str, Any]]:
    """The replay's timed queries, errors left out."""
    path = run / "results.jsonl"
    if not path.is_file():
        raise SystemExit(f"{run} has no results.jsonl: --from must name a query replay. "
                         f"Fix: eval.py query-replay --set <set-dir> --label perf-01")
    return [r for r in ledger.read_jsonl(path) if not r.get("error") and r.get("median_ms") is not None]


def objective(cfg: Any) -> dict[str, Any]:
    """The speed objective from [performance], with every default said."""
    get = lambda k: cfg.get("performance", k)  # noqa: E731
    obj = get("objective")
    if obj not in OBJECTIVES:
        raise SystemExit(f"Invalid eval.toml on 'performance.objective': expected one of "
                         f"{', '.join(OBJECTIVES)}, got {obj!r}. Fix: objective = \"weighted_ms\"")
    return {"objective": obj,
            "max_staleness_hours": get("max_staleness_hours"),
            "max_build_ms_per_day": get("max_build_ms_per_day"),
            "max_usd_per_day": get("max_usd_per_day"),
            "usd_per_compute_hour": get("usd_per_compute_hour"),
            "usd_per_tb_scanned": get("usd_per_tb_scanned"),
            "table_read_ms": get("table_read_ms")}


# ------------------------------------------------------------------ coverage

def dims(shape: dict[str, Any] | None) -> set[str]:
    return set((shape or {}).get("groupedBy") or []) | set((shape or {}).get("filteredBy") or [])


def covers(table: dict[str, Any], row: dict[str, Any]) -> tuple[bool, str | None]:
    """Would `table` answer this query? (yes, why not). The router's rule for a
    rollup: same source, grain holds every grouped and filtered field, and
    every measure is stored. A materialized source answers any query on it."""
    shape = row.get("shape")
    if not shape:
        return False, "no path report for this query, so its shape is unknown"
    if shape.get("sourceName") != table["source"]:
        return False, "another source"
    if table["kind"] == "materialized":
        return True, None
    missing = sorted(dims(shape) - set(table["grain"]))
    if missing:
        return False, f"GRAIN_TOO_COARSE: the grain lacks {', '.join(missing)}"
    absent = sorted(set(shape.get("measures") or []) - set(table["measures"]))
    if absent:
        return False, f"MISSING_FIELD: the table does not store {', '.join(absent)}"
    return True, None


def proposed_tables(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Rollup grains worth trying, from the queries still answered live: each
    query's own grain, and per source the union of them while it stays small.
    Each table holds the measures of the queries its grain covers."""
    live = [r for r in rows if (r.get("served_by") or {}).get("kind") in ("live", None)
            and r.get("shape")]
    by_source: dict[str, list[dict[str, Any]]] = {}
    for r in live:
        by_source.setdefault(r["shape"]["sourceName"], []).append(r)
    out, seen = [], set()
    for src, rs in sorted(by_source.items()):
        grains = [sorted(dims(r["shape"])) for r in rs]
        union = sorted(set().union(*map(set, grains)))
        if len(union) <= MAX_GRAIN:
            grains.append(union)
        for g in grains:
            key = (src, tuple(g))
            if key in seen:
                continue
            seen.add(key)
            measures = sorted({m for r in rs if dims(r["shape"]) <= set(g)
                               for m in r["shape"].get("measures") or []})
            if measures:
                out.append({"kind": "rollup", "source": src, "grain": g, "measures": measures,
                            "tier": "estimate"})
    return out


def dry_run_tables(base: str, environment: str, name: str,
                   location: pathlib.Path) -> tuple[list[dict[str, Any]], list[str]]:
    """The tables an edited package would build, from a dry-run publish: each
    with its grain, measures and serve verdict. Nothing is built. Returns
    (tables, the package's error warnings)."""
    url = f"{base.rstrip('/')}/api/v0/environments/{environment}/packages?dryRun=true"
    req = urllib.request.Request(url, data=json.dumps({"name": name, "location": str(location)}).encode(),
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=300) as r:
            d = json.loads(r.read())
    except urllib.error.HTTPError as e:
        # The package would not load: an annotation that cannot be honoured,
        # such as a rollup on a measure it cannot merge. Nothing would serve.
        body = e.read().decode()[:600]
        return [{"name": str(location.name), "kind": "refused", "source": "(package)",
                 "grain": [], "measures": [], "tier": "dry-run", "verdict": "refused",
                 "verdict_message": f"the package does not load (HTTP {e.code}): {body}"}], []
    verdicts = {v.get("sourceName"): v for v in d.get("serveVerdicts") or []}
    tables = []
    plan = d.get("buildPlan") or {}
    for s in (plan.get("sources") or {}).values():
        v = verdicts.get(s.get("name")) or {}
        t = {"name": s.get("name"), "tier": "dry-run", "verdict": v.get("verdict"),
             "verdict_message": v.get("message")}
        pre = s.get("preaggregate")
        if pre:
            t.update(kind="rollup", source=pre.get("baseSourceName"),
                     grain=list(pre.get("grainDimensions") or []),
                     measures=list(pre.get("measures") or []))
        else:
            t.update(kind="materialized", source=s.get("name"), grain=[], measures=[])
        tables.append(t)
    for name, why in (plan.get("refusedSources") or {}).items():
        said = (why.get("message") or why.get("reason") or json.dumps(why)) \
            if isinstance(why, dict) else str(why)
        reason = why.get("reason") if isinstance(why, dict) else None
        tables.append({"name": name, "kind": "refused", "source": str(name).split("@")[0],
                       "grain": [], "measures": [], "tier": "dry-run", "verdict": "refused",
                       "verdict_message": f"{str(name).split('@')[0]} is refused"
                                          + (f" ({reason})" if reason else "") + f": {said}"})
    errors = [w.get("message") for w in d.get("warnings") or [] if w.get("severity") == "error"]
    return tables, errors


# ------------------------------------------------------------------ prediction

def p95(values: list[tuple[float, int]]) -> float | None:
    """The 95th percentile of query times, each query counted as often as it ran."""
    xs = sorted(values)
    total = sum(c for _, c in xs)
    if not total:
        return None
    seen = 0
    for ms, c in xs:
        seen += c
        if seen >= 0.95 * total:
            return ms
    return xs[-1][0]


def score(rows: list[dict[str, Any]], tables: list[dict[str, Any]],
          obj: dict[str, Any], label: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """One option: these tables added. Its predicted metrics, and per query."""
    read_ms = obj["table_read_ms"]
    preds, before, after, saved = [], [], [], 0.0
    serving = [t for t in tables if t.get("verdict") not in ("live", "refused")]
    for r in rows:
        count = r.get("count") or 1
        hit, why = False, None
        for t in serving:
            ok, w = covers(t, r)
            if ok:
                hit = True
                break
            why = why or (w if w != "another source" else None)
        if not serving and tables:
            why = "; ".join(f"{t.get('name')}: {t.get('verdict_message') or t.get('verdict')}"
                            for t in tables)
        already = (r.get("served_by") or {}).get("kind") not in ("live", None)
        ms = r["median_ms"]
        new = min(ms, read_ms) if hit and not already else ms
        saved += (ms - new) * count
        before.append((ms, count))
        after.append((new, count))
        preds.append({"option": label, "qid": r["qid"], "source": r.get("source"), "count": count,
                      "before_ms": ms, "predicted_ms": round(new, 1), "covered": hit or already,
                      "why_not": None if hit or already else why})

    def objective_value(xs: list[tuple[float, int]]) -> float | None:
        if obj["objective"] == "p95_ms":
            return p95(xs)
        return round(sum(ms * c for ms, c in xs), 1)

    # The build: about one pass over the base, as the slowest live query on it.
    build = 0.0
    for t in serving:
        on = [r["median_ms"] for r in rows if (r.get("shape") or {}).get("sourceName") == t["source"]]
        build += max(on) if on else 0.0
    stale = obj["max_staleness_hours"]
    per_day = math.ceil(24 / stale) if stale else 24
    build_day = round(build * per_day, 1)
    usd = None
    if obj["usd_per_compute_hour"]:
        usd = round((build_day - saved) / 3_600_000 * obj["usd_per_compute_hour"], 4)
    why_not = []
    if obj["max_build_ms_per_day"] and build_day > obj["max_build_ms_per_day"]:
        why_not.append(f"builds {build_day:g} ms a day, over max_build_ms_per_day")
    if obj["max_usd_per_day"] and usd is not None and usd > obj["max_usd_per_day"]:
        why_not.append(f"costs ${usd:g} a day, over max_usd_per_day")
    if tables and not serving:
        why_not.append("no table would serve: " + "; ".join(
            f"{t.get('verdict_message') or t.get('verdict')}" for t in tables)[:500])
    option = {
        "option": label,
        "kind": ",".join(sorted({t["kind"] for t in tables})) or "none",
        "source": ",".join(sorted({t["source"] for t in tables})),
        "grain": "; ".join(",".join(t["grain"]) for t in tables if t["kind"] == "rollup"),
        "measures": "; ".join(",".join(t["measures"]) for t in tables if t["kind"] == "rollup"),
        "tier": ",".join(sorted({t["tier"] for t in tables})) or "baseline",
        "tables": tables,
        "covered": sum(1 for p in preds if p["covered"]),
        "objective_before": objective_value(before),
        "objective_after": objective_value(after),
        "saved_ms_per_day": round(saved, 1),
        "build_ms": round(build, 1),
        "refreshes_per_day": per_day,
        "build_ms_per_day": build_day,
        "net_ms_per_day": round(saved - build_day, 1),
        "staleness_hours": round(24 / per_day, 2),
        "usd_per_day": usd,
        "feasible": not why_not,
        "why_not": "; ".join(why_not) or None,
    }
    return option, preds


def options(rows: list[dict[str, Any]], tables: list[dict[str, Any]],
            obj: dict[str, Any], candidate: bool) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Each table alone, every table together, and the baseline, ranked by the
    objective (lowest first), feasible options before the rest."""
    sets = [("baseline", [])]
    if candidate:
        sets.append(("candidate", tables))
    else:
        sets += [(f"rollup-{t['source']}-{'-'.join(t['grain']) or 'total'}", [t]) for t in tables]
        if len(tables) > 1:
            sets.append(("all", tables))
    got = [score(rows, ts, obj, name) for name, ts in sets]
    opts = [o for o, _ in got]
    preds = [p for _, ps in got for p in ps]
    opts.sort(key=lambda o: (not o["feasible"], o["objective_after"] if o["objective_after"]
                             is not None else math.inf, -o["net_ms_per_day"], o["option"]))
    return opts, preds


# ------------------------------------------------------------------ check (learn)

def check(sim: dict[str, Any], replay_rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Each prediction of the simulation's first non-baseline option against a
    replay of the built candidate: did a table answer it, and how close was the
    predicted time."""
    opt = next((o for o in sim["options"] if o["option"] != "baseline"), None)
    if not opt:
        raise SystemExit("the simulation has no option to check")
    measured = {r["qid"]: r for r in replay_rows}
    out = []
    for p in sim["predictions"]:
        if p["option"] != opt["option"] or p["qid"] not in measured:
            continue
        m = measured[p["qid"]]
        kind = (m.get("served_by") or {}).get("kind")
        out.append({"qid": p["qid"], "predicted_covered": p["covered"],
                    "answered_by_table": kind not in ("live", None),
                    "predicted_ms": p["predicted_ms"], "measured_ms": m["median_ms"],
                    "error_ms": round(m["median_ms"] - p["predicted_ms"], 1)})
    wrong = [c for c in out if c["predicted_covered"] != c["answered_by_table"]]
    hits = [c for c in out if c["answered_by_table"]]
    read = sorted(c["measured_ms"] for c in hits)
    return {"option": opt["option"], "queries": len(out), "coverage_wrong": wrong,
            "measured_table_read_ms": read[len(read) // 2] if read else None,
            "assumed_table_read_ms": sim["objective"]["table_read_ms"], "per_query": out}


# ------------------------------------------------------------------ output

def write(dir_: pathlib.Path, sim: dict[str, Any]) -> None:
    dir_.mkdir(parents=True, exist_ok=True)
    (dir_ / "options.json").write_text(json.dumps(sim, indent=1, default=str))
    for name, cols, rows in (("options.csv", OPTION_COLUMNS, sim["options"]),
                             ("predictions.csv", PREDICTION_COLUMNS, sim["predictions"])):
        with (dir_ / name).open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
            w.writeheader()
            w.writerows(rows)


def report(sim: dict[str, Any]) -> str:
    o = sim["objective"]
    lines = [f"objective: lower {o['objective']}"
             + (f", data at most {o['max_staleness_hours']:g} h old" if o["max_staleness_hours"] else "")
             + (f", builds at most {o['max_build_ms_per_day']:g} ms a day" if o["max_build_ms_per_day"] else "")
             + (f", at most ${o['max_usd_per_day']:g} a day" if o["max_usd_per_day"] else ""), ""]
    for x in sim["options"]:
        usd = "" if x["usd_per_day"] is None else f", ${x['usd_per_day']:g}/day"
        lines.append(f"  {'ok ' if x['feasible'] else 'NO '} {x['option']:<44} "
                     f"{o['objective']} {x['objective_before']} -> {x['objective_after']}, "
                     f"{x['covered']} queries answered by a table, build {x['build_ms_per_day']:g} "
                     f"ms/day, net {x['net_ms_per_day']:g} ms/day{usd}"
                     + (f"  ({x['why_not']})" if x["why_not"] else ""))
    lines += ["", "assumptions:"] + [f"  - {a}" for a in sim["assumptions"]]
    return "\n".join(lines)


def assumptions(obj: dict[str, Any], tier: str) -> list[str]:
    out = [f"a query a new table answers takes table_read_ms = {obj['table_read_ms']:g} ms "
           f"(measured once built; check with --check)",
           "a build costs about the slowest live query on the same source",
           "each query's count is how often it runs in a day",
           ("tables refresh every " + (f"{24 / math.ceil(24 / obj['max_staleness_hours']):g} h "
                                       "to meet max_staleness_hours" if obj["max_staleness_hours"]
                                       else "hour (no max_staleness_hours set)")),
           "storage is not priced: the server reports no table size here"]
    if tier == "estimate":
        out.append("which queries a rollup answers follows the router's grain rule; a measure "
                   "the router cannot merge (a distinct count, an average) is refused only at "
                   "a dry run or build")
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--set", dest="set_dir", required=True)
    ap.add_argument("--from", dest="from_run", help="the query replay to simulate against")
    ap.add_argument("--candidate", type=pathlib.Path,
                    help="an edited package directory, dry-run published to read its tables")
    ap.add_argument("--check", help="a simulation's label, to compare with --against")
    ap.add_argument("--against", help="with --check: a replay of the built candidate")
    ap.add_argument("--label", default=None)
    a = ap.parse_args(argv)
    cfg = config.load(pathlib.Path(a.set_dir))
    runs, sims = cfg.workdir() / "runs", cfg.workdir() / "simulations"
    if a.check:
        if not a.against:
            raise SystemExit("--check needs --against <replay label>")
        sim = json.loads((sims / a.check / "options.json").read_text())
        got = check(sim, read_replay(runs / a.against))
        (sims / a.check / f"check-{a.against}.json").write_text(json.dumps(got, indent=1))
        print(f"{got['queries']} predictions checked against {a.against}: "
              f"{len(got['coverage_wrong'])} wrong about which table answers; a table answered in "
              f"{got['measured_table_read_ms']} ms (assumed {got['assumed_table_read_ms']:g})")
        for c in got["coverage_wrong"]:
            print(f"  {c['qid']}: predicted {'a table' if c['predicted_covered'] else 'live'}, "
                  f"measured {'a table' if c['answered_by_table'] else 'live'}")
        return 0 if not got["coverage_wrong"] else 1
    if not a.from_run:
        raise SystemExit("--from <replay label> is required")
    obj = objective(cfg)
    rows = read_replay(runs / a.from_run)
    errors: list[str] = []
    if a.candidate:
        tables, errors = dry_run_tables(cfg.model_publisher(), cfg.get("model", "environment"),
                                        f"{cfg.get('model', 'package')}-sim", a.candidate.resolve())
    else:
        tables = proposed_tables(rows)
    tier = "dry-run" if a.candidate else "estimate"
    opts, preds = options(rows, tables, obj, bool(a.candidate))
    label = a.label or f"sim-{a.from_run}"
    sim = {"label": label, "from": a.from_run, "candidate": str(a.candidate) if a.candidate else None,
           "tier": tier, "objective": obj, "assumptions": assumptions(obj, tier),
           "packageErrors": errors, "options": opts, "predictions": preds}
    write(sims / label, sim)
    print(report(sim))
    for e in errors:
        print(f"  ! the candidate package reports: {e}")
    print(f"\n{sims / label / 'options.json'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
