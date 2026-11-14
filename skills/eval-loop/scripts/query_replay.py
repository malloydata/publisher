#!/usr/bin/env python3
"""Re-run a run's accepted queries against the served package, and time them. Stdlib only.

  python3 query_replay.py --set <set-dir> --from <run> [--label <name>]
                          [--baseline <replay>] [--target-version 1.2.0]
                          [--repeats 5] [--slice <name> | --tag <t> | --only <qid>]
  # or: python3 eval.py query-replay --set <set-dir> --from baseline-01 --label perf-01

No answering agent and no judge run. The queries come from `--from`:

  an answer run   each case it scored a pass, with the query the answer rested
                  on (`final_query`, `final_givens`, `final_model_path`)
  a query replay  the same queries that replay ran, so two replays always
                  time the same work

or from `--queries <file>`, a workload file: one JSON object per line with
`query` and optionally `count`, `modelPath`, `givens`, `qid`. That is for the
queries people ran, from a host's logs. Each query's `count` weighs it in
`weighted_ms`, the total by how often each query ran.

Each query is sent over REST once as a warm-up and then `--repeats` times. The
warm-up is not counted: the first call after a load compiles the model.

PER QUERY it records the median and max of the server's `executionTimeMs`, the
median round trip, `servedFrom`, `queryCostBytes`, the source the query reads,
and `rows_sha`, a hash of the rows that ignores row order and float noise in the
last digits. The rows themselves go in rows/<qid>.json (first 500).

`--baseline <replay>` compares this replay with an earlier one, query by query:
any query whose rows changed is listed first and makes the exit status 1,
because a speed edit must not change an answer. Then each query is faster,
slower, or within the band (`latency_band` in eval.toml, default 20%; a change
under `--min-delta-ms` is noise however large its ratio). `flip_table.py` prints
the same comparison for any two replays.

THE MATERIALIZATION STATE is read before the queries run and kept in run.json
(`materialization`): what each built table cost (`builds`: rows, build time,
bytes scanned), the persist sources the build plan makes and refuses,
whether their tables are bound, and the newest build. A persisted source whose
table is not bound computes live, so a "before" number taken then is a number
for the wrong thing. The replay warns about it.

`--target-version` pins a published version with `versionId` on every request.
REST can pin a version, unlike the MCP tools, so `latest` does not have to move.

OUTPUT, in <workdir>/runs/<label>/ unless --out says otherwise:
  run.json       the pins, phase `query-replay`
  results.jsonl  one row per query
  rows/<qid>.json
  summary.json   p50, p95, total, slow queries, per source, and the comparison
"""
from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import re
import statistics
import sys
from typing import Any

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent.parent / "eval-answer" / "scripts"))
sys.path.insert(0, str(HERE))

import config  # noqa: E402
import ledger  # noqa: E402
import slices  # noqa: E402
import run_baseline as rb  # noqa: E402
from flip_table import verdicts  # noqa: E402
from publisher_rest import (materialization_state, package_version,  # noqa: E402
                            package_versions, timed_query)

PHASE = "query-replay"
ROWS_KEPT = 500
DEFAULT_MIN_DELTA_MS = 10.0


# --------------------------------------------------------------------------- queries

def source_of(query: str | None) -> str | None:
    """The served source a query reads, or None when it cannot be read off the text.

    The last `run: <name> ->` names it. A name the query defines itself
    (`source: oi is order_items extend {…}`) is followed back to the source it
    builds on, so a query that extends `order_items` groups with `order_items`.
    """
    runs = re.findall(r"(?m)^\s*run:\s*([A-Za-z_]\w*)\s*->", query or "")
    if not runs:
        return None
    local = dict(re.findall(r"(?m)^\s*source:\s*([A-Za-z_]\w*)\s+is\s+([A-Za-z_]\w*)\b(?!\s*\.)",
                            query or ""))
    name, seen = runs[-1], set()
    while name in local and name not in seen:
        seen.add(name)
        name = local[name]
    return name


def workload(path: pathlib.Path, default_model: str | None) -> list[dict[str, Any]]:
    """The queries of a workload file: what people actually ran, from a host's logs.

    One JSON object per line: `query` (Malloy), and optionally `modelPath`,
    `givens`, `count` (how many times it ran, default 1) and `qid`. The same
    query, givens and model on two lines is one query whose counts add. With no
    `qid` a query is named by a hash of those three, so the same query has the
    same name in tomorrow's file and the two replays compare.
    """
    merged: dict[str, dict[str, Any]] = {}
    for n, line in enumerate(path.read_text().splitlines(), 1):
        if not line.strip():
            continue
        try:
            r = json.loads(line)
        except ValueError:
            raise SystemExit(f"Invalid {path}:{n}: expected one JSON object per line, got "
                             f"{line[:60]!r}. Fix: write each query as "
                             f'{{"query": "run: ...", "count": 3}}') from None
        if not isinstance(r, dict) or not isinstance(r.get("query"), str) or not r["query"].strip():
            raise SystemExit(f"Invalid {path}:{n}: expected an object with a `query` string. "
                             f'Fix: {{"query": "run: orders -> by_month"}}')
        model = r.get("modelPath") or default_model
        key = json.dumps([r["query"], r.get("givens"), model], sort_keys=True)
        qid = r.get("qid") or "wl-" + hashlib.sha256(key.encode()).hexdigest()[:10]
        count = r.get("count", 1)
        if not isinstance(count, int) or isinstance(count, bool) or count < 1:
            raise SystemExit(f"Invalid count on {path}:{n}: expected a whole number of 1 or "
                             f"more, got {count!r}. Fix: leave it out for a query that ran once")
        if qid in merged:
            merged[qid]["count"] += count
        else:
            merged[qid] = {"qid": qid, "tags": r.get("tags") or [], "query": r["query"],
                           "givens": r.get("givens"), "modelPath": model, "count": count,
                           "source": source_of(r["query"])}
    return sorted(merged.values(), key=lambda q: q["qid"])


def queries_from(run: pathlib.Path, default_model: str | None) -> list[dict[str, Any]]:
    """The queries to replay, one per case, from an answer run or a query replay."""
    meta = json.loads((run / "run.json").read_text())
    if meta.get("phase") == PHASE:
        return [{**{k: r.get(k) for k in ("qid", "tags", "query", "givens", "modelPath")},
                 "count": r.get("count") or 1, "source": source_of(r.get("query"))}
                for r in ledger.read_jsonl(run / "results.jsonl")]
    paths: dict[str, Any] = {}
    for e in ledger.read_jsonl(run / "events.jsonl"):
        if e.get("kind") == "attempt":
            paths[e["qid"]] = e.get("final_model_path")
    out = []
    for qid, v in sorted(verdicts(run).items()):
        if v["passed"] and v.get("final_query"):
            out.append({"qid": qid, "query": v["final_query"], "givens": v.get("final_givens"),
                        "modelPath": paths.get(qid) or default_model,
                        "source": source_of(v["final_query"])})
    return out


# --------------------------------------------------------------------------- rows

def canon(v: Any) -> Any:
    """A value with float noise in the last digits removed. A persisted table
    can sum in another order and differ in the fifteenth digit, which is not a
    different answer."""
    if isinstance(v, bool) or v is None:
        return v
    if isinstance(v, float):
        return float(f"{v:.9g}")
    if isinstance(v, dict):
        return {str(k): canon(x) for k, x in v.items()}
    if isinstance(v, list):
        return [canon(x) for x in v]
    return v


def rows_sha(rows: list[Any]) -> str:
    """A hash of the rows that ignores their order."""
    lines = sorted(json.dumps(canon(r), sort_keys=True, default=str) for r in rows)
    return hashlib.sha256("\n".join(lines).encode()).hexdigest()[:16]


# --------------------------------------------------------------------------- timing

def time_one(q: dict[str, Any], a: argparse.Namespace, version: str | None) -> dict[str, Any]:
    """One query: a warm-up, then `a.repeats` timed calls."""
    calls = [timed_query(a.publisher, a.environment, a.package, q["modelPath"], q["query"],
                         timeout=a.timeout, givens=q.get("givens"), version=version)
             for _ in range(a.repeats + 1)]
    row: dict[str, Any] = {k: q.get(k) for k in ("qid", "tags", "query", "givens",
                                                 "modelPath", "source")}
    row["count"] = q.get("count") or 1
    errors = [c["error"] for c in calls if c["error"]]
    if errors:
        row.update(error=errors[0], rows_sha=None, n_rows=None, median_ms=None)
        return row
    timed = calls[1:]
    server = [c["executionTimeMs"] for c in timed if isinstance(c["executionTimeMs"], (int, float))]
    trip = [c["roundTripMs"] for c in timed]
    # An older server reports no executionTimeMs; the round trip stands in, and says so.
    ms = server if len(server) == len(timed) else trip
    shas = {rows_sha(c["rows"]) for c in calls}
    row.update(
        error=None,
        timing="server" if ms is server else "roundTrip",
        median_ms=round(statistics.median(ms), 1),
        max_ms=round(max(ms), 1),
        min_ms=round(min(ms), 1),
        round_trip_ms=round(statistics.median(trip), 1),
        served_from=sorted({c["servedFrom"] or "none" for c in timed}),
        query_cost_bytes=timed[-1]["queryCostBytes"],
        rows_sha=rows_sha(calls[-1]["rows"]),
        n_rows=len(calls[-1]["rows"]),
        # Different rows from the same query on the same model: the query is
        # not deterministic (a tie under a limit, an unordered nest), so a
        # changed hash against another replay is not evidence on its own.
        unstable_rows=len(shas) > 1,
    )
    row["rows"] = calls[-1]["rows"][:ROWS_KEPT]
    return row


def percentile(xs: list[float], p: float) -> float | None:
    if not xs:
        return None
    xs = sorted(xs)
    k = (len(xs) - 1) * p
    lo, hi = int(k), min(int(k) + 1, len(xs) - 1)
    return round(xs[lo] + (xs[hi] - xs[lo]) * (k - lo), 1)


def summarize(rows: list[dict[str, Any]], budget_ms: int | None) -> dict[str, Any]:
    ok = [r for r in rows if not r.get("error")]
    med = [r["median_ms"] for r in ok]
    by_source: dict[str, dict[str, Any]] = {}
    for r in ok:
        s = by_source.setdefault(r.get("source") or "(other)", {"queries": 0, "total_ms": 0.0,
                                                               "slow": 0})
        s["queries"] += 1
        s["total_ms"] = round(s["total_ms"] + r["median_ms"], 1)
        # What the source costs the people using it: each query's time by how
        # often it ran. Equal to total_ms when no query carries a count.
        s["weighted_ms"] = round(s.get("weighted_ms", 0.0) + r["median_ms"] * (r.get("count") or 1), 1)
        s["slow"] += bool(r.get("slow"))
    return {
        "queries": len(rows),
        "errors": len(rows) - len(ok),
        "p50_ms": percentile(med, 0.5),
        "p95_ms": percentile(med, 0.95),
        "max_ms": max(med) if med else None,
        "total_ms": round(sum(med), 1) if med else None,
        "weighted_ms": round(sum(r["median_ms"] * (r.get("count") or 1) for r in ok), 1)
        if ok else None,
        "budget_ms": budget_ms,
        "slow": sorted(r["qid"] for r in ok if r.get("slow")),
        "served_from_storage": sum(1 for r in ok if "storage" in r["served_from"]),
        "live_fallback": sorted(r["qid"] for r in ok if "live_fallback" in r["served_from"]),
        "unstable_rows": sorted(r["qid"] for r in ok if r.get("unstable_rows")),
        "by_source": dict(sorted(by_source.items(), key=lambda kv: -kv[1]["total_ms"])),
    }


def build_summary(builds: list[dict[str, Any]] | None) -> dict[str, Any] | None:
    """What the tables these queries read cost to build, summed. None when the
    package has no built table.

    This is the other side of a speed edit: a persisted source makes reads
    faster and costs a build every refresh. A total is null when no table
    reported that measure, and `unreported` counts the tables that did not, so
    a sum over some of them is never read as the whole.
    """
    if not builds:
        return None

    def total(key: str) -> tuple[float | None, int]:
        got = [b[key] for b in builds if b.get(key) is not None]
        return (sum(got) if got else None), len(builds) - len(got)
    ms, no_ms = total("buildMs")
    by, no_by = total("scannedBytes")
    rows, _ = total("rows")
    return {"tables": len(builds), "build_ms": ms, "scanned_bytes": by, "rows": rows,
            "unreported": {"build_ms": no_ms, "scanned_bytes": no_by},
            "by_source": {str(b["source"]): {"build_ms": b.get("buildMs"),
                                             "scanned_bytes": b.get("scannedBytes"),
                                             "rows": b.get("rows")} for b in builds}}


def fmt_build(b: dict[str, Any] | None, before: dict[str, Any] | None = None) -> list[str]:
    """The build line of a replay's report, with the baseline's beside it."""
    if not b and not before:
        return []

    def said(x: dict[str, Any] | None) -> str:
        if not x:
            return "no built table"
        ms = "not reported" if x["build_ms"] is None else f"{x['build_ms']:g} ms"
        by = "" if x["scanned_bytes"] is None else f", {x['scanned_bytes']:g} bytes scanned"
        part = x["unreported"]["build_ms"]
        return (f"{x['tables']} table(s), {ms} to build{by}"
                + (f" ({part} did not report a build time)" if part and x["build_ms"] is not None
                   else ""))
    return [f"  build cost: {said(b)}" + (f"   (baseline: {said(before)})"
                                         if before is not None or b is None else "")]


# --------------------------------------------------------------------------- comparison

def compare(old: list[dict[str, Any]], new: list[dict[str, Any]], band: float,
            min_delta_ms: float = DEFAULT_MIN_DELTA_MS) -> dict[str, Any]:
    """Query-by-query comparison of two replays.

    Only a query with the same text and givens in both is compared. Rows first:
    a query whose rows changed, or that ran before and errors now, blocks. Speed
    second, only over queries whose rows held: faster or slower when the median
    moved by more than `band` of the old median AND by more than `min_delta_ms`.
    """
    A = {r["qid"]: r for r in old}
    B = {r["qid"]: r for r in new}
    out: dict[str, Any] = {"compared": 0, "rows_changed": [], "broke": [], "fixed": [],
                           "query_changed": [], "unstable": [], "faster": [], "slower": [],
                           "within_band": 0, "band": band, "min_delta_ms": min_delta_ms}
    tot_a = tot_b = 0.0
    for qid in sorted(set(A) & set(B)):
        a, b = A[qid], B[qid]
        if (a.get("query"), a.get("givens")) != (b.get("query"), b.get("givens")):
            out["query_changed"].append(qid)
            continue
        if a.get("error") and b.get("error"):
            continue
        if b.get("error"):
            out["broke"].append(qid)
            continue
        if a.get("error"):
            out["fixed"].append(qid)
            continue
        out["compared"] += 1
        if a["rows_sha"] != b["rows_sha"]:
            if a.get("unstable_rows") or b.get("unstable_rows"):
                out["unstable"].append(qid)
            else:
                out["rows_changed"].append(qid)
            continue
        tot_a += a["median_ms"]
        tot_b += b["median_ms"]
        delta = b["median_ms"] - a["median_ms"]
        ratio = b["median_ms"] / a["median_ms"] if a["median_ms"] else None
        entry = {"qid": qid, "before_ms": a["median_ms"], "after_ms": b["median_ms"],
                 "ratio": round(ratio, 3) if ratio is not None else None}
        if abs(delta) <= min_delta_ms or ratio is None or abs(ratio - 1) <= band:
            out["within_band"] += 1
        elif delta < 0:
            out["faster"].append(entry)
        else:
            out["slower"].append(entry)
    out["total_before_ms"] = round(tot_a, 1)
    out["total_after_ms"] = round(tot_b, 1)
    out["total_ratio"] = round(tot_b / tot_a, 3) if tot_a else None
    out["blocked"] = bool(out["rows_changed"] or out["broke"])
    return out


def fmt_compare(c: dict[str, Any], before: str, after: str) -> list[str]:
    lines = [f"\nspeed: {after} vs {before}, over {c['compared']} queries with the same text "
             f"(band {c['band']:.0%}, ignoring changes under {c['min_delta_ms']:g} ms)"]
    if c["rows_changed"] or c["broke"]:
        lines.append("  BLOCKED: a speed change must not change an answer.")
        for q in c["rows_changed"]:
            lines.append(f"    rows changed  {q}")
        for q in c["broke"]:
            lines.append(f"    now errors    {q}")
        lines.append("  Judge each changed query against its golden before accepting anything.")
    for q in c["unstable"]:
        lines.append(f"  rows differ, but the query is not deterministic: {q}")
    if c["query_changed"]:
        lines.append(f"  not compared, query text changed: {', '.join(c['query_changed'])}")
    for label, xs in (("faster", c["faster"]), ("slower", c["slower"])):
        for e in xs:
            lines.append(f"  {label:7s} {e['qid'][:44]:44s} {e['before_ms']:>9.1f} -> "
                         f"{e['after_ms']:>9.1f} ms  x{e['ratio']}")
    timed = len(c["faster"]) + len(c["slower"]) + c["within_band"]
    if not timed:
        lines.append("  no query kept its rows and ran in both, so speed was not compared")
        return lines
    lines.append(f"  {len(c['faster'])} faster, {len(c['slower'])} slower, "
                 f"{c['within_band']} within the band; total {c['total_before_ms']} -> "
                 f"{c['total_after_ms']} ms" + (f" (x{c['total_ratio']})" if c["total_ratio"] else ""))
    return lines


# --------------------------------------------------------------------------- run

def resolve(a: argparse.Namespace) -> config.Config:
    cfg = config.load(a.set_dir)
    a.environment = cfg.need(a.environment, "model", "environment", "--environment")
    a.package = cfg.need(a.package, "model", "package", "--package")
    a.publisher = a.publisher or cfg.model_publisher()
    a.repeats = a.repeats or cfg.get("performance", "repeats")
    a.band = a.latency_band or cfg.get("performance", "latency_band")
    a.budget_ms = a.budget_ms or cfg.get("performance", "budget_ms")
    runs = cfg.workdir() / "runs"
    if bool(a.from_run) == bool(a.queries):
        raise SystemExit("Invalid arguments: expected exactly one of --from <run> and "
                         "--queries <file>. Fix: --from for an eval run's or a replay's "
                         "queries, --queries for a file of logged ones")
    if a.from_run:
        a.from_run = run_path(runs, a.from_run, "--from")
    elif not a.queries.is_file():
        raise SystemExit(f"no workload file at {a.queries}")
    if a.baseline:
        a.baseline = run_path(runs, a.baseline, "--baseline")
    if a.out is None:
        a.label = a.label or rb.next_run_label(runs / "_", cfg.set_name, "perf")
        a.out = runs / a.label
    a.label = a.label or a.out.name
    return cfg


def run_path(runs: pathlib.Path, name: str, flag: str) -> pathlib.Path:
    """A run label under the set's workdir, or a path to a run directory."""
    p = pathlib.Path(name).expanduser()
    p = p if p.is_dir() else runs / name
    if not (p / "run.json").is_file():
        raise SystemExit(f"Invalid {flag} {name!r}: expected a run label under {runs} or a run "
                         f"directory holding run.json. Fix: python3 eval.py runs --set <set-dir>")
    return p


def pin_version(a: argparse.Namespace) -> dict[str, Any] | None:
    """The version every request names. REST pins it, so nothing has to be `latest`."""
    pv = package_version(a.publisher, a.environment, a.package)
    if not a.target_version:
        if not pv.get("id"):
            # An unversioned package: record the version its publisher.json
            # names, as an answer run does, so `versions` can tell edits apart.
            base = rb.base_version(rb.served_package_dir(a.publisher, a.environment, a.package))
            return ({"id": None, "contentHash": pv.get("contentHash"), "latest": None,
                     "served": "publisher", "baseVersion": base} if base else None)
        return {"id": pv["id"], "contentHash": pv.get("contentHash"), "latest": True,
                "served": "publisher", "pinned": True}
    held = {v.get("id"): v for v in package_versions(a.publisher, a.environment, a.package)}
    if a.target_version not in held:
        raise SystemExit(
            f"Invalid --target-version on '{a.package}': expected a published version, got "
            f"{a.target_version}; the server holds {', '.join(sorted(filter(None, held))) or 'none'}. "
            f"Fix: publish it (POST /api/v0/environments/{a.environment}/packages), or pass one "
            f"of those.")
    v = held[a.target_version]
    return {"id": a.target_version, "contentHash": v.get("contentHash"),
            "latest": bool(v.get("latest")), "served": "publisher", "pinned": True}


def replay(a: argparse.Namespace, cfg: config.Config) -> int:
    held = [f for f in ("run.json", "results.jsonl") if (a.out / f).exists()]
    if held:
        raise SystemExit(f"{a.out} already holds a run ({', '.join(held)}). "
                         "Fix: pass a new --label or --out.")
    if a.queries:
        default_model = a.model_path or cfg.set_meta.get("targetModelPath")
        queries = workload(a.queries, default_model)
        origin = a.queries
    else:
        src_meta = json.loads((a.from_run / "run.json").read_text())
        default_model = (a.model_path or src_meta.get("modelPath")
                         or cfg.set_meta.get("targetModelPath"))
        queries = queries_from(a.from_run, default_model)
        origin = a.from_run
    picked = None
    if a.slice or a.tag:
        picked = slices.narrow(a, a.set_dir, ledger.read_jsonl(a.set_dir / "cases.jsonl"),
                               joined=False)
    if a.only:
        queries = [q for q in queries if q["qid"] in set(a.only)]
    if not queries:
        raise SystemExit(f"{origin}: no passing case with a query to replay"
                         + (" in this slice." if a.only else "."))
    missing = [q["qid"] for q in queries if not q.get("modelPath")]
    if missing:
        raise SystemExit(f"no model path for {', '.join(missing)}. Fix: --model-path <file>.malloy")
    tags = {c.get("qid"): c.get("tags") or [] for c in ledger.read_jsonl(a.set_dir / "cases.jsonl")}
    version = pin_version(a)
    vid = version["id"] if version else None
    mat = materialization_state(a.publisher, a.environment, a.package, vid)
    if mat.get("persistSources") and not mat.get("bound"):
        print(f"! {len(mat['persistSources'])} persisted source(s) and no bound table: every one "
              "computes live. Build first (malloy-pub materialize --wait) unless live is what "
              "you mean to measure.", flush=True)
    a.out.mkdir(parents=True, exist_ok=True)
    started = ledger.now()
    rows = []
    for i, q in enumerate(queries, 1):
        q["tags"] = tags.get(q["qid"], q.get("tags") or [])
        row = time_one(q, a, vid)
        row["slow"] = bool(a.budget_ms and row.get("median_ms") is not None
                           and row["median_ms"] > a.budget_ms)
        rows.append(row)
        print(f"[{a.label}] {i}/{len(queries)} {q['qid'][:40]:40s} "
              + (f"ERROR {row['error'][:60]}" if row.get("error")
                 else f"{row['median_ms']:>9.1f} ms" + ("  SLOW" if row["slow"] else "")),
              flush=True)

    summary = summarize(rows, a.budget_ms)
    # A replay in which no query ran timed nothing. Marked complete, it became
    # the record the next replay compared with, and every query read as fixed.
    status = "aborted" if summary["errors"] == summary["queries"] else "complete"
    summary["build"] = build_summary(mat.get("builds"))
    if a.baseline:
        base_rows = ledger.read_jsonl(a.baseline / "results.jsonl")
        summary["compare"] = {"baseline": a.baseline.name,
                              **compare(base_rows, rows, a.band, a.min_delta_ms)}
        try:
            before = json.loads((a.baseline / "summary.json").read_text()).get("build")
        except (OSError, ValueError):
            before = None
        summary["compare"]["build_before"] = before
    pins: dict[str, Any] = {
        "runId": a.out.name, "label": a.label, "target": "local", "answererModel": None,
        "phase": PHASE, "started": started, "status": status,
        "packageVersion": version, "fromRun": origin.name,
        "setName": cfg.set_meta.get("name") or cfg.set_name,
        "datasetVersion": cfg.set_meta.get("datasetVersion"),
        "datasetSha": ledger.dataset_sha(a.set_dir),
        "environment": a.environment, "package": a.package, "modelPath": default_model,
        "publisher": a.publisher, "repeats": a.repeats, "budgetMs": a.budget_ms,
        "latencyBand": a.band, "materialization": mat,
        "harnessVersion": ledger.skills_git_sha(),
    }
    if picked:
        pins["slice"] = picked
    (a.out / "run.json").write_text(json.dumps(ledger.run_config(**pins), indent=1) + "\n")
    (a.out / "rows").mkdir(exist_ok=True)
    for r in rows:
        (a.out / "rows" / f"{r['qid']}.json").write_text(json.dumps(r.pop("rows", []), indent=1,
                                                                    default=str))
    ledger.write_events(a.out / "results.jsonl", rows)
    (a.out / "summary.json").write_text(json.dumps(summary, indent=1) + "\n")

    s = summary
    base = (version or {}).get("baseVersion")
    shown = vid or (f"{base} (unpublished)" if base else "working copy")
    print(f"\nquery replay of {origin.name}: {s['queries']} queries, {s['errors']} errors, "
          f"version {shown}")
    print(f"p50 {s['p50_ms']} ms   p95 {s['p95_ms']} ms   max {s['max_ms']} ms   "
          f"total {s['total_ms']} ms" + (f"   over {a.budget_ms} ms: {len(s['slow'])}"
                                         if a.budget_ms else ""))
    for src, v in list(s["by_source"].items())[:8]:
        print(f"  source {src:30s} {v['queries']:3d} queries  {v['total_ms']:>10.1f} ms"
              + (f"  {v['slow']} slow" if v["slow"] else ""))
    if s["live_fallback"]:
        print(f"  live_fallback (the stored table was not used): {', '.join(s['live_fallback'])}")
    if "compare" in s:
        print("\n".join(fmt_compare(s["compare"], a.baseline.name, a.label)))
    print("\n".join(fmt_build(s["build"], s.get("compare", {}).get("build_before")
                              if "compare" in s else None)))
    print(f"\nrun directory: {a.out}")
    if status == "aborted":
        print("! every query errored, so nothing was timed. The run is marked aborted: it is "
              "not a record and is not shared. Fix the first error above and replay again.")
        return 1
    return 1 if s.get("compare", {}).get("blocked") else 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--set", dest="set_dir", required=True, type=pathlib.Path)
    ap.add_argument("--from", dest="from_run", default=None,
                    help="the run whose queries to replay: an answer run (its passing cases) "
                         "or another query replay (the same queries). A label or a directory")
    ap.add_argument("--queries", type=pathlib.Path, default=None,
                    help="a workload file in place of --from: one JSON object per line with "
                         "`query`, and optionally `modelPath`, `givens`, `count` and `qid`. "
                         "For the queries people ran, taken from a host's logs")
    ap.add_argument("--baseline", default=None,
                    help="an earlier query replay to compare with, query by query")
    ap.add_argument("--label", default=None,
                    help="the run's name under <workdir>/runs. Default: <set>-perf-<nn>")
    ap.add_argument("--out", type=pathlib.Path, default=None)
    ap.add_argument("--environment", default=None)
    ap.add_argument("--package", default=None)
    ap.add_argument("--publisher", default=None, help="default: [model] in eval.toml")
    ap.add_argument("--model-path", default=None,
                    help="for a query with no recorded model path; default: the run's modelPath")
    ap.add_argument("--target-version", default=None,
                    help="a published version to pin on every request; default: latest")
    ap.add_argument("--repeats", type=int, default=None,
                    help="timed calls per query after one warm-up; default [performance] repeats, 5")
    ap.add_argument("--budget-ms", type=int, default=None,
                    help="mark a query slower than this; default [performance] budget_ms")
    ap.add_argument("--latency-band", type=float, default=None,
                    help="a change under this fraction is noise; default [performance] "
                         "latency_band, 0.2")
    ap.add_argument("--min-delta-ms", type=float, default=DEFAULT_MIN_DELTA_MS,
                    help="a change under this many ms is noise whatever its ratio")
    ap.add_argument("--only", action="append", default=None, metavar="QID")
    slices.add_arguments(ap)
    ap.add_argument("--timeout", type=int, default=300, help="seconds per query call")
    a = ap.parse_args(argv)
    if a.repeats is not None and a.repeats < 1:
        raise SystemExit("Invalid --repeats: expected 1 or more. Fix: --repeats 5")
    try:
        cfg = resolve(a)
    except config.ConfigError as e:
        print(e, file=sys.stderr)
        return 3
    return replay(a, cfg)


if __name__ == "__main__":
    sys.exit(main())
