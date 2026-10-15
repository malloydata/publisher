#!/usr/bin/env python3
"""Replay a set's search targets against get_context and score the fields that come back. Stdlib only.

  python3 retrieval_replay.py --set <set-dir> [--label <name>] [--mcp-url URL]
                              [--mcp-header 'Name: value' ...]
                              [--set-version 1.2.0 --set-manifest <bundle>/manifest.json]
  # or: python3 eval.py replay --set <set-dir> --label <name>

No answering agent and no judge run. Each case that carries `searchTargets` is
sent as ONE get_context call, with one search target per entry, and the field
ids that come back are scored against the case's labels. Nothing else varies
between two runs, so a retrieval change is measured in minutes.

CASE FORMAT (a line of the set's cases.jsonl; `reference/retrieval-replay.md`)

  {"qid": "...", "question": "...", "tags": ["..."],
   "searchTargets": [{"target_type": "measure", "search_text": "total revenue",
                      "expectedEntities": {"required": ["measure:order_items:total_sales"]}}],
   "expectedEntities": {...}}                                     # optional

A case's labels are its own `expectedEntities` plus every target's, merged.
`required` ids, `requiredAnyOf` groups and `acceptable` ids mean what they mean
for an answer run (eval-answer's ledger-schema.md).

SCORING is `score_retrieval.score_case`, the code that scores an answer run, on
a `tool_call` event this script writes. So `precision` and the credited
`recall` are the same numbers an answer run reports. `recall_exact` counts a
need only when an id it names came back exactly; it is the headline, because
the credited `recall` also accepts the same field name under another source,
or a name mentioned in a returned source's docs.

OUTPUT, in <workdir>/runs/<label>/ unless --out says otherwise:
  run.json       the run's pins (ledger.run_config), phase `replay`
  events.jsonl   one `tool_call` event per case, in the shape an answer run writes
  results.jsonl  one scored row per case
  summary.json   pooled means, and the same means per case tag
"""
from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import sys
import time
import urllib.error
from typing import Any

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent.parent / "eval-answer" / "scripts"))
sys.path.insert(0, str(HERE))

import config  # noqa: E402
import ledger  # noqa: E402
import run_baseline as rb  # noqa: E402
from mcp_payload import (doc_tokens, entity_hits, entity_ids,  # noqa: E402
                         search_terms, target_shapes)
from score_retrieval import KINDS_BY_TARGET, groups, score_case  # noqa: E402

PHASE = "replay"
# The values get_context accepts for `target_type`. score_retrieval's table is
# pinned against the server's own by its test, so this list stays in step.
TARGET_TYPES = tuple(KINDS_BY_TARGET)
LABEL_KEYS = ("required", "requiredAnyOf", "acceptable")


# --------------------------------------------------------------------------- inputs

def parse_header(text: str) -> tuple[str, str]:
    """`Name: value` -> (name, value)."""
    name, sep, value = text.partition(":")
    if not sep or not name.strip() or not value.strip():
        raise SystemExit(f"Invalid --mcp-header {text!r}: expected 'Name: value'. "
                         "Fix: --mcp-header 'Authorization: Bearer <token>'")
    return name.strip(), value.strip()


def case_problems(case: dict[str, Any]) -> list[str]:
    """Every reason one case cannot be replayed, phrased so the fix is plain."""
    qid = case.get("qid") or "<no qid>"
    targets = case.get("searchTargets")
    if not isinstance(targets, list) or not targets:
        return [f"{qid}: searchTargets must be a non-empty list of "
                '{"target_type", "search_text"} objects']
    out = []
    for i, t in enumerate(targets):
        where = f"{qid}: searchTargets[{i}]"
        if not isinstance(t, dict):
            out.append(f"{where}: expected an object, got {json.dumps(t)}")
            continue
        if t.get("target_type") not in TARGET_TYPES:
            out.append(f"{where}: invalid target_type {t.get('target_type')!r}: expected one of "
                       f"{', '.join(TARGET_TYPES)}. Fix: \"target_type\": \"measure\"")
        text = t.get("search_text")
        if not isinstance(text, str) or not text.strip():
            out.append(f"{where}: search_text must be a non-empty string; a target with "
                       "none lists every entity of its type instead of searching. "
                       "Fix: \"search_text\": \"total revenue\"")
        out += label_problems(t.get("expectedEntities"), f"{where}.expectedEntities")
    out += label_problems(case.get("expectedEntities"), f"{qid}: expectedEntities")
    return out


def label_problems(exp: Any, where: str) -> list[str]:
    if exp is None:
        return []
    if not isinstance(exp, dict):
        return [f"{where}: expected an object, got {json.dumps(exp)}"]
    out = []
    for key in ("required", "acceptable"):
        v = exp.get(key)
        if v is not None and not (isinstance(v, list) and all(isinstance(e, str) for e in v)):
            out.append(f"{where}.{key}: expected a list of entity ids, got {json.dumps(v)}")
    v = exp.get("requiredAnyOf")
    if v is not None and not (isinstance(v, list) and all(
            isinstance(g, list) and g and all(isinstance(e, str) for e in g) for g in v)):
        out.append(f"{where}.requiredAnyOf: expected a list of non-empty lists of entity ids, "
                   f"got {json.dumps(v)}")
    return out


def read_cases(set_dir: pathlib.Path, only: list[str] | None) -> tuple[list[dict[str, Any]], int]:
    """The cases to replay, and how many in the file carry no searchTargets."""
    path = set_dir / "cases.jsonl"
    if not path.is_file():
        raise SystemExit(f"{path} does not exist. Fix: --set must be a set directory holding cases.jsonl.")
    cases = ledger.read_jsonl(path)
    if only:
        unknown = sorted(set(only) - {c.get("qid") for c in cases})
        if unknown:
            raise SystemExit(f"--only names qids not in {path}: {', '.join(unknown)}")
        cases = [c for c in cases if c.get("qid") in only]
    with_targets = [c for c in cases if "searchTargets" in c]
    problems = [p for c in with_targets for p in case_problems(c)]
    if problems:
        raise SystemExit(f"{path}: {len(problems)} problem(s), nothing was sent:\n  "
                         + "\n  ".join(problems))
    return with_targets, len(cases) - len(with_targets)


def labels_of(case: dict[str, Any]) -> dict[str, Any]:
    """The case's own `expectedEntities` merged with every target's.

    A need named twice (the same id under two targets, or the same any-of
    group) counts once, and a one-member group that repeats a required id is
    dropped, so a repeated label cannot weigh a case twice.
    """
    parts = [case.get("expectedEntities") or {}] + [t.get("expectedEntities") or {}
                                                    for t in case["searchTargets"]]
    required = list(dict.fromkeys(e for p in parts for e in p.get("required") or []))
    any_of: list[list[str]] = []
    for p in parts:
        for g in p.get("requiredAnyOf") or []:
            g = list(dict.fromkeys(g))
            if set(g) <= set(required) and len(g) == 1:
                continue
            if not any(set(g) == set(h) for h in any_of):
                any_of.append(g)
    acceptable = list(dict.fromkeys(e for p in parts for e in p.get("acceptable") or []))
    return {"required": required, "requiredAnyOf": any_of, "acceptable": acceptable}


def request_for(case: dict[str, Any], scope: dict[str, str]) -> dict[str, Any]:
    """The get_context arguments for one case. The question goes along as
    `user_prompt`, which the server records and does not match on."""
    args: dict[str, Any] = {
        "search_targets": [{"target_type": t["target_type"], "search_text": t["search_text"]}
                           for t in case["searchTargets"]],
        "scopes": [scope],
    }
    if isinstance(case.get("question"), str) and case["question"].strip():
        args["user_prompt"] = case["question"]
    return args


# --------------------------------------------------------------------------- scoring

def call_event(qid: str, args: dict[str, Any], payload: Any, error: str | None) -> dict[str, Any]:
    """The `tool_call` ledger event for one replayed request, shaped as an answer run writes it."""
    ranked = None
    if error is None:
        ids = entity_ids(payload or {})
        ranked = {"entityIds": ids, "hits": entity_hits(payload or {}), "resultCount": len(ids),
                  "docTokens": doc_tokens(payload or {})}
    mode = payload.get("retrieval") if isinstance(payload, dict) else None
    return ledger.event("tool_call", qid=qid, sample=None, phase=PHASE, tool="get_context",
                        targets=search_terms(args), target_shapes=target_shapes(args),
                        scopes=args["scopes"], retrieval_mode=mode, rankedSummary=ranked,
                        error=error)


def recall_exact(labels: dict[str, Any], route: dict[str, str]) -> float | None:
    """Share of needs with a member returned under its exact id; None when the case names no need."""
    needs = groups(labels)
    if not needs:
        return None
    return sum(1 for g in needs if any(route.get(e) == "exact" for e in g)) / len(needs)


def score(case: dict[str, Any], event: dict[str, Any]) -> dict[str, Any]:
    """One case's row. An errored request has no score: it is counted, never averaged."""
    qid = case["qid"]
    base = {"qid": qid, "tags": list(case.get("tags") or [])}
    if event.get("error"):
        return {**base, "error": event["error"], "recall": None, "recall_exact": None,
                "precision": None, "n_returned": None}
    labels = labels_of(case)
    row = score_case({"qid": qid, "coverage": case.get("coverage"), "expectedEntities": labels},
                     [event], (qid, None, PHASE), verdict=None)
    keep = ("recall", "precision", "n_required", "n_returned", "delivery", "missing", "noise")
    out = {**base, "error": None, **{k: row[k] for k in keep}}
    out["recall_exact"] = None if row["recall"] is None else recall_exact(labels, row["delivery"])
    out["returned"] = event["rankedSummary"]["entityIds"]
    out["retrieval_mode"] = event.get("retrieval_mode")
    return out


def mean(xs: list[Any]) -> float | None:
    xs = [x for x in xs if x is not None]
    return sum(xs) / len(xs) if xs else None


def pooled(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Means over the rows that have a score. Every case weighs the same.

    `precision` is undefined for a case that returned nothing, so it is left
    out of the precision mean and counted in `empty_responses` instead.
    """
    scored = [r for r in rows if not r.get("error") and r.get("recall") is not None]
    return {
        "scored": len(scored),
        "recall_exact": mean([r["recall_exact"] for r in scored]),
        "recall": mean([r["recall"] for r in scored]),
        "precision": mean([r["precision"] for r in scored]),
        "complete": sum(1 for r in scored if r["recall_exact"] == 1.0),
        "empty_responses": sum(1 for r in scored if r["n_returned"] == 0),
        "mean_returned": mean([r["n_returned"] for r in scored]),
    }


def summarize(rows: list[dict[str, Any]], skipped: int) -> dict[str, Any]:
    ok = [r for r in rows if not r.get("error")]
    out: dict[str, Any] = {
        "cases": len(rows),
        "errors": len(rows) - len(ok),
        # Answered, but no case label names a need, so there is no recall.
        "unlabelled": sum(1 for r in ok if r.get("recall") is None),
        "skipped_no_search_targets": skipped,
        **pooled(rows),
    }
    tags = sorted({t for r in rows for t in r.get("tags") or []})
    out["by_tag"] = {t: {"cases": sum(1 for r in rows if t in r["tags"]),
                         **pooled([r for r in rows if t in r["tags"]])} for t in tags}
    return out


# --------------------------------------------------------------------------- run

def fmt(x: Any) -> str:
    return "-" if x is None else (f"{x:.3f}" if isinstance(x, float) else str(x))


def print_report(rows: list[dict[str, Any]], s: dict[str, Any], mode: str) -> None:
    print(f"\n{'qid':44s} {'recall':>7s} {'exact':>7s} {'prec':>7s} {'back':>5s}  missing")
    for r in rows:
        if r.get("error"):
            print(f"{r['qid'][:44]:44s} ERROR {r['error'][:80]}")
            continue
        print(f"{r['qid'][:44]:44s} {fmt(r['recall']):>7s} {fmt(r['recall_exact']):>7s} "
              f"{fmt(r['precision']):>7s} {r['n_returned']:>5d}  {'; '.join(r['missing'])[:60]}")
    print(f"\nretrieval {mode}. cases {s['cases']} (scored {s['scored']}, errors {s['errors']}, "
          f"unlabelled {s['unlabelled']}, without searchTargets {s['skipped_no_search_targets']})")
    print(f"recall (exact ids) {fmt(s['recall_exact'])}   recall (credited) {fmt(s['recall'])}   "
          f"precision {fmt(s['precision'])}   complete {s['complete']}/{s['scored']}   "
          f"returned/case {fmt(s['mean_returned'])}   empty {s['empty_responses']}")
    for t, v in s["by_tag"].items():
        print(f"  tag {t:28s} scored {v['scored']:3d} of {v['cases']:3d}  recall exact {fmt(v['recall_exact'])}  "
              f"precision {fmt(v['precision'])}")


def file_sha256(path: pathlib.Path) -> str:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError as e:
        raise SystemExit(f"Invalid --set-manifest {path}: cannot read it ({e}).")


def resolve(a: argparse.Namespace) -> config.Config:
    """Fill the server, names and run directory from flags, then the set's eval.toml.

    As for an answer run, a platform target never reads its environment and
    package from eval.toml, which describes a local server.
    """
    cfg = config.load(a.set_dir)
    if a.target == "platform":
        if not (a.environment and a.package and a.mcp_url):
            raise SystemExit("--target platform needs --mcp-url, --environment and --package; "
                             "none of them is read from eval.toml, which describes a local server.")
    else:
        a.environment = cfg.need(a.environment, "model", "environment", "--environment")
        a.package = cfg.need(a.package, "model", "package", "--package")
        a.mcp_url = a.mcp_url or cfg.model_mcp_url()
    a.set_meta = cfg.set_meta
    a.model_path = a.model_path or a.set_meta.get("targetModelPath")
    if a.out is None:
        runs = cfg.workdir() / "runs"
        a.label = a.label or rb.next_run_label(runs / "_", cfg.set_name, PHASE)
        a.out = runs / a.label
    a.label = a.label or a.out.name
    return cfg


def replay(a: argparse.Namespace, cfg: config.Config) -> int:
    cases, skipped = read_cases(a.set_dir, a.only)
    if a.limit:
        cases = cases[: a.limit]
    if not cases:
        raise SystemExit(f"{a.set_dir / 'cases.jsonl'}: no case carries searchTargets, so there is "
                         "nothing to replay. Add searchTargets (reference/retrieval-replay.md).")
    held = [f for f in ("run.json", "events.jsonl") if (a.out / f).exists()]
    if held:
        raise SystemExit(f"{a.out} already holds a run ({', '.join(held)}). "
                         "Fix: pass a new --label or --out.")
    a.out.mkdir(parents=True, exist_ok=True)
    scope = {"environment": a.environment, "package": a.package}
    if a.model_path:
        scope["model_path"] = a.model_path
    started = ledger.now()

    try:
        ready, gate = rb.wait_retrieval_ready(a, tries=a.ready_tries, pause=a.ready_pause)
    except rb.AuthRequired as e:
        raise SystemExit(f"{e}: the MCP server wants credentials. Fix: pass them with "
                         "--mcp-header 'Authorization: Bearer <token>'.")
    if not ready:
        raise SystemExit(f"retrieval is not ready: {gate}")
    print(f"retrieval gate: {gate}", flush=True)

    events, rows = [], []
    for i, case in enumerate(cases, 1):
        args = request_for(case, scope)
        payload, error = None, None
        t0 = time.time()
        try:
            payload = rb.mcp_call(a.mcp_url, "get_context", args, timeout=a.timeout,
                                  headers=a.mcp_headers)
            state = payload.get("retrieval") if isinstance(payload, dict) else None
            if state in ("indexing", "error"):
                # No ranking happened, so there is nothing to score; a zero here would read as a miss.
                error = f"retrieval {state} mid-run ({payload.get('retrieval_reason') or 'no reason given'})"
        except (OSError, ValueError, urllib.error.URLError) as e:
            error = f"{type(e).__name__}: {str(e)[:300]}"
        ev = call_event(case["qid"], args, payload, error)
        events.append(ev)
        row = score(case, ev)
        row["latency_s"] = round(time.time() - t0, 3)
        rows.append(row)
        if i % 10 == 0 or i == len(cases):
            print(f"[{a.label}] {i}/{len(cases)}", flush=True)

    mode, calls = rb.retrieval_summary([{"calls": events}])
    summary = summarize(rows, skipped)
    pins: dict[str, Any] = {
        "runId": a.out.name, "label": a.label, "target": a.target, "answererModel": None,
        "phase": PHASE, "mode": "retrieval", "started": started, "status": "complete",
        "setName": a.set_meta.get("name") or cfg.set_name,
        "datasetVersion": a.set_meta.get("datasetVersion"),
        "datasetSha": ledger.dataset_sha(a.set_dir),
        "environment": a.environment, "package": a.package, "modelPath": a.model_path,
        "mcpUrl": a.mcp_url, "mcpHeaderNames": sorted(a.mcp_headers),
        "retrievalMode": mode, "retrievalCalls": calls, "retrievalGate": gate,
        "harnessVersion": ledger.skills_git_sha(),
    }
    if a.set_version:
        pins["setVersion"] = a.set_version
    if a.set_manifest:
        pins["setManifestSha256"] = file_sha256(a.set_manifest)
    (a.out / "run.json").write_text(json.dumps(ledger.run_config(**pins), indent=1) + "\n")
    ledger.write_events(a.out / "events.jsonl", events)
    ledger.write_events(a.out / "results.jsonl", rows)
    summary["run"] = {k: pins.get(k) for k in ("label", "setName", "datasetVersion", "datasetSha",
                                               "setVersion", "setManifestSha256", "retrievalMode")}
    (a.out / "summary.json").write_text(json.dumps(summary, indent=1) + "\n")
    print_report(rows, summary, mode)
    print(f"\nrun directory: {a.out}")
    if mode in ("mixed", "unavailable"):
        print(f"WARNING: retrieval was {mode} during this run ({calls}); its means mix "
              "results from different retrievers and should not be compared.")
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--set", dest="set_dir", required=True, type=pathlib.Path)
    ap.add_argument("--label", default=None,
                    help="the run's name and directory under <workdir>/runs. Default: <set>-replay-<nn>")
    ap.add_argument("--out", type=pathlib.Path, default=None, help="the run directory, instead of the label's")
    ap.add_argument("--target", choices=("local", "platform"), default="local",
                    help="local: names and server from eval.toml. platform: a hosted MCP server; "
                         "--mcp-url, --environment and --package are then required")
    ap.add_argument("--mcp-url", default=None, help="default: the [model] server in eval.toml")
    ap.add_argument("--environment", default=None)
    ap.add_argument("--package", default=None)
    ap.add_argument("--model-path", default=None, help="default: set.json targetModelPath; omit for the whole package")
    ap.add_argument("--mcp-header", action="append", default=[], metavar="'NAME: VALUE'",
                    help="an extra header on every MCP request, such as auth. Repeatable. "
                         "Only the names are recorded")
    ap.add_argument("--set-version", default=None,
                    help="the version the set was published under, recorded in run.json")
    ap.add_argument("--set-manifest", type=pathlib.Path, default=None,
                    help="that version's manifest file; its sha256 is recorded in run.json")
    ap.add_argument("--only", action="append", default=None, metavar="QID", help="replay only this case (repeatable)")
    ap.add_argument("--limit", type=int, default=0, help="the first N cases only (smoke test)")
    ap.add_argument("--timeout", type=int, default=120, help="seconds per get_context call")
    ap.add_argument("--ready-tries", type=int, default=12)
    ap.add_argument("--ready-pause", type=float, default=10.0)
    a = ap.parse_args(argv)
    a.mcp_headers = dict(parse_header(h) for h in a.mcp_header)
    try:
        cfg = resolve(a)
    except config.ConfigError as e:
        print(e, file=sys.stderr)
        return 3
    return replay(a, cfg)


if __name__ == "__main__":
    sys.exit(main())
