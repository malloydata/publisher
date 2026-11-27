#!/usr/bin/env python3
"""Build a new version of the eval set from judged production history. Stdlib only.

  python3 eval.py build-set --set <set-dir> --label prod-20261126 [--questions 100]

Step O1 of the optimization loop (reference/optimization-loop.md). Stage 1
(`eval.py ingest`, `eval.py judge-sample`) judged a sample of production and
kept each verdict, its key and its reason. This turns that record into the set
the improve step climbs on.

Each run builds a NEW version, never an edit of the last:
`<optimization package>/sets/<label>/`, holding `cases.jsonl`, `gold/`,
`workloads/<label>.jsonl`, `set.json` (with `version: <label>`) and an
`eval.json` that points at the same servers, work directory and optimization
package as `<set-dir>`. Run every later step with `--set` on that directory.
The model's repository holds no eval files: the set holds the answer key, and
the pull request names the version it was checked against.

WHICH QUESTIONS

Only questions production judging keyed (a verified golden), one case per
question group (judge-sample's grouping of identical questions):

1. **Failures**, up to half of `--questions`: groups judged wrong, including
   groups with mixed verdicts (right on one day, wrong on another). Taken
   round-robin across failure codes, so one common failure does not crowd out
   the rest. Tagged `failure` and `failure:<code>`, and `mixed` when mixed.
2. **Controls**: groups judged right every time, as many as the failures and
   at least `--min-controls`, round-robin across the source each answer
   queried. Tagged `control`. An edit that breaks one is refused by
   `accept`, like any case that passed before.

A follow-up (judging recorded `depends_on`) carries its earlier questions as
`context.priorQuestions`, and the run answers them first in the same session.
A chain longer than three, or one through a build request, is left out and
counted. Groups with no verified key are left out and counted: stage 1 keys
them.

Also writes the version's **workload**: every production query of the served
package that succeeded, one line per distinct query with how often it ran, in
the shape `query-replay --queries` and `simulate` read.

The question text is kept exactly as asked. Nothing is invented.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import sys
from typing import Any

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent.parent / "eval-answer" / "scripts"))
sys.path.insert(0, str(HERE))

import config  # noqa: E402
from optimization_package import install_for, read  # noqa: E402
from workload_from_logs import workload  # noqa: E402
import attempts  # noqa: E402

MAX_CHAIN = 3


def normal(q: str) -> str:
    return " ".join((q or "").lower().split())


def qid_for(q: str, prior: list[str] | None = None) -> str:
    key = "\n".join([normal(x) for x in prior or []] + [normal(q)])
    return "prod-" + hashlib.sha256(key.encode()).hexdigest()[:10]


Key = tuple[str, dict[str, Any], pathlib.Path, dict[str, Any]]


def judged_keys(workdir: pathlib.Path) -> tuple[dict[str, Key], set[str]]:
    """From every judging set: turn id -> (judging set, verified golden, its
    gold/<qid>.json, the judging case), and the turns judging drew but gave
    no golden (a remark, a build request)."""
    keys: dict[str, Key] = {}
    seen: set[str] = set()
    for f in sorted((workdir / "judging").glob("*/cases.jsonl")):
        for line in f.read_text(encoding="utf-8").splitlines():
            c = json.loads(line) if line.strip() else {}
            if not c.get("source"):
                continue
            seen.add(str(c["source"]))
            g = c.get("golden") or {}
            if g.get("status") == "verified":
                keys.setdefault(str(c["source"]),
                                (f.parent.name, g, f.parent / "gold" / f"{c.get('qid')}.json", c))
    return keys, seen - set(keys)


def groups(turns: list[dict[str, str]], judgments: list[dict[str, str]],
           keys: dict[str, Key], sources: dict[str, str]) -> dict[str, dict[str, Any]]:
    """Every judged question group: its verdicts, failure codes, the turn whose
    key it uses, and the source that turn's answer queried."""
    by_turn = {t["turn_id"]: t for t in turns}
    out: dict[str, dict[str, Any]] = {}
    for j in judgments:
        t = by_turn.get(j["turn_id"])
        if not t or (t.get("kind") or "") == "build":
            continue
        gid = j.get("question_group") or normal(t.get("question", ""))
        g = out.setdefault(gid, {"group": gid, "question": t["question"].strip(), "turns": set(),
                                 "right": 0, "wrong": 0, "codes": set(), "key": None,
                                 "depends_on": [], "last": ""})
        g["turns"].add(j["turn_id"])
        g["last"] = max(g["last"], t.get("at") or "")
        g["right"] += j.get("right") == "True"
        g["wrong"] += j.get("right") == "False"
        if j.get("right") == "False":
            g["codes"].add(j.get("failure_code") or "undiagnosed")
        judged = j.get("judged_turn") or j["turn_id"]
        if g["key"] is None and judged in keys:
            g["key"] = judged
            g["depends_on"] = json.loads(j["depends_on"]) if j.get("depends_on") else []
        g.setdefault("source", sources.get(judged))
    return out


def round_robin(buckets: dict[str, list[dict[str, Any]]], n: int) -> list[dict[str, Any]]:
    """Up to n items, one from each bucket in turn, so no bucket crowds out the rest."""
    order = {k: sorted(v, key=lambda g: (-len(g["turns"]), g["question"])) for k, v in sorted(buckets.items())}
    out: list[dict[str, Any]] = []
    while len(out) < n and any(order.values()):
        for k in list(order):
            if order[k] and len(out) < n:
                out.append(order[k].pop(0))
    return out


def chain(g: dict[str, Any], keys: dict[str, Key]) -> tuple[list[str] | None, str | None]:
    """The earlier questions a follow-up needs, or (None, why it is left out)."""
    deps = g["depends_on"]
    if not deps:
        return [], None
    if len(deps) > MAX_CHAIN:
        return None, f"a follow-up of more than {MAX_CHAIN} earlier questions"
    earlier = ((keys[g["key"]][3].get("context") or {}).get("earlierMessages") or [])
    kinds = {normal(e.get("question", "")): e.get("kind") for e in earlier}
    if any(kinds.get(normal(d)) == "build" for d in deps):
        return None, "a follow-up of a build request"
    return deps, None


def choose(found: dict[str, dict[str, Any]], keys: dict[str, Key], n: int,
           min_controls: int = 5) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Failures across codes, then controls across sources. Returns the chosen
    groups (each with `role` and `prior`) and what was left out, by reason."""
    left: dict[str, int] = {}
    usable = []
    for g in found.values():
        if not g["key"]:
            left["no verified key"] = left.get("no verified key", 0) + 1
            continue
        prior, why = chain(g, keys)
        if why:
            left[why] = left.get(why, 0) + 1
            continue
        g["prior"] = prior
        usable.append(g)
    failing: dict[str, list[dict[str, Any]]] = {}
    passing: dict[str, list[dict[str, Any]]] = {}
    for g in usable:
        if g["wrong"]:
            for code in sorted(g["codes"])[:1]:
                failing.setdefault(code, []).append(g)
        elif g["right"]:
            passing.setdefault(g.get("source") or "unknown", []).append(g)
    failures = round_robin(failing, n // 2 if n > 1 else n)
    for g in failures:
        g["role"] = "failure"
    controls = round_robin(passing, min(n - len(failures), max(len(failures), min_controls)))
    for g in controls:
        g["role"] = "control"
    undecided = sum(1 for g in usable if not g["wrong"] and not g["right"])
    if undecided:
        left["judged, undecided"] = undecided
    return failures + controls, left


def case(g: dict[str, Any], label: str, keys: dict[str, Key],
         set_dir: pathlib.Path | None = None) -> dict[str, Any]:
    """The case, keyed from judging. Its derivation record (gold/<qid>.json,
    what --refresh re-runs) is copied under the new qid."""
    prior = g.get("prior") or []
    tags = ["production", g["role"]]
    if g["role"] == "failure":
        tags += [f"failure:{code}" for code in sorted(g["codes"])]
        if g["right"]:
            tags.append("mixed")
    if prior:
        tags.append("follow-up")
    c: dict[str, Any] = {
        "qid": qid_for(g["question"], prior), "question": g["question"], "split": "dev",
        "tags": tags, "source": f"production:{label}",
        "sourceFields": {"asked": len(g["turns"]), "judgedWrong": g["wrong"],
                         "judgedRight": g["right"], "lastAsked": g["last"],
                         "turns": sorted(g["turns"])[:5]}}
    if prior:
        c["context"] = {"priorQuestions": prior}
    judging, golden, gold, _ = keys[g["key"]]
    c["golden"] = golden
    c["sourceFields"]["keyFrom"] = f"judging:{judging}:{g['key']}"
    if set_dir is not None and gold.is_file():
        rec = json.loads(gold.read_text(encoding="utf-8"))
        rec["qid"] = c["qid"]
        out = set_dir / "gold" / f"{c['qid']}.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(rec, indent=1) + "\n", encoding="utf-8")
    return c


def version_config(cfg: Any) -> dict[str, Any]:
    """The parent set's config for a version: the same servers, work directory,
    store and optimization package, every path absolute."""
    out: dict[str, Any] = {}
    for section, values in cfg.data.items():
        out[section] = {k: str(v) if isinstance(v, pathlib.Path) else v for k, v in values.items()}
    out.setdefault("paths", {})["workdir"] = str(cfg.workdir())
    out.setdefault("optimization", {})["package_dir"] = str(cfg.optimization_dir())
    return out


def write_version(cfg: Any, label: str, cases: list[dict[str, Any]]) -> pathlib.Path:
    """Write <optimization package>/sets/<label>/ and return it. Refuses to
    overwrite a version: a run that used it would no longer match it."""
    out = cfg.optimization_dir() / "sets" / label
    if (out / "cases.jsonl").exists():
        raise SystemExit(f"Set version {label} already exists at {out}. Fix: pass a new --label")
    out.mkdir(parents=True, exist_ok=True)
    meta = dict(cfg.set_meta)
    meta.pop("datasetVersion", None)
    # Every version shares one record of what was tried, so a later version's
    # improve step still sees the edits an earlier one rejected.
    tried = cfg.optimization_dir() / attempts.FILE
    if not tried.exists() and attempts.path(cfg.set_dir).is_file():
        tried.write_bytes(attempts.path(cfg.set_dir).read_bytes())
    meta.update({"name": cfg.set_name, "version": label, "builtFrom": "production judging",
                 "attemptsFile": str(tried),
                 "description": "Built by eval.py build-set from judged production history: "
                                "failures across failure codes, and controls judged right."})
    (out / "set.json").write_text(json.dumps(meta, indent=1) + "\n")
    (out / "eval.json").write_text(json.dumps(version_config(cfg), indent=1) + "\n")
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--set", dest="set_dir", required=True)
    ap.add_argument("--label", required=True, help="the new version's name; also names the workload")
    ap.add_argument("--questions", type=int, default=100, help="cases in the version, at most")
    ap.add_argument("--min-controls", type=int, default=5, help="controls even when few failures")
    a = ap.parse_args(argv)
    cfg = config.load(pathlib.Path(a.set_dir))
    package = cfg.get("model", "package")
    data = install_for(cfg)
    turns = read(data / "prod_turns.csv")
    if not turns:
        raise SystemExit(f"No production turns in {data}. Fix: eval.py ingest --set {a.set_dir} --turns turns.jsonl")
    judgments = read(data / "prod_judgments.csv")
    if not judgments:
        raise SystemExit(f"No judged production turns in {data}. Fix: eval.py judge-sample pick, "
                         f"run, diagnose and collect first")
    queries = read(data / "prod_queries.csv")
    sources: dict[str, str] = {}
    for q in queries:
        if q.get("turn_id") and q.get("source"):
            sources.setdefault(q["turn_id"], q["source"])
    keys, _ = judged_keys(cfg.workdir())
    chosen, left = choose(groups(turns, judgments, keys, sources), keys, a.questions, a.min_controls)
    out = write_version(cfg, a.label, [])
    new = [case(g, a.label, keys, out) for g in chosen]
    (out / "cases.jsonl").write_text("".join(json.dumps(c) + "\n" for c in new))
    nf = sum(1 for c in new if "failure" in c["tags"])
    print(f"set version {a.label}: {len(new)} cases ({nf} failures across "
          f"{len({t for c in new for t in c['tags'] if t.startswith('failure:')})} failure codes, "
          f"{len(new) - nf} controls, {sum(1 for c in new if 'follow-up' in c['tags'])} follow-ups)")
    if left:
        print("  left out: " + ", ".join(f"{n} {why}" for why, n in sorted(left.items())))

    rows = [{"query": q["query_text"], "modelPath": q["model_path"], "package": q["package"]}
            for q in queries
            if q.get("package") == package and q.get("status") in ("ok", "200", "")]
    items = ({"query": r["query"], "modelPath": r["modelPath"], "count": 1} for r in rows)
    lines, skipped = workload(items)
    wl = out / "workloads" / f"{a.label}.jsonl"
    wl.parent.mkdir(parents=True, exist_ok=True)
    wl.write_text("".join(json.dumps(w) + "\n" for w in lines))
    print(f"workload: {len(lines)} distinct queries from {len(rows)} that succeeded -> {wl}"
          + (f"; left out {skipped}" if skipped else ""))
    print(f"next: python3 eval.py run --set {out} --label <run>")
    print(out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
