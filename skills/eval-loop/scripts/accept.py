#!/usr/bin/env python3
"""Check an edit's runs against the acceptance rules, in one command. Stdlib only.

  python3 eval.py accept --set <set-dir> --run tip-01 --run tip-02 \
      [--baseline record] [--targets q1,q2] [--replay accept-01]

It checks what reference/acceptance-check.md states as rules and a script can
decide, over the cases the candidate runs answered:

  twice        at least two candidate runs, each with fresh answerers
  no regression  no case passing in the baseline fails in any candidate run
  targets fixed  every target case passes in every candidate run (default
               targets: the cases the baseline did not pass)
  both splits  the candidates answered holdout cases as well as dev, when the
               set has holdout
  one change   the runs differ from the baseline only in the model
  rows held    a query replay of the edit changed no stored answer (--replay)

It decides nothing a rule cannot: a judge's verdict is taken as given, a
suspect golden still halts the check (read `doubtedGoldens` in run.json), and
whether a fix clears the flip-count bar for its size (measurement.md) is yours.
Exit 0 when every rule holds, 1 when one does not, 3 when it could not check.
The result is written to <workdir>/acceptances/, with a checkpoint commit
message that names the runs.
"""
from __future__ import annotations

import argparse
import datetime
import json
import pathlib
import sys
from typing import Any

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent.parent / "eval-answer" / "scripts"))
sys.path.insert(0, str(HERE))

import config  # noqa: E402
import run_index  # noqa: E402
from flip_table import (COMPARABLE, MODEL_PINS, SET_PINS, config as run_config,  # noqa: E402
                        incomparable, verdicts)
from ledger import read_jsonl  # noqa: E402


def resolve(cfg: config.Config, name: str, kind: str = "run") -> pathlib.Path:
    """A run by label or path; `record` is the main line's newest full run."""
    if name == "record":
        import run_store
        run_store.sync(cfg)
        r = run_index.record(run_index.set_runs(cfg), kind)
        if r is None:
            raise SystemExit(f"no record run: no complete, full main-line {kind} in "
                             f"{cfg.workdir() / 'runs'}. Fix: run the whole set once, "
                             f"or pull the team's (eval.py pull)")
        return pathlib.Path(r["path"])
    p = pathlib.Path(name)
    p = p if p.is_dir() else cfg.workdir() / "runs" / name
    if not (p / "run.json").exists():
        raise SystemExit(f"no run at {p}")
    return p


def check(cfg: config.Config, baseline: pathlib.Path, candidates: list[pathlib.Path],
          targets: list[str] | None, replay: pathlib.Path | None) -> dict[str, Any]:
    split = {c["qid"]: c.get("split") for c in read_jsonl(cfg.set_dir / "cases.jsonl")}
    B = verdicts(baseline)
    runs = {c.name: verdicts(c) for c in candidates}
    answered = sorted(set().union(*[set(v) for v in runs.values()]))
    if targets is None:
        targets = [q for q in answered if q in B and not B[q]["passed"]]
    rules: dict[str, dict[str, Any]] = {}
    rules["twice"] = {"ok": len(candidates) >= 2,
                      "detail": f"{len(candidates)} candidate run(s)"}
    regressions, unfixed, set_aside = [], [], set()
    for name, C in runs.items():
        moved = incomparable(B, C)
        set_aside |= set(moved)
        for q in C:
            if q in moved or q not in B:
                continue
            if B[q]["passed"] and C[q]["passed"] is False:
                regressions.append(f"{q} in {name}")
        for q in targets:
            if q in C and not C[q]["passed"]:
                unfixed.append(f"{q} in {name} ({C[q]['verdict']})")
            if q not in C:
                unfixed.append(f"{q} not answered in {name}")
    rules["no regression"] = {"ok": not regressions, "detail": ", ".join(regressions) or "none"}
    rules["targets fixed"] = {"ok": bool(targets) and not unfixed,
                              "detail": (", ".join(unfixed) if unfixed else
                                         f"{len(targets)} target(s) pass in every run")
                              if targets else "no targets: the baseline passed every "
                                              "case answered. Name them with --targets"}
    has_holdout = "holdout" in split.values()
    splits = {split.get(q) for q in answered}
    rules["both splits"] = {"ok": not has_holdout or {"dev", "holdout"} <= splits,
                            "detail": ("the set has no holdout" if not has_holdout else
                                       f"answered {', '.join(sorted(s or '?' for s in splits))}")}
    base_pins = run_config(baseline)
    other = sorted({k for c in candidates for k in COMPARABLE
                    if k not in MODEL_PINS + SET_PINS
                    and run_config(c).get(k) != base_pins.get(k)})
    rules["one change"] = {"ok": not other,
                           "detail": "only the model moved" if not other else
                           f"also moved: {', '.join(other)}"}
    if replay is not None:
        s = json.loads((replay / "summary.json").read_text()) if (replay / "summary.json").exists() else {}
        c = s.get("compare")
        rules["rows held"] = {"ok": bool(c) and not c.get("blocked"),
                              "detail": ("no comparison in that replay: run it with --baseline"
                                         if not c else f"{len(c['rows_changed'])} changed, "
                                         f"{len(c['broke'])} broke, over {c['compared']}")}
    doubted = [d.get("qid") for c in candidates for d in run_config(c).get("doubtedGoldens") or []]
    return {"baseline": baseline.name, "candidates": [c.name for c in candidates],
            "replay": replay.name if replay else None, "targets": targets,
            "setAside": sorted(set_aside), "doubtedGoldens": sorted(set(filter(None, doubted))),
            "rules": rules,
            "accepted": all(r["ok"] for r in rules.values()) and not doubted}


def commit_message(r: dict[str, Any], what: str) -> str:
    lines = [f"checkpoint: {what}", "",
             f"Accepted against {r['baseline']}: " + ", ".join(
                 f"{k} ({v['detail']})" for k, v in r["rules"].items()) + ".",
             f"Targets: {', '.join(r['targets']) or 'none'}.",
             f"Runs: {', '.join(r['candidates'])}"
             + (f"; replay {r['replay']}" if r["replay"] else "") + "."]
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--set", dest="set_dir", required=True, type=pathlib.Path)
    ap.add_argument("--run", action="append", required=True,
                    help="a candidate run, by label or path; give it twice")
    ap.add_argument("--baseline", default="record",
                    help="the run to compare with (default: record, the newest full main-line run)")
    ap.add_argument("--targets", default=None, help="comma-separated qids the edit is meant to fix")
    ap.add_argument("--replay", default=None, help="a query replay of the edit, compared with --baseline")
    ap.add_argument("--what", default="<the edit>", help="one line for the commit message")
    a = ap.parse_args(argv)
    cfg = config.load(a.set_dir)
    baseline = resolve(cfg, a.baseline)
    candidates = [resolve(cfg, r) for r in a.run]
    replay = resolve(cfg, a.replay, "perf") if a.replay else None
    targets = a.targets.split(",") if a.targets else None
    r = check(cfg, baseline, candidates, targets, replay)
    print(f"acceptance: {', '.join(r['candidates'])} against {r['baseline']}")
    for name, rule in r["rules"].items():
        print(f"  {'ok  ' if rule['ok'] else 'FAIL'} {name:<14} {rule['detail']}")
    if r["setAside"]:
        print(f"  set aside (question or key changed since the baseline): {', '.join(r['setAside'])}")
    if r["doubtedGoldens"]:
        print(f"  HALT: the judge doubts the key on {', '.join(r['doubtedGoldens'])}. "
              f"Settle each through the golden side door first (golden-side-door.md).")
    out = cfg.workdir() / "acceptances"
    out.mkdir(parents=True, exist_ok=True)
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    (out / f"{stamp}.json").write_text(json.dumps(r, indent=1) + "\n")
    if r["accepted"]:
        print("\nevery rule holds. Checkpoint with:\n")
        print(commit_message(r, a.what))
        return 0
    print("\nnot accepted: fix the failing rule, or revert the edit.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
