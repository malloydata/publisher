#!/usr/bin/env python3
"""Check an edit's runs against the acceptance rules, in one command. Stdlib only.

  python3 eval.py accept --set <set-dir> --run tip-01 --run tip-02 \
      [--baseline record] [--targets q1,q2] [--replay accept-01]

It checks what reference/acceptance-check.md states as rules and a script can
decide, over the cases the candidate runs answered:

  twice        at least two different candidate runs (the same run named twice,
               or the baseline named as a candidate, is refused)
  no regression  no case passing in the baseline fails in any candidate run
  targets fixed  every target case passes in every candidate run (default
               targets: the cases the baseline did not pass)
  both splits  the candidates answered holdout cases as well as dev, when the
               set has holdout
  one change   the runs differ from the baseline only in the model: same judge,
               judge prompt, answerer, turn cap and effort
  rows held    a query replay of the edit changed no stored answer (--replay)

NOISE. An answer run is one sample, and some cases pass one night and fail the
next with nothing changed. Given a second run of the baseline (`--aa`, or found
by itself: another complete full run with the baseline's exact pins, which two
nights with no edit between them produce), a case whose verdict differs between
the two is UNSTABLE. A fix on an unstable case is not evidence, so it is not a
default target and a named one is not required to pass. A regression on one is
not evidence either, so it does not fail the edit. Both are listed. With no
second run, noise is not measured, the check says so, and every case counts.

A case on the `test` split is left out of every rule. It is the number that is
reported, so no decision may be made on it.

Every check, accepted or rejected, is appended to <set>/attempts.jsonl
(attempts.py): what was tried, on which cases, and how it went. That file is
what the next edit reads.

It decides nothing a rule cannot: a judge's verdict is taken as given, a
suspect golden still halts the check (read `doubtedGoldens` in run.json), and
whether a fix clears the flip-count bar for its size (measurement.md) is yours.
Exit 0 when every rule holds, 1 when one does not, 3 when it could not check
(a run that is not there, the same run named twice).
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

import attempts  # noqa: E402
import config  # noqa: E402
import run_index  # noqa: E402
from flip_table import (COMPARABLE, MODEL_PINS, SET_PINS, config as run_config,  # noqa: E402
                        incomparable, verdicts)
from ledger import read_jsonl  # noqa: E402


class CannotCheck(Exception):
    """The check could not be made at all. Exit 3, never 1: a 1 says a rule failed."""


# Settings that change what an answer run measures and are not the model. They
# sit beside flip_table's COMPARABLE rather than in it: a noise band quoted
# across two turn caps is still a band, but an edit accepted across them is
# two changes. `packageSkillsSha` is deliberately absent: a package skill is
# part of the model's repository and an edit may be to it. A pin one side never
# recorded is not counted as moved, so a record made before the pin compares.
RUN_SETTINGS = ("maxTurns", "judgePromptSha", "effort")


def resolve(cfg: config.Config, name: str, kind: str = "run") -> pathlib.Path:
    """A run by label or path; `record` is the main line's newest full run."""
    if name == "record":
        import run_store
        run_store.sync(cfg)
        r = run_index.record(run_index.set_runs(cfg), kind)
        if r is None:
            raise CannotCheck(f"no record run: no complete, full main-line {kind} in "
                              f"{cfg.workdir() / 'runs'}. Fix: run the whole set once, "
                              f"or pull the team's (eval.py pull)")
        return pathlib.Path(r["path"])
    p = pathlib.Path(name)
    p = p if p.is_dir() else cfg.workdir() / "runs" / name
    if not (p / "run.json").exists():
        raise CannotCheck(f"no run at {p}")
    return p


def distinct(baseline: pathlib.Path, candidates: list[pathlib.Path]) -> None:
    """Refuse one run counted twice. "Twice" is two measurements, not two names."""
    seen: dict[pathlib.Path, str] = {}
    for c in candidates:
        r = c.resolve()
        if r == baseline.resolve():
            raise CannotCheck(f"{c.name} is the baseline, so it cannot also be a candidate. "
                              f"Fix: name the runs made after the edit")
        if r in seen:
            raise CannotCheck(f"{c.name} is named twice. The rule is two runs, each with "
                              f"its own answers. Fix: run the edit a second time under "
                              f"another --label and name both")
        seen[r] = c.name


def same_measurement(a: dict[str, Any], b: dict[str, Any]) -> bool:
    """Do two runs share every pin, the model's included, and the same cases?

    The model is the same when the served package's content hash is the same.
    The repository's commit is only consulted when a run has no content hash:
    in a repository that holds more than the model, the commit moves with every
    unrelated change, and two runs of one unchanged package then never pair.
    Measured: two nights of one package, identical in every other pin, differed
    in `modelGitSha` alone.
    """
    def content(r: dict[str, Any]) -> Any:
        return (r.get("packageVersion") or {}).get("contentHash")
    same_content = content(a) is not None and content(a) == content(b)
    pins = [k for k in (*COMPARABLE, *RUN_SETTINGS) if not (same_content and k == "modelGitSha")]
    return (all(a.get(k) == b.get(k) for k in pins)
            and (a.get("slice") or {}).get("qids") == (b.get("slice") or {}).get("qids"))


def find_aa(cfg: config.Config, baseline: pathlib.Path) -> pathlib.Path | None:
    """A second run of the baseline: the newest other complete main-line run
    with its exact pins. Two nights with no edit between them are one."""
    base = run_config(baseline)
    fits = [r for r in run_index.set_runs(cfg)
            if r["kind"] in ("run", "slice") and r.get("status") == "complete"
            and not r.get("experiment")
            and pathlib.Path(r["path"]).resolve() != baseline.resolve()
            and same_measurement(base, run_config(pathlib.Path(r["path"])))]
    best = max(fits, key=lambda r: (r.get("started") or "", r["run"]), default=None)
    return pathlib.Path(best["path"]) if best else None


def unstable_cases(B: dict[str, dict[str, Any]], A2: dict[str, dict[str, Any]]) -> list[str]:
    """The cases two runs of one configuration disagree on."""
    return sorted(q for q in set(B) & set(A2)
                  if None not in (B[q]["passed"], A2[q]["passed"])
                  and B[q]["passed"] != A2[q]["passed"])


def check(cfg: config.Config, baseline: pathlib.Path, candidates: list[pathlib.Path],
          targets: list[str] | None, replay: pathlib.Path | None,
          aa: pathlib.Path | None = None) -> dict[str, Any]:
    split = {c["qid"]: c.get("split") for c in read_jsonl(cfg.set_dir / "cases.jsonl")}
    held_out = {q for q, s in split.items() if s == "test"}

    def decided_on(v: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
        return {q: x for q, x in v.items() if q not in held_out}
    B = decided_on(verdicts(baseline))
    runs = {c.name: decided_on(verdicts(c)) for c in candidates}
    answered = sorted(set().union(*[set(v) for v in runs.values()]))
    unstable = set(unstable_cases(B, decided_on(verdicts(aa)))) if aa else set()
    named = targets is not None
    if targets is None:
        targets = [q for q in answered if q in B and not B[q]["passed"]]
    unstable_targets = sorted(q for q in targets if q in unstable)
    targets = [q for q in targets if q not in unstable and q not in held_out]
    rules: dict[str, dict[str, Any]] = {}
    rules["twice"] = {"ok": len(candidates) >= 2,
                      "detail": f"{len(candidates)} candidate run(s)"}
    regressions, unfixed, set_aside, noise = [], [], set(), []
    for name, C in runs.items():
        moved = incomparable(B, C)
        set_aside |= set(moved)
        for q in C:
            if q in moved or q not in B:
                continue
            if B[q]["passed"] and C[q]["passed"] is False:
                (noise if q in unstable else regressions).append(f"{q} in {name}")
        for q in targets:
            if q in C and not C[q]["passed"]:
                unfixed.append(f"{q} in {name} ({C[q]['verdict']})")
            if q not in C:
                unfixed.append(f"{q} not answered in {name}")
    rules["no regression"] = {"ok": not regressions, "detail": ", ".join(regressions) or "none"}
    rules["targets fixed"] = {"ok": bool(targets) and not unfixed,
                              "detail": (", ".join(unfixed) if unfixed else
                                         f"{len(targets)} target(s) pass in every run")
                              if targets else (
                                  "every target is unstable: it passes or fails with "
                                  "nothing changed, so passing now shows nothing"
                                  if unstable_targets else
                                  "no targets: the baseline passed every "
                                  "case answered. Name them with --targets")}
    has_holdout = "holdout" in split.values()
    splits = {split.get(q) for q in answered}
    rules["both splits"] = {"ok": not has_holdout or {"dev", "holdout"} <= splits,
                            "detail": ("the set has no holdout" if not has_holdout else
                                       f"answered {', '.join(sorted(s or '?' for s in splits))}")}
    base_pins = run_config(baseline)
    other = sorted({k for c in candidates for k in COMPARABLE
                    if k not in MODEL_PINS + SET_PINS
                    and run_config(c).get(k) != base_pins.get(k)}
                   | {k for c in candidates for k in RUN_SETTINGS
                      if None not in (run_config(c).get(k), base_pins.get(k))
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
    fixed = [q for q in targets if all(q in C and C[q]["passed"] for C in runs.values())]
    return {"baseline": baseline.name, "candidates": [c.name for c in candidates],
            "aa": aa.name if aa else None, "unstable": sorted(unstable),
            "unstableTargets": unstable_targets, "namedTargets": named,
            "noise": sorted(set(noise)), "regressions": sorted(set(regressions)),
            "targetsFixed": fixed, "testCasesLeftOut": len(held_out),
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
    ap.add_argument("--aa", default=None,
                    help="a second run of the baseline, to tell an unstable case from a "
                         "changed one. Default: found by itself, the newest other complete "
                         "run with the baseline's exact pins. `none` to use none")
    ap.add_argument("--issue", action="append", default=None,
                    help="the diagnosed issue this edit is for (recorded with the attempt)")
    ap.add_argument("--patch", type=pathlib.Path, default=None,
                    help="the edit as a diff file (recorded with the attempt, so the same "
                         "edit is recognised if it is proposed again)")
    ap.add_argument("--lesson", default=None,
                    help="one sentence about the model that the next edit should know")
    ap.add_argument("--no-record", action="store_true",
                    help="do not append this check to <set>/attempts.jsonl")
    a = ap.parse_args(argv)
    cfg = config.load(a.set_dir)
    try:
        baseline = resolve(cfg, a.baseline)
        candidates = [resolve(cfg, r) for r in a.run]
        replay = resolve(cfg, a.replay, "perf") if a.replay else None
        distinct(baseline, candidates)
        aa = (None if a.aa == "none" else resolve(cfg, a.aa) if a.aa
              else find_aa(cfg, baseline))
        if aa is not None and aa.resolve() in {c.resolve() for c in candidates}:
            raise CannotCheck(f"{aa.name} is a candidate, so it cannot also be the second "
                              f"run of the baseline")
        if aa is not None and not same_measurement(run_config(baseline), run_config(aa)):
            raise CannotCheck(f"{aa.name} is not a second run of {baseline.name}: their pins "
                              f"differ, so a case that differs between them is not noise. "
                              f"Fix: name a run made with nothing changed, or --aa none")
    except CannotCheck as e:
        print(f"could not check: {e}", file=sys.stderr)
        return 3
    targets = a.targets.split(",") if a.targets else None
    r = check(cfg, baseline, candidates, targets, replay, aa)
    print(f"acceptance: {', '.join(r['candidates'])} against {r['baseline']}")
    for name, rule in r["rules"].items():
        print(f"  {'ok  ' if rule['ok'] else 'FAIL'} {name:<14} {rule['detail']}")
    if r["aa"]:
        print(f"  noise: measured against {r['aa']}, a second run of the baseline. "
              f"{len(r['unstable'])} unstable case(s)"
              + (f": {', '.join(r['unstable'])}" if r["unstable"] else ""))
        if r["unstableTargets"]:
            print(f"    not counted as targets (unstable): {', '.join(r['unstableTargets'])}")
        if r["noise"]:
            print(f"    not counted as regressions (unstable): {', '.join(r['noise'])}")
    else:
        print("  noise: NOT MEASURED. No second run of the baseline was found, so a fix or a "
              "regression on a case that flips by itself cannot be told from a real one. "
              "Fix: run the baseline once more with nothing changed, or pass --aa <run>")
    if r["testCasesLeftOut"]:
        print(f"  {r['testCasesLeftOut']} case(s) on the test split left out of every rule")
    if r["setAside"]:
        print(f"  set aside (question or key changed since the baseline): {', '.join(r['setAside'])}")
    if r["doubtedGoldens"]:
        print(f"  HALT: the judge doubts the key on {', '.join(r['doubtedGoldens'])}. "
              f"Settle each through the golden side door first (golden-side-door.md).")
    out = cfg.workdir() / "acceptances"
    out.mkdir(parents=True, exist_ok=True)
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    (out / f"{stamp}.json").write_text(json.dumps(r, indent=1) + "\n")
    if not a.no_record:
        diff = None
        if a.patch:
            try:
                diff = a.patch.read_text()
            except OSError as e:
                print(f"  ! --patch {a.patch} could not be read ({e}); the attempt is "
                      f"recorded without its edit", file=sys.stderr)
        where = attempts.append(cfg.set_dir, attempts.row(
            what=a.what, outcome="accepted" if r["accepted"] else "rejected",
            baseline=r["baseline"], aa=r["aa"], candidates=r["candidates"],
            targets=r["targets"] + r["unstableTargets"], issue_ids=a.issue,
            failed={k: v["detail"] for k, v in r["rules"].items() if not v["ok"]}
            | ({"doubted goldens": ", ".join(r["doubtedGoldens"])} if r["doubtedGoldens"] else {}),
            effect={"targets": len(r["targets"]), "targetsFixed": len(r["targetsFixed"]),
                    "regressions": r["regressions"], "unstable": len(r["unstable"])},
            diff=diff, lesson=a.lesson))
        print(f"  recorded in {where}")
    if r["accepted"]:
        print("\nevery rule holds. Checkpoint with:\n")
        print(commit_message(r, a.what))
        return 0
    print("\nnot accepted: fix the failing rule, or revert the edit.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
