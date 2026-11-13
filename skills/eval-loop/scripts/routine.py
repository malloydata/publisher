#!/usr/bin/env python3
"""One scheduled pass of the main line, as one command. Stdlib only.

  python3 eval.py routine --kind quality --set <set-dir> [--serve] [--label L]
  python3 eval.py routine --kind speed   --set <set-dir> [--serve] [--label L]

For whatever runs the set on a schedule or after a publish (a person, CI, an
agent). There are two kinds, and each reports one number:

  quality   answers every case and scores it. The number is the pass rate.
    1. check       the set and the servers are ready
    2. verify      every golden still re-derives from the truth package
    3. run         every case, against the model server's package as served.
                   A case cut off at the turn cap, or whose isolation broke,
                   is answered once more (run --recover)
    4. diagnose    each failure, then cluster them
    5. package     the run's page, on the truth server
    6. history     the set's history page, rebuilt

  speed     times the stored queries again. The number is their total time.
    1. check
    2. query-replay  the last record replay's queries, compared with it (the
                     first time: the passing queries of the last record run)
    3. diagnose      slow, refused and unbuilt sources, with no agent
    4. package       the replay's page
    5. history

It stops at the first step that fails, so a broken server or a drifted key
never produces a number. The run is shared last, once everything about it is
written, when [store] is set and it is a record. `--serve` starts the servers
the kind needs and stops the ones it started. It publishes nothing: run
`eval.py publish` first when the model on disk is what should be measured.

What happened is written to <workdir>/routines/<label>.json, and its path is
the last line printed. The exit status says whether the number can be used:

  0  done: the number is recorded
  1  a step failed; nothing after it ran
  3  could not start (the set's config, or no run to take queries from)
  4  done, and something needs a person: a case was voided twice and left
     out, a stored answer's rows changed, or a query errored. The number is
     recorded
  5  the run did not finish, or more than half its cases are voided, so there
     is no number
"""
from __future__ import annotations

import argparse
import datetime
import json
import pathlib
import subprocess
import sys
import time
from typing import Any

HERE = pathlib.Path(__file__).resolve().parent
EVAL = HERE / "eval.py"
sys.path.insert(0, str(HERE.parent.parent / "eval-answer" / "scripts"))
sys.path.insert(0, str(HERE))

DONE, STEP_FAILED, CANNOT_START, NEEDS_A_LOOK, NO_NUMBER = 0, 1, 3, 4, 5
KINDS = ("quality", "speed")


def step(name: str, *args: str) -> int:
    print(f"\n== {name}: eval.py {' '.join(args)}", flush=True)
    return subprocess.run([sys.executable, str(EVAL), *args]).returncode


def read_json(path: pathlib.Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return {}


def quality_number(run: pathlib.Path) -> dict[str, Any]:
    """What a quality routine reports: the pass rate over the cases with a verdict."""
    import run_index
    meta = read_json(run / "run.json")
    passed, decided = run_index.score_of(run)
    return {"passed": passed, "decided": decided,
            "passRate": round(passed / decided, 4) if decided else None,
            "truncated": meta.get("truncated") or [],
            "contaminated": meta.get("contaminated") or [],
            "recovered": meta.get("recovered") or [],
            "costUsd": {"answerer": meta.get("answererCostUsd"),
                        "judge": meta.get("judgeCostUsd")}}


def speed_number(run: pathlib.Path) -> dict[str, Any]:
    """What a speed routine reports: the stored queries' time, and what moved."""
    s = read_json(run / "summary.json")
    c = s.get("compare") or {}
    return {"queries": s.get("queries"), "errors": s.get("errors"),
            "p50Ms": s.get("p50_ms"), "p95Ms": s.get("p95_ms"), "totalMs": s.get("total_ms"),
            "slow": s.get("slow") or [],
            "compare": {"baseline": c.get("baseline"),
                        "rowsChanged": c.get("rows_changed") or [],
                        "broke": c.get("broke") or [],
                        "faster": len(c.get("faster") or []),
                        "slower": len(c.get("slower") or []),
                        "totalBeforeMs": c.get("total_before_ms"),
                        "totalAfterMs": c.get("total_after_ms")} if c else None}


def speed_source(cfg) -> tuple[str | None, bool]:
    """(the run whose queries to time, whether a record replay exists to compare with)."""
    import run_index
    rows = run_index.set_runs(cfg)
    rec = run_index.record(rows, "perf")
    if rec:
        # A fixed workload: the last record replay's own queries, so the two
        # time the same work. Fresh answers word their queries differently, and
        # a replay of a new run's queries compared only 5 of 18 with the record.
        return rec["path"], True
    run = run_index.record(rows, "run")
    return (run["path"] if run else None), False


def servers(cfg) -> list[str]:
    """The servers a routine needs. The model server answers. The truth server
    re-derives goldens and serves the pages, for a set that has one."""
    return ["model", *(["truth"] if cfg.has_truth else [])]


class Routine:
    def __init__(self, a: argparse.Namespace, cfg, label: str):
        self.a, self.cfg, self.label = a, cfg, label
        self.s = ["--set", a.set_dir]
        self.run_dir = cfg.workdir() / "runs" / label
        self.started: list[str] = []
        self.result: dict[str, Any] = {
            "label": label, "kind": a.kind, "mode": "run", "set": cfg.set_name,
            "started": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
            "run": label, "steps": [], "number": None, "shared": None}

    def do(self, name: str, *args: str) -> int:
        t = time.monotonic()
        code = step(name, *args)
        self.result["steps"].append({"step": name, "exit": code,
                                     "seconds": round(time.monotonic() - t, 1)})
        return code

    def serve(self) -> int:
        import check_set
        for role in servers(self.cfg):
            if check_set.ours(self.cfg, role, self.cfg.get(role, "port")):
                continue          # already ours and up: use it, and leave it up
            code = self.do(f"serve {role}", "serve", role, *self.s)
            if code != 0:
                return code
            self.started.append(role)
        return 0

    def stop(self) -> None:
        for role in reversed(self.started):
            step(f"stop {role}", "serve", role, *self.s, "--stop")

    def share(self) -> None:
        """Last, so the shared copy holds the diagnosis. A shared run never changes."""
        import run_store
        if run_store.kind(self.cfg) is None:
            self.result["shared"] = "no [store]: the run stays in the workdir"
            return
        try:
            run_store.after_run(self.cfg, self.run_dir, False)
            held = self.label in run_store.listed(self.cfg) if run_store.can_list(self.cfg) else None
            self.result["shared"] = ("in the store" if held else "not shared: not a record"
                                     if held is False else "pushed (the store cannot list)")
        except SystemExit as e:
            self.result["shared"] = f"not shared: {e}"
            print(f"\n! {self.result['shared']}")

    def quality(self) -> tuple[int, str]:
        for name, *args in (("check", "check", *self.s), ("verify", "verify", *self.s),
                            ("run", "run", *self.s, "--label", self.label, "--no-share", "--recover",
                             "--max-turns", self.a.max_turns, "--parallel", self.a.parallel)):
            code = self.do(name, *args)
            if code != 0:
                return STEP_FAILED, f"stopped at {name} (exit {code}); nothing after it ran"
        status = read_json(self.run_dir / "run.json").get("status")
        self.result["number"] = quality_number(self.run_dir)
        if status != "complete":
            # A diagnosis of a run that did not finish reads harness faults as
            # model failures, so it is not made.
            n = self.result["number"]
            return NO_NUMBER, (f"the run is {status}: {len(n['truncated'])} truncated, "
                               f"{len(n['contaminated'])} contaminated. No number is recorded")
        for name, *args in (("diagnose", "diagnose", *self.s, "--label", self.label,
                             "--verdicts", "no_match,near_match"),
                            ("package", "package", *self.s, "--label", self.label),
                            ("history", "history", *self.s)):
            code = self.do(name, *args)
            if code != 0:
                return STEP_FAILED, f"stopped at {name} (exit {code}); nothing after it ran"
        self.share()
        n = self.result["number"]
        said = f"{n['passed']} of {n['decided']} decided cases passed"
        voided = n["truncated"] + n["contaminated"]
        if voided:
            # Each was answered twice and is still voided. The number stands
            # over the rest; a person reads why these will not finish.
            return NEEDS_A_LOOK, (f"{said}. {len(voided)} case(s) voided twice and left "
                                  f"out: {', '.join(voided)}")
        return DONE, said

    def speed(self) -> tuple[int, str]:
        code = self.do("check", "check", *self.s)
        if code != 0:
            return STEP_FAILED, f"stopped at check (exit {code}); nothing after it ran"
        source, compared = speed_source(self.cfg)
        if source is None:
            return CANNOT_START, ("no run to take queries from: a speed routine times the "
                                  "queries of passing answers. Fix: run "
                                  "`eval.py routine --kind quality` once first")
        code = self.do("query-replay", "query-replay", *self.s, "--from", source,
                       *(("--baseline", "record") if compared else ()),
                       "--label", self.label, "--no-share")
        status = read_json(self.run_dir / "run.json").get("status")
        if status is None:
            return STEP_FAILED, f"stopped at query-replay (exit {code}); it wrote no run"
        self.result["number"] = n = speed_number(self.run_dir)
        if status != "complete":
            return NO_NUMBER, (f"the replay is {status}: {n['errors']} of {n['queries']} "
                               f"queries errored. No number is recorded")
        for name, *args in (("diagnose", "diagnose", *self.s, "--label", self.label),
                            ("package", "package", *self.s, "--label", self.label),
                            ("history", "history", *self.s)):
            code = self.do(name, *args)
            if code != 0:
                return STEP_FAILED, f"stopped at {name} (exit {code}); nothing after it ran"
        self.share()
        said = f"{n['queries']} queries in {n['totalMs']} ms"
        c = n["compare"] or {}
        if c.get("rowsChanged") or c.get("broke"):
            # After an edit meant to change answers this is expected. Either way
            # a person reads it: a speed edit must not change an answer.
            return NEEDS_A_LOOK, (f"{said}. {len(c['rowsChanged'])} stored answer(s) changed "
                                  f"rows and {len(c['broke'])} now error, against {c['baseline']}")
        if n["errors"]:
            return NEEDS_A_LOOK, f"{said}. {n['errors']} query(ies) errored"
        return DONE, said

    def go(self) -> int:
        code, reason = CANNOT_START, "did not start"
        try:
            if self.a.serve and self.serve() != 0:
                code, reason = STEP_FAILED, "a server did not start"
            else:
                code, reason = self.quality() if self.a.kind == "quality" else self.speed()
        finally:
            self.stop()
            self.result.update(
                exit=code, reason=reason,
                finished=datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"))
            out = self.cfg.workdir() / "routines" / f"{self.label}.json"
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(json.dumps(self.result, indent=1) + "\n")
            print(f"\nroutine {self.label} ({self.a.kind}): {reason}. Exit {code}.\n{out}")
        return code


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--set", dest="set_dir", required=True)
    ap.add_argument("--kind", required=True, choices=KINDS,
                    help="quality: answer and score every case. speed: time the stored queries")
    ap.add_argument("--label", default=None,
                    help="default: <kind>-<UTC date and time>")
    ap.add_argument("--serve", action="store_true",
                    help="start the servers this kind needs, and stop the ones it started")
    ap.add_argument("--max-turns", default="40")
    ap.add_argument("--parallel", default="4")
    a = ap.parse_args(argv)
    import config
    cfg = config.load(pathlib.Path(a.set_dir))
    label = a.label or f"{a.kind}-" + datetime.datetime.now(
        datetime.timezone.utc).strftime("%Y%m%d-%H%M")
    return Routine(a, cfg, label).go()


if __name__ == "__main__":
    sys.exit(main())
