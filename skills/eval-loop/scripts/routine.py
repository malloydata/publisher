#!/usr/bin/env python3
"""One full pass of the main line, as one command. Stdlib only.

  python3 eval.py routine --set <set-dir> [--label nightly-20261111] [--no-replay]

For whatever runs the set on a schedule or after a publish (a person, CI, an
agent): it runs each step `eval.py` already has, in order, and stops at the
first one that fails, so a broken server or a drifted key never produces a
score.

  1. check          the set and both servers are ready
  2. verify         every golden still re-derives from the truth package
  3. run            every case, against the model server's package as served
  4. diagnose       each failure, then cluster them (an all-pass run records
                    an empty diagnosis)
  5. package        the run's page, on the truth server
  6. query-replay   the last record replay's queries, timed again and compared
                    with it (the first time, this run's passing queries)
  7. history        the set's history page, rebuilt

The run and the replay are full runs of the main line, so with a [store] set
they share themselves (run_store.py). It publishes nothing: run
`eval.py publish` first when the model on disk is what should be measured.
Costs what a full run and its diagnosis cost; the replay costs server time only.
"""
from __future__ import annotations

import argparse
import datetime
import pathlib
import subprocess
import sys

HERE = pathlib.Path(__file__).resolve().parent
EVAL = HERE / "eval.py"
sys.path.insert(0, str(HERE.parent.parent / "eval-answer" / "scripts"))
sys.path.insert(0, str(HERE))


def step(name: str, *args: str) -> int:
    print(f"\n== {name}: eval.py {' '.join(args)}", flush=True)
    return subprocess.run([sys.executable, str(EVAL), *args]).returncode


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--set", dest="set_dir", required=True)
    ap.add_argument("--label", default=None,
                    help="default: routine-<UTC date and time>")
    ap.add_argument("--max-turns", default="40")
    ap.add_argument("--parallel", default="4")
    ap.add_argument("--no-replay", action="store_true")
    a = ap.parse_args(argv)
    label = a.label or "routine-" + datetime.datetime.now(
        datetime.timezone.utc).strftime("%Y%m%d-%H%M")
    s = ["--set", a.set_dir]
    steps = [("check", "check", *s),
             ("verify", "verify", *s),
             ("run", "run", *s, "--label", label, "--max-turns", a.max_turns,
              "--parallel", a.parallel),
             ("diagnose", "diagnose", *s, "--label", label,
              "--verdicts", "no_match,near_match"),
             ("package", "package", *s, "--label", label)]
    if not a.no_replay:
        import config
        import run_index
        rec = run_index.record(run_index.set_runs(config.load(pathlib.Path(a.set_dir))), "perf")
        # A fixed workload: the last record replay's own queries, so the two
        # time the same work. Fresh answers word their queries differently, and
        # a replay of this run's queries compared only 5 of 18 with the record.
        # The first replay of a set takes this run's queries.
        source = rec["path"] if rec else label
        steps.append(("query-replay", "query-replay", *s, "--from", source,
                      *(("--baseline", "record") if rec else ()),
                      "--label", f"{label}-perf"))
    steps.append(("history", "history", *s))
    for name, *args in steps:
        code = step(name, *args)
        if code == 1 and name == "query-replay":
            # Blocked: a stored query's rows differ from the last record replay.
            # After an edit meant to change answers that is expected; either way
            # it is news for whoever reads the routine, not a reason to stop.
            print("\n! answers changed since the last record replay: listed above. "
                  "Expected after an edit that changes answers; otherwise read them first.")
            continue
        if code != 0:
            print(f"\nroutine stopped at {name} (exit {code}); nothing after it ran.")
            return code
    print(f"\nroutine {label}: done. `eval.py versions --set {a.set_dir}` shows where it lands.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
