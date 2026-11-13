#!/usr/bin/env python3
"""One entry point for a local evaluation run. Stdlib only.

  python3 eval.py check    --set <set-dir>         # every gap in the set, before anything starts
  python3 eval.py serve model --set <set-dir>      # the server the answerer queries
  python3 eval.py serve truth --set <set-dir>      # the answer key's server
  python3 eval.py publish  --set <set-dir>         # an edit, served and built
  python3 eval.py experiment start tip-fix --set <set-dir>   # a variant beside it
  python3 eval.py verify   --set <set-dir>         # goldens still re-derive
  python3 eval.py run      --set <set-dir> --label baseline-01
  python3 eval.py replay   --set <set-dir> --label replay-01   # retrieval only, no answerer
  python3 eval.py query-replay --set <set-dir> --from baseline-01 --label perf-01   # time passing queries
  python3 eval.py accept   --set <set-dir> --run fix-01 --run fix-02   # the acceptance rules
  python3 eval.py diagnose --set <set-dir> --label baseline-01
  python3 eval.py improve  --set <set-dir> --label baseline-01 --experiment fix1   # one edit per cluster
  python3 eval.py package  --set <set-dir> --label baseline-01
  python3 eval.py history  --set <set-dir>         # every run as one servable page
  python3 eval.py routine --kind quality --set <set-dir>   # check, verify, run, diagnose, package, history, share
  python3 eval.py routine --kind speed   --set <set-dir>   # check, time the stored queries, diagnose, package, history, share
  python3 eval.py push     --set <set-dir> --label baseline-01   # share a run ([store])
  python3 eval.py pull     --set <set-dir>         # every shared run missing here
  python3 eval.py runs     --set <set-dir>         # every run of the set, by kind and version
  python3 eval.py versions --set <set-dir>         # how each package version scored
  python3 eval.py export   --set <set-dir> --label baseline-01   # a run as CSV tables to share
  python3 eval.py import-run --set <set-dir> --from <dir>         # a run rebuilt from those tables

Each verb runs one script's `main` with the arguments given, after `--set`
has picked up the set's eval.toml (skills/eval-answer/scripts/config.py). Any
flag the script takes passes through, so `run --only q3 --no-judge` works.

`--label` names a run the same way for every verb: `run` and `replay` write it to
<workdir>/runs/<label>, and `diagnose` and `package` read it from there
unless `--run` is given.

There is no default verb and no sequencing. Each step is a separate command
the caller chooses to run.
"""
from __future__ import annotations

import pathlib
import sys

SKILLS = pathlib.Path(__file__).resolve().parent.parent.parent
for d in ("eval-answer", "eval-loop", "eval-diagnose"):
    sys.path.insert(0, str(SKILLS / d / "scripts"))

import config  # noqa: E402

VERBS = ("check", "serve", "publish", "experiment", "verify", "run", "replay",
         "query-replay", "accept", "diagnose", "improve", "package", "history", "push", "pull",
         "routine", "runs", "versions", "export", "import-run")


def flag_value(args: list[str], flag: str) -> str | None:
    for i, x in enumerate(args):
        if x == flag and i + 1 < len(args):
            return args[i + 1]
        if x.startswith(flag + "="):
            return x.split("=", 1)[1]
    return None


def run_dir_from_label(args: list[str]) -> list[str]:
    """`--label L` becomes `--run <workdir>/runs/L` for a verb that reads a run.

    The label is always taken out: `diagnose` and `package` have no `--label`,
    so one left in is an argparse error. With `--run` also given, `--run` wins.
    """
    given = sum(x == "--label" or x.startswith("--label=") for x in args)
    if given > 1:
        # Only one run can come from a label; the rest would be dropped.
        raise SystemExit("Invalid --label: expected one run name, got "
                         f"{given}. Fix: name each run with --run for an A/B, "
                         "e.g. --run <workdir>/runs/a --run <workdir>/runs/b")
    label = flag_value(args, "--label")
    if label is None:
        if "--label" in args:
            raise SystemExit("Invalid --label: expected a run name, got none. "
                             "Fix: --label baseline-01")
        return args
    if not label:
        # An empty name would point at the runs/ directory itself.
        raise SystemExit("Invalid --label: expected a run name, got an empty "
                         "value. Fix: --label baseline-01")
    out, skip = [], False
    for x in args:
        if skip:
            skip = False
            continue
        if x == "--label":
            skip = True
            continue
        if x.startswith("--label="):
            continue
        out.append(x)
    if flag_value(out, "--run") is not None:
        return out
    cfg = config.load(pathlib.Path(flag_value(out, "--set")))
    return [*out, "--run", str(cfg.workdir() / "runs" / label)]


def drop_flag(args: list[str], flag: str) -> list[str]:
    """`args` without `flag` and its value, in either spelling."""
    out, skip = [], False
    for x in args:
        if skip:
            skip = False
        elif x == flag:
            skip = True
        elif not x.startswith(flag + "="):
            out.append(x)
    return out


def with_experiment(verb: str, args: list[str]) -> list[str]:
    """`--experiment <name>` becomes the flags that point a verb at it."""
    name = flag_value(args, "--experiment")
    if name is None:
        return args
    import experiment
    cfg = config.load(pathlib.Path(flag_value(args, "--set")))
    return [*drop_flag(args, "--experiment"), *experiment.run_flags(cfg, name, verb)]


def record_baseline(args: list[str], kind: str) -> list[str]:
    """`--baseline record` becomes the newest record run's directory, after
    pulling anything new from the store."""
    if flag_value(args, "--baseline") != "record":
        return args
    import run_index
    import run_store
    cfg = config.load(pathlib.Path(flag_value(args, "--set")))
    run_store.sync(cfg)
    r = run_index.record(run_index.set_runs(cfg), kind)
    if r is None:
        raise SystemExit(f"--baseline record: no complete main-line {kind} run in "
                         f"{cfg.workdir() / 'runs'} or the store. Fix: run the whole set once")
    print(f"baseline: {r['run']} (the newest record)")
    return [*drop_flag(args, "--baseline"), "--baseline", r["path"]]


def run_then_share(verb: str, args: list[str], main) -> int:
    """Run an answer run or a query replay, then share it if it is a record."""
    share, hold = "--share" in args, "--no-share" in args
    args = [x for x in args if x not in ("--share", "--no-share")]
    code = main(args)
    label, out = flag_value(args, "--label"), flag_value(args, "--out")
    # `--no-share` is the routine's: it shares the run itself, after the
    # diagnosis is written into it. A shared run never changes, so sharing
    # here left the store's copy without one.
    if code == 0 and (label or out) and not hold:
        import run_store
        cfg = config.load(pathlib.Path(flag_value(args, "--set")))
        run_store.after_run(cfg, pathlib.Path(out) if out else cfg.workdir() / "runs" / label,
                            share)
    return code


def pull_shared(args: list[str]) -> None:
    """Pull the runs the store holds and the workdir does not, before a verb
    that reads every run. Without it `runs`, `versions` and `history` showed
    one laptop's runs under a heading that reads as the team's. A store that
    cannot be reached is said and skipped: the local runs are still worth
    listing."""
    import run_store
    try:
        run_store.sync(config.load(pathlib.Path(flag_value(args, "--set"))))
    except SystemExit as e:
        print(f"  note: the store was not read, so only the runs here are shown ({e})",
              file=sys.stderr)


CANNOT_RUN = 3


def main(argv: list[str] | None = None) -> int:
    """Exit 3 for a set whose config cannot be read, as every script does:
    nothing was checked or run, which is not the same as a check failing."""
    try:
        return dispatch(list(sys.argv[1:] if argv is None else argv))
    except config.ConfigError as e:
        print(e, file=sys.stderr)
        return CANNOT_RUN


def dispatch(argv: list[str]) -> int:
    if not argv or argv[0] in ("-h", "--help") or argv[0] not in VERBS:
        print(__doc__)
        return 0 if argv and argv[0] in ("-h", "--help") else 2
    verb, rest = argv[0], argv[1:]
    # `<verb> --help` is the script's own help, which holds every flag.
    asks_help = any(x in ("-h", "--help") for x in rest)
    if verb == "serve" and asks_help and (not rest or rest[0] not in ("model", "truth")):
        rest = ["model", *rest]
    if verb == "serve":
        if not rest or rest[0] not in ("model", "truth"):
            print("usage: eval.py serve model|truth --set <set-dir> [serve.py flags]")
            return 2
        rest = ["--role", rest[0], *rest[1:]]
    if flag_value(rest, "--set") is None and not asks_help:
        print(f"eval.py {verb}: --set <set-dir> is required")
        return 2

    if verb == "check":
        import check_set
        return check_set.main(rest)
    if verb == "serve":
        import serve
        return serve.main(rest)
    if verb == "publish":
        import publish
        return publish.main(rest)
    if verb == "experiment":
        import experiment
        return experiment.main(rest)
    if verb == "accept":
        import accept
        return accept.main(rest)
    if verb == "history":
        import build_history
        pull_shared(rest)
        return build_history.main(rest)
    if verb == "routine":
        import routine
        return routine.main(rest)
    if verb in ("run", "replay", "query-replay"):
        rest = with_experiment(verb, rest)
    if verb == "verify":
        import verify_goldens
        return verify_goldens.main(rest)
    if verb == "run":
        import run_baseline
        return run_then_share(verb, rest, run_baseline.main)
    if verb == "replay":
        import retrieval_replay
        return retrieval_replay.main(rest)
    if verb == "query-replay":
        import query_replay
        return run_then_share(verb, record_baseline(rest, "perf"), query_replay.main)
    if verb in ("push", "pull"):
        import run_store
        return run_store.main([verb, *run_dir_from_label(rest)])
    if verb == "runs" and "--store" in rest:
        import run_store
        cfg = config.load(pathlib.Path(flag_value(rest, "--set")))
        if run_store.kind(cfg) and not run_store.can_list(cfg):
            print(run_store.NO_LIST.format(hint=cfg.file_hint), file=sys.stderr)
            return 1
        held = run_store.listed(cfg)
        print("\n".join(held) if held else f"the store holds no runs of {cfg.set_name}"
              if run_store.kind(cfg) else f"no [store] in {cfg.file_hint}")
        return 0
    if verb == "export":
        import export_run
        return export_run.main(run_dir_from_label(rest))
    if verb == "import-run":
        import import_run
        return import_run.main(rest)
    if verb in ("runs", "versions"):
        import run_index
        pull_shared(rest)
        return run_index.main([verb, *rest])
    if verb == "improve":
        sys.path.insert(0, str(SKILLS / "eval-improve" / "scripts"))
        import improve
        return improve.main(run_dir_from_label(rest))
    if verb == "diagnose":
        import diagnose
        return diagnose.main(run_dir_from_label(rest))
    import build_run_package
    return build_run_package.main(run_dir_from_label(rest))


if __name__ == "__main__":
    sys.exit(main())
