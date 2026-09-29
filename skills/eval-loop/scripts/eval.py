#!/usr/bin/env python3
"""One entry point for a local evaluation run. Stdlib only.

  python3 eval.py check    --set <set-dir>         # every gap in the set, before anything starts
  python3 eval.py serve model --set <set-dir>      # the server the answerer queries
  python3 eval.py serve truth --set <set-dir>      # the answer key's server
  python3 eval.py verify   --set <set-dir>         # goldens still re-derive
  python3 eval.py run      --set <set-dir> --label baseline-01
  python3 eval.py diagnose --set <set-dir> --label baseline-01
  python3 eval.py package  --set <set-dir> --label baseline-01

Each verb runs one script's `main` with the arguments given, after `--set`
has picked up the set's eval.toml (skills/eval-answer/scripts/config.py). Any
flag the script takes passes through, so `run --only q3 --no-judge` works.

`--label` names a run the same way for every verb: `run` writes it to
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

VERBS = ("check", "serve", "verify", "run", "diagnose", "package")


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
    if verb == "verify":
        import verify_goldens
        return verify_goldens.main(rest)
    if verb == "run":
        import run_baseline
        return run_baseline.main(rest)
    if verb == "diagnose":
        import diagnose
        return diagnose.main(run_dir_from_label(rest))
    import build_run_package
    return build_run_package.main(run_dir_from_label(rest))


if __name__ == "__main__":
    sys.exit(main())
