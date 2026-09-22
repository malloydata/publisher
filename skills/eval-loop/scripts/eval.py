#!/usr/bin/env python3
"""One entry point for a local evaluation run. Stdlib only.

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

VERBS = ("serve", "verify", "run", "diagnose", "package")


def flag_value(args: list[str], flag: str) -> str | None:
    for i, x in enumerate(args):
        if x == flag and i + 1 < len(args):
            return args[i + 1]
        if x.startswith(flag + "="):
            return x.split("=", 1)[1]
    return None


def run_dir_from_label(args: list[str]) -> list[str]:
    """`--label L` becomes `--run <workdir>/runs/L` for a verb that reads a run."""
    label = flag_value(args, "--label")
    if label is None or flag_value(args, "--run") is not None:
        return args
    cfg = config.load(pathlib.Path(flag_value(args, "--set")))
    out, skip = [], False
    for i, x in enumerate(args):
        if skip:
            skip = False
            continue
        if x == "--label":
            skip = True
            continue
        if x.startswith("--label="):
            continue
        out.append(x)
    return [*out, "--run", str(cfg.workdir() / "runs" / label)]


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv or argv[0] in ("-h", "--help") or argv[0] not in VERBS:
        print(__doc__)
        return 0 if argv and argv[0] in ("-h", "--help") else 2
    verb, rest = argv[0], argv[1:]
    if verb == "serve":
        if not rest or rest[0] not in ("model", "truth"):
            print("usage: eval.py serve model|truth --set <set-dir> [serve.py flags]")
            return 2
        rest = ["--role", rest[0], *rest[1:]]
    if flag_value(rest, "--set") is None:
        print(f"eval.py {verb}: --set <set-dir> is required")
        return 2

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
