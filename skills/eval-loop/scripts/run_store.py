#!/usr/bin/env python3
"""Share finished runs, and get them back. Stdlib only.

  python3 eval.py push --set <set-dir> --label baseline-01   # one run, to the store
  python3 eval.py pull --set <set-dir> [--label baseline-01]  # one run, or every run missing here
  python3 eval.py runs --set <set-dir> --store                # what the store holds

The store is where a team keeps runs, set in eval.toml. Unset, runs stay in
the workdir and nothing here is needed:

  [store]
  path = "/Volumes/team/evals"      # a shared drive, a synced folder, a mounted bucket

  [store]                           # anything else: commands, run by the shell
  push = "aws s3 sync {run} s3://team-evals/{set}/runs/{run_id}"
  pull = "aws s3 sync s3://team-evals/{set}/runs/{run_id} {dest}"
  list = "aws s3 ls s3://team-evals/{set}/runs/ | awk '{{print $2}}' | tr -d /"

The rules are the same whichever it is:

- What is stored is the whole run directory, as the workdir holds it: run.json,
  events.jsonl, the transcripts. Every other form (the run page, the history
  page, the CSV export) is built from it.
- A run is pushed once it is complete and never changes after. Pushing a run
  the store already holds with the same content does nothing; with other
  content it is refused. Two runs are two directories, so nothing appends.
- The store holds the answer key (goldens are in the transcripts and the
  verdicts), so it must not be readable by the answerer, and it is never the
  model's git repository: `check` refuses a store inside it.
- `eval.py run` pushes a full run of the main line by itself when a store is
  set, and `query-replay` pushes a replay of the main line. Those are the
  team's record. A slice, an experiment or a partial replay stays local unless
  run with `--share`. What counts as a record is one rule,
  `run_index.not_a_record`, and `--baseline record` reads the same rule.
- A command store checks its `list` before pushing, and pulls a run listed
  under the same name to compare it. Two machines pushing one name at the
  same moment can still both pass that check; only a push command that
  refuses to overwrite closes it.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import shlex
import shutil
import subprocess
import sys
import tempfile

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent.parent / "eval-answer" / "scripts"))
sys.path.insert(0, str(HERE))

import config  # noqa: E402


def kind(cfg: config.Config) -> str | None:
    if cfg.get("store", "path"):
        return "folder"
    if cfg.get("store", "push") or cfg.get("store", "pull"):
        return "command"
    return None


def content_sha(run: pathlib.Path) -> str:
    """What makes two copies of a run the same run: its record, not its transcripts."""
    h = hashlib.sha256()
    for f in ("run.json", "events.jsonl", "results.jsonl"):
        if (run / f).exists():
            h.update(f.encode() + b"\0" + (run / f).read_bytes())
    return h.hexdigest()


def folder(cfg: config.Config) -> pathlib.Path:
    return pathlib.Path(cfg.get("store", "path")) / cfg.set_name / "runs"


def shell(template: str, **values: str) -> str:
    try:
        cmd = template.format(**{k: shlex.quote(v) for k, v in values.items()})
    except (KeyError, IndexError, ValueError) as e:
        # `awk '{print $2}'` reads as a placeholder named `print $2`.
        raise SystemExit(f"Invalid [store] command {template!r}: expected only "
                         f"{', '.join('{' + k + '}' for k in values)} in braces, got {e!r}. "
                         f"Fix: double a literal brace, as in awk '{{{{print $2}}}}'") from None
    p = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if p.returncode != 0:
        raise SystemExit(f"store command failed ({p.returncode}): {cmd}\n{p.stderr.strip()[:800]}")
    return p.stdout


def push(cfg: config.Config, run: pathlib.Path) -> str:
    """Copy a complete run to the store. Returns what happened, in words."""
    meta = json.loads((run / "run.json").read_text())
    if meta.get("status") != "complete":
        raise SystemExit(f"{run.name} is {meta.get('status')!r}, not complete: only a finished "
                         f"run is shared, because a shared run never changes")
    k = kind(cfg)
    if k is None:
        raise SystemExit(f"no [store] in {cfg.file_hint}: runs stay in {cfg.workdir() / 'runs'}. "
                         f"Fix: add [store] path = \"<shared folder>\" (or push/pull commands)")
    if k == "command":
        if not cfg.get("store", "push"):
            raise SystemExit(f"[store] in {cfg.file_hint} has no push command")
        # A push command overwrites whatever is under the name (`aws s3 sync`
        # does), so the name is checked first. Without `list` it cannot be.
        if not can_list(cfg):
            raise SystemExit("nothing pushed: " + NO_LIST.format(hint=cfg.file_hint)
                             + ". A shared run never changes, and without a list the "
                               "store cannot say whether the name is taken")
        if run.name in listed(cfg):
            return held_already(cfg, run)
        shell(cfg.get("store", "push"), run=str(run), run_id=run.name, set=cfg.set_name)
        return f"pushed {run.name} with the store's push command"
    dest = folder(cfg) / run.name
    if dest.exists():
        if content_sha(dest) == content_sha(run):
            return f"{run.name} is already in the store"
        raise SystemExit(f"the store already holds a different run named {run.name}. A shared "
                         f"run never changes. Fix: run again under another --label")
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = pathlib.Path(tempfile.mkdtemp(prefix=f".{run.name}-", dir=dest.parent))
    shutil.copytree(run, tmp, dirs_exist_ok=True, ignore=shutil.ignore_patterns("export"))
    tmp.rename(dest)          # whole or not at all: a reader never sees half a run
    return f"pushed {run.name} to {dest}"


def held_already(cfg: config.Config, run: pathlib.Path) -> str:
    """The command store lists a run under this name: the same run, or refused."""
    if not cfg.get("store", "pull"):
        raise SystemExit(f"the store already holds a run named {run.name}, and with no pull "
                         f"command in {cfg.file_hint} it cannot be compared. A shared run never "
                         f"changes. Fix: run again under another --label")
    with tempfile.TemporaryDirectory() as t:
        there = pathlib.Path(t) / run.name
        shell(cfg.get("store", "pull"), run_id=run.name, dest=str(there), set=cfg.set_name)
        if content_sha(there) != content_sha(run):
            raise SystemExit(f"the store already holds a different run named {run.name}. A "
                             f"shared run never changes. Fix: run again under another --label")
    return f"{run.name} is already in the store"


def can_list(cfg: config.Config) -> bool:
    """Can the store say what it holds? A command store needs a `list` command."""
    return kind(cfg) == "folder" or bool(kind(cfg) == "command" and cfg.get("store", "list"))


NO_LIST = ("[store] in {hint} has no list command, so the store cannot say what it "
           "holds and nothing is pulled from it. Fix: add list = \"<a command that prints "
           "one run id per line>\", or pull one run by name with --label")


def listed(cfg: config.Config) -> list[str]:
    k = kind(cfg)
    if k == "folder":
        d = folder(cfg)
        # A dot name is a push still copying, or one that crashed: never a run.
        return sorted(p.name for p in d.iterdir() if not p.name.startswith(".")
                      and (p / "run.json").exists()) if d.is_dir() else []
    if k == "command" and cfg.get("store", "list"):
        return sorted(filter(None, (x.strip() for x in
                                    shell(cfg.get("store", "list"), set=cfg.set_name).splitlines())))
    return []


def pull(cfg: config.Config, run_id: str) -> str:
    dest = cfg.workdir() / "runs" / run_id
    if dest.exists():
        return f"{run_id} is already here"
    k = kind(cfg)
    if not (k == "folder" or (k == "command" and cfg.get("store", "pull"))):
        raise SystemExit(f"no way to pull: [store] in {cfg.file_hint} has no path or pull command")
    dest.parent.mkdir(parents=True, exist_ok=True)
    # Copied beside the runs under a dot name, then renamed: `dest` exists only
    # whole, so a pull that fails leaves nothing and the next one tries again.
    tmp = pathlib.Path(tempfile.mkdtemp(prefix=f".pull-{run_id}-", dir=dest.parent))
    try:
        got = tmp / run_id
        if k == "folder":
            src = folder(cfg) / run_id
            if not (src / "run.json").exists():
                raise SystemExit(f"the store has no run {run_id} ({folder(cfg)})")
            shutil.copytree(src, got)
        else:
            shell(cfg.get("store", "pull"), run_id=run_id, dest=str(got), set=cfg.set_name)
            if not (got / "run.json").exists():
                raise SystemExit(f"the pull command ran but left no run.json for {run_id}")
        got.rename(dest)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    return f"pulled {run_id}"


def sync(cfg: config.Config, quiet: bool = False) -> list[str]:
    """Pull every run the store holds that the workdir does not. Returns their ids."""
    if kind(cfg) is None:
        return []
    if not can_list(cfg):
        # Said every time, on stderr. Returning nothing in silence made
        # `--baseline record` compare against local runs while reading as if it
        # had checked the team's.
        print("  note: " + NO_LIST.format(hint=cfg.file_hint), file=sys.stderr)
        return []
    have = {p.name for p in (cfg.workdir() / "runs").iterdir()} if (cfg.workdir() / "runs").is_dir() else set()
    new = [r for r in listed(cfg) if r not in have]
    for r in new:
        pull(cfg, r)
        if not quiet:
            print(f"  pulled {r} from the store")
    return new


def after_run(cfg: config.Config, run: pathlib.Path, share: bool) -> None:
    """Push a just-finished run when it is a record, or when asked to."""
    if kind(cfg) is None or not (run / "run.json").exists():
        return
    import run_index
    why = run_index.why_not_record(cfg, run)
    if share or why is None:
        print(push(cfg, run))
    else:
        print(f"  {run.name} stays local: {why}. "
              f"Share it with: eval.py push --set {cfg.set_dir} --label {run.name}")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("action", choices=("push", "pull"))
    ap.add_argument("--set", dest="set_dir", required=True, type=pathlib.Path)
    ap.add_argument("--run", type=pathlib.Path, default=None)
    a = ap.parse_args(argv)
    cfg = config.load(a.set_dir)
    if a.action == "push":
        if not a.run:
            ap.error("push needs --label <run> (or --run <dir>)")
        print(push(cfg, a.run))
        return 0
    if a.run:
        print(pull(cfg, a.run.name))
    else:
        if kind(cfg) is not None and not can_list(cfg):
            raise SystemExit(NO_LIST.format(hint=cfg.file_hint))
        new = sync(cfg)
        print(f"{len(new)} run(s) pulled; {len(listed(cfg))} in the store")
    return 0


if __name__ == "__main__":
    sys.exit(main())
