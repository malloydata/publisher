#!/usr/bin/env python3
"""Serve a variant of the model beside the main one, so it can be measured
without replacing it. Stdlib only.

  python3 eval.py experiment start tip-fix --set <set-dir>              # the working tree now
  python3 eval.py experiment start tip-fix --set <set-dir> --from <ref> # a commit or branch
  python3 eval.py experiment start tip-fix --set <set-dir> --dir <pkg>  # any package directory
  python3 eval.py run   --set <set-dir> --experiment tip-fix --slice tip --label tip-01
  python3 eval.py query-replay --set <set-dir> --experiment tip-fix --from baseline-01 ...
  python3 eval.py experiment sync tip-fix --set <set-dir> --dir <pkg>   # serve <pkg> as it is NOW
  python3 eval.py experiment list --set <set-dir>
  python3 eval.py experiment stop tip-fix --set <set-dir>

An experiment is the package published again under the name
`<package>.<name>` on the same model server. The main package keeps serving
untouched, so the stored baseline stays the baseline, and several experiments
can be answered at once. A run told `--experiment <name>` is pointed at that
package (`--package`) and pins that copy's model (`--model-repo`), and compares
against the main line's runs like any other edit.

Why a second package name rather than a second version: a local Publisher's
MCP tools answer from a package's `latest` and cannot be told a version, so
two versions of one package cannot be answered side by side. On a hosted
target, where they can, publish versions instead.

Where the variant comes from:
- default: a copy of the package directory as it is now, uncommitted edits
  included. Files over 1 MB (data) are linked, not copied.
- `--from <ref>`: a git worktree of that commit. Files git ignores in the main
  package (data that is never committed) are linked in.
- `--dir <path>`: that directory, as it is.
Copies and worktrees live in <workdir>/experiments/<name>/, never in the repo.

`sync` is for a directory that is still being edited. It publishes the
directory as the experiment, replacing what the experiment served before, and
starts it if it was not started. A published package never changes, so this is
how an edit in progress reaches a server that publishes versions: the improve
step's agent runs it after each edit to probe what it wrote. It prints the
package's `sourceContentSha`, which changes when the served content did.
"""
from __future__ import annotations

import argparse
import datetime
import json
import os
import pathlib
import shutil
import subprocess
import sys
import urllib.parse
from typing import Any

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent.parent / "eval-answer" / "scripts"))
sys.path.insert(0, str(HERE))

import config  # noqa: E402
import publish  # noqa: E402

LINK_OVER_BYTES = 1_000_000


def home(cfg: config.Config) -> pathlib.Path:
    return cfg.workdir() / "experiments"


def record_path(cfg: config.Config, name: str) -> pathlib.Path:
    return home(cfg) / f"{name}.json"


def load(cfg: config.Config, name: str) -> dict[str, Any]:
    p = record_path(cfg, name)
    if not p.exists():
        have = sorted(x.stem for x in home(cfg).glob("*.json")) if home(cfg).is_dir() else []
        raise SystemExit(f"no experiment {name!r}. Started: {', '.join(have) or 'none'}. "
                         f"Fix: eval.py experiment start {name} --set {cfg.set_dir}")
    return json.loads(p.read_text())


def git(cwd: pathlib.Path, *args: str) -> str | None:
    try:
        return subprocess.run(["git", "-C", str(cwd), *args], check=True,
                              capture_output=True, text=True).stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None


def snapshot(src: pathlib.Path, dst: pathlib.Path) -> None:
    """Copy a package directory; link files over LINK_OVER_BYTES instead."""
    for path in src.rglob("*"):
        rel = path.relative_to(src)
        if any(part.startswith(".") for part in rel.parts) or path.is_dir():
            continue
        out = dst / rel
        out.parent.mkdir(parents=True, exist_ok=True)
        if path.stat().st_size > LINK_OVER_BYTES:
            os.symlink(path.resolve(), out)
        else:
            shutil.copy2(path, out)


def worktree(src: pathlib.Path, ref: str, dst: pathlib.Path) -> pathlib.Path:
    """A worktree of `ref` at dst; returns the package directory inside it."""
    top = git(src, "rev-parse", "--show-toplevel")
    if not top:
        raise SystemExit(f"--from needs {src} to be in a git repository")
    sub = src.resolve().relative_to(pathlib.Path(top).resolve())
    if git(pathlib.Path(top), "worktree", "add", "--detach", str(dst), ref) is None:
        raise SystemExit(f"could not check out {ref!r} into {dst}: is it a commit or branch?")
    pkg = dst / sub
    ignored = git(src, "ls-files", "--others", "--ignored", "--exclude-standard", ".") or ""
    for rel in filter(None, ignored.splitlines()):
        out = pkg / rel
        if not out.exists():
            out.parent.mkdir(parents=True, exist_ok=True)
            os.symlink((src / rel).resolve(), out)
    return pkg


def start(cfg: config.Config, name: str, ref: str | None, given: pathlib.Path | None,
          build: bool) -> dict[str, Any]:
    if ("--" in name or "__" in name or name.startswith("-")
            or not name.replace("-", "").replace("_", "").isalnum()):
        raise SystemExit(f"Invalid experiment name {name!r}: expected letters, digits, "
                         f"and single dashes or underscores between them, e.g. tip-fix")
    pkg = cfg.need(None, "model", "package")
    env = cfg.need(None, "model", "environment")
    src = pathlib.Path(cfg.need(None, "model", "repo"))
    if record_path(cfg, name).exists():
        raise SystemExit(f"experiment {name!r} is already started. Fix: eval.py experiment "
                         f"stop {name} --set {cfg.set_dir}, or pick another name")
    dst = home(cfg) / name
    if given:
        location, how = given.resolve(), f"directory {given}"
    elif ref:
        location, how = worktree(src, ref, dst), f"git {ref}"
    else:
        snapshot(src, dst)
        location, how = dst, "working tree"
    package = publish.experiment_package(pkg, name)
    pub = publish.publish(cfg.model_publisher(), env, package, location,
                          publish.server_versioning(cfg.model_publisher()))
    rec = {"name": name, "package": package, "environment": env, "location": str(location),
           "worktree": str(dst) if ref else None, "from": how,
           "gitSha": git(src, "rev-parse", "HEAD") if not ref else git(location, "rev-parse", "HEAD"),
           "version": pub.get("version") or pub.get("baseVersion"),
           "started": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}
    home(cfg).mkdir(parents=True, exist_ok=True)
    record_path(cfg, name).write_text(json.dumps(rec, indent=1) + "\n")
    if build:
        b = publish.build(cfg.model_publisher(), env, package, pub.get("version"))
        rec["build"] = b
    return rec


def unpublish(cfg: config.Config, rec: dict[str, Any]) -> tuple[int, Any]:
    return publish.send(cfg.model_publisher(), "DELETE", f"api/v0/environments/"
                        f"{urllib.parse.quote(rec['environment'])}/packages/"
                        f"{urllib.parse.quote(rec['package'])}")


def content_sha(cfg: config.Config, rec: dict[str, Any]) -> str | None:
    _, st = publish.send(cfg.model_publisher(), "GET", f"api/v0/environments/"
                         f"{urllib.parse.quote(rec['environment'])}/packages/"
                         f"{urllib.parse.quote(rec['package'])}/status")
    return st.get("sourceContentSha") if isinstance(st, dict) else None


def sync(cfg: config.Config, name: str, location: pathlib.Path, build: bool) -> dict[str, Any]:
    """Serve `location` as experiment `name`, as the directory is now."""
    if not record_path(cfg, name).exists():
        return start(cfg, name, None, location, build)
    rec = load(cfg, name)
    code, body = unpublish(cfg, rec)
    if code >= 300 and code != 404:
        raise SystemExit(f"could not replace {rec['package']}: the server kept the old one, "
                         f"HTTP {code} {publish.message(body)}")
    pub = publish.publish(cfg.model_publisher(), rec["environment"], rec["package"], location,
                          publish.server_versioning(cfg.model_publisher()))
    rec.update(location=str(location.resolve()), **{"from": f"directory {location}"},
               version=pub.get("version") or pub.get("baseVersion"),
               gitSha=git(location, "rev-parse", "HEAD"))
    if build:
        rec["build"] = publish.build(cfg.model_publisher(), rec["environment"], rec["package"],
                                     pub.get("version"))
    record_path(cfg, name).write_text(json.dumps(rec, indent=1) + "\n")
    return rec


def stop(cfg: config.Config, name: str) -> None:
    rec = load(cfg, name)
    code, body = unpublish(cfg, rec)
    if code >= 300 and code != 404:
        print(f"  ! the server kept {rec['package']}: HTTP {code} {publish.message(body)}")
    if rec.get("worktree"):
        top = git(pathlib.Path(cfg.need(None, "model", "repo")), "rev-parse", "--show-toplevel")
        if top:
            git(pathlib.Path(top), "worktree", "remove", "--force", rec["worktree"])
    shutil.rmtree(home(cfg) / name, ignore_errors=True)
    record_path(cfg, name).unlink()
    print(f"stopped {name}: {rec['package']} removed from the server; runs kept")


def run_flags(cfg: config.Config, name: str, verb: str) -> list[str]:
    """What `--experiment <name>` becomes for a verb that answers or replays."""
    rec = load(cfg, name)
    flags = ["--package", rec["package"]]
    if verb == "run":
        flags += ["--model-repo", rec["location"]]
    return flags


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("action", choices=("start", "stop", "list", "sync"))
    ap.add_argument("name", nargs="?")
    ap.add_argument("--set", dest="set_dir", required=True, type=pathlib.Path)
    ap.add_argument("--from", dest="ref", default=None, help="a git commit or branch")
    ap.add_argument("--dir", type=pathlib.Path, default=None, help="a package directory")
    ap.add_argument("--no-build", action="store_true")
    a = ap.parse_args(argv)
    cfg = config.load(a.set_dir)
    if a.action == "list":
        for p in sorted(home(cfg).glob("*.json")) if home(cfg).is_dir() else []:
            r = json.loads(p.read_text())
            print(f"{r['name']:<20} {r['package']:<32} {r['from']:<24} started {r['started']}")
        return 0
    if not a.name:
        ap.error(f"{a.action} needs an experiment name")
    if a.action == "stop":
        stop(cfg, a.name)
        return 0
    if a.ref and a.dir:
        ap.error("--from and --dir name two different sources; pass one")
    if a.action == "sync":
        if not a.dir:
            ap.error("sync needs --dir <the package directory being edited>")
        rec = sync(cfg, a.name, a.dir, not a.no_build)
        publish.warm(cfg, rec["environment"], rec["package"])
        # The line the improve step's agent reads to know its edit is served.
        print(f"synced {a.name}: {rec['environment']}/{rec['package']} serves {a.dir}")
        print(f'"sourceContentSha":"{content_sha(cfg, rec)}"')
        return 0
    rec = start(cfg, a.name, a.ref, a.dir, not a.no_build)
    print(f"started {a.name}: {rec['environment']}/{rec['package']} from {rec['from']}")
    b = rec.get("build")
    if b:
        print(f"  build: {b['status']}, built {b['built'] or 0}, reused {b['reused'] or 0}, "
              f"binding {b['binding']}")
    status = publish.warm(cfg, rec["environment"], rec["package"])
    if status:
        print(f"  retrieval index: {status}")
    print(f"  measure it: eval.py run --set {cfg.set_dir} --experiment {a.name} "
          f"--slice <slice> --label {a.name}-01")
    return 0


if __name__ == "__main__":
    sys.exit(main())
