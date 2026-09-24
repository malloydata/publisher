#!/usr/bin/env python3
"""Is this set ready to run? Every gap at once, before anything starts. Stdlib only.

  python3 eval.py check --set <set-dir>

Reads the set and its eval.toml, and asks nothing of a model. Each gap it
finds would otherwise surface on its own, from a different script, after a
server was already started: no cases.jsonl, a case the importer refuses, no
[truth] section, a truth package the model server would serve, a table path
that resolves to nothing once Publisher serves its copy, a port another
process holds.

Exit 0 when nothing blocks a run (notes may remain), 1 when something does,
2 on a usage error.
"""
from __future__ import annotations

import argparse
import json
import os
import pathlib
import sys

HERE = pathlib.Path(__file__).resolve().parent
for d in ("eval-answer", "eval-import", "eval-loop"):
    sys.path.insert(0, str(HERE.parent.parent / d / "scripts"))

import config  # noqa: E402
import import_cases  # noqa: E402
import init_truth_package  # noqa: E402
import serve  # noqa: E402

GUIDE = "skills/eval-loop/reference/setting-up-a-set.md"


def ours(cfg: config.Config, role: str, port: int) -> bool:
    """The process on `port` is the one `eval.py serve <role>` recorded."""
    pidfile = cfg.server_root(role) / "publisher.pid"
    try:
        info = json.loads(pidfile.read_text())
        os.kill(info["pid"], 0)
    except (OSError, ValueError, KeyError):
        return False
    return port in (info.get("port"), info.get("mcpPort"))


def check_cases(cfg: config.Config) -> tuple[list[str], list[str]]:
    path = cfg.set_dir / "cases.jsonl"
    if not path.exists():
        return [f"no cases.jsonl in {cfg.set_dir}. Fix: import the questions "
                f"(skill:eval-import, then `import_cases.py --set "
                f"{cfg.set_dir} --stamp`)"], []
    cases, problems = import_cases.read_cases(path)
    for n, case in enumerate(cases, start=1):
        found, _ = import_cases.check_case(case, f"cases.jsonl:{n}")
        problems += found
    if not cases and not problems:
        problems.append(f"{path} holds no cases")
    lines = sum(1 for l in path.read_text().splitlines() if l.strip())
    return problems, import_cases.summarize(cases, lines)[:4]


def check_model(cfg: config.Config) -> list[str]:
    problems = []
    for key in ("environment", "package"):
        if cfg.get("model", key) is None:
            problems.append(f"no [model] {key}. Fix: add `{key} = \"<value>\"` "
                            f"under [model] in {cfg.file_hint}")
    repo = cfg.get("model", "repo")
    if repo is None:
        problems.append(f"no [model] repo, the model package directory. Fix: "
                        f"add `repo = \"<path>\"` under [model] in {cfg.file_hint}")
    elif not (repo / "publisher.json").exists():
        problems.append(f"[model] repo {repo} has no publisher.json, so it is "
                        f"not a package. Fix: point it at the package root")
    else:
        model = cfg.set_meta.get("targetModelPath")
        if model and not (repo / model).exists():
            problems.append(f"set.json targetModelPath {model} is not in {repo}")
    return problems


def check_truth(cfg: config.Config) -> tuple[list[str], list[str]]:
    name = cfg.set_meta.get("truthPackage")
    if not name:
        return [], ["set.json names no truthPackage, so no golden holding a "
                    "value can be re-derived and verify exits 3. Fine for a "
                    "set of criteria or bare questions; otherwise see "
                    f"init_truth_package.py in {GUIDE}"]
    problems = []
    if not cfg.has_truth:
        problems.append(f"set.json names truthPackage {name!r} but "
                        f"{cfg.file_hint} has no [truth] section. Fix: add "
                        f"`[truth]` and `package_dir = \"<path>\"` under it")
        return problems, []
    leak = serve.truth_inside_model(cfg)
    if leak:
        problems.append(leak)
    where = cfg.truth_package_dir()
    if not (where / "publisher.json").exists():
        problems.append(f"no truth package at {where} (no publisher.json). "
                        f"Fix: init_truth_package.py, or set [truth] "
                        f"package_dir in {cfg.file_hint}")
        return problems, []
    for conn, ref, src in init_truth_package.table_refs(where):
        if "://" in ref or pathlib.Path(ref).is_absolute():
            continue
        if "/" not in ref and not init_truth_package.DATA_EXT.search(ref):
            continue
        # By path, not by resolved target: init_truth_package links the model's
        # data directory in, and a link whose target is outside the package is
        # what makes the ref resolve once served.
        if ".." in pathlib.PurePath(ref).parts or not (src.parent / ref).exists():
            problems.append(
                f"{src.name}: {conn}.table('{ref}') resolves to nothing once "
                f"served: Publisher serves a copy of the package, so a "
                f"relative path must stay inside it and exist there. Fix: "
                f"link or copy the data into {where}, or use an absolute path")
    return problems, []


def check_ports(cfg: config.Config) -> tuple[list[str], list[str]]:
    problems, notes = [], []
    roles = ["model"] + (["truth"] if cfg.has_truth else [])
    if cfg.has_truth:
        clash = serve.port_clash(cfg, "truth", cfg.get("truth", "port"),
                                 cfg.get("truth", "mcp_port"))
        if clash:
            problems.append(clash)
    for role in roles:
        for key in ("port", "mcp_port"):
            port = cfg.get(role, key)
            if not serve.listening(port):
                continue
            if ours(cfg, role, port):
                notes.append(f"{role} {key} {port}: served by `eval.py serve {role}`")
            else:
                problems.append(f"{role} {key} {port} is in use by another "
                                f"process. Fix: change `{key}` under [{role}] "
                                f"in {cfg.file_hint}")
    return problems, notes


def report(set_dir: pathlib.Path) -> tuple[list[str], list[str]]:
    """(problems, notes) for the set."""
    try:
        cfg = config.load(set_dir)
    except config.ConfigError as e:
        return [str(e)], []
    except json.JSONDecodeError as e:
        return [f"{set_dir / 'set.json'} is not valid JSON: {e}"], []
    problems, notes = [], []
    if not (set_dir / "set.json").exists():
        problems.append(f"no set.json in {set_dir}. Fix: see {GUIDE}")
    if cfg.path is None:
        problems.append(f"no eval.toml in {set_dir}, so every command needs its "
                        f"server flags. Fix: write one (template in {GUIDE})")
    p, n = check_cases(cfg)
    problems += p
    notes += n
    problems += check_model(cfg)
    p, n = check_truth(cfg)
    problems += p
    notes += n
    p, n = check_ports(cfg)
    problems += p
    notes += n
    if cfg.publisher_dir() is None:
        problems.append("no built Publisher in this clone. Fix: `bun install "
                        "&& bun run build`, or set [paths] publisher_dir")
    return problems, notes


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--set", dest="set_dir", required=True, type=pathlib.Path)
    a = ap.parse_args(argv)
    problems, notes = report(a.set_dir.resolve())
    for line in notes:
        print(f"  {line}")
    for line in problems:
        print(f"! {line}")
    print(f"\n{len(problems)} problem(s)" if problems
          else "\nready: nothing blocks a run")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
