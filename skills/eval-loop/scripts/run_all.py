#!/usr/bin/env python3
"""Drive a whole eval night unattended: answer, diagnose, improve, gate.

  python run_all.py --set .../evals/ecommerce --out .../runs \
      --model-dir .../ecommerce --server-root .../pub-evals

Stdlib only. Each stage is skipped if its output already exists, so a sequence
that dies at 3am resumes instead of restarting.

WHY A DRIVER RATHER THAN A SHELL SCRIPT

Three things a shell `&&` chain gets wrong overnight, all of which cost a whole
night the first time they happen:

**A failed stage must not end the run.** If improve produces nothing for
cluster 2, the gate for cluster 1 is still worth having. Stages record their
failure and the sequence continues; the summary at the end says what did not
run and why.

**Publisher has to be alive, and its failure mode is silence.** It dies when
its launching shell exits, and the next MCP request returns an empty body
rather than refusing the connection -- which the answerer records as an agent
that said nothing, so a dead server looks like 35 uniformly terrible answers.
Health is checked before every stage, and a stage whose server is down is
skipped rather than run into the ground.

**Cost has to be visible while it accrues, not after.** Each stage's spend is
read back out of the artifacts it wrote and printed as a running total.
"""
from __future__ import annotations

import argparse
import json
import pathlib
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Any

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent.parent
                       / "eval-answer" / "scripts"))
from mcp_client import tool_ok  # noqa: E402

HERE = pathlib.Path(__file__).resolve().parent
SKILLS = HERE.parent.parent


@dataclass
class Stage:
    base: str          # what --only-stages matches, e.g. "noise-band"
    name: str          # base plus the run tag, e.g. "noise-band-n1"
    cmd: list[str]
    produces: pathlib.Path | None = None
    needs_publisher: bool = True
    optional: bool = False
    status: str = "pending"
    seconds: float = 0.0
    note: str = ""
    cost: float = 0.0


def publisher_ok(mcp_url: str, environment: str, package: str) -> bool:
    """A real tool call. `/health` can answer while the package does not load,
    and it is the package that every stage actually depends on."""
    return tool_ok(mcp_url, "malloy_getContext", {
        "environmentName": environment, "packageName": package,
        "searchTargets": [{"term": "order", "entityType": "source"}]})


def spend_in(run: pathlib.Path) -> float:
    """Cost is scattered across the artifacts each stage writes; add it up."""
    total = 0.0
    ev = run / "events.jsonl"
    if ev.exists():
        for line in ev.read_text().splitlines():
            try:
                e = json.loads(line)
            except json.JSONDecodeError:
                continue
            total += e.get("cost_usd") or 0
    for pat in ("diagnoses.jsonl",):
        p = run / pat
        if p.exists():
            for line in p.read_text().splitlines():
                try:
                    total += json.loads(line).get("cost_usd") or 0
                except json.JSONDecodeError:
                    pass
    for res in (run / "artifacts").glob("clusters/*/result.json"):
        try:
            total += json.loads(res.read_text()).get("cost_usd") or 0
        except (json.JSONDecodeError, OSError):
            pass
    return total


def run_stage(s: Stage, a: argparse.Namespace, log: pathlib.Path) -> None:
    if s.produces and s.produces.exists() and not a.force:
        s.status, s.note = "skipped", f"{s.produces.name} already exists"
        print(f"  = {s.name}: {s.note}")
        return
    if s.needs_publisher and not publisher_ok(a.mcp_url, a.environment,
                                              a.package):
        s.status, s.note = "blocked", "publisher not answering"
        print(f"  ! {s.name}: {s.note}")
        return

    print(f"  > {s.name}", flush=True)
    t0 = time.time()
    with log.open("a") as fh:
        fh.write(f"\n{'=' * 70}\n{s.name}\n{' '.join(s.cmd)}\n{'=' * 70}\n")
        fh.flush()
        p = subprocess.run(s.cmd, stdout=fh, stderr=subprocess.STDOUT)
    s.seconds = time.time() - t0
    s.status = "ok" if p.returncode == 0 else "failed"
    if p.returncode != 0:
        s.note = f"exit {p.returncode}; see {log}"
    print(f"  {'.' if s.status == 'ok' else '!'} {s.name}: {s.status} "
          f"in {s.seconds / 60:.1f}m {s.note}")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--set", dest="set_dir", required=True, type=pathlib.Path)
    ap.add_argument("--out", required=True, type=pathlib.Path)
    ap.add_argument("--model-dir", required=True, type=pathlib.Path)
    ap.add_argument("--server-root", type=pathlib.Path, default=None)
    ap.add_argument("--watch-mode", action="store_true")
    ap.add_argument("--answerer", default="sonnet")
    ap.add_argument("--improver", default="opus")
    ap.add_argument("--environment", default="samples")
    ap.add_argument("--package", default="ecommerce")
    ap.add_argument("--mcp-url", default="http://localhost:4040/mcp")
    ap.add_argument("--parallel", type=int, default=4)
    ap.add_argument("--tag", default=time.strftime("%m%d-%H%M"))
    ap.add_argument("--only-stages", default=None,
                    help="comma-separated stage names")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args(argv)

    cases = [json.loads(l) for l in
             (a.set_dir / "cases.jsonl").read_text().splitlines() if l.strip()]
    dev = ",".join(c["qid"] for c in cases if c.get("split") == "dev")
    n_dev = dev.count(",") + 1

    a.out.mkdir(parents=True, exist_ok=True)
    log = a.out / f"run_all-{a.tag}.log"

    def answer(base_name: str, model: str, only: str | None) -> Stage:
        name = f"{base_name}-{a.tag}"
        run = a.out / name
        cmd = [sys.executable, str(HERE / "run_baseline.py"),
               "--set", str(a.set_dir), "--out", str(run), "--model", model,
               "--label", name, "--environment", a.environment,
               "--package", a.package, "--mcp-url", a.mcp_url,
               "--parallel", str(a.parallel)]
        if only:
            cmd += ["--only", only]
        return Stage(base_name, name, cmd, produces=run / "events.jsonl")

    # A/A first: every acceptance claim later in the night is measured against
    # the band it produces, so it is not optional and it goes first.
    aa1, aa2 = answer("aa1", a.answerer, dev), answer("aa2", a.answerer, dev)
    base = answer("baseline", a.answerer, None)

    stages = [
        aa1, aa2,
        Stage("noise-band", f"noise-band-{a.tag}",
              [sys.executable, str(HERE / "flip_table.py"),
               "--a", str(a.out / aa1.name), "--b", str(a.out / aa2.name)],
              needs_publisher=False),
        base,
        Stage("package", f"package-{a.tag}",
              [sys.executable, str(HERE / "build_run_package.py"),
               "--run", str(a.out / base.name),
               "--set", str(a.set_dir),
               "--out", str(a.out / f"pkg-{a.tag}")],
              needs_publisher=False, optional=True),
        Stage("diagnose", f"diagnose-{a.tag}",
              [sys.executable,
               str(SKILLS / "eval-diagnose" / "scripts" / "diagnose.py"),
               "--run", str(a.out / base.name), "--set", str(a.set_dir),
               "--model-dir", str(a.model_dir), "--mcp-url", a.mcp_url,
               "--environment", a.environment, "--package", a.package,
               "--parallel", str(a.parallel)],
              produces=a.out / base.name / "diagnoses.jsonl"),
        Stage("improve", f"improve-{a.tag}",
              [sys.executable,
               str(SKILLS / "eval-improve" / "scripts" / "improve.py"),
               "--run", str(a.out / base.name), "--set", str(a.set_dir),
               "--model-dir", str(a.model_dir), "--model", a.improver,
               "--mcp-url", a.mcp_url, "--environment", a.environment,
               "--package", a.package]
              + (["--watch-mode"] if a.watch_mode else [])
              + (["--server-root", str(a.server_root)] if a.server_root else [])),
    ]
    if a.only_stages:
        want = {s.strip() for s in a.only_stages.split(",")}
        unknown = want - {s.base for s in stages} - {s.name for s in stages}
        if unknown:
            raise SystemExit(
                f"no such stage: {', '.join(sorted(unknown))}. "
                f"Known: {', '.join(s.base for s in stages)}")
        stages = [s for s in stages if s.base in want or s.name in want]

    print(f"{len(stages)} stages, {n_dev} dev cases, answerer {a.answerer}, "
          f"improver {a.improver}\nlog: {log}\n")
    if a.dry_run:
        for s in stages:
            print(f"  {s.name}\n      {' '.join(s.cmd)}")
        return 0

    t0 = time.time()
    for s in stages:
        run_stage(s, a, log)
        spent = sum(spend_in(d) for d in a.out.glob(f"*{a.tag}*")
                    if d.is_dir())
        print(f"      running total ${spent:.2f}, "
              f"{(time.time() - t0) / 60:.0f}m elapsed", flush=True)

    print(f"\n{'stage':<26} {'status':<9} {'min':>6}  note")
    for s in stages:
        print(f"{s.name:<26} {s.status:<9} {s.seconds / 60:>6.1f}  {s.note}")
    bad = [s for s in stages if s.status in ("failed", "blocked")]
    print(f"\n{len(stages) - len(bad)}/{len(stages)} stages ok, "
          f"${sum(spend_in(d) for d in a.out.glob(f'*{a.tag}*') if d.is_dir()):.2f}, "
          f"{(time.time() - t0) / 60:.0f} minutes")
    if bad:
        print("did not run: " + ", ".join(f"{s.name} ({s.note})" for s in bad))
    # The gate is deliberately not a stage: it needs the noise band read off
    # the A/A result and the target qids read off the candidates, both of which
    # are judgement calls made after looking at what improve actually did.
    print(f"\nNot done automatically: the gate. Read the band from "
          f"noise-band, the targets from the candidate events, then re-answer "
          f"and run flip_table.py --targets --noise-band.")
    return 1 if any(s.status == "failed" and not s.optional
                    for s in stages) else 0


if __name__ == "__main__":
    sys.exit(main())
