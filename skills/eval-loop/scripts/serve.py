#!/usr/bin/env python3
"""Start a Publisher that outlives the shell that started it. Stdlib only.

  python3 serve.py --publisher-dir <publisher>/packages/server \\
      --server-root <scratch>/evalroot --port 4811 --mcp-port 4040 \\
      [--allow-proxy] [--trace-retrieval] [--reinit] [--wait 60] \\
      [--warm-retrieval --environment <env> --package <pkg>]

  python3 serve.py --stop --server-root <scratch>/evalroot

  python3 serve.py --role model|truth --set <set-dir> [--reinit] [--stop]

WITH --role

The set's eval.toml (skills/eval-answer/scripts/config.py) supplies the ports,
the server root (under the set's workdir) and this clone's packages/server.
The script writes the server's publisher.config.json itself: for `model`, the
[model] environment serving the package at [model] repo; for `truth`, the
[truth] environment serving set.json's truthPackage from truth-package/. It
refuses a role whose ports collide with the other role's, because the truth
server exists so the answerer cannot reach it.

WHY THIS EXISTS

The doctrine has always said "serve the model persistently, not `&` from a
shell that exits", and gave no recipe. A server launched from a shell wrapper
is SIGTERM'd with the wrapper's session when it exits; both VideoAmp servers
were lost that way once, and the answerer read the dead server's empty body as
a bad answer. This starts the server in its OWN session (setsid semantics via
start_new_session), redirects its output to a log in the server root, writes a
pidfile beside it, and waits until the REST root answers -- so the command
returns only when the server can actually take a query, and the failure mode
where both ports bind before the database initialises (Publisher's own
friction log, item 7) is caught here rather than 25 hours later.

A restart PRESERVES the server root. `--init` is passed only to seed a fresh
one, or when `--reinit` asks for a wipe, because it drops every table including
`entity_embeddings` and a restart that re-embeds silently answers the calls
arriving during the sync from lexical retrieval instead. `--reinit` is also how
a `publisher.config.json` edit takes effect, since reading that manifest is
the flag's other job; the start line says which mode it chose.

`--warm-retrieval` closes the gap between "the server answers" and "the server
answers SEMANTICALLY". The embedding sync is lazy: it is kicked by the first
call that ranks, and calls arriving while it runs are answered lexically
without recording that they were. So a run that starts measuring the moment
the REST root replies measures the lexical matcher for its first few cases and
reports the number as if it were the model's.

It drives one ranking call, then polls the package resource until
`embeddingIndex.status` reaches a terminal value and prints it. Only `ready`
licenses reading a run's discoverability findings.

The status field is the readiness signal to poll; an earlier version of this
script scraped the log for a "Synced entity embeddings" line instead and
documented that Publisher exposed nothing pollable. That was wrong twice over:
the field exists, and the scrape could never work anyway, because a server
whose cache is already current logs no sync line at all.

Caveat worth knowing: before publisher#1131, `status` could report `ready`
while the next question was still ranked lexically, because readiness was
derived from cached rows covering the current entity NAMES and rows outlive a
reload. On such a build, `ready` here is necessary but not sufficient.

--allow-proxy sets PUBLISHER_ALLOW_PROXY_CONNECTIONS=true: a `publisher`-type
connection is refused without it, and the server still reports `serving` with
load_errors=1. --trace-retrieval sets PUBLISHER_MCP_TRACE=retrieval, which
open-source Publisher does not read: it has no trace store, and attribution
reads each call's rankedSummary, copied at capture. The flag is kept so older
commands still parse.

A server started without EMBEDDING_API_KEY ranks get_context lexically, and
the start line says so. A retrieval number from it is not comparable with a
semantic run.
"""
from __future__ import annotations

import argparse
import json
import os
import pathlib
import signal
import subprocess
import sys
import time
import urllib.request

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent.parent
                       / "eval-answer" / "scripts"))
import config  # noqa: E402


def alive(port: int) -> bool:
    try:
        with urllib.request.urlopen(f"http://localhost:{port}/api/v0/projects",
                                    timeout=3) as r:
            return r.status == 200
    except Exception:  # noqa: BLE001
        return False


# `--init` only when there is no store to preserve, or when asked for.
#
# It sets `force=true` on `initializeSchema`, which calls `dropAllTables`, and
# that list includes `entity_embeddings` -- the semantic retrieval index.
# Passed on every start, as it was, each restart wiped the embedded facets and
# the next `get_context` re-embedded all of them. The cost is real money and
# ~180 log lines, but the reason this is a correctness bug rather than a slow
# one is that the sync is lazy and NON-BLOCKING: calls arriving in that window
# fall back to lexical retrieval and say nothing. A run measured across it
# reads as a mix of semantic and lexical answers with no recorded cause, which
# is how four runs came back inconclusive. Publisher's own incremental sync is
# content-addressed by `content_hash` and re-embeds only documents that
# changed; the drop is what defeats it.
#
# It cannot simply be removed, which is why it was unconditional. `--init` is
# also what makes the server read `publisher.config.json`: with it the
# environments come from the manifest, without it from the database
# (`environment_store.ts`, `reInit`). So a config edit -- a new environment, a
# changed connection -- does not take on a preserved root. Hence a fresh root
# is seeded, an existing one is kept, and the choice is RETURNED so the caller
# can print it: "I edited the config and nothing happened" is otherwise a
# silent failure with no line anywhere that names the cause.
DB_NAME = "publisher.db"


def init_decision(root: pathlib.Path, reinit: bool,
                  config_changed: bool = False) -> tuple[bool, str]:
    """Whether to pass `--init`, and the line that says why."""
    db = root / DB_NAME
    if config_changed and db.exists():
        # The server reads publisher.config.json only under --init, so a
        # config this script just rewrote would otherwise be ignored.
        return True, ("publisher.config.json changed: re-reading it with "
                      "--init; the semantic index re-embeds on the first "
                      "get_context")
    if reinit and db.exists():
        return True, (f"--reinit: dropping {DB_NAME} and re-reading "
                      f"publisher.config.json; the semantic index re-embeds "
                      f"on the first get_context")
    if not db.exists():
        return True, ("fresh server root: seeding the store from "
                      "publisher.config.json")
    return False, (f"preserving {DB_NAME} (and its embeddings); pass --reinit "
                   f"to drop it, which is also what re-reads "
                   f"publisher.config.json")


# One ranking call, unscoped, is what kicks the embedding sync.
#
# RANKING, because the marker and the sync both hang off the ranking path: a
# target with no `search_text` enumerates instead, which returns rows without
# embedding anything, so a warm-up built from one would report success having
# done nothing.
#
# UNSCOPED, because `scopes` narrowing (`source`, `model_path`, `entity_name`)
# is what the sync used to be handed as its desired row set, and it obliged by
# deleting everything outside it -- publisher#1028 fixed that, but a warm-up
# whose whole job is "make the cache whole" should not be the call that tests
# the fix.
WARM_SEARCH_TEXT = "what data is in this package"

# `indexing` is the only non-terminal one: the other three are all settled
# answers, and two of them are settled bad news.
TERMINAL_INDEX_STATES = frozenset({"ready", "cooldown", "oversize"})


def warm_arguments(environment: str, package: str) -> dict:
    """The get_context arguments for the warm-up call."""
    return {
        "search_targets": [{"target_type": "source",
                            "search_text": WARM_SEARCH_TEXT}],
        "scopes": [{"environment": environment, "package": package}],
    }


def index_status(package_payload: dict) -> str | None:
    """`embeddingIndex.status` from a package resource, or None if absent.

    Absent means the server has no embedding provider configured, which is a
    different fact from `indexing` and must not be waited on.
    """
    index = package_payload.get("embeddingIndex")
    return index.get("status") if isinstance(index, dict) else None


def _mcp_call(mcp_port: int, name: str, arguments: dict, timeout: int) -> None:
    """One MCP tools/call, response ignored -- the point is the side effect."""
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                       "params": {"name": name, "arguments": arguments}})
    req = urllib.request.Request(
        f"http://localhost:{mcp_port}/mcp", data=body.encode(),
        headers={"Content-Type": "application/json",
                 "Accept": "application/json, text/event-stream"})
    urllib.request.urlopen(req, timeout=timeout).read()


def warm_retrieval(port: int, mcp_port: int, environment: str, package: str,
                   wait: int) -> tuple[str | None, str]:
    """Kick the sync, then poll to a terminal status. Returns (status, line)."""
    try:
        _mcp_call(mcp_port, "get_context",
                  warm_arguments(environment, package), timeout=60)
    except Exception as e:  # noqa: BLE001
        return None, f"warm-up call failed: {e}"

    url = (f"http://localhost:{port}/api/v0/environments/{environment}"
           f"/packages/{package}")
    deadline = time.time() + wait
    status, last_error, read_one = None, None, False
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=10) as r:
                payload = json.loads(r.read().decode())
        except Exception as e:  # noqa: BLE001
            # A transport failure is not a capability verdict. Collapsing it
            # into `status = None` handed None a second meaning `index_status`
            # is written not to have, so a 404 from a mistyped env or package
            # reported as "this server has no embedding provider". And
            # returning on the first failure ended the warm-up inside a poll
            # loop whose whole premise is that the server may not be answering
            # cleanly yet: an early-startup 503 is the expected case here, not
            # the exotic one.
            last_error = f"{type(e).__name__}: {e}"
            time.sleep(2)
            continue
        read_one = True
        status = index_status(payload)
        if status is None:
            return None, ("no embeddingIndex on the package resource; this "
                          "server has no embedding provider, so every run "
                          "against it is LEXICAL -- do not report "
                          "discoverability findings from it")
        if status in TERMINAL_INDEX_STATES:
            break
        time.sleep(2)
    if not read_one:
        return None, (f"could not read the package resource at {url} within "
                      f"{wait}s ({last_error}). The warm-up did not run, which "
                      f"is not a finding about the server's embedding provider "
                      f"-- check the environment and package names first")
    if status == "ready":
        return status, "retrieval index ready: rankings are semantic"
    if status in TERMINAL_INDEX_STATES:
        return status, (f"retrieval index {status}: rankings are NOT semantic "
                        f"-- do not report discoverability findings")
    return status, (f"retrieval index still {status} after {wait}s; it may "
                    f"settle later, but nothing measured now is semantic")


def server_cmd(server: pathlib.Path, root: pathlib.Path, port: int,
               mcp_port: int, seed: bool) -> list[str]:
    """The argv for one Publisher, with `--init` only when seeding."""
    cmd = ["bun", "run", str(server), "--server_root", str(root),
           "--port", str(port), "--mcp_port", str(mcp_port)]
    return [*cmd, "--init"] if seed else cmd


def truth_inside_model(cfg: config.Config) -> str | None:
    """Why the set's truth package leaks through the model server, or None.

    Publisher serves every .malloy under a package directory. A truth package
    inside the model package is served by the MODEL server, as one of the
    model's own files, so both roles refuse to start over that layout.
    """
    repo, truth = cfg.get("model", "repo"), cfg.truth_package_dir()
    if not (repo and cfg.set_meta.get("truthPackage")):
        return None
    if repo in truth.resolve().parents:
        return (f"the truth package {truth} is inside the model package {repo}, "
                f"so the model server serves it to the answerer as one of the "
                f"model's own files. Fix: move it outside {repo} and set "
                f"[truth] package_dir in {cfg.file_hint}")
    return None


def role_config(cfg: config.Config, role: str) -> dict:
    """The publisher.config.json a role's server serves: one environment, one package."""
    leak = truth_inside_model(cfg)
    if leak:
        raise SystemExit(leak)
    if role == "model":
        env = cfg.need(None, "model", "environment")
        name = cfg.need(None, "model", "package")
        location = cfg.need(None, "model", "repo")
    else:
        env = cfg.get("truth", "environment")
        name = cfg.set_meta.get("truthPackage")
        if not name:
            raise SystemExit(f"{cfg.set_dir / 'set.json'} names no truthPackage, "
                             f"so there is nothing to serve as truth. "
                             f"init_truth_package.py scaffolds one.")
        location = cfg.truth_package_dir()
        if not location.is_dir():
            raise SystemExit(f"no truth package at {location}. Fix: set "
                             f"[truth] package_dir in {cfg.file_hint}")
    return {"frozenConfig": False, "environments": [
        {"name": env, "connections": [],
         "packages": [{"name": name, "location": str(location)}]}]}


def port_clash(cfg: config.Config, role: str, port: int, mcp_port: int) -> str | None:
    """Why this role may not bind these ports, or None."""
    other = "truth" if role == "model" else "model"
    theirs = {cfg.get(other, "port"), cfg.get(other, "mcp_port")} - {None}
    if theirs & {port, mcp_port}:
        return (f"--role {role} would bind {port}/{mcp_port}, which the {other} "
                f"server uses. The truth server must be a separate server the "
                f"answerer cannot reach. Fix: give [model] and [truth] "
                f"different ports in {cfg.file_hint}")
    return None


def write_config(root: pathlib.Path, wanted: dict) -> bool:
    """Write publisher.config.json; True when it differs from what was there."""
    path = root / "publisher.config.json"
    try:
        before = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        before = None
    path.write_text(json.dumps(wanted, indent=2) + "\n")
    return before is not None and before != wanted


def retrieval_note(env: dict[str, str]) -> str | None:
    """The warning for a server that will rank lexically, or None."""
    if (env.get("EMBEDDING_API_KEY") or "").strip():
        return None
    return ("  ! no EMBEDDING_API_KEY: this server ranks get_context lexically, "
            "so a run against it measures the lexical matcher. Its retrieval "
            "numbers are not comparable with a semantic run")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--role", choices=("model", "truth"),
                    help="serve this role for --set, configured from its eval.toml")
    ap.add_argument("--set", dest="set_dir", type=pathlib.Path,
                    help="the eval set (with --role)")
    ap.add_argument("--server-root", type=pathlib.Path,
                    help="required without --role")
    ap.add_argument("--publisher-dir", type=pathlib.Path,
                    help="Publisher's packages/server directory (holds "
                         "dist/server.mjs). With --role, defaults to this clone's")
    ap.add_argument("--port", type=int, default=None, help="default 4811, or "
                    "the role's port in eval.toml")
    ap.add_argument("--mcp-port", type=int, default=None, help="default 4040, or "
                    "the role's mcp_port in eval.toml")
    ap.add_argument("--allow-proxy", action="store_true")
    ap.add_argument("--trace-retrieval", action="store_true")
    ap.add_argument("--reinit", action="store_true",
                    help="drop the store and re-read publisher.config.json. "
                         "Needed after a config edit -- a new environment or a "
                         "changed connection does not take otherwise -- and it "
                         "costs a full re-embed of the semantic index. Without "
                         "it an existing server root is preserved")
    ap.add_argument("--wait", type=int, default=90,
                    help="seconds to wait for the REST root before giving up")
    ap.add_argument("--warm-retrieval", action="store_true",
                    help="after the server answers, drive one ranking call and "
                         "poll embeddingIndex.status until it settles. Requires "
                         "--environment and --package")
    ap.add_argument("--environment", help="environment to warm (--warm-retrieval)")
    ap.add_argument("--package", help="package to warm (--warm-retrieval)")
    ap.add_argument("--stop", action="store_true",
                    help="stop the server recorded in <server-root>/publisher.pid")
    a = ap.parse_args(argv)

    cfg = None
    if a.role:
        if not a.set_dir:
            ap.error("--role needs --set")
        cfg = config.load(a.set_dir)
        a.server_root = a.server_root or cfg.server_root(a.role)
        a.publisher_dir = a.publisher_dir or cfg.publisher_dir()
        a.port = a.port or cfg.get(a.role, "port")
        a.mcp_port = a.mcp_port or cfg.get(a.role, "mcp_port")
        if a.role == "model" and a.warm_retrieval:
            a.environment = a.environment or cfg.get("model", "environment")
            a.package = a.package or cfg.get("model", "package")
    elif not a.server_root:
        ap.error("--server-root is required without --role")
    a.port = a.port or 4811
    a.mcp_port = a.mcp_port or 4040

    root = a.server_root.resolve()
    pidfile = root / "publisher.pid"

    if a.stop:
        if not pidfile.exists():
            print(f"no pidfile at {pidfile}")
            return 1
        info = json.loads(pidfile.read_text())
        try:
            os.killpg(info["pid"], signal.SIGTERM)
            print(f"stopped pid {info['pid']} (port {info['port']})")
        except ProcessLookupError:
            print(f"pid {info['pid']} was not running")
        pidfile.unlink()
        return 0

    if a.warm_retrieval and not (a.environment and a.package):
        raise SystemExit("--warm-retrieval needs --environment and --package")
    if not a.publisher_dir:
        raise SystemExit("--publisher-dir is required to start. Build Publisher "
                         "in this clone (bun run build), or set [paths] "
                         "publisher_dir in the set's eval.toml")
    # Resolved: it is also the child's cwd, so a relative path would be read
    # twice over, once from here and once from inside itself.
    a.publisher_dir = a.publisher_dir.resolve()
    server = a.publisher_dir / "dist" / "server.mjs"
    if not server.exists():
        raise SystemExit(f"{server} not found; build Publisher first")
    if alive(a.port):
        raise SystemExit(f"something already answers on port {a.port}; use another "
                         f"port or --stop the recorded server first")

    if cfg is not None:
        clash = port_clash(cfg, a.role, a.port, a.mcp_port)
        if clash:
            raise SystemExit(clash)

    root.mkdir(parents=True, exist_ok=True)
    changed = (write_config(root, role_config(cfg, a.role))
               if cfg is not None else False)
    env = {**os.environ, "SERVER_ROOT": str(root)}
    if a.allow_proxy:
        env["PUBLISHER_ALLOW_PROXY_CONNECTIONS"] = "true"
    if a.trace_retrieval:
        env["PUBLISHER_MCP_TRACE"] = "retrieval"
    seed, why = init_decision(root, a.reinit, changed)
    print(why)
    note = retrieval_note(env) if a.role != "truth" else None
    if note:
        print(note)
    cmd = server_cmd(server, root, a.port, a.mcp_port, seed)
    log = (root / "publisher.log").open("a")
    p = subprocess.Popen(
        cmd, cwd=str(a.publisher_dir), stdout=log, stderr=log, env=env,
        start_new_session=True)   # its own session: the shell's exit cannot reach it
    pidfile.write_text(json.dumps({"pid": p.pid, "port": a.port, "mcpPort": a.mcp_port,
                                   "startedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                                              time.gmtime())}))

    deadline = time.time() + a.wait
    while time.time() < deadline:
        if p.poll() is not None:
            print(f"server exited with {p.returncode}; see {root / 'publisher.log'}")
            return 1
        if alive(a.port):
            text = (root / "publisher.log").read_text()
            errs = [l for l in text.splitlines() if "load_errors=" in l]
            print(f"serving: http://localhost:{a.port}  mcp: http://localhost:{a.mcp_port}"
                  f"  pid {p.pid}  log {root / 'publisher.log'}")
            if errs and "load_errors=0" not in errs[-1]:
                print(f"  ! {errs[-1].strip()} -- a package failed to load; it will "
                      f"answer HTTP and serve nothing (check the log)")
                return 2
            if a.warm_retrieval:
                status, line = warm_retrieval(a.port, a.mcp_port, a.environment,
                                              a.package, a.wait)
                print(f"  {line}")
                # Not a server failure: it started and serves. The caller
                # decides whether a lexical run is worth having, so say which
                # it is and let the exit code mean "the server is up".
                if status != "ready":
                    return 3
            return 0
        time.sleep(1)
    print(f"no answer on port {a.port} after {a.wait}s; server still running as "
          f"pid {p.pid}, log {root / 'publisher.log'}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
