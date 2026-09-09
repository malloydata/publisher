#!/usr/bin/env python3
"""Start a Publisher that outlives the shell that started it. Stdlib only.

  python3 serve.py --publisher-dir <publisher>/packages/server \\
      --server-root <scratch>/evalroot --port 4811 --mcp-port 4040 \\
      [--allow-proxy] [--trace-retrieval] [--reinit] [--wait 60] \\
      [--warm-retrieval --environment <env> --package <pkg>]

  python3 serve.py --stop --server-root <scratch>/evalroot

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

Two flags name the two things a run needs that are off by default:
--allow-proxy sets PUBLISHER_ALLOW_PROXY_CONNECTIONS=true (a `publisher`-type
connection is refused without it, and the server still reports `serving` with
load_errors=1); --trace-retrieval sets PUBLISHER_MCP_TRACE=retrieval, without
which failures cannot be attributed.
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


def init_decision(root: pathlib.Path, reinit: bool) -> tuple[bool, str]:
    """Whether to pass `--init`, and the line that says why."""
    db = root / DB_NAME
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
    status = None
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=10) as r:
                status = index_status(json.loads(r.read().decode()))
        except Exception:  # noqa: BLE001
            status = None
        if status is None:
            return None, ("no embeddingIndex on the package resource; this "
                          "server has no embedding provider, so every run "
                          "against it is LEXICAL -- do not report "
                          "discoverability findings from it")
        if status in TERMINAL_INDEX_STATES:
            break
        time.sleep(2)
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


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--server-root", required=True, type=pathlib.Path)
    ap.add_argument("--publisher-dir", type=pathlib.Path,
                    help="Publisher's packages/server directory (holds dist/server.mjs)")
    ap.add_argument("--port", type=int, default=4811)
    ap.add_argument("--mcp-port", type=int, default=4040)
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
    a = ap.parse_args()

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
        raise SystemExit("--publisher-dir is required to start")
    server = a.publisher_dir / "dist" / "server.mjs"
    if not server.exists():
        raise SystemExit(f"{server} not found; build Publisher first")
    if alive(a.port):
        raise SystemExit(f"something already answers on port {a.port}; use another "
                         f"port or --stop the recorded server first")

    root.mkdir(parents=True, exist_ok=True)
    env = {**os.environ, "SERVER_ROOT": str(root)}
    if a.allow_proxy:
        env["PUBLISHER_ALLOW_PROXY_CONNECTIONS"] = "true"
    if a.trace_retrieval:
        env["PUBLISHER_MCP_TRACE"] = "retrieval"
    seed, why = init_decision(root, a.reinit)
    print(why)
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
