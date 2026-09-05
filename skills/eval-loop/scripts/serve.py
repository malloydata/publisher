#!/usr/bin/env python3
"""Start a Publisher that outlives the shell that started it. Stdlib only.

  python3 serve.py --publisher-dir <publisher>/packages/server \\
      --server-root <scratch>/evalroot --port 4811 --mcp-port 4040 \\
      [--allow-proxy] [--trace-retrieval] [--wait 60]

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
    ap.add_argument("--wait", type=int, default=90,
                    help="seconds to wait for the REST root before giving up")
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
    log = (root / "publisher.log").open("a")
    p = subprocess.Popen(
        ["bun", "run", str(server), "--server_root", str(root),
         "--port", str(a.port), "--mcp_port", str(a.mcp_port), "--init"],
        cwd=str(a.publisher_dir), stdout=log, stderr=log, env=env,
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
            return 0
        time.sleep(1)
    print(f"no answer on port {a.port} after {a.wait}s; server still running as "
          f"pid {p.pid}, log {root / 'publisher.log'}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
