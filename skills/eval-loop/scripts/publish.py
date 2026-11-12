#!/usr/bin/env python3
"""Put the model on disk in front of the answerer, built. Stdlib only.

  python3 eval.py publish --set <set-dir>                 # the [model] package
  python3 eval.py publish --set <set-dir> --dir <dir> --as tip-fix   # an experiment

After an edit, the model server still answers from its copy of the package.
This replaces the steps that close that gap by hand:

1. Publish the package directory (`POST …/environments/{env}/packages`). With
   `[model] versioning = true` the server publishes the version its
   publisher.json names, and a changed package under a published version is
   refused (409): bump the version. A server with versioning off (`[model]
   versioning = false`) installs it in place.
2. Read it back: the package loads, compiles, and is not serving an older
   model (`/status` `loadErrors`).
3. Build, when the model persists or pre-aggregates anything. A load unbinds
   every table, so a timing taken before the build measures the live query.
   It waits for the build and reports what it built, reused, refused or lost.
4. Warm the retrieval index when EMBEDDING_API_KEY is set, as `serve` does, so
   the first questions of the next run are ranked semantically.

`--as <name>` publishes under the package name `<package>.<name>` instead, on
the same server, so an experiment is answered beside the main line rather than
in place of it (`experiment.py`).
"""
from __future__ import annotations

import argparse
import json
import os
import pathlib
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent.parent / "eval-answer" / "scripts"))
sys.path.insert(0, str(HERE))

import config  # noqa: E402

TERMINAL = ("MANIFEST_FILE_READY", "FAILED", "CANCELLED")
# Not `--`: the served directory's path would then hold `--`, which Publisher
# refuses in a DuckDB file path (it reads as a SQL comment). Not `__` either,
# which runs into the `___` Publisher reserves for `<package>___<version>`.
SEPARATOR = "."


def experiment_package(package: str, name: str) -> str:
    return f"{package}{SEPARATOR}{name}"


def send(base: str, method: str, path: str, body: Any = None,
         timeout: int = 300) -> tuple[int, Any]:
    """(status, parsed body). An HTTP error is a status, not an exception."""
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(f"{base.rstrip('/')}/{path.lstrip('/')}", data=data,
                                 method=method, headers={"content-type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            text = r.read().decode()
            return r.status, (json.loads(text) if text.strip() else None)
    except urllib.error.HTTPError as e:
        text = e.read().decode(errors="replace")
        try:
            return e.code, json.loads(text)
        except ValueError:
            return e.code, text


def message(body: Any) -> str:
    return (body.get("message") or json.dumps(body)) if isinstance(body, dict) else str(body)


def base_version(package_dir: pathlib.Path) -> str | None:
    try:
        return json.loads((package_dir / "publisher.json").read_text()).get("version")
    except (OSError, ValueError):
        return None


def server_versioning(base: str) -> bool:
    """Whether the server publishes packages as versions. Asked, not configured:
    the default changed (on from 0.13), and the server is what decides."""
    _, st = send(base, "GET", "api/v0/status")
    return str((st or {}).get("packageVersioning", "off")).lower() == "on"


def ensure_environment(base: str, env: str) -> None:
    code, _ = send(base, "GET", f"api/v0/environments/{urllib.parse.quote(env)}")
    if code == 404:
        code, body = send(base, "POST", "api/v0/environments", {"name": env})
        if code >= 300:
            raise SystemExit(f"could not create environment {env!r}: HTTP {code} {message(body)}")


def publish(base: str, env: str, name: str, location: pathlib.Path,
            versioned: bool) -> dict[str, Any]:
    """Publish and read back. Returns {version, latest}; exits with the fix on refusal."""
    ensure_environment(base, env)
    code, body = send(base, "POST", f"api/v0/environments/{urllib.parse.quote(env)}/packages",
                      {"name": name, "location": str(location.resolve())})
    version = base_version(location)
    if code == 409 and "VERSION_CONFLICT" in json.dumps(body):
        raise SystemExit(
            f"version {version} of {name} is already published with other content. "
            f"A published version never changes. Fix: bump `version` in "
            f"{location / 'publisher.json'} and publish again.")
    if code == 409 and "PACKAGE_IS_VERSIONED" in json.dumps(body):
        raise SystemExit(
            f"{name} already has published versions, so it cannot be installed in "
            f"place. Fix: set `versioning = true` under [model] in eval.toml and "
            f"restart the model server.")
    if code >= 300:
        raise SystemExit(f"publish of {name} from {location} refused: HTTP {code}\n{message(body)}")
    pkg = f"api/v0/environments/{urllib.parse.quote(env)}/packages/{urllib.parse.quote(name)}"
    code, info = send(base, "GET", pkg)
    if code >= 300:
        raise SystemExit(f"{name} was published but does not load: HTTP {code}\n{message(info)}")
    _, st = send(base, "GET", "api/v0/status")
    for e in (st or {}).get("loadErrors") or []:
        if e.get("package") == name and e.get("environment") == env:
            raise SystemExit(f"{name} is serving an older model: its latest load failed.\n"
                             f"{e.get('message', '')[:2000]}")
    info = info if isinstance(info, dict) else {}
    out = {"version": info.get("versionId") or (version if versioned else None),
           "latest": info.get("latestVersion"), "baseVersion": version}
    if versioned and out["version"] and out["latest"] != out["version"]:
        out["note"] = (f"{out['version']} is published but `latest` is {out['latest']}, "
                       f"which a higher version holds. An answer run answers from latest. "
                       f"Fix: PUT {pkg}/latest {{\"version\": \"{out['version']}\"}}")
    return out


def build(base: str, env: str, name: str, version: str | None,
          wait: int = 600) -> dict[str, Any] | None:
    """Run a materialization build when there is anything to build. None when there is not."""
    pkg = f"api/v0/environments/{urllib.parse.quote(env)}/packages/{urllib.parse.quote(name)}"
    q = f"?versionId={urllib.parse.quote(version)}" if version else ""
    _, plan = send(base, "GET", f"{pkg}/build-plan{q}")
    plan = plan if isinstance(plan, dict) else {}
    if not plan.get("sources") and not plan.get("refusedSources"):
        return None
    body = {"versionId": version} if version else {}
    code, started = send(base, "POST", f"{pkg}/materializations", body)
    if code >= 300:
        raise SystemExit(f"build of {name} refused: HTTP {code} {message(started)}")
    mid = (started or {}).get("id")
    deadline = time.time() + wait
    while True:
        _, m = send(base, "GET", f"{pkg}/materializations/{mid}{q}")
        status = (m or {}).get("status") if isinstance(m, dict) else None
        if status in TERMINAL or time.time() > deadline:
            break
        time.sleep(1)
    meta = (m or {}).get("metadata") or {}
    _, st = send(base, "GET", f"{pkg}/status{q}")
    return {"id": mid, "status": status, "built": meta.get("sourcesBuilt"),
            "reused": meta.get("sourcesReused"), "failed": meta.get("sourcesFailed"),
            "refused": sorted((meta.get("refusedSources") or {}) if isinstance(
                meta.get("refusedSources"), dict) else meta.get("refusedSources") or []),
            "error": (m or {}).get("error"),
            "binding": (st or {}).get("manifestBindingStatus") if isinstance(st, dict) else None}


def warm(cfg: config.Config, env: str, name: str) -> str | None:
    if not os.environ.get("EMBEDDING_API_KEY"):
        return None
    import serve
    _, line = serve.warm_retrieval(cfg.get("model", "port"), cfg.get("model", "mcp_port"),
                                   env, name, wait=300)
    return line


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--set", dest="set_dir", required=True, type=pathlib.Path)
    ap.add_argument("--dir", type=pathlib.Path, default=None,
                    help="the package directory to publish (default: [model] repo)")
    ap.add_argument("--as", dest="as_name", default=None,
                    help="publish as <package>.<name>, beside the main package")
    ap.add_argument("--no-build", action="store_true",
                    help="skip the materialization build; queries on persisted "
                         "sources then compute live")
    ap.add_argument("--no-warm", action="store_true")
    a = ap.parse_args(argv)
    cfg = config.load(a.set_dir)
    base = cfg.model_publisher()
    env = cfg.need(None, "model", "environment")
    pkg = cfg.need(None, "model", "package")
    location = a.dir or cfg.need(None, "model", "repo")
    name = experiment_package(pkg, a.as_name) if a.as_name else pkg
    if not (pathlib.Path(location) / "publisher.json").exists():
        raise SystemExit(f"no publisher.json in {location}: not a package directory")
    out = publish(base, env, name, pathlib.Path(location), server_versioning(base))
    shown = out["version"] or f"{out['baseVersion'] or '?'} (unpublished, installed in place)"
    print(f"published {env}/{name} {shown} from {location}")
    if out.get("note"):
        print(f"  ! {out['note']}")
    if not a.no_build:
        b = build(base, env, name, out["version"])
        if b is None:
            print("  nothing to build: no persist or pre-aggregate in the model")
        else:
            print(f"  build {b['id']}: {b['status']}, built {b['built']}, reused "
                  f"{b['reused'] or 0}, failed {b['failed'] or 0}, refused {len(b['refused'])}; "
                  f"binding {b['binding']}")
            if b["status"] != "MANIFEST_FILE_READY" or b["failed"] or b["refused"]:
                print(f"  ! the build did not make everything: {b.get('error') or b['refused']}. "
                      f"Persisted sources it lost compute live.")
                return 1
    if not a.no_warm:
        status = warm(cfg, env, name)
        if status:
            print(f"  retrieval index: {status}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
