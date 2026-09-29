# Copyright (c) Credible Data Inc.
# SPDX-License-Identifier: MIT
"""Start a Publisher with a given retrieval config, warm it, and ask it questions.

Used by the scenario scripts in this directory. Each `Pub` is one server with
its own server root, so an index-time setting (prefixes, enrichment, values)
gets a fresh index, and a query-time setting is swept on one warm server with
the X-Publisher-Retrieval header.
"""

from __future__ import annotations

import json
import os
import pathlib
import shutil
import signal
import socket
import subprocess
import time
import urllib.error
import urllib.request
from typing import Any

HERE = pathlib.Path(__file__).resolve().parent
SERVER_DIR = HERE.parents[1] / "packages" / "server"
PKG = pathlib.Path("/tmp/llm-val/pkg/ecommerce")
ROOTS = pathlib.Path("/tmp/llm-val/roots")
MOCK = "http://127.0.0.1:4977"
ENV, PACKAGE = "samples", "ecommerce"


def free_port(start: int) -> int:
    p = start
    while True:
        with socket.socket() as s:
            if s.connect_ex(("127.0.0.1", p)) != 0:
                return p
        p += 1


def http(url: str, body: dict | None = None, headers: dict | None = None,
         timeout: int = 60) -> Any:
    req = urllib.request.Request(
        url, data=json.dumps(body).encode() if body is not None else None,
        method="POST" if body is not None else "GET",
        headers={"Content-Type": "application/json",
                 "Accept": "application/json, text/event-stream", **(headers or {})})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read().decode()
    if raw.lstrip().startswith("event:") or "\ndata: " in raw:
        frames = [l[6:] for l in raw.splitlines() if l.startswith("data: ")]
        raw = frames[-1]
    return json.loads(raw)


def mock(path: str, body: dict | None = None) -> Any:
    return http(MOCK + path, body)


class Pub:
    def __init__(self, name: str, retrieval: dict | None = None,
                 embeddings: bool = True, llm: bool = True, gate: bool = True,
                 env: dict | None = None, package: pathlib.Path = PKG,
                 embedding_model: str = "mock-embed"):
        self.name, self.retrieval = name, retrieval
        self.embeddings, self.llm, self.gate = embeddings, llm, gate
        self.extra_env, self.package = env or {}, package
        self.embedding_model = embedding_model
        self.root = ROOTS / name
        self.proc: subprocess.Popen | None = None
        self.port = self.mcp_port = 0

    # -- lifecycle -----------------------------------------------------------
    def config(self) -> dict:
        cfg: dict = {"frozenConfig": False, "environments": [{
            "name": ENV, "connections": [],
            "packages": [{"name": PACKAGE, "location": str(self.package)}]}]}
        if self.retrieval is not None:
            cfg["retrieval"] = self.retrieval
        return cfg

    def restart(self, retrieval: dict | None = None, **changes) -> "Pub":
        """Stop and start again on the SAME server root, without `--init`.

        What is cached in publisher.db (vectors, generated text, values) is kept,
        so this is how to see what survives a restart and what a changed setting
        makes it redo. `retrieval` replaces the retrieval block; other keyword
        arguments replace the attribute of that name (embedding_model, llm, ...).
        """
        self.stop()
        if retrieval is not None:
            self.retrieval = retrieval
        for k, v in changes.items():
            setattr(self, k, v)
        return self.start(fresh=False)

    def start(self, wait: int = 90, fresh: bool = True) -> "Pub":
        if fresh and self.root.exists():
            shutil.rmtree(self.root)
        self.root.mkdir(parents=True, exist_ok=True)
        (self.root / "publisher.config.json").write_text(json.dumps(self.config()))
        self.port, self.mcp_port = free_port(5200), 0
        self.mcp_port = free_port(self.port + 1)
        env = {k: v for k, v in os.environ.items()
               if not k.startswith(("EMBEDDING_", "LLM_", "PUBLISHER_RETRIEVAL"))}
        env.update({"SERVER_ROOT": str(self.root), "NODE_ENV": "production"})
        if self.embeddings:
            env.update({"EMBEDDING_API_BASE": MOCK + "/v1",
                        "EMBEDDING_MODEL": self.embedding_model})
        if self.llm:
            env.update({"LLM_API_BASE": MOCK + "/v1", "LLM_MODEL": "mock-chat"})
        if self.gate:
            env["PUBLISHER_RETRIEVAL_OVERRIDES"] = "1"
        env.update(self.extra_env)
        log = (self.root / "publisher.log").open("w")
        self.proc = subprocess.Popen(
            ["bun", "run", str(SERVER_DIR / "dist" / "server.mjs"),
             "--server_root", str(self.root), "--port", str(self.port),
             "--mcp_port", str(self.mcp_port), *(["--init"] if fresh else [])],
            cwd=str(SERVER_DIR), stdout=log, stderr=log, env=env,
            start_new_session=True)
        deadline = time.time() + wait
        while time.time() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError(f"{self.name}: server exited {self.proc.returncode}:\n"
                                   + self.log_tail())
            try:
                http(f"http://127.0.0.1:{self.port}/api/v0/environments", timeout=3)
                break
            except Exception:  # noqa: BLE001
                time.sleep(1)
        else:
            raise RuntimeError(f"{self.name}: no answer in {wait}s:\n" + self.log_tail())
        # A package that failed to load answers HTTP and serves nothing.
        deadline = time.time() + wait
        while time.time() < deadline:
            try:
                http(f"http://127.0.0.1:{self.port}/api/v0/environments/{ENV}/packages/{PACKAGE}",
                     timeout=5)
                return self
            except Exception:  # noqa: BLE001
                time.sleep(1)
        raise RuntimeError(f"{self.name}: package never loaded:\n" + self.log_tail())

    def stop(self) -> None:
        if self.proc and self.proc.poll() is None:
            try:
                os.killpg(self.proc.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                self.proc.wait(10)
            except subprocess.TimeoutExpired:
                os.killpg(self.proc.pid, signal.SIGKILL)

    def log_tail(self, n: int = 40) -> str:
        p = self.root / "publisher.log"
        return "\n".join(p.read_text().splitlines()[-n:]) if p.exists() else ""

    def __enter__(self):
        return self.start()

    def __exit__(self, *_a):
        self.stop()

    # -- asking --------------------------------------------------------------
    def ask(self, targets: list[tuple[str, str | None]] | list[dict],
            override: dict | None = None, trace: str | None = None,
            scope: dict | None = None, timeout: int = 90) -> dict:
        tg = [t if isinstance(t, dict) else
              {"target_type": t[0], **({"search_text": t[1]} if t[1] is not None else {})}
              for t in targets]
        headers = {}
        if override is not None:
            headers["X-Publisher-Retrieval"] = json.dumps(override)
        if trace:
            headers["X-Publisher-Retrieval-Trace"] = trace
        body = {"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {
            "name": "get_context",
            "arguments": {"search_targets": tg,
                          "scopes": [{"environment": ENV, "package": PACKAGE, **(scope or {})}]}}}
        env = http(f"http://127.0.0.1:{self.mcp_port}/mcp", body, headers, timeout)
        result = env.get("result") or {}
        for chunk in result.get("content") or []:
            text = chunk.get("text") or (chunk.get("resource") or {}).get("text")
            if text:
                try:
                    payload = json.loads(text)
                except ValueError:
                    payload = {"_text": text}
                if result.get("isError"):
                    payload["_isError"] = True
                return payload
        return {"_error": env.get("error") or env}

    def package_status(self) -> dict:
        p = http(f"http://127.0.0.1:{self.port}/api/v0/environments/{ENV}/packages/{PACKAGE}")
        return p.get("embeddingIndex") or {}

    def warm(self, timeout: int = 240, settle: int = 3) -> dict:
        """Ask once to start every background index, then wait for all of them."""
        self.ask([("source", "what data is in this package")], override={} if self.gate else None)
        deadline = time.time() + timeout
        quiet = 0
        st: dict = {}
        while time.time() < deadline:
            st = self.package_status()
            busy = (st.get("status") == "indexing"
                    or (st.get("enrichment") or {}).get("status") in ("pending", "running")
                    or (st.get("valueIndex") or {}).get("status") == "building")
            if not busy and st.get("status"):
                quiet += 1
                if quiet >= settle:
                    return st
            else:
                quiet = 0
            time.sleep(1)
        raise RuntimeError(f"{self.name}: index never settled: {st}")


# ---------------------------------------------------------------------------
# reading a response


def _card_source(card: dict) -> str:
    return ((card.get("source_info") or {}).get("resource_id") or {}).get("source") or "?"


def entities(payload: dict) -> list[str]:
    """`source.name` for each entity, in delivered order, once each.

    A source that two model files both declare arrives as two cards; the entity
    is the same, so it is counted once.
    """
    seen: dict[str, None] = {}
    for card in payload.get("sources") or []:
        for e in card.get("entities") or []:
            seen.setdefault(f"{_card_source(card)}.{e.get('name')}", None)
    return list(seen)


def sources(payload: dict) -> list[str]:
    seen: dict[str, None] = {}
    for c in payload.get("sources") or []:
        seen.setdefault(_card_source(c), None)
    return list(seen)


def chars(payload: dict) -> int:
    return len(json.dumps(payload, separators=(",", ":")))


def find(payload: dict, name: str) -> dict | None:
    """The first entity called `name` (or `source.name`), with its card's source."""
    for card in payload.get("sources") or []:
        for e in card.get("entities") or []:
            full = f"{_card_source(card)}.{e.get('name')}"
            if name in (e.get("name"), full):
                return {**e, "_source": _card_source(card)}
    return None


def rank(payload: dict, wanted: str) -> int | None:
    """1-based position of an entity in the delivered order, or None."""
    es = entities(payload)
    if wanted in es:
        return es.index(wanted) + 1
    hits = [i for i, e in enumerate(es) if e.split(".", 1)[-1] == wanted]
    return hits[0] + 1 if hits else None


def brief(payload: dict) -> dict:
    """The parts of a response worth recording."""
    keys = ("retrieval", "retrieval_reason", "below_cutoff_count", "total_entities",
            "returned", "total_available", "warnings", "retrieval_stages",
            "retrieval_config")
    out = {k: payload[k] for k in keys if k in payload}
    out["entities"] = entities(payload)
    out["sources"] = sources(payload)
    out["chars"] = chars(payload)
    if "retrieval_trace" in payload:
        out["trace"] = payload["retrieval_trace"]
    return out
