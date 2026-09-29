# Copyright (c) Credible Data Inc.
# SPDX-License-Identifier: MIT
"""How long indexing takes and how much memory the server holds while it does.

Samples the server's resident memory once a second from start until every
background index has settled, for a few configurations.
"""

import subprocess
import sys
import threading
import time
import pathlib

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from harness import Pub, mock  # noqa: E402


def rss_mb(pid: int) -> float:
    out = subprocess.run(["ps", "-o", "rss=", "-p", str(pid)], capture_output=True, text=True).stdout.strip()
    return int(out) / 1024 if out else 0.0


def run(label: str, retrieval: dict) -> None:
    mock("/__reset", {})
    p = Pub("cost", retrieval=retrieval)
    p.start()
    idle = rss_mb(p.proc.pid)
    peak = [idle]
    stop = threading.Event()

    def sample():
        while not stop.is_set():
            peak[0] = max(peak[0], rss_mb(p.proc.pid))
            time.sleep(0.5)

    t = threading.Thread(target=sample, daemon=True)
    t.start()
    t0 = time.time()
    st = p.warm(timeout=900)
    secs = time.time() - t0
    stop.set()
    t.join()
    after = rss_mb(p.proc.pid)
    log = mock("/__log")
    chat = sum(1 for r in log if r["kind"] == "chat")
    emb = sum(r["n"] for r in log if r["kind"] == "embeddings")
    v = st.get("valueIndex") or {}
    e = st.get("enrichment") or {}
    print(f"{label:<34} {secs:6.1f}s  rss idle {idle:5.0f}MB  peak {peak[0]:5.0f}MB  settled {after:5.0f}MB  "
          f"chat calls {chat:>3}  embedded texts {emb:>5}  rows {st.get('embeddedRows')}  "
          f"values {v.get('values')}/{v.get('dimensions')} dims  enriched {e.get('enriched')}")
    p.stop()


run("embeddings only", {})
run("+ keyphrases and summaries", {"enrichment": {"enabled": True, "sourceSummary": {"enabled": True}}})
run("+ values (annotated, 4 dims)", {"enrichment": {"enabled": True, "sourceSummary": {"enabled": True}},
                                     "dimensionalValues": {"mode": "annotated"}})
run("+ values (auto, all: 32 dims)", {"enrichment": {"enabled": True, "sourceSummary": {"enabled": True}},
                                      "dimensionalValues": {"mode": "auto", "include": ["*.*"]}})
run("keyphrases for everything", {"enrichment": {"enabled": True, "sourceSummary": {"enabled": True},
                                                 "keyphrase": {"mode": "always"}}})
mock("/__reset", {})
