#!/usr/bin/env python3
# Copyright (c) Credible Data Inc.
# SPDX-License-Identifier: MIT
"""A deterministic stand-in for an OpenAI-compatible embedding + chat endpoint.

WHY IT EXISTS. To check that each retrieval setting does what it was designed
to do, the model behind it has to be predictable. A real model changes its
mind between runs and between versions; this one does not. It is NOT a model of
quality: what it says about a field is a word-overlap rule, not understanding.
Use it to learn what a knob does. Use a real model (Ollama, a hosted API) to
learn how well it does it.

WHAT IT DOES.
  POST /v1/embeddings       Words that mean the same thing (revenue / sales /
                            income) share one dimension, so their cosine is high.
                            A model whose name contains "prefixed" is
                            deliberately prefix-sensitive, like nomic-embed-text:
                            text without a `search_query: ` / `search_document: `
                            prefix is blurred by a fixed noise vector, which
                            lowers every cosine. Any other model name is not.
  POST /v1/chat/completions Answers the four prompts Publisher sends (refine,
                            rerank, keyphrase, source summary) by word overlap.
  POST /__control           {"mode": "ok|http500|badjson|hang", "latencyMs": N,
                             "embedMode": "ok|http500"} to inject failures.
                            `mode` and `latencyMs` apply to chat only.
  GET  /__log               Every request seen, by stage, with bodies.
  POST /__reset             Clear the log and the control settings.

Run:  python3 mock_provider.py --port 4960
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

DIMS = 4096  # large, so two unrelated words rarely share a dimension by chance

# Words that mean the same thing share a concept. Anything not listed is its own
# concept, hashed. Kept small and specific to the ecommerce sample.
CONCEPTS = {
    "money": "revenue sales sale price prices spend spent spending amount income earnings dollars money paid".split(),
    "cost": "cost costs wholesale cogs expense".split(),
    "margin": "margin margins profit profits profitability".split(),
    "customer": "user users customer customers shopper shoppers buyer buyers account accounts client clients person people".split(),
    "order": "order orders purchase purchases transaction transactions basket".split(),
    "product": "product products item items sku merchandise goods catalog".split(),
    "geo": "city state country region location postal zip geographic geography latitude longitude".split(),
    "time": "date time timestamp created when day month year placed period".split(),
    "ship": "ship shipped shipping shipment delivery delivered dispatch".split(),
    "return": "return returned returns refund refunded".split(),
    "status": "status lifecycle stage".split(),
    "demo": "age gender sex demographic demographics".split(),
    "channel": "channel acquisition traffic marketing referral".split(),
    "count": "count number many total distinct quantity units volume".split(),
    "avg": "average avg mean typical".split(),
    "name": "name title label".split(),
    "email": "email mail".split(),
    "stock": "inventory stock warehouse".split(),
    "dc": "distribution center centre".split(),
    "brand": "brand brands manufacturer".split(),
    "category": "category categories type kind class".split(),
    "vip": "vip loyal top best frequent repeat".split(),
}
WORD_TO_CONCEPT = {w: c for c, ws in CONCEPTS.items() for w in ws}
STOP = set("the of a an by for to per in is are what how and or with on at from as that this it its be was were which who whom".split())
PREFIXES = ("search_query: ", "search_document: ")


def tokens(text: str) -> list[str]:
    text = re.sub(r"([a-z])([A-Z])", r"\1 \2", text)
    return [t for t in re.split(r"[^a-z0-9]+", text.lower()) if t and t not in STOP]


def concept_of(tok: str) -> str:
    if tok in WORD_TO_CONCEPT:
        return WORD_TO_CONCEPT[tok]
    # A crude stem so plurals meet their singulars.
    if tok.endswith("s") and tok[:-1] in WORD_TO_CONCEPT:
        return WORD_TO_CONCEPT[tok[:-1]]
    return tok


def concepts(text: str) -> list[str]:
    return [concept_of(t) for t in tokens(text)]


def dim_of(concept: str) -> int:
    return int(hashlib.md5(concept.encode()).hexdigest(), 16) % DIMS


NOISE = [math.sin(i * 12.9898) for i in range(DIMS)]


def embed(text: str, prefix_sensitive: bool = False) -> tuple[list[float], bool]:
    """(vector, had_prefix). Only a model named `*prefixed*` blurs unprefixed text."""
    had_prefix = any(text.startswith(p) for p in PREFIXES)
    for p in PREFIXES:
        if text.startswith(p):
            text = text[len(p):]
    v = [0.0] * DIMS
    for c in concepts(text):
        v[dim_of(c)] += 1.0
    n = math.sqrt(sum(x * x for x in v)) or 1.0
    v = [x / n for x in v]
    if prefix_sensitive and not had_prefix:
        # A prefix-sensitive model: no prefix, a blurred vector.
        nn = math.sqrt(sum(x * x for x in NOISE))
        v = [a + 0.9 * b / nn for a, b in zip(v, NOISE)]
        n2 = math.sqrt(sum(x * x for x in v)) or 1.0
        v = [x / n2 for x in v]
    return v, had_prefix


# --------------------------------------------------------------------------- state

LOCK = threading.Lock()
STATE = {"mode": "ok", "latencyMs": 0, "embedMode": "ok"}
LOG: list[dict] = []


def log(kind: str, **fields) -> None:
    with LOCK:
        LOG.append({"kind": kind, "t": time.time(), **fields})


# --------------------------------------------------------------------------- stages


def stage_of(system: str, user: str) -> str:
    if "evaluating how well database entities" in system:
        return "refine"
    if "matching natural language queries to data sources" in system:
        return "rerank"
    if "distill a single concise retrieval keyphrase" in system:
        return "keyphrase_batch" if "Produce one keyphrase for EACH" in user else "keyphrase"
    if "data modeling expert" in system:
        return "summary"
    return "unknown"


def overlap(phrase: set[str], text_concepts: set[str]) -> float:
    return len(phrase & text_concepts) / len(phrase) if phrase else 0.0


def reply_refine(user: str) -> str:
    m = re.search(r'PHRASE:\s*Text: "(.*?)"\n', user, re.S)
    phrase = set(concepts(m.group(1))) if m else set()
    out = []
    for line in user.splitlines():
        lm = re.match(r"^- \[(\d+)\] (\S+) \((.*?), source: (.*?)\): (.*)$", line)
        if not lm:
            continue
        idx, name, _typ, _src, desc = int(lm.group(1)), lm.group(2), lm.group(3), lm.group(4), lm.group(5)
        in_name = overlap(phrase, set(concepts(name)))
        in_desc = overlap(phrase, set(concepts(desc)))
        if in_name >= 0.6 or (in_name > 0 and in_desc >= 0.5):
            score, why = "HIGH", "this measure or dimension directly covers the phrase"
        elif in_name > 0 or in_desc >= 0.3:
            score, why = "MEDIUM", "this field is related to the phrase but not an obvious match"
        elif in_desc > 0:
            score, why = "LOW", "this field is only loosely related to the phrase"
        else:
            continue  # omitted = not relevant
        out.append({"index": idx, "score": score, "reason": why + "."})
    return json.dumps(out)


def reply_rerank(user: str) -> str:
    q = user.split("## Natural Language Query", 1)[-1].split("# Output Format", 1)[0]
    phrase = set(concepts(q))
    blocks: list[tuple[int, str, str]] = []
    cur = None
    for line in user.splitlines():
        sm = re.match(r"^\[(\d+)\] Source: ([^,]+),", line)
        if sm:
            cur = [int(sm.group(1)), sm.group(2), line]
            blocks.append(cur)  # type: ignore[arg-type]
        elif cur is not None and line.startswith("    ") or (cur is not None and line.startswith("      -")):
            cur[2] += " " + line
        elif line.startswith("#") and cur is not None:
            cur = None
    out = []
    for idx, src, text in blocks:
        cov = overlap(phrase, set(concepts(text)))
        score = 3 if cov >= 0.6 else 2 if cov >= 0.3 else 1 if cov > 0 else 0
        out.append({"source": src, "index": idx, "score": score, "_cov": cov})
    out.sort(key=lambda r: (-r["score"], -r["_cov"], r["index"]))
    for r in out:
        del r["_cov"]
    return json.dumps(out)


def clarifier(name_tokens: list[str], desc: str) -> list[str]:
    """Extra words for a keyphrase: synonyms of the concepts in the name."""
    have = set(name_tokens)
    extra: list[str] = []
    for t in name_tokens + tokens(desc):
        c = WORD_TO_CONCEPT.get(t)
        if c:
            for w in CONCEPTS[c][:4]:
                if w not in have and w not in extra:
                    extra.append(w)
        if len(extra) >= 3:
            break
    return extra[:3]


# What a real model would know about an opaque field name that the name itself
# does not say. Without this a keyphrase could only re-say the name, and the
# stand-in's embedding already treats synonyms as one word, so a keyphrase
# would never change a ranking. With it, a query the name and doc cannot answer
# ("registration signup") can be answered by the generated phrase alone.
KNOWS = {
    "created_at": "signup registration",
    "lifetime_orders": "repeat frequency loyalty",
    "gross_margin": "profitability markup",
    "days_to_ship": "fulfillment speed",
    "traffic_source": "referral origin",
}


def reply_keyphrase_batch(user: str) -> str:
    out = []
    for blk in re.split(r"### Field \[", user)[1:]:
        idx = int(blk.split("]", 1)[0])
        nm = re.search(r"Field name: (.*)", blk)
        ds = re.search(r"Description: (.*)", blk)
        name = nm.group(1).strip() if nm else ""
        desc = ds.group(1).strip() if ds else ""
        nt = tokens(name)
        phrase = " ".join(nt + clarifier(nt, desc) + KNOWS.get(name, "").split())
        out.append({"index": idx, "keyphrase": phrase or name})
    return json.dumps(out)


def reply_summary(user: str) -> str:
    nm = re.search(r"## Source Name\s+(.*?)\s+##", user, re.S)
    ds = re.search(r"## Source Annotations / Documentation\s+(.*?)\s+##", user, re.S)
    name = nm.group(1).strip() if nm else "source"
    docs = re.sub(r"\s+", " ", ds.group(1)).strip() if ds else ""
    return json.dumps({
        "summary": f"The `{name}` source. {docs}".strip(),
        "one_line_summary": (f"{docs}"[:118] if docs else f"The `{name}` source."),
    })


REPLIERS = {
    "refine": reply_refine,
    "rerank": reply_rerank,
    "keyphrase_batch": reply_keyphrase_batch,
    "summary": reply_summary,
}


# --------------------------------------------------------------------------- http


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_a):
        pass

    def _send(self, code: int, body: object) -> None:
        raw = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def _body(self) -> dict:
        n = int(self.headers.get("Content-Length") or 0)
        return json.loads(self.rfile.read(n) or b"{}")

    def do_GET(self):
        if self.path.startswith("/__log"):
            with LOCK:
                return self._send(200, LOG)
        self._send(404, {"error": "not found"})

    def do_POST(self):
        path = self.path.split("?")[0]
        body = self._body()
        if path == "/__control":
            with LOCK:
                STATE.update({k: v for k, v in body.items() if k in STATE})
            return self._send(200, STATE)
        if path == "/__reset":
            with LOCK:
                LOG.clear()
                STATE.update({"mode": "ok", "latencyMs": 0, "embedMode": "ok"})
            return self._send(200, STATE)

        if path.endswith("/embeddings"):
            inputs = body.get("input") or []
            if isinstance(inputs, str):
                inputs = [inputs]
            data, prefixed = [], 0
            with LOCK:
                embed_mode = STATE["embedMode"]
            if embed_mode == "http500":
                log("embeddings", n=len(inputs), failed=True, texts=inputs)
                return self._send(500, {"error": {"message": "injected embedding failure"}})
            req = {k: v for k, v in body.items() if k != "input"}
            if FORWARD["base"]:
                real = forward("/embeddings", {**body, "model": FORWARD["embed_model"]})
                log("embeddings", n=len(inputs), prefixed=sum(1 for t in inputs if t.startswith(PREFIXES)),
                    sample=inputs[:3], texts=inputs, req=req)
                return self._send(200, real)
            sensitive = "prefixed" in str(body.get("model") or "")
            for i, t in enumerate(inputs):
                v, had = embed(t, sensitive)
                prefixed += 1 if had else 0
                data.append({"index": i, "embedding": v})
            log("embeddings", n=len(inputs), prefixed=prefixed,
                sample=inputs[:3], texts=inputs, req=req)
            return self._send(200, {"data": data, "model": body.get("model"),
                                    "usage": {"prompt_tokens": sum(len(t) // 4 for t in inputs)}})

        if path.endswith("/chat/completions"):
            msgs = body.get("messages") or []
            system = next((m["content"] for m in msgs if m["role"] == "system"), "")
            user = next((m["content"] for m in msgs if m["role"] == "user"), "")
            st = stage_of(system, user)
            log("chat", stage=st, system=system, user=user, model=body.get("model"),
                req={k: v for k, v in body.items() if k != "messages"})
            with LOCK:
                mode, lat = STATE["mode"], STATE["latencyMs"]
            if lat:
                time.sleep(lat / 1000)
            if mode == "http500":
                return self._send(500, {"error": {"message": "injected failure"}})
            if mode == "hang":
                time.sleep(120)
            if FORWARD["base"] and mode not in ("badjson",):
                real = forward("/chat/completions", {**body, "model": FORWARD["chat_model"]})
                return self._send(200, real)
            if mode == "badjson":
                text = "Sure! Here is what I think: not json at all."
            else:
                fn = REPLIERS.get(st)
                text = fn(user) if fn else "[]"
            return self._send(200, {
                "model": body.get("model"),
                "choices": [{"index": 0, "message": {"role": "assistant", "content": text},
                             "finish_reason": "stop"}],
                "usage": {"prompt_tokens": len(user) // 4, "completion_tokens": len(text) // 4},
            })
        self._send(404, {"error": "not found"})


FORWARD = {"base": "", "key": "", "embed_model": "", "chat_model": ""}


def forward(path: str, body: dict) -> dict:
    """Send a request on to the real provider and return its JSON reply."""
    import urllib.request
    req = urllib.request.Request(
        FORWARD["base"] + path, data=json.dumps(body).encode(), method="POST",
        headers={"Content-Type": "application/json",
                 "Authorization": f"Bearer {FORWARD['key']}"})
    with urllib.request.urlopen(req, timeout=120) as r:
        return json.loads(r.read().decode())


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=4960)
    ap.add_argument("--forward-base", default="",
                    help="send embeddings and chat on to this OpenAI-compatible base "
                         "(e.g. https://api.openai.com/v1) instead of answering them. "
                         "The key is read from FORWARD_API_KEY, never from the command line. "
                         "Requests are still logged, and failure injection still applies.")
    ap.add_argument("--forward-embed-model", default="text-embedding-3-small")
    ap.add_argument("--forward-chat-model", default="gpt-4o-mini")
    a = ap.parse_args()
    if a.forward_base:
        import os
        FORWARD.update(base=a.forward_base.rstrip("/"), key=os.environ["FORWARD_API_KEY"],
                       embed_model=a.forward_embed_model, chat_model=a.forward_chat_model)
        print(f"forwarding to {FORWARD['base']} ({a.forward_embed_model}, {a.forward_chat_model})", flush=True)
    srv = ThreadingHTTPServer(("127.0.0.1", a.port), Handler)
    print(f"mock provider on http://127.0.0.1:{a.port}/v1", flush=True)
    srv.serve_forever()


if __name__ == "__main__":
    main()
