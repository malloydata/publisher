# Copyright (c) Credible Data Inc.
# SPDX-License-Identifier: MIT
"""The settings the other scenarios do not reach, each checked against what it should do.

Every check prints PASS or FAIL with the evidence. Where a setting changes what is
sent to a provider, the evidence is the request as the proxy saw it.
"""

import json
import pathlib
import re
import subprocess
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from harness import Pub, SERVER_DIR, entities, find, mock  # noqa: E402

OUT = pathlib.Path(__import__("os").environ.get("VAL_OUT", "/tmp/llm-val/results"))
HERE = pathlib.Path(__file__).resolve().parent
Q = [("measure", "total revenue")]
MULTI = [("measure", "total sales"), ("dimension", "customer country"), ("dimension", "order status")]
NOCACHE = {"cache": {"enabled": False}}
results: list = []


def check(name: str, ok: bool, detail: str = "") -> None:
    results.append({"name": name, "ok": bool(ok), "detail": detail})
    print(f"   {'PASS' if ok else 'FAIL'}  {name}" + (f"  [{detail}]" if detail else ""))


def chat(stage: str | None = None) -> list:
    return [r for r in mock("/__log") if r["kind"] == "chat" and (stage is None or r["stage"] == stage)]


def embeds() -> list:
    return [r for r in mock("/__log") if r["kind"] == "embeddings" and not r.get("failed")]


def reset() -> None:
    mock("/__reset", {})


def timed(p: Pub, targets, ov=None) -> tuple[dict, float]:
    t0 = time.time()
    r = p.ask(targets, override=ov if ov is not None else {})
    return r, time.time() - t0


def db(root: str, sql: str):
    out = subprocess.run(["bun", "run", str(HERE / "dbq.ts"), f"/tmp/llm-val/roots/{root}/publisher.db", sql],
                         cwd=str(SERVER_DIR), capture_output=True, text=True).stdout.strip().splitlines()
    return json.loads(out[-1]) if out else None


REFINE = {"refine": {"enabled": True}}

print("== what goes on the wire to the embedding endpoint")
reset()
with Pub("s-embed", retrieval={"embedding": {"extraBody": {"user": "idx"}, "queryExtraBody": {"user": "qry"}}}) as p:
    p.warm()
    docs = embeds()
    check("embedding.extraBody is sent with indexed text", docs and all(r["req"].get("user") == "idx" for r in docs),
          f"{len(docs)} index requests")
    reset()
    p.ask(Q, override={})
    q = embeds()
    check("embedding.queryExtraBody is merged over it for a question", q and all(r["req"].get("user") == "qry" for r in q))

print("\n== what goes on the wire to the chat endpoint")
reset()
with Pub("s-llm-default", retrieval={**REFINE, "llm": NOCACHE}) as p:
    p.warm()
    reset()
    p.ask(Q, override={})
    req = chat("refine")[0]["req"]
    check("defaults: temperature 0, seed 7, no response_format",
          req.get("temperature") == 0 and req.get("seed") == 7 and "response_format" not in req, json.dumps(req)[:160])
reset()
with Pub("s-llm-custom", retrieval={**REFINE, "llm": {**NOCACHE, "temperature": 0.3, "seed": 11,
                                                      "jsonMode": "json_object", "extraBody": {"user": "eval"}}}) as p:
    p.warm()
    reset()
    r = p.ask(Q, override={})
    req = chat("refine")[0]["req"]
    check("llm.temperature and llm.seed reach the request", req.get("temperature") == 0.3 and req.get("seed") == 11)
    check("llm.jsonMode=json_object sets response_format", req.get("response_format") == {"type": "json_object"})
    check("llm.extraBody is merged into the request", req.get("user") == "eval")
    check("json mode still gives a readable refine result", r.get("retrieval_stages", {}).get("refine") == "ok",
          str(r.get("retrieval_stages")))

print("\n== a model per stage")
reset()
with Pub("s-models", retrieval={"refine": {"enabled": True}, "rerank": {"enabled": True},
                                "enrichment": {"enabled": True, "sourceSummary": {"enabled": True}},
                                "llm": {**NOCACHE, "model": "base-m",
                                        "models": {"refine": "refine-m", "rerank": "rerank-m",
                                                   "keyphrase": "kp-m", "summary": "sum-m"}}}) as p:
    p.warm()
    p.ask(MULTI, override={})
    seen = {}
    for r in chat():
        seen.setdefault(r["stage"], set()).add(r["req"].get("model"))
    check("each stage is called with its own model",
          seen.get("refine") == {"refine-m"} and seen.get("rerank") == {"rerank-m"}
          and seen.get("keyphrase_batch") == {"kp-m"} and seen.get("summary") == {"sum-m"}, str(seen))
reset()
with Pub("s-models2", retrieval={"refine": {"enabled": True}, "llm": {**NOCACHE, "model": "base-m"}}) as p:
    p.warm()
    reset()
    p.ask(Q, override={"llm": {"models": {"refine": "override-m"}}})
    check("llm.models.<stage> can be overridden per request",
          {r["req"]["model"] for r in chat("refine")} == {"override-m"})

print("\n== concurrency (every call is held for 400ms so the difference shows)")
times = {}
for c in (1, 4, 8):
    reset()
    with Pub("s-conc", retrieval={"refine": {"enabled": True, "batchSize": 3},
                                  "llm": {**NOCACHE, "concurrency": c, "maxCallsPerRequest": 300,
                                          "requestBudgetMs": 180_000, "timeoutMs": 60_000}}) as p:
        p.warm()
        reset()
        mock("/__control", {"latencyMs": 400})
        r, secs = timed(p, MULTI)
        times[c] = (secs, len(chat("refine")))
        print(f"   llm.concurrency={c}: {secs:.1f}s for {len(chat('refine'))} refine calls")
        mock("/__control", {"latencyMs": 0})
check("llm.concurrency: more slots, less waiting", times[1][0] > times[4][0] > times[8][0] * 0.9,
      str({k: round(v[0], 1) for k, v in times.items()}))
reset()
with Pub("s-refconc", retrieval={"refine": {"enabled": True, "batchSize": 3},
                                 "llm": {**NOCACHE, "concurrency": 8, "maxCallsPerRequest": 300,
                                         "requestBudgetMs": 180_000}}) as p:
    p.warm()
    ts = {}
    for c in (1, 8):
        reset()
        mock("/__control", {"latencyMs": 400})
        _, ts[c] = timed(p, MULTI, {"refine": {"enabled": True, "batchSize": 3, "concurrency": c}})
    mock("/__control", {"latencyMs": 0})
    check("refine.concurrency caps refine below llm.concurrency", ts[1] > ts[8] * 1.5,
          f"1: {ts[1]:.1f}s  8: {ts[8]:.1f}s")

print("\n== retries and backoff (the LLM answers 500 every time; one refine batch)")
for attempts, backoff in ((1, 300), (3, 300)):
    reset()
    mock("/__control", {"mode": "http500"})
    with Pub("s-retry", retrieval={"refine": {"enabled": True, "batchSize": 100},
                                   "llm": {**NOCACHE, "maxAttempts": attempts, "backoffMs": backoff,
                                           "breaker": {"failures": 50, "cooldownMs": 1000}}}) as p:
        p.warm()
        reset()
        mock("/__control", {"mode": "http500"})
        _, secs = timed(p, Q)
        n = len(chat("refine"))
        print(f"   maxAttempts={attempts}, backoffMs={backoff}: {n} calls in {secs:.1f}s")
        results.append({"name": f"retry {attempts}", "calls": n, "seconds": round(secs, 1), "ok": True, "detail": ""})
        if attempts == 1:
            one = n
        else:
            check("llm.maxAttempts sets the tries per call", n == attempts * one, f"{n} vs {one} x {attempts}")
            check("llm.backoffMs waits between tries (300ms then 600ms)", secs >= 0.85, f"{secs:.1f}s")
    mock("/__control", {"mode": "ok"})

print("\n== how much of each description the LLM sees (refine.descChars)")
reset()
with Pub("s-desc", retrieval={"refine": {"enabled": True}, "llm": NOCACHE}) as p:
    p.warm()
    for n in (300, 12):
        reset()
        p.ask([("measure", "customer lifetime spend")], override={"refine": {"enabled": True, "descChars": n}})
        longest = 0
        for r in chat("refine"):
            for line in r["user"].splitlines():
                m = re.match(r"^- \[\d+\] .*?\): (.*)$", line)
                if m:
                    longest = max(longest, len(m.group(1)))
        print(f"   descChars={n}: longest description sent is {longest} characters")
        results.append({"name": f"descChars {n}", "longest": longest, "ok": True, "detail": ""})
        if n == 300:
            long_default = longest
        else:
            check("refine.descChars truncates descriptions (with an ellipsis)", longest <= n + 1 and longest < long_default,
                  f"{longest} vs {long_default}")

print("\n== the word-match path (no embedding provider): refine.onLexical and rerank.onLexical")
reset()
with Pub("s-lex", retrieval={"refine": {"enabled": True}, "rerank": {"enabled": True, "skipIfAtMost": 0},
                             "llm": NOCACHE}, embeddings=False) as p:
    r = p.ask([("measure", "revenue"), ("dimension", "customer")], override={})
    check("refine and rerank run on word-match candidates by default",
          r.get("retrieval_stages", {}).get("refine") == "ok", str(r.get("retrieval_stages")))
    r = p.ask([("measure", "revenue"), ("dimension", "customer")],
              override={"refine": {"enabled": True, "onLexical": False}, "rerank": {"enabled": True, "onLexical": False}})
    check("onLexical=false skips both", r.get("retrieval_stages") == {"refine": "skipped:lexical", "rerank": "skipped:lexical"},
          str(r.get("retrieval_stages")))

print("\n== what the rerank prompt contains")
reset()
with Pub("s-rr", retrieval={"rerank": {"enabled": True, "skipIfAtMost": 0}, "llm": NOCACHE}) as p:
    p.warm()
    reset()
    p.ask(MULTI, override={"rerank": {"enabled": True, "maxEntityLines": 3, "skipIfAtMost": 0}})
    per_source = []
    for r in chat("rerank"):
        cur = 0
        for line in r["user"].splitlines():
            if re.match(r"^\[\d+\] Source:", line):
                per_source.append(cur)
                cur = 0
            elif line.startswith("      - "):
                cur += 1
        per_source.append(cur)
    check("rerank.maxEntityLines caps the entity lines per source", per_source and max(per_source) <= 3, f"max {max(per_source)}")
reset()
RR = {"rerank": {"enabled": True, "skipIfAtMost": 0}, "llm": NOCACHE,
      "enrichment": {"enabled": True, "keyphrase": {"mode": "never"}, "sourceSummary": {"enabled": True}},
      "dimensionalValues": {"mode": "annotated"}}
VALQ = [("dimension", "product category"), ("dimensional_value", "Jeans")]
with Pub("s-rr-default", retrieval=RR) as p:
    p.warm()
    reset()
    p.ask(VALQ, override={})
    prompt = "\n".join(r["user"] for r in chat("rerank"))
    check("the rerank prompt carries the generated source summary", "Summary:" in prompt)
    check("but no dimension values with the default egress", "Values:" not in prompt)
reset()
with Pub("s-rr-values", retrieval={**RR, "egress": {"dimensionalValues": True}}) as p:
    p.warm()
    reset()
    p.ask(VALQ, override={})
    prompt = "\n".join(r["user"] for r in chat("rerank"))
    check("with egress.dimensionalValues the prompt lists matched values", "Values: Jeans" in prompt)
    reset()
    p.ask(VALQ, override={"rerank": {"enabled": True, "skipIfAtMost": 0, "valuesPerEntity": 0}})
    prompt = "\n".join(r["user"] for r in chat("rerank"))
    check("rerank.valuesPerEntity=0 removes them again", "Values:" not in prompt)

print("\n== generated text: template, code cap, views")
reset()
with Pub("s-kp-template", retrieval={"enrichment": {"enabled": True, "keyphrase": {"template": "{name} => {keyphrase}"}}}) as p:
    p.warm()
    texts = [t for r in embeds() for t in r["texts"] if " => " in t]
    check("keyphrase.template shapes the embedded keyphrase text", len(texts) > 50, f"{len(texts)} texts, e.g. {texts[:1]}")
reset()
with Pub("s-kp-code", retrieval={"egress": {"code": True},
                                 "enrichment": {"enabled": True, "keyphrase": {"mode": "always", "maxCodeChars": 40}}}) as p:
    p.warm()
    longest = 0
    for r in chat("keyphrase_batch"):
        for m in re.finditer(r"Field code:\n(.*?)\n\n<END_OF_ENTITY_CODE>", r["user"], re.S):
            longest = max(longest, len(m.group(1)))
    check("keyphrase.maxCodeChars caps the code in a prompt", 0 < longest <= 45, f"longest {longest}")
elig = {}
for vt in (0, 1000):
    reset()
    with Pub("s-kp-views", retrieval={"enrichment": {"enabled": True, "keyphrase": {"viewWordThreshold": vt}}}) as p:
        st = p.warm()
        elig[vt] = st["enrichment"]["eligible"]
check("keyphrase.viewWordThreshold decides which views get one", elig[0] > elig[1000], str(elig))

print("\n== dimension values: template, refresh")
reset()
with Pub("s-vt", retrieval={"dimensionalValues": {"mode": "annotated", "template": "{value} ({dimension} in {source})"}}) as p:
    p.warm()
    texts = [t for r in embeds() for t in r["texts"] if " in product)" in t]
    check("dimensionalValues.template shapes the embedded value text", len(texts) > 5, f"e.g. {texts[:1]}")
reset()
V = {"dimensionalValues": {"mode": "annotated"}}
with Pub("s-refresh", retrieval=V) as p:
    p.warm()
    p.stop()
    t1 = db("s-refresh", "select max(fetched_at) t from dimension_value_state")[0]["t"]
    p.restart(retrieval=V)
    p.warm()
    p.stop()
    t2 = db("s-refresh", "select max(fetched_at) t from dimension_value_state")[0]["t"]
    check("values read less than refreshMinutes ago are not read again on restart", t1 == t2, f"{t1} then {t2}")
    time.sleep(62)
    V1 = {"dimensionalValues": {"mode": "annotated", "refreshMinutes": 1}}
    p.restart(retrieval=V1)
    p.warm()
    p.ask(Q, override={})
    p.stop()
    t3 = db("s-refresh", "select max(fetched_at) t from dimension_value_state")[0]["t"]
    check("with refreshMinutes=1 they are read again once a minute has passed", t3 != t2, f"{t2} then {t3}")

print("\n== trace, egress switches, cache size, similarity floor from the environment")
reset()
with Pub("s-trace", retrieval={"trace": {"defaultLevel": "summary"}}) as p:
    p.warm()
    r = p.ask(Q, override={})
    check("trace.defaultLevel=summary adds a trace with no header", "retrieval_trace" in r)
with Pub("s-trace-off", retrieval={}) as p:
    p.warm()
    r = p.ask(Q, override={})
    check("and by default there is none", "retrieval_trace" not in r)
reset()
with Pub("s-docs-off", retrieval={"refine": {"enabled": True}, "egress": {"docs": False}, "llm": NOCACHE}) as p:
    p.warm()
    reset()
    p.ask([("measure", "customer lifetime spend")], override={})
    filled = 0
    for r in chat("refine"):
        for line in r["user"].splitlines():
            m = re.match(r"^- \[\d+\] .*?\): (.*)$", line)
            if m and m.group(1).strip():
                filled += 1
    check("egress.docs=false sends no descriptions to refine", filled == 0, f"{filled} lines with text")
reset()
with Pub("s-names-off", retrieval={"refine": {"enabled": True}, "rerank": {"enabled": True}, "egress": {"names": False}}) as p:
    p.warm()
    r = p.ask(MULTI, override={})
    check("egress.names=false stops refine and rerank", r.get("retrieval_stages") == {"refine": "skipped:egress", "rerank": "skipped:egress"},
          str(r.get("retrieval_stages")))
reset()
with Pub("s-cache", retrieval={"refine": {"enabled": True, "batchSize": 3},
                               "llm": {"cache": {"enabled": True, "maxEntries": 2}, "maxCallsPerRequest": 300,
                                       "requestBudgetMs": 120_000}}) as p:
    p.warm()
    p.ask(MULTI, override={})
    reset()
    p.ask(MULTI, override={})
    again = len(chat("refine"))
with Pub("s-cache-big", retrieval={"refine": {"enabled": True, "batchSize": 3},
                                   "llm": {"cache": {"enabled": True, "maxEntries": 2000}, "maxCallsPerRequest": 300,
                                           "requestBudgetMs": 120_000}}) as p:
    p.warm()
    p.ask(MULTI, override={})
    reset()
    p.ask(MULTI, override={})
    big = len(chat("refine"))
check("llm.cache.maxEntries: a tiny cache forgets, a big one remembers", again > 0 and big == 0, f"repeat ask made {again} calls vs {big}")
reset()
base_n = None
with Pub("s-floor-env", retrieval={}, env={"EMBEDDING_MIN_SIMILARITY": "0.55"}) as p:
    p.warm()
    r_env = p.ask(Q, override={})
    r_cfg = p.ask(Q, override={"embedding": {"minSimilarity": 0.2}})
with Pub("s-floor-none", retrieval={}) as p:
    p.warm()
    r_base = p.ask(Q, override={})
check("EMBEDDING_MIN_SIMILARITY is the floor when the config leaves it null",
      len(entities(r_env)) < len(entities(r_base)), f"{len(entities(r_env))} vs {len(entities(r_base))}")
check("and retrieval.embedding.minSimilarity beats it", len(entities(r_cfg)) > len(entities(r_env)),
      f"{len(entities(r_cfg))} vs {len(entities(r_env))}")

# Published scores (join damping, knots, source relevance) are checked in scenario_scoring.py.

bad = [r for r in results if not r["ok"]]
print(f"\n{len(results) - len(bad)} passed, {len(bad)} failed, of {len(results)} checks")
(OUT / "settings.json").write_text(json.dumps(results, indent=1, default=str))
reset()
