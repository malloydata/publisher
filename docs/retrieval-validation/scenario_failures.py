# Copyright (c) Credible Data Inc.
# SPDX-License-Identifier: MIT
"""When things go wrong: providers failing, budgets, overrides, bad config, cache."""

import json
import pathlib
import subprocess
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import harness  # noqa: E402
from harness import Pub, entities, mock  # noqa: E402

OUT = pathlib.Path(__import__("os").environ.get("VAL_OUT", "/tmp/llm-val/results"))
results: dict = {}
Q = [("measure", "total revenue")]
MULTI = [("measure", "total sales"), ("dimension", "customer country"), ("dimension", "order status")]
NOCACHE = {"llm": {"cache": {"enabled": False}}}
REFINE = {"refine": {"enabled": True}}


def line(r: dict) -> str:
    return (f"{len(entities(r)):>3} entities  stages={r.get('retrieval_stages')}  "
            f"retrieval={r.get('retrieval')}  reason={r.get('retrieval_reason')}  warnings={r.get('warnings')}")


def llm_calls() -> int:
    return sum(1 for r in mock("/__log") if r["kind"] == "chat")


print("== the LLM misbehaves at query time (refine on)")
with Pub("fail-q", retrieval={"refine": {"enabled": True}, "rerank": {"enabled": True},
                              "llm": {"cache": {"enabled": False}, "backoffMs": 50, "timeoutMs": 2000,
                                      "maxAttempts": 2, "breaker": {"failures": 3, "cooldownMs": 4000}}}) as p:
    p.warm()
    for mode in ["ok", "http500", "badjson"]:
        if mode == "badjson":
            time.sleep(4.5)  # let the breaker from the previous case close
        mock("/__reset", {})
        mock("/__control", {"mode": mode})
        r = p.ask(Q, override={})
        results[f"llm {mode}"] = {"entities": len(entities(r)), "stages": r.get("retrieval_stages"),
                                  "warnings": r.get("warnings"), "llm_calls": llm_calls()}
        print(f"   LLM {mode:<8} calls={llm_calls():>2}  {line(r)}")
    print("   -- the same LLM failure repeated: the breaker")
    mock("/__reset", {})
    mock("/__control", {"mode": "http500"})
    for i in range(5):
        before = llm_calls()
        r = p.ask(Q, override={})
        print(f"   ask {i + 1}: LLM calls this ask={llm_calls() - before}  stages={r.get('retrieval_stages')}")
    mock("/__control", {"mode": "ok"})
    r = p.ask(Q, override={})
    print(f"   LLM healthy again, breaker still open: stages={r.get('retrieval_stages')}")
    time.sleep(4.5)
    r = p.ask(Q, override={})
    print(f"   after the 4s cooldown: stages={r.get('retrieval_stages')}")
    results["breaker recovered"] = r.get("retrieval_stages")
    mock("/__reset", {})

print("\n== a slow LLM, and the request budget")
with Pub("slow", retrieval={"refine": {"enabled": True},
                            "llm": {"cache": {"enabled": False}, "timeoutMs": 1500, "maxAttempts": 1,
                                    "requestBudgetMs": 2500, "breaker": {"failures": 50, "cooldownMs": 1000}}}) as p:
    p.warm()
    for lat in (0, 700, 5000):
        mock("/__reset", {})
        mock("/__control", {"latencyMs": lat})
        t0 = time.time()
        r = p.ask(Q, override={})
        secs = round(time.time() - t0, 1)
        results[f"latency {lat}"] = {"seconds": secs, "stages": r.get("retrieval_stages")}
        print(f"   LLM takes {lat:>4}ms per call: answered in {secs:>4}s  {line(r)}")
    mock("/__control", {"latencyMs": 700})
    for label, ov in [("multi-target, refine batchSize 5", {"refine": {"batchSize": 5}})]:
        mock("/__reset", {})
        t0 = time.time()
        r = p.ask(MULTI, override=ov)
        print(f"   {label} at 700ms/call: {round(time.time() - t0, 1)}s calls={llm_calls()} {line(r)}")
    mock("/__reset", {})

print("\n== maxCallsPerRequest")
with Pub("calls", retrieval={"refine": {"enabled": True, "batchSize": 3},
                             "llm": {"cache": {"enabled": False}, "maxCallsPerRequest": 4}}) as p:
    p.warm()
    mock("/__reset", {})
    r = p.ask(MULTI, override={})
    print(f"   maxCallsPerRequest=4, batchSize=3, three targets: LLM calls={llm_calls()}  {line(r)}")
    results["maxCallsPerRequest 4"] = {"calls": llm_calls(), "stages": r.get("retrieval_stages")}
    mock("/__reset", {})

print("\n== the embedding provider goes away")
with Pub("embed-down", retrieval={"refine": {"enabled": True}}) as p:
    p.warm()
    mock("/__reset", {})
    r0 = p.ask(Q, override={})
    print("   healthy      :", line(r0))
    mock("/__control", {"embedMode": "http500"})
    r = p.ask([("measure", "cash receipts")], override={})
    print("   embeddings 500:", line(r))
    results["embedding down"] = {"retrieval": r.get("retrieval"), "reason": r.get("retrieval_reason"),
                                 "entities": len(entities(r)), "stages": r.get("retrieval_stages")}
    r = p.ask([("measure", "cash receipts")], override={})
    print("   asked again   :", line(r))
    mock("/__reset", {})

print("\n== the per-request override header")
with Pub("gate-on", retrieval={}) as p:
    p.warm()
    r = p.ask(Q, override={"response": {"maxEntitiesPerSourceTarget": 1}})
    print("   valid override        :", len(entities(r)), "entities; fingerprint present:", "retrieval_config" in r)
    for label, ov in [("unknown key (typo)", {"refin": {"enabled": True}}),
                      ("bad value", {"refine": {"minLevel": "MAYBE"}}),
                      ("index-time key", {"enrichment": {"enabled": True}}),
                      ("egress key", {"egress": {"preset": "full"}}),
                      ("not an object", [1, 2])]:
        r = p.ask(Q, override=ov)
        msg = r.get("_text") or r.get("_error") or r
        print(f"   {label:<22}: error={bool(r.get('_isError'))} -> {str(msg)[:330]}")
        results[f"override {label}"] = str(msg)[:400]
    r = p.ask(Q, override={}, trace="bogus")
    print("   bad trace level       :", str(r.get("_text") or r.get("retrieval_trace") or "ignored")[:200])
with Pub("gate-off", retrieval={}, gate=False) as p:
    p.warm()
    r = p.ask(Q, override={"response": {"maxEntitiesPerSourceTarget": 1}}, trace="full")
    print(f"   gate OFF (env unset)  : override ignored -> {len(entities(r))} entities (default 35 or so); trace present: {'retrieval_trace' in r}")
    results["gate off"] = {"entities": len(entities(r)), "trace": "retrieval_trace" in r}

print("\n== the LLM result cache")
with Pub("cache", retrieval={"refine": {"enabled": True}}) as p:
    p.warm()
    for label, ov in [("first ask", {}), ("same ask again", {}), ("cache disabled for this ask", NOCACHE)]:
        mock("/__reset", {})
        r = p.ask(Q, override=ov, trace="summary")
        gate = next((g for g in (r.get("retrieval_trace") or {}).get("gates", []) if g["gate"] == "refine"), {})
        print(f"   {label:<28} LLM calls={llm_calls()}  trace llm_calls={gate.get('llm_calls')} cache_hits={gate.get('cache_hits')}")
        results[f"cache {label}"] = {"calls": llm_calls(), "cache_hits": gate.get("cache_hits")}
    r = p.ask([("measure", "total revenue and sales")], override={})
    mock("/__reset", {})

print("\n== start-up validation (the server must refuse a bad config and say how to fix it)")
def boot(name: str, retrieval, env=None, llm=True, embeddings=True) -> str:
    p = Pub(name, retrieval=retrieval, env=env, llm=llm, embeddings=embeddings)
    try:
        p.start(wait=25)
        warned = [l for l in p.log_tail(400).splitlines() if "retrieval" in l and "warn" in l.lower()]
        p.stop()
        return "STARTED" + (f", logged a warning: {warned[0][-330:]}" if warned else " (no warning)")
    except RuntimeError as e:
        out = str(e)
        keep = [l for l in out.splitlines() if "retrieval" in l.lower() or "LLM_" in l or "EMBEDDING_" in l or "Invalid" in l or "Fix:" in l]
        return (keep[0] if keep else out.splitlines()[-1])[:420]
    finally:
        p.stop()


for label, retr, kw in [
    ("unknown key", {"refin": {"enabled": True}}, {}),
    ("unknown nested key", {"refine": {"minLevl": "HIGH"}}, {}),
    ("bad enum", {"refine": {"minLevel": "MAYBE"}}, {}),
    ("out of range", {"embedding": {"minSimilarity": 1.5}}, {}),
    ("wrong type", {"response": {"gapCut": "half"}}, {}),
    ("refine on, no LLM configured", {"refine": {"enabled": True}}, {"llm": False}),
    ("refine on, LLM base but no model", {"refine": {"enabled": True}},
     {"llm": False, "env": {"LLM_API_BASE": harness.MOCK + "/v1"}}),
    ("enrichment on, no embeddings", {"enrichment": {"enabled": True}}, {"embeddings": False}),
    ("llm.enabled=false, refine on", {"llm": {"enabled": False}, "refine": {"enabled": True}}, {}),
    ("values on, no embeddings ok", {"dimensionalValues": {"mode": "annotated"}}, {"embeddings": False}),
    ("hybrid bad mode", {"hybrid": {"mode": "both"}}, {}),
    ("knots not increasing", {"scoring": {"knots": [[0, 0], [0, 1]]}}, {}),
]:
    msg = boot("cfg", retr, **kw)
    results[f"boot {label}"] = msg
    print(f"   {label:<30} -> {msg}")

(OUT / "failures.json").write_text(json.dumps(results, indent=1, default=str))
mock("/__reset", {})
