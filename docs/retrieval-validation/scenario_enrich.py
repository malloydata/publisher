# Copyright (c) Credible Data Inc.
# SPDX-License-Identifier: MIT
"""Index-time LLM text: keyphrases and source summaries (`enrichment.*`)."""

import json
import pathlib
import re
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from harness import Pub, entities, find, mock, rank  # noqa: E402

OUT = pathlib.Path(__import__("os").environ.get("VAL_OUT", "/tmp/llm-val/results"))
results: dict = {}
# Words that appear only in a generated keyphrase (never in a name or a doc), so
# only the keyphrase facet can answer it.
KP_QUERY = [("dimension", "signup registration")]
KP_TARGET = "created_at"


def calls() -> dict:
    out: dict = {"embed_texts": 0}
    for r in mock("/__log"):
        if r["kind"] == "chat":
            out[r["stage"]] = out.get(r["stage"], 0) + 1
        elif r["kind"] == "embeddings" and not r.get("failed"):
            out["embed_texts"] += r["n"]
    return out


def fields_in_keyphrase_prompts() -> int:
    return sum(len(re.findall(r"### Field \[", r["user"]))
               for r in mock("/__log") if r["kind"] == "chat" and r["stage"] == "keyphrase_batch")


def summarize_status(st: dict) -> str:
    e = st.get("enrichment") or {}
    return (f"{e.get('status')} eligible={e.get('eligible')} enriched={e.get('enriched')} "
            f"failed={e.get('failed')} deferred={e.get('deferredByBudget')}")


def timed_warm(p: Pub, latency_ms: int = 0):
    mock("/__reset", {})
    if latency_ms:
        mock("/__control", {"latencyMs": latency_ms})
    t0 = time.time()
    st = p.warm()
    return st, round(time.time() - t0, 1)


ON = {"enrichment": {"enabled": True, "sourceSummary": {"enabled": True}}}

# ---------------------------------------------------------------- baseline: no enrichment
print("== the question only a keyphrase can answer:", KP_QUERY)
with Pub("enrich-off", retrieval={}) as p:
    p.warm()
    r = p.ask(KP_QUERY, override={})
    results["off rank"] = rank(r, KP_TARGET)
    print(f"   enrichment off: users.created_at is at position {results['off rank']} of {len(entities(r))}")

# ---------------------------------------------------------------- on
with Pub("enrich-on", retrieval=ON) as p:
    st, secs = timed_warm(p)
    c = calls()
    results["on build"] = {"seconds": secs, "calls": c, "status": st.get("enrichment"),
                           "keyphrase_fields": fields_in_keyphrase_prompts()}
    print(f"\n== enrichment on: index ready in {secs}s; {summarize_status(st)}")
    print(f"   LLM calls {c}; fields covered by keyphrase calls: {fields_in_keyphrase_prompts()}")
    vi = st.get("valueIndex")
    for label, ov in [("default facets", {}),
                      ("facets=[name,doc] (no kw)", {"embedding": {"facets": ["name", "doc"]}}),
                      ("facets=[kw] only", {"embedding": {"facets": ["kw"]}}),
                      ("facets=[sum] only", {"embedding": {"facets": ["sum"]}})]:
        r = p.ask(KP_QUERY, override=ov)
        results[f"on rank {label}"] = rank(r, KP_TARGET)
        print(f"   {label:<28} users.created_at at position {rank(r, KP_TARGET)} of {len(entities(r))}")

    print("\n== a source-level question (summaries)")
    for label, ov in [("default", {}), ("facets=[name,doc]", {"embedding": {"facets": ["name", "doc"]}}),
                      ("facets=[sum]", {"embedding": {"facets": ["sum"]}})]:
        r = p.ask([("source", "customer accounts and demographics")], override=ov)
        print(f"   {label:<20} sources: {[e for e in entities(r) if e.endswith('.users')][:1] or 'users not found'}"
              f" first cards: {[c['source_info']['resource_id']['source'] for c in r.get('sources', [])][:4]}")

    print("\n== generated text stays out of the response unless asked")
    r = p.ask(KP_QUERY, override={})
    e = find(r, KP_TARGET)
    print("   default keys on created_at:", sorted(e) if e else None)
    results["default keys"] = sorted(e) if e else None
    p.stop()

with Pub("enrich-surface", retrieval={**ON, "response": {"surfaceGenerated": True}}) as p:
    p.warm()
    r = p.ask(KP_QUERY, override={})
    e = find(r, KP_TARGET)
    print("   surfaceGenerated keys on created_at:", sorted(e) if e else None)
    print("   generated_description:", (e or {}).get("generated_description"))
    r2 = p.ask([("source", "customer accounts")], override={})
    card = next((c for c in r2.get("sources", []) if c["source_info"]["resource_id"]["source"] == "users"), None)
    print("   users card generated_summary:", (card or {}).get("source_info", {}).get("generated_summary")
          or (card or {}).get("generated_summary"))
    results["surface keys"] = sorted(e) if e else None

# ---------------------------------------------------------------- restart keeps the cache
print("\n== restart: is the generated text kept?")
with Pub("enrich-restart", retrieval=ON) as p:
    p.warm()
    mock("/__reset", {})
    p.restart()
    st = p.warm()
    c = calls()
    results["restart"] = {"calls": c, "status": st.get("enrichment")}
    print(f"   after restart: {summarize_status(st)}; LLM calls {c}")
    r = p.ask(KP_QUERY, override={})
    print(f"   keyphrase answer available immediately: position {rank(r, KP_TARGET)}")

# ---------------------------------------------------------------- knobs that change what is generated
print("\n== what gets generated, by setting (LLM calls / fields covered / eligible)")
for label, retr in [
    ("when-sparse (default)", ON),
    ("keyphrase mode=always", {"enrichment": {"enabled": True, "keyphrase": {"mode": "always"}}}),
    ("keyphrase mode=never", {"enrichment": {"enabled": True, "keyphrase": {"mode": "never"}}}),
    ("wordThreshold=3", {"enrichment": {"enabled": True, "keyphrase": {"wordThreshold": 3}}}),
    ("wordThreshold=40", {"enrichment": {"enabled": True, "keyphrase": {"wordThreshold": 40}}}),
    ("batchSize=2", {"enrichment": {"enabled": True, "keyphrase": {"batchSize": 2}}}),
    ("summary only", {"enrichment": {"enabled": True, "keyphrase": {"mode": "never"},
                                     "sourceSummary": {"enabled": True}}}),
    ("summary off (keyphrases only)", {"enrichment": {"enabled": True}}),
]:
    with Pub("enrich-knob", retrieval=retr) as p:
        st, secs = timed_warm(p)
        c = calls()
        llm = {k: v for k, v in c.items() if k != "embed_texts"}
        results[f"knob {label}"] = {"calls": llm, "fields": fields_in_keyphrase_prompts(),
                                    "status": st.get("enrichment"), "embed_texts": c["embed_texts"]}
        print(f"   {label:<32} calls={llm}  keyphrase fields={fields_in_keyphrase_prompts():>3}  "
              f"{summarize_status(st)}  embedded texts={c['embed_texts']}")

# ---------------------------------------------------------------- budgets
print("\n== hard per-sync limits")
with Pub("enrich-budget", retrieval={"enrichment": {"enabled": True, "sourceSummary": {"enabled": True}},
                                     "indexing": {"maxLlmCallsPerSync": 2}}) as p:
    st, secs = timed_warm(p)
    c = calls()
    print(f"   maxLlmCallsPerSync=2: {summarize_status(st)}; calls {c}")
    results["budget 2"] = {"status": st.get("enrichment"), "calls": c}
    # The package still serves on what exists.
    r = p.ask([("measure", "total revenue")], override={})
    print("   still answers:", r.get("retrieval"), len(entities(r)), "entities")
    mock("/__reset", {})
    p.restart(retrieval={"enrichment": {"enabled": True, "sourceSummary": {"enabled": True}},
                         "indexing": {"maxLlmCallsPerSync": 2}})
    st = p.warm()
    c = calls()
    print(f"   next sync (same cap 2): {summarize_status(st)}; calls {c}")
    results["budget 2 next"] = {"status": st.get("enrichment"), "calls": c}
    mock("/__reset", {})
    p.restart(retrieval={"enrichment": {"enabled": True, "sourceSummary": {"enabled": True}}})
    st = p.warm()
    c = calls()
    print(f"   cap lifted: {summarize_status(st)}; calls {c}")
    results["budget lifted"] = {"status": st.get("enrichment"), "calls": c}

with Pub("enrich-deadline", retrieval={"enrichment": {"enabled": True, "sourceSummary": {"enabled": True}},
                                       "indexing": {"deadlineMs": 1500}}) as p:
    st, secs = timed_warm(p, latency_ms=800)
    print(f"   deadlineMs=1500 with 800ms per call: done in {secs}s; {summarize_status(st)}; calls {calls()}")
    results["deadline"] = {"seconds": secs, "status": st.get("enrichment"), "calls": calls()}
    mock("/__reset", {})

with Pub("enrich-items", retrieval={"enrichment": {"enabled": True},
                                    "indexing": {"maxItemsPerPackage": 300}}) as p:
    st = p.warm()
    print(f"   maxItemsPerPackage=300 (package has ~739 rows): embeddingIndex={json.dumps({k: v for k, v in st.items() if k != 'enrichment'})}")
    r = p.ask([("measure", "total revenue")], override={})
    print("   answers with:", r.get("retrieval"), r.get("retrieval_reason"), len(entities(r)))
    results["items 300"] = {"status": {k: v for k, v in st.items() if k != "enrichment"},
                            "retrieval": r.get("retrieval"), "reason": r.get("retrieval_reason")}

# ---------------------------------------------------------------- LLM failing during indexing
print("\n== the LLM failing while indexing")
mock("/__reset", {})
mock("/__control", {"mode": "http500"})
with Pub("enrich-fail", retrieval={"enrichment": {"enabled": True, "retryAfterMs": 4000,
                                                   "sourceSummary": {"enabled": True}},
                                    "llm": {"breaker": {"failures": 3, "cooldownMs": 3000}}}) as p:
    st = p.warm()
    print(f"   LLM returns 500: {summarize_status(st)}; embeddingIndex status {st.get('status')}")
    r = p.ask([("measure", "total revenue")], override={})
    print("   base index still serves:", r.get("retrieval"), len(entities(r)), "entities")
    results["fail"] = {"status": st.get("enrichment"), "index": st.get("status")}
    mock("/__control", {"mode": "ok"})
    time.sleep(5)
    p.ask([("measure", "total revenue")], override={})
    st = p.warm()
    print(f"   LLM back, after retryAfterMs: {summarize_status(st)}")
    results["fail recovered"] = st.get("enrichment")
mock("/__reset", {})

(OUT / "enrich.json").write_text(json.dumps(results, indent=1, default=str))
