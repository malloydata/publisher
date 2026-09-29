# Copyright (c) Credible Data Inc.
# SPDX-License-Identifier: MIT
"""Embedding prefixes (`embedding.queryPrefix` / `documentPrefix`).

Uses the stand-in's prefix-sensitive model, which blurs any text that lacks its
prefix, the way nomic-embed-text degrades without `search_query: ` and
`search_document: `.
"""

import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from harness import Pub, brief, entities, mock  # noqa: E402

OUT = pathlib.Path(__import__("os").environ.get("VAL_OUT", "/tmp/llm-val/results"))
Q = [("revenue", [("measure", "total revenue")]),
     ("postal", [("dimension", "customer postal code")]),
     ("nothing", [("measure", "weather forecast temperature")])]
QP, DP = "search_query: ", "search_document: "
results: dict = {}


def measure(label: str, p: Pub) -> None:
    rows = {}
    for qid, t in Q:
        r = p.ask(t, override={})
        top = None
        for c in r.get("sources") or []:
            for e in c.get("entities") or []:
                top = max(top or 0, e.get("relevance") or 0)
        rows[qid] = {"top_relevance": top, "n": len(entities(r)),
                     "below_cutoff": r.get("below_cutoff_count"),
                     "total": r.get("total_entities")}
    results[label] = rows
    print(f"{label:<34}", "  ".join(
        f"{q}: top {rows[q]['top_relevance']} n={rows[q]['n']} below={rows[q]['below_cutoff']}/{rows[q]['total']}"
        for q, _ in Q))


def embed_calls() -> tuple[int, int, int]:
    """(requests, texts, texts that carried a prefix) since the last reset."""
    log = [r for r in mock("/__log") if r["kind"] == "embeddings" and not r.get("failed")]
    return (len(log), sum(r["n"] for r in log), sum(r.get("prefixed", 0) for r in log))


mock("/__reset", {})
print("== a model that needs prefixes, with and without them")
with Pub("prefix-none", retrieval={}, embedding_model="mock-embed-prefixed") as p:
    p.warm()
    measure("prefixed model, no prefixes", p)
with Pub("prefix-both", embedding_model="mock-embed-prefixed",
         retrieval={"embedding": {"queryPrefix": QP, "documentPrefix": DP}}) as p:
    mock("/__reset", {})
    p.warm()
    req, texts, prefixed = embed_calls()
    print(f"   index build sent {texts} texts in {req} requests; {prefixed} carried a prefix")
    results["prefix-both index"] = {"requests": req, "texts": texts, "prefixed": prefixed}
    measure("prefixed model, both prefixes", p)
    mock("/__reset", {})
    p.ask([("measure", "revenue")], override={"cache": None} if False else {})
    log = [r for r in mock("/__log") if r["kind"] == "embeddings"]
    print("   a query embeds:", [t for r in log for t in r["texts"]])
with Pub("prefix-query-only", embedding_model="mock-embed-prefixed",
         retrieval={"embedding": {"queryPrefix": QP}}) as p:
    p.warm()
    measure("prefixed model, query prefix only", p)
with Pub("prefix-plain", retrieval={}) as p:
    p.warm()
    measure("ordinary model, no prefixes", p)

print("\n== does a changed document prefix re-embed, and an unchanged one not?")
with Pub("prefix-restart", embedding_model="mock-embed-prefixed",
         retrieval={"embedding": {"queryPrefix": QP, "documentPrefix": DP}}) as p:
    p.warm()
    mock("/__reset", {})
    p.restart()
    p.warm()
    req, texts, _ = embed_calls()
    print(f"   restart, same settings: {texts} texts embedded")
    results["restart same"] = texts
    mock("/__reset", {})
    p.restart(retrieval={"embedding": {"queryPrefix": QP, "documentPrefix": "search_document:  "}})
    p.warm()
    req, texts, _ = embed_calls()
    print(f"   restart, documentPrefix changed: {texts} texts embedded")
    results["restart doc prefix changed"] = texts
    mock("/__reset", {})
    p.restart(retrieval={"embedding": {"queryPrefix": "search_query:  ", "documentPrefix": "search_document:  "}})
    p.warm()
    req, texts, _ = embed_calls()
    print(f"   restart, only queryPrefix changed: {texts} texts embedded")
    results["restart query prefix changed"] = texts

(OUT / "prefix.json").write_text(json.dumps(results, indent=1))
