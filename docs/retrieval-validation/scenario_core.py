# Copyright (c) Credible Data Inc.
# SPDX-License-Identifier: MIT
"""Query-time settings, one at a time, on one warm server.

Every row changes exactly one setting from the baseline (embeddings only, no LLM
stage) through the X-Publisher-Retrieval header, so a difference in a row is that
setting's doing.
"""

import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from harness import Pub, brief, chars, entities, mock, rank  # noqa: E402

OUT = pathlib.Path(__import__("os").environ.get("VAL_OUT", "/tmp/llm-val/results"))
OUT.mkdir(parents=True, exist_ok=True)

# (id, targets, the entity a person would expect at the top)
QUERIES = [
    ("revenue", [("measure", "total revenue")], "order_items.total_sales"),
    ("customers", [("measure", "number of customers")], "order_items.user_count"),
    ("postal", [("dimension", "customer postal code")], "order_items.users.zip"),
    ("margin", [("measure", "profit margin"), ("dimension", "product category")],
     "order_items.total_gross_margin"),
    ("ship", [("dimension", "time to ship an order")], "order_items.days_to_ship"),
    ("multi", [("measure", "total sales"), ("dimension", "customer country"),
               ("dimension", "order status")], "order_items.total_sales"),
    ("nothing", [("measure", "weather forecast temperature")], None),
]


def calls_since_reset() -> dict:
    log = mock("/__log")
    out: dict = {}
    for r in log:
        k = r.get("stage") if r["kind"] == "chat" else r["kind"]
        out[k] = out.get(k, 0) + 1
    return out


def one(p: Pub, qid: str, targets, override=None, trace=None) -> dict:
    mock("/__reset", {})
    payload = p.ask(targets, override=override, trace=trace)
    b = brief(payload)
    b["llm_calls"] = {k: v for k, v in calls_since_reset().items() if k not in ("embeddings",)}
    b["qid"] = qid
    b["_payload"] = payload
    return b


def sweep(p: Pub, label: str, override, results: dict, trace=None) -> dict:
    rows, tot = [], {"ents": 0, "chars": 0, "found": 0, "expected": 0, "rank": 0, "llm": 0}
    for qid, targets, want in QUERIES:
        b = one(p, qid, targets, override, trace)
        r = rank(b["_payload"], want) if want else None
        b["rank_of_expected"] = r
        b["expected"] = want
        b.pop("_payload")
        rows.append(b)
        tot["ents"] += len(b["entities"])
        tot["chars"] += b["chars"]
        tot["llm"] += sum(b["llm_calls"].values())
        if want:
            tot["expected"] += 1
            tot["found"] += 1 if r else 0
            tot["rank"] += r or 0
    n = len(QUERIES)
    summary = {
        "label": label, "override": override,
        "mean_entities": round(tot["ents"] / n, 1),
        "mean_chars": round(tot["chars"] / n),
        "expected_found": f"{tot['found']}/{tot['expected']}",
        "mean_rank_when_found": round(tot["rank"] / tot["found"], 2) if tot["found"] else None,
        "llm_calls": tot["llm"],
        "nothing_returned": len(next(r for r in rows if r["qid"] == "nothing")["entities"]),
    }
    results[label] = {"summary": summary, "rows": rows}
    s = summary
    print(f"{label:<44} ents {s['mean_entities']:>5}  chars {s['mean_chars']:>6}  "
          f"found {s['expected_found']:>4}  rank {s['mean_rank_when_found']!s:>5}  "
          f"llm {s['llm_calls']:>3}  nothing→{s['nothing_returned']}")
    return summary


def main() -> None:
    results: dict = {}
    with Pub("core", retrieval={}) as p:
        st = p.warm()
        print("index:", json.dumps(st))
        def S(label, ov=None, trace=None):
            # An LLM row runs with the result cache off, so its call count is
            # what the setting costs and not what an earlier row already paid.
            ov = dict(ov or {})
            if ("refine" in ov or "rerank" in ov) and "llm" not in ov:
                ov["llm"] = {"cache": {"enabled": False}}
            return sweep(p, label, ov, results, trace)

        print("\n== is it deterministic? (same request, three times)")
        runs = [one(p, "revenue", QUERIES[0][1], {}) for _ in range(3)]
        same = all(r["entities"] == runs[0]["entities"] for r in runs)
        results["determinism"] = {"same_order_3x": same}
        print("same delivered order 3x:", same)
        S("baseline (embeddings only)", {})
        S("baseline again", {})
        for v in (0.05, 0.1, 0.3, 0.4, 0.5, 0.7):
            S(f"embedding.minSimilarity={v}", {"embedding": {"minSimilarity": v}})

        print("\n== which facets are scored")
        S("facets=[name]", {"embedding": {"facets": ["name"]}})
        S("facets=[doc]", {"embedding": {"facets": ["doc"]}})
        S("facets=[name,doc]", {"embedding": {"facets": ["name", "doc"]}})

        print("\n== how many candidates and how many are shown")
        for v in (2, 5, 30):
            S(f"candidates.perTargetLimit={v}", {"candidates": {"perTargetLimit": v}})
        for v in (1, 3, 5, 30):
            S(f"response.maxEntitiesPerSourceTarget={v}",
              {"response": {"maxEntitiesPerSourceTarget": v}})

        print("\n== response-size levers")
        for v in (0.3, 0.6, 0.8, 0.95):
            S(f"response.gapCut={v}", {"response": {"gapCut": v}})
        for v in (1500, 4000, 8000):
            S(f"response.maxChars={v}", {"response": {"maxChars": v}})

        print("\n== refine (LLM levels)")
        S("refine on (MEDIUM)", {"refine": {"enabled": True}})
        S("refine minLevel=LOW", {"refine": {"enabled": True, "minLevel": "LOW"}})
        S("refine minLevel=HIGH", {"refine": {"enabled": True, "minLevel": "HIGH"}})
        S("refine dropOmitted=false", {"refine": {"enabled": True, "dropOmitted": False}})
        S("refine dropOmitted=false unscored=LOW",
          {"refine": {"enabled": True, "dropOmitted": False, "unscoredLevel": "LOW"}})
        S("refine maxPerSource=2", {"refine": {"enabled": True, "maxPerSource": 2}})
        S("refine maxCandidates=5", {"refine": {"enabled": True, "maxCandidates": 5}})
        S("refine batchSize=3", {"refine": {"enabled": True, "batchSize": 3}})
        S("refine skipIfAtMost=100", {"refine": {"enabled": True, "skipIfAtMost": 100}})
        S("refine matchReason=false",
          {"refine": {"enabled": True}, "response": {"matchReason": False}})

        print("\n== rerank (LLM source order)")
        S("rerank on", {"rerank": {"enabled": True}})
        S("rerank topSources=1", {"rerank": {"enabled": True, "topSources": 1}})
        S("rerank topSources=2", {"rerank": {"enabled": True, "topSources": 2}})
        for v in (0, 1, 3):
            S(f"rerank minScore={v}", {"rerank": {"enabled": True, "minScore": v}})
        S("rerank beyondTop=drop topSources=2",
          {"rerank": {"enabled": True, "topSources": 2, "beyondTop": "drop"}})
        S("rerank skipIfAtMost=100", {"rerank": {"enabled": True, "skipIfAtMost": 100}})
        S("refine + rerank", {"refine": {"enabled": True}, "rerank": {"enabled": True}})

        print("\n== scoring")
        S("scoring.joinDepthDamping=0.5", {"scoring": {"joinDepthDamping": 0.5}})
        S("refine + sourceRelevance=coverage",
          {"refine": {"enabled": True}, "scoring": {"sourceRelevance": "coverage"}})
        S("refine + knots flat [[0,0],[4,1]]",
          {"refine": {"enabled": True}, "scoring": {"knots": [[0, 0], [4, 1]]}})

        print("\n== hybrid (lunr merged into the embedding ranking)")
        S("hybrid rerank-only", {"hybrid": {"mode": "rerank-only"}})
        S("hybrid union", {"hybrid": {"mode": "union"}})
        S("hybrid rerank-only rrfK=1", {"hybrid": {"mode": "rerank-only", "rrfK": 1}})
        S("hybrid rerank-only rrfK=1000", {"hybrid": {"mode": "rerank-only", "rrfK": 1000}})

    (OUT / "core.json").write_text(json.dumps(results, indent=1))
    print("\nwrote", OUT / "core.json")


if __name__ == "__main__":
    main()
