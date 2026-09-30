# Copyright (c) Credible Data Inc.
# SPDX-License-Identifier: MIT
"""The settings that match Credible's hosted retrieval, against Publisher's defaults.

One server per index (the representation is part of the index), then query-time
settings swept on each with the override header.
"""

import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import scenario_core as core  # noqa: E402
from harness import Pub, find, mock, sources  # noqa: E402

OUT = pathlib.Path(__import__("os").environ.get("VAL_OUT", "/tmp/llm-val/results"))
NC = {"llm": {"cache": {"enabled": False}}}
results: dict = {}


def S(p: Pub, label: str, ov: dict | None = None):
    ov = dict(ov or {})
    if ("refine" in ov or "rerank" in ov) and "llm" not in ov:
        ov["llm"] = NC["llm"]
    return core.sweep(p, label, ov, results)


print("== entity retrieval: representation and window (7 questions; see scenario_core for the columns)")
ENRICH = {"enrichment": {"enabled": True}}
with Pub("par-facets", retrieval={}) as p:
    p.warm()
    S(p, "facets, global window (Publisher's defaults)", {})
    S(p, "facets, per-source window", {"candidates": {"window": "per-source"}})
    S(p, "facets, per-source, refine", {"candidates": {"window": "per-source"}, "refine": {"enabled": True}})

with Pub("par-single", retrieval={"embedding": {"representation": "single"}, **ENRICH}) as p:
    st = p.warm()
    print("   single index:", json.dumps({k: v for k, v in st.items() if k != "enrichment"}), "| enrichment", (st.get("enrichment") or {}).get("status"))
    S(p, "single, global window", {})
    S(p, "single, per-source window", {"candidates": {"window": "per-source"}})
    S(p, "single, per-source, refine (Credible-like)", {"candidates": {"window": "per-source"}, "refine": {"enabled": True}})
    S(p, "single, per-source, refine, rerank drop", {"candidates": {"window": "per-source"}, "refine": {"enabled": True},
                                                     "rerank": {"enabled": True, "beyondTop": "drop"}})

print("\n== dimension values: what the LLM refine removes")
VAL = {"dimensionalValues": {"mode": "annotated"}, "egress": {"dimensionalValues": True}}
with Pub("par-values", retrieval=VAL) as p:
    p.warm()
    for q in ["Jeans", "Denim", "Organic", "Female", "Nonexistent", "Men", "Levi"]:
        row = {}
        for label, ov in (("off", {}), ("refine", {"dimensionalValues": {"refine": {"enabled": True}}})):
            mock("/__reset", {})
            r = p.ask([("dimensional_value", q)], override={**ov, **NC})
            vals = [(c["source_info"]["resource_id"]["source"], e["name"], [v["value"] for v in e.get("values", [])])
                    for c in r.get("sources", []) for e in c.get("entities", []) if e.get("values")]
            seen = {(s, n): v for s, n, v in vals}
            row[label] = {"values": sum(len(v) for v in seen.values()), "dims": len(seen),
                          "calls": sum(1 for x in mock("/__log") if x["kind"] == "chat"),
                          "stage": (r.get("retrieval_stages") or {}).get("valueRefine"),
                          "sample": [(f"{s}.{n}", v[:4]) for (s, n), v in list(seen.items())[:2]]}
        results[f"values {q}"] = row
        print(f"   {q!r:<14} off: {row['off']['values']:>3} values in {row['off']['dims']} dims"
              f"   refine: {row['refine']['values']:>3} values in {row['refine']['dims']} dims"
              f" ({row['refine']['calls']} calls, {row['refine']['stage']})  kept: {row['refine']['sample']}")

(OUT / "parity.json").write_text(json.dumps(results, indent=1, default=str))
mock("/__reset", {})
