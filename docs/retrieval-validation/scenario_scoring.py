# Copyright (c) Credible Data Inc.
# SPDX-License-Identifier: MIT
"""Published scores: join damping, knots and source relevance.

These only show once refine has rated candidates, so refine is on for every ask.
"""

import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from harness import Pub, find, mock  # noqa: E402

OUT = pathlib.Path(__import__("os").environ.get("VAL_OUT", "/tmp/llm-val/results"))
MULTI = [("measure", "total sales"), ("dimension", "customer country"), ("dimension", "order status")]
results: list = []


def check(name: str, ok: bool, detail: str = "") -> None:
    results.append({"name": name, "ok": bool(ok), "detail": detail})
    print(f"   {'PASS' if ok else 'FAIL'}  {name}" + (f"  [{detail}]" if detail else ""))


mock("/__reset", {})
with Pub("s-scoring", retrieval={}) as p:
    p.warm()
    NC = {"llm": {"cache": {"enabled": False}}}

    def ask(targets, ov):
        return p.ask(targets, override={"refine": {"enabled": True}, **ov, **NC})

    zip_q = [("dimension", "customer postal code")]
    joined, direct = "order_items.users.zip", "users.zip"
    a = find(ask(zip_q, {}), joined)
    b = find(ask(zip_q, {"scoring": {"joinDepthDamping": 0.5}}), joined)
    check("joinDepthDamping lowers the score of a field reached through a join",
          a and b and b["relevance"] < a["relevance"], f"{a and a['relevance']} -> {b and b['relevance']}")
    d_a = find(ask([("measure", "total revenue")], {}), "order_items.total_sales")
    d_b = find(ask([("measure", "total revenue")], {"scoring": {"joinDepthDamping": 0.5}}), "order_items.total_sales")
    check("and leaves a field of the source itself alone",
          d_a and d_b and d_a["relevance"] == d_b["relevance"], f"{d_a and d_a['relevance']} vs {d_b and d_b['relevance']}")

    k1 = find(ask(zip_q, {}), joined)
    k2 = find(ask(zip_q, {"scoring": {"knots": [[0, 0], [4, 1]]}}), joined)
    check("knots change the published relevance numbers", k1 and k2 and k1["relevance"] != k2["relevance"],
          f"{k1 and k1['relevance']} vs {k2 and k2['relevance']}")

    def cards(ov):
        r = ask(MULTI, ov)
        return [(c["source_info"]["resource_id"]["source"], c.get("relevance")) for c in r["sources"]]
    best, cov = cards({}), cards({"scoring": {"sourceRelevance": "coverage"}})
    check("sourceRelevance=coverage changes at least one source card's relevance",
          [r for _, r in best] != [r for _, r in cov],
          f"{len(best)} cards; best-hit {[r for _, r in best][:8]} vs coverage {[r for _, r in cov][:8]}")

bad = [r for r in results if not r["ok"]]
print(f"\n{len(results) - len(bad)} passed, {len(bad)} failed, of {len(results)} checks")
(OUT / "scoring.json").write_text(json.dumps(results, indent=1))
mock("/__reset", {})
