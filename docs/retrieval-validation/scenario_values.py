# Copyright (c) Credible Data Inc.
# SPDX-License-Identifier: MIT
"""Dimension value search (`dimensionalValues.*`)."""

import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from harness import Pub, entities, mock, sources  # noqa: E402

OUT = pathlib.Path(__import__("os").environ.get("VAL_OUT", "/tmp/llm-val/results"))
results: dict = {}
V = lambda text: [("dimensional_value", text)]  # noqa: E731


def vi(st: dict) -> str:
    v = st.get("valueIndex") or {}
    return (f"{v.get('status')} dimensions={v.get('dimensions')} values={v.get('values')} "
            f"truncated={v.get('truncated')} failed={v.get('failed')}")


def hits(r: dict) -> list:
    out = []
    for c in r.get("sources") or []:
        src = c["source_info"]["resource_id"]["source"]
        for e in c.get("entities") or []:
            if "values" in e:
                out.append((f"{src}.{e['name']}", [(v["value"], v.get("relevance")) for v in e["values"]],
                            {k: e[k] for k in ("values_indexed", "values_truncated") if k in e}))
    return out


def ask(p: Pub, text: str, ov: dict | None = None):
    r = p.ask(V(text), override=ov if ov is not None else {})
    return r, hits(r)


ANN = {"dimensionalValues": {"mode": "annotated"}}

print("== off by default: the message a value target got before")
with Pub("values-off", retrieval={}) as p:
    p.warm()
    r = p.ask(V("Jeans"), override={})
    print("   warnings:", r.get("warnings"))
    results["off"] = r.get("warnings")

print("\n== annotated: what is indexed, and what it finds")
with Pub("values-on", retrieval=ANN) as p:
    st = p.warm()
    print(f"   {vi(st)}   (6 dimensions are tagged; 2 sit on gated sources)")
    results["annotated status"] = st.get("valueIndex")
    for q in ["Jeans", "jeans", "Organic", "Female", "Jeanss", "Jea", "Denim", "Men", "Nonexistent"]:
        r, h = ask(p, q)
        print(f"   {q!r:<14} -> {h if h else 'no hits'}  sources={sources(r)}"
              f"{'  warnings=' + str(r.get('warnings')) if r.get('warnings') else ''}")
        results[f"query {q}"] = h
    r, _ = ask(p, "Organic")
    print("   value-only response keys:", sorted(k for k in r if k != "sources"))
    gated = [s for s in sources(r) if s in ("restricted_users", "country_users")]
    print("   gated sources in any value hit:", gated or "none")
    results["gated in hits"] = gated
    print("\n   mixed request (an entity target and a value target):")
    r = p.ask([("dimension", "product category"), ("dimensional_value", "Jeans")], override={})
    print("   ", hits(r)[:2], "retrieval marker:", r.get("retrieval"))
    print("\n   scoped to one source:")
    r = p.ask(V("Organic"), override={}, scope={"source": "users"})
    print("    source=users   ->", [h[0] for h in hits(r)])
    r = p.ask(V("Organic"), override={}, scope={"source": "product"})
    print("    source=product ->", [h[0] for h in hits(r)] or "no hits")

    print("\n== query-time settings")
    for label, ov in [("maxHitsPerTarget=1", {"dimensionalValues": {"maxHitsPerTarget": 1}}),
                      ("minSimilarity=0.95", {"dimensionalValues": {"minSimilarity": 0.95}})]:
        r, h = ask(p, "Jeans", ov)
        print(f"   {label:<22} 'Jeans' -> {h}")
    r, h = ask(p, "ea", {"dimensionalValues": {"maxHitsPerTarget": 1}})
    print("   maxHitsPerTarget=1 'ea' ->", h)
    r, h = ask(p, "ea")
    print("   default          'ea' ->", h)

print("\n== what gets indexed, by setting")
for label, retr in [
    ("annotated (default)", {"dimensionalValues": {"mode": "annotated"}}),
    ("auto, include product.*", {"dimensionalValues": {"mode": "auto", "include": ["product.*"]}}),
    ("auto, include everything", {"dimensionalValues": {"mode": "auto", "include": ["*.*"]}}),
    ("auto, include all but brand", {"dimensionalValues": {"mode": "auto", "include": ["*.*"], "exclude": ["*.brand"]}}),
    ("maxValuesPerDimension=2", {"dimensionalValues": {"mode": "annotated", "maxValuesPerDimension": 2}}),
    ("maxValuesPerPackage=5", {"dimensionalValues": {"mode": "annotated", "maxValuesPerPackage": 5}}),
    ("onOverflow=skip, max 2", {"dimensionalValues": {"mode": "annotated", "maxValuesPerDimension": 2, "onOverflow": "skip"}}),
    ("maxValueChars=5", {"dimensionalValues": {"mode": "annotated", "maxValueChars": 5}}),
    ("embed=false (words only)", {"dimensionalValues": {"mode": "annotated", "embed": False}}),
]:
    with Pub("values-knob", retrieval=retr) as p:
        mock("/__reset", {})
        st = p.warm()
        embedded = sum(r["n"] for r in mock("/__log") if r["kind"] == "embeddings")
        r, h = ask(p, "Jeans")
        rr, hh = ask(p, "Organic")
        results[f"knob {label}"] = {"status": st.get("valueIndex"), "jeans": h, "organic": hh}
        print(f"   {label:<30} {vi(st)}")
        print(f"      'Jeans' -> {h if h else 'no hits'};  'Organic' -> {[(x[0], x[2]) for x in hh] if hh else 'no hits'}")

print("\n== words versus meaning")
for label, retr in [("lexical + embeddings (default)", {"dimensionalValues": {"mode": "annotated"}}),
                    ("lexical off (embeddings only)", {"dimensionalValues": {"mode": "annotated", "lexical": False}}),
                    ("embeddings off (words only)", {"dimensionalValues": {"mode": "annotated", "embed": False}})]:
    with Pub("values-arms", retrieval=retr) as p:
        p.warm()
        row = {}
        for q in ["Jeans", "Jeanss", "Jea", "Denim"]:
            _, h = ask(p, q)
            row[q] = [(x[0], [v[0] for v in x[1]]) for x in h]
        results[f"arms {label}"] = row
        print(f"   {label:<32} {row}")

print("\n== a source whose values the server cannot read")
with Pub("values-noembed", retrieval=ANN, embeddings=False) as p:
    import time as _t
    r, h = ask(p, "Jeans")
    print("   first question:", h or "no hits", "|", r.get("warnings"))
    for _ in range(60):
        r, h = ask(p, "Jeans")
        if h:
            break
        _t.sleep(1)
    print(f"   after the index built: 'Jeans' -> {[(x[0], [v[0] for v in x[1]][:2]) for x in h][:2]}")
    st = p.package_status()
    print("   package resource embeddingIndex:", st or "absent (no embedding provider), so valueIndex progress is not visible")
    results["no embeddings"] = {"hits": h, "package status": st}

print("\n== a dimension cut at the cap still comes back for a value it cannot rule out")
with Pub("values-cut", retrieval={"dimensionalValues": {"mode": "annotated", "maxValuesPerDimension": 2}}) as p:
    p.warm()
    r = p.ask(V("Organic"), override={})
    for c in r.get("sources", [])[:1]:
        for e in c.get("entities", []):
            print("  ", json.dumps(e)[:300])
    print("   warnings:", r.get("warnings"))

(OUT / "values.json").write_text(json.dumps(results, indent=1, default=str))
