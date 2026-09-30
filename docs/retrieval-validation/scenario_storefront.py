# Copyright (c) Credible Data Inc.
# SPDX-License-Identifier: MIT
"""Every retrieval configuration, on the storefront example, end to end.

Storefront is the ideal-doc example shipped in `examples/storefront`, with 12
verified questions and their expected entities. Each configuration is started
on it (or set per request where it can be) and asked all 12 questions; a config
PASSES when it answers every request without an error, its stages report
success, and it finds the expected entities at least as often as a floor.
"""

import json
import pathlib
import re
import shutil
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from harness import Pub, brief, chars, entities, mock  # noqa: E402

OUT = pathlib.Path(__import__("os").environ.get("VAL_OUT", "/tmp/llm-val/results"))
SRC = pathlib.Path(__file__).resolve().parents[2] / "examples" / "storefront"
DST = pathlib.Path("/tmp/llm-val/pkg/storefront")
NC = {"llm": {"cache": {"enabled": False}}}
results: list = []


def setup_package() -> None:
    if DST.exists():
        shutil.rmtree(DST)
    shutil.copytree(SRC, DST, ignore=shutil.ignore_patterns("node_modules", "evals", "tests", "public"))
    model = DST / "storefront.malloy"
    text = model.read_text()
    for label in ("Category", "Brand", "Region"):
        pat = re.compile(rf'(#\(doc\) [^\n]*\n)(\s*)(# label="{label}")')
        assert pat.search(text), label
        text = pat.sub(rf"\1\2#(index)\n\2\3", text, count=1)
    model.write_text(text)


CASES = [json.loads(l) for l in (SRC / "evals" / "storefront-tour" / "cases.jsonl").read_text().splitlines()]


def to_flat(eid: str) -> str:
    """`kind:source:name` -> `source.name`, the form a response uses."""
    _, source, name = eid.split(":", 2)
    return f"{source}.{name}"


def satisfied(case: dict, got: set[str]) -> tuple[int, int]:
    """(requirements met, requirements) for one case."""
    exp = case.get("expectedEntities") or {}
    met = total = 0
    for eid in exp.get("required") or []:
        total += 1
        met += to_flat(eid) in got
    for group in exp.get("requiredAnyOf") or []:
        total += 1
        met += any(to_flat(e) in got for e in group)
    return met, total


def ask_all(p: Pub, override: dict | None) -> dict:
    met = total = 0
    ents = size = 0
    errors: list[str] = []
    stages: dict[str, set] = {}
    warnings: set[str] = set()
    retrieval: set = set()
    for c in CASES:
        q = c["question"]
        r = p.ask([("measure", q), ("dimension", q), ("view", q)], override=override if override is not None else {})
        if r.get("_isError") or r.get("_error"):
            errors.append(str(r.get("_text") or r.get("error") or r)[:200])
            continue
        got = set(entities(r))
        m, t = satisfied(c, got)
        met, total = met + m, total + t
        ents += len(got)
        size += chars(r)
        retrieval.add(r.get("retrieval"))
        for k, v in (r.get("retrieval_stages") or {}).items():
            stages.setdefault(k, set()).add(v)
        warnings.update(w[:90] for w in (r.get("warnings") or []) if "cut at" not in w)
    n = len(CASES)
    return {"recall": round(met / total, 3) if total else None, "met": f"{met}/{total}",
            "entities": round(ents / n, 1), "chars": round(size / n), "errors": errors,
            "stages": {k: sorted(v) for k, v in stages.items()}, "retrieval": sorted(str(x) for x in retrieval),
            "warnings": sorted(warnings)}


def report(label: str, row: dict, need_stages: list[str] = (), floor: float = 0.5, want_retrieval: str | None = None) -> None:
    problems = []
    if row["errors"]:
        problems.append(f"{len(row['errors'])} errors: {row['errors'][0]}")
    for s in need_stages:
        seen = row["stages"].get(s, [])
        ok = [x for x in seen if x == "ok" or x.startswith("skipped:few") or x.startswith("skipped:no_cand")]
        if not seen or len(ok) != len(seen):
            problems.append(f"stage {s} reported {seen or 'nothing'}")
    if want_retrieval and row["retrieval"] != [want_retrieval]:
        problems.append(f"retrieval was {row['retrieval']}, wanted {want_retrieval}")
    if row["recall"] is not None and row["recall"] < floor:
        problems.append(f"recall {row['recall']} under the floor {floor}")
    ok = not problems
    results.append({"config": label, "ok": ok, "problems": problems, **row})
    print(f"   {'PASS' if ok else 'FAIL'}  {label:<52} recall {row['met']:>6}  ents {row['entities']:>5}  chars {row['chars']:>6}"
          f"  {row['stages'] or ''}" + (f"  !! {'; '.join(problems)}" if problems else ""))


setup_package()
mock("/__reset", {})

print("== no provider at all: the word-match path, exactly as before")
with Pub("sf-lexical", retrieval=None, embeddings=False, llm=False, gate=False, package=DST, package_name="storefront") as p:
    p.ask([("source", "orders")])
    report("unconfigured (lexical)", ask_all(p, None), want_retrieval="None", floor=0.3)

print("\n== embeddings only, query-time settings on one warm server")
with Pub("sf-emb", retrieval={}, package=DST, package_name="storefront") as p:
    p.warm()
    S = lambda label, ov=None, stages=(), **kw: report(label, ask_all(p, ov), list(stages), **kw)  # noqa: E731
    S("default (embeddings only)", {}, want_retrieval="semantic")
    S("embedding.minSimilarity 0.3", {"embedding": {"minSimilarity": 0.3}})
    S("embedding.facets [doc]", {"embedding": {"facets": ["doc"]}})
    S("candidates.perTargetLimit 5", {"candidates": {"perTargetLimit": 5}})
    S("candidates.window per-source", {"candidates": {"window": "per-source"}})
    S("response.gapCut 0.8", {"response": {"gapCut": 0.8}}, floor=0.3)
    S("response.maxEntitiesPerSourceTarget 3", {"response": {"maxEntitiesPerSourceTarget": 3}})
    S("response.maxChars 6000", {"response": {"maxChars": 6000}}, floor=0.2)
    S("hybrid rerank-only", {"hybrid": {"mode": "rerank-only"}})
    S("hybrid union", {"hybrid": {"mode": "union"}})
    S("refine", {"refine": {"enabled": True}, **NC}, ["refine"])
    S("refine minLevel HIGH", {"refine": {"enabled": True, "minLevel": "HIGH"}, **NC}, ["refine"], floor=0.3)
    S("refine maxPerSource 3", {"refine": {"enabled": True, "maxPerSource": 3}, **NC}, ["refine"])
    S("refine + join damping whole", {"refine": {"enabled": True}, "scoring": {"joinDepthDamping": 0.9, "joinDampingMode": "whole"}, **NC}, ["refine"])
    S("rerank", {"rerank": {"enabled": True}, **NC}, ["rerank"])
    S("rerank beyondTop drop", {"rerank": {"enabled": True, "beyondTop": "drop"}, **NC}, ["rerank"], floor=0.3)
    S("refine + rerank", {"refine": {"enabled": True}, "rerank": {"enabled": True}, **NC}, ["refine", "rerank"])
    S("refine, per-source window, rerank drop", {"candidates": {"window": "per-source"}, "refine": {"enabled": True},
                                                 "rerank": {"enabled": True, "beyondTop": "drop"}, **NC}, ["refine", "rerank"], floor=0.3)
    S("trace summary", {"trace": {"defaultLevel": "summary"}})
    r = p.ask([("measure", "total revenue")], override={"refine": {"enabled": True}, **NC}, trace="full")
    cands = (r.get("retrieval_trace") or {}).get("candidates") or []
    levelled = [c for c in cands if c.get("levels")]
    ok = bool(cands) and len(levelled) > 0
    results.append({"config": "trace full carries refine levels", "ok": ok})
    print(f"   {'PASS' if ok else 'FAIL'}  trace full carries refine levels ({len(levelled)} of {len(cands)} candidates)")

print("\n== keyphrases and summaries (facets), and one vector per entity")
with Pub("sf-enrich", retrieval={"enrichment": {"enabled": True, "sourceSummary": {"enabled": True}}}, package=DST, package_name="storefront") as p:
    st = p.warm()
    e = st.get("enrichment") or {}
    ok = e.get("status") == "ready" and e.get("enriched", 0) > 0
    results.append({"config": "enrichment ready", "ok": ok, "status": e})
    print(f"   {'PASS' if ok else 'FAIL'}  enrichment ready: {e}")
    report("facets + keyphrases + summaries", ask_all(p, {}), want_retrieval="semantic")
    report("  + refine", ask_all(p, {"refine": {"enabled": True}, **NC}), ["refine"])
with Pub("sf-single", retrieval={"embedding": {"representation": "single"}, "enrichment": {"enabled": True}}, package=DST, package_name="storefront") as p:
    st = p.warm()
    rows = st.get("embeddedRows")
    ents = st.get("totalEntities")
    ok = rows is not None and rows <= (ents or 0) + 5
    results.append({"config": "single index has about one row per entity", "ok": ok, "rows": rows, "entities": ents})
    print(f"   {'PASS' if ok else 'FAIL'}  single index: {rows} rows for {ents} entities")
    report("single representation", ask_all(p, {}), want_retrieval="semantic")
    report("single + per-source + refine", ask_all(p, {"candidates": {"window": "per-source"}, "refine": {"enabled": True}, **NC}), ["refine"])

print("\n== dimension values, with and without the LLM refine")
VALCFG = {"dimensionalValues": {"mode": "annotated"}, "egress": {"dimensionalValues": True}}
with Pub("sf-values", retrieval=VALCFG, package=DST, package_name="storefront") as p:
    st = p.warm()
    v = st.get("valueIndex") or {}
    # Three tags, inherited by every source that extends order_items.
    ok = v.get("status") == "ready" and v.get("dimensions", 0) >= 3 and v.get("failed") == 0
    results.append({"config": "value index ready (3 tags, inherited by extending sources)", "ok": ok, "status": v})
    print(f"   {'PASS' if ok else 'FAIL'}  value index: {v}")

    def values(q: str, ov: dict) -> list[str]:
        r = p.ask([("dimensional_value", q)], override={**ov, **NC})
        return sorted({x["value"] for c in r.get("sources", []) for e in c.get("entities", []) for x in e.get("values", [])}), r

    off, _ = values("Outerwear", {})
    on, r = values("Outerwear", {"dimensionalValues": {"refine": {"enabled": True}}})
    ok = "Outerwear" in off and on == ["Outerwear"] and (r.get("retrieval_stages") or {}).get("valueRefine") == "ok"
    results.append({"config": "value refine keeps only the value asked for", "ok": ok, "off": off[:8], "on": on})
    print(f"   {'PASS' if ok else 'FAIL'}  'Outerwear': {len(off)} values without refine, {on} with")
    none_on, r2 = values("Nonexistentium", {"dimensionalValues": {"refine": {"enabled": True}}})
    ok = none_on == [] and (r2.get("retrieval_stages") or {}).get("valueRefine") in ("ok", None)
    results.append({"config": "value refine returns nothing for a value that is not there", "ok": ok, "on": none_on})
    print(f"   {'PASS' if ok else 'FAIL'}  'Nonexistentium' with refine: {none_on}")

print("\n== Credible-like combination, everything on")
FULL = {
    "embedding": {"representation": "single"},
    "candidates": {"window": "per-source", "perSourceLimit": 10},
    "enrichment": {"enabled": True},
    "refine": {"enabled": True},
    "rerank": {"enabled": True, "beyondTop": "drop"},
    "scoring": {"joinDepthDamping": 0.9, "joinDampingMode": "whole"},
    "dimensionalValues": {"mode": "annotated", "refine": {"enabled": True}},
    "egress": {"dimensionalValues": True},
    "llm": {"cache": {"enabled": False}},
}
with Pub("sf-full", retrieval=FULL, package=DST, package_name="storefront") as p:
    p.warm()
    report("Credible-like, all on", ask_all(p, {}), ["refine", "rerank"], want_retrieval="semantic")
    r = p.ask([("dimension", "product category"), ("dimensional_value", "Outerwear")], override={})
    ok = (r.get("retrieval_stages") or {}).get("valueRefine") == "ok"
    results.append({"config": "full config: value target with an entity target", "ok": ok, "stages": r.get("retrieval_stages")})
    print(f"   {'PASS' if ok else 'FAIL'}  mixed request stages: {r.get('retrieval_stages')}")

print("\n== word-match path with the LLM stages on (no embeddings)")
with Pub("sf-lexllm", retrieval={"refine": {"enabled": True}, "rerank": {"enabled": True}, **NC}, embeddings=False, package=DST, package_name="storefront") as p:
    p.ask([("source", "orders")])
    report("lexical + refine + rerank", ask_all(p, {}), ["refine", "rerank"], floor=0.3)

print("\n== the LLM failing: every stage keeps its order and says so")
with Pub("sf-fail", retrieval={"refine": {"enabled": True}, "rerank": {"enabled": True},
                               "llm": {"cache": {"enabled": False}, "backoffMs": 10}}, package=DST, package_name="storefront") as p:
    p.warm()
    mock("/__control", {"mode": "http500"})
    row = ask_all(p, {})
    mock("/__reset", {})
    seen = {s for v in row["stages"].values() for s in v}
    ok = not row["errors"] and row["entities"] > 0 and all(s.startswith(("failed", "skipped")) for s in seen)
    results.append({"config": "LLM down: answers anyway", "ok": ok, **row})
    print(f"   {'PASS' if ok else 'FAIL'}  LLM 500: {row['entities']} entities per request, stages {row['stages']}")

bad = [r for r in results if not r["ok"]]
print(f"\n{len(results) - len(bad)} passed, {len(bad)} failed, of {len(results)} configurations")
(OUT / "storefront.json").write_text(json.dumps(results, indent=1, default=str))
mock("/__reset", {})
