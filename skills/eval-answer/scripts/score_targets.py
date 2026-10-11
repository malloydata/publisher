#!/usr/bin/env python3
"""Score an attempt's get_context searches against the golden search targets.
Stdlib only.

  python3 score_targets.py --events events.jsonl --cases cases.jsonl [--json]

`score_retrieval.py` asks one question per attempt: did the entities the
answer needs reach the agent at all, pooled over every call. This asks the
same thing one search target at a time, which splits a retrieval miss into the
two things that cause it:

  1. Decomposition. Did the agent ask for the right things? Each golden
     target is paired with the agent targets that cover it: the same
     `target_type`, and either a shared content word or a returned entity that
     satisfies the golden target's key. Reported: golden targets covered,
     golden targets the agent asked for under the wrong type, and agent targets
     that cover no golden target (extras).
  2. Retrieval. For each golden target the agent covered, did the entities
     its search returned include the golden target's entities? Recall and
     precision per target, over only the entities that target returned, and
     per attempt over everything returned.
  3. The answer. The verdict on the attempt's `score` event, unchanged.

A golden target the agent never asked for has no retrieval number, because no
search of the agent's could have returned its entities. Its
`recall_anywhere` says whether they arrived through some other target anyway.

WHAT A CASE CARRIES

`searchTargets` on the case, optional. Each target uses the get_context
request's own field names, plus the expected entities in the
`expectedEntities` shape (`required`, `requiredAnyOf`, `acceptable`):

  {"target_type": "measure", "search_text": "revenue",
   "expectedEntities": {"required": ["measure:order_items:total_sales"]}}

A case without `searchTargets` is skipped. Case-level `expectedEntities` is
not read here; `score_retrieval.py` owns it.

WHAT AN ATTEMPT CARRIES

`tool_call` events with `tool: get_context`. `search_targets` (the targets as
sent) when the run recorded it; otherwise `targets` (`"measure: revenue"`)
and `target_shapes` (for a target with no text) are read instead.

Which entities a target returned comes from the server's own attribution:
each hit's `matched_targets` names the search texts that matched it. A source
card carries no attribution, and a lexical run carries none at all, so there a
target is credited with every entity of a kind it can return
(`score_retrieval.KINDS_BY_TARGET`). `attribution` on the row says which rule
applied: `server`, or `kind`, which can only over-credit.

THE WORD MATCH IS A HEURISTIC

Two targets share a content word when their search texts do after lower
casing, dropping a short stop list and a plural ending. "total revenue" covers
"revenue"; "sales" does not cover "revenue", though the entity route still
pairs them when the agent's "sales" target returned the golden target's
entity. It errs both ways: a shared generic word ("total sales" against "total
margin") pairs two different concepts, and a synonym that also returned the
wrong entities pairs nothing. Both target lists are on the row, so a reader
can check a pairing rather than trust it.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from typing import Any

from score_retrieval import (KINDS_BY_TARGET, PASSING, UNSCORED,  # noqa: E402
                             attempt_key, delivery, groups, read_jsonl,
                             split_entity)

# Words that carry no concept. Short on purpose: a word like "total" or
# "count" can be the whole difference between two measures, so it stays.
STOP = {"a", "an", "the", "of", "for", "by", "per", "in", "on", "to", "and",
        "or", "with", "from", "at", "each", "all", "our", "we", "is", "are"}


def words(text: str | None) -> set[str]:
    """Content words of a search text, lower-cased, with a plural removed."""
    out = set()
    for w in re.findall(r"[a-z0-9]+", (text or "").lower()):
        if w in STOP:
            continue
        if len(w) > 4 and w.endswith("ies"):
            w = w[:-3] + "y"
        elif len(w) > 3 and w.endswith("s") and not w.endswith("ss"):
            w = w[:-1]
        out.add(w)
    return out


def agent_targets(call: dict[str, Any]) -> tuple[list[dict[str, Any]], str]:
    """One call's targets as `{target_type, search_text, example_values}`,
    and which field they were read from."""
    sent = call.get("search_targets")
    if isinstance(sent, list):
        return ([{"target_type": str(t.get("target_type") or "?").lower(),
                  "search_text": t.get("search_text"),
                  "example_values": t.get("example_values") or []}
                 for t in sent if isinstance(t, dict)], "search_targets")
    # A run written before `search_targets` was recorded. `targets` holds the
    # targets that carried text; `target_shapes` the bare ones.
    out = []
    for t in call.get("targets") or []:
        if isinstance(t, str) and ": " in t:
            kind, text = t.split(": ", 1)
            if kind.strip().lower() in KINDS_BY_TARGET:
                out.append({"target_type": kind.strip().lower(),
                            "search_text": text.strip(), "example_values": []})
                continue
        if isinstance(t, str) and t.strip():
            out.append({"target_type": "?", "search_text": t.strip(),
                        "example_values": []})
    for s in call.get("target_shapes") or []:
        if isinstance(s, dict) and not s.get("has_text"):
            out.append({"target_type": str(s.get("type") or "?").lower(),
                        "search_text": None, "example_values": []})
    return out, "targets"


def returned_by(target: dict[str, Any],
                call: dict[str, Any]) -> tuple[set[str], str]:
    """The entities this one target returned in its call, and the rule used.

    `server` when the call's hits carry `matched_targets`: the entities whose
    attribution names this target's text, plus the source cards when the
    target asked for sources (a card never carries attribution). `kind`
    otherwise: every returned entity of a kind this target type can return.
    """
    rs = call.get("rankedSummary") or {}
    hits = [h for h in rs.get("hits") or [] if isinstance(h, dict)]
    kinds = KINDS_BY_TARGET.get(target["target_type"], set())
    attributed = any(isinstance(h.get("matched_targets"), list) for h in hits)
    text = target.get("search_text")
    if attributed and text:
        got = {h["entity_id"] for h in hits
               if any(isinstance(m, dict) and m.get("search_text") == text
                      for m in h.get("matched_targets") or [])}
        if "source" in kinds:
            got |= {h["entity_id"] for h in hits
                    if split_entity(h.get("entity_id", ""))[0] == "source"}
        return got, "server"
    ids = [h.get("entity_id") for h in hits] or list(rs.get("entityIds") or [])
    return {e for e in ids if e and split_entity(e)[0] in kinds}, "kind"


def key_of(target: dict[str, Any]) -> tuple[list[list[str]], set[str]]:
    """A golden target's required groups, and every id that is not noise."""
    exp = target.get("expectedEntities") or {}
    req = groups(exp)
    ok = {e for g in req for e in g} | set(exp.get("acceptable") or [])
    return req, ok


def satisfied(req: list[list[str]], got: set[str],
              tokens: set[str] = frozenset()) -> list[bool]:
    return [any(delivery(e, got, tokens) != "missing" for e in g) for g in req]


def score_attempt(case: dict[str, Any], calls: list[dict[str, Any]],
                  key: tuple, verdict: str | None) -> dict[str, Any]:
    golden = [t for t in case.get("searchTargets") or [] if isinstance(t, dict)]
    asked: list[dict[str, Any]] = []
    pooled: set[str] = set()
    tokens: set[str] = set()
    read_from = set()
    for n, call in enumerate(calls, 1):
        rs = call.get("rankedSummary") or {}
        pooled |= set(rs.get("entityIds") or [])
        tokens |= set(rs.get("docTokens") or [])
        targets, src = agent_targets(call)
        read_from.add(src)
        for t in targets:
            got, rule = returned_by(t, call)
            asked.append({**t, "call": n, "returned": got, "attribution": rule})

    used: set[int] = set()
    rows = []
    for i, g in enumerate(golden, 1):
        gtype = str(g.get("target_type") or "").lower()
        gwords = words(g.get("search_text"))
        req, ok = key_of(g)
        cover: list[int] = []
        route: dict[int, str] = {}
        wrong_type: list[int] = []
        for j, a in enumerate(asked):
            # A golden target with no text enumerates its type, and only an
            # agent target that also enumerates matches it by words.
            by_words = (not gwords and not a.get("search_text")) or \
                bool(gwords & words(a.get("search_text")))
            by_entities = bool(req) and any(satisfied(req, a["returned"]))
            if a["target_type"] == gtype and (by_words or by_entities):
                cover.append(j)
                route[j] = "words" if by_words else "entities"
            elif a["target_type"] != gtype and by_words and gwords:
                wrong_type.append(j)
            else:
                continue
            used.add(j)
        own = set().union(*(asked[j]["returned"] for j in cover)) if cover else set()
        rule = sorted({asked[j]["attribution"] for j in cover})
        hit = satisfied(req, own)
        anywhere = satisfied(req, pooled, tokens)
        sent_values = " ".join(
            [str(v) for j in cover for v in asked[j].get("example_values") or []]
            + [asked[j].get("search_text") or "" for j in cover]).lower()
        rows.append({
            "index": i,
            "target_type": gtype,
            "search_text": g.get("search_text"),
            "covered": bool(cover),
            "covered_by": [{"call": asked[j]["call"],
                            "target_type": asked[j]["target_type"],
                            "search_text": asked[j].get("search_text"),
                            "route": route[j]} for j in cover],
            "wrong_type": [{"call": asked[j]["call"],
                            "target_type": asked[j]["target_type"],
                            "search_text": asked[j].get("search_text")}
                           for j in wrong_type],
            "values_missing": ([v for v in g.get("example_values") or []
                                if str(v).lower() not in sent_values]
                               if cover else []),
            "attribution": "/".join(rule) or None,
            "n_required": len(req),
            # Retrieval is scored only where the agent asked. A target nobody
            # searched for is a decomposition miss, not a ranking one.
            "recall": (sum(hit) / len(req)) if (cover and req) else None,
            "precision": (len(own & ok) / len(own)) if (cover and req and own)
            else None,
            "recall_anywhere": (sum(anywhere) / len(req)) if req else None,
            "missing": [" | ".join(grp) for grp, h in zip(req, hit) if not h]
            if cover else [],
            "n_returned": len(own),
        })

    extras = []
    seen = set()
    for j, a in enumerate(asked):
        if j in used:
            continue
        sig = (a["target_type"], " ".join(sorted(words(a.get("search_text")))))
        if sig in seen:
            continue
        seen.add(sig)
        extras.append({"call": a["call"], "target_type": a["target_type"],
                       "search_text": a.get("search_text")})

    # The attempt's own numbers: every golden target's key pooled, against
    # everything any call returned.
    all_req: list[list[str]] = []
    all_ok: set[str] = set()
    for g in golden:
        req, ok = key_of(g)
        for grp in req:
            if grp not in all_req:
                all_req.append(grp)
        all_ok |= ok
    hit = satisfied(all_req, pooled, tokens)
    passed = None if verdict in UNSCORED else verdict in PASSING
    return {
        "qid": case.get("qid"), "sample": key[1], "phase": key[2],
        "verdict": verdict, "passed": passed,
        "n_get_context": len(calls),
        "request_field": "/".join(sorted(read_from)) or None,
        "n_golden": len(rows),
        "n_covered": sum(1 for r in rows if r["covered"]),
        "n_wrong_type": sum(1 for r in rows
                            if not r["covered"] and r["wrong_type"]),
        "n_agent_targets": len(asked),
        "extras": extras,
        "targets": rows,
        "recall": (sum(hit) / len(all_req)) if all_req else None,
        "precision": (len(pooled & all_ok) / len(pooled))
        if (all_req and pooled) else None,
        "n_returned": len(pooled),
    }


def summarise(rows: list[dict[str, Any]]) -> dict[str, Any]:
    mean = lambda xs: (sum(xs) / len(xs)) if xs else None
    targets = [t for r in rows for t in r["targets"]]
    decided = [r for r in rows if r["verdict"] in ("match", "no_match")]
    full = [r for r in decided if r["n_covered"] == r["n_golden"]]
    part = [r for r in decided if r["n_covered"] < r["n_golden"]]
    return {
        "attempts": len(rows),
        "golden_targets": len(targets),
        "covered": sum(1 for t in targets if t["covered"]),
        "wrong_type": sum(1 for t in targets
                          if not t["covered"] and t["wrong_type"]),
        "never_asked": sum(1 for t in targets
                           if not t["covered"] and not t["wrong_type"]),
        "extras": sum(len(r["extras"]) for r in rows),
        "values_missing": sum(1 for t in targets if t["values_missing"]),
        "target_recall": mean([t["recall"] for t in targets
                               if t["recall"] is not None]),
        "target_precision": mean([t["precision"] for t in targets
                                  if t["precision"] is not None]),
        "targets_scored": sum(1 for t in targets if t["recall"] is not None),
        "attempt_recall": mean([r["recall"] for r in rows
                                if r["recall"] is not None]),
        "attempt_precision": mean([r["precision"] for r in rows
                                   if r["precision"] is not None]),
        "decided": len(decided),
        "passed": sum(1 for r in decided if r["verdict"] == "match"),
        "decided_full_cover": len(full),
        "passed_full_cover": sum(1 for r in full if r["verdict"] == "match"),
        "decided_part_cover": len(part),
        "passed_part_cover": sum(1 for r in part if r["verdict"] == "match"),
    }


def score(events: list[dict[str, Any]],
          cases: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    verdicts = {attempt_key(e): e.get("verdict")
                for e in events if e.get("kind") == "score"}
    rows = []
    for e in events:
        if e.get("kind") != "attempt":
            continue
        case = cases.get(e.get("qid"))
        if not case or not case.get("searchTargets"):
            continue
        k = attempt_key(e)
        calls = [t for t in events if t.get("kind") == "tool_call"
                 and t.get("tool") == "get_context" and attempt_key(t) == k]
        rows.append(score_attempt(case, calls, k, verdicts.get(k)))
    rows.sort(key=lambda r: (str(r["qid"]), str(r["sample"])))
    return rows


def fmt_target(t: dict[str, Any]) -> str:
    text = t.get("search_text")
    return f'{t["target_type"]} "{text}"' if text else f"{t['target_type']} (no text)"


def summary_lines(s: dict[str, Any]) -> list[str]:
    """The three parts, in the order each conditions the next."""
    if not s["golden_targets"]:
        return []
    pct = lambda x: "n/a" if x is None else f"{100 * x:.0f}%"
    lines = [f"  search targets  {s['attempts']} attempts, "
             f"{s['golden_targets']} golden targets",
             f"    1. asked?     {s['covered']} of {s['golden_targets']} golden "
             f"targets asked for; {s['wrong_type']} asked as the wrong type, "
             f"{s['never_asked']} never asked; {s['extras']} extra agent "
             f"targets"
             + (f"; {s['values_missing']} left out an example value"
                if s["values_missing"] else ""),
             f"    2. returned?  over the {s['targets_scored']} asked targets "
             f"with a key: recall {pct(s['target_recall'])}, precision "
             f"{pct(s['target_precision'])}; per attempt: recall "
             f"{pct(s['attempt_recall'])}, precision "
             f"{pct(s['attempt_precision'])}",
             f"    3. correct?   {s['passed']} of {s['decided']} decided"]
    if s["decided"]:
        lines.append(f"                  every golden target asked: "
                     f"{s['passed_full_cover']} of {s['decided_full_cover']} "
                     f"correct; one or more missed: {s['passed_part_cover']} "
                     f"of {s['decided_part_cover']}")
    return lines


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", required=True)
    ap.add_argument("--cases", required=True)
    ap.add_argument("--json", action="store_true", help="emit rows as JSONL")
    a = ap.parse_args(argv)

    events = read_jsonl(a.events)
    cases = {c["qid"]: c for c in read_jsonl(a.cases)}
    rows = score(events, cases)
    if a.json:
        for r in rows:
            print(json.dumps(r))
        return 0
    if not rows:
        print("no attempt is on a case with `searchTargets`; nothing to score")
        return 0

    num = lambda x: "  -  " if x is None else f"{x:5.2f}"
    for r in rows:
        print(f"{r['qid']}  sample {r['sample']}  verdict {r['verdict']}  "
              f"asked {r['n_covered']}/{r['n_golden']}  recall {num(r['recall'])}"
              f"  precision {num(r['precision'])}  ({r['n_get_context']} calls)")
        for t in r["targets"]:
            head = f"    T{t['index']} {fmt_target(t)}"
            if t["covered"]:
                by = ", ".join(f"{fmt_target(c)} ({c['route']})"
                               for c in t["covered_by"])
                print(f"{head}  <- {by}")
                print(f"{'':8s}recall {num(t['recall'])}  precision "
                      f"{num(t['precision'])}  of {t['n_returned']} returned"
                      f" [{t['attribution']}]")
                for m in t["missing"]:
                    print(f"{'':8s}missing: {m}")
                if t["values_missing"]:
                    print(f"{'':8s}example values not sent: "
                          f"{', '.join(map(str, t['values_missing']))}")
            elif t["wrong_type"]:
                print(f"{head}  WRONG TYPE: asked as "
                      + ", ".join(fmt_target(w) for w in t["wrong_type"])
                      + f"  (arrived anyway: {num(t['recall_anywhere'])})")
            else:
                print(f"{head}  NEVER ASKED  (arrived anyway: "
                      f"{num(t['recall_anywhere'])})")
        for x in r["extras"]:
            print(f"    extra: {fmt_target(x)}")
    print()
    for line in summary_lines(summarise(rows)):
        print(line)
    return 0


if __name__ == "__main__":
    sys.exit(main())
