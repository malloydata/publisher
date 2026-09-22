#!/usr/bin/env python3
"""Pull search terms and entity ids out of a get_context exchange. Stdlib only.

`get_context` has more than one request shape and more than one response
shape, and an extractor written against the one you happen to see first will
silently return nothing for the others -- scoring every attempt at zero recall
while the agent was in fact handed exactly what it needed.

Requests come as either:

    {"query": "total sales 2022"}
    {"search_targets": [{"target_type": "measure", "search_text": "revenue"}],
     "scopes": [{"environment": ..., "package": ..., "source": ...}]}

Responses put entities in at least three places: a flat `results` list, a
per-target `targets[].results` list, and nested under `sources[].entities`
keyed by kind. Some shapes supply `entityId` outright; others give
kind/name/source to assemble it from.

So rather than encode a shape, `entity_ids` walks the payload and takes every
entity it finds anywhere. New nesting is picked up without a change here, which
is the point: the failure mode being avoided is a parser that reports zero
instead of raising.

THE SOURCE-CENTRIC SHAPE IS NOT A GUESS

A hosted `GetContextResponse` is source-centric where Publisher was
entity-centric, and shares no field name with it:

    {"sources": [{"source_info": {"resource_id": {"environment", "package",
                                                  "model_path", "source"}},
                  "entities": [{"name", "entity_type", "relevance"}]}]}

It has NO `entityId` and NO `kind`. An earlier version of this file assumed
both, which is worse than assuming nothing: each `SourceEntity` has a `name`
and no `kind`, so the generic sources-block rule claimed it as a source and
`measure:order_items:total_sales` came back as `source:total_sales:total_sales`
-- every entity misfiled, under an identity nothing can match. Recall and
precision both read zero on a payload that had delivered the right answer.

So this shape is matched explicitly, by `source_info`, rather than left to the
generic walk. The generic walk stays for everything else.

One divergence no parser can reconcile: on some hosts an entity `name` is a
full Malloy field path, so a joined field arrives as
`hiring_manager.employee_count` where Publisher reports `employee_count` on the
joined source. The ids differ for the same logical entity, and a set's
`expectedEntities` is therefore not portable across those targets until both
emit a canonical id. Noted here rather than papered over with a guess about
which side to rewrite.
"""
from __future__ import annotations

import re
from typing import Any

ENTITY_KINDS = {"source", "dimension", "measure", "view", "join", "query"}


def search_terms(tool_input: dict[str, Any]) -> list[str]:
    """What the answerer asked for, across both request conventions."""
    terms: list[str] = []
    q = tool_input.get("query")
    if isinstance(q, str) and q.strip():
        terms.append(q.strip())
    for t in tool_input.get("search_targets") or []:
        if not isinstance(t, dict):
            continue
        text = t.get("search_text") or t.get("text")
        if isinstance(text, str) and text.strip():
            kind = t.get("target_type")
            terms.append(f"{kind}: {text.strip()}" if kind else text.strip())
    return terms


def entity_hits(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Every returned entity with the provenance the response carries.

    `entity_ids` answers "what came back". This answers "how", and the two
    facts it adds are the ones the scorer needs and never had:

    - `relevance`, the server's own score. On the semantic path it is a cosine
      and IS comparable across targets, which is exactly why the server
      publishes it there and withholds it on the lexical path, where a lunr
      score is relative to its own query.
    - `matched_targets`, naming WHICH of the caller's search targets matched
      this entity and how well. Without it a required entity that never came
      back is indistinguishable from one the agent never asked for -- the
      distinction between a retrieval defect and a phrase-detection defect,
      and the whole reason retrieval scoring could not attribute a miss.

    `position` is the entity's index in the flattened response and is NOT a
    rank. The response is sorted globally by relevance and then bucketed into
    source cards, so flattening card by card interleaves. The harness used to
    write `range(1, n+1)` here and call it a rank; read `relevance` instead.

    Entities are the only thing that carries this. A source card's own target
    attribution is discarded by the server before an entity object exists, so
    a `source:` hit has none and says so with `matched_targets: None`.
    """
    hits: list[dict[str, Any]] = []
    seen: set[str] = set()

    def add(ident: str, source: str, ent: dict[str, Any] | None,
            in_card: int | None) -> None:
        if ident in seen:
            return
        seen.add(ident)
        mt = (ent or {}).get("matched_targets")
        hits.append({
            "entity_id": ident,
            "source": source,
            "relevance": (ent or {}).get("relevance"),
            # None and [] are different claims: None is "this path publishes
            # no attribution" (a source hit, or a lexical run), [] is "the
            # server attributed it to no target".
            "matched_targets": ([{"search_text": m.get("search_text"),
                                  "relevance": m.get("relevance")}
                                 for m in mt if isinstance(m, dict)]
                                if isinstance(mt, list) else None),
            "position": len(hits) + 1,
            "position_in_source": in_card,
        })

    for node in payload.get("sources") or []:
        if not isinstance(node, dict):
            continue
        info = node.get("source_info")
        rid = info.get("resource_id") if isinstance(info, dict) else None
        src = rid.get("source") if isinstance(rid, dict) else None
        if not isinstance(src, str) or not src:
            continue
        add(f"source:{src}:{src}", src, None, 0)
        for i, ent in enumerate(node.get("entities") or [], 1):
            if not isinstance(ent, dict):
                continue
            # The same id rule `entity_ids` uses, so the two cannot disagree
            # about what an entity is called.
            eid = ent.get("entity_id")
            if not (isinstance(eid, str) and eid):
                name, kind = ent.get("name"), ent.get("entity_type")
                if not (isinstance(name, str) and name and isinstance(kind, str)):
                    continue
                eid = f"{kind.lower()}:{src}:{name}"
            add(eid, src, ent, i)
    return hits


def target_shapes(tool_input: dict[str, Any]) -> list[dict[str, Any]]:
    """Every search target's TYPE, and whether it carried search text.

    Separate from `search_terms` because that function answers a different
    question -- what did the answerer search FOR -- and to answer it, it drops
    a target with no `search_text`: there is no term to record. A target with
    no text is not a search, it is an enumeration of that type over the scope,
    and dropping it means the ledger cannot express the single statistic the
    "the agent enumerates instead of searching" argument is made of. Measured
    on a real 10-case run, every attempt read `targetsWithoutSearchText: 0`
    because a bare target had already been discarded before the ledger saw it.

    So this keeps one row per target, text or not, and nothing else. Reading
    the bare-target rate off transcripts instead works and is what has been
    done; it means the number cannot be recomputed from a run directory after
    the transcripts are pruned.
    """
    out: list[dict[str, Any]] = []
    for t in tool_input.get("search_targets") or []:
        if not isinstance(t, dict):
            continue
        text = t.get("search_text") or t.get("text")
        out.append({"type": t.get("target_type") or "?",
                    "has_text": bool(isinstance(text, str) and text.strip())})
    return out


def entity_id(kind: str, source: str | None, name: str) -> str:
    """The `kind:source:name` id, minted in one place.

    This file was already the only place ids are BUILT; naming the format as a
    function keeps it that way now that the definition ledger needs to build
    them too. `score_retrieval.split_entity()` is the matching reader.
    """
    return f"{kind}:{source}:{name}" if source else f"{kind}:{name}"


def _ident(node: dict[str, Any]) -> str | None:
    eid = node.get("entityId")
    if isinstance(eid, str) and eid:
        return eid
    kind, name = node.get("kind"), node.get("name")
    if kind in ENTITY_KINDS and isinstance(name, str) and name:
        return entity_id(kind, node.get("source"), name)
    return None


def entity_ids(payload: Any) -> list[str]:
    """Every entity anywhere in the payload, best rank first, deduplicated.

    Ranks are per-target and so repeat across targets; ordering by rank then by
    discovery keeps a rank-1 hit ahead of a rank-9 one from another target,
    which is what a precision-at-k reading of this list assumes.
    """
    found: dict[str, tuple[int, int]] = {}
    seq = 0

    def take(ident: str, rank: Any) -> None:
        nonlocal seq
        seq += 1
        rank = rank if isinstance(rank, int) else 999
        prev = found.get(ident)
        if prev is None or rank < prev[0]:
            found[ident] = (rank, seq)

    def source_result(node: dict[str, Any]) -> bool:
        """A `SourceResult`: the source, then its nested entities.

        Both targets take this path now -- Publisher's get_context converged on
        this shape (malloydata/publisher#1028), which is the whole reason a
        set's `expectedEntities` is portable between them.

        Handled here because the generic rules below read a `SourceEntity` as a
        source -- it carries a bare `name` and no `kind`. Returns True when it
        consumed the node, so the caller does not walk it twice.
        """
        info = node.get("source_info")
        if not isinstance(info, dict):
            return False
        rid = info.get("resource_id")
        src = rid.get("source") if isinstance(rid, dict) else None
        if not isinstance(src, str) or not src:
            return False
        take(f"source:{src}:{src}", 1)
        for ent in node.get("entities") or []:
            if not isinstance(ent, dict):
                continue
            # Publisher states the id; a host that does not gets one
            # assembled. Preferring the stated one means the server, not this
            # file, is the authority on how an entity is named.
            eid = ent.get("entity_id")
            if isinstance(eid, str) and eid:
                take(eid, 1)
                continue
            name, kind = ent.get("name"), ent.get("entity_type")
            if isinstance(name, str) and name and isinstance(kind, str):
                take(f"{kind.lower()}:{src}:{name}", 1)
        return True

    def walk(node: Any, in_sources: bool) -> None:
        nonlocal seq
        if isinstance(node, list):
            for item in node:
                walk(item, in_sources)
            return
        if not isinstance(node, dict):
            return

        if source_result(node):
            return

        # A `sources` entry describes a source and also carries its entities.
        # It has a name but usually no `kind`, so name it explicitly.
        if in_sources and isinstance(node.get("name"), str) and "kind" not in node:
            ident = f"source:{node['name']}:{node['name']}"
            seq += 1
            found.setdefault(ident, (node.get("rank", 1), seq))

        ident = _ident(node)
        if ident:
            seq += 1
            rank = node.get("rank")
            rank = rank if isinstance(rank, int) else 999
            prev = found.get(ident)
            if prev is None or rank < prev[0]:
                found[ident] = (rank, seq)

        for key, value in node.items():
            if key in ("resource_id", "joins"):
                # resource_id repeats the parent's identity; joins name other
                # sources without returning them, so neither is a hit.
                continue
            walk(value, in_sources or key == "sources")

    walk(payload, False)
    return [k for k, _ in sorted(found.items(), key=lambda kv: kv[1])]


_IDENT = re.compile(r"(?<![A-Za-z0-9_])[a-z][a-z0-9]*(?:_[a-z0-9]+)+(?![A-Za-z0-9_])")


def doc_tokens(payload: Any) -> list[str]:
    """Identifier-shaped words in the documentation of every source returned.

    A get_context response carries each source's own documentation, which
    names the source's fields and how to use them -- and the answerer reads it.
    An entity named there has reached the answerer's context even when it was
    not returned as a ranked entity. The ledger keeps only the ranked ids, so
    the identifiers in that text are recorded beside them (snake_case words
    only: that is what a Malloy field name looks like, and it keeps prose out).
    """
    out: dict[str, None] = {}

    def walk(node: Any) -> None:
        if isinstance(node, list):
            for x in node:
                walk(x)
        elif isinstance(node, dict):
            info = node.get("source_info")
            texts = []
            if isinstance(info, dict):
                texts += [v for v in info.values() if isinstance(v, str)]
            for k in ("docs", "doc", "summary", "one_line_summary", "description"):
                if isinstance(node.get(k), str):
                    texts.append(node[k])
            for t in texts:
                for m in _IDENT.findall(t):
                    out.setdefault(m, None)
            for v in node.values():
                walk(v)

    walk(payload)
    return list(out)
