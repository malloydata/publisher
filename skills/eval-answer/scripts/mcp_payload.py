#!/usr/bin/env python3
"""Pull search terms and entity ids out of a get_context exchange. Stdlib only.

`malloy_getContext` has more than one request shape and more than one response
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

CREDIBLE'S SHAPE IS NOT A GUESS, AND IT IS NOT PUBLISHER'S

`apis/retrieval-public-api.yaml` (`GetContextResponse`) is source-centric where
Publisher is entity-centric, and shares no field name with it:

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

One divergence no parser can reconcile: a Credible entity `name` is a full
Malloy field path, so a joined field arrives as `hiring_manager.employee_count`
where Publisher reports `employee_count` on the joined source. The ids differ
for the same logical entity, and a set's `expectedEntities` is therefore not
portable across targets until one side emits a canonical id. Noted in the eval
docs rather than papered over with a guess about which side to rewrite.
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


def _ident(node: dict[str, Any]) -> str | None:
    eid = node.get("entityId")
    if isinstance(eid, str) and eid:
        return eid
    kind, name = node.get("kind"), node.get("name")
    if kind in ENTITY_KINDS and isinstance(name, str) and name:
        src = node.get("source")
        return f"{kind}:{src}:{name}" if src else f"{kind}:{name}"
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

    def credible_source(node: dict[str, Any]) -> bool:
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
            # Publisher states the id; Credible does not, so assemble it there.
            # Preferring the stated one means the server, not this file, is the
            # authority on how an entity is named.
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

        if credible_source(node):
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
