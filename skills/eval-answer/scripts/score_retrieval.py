#!/usr/bin/env python3
"""Deterministic get_context retrieval score. Stdlib only.

This scores WHETHER needed entities were returned, and how high they ranked.
Nothing here is a judgment call. Do not re-implement it in prose.

  python score_retrieval.py --needed NEEDED.json --ranked RANKED.json [--max-rank K] [--json]

Exit 0 when the comparison ran (including recall 0). Exit 1 if the inputs
cannot be read or the needed set is empty.

NEEDED.json is a JSON list of entity ids, or {"entities": [...]}.

RANKED.json is one rankedSummary or a list of them, each:

  {"entityIds": ["join:space_detail:space_floor", ...], "ranks": [1, 2, ...]}

`ranks[i]` is the 1-based rank of `entityIds[i]`. If `ranks` is omitted, ids
are treated as already in rank order starting at 1.

Matching (needed vs a returned id):

1. Exact string match.
2. Bare name: `space_floor` matches `join:space_detail:space_floor`.
3. Kind+name: `join:space_floor` matches `join:space_detail:space_floor`.

A needed name never matches a different last segment (`floor` is not
`floor_key`). Best rank is the minimum across all lists. A rank worse than
`--max-rank` (when set) counts as a miss.

Scores, over the needed set:

- `context_recall` = n_found / n_needed  (binary presence; ignores rank)
- `mrr` = mean(1/best_rank if found else 0)

`mrr` is mean reciprocal *best rank of each needed entity*, not classic
single-relevant-document MRR (1/rank of the first relevant hit). Classic
first-hit RR is `rr_first` when you need it.
"""
from __future__ import annotations

import argparse
import json
import sys
from typing import Any


def _as_list(raw: Any) -> list[Any]:
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict) and isinstance(raw.get("entities"), list):
        return raw["entities"]
    raise ValueError("needed must be a JSON list or {\"entities\": [...]}")


def _needed_ids(raw: Any) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for item in _as_list(raw):
        if isinstance(item, dict):
            item = item.get("id") or item.get("entityId") or ""
        s = str(item).strip()
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def _summaries(raw: Any) -> list[dict[str, Any]]:
    if raw is None:
        return []
    if isinstance(raw, dict) and "entityIds" in raw:
        return [raw]
    if isinstance(raw, list):
        return [row for row in raw if isinstance(row, dict)]
    raise ValueError("ranked must be a rankedSummary object or a list of them")


def matches(needed: str, ranked_id: str) -> bool:
    n = needed.strip()
    r = ranked_id.strip()
    if not n or not r:
        return False
    if n == r:
        return True
    n_parts = n.split(":")
    r_parts = r.split(":")
    if len(n_parts) == 1:
        return r_parts[-1] == n_parts[0]
    if len(n_parts) == 2 and len(r_parts) >= 2:
        return n_parts[0] == r_parts[0] and n_parts[-1] == r_parts[-1]
    return False


def best_rank(needed: str, summaries: list[dict[str, Any]], max_rank: int | None) -> int | None:
    best: int | None = None
    for row in summaries:
        ids = row.get("entityIds") or []
        ranks = row.get("ranks")
        if not isinstance(ids, list):
            continue
        if not isinstance(ranks, list) or len(ranks) != len(ids):
            ranks = list(range(1, len(ids) + 1))
        for entity_id, rank in zip(ids, ranks):
            try:
                rank_n = int(rank)
            except (TypeError, ValueError):
                continue
            if rank_n < 1:
                continue
            if max_rank is not None and rank_n > max_rank:
                continue
            if matches(needed, str(entity_id)):
                if best is None or rank_n < best:
                    best = rank_n
    return best


def score(needed: list[str], ranked: Any, max_rank: int | None = None) -> dict[str, Any]:
    if not needed:
        raise ValueError("needed set is empty")
    summaries = _summaries(ranked)
    ranks: dict[str, int | None] = {}
    rrs: dict[str, float] = {}
    first: int | None = None
    found = 0
    for entity in needed:
        rank = best_rank(entity, summaries, max_rank)
        ranks[entity] = rank
        if rank is None:
            rrs[entity] = 0.0
            continue
        found += 1
        rrs[entity] = 1.0 / rank
        if first is None or rank < first:
            first = rank
    n = len(needed)
    return {
        "n_needed": n,
        "n_found": found,
        "context_recall": found / n,
        "mrr": sum(rrs.values()) / n,
        "rr_first": 0.0 if first is None else 1.0 / first,
        "ranks": ranks,
        "reciprocal_ranks": rrs,
    }


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--needed", required=True, help="JSON file: needed entity ids")
    p.add_argument("--ranked", required=True, help="JSON file: rankedSummary or list")
    p.add_argument("--max-rank", type=int, default=None, help="Treat worse ranks as misses")
    p.add_argument("--json", action="store_true", help="Print JSON (default)")
    args = p.parse_args(argv)
    try:
        with open(args.needed, encoding="utf-8") as f:
            needed_raw = json.load(f)
        with open(args.ranked, encoding="utf-8") as f:
            ranked_raw = json.load(f)
        needed = _needed_ids(needed_raw)
        out = score(needed, ranked_raw, args.max_rank)
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(json.dumps(out, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
