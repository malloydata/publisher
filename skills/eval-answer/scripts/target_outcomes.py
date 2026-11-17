#!/usr/bin/env python3
"""Score each search target of one replayed get_context call. Stdlib only.

`score_targets.py` scores an AGENT's searches against golden targets, so it
first has to decide which golden target each agent search covers. A replay
sends the golden targets themselves, so nothing is paired: target i of the
case is target i of the request. This module scores that simpler case, and
adds the three things a retrieval engine is tuned on that an answer run does
not need:

  recall       required answers that came back for their own target, over
               required answers. A target with two required groups counts
               twice. Exact ids only: the same name under another source, or a
               name in a returned source's docs, is not the answer.
  precision    of the entities a target returned, the share that are required,
               in an any-of group, acceptable or the labelled closest field.
               Pooled over targets as one ratio (sum over sum), so a target
               that returned 30 fields weighs 30 times one that returned 1.
               Source cards count only for a `source` target.
  best pick    the target's highest-relevance entity is a required answer.
               Entities tied at the top all count, so a tie is right when any
               of them is required.
  not found    a target labelled `absent` (the model holds nothing that
               answers it) returned nothing, or only fields its label lists as
               `closest` or `acceptable`. Anything else is a wrong pick.

WHAT A TARGET'S LABEL CARRIES (`expectedEntities` on the search target)

  required, requiredAnyOf, acceptable   as everywhere else
  absent    true when nothing in the model answers the target. It then names
            no required id. This is a label with a right answer ("not found"),
            which is different from a target nobody has labelled.
  closest   optional, with `absent`: the near misses a response may offer
            instead (gross revenue for a "net revenue" target). Returning one
            is correct; returning anything else is a wrong pick.

WHICH ENTITIES A TARGET RETURNED

From the server's attribution, as `score_targets.returned_by` reads it: a hit
belongs to a target when the hit is of a kind the target type can return and
its `matched_targets` names the target's search text. A source card is read
the same way when the host attributes cards, and belongs to every `source`
target when it does not. With no attribution at all (a lexical run) a target
gets every returned entity of its kind, which can only over-credit;
`attribution` on the row says which rule applied.

`recall_anywhere` is reported beside `recall`: the required answer came back
somewhere in the response, under any target. The gap between the two is
answers the caller received without the engine saying which target they
answer.
"""
from __future__ import annotations

from typing import Any

from score_retrieval import KINDS_BY_TARGET, groups, split_entity

# Publisher indexes no dimensional values, so its `dimensional_value` target
# selects nothing (`KINDS_BY_TARGET`). A host that does index them answers a
# value target with the dimension that holds the value, so that is the kind a
# value target's label names.
KINDS = {**KINDS_BY_TARGET, "dimensional_value": {"dimension"}}


def ranked(target: dict[str, Any], call: dict[str, Any]) -> tuple[list[tuple[str, float | None]], str]:
    """This target's returned entities, best first, and the attribution rule.

    Relevance is the server's score for THIS target when the hit names it in
    `matched_targets`, else the hit's own. An entity with no score sorts last.
    The sort is stable, so equal scores keep the response's order.
    """
    rs = call.get("rankedSummary") or {}
    hits = [h for h in rs.get("hits") or [] if isinstance(h, dict) and h.get("entity_id")]
    kinds = KINDS.get(str(target.get("target_type") or "").lower(), set())
    text = target.get("search_text")
    of_kind = [h for h in hits if split_entity(h["entity_id"])[0] in kinds]
    attributed = any(isinstance(h.get("matched_targets"), list) for h in hits)
    out: list[tuple[str, float | None]] = []
    if attributed and text:
        for h in of_kind:
            mt = h.get("matched_targets")
            if mt is None:
                # A hit that carries no attribution on an attributed response:
                # a source card on a host that does not attribute cards.
                out.append((h["entity_id"], h.get("relevance")))
                continue
            scores = [m.get("relevance") for m in mt
                      if isinstance(m, dict) and m.get("search_text") == text]
            if scores:
                known = [s for s in scores if isinstance(s, (int, float))]
                out.append((h["entity_id"], max(known) if known else h.get("relevance")))
        rule = "server"
    else:
        out = [(h["entity_id"], h.get("relevance")) for h in of_kind]
        rule = "kind"
    out.sort(key=lambda p: -p[1] if isinstance(p[1], (int, float)) else float("inf"))
    return out, rule


def outcome(index: int, target: dict[str, Any], call: dict[str, Any]) -> dict[str, Any]:
    """One target's row. `labelled` is false when the target carries neither a
    required answer nor `absent`; such a target is counted and never scored."""
    exp = target.get("expectedEntities") or {}
    req = groups(exp)
    absent = bool(exp.get("absent"))
    closest = set(exp.get("closest") or [])
    ok = {e for g in req for e in g} | set(exp.get("acceptable") or []) | closest
    got, rule = ranked(target, call)
    own = [e for e, _ in got]
    own_set = set(own)
    everything = set((call.get("rankedSummary") or {}).get("entityIds") or [])
    labelled = bool(req) or absent
    top = [e for e, r in got if got and r == got[0][1]] if got else []
    members = {e for g in req for e in g}
    return {
        "index": index,
        "target_type": str(target.get("target_type") or "").lower(),
        "search_text": target.get("search_text"),
        "tags": list(target.get("tags") or []),
        "labelled": labelled,
        "absent": absent,
        "attribution": rule,
        "n_required": len(req),
        "n_found": sum(1 for g in req if own_set & set(g)),
        "n_found_anywhere": sum(1 for g in req if everything & set(g)),
        "found": [" | ".join(g) for g in req if own_set & set(g)],
        "missing": [" | ".join(g) for g in req if not own_set & set(g)],
        # Every id the label counts as right, so two runs compare a target
        # only when it was scored against the same label.
        "label_ids": sorted(ok),
        "n_returned": len(own),
        "n_ok": len(own_set & ok) if labelled else None,
        "noise": sorted(own_set - ok) if labelled else [],
        "best_pick": top[0] if top else None,
        "best_pick_ok": (bool(set(top) & members) if top else False) if req else None,
        # `ok` is closest plus acceptable here: an absent target names no required id.
        "not_found_ok": (own_set <= ok) if absent else None,
        # The same two numbers `score_targets.py` reports per target, so a
        # replay row and an answer-run row read alike.
        "recall": (sum(1 for g in req if own_set & set(g)) / len(req)) if req else None,
        "precision": (len(own_set & ok) / len(own)) if (labelled and own) else None,
    }


def outcomes(case: dict[str, Any], call: dict[str, Any]) -> list[dict[str, Any]]:
    return [outcome(i, t, call) for i, t in enumerate(case.get("searchTargets") or [], 1)
            if isinstance(t, dict)]


def share(n: int, d: int) -> float | None:
    return n / d if d else None


def pool(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Target rows pooled as ratios of sums: every required answer weighs the
    same in recall, every returned entity the same in precision."""
    lab = [t for t in rows if t["labelled"]]
    req = sum(t["n_required"] for t in lab)
    back = sum(t["n_returned"] for t in lab)
    bp = [t for t in lab if t["best_pick_ok"] is not None]
    nf = [t for t in lab if t["not_found_ok"] is not None]
    return {
        "targets": len(rows),
        "labelled": len(lab),
        "required_answers": req,
        "recall": share(sum(t["n_found"] for t in lab), req),
        "recall_anywhere": share(sum(t["n_found_anywhere"] for t in lab), req),
        "returned": back,
        "precision": share(sum(t["n_ok"] for t in lab), back),
        "returned_per_target": share(back, len(lab)),
        "best_pick": share(sum(1 for t in bp if t["best_pick_ok"]), len(bp)),
        "best_pick_targets": len(bp),
        "not_found": share(sum(1 for t in nf if t["not_found_ok"]), len(nf)),
        "not_found_targets": len(nf),
    }
