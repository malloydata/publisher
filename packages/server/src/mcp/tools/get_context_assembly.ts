// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Assembly: turn the ranked rows into source cards. This is one plain
 * function, not a pluggable stage, because it needs every target's rows at
 * once. Card stages run on its output; shapeCards in get_context_tool.ts
 * pages and serializes it.
 */

import type {
   CardDraft,
   CardState,
   PipelineContext,
   RankedState,
} from "./get_context_pipeline";
import { sourceContextKey, type ResultEntity } from "./get_context_tool";

/**
 * A card's relevance after reading one more row: a source's own row sets it,
 * and any scored row raises it. One definition, used here and by
 * toSourceResults, so a card and its wire form cannot disagree.
 */
export function foldRelevance(
   current: number | undefined,
   row: ResultEntity,
): number | undefined {
   if (row.score === undefined) return current;
   // The source itself matched: its score belongs on the card outright.
   if (row.kind === "source") return row.score;
   // A source with no hit of its own still ranks by its best entity, so a
   // caller reading source relevance never sees a matched source at null.
   return current === undefined || row.score > current ? row.score : current;
}

/**
 * Group the ranked rows into one card per (model path, source), in the order
 * sources first appear, and cap the entities each card carries per search
 * target.
 *
 * Nothing is folded across sources: two sources exposing a same-named
 * measure are two different numbers, each nested under its own card. Cards
 * are not paged here; shapeCards keeps the first `limit` and sums the
 * entities dropped from those alone.
 */
export function assembleCards(
   state: RankedState,
   ctx: PipelineContext,
): CardState {
   const perSourcePerTarget = ctx.settings.entityWindow.perSourcePerTarget;
   const cards = new Map<string, CardDraft>();
   const perTarget = new Map<string, Map<number, number>>();
   for (const r of state.rows) {
      const source = r.source ?? "";
      const key = sourceContextKey(r.modelPath, source);
      let card = cards.get(key);
      if (!card) {
         card = {
            key,
            modelPath: r.modelPath,
            source,
            rows: [],
            entitiesDropped: 0,
         };
         cards.set(key, card);
         perTarget.set(key, new Map());
      }
      // A source row becomes the card itself, so it never spends a slot.
      if (r.kind !== "source") {
         // -1 buckets a row no target claims, so it shares one cap rather
         // than none. Both ranked paths set bestTarget, so on them this is
         // only a guard.
         const target = r.bestTarget ?? -1;
         const counts = perTarget.get(key) as Map<number, number>;
         const taken = counts.get(target) ?? 0;
         if (taken >= perSourcePerTarget) {
            card.entitiesDropped += 1;
            continue;
         }
         counts.set(target, taken + 1);
      }
      card.rows.push(r);
      card.relevance = foldRelevance(card.relevance, r);
   }
   const { rows: _rows, ...rest } = state;
   return { ...rest, cards: Array.from(cards.values()) };
}
