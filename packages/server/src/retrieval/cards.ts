// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { finalizeRelevance, round4 } from "./scoring";
import type { StageRow } from "./stage_types";

/**
 * A source card is one (model path, source) pairing, the unit `limit` counts
 * and the response nests entities under. These helpers score and order cards;
 * they never touch a row's own score.
 */

const SEP = "\u0000";

export const cardKeyOf = (r: StageRow): string =>
   [r.modelPath, r.source ?? r.name].join(SEP);

/** Rows grouped by card, in the order each card first appears. */
export function groupByCard<T extends StageRow>(rows: T[]): Map<string, T[]> {
   const cards = new Map<string, T[]>();
   for (const r of rows) {
      const key = cardKeyOf(r);
      const at = cards.get(key);
      if (at) at.push(r);
      else cards.set(key, [r]);
   }
   return cards;
}

/**
 * A card's raw score.
 *
 * `best-hit` is its strongest row, which is how the ranking has always
 * ordered cards. `coverage` keeps the integer level dominant and breaks ties
 * inside it by how many of the caller's targets the card answers, so a source
 * that answers three of four phrases outranks one that answers a single phrase
 * very well, unless the single phrase earned a higher LLM level. The fraction
 * of the best hit is the last tiebreak.
 */
export function cardScore<T extends StageRow>(
   rows: T[],
   mode: "best-hit" | "coverage",
   targetCount: number,
): number {
   let best = 0;
   const covered = new Set<number>();
   for (const r of rows) {
      const s = r.rankScore ?? 0;
      if (s > best) best = s;
      for (const t of r.candidateScores?.keys() ?? []) {
         // Only targets the row still holds after any LLM stage count.
         if (!r.targetScores || r.targetScores.has(t)) covered.add(t);
      }
   }
   if (mode === "best-hit" || targetCount <= 0) return best;
   const level = Math.floor(best);
   const coverage = Math.min(1, covered.size / targetCount);
   return level + 0.9 * coverage + 0.1 * (best - level);
}

/** Card keys ordered best first; ties keep the order they arrived in. */
export function orderCards(scores: Map<string, number>): string[] {
   return [...scores.entries()]
      .map(([key, score], i) => ({ key, score, i }))
      .sort((a, b) => b.score - a.score || a.i - b.i)
      .map((c) => c.key);
}

/** Rows regrouped so cards appear in `order`; rows keep their order within a card. */
export function reorderByCards<T extends StageRow>(
   rows: T[],
   order: string[],
): T[] {
   const cards = groupByCard(rows);
   return order.flatMap((key) => cards.get(key) ?? []);
}

/**
 * The relevance a card publishes. A score an LLM produced (level + similarity)
 * goes through the knots like an entity's; a plain similarity is already a
 * relevance and is published as it is.
 */
export function publishCardRelevance(
   raw: number,
   fromLlm: boolean,
   knots: ReadonlyArray<readonly [number, number]>,
): number {
   return round4(fromLlm ? finalizeRelevance(raw, knots) : raw);
}
