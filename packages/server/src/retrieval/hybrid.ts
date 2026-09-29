// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Reciprocal rank fusion of the embedding ranking and the lunr ranking.
//
// Cosine finds meaning ("revenue" for "sales"); lunr finds exact tokens
// (an SKU, an acronym) that an embedding blurs. Their scores are on different
// scales, so they are fused on RANK, not score: each list gives a row
// 1 / (k + rank), and the two add.

/** row key -> (search target index -> that target's score for the row) */
export type TargetScores = ReadonlyMap<string, ReadonlyMap<number, number>>;

export type HybridMode = "rerank-only" | "union";

/**
 * A fused score in [0, 1] for each row, keyed like the inputs.
 *
 * Ranks are taken per target, over the rows that target scored, so a broad
 * target cannot push a narrow one down. A row's score is its best target's
 * (the same max-over-targets rule the rest of the pipeline uses). Dividing by
 * 2 / (k + 1), the most one target can give (rank 1 in both lists), keeps the
 * result in [0, 1] so the gap cut reads it like any other score.
 *
 * `rerank-only` returns only rows the embedding search already found, so the
 * floor and `below_cutoff_count` keep meaning what they did. `union` also
 * returns rows only lunr found.
 */
export function rrfFuse(args: {
   semantic: TargetScores;
   lexical: TargetScores;
   k: number;
   mode: HybridMode;
}): Map<string, number> {
   const { semantic, lexical, k, mode } = args;
   const perTarget = new Map<number, Map<string, number>>();

   const addList = (scores: TargetScores) => {
      const byTarget = new Map<number, Array<[string, number]>>();
      for (const [key, targets] of scores) {
         for (const [target, score] of targets) {
            const list = byTarget.get(target) ?? [];
            list.push([key, score]);
            byTarget.set(target, list);
         }
      }
      for (const [target, list] of byTarget) {
         // Ties keep input order, so the result is the same on every run.
         list.sort((a, b) => b[1] - a[1]);
         const sums = perTarget.get(target) ?? new Map<string, number>();
         list.forEach(([key], i) => {
            sums.set(key, (sums.get(key) ?? 0) + 1 / (k + i + 1));
         });
         perTarget.set(target, sums);
      }
   };
   addList(semantic);
   addList(lexical);

   const best = new Map<string, number>();
   for (const sums of perTarget.values()) {
      for (const [key, sum] of sums) {
         if (mode === "rerank-only" && !semantic.has(key)) continue;
         if (sum > (best.get(key) ?? 0)) best.set(key, sum);
      }
   }
   const ceiling = 2 / (k + 1);
   for (const [key, sum] of best) best.set(key, Math.min(1, sum / ceiling));
   return best;
}
