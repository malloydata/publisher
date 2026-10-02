// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Scores when an LLM stage rated the candidates.
 *
 * Before refine a row's score is its cosine similarity, 0 to 1. Refine rates
 * each candidate LOW, MEDIUM or HIGH and the row's raw score becomes
 * `level + cosine`, so the level decides the band and the cosine orders rows
 * inside it. Raw scores run 0 to 4 and are published through a piecewise
 * linear map, which keeps the published number between 0 and 1.
 */

/** LOW, MEDIUM and HIGH as the numbers a raw score starts from. */
export const LEVEL_VALUE = { LOW: 1, MEDIUM: 2, HIGH: 3 } as const;
export type Level = keyof typeof LEVEL_VALUE;
export const LEVELS: readonly Level[] = ["LOW", "MEDIUM", "HIGH"];

/** Raw score to published score. The knots, as [raw, published] pairs. */
export const SCORE_KNOTS: ReadonlyArray<readonly [number, number]> = [
   [0, 0],
   [1, 0.4],
   [2, 0.7],
   [3, 0.9],
   [4, 1.0],
];

const round2 = (x: number) => Math.round(x * 100) / 100;

/** The published score for a raw one: linear between knots, clamped at both ends, 2 places. */
export function mapRawScore(raw: number): number {
   const first = SCORE_KNOTS[0];
   const last = SCORE_KNOTS[SCORE_KNOTS.length - 1];
   if (raw <= first[0]) return first[1];
   if (raw >= last[0]) return last[1];
   for (let i = 1; i < SCORE_KNOTS.length; i++) {
      const [x1, y1] = SCORE_KNOTS[i];
      if (raw <= x1) {
         const [x0, y0] = SCORE_KNOTS[i - 1];
         return round2(y0 + ((raw - x0) / (x1 - x0)) * (y1 - y0));
      }
   }
   return last[1];
}
