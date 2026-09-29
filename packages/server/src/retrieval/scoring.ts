// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { RelevanceLevel } from "./retrieval_config";

/**
 * The raw score an LLM stage produces is `level + cosine`: level 1-3 for
 * LOW/MEDIUM/HIGH, plus a similarity in [0, 1]. The level dominates, and
 * similarity orders candidates the LLM rated alike. That is the service's
 * scale, kept so a score computed here means what one computed there does.
 */
export const LEVEL_VALUE: Record<RelevanceLevel, number> = {
   LOW: 1,
   MEDIUM: 2,
   HIGH: 3,
};

export function levelValue(level: RelevanceLevel): number {
   return LEVEL_VALUE[level];
}

/** True when `a` is strictly below `b`. */
export function levelBelow(a: RelevanceLevel, b: RelevanceLevel): boolean {
   return LEVEL_VALUE[a] < LEVEL_VALUE[b];
}

/**
 * Map a raw score onto the wire's [0, 1] through a piecewise-linear curve.
 * Values outside the knots clamp to the end points, and a NaN maps to the
 * lowest, so a bad score can never publish as a high relevance.
 */
export function finalizeRelevance(
   raw: number,
   knots: ReadonlyArray<readonly [number, number]>,
): number {
   if (!Number.isFinite(raw) || knots.length === 0) return knots[0]?.[1] ?? 0;
   if (raw <= knots[0][0]) return knots[0][1];
   const last = knots[knots.length - 1];
   if (raw >= last[0]) return last[1];
   for (let i = 1; i < knots.length; i++) {
      const [x1, y1] = knots[i];
      if (raw <= x1) {
         const [x0, y0] = knots[i - 1];
         return y0 + ((raw - x0) / (x1 - x0)) * (y1 - y0);
      }
   }
   return last[1];
}

/** Round to the four places the semantic path already publishes. */
export const round4 = (n: number): number => Math.round(n * 10_000) / 10_000;
