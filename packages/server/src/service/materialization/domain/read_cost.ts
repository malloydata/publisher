// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The order the serving legs try stored tables in: cheapest read first, from
 * facts the plan already holds. The legs themselves follow it (a persisted
 * result by exact call, then rollups, then the served copy over base tables),
 * and the plan orders each base's rollups by {@link byReadCost}, once:
 * fewer grain dimensions first (a coarser rollup holds fewer rows), then a
 * database table before a `storage=` one at equal grain, then by name, so
 * the order is deterministic. Database first is a heuristic, not a
 * guarantee: it reads where the query already runs with no destination to
 * attach, but on a warehouse that bills per scan (BigQuery) a database read
 * is billed where a `storage=` one is not. Row counts are deliberately not
 * read: the order would then move with each build.
 *
 * Only the order changes what is tried: every candidate answers exactly as
 * live, so the answer never depends on it.
 *
 * Pure.
 */

/** What a rollup candidate's read cost is judged by. */
interface RollupCost {
   grainDimensions: readonly string[];
   /** The rollup's `storage=` destination; undefined when database. */
   storage?: string;
   rollupSourceName: string;
}

/** Cheapest rollup read first. */
export function byReadCost(a: RollupCost, b: RollupCost): number {
   return (
      a.grainDimensions.length - b.grainDimensions.length ||
      Number(a.storage !== undefined) - Number(b.storage !== undefined) ||
      (a.rollupSourceName < b.rollupSourceName
         ? -1
         : a.rollupSourceName > b.rollupSourceName
           ? 1
           : 0)
   );
}
