// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The strings a `storage=` build over its upstreams' stored tables compares,
 * recorded at plan (a pure walk of the compiled query, independent of any
 * query or run), and the one rule that judges them against those tables'
 * column contracts. The build runs in its destination, which compares
 * strings byte-exact; each string it orders must compare there as live
 * orders it (`compare: exact`), and each it groups or compares for equality
 * as live does (`exact` or `equality`). Otherwise the stored table would
 * hold other groups or rows than live computes.
 *
 * Each column is judged on its own: a value combining columns that each
 * meet the need compares as live does, since `equality` is byte equality
 * and `exact` is binary.
 *
 * Pure.
 */

import { type ColumnContract, contractColumn } from "./column_contract";

/** What a read of a string needs its stored column to compare as. */
type StringReadNeed = "exact" | "equality";

/** One string a build compares, by the names its stored column may go by. */
export type StringRead = {
   names: readonly string[];
   need: StringReadNeed;
};

/**
 * Every string a build compares; or why it is refused, decided at plan: its
 * strings could not be read (fail closed), or a table it reads, as bound
 * when the plan was derived, records a column compared otherwise than the
 * build needs.
 */
export type StringReads =
   | { reads: readonly StringRead[] }
   | { refused: string };

const MEETS: Record<StringReadNeed, ReadonlySet<string>> = {
   exact: new Set(["exact"]),
   equality: new Set(["exact", "equality"]),
};

/**
 * Why a build reading `reads` over tables recorded by `contracts` would not
 * compare a string as live does, or undefined when every string it compares
 * does. An upstream with no contract, a column no contract lists, and reads
 * that were never recorded all refuse.
 */
export function stringReadsRefusal(
   reads: StringReads | undefined,
   contracts: readonly (ColumnContract | undefined)[],
): string | undefined {
   if (reads === undefined) return "its string reads were not recorded";
   if ("refused" in reads) return reads.refused;
   if (reads.reads.length === 0) return undefined;
   if (contracts.length === 0 || contracts.some((c) => c === undefined)) {
      return "an upstream it reads has no column contract";
   }
   for (const read of reads.reads) {
      const compare = recordedCompare(
         read.names,
         contracts as ColumnContract[],
      );
      if (!MEETS[read.need].has(compare)) {
         const name = read.names[read.names.length - 1] ?? "?";
         return (
            `it ${read.need === "exact" ? "orders" : "groups or compares"} ` +
            `'${name}', which its upstream's table records as compared ` +
            `${compare === "none" ? "unlike live" : `by ${compare} only`}`
         );
      }
   }
   return undefined;
}

/**
 * The compare recorded for the stored column a read names: the first name
 * some contract lists (a read names its column by every name it goes by on
 * the way to the table), else `none`.
 */
function recordedCompare(
   names: readonly string[],
   contracts: readonly ColumnContract[],
): string {
   for (const name of [...names].reverse()) {
      for (const contract of contracts) {
         const column = contractColumn(contract, name);
         if ("nativeType" in column) return column.compare;
      }
   }
   return "none";
}
