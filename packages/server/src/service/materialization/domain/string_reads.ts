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
 * Each column is judged on its own, against the contract of the one
 * upstream whose table holds it (recorded with the read, by sourceID), never
 * another stored table that lists a column of the same name: a value
 * combining columns that each meet the need compares as live does, since
 * `equality` is byte equality and `exact` is binary.
 *
 * Pure.
 */

import { type ColumnContract, contractColumn } from "./column_contract";

/** What a read of a string needs its stored column to compare as. */
type StringReadNeed = "exact" | "equality";

/**
 * One string a build compares: the stored upstream whose table holds its
 * column, by sourceID (undefined when the read reaches none, which
 * refuses), and the names that column may go by there.
 */
export type StringRead = {
   upstream: string | undefined;
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
   /**
    * `unstored`: refused because a string it compares is read from no
    * stored upstream's table ({@link unstoredReadRefusal}), not for a
    * collation.
    */
   | { refused: string; unstored?: true };

const MEETS: Record<StringReadNeed, ReadonlySet<string>> = {
   exact: new Set(["exact"]),
   equality: new Set(["exact", "equality"]),
};

/**
 * Why a build reading `reads` cannot be built over its stored upstreams at
 * all, whatever any contract records: a string it compares is read from a
 * table no stored upstream holds (the source warehouse, through a join or
 * a source nothing stores). That is not a collation the destination lacks,
 * so callers report it as itself, never framed as a string comparison.
 * Undefined when every read reaches a stored upstream (or none is recorded).
 */
export function unstoredReadRefusal(
   reads: StringReads | undefined,
): string | undefined {
   if (reads === undefined) return undefined;
   if ("refused" in reads) return reads.unstored ? reads.refused : undefined;
   const unstored = reads.reads.find((r) => r.upstream === undefined);
   if (unstored === undefined) return undefined;
   const name = unstored.names[unstored.names.length - 1] ?? "?";
   return (
      `it reads '${name}' from a table no stored upstream holds (the source ` +
      `warehouse, through a join or a source nothing stores), which no ` +
      `stored table can stand in for`
   );
}

/**
 * Why a build reading `reads` over tables recorded by `contracts` (by the
 * sourceIDs whose tables they record) would not compare a string as live
 * does, or undefined when every string it compares does. An upstream with
 * no contract, a read that reaches no upstream or one `contracts` does not
 * name, a column its upstream's contract does not list, and reads that were
 * never recorded all refuse.
 */
export function stringReadsRefusal(
   reads: StringReads | undefined,
   contracts: ReadonlyMap<string, ColumnContract | undefined>,
): string | undefined {
   if (reads === undefined) return "its string reads were not recorded";
   if ("refused" in reads) return reads.refused;
   if (reads.reads.length === 0) return undefined;
   const unstored = unstoredReadRefusal(reads);
   if (unstored !== undefined) return unstored;
   if (contracts.size === 0 || [...contracts.values()].some((c) => !c)) {
      return "an upstream it reads has no column contract";
   }
   for (const read of reads.reads) {
      const name = read.names[read.names.length - 1] ?? "?";
      const contract =
         read.upstream === undefined ? undefined : contracts.get(read.upstream);
      if (contract === undefined) {
         return (
            `it compares '${name}', which could not be followed to the ` +
            `stored table of an upstream it reads`
         );
      }
      const compare = recordedCompare(read.names, contract);
      if (!MEETS[read.need].has(compare)) {
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
 * The compare its upstream's contract records for the stored column a read
 * names: the first name the contract lists (a read names its column by
 * every name it goes by on the way to the table), else `none`.
 */
function recordedCompare(
   names: readonly string[],
   contract: ColumnContract,
): string {
   for (const name of [...names].reverse()) {
      const column = contractColumn(contract, name);
      if ("nativeType" in column) return column.compare;
   }
   return "none";
}
