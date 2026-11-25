// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Who may drop a table a run built, over time: the rules the reclaim
 * executes, as total functions of facts it gathers. A table is dropped only
 * when every rule says nothing still reads it; any doubt keeps it, so each
 * rule's safety direction is "keep". Two rules:
 *
 * - {@link renameVerdict}: a table a readable name replaced (the run built
 *   its source under a new name or in a new place). Kept until the
 *   replacement is committed and served, and while anything may still read
 *   the old name; an author's own `name=` is never dropped.
 * - {@link dropVerdict}: a table a run being given back named. Kept while a
 *   committed run names it, there or (by its table segment) anywhere.
 *
 * Pure: the reclaimer gathers the facts (the run store, the serving line)
 * and acts on the verdict (the drop, the record of what it kept).
 */

import type { KeptTableReason } from "./types";

/**
 * Why a table a readable name replaced is kept, in the order the rules ask.
 * `author_name` and `may_be_served` are listed as kept tables
 * ({@link KeptTableReason}); the rest are logged only.
 */
export type RenameGuard =
   | "replacement-not-committed"
   | "not-serving-replacement"
   | "not-in-package-records"
   | "still-served"
   | "may_be_served"
   | "still-referenced"
   | "author_name";

/** What the reclaim knows about one replaced table, all of it in memory. */
export interface RenameFacts {
   /** This run committed a table for the source. */
   replacementCommitted: boolean;
   /** The serving line's newest run serves that table, in that place. */
   replacementServed: boolean;
   /** An earlier auto-run of the package built the old table there. */
   builtByPackage: boolean;
   /** A served table at the old place may be the old table. */
   servedThere: boolean;
   /** A served table anywhere shares the old name's table segment. */
   servedByLeaf: boolean;
   /** A committed run off the serving line names the old table segment. */
   referencedElsewhere: boolean;
   /** An active run has claimed a name with the old table segment, to write. */
   claimedByActiveRun: boolean;
   /** The old name carries Publisher's reserved suffix (not an author's). */
   publisherNamed: boolean;
}

/**
 * The guard that keeps a replaced table, or undefined when it may be
 * dropped. A missing destination and the owner's mark are checked later,
 * by the drop.
 */
export function renameVerdict(facts: RenameFacts): RenameGuard | undefined {
   if (!facts.replacementCommitted) return "replacement-not-committed";
   if (!facts.replacementServed) return "not-serving-replacement";
   if (!facts.builtByPackage) return "not-in-package-records";
   if (facts.servedThere) return "still-served";
   if (facts.servedByLeaf) return "may_be_served";
   if (facts.referencedElsewhere || facts.claimedByActiveRun) {
      return "still-referenced";
   }
   if (!facts.publisherNamed) return "author_name";
   return undefined;
}

/**
 * The guards a later run may find passing (its replacement commits, it
 * becomes what is served, the other run naming the table goes): a table kept
 * by one stays recorded, to be decided again.
 */
export function isTransientGuard(guard: RenameGuard): boolean {
   return (
      guard === "replacement-not-committed" ||
      guard === "not-serving-replacement" ||
      guard === "still-referenced"
   );
}

/** The kept-table reason a guard lists the table under, if it lists it. */
export function keptReasonOf(
   guard: RenameGuard,
): Extract<KeptTableReason, "author_name" | "may_be_served"> | undefined {
   return guard === "author_name" || guard === "may_be_served"
      ? guard
      : undefined;
}

/** Why a table a run is giving back stays in place. */
type DropKeep = "still_referenced" | "may_be_served";

/**
 * Whether a table a run is giving back may be dropped, from what committed
 * runs reference: kept when one names it in its place, or names its table
 * segment anywhere (a name does not say where it is: two names for one
 * DuckLake, a DuckDB catalog name). Another package's ownership mark is
 * the drop's own probe.
 */
export function dropVerdict(facts: {
   referencedThere: boolean;
   referencedByLeaf: boolean;
}): DropKeep | "drop" {
   if (facts.referencedThere) return "still_referenced";
   if (facts.referencedByLeaf) return "may_be_served";
   return "drop";
}
