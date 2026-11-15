// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * One table of reasons a stored source is not served: the bind's (the plan
 * could not be computed, it refused the source, or the table it holds was
 * built from another definition) and the served copy's (its re-root of the
 * source did not compile, or did not keep what live keeps). Each row is a
 * value of `publisher_served_copy_bind_total`'s `reason` label, so the label
 * and the code that declines can never name different things.
 *
 * A reason whose row says `servesInPart` leaves the source served with part
 * of its copy left out; the bind counter counts it `partial`.
 *
 * Pure.
 */

/** Every reason a source entry is declined, with where it is decided. */
const SOURCE_DECLINES = {
   // The bind (`serve/bind.ts`, the facade).
   build_plan_unavailable: { stage: "bind" },
   refused: { stage: "bind" },
   missing_table: { stage: "bind" },
   // The served copy's compile (`serve/served_copy_serve.ts`).
   compile_failed: { stage: "served_copy" },
   cross_connection_unbuilt: { stage: "served_copy" },
   column_types: { stage: "served_copy" },
   no_declaration: { stage: "served_copy" },
   range_not_found: { stage: "served_copy" },
   definition_not_parsed: { stage: "served_copy" },
   pair_join_not_found: { stage: "served_copy" },
   pair_not_compiled: { stage: "served_copy" },
   fields_not_carried: { stage: "served_copy" },
   read_terms_not_kept: { stage: "served_copy" },
   // These two leave the source served, with one join answered live: the
   // copy keeps the join as written, or leaves out a join across connections.
   cross_connection_join: { stage: "served_copy", servesInPart: true },
   inline_join_kept: { stage: "served_copy", servesInPart: true },
   reroot_not_compiled: { stage: "served_copy" },
   classification_not_kept: { stage: "served_copy" },
} as const satisfies Record<
   string,
   { stage: "bind" | "served_copy"; servesInPart?: true }
>;

/** Why a source entry is not served. */
export type SourceDeclineReason = keyof typeof SOURCE_DECLINES;

type DeclinedAt<S extends "bind" | "served_copy"> = {
   [R in SourceDeclineReason]: (typeof SOURCE_DECLINES)[R]["stage"] extends S
      ? R
      : never;
}[SourceDeclineReason];

/** The bind's reasons. */
export type BindDeclineReason = DeclinedAt<"bind">;

/** The served copy's reasons (`ServedCopyProblem`'s). */
export type ServedCopyProblemReason = DeclinedAt<"served_copy">;

/** Every reason, in the table's order. */
export const SOURCE_DECLINE_REASONS = Object.keys(
   SOURCE_DECLINES,
) as SourceDeclineReason[];

/**
 * Whether a source declined for `reason` still serves, with part of its copy
 * left out (a join answered live): the bind counter's `partial`, not
 * `declined`.
 */
export function servesInPart(reason: SourceDeclineReason): boolean {
   const row: { stage: string; servesInPart?: true } = SOURCE_DECLINES[reason];
   return row.servesInPart === true;
}
