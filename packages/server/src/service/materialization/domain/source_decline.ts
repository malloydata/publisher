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
   cross_connection_join: { stage: "served_copy" },
   inline_join_kept: { stage: "served_copy" },
   reroot_not_compiled: { stage: "served_copy" },
   classification_not_kept: { stage: "served_copy" },
} as const satisfies Record<string, { stage: "bind" | "served_copy" }>;

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
