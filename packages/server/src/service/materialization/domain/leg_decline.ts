// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * One vocabulary for why a serving leg does not answer a query: each decline
 * carries its public `SkipReason`, its code, the package warning a `WARNED`
 * decline points to, and the caller's detail, all from one table.
 *
 * Thrown as a `LegDecline` by the view, rollup and served legs; read by the
 * router (query_router.ts, through `VariantSkipExplanation`) and the path
 * report (serve/query_path.ts). A decline's public form is fixed by its row,
 * never chosen at the throw site.
 *
 * Pure.
 */

import type { components } from "../../../api";

/** A candidate's own account of why it did not answer (see the router's `Variant.explain`). */
export interface VariantSkipExplanation {
   code: string;
   callerDetail?: string;
}

type SkipReason = components["schemas"]["SkipReason"];

/**
 * Every reason a leg declines a query, each with the public reason the path
 * report gives for it. A `WARNED` decline also names the package warning
 * that explains it, or the reader's own `#@ -persist`.
 *
 * - `rollup_contract`: a joined source's gate, which the rollup read through
 *   the join does not carry; `rollup_cross_connection`: a rollup in a storage
 *   destination, across from the query source.
 * - `served_copy_time_zone`: the query sets a timezone over a stored computed
 *   dimension; `served_copy_cross_connection`: the copy waits on a join
 *   across connections, or the query reads a join it leaves out across
 *   connections; `served_copy_not_served`: the copy (or the queried source
 *   in it) carries a `served-copy-not-served` warning; `served_copy_opt_out`:
 *   the queried source opts out with `#@ -persist`.
 * - `served_copy_count_semantics`: the query aggregates at the grain of a
 *   join the copy answers from stored columns (an inlined join_one), which
 *   only live's join computes. `served_copy_joined_annotations`: the query
 *   returns a field of such a join that the author annotated, whose notes
 *   its record cannot carry, so the table lacks the field as live returns it.
 * - `served_copy_pair_shape`: the query aggregates a pair table (an inlined
 *   join_many, served flat) in a way that table does not reproduce exactly.
 * - `served_copy_pair_parent_only`: exact or not, a query that reads nothing
 *   through the inlined join_many reads each parent off a flag over every
 *   pair row, so the table's rows are not the parents the query counts
 *   (see pair_flat_query.ts).
 */
const DECLINES = {
   rollup_missing_field: { reason: "MISSING_FIELD" },
   rollup_count_semantics: { reason: "COUNT_SEMANTICS" },
   rollup_grain_too_coarse: { reason: "GRAIN_TOO_COARSE" },
   rollup_time_zone: { reason: "TIME_ZONE" },
   rollup_not_built: { reason: "NOT_BUILT" },
   rollup_contract: { reason: "CONTRACT" },
   rollup_cross_connection: { reason: "CROSS_CONNECTION" },
   view_missing_field: { reason: "MISSING_FIELD" },
   view_not_built: { reason: "NOT_BUILT" },
   view_stale: { reason: "STALE" },
   served_copy_time_zone: { reason: "TIME_ZONE" },
   served_copy_cross_connection: { reason: "CROSS_CONNECTION" },
   served_copy_not_served: {
      reason: "WARNED",
      warnedCode: "served-copy-not-served",
   },
   served_copy_opt_out: { reason: "WARNED", warnedCode: "persist-opt-out" },
   served_copy_count_semantics: { reason: "COUNT_SEMANTICS" },
   served_copy_joined_annotations: { reason: "MISSING_FIELD" },
   served_copy_pair_shape: { reason: "COUNT_SEMANTICS" },
   served_copy_pair_parent_only: { reason: "COUNT_SEMANTICS" },
} as const satisfies Record<
   string,
   { reason: SkipReason; warnedCode?: string }
>;

type LegDeclineCode = keyof typeof DECLINES;

/** The served copy's own codes. */
export type ServedCopyDeclineCode = Extract<
   LegDeclineCode,
   `served_copy_${string}`
>;

/** The leg a code belongs to, by its prefix. */
type DecliningLeg = "rollup" | "view" | "served_copy";

/**
 * A leg that did not answer one query: expected, so logged quietly. Its
 * `reason` is the public reason the path report gives for it.
 */
export class LegDecline extends Error {
   readonly reason: SkipReason;
   /** The package warning a `WARNED` decline points at. */
   readonly warnedCode?: string;
   /** Decided from the query's shape alone, before compiling against it. */
   readonly shapeCheck: boolean;

   constructor(
      readonly code: LegDeclineCode,
      /** In the caller's own terms, safe to show: never a table name. */
      readonly callerDetail?: string,
      options: {
         shapeCheck?: boolean;
         /** For logs only, when it says more than the caller's detail. */
         message?: string;
      } = {},
   ) {
      super(options.message ?? callerDetail ?? code);
      const decline: { reason: SkipReason; warnedCode?: string } =
         DECLINES[code];
      this.reason = decline.reason;
      this.warnedCode = decline.warnedCode;
      this.shapeCheck = options.shapeCheck ?? false;
   }

   get leg(): DecliningLeg {
      return this.code.startsWith("rollup_")
         ? "rollup"
         : this.code.startsWith("view_")
           ? "view"
           : "served_copy";
   }
}

/** Whether `error` is a decline of `leg`. */
export function isDeclineOf(
   error: unknown,
   leg: DecliningLeg,
): error is LegDecline {
   return error instanceof LegDecline && error.leg === leg;
}

/** A decline of `leg` as the router records it (see Variant.explain). */
export function explainDecline(
   error: unknown,
   leg: DecliningLeg,
): VariantSkipExplanation | undefined {
   if (!isDeclineOf(error, leg)) return undefined;
   return {
      code: error.code,
      ...(error.callerDetail ? { callerDetail: error.callerDetail } : {}),
   };
}

/** The public reason and warning code of a decline the router recorded by code. */
export function declineOfCode(
   code: string | undefined,
): { reason: SkipReason; warnedCode?: string } | undefined {
   return code !== undefined && Object.hasOwn(DECLINES, code)
      ? DECLINES[code as LegDeclineCode]
      : undefined;
}
