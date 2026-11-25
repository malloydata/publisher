// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * One vocabulary for why a serving leg does not answer a query: each decline
 * carries its public `SkipReason`, its code, the candidate `code` a
 * `NOT_SERVED` decline reports, and the caller's detail, all from one table.
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
   /**
    * The rollup leg's own decline for each rollup it tried, by the rollup's
    * synthesized source name, when it tried more than one: the path report
    * gives each rollup its own reason, and {@link code} stays the leg's
    * summary.
    */
   members?: Readonly<Record<string, Omit<VariantSkipExplanation, "members">>>;
   /**
    * The tables the decline is about, by content address, when it is about
    * some of the tables the leg read rather than all of them.
    */
   tableIds?: readonly string[];
}

type SkipReason = components["schemas"]["SkipReason"];

/**
 * Every reason a leg declines a query, each with the public reason the path
 * report gives for it. A `NOT_SERVED` decline also names the candidate
 * `code` that explains it, in the serve verdict's words.
 *
 * - `view_shape_mismatch`, `query_shape_mismatch`: the query is not the shape
 *   the stored result answers (not a call of it, a refined call, or text that
 *   names beside the call a name the variant takes over).
 * - `rollup_row_level_gate`: a joined source's gate, which the rollup read through
 *   the join does not carry; `rollup_cross_connection`: a rollup in a storage
 *   destination, across from the query source.
 * - `rollup_stale`, `served_copy_stale`: the table the leg would read is past
 *   its freshness window, so the query's pin leaves it out.
 * - `view_storage_mode`, `query_storage_mode`: the stored result is a storage
 *   table, and `PERSIST_STORAGE_MODE` (write-only or off) reads none; the
 *   serve verdict's `storage_mode`.
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
 * - `served_copy_column_left_out`: the query reads a stored column the
 *   re-root leaves out of its table's type, since no cast reads what the
 *   table holds as the source declares it.
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
   rollup_stale: { reason: "STALE" },
   rollup_row_level_gate: { reason: "ROW_LEVEL_GATE" },
   rollup_cross_connection: { reason: "CROSS_CONNECTION" },
   view_missing_field: { reason: "MISSING_FIELD" },
   view_shape_mismatch: { reason: "SHAPE_MISMATCH" },
   view_not_built: { reason: "NOT_BUILT" },
   view_stale: { reason: "STALE" },
   view_storage_mode: { reason: "NOT_SERVED", code: "storage_mode" },
   query_missing_field: { reason: "MISSING_FIELD" },
   query_shape_mismatch: { reason: "SHAPE_MISMATCH" },
   query_not_built: { reason: "NOT_BUILT" },
   query_stale: { reason: "STALE" },
   query_storage_mode: { reason: "NOT_SERVED", code: "storage_mode" },
   served_copy_time_zone: { reason: "TIME_ZONE" },
   served_copy_cross_connection: { reason: "CROSS_CONNECTION" },
   served_copy_stale: { reason: "STALE" },
   // The same words as the serve verdict for a table the served copy does
   // not re-root; the `served-copy-not-served` warning says why.
   served_copy_not_served: { reason: "NOT_SERVED", code: "served_copy" },
   served_copy_opt_out: { reason: "OPTED_OUT" },
   served_copy_count_semantics: { reason: "COUNT_SEMANTICS" },
   served_copy_joined_annotations: { reason: "MISSING_FIELD" },
   served_copy_column_left_out: { reason: "MISSING_FIELD" },
   served_copy_pair_shape: { reason: "COUNT_SEMANTICS" },
   served_copy_pair_parent_only: { reason: "COUNT_SEMANTICS" },
} as const satisfies Record<string, { reason: SkipReason; code?: string }>;

type LegDeclineCode = keyof typeof DECLINES;

/** The served copy's own codes. */
export type ServedCopyDeclineCode = Extract<
   LegDeclineCode,
   `served_copy_${string}`
>;

/** The leg a code belongs to, by its prefix. */
type DecliningLeg = "rollup" | "view" | "query" | "served_copy";

/**
 * A leg that did not answer one query: expected, so logged quietly. Its
 * `reason` is the public reason the path report gives for it.
 */
export class LegDecline extends Error {
   readonly reason: SkipReason;
   /** The candidate `code` a `NOT_SERVED` decline reports. */
   readonly candidateCode?: string;
   /** Decided from the query's shape alone, before compiling against it. */
   readonly shapeCheck: boolean;
   /**
    * For the rollup leg's summary decline: each rollup it tried, by its
    * synthesized source name, with that rollup's own decline.
    */
   readonly members?: ReadonlyMap<string, LegDecline>;
   /** See {@link VariantSkipExplanation.tableIds}. */
   readonly tableIds?: readonly string[];

   constructor(
      readonly code: LegDeclineCode,
      /** In the caller's own terms, safe to show: never a table name. */
      readonly callerDetail?: string,
      options: {
         shapeCheck?: boolean;
         /** For logs only, when it says more than the caller's detail. */
         message?: string;
         members?: ReadonlyMap<string, LegDecline>;
         tableIds?: readonly string[];
      } = {},
   ) {
      super(options.message ?? callerDetail ?? code);
      if (options.members && options.members.size > 0) {
         this.members = options.members;
      }
      if (options.tableIds && options.tableIds.length > 0) {
         this.tableIds = options.tableIds;
      }
      const decline: { reason: SkipReason; code?: string } = DECLINES[code];
      this.reason = decline.reason;
      this.candidateCode = decline.code;
      this.shapeCheck = options.shapeCheck ?? false;
   }

   get leg(): DecliningLeg {
      return this.code.startsWith("rollup_")
         ? "rollup"
         : this.code.startsWith("view_")
           ? "view"
           : this.code.startsWith("query_")
             ? "query"
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

/** The public reason and candidate code of a decline the router recorded by code. */
export function declineOfCode(
   code: string | undefined,
): { reason: SkipReason; code?: string } | undefined {
   return code !== undefined && Object.hasOwn(DECLINES, code)
      ? DECLINES[code as LegDeclineCode]
      : undefined;
}

/**
 * `summary`, the leg's one decline, carrying `members`, each tried rollup's
 * own: a fresh decline, so a shared instance is never changed.
 */
export function withMemberDeclines(
   summary: LegDecline,
   members: ReadonlyMap<string, LegDecline>,
): LegDecline {
   if (members.size === 0) return summary;
   return new LegDecline(summary.code, summary.callerDetail, {
      shapeCheck: summary.shapeCheck,
      message: summary.message,
      members,
   });
}

/** A decline's {@link VariantSkipExplanation}, its members' included. */
export function explanationOfDecline(
   decline: LegDecline,
): VariantSkipExplanation {
   const one = (d: LegDecline) => ({
      code: d.code,
      ...(d.callerDetail ? { callerDetail: d.callerDetail } : {}),
   });
   return {
      ...one(decline),
      ...(decline.tableIds ? { tableIds: decline.tableIds } : {}),
      ...(decline.members
         ? {
              members: Object.fromEntries(
                 [...decline.members].map(([name, d]) => [name, one(d)]),
              ),
           }
         : {}),
   };
}
