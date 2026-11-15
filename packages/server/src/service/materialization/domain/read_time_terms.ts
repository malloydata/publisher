// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A materialized source's read-time `where:` terms as data: where a term reads
 * a field or a given in its file, a current-time comparison in the one shape a
 * stored table can re-apply (a rolling window, with its closed sets of
 * comparisons and offset units), and the terms a plan carries for its served
 * copy to apply again at every read.
 *
 * The planner's term classifier (`plan/persist_dynamic_terms.ts`) and the
 * rolling-window reader (`plan/rolling_window.ts`) produce them; the served
 * copy writes them again over the stored table, and the run deletes below a
 * lower bound. `malloy_text.ts` writes a window's comparison.
 *
 * Pure.
 */

/** The current-time offset units a rolling-window term may use. */
export const NOW_OFFSET_UNITS = [
   "second",
   "minute",
   "hour",
   "day",
   "week",
   "month",
   "quarter",
   "year",
] as const;
export type NowOffsetUnit = (typeof NOW_OFFSET_UNITS)[number];

/** The comparisons a rolling-window term may use. */
export type NowComparisonOp = ">=" | ">" | "<" | "<=";

/** A zero-based range in a Malloy file, as the compiler reports one. */
export interface MalloyRange {
   start: { line: number; character: number };
   end: { line: number; character: number };
}

/** Where a term reads a field or a given, as its `refSummary` records it. */
export interface TermUse {
   url: string;
   range: MalloyRange;
}

/**
 * A current-time comparison in the one shape a stored table can re-apply at
 * read: a one-segment field compared with `now`, optionally offset by a whole
 * number of one unit, read with the field on the left. The direction is the
 * comparison: `>=`/`>` keep rows newer than the cut-off (a lower bound),
 * `<`/`<=` rows older than it (an upper bound).
 */
export interface RollingWindow {
   /** The field the term compares (a stored column, when it is stored). */
   field: string;
   /** As read with the field on the left. */
   op: NowComparisonOp;
   /** `now + sign * amount unit`; absent for a bare `now`. */
   offset?: { sign: 1 | -1; amount: number; unit: NowOffsetUnit };
}

/**
 * One comparison of a current-time `where:` term of a materialized source,
 * which its served copy writes again over the stored table, at every read's
 * clock. A two-sided term is two of these, with the same `code`.
 */
export interface CurrentTimeReadTerm {
   kind: "current_time";
   /** The author's text of the term, for messages. */
   code: string;
   /** The stored columns it reads: the window's field. */
   columns: string[];
   window: RollingWindow;
   /**
    * Set when the build leaves the whole term out (it holds an upper bound):
    * where the term reads each field, by which the build copy finds its text
    * and replaces it with `true`. Absent: the term is built as written.
    */
   leftOut?: TermUse[];
}

/**
 * A given-reading `where:` term a materialized source's build leaves out and
 * its served copy keeps (P2). `columns` are the stored columns it reads, so a
 * read over the table can apply it. The served copy writes nothing for it:
 * the term is in the declaration's own trailing `extend`, which the re-root
 * keeps as written (see {@link termSpans}).
 */
export interface GivenReadTerm {
   kind: "given";
   /** The term as the author wrote it, verbatim. */
   code: string;
   /** The givens it reads. */
   givens: string[];
   /** The stored columns it reads. */
   columns: string[];
   /** Where it reads each field and given, in the declaring file. */
   at: TermUse[];
}

/**
 * A `where:` term of a materialized source that depends on the read, so the
 * served copy applies it again over the stored table, by what it reads
 * (`classifyWhereTerms`, the one term classifier), told apart by `kind`. A
 * current-time lower bound (rolling_window.ts) is built as written, since the
 * build's own `now` stores the widest set, and is also the bound an
 * incremental refresh deletes below; a current-time term holding an upper
 * bound is left out of the build (`leftOut`), since with no refresh interval
 * the widest set is every row. A given-reading term (P2) is left out of the
 * build and kept by the served copy as the author wrote it.
 */
export type MaterializeReadTimeTerm = CurrentTimeReadTerm | GivenReadTerm;

/** The windows among a plan's read-time terms, lower and upper bounds alike. */
export function rollingWindowsOf(
   terms: readonly { kind: string; window?: RollingWindow }[] | undefined,
): RollingWindow[] {
   return (terms ?? []).flatMap((t) =>
      t.kind === "current_time" && t.window ? [t.window] : [],
   );
}
