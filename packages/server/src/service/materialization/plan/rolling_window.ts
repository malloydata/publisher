// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Rolling windows (materialization v2, phase 3): what a `#(materialize)`
 * source may do with a `where:` term that compares a stored time field with
 * the current time, `ts >= now - 30 days`. The terms themselves are read by
 * the one `where:` classifier, `classifyWhereTerms` in
 * `persist_dynamic_terms.ts`; this module is the policy over its output and
 * reads no IR.
 *
 * The build evaluates the term once, at the build's own clock, and the served
 * copy keeps the term so every read re-applies it at the read's clock. The
 * stored set must be the WIDEST any read in the table's serving window needs
 * (plan, Rolling windows):
 *
 * - A lower bound (`ts >= now - 30 days`, "the last 30 days") is evaluated at
 *   build start *b*: every later read's cut-off is later, so the read trims.
 *   The build file writes the term as the author did, with no literal: the
 *   build query's own `now` is at or after *b*, and every read runs after the
 *   table it reads was written. The content address is computed over that
 *   unsubstituted text, so it never moves with the clock.
 * - An upper bound (`ts < now - 30 days`, "older than 30 days") must be
 *   evaluated at *b* + the refresh interval, which Publisher does not know for
 *   a build on demand (question 2). It is refused: the source is not built and
 *   serves live. A later pull request can accept it where the scheduler's cron
 *   names the interval.
 *
 * Each refresh of an incremental table appends rows above the watermark and
 * then deletes the rows below the new lower bound ({@link windowDeleteBefore}),
 * so the table stays one window plus one interval in size.
 *
 * The origin's build and the destination's read must agree on the instant
 * `now` names. They do because Malloy's connectors read it in UTC: DuckDB,
 * Postgres, Snowflake and Trino pin the session to UTC, and BigQuery's
 * `CURRENT_TIMESTAMP()` and SQL Server's `SYSUTCDATETIME()` are UTC. A
 * connection whose setup SQL moves the session's time zone breaks that, as it
 * breaks Malloy's own time arithmetic. A query `timezone:` moves the cut-off
 * to that zone's wall time, so the served copy answers such a query live.
 */

import type { NowOffsetUnit } from "../../malloy_text";
import type {
   ClassifiedWhereTerm,
   RollingWindow,
} from "./persist_dynamic_terms";

export type { RollingWindow };

/**
 * A current-time `where:` term a materialized source builds as written and its
 * served copy writes again over the stored table, at every read's clock.
 */
export interface CurrentTimeReadTerm {
   kind: "current_time";
   /** The author's text of the term, for messages. */
   code: string;
   /** The stored columns it reads: the window's field. */
   columns: string[];
   window: RollingWindow;
}

/** Why a source's current-time terms cannot be materialized. */
export interface RollingWindowRefusal {
   reason:
      | "current_time_shape"
      | "current_time_upper_bound"
      | "current_time_field"
      | "current_time_given";
   detail: string;
}

export type RollingWindowClassification =
   | { ok: true; terms: CurrentTimeReadTerm[] }
   | ({ ok: false } & RollingWindowRefusal);

const TIME_TYPES: ReadonlySet<string> = new Set(["timestamp", "timestamptz"]);

/** Whether the term keeps rows newer than its cut-off ("the last 30 days"). */
export function isLowerBound(window: Pick<RollingWindow, "op">): boolean {
   return window.op === ">=" || window.op === ">";
}

/**
 * The current-time terms of a materialized source, from the classified
 * `where:` terms of its compiled definition, or the first that cannot be
 * stored. `storedColumns` maps each stored column to its Malloy type.
 *
 * Accepted: a lower bound on a stored timestamp column. Refused: any other
 * shape that reads `now` (directly or through a field or join), an upper
 * bound (question 2), a field that is not a stored timestamp column (the read
 * must re-apply the term over the stored table), and a term that also reads a
 * given. Fixed and given-reading terms are not this module's to judge.
 */
export function rollingWindowTerms(
   classified: readonly ClassifiedWhereTerm[],
   storedColumns: ReadonlyMap<string, string>,
): RollingWindowClassification {
   const terms: CurrentTimeReadTerm[] = [];
   for (const { code: rawCode, dependsOn } of classified) {
      const code = rawCode || "(unreadable)";
      if (dependsOn.kind === "given_and_current_time") {
         return {
            ok: false,
            reason: "current_time_given",
            detail:
               `the \`where:\` term \`${code}\` reads both a given and the ` +
               "current time, which a stored table cannot re-apply as one " +
               "term; write them as two `where:` terms",
         };
      }
      if (dependsOn.kind !== "current_time") continue;
      const window = dependsOn.window;
      if (!window) {
         return {
            ok: false,
            reason: "current_time_shape",
            detail:
               `the \`where:\` term \`${code}\` reads the current time but is not ` +
               "a rolling window Publisher can store: write one comparison of a " +
               "stored timestamp with `now`, optionally offset by a whole number " +
               "of one unit (`ts >= now - 30 days`)",
         };
      }
      if (!isLowerBound(window)) {
         return {
            ok: false,
            reason: "current_time_upper_bound",
            detail:
               `the \`where:\` term \`${code}\` keeps rows OLDER than a moving ` +
               "cut-off, so the table would have to be built as of the end of its " +
               "refresh interval, which a build on demand does not know; it " +
               "serves live",
         };
      }
      const type = storedColumns.get(window.field);
      if (type === undefined || !TIME_TYPES.has(type)) {
         return {
            ok: false,
            reason: "current_time_field",
            detail:
               `the \`where:\` term \`${code}\` compares \`${window.field}\`, which ` +
               "is not a stored timestamp column of the source, so a read cannot " +
               "re-apply it over the stored table",
         };
      }
      terms.push({
         kind: "current_time",
         code,
         columns: [window.field],
         window,
      });
   }
   return { ok: true, terms };
}

/** The windows among a plan's read-time terms: what a refresh deletes below. */
export function rollingWindowsOf(
   terms: readonly { kind: string; window?: RollingWindow }[] | undefined,
): RollingWindow[] {
   return (terms ?? []).flatMap((t) =>
      t.kind === "current_time" && t.window ? [t.window] : [],
   );
}

const DAY_MS = 86_400_000;

/**
 * Each unit's length at its longest and at its shortest, so a bound computed
 * from it is never later than the warehouse's own calendar arithmetic would
 * put it: a subtracted unit is taken at its longest, an added one at its
 * shortest.
 */
const UNIT_MS: Record<NowOffsetUnit, { longest: number; shortest: number }> = {
   second: { longest: 1000, shortest: 1000 },
   minute: { longest: 60_000, shortest: 60_000 },
   hour: { longest: 3_600_000, shortest: 3_600_000 },
   day: { longest: DAY_MS, shortest: DAY_MS },
   week: { longest: 7 * DAY_MS, shortest: 7 * DAY_MS },
   month: { longest: 31 * DAY_MS, shortest: 28 * DAY_MS },
   quarter: { longest: 92 * DAY_MS, shortest: 89 * DAY_MS },
   year: { longest: 366 * DAY_MS, shortest: 365 * DAY_MS },
};

/**
 * The margin taken off a delete bound: a day covers a warehouse clock or
 * session that differs from Publisher's, a daylight-saving hour in a day-sized
 * unit, and the build's own delay after the run's start. A row kept by the
 * margin is still trimmed by every read.
 */
const DELETE_MARGIN_MS = DAY_MS;

/**
 * The instant below which a refresh at `runStart` may delete rows of a lower
 * bound term: every read from `runStart` on has a later cut-off, so no read
 * needs a row older than this. Never later than the term's own cut-off at
 * `runStart`; calendar units are taken at their longest when subtracted and
 * their shortest when added, and a day's margin is subtracted, so an error
 * only ever keeps a row.
 */
export function windowDeleteBefore(
   window: Pick<RollingWindow, "op" | "offset">,
   runStart: Date,
): Date {
   if (!isLowerBound(window)) {
      throw new Error("only a lower-bound term deletes rows");
   }
   const o = window.offset;
   const offset = o
      ? o.sign < 0
         ? -o.amount * UNIT_MS[o.unit].longest
         : o.amount * UNIT_MS[o.unit].shortest
      : 0;
   return new Date(runStart.getTime() + offset - DELETE_MARGIN_MS);
}
