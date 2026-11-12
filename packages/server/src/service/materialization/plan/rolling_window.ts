// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Rolling windows (materialization v2, phase 3): a `where:` term that compares
 * a stored time field with the current time, `ts >= now - 30 days`.
 *
 * The build evaluates the term once, at the build's own clock, and the served
 * copy keeps the term so every read re-applies it at the read's clock. The
 * stored set must be the WIDEST any read in the table's serving window needs
 * (plan, Rolling windows):
 *
 * - A lower bound (`ts >= now - 30 days`, "the last 30 days") is evaluated at
 *   build start *b*: every later read's cut-off is later, so the read trims.
 *   The build file needs no rewrite, because the build query's own `now` IS
 *   *b* (the build runs at or after *b*, and every read runs after the table
 *   it reads was written).
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
 * The term is read from the compiled source's filter list, never from its
 * text, and only one shape is accepted: a single comparison of a stored
 * timestamp field with `now`, optionally offset by a whole number of one unit.
 * Anything else that reads the current time in a `where:` is refused.
 *
 * Reading `filterList` and `fields` off a compiled source is the same read
 * `persist_dynamic_terms.ts` (the given-reading term classifier) makes; this
 * module classifies the current-time terms beside it.
 */

import {
   NOW_OFFSET_UNITS,
   type NowComparisonOp,
   type NowOffsetUnit,
} from "../../malloy_text";

/** One accepted current-time term of a materialized source. */
export interface RollingWindowTerm {
   /** The stored timestamp column the term compares. */
   field: string;
   /** As written, with the field on the left. */
   op: NowComparisonOp;
   /** `now + sign * amount unit`; absent for a bare `now`. */
   offset?: { sign: 1 | -1; amount: number; unit: NowOffsetUnit };
   /** The author's text of the term, for messages. */
   code: string;
}

/** Why a source's current-time terms cannot be materialized. */
export interface RollingWindowRefusal {
   reason:
      | "current_time_shape"
      | "current_time_upper_bound"
      | "current_time_field";
   detail: string;
}

export type RollingWindowClassification =
   | { ok: true; terms: RollingWindowTerm[] }
   | ({ ok: false } & RollingWindowRefusal);

/** Max IR depth to walk; deep enough for real expressions, a hang backstop. */
const MAX_DEPTH = 200;

/** Whether a slice of compiled IR reads the current time (`now`). */
export function readsCurrentTime(node: unknown, depth = 0): boolean {
   if (depth > MAX_DEPTH) return true; // fail closed
   if (node === null || typeof node !== "object") return false;
   if (Array.isArray(node)) {
      return node.some((item) => readsCurrentTime(item, depth + 1));
   }
   const record = node as Record<string, unknown>;
   if (record.node === "now") return true;
   for (const [key, value] of Object.entries(record)) {
      // Locations and annotations carry no expression.
      if (key === "at" || key === "location" || key === "annotations") continue;
      if (readsCurrentTime(value, depth + 1)) return true;
   }
   return false;
}

const COMPARISONS: ReadonlySet<string> = new Set([">=", ">", "<", "<="]);
const MIRROR: Record<NowComparisonOp, NowComparisonOp> = {
   ">=": "<=",
   ">": "<",
   "<": ">",
   "<=": ">=",
};
const TIME_TYPES: ReadonlySet<string> = new Set(["timestamp", "timestamptz"]);

interface Expr {
   node?: unknown;
   kids?: Record<string, unknown>;
   path?: unknown;
   op?: unknown;
   units?: unknown;
   literal?: unknown;
}

/** `now` or `now ± <n> <unit>`, read as an offset; undefined for any other shape. */
function readNow(
   e: Expr | undefined,
): { offset?: RollingWindowTerm["offset"] } | undefined {
   if (!e || typeof e !== "object") return undefined;
   if (e.node === "now") return {};
   if (e.node !== "delta") return undefined;
   const base = e.kids?.base as Expr | undefined;
   const delta = e.kids?.delta as Expr | undefined;
   if (base?.node !== "now") return undefined;
   if (delta?.node !== "numberLiteral" || typeof delta.literal !== "string") {
      return undefined;
   }
   if (!/^\d+$/.test(delta.literal)) return undefined;
   const amount = Number(delta.literal);
   if (!Number.isSafeInteger(amount)) return undefined;
   if (e.op !== "+" && e.op !== "-") return undefined;
   if (
      typeof e.units !== "string" ||
      !(NOW_OFFSET_UNITS as readonly string[]).includes(e.units)
   ) {
      return undefined;
   }
   return {
      offset: {
         sign: e.op === "-" ? -1 : 1,
         amount,
         unit: e.units as NowOffsetUnit,
      },
   };
}

/** A one-segment field reference's name, or undefined. */
function readField(e: Expr | undefined): string | undefined {
   if (!e || e.node !== "field" || !Array.isArray(e.path)) return undefined;
   if (e.path.length !== 1 || typeof e.path[0] !== "string") return undefined;
   return e.path[0];
}

/** One filter entry as a rolling-window term, or undefined when its shape is not one. */
export function readRollingWindowTerm(
   entry: unknown,
): Omit<RollingWindowTerm, "code"> | undefined {
   const e = (entry as { e?: Expr } | null)?.e;
   if (!e || typeof e.node !== "string" || !COMPARISONS.has(e.node)) {
      return undefined;
   }
   const left = e.kids?.left as Expr | undefined;
   const right = e.kids?.right as Expr | undefined;
   const op = e.node as NowComparisonOp;
   const leftField = readField(left);
   const rightNow = readNow(right);
   if (leftField !== undefined && rightNow) {
      return { field: leftField, op, ...rightNow };
   }
   const rightField = readField(right);
   const leftNow = readNow(left);
   if (rightField !== undefined && leftNow) {
      return { field: rightField, op: MIRROR[op], ...leftNow };
   }
   return undefined;
}

/** Whether the term keeps rows newer than its cut-off ("the last 30 days"). */
export function isLowerBound(term: Pick<RollingWindowTerm, "op">): boolean {
   return term.op === ">=" || term.op === ">";
}

/**
 * Classify the current-time terms of a compiled source's `where:` (its whole
 * filter list, inherited terms included: an extension's list carries its
 * parent's). `storedColumns` maps each stored column to its Malloy type.
 *
 * Accepted: a lower bound on a stored timestamp column. Refused, with the
 * first offending term: any other shape that reads `now`, an upper bound
 * (question 2), or a field that is not a stored timestamp column (the read
 * must re-apply the term over the stored table).
 */
export function classifyRollingWindow(
   def: { filterList?: unknown },
   storedColumns: ReadonlyMap<string, string>,
): RollingWindowClassification {
   const terms: RollingWindowTerm[] = [];
   const list = Array.isArray(def.filterList) ? def.filterList : [];
   for (const entry of list) {
      if (!readsCurrentTime((entry as { e?: unknown })?.e)) continue;
      const rawCode = (entry as { code?: unknown }).code;
      const code = typeof rawCode === "string" ? rawCode : "(unreadable)";
      const term = readRollingWindowTerm(entry);
      if (!term) {
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
      if (!isLowerBound(term)) {
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
      const type = storedColumns.get(term.field);
      if (type === undefined || !TIME_TYPES.has(type)) {
         return {
            ok: false,
            reason: "current_time_field",
            detail:
               `the \`where:\` term \`${code}\` compares \`${term.field}\`, which ` +
               "is not a stored timestamp column of the source, so a read cannot " +
               "re-apply it over the stored table",
         };
      }
      terms.push({ ...term, code });
   }
   return { ok: true, terms };
}

const MS = {
   second: 1000,
   minute: 60_000,
   hour: 3_600_000,
   day: 86_400_000,
   week: 7 * 86_400_000,
   // Calendar units at their longest, so the bound is never later than the
   // warehouse's own calendar arithmetic would put it.
   month: 31 * 86_400_000,
   quarter: 92 * 86_400_000,
   year: 366 * 86_400_000,
} as const;

/**
 * The margin taken off a delete bound: a day covers any session time zone
 * (the warehouse's `now` may be local time, Publisher's clock is UTC) and the
 * build's own delay after the run's start. A row kept by the margin is still
 * trimmed by every read.
 */
const DELETE_MARGIN_MS = MS.day;

/**
 * The instant below which a refresh at `runStart` may delete rows of a lower
 * bound term: every read from `runStart` on has a later cut-off, so no read
 * needs a row older than this. Never later than the term's own cut-off at
 * `runStart`; calendar units are taken at their longest and a day's margin is
 * subtracted, so an error only ever keeps a row.
 */
export function windowDeleteBefore(
   term: Pick<RollingWindowTerm, "op" | "offset">,
   runStart: Date,
): Date {
   if (!isLowerBound(term)) {
      throw new Error("only a lower-bound term deletes rows");
   }
   const offset = term.offset
      ? term.offset.sign * term.offset.amount * MS[term.offset.unit]
      : 0;
   // A positive offset on a calendar unit is shortest, not longest, when
   // added; cap it at its shortest so the bound stays early.
   const safeOffset =
      term.offset && term.offset.sign > 0
         ? term.offset.amount *
           (term.offset.unit === "month"
              ? 28 * MS.day
              : term.offset.unit === "quarter"
                ? 89 * MS.day
                : term.offset.unit === "year"
                  ? 365 * MS.day
                  : MS[term.offset.unit])
         : offset;
   return new Date(runStart.getTime() + safeOffset - DELETE_MARGIN_MS);
}

/** The one-segment field names an expression reads. */
function fieldRefs(node: unknown, out: Set<string>, depth = 0): void {
   if (depth > MAX_DEPTH || node === null || typeof node !== "object") return;
   if (Array.isArray(node)) {
      for (const item of node) fieldRefs(item, out, depth + 1);
      return;
   }
   const record = node as Record<string, unknown>;
   const name = readField(record as Expr);
   if (name !== undefined) out.add(name);
   for (const [key, value] of Object.entries(record)) {
      if (key === "at" || key === "location" || key === "annotations") continue;
      fieldRefs(value, out, depth + 1);
   }
}

/**
 * Every field of a source whose value depends on the current time: it reads
 * `now`, or reads (through any chain of the source's own fields) one that
 * does. Such a dimension is never stored, since its value differs per read.
 */
export function currentTimeFields(fields: unknown): Set<string> {
   const out = new Set<string>();
   if (!Array.isArray(fields)) return out;
   const deps = new Map<string, Set<string>>();
   for (const raw of fields) {
      const f = raw as { name?: unknown; as?: unknown; e?: unknown };
      const name =
         typeof f.as === "string"
            ? f.as
            : typeof f.name === "string"
              ? f.name
              : undefined;
      if (name === undefined) continue;
      if (readsCurrentTime(f.e)) out.add(name);
      const refs = new Set<string>();
      fieldRefs(f.e, refs);
      deps.set(name, refs);
   }
   for (let changed = true; changed; ) {
      changed = false;
      for (const [name, refs] of deps) {
         if (out.has(name)) continue;
         if ([...refs].some((r) => out.has(r))) {
            out.add(name);
            changed = true;
         }
      }
   }
   return out;
}
