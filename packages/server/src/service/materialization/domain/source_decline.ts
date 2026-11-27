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
 * A reason whose row says `servesInPart` leaves the source served, with a
 * join read as written or left out; the bind counter counts it `partial`.
 *
 * Pure.
 */

/** Every reason a source entry is declined, with where it is decided. */
const SOURCE_DECLINES = {
   // The bind (`serve/bind.ts`, the facade).
   build_plan_unavailable: { stage: "bind" },
   refused: { stage: "bind" },
   missing_table: { stage: "bind" },
   // A manifest entry's table name its dialect cannot read or quote.
   unquotable_name: { stage: "bind" },
   // The served copy's compile (`serve/served_copy_serve.ts`).
   compile_failed: { stage: "served_copy" },
   cross_connection_unbuilt: { stage: "served_copy" },
   column_types: { stage: "served_copy" },
   no_declaration: { stage: "served_copy" },
   definition_not_parsed: { stage: "served_copy" },
   pair_join_not_found: { stage: "served_copy" },
   pair_not_compiled: { stage: "served_copy" },
   fields_not_carried: { stage: "served_copy" },
   read_terms_not_kept: { stage: "served_copy" },
   // These two leave the source served, with a join read as written (kept
   // inline) or left out (across connections, read live when a query uses it).
   cross_connection_join: { stage: "served_copy", servesInPart: true },
   inline_join_kept: { stage: "served_copy", servesInPart: true },
   reroot_not_compiled: { stage: "served_copy" },
   classification_not_kept: { stage: "served_copy" },
   // A `.sql()` source reads it through `%{}` on another connection than its
   // table is read on (a storage destination, a managed catalog's).
   sql_embed_cross_connection: { stage: "served_copy" },
   // A `.sql()` source embeds it bare (`%{ x }`), which Malloy allows only of
   // a persistable source, and its re-root is not one.
   sql_embed_whole_source: { stage: "served_copy" },
} as const satisfies Record<
   string,
   { stage: "bind" | "served_copy"; servesInPart?: true }
>;

/**
 * How to serve a source a `.sql()` source's `%{}` reads where the `%{}`
 * cannot reach its table (`sql_embed_cross_connection`), and one it embeds
 * bare (`sql_embed_whole_source`): said by the warning and the serve verdict
 * alike. Each way is one the component spec serves.
 */
export const SQL_EMBED_FIX: Readonly<
   Record<"sql_embed_cross_connection" | "sql_embed_whole_source", string>
> = {
   sql_embed_cross_connection:
      "To serve it, persist the `.sql()` source itself with the same `storage=` as the sources " +
      "its `%{}` reads: the copy then reads the `.sql()` source's own table, and theirs serve too. " +
      "Or drop `storage=` from the sources it reads, so they are built in their own database, where " +
      "its `%{}` reads their tables. On a DuckLake or managed connection, which builds no author SQL " +
      "and places every table itself, build what it reads into a warehouse table and read that with " +
      "`.table()`.",
   sql_embed_whole_source:
      "To serve it, embed it as a query, `%{ <source> -> { select: * } }` (whose columns are the " +
      "source's fields, not its SQL's): in the source's own database the copy then reads its table, " +
      "and in a storage destination the fix for `sql_embed_cross_connection` applies.",
};

/** Why a source entry is not served. */
type SourceDeclineReason = keyof typeof SOURCE_DECLINES;

type DeclinedAt<S extends "bind" | "served_copy"> = {
   [R in SourceDeclineReason]: (typeof SOURCE_DECLINES)[R]["stage"] extends S
      ? R
      : never;
}[SourceDeclineReason];

/** The bind's reasons. */
export type BindDeclineReason = DeclinedAt<"bind">;

/** The served copy's reasons (`ServedCopyProblem`'s). */
export type ServedCopyProblemReason = DeclinedAt<"served_copy">;

/**
 * The served copy's reasons, most actionable first: what an author changes
 * in the model (a join across connections, a filter, inherited fields, a
 * column's type) before what only a Publisher fix or a rebuild changes.
 */
const ACTIONABLE_ORDER: readonly string[] = [
   "cross_connection_join",
   "cross_connection_unbuilt",
   "sql_embed_cross_connection",
   "sql_embed_whole_source",
   "inline_join_kept",
   "read_terms_not_kept",
   "fields_not_carried",
   "column_types",
   "classification_not_kept",
   "pair_join_not_found",
   "pair_not_compiled",
   "definition_not_parsed",
   "compile_failed",
   "no_declaration",
   "reroot_not_compiled",
] satisfies readonly ServedCopyProblemReason[];

/**
 * `reasons` without repeats, most actionable first; a reason the order does
 * not know goes last, in the order given.
 */
export function byActionability(reasons: readonly string[]): string[] {
   const rank = (r: string) => {
      const at = ACTIONABLE_ORDER.indexOf(r);
      return at < 0 ? ACTIONABLE_ORDER.length : at;
   };
   return [...new Set(reasons)]
      .map((r, i) => ({ r, i }))
      .sort((a, b) => rank(a.r) - rank(b.r) || a.i - b.i)
      .map((x) => x.r);
}

/** Every reason, in the table's order. */
export const SOURCE_DECLINE_REASONS = Object.keys(
   SOURCE_DECLINES,
) as SourceDeclineReason[];

/**
 * Whether a source declined for `reason` still serves, with part of its copy
 * left out (a join read as written or left out): the bind counter's
 * `partial`, not
 * `declined`.
 */
export function servesInPart(reason: SourceDeclineReason): boolean {
   const row: { stage: string; servesInPart?: true } = SOURCE_DECLINES[reason];
   return row.servesInPart === true;
}

/** One source the served copy's compile did not serve whole, as counted. */
interface SourceOutcome {
   readonly outcome: "partial" | "declined";
   readonly reason: ServedCopyProblemReason;
}

/**
 * One outcome per source for one compile of the served copy. A source's
 * problems are grouped by its `sourceID` (else its name); `declined` beats
 * `partial`, so a source with any reason that leaves it out is declined, by
 * the first such reason; a source whose every reason serves it in part is
 * `partial` only while it is still re-rooted in the compiled copy, else
 * declined. A problem that names no source is one `declined` of its own.
 */
export function servedCopyOutcomes(
   problems: readonly {
      readonly sourceID?: string;
      readonly sourceName?: string;
      readonly reason: ServedCopyProblemReason;
   }[],
   rerootedSourceIDs: ReadonlySet<string>,
): SourceOutcome[] {
   const bySource = new Map<string, ServedCopyProblemReason[]>();
   const out: SourceOutcome[] = [];
   for (const p of problems) {
      const key =
         p.sourceID !== undefined
            ? `id:${p.sourceID}`
            : p.sourceName !== undefined
              ? `name:${p.sourceName}`
              : undefined;
      if (key === undefined) {
         out.push({ outcome: "declined", reason: p.reason });
         continue;
      }
      const reasons = bySource.get(key) ?? [];
      reasons.push(p.reason);
      bySource.set(key, reasons);
   }
   for (const [key, reasons] of bySource) {
      const leavesOut = reasons.find((r) => !servesInPart(r));
      if (leavesOut !== undefined) {
         out.push({ outcome: "declined", reason: leavesOut });
         continue;
      }
      const stillServed =
         key.startsWith("id:") && rerootedSourceIDs.has(key.slice(3));
      out.push({
         outcome: stillServed ? "partial" : "declined",
         reason: reasons[0],
      });
   }
   return out;
}
