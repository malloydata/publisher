// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The served copy's answer-time checks: each reads one query's answer in
 * progress ({@link AnswerAttempt}) on one compiled copy and declines the
 * query when the copy would not answer it as live does (it reads no
 * re-rooted table, an opted-out or unsafe source, a value stored in another
 * time zone, a join's grain or annotations a record of stored columns cannot
 * keep, a gate column the copy leaves out). A decline sends the query on to
 * the next candidate. The pair-table checks stay beside the served copy
 * (served_copy_serve.ts).
 */

import { queryTimezoneOf, sameZone } from "../../governance/ir/compiled_model";
import { annotatedJoinedOutput } from "../../governance/ir/joined_field_output";
import {
   joinGrainAggregatePaths,
   queryFieldUsagePaths,
   queryTimezones,
   structRefName,
   type PreparedQueryView,
   type readPreparedQuery,
   resolveStructRefSource,
} from "../../governance/ir/prepared_query";
import type { Binding } from "../domain/binding";
import type { ServedCopyDeclineCode } from "../domain/leg_decline";
import { lexAll } from "../../malloy_lexer_core";
import { SourceMap } from "../text/source_map";
import { boundTablePath, sqlReadsTable } from "./bindings";
import { entryPointGateExprs, gateReadNames } from "./gate_reads";
import type {
   AnswerAttempt,
   Compiled,
   SourceDecline,
} from "./served_copy_types";

/** The caller's detail for a join-grain aggregate through an inlined join. */
const JOIN_GRAIN_DETAIL =
   "The query aggregates at the grain of a join whose fields this table stores inline (a measure of the joined source, or an aggregate such as j.x.sum() or j.count()), which only the join computes, so it is answered live.";

/** The caller's detail for a query that returns an annotated field of an inlined join. */
const JOINED_ANNOTATIONS_DETAIL =
   "The query returns a field of a join whose fields this table stores inline, and the field carries annotations (a #(doc), a #(classification), a render tag) that the stored copy cannot attach to it, so it is answered live.";

/** Why a query reading a timestamp truncation in a zone it was not built in is declined. */
export const ZONED_TRUNCATION_REASON =
   "the query reads a timestamp truncation stored in another zone than the one it reads it in";

/** Why a query reading a zone-dependent column in a zone it was not built in is declined. */
const ZONED_COLUMN_REASON =
   "the query reads a value stored in another time zone than the one it reads it in (a computed dimension or a timestamp truncation)";

/**
 * The query's SQL on the copy, compiled eagerly so a query the copy cannot
 * answer fails inside the router's attempt, not at run time past the
 * fallback, and the re-rooted tables it reads. A query that reads none would
 * only add work over live: declined, so the next candidate answers (a table
 * on the queried source's chain that is not re-rooted carries a package
 * warning saying why).
 */
export async function readsRerootedTables(
   attempt: AnswerAttempt,
): Promise<{ sql: string; tablesRead: Binding[] }> {
   const { compiled, runnable, virtualMap, givens, compiling } = attempt;
   const sql = await compiling(() =>
      runnable.getSQL({ virtualMap, givens: givens as never }),
   );
   const tablesRead = compiled.rerooted.filter((b) =>
      sqlReadsTable(sql, boundTablePath(b)),
   );
   if (tablesRead.length === 0) {
      const asWritten = leftAsWritten(compiled);
      attempt.decline(
         "the query reads no re-rooted table",
         "served_copy_not_served",
         asWritten?.detail,
         asWritten?.tableIds,
      );
   }
   return { sql, tablesRead };
}

/**
 * The database tables whose sources the copy left as written, each for a
 * problem the package warns about (`served-copy-not-served`), and why in the
 * caller's terms: what a decline for a query reading none of the re-rooted
 * tables names, so the path report says why the queried source's own table
 * did not answer rather than nothing. The report names it only against a
 * table on the queried chain. The reason codes alone, never a column or a
 * compiler message, since the tables may be other sources'.
 */
export function leftAsWritten(
   compiled: Pick<Compiled, "over" | "rerooted" | "problems">,
): { tableIds: string[]; detail: string } | undefined {
   const rerooted = new Set(compiled.rerooted.map((b) => b.virtualHandle));
   const tableIds: string[] = [];
   const reasons = new Set<string>();
   for (const b of compiled.over) {
      if (b.tier !== "database" || rerooted.has(b.virtualHandle)) continue;
      const problem = compiled.problems.find(
         (p) => p.sourceID !== undefined && b.sourceIDs?.includes(p.sourceID),
      );
      if (!problem) continue;
      tableIds.push(b.virtualHandle);
      reasons.add(problem.reason);
   }
   if (tableIds.length === 0) return undefined;
   return {
      tableIds,
      detail: `the served copy leaves the table's source as written (${[...reasons].sort().join(", ")}), as the package's served-copy-not-served warning says`,
   };
}

/**
 * The joins an answer reads as a record of stored columns (an inlined
 * join_one, or a pair table's join_many read flat), with each one's
 * annotated fields: an aggregate at a record join's grain would be computed
 * per row of the source, which live's join answers instead.
 */
export function recordJoinsRead(
   compiled: Compiled,
   tablesRead: readonly Binding[],
   pairsRead: NonNullable<Compiled["pairs"]>,
): {
   inlined: Set<string>;
   annotated: Map<string, ReadonlySet<string>>;
} {
   const inlined = new Set<string>();
   const annotated = new Map<string, ReadonlySet<string>>();
   for (const b of tablesRead) {
      for (const [join, fields] of compiled.inlinedJoins?.get(b.tablePath) ??
         []) {
         inlined.add(join);
         annotated.set(join, fields);
      }
   }
   for (const p of pairsRead)
      annotated.set(p.pair.join, p.annotatedChildFields);
   return { inlined, annotated };
}

/**
 * A backstop under every answer: the source the query runs on, and each one
 * on its chain the author's model declares, must compute in the same zone in
 * the copy as in the author's model (their `queryTimezone`, which Malloy
 * gives a query's first stage). A re-root that dropped a zone (one the source
 * inherits from the base it replaces) would compute every query on it in
 * another zone than live: declined, never served. Fails closed on a source
 * that cannot be resolved.
 */
export async function checkSourceZone(attempt: AnswerAttempt): Promise<void> {
   const { compiled, decline, prepared } = attempt;
   if (compiled.authorModelDef === undefined) return;
   let differs: boolean;
   try {
      differs = sourceZoneDiffers(compiled, (await prepared()).structRef);
   } catch {
      differs = true;
   }
   if (differs) decline(SOURCE_ZONE_DECLINE.reason, SOURCE_ZONE_DECLINE.code);
}

/** {@link checkSourceZone}'s decline. */
export const SOURCE_ZONE_DECLINE: SourceDecline = {
   reason:
      "the source the query runs on computes in another time zone in the served copy than in the author's model",
   code: "served_copy_time_zone",
};

/**
 * {@link checkSourceZone}'s test for the source `structRef` names (a name,
 * or a struct written inline): whether it, or anything on its chain the
 * author's model declares, computes in another zone in the copy. Pure: the
 * served copy records it per named source at its compile.
 */
export function sourceZoneDiffers(
   compiled: Compiled,
   structRef: unknown,
): boolean {
   const author = compiled.authorModelDef;
   if (author === undefined) return false;
   const zoneOf = (def: unknown) => {
      const z = (def as { queryTimezone?: unknown } | undefined)?.queryTimezone;
      return typeof z === "string" ? z : undefined;
   };
   try {
      const struct = resolveStructRefSource<unknown>(
         structRef as never,
         compiled.modelDef as never,
      );
      if (struct === undefined) return true;
      const served = SourceMap.of(compiled.modelDef);
      const authored = SourceMap.of(author);
      const name = structRefName(structRef);
      const authorStruct =
         name !== undefined
            ? resolveStructRefSource<unknown>(name as never, author as never)
            : undefined;
      return (
         (authorStruct !== undefined &&
            !sameZone(zoneOf(struct), zoneOf(authorStruct))) ||
         served.chainOf(struct).some((id) => {
            const a = authored.node(id);
            return (
               a !== undefined &&
               !sameZone(zoneOf(served.node(id)?.def), zoneOf(a.def))
            );
         })
      );
   } catch {
      return true;
   }
}

/**
 * The zones a query reads its stored tables in: `read`, the zone its first
 * stage computes in (its own `timezone:`, which overrides the source's, else
 * the zone the source it runs on sets or inherits from anything on its chain,
 * `ny is orders extend { timezone: … }`, which its text never names, else the
 * default); `set`, every zone its text sets, a later stage's or a nest's
 * included; and `chain`, the `sourceID`s the source derives from. A joined
 * source's own `timezone:` is not applied through the join, so a table read
 * through one is read in `read` too. `stage`: the zone the query's first
 * stage computes in on the served copy, as Malloy resolves it (its own
 * `timezone:`, else the zone the source it runs on carries, inherited
 * through each `extend`), only when that source is not query-shaped (whose
 * stage would read the tables in its own query's zone instead). Undefined
 * when the source cannot be resolved.
 */
interface ReadZones {
   read: string | undefined;
   set: string[];
   chain: ReadonlySet<string>;
   stage?: { zone: string | undefined };
}

export function readZonesOf(
   compiled: Compiled,
   view: Pick<PreparedQueryView, "query" | "structRef" | "pipeline">,
): ReadZones | undefined {
   try {
      const struct = resolveStructRefSource<unknown>(
         view.structRef as never,
         compiled.modelDef as never,
      );
      if (struct === undefined) return undefined;
      const map = SourceMap.of(compiled.modelDef);
      const chain = map.chainOf(struct);
      const first = (
         Array.isArray(view.pipeline) ? view.pipeline[0] : undefined
      ) as { queryTimezone?: unknown } | undefined;
      const own =
         typeof first?.queryTimezone === "string"
            ? first.queryTimezone
            : undefined;
      // The author's source of the same name keeps a zone it inherits, which
      // its re-root in the copy can drop.
      const name = structRefName(view.structRef);
      const author =
         name !== undefined && compiled.authorModelDef !== undefined
            ? resolveStructRefSource<unknown>(
                 name as never,
                 compiled.authorModelDef as never,
              )
            : undefined;
      return {
         read:
            own ??
            chainZoneOf(map, struct) ??
            (author !== undefined
               ? chainZoneOf(SourceMap.of(compiled.authorModelDef), author)
               : undefined),
         set: queryTimezones(view.query),
         chain: new Set(chain),
         ...((struct as { query?: unknown }).query === undefined
            ? {
                 stage: {
                    zone:
                       own ??
                       queryTimezoneOf({
                          queryTimezone: (struct as { queryTimezone?: unknown })
                             .queryTimezone,
                       }),
                 },
              }
            : {}),
      };
   } catch {
      return undefined;
   }
}

/**
 * The zone a source computes in: its own, or the one it inherits from
 * anything on its chain (`ny is orders extend { timezone: … }`).
 */
export function chainZoneOf(
   map: SourceMap,
   struct: unknown,
): string | undefined {
   return (
      queryTimezoneOf(struct) ??
      map
         .chainOf(struct)
         .map((id) => queryTimezoneOf(map.node(id)?.def))
         .find((z) => z !== undefined)
   );
}

/**
 * The zone of the base a query-shaped source's query runs on when that base
 * is written inline (`ev extend { timezone: … } -> { … }`): it has no
 * `sourceID` of its own, so {@link chainZoneOf} never reaches it, yet the
 * query's stage computes in it.
 */
export function inlineBaseZoneOf(def: unknown): string | undefined {
   const base = (def as { query?: { structRef?: unknown } } | undefined)?.query
      ?.structRef;
   return base !== null && typeof base === "object"
      ? queryTimezoneOf(base)
      : undefined;
}

/**
 * Whether a query whose zones are `zones` ({@link readZonesOf}) reads `table`
 * in another time zone than the one it was built in (`builtZones`);
 * an explicit `'UTC'` is the default zone ({@link sameZone}). One rule
 * for every zone-dependent stored value: a computed dimension, a rolling
 * window's cut-off, and a restated timestamp truncation. Fails closed on a
 * source that cannot be resolved, and on a table whose stored source is not
 * known.
 */
export function readsInAnotherZone(
   compiled: Compiled,
   zones: ReadZones | undefined,
   table: Binding,
): boolean {
   const id = table.sourceIDs?.[0];
   if (
      zones === undefined ||
      id === undefined ||
      !compiled.builtZones?.has(id)
   ) {
      return true;
   }
   const built = compiled.builtZones.get(id);
   return (
      !sameZone(zones.read, built) || zones.set.some((z) => !sameZone(z, built))
   );
}

/**
 * `#@ -persist` means recompute rather than read the pre-built table. In
 * the served copy every name reads its re-rooted base, so a query whose
 * source is an opted-out reader, or derives from one, is declined here
 * and answered by the next candidate (which never carries one).
 * A source whose served surface the load-time classification checks
 * flag is declined the same way: answered live, or refused there.
 */
export async function checkOptOut(attempt: AnswerAttempt): Promise<void> {
   const { compiled, decline, compiling, prepared } = attempt;
   if (readsOptOuts(compiled)) {
      const { structRef } = await compiling(prepared);
      const d = optOutDeclineOf(compiled, structRef);
      if (d) decline(d.reason, d.code);
   }
}

/** Whether the copy holds any opted-out or unsafe source {@link checkOptOut} reads. */
export function readsOptOuts(compiled: Compiled): boolean {
   return (
      (compiled.optOutIDs !== undefined && compiled.optOutIDs.size > 0) ||
      (compiled.unsafeIDs !== undefined && compiled.unsafeIDs.size > 0)
   );
}

/**
 * {@link checkOptOut}'s decline for the source `structRef` names, or
 * undefined. Pure: the served copy records it per named source at its
 * compile.
 */
export function optOutDeclineOf(
   compiled: Compiled,
   structRef: unknown,
): SourceDecline | undefined {
   const struct = resolveStructRefSource<unknown>(
      structRef as never,
      compiled.modelDef as never,
   );
   const chain = SourceMap.of(compiled.modelDef).chainOf(struct);
   if (chain.some((id) => compiled.unsafeIDs?.has(id))) {
      return {
         reason:
            "the query reads a source whose served copy exposes a field the compile of record does not classify or carry",
         code: "served_copy_not_served",
      };
   }
   if (chain.some((id) => compiled.optOutIDs?.has(id))) {
      return {
         reason: "the query reads a source that opts out of persist",
         code: "served_copy_opt_out",
      };
   }
   return undefined;
}

/**
 * The checks for a join the copy answers from a record of stored columns (an
 * inlined join_one, or a pair table's join_many read flat), on one query
 * compiled over the copy: an aggregate at such a join's grain is declined
 * (`count_semantics`), and so is a query that returns one of its annotated
 * fields as a column (`joined_annotations`). `inlined` are the inlined
 * join_ones the query's tables answer from records, `annotated` each record
 * join's annotated fields; `modelDef` resolves the query's source.
 */
export async function checkRecordJoins(args: {
   inlined: ReadonlySet<string>;
   annotated: ReadonlyMap<string, ReadonlySet<string>>;
   prepared: () => ReturnType<typeof readPreparedQuery>;
   modelDef: unknown;
   queryString: string;
   decline: (
      reason: string,
      code: ServedCopyDeclineCode,
      callerDetail?: string,
   ) => never;
}): Promise<void> {
   const { inlined, annotated, prepared, decline } = args;
   if (inlined.size > 0) {
      const grain = joinGrainAggregatePaths(await prepared());
      if (
         grain.unreadable ||
         grain.paths.some((p) => p.some((name) => inlined.has(name))) ||
         readsJoinStar(args.queryString, inlined)
      ) {
         decline(
            "the query aggregates at the grain of a join the served copy answers from stored columns",
            "served_copy_count_semantics",
            JOIN_GRAIN_DETAIL,
         );
      }
   }
   // A record literal's field carries no annotation onto a result, so a
   // query that returns an annotated joined field as a column (its
   // `#(doc)`, `#(classification)`, render tags) is answered live, and
   // every other query keeps the table: one check for both legs, before
   // the pair table's own.
   if ([...annotated.values()].some((f) => f.size > 0)) {
      const { structRef, pipeline } = await prepared();
      const struct = resolveStructRefSource<{ fields?: unknown }>(
         structRef,
         args.modelDef as never,
      );
      const out = annotatedJoinedOutput(pipeline, struct?.fields, annotated);
      if (out.unreadable || out.output) {
         decline(
            out.output
               ? `the query returns ${out.output.path.join(".")}, whose annotations the served copy's record of stored columns cannot carry`
               : "the query's output columns cannot be read",
            "served_copy_joined_annotations",
            JOINED_ANNOTATIONS_DETAIL,
         );
      }
   }
}

/**
 * Whether a query reads `<join>.*` through any of `joins`: a star expands to
 * the joined source's fields, which a record of its stored fields may not all
 * be. Unlexable text counts.
 */
function readsJoinStar(
   queryString: string,
   joins: ReadonlySet<string>,
): boolean {
   const tokens = lexAll(queryString);
   if (!tokens) return true;
   return tokens.some(
      (t, i) =>
         joins.has(t.text.startsWith("`") ? t.text.slice(1, -1) : t.text) &&
         tokens[i + 1]?.text === "." &&
         tokens[i + 2]?.text === "*",
   );
}

/**
 * A query that reads a table's zone-dependent column (a stored computed
 * dimension, a restated timestamp truncation) in another zone than the table
 * was built in ({@link readsInAnotherZone}) is answered live. Decided per
 * column: a query that reads none of them stays on the table, whatever zone it
 * reads in. A column of the table the query runs on is read by a field path of
 * one name; a joined table's, by a longer path ending in its name (an
 * unrelated field of the same name is declined too, never served wrong).
 *
 * The entry point's row-level gate reads columns too, and is grafted on this
 * compile after the check, in the query's zone: a gate over a stored
 * truncation (`dday = $ON_DAY`) would compare the value truncated again in
 * the caller's zone, which can be the next day's, so it admits another
 * partition's rows. Every name the gates read, and every field those are
 * computed from ({@link gateReadNames}), is a read here, on any table.
 * Fails closed on a query whose field reads, or whose gates, cannot be
 * listed.
 */
export async function checkZonedColumns(
   attempt: AnswerAttempt,
   tablesRead: readonly Binding[],
): Promise<void> {
   const { compiled, prepared } = attempt;
   const zonedOf = (b: Binding) =>
      compiled.zonedColumns?.get(b.sourceIDs?.[0] ?? "");
   const stagedOf = (b: Binding) =>
      compiled.stageZonedTruncations?.get(b.sourceIDs?.[0] ?? "");
   const zoned = tablesRead.filter((b) => zonedOf(b) !== undefined);
   const staged = tablesRead.filter((b) => stagedOf(b) !== undefined);
   if (zoned.length === 0 && staged.length === 0) return;
   const view = await prepared();
   const zones = readZonesOf(compiled, view);
   // A query-shaped re-root's zoned truncation is truncated again in the
   // zone the query's first stage computes in: the identity only in the one
   // its query truncated it in, and only when every zone the query sets is
   // that one too.
   const stagedElsewhere = staged.flatMap((b) => {
      const { zone, names } = stagedOf(b)!;
      return zones?.stage === undefined ||
         !sameZone(zones.stage.zone, zone) ||
         zones.set.some((z) => !sameZone(z, zone))
         ? [{ binding: b, names }]
         : [];
   });
   const elsewhere = zoned
      .filter((b) => readsInAnotherZone(compiled, zones, b))
      .map((b) => ({ binding: b, names: zonedOf(b) ?? new Set<string>() }));
   await declineZonedReads(attempt, view, zones, [
      ...elsewhere,
      ...stagedElsewhere,
   ]);
}

/**
 * Declines a query that reads, or whose gates read, any of `elsewhere`'s
 * columns: each a table's zone-dependent columns read in another zone than
 * it was built in ({@link checkZonedColumns}).
 */
async function declineZonedReads(
   attempt: AnswerAttempt,
   view: Awaited<ReturnType<AnswerAttempt["prepared"]>>,
   zones: ReadZones | undefined,
   elsewhere: readonly {
      binding: Binding;
      names: ReadonlySet<string>;
   }[],
): Promise<void> {
   const { compiled, decline } = attempt;
   if (elsewhere.length === 0) return;
   const usage = queryFieldUsagePaths(view);
   if (usage === undefined) {
      decline(ZONED_COLUMN_REASON, "served_copy_time_zone");
   }
   const gates = gateNamesOf(compiled, view);
   if (gates === undefined) {
      decline(ZONED_COLUMN_REASON, "served_copy_time_zone");
   }
   for (const { names } of elsewhere) {
      if ([...gates!].some((n) => readsColumn(names, n))) {
         decline(ZONED_COLUMN_REASON, "served_copy_time_zone");
      }
   }
   // A table reached only through one of the query's source's joins is read
   // by a path through that join, never by a field path of one name. Any
   // other table may be the query's own (an extension of a table-backed or
   // query-shaped source names no base to tell), so any path may read it.
   const joinedOnly = joinedOnlyIDs(compiled, view, zones);
   for (const { binding: b, names } of elsewhere) {
      const joined = joinedOnly.has(b.sourceIDs?.[0] ?? "");
      if (
         usage!.some(
            (p) =>
               (p.length > 1 || !joined) && readsColumn(names, p[p.length - 1]),
         )
      ) {
         decline(ZONED_COLUMN_REASON, "served_copy_time_zone");
      }
   }
}

/**
 * A row-level gate that reads a column the copy leaves out of a table's type
 * (a truncation stored in another zone than any read's, a type no read gives
 * exactly) cannot be grafted here, and the graft would deny every caller where
 * live answers: the query is declined and live answers it under the author's
 * gate. Fails closed on gates that cannot be read.
 */
export async function checkGateColumnsKept(
   attempt: AnswerAttempt,
   tablesRead: readonly Binding[],
): Promise<void> {
   const { compiled, decline, prepared } = attempt;
   const leftOut = new Set(
      tablesRead.flatMap(
         (b) => compiled.leftOutColumns?.get(b.sourceIDs?.[0] ?? "") ?? [],
      ),
   );
   if (leftOut.size === 0) return;
   const gates = gateNamesOf(compiled, await prepared());
   const zoned = new Set(
      tablesRead.flatMap((b) => [
         ...(compiled.zonedLeftOut?.get(b.sourceIDs?.[0] ?? "") ?? []),
      ]),
   );
   const read =
      gates === undefined
         ? [...leftOut]
         : [...leftOut].filter((n) => gates.has(n));
   if (read.length === 0) return;
   if (read.every((n) => zoned.has(n))) {
      decline(ZONED_TRUNCATION_REASON, "served_copy_time_zone");
   }
   decline(
      "a row-level gate reads a column the served copy leaves out of a stored table's type",
      "served_copy_not_served",
   );
}

/**
 * Every name the queried source's entry-point gates read on the served copy
 * ({@link gateReadNames}), or undefined when its gates cannot be read.
 */
function gateNamesOf(
   compiled: Compiled,
   view: Pick<PreparedQueryView, "structRef">,
): Set<string> | undefined {
   let struct: unknown;
   try {
      struct = resolveStructRefSource(
         view.structRef as never,
         compiled.modelDef as never,
      );
   } catch {
      return undefined;
   }
   const exprs = entryPointGateExprs(struct, compiled.modelDef);
   return exprs === undefined
      ? undefined
      : gateReadNames(struct, exprs, compiled.modelDef);
}

/**
 * The `sourceID`s the query's source reaches only through one of its own
 * joins: on a joined source's chain, and not on its own.
 */
function joinedOnlyIDs(
   compiled: Compiled,
   view: Pick<PreparedQueryView, "structRef">,
   zones: ReadZones | undefined,
): Set<string> {
   const out = new Set<string>();
   if (zones === undefined) return out;
   try {
      const struct = resolveStructRefSource<{ fields?: unknown[] }>(
         view.structRef as never,
         compiled.modelDef as never,
      );
      const map = SourceMap.of(compiled.modelDef);
      for (const f of struct?.fields ?? []) {
         if ((f as { join?: unknown }).join === undefined) continue;
         for (const id of map.chainOf(f)) {
            if (!zones.chain.has(id)) out.add(id);
         }
      }
   } catch {
      out.clear();
   }
   return out;
}

/**
 * Whether a field path's last name reads one of `names`: the column itself,
 * or a restated truncation's stored column under the internal name the
 * served copy renames it to (`<name>__stored`, `__stored_2`, …), which a
 * wildcard reads.
 */
function readsColumn(
   names: ReadonlySet<string>,
   field: string | undefined,
): boolean {
   if (field === undefined) return false;
   return [...names].some((n) => {
      const internal = `${n}__stored`;
      return (
         field === n ||
         field === internal ||
         (field.startsWith(`${internal}_`) &&
            /^\d+$/.test(field.slice(internal.length + 1)))
      );
   });
}
