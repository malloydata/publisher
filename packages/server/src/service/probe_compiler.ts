// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The probe compiler (materialization v2, the shared pieces): small generated
 * queries COMPILED against an author's model, never run, to ask the compiler
 * what an expression depends on that a caller sets. Three questions:
 *
 *  - which givens it reads (`_query.givenUsage`, read through the one reader
 *    in `governance/ir/prepared_query.ts`, and Malloy's public
 *    `PreparedQuery.givens`): transitive through the dimensions it reads and
 *    their defaults (Evidence: given-reading dimensions detectable);
 *  - whether it reads the current time: the generated SQL is scanned for the
 *    dialects' current-time expressions, counted against a baseline probe of
 *    the same source, so a `where:` or base relation that reads the clock
 *    does not mark every field;
 *  - whether it depends on the query's time zone: compiled with no
 *    `timezone:` and at two zones ({@link PROBE_TIME_ZONES}), and the SQL
 *    compared (Evidence: `hour()`, `.month` and `::date` change; `upper()` and
 *    a raw timestamp do not). Malloy writes a query's zone into the SQL and
 *    the connections run their sessions in UTC, so equal SQL is an equal value.
 *
 * Compiled, never run: a probe only calls `loadQuery(…).getPreparedQuery()`
 * and reads `preparedResult.sql`. On the package-load worker the connection's
 * `runSQL` throws, so a probe that tried to run would fail loudly.
 *
 * Every answer fails closed for the caller that stores values: a probe that
 * does not compile, or a field it cannot select (a `private` field, which
 * only its own source reads), is reported `unprobed`, and the caller falls
 * back to the IR walks ({@link readsAGiven} here, `currentTimeFields` in
 * `materialization/plan/persist_dynamic_terms.ts`), which also run alongside
 * every probe so either signal marks a field.
 *
 * The other readers of "which givens does this read" are consolidated here as
 * far as their contracts allow: the IR walk {@link readsAGiven} (v1
 * persistence's position rule and P9 both use it) and the prepared query's
 * `givenUsage` slot (the governor, report tiles and these probes). Two stay
 * where they are, because they answer a gate's question rather than a
 * field's: `gate_dimension.ts`'s transitive `refSummary` expansion, read
 * against a gate's compiled condition, and `authorize.ts`'s gate probes,
 * which lift the condition itself.
 *
 * Runs on the package-load worker, inside the build plan: it imports only
 * `./malloy_text` and `./governance/ir/prepared_query`, neither of which
 * imports `service/model.ts`, so governance code may call it too.
 */

import { quotedIdentifier, quotedPath, stringLiteral } from "./malloy_text";
import { queryGivenUsageIds } from "./governance/ir/prepared_query";

/** What a probe compiles against: a model's materializer, or one like it. */
export interface ProbeCompiler {
   loadQuery(query: string): { getPreparedQuery(): Promise<unknown> };
}

/**
 * The two zones a time-zone probe compiles at, besides none. Both differ from
 * UTC (the zone a query without one is read in), and from each other, one of
 * them by a half hour, so an expression that reads the zone at all shows.
 */
const PROBE_TIME_ZONES = ["America/Los_Angeles", "Asia/Kolkata"];

/**
 * What one field's value depends on that the caller sets, as far as the probe
 * could tell. Each signal is undefined when the probe could not answer it, and
 * a caller then falls back to the IR walks (givens, current time) or assumes
 * the worst (time zone): a field it cannot clear is never treated as clear.
 *
 * Read relative to the source: a given the source's own `where:` reads, or a
 * clock or zone read its base relation makes, shows in every probe of it, so
 * it is not attributed to the field. A field that reads the same given as the
 * source's own `where:` is found by the IR walk, which runs beside every probe.
 */
export interface FieldDependence {
   /** The given ids it reads beyond the source's own reads; empty for none. */
   givens?: string[];
   currentTime?: boolean;
   timeZone?: boolean;
   /** Why a signal is missing, when one is. */
   unprobed?: string;
}

/** Whether a probe answered every signal for a field and found none set. */
export function callerIndependent(d: FieldDependence | undefined): boolean {
   return (
      d !== undefined &&
      d.givens !== undefined &&
      d.givens.length === 0 &&
      d.currentTime === false &&
      d.timeZone === false
   );
}

/**
 * The dialects' current-time expressions, as Malloy writes `now` (`LOCALTIMESTAMP`,
 * `CURRENT_TIMESTAMP()`, `SYSUTCDATETIME()`) and as an author's raw SQL might
 * (`now()`, `current_date`, `getdate()`). Over-matching is safe: it only
 * keeps a value out of a stored table.
 */
const CURRENT_TIME_SQL =
   /\b(?:current_timestamp|current_date|current_time|current_datetime|localtimestamp|localtime|sysdate|sysdatetime|sysutcdatetime|sysdatetimeoffset|getdate|getutcdate|transaction_timestamp|statement_timestamp|clock_timestamp|today|unix_timestamp|now)\b/gi;

/** How many current-time expressions a SQL text holds. */
export function currentTimeCalls(sql: string): number {
   return (sql.match(CURRENT_TIME_SQL) ?? []).length;
}

/** Max IR depth to walk; deep enough for real sources, a hang backstop. */
const MAX_WALK_DEPTH = 200;

/**
 * Whether a slice of compiled IR reads a given, by EITHER a `{node:'given'}`
 * reference or a non-empty `givenUsage` summary, so a compiler that prunes one
 * of the two signals cannot make a given read clean. Throws past a depth
 * bound, which every caller treats as a read.
 */
export function readsAGiven(
   node: unknown,
   seen: WeakSet<object> = new WeakSet(),
   depth = 0,
): boolean {
   if (depth > MAX_WALK_DEPTH) {
      throw new Error("given walk exceeded max depth");
   }
   if (node === null || typeof node !== "object") return false;
   if (seen.has(node as object)) return false;
   seen.add(node as object);

   if (Array.isArray(node)) {
      return node.some((item) => readsAGiven(item, seen, depth + 1));
   }
   const record = node as Record<string, unknown>;
   if (record.node === "given" || record.node === "givenReference") return true;
   if (record.givenRef !== undefined) return true;
   const summary = record.refSummary as { givenUsage?: unknown[] } | undefined;
   if (summary?.givenUsage && summary.givenUsage.length > 0) return true;
   if (Array.isArray(record.givenUsage) && record.givenUsage.length > 0) {
      return true;
   }
   return Object.values(record).some((v) => readsAGiven(v, seen, depth + 1));
}

/** One compiled probe: the givens it reads and, when it generates, its SQL. */
interface Probe {
   givens: string[];
   /** Undefined when the SQL could not be generated (a given with no value). */
   sql?: string;
}

/**
 * The probe text: `source` extended with one dimension per field, each the
 * field read as written, then selected. Reading a field through the
 * extension rather than selecting it lets an `internal` field be probed too.
 */
function probeText(
   source: string,
   fields: readonly string[],
   names: readonly string[],
   zone: string | undefined,
): string {
   const dims = fields
      .map((f, i) => `${quotedIdentifier(names[i])} is ${quotedPath(f)}`)
      .join(", ");
   const selected = names.map((n) => quotedIdentifier(n)).join(", ");
   const tz = zone === undefined ? "" : `timezone: ${stringLiteral(zone)}; `;
   return (
      `run: ${quotedIdentifier(source)} extend { dimension: ${dims} } ` +
      `-> { ${tz}select: ${selected} }`
   );
}

async function compileProbe(
   compiler: ProbeCompiler,
   text: string,
): Promise<Probe> {
   const prepared = (await compiler.loadQuery(text).getPreparedQuery()) as {
      givens?: ReadonlyMap<string, unknown>;
      preparedResult?: { sql?: unknown };
   };
   const ids = queryGivenUsageIds(prepared);
   if (ids === undefined) {
      throw new Error("the probe's compiled query carries no given usage");
   }
   // Malloy's public list (by name) too, should the id list ever come back
   // empty while it does not: either one naming a given marks the read.
   const surfaced = [...(prepared.givens?.keys() ?? [])];
   const givens = ids.length > 0 ? ids : surfaced;
   let sql: string | undefined;
   try {
      const generated = prepared.preparedResult?.sql;
      sql = typeof generated === "string" ? generated : undefined;
   } catch {
      // A given with no default has no value at compile time; the field
      // reads a given, which already decides it.
      sql = undefined;
   }
   return { givens, sql };
}

/** What a source's rows, and each of its probed fields, depend on (P9). */
interface SourceDependence {
   /**
    * Whether the source with no field selected compiles to the same SQL at
    * every probe zone as at none, so the rows it holds (its `where:` terms,
    * its base relation) do not depend on the query time zone. False when it
    * moves with the zone, or when the probe could not tell.
    */
   rowsZoneFree: boolean;
   fields: Map<string, FieldDependence>;
}

/**
 * What each of `fields` of `source` (a source's name in the model `compiler`
 * compiled; a field is a name or a dotted path through a join) depends on that
 * a caller sets, beyond what the source itself reads. Every field is
 * answered; a signal the probe could not settle is left undefined (see
 * {@link FieldDependence}).
 *
 * Also whether the source's own rows depend on the query time zone: a
 * `where: ts > @2026-03-08 09:00` filters a different set of rows in every
 * zone, so a table built in the default zone holds the wrong rows for a query
 * that sets another.
 *
 * A baseline probe (a constant selected from the source) reads what the
 * source reads with no field: its `where:` givens, its clock reads, and
 * whether its SQL moves with the zone. One probe of every field at once then
 * settles a source whose fields add nothing; otherwise each field is probed
 * alone.
 */
export async function probeSourceDependence(
   compiler: ProbeCompiler,
   source: string,
   fields: readonly string[],
   reservedNames: ReadonlySet<string> = new Set(),
): Promise<SourceDependence> {
   const out = new Map<string, FieldDependence>();
   const nameFor = probeNames(reservedNames);
   const at = (
      of: readonly string[] | undefined,
      zone: string | undefined,
   ): Promise<Probe> =>
      compileProbe(
         compiler,
         of === undefined
            ? baselineText(source, nameFor(0), zone)
            : probeText(
                 source,
                 of,
                 of.map((_, i) => nameFor(i)),
                 zone,
              ),
      );
   // The source with no field: what every probe of it reads anyway.
   let baseline: { givens: Set<string>; clock?: number; zoned?: boolean };
   try {
      const plain = await at(undefined, undefined);
      let zoned: boolean | undefined = false;
      for (const zone of PROBE_TIME_ZONES) {
         const z = await at(undefined, zone);
         if (z.sql === undefined || plain.sql === undefined) zoned = undefined;
         else if (z.sql !== plain.sql && zoned === false) zoned = true;
      }
      baseline = {
         givens: new Set(plain.givens),
         ...(plain.sql !== undefined
            ? { clock: currentTimeCalls(plain.sql) }
            : {}),
         zoned,
      };
   } catch (error) {
      const why = `the source's baseline probe did not compile: ${messageOf(error)}`;
      for (const f of fields) out.set(f, { unprobed: why });
      return { rowsZoneFree: false, fields: out };
   }
   const rowsZoneFree = baseline.zoned === false;
   if (fields.length === 0) return { rowsZoneFree, fields: out };
   const probe = async (of: readonly string[]): Promise<FieldDependence> => {
      const plain = await at(of, undefined);
      const givens = plain.givens.filter((g) => !baseline.givens.has(g));
      if (givens.length > 0) return { givens };
      const out: FieldDependence = { givens: [] };
      if (plain.sql === undefined || baseline.clock === undefined) {
         out.unprobed = "the probe's SQL could not be generated";
         return out;
      }
      out.currentTime = currentTimeCalls(plain.sql) > baseline.clock;
      // A base whose SQL moves with the zone moves every probe's: the field's
      // own dependence cannot be read off the whole text.
      if (baseline.zoned !== false) {
         out.unprobed = "the source's own SQL depends on the query time zone";
         return out;
      }
      let timeZone = false;
      for (const zone of PROBE_TIME_ZONES) {
         const zoned = await at(of, zone);
         if (zoned.sql === undefined) {
            out.unprobed = "the probe's SQL could not be generated";
            return out;
         }
         if (zoned.sql !== plain.sql) timeZone = true;
      }
      out.timeZone = timeZone;
      return out;
   };
   const all = await probe(fields).catch(
      (error): FieldDependence => ({ unprobed: messageOf(error) }),
   );
   if (callerIndependent(all)) {
      for (const f of fields) out.set(f, all);
      return { rowsZoneFree, fields: out };
   }
   for (const f of fields) {
      out.set(
         f,
         await probe([f]).catch(
            (error): FieldDependence => ({ unprobed: messageOf(error) }),
         ),
      );
   }
   return { rowsZoneFree, fields: out };
}

/** The baseline probe: a constant selected from the source. */
function baselineText(
   source: string,
   name: string,
   zone: string | undefined,
): string {
   const tz = zone === undefined ? "" : `timezone: ${stringLiteral(zone)}; `;
   return `run: ${quotedIdentifier(source)} -> { ${tz}select: ${quotedIdentifier(name)} is 1 }`;
}

/** Probe field names that no field of the source spells. */
function probeNames(reserved: ReadonlySet<string>): (i: number) => string {
   let prefix = "__probe";
   while ([...reserved].some((n) => n.startsWith(prefix))) prefix += "_";
   return (i) => `${prefix}${i}`;
}

function messageOf(error: unknown): string {
   return error instanceof Error ? error.message : String(error);
}
