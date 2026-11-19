// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Whether a gate or a `where:` that a stored copy re-applies compares a string
 * column there as live compares it. The caller's own query filters are judged
 * by the same rules, read as `GateReads` of their own
 * (`queryFilterReadsOfQuery` in `gate_reads.ts`).
 *
 * A `storage=` table is read in a DuckLake destination, whose string
 * comparison is byte-exact. Live compares the source column under its own
 * collation: case-insensitive under a MySQL `*_ci`, a SQL Server CI, a
 * Postgres nondeterministic ICU or a DuckDB `NOCASE` collation, so
 * `region != 'east'` hides `East` live and shows it from the copy. Ordering
 * (`<`, a pattern, a range, a function's value) differs under any collation
 * that is not binary, a default Postgres `en_US.UTF-8` database included.
 *
 * So, for each string column a gate or a re-applied `where:` compares:
 *
 * - `binary` (byte-exact, every operator): BigQuery; a DuckDB-dialect source
 *   whose instance has no `default_collation` and that names no `COLLATE` in
 *   its SQL or in any table or view it can read; a Postgres column whose
 *   collation is `C` or `POSIX`;
 * - `deterministic` (equality is byte-exact, ordering is not): any other
 *   deterministic Postgres collation, the database default included;
 * - `unknown`: everything else, every other dialect, and any probe that fails.
 *
 * An equality-only comparison holds on the copy under `binary` or
 * `deterministic`; anything else only under `binary`. Fail closed: what this
 * cannot read is `unknown`, and the answer is then not kept (live answers).
 *
 * A database table in Postgres is judged against its source instead: it sits
 * in the source's own warehouse, and a build's `CREATE TABLE AS` keeps each
 * column's collation, so it is kept when every column the gates and scopes
 * read has the same resolved collation stored as live (by provider and
 * locale, never by name), whatever the operator. A column altered after the
 * build to another collation answers live.
 *
 * When it is read: the source side once per compiled model (a reload or a
 * rebind compiles a new one, which reads it again), and for a DuckDB
 * instance once per query SQL, so a source altered in place is read again
 * only after a reload; the stored side on every answer, since a stored
 * table is the one place altered after Publisher built it.
 */

import {
   type Connection,
   EMPTY_BUILD_MANIFEST,
   type ModelDef,
   type ModelMaterializer,
   type QueryMaterializer,
} from "@malloydata/malloy";
import { malloyIdentifiers } from "../../query_text";
import {
   readPreparedQuery,
   resolveStructRefSource,
} from "../../governance/ir/prepared_query";
import { identifier } from "../../malloy_text";
import { capabilitiesOf } from "../domain/dialect";

import type { CollationProbe, GateReads } from "./gate_reads_types";
import { fieldPathsIn } from "./gate_paths";
import { entryGates } from "./gate_reads";

type Collation = "binary" | "deterministic" | "unknown";

/** What the probe reads through: the author's compile and its connections. */
interface ProbeRuntime {
   connections: {
      lookupConnection(name?: string): Promise<Connection>;
   };
   _loadModelFromModelDef(modelDef: ModelDef): ModelMaterializer;
}

/** Each probe's answer, by model, then connection and column SQL. */
const probes = new WeakMap<object, Map<string, Promise<Collation>>>();

/**
 * Why an answer that read a `storage=` table past the entry point's gates
 * does not compare a string column as live does, or undefined when every
 * string column it compares does. No probe runs only when nothing the gates
 * and scopes compare is a string (`GateReads.comparesNoString`).
 */
export async function stringGateCollationRefusal(args: {
   authorRunnable: QueryMaterializer;
   reads: GateReads | undefined;
   /** The caller's givens: a probe of a source whose rows read one needs it. */
   givens?: Record<string, unknown>;
}): Promise<string | undefined> {
   const { reads } = args;
   // Nothing the gates and scopes compare is a string (an allow-list over
   // every leaf: a given, a literal, a cast or a function that could make one
   // reads false), so no collation can change what they admit.
   if (reads !== undefined && !reads.unresolved && reads.comparesNoString) {
      return undefined;
   }
   if (reads === undefined || reads.unresolved) return "gate reads unread";
   const need: Collation[] = reads.equalityOnly
      ? ["binary", "deterministic"]
      : ["binary"];
   let prepared: Awaited<ReturnType<typeof readPreparedQuery>>;
   try {
      prepared = await readPreparedQuery(args.authorRunnable);
   } catch {
      return "no author compile";
   }
   const modelDef = prepared.modelDef as ModelDef | undefined;
   const entryName =
      typeof prepared.structRef === "string" ? prepared.structRef : undefined;
   if (!modelDef) return "no author compile";
   const struct = resolveStructRefSource(
      prepared.structRef as never,
      modelDef as never,
   ) as { dialect?: unknown; connection?: unknown } | undefined;
   const named = struct?.dialect;
   const dialect = typeof named === "string" ? named : "";
   const connectionName =
      typeof struct?.connection === "string" ? struct.connection : undefined;
   const runtime = (
      args.authorRunnable as unknown as { runtime?: ProbeRuntime }
   ).runtime;
   if (!runtime) return "no runtime";
   const read = capabilitiesOf(dialect).stringCollation;
   if (read === "binary") return undefined;
   if (read === undefined) return "a dialect whose collation is not read";
   if (read === "instance") {
      // Instance-wide: the instance's setting, its tables and views, and the
      // whole query's SQL, so every column the gates and scopes read counts,
      // however far from the entry point.
      let sql: string;
      try {
         ({ sql } = await args.authorRunnable.getPreparedResult(
            (args.givens ? { givens: args.givens } : {}) as never,
         ));
      } catch {
         return "no author SQL";
      }
      const collation = await probeOnce(
         modelDef,
         `${connectionName}\u0000instance\u0000${sql}`,
         async () =>
            duckdbCollation(
               await runtime.connections.lookupConnection(connectionName),
               sql,
            ),
      );
      return collation === "binary"
         ? undefined
         : "an instance that compares strings under a collation";
   }
   // Per column: only a named entry point's own paths can be probed (a
   // caller's inline `extend` or `include` has no name to compile a probe
   // over; the instance-wide check above needs none).
   if (entryName === undefined) return "no named entry point";
   if (!reads.entryPathsComplete) return "a scope from elsewhere";
   // A comparison of a string the query makes (a given, a literal, a cast,
   // a `pick`) compares under the database's default collation, which must
   // then meet the comparison's need as a column's would.
   if (!reads.comparesNoString) {
      const fallback = await probeOnce(
         modelDef,
         `${connectionName}\u0000default`,
         async () =>
            postgresDefaultCollation(
               await runtime.connections.lookupConnection(connectionName),
            ),
      );
      if (!need.includes(fallback)) {
         return `a comparison under a ${fallback} default collation`;
      }
   }
   const paths = stringPaths(struct, reads.entryPaths);
   if (paths === undefined) return "an unreadable gate path";
   for (const path of paths) {
      const collation = await probeOnce(
         modelDef,
         `${connectionName}\u0000${path.join(".")}`,
         () =>
            probe({
               runtime,
               modelDef,
               entryName,
               path,
               dialect,
               connectionName,
               ...(args.givens ? { givens: args.givens } : {}),
            }),
      );
      if (!need.includes(collation)) {
         return `\`${path.join(".")}\` compares under a ${collation} collation`;
      }
   }
   return undefined;
}

/** The collation probes admission reads through (`serve/read_admission.ts`). */
export const collationProbe: CollationProbe = {
   sourceRefusal: stringGateCollationRefusal,
   storedRefusal: storedTableCollationRefusal,
};

function probeOnce(
   modelDef: object,
   key: string,
   run: () => Promise<Collation>,
): Promise<Collation> {
   let byKey = probes.get(modelDef);
   if (!byKey) {
      byKey = new Map();
      probes.set(modelDef, byKey);
   }
   let p = byKey.get(key);
   if (!p) {
      // Bounded: a model's distinct queries each hold one entry.
      if (byKey.size >= 1000) byKey.clear();
      p = run().catch(() => "unknown" as const);
      byKey.set(key, p);
   }
   return p;
}

/**
 * The entry-level paths that read a string field, or undefined when a path
 * cannot be followed (fail closed). A path through a join is followed into
 * the joined source's fields. A computed field compares the fields its
 * expression reads, so each of those must be a path here too (the walk
 * lists them), and a field of a query-shaped source, whose stage output may
 * be computed from fields no path names, fails closed unless it is a string.
 */
function stringPaths(
   struct: unknown,
   paths: readonly (readonly string[])[],
): string[][] | undefined {
   const out: string[][] = [];
   const seen = new Set<string>();
   for (const path of paths) {
      let at: unknown = struct;
      let owner: unknown = struct;
      let field: { type?: unknown; e?: unknown } | undefined;
      for (const name of path) {
         owner = at;
         field = (
            ((at as { fields?: unknown[] } | undefined)?.fields ?? []) as {
               name?: unknown;
               as?: unknown;
               type?: unknown;
               e?: unknown;
            }[]
         ).find((f) => String(f.as ?? f.name) === name);
         if (!field) return undefined;
         at = field;
      }
      if (field?.type !== "string") {
         if ((owner as { query?: unknown } | undefined)?.query !== undefined) {
            return undefined;
         }
         if (field?.e !== undefined) {
            const prefix = path.slice(0, -1);
            const listed = new Set(paths.map((p) => p.join(".")));
            const reads = fieldPathsIn(field.e);
            if (
               reads.length === 0 ||
               !reads.every((p) => listed.has([...prefix, ...p].join(".")))
            ) {
               return undefined;
            }
         }
         continue;
      }
      const key = path.join(".");
      if (seen.has(key)) continue;
      seen.add(key);
      out.push([...path]);
   }
   return out;
}

/**
 * The SQL that selects the field at `path` of the entry point, as
 * `probe_value`, through the author's compile: its column keeps the
 * collation the field compares under.
 */
async function probeSql(args: {
   runtime: ProbeRuntime;
   modelDef: ModelDef;
   entryName: string;
   path: readonly string[];
   givens?: Record<string, unknown>;
}): Promise<string> {
   const ref = args.path.map((p) => identifier(p)).join(".");
   const { sql } = await args.runtime
      ._loadModelFromModelDef(args.modelDef)
      .loadQuery(
         // An extension reads an `internal` field, as a gate does.
         `run: ${identifier(args.entryName)} extend { dimension: gate_probe_value__ is ${ref} } -> { select: probe_value is gate_probe_value__; limit: 1 }`,
      )
      // Live, as the source compares it: with no manifest, Malloy falls back
      // to the one the model's runtime was bound with, and reads the stored
      // table a `#@ persist` source resolves to.
      .getPreparedResult({
         ...(args.givens ? { givens: args.givens } : {}),
         buildManifest: EMPTY_BUILD_MANIFEST,
      } as never);
   return sql;
}

async function probe(args: {
   runtime: ProbeRuntime;
   modelDef: ModelDef;
   entryName: string;
   path: readonly string[];
   dialect: string;
   connectionName: string | undefined;
   givens?: Record<string, unknown>;
}): Promise<Collation> {
   const read = capabilitiesOf(args.dialect).stringCollation;
   if (read === "binary") return "binary";
   if (read === undefined) return "unknown";
   if (args.path.some((p) => malloyIdentifiers(p).length !== 1)) {
      return "unknown";
   }
   const sql = await probeSql(args);
   const connection = await args.runtime.connections.lookupConnection(
      args.connectionName,
   );
   try {
      return read === "instance"
         ? await duckdbCollation(connection, sql)
         : await postgresCollation(connection, unfinalizedPostgresSQL(sql));
   } catch {
      // Unreadable: fail closed.
      return "unknown";
   }
}

type Rows = { rows: Record<string, unknown>[] };

async function rows(
   connection: Connection,
   sql: string,
): Promise<Rows["rows"]> {
   const result = (await connection.runSQL(sql)) as unknown as Rows;
   return result.rows ?? [];
}

/** Postgres rows, which Malloy's Postgres connection reads from a `row` column. */
function pgRows(connection: Connection, sql: string): Promise<Rows["rows"]> {
   return rows(connection, `SELECT row_to_json(t) AS row FROM (${sql}) t`);
}

/**
 * A DuckDB-dialect source compares bytes unless the instance sets a
 * `default_collation`, or a `COLLATE` appears in the source's SQL or in the
 * definition of any table or view the instance holds (a column's collation
 * is not otherwise readable).
 */
/**
 * How many user definitions in the instance name a `COLLATE`: tables, views,
 * and macros (a macro can return a collated value to a view or a query).
 * Attached databases' definitions are listed too.
 */
const DUCKDB_COLLATE_DEFINITIONS =
   "SELECT (SELECT count(*) FROM duckdb_tables() WHERE NOT internal AND sql ILIKE '%collate%') + (SELECT count(*) FROM duckdb_views() WHERE NOT internal AND sql ILIKE '%collate%') + (SELECT count(*) FROM duckdb_functions() WHERE NOT internal AND function_type IN ('macro', 'table_macro') AND (macro_definition IS NULL OR macro_definition ILIKE '%collate%')) AS n";

export async function duckdbCollation(
   connection: Connection,
   sql: string,
): Promise<Collation> {
   if (/\bcollate\b/i.test(sql)) return "unknown";
   const [setting] = await rows(
      connection,
      "SELECT current_setting('default_collation') AS c",
   );
   if (String(setting?.c ?? "").trim() !== "") return "unknown";
   const [defs] = await rows(connection, DUCKDB_COLLATE_DEFINITIONS);
   return Number(defs?.n ?? 1) === 0 ? "binary" : "unknown";
}

/**
 * A Postgres collation row, judged: the database default by the database's
 * own collation and locale provider (an ICU default is deterministic, never
 * binary, whatever `datcollate` says), any other by its own row. Binary only
 * under a libc `C` or `POSIX`.
 */
function postgresJudged(row: Record<string, unknown> | undefined): Collation {
   if (!row) return "unknown";
   const binaryName = (c: unknown) => c === "C" || c === "POSIX";
   if (row.name === "default") {
      return row.dbp !== "i" && binaryName(row.dbc)
         ? "binary"
         : "deterministic";
   }
   if (row.det !== true) return "unknown";
   return row.prov === "c" && binaryName(row.cc) ? "binary" : "deterministic";
}

/** The columns {@link postgresJudged} reads, from `pg_collation c, pg_database d`. */
const PG_COLLATION_COLUMNS =
   "c.collname AS name, c.collisdeterministic AS det, c.collcollate AS cc, c.collprovider AS prov, d.datcollate AS dbc, to_jsonb(d) ->> 'datlocprovider' AS dbp";

/** The Postgres database's default collation, as {@link postgresJudged} reads it. */
async function postgresDefaultCollation(
   connection: Connection,
): Promise<Collation> {
   const [row] = await pgRows(
      connection,
      `SELECT ${PG_COLLATION_COLUMNS} FROM pg_collation c, pg_database d WHERE c.collname = 'default' AND d.datname = current_database()`,
   );
   return postgresJudged(row);
}

/**
 * Malloy's Postgres query SQL with its final stage taken off: a compiled
 * query ends `SELECT row_to_json(finalStage) as row FROM <stage> AS
 * finalStage`, one JSON column with no collation, so the probe reads the
 * stage it wraps instead, whose `probe_value` keeps its column's collation.
 * SQL with no such stage is returned as it is.
 */
export function unfinalizedPostgresSQL(sql: string): string {
   return sql.replace(
      /\bSELECT row_to_json\(finalStage\) as row FROM (__stage\d+) AS finalStage\s*$/,
      "SELECT * FROM $1",
   );
}

/**
 * A Postgres column's collation, read from the source's own SQL
 * (`pg_collation_for`, looked up by its OID): binary under a libc `C` or
 * `POSIX`, deterministic under any other deterministic collation, the
 * database default included.
 */
export async function postgresCollation(
   connection: Connection,
   sql: string,
): Promise<Collation> {
   const [row] = await pgRows(
      connection,
      `SELECT ${PG_COLLATION_COLUMNS} FROM pg_collation c, pg_database d WHERE c.oid = (pg_collation_for((SELECT q.probe_value FROM (${sql}) q LIMIT 1)))::regcollation AND d.datname = current_database()`,
   );
   return postgresJudged(row);
}

/** A table an answer read, as the stored-side check reads it. */
interface StoredTable {
   readonly tier: "database" | "storage";
   readonly virtualHandle?: string;
   readonly tablePath?: string;
   readonly destinationName?: string;
}

/**
 * Why a stored table an answer read past a gate, or under a scope, does not
 * compare a string column as its source does, or undefined: the stored
 * column's own collation, which an `ALTER … SET DATA TYPE … COLLATE` after
 * the build, or a destination's `default_collation`, can change. Every table
 * read is checked, however it was admitted (database, a `storage=` table
 * past its own gate, or one holding only ungated rows), so a new admission
 * mark needs nothing here.
 *
 * - a `storage=` table, and a database one in a DuckDB-dialect warehouse:
 *   byte-exact only when the instance sets no `default_collation` and the
 *   table's definition names no `COLLATE`;
 * - a database table in Postgres: each collatable column's collation, as
 *   the source side reads it (binary under `C`/`POSIX`, deterministic under
 *   any other deterministic one);
 * - anything else, or a table this cannot find: unknown.
 *
 * `equalityOnly` admits a deterministic collation; otherwise only binary.
 */
export async function storedTableCollationRefusal(args: {
   tables: readonly (StoredTable & { gateColumnTypes?: readonly string[] })[];
   answerRunnable: QueryMaterializer;
   virtualMap: ReadonlyMap<string, ReadonlyMap<string, string>> | undefined;
   /** The author's compile: a database table shares its source's dialect. */
   authorRunnable: QueryMaterializer;
   reads: GateReads | undefined;
   /** The caller's givens: a probe of a source whose rows read one needs it. */
   givens?: Record<string, unknown>;
}): Promise<string | undefined> {
   if (args.tables.length === 0) return undefined;
   if (await comparesNoString(args)) return undefined;
   const sourceDialect = await entryDialectOf(args.authorRunnable);
   const equalityOnly = args.reads?.equalityOnly === true;
   const runtime = (
      args.answerRunnable as unknown as { runtime?: ProbeRuntime }
   ).runtime;
   if (!runtime) return "no runtime";
   const need: Collation[] = equalityOnly
      ? ["binary", "deterministic"]
      : ["binary"];
   // A database table in a per-column dialect (Postgres) sits in its
   // source's own warehouse, so it is judged against its source, not an
   // absolute bar: it is kept when it holds every column the scope reads
   // under its source's own collation (a stored `C` column under a
   // case-insensitive source compares otherwise, though it is binary).
   const perColumn = capabilitiesOf(sourceDialect).stringCollation === "column";
   let sameCollationNeeded = false;
   for (const t of args.tables) {
      if (perColumn && t.tier === "database") {
         sameCollationNeeded = true;
         continue;
      }
      let collation: Collation;
      try {
         collation = await storedSide(t, args, runtime, sourceDialect);
      } catch {
         collation = "unknown";
      }
      if (!need.includes(collation)) {
         return `a stored table compares strings under a ${collation} collation`;
      }
   }
   if (!sameCollationNeeded) return undefined;
   let why: string | undefined;
   try {
      why = await databaseCollationRefusal({
         tables: args.tables.filter((t) => t.tier === "database"),
         authorRunnable: args.authorRunnable,
         storedRuntime: runtime,
         reads: args.reads,
         ...(args.givens ? { givens: args.givens } : {}),
      });
   } catch {
      why = "a collation that could not be read";
   }
   return why === undefined
      ? undefined
      : `a stored table does not keep its source's collation: ${why}`;
}

/**
 * The resolved identity of a Postgres collation, from `pg_collation c` and
 * `pg_database d`: the database default by the database's own provider and
 * locale, any other by its provider, determinism and locale, each with the
 * library version it was recorded under (two clusters on different ICU or
 * libc versions do not match). Never by name, since `default` and a
 * same-named collation mean other things in other databases. An explicit
 * column collation equal to the database default reads as another identity:
 * that over-blocks, never under-blocks.
 */
const PG_COLLATION_IDENTITY = `CASE WHEN c.collname = 'default' AND c.collnamespace = 'pg_catalog'::regnamespace THEN concat_ws('|', 'db', coalesce(to_jsonb(d) ->> 'datlocprovider', ''), d.datcollate, d.datctype, coalesce(to_jsonb(d) ->> 'daticulocale', ''), coalesce(to_jsonb(d) ->> 'datlocale', ''), coalesce(to_jsonb(d) ->> 'daticurules', ''), coalesce(to_jsonb(d) ->> 'datcollversion', '')) ELSE concat_ws('|', 'coll', c.collprovider, c.collisdeterministic::text, coalesce(c.collcollate, ''), coalesce(c.collctype, ''), coalesce(to_jsonb(c) ->> 'colliculocale', ''), coalesce(to_jsonb(c) ->> 'colllocale', ''), coalesce(to_jsonb(c) ->> 'collicurules', ''), coalesce(to_jsonb(c) ->> 'collversion', '')) END`;

/** The identity of the collation `sql`'s `probe_value` compares under. */
async function postgresCollationIdentity(
   connection: Connection,
   sql: string,
): Promise<string | undefined> {
   const [row] = await pgRows(
      connection,
      `SELECT ${PG_COLLATION_IDENTITY} AS id FROM pg_collation c, pg_database d WHERE c.oid = (pg_collation_for((SELECT q.probe_value FROM (${sql}) q LIMIT 1)))::regcollation AND d.datname = current_database()`,
   );
   return typeof row?.id === "string" ? row.id : undefined;
}

/** The identity of the database's default collation. */
async function postgresDefaultIdentity(
   connection: Connection,
): Promise<string | undefined> {
   const [row] = await pgRows(
      connection,
      `SELECT ${PG_COLLATION_IDENTITY} AS id FROM pg_collation c, pg_database d WHERE c.collname = 'default' AND c.collnamespace = 'pg_catalog'::regnamespace AND d.datname = current_database()`,
   );
   return typeof row?.id === "string" ? row.id : undefined;
}

/**
 * Every column of a Postgres table, by name: its collation's identity, or
 * null for a column that has none (not a string).
 */
async function postgresColumnIdentities(
   connection: Connection,
   tablePath: string,
): Promise<Map<string, string | null>> {
   const path = tablePath.replace(/'/g, "''");
   const columns = await pgRows(
      connection,
      `SELECT a.attname AS col, CASE WHEN a.attcollation = 0 THEN NULL ELSE (SELECT ${PG_COLLATION_IDENTITY} FROM pg_collation c, pg_database d WHERE c.oid = a.attcollation AND d.datname = current_database()) END AS id FROM pg_attribute a WHERE a.attrelid = '${path}'::regclass AND a.attnum > 0 AND NOT a.attisdropped`,
   );
   return new Map(
      columns.map((c) => [
         String(c.col),
         typeof c.id === "string" ? c.id : null,
      ]),
   );
}

/**
 * The model source a compiled query's entry point is judged as: its own name,
 * or, for an inline `extend` (`run: t extend { dimension: z is 1 } -> …`),
 * the name of the source it extends when the extension adds no `where:` and
 * no gate of its own; else undefined.
 */
function namedEntryOf(
   structRef: unknown,
   struct: unknown,
   modelDef: ModelDef,
): string | undefined {
   if (typeof structRef === "string") return structRef;
   type Shape = {
      as?: unknown;
      extends?: unknown;
      sourceID?: unknown;
      type?: unknown;
      name?: unknown;
      connection?: unknown;
      filterList?: unknown;
      fields?: unknown;
   };
   const s = struct as Shape | undefined;
   if (typeof s?.as !== "string") return undefined;
   const base = (modelDef as unknown as { contents?: Record<string, Shape> })
      .contents?.[s.as];
   if (base === undefined) return undefined;
   // The same source: where Malloy records what it extends, that; and the
   // same root (type, table or SQL, connection), with every one of the
   // source's fields kept as it is (an extension only adds fields).
   if (s.extends !== undefined && s.extends !== base.sourceID) return undefined;
   if (
      s.type !== base.type ||
      s.name !== base.name ||
      s.connection !== base.connection
   ) {
      return undefined;
   }
   const fields = new Set(
      ((s.fields ?? []) as unknown[]).map((f) => JSON.stringify(f)),
   );
   if (
      !((base.fields ?? []) as unknown[]).every((f) =>
         fields.has(JSON.stringify(f)),
      )
   ) {
      return undefined;
   }
   const same = (a: unknown, b: unknown) =>
      JSON.stringify(a ?? []) === JSON.stringify(b ?? []);
   if (!same(base.filterList, s.filterList)) return undefined;
   // The walk's fail-closed deny reads as unequal.
   const gates = (x: unknown) => {
      const entries = entryGates(x, modelDef);
      return entries === "unreadable"
         ? undefined
         : entries.map((e) => [e.route, [...e.exprs]]);
   };
   const own = gates(struct);
   if (own === undefined || !same(gates(base), own)) return undefined;
   return s.as;
}

/**
 * Why database tables in a per-column dialect do not compare the scope's
 * strings as live does, or undefined when they do: every string the gates
 * and scopes compare (each entry path, probed through the author's compile)
 * and every stored column they read (by any name the column goes by: a
 * rename's, a flattened join's, a computed field's reads) have one and the
 * same resolved collation, and the stored tables' connection has the
 * source's default (a literal or a given compares under it). The same
 * collation compares alike under every operator, a pattern or a function
 * included. Anything unread or unmatched refuses, but one case: a column
 * of a rollup's base's own `where:`, which the rollup's build applied, so it
 * is no column of the rollup's table and is not compared when it is read.
 *
 * The entry point is the query's named source, or the named source a
 * query's inline `extend` extends when that adds no `where:` and no gate
 * (it reads that source's rows under that source's gates and scopes).
 */
async function databaseCollationRefusal(args: {
   tables: readonly StoredTable[];
   authorRunnable: QueryMaterializer;
   storedRuntime: ProbeRuntime;
   reads: GateReads | undefined;
   givens?: Record<string, unknown>;
}): Promise<string | undefined> {
   const { reads } = args;
   if (reads === undefined || reads.unresolved) return "gate reads unread";
   if (!reads.entryPathsComplete) return "a scope from elsewhere";
   const prepared = await readPreparedQuery(args.authorRunnable);
   const modelDef = prepared.modelDef as ModelDef | undefined;
   if (!modelDef) return "no author compile";
   const struct = resolveStructRefSource(
      prepared.structRef as never,
      modelDef as never,
   ) as { connection?: unknown; filterList?: unknown } | undefined;
   const entryName = namedEntryOf(prepared.structRef, struct, modelDef);
   if (entryName === undefined) return "no named entry point";
   const connectionName =
      typeof struct?.connection === "string" ? struct.connection : undefined;
   const runtime = (
      args.authorRunnable as unknown as { runtime?: ProbeRuntime }
   ).runtime;
   if (!runtime) return "no runtime";
   const source = await runtime.connections.lookupConnection(connectionName);
   const paths = stringPaths(struct, reads.entryPaths);
   if (paths === undefined) return "an unreadable gate path";
   const compared = new Set<string>();
   for (const path of paths) {
      const id = await postgresCollationIdentity(
         source,
         unfinalizedPostgresSQL(
            await probeSql({
               runtime,
               modelDef,
               entryName,
               path,
               ...(args.givens ? { givens: args.givens } : {}),
            }),
         ),
      );
      if (id === undefined) return `\`${path.join(".")}\` unread`;
      compared.add(id);
   }
   const sourceDefault = await postgresDefaultIdentity(source);
   if (sourceDefault === undefined) return "the source's default unread";
   // Each column the reads name, by every name it goes by, must be one of
   // the tables' own; each string one joins the compared collations.
   const held = new Map<string, string | null>();
   for (const t of args.tables) {
      if (t.tablePath === undefined) return "a table with no path";
      const connection = await args.storedRuntime.connections.lookupConnection(
         t.destinationName ?? connectionName,
      );
      if ((await postgresDefaultIdentity(connection)) !== sourceDefault) {
         return "a stored table's connection has another default collation";
      }
      for (const [col, id] of await postgresColumnIdentities(
         connection,
         t.tablePath,
      )) {
         held.set(col, id);
      }
   }
   // The names the entry point's own `where:` reads, and whether a rollup's
   // table was read: a rollup is built with its base's own `where:` applied.
   const ownWhere = new Set(fieldPathsIn(struct?.filterList).flat());
   const rollupRead = args.tables.some(
      (t) => (t as { kind?: unknown }).kind === "rollup",
   );
   let storedStrings = 0;
   for (const names of reads.columns) {
      const found = names.filter((n) => held.has(n));
      if (found.length === 0) {
         if (rollupRead && names.some((n) => ownWhere.has(n))) continue;
         return "a scope column no stored table holds";
      }
      for (const n of found) {
         const id = held.get(n);
         if (id === null || id === undefined) continue;
         storedStrings++;
         compared.add(id);
      }
   }
   if (storedStrings > 0 && paths.length === 0) {
      return "a stored string column with no compared source path";
   }
   return compared.size <= 1 ? undefined : "a column under another collation";
}

/** The last name of a table path, its quotes off. */
function tableNameOf(path: string): string {
   const parts = path.match(/"(?:[^"]|"")*"|`[^`]*`|[^.]+/g) ?? [path];
   const last = parts[parts.length - 1].trim();
   return /^["`]/.test(last) ? last.slice(1, -1).replace(/""/g, '"') : last;
}

/**
 * A DuckDB or DuckLake table compares bytes unless the instance sets a
 * `default_collation` or the table's definition names a `COLLATE`; a table
 * the catalog does not list is unknown.
 */
export async function duckdbTableCollation(
   connection: Connection,
   tablePath: string,
): Promise<Collation> {
   const [setting] = await rows(
      connection,
      "SELECT current_setting('default_collation') AS c",
   );
   if (String(setting?.c ?? "").trim() !== "") return "unknown";
   const name = tableNameOf(tablePath).replace(/'/g, "''");
   const defs = await rows(
      connection,
      `SELECT sql FROM duckdb_tables() WHERE table_name = '${name}'`,
   );
   if (defs.length === 0) return "unknown";
   return defs.every(
      (d) => typeof d.sql === "string" && !/\bcollate\b/i.test(d.sql),
   )
      ? "binary"
      : "unknown";
}

/**
 * A Postgres table's collatable columns, together: binary when every one is
 * `C`/`POSIX`, deterministic when every one is deterministic, else unknown.
 */
export async function postgresTableCollation(
   connection: Connection,
   tablePath: string,
): Promise<Collation> {
   const path = tablePath.replace(/'/g, "''");
   const columns = await pgRows(
      connection,
      `SELECT ${PG_COLLATION_COLUMNS} FROM pg_attribute a JOIN pg_collation c ON c.oid = a.attcollation, pg_database d WHERE a.attrelid = '${path}'::regclass AND a.attnum > 0 AND NOT a.attisdropped AND d.datname = current_database()`,
   );
   const judged = columns.map(postgresJudged);
   if (judged.includes("unknown")) return "unknown";
   return judged.includes("deterministic") ? "deterministic" : "binary";
}

/** The dialect of the source a compiled query runs on, or "" when unread. */
async function entryDialectOf(runnable: QueryMaterializer): Promise<string> {
   try {
      const prepared = await readPreparedQuery(runnable);
      const struct = resolveStructRefSource(
         prepared.structRef as never,
         prepared.modelDef as never,
      ) as { dialect?: unknown } | undefined;
      const named = struct?.dialect;
      return typeof named === "string" ? named : "";
   } catch {
      return "";
   }
}

/**
 * One table's stored-side collation. A `storage=` table is found through the
 * answer's virtual map, in its DuckLake destination; one it cannot find is
 * unknown. A database table is in its source's own warehouse, built from
 * the source with its collations: a DuckDB-dialect one is checked by its
 * definition (or, when the answer names no path, the instance's every
 * table), a Postgres one by its columns; in any other warehouse it compares
 * as its source does, which the source side judges.
 */
async function storedSide(
   t: StoredTable,
   args: {
      virtualMap: ReadonlyMap<string, ReadonlyMap<string, string>> | undefined;
      authorRunnable: QueryMaterializer;
   },
   runtime: ProbeRuntime,
   sourceDialect: string,
): Promise<Collation> {
   if (t.tier === "storage") {
      const name = [...(args.virtualMap ?? new Map())].find(
         ([, inner]) =>
            t.virtualHandle !== undefined && inner.has(t.virtualHandle),
      )?.[0];
      // The answer's virtual map names the connection the table is read
      // through (a destination's alias on the served copy); else its own
      // destination's name, which a runtime without that alias does not
      // resolve (unknown).
      const connectionName = name ?? t.destinationName;
      if (connectionName === undefined || t.tablePath === undefined) {
         return "unknown";
      }
      return duckdbTableCollation(
         await runtime.connections.lookupConnection(connectionName),
         t.tablePath,
      );
   }
   const read = capabilitiesOf(sourceDialect).stringCollation;
   if (read !== "instance" && read !== "column") return "binary";
   const connectionName =
      t.destinationName ?? (await entryConnectionOf(args.authorRunnable));
   const connection =
      await runtime.connections.lookupConnection(connectionName);
   if (t.tablePath === undefined) {
      // No path to read the table's own columns by: the instance-wide check
      // for DuckDB, and unknown elsewhere.
      return read === "instance" ? duckdbCollation(connection, "") : "unknown";
   }
   return read === "instance"
      ? duckdbTableCollation(connection, t.tablePath)
      : postgresTableCollation(connection, t.tablePath);
}

/** The connection of the source a compiled query runs on. */
async function entryConnectionOf(
   runnable: QueryMaterializer,
): Promise<string | undefined> {
   try {
      const prepared = await readPreparedQuery(runnable);
      const struct = resolveStructRefSource(
         prepared.structRef as never,
         prepared.modelDef as never,
      ) as { connection?: unknown } | undefined;
      return typeof struct?.connection === "string"
         ? struct.connection
         : undefined;
   } catch {
      return undefined;
   }
}

/**
 * Whether the entry point's gates and scopes compare no string at all
 * (`GateReads.comparesNoString`, an allow-list over every leaf), so no
 * collation can change what they admit. Any doubt reads false.
 */
async function comparesNoString(args: {
   reads: GateReads | undefined;
}): Promise<boolean> {
   const { reads } = args;
   return reads !== undefined && !reads.unresolved && reads.comparesNoString;
}
