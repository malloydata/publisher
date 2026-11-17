// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Whether a gate or a `where:` that a stored copy re-applies compares a string
 * column there as live compares it.
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
 */

import type {
   Connection,
   ModelDef,
   ModelMaterializer,
   QueryMaterializer,
} from "@malloydata/malloy";
import { malloyIdentifiers } from "../../query_text";
import {
   readPreparedQuery,
   resolveStructRefSource,
} from "../../governance/ir/prepared_query";
import { identifier } from "../../malloy_text";
import { capabilitiesOf } from "../domain/dialect";
import type { GateReads } from "./gate_reads";

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

const STRING_STORED_TYPE = /\b(VARCHAR|TEXT|STRING|CHAR|CHARACTER)\b/i;

/** Whether a stored column type holds a string. */
export function storedTypeIsString(type: string): boolean {
   return STRING_STORED_TYPE.test(type);
}

/**
 * Why an answer that read a `storage=` table past the entry point's gates
 * does not compare a string column as live does, or undefined when every
 * string column it compares does. `stringStored` says whether any `storage=`
 * table the answer read holds a gate column as a string; when none does, no
 * probe runs.
 */
export async function stringGateCollationRefusal(args: {
   authorRunnable: QueryMaterializer;
   reads: GateReads | undefined;
   stringStored: boolean;
   /** The caller's givens: a probe of a source whose rows read one needs it. */
   givens?: Record<string, unknown>;
}): Promise<string | undefined> {
   const { reads } = args;
   if (!args.stringStored) return undefined;
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
   if (!modelDef || entryName === undefined) return "no named entry point";
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
   // Per column: only the entry point's own paths can be probed.
   if (!reads.entryPathsComplete) return "a scope from elsewhere";
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
 * the joined source's fields.
 */
function stringPaths(
   struct: unknown,
   paths: readonly (readonly string[])[],
): string[][] | undefined {
   const out: string[][] = [];
   const seen = new Set<string>();
   for (const path of paths) {
      let at: unknown = struct;
      let field: { type?: unknown } | undefined;
      for (const name of path) {
         field = (
            ((at as { fields?: unknown[] } | undefined)?.fields ?? []) as {
               name?: unknown;
               as?: unknown;
               type?: unknown;
            }[]
         ).find((f) => String(f.as ?? f.name) === name);
         if (!field) return undefined;
         at = field;
      }
      if (field?.type !== "string") continue;
      const key = path.join(".");
      if (seen.has(key)) continue;
      seen.add(key);
      out.push([...path]);
   }
   return out;
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
   const ref = args.path.map((p) => identifier(p)).join(".");
   const { sql } = await args.runtime
      ._loadModelFromModelDef(args.modelDef)
      .loadQuery(
         // An extension reads an `internal` field, as a gate does.
         `run: ${identifier(args.entryName)} extend { dimension: gate_probe_value__ is ${ref} } -> { select: probe_value is gate_probe_value__; limit: 1 }`,
      )
      .getPreparedResult((args.givens ? { givens: args.givens } : {}) as never);
   const connection = await args.runtime.connections.lookupConnection(
      args.connectionName,
   );
   try {
      return read === "instance"
         ? await duckdbCollation(connection, sql)
         : await postgresCollation(connection, sql);
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
   const [defs] = await rows(
      connection,
      "SELECT (SELECT count(*) FROM duckdb_tables() WHERE NOT internal AND sql ILIKE '%collate%') + (SELECT count(*) FROM duckdb_views() WHERE NOT internal AND sql ILIKE '%collate%') AS n",
   );
   return Number(defs?.n ?? 1) === 0 ? "binary" : "unknown";
}

/**
 * A Postgres column's collation, read from the source's own SQL
 * (`pg_collation_for`): binary under `C` or `POSIX`, deterministic under any
 * other deterministic collation, the database default included.
 */
export async function postgresCollation(
   connection: Connection,
   sql: string,
): Promise<Collation> {
   const [row] = await pgRows(
      connection,
      `SELECT pg_collation_for((SELECT q.probe_value FROM (${sql}) q LIMIT 1)) AS coll, (SELECT datcollate FROM pg_database WHERE datname = current_database()) AS dbc`,
   );
   const coll = row?.coll;
   if (typeof coll !== "string") return "unknown";
   const binaryName = (c: unknown) => c === "C" || c === "POSIX";
   if (coll === "default" || coll === '"default"') {
      return binaryName(row?.dbc) ? "binary" : "deterministic";
   }
   const name = coll.replace(/^"(.*)"$/, "$1");
   const [c] = await pgRows(
      connection,
      `SELECT collisdeterministic AS det, collcollate AS cc FROM pg_collation WHERE collname = '${name.replace(/'/g, "''")}' LIMIT 1`,
   );
   if (!c) return "unknown";
   if (c.det !== true) return "unknown";
   return binaryName(c.cc) || name === "C" || name === "POSIX"
      ? "binary"
      : "deterministic";
}
