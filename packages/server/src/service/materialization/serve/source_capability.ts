// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * How each source of a compiled model compares its strings live, read once
 * per bind (a load, a reload, a publish and a rebuild each bind) and kept
 * with the model's runtime, so a query looks it up instead of probing the
 * warehouse (`admission_lookup.ts`). The source-side half of what
 * `admission_lookup.ts` reads per query:
 *
 * - Postgres: each string field of each named source, through its joins to
 *   {@link MAX_JOIN_DEPTH} joins deep, as the database compares it
 *   (`pg_collation_for` of the field as the source computes it, judged and
 *   identified), from one compile per source and one catalog statement; and
 *   each connection's default collation, for a literal or a given;
 * - DuckDB: each connection's `default_collation` and whether any definition
 *   it holds names a `COLLATE` (the source's own text is scanned per query,
 *   with no read);
 * - BigQuery: each connection's catalog for every table a source of the
 *   model reads, through every join: the projects' collated datasets, the
 *   tables' types, and the collated columns of each table in a collated
 *   dataset, as `BigQueryCatalog` reads them.
 *
 * A field past the join bound, one Malloy cannot select alone (a measure),
 * and anything a read failed on is absent, which a lookup reads as unknown:
 * the comparison answers live. Never thrown: a failed read records nothing.
 *
 * Kept per runtime, so a reload (a new runtime) starts from nothing and a
 * rebind of the same model replaces what it read. The compiles are kept per
 * compiled model, so a rebind only reads the catalog again: a collation
 * altered in the warehouse is seen at the next bind.
 */

import {
   type Connection,
   EMPTY_BUILD_MANIFEST,
   type ModelDef,
   type QueryMaterializer,
} from "@malloydata/malloy";
import { malloyIdentifiers } from "../../query_text";
import { identifier } from "../../malloy_text";
import { queryTableReads } from "../../governance/ir/source_reads";
import {
   type Collation,
   PG_COLLATION_COLUMNS,
   PG_COLLATION_IDENTITY,
   postgresJudged,
} from "../domain/collation";
import { DUCKDB_COLLATE_DEFINITIONS } from "../domain/collation";
import { collationReaderOf } from "./collation";
import {
   BigQueryCatalog,
   type PlacedTable,
   placeTable,
   sqlProjects,
} from "./collation/bigquery";
import type { CatalogIO } from "./collation/catalog_io";
import {
   POSTGRES_COLLATION,
   unfinalizedPostgresSQL,
} from "./collation/postgres";
import { catalogIO } from "./collation/catalog_io";

/** How many joins deep a source's string fields are read; past it, unknown. */
export const MAX_JOIN_DEPTH = 4;

/** Most string fields read for one source; past it, the rest are unknown. */
const MAX_FIELDS_PER_SOURCE = 2000;

/** Most columns one Postgres catalog statement reads. */
const COLUMNS_PER_STATEMENT = 100;

/** How one string field compares live. */
export interface FieldCapability {
   readonly collation: Collation;
   /** Postgres: the collation's resolved identity (`PG_COLLATION_IDENTITY`). */
   readonly identity?: string;
}

/** What a BigQuery connection's catalog held for a model's tables. */
export interface RecordedBigQuery {
   readonly projects: readonly string[];
   /** Each project's collated datasets; undefined: not read or not provable. */
   readonly collated: ReadonlyMap<string, ReadonlySet<string> | undefined>;
   /** Every table's type by `project.dataset.name`, as `tableTypes` read it. */
   readonly types: ReadonlyMap<string, string> | "no_region" | undefined;
   /** The datasets read table by table; undefined: the read failed. */
   readonly datasets: ReadonlyMap<
      string,
      | ReadonlyMap<
           string,
           { type: string; collated: ReadonlyMap<string, string> }
        >
      | undefined
   >;
}

/** How one connection compares strings, apart from any one column. */
export interface ConnectionCapability {
   readonly dialect: string;
   /** Postgres: the database's default collation, judged. */
   readonly defaultCollation?: Collation;
   /** Postgres: its identity. */
   readonly defaultIdentity?: string;
   /** DuckDB: the instance sets no `default_collation`. */
   readonly noDefaultCollation?: boolean;
   /** DuckDB: no definition the instance holds names a `COLLATE`. */
   readonly noCollateDefinitions?: boolean;
   /** BigQuery: its catalog, for every table the model reads. */
   readonly catalog?: RecordedBigQuery;
}

/** How one named source's string fields compare, by path. */
interface SourceCapability {
   readonly connectionName: string | undefined;
   /** By `path.join("\0")`; a path absent is unknown. */
   readonly fields: ReadonlyMap<string, FieldCapability>;
}

/** What a model's bind read of how its sources compare strings. */
export interface StringCapabilities {
   readonly connections: ReadonlyMap<string, ConnectionCapability>;
   /** By source name in the model. */
   readonly sources: ReadonlyMap<string, SourceCapability>;
}

/** The key a field path is looked up by. */
export function fieldKey(path: readonly string[]): string {
   return path.join("\u0000");
}

const recorded = new WeakMap<object, StringCapabilities>();

/** Keep `capabilities` for queries compiled by `runtime`. */
export function recordStringCapabilities(
   runtime: object,
   capabilities: StringCapabilities,
): void {
   recorded.set(runtime, capabilities);
}

/** What the last bind read for `runtime`'s model, or undefined: none yet. */
export function stringCapabilitiesOf(
   runtime: object | undefined,
): StringCapabilities | undefined {
   return runtime ? recorded.get(runtime) : undefined;
}

/** A named source of a compiled model, as this module reads it. */
interface SourceShape {
   type?: unknown;
   dialect?: unknown;
   connection?: unknown;
   fields?: unknown;
}

/** One string field of a source: its path and whether it can be selected alone. */
interface StringLeaf {
   path: string[];
   selectable: boolean;
}

/**
 * Every string field of `struct` and of its joins, to {@link MAX_JOIN_DEPTH}
 * joins deep, by path. A field Malloy cannot select alone (an aggregate or
 * an analytic one), or whose name Malloy would not read back as one
 * identifier, is listed unselectable.
 */
export function stringLeaves(struct: unknown): StringLeaf[] {
   const out: StringLeaf[] = [];
   const walk = (fields: unknown, prefix: string[], depth: number) => {
      if (!Array.isArray(fields)) return;
      for (const f of fields as Record<string, unknown>[]) {
         if (out.length >= MAX_FIELDS_PER_SOURCE) return;
         const name = String(f.as ?? f.name ?? "");
         if (name === "") continue;
         if (typeof f.join === "string" && Array.isArray(f.fields)) {
            if (depth < MAX_JOIN_DEPTH)
               walk(f.fields, [...prefix, name], depth + 1);
            continue;
         }
         if (f.type !== "string") continue;
         const path = [...prefix, name];
         const kind = f.expressionType;
         out.push({
            path,
            selectable:
               (kind === undefined || kind === "scalar") &&
               path.every((p) => malloyIdentifiers(p).length === 1),
         });
      }
   };
   walk((struct as SourceShape | undefined)?.fields, [], 0);
   return out;
}

/** The compiled probe SQL of a source's fields, kept per compiled model. */
const compiled = new WeakMap<
   object,
   Map<string, Promise<{ sql: string; column: string }[] | undefined>[]>
>();

/** What reading a model's capabilities goes through. */
interface CapabilityReadArgs {
   readonly modelDef: ModelDef;
   /** Compiles a query in the model as it serves (`Model.authorQueryLoader`). */
   readonly load: (query: string) => QueryMaterializer;
   /** The model's live connection of that name. */
   readonly connection: (name: string) => Promise<Connection>;
   /** A value for each given that has no default, for the probe compiles. */
   readonly givens?: Record<string, unknown>;
}

/**
 * Read how every named source of a model compares its strings, and each
 * connection those sources read. Never throws: what fails is left out.
 */
export async function readStringCapabilities(
   args: CapabilityReadArgs,
): Promise<StringCapabilities> {
   const contents = ((args.modelDef as unknown as { contents?: unknown })
      .contents ?? {}) as Record<string, SourceShape>;
   const named = Object.entries(contents).filter(
      ([, s]) =>
         s !== undefined &&
         typeof s.dialect === "string" &&
         typeof s.connection === "string" &&
         Array.isArray(s.fields),
   );
   const connectionNames = [
      ...new Set(named.map(([, s]) => s.connection as string)),
   ];
   const connections = new Map<string, ConnectionCapability>();
   const ios = new Map<string, CatalogIO>();
   for (const name of connectionNames) {
      try {
         const io = catalogIO(await args.connection(name));
         ios.set(name, io);
         const dialect = named.find(([, s]) => s.connection === name)?.[1]
            .dialect as string;
         connections.set(
            name,
            await connectionCapability(
               io,
               dialect,
               name,
               named.filter(([, s]) => s.connection === name).map(([, s]) => s),
            ),
         );
      } catch {
         // Unread: every comparison over it is unknown.
      }
   }
   const sources = new Map<string, SourceCapability>();
   for (const [name, struct] of named) {
      const connectionName = struct.connection as string;
      const io = ios.get(connectionName);
      const fields =
         io && collationReaderOf(struct.dialect)?.scope === "column"
            ? await postgresFields(args, name, struct, io).catch(
                 () => new Map<string, FieldCapability>(),
              )
            : new Map<string, FieldCapability>();
      sources.set(name, { connectionName, fields });
   }
   return { connections, sources };
}

/** A connection's capability by its dialect's reader. */
async function connectionCapability(
   io: CatalogIO,
   dialect: string,
   name: string,
   structs: readonly SourceShape[],
): Promise<ConnectionCapability> {
   const reader = collationReaderOf(dialect);
   if (reader?.scope === "column") {
      const [defaultCollation, defaultIdentity] = await Promise.all([
         POSTGRES_COLLATION.defaultCollation(io).catch(
            () => "unknown" as const,
         ),
         POSTGRES_COLLATION.defaultIdentity(io).catch(() => undefined),
      ]);
      return {
         dialect,
         defaultCollation,
         ...(defaultIdentity !== undefined ? { defaultIdentity } : {}),
      };
   }
   if (reader?.scope === "instance") {
      const [setting] = await io
         .rows("SELECT current_setting('default_collation') AS c")
         .catch(() => [undefined]);
      const [defs] = await io
         .rows(DUCKDB_COLLATE_DEFINITIONS)
         .catch(() => [undefined]);
      return {
         dialect,
         noDefaultCollation:
            setting !== undefined && String(setting.c ?? "").trim() === "",
         noCollateDefinitions: defs !== undefined && Number(defs.n ?? 1) === 0,
      };
   }
   if (reader?.scope === "dataset") {
      return {
         dialect,
         catalog: await recordBigQuery(io, name, structs).catch(() => ({
            projects: [],
            collated: new Map(),
            types: undefined,
            datasets: new Map(),
         })),
      };
   }
   return { dialect };
}

/**
 * Every piece of SQL text in `node`, a compiled source or query: author SQL
 * (`selectStr` and its segments' `sql`), table paths (`tablePath`), and raw
 * SQL expressions (`genericSQLExpr`'s `src`) and string and regex literals:
 * what the query's compiled SQL holds apart from its identifiers, which a
 * query's own check scans for a `COLLATE` and for the projects it may read.
 * Field names, paths and annotations are not SQL, so a field path such as
 * `params.tags` names no project.
 */
export function irSql(node: unknown, out: string[] = [], depth = 0): string[] {
   if (depth > 200) return out;
   if (Array.isArray(node)) {
      for (const n of node) irSql(n, out, depth + 1);
   } else if (node !== null && typeof node === "object") {
      const record = node as Record<string, unknown>;
      for (const key of ["selectStr", "tablePath"]) {
         if (typeof record[key] === "string") out.push(record[key] as string);
      }
      if (
         typeof record.sql === "string" &&
         Object.keys(record).every((k) => k === "sql")
      ) {
         out.push(record.sql);
      }
      // A literal is in the SQL a query compiles to, as the probe reads it.
      if (
         (record.node === "stringLiteral" || record.node === "regexpLiteral") &&
         typeof record.literal === "string"
      ) {
         out.push(record.literal);
      }
      if (record.node === "genericSQLExpr" && Array.isArray(record.src)) {
         for (const t of record.src) if (typeof t === "string") out.push(t);
      }
      for (const v of Object.values(record)) irSql(v, out, depth + 1);
   }
   return out;
}

/** What `catalog` reads for every table `structs` read, through every join. */
async function recordBigQuery(
   io: CatalogIO,
   name: string,
   structs: readonly SourceShape[],
): Promise<RecordedBigQuery> {
   const catalog = new BigQueryCatalog(new Map(), io, name);
   const { job } = catalog;
   const projects = new Set(job.projects);
   const placed: PlacedTable[] = [];
   for (const struct of structs) {
      // One stage with no join usage recorded reads every join it reaches.
      const tables = queryTableReads(struct, [{}]);
      if (tables === undefined) {
         for (const p of sqlProjects(irSql(struct).join("\n"))) {
            projects.add(p);
         }
         continue;
      }
      for (const t of tables) {
         if (t.connectionName !== name) continue;
         const p = placeTable(t.table, job.projects);
         if (p === undefined) continue;
         placed.push(p);
         for (const project of p.projects) projects.add(project);
      }
   }
   const collated = new Map<string, ReadonlySet<string> | undefined>();
   for (const project of projects) {
      collated.set(project, await catalog.collatedDatasets(project));
   }
   const types =
      placed.length > 0 ? await catalog.tableTypes(placed) : new Map();
   const datasets = new Map<
      string,
      | ReadonlyMap<
           string,
           { type: string; collated: ReadonlyMap<string, string> }
        >
      | undefined
   >();
   for (const t of placed) {
      if (datasets.has(t.dataset)) continue;
      const inCollated = t.projects.some((p) => {
         const d = collated.get(p);
         return d === undefined || d.has(t.datasetName);
      });
      if (types !== "no_region" && !inCollated) continue;
      datasets.set(
         t.dataset,
         await catalog.datasetTables(t.dataset).catch(() => undefined),
      );
   }
   return { projects: job.projects, collated, types, datasets };
}

/** One source's string fields on Postgres, each judged and identified. */
async function postgresFields(
   args: CapabilityReadArgs,
   sourceName: string,
   struct: SourceShape,
   io: CatalogIO,
): Promise<Map<string, FieldCapability>> {
   const leaves = stringLeaves(struct).filter((l) => l.selectable);
   if (leaves.length === 0) return new Map();
   let byModel = compiled.get(args.modelDef);
   if (!byModel) {
      byModel = new Map();
      compiled.set(args.modelDef, byModel);
   }
   let probes = byModel.get(sourceName);
   if (!probes) {
      probes = probeQueries(args, sourceName, leaves);
      byModel.set(sourceName, probes);
   }
   const [all, ...each] = await Promise.all(probes);
   const out = new Map<string, FieldCapability>();
   const reads: { leaf: StringLeaf; sql: string; column: string }[] = [];
   // One compile per source, or one per field after it failed.
   if (all) {
      all.forEach((col, j) => reads.push({ leaf: leaves[j], ...col }));
   } else {
      each.forEach((c, j) => {
         if (c?.[0]) reads.push({ leaf: leaves[j], ...c[0] });
      });
   }
   // One statement per batch; a batch that fails (one column of a type
   // Postgres collates nothing of, a `bytea` or an `interval` Malloy reads
   // as a string, fails the whole statement) is read column by column, so
   // only the column that fails is unknown.
   const read = async (
      batch: typeof reads,
   ): Promise<(Record<string, unknown> | undefined)[]> => {
      const rows = await io
         .rows(collationStatement(batch))
         .catch(() => undefined);
      if (rows !== undefined) {
         const row = rows[0];
         return batch.map((_, j) =>
            row
               ? (fieldOf(row, `c${j}`) as Record<string, unknown>)
               : undefined,
         );
      }
      if (batch.length === 1) return [undefined];
      const each: (Record<string, unknown> | undefined)[] = [];
      for (const r of batch) each.push(...(await read([r])));
      return each;
   };
   for (let i = 0; i < reads.length; i += COLUMNS_PER_STATEMENT) {
      const batch = reads.slice(i, i + COLUMNS_PER_STATEMENT);
      const judged = await read(batch);
      batch.forEach((r, j) => {
         const raw = judged[j];
         const judgedRow =
            typeof raw === "string"
               ? (JSON.parse(raw) as Record<string, unknown>)
               : (raw as Record<string, unknown> | null | undefined);
         const collation = postgresJudged(judgedRow ?? undefined);
         const identity =
            typeof judgedRow?.id === "string" ? judgedRow.id : undefined;
         out.set(fieldKey(r.leaf.path), {
            collation,
            ...(identity !== undefined ? { identity } : {}),
         });
      });
   }
   return out;
}

/**
 * The probe compiles of a source's fields: one selecting every field, and,
 * only when that one fails, one per field, in the fields' order.
 */
function probeQueries(
   args: CapabilityReadArgs,
   sourceName: string,
   leaves: readonly StringLeaf[],
): Promise<{ sql: string; column: string }[] | undefined>[] {
   const compile = async (
      chosen: readonly StringLeaf[],
   ): Promise<{ sql: string; column: string }[] | undefined> => {
      const dims = chosen
         .map(
            (l, i) =>
               `gate_probe_value__${i} is ${l.path.map((p) => identifier(p)).join(".")}`,
         )
         .join(", ");
      const select = chosen
         .map((_, i) => `probe_value_${i} is gate_probe_value__${i}`)
         .join(", ");
      try {
         const { sql } = await args
            .load(
               `run: ${identifier(sourceName)} extend { dimension: ${dims} } -> { select: ${select}; limit: 1 }`,
            )
            .getPreparedResult({
               ...(args.givens ? { givens: args.givens } : {}),
               buildManifest: EMPTY_BUILD_MANIFEST,
            } as never);
         const unfinalized = unfinalizedPostgresSQL(sql);
         return chosen.map((_, i) => ({
            sql: unfinalized,
            column: `probe_value_${i}`,
         }));
      } catch {
         return undefined;
      }
   };
   const all = compile(leaves);
   const each = leaves.map(async (leaf) =>
      (await all) === undefined ? compile([leaf]) : undefined,
   );
   return [all, ...each];
}

/** One statement reading each column's collation, judged and identified. */
function collationStatement(
   reads: readonly { sql: string; column: string }[],
): string {
   const ctes = new Map<string, string>();
   for (const r of reads) {
      if (!ctes.has(r.sql)) ctes.set(r.sql, `q${ctes.size}`);
   }
   const withs = [...ctes].map(([sql, name]) => `${name} AS (${sql})`);
   const select = reads
      .map(
         (r, i) =>
            `(SELECT row_to_json(j) FROM (SELECT ${PG_COLLATION_COLUMNS}, ${PG_COLLATION_IDENTITY} AS id FROM pg_collation c, pg_database d ` +
            `WHERE c.oid = (pg_collation_for((SELECT b.${r.column} FROM ${ctes.get(r.sql)} b LIMIT 1)))::regcollation ` +
            `AND d.datname = current_database()) j) AS c${i}`,
      )
      .join(", ");
   return `SELECT row_to_json(x) AS row FROM (WITH ${withs.join(", ")} SELECT ${select}) x`;
}

/** A row's field, matched exactly then case-blind. */
function fieldOf(row: Record<string, unknown>, name: string): unknown {
   if (name in row) return row[name];
   const key = Object.keys(row).find(
      (k) => k.toLowerCase() === name.toLowerCase(),
   );
   return key === undefined ? undefined : row[key];
}

/**
 * Read a model's capabilities and keep them with `runtime`, for the
 * lookups: what a bind commit runs for each model. Never throws.
 */
export async function refreshStringCapabilities(
   runtime: object,
   args: CapabilityReadArgs,
): Promise<void> {
   recordStringCapabilities(runtime, await readStringCapabilities(args));
}
