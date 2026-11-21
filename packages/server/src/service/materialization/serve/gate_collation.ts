// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Whether a gate or a `where:` that a stored copy re-applies compares a string
 * column there as live compares it. The caller's own query filters, and the
 * strings the query's result orders and groups by, are judged by the same
 * rules, read as `GateReads` of their own (`queryFilterReadsOfQuery` in
 * `gate_reads.ts`, `resultReadsOfQuery` in `result_reads.ts`).
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
 * - `binary` (byte-exact, every operator): a BigQuery query that names no
 *   `COLLATE` and reads only tables whose columns and fields carry no
 *   collation (`INFORMATION_SCHEMA`, read project-wide in each region the
 *   project has datasets in and then per listed dataset, once per loaded
 *   model; see {@link bigquerySource}). Residual risk: an `.sql()` source's
 *   tables are not known, so a table its credentials may query but whose
 *   metadata no read lists is compared byte-exact;
 *   a DuckDB-dialect source
 *   whose instance has no `default_collation` and that names no `COLLATE` in
 *   its SQL or in any table or view it can read; a Postgres column whose
 *   collation is `C`, `POSIX` or of the builtin provider;
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
 * table is the one place altered after Publisher built it, once per answer
 * however many comparisons (the gates, the filters, the result) ask.
 */

import {
   type Connection,
   EMPTY_BUILD_MANIFEST,
   type ModelDef,
   type ModelMaterializer,
   type QueryMaterializer,
} from "@malloydata/malloy";
import { runAsInternalStatement } from "../../duckdb_lockdown";
import { malloyIdentifiers } from "../../query_text";
import {
   readPreparedQuery,
   resolveStructRefSource,
} from "../../governance/ir/prepared_query";
import { identifier } from "../../malloy_text";
import { queryTableReads } from "../../governance/ir/source_reads";
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
   if (!modelDef) return "no author compile";
   const struct = resolveStructRefSource(
      prepared.structRef as never,
      modelDef as never,
   ) as { dialect?: unknown; connection?: unknown } | undefined;
   const entryName = namedEntryOf(prepared.structRef, struct, modelDef);
   const named = struct?.dialect;
   const dialect = typeof named === "string" ? named : "";
   const connectionName =
      typeof struct?.connection === "string" ? struct.connection : undefined;
   const runtime = (
      args.authorRunnable as unknown as { runtime?: ProbeRuntime }
   ).runtime;
   if (!runtime) return "no runtime";
   const connect = (name: string | undefined = connectionName) =>
      runtime.connections.lookupConnection(name);
   const read = capabilitiesOf(dialect).stringCollation;
   if (read === "binary") return undefined;
   if (read === undefined) return "a dialect whose collation is not read";
   if (read === "dataset") {
      // Query-wide, as for a DuckDB instance: every table the query reads,
      // and its SQL, so every column the gates and scopes read counts.
      const source = await bigquerySource({
         authorRunnable: args.authorRunnable,
         ...(args.givens ? { givens: args.givens } : {}),
      });
      return source.known && source.collated.size === 0
         ? undefined
         : "a query that reads a string under a collation";
   }
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
         async () => duckdbCollation(await connect(), sql),
      );
      return collation === "binary"
         ? undefined
         : "an instance that compares strings under a collation";
   }
   // Per column: only a named entry point's own paths can be probed (a
   // caller's inline `extend` or `include` has no name to compile a probe
   // over; the instance-wide check above needs none). A source imported from
   // another file is named too: Malloy hands its query the source's struct,
   // not its name, and `namedEntryOf` resolves it to the name only when it
   // is that source unchanged.
   if (entryName === undefined) return "no named entry point";
   if (!reads.entryPathsComplete) return "a scope from elsewhere";
   // A comparison of a string the query makes (a given, a literal, a cast,
   // a `pick`) compares under the database's default collation, which must
   // then meet the comparison's need as a column's would.
   if (!reads.comparesNoString && reads.comparesOnlyColumns !== true) {
      const fallback = await probeOnce(
         modelDef,
         `${connectionName}\u0000default`,
         async () => postgresDefaultCollation(await connect()),
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
   // Columns one value combines: live derives no single collation from two
   // different ones and fails, where the copy would answer.
   for (const group of reads.sameCollation ?? []) {
      const ids = new Set<string | undefined>();
      for (const path of group) {
         ids.add(
            await probeIdentityOnce(
               modelDef,
               `${connectionName}\u0000${path.join(".")}`,
               async () =>
                  postgresCollationIdentity(
                     await connect(),
                     unfinalizedPostgresSQL(
                        await probeSql({
                           runtime,
                           modelDef,
                           entryName,
                           path,
                           ...(args.givens ? { givens: args.givens } : {}),
                        }),
                     ),
                  ),
            ),
         );
      }
      if (ids.has(undefined) || ids.size > 1) {
         return `one value combines columns under different collations (${group.map((p) => p.join(".")).join(", ")})`;
      }
   }
   return undefined;
}

/** The collation probes admission reads through (`serve/read_admission.ts`). */
export const collationProbe: CollationProbe = {
   sourceRefusal: stringGateCollationRefusal,
   storedRefusal: storedTableCollationRefusal,
};

/**
 * The BigQuery catalog reads, by the loaded model's runtime, then what each
 * reads. Each query compiles a model of its own, so a compiled model would
 * read the catalog per query; the runtime is replaced whenever the package
 * reloads its models, which a reload does and every build's bind does
 * (`Package.reloadAllModels` hydrates new models, each with a runtime of its
 * own), so a build or a reload drops these. A failed or denied read fails
 * its askers closed and is kept for {@link BIGQUERY_FAILURE_TTL_MS}, so it is
 * not issued again on every query, then read again.
 */
const bigqueryProbes = new WeakMap<object, Map<string, Promise<unknown>>>();

/** How long a failed or denied catalog read answers live before it is retried. */
const BIGQUERY_FAILURE_TTL_MS = 60_000;

function bigqueryProbeOnce<T>(
   runtime: object,
   parts: readonly (string | undefined)[],
   run: () => Promise<T>,
): Promise<T> {
   let byKey = bigqueryProbes.get(runtime);
   if (!byKey) {
      byKey = new Map();
      bigqueryProbes.set(runtime, byKey);
   }
   const key = JSON.stringify(parts);
   let p = byKey.get(key) as Promise<T> | undefined;
   if (!p) {
      if (byKey.size >= 1000) byKey.clear();
      const held = byKey;
      const mine = run();
      p = mine;
      // A failure is kept for the TTL (no clock is read here: a timer drops
      // it), and only if nothing has replaced it since.
      mine.catch(() => {
         const timer = setTimeout(() => {
            if (held.get(key) === mine) held.delete(key);
         }, BIGQUERY_FAILURE_TTL_MS);
         (timer as { unref?: () => void }).unref?.();
      });
      byKey.set(key, mine);
   }
   return p;
}

/** A backquoted BigQuery name segment. */
function bqName(segment: string): string {
   return `\`${segment.replace(/[`\\]/g, "")}\``;
}

/** Most datasets one project-wide read lists before it proves nothing. */
const MAX_PROJECT_DATASETS = 10000;

/**
 * The datasets of BigQuery project `project` in region `location` that hold
 * a table with a collated column or `STRUCT` field (an empty set: none
 * does), or undefined when there are more than {@link MAX_PROJECT_DATASETS}.
 * One region-qualified `INFORMATION_SCHEMA` query over `COLUMNS` and
 * `COLUMN_FIELD_PATHS` (the region in lower case, as BigQuery's
 * documentation writes it: `region-us`), billed on demand at least 10 MB for
 * each view it reads, run in that region.
 */
async function bigqueryCollatedDatasets(
   connection: Connection,
   project: string,
   location: string,
): Promise<ReadonlySet<string> | undefined> {
   const schema = `${bqName(project)}.${bqName(`region-${location.toLowerCase()}`)}.INFORMATION_SCHEMA`;
   const found = await rows(
      connection,
      `SELECT DISTINCT table_schema FROM (SELECT table_schema FROM ${schema}.COLUMNS WHERE IFNULL(collation_name, '') != '' UNION ALL SELECT table_schema FROM ${schema}.COLUMN_FIELD_PATHS WHERE IFNULL(collation_name, '') != '') LIMIT ${MAX_PROJECT_DATASETS + 1}`,
   );
   if (found.length > MAX_PROJECT_DATASETS) return undefined;
   return new Set(found.map((r) => String(r.table_schema)));
}

/** A string literal for BigQuery SQL. */
function bqString(s: string): string {
   return `'${s.replace(/\\/g, "\\\\").replace(/'/g, "\\'")}'`;
}

/**
 * The type (`BASE TABLE`, `VIEW`, …) of each of `tables` (dataset, name) that
 * a region-qualified `INFORMATION_SCHEMA.TABLES` read of `project` in
 * `location` sees, by `dataset.name`; a table it does not see is absent.
 * One query, filtered to those tables, billed at least 10 MB.
 */
async function bigqueryTableTypes(
   connection: Connection,
   project: string,
   location: string,
   tables: readonly { dataset: string; name: string }[],
): Promise<ReadonlyMap<string, string>> {
   const schema = `${bqName(project)}.${bqName(`region-${location.toLowerCase()}`)}.INFORMATION_SCHEMA`;
   // A dataset or table name holds no dot, so `dataset.table` names one.
   const wanted = tables
      .map((t) => bqString(`${t.dataset}.${t.name}`))
      .join(", ");
   const found = await rows(
      connection,
      `SELECT table_schema, table_name, table_type FROM ${schema}.TABLES WHERE CONCAT(table_schema, '.', table_name) IN (${wanted})`,
   );
   return new Map(
      found.map((r) => [
         `${String(r.table_schema)}.${String(r.table_name)}`,
         String(r.table_type),
      ]),
   );
}

/** What one dataset's read sees of a table: its type and collated paths. */
interface DatasetTable {
   readonly type: string;
   readonly collated: ReadonlyMap<string, string>;
}

/**
 * Every table of BigQuery dataset `dataset` (`project.dataset`, or a dataset
 * of the job's own project) the read sees, with its type and its collated
 * columns and `STRUCT` fields by path. One `INFORMATION_SCHEMA` query over
 * `TABLES`, `COLUMNS` and `COLUMN_FIELD_PATHS`, billed at least 10 MB for
 * each.
 */
async function bigqueryDatasetTables(
   connection: Connection,
   dataset: string,
): Promise<ReadonlyMap<string, DatasetTable>> {
   const parts = dataset.split(".");
   if (parts.length > 2 || parts.some((p) => p === "")) {
      throw new Error("not a dataset");
   }
   const schema = `${parts.map(bqName).join(".")}.INFORMATION_SCHEMA`;
   const found = await rows(
      connection,
      `SELECT table_name, table_type, '' AS path, '' AS collation_name FROM ${schema}.TABLES UNION ALL SELECT table_name, '' AS table_type, column_name AS path, collation_name FROM ${schema}.COLUMNS WHERE IFNULL(collation_name, '') != '' UNION ALL SELECT table_name, '' AS table_type, field_path AS path, collation_name FROM ${schema}.COLUMN_FIELD_PATHS WHERE IFNULL(collation_name, '') != ''`,
   );
   const types = new Map<string, string>();
   const collated = new Map<string, Map<string, string>>();
   for (const r of found) {
      const table = String(r.table_name);
      const path = String(r.path ?? "");
      if (path === "") {
         types.set(table, String(r.table_type));
         continue;
      }
      let cols = collated.get(table);
      if (!cols) {
         cols = new Map();
         collated.set(table, cols);
      }
      cols.set(path, String(r.collation_name));
   }
   const out = new Map<string, DatasetTable>();
   for (const [table, type] of types) {
      out.set(table, { type, collated: collated.get(table) ?? new Map() });
   }
   return out;
}

/**
 * The table types whose columns hold their own collation in the catalog.
 * A view's (or a materialized view's, an external table's) columns are not
 * documented to carry the collation of what it reads.
 */
const TABLE_TYPES = new Set(["BASE TABLE", "CLONE", "SNAPSHOT"]);

/**
 * The locations of BigQuery project `project`'s datasets, through the
 * client the connection holds (`datasets.list`, a metadata call, which
 * lists only the datasets the caller may get).
 */
async function bigqueryDatasetLocations(
   connection: Connection,
   project: string,
): Promise<string[]> {
   const client = (
      connection as unknown as {
         bigQuery?: {
            getDatasets(options: {
               projectId: string;
               autoPaginate: boolean;
            }): Promise<[{ metadata?: { location?: unknown } }[]]>;
         };
      }
   ).bigQuery;
   if (!client || typeof client.getDatasets !== "function") {
      throw new Error("no BigQuery client");
   }
   const [datasets] = await client.getDatasets({
      projectId: project,
      autoPaginate: true,
   });
   const locations = new Set<string>();
   for (const d of datasets) {
      const l = d.metadata?.location;
      if (typeof l !== "string" || l === "") throw new Error("no location");
      locations.add(l.toLowerCase());
   }
   return [...locations];
}

/** A string that can be a BigQuery project ID (a domain-scoped one too). */
const PROJECT_ID = /^(?:[a-z0-9.-]+:)?[a-z][a-z0-9-]{4,28}[a-z0-9]$/;

/**
 * The projects SQL text may read a table of: the first segment of every
 * name of three or more dotted segments that can be a project ID. Over-reads
 * on purpose (a dotted literal, a struct path whose alias looks like a
 * project): each one is then checked too, and one that cannot be read
 * answers live.
 */
function sqlProjects(sql: string): string[] {
   const unquoted = sql.split("`").join("");
   const out = new Set<string>();
   for (const [token] of unquoted.matchAll(/[A-Za-z0-9_:.$*-]+/g)) {
      const segments = token.split(".");
      if (segments.length >= 3 && PROJECT_ID.test(segments[0])) {
         out.add(segments[0]);
      }
   }
   return [...out];
}

/** What a BigQuery connection runs jobs as: its projects and location. */
function bigqueryJobContext(connection: Connection): {
   projects: string[];
   location: string | undefined;
} {
   const c = connection as unknown as {
      projectId?: unknown;
      billingProjectId?: unknown;
      location?: unknown;
   };
   // The client's `{{projectId}}` placeholder stands for a project not
   // resolved yet: no project to read.
   const projects = [c.projectId, c.billingProjectId].filter(
      (p): p is string =>
         typeof p === "string" && p !== "" && !p.includes("{{"),
   );
   return {
      projects: [...new Set(projects)],
      location:
         typeof c.location === "string" && c.location !== ""
            ? c.location.toLowerCase()
            : undefined,
   };
}

/** A BigQuery table path placed: the projects it may be in, its dataset, its name. */
interface PlacedTable {
   readonly projects: readonly string[];
   /** As written: `dataset` or `project.dataset`. */
   readonly dataset: string;
   /** The dataset's own name. */
   readonly datasetName: string;
   readonly name: string;
}

/** `path` placed under the job's `projects`, or undefined when it cannot be. */
function placeTable(
   path: string,
   projects: readonly string[],
): PlacedTable | undefined {
   const segments = path.split("`").join("").split(".");
   if (segments.length < 2 || segments.length > 3) return undefined;
   const name = segments[segments.length - 1];
   // A wildcard (`events_*`) names no one table the catalog lists.
   if (name.includes("*")) return undefined;
   return {
      projects: segments.length === 3 ? [segments[0]] : projects,
      dataset: segments.slice(0, -1).join("."),
      datasetName: segments[segments.length - 2],
      name,
   };
}

/**
 * The catalog reads one BigQuery query's checks go through: its
 * connection, and each project's regions and collated datasets, read once
 * per loaded model.
 */
class BigQueryCatalog {
   readonly job: ReturnType<typeof bigqueryJobContext>;

   constructor(
      private readonly runtime: object,
      readonly connection: Connection,
      private readonly connectionName: string | undefined,
   ) {
      this.job = bigqueryJobContext(connection);
   }

   /**
    * `project`'s regions: the connection's `location`, or each location
    * `datasets.list` names; undefined when neither can be read, or when it
    * lists no dataset (no region to prove anything in).
    */
   async regions(project: string): Promise<string[] | undefined> {
      if (this.job.location !== undefined) return [this.job.location];
      try {
         const regions = await bigqueryProbeOnce(
            this.runtime,
            [this.connectionName, "locations", project],
            () => bigqueryDatasetLocations(this.connection, project),
         );
         return regions.length > 0 ? regions : undefined;
      } catch {
         return undefined;
      }
   }

   /**
    * `project`'s collated datasets, across its regions; undefined when they
    * cannot be read or proved (a failed read, too many datasets, no region).
    */
   async collatedDatasets(
      project: string,
   ): Promise<ReadonlySet<string> | undefined> {
      const regions = await this.regions(project);
      if (regions === undefined) return undefined;
      const out = new Set<string>();
      for (const region of regions) {
         let r: ReadonlySet<string> | undefined;
         try {
            r = await bigqueryProbeOnce(
               this.runtime,
               [this.connectionName, "project", project, region],
               () => bigqueryCollatedDatasets(this.connection, project, region),
            );
         } catch {
            return undefined;
         }
         if (r === undefined) return undefined;
         for (const d of r) out.add(d);
      }
      return out;
   }

   /**
    * Each of `tables`' type, read project-wide in each of its projects'
    * regions, by `project.dataset.name`; `no_region` when a project has no
    * region to read; undefined when a read fails.
    */
   async tableTypes(
      tables: readonly PlacedTable[],
   ): Promise<Map<string, string> | "no_region" | undefined> {
      const out = new Map<string, string>();
      const byProject = new Map<string, PlacedTable[]>();
      for (const t of tables) {
         for (const p of t.projects) {
            byProject.set(p, [...(byProject.get(p) ?? []), t]);
         }
      }
      for (const [project, list] of byProject) {
         const regions = await this.regions(project);
         if (regions === undefined) return "no_region";
         const wanted = list
            .map((t) => ({ dataset: t.datasetName, name: t.name }))
            .sort((a, b) =>
               `${a.dataset}.${a.name}` < `${b.dataset}.${b.name}` ? -1 : 1,
            );
         for (const region of regions) {
            let types: ReadonlyMap<string, string>;
            try {
               types = await bigqueryProbeOnce(
                  this.runtime,
                  [
                     this.connectionName,
                     "tables",
                     project,
                     region,
                     ...wanted.map((w) => `${w.dataset}.${w.name}`),
                  ],
                  () =>
                     bigqueryTableTypes(
                        this.connection,
                        project,
                        region,
                        wanted,
                     ),
               );
            } catch {
               return undefined;
            }
            for (const [k, v] of types) out.set(`${project}.${k}`, v);
         }
      }
      return out;
   }

   /** Dataset `dataset`'s tables, read once per loaded model. */
   datasetTables(dataset: string): Promise<ReadonlyMap<string, DatasetTable>> {
      return bigqueryProbeOnce(
         this.runtime,
         [this.connectionName, "dataset", dataset],
         () => bigqueryDatasetTables(this.connection, dataset),
      );
   }
}

/**
 * What a BigQuery query's strings compare under. `known: false` (answer
 * live): its SQL names a `COLLATE`; a read failed or was denied (and is
 * then not issued again for {@link BIGQUERY_FAILURE_TTL_MS}); a table it
 * reads is not one a read saw; a view (or a materialized view, an external
 * table) it reads is in a project holding a collated column; it holds
 * author SQL (an `.sql()` source, a raw SQL expression) and a project it
 * may read holds a collated column, or could not be read; or a table path
 * this cannot place. Else `collated`: each collated column and field of
 * the tables it reads, by path, with its collations; empty, every string it
 * reads compares byte-exact.
 *
 * Each project is read project-wide, once per region it has datasets in
 * (the connection's `location`, or each location `datasets.list` names):
 * one query lists the datasets holding a collated column, and one, filtered
 * to the tables Malloy says the query reads, confirms each of them and its
 * type, so a hidden table is never taken for byte-exact (BigQuery documents
 * that a region-qualified read needs permission on the project, not which
 * rows a caller permitted on less sees). A table in a collated dataset is
 * read again with its dataset. With no region to read (no `location`, and
 * `datasets.list` unreachable or empty), each dataset a `.table()` source
 * reads is read on its own, and a view or author SQL answers live. A
 * default collation (a table's, a dataset's) is taken to be resolved into
 * each column's own, which BigQuery does not document.
 *
 * The residual risk is author SQL and views over a project whose reads list
 * no collated column: their tables are not known, so a table the
 * credentials may query but whose metadata no read lists would be compared
 * byte-exact.
 */
async function bigquerySource(args: {
   authorRunnable: QueryMaterializer;
   givens?: Record<string, unknown>;
}): Promise<
   | { known: false }
   | {
        known: true;
        collated: ReadonlyMap<string, ReadonlySet<string>>;
        catalog: BigQueryCatalog;
     }
> {
   const unknown = { known: false } as const;
   try {
      const prepared = await readPreparedQuery(args.authorRunnable);
      const modelDef = prepared.modelDef as ModelDef | undefined;
      if (!modelDef) return unknown;
      const struct = resolveStructRefSource(
         prepared.structRef as never,
         modelDef as never,
      ) as { connection?: unknown } | undefined;
      const runtime = (
         args.authorRunnable as unknown as { runtime?: ProbeRuntime }
      ).runtime;
      if (!runtime) return unknown;
      const { sql } = await args.authorRunnable.getPreparedResult(
         (args.givens ? { givens: args.givens } : {}) as never,
      );
      if (/\bcollate\b/i.test(sql)) return unknown;
      const connectionName =
         typeof struct?.connection === "string" ? struct.connection : undefined;
      const catalog = new BigQueryCatalog(
         runtime,
         await runtime.connections.lookupConnection(connectionName),
         connectionName,
      );
      const { job } = catalog;
      if (job.projects.length === 0) return unknown;
      const tables = queryTableReads(
         prepared.compositeResolvedSourceDef ?? struct,
         prepared.pipeline,
      );
      const placed: PlacedTable[] = [];
      for (const t of tables ?? []) {
         // A table on another connection is not this query's to place.
         if (
            connectionName !== undefined &&
            t.connectionName !== connectionName
         ) {
            return unknown;
         }
         const p = placeTable(t.table, job.projects);
         if (p === undefined) return unknown;
         placed.push(p);
      }
      // Whether every project in `projects` is read and holds no collated
      // column: what author SQL and a view are served under.
      const clean = async (projects: Iterable<string>) => {
         for (const project of projects) {
            const d = await catalog.collatedDatasets(project);
            if (d === undefined || d.size > 0) return false;
         }
         return true;
      };
      if (tables === undefined) {
         const projects = new Set([...job.projects, ...sqlProjects(sql)]);
         return (await clean(projects))
            ? { known: true, collated: new Map(), catalog }
            : unknown;
      }
      const types = await catalog.tableTypes(placed);
      // A failed or denied read answers live; only a project with no region
      // to read is read dataset by dataset.
      if (types === undefined) return unknown;
      const collated = new Map<string, Set<string>>();
      for (const t of placed) {
         if (types !== "no_region") {
            // Coverage, by table: the project-wide read must see it.
            const type = t.projects
               .map((p) => types.get(`${p}.${t.datasetName}.${t.name}`))
               .find((x) => x !== undefined);
            if (type === undefined) return unknown;
            if (!TABLE_TYPES.has(type)) {
               if (!(await clean(t.projects))) return unknown;
               continue;
            }
            let inCollated = false;
            for (const p of t.projects) {
               const d = await catalog.collatedDatasets(p);
               if (d === undefined) return unknown;
               if (d.has(t.datasetName)) inCollated = true;
            }
            if (!inCollated) continue;
         }
         const datasetTable = (await catalog.datasetTables(t.dataset)).get(
            t.name,
         );
         // Coverage, by table: the dataset's read must see it, as a table.
         if (
            datasetTable === undefined ||
            !TABLE_TYPES.has(datasetTable.type)
         ) {
            return unknown;
         }
         for (const [path, c] of datasetTable.collated) {
            let set = collated.get(path);
            if (!set) {
               set = new Set();
               collated.set(path, set);
            }
            set.add(c);
         }
      }
      return { known: true, collated, catalog };
   } catch {
      return unknown;
   }
}

/**
 * A BigQuery database table, judged against its source: kept only when its
 * own collated columns and fields are exactly the source's, path for path
 * and collation for collation, an empty set on both sides included (a
 * build's `CREATE TABLE AS` is not known to keep or drop a collation, and a
 * source replaced after the build may have lost the one its copy kept).
 * The table's dataset is read once per loaded model when its project-wide
 * read lists it as holding no collated column (a build reloads the models,
 * so a rebuilt table is read again); else on every answer. Unknown whenever
 * either side cannot be read.
 */
async function bigqueryDatabaseTableCollation(
   t: StoredTable,
   args: {
      authorRunnable: QueryMaterializer;
      givens?: Record<string, unknown>;
   },
   connection: Connection,
   connectionName: string | undefined,
   memo: Map<string, Promise<unknown>>,
): Promise<Collation> {
   const source = await bigquerySource(args);
   if (!source.known) return "unknown";
   const tablePath = t.tablePath;
   if (tablePath === undefined) return "unknown";
   const placed = placeTable(tablePath, source.catalog.job.projects);
   if (placed === undefined) return "unknown";
   if (source.collated.size === 0) {
      // A table in a dataset no project-wide read lists as collated, and
      // seen by the read filtered to it, holds no collated column.
      const types = await source.catalog.tableTypes([placed]);
      if (types === undefined) return "unknown";
      const type =
         types === "no_region"
            ? undefined
            : placed.projects
                 .map((p) =>
                    types.get(`${p}.${placed.datasetName}.${placed.name}`),
                 )
                 .find((x) => x !== undefined);
      let listed = types === "no_region";
      for (const p of placed.projects) {
         const d = await source.catalog.collatedDatasets(p);
         if (d === undefined || d.has(placed.datasetName)) listed = true;
      }
      if (!listed && type !== undefined && TABLE_TYPES.has(type)) {
         return "binary";
      }
   }
   // The stored side, read on every answer (once per answer).
   const stored = (
      await memoized(
         memo,
         ["bigquery-dataset", connectionName, placed.dataset],
         () => bigqueryDatasetTables(connection, placed.dataset),
      )
   ).get(placed.name);
   if (stored === undefined || !TABLE_TYPES.has(stored.type)) return "unknown";
   for (const [path, collations] of source.collated) {
      if (
         collations.size !== 1 ||
         !collations.has(stored.collated.get(path) ?? "")
      ) {
         return "unknown";
      }
   }
   for (const [path, c] of stored.collated) {
      if (!source.collated.get(path)?.has(c)) return "unknown";
   }
   return "binary";
}

/** Each column's collation identity, by model, then connection and path. */
const identities = new WeakMap<
   object,
   Map<string, Promise<string | undefined>>
>();

function probeIdentityOnce(
   modelDef: object,
   key: string,
   run: () => Promise<string | undefined>,
): Promise<string | undefined> {
   let byKey = identities.get(modelDef);
   if (!byKey) {
      byKey = new Map();
      identities.set(modelDef, byKey);
   }
   let p = byKey.get(key);
   if (!p) {
      if (byKey.size >= 1000) byKey.clear();
      p = run().catch(() => undefined);
      byKey.set(key, p);
   }
   return p;
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
   // Read query-wide, never per column.
   if (read === undefined || read === "dataset") return "unknown";
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

/**
 * Publisher's own catalog read, admitted past a locked DuckDB instance's gate,
 * which refuses settings reads to caller and author SQL (duckdb_lockdown.ts).
 */
async function rows(
   connection: Connection,
   sql: string,
): Promise<Rows["rows"]> {
   const result = (await runAsInternalStatement(sql, () =>
      connection.runSQL(sql),
   )) as unknown as Rows;
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
 * binary, whatever `datcollate` says), any other by its own row. Binary under
 * a libc `C`, `POSIX` or `C.UTF-8`, and under the builtin provider (Postgres 17+:
 * `pg_c_utf8`, `ucs_basic`, `pg_unicode_fast`, or a database created with
 * it), which orders and compares by code point, as UTF-8 bytes do.
 */
export function postgresJudged(
   row: Record<string, unknown> | undefined,
): Collation {
   if (!row) return "unknown";
   // libc `C`, `POSIX`, and `C.UTF-8` (its codeset spelled any way; the
   // locale name itself compares exactly),
   // which glibc orders by code point, as UTF-8 bytes do.
   const binaryName = (c: unknown) =>
      typeof c === "string" &&
      (c === "C" || c === "POSIX" || /^C\.[Uu][Tt][Ff]-?8$/.test(c));
   if (row.name === "default") {
      if (row.dbp === "b") return "binary";
      return row.dbp !== "i" && binaryName(row.dbc)
         ? "binary"
         : "deterministic";
   }
   if (row.det !== true) return "unknown";
   if (row.prov === "b") return "binary";
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
   /** This answer's stored-side reads, shared across its comparisons. */
   storedReads?: Map<string, Promise<unknown>>;
}): Promise<string | undefined> {
   if (args.tables.length === 0) return undefined;
   const memo = args.storedReads ?? new Map<string, Promise<unknown>>();
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
         collation = await storedSide(t, args, runtime, sourceDialect, memo);
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
         memo,
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
export function namedEntryOf(
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
   /** This answer's stored-side reads, shared across its comparisons. */
   memo: Map<string, Promise<unknown>>;
   givens?: Record<string, unknown>;
}): Promise<string | undefined> {
   const { reads, memo } = args;
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
   const sourceDefault = await memoized(
      memo,
      ["source-default", connectionName],
      () => postgresDefaultIdentity(source),
   );
   if (sourceDefault === undefined) return "the source's default unread";
   // Every column the stored tables hold, by name: its collation's identity.
   const held = new Map<string, string | null>();
   for (const t of args.tables) {
      if (t.tablePath === undefined) return "a table with no path";
      const storedName = t.destinationName ?? connectionName;
      const connection =
         await args.storedRuntime.connections.lookupConnection(storedName);
      const storedDefault = await memoized(
         memo,
         ["stored-default", storedName],
         () => postgresDefaultIdentity(connection),
      );
      if (storedDefault !== sourceDefault) {
         return "a stored table's connection has another default collation";
      }
      const tablePath = t.tablePath;
      for (const [col, id] of await memoized(
         memo,
         ["columns", storedName, tablePath],
         () => postgresColumnIdentities(connection, tablePath),
      )) {
         held.set(col, id);
      }
   }
   // A result read of a source known to be read live, which no stored table
   // holds under any name it goes by, is no stored column: it is passed
   // over, path and column both. A gate's reads never are (no `livePaths`).
   const live = new Set((reads.livePaths ?? []).map((p) => p.join("\u0000")));
   const kept = reads.entryPaths
      .map((path, i) => ({ path, names: reads.columns[i] ?? [] }))
      .filter(
         ({ path, names }) =>
            !live.has(path.join("\u0000")) || names.some((n) => held.has(n)),
      );
   const paths = stringPaths(
      struct,
      kept.map((k) => k.path),
   );
   if (paths === undefined) return "an unreadable gate path";
   const compared = new Set<string>();
   for (const path of paths) {
      const id = await memoized(
         memo,
         ["source", connectionName, path.join(".")],
         async () =>
            postgresCollationIdentity(
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
            ),
      );
      if (id === undefined) return `\`${path.join(".")}\` unread`;
      compared.add(id);
   }
   // The names the entry point's own `where:` reads, and whether a rollup's
   // table was read: a rollup is built with its base's own `where:` applied.
   const ownWhere = new Set(fieldPathsIn(struct?.filterList).flat());
   const rollupRead = args.tables.some(
      (t) => (t as { kind?: unknown }).kind === "rollup",
   );
   let storedStrings = 0;
   const columns = live.size === 0 ? reads.columns : kept.map((k) => k.names);
   for (const names of columns) {
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
      givens?: Record<string, unknown>;
   },
   runtime: ProbeRuntime,
   sourceDialect: string,
   memo: Map<string, Promise<unknown>>,
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
      const tablePath = t.tablePath;
      if (connectionName === undefined || tablePath === undefined) {
         return "unknown";
      }
      return memoized(
         memo,
         ["duckdb-table", connectionName, tablePath],
         async () =>
            duckdbTableCollation(
               await runtime.connections.lookupConnection(connectionName),
               tablePath,
            ),
      );
   }
   const read = capabilitiesOf(sourceDialect).stringCollation;
   if (read !== "instance" && read !== "column" && read !== "dataset") {
      return "binary";
   }
   const connectionName =
      t.destinationName ?? (await entryConnectionOf(args.authorRunnable));
   const connection =
      await runtime.connections.lookupConnection(connectionName);
   if (read === "dataset") {
      return bigqueryDatabaseTableCollation(
         t,
         args,
         connection,
         connectionName,
         memo,
      );
   }
   const tablePath = t.tablePath;
   if (tablePath === undefined) {
      // No path to read the table's own columns by: the instance-wide check
      // for DuckDB, and unknown elsewhere.
      return read === "instance"
         ? memoized(memo, ["duckdb-instance", connectionName], () =>
              duckdbCollation(connection, ""),
           )
         : "unknown";
   }
   return memoized(memo, [read, connectionName, tablePath], () =>
      read === "instance"
         ? duckdbTableCollation(connection, tablePath)
         : postgresTableCollation(connection, tablePath),
   );
}

/**
 * `run`'s answer, read once per `parts` in `memo` (one answer's stored-side
 * reads): a failed read is kept too, and fails each asker closed.
 */
function memoized<T>(
   memo: Map<string, Promise<unknown>>,
   parts: readonly (string | undefined)[],
   run: () => Promise<T>,
): Promise<T> {
   const key = JSON.stringify(parts);
   let p = memo.get(key) as Promise<T> | undefined;
   if (p === undefined) {
      p = run();
      // Read once even when it fails; each awaiter sees the failure.
      p.catch(() => undefined);
      memo.set(key, p);
   }
   return p;
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
