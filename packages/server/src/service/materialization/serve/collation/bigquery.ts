// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The BigQuery dialect's collation reads: a project's collated datasets
 * (read region-qualified from `INFORMATION_SCHEMA`), the types of the tables
 * a query reads, and a dataset's tables with their collated columns and
 * `STRUCT` fields, each through the ProbeIO. A column's collation is read
 * from the catalog, never from a query, so BigQuery is read query-wide.
 *
 * The reads are kept in a map the caller holds per loaded model's runtime
 * and bind (`gate_collation.ts`, `source_capability.ts`), for as long as
 * that bind serves; nothing expires on a clock. This module keeps none.
 */

import { quoteIdentifier } from "../../../quoting";
import type { ProbeIO } from "./probe_io";

/**
 * `run`'s answer, read once per `parts` in `byKey` (the reads of one runtime
 * and bind, which the caller keeps); a failed read is kept too, and answers
 * live until the next bind. Nothing expires on a clock: a catalog changed in
 * place is read again at the next bind (`source_capability.ts` reads it at
 * every bind for the lookups).
 */
function bigqueryProbeOnce<T>(
   byKey: Map<string, Promise<unknown>>,
   parts: readonly (string | undefined)[],
   run: () => Promise<T>,
): Promise<T> {
   const key = JSON.stringify(parts);
   let p = byKey.get(key) as Promise<T> | undefined;
   if (!p) {
      // Bounded: at most this many entries per bind.
      if (byKey.size >= 1000) byKey.clear();
      p = run();
      p.catch(() => undefined);
      byKey.set(key, p);
   }
   return p;
}

/** A name segment quoted as `io`'s dialect quotes it (BigQuery's backquotes). */
function bqName(io: ProbeIO, segment: string): string {
   return quoteIdentifier(segment, io.dialectName);
}

/**
 * A BigQuery catalog predicate: `collation_name` names a collation. BigQuery
 * reports an uncollated column's as the string `'NULL'`, not as NULL, so
 * that (and NULL and `''`) reads as none.
 */
const BQ_COLLATED = "NULLIF(NULLIF(collation_name, 'NULL'), '') IS NOT NULL";

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
   io: ProbeIO,
   project: string,
   location: string,
): Promise<ReadonlySet<string> | undefined> {
   const schema = `${bqName(io, project)}.${bqName(io, `region-${location.toLowerCase()}`)}.INFORMATION_SCHEMA`;
   const found = await io.completeRows(
      `SELECT DISTINCT table_schema FROM (SELECT table_schema FROM ${schema}.COLUMNS WHERE ${BQ_COLLATED} UNION ALL SELECT table_schema FROM ${schema}.COLUMN_FIELD_PATHS WHERE ${BQ_COLLATED}) LIMIT ${MAX_PROJECT_DATASETS + 1}`,
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
   io: ProbeIO,
   project: string,
   location: string,
   tables: readonly { dataset: string; name: string }[],
): Promise<ReadonlyMap<string, string>> {
   const schema = `${bqName(io, project)}.${bqName(io, `region-${location.toLowerCase()}`)}.INFORMATION_SCHEMA`;
   // A dataset or table name holds no dot, so `dataset.table` names one.
   const wanted = tables
      .map((t) => bqString(`${t.dataset}.${t.name}`))
      .join(", ");
   const found = await io.completeRows(
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
   io: ProbeIO,
   dataset: string,
): Promise<ReadonlyMap<string, DatasetTable>> {
   const parts = dataset.split(".");
   if (parts.length > 2 || parts.some((p) => p === "")) {
      throw new Error("not a dataset");
   }
   const schema = `${parts.map((p) => bqName(io, p)).join(".")}.INFORMATION_SCHEMA`;
   const found = await io.completeRows(
      `SELECT table_name, table_type, '' AS path, '' AS collation_name FROM ${schema}.TABLES UNION ALL SELECT table_name, '' AS table_type, column_name AS path, collation_name FROM ${schema}.COLUMNS WHERE ${BQ_COLLATED} UNION ALL SELECT table_name, '' AS table_type, field_path AS path, collation_name FROM ${schema}.COLUMN_FIELD_PATHS WHERE ${BQ_COLLATED}`,
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
export const TABLE_TYPES = new Set(["BASE TABLE", "CLONE", "SNAPSHOT"]);

/** A string that can be a BigQuery project ID (a domain-scoped one too). */
const PROJECT_ID = /^(?:[a-z0-9.-]+:)?[a-z][a-z0-9-]{4,28}[a-z0-9]$/;

/**
 * The projects SQL text may read a table of: the first segment of every
 * name of three or more dotted segments that can be a project ID. Over-reads
 * on purpose (a dotted literal, a struct path whose alias looks like a
 * project): each one is then checked too, and one that cannot be read
 * answers live.
 */
export function sqlProjects(sql: string): string[] {
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

/** A BigQuery table path placed: the projects it may be in, its dataset, its name. */
export interface PlacedTable {
   readonly projects: readonly string[];
   /** As written: `dataset` or `project.dataset`. */
   readonly dataset: string;
   /** The dataset's own name. */
   readonly datasetName: string;
   readonly name: string;
}

/** `path` placed under the job's `projects`, or undefined when it cannot be. */
export function placeTable(
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
export class BigQueryCatalog {
   readonly job: ReturnType<ProbeIO["jobContext"]>;

   constructor(
      /** The reads of one runtime and bind, which the caller keeps. */
      private readonly reads: Map<string, Promise<unknown>>,
      readonly io: ProbeIO,
      private readonly connectionName: string | undefined,
   ) {
      this.job = io.jobContext();
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
            this.reads,
            [this.connectionName, "locations", project],
            () => this.io.datasetLocations(project),
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
               this.reads,
               [this.connectionName, "project", project, region],
               () => bigqueryCollatedDatasets(this.io, project, region),
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
                  this.reads,
                  [
                     this.connectionName,
                     "tables",
                     project,
                     region,
                     ...wanted.map((w) => `${w.dataset}.${w.name}`),
                  ],
                  () => bigqueryTableTypes(this.io, project, region, wanted),
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
         this.reads,
         [this.connectionName, "dataset", dataset],
         () => bigqueryDatasetTables(this.io, dataset),
      );
   }
}

/**
 * The BigQuery dialect's reader: read query-wide from the catalog
 * ({@link BigQueryCatalog}); it has no per-column or per-table read.
 */
export const BIGQUERY_COLLATION = Object.freeze({ scope: "dataset" as const });
