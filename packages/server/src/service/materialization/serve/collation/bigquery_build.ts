// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The build side of BigQuery collation: whether a `storage=` build's
 * warehouse SQL reads only strings compared byte-exact, from the catalog
 * reads of the tables it reads. Read once per build, never on a query's path.
 */

import type { Connection, PersistSource } from "@malloydata/malloy";
import { sourceReads } from "../../../governance/ir/source_reads";
import { type CatalogIO, catalogIO } from "./catalog_io";
import {
   BigQueryCatalog,
   type PlacedTable,
   placeTable,
   sqlProjects,
   TABLE_TYPES,
} from "./bigquery";

/**
 * Whether a `storage=` build's warehouse SQL on BigQuery reads only strings
 * compared byte-exact: `sql`, the build of `source`, reading the tables its
 * definition reads (unknown: every project the SQL names is read instead),
 * judged by the catalog reads an answer's check makes
 * ({@link bigqueryCollatedPaths}), complete or not at all. False whenever a
 * read fails or is incomplete: the build's column contract then records its
 * strings compared `none`, which fails closed.
 */
export async function bigqueryBuildCompareBinary(args: {
   connection: Connection;
   source: PersistSource;
   sql: string;
}): Promise<boolean> {
   try {
      const read = await bigqueryCollatedPaths({
         reads: new Map(),
         io: catalogIO(args.connection),
         connectionName: args.source.connectionName,
         sql: args.sql,
         tables: sourceReads(args.source),
      });
      return read.known && read.collated.size === 0;
   } catch {
      return false;
   }
}

/**
 * The collated column paths of the tables BigQuery `sql` reads, from the
 * catalog through `io`, or unknown when they cannot be read whole.
 */
async function bigqueryCollatedPaths(args: {
   reads: Map<string, Promise<unknown>>;
   io: CatalogIO;
   connectionName: string | undefined;
   sql: string;
   tables: ReturnType<typeof sourceReads>;
}): Promise<
   | { known: false }
   | {
        known: true;
        collated: ReadonlyMap<string, ReadonlySet<string>>;
        catalog: BigQueryCatalog;
     }
> {
   const unknown = { known: false } as const;
   const { sql, tables, connectionName } = args;
   try {
      if (/\bcollate\b/i.test(sql)) return unknown;
      const catalog = new BigQueryCatalog(args.reads, args.io, connectionName);
      const { job } = catalog;
      if (job.projects.length === 0) return unknown;
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
