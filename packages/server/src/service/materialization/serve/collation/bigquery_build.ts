// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The build side of BigQuery collation: whether a `storage=` build's
 * warehouse SQL reads only strings compared byte-exact, from the catalog
 * reads of the tables it reads. Read once per build, never on a query's path.
 */

import type { Connection, PersistSource } from "@malloydata/malloy";
import type { BigQueryBuildUnread } from "../../domain/collation";
import { sourceReads } from "../../../governance/ir/source_reads";
import { type CatalogIO, catalogIO } from "./catalog_io";
import {
   BigQueryCatalog,
   MAX_DATASET_READS,
   type PlacedTable,
   placeTable,
   sqlProjects,
   TABLE_TYPES,
   withJobContext,
} from "./bigquery";

/**
 * Whether a `storage=` build's warehouse SQL on BigQuery reads only strings
 * compared byte-exact: `sql`, the build of `source`, reading the tables its
 * definition reads, judged from the catalog ({@link bigqueryCollatedPaths}),
 * complete or not at all. Each dataset those tables are in is read in its
 * own `INFORMATION_SCHEMA`, which a dataset's READER may read, up to
 * {@link MAX_DATASET_READS} of them; a source whose tables are not
 * known up front (author SQL, which may read any dataset), or one reading
 * more datasets, is read project-wide in each region, which needs the
 * project's `bigquery.tables.list`. False whenever a
 * read fails or is incomplete: the build's column contract then records its
 * strings compared `none`, which fails closed. Each such false tells
 * `onUnread` why and where ({@link BigQueryBuildUnread}), since a contract
 * recorded `none` refuses every `storage=` table built over it that compares
 * those strings.
 */
export async function bigqueryBuildCompareBinary(args: {
   connection: Connection;
   source: PersistSource;
   sql: string;
   /** Told why, when a read failed or saw too little (the caller logs it). */
   onUnread?: (unread: BigQueryBuildUnread) => void;
   /** {@link MAX_DATASET_READS}, which a spec lowers. */
   datasetReadLimit?: number;
}): Promise<boolean> {
   const io = catalogIO(args.connection);
   let read: Awaited<ReturnType<typeof bigqueryCollatedPaths>>;
   try {
      read = await bigqueryCollatedPaths({
         reads: new Map(),
         io,
         connectionName: args.source.connectionName,
         sql: args.sql,
         tables: sourceReads(args.source),
         datasetReadLimit: args.datasetReadLimit ?? MAX_DATASET_READS,
      });
   } catch {
      read = { known: false, unread: { reason: "read_failed" } };
   }
   if (!read.known) {
      args.onUnread?.(withJobContext(read.unread, io));
      return false;
   }
   return read.collated.size === 0;
}

/**
 * The collated column paths of the tables BigQuery `sql` reads, from the
 * catalog through `io`, or unknown, with what did not read, when they
 * cannot be read whole.
 */
async function bigqueryCollatedPaths(args: {
   reads: Map<string, Promise<unknown>>;
   io: CatalogIO;
   connectionName: string | undefined;
   sql: string;
   tables: ReturnType<typeof sourceReads>;
   datasetReadLimit: number;
}): Promise<
   | { known: false; unread: BigQueryBuildUnread }
   | {
        known: true;
        collated: ReadonlyMap<string, ReadonlySet<string>>;
        catalog: BigQueryCatalog;
     }
> {
   const unknown = (unread: BigQueryBuildUnread) =>
      ({ known: false, unread }) as const;
   const { sql, tables, connectionName } = args;
   // The table being read, for a read that throws.
   let at: Omit<BigQueryBuildUnread, "reason"> = {};
   try {
      if (/\bcollate\b/i.test(sql))
         return unknown({ reason: "collate_in_sql" });
      const catalog = new BigQueryCatalog(args.reads, args.io, connectionName);
      const { job } = catalog;
      if (job.projects.length === 0) {
         return unknown({ reason: "no_job_project" });
      }
      const placed: PlacedTable[] = [];
      for (const t of tables ?? []) {
         // A table on another connection is not this query's to place.
         if (
            connectionName !== undefined &&
            t.connectionName !== connectionName
         ) {
            return unknown({ reason: "table_on_other_connection" });
         }
         const p = placeTable(t.table, job.projects);
         if (p === undefined) return unknown({ reason: "table_unplaceable" });
         placed.push(p);
      }
      // Why not every project in `projects` is read and free of collated
      // columns, or undefined when every one is: what author SQL and a view
      // are served under.
      const unclean = async (
         projects: Iterable<string>,
         where: Omit<BigQueryBuildUnread, "reason" | "project"> = {},
      ): Promise<BigQueryBuildUnread | undefined> => {
         for (const project of projects) {
            const d = await catalog.collatedDatasets(project);
            if (d === undefined) {
               return { reason: "project_unread", project, ...where };
            }
            if (d.size > 0) {
               return { reason: "project_collated", project, ...where };
            }
         }
         return undefined;
      };
      if (tables === undefined) {
         const projects = new Set([...job.projects, ...sqlProjects(sql)]);
         const why = await unclean(projects);
         return why
            ? unknown(why)
            : { known: true, collated: new Map(), catalog };
      }
      // Every table known: each dataset's own catalog, which a dataset's
      // READER may read, unlike the project-wide region views.
      if (new Set(placed.map((t) => t.dataset)).size <= args.datasetReadLimit) {
         const collated = new Map<string, Set<string>>();
         for (const t of placed) {
            const where = { dataset: t.datasetName, table: t.name };
            at = { project: t.projects[0], ...where };
            let listed: Awaited<ReturnType<BigQueryCatalog["datasetTables"]>>;
            try {
               listed = await catalog.datasetTables(t.dataset);
            } catch {
               return unknown({ reason: "dataset_unread", ...at });
            }
            const datasetTable = listed.get(t.name);
            if (datasetTable === undefined) {
               return unknown({ reason: "dataset_table_not_listed", ...at });
            }
            // A view's columns carry no collation of what it reads: proved
            // only by its whole project holding no collated column.
            if (!TABLE_TYPES.has(datasetTable.type)) {
               const why = await unclean(t.projects, {
                  ...where,
                  tableType: datasetTable.type,
               });
               if (why) return unknown(why);
               continue;
            }
            addCollated(collated, datasetTable.collated);
         }
         return { known: true, collated, catalog };
      }
      const types = await catalog.tableTypes(placed);
      // A failed or denied read answers live; only a project with no region
      // to read is read dataset by dataset.
      if (types === undefined) {
         return unknown({
            reason: "table_types_unread",
            project: placed[0]?.projects[0],
         });
      }
      const collated = new Map<string, Set<string>>();
      for (const t of placed) {
         const where = { dataset: t.datasetName, table: t.name };
         at = { project: t.projects[0], ...where };
         if (types !== "no_region") {
            // Coverage, by table: the project-wide read must see it.
            const type = t.projects
               .map((p) => types.get(`${p}.${t.datasetName}.${t.name}`))
               .find((x) => x !== undefined);
            if (type === undefined) {
               return unknown({ reason: "table_not_listed", ...at });
            }
            if (!TABLE_TYPES.has(type)) {
               const why = await unclean(t.projects, {
                  ...where,
                  tableType: type,
               });
               if (why) return unknown(why);
               continue;
            }
            let inCollated = false;
            for (const p of t.projects) {
               const d = await catalog.collatedDatasets(p);
               if (d === undefined) {
                  return unknown({
                     reason: "project_unread",
                     ...at,
                     project: p,
                  });
               }
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
            return unknown({
               reason: "dataset_table_not_listed",
               ...at,
               ...(datasetTable ? { tableType: datasetTable.type } : {}),
            });
         }
         addCollated(collated, datasetTable.collated);
      }
      return { known: true, collated, catalog };
   } catch {
      return unknown({ reason: "read_failed", ...at });
   }
}

/** `table`'s collated paths added to `collated`, by path. */
function addCollated(
   collated: Map<string, Set<string>>,
   table: ReadonlyMap<string, string>,
): void {
   for (const [path, c] of table) {
      let set = collated.get(path);
      if (!set) {
         set = new Set();
         collated.set(path, set);
      }
      set.add(c);
   }
}
