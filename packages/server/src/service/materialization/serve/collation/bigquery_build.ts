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
      });
   } catch {
      read = { known: false, unread: { reason: "read_failed" } };
   }
   if (!read.known) {
      args.onUnread?.(withJob(read.unread, io));
      return false;
   }
   return read.collated.size === 0;
}

/** `unread` with the connection's project and location where it names none. */
function withJob(
   unread: BigQueryBuildUnread,
   io: CatalogIO,
): BigQueryBuildUnread {
   let job: { projects: string[]; location: string | undefined } = {
      projects: [],
      location: undefined,
   };
   try {
      job = io.jobContext();
   } catch {
      // Nothing more to name.
   }
   const project = unread.project ?? job.projects[0];
   return {
      ...unread,
      ...(project !== undefined ? { project } : {}),
      ...(job.location !== undefined ? { location: job.location } : {}),
   };
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
      return unknown({ reason: "read_failed", ...at });
   }
}
