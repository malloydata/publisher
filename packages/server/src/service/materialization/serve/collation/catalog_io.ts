// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What a collation read goes through: the I/O of one connection
 * ({@link catalogIO}), each catalog read admitted past a locked DuckDB
 * instance's gate, and the answer every read gives. A reader in this
 * directory sends SQL and reads rows through it and never holds a
 * connection itself.
 */

import type { Connection } from "@malloydata/malloy";
import { runAsInternalStatement } from "../../../duckdb_lockdown";
import type { Collation } from "../../domain/collation";

export type { Collation };

/** Most rows one complete catalog read takes; a read that fills it proves nothing. */
export const CATALOG_ROW_LIMIT = 100_000;

/** One connection's I/O, as the collation reads use it. */
export interface CatalogIO {
   /** The connection's Malloy dialect, which quotes a name it reads. */
   readonly dialectName: string;
   /**
    * Rows of Publisher's own catalog read, admitted past a locked DuckDB
    * instance's gate (which refuses settings reads to caller and author SQL).
    */
   rows(sql: string): Promise<Record<string, unknown>[]>;
   /**
    * Every row of a catalog read, or a throw (the check then answers live).
    * Malloy's BigQuery connection reads one results page: 10 rows unless a
    * `rowLimit` asks for more, and BigQuery ends a page at about 10 MB
    * whatever it asks for. So this asks for {@link CATALOG_ROW_LIMIT} rows
    * and answers only when the read is provably whole: fewer rows than that,
    * and every row its `totalRows` reports.
    */
   completeRows(sql: string): Promise<Record<string, unknown>[]>;
   /**
    * BigQuery: the locations of project `project`'s datasets, through the
    * client the connection holds (`datasets.list`, a metadata call, which
    * lists only the datasets the caller may get). Throws when the
    * connection holds no client, or a dataset names no location.
    */
   datasetLocations(project: string): Promise<string[]>;
   /**
    * BigQuery: what the connection runs jobs as, its projects (none when a
    * project is not resolved yet) and its location, lower-cased.
    */
   jobContext(): { projects: string[]; location: string | undefined };
}

/** One dialect's collation reads, each through a {@link CatalogIO}. */
export interface CollationReader {
   /**
    * How a query's strings are read: `instance`, once over the whole
    * instance and the query's SQL; `column`, per string column the gates and
    * scopes compare. (BigQuery's reader is `dataset`: from the catalog of
    * every table the query reads.) It agrees with the dialect's
    * `stringCollation` fact (`domain/dialect.ts`), which a spec checks.
    */
   readonly scope: "instance" | "column";
   /**
    * What the strings of a source compare under, read from SQL that
    * selects them (as `probe_value`, through the author's compile).
    */
   sourceColumns(io: CatalogIO, sql: string): Promise<Collation>;
   /** What the strings of a stored table compare under, by its path. */
   storedTable(io: CatalogIO, tablePath: string): Promise<Collation>;
   /** Whether the catalog lists the table at `tablePath`. */
   tableListed(io: CatalogIO, tablePath: string): Promise<boolean>;
}

type Rows = { rows: Record<string, unknown>[] };

/**
 * Publisher's own catalog read over `connection`, admitted past a locked
 * DuckDB instance's gate (duckdb_lockdown.ts), at the connection's default
 * page unless `options` asks for more rows.
 */
async function catalogRead(
   connection: Connection,
   sql: string,
   options?: { rowLimit: number },
): Promise<Rows & { totalRows?: unknown }> {
   return (await runAsInternalStatement(sql, () =>
      connection.runSQL(sql, options),
   )) as unknown as Rows & { totalRows?: unknown };
}

/**
 * The I/O the collation reads go through over `connection`: its catalog
 * reads, each admitted past a locked DuckDB instance's gate
 * (duckdb_lockdown.ts), and, for BigQuery, what its driver holds (the
 * client's `datasets.list`, its projects and location), read duck-typed
 * here and nowhere else.
 */
export function catalogIO(connection: Connection): CatalogIO {
   return {
      dialectName: connection.dialectName,
      async rows(sql) {
         return (await catalogRead(connection, sql)).rows ?? [];
      },
      async completeRows(sql) {
         const result = await catalogRead(connection, sql, {
            rowLimit: CATALOG_ROW_LIMIT,
         });
         const found = result.rows ?? [];
         if (
            found.length >= CATALOG_ROW_LIMIT ||
            result.totalRows === undefined ||
            result.totalRows === null ||
            Number(result.totalRows) !== found.length
         ) {
            throw new Error("a catalog read that may be incomplete");
         }
         return found;
      },
      async datasetLocations(project) {
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
            if (typeof l !== "string" || l === "")
               throw new Error("no location");
            locations.add(l.toLowerCase());
         }
         return [...locations];
      },
      jobContext() {
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
      },
   };
}
