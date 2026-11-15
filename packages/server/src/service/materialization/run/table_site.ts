// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Where a materialized table's marks are read, and a leftover view dropped:
 * the source's own connection for a colocated table, or a session attached
 * read-write to its storage destination for a stored one. Used by the
 * ownership check and mark (table_ownership.ts), the drop's guards
 * (reclaimer.ts) and the leftover `name=` view cleanup
 * (leftover_name_views.ts). Each call opens and closes its own destination
 * session; a colocated site runs on the connection it is given.
 */

import type { Connection as MalloyConnection } from "@malloydata/malloy";
import type { ApiConnection } from "../../model";
import {
   destinationSchemaOf,
   storageTablePath,
} from "../domain/storage_table_path";
import {
   STORAGE_TARGET_DIALECT,
   withStorageDestinationSession,
} from "./materialization_build_session";
import { pathSegments } from "./table_mark_sql";

/** A table's site, in its dialect. */
export interface TableSite {
   dialect: string;
   /** The unquoted segments of a relation's full path. */
   segments(name: string): string[];
   run(sql: string): Promise<Record<string, unknown>[]>;
}

function rowsOf(result: unknown): Record<string, unknown>[] {
   const rows = (result as { rows?: unknown } | null)?.rows;
   return Array.isArray(rows) ? (rows as Record<string, unknown>[]) : [];
}

/** A colocated table's site: the source's own connection. */
export function colocatedTableSite(
   connection: Pick<MalloyConnection, "runSQL" | "dialectName">,
   runOptions: Record<string, unknown> = {},
): TableSite {
   return {
      dialect: connection.dialectName,
      segments: pathSegments,
      run: async (sql) => rowsOf(await connection.runSQL(sql, runOptions)),
   };
}

/**
 * A stored table's site: a session with the destination attached under its
 * own name, so a relation's path is the one the build wrote.
 */
export function storageTableSite(
   destinationName: string,
   destinationConnection: ApiConnection,
   session: { runSQL(sql: string): Promise<unknown> },
): TableSite {
   const schema = destinationSchemaOf(destinationConnection);
   return {
      dialect: STORAGE_TARGET_DIALECT,
      segments: (name) =>
         pathSegments(storageTablePath(destinationName, name, schema)),
      run: async (sql) => rowsOf(await session.runSQL(sql)),
   };
}

/** What reaching a table's site needs from the run or the sweep. */
export interface TableSiteContext {
   /** The source connection a colocated table lives on, or undefined. */
   connectionFor(
      connectionName: string,
   ): Promise<MalloyConnection | undefined> | MalloyConnection | undefined;
   /** A destination's config, or undefined when it is no longer configured. */
   destinationFor(destinationName: string): ApiConnection | undefined;
   environmentPath: string;
   /** Options every colocated statement carries (query metadata). */
   runOptions?(connectionName: string): Record<string, unknown>;
}

/**
 * Run `fn` at the site of the table `target` names: on its connection, or on
 * a session attached to its destination for the call. Undefined when the
 * site cannot be reached (an unloaded connection, a destination no longer
 * configured).
 */
export async function atTableSite<T>(
   target: { connectionName: string; storageDestinationName?: string },
   context: TableSiteContext,
   fn: (site: TableSite) => Promise<T>,
): Promise<T | undefined> {
   const destinationName = target.storageDestinationName;
   if (destinationName) {
      const destinationConnection = context.destinationFor(destinationName);
      if (!destinationConnection) return undefined;
      return withStorageDestinationSession(
         {
            destinationName,
            destinationConnection,
            environmentPath: context.environmentPath,
         },
         (session) =>
            fn(
               storageTableSite(
                  destinationName,
                  destinationConnection,
                  session,
               ),
            ),
      );
   }
   const connection = await context.connectionFor(target.connectionName);
   if (!connection) return undefined;
   return fn(
      colocatedTableSite(
         connection,
         context.runOptions?.(target.connectionName),
      ),
   );
}
