// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A bind's read of its `storage=` tables, as their builds' sessions read
 * them, so each is held to the column contract its build recorded: on a
 * destination session of its own, read-only, the catalog as it is now.
 */

import type { Connection } from "@malloydata/malloy";
import type { components } from "../../../api";
import { errMessage } from "../../../utils";
import type { ObservedColumns } from "../domain/types";
import {
   readTableCollations,
   readTableColumns,
   withCollations,
} from "./database_column_read";
import { withDestinationSession } from "./materialization_build_session";

type ReadConnection = Pick<Connection, "runSQL" | "dialectName">;

type ApiConnection = components["schemas"]["Connection"];

/** Where a bind reads a `storage=` destination's tables from (the environment). */
export interface StoredTableSite {
   getStorageDestination(destinationName: string): ApiConnection;
   getEnvironmentPath(): string;
}

/**
 * Each `storage=` table's columns and collations, read as its build's
 * session read them: on a destination session of its own (one per
 * destination, read-only, the catalog as it is now), its `DESCRIBE` with no
 * DuckLake rewrite and its definition's collations. A table whose read
 * fails, or whose destination does not open, is unread, never judged.
 */
export async function readStoredTables(
   site: StoredTableSite,
   tables: readonly {
      key: string;
      destinationName: string;
      tablePath: string;
   }[],
): Promise<Map<string, ObservedColumns>> {
   const out = new Map<string, ObservedColumns>();
   const byDestination = new Map<string, typeof tables>();
   for (const t of tables) {
      byDestination.set(t.destinationName, [
         ...(byDestination.get(t.destinationName) ?? []),
         t,
      ]);
   }
   for (const [destinationName, own] of byDestination) {
      try {
         await withDestinationSession(
            {
               destinationName,
               destinationConnection:
                  site.getStorageDestination(destinationName),
               environmentPath: site.getEnvironmentPath(),
            },
            { purpose: "bind", readOnly: true },
            async (session) => {
               const reading = own.map((t) => ({
                  key: t.key,
                  connectionName: "d",
                  tableName: t.tablePath,
               }));
               const open = async () => session as unknown as ReadConnection;
               const columns = await readTableColumns({
                  tables: reading,
                  connection: open,
                  asDescribed: true,
                  concurrency: 1,
               });
               const collations = await readTableCollations({
                  tables: reading,
                  connection: open,
                  concurrency: 1,
               });
               for (const [key, o] of withCollations(
                  reading,
                  columns,
                  collations,
               )) {
                  out.set(key, o);
               }
            },
         );
      } catch (err) {
         for (const t of own) {
            out.set(t.key, {
               unread: "connection_failed",
               detail: errMessage(err),
            });
         }
      }
   }
   return out;
}
