// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A bind's read of its `storage=` tables, as their builds' sessions read
 * them, so each is held to the column contract its build recorded: on a
 * destination session of its own, read-only, the catalog as it is now.
 *
 * Never through the shared serve connection. Reading there right after a
 * build met a DuckLake attach made before the build's commit: `DESCRIBE
 * "lake"."<new table>"` failed with "Invalid Input Error: No snapshot found
 * in DuckLake", and the next `DESCRIBE` on that connection crashed Bun with a
 * segmentation fault (tests/component/materialization_storage_stacking
 * .component.spec.ts, its file-catalog fixture's serve connection, on the
 * first build's bind). A fresh read-only session sees the catalog as
 * committed and is closed with the read, so it never reaches that state.
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
import {
   destinationSchemaOf,
   storageTablePath,
} from "../domain/storage_table_path";
import type { ManifestEntry } from "../domain/types";

type ReadConnection = Pick<Connection, "runSQL" | "dialectName">;

type ApiConnection = components["schemas"]["Connection"];

/** Where a bind reads a `storage=` destination's tables from (the environment). */
export interface StoredTableSite {
   getStorageDestination(destinationName: string): ApiConnection;
   getEnvironmentPath(): string;
}

/**
 * {@link readStoredTables} for manifest entries, by their key: each
 * `storage=` entry's table at its path in its destination (its schema, if
 * the destination names one). An entry with no destination is not read.
 */
export function readStoredEntries(
   site: StoredTableSite,
   entries: Readonly<Record<string, ManifestEntry>>,
): Promise<Map<string, ObservedColumns>> {
   return readStoredTables(
      site,
      Object.entries(entries).flatMap(([key, entry]) => {
         const destinationName = entry.storageDestinationName;
         if (!destinationName) return [];
         let schema: string | undefined;
         try {
            schema = destinationSchemaOf(
               site.getStorageDestination(destinationName),
            );
         } catch {
            schema = undefined;
         }
         return [
            {
               key,
               destinationName,
               tablePath: storageTablePath(
                  destinationName,
                  entry.physicalTableName,
                  schema,
               ),
            },
         ];
      }),
   );
}

/**
 * Each `storage=` table's columns and collations, read as its build's
 * session read them: on a destination session of its own (one per
 * destination, read-only, the catalog as it is now), its `DESCRIBE` with no
 * DuckLake rewrite and its definition's collations. A table whose read
 * fails, or whose destination does not open, is unread, never judged.
 */
async function readStoredTables(
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
