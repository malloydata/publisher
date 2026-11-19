// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Administering a `storage=` destination's tables outside a build: dropping one
 * recorded table (and its backup), listing a schema's tables, and reclaiming a
 * DuckLake catalog's expired snapshots and unreferenced files. Each runs on an
 * isolated, locked-down session from the build session. Read by the `storage=`
 * tier, catalog_admin.ts and the destination controller.
 *
 * Effects: SQL in the destination (drops, catalog reads, DuckLake snapshot
 * expiry and file deletion); a read of the environment's destinations
 * directory.
 */

import { existsSync, readdirSync } from "node:fs";
import path from "node:path";
import type { components } from "../../../api";
import { BadRequestError, StorageCatalogNotFoundError } from "../../../errors";
import {
   attachDuckLakeReadOnlyIfPresent,
   attachDuckLakeReadWrite,
   escapeSQL,
} from "../../connection";
import { storageDestinationRoot } from "../../connection_config";
import { quoteIdentifier, quoteManifestTablePath } from "../../quoting";
import { STORAGE_DIALECT } from "../domain/dialect";
import {
   destinationSchemaOf,
   storageTablePath,
} from "../domain/storage_table_path";
import { resultRows } from "./build_read_cost";
import {
   assertSupportedDestination,
   destinationAllowlist,
   lockDownDestinationSession,
   requireDuckLakeConfig,
   withDestinationSession,
   withIsolatedSession,
} from "./materialization_build_session";

type ApiConnection = components["schemas"]["Connection"];

/** DDL to drop a storage table by its recorded name, catalog-qualified for DuckDB. */
export function dropStorageTableSql(
   destinationName: string,
   physicalTableName: string,
   schema?: string,
): string {
   // Must name the table the way the build CREATEd it (see the CTAS sites).
   return `DROP TABLE IF EXISTS ${quoteManifestTablePath(
      storageTablePath(destinationName, physicalTableName, schema),
      STORAGE_DIALECT,
   )}`;
}

/**
 * Drop one materialized table from a `storage=` destination, on an isolated
 * read-write session (the serve attach is read-only; isolation keeps the attach
 * from colliding with another tenant's — see `createIsolatedBuildSession`).
 *
 * Exactly one named table, never a pattern or scan, so the caller decides what
 * goes: a name Publisher recorded building, or one an orchestrator assigned.
 */
export async function dropStorageTable(params: {
   destinationName: string;
   destinationConnection: ApiConnection;
   physicalTableName: string;
   environmentPath: string;
   /**
    * More tables to drop in the same session once the table is dropped (or
    * found not to be one): a table's own backup name (reclaimer.ts).
    */
   alsoDrop?: string[];
}): Promise<void> {
   const {
      destinationName,
      destinationConnection,
      physicalTableName,
      environmentPath,
   } = params;
   return withDestinationSession(
      { destinationName, destinationConnection, environmentPath },
      { purpose: "gc" },
      async (session) => {
         const schema = destinationSchemaOf(destinationConnection);
         let dropErr: unknown;
         try {
            await session.runSQL(
               dropStorageTableSql(destinationName, physicalTableName, schema),
            );
         } catch (err) {
            dropErr = err;
         }
         for (const table of params.alsoDrop ?? []) {
            await session.runSQL(
               dropStorageTableSql(destinationName, table, schema),
            );
         }
         if (dropErr !== undefined) throw dropErr;
      },
   );
}

/**
 * The tables in one schema of a `storage=` destination, in name order.
 * Tables only: a view is never listed, so a sweep that drops what it lists
 * never reaches one.
 *
 * Read-only, on an isolated instance. A plain DuckDB destination with no
 * database file yet is reported empty rather than created by looking.
 *
 * A DuckLake destination with no catalog yet is a 404
 * ({@link StorageCatalogNotFoundError}), not empty: an empty listing invites a
 * follow-up drop or cleanup, and those attach read-write, creating the catalog.
 */
export async function listStorageTables(params: {
   destinationName: string;
   destinationConnection: ApiConnection;
   schemaName: string;
   environmentPath: string;
}): Promise<string[]> {
   const {
      destinationName,
      destinationConnection,
      schemaName,
      environmentPath,
   } = params;
   assertSupportedDestination(destinationName, destinationConnection);

   return withIsolatedSession(`list_${destinationName}`, async (session) => {
      if (destinationConnection.type === "ducklake") {
         const cfg = requireDuckLakeConfig(
            destinationName,
            destinationConnection,
         );
         if (
            !(await attachDuckLakeReadOnlyIfPresent(
               session,
               destinationName,
               cfg,
            ))
         ) {
            throw new StorageCatalogNotFoundError(
               `Storage destination '${destinationName}' has no catalog ` +
                  `yet: nothing has been built into it.`,
            );
         }
      } else {
         // Existence is checked by listing the environment's destinations
         // directory, so the request-supplied name is only compared, never used
         // to reach the filesystem before the attach.
         const destinationRoot = storageDestinationRoot(environmentPath);
         const fileName = `${destinationName}.duckdb`;
         if (
            !existsSync(destinationRoot) ||
            !readdirSync(destinationRoot).includes(fileName)
         ) {
            return [];
         }
         const dbPath = path.join(destinationRoot, fileName);
         await session.runSQL(
            `ATTACH '${escapeSQL(dbPath)}' AS ${quoteIdentifier(destinationName, STORAGE_DIALECT)} (READ_ONLY)`,
         );
      }
      await lockDownDestinationSession(
         session,
         destinationAllowlist({ destinationConnection, environmentPath }),
      );
      const rows = resultRows(
         await session.runSQL(
            `SELECT table_name FROM duckdb_tables() ` +
               `WHERE database_name = '${escapeSQL(destinationName)}' ` +
               `AND schema_name = '${escapeSQL(schemaName)}' ` +
               `ORDER BY table_name`,
         ),
      );
      return rows.map((row) => String(row.table_name));
   });
}

/** What one storage file cleanup reclaimed. */
interface StorageFileCleanupResult {
   expiredSnapshots: number;
   deletedFiles: number;
}

/**
 * Reclaim object-storage bytes in a DuckLake destination.
 *
 * Dropping a DuckLake table only retires it in the catalog; its files stay while
 * any snapshot references them. Two calls, in order:
 *
 *  1. `ducklake_expire_snapshots(older_than)` — forgets snapshots older than the
 *     cutoff (never the latest) and schedules files only they referenced. This
 *     cutoff is the recovery window for a dropped table.
 *  2. `ducklake_cleanup_old_files(older_than)` — deletes files scheduled before
 *     the cutoff, protecting a reader still scanning a just-expired snapshot.
 *
 * NOT `CHECKPOINT` or `ducklake_delete_orphaned_files`: both delete untracked
 * files, which include another catalog's live files on a shared data path and a
 * concurrent build's uncommitted ones. Non-DuckLake destinations are refused.
 */
export async function cleanupStorageFiles(params: {
   destinationName: string;
   destinationConnection: ApiConnection;
   snapshotsOlderThan: Date;
   filesOlderThan: Date;
}): Promise<StorageFileCleanupResult> {
   const {
      destinationName,
      destinationConnection,
      snapshotsOlderThan,
      filesOlderThan,
   } = params;
   if (destinationConnection.type !== "ducklake") {
      throw new BadRequestError(
         `Storage destination '${destinationName}' is type ` +
            `'${destinationConnection.type ?? ""}'; file cleanup applies only to ` +
            `'ducklake' destinations.`,
      );
   }
   const cfg = requireDuckLakeConfig(destinationName, destinationConnection);

   return withIsolatedSession(`cleanup_${destinationName}`, async (session) => {
      await attachDuckLakeReadWrite(session, destinationName, cfg);
      // Confined to the catalog's data path (and what the attach recorded)
      // before the cleanup deletes any file there.
      const dataPath = cfg.storage?.bucketUrl;
      await lockDownDestinationSession(session, dataPath ? [dataPath] : []);
      const catalog = escapeSQL(destinationName);
      const expired = resultRows(
         await session.runSQL(
            `CALL ducklake_expire_snapshots('${catalog}', older_than => ` +
               `TIMESTAMPTZ '${snapshotsOlderThan.toISOString()}')`,
         ),
      );
      const deleted = resultRows(
         await session.runSQL(
            `CALL ducklake_cleanup_old_files('${catalog}', older_than => ` +
               `TIMESTAMPTZ '${filesOlderThan.toISOString()}')`,
         ),
      );
      return { expiredSnapshots: expired.length, deletedFiles: deleted.length };
   });
}
