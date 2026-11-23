// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What an attached DuckLake catalog records about itself, read from its
 * metadata catalog (its settings, a schema's path, a table's partition spec),
 * and the storage-secret scope derived from its data path.
 */

import type { DuckDBConnection } from "@malloydata/db-duckdb";
import { runInternalSQL } from "../duckdb_lockdown";
import { escapeSQL, runSQLRows } from "../duckdb_session_setup";
import { quoteIdentifier } from "../quoting";

/**
 * The metadata catalog of a DuckLake catalog attached as `alias`: DuckLake
 * exposes it as `__ducklake_metadata_<alias>`, with the `ducklake_*` tables in
 * the catalog's metadata schema when one is configured, and in its default
 * schema otherwise.
 */
export function metadataCatalogRef(
   alias: string,
   metadataSchema: string | undefined,
): string {
   return [
      `__ducklake_metadata_${alias}`,
      ...(metadataSchema ? [metadataSchema] : []),
   ]
      .map((part) => quoteIdentifier(part, "duckdb"))
      .join(".");
}

/**
 * The settings a catalog attached as `alias` recorded in its `ducklake_metadata`
 * table (`data_path`, `encrypted`, `version`, `data_inlining_row_limit`,
 * `delete_older_than`, …), keyed by name. The metadata tables live in the
 * catalog's metadata schema when one is configured, and in its default schema
 * otherwise.
 */
export async function recordedCatalogSettings(
   connection: DuckDBConnection,
   alias: string,
   metadataSchema: string | undefined,
): Promise<Record<string, string>> {
   const rows = runSQLRows(
      await runInternalSQL(
         connection,
         `SELECT key, value FROM ${metadataCatalogRef(alias, metadataSchema)}.ducklake_metadata WHERE scope IS NULL`,
      ),
   );
   const settings: Record<string, string> = {};
   for (const row of rows) {
      if (typeof row.key === "string" && row.value != null) {
         settings[row.key] = String(row.value);
      }
   }
   return settings;
}

/**
 * The path a catalog attached as `alias` records for one of its schemas in
 * `ducklake_schema`, and whether it is relative to the data path, or undefined
 * when the schema is not recorded. A path of `''` is what a catalog migrated
 * from DuckLake 0.2 records for `main`: its files sit directly under the data
 * path. A NULL `path_is_relative` reads as relative, the recorded default.
 */
export async function recordedSchemaPath(
   connection: DuckDBConnection,
   alias: string,
   metadataSchema: string | undefined,
   schemaName: string,
): Promise<{ path: string; relative: boolean } | undefined> {
   const row = runSQLRows(
      await runInternalSQL(
         connection,
         `SELECT path, path_is_relative FROM ${metadataCatalogRef(alias, metadataSchema)}.ducklake_schema ` +
            `WHERE schema_name = '${escapeSQL(schemaName)}' AND end_snapshot IS NULL LIMIT 1`,
      ),
   )[0];
   if (!row || typeof row.path !== "string") return undefined;
   return { path: row.path, relative: row.path_is_relative !== false };
}

/**
 * One key of a DuckLake table's partition spec: a column, and the transform
 * DuckLake records for it (`identity`, `year`, `month`, `day`, `hour`, or
 * `bucket(N)`).
 */
export interface PartitionKey {
   column: string;
   transform: string;
}

/**
 * The partition spec `schemaName.tableName` has now, in the catalog attached as
 * `alias`, in key order. Empty when the table is not partitioned, including a
 * table whose spec was reset (DuckLake records that as a spec with no keys), and
 * when no such table exists.
 */
export async function tablePartitionKeys(
   connection: DuckDBConnection,
   alias: string,
   metadataSchema: string | undefined,
   schemaName: string,
   tableName: string,
): Promise<PartitionKey[]> {
   const meta = metadataCatalogRef(alias, metadataSchema);
   const rows = runSQLRows(
      await runInternalSQL(
         connection,
         `SELECT c.column_name, pc.transform
            FROM ${meta}.ducklake_table t
            JOIN ${meta}.ducklake_schema s
              ON s.schema_id = t.schema_id AND s.end_snapshot IS NULL
            JOIN ${meta}.ducklake_partition_info pi
              ON pi.table_id = t.table_id AND pi.end_snapshot IS NULL
            JOIN ${meta}.ducklake_partition_column pc
              ON pc.partition_id = pi.partition_id AND pc.table_id = t.table_id
            JOIN ${meta}.ducklake_column c
              ON c.table_id = t.table_id AND c.column_id = pc.column_id AND c.end_snapshot IS NULL
           WHERE s.schema_name = '${escapeSQL(schemaName)}'
             AND t.table_name = '${escapeSQL(tableName)}'
             AND t.end_snapshot IS NULL
           ORDER BY pc.partition_key_index`,
      ),
   );
   return rows.map((row) => ({
      column: String(row.column_name),
      transform: String(row.transform),
   }));
}

/** `url` without its trailing slashes. A loop, not `/\/+$/`, which backtracks on a long run of them. */
export function stripTrailingSlashes(url: string): string {
   let end = url.length;
   while (end > 0 && url[end - 1] === "/") end--;
   return url.slice(0, end);
}

/** `url` with its trailing slashes replaced by exactly one. */
function oneTrailingSlash(url: string): string {
   return `${stripTrailingSlashes(url)}/`;
}

/**
 * Whether a URL names a scheme but no bucket (`s3://`, `gs:///`). As a scope it
 * would become `s3:/`, which DuckDB matches against every S3 URL, so such a
 * root is refused. A path with no scheme is not a URL and is left alone.
 */
function isBucketlessUrl(url: string): boolean {
   const at = url.indexOf("://");
   return at >= 0 && stripTrailingSlashes(url.slice(at + 3)) === "";
}

/**
 * The URL prefix a DuckLake catalog's storage secret is scoped to, with exactly
 * one trailing slash so `…/org_a/` never matches `…/org_ab`. The configured data
 * path when the config states one, else the data path the catalog recorded,
 * which the preflight read. A managed catalog attached read-only is the
 * user-facing view of its own tables, so its secret reaches only the `main`
 * schema's files: the schema path the catalog recorded, under the data path when
 * relative. Undefined when nothing states a path, when a managed view's main
 * path is unknown, or when the root names no bucket: the caller then refuses
 * the attach rather than creating an unscoped secret.
 */
export function storageSecretScope(input: {
   configuredDataPath?: string;
   recordedDataPath?: string;
   mainSchemaPath?: { path: string; relative: boolean };
   readOnly: boolean;
   managed: boolean;
}): string | undefined {
   const root = input.configuredDataPath || input.recordedDataPath;
   if (!root || isBucketlessUrl(root)) return undefined;
   if (input.managed && input.readOnly) {
      const main = input.mainSchemaPath;
      if (!main) return undefined;
      if (!main.relative) {
         return isBucketlessUrl(main.path)
            ? undefined
            : oneTrailingSlash(main.path);
      }
      // An empty relative path is the data path itself (a catalog migrated from
      // DuckLake 0.2 records it for `main`).
      let start = 0;
      while (start < main.path.length && main.path[start] === "/") start++;
      return oneTrailingSlash(
         `${oneTrailingSlash(root)}${main.path.slice(start)}`,
      );
   }
   return oneTrailingSlash(root);
}
