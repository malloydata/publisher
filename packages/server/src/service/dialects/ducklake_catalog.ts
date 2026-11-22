// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What an attached DuckLake catalog records about itself, read from its
 * metadata catalog (its settings, its schemas' paths, a table's partition
 * spec), and the storage-secret scope derived from its data path.
 */

import type { DuckDBConnection } from "@malloydata/db-duckdb";
import { logger } from "../../logger";
import { runInternalSQL } from "../duckdb_lockdown";
import { escapeSQL, runSQLRows } from "../duckdb_session_setup";
import { quoteIdentifier } from "../quoting";

/** The prefix of an attached catalog's metadata catalog name, whose tables hold every schema's file lists. */
export const METADATA_CATALOG_PREFIX = "__ducklake_metadata_";

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
      `${METADATA_CATALOG_PREFIX}${alias}`,
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

/** The path a catalog records for a schema, and whether it is relative to the data path. */
export interface RecordedSchemaPath {
   path: string;
   relative: boolean;
}

/**
 * The paths a catalog attached as `alias` records in `ducklake_schema` for the
 * named schemas, in one query, keyed by schema name; a schema the catalog does
 * not record is absent. A path of `''` is what a catalog migrated from DuckLake
 * 0.2 records for `main`: its files sit directly under the data path. A NULL
 * `path_is_relative` reads as relative, the recorded default.
 */
export async function recordedSchemaPaths(
   connection: DuckDBConnection,
   alias: string,
   metadataSchema: string | undefined,
   schemaNames: string[],
): Promise<Map<string, RecordedSchemaPath>> {
   const names = schemaNames.map((name) => `'${escapeSQL(name)}'`).join(", ");
   const rows = runSQLRows(
      await runInternalSQL(
         connection,
         `SELECT schema_name, path, path_is_relative FROM ${metadataCatalogRef(alias, metadataSchema)}.ducklake_schema ` +
            `WHERE schema_name IN (${names}) AND end_snapshot IS NULL`,
      ),
   );
   const recorded = new Map<string, RecordedSchemaPath>();
   for (const row of rows) {
      if (typeof row.schema_name === "string" && typeof row.path === "string") {
         recorded.set(row.schema_name, {
            path: row.path,
            relative: row.path_is_relative !== false,
         });
      }
   }
   return recorded;
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
 * The scope prefix of a readable schema other than `main`: its recorded path,
 * which must resolve strictly under `rootSlash` (the data path, one trailing
 * slash) as plain segments. Undefined for anything else (the data path itself,
 * a `..`, `.` or empty segment, an absolute path elsewhere), because the
 * prefix would then reach files outside the schema's own directory.
 */
function readableSchemaScope(
   rootSlash: string,
   recorded: RecordedSchemaPath,
): string | undefined {
   let rest = recorded.path;
   if (!recorded.relative) {
      if (!rest.startsWith(rootSlash)) return undefined;
      rest = rest.slice(rootSlash.length);
   }
   const trimmed = stripTrailingSlashes(rest);
   if (
      trimmed === "" ||
      trimmed
         .split("/")
         .some((part) => part === "" || part === "." || part === "..")
   ) {
      return undefined;
   }
   return `${rootSlash}${trimmed}/`;
}

/**
 * The URL prefixes a DuckLake catalog's storage secret is scoped to, each with
 * exactly one trailing slash so `…/org_a/` never matches `…/org_ab`. The
 * configured data path when the config states one, else the data path the
 * catalog recorded, which the preflight read. A managed catalog attached
 * read-only is the user-facing view of its own tables, so its secret reaches
 * only the files of `main` and of `readableSchemas`: the schema paths the
 * catalog recorded, under the data path when relative. `main` keeps the
 * leniency of catalogs migrated from DuckLake 0.2 (an absolute path, an empty
 * path for the data path itself); every other schema must resolve strictly under
 * the data path, and one that does not, or that the catalog does not record, is
 * left out and logged, so a bad entry narrows the view rather than widening it.
 * Undefined when nothing states a path, when a managed view's main path is
 * unknown, or when the root names no bucket: the caller then refuses the attach
 * rather than creating an unscoped secret.
 */
export function storageSecretScope(input: {
   catalog: string;
   configuredDataPath?: string;
   recordedDataPath?: string;
   schemaPaths?: ReadonlyMap<string, RecordedSchemaPath>;
   readableSchemas?: string[];
   readOnly: boolean;
   managed: boolean;
}): string[] | undefined {
   const root = input.configuredDataPath || input.recordedDataPath;
   if (!root || isBucketlessUrl(root)) return undefined;
   const rootSlash = oneTrailingSlash(root);
   if (!(input.managed && input.readOnly)) return [rootSlash];
   const main = input.schemaPaths?.get("main");
   if (!main) return undefined;
   let mainScope: string;
   if (!main.relative) {
      if (isBucketlessUrl(main.path)) return undefined;
      mainScope = oneTrailingSlash(main.path);
   } else {
      // An empty relative path is the data path itself.
      let start = 0;
      while (start < main.path.length && main.path[start] === "/") start++;
      mainScope = oneTrailingSlash(`${rootSlash}${main.path.slice(start)}`);
   }
   const scope = [mainScope];
   for (const schema of input.readableSchemas ?? []) {
      const recorded = input.schemaPaths?.get(schema);
      const prefix = recorded && readableSchemaScope(rootSlash, recorded);
      if (prefix) {
         scope.push(prefix);
      } else {
         logger.error(
            "A readable schema is left out of the managed view's storage scope: " +
               "the catalog does not record it, or its path is not strictly under the data path",
            { catalog: input.catalog, schema },
         );
      }
   }
   return [...new Set(scope)];
}
