// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { DuckDBConnection } from "@malloydata/db-duckdb";
import path from "path";
import { components } from "../api";
import { BadRequestError, ConflictError, logInternalFailure } from "../errors";
import {
   applySessionResourceLimits,
   attachDuckLakeReadOnly,
   attachDuckLakeReadWrite,
   escapeSQL,
   installAndLoadExtension,
   recordedCatalogSettings,
} from "./connection";
import { createIsolatedBuildSession } from "./materialization_build_session";
import { quoteIdentifier } from "./quoting";

type ApiConnection = components["schemas"]["Connection"];

export type LoadFormat = "csv" | "parquet" | "xlsx" | "json";

/**
 * Reader options a caller may pass, each typed and rendered as a literal: an
 * allowlist, never a string spliced into SQL.
 */
export interface LoadOptions {
   header?: boolean;
   delimiter?: string;
   sheet?: string;
   skip?: number;
}

const IDENT = /^[A-Za-z_][A-Za-z0-9_]*$/;

/** The reader call for one object, with only allowlisted, typed options. */
export function readerSql(
   uri: string,
   format: LoadFormat,
   options: LoadOptions = {},
): string {
   const path = `'${escapeSQL(uri)}'`;
   const args: string[] = [];
   if (format === "csv") {
      if (options.header !== undefined) {
         args.push(`header = ${options.header ? "true" : "false"}`);
      }
      if (options.delimiter !== undefined) {
         if (
            typeof options.delimiter !== "string" ||
            options.delimiter.length !== 1
         ) {
            throw new BadRequestError(
               "'delimiter' must be a single character.",
            );
         }
         args.push(`delim = '${escapeSQL(options.delimiter)}'`);
      }
      if (options.skip !== undefined) {
         if (!Number.isInteger(options.skip) || options.skip < 0) {
            throw new BadRequestError("'skip' must be a non-negative integer.");
         }
         args.push(`skip = ${options.skip}`);
      }
      return `read_csv(${[path, ...args].join(", ")})`;
   }
   if (format === "xlsx") {
      if (options.header !== undefined) {
         args.push(`header = ${options.header ? "true" : "false"}`);
      }
      if (options.sheet !== undefined) {
         if (typeof options.sheet !== "string" || options.sheet.length === 0) {
            throw new BadRequestError("'sheet' must be a sheet name.");
         }
         args.push(`sheet = '${escapeSQL(options.sheet)}'`);
      }
      return `read_xlsx(${[path, ...args].join(", ")})`;
   }
   if (format === "json") {
      return `read_json_auto(${path})`;
   }
   return `read_parquet(${path})`;
}

/**
 * Refuses an object outside the catalog's data path: the session holds only
 * that location's credential, and a load may read nothing else. A sibling
 * prefix that merely shares the string (`…/ds_a` vs `…/ds_ab`) is outside.
 */
export function assertUnderDataPath(uri: string, dataPath: string): void {
   const root = dataPath.replace(/\/+$/, "");
   const outside = () =>
      new BadRequestError(
         `The object must sit under the catalog's data path (${root}/).`,
      );
   if (!root || !uri.startsWith(`${root}/`)) throw outside();
   // A prefix match is only a containment check when nothing in the rest can
   // climb or fan out: DuckDB resolves `..` in a local path, and its readers
   // expand glob patterns, so either would read outside the prefix through a
   // string that starts inside it.
   const rest = uri.slice(root.length + 1);
   if (
      rest.length === 0 ||
      /[\\*?[\]{}]/.test(rest) ||
      rest
         .split("/")
         .some((part) => part === "" || part === "." || part === "..")
   ) {
      throw outside();
   }
   // A local path is also resolved, so no spelling of it lands elsewhere.
   if (!/^[a-z][a-z0-9+.-]*:\/\//i.test(uri)) {
      const resolved = path.resolve(uri);
      if (!resolved.startsWith(`${path.resolve(root)}${path.sep}`)) {
         throw outside();
      }
   }
}

/**
 * Loads one object into a table of a DuckLake destination, for an orchestrator
 * that stages a file in the destination's own bucket.
 *
 * - The object must sit under the destination's `bucketUrl`: the session holds
 *   only that bucket's credential, and a load may read nothing else.
 * - The bytes are rewritten as the catalog's own Parquet, never registered in
 *   place, so an encrypted catalog stays encrypted throughout.
 * - One DuckLake transaction: a reader sees the old table or the new one.
 *   `create` refuses a table that exists (409); `replace` swaps it.
 * - A transient session with the build session's teardown, so it strands no
 *   catalog backend.
 */
export async function loadStorageTable(params: {
   destinationName: string;
   destinationConnection: ApiConnection;
   schemaName: string;
   tableName: string;
   uri: string;
   format: LoadFormat;
   options?: LoadOptions;
   mode: "create" | "replace";
}): Promise<{ rowCount: number; columns: { name: string; type: string }[] }> {
   const { destinationName, destinationConnection, schemaName, tableName } =
      params;
   if (
      destinationConnection.type !== "ducklake" ||
      !destinationConnection.ducklakeConnection
   ) {
      throw new BadRequestError(
         `Storage destination '${destinationName}' is not DuckLake; only a DuckLake catalog takes a load.`,
      );
   }
   if (!IDENT.test(schemaName) || !IDENT.test(tableName)) {
      throw new BadRequestError(
         "Schema and table names must be plain identifiers.",
      );
   }
   // A configured data path is checked before anything attaches; with none,
   // the catalog's recorded path is checked once it is attached.
   const configuredPath =
      destinationConnection.ducklakeConnection.storage?.bucketUrl;
   if (configuredPath) {
      assertUnderDataPath(params.uri, configuredPath);
   }
   const reader = readerSql(params.uri, params.format, params.options);

   const { session, dispose, workDir } = createIsolatedBuildSession(
      `load_${destinationName}`,
   );
   try {
      await applySessionResourceLimits(session, { tempDirectory: workDir });
      await attachDuckLakeReadWrite(
         session,
         destinationName,
         destinationConnection.ducklakeConnection,
      );
      if (!configuredPath) {
         const recorded = await recordedCatalogSettings(
            session,
            destinationName,
            destinationConnection.ducklakeConnection.catalog?.metadataSchema,
         );
         assertUnderDataPath(params.uri, recorded.data_path ?? "");
      }
      if (params.format === "xlsx") {
         await installAndLoadExtension(session, "excel");
      }
      return await writeTableAs(session, params, reader, (error) =>
         readerRefusal(error, params.uri),
      );
   } finally {
      await dispose();
   }
}

/**
 * A load the file reader refused, as the person who sent the file should read it: a 400
 * carrying DuckDB's own words ("No magic bytes found", `Could not convert string "N/A" to
 * 'DOUBLE'`), which name the fix, rather than a 500 a caller can only retry. The object's URI,
 * an internal staging path, is replaced by its file name, and the SQL DuckDB echoes after
 * `LINE` is dropped. Null for anything else (a storage or catalog failure), which stays a 500.
 *
 * Applied only to the `CREATE TABLE ... AS SELECT` that runs the reader (see
 * {@link writeTableAs}): the flush, count and describe after it run against the
 * catalog, not the file, so their failures stay 500 whatever their prefix.
 *
 * A 400 is not logged on its own, and the route logs only the message returned, so
 * the original -- the staging path and the SQL -- is logged here at warn for the
 * operator reading an odd 400.
 */
export function readerRefusal(
   error: unknown,
   uri: string,
): BadRequestError | null {
   if (!(error instanceof Error)) return null;
   if (!/^(Invalid Input|Conversion) Error:/.test(error.message)) return null;
   logInternalFailure("Load refused by the file reader", error, "warn");
   const fileName = path.posix.basename(uri);
   const words = error.message
      .split(/\n\s*LINE \d+:/)[0]
      .split(uri)
      .join(fileName)
      .trim();
   return new BadRequestError(words);
}

/**
 * Copies a table from one DuckLake catalog into a table of another (or the
 * same) catalog, both in the same catalog database: a migration's copy, run on
 * the bytes the source catalog already holds.
 *
 * - A source in another catalog is attached read-only and the destination
 *   read-write, each as its own owner; a source within the destination's own
 *   catalog is read through the destination's attach. The rows are rewritten
 *   as the destination's own Parquet, so an encrypted destination stays
 *   encrypted whatever the source is.
 * - The same table guarantees as {@link loadStorageTable}: one DuckLake
 *   transaction, `create` refusing a table that exists (409), a transient
 *   session with the build session's teardown.
 * - The caller checks that the source exists and that the destination may
 *   hold its rows before calling: this attaches and writes.
 *
 * Exists for one migration (legacy per-organization catalogs into per-database
 * ones); remove it with the `table` source once those catalogs are dropped.
 */
export async function copyStorageTable(params: {
   destinationName: string;
   destinationConnection: ApiConnection;
   schemaName: string;
   tableName: string;
   source: {
      catalogName: string;
      connection: ApiConnection;
      schemaName: string;
      tableName: string;
   };
   mode: "create" | "replace";
}): Promise<{ rowCount: number; columns: { name: string; type: string }[] }> {
   const { destinationName, destinationConnection, source } = params;
   for (const conn of [destinationConnection, source.connection]) {
      if (conn.type !== "ducklake" || !conn.ducklakeConnection) {
         throw new BadRequestError(
            "Only a DuckLake catalog is a copy's source or destination.",
         );
      }
   }
   for (const part of [
      params.schemaName,
      params.tableName,
      source.schemaName,
      source.tableName,
   ]) {
      if (!IDENT.test(part)) {
         throw new BadRequestError(
            "Schema and table names must be plain identifiers.",
         );
      }
   }
   const sameCatalog = source.catalogName === destinationName;
   // Attached under its own alias, so it never shadows the destination: the
   // attach is ATTACH OR REPLACE, so a destination catalog named like the alias
   // would otherwise be replaced by the read-only source.
   const sourceAlias = sameCatalog
      ? destinationName
      : destinationName === COPY_SOURCE_ALIAS
        ? `${COPY_SOURCE_ALIAS}_2`
        : COPY_SOURCE_ALIAS;

   const { session, dispose, workDir } = createIsolatedBuildSession(
      `copy_${destinationName}`,
   );
   try {
      await applySessionResourceLimits(session, { tempDirectory: workDir });
      await attachDuckLakeReadWrite(
         session,
         destinationName,
         destinationConnection.ducklakeConnection!,
      );
      if (!sameCatalog) {
         await attachDuckLakeReadOnly(
            session,
            sourceAlias,
            source.connection.ducklakeConnection!,
         );
      }
      const from = [sourceAlias, source.schemaName, source.tableName]
         .map((part) => quoteIdentifier(part, "duckdb"))
         .join(".");
      return await writeTableAs(session, params, from);
   } finally {
      await dispose();
   }
}

/** The alias a copy attaches its source catalog under. */
const COPY_SOURCE_ALIAS = "__copy_source";

/**
 * `CREATE [OR REPLACE] TABLE <destination>.<schema>.<table> AS SELECT * FROM
 * <from>` on a session that has the destination attached read-write, with
 * inlining off, and the table's row count and columns after.
 *
 * `refused` classifies a failure of that statement alone: the error it returns
 * is thrown in place of the original (the load passes {@link readerRefusal}).
 * It never sees the flush, count or describe that follow, which fail against the
 * catalog after the table is committed, so a retry would meet the table.
 */
async function writeTableAs(
   session: DuckDBConnection,
   params: {
      destinationName: string;
      schemaName: string;
      tableName: string;
      mode: "create" | "replace";
   },
   from: string,
   refused?: (error: unknown) => Error | null,
): Promise<{ rowCount: number; columns: { name: string; type: string }[] }> {
   const { destinationName, schemaName, tableName } = params;
   // Rows go to Parquet, never into the catalog database: inlining off for this
   // session, and the flush below for a catalog that records its own limit.
   await session.runSQL("SET ducklake_default_data_inlining_row_limit = 0");
   const target = [destinationName, schemaName, tableName]
      .map((part) => quoteIdentifier(part, "duckdb"))
      .join(".");
   try {
      await session.runSQL(
         `${params.mode === "replace" ? "CREATE OR REPLACE TABLE" : "CREATE TABLE"} ${target} AS SELECT * FROM ${from}`,
      );
   } catch (error) {
      if (
         params.mode === "create" &&
         error instanceof Error &&
         /already exists/i.test(error.message)
      ) {
         throw new ConflictError(
            `Table '${schemaName}.${tableName}' already exists in '${destinationName}'.`,
         );
      }
      throw refused?.(error) ?? error;
   }
   // The SET above is only DuckLake's default: a catalog that records its own
   // data_inlining_row_limit (dataInliningRowLimit on POST/PATCH) keeps it, so a
   // table under that limit would sit as rows in the catalog database,
   // unencrypted whatever the catalog's encryption. Flush them to its Parquet.
   await session.runSQL(
      `CALL ducklake_flush_inlined_data('${escapeSQL(destinationName)}', schema_name => '${escapeSQL(schemaName)}', table_name => '${escapeSQL(tableName)}')`,
   );
   const counted = rows(
      await session.runSQL(`SELECT count(*) AS n FROM ${target}`),
   );
   const described = rows(await session.runSQL(`DESCRIBE ${target}`));
   return {
      rowCount: Number(counted[0]?.n ?? 0),
      columns: described.map((r) => ({
         name: String(r.column_name),
         type: String(r.column_type),
      })),
   };
}

function rows(result: unknown): Record<string, unknown>[] {
   const r = result as
      | { rows?: Record<string, unknown>[] }
      | Record<string, unknown>[];
   return Array.isArray(r) ? r : (r?.rows ?? []);
}
