// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { components } from "../api";
import { BadRequestError, ConflictError } from "../errors";
import {
   applySessionResourceLimits,
   attachDuckLakeReadWrite,
   escapeSQL,
   installAndLoadExtension,
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
 * Loads one object into a table of a DuckLake destination, for an orchestrator
 * that stages a file in the destination's own bucket (Credible's data stores).
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
   const bucketUrl =
      destinationConnection.ducklakeConnection.storage.bucketUrl.replace(
         /\/+$/,
         "",
      );
   if (!params.uri.startsWith(`${bucketUrl}/`)) {
      throw new BadRequestError(
         `The object must sit under the destination's storage location (${bucketUrl}/).`,
      );
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
      // Rows go to Parquet, never into the catalog database.
      await session.runSQL("SET ducklake_default_data_inlining_row_limit = 0");
      if (params.format === "xlsx") {
         await installAndLoadExtension(session, "excel");
      }
      const target = [destinationName, schemaName, tableName]
         .map((part) => quoteIdentifier(part, "duckdb"))
         .join(".");
      try {
         await session.runSQL(
            `${params.mode === "replace" ? "CREATE OR REPLACE TABLE" : "CREATE TABLE"} ${target} AS SELECT * FROM ${reader}`,
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
         throw error;
      }
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
   } finally {
      await dispose();
   }
}

function rows(result: unknown): Record<string, unknown>[] {
   const r = result as
      | { rows?: Record<string, unknown>[] }
      | Record<string, unknown>[];
   return Array.isArray(r) ? r : (r?.rows ?? []);
}
