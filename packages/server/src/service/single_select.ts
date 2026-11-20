// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBInstance } from "@duckdb/node-api";
import { BadRequestError } from "../errors";
import {
   introspectionCallInParse,
   introspectionNameIn,
} from "./duckdb_introspection";

/**
 * Raw SQL on a managed DuckLake connection (`Connection.managed`) runs only when
 * it is exactly one SELECT. The connection view is read-only, but a session that
 * accepts arbitrary statements can create a secret or `SET` its configuration.
 * One SELECT can do neither, so it can be no worse than a model's query, whose
 * `sql()` sources the compiler already places inside a subquery. It is not a read
 * boundary: a SELECT still reads whatever the session's secrets, catalog role and
 * filesystem reach, so the storage secret's `SCOPE` is what keeps it to
 * the catalog's prefix.
 *
 * Classified by DuckDB's own parser, the same engine the statement runs on:
 * `json_serialize_sql` serializes SELECT statements only, and answers one entry
 * per statement. It parses without binding, so it needs no catalog. Anything it
 * cannot classify is refused.
 */
let parser: Promise<DuckDBInstance> | undefined;

async function parserInstance(): Promise<DuckDBInstance> {
   if (!parser) {
      parser = DuckDBInstance.create(":memory:", {
         threads: "1",
         enable_external_access: "false",
      }).catch((error) => {
         parser = undefined;
         throw error;
      });
   }
   return parser;
}

/**
 * DuckDB's own parse of `sql`, by `json_serialize_sql`: the statements when
 * all are SELECT, else the error and its type (`parser` for SQL that does not
 * parse, `not implemented` for a statement kind it does not serialize).
 */
export async function serializeSQL(sql: string): Promise<{
   error?: boolean;
   errorType?: string;
   errorMessage?: string;
   statements?: unknown[];
}> {
   const connection = await (await parserInstance()).connect();
   try {
      const prepared = await connection.prepare(
         "SELECT json_serialize_sql($1::VARCHAR) AS j",
      );
      prepared.bindVarchar(1, sql);
      const rows = await (await prepared.run()).getRowObjectsJson();
      const parsed = JSON.parse(String(rows[0]?.j)) as {
         error?: boolean;
         error_type?: string;
         error_message?: string;
         statements?: unknown[];
      };
      return {
         error: parsed.error,
         errorType: parsed.error_type,
         errorMessage: parsed.error_message,
         statements: parsed.statements,
      };
   } finally {
      connection.closeSync();
   }
}

/**
 * How many statements DuckDB's own parser reads in `sql` (of any kind), or
 * undefined when it does not parse.
 */
export async function countStatements(
   sql: string,
): Promise<number | undefined> {
   const connection = await (await parserInstance()).connect();
   try {
      return (await connection.extractStatements(sql)).count;
   } catch {
      return undefined;
   } finally {
      connection.closeSync();
   }
}

/** How many SELECT statements `sql` holds, or why it is not only SELECTs. */
export async function classifySelect(
   sql: string,
): Promise<{ selects: number } | { refused: string }> {
   const connection = await (await parserInstance()).connect();
   try {
      const prepared = await connection.prepare(
         "SELECT json_serialize_sql($1::VARCHAR) AS j",
      );
      prepared.bindVarchar(1, sql);
      const rows = await (await prepared.run()).getRowObjectsJson();
      const parsed = JSON.parse(String(rows[0]?.j)) as {
         error?: boolean;
         error_message?: string;
         statements?: unknown[];
      };
      if (parsed.error) {
         return { refused: parsed.error_message ?? "it could not be parsed" };
      }
      return { selects: parsed.statements?.length ?? 0 };
   } finally {
      connection.closeSync();
   }
}

/**
 * The @malloydata/db-duckdb connector splits caller SQL on this marker and runs EVERY
 * fragment (duckdb_connection.ts `runSQLStream`, duckdb_common.ts `runSQL`): each leading
 * fragment as a raw query, then the last as the main statement. Because the marker is a
 * line comment, `json_serialize_sql` treats everything after it as a comment and classifies
 * the whole string as a single SELECT — so a second statement (`SET`, `CREATE SECRET`, …)
 * placed after the marker is invisible to this gate but still executed by the connector.
 * The gate therefore classifies each fragment the connector would run, not the raw string.
 */
const CONNECTOR_STATEMENT_SEPARATOR = "-- hack: split on this";

/**
 * Throws a 400 unless `sql`, as the connector will run it, is exactly one SELECT that names
 * no refused name (duckdb_introspection.ts): an introspection function or view, a DuckLake
 * metadata catalog, a name the lockdown gate refuses, or an attached database's Postgres
 * system schema.
 */
export async function assertSingleSelect(
   connectionName: string,
   sql: string,
   kind:
      | "managed database"
      | "locked-down DuckDB connection" = "managed database",
): Promise<void> {
   const refuse = (why: string): never => {
      throw new BadRequestError(
         `'${connectionName}' is a ${kind}: raw SQL on it runs only as a single SELECT, and ${why}.`,
      );
   };

   // Mirror the connector's split so a statement hidden behind the marker's line comment is
   // classified too, not silently passed as a comment.
   let totalSelects = 0;
   for (const fragment of sql.split(CONNECTOR_STATEMENT_SEPARATOR)) {
      if (fragment.trim() === "") continue;
      const verdict = await classifySelect(fragment);
      if ("refused" in verdict) refuse("it is not a SELECT");
      totalSelects += (verdict as { selects: number }).selects;
   }
   if (totalSelects !== 1) {
      refuse(
         totalSelects === 0 ? "it holds no statement" : `it holds ${totalSelects} statements`,
      );
   }

   // Exactly one SELECT across every fragment: now apply the introspection-name gate over
   // the full text (it scans all fragments).
   const name =
      introspectionNameIn(sql) ??
      introspectionCallInParse((await serializeSQL(sql)).statements);
   if (name === undefined) return;
   throw new BadRequestError(
      `'${connectionName}' is a ${kind}: raw SQL on it may not name '${name}', ` +
         `because Publisher's DuckDB connections do not expose catalog and ` +
         `settings introspection, logging, profiling, SQL run from text, ` +
         `or an attached database beyond its tables, to SQL.`,
   );
}
