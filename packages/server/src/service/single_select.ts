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
 * the prefixes of the schemas the view may read.
 *
 * Classified by DuckDB's own parser, the same engine the statement runs on:
 * `json_serialize_sql` serializes SELECT statements only, and answers one entry
 * per statement. It parses without binding, so it needs no catalog. Anything it
 * cannot classify is refused.
 */
let parser: Promise<DuckDBInstance> | undefined;

/**
 * The statement separator `@malloydata/db-duckdb` splits raw SQL on before it
 * runs any of it (its DuckDB dialect's `hackSplitComment`). The connector
 * splits with `String.prototype.split` on this exact, case-sensitive text,
 * wherever it appears: inside a comment or a string literal too. So DuckDB's
 * parser reads the text after it as part of a comment, while the connector
 * runs it as a statement of its own.
 */
export const DUCKDB_STATEMENT_SEPARATOR = "-- hack: split on this";

/** Whether the connector would split `sql` into more than one statement. */
export function containsStatementSeparator(sql: string): boolean {
   return sql.includes(DUCKDB_STATEMENT_SEPARATOR);
}

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
 * Throws a 400 unless `sql` is exactly one SELECT that names no refused name
 * (duckdb_introspection.ts): an introspection function or view, a DuckLake
 * metadata catalog, a name the lockdown gate refuses, or an attached
 * database's Postgres system schema.
 *
 * The text is judged as the connector will run it: SQL that spells the
 * connector's statement separator ({@link DUCKDB_STATEMENT_SEPARATOR}) is
 * refused outright, because the connector would run what follows it as a
 * separate statement that DuckDB's parser, here, reads as a comment.
 */
export async function assertSingleSelect(
   connectionName: string,
   sql: string,
   kind:
      | "managed database"
      | "locked-down DuckDB connection" = "managed database",
): Promise<void> {
   if (containsStatementSeparator(sql)) {
      throw new BadRequestError(
         `'${connectionName}' is a ${kind}: raw SQL on it runs only as a single SELECT, ` +
            `and it contains the DuckDB connector's statement separator comment, ` +
            `which the connector splits into separate statements wherever it appears ` +
            `(in a comment or a string literal too).`,
      );
   }
   const verdict = await classifySelect(sql);
   if ("selects" in verdict && verdict.selects === 1) {
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
   const why =
      "refused" in verdict
         ? "it is not a SELECT"
         : verdict.selects === 0
           ? "it holds no statement"
           : `it holds ${verdict.selects} statements`;
   throw new BadRequestError(
      `'${connectionName}' is a ${kind}: raw SQL on it runs only as a single SELECT, and ${why}.`,
   );
}
