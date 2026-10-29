// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBInstance } from "@duckdb/node-api";
import { BadRequestError } from "../errors";

/**
 * Raw SQL on a managed DuckLake connection (`Connection.managed`) runs only when
 * it is exactly one SELECT. The connection view is read-only, but a session that
 * accepts arbitrary statements can create a secret or `SET` its configuration,
 * and the storage secret's `SCOPE` does not reach a secret the session made
 * itself. One SELECT can do neither, so it can be no worse than a model's query,
 * whose `sql()` sources the compiler already places inside a subquery.
 *
 * Classified by DuckDB's own parser, the same engine the statement runs on:
 * `json_serialize_sql` serializes SELECT statements only, and answers one entry
 * per statement. It parses without binding, so it needs no catalog. Anything it
 * cannot classify is refused.
 */
let parser: Promise<DuckDBInstance> | undefined;

async function parserInstance(): Promise<DuckDBInstance> {
   if (!parser) {
      parser = DuckDBInstance.create(":memory:").catch((error) => {
         parser = undefined;
         throw error;
      });
   }
   return parser;
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

/** Throws a 400 unless `sql` is exactly one SELECT. */
export async function assertSingleSelect(
   connectionName: string,
   sql: string,
): Promise<void> {
   const verdict = await classifySelect(sql);
   if ("selects" in verdict && verdict.selects === 1) return;
   const why =
      "refused" in verdict
         ? "it is not a SELECT"
         : verdict.selects === 0
           ? "it holds no statement"
           : `it holds ${verdict.selects} statements`;
   throw new BadRequestError(
      `'${connectionName}' is a managed database: raw SQL on it runs only as a single SELECT, and ${why}.`,
   );
}
