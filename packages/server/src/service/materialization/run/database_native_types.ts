// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Each column of a database table just built, with its type as the
 * warehouse's own catalog names it (`bigint`, `numeric`, `NUMBER(38,0)`,
 * `UHUGEINT`): the entry's `nativeSchema`.
 *
 * Malloy reads a table's columns back as Malloy types only, and a type it
 * reads as a `bigint` number can be narrower than the column (known limitation
 * 12): Postgres stores a `sum` of BIGINTs as `numeric`, which holds values past
 * 2^63; SQL Server's `decimal(38,0)` is typed `bigint`; DuckDB's `UHUGEINT`
 * holds values past `HUGEINT`. The served copy reads a big-integer column
 * through a cast to exactly the type captured here, so nothing is narrowed,
 * and answers a column it has no type for live.
 *
 * One catalog read per dialect (warehouse_sql.ts): DuckDB's `DESCRIBE`,
 * Postgres's `format_type`, and `information_schema.columns` for the rest,
 * read in the table's own catalog when its name gives one. A read that
 * fails, finds nothing, or finds the name in more than one schema (an
 * unqualified name matches a table of that name in every schema
 * `information_schema` shows) captures nothing: the build stands, and the
 * served copy answers the table's big-integer columns live.
 */

import { errorCodeOf } from "../../../error_code";
import { logger } from "../../../logger";
import { nativeTypeCatalogRead, type NativeColumn } from "./warehouse_sql";

/** Enough rows for any table's columns (runSQL's default is ten). */
export const NATIVE_SCHEMA_ROW_LIMIT = 100_000;

/**
 * The columns of the database table `physicalTableName` with their warehouse
 * types, or undefined when the catalog read fails or finds none.
 */
export async function capturedNativeSchema(args: {
   /**
    * Runs one SQL statement on the table's own connection, with the row limit
    * given (a connection's own default can be as few as ten rows).
    */
   run: (
      sql: string,
      options: { rowLimit: number },
   ) => Promise<{ rows: unknown[] }>;
   physicalTableName: string;
   dialect: string;
   sourceName: string;
}): Promise<NativeColumn[] | undefined> {
   const read = nativeTypeCatalogRead(args.physicalTableName, args.dialect);
   if (!read) return undefined;
   try {
      const result = await args.run(read.sql, {
         rowLimit: NATIVE_SCHEMA_ROW_LIMIT,
      });
      const rows = result.rows as Record<string, unknown>[];
      const tables = new Set(read.tableOf ? rows.map(read.tableOf) : []);
      if (tables.size > 1) {
         logger.warn(
            "A database table's name matched more than one table in the warehouse's catalog; its big-integer columns are answered live",
            {
               sourceName: args.sourceName,
               physicalTableName: args.physicalTableName,
               tables: [...tables],
            },
         );
         return undefined;
      }
      const columns = rows
         .map(read.read)
         .filter((c) => c.name !== "" && c.type !== "");
      if (columns.length > 0) return columns;
   } catch (err) {
      logger.warn(
         "Could not read a database table's warehouse column types after its build; its big-integer columns are answered live",
         {
            sourceName: args.sourceName,
            physicalTableName: args.physicalTableName,
            code: errorCodeOf(err),
         },
      );
      return undefined;
   }
   logger.warn(
      "A database table's warehouse column types read back empty after its build; its big-integer columns are answered live",
      {
         sourceName: args.sourceName,
         physicalTableName: args.physicalTableName,
      },
   );
   return undefined;
}
