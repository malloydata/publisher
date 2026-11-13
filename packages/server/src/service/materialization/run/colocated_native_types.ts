// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Each column of a colocated table just built, with its type as the
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
 * One catalog read per dialect: DuckDB's `DESCRIBE`, Postgres's
 * `format_type`, and `information_schema.columns` for the rest. A read that
 * fails or finds nothing captures nothing: the build stands, and the served
 * copy answers the table's big-integer columns live.
 */

import { logger } from "../../../logger";
import { errMessage } from "../../../utils";
import { quoteTablePath } from "../../quoting";

/** A column and its warehouse type. */
export interface NativeColumn {
   name: string;
   type: string;
}

/** Enough rows for any table's columns (runSQL's default is ten). */
export const NATIVE_SCHEMA_ROW_LIMIT = 100_000;

/** A SQL string literal (single quotes doubled). */
function sqlString(value: string): string {
   return `'${value.replace(/'/g, "''")}'`;
}

/** A row's value for `key`, whatever case the warehouse returns names in. */
function field(row: Record<string, unknown>, key: string): unknown {
   if (key in row) return row[key];
   const k = Object.keys(row).find((x) => x.toLowerCase() === key);
   return k === undefined ? undefined : row[k];
}

/**
 * `information_schema.columns`' `data_type` as a full type: a decimal type
 * reported without its precision and scale (Snowflake's `NUMBER`, SQL
 * Server's `decimal`) gets them from their own columns.
 */
function informationSchemaType(row: Record<string, unknown>): string {
   const type = String(field(row, "data_type") ?? "").trim();
   const precision = field(row, "numeric_precision");
   const scale = field(row, "numeric_scale");
   if (
      /^(number|numeric|decimal)$/i.test(type) &&
      precision !== null &&
      precision !== undefined
   ) {
      return `${type}(${Number(precision)},${Number(scale ?? 0)})`;
   }
   return type;
}

/** The catalog query for a table's columns, and how to read its rows. */
function catalogRead(
   physicalTableName: string,
   dialect: string,
):
   | { sql: string; read: (row: Record<string, unknown>) => NativeColumn }
   | undefined {
   const quoted = quoteTablePath(physicalTableName, dialect);
   const parts = physicalTableName.split(".");
   const table = parts[parts.length - 1];
   const schema = parts.length > 1 ? parts[parts.length - 2] : undefined;
   switch (dialect) {
      case "duckdb":
         return {
            sql: `DESCRIBE ${quoted}`,
            read: (r) => ({
               name: String(field(r, "column_name")),
               type: String(field(r, "column_type")),
            }),
         };
      case "postgres":
         return {
            // Wrapped as Malloy's Postgres connection reads every result:
            // one JSON `row` per row.
            sql:
               "SELECT row_to_json(c) AS row FROM (" +
               "SELECT a.attname AS name, format_type(a.atttypid, a.atttypmod) AS type, a.attnum " +
               `FROM pg_attribute a WHERE a.attrelid = to_regclass(${sqlString(quoted)}) ` +
               "AND a.attnum > 0 AND NOT a.attisdropped) c ORDER BY c.attnum",
            read: (r) => ({
               name: String(field(r, "name")),
               type: String(field(r, "type")),
            }),
         };
      case "standardsql": {
         if (parts.length < 2) return undefined;
         const dataset = quoteTablePath(parts.slice(0, -1).join("."), dialect);
         return {
            sql:
               `SELECT column_name, data_type FROM ${dataset}.INFORMATION_SCHEMA.COLUMNS ` +
               `WHERE table_name = ${sqlString(table)} ORDER BY ordinal_position`,
            read: (r) => ({
               name: String(field(r, "column_name")),
               type: String(field(r, "data_type")),
            }),
         };
      }
      default:
         return {
            sql:
               "SELECT column_name, data_type, numeric_precision, numeric_scale " +
               "FROM information_schema.columns " +
               `WHERE lower(table_name) = lower(${sqlString(table)})` +
               (schema
                  ? ` AND lower(table_schema) = lower(${sqlString(schema)})`
                  : "") +
               " ORDER BY ordinal_position",
            read: (r) => ({
               name: String(field(r, "column_name")),
               type: informationSchemaType(r),
            }),
         };
   }
}

/**
 * The columns of the colocated table `physicalTableName` with their warehouse
 * types, or undefined when the catalog read fails or finds none.
 */
export async function capturedNativeSchema(args: {
   /** Runs one SQL statement on the table's own connection, every row back. */
   run: (sql: string) => Promise<{ rows: unknown[] }>;
   physicalTableName: string;
   dialect: string;
   sourceName: string;
}): Promise<NativeColumn[] | undefined> {
   const read = catalogRead(args.physicalTableName, args.dialect);
   if (!read) return undefined;
   try {
      const result = await args.run(read.sql);
      const columns = (result.rows as Record<string, unknown>[])
         .map(read.read)
         .filter((c) => c.name !== "" && c.type !== "");
      if (columns.length > 0) return columns;
   } catch (err) {
      logger.warn(
         "Could not read a colocated table's warehouse column types after its build; its big-integer columns are answered live",
         {
            sourceName: args.sourceName,
            physicalTableName: args.physicalTableName,
            error: errMessage(err),
         },
      );
      return undefined;
   }
   logger.warn(
      "A colocated table's warehouse column types read back empty after its build; its big-integer columns are answered live",
      {
         sourceName: args.sourceName,
         physicalTableName: args.physicalTableName,
      },
   );
   return undefined;
}
