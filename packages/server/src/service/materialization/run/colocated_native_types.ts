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
 * `format_type`, and `information_schema.columns` for the rest, read in the
 * table's own catalog when its name gives one. A read that fails, finds
 * nothing, or finds the name in more than one schema (an unqualified name
 * matches a table of that name in every schema `information_schema` shows)
 * captures nothing: the build stands, and the served copy answers the
 * table's big-integer columns live.
 */

import { logger } from "../../../logger";
import { errMessage } from "../../../utils";
import { quoteTablePath } from "../../quoting";

/** A column and its warehouse type. */
interface NativeColumn {
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

/**
 * The catalog query for a table's columns, how to read its rows, and, for a
 * read that can match more than one table, which table a row describes.
 */
function catalogRead(
   physicalTableName: string,
   dialect: string,
):
   | {
        sql: string;
        read: (row: Record<string, unknown>) => NativeColumn;
        tableOf?: (row: Record<string, unknown>) => string;
     }
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
      default: {
         // A three-part name's catalog (Snowflake's or SQL Server's database,
         // Trino's or Databricks' catalog) holds its own `information_schema`;
         // unqualified, the view is the session's, where a table of the same
         // schema and name would be read instead.
         const catalog =
            parts.length > 2
               ? `${quoteTablePath(parts.slice(0, -2).join("."), dialect)}.`
               : "";
         // Trino's `information_schema.columns` has no precision columns: its
         // `data_type` carries them (`decimal(38,0)`).
         const precision =
            dialect === "trino" || dialect === "presto"
               ? ""
               : ", numeric_precision, numeric_scale";
         // MySQL resolves an unqualified name in the connection's database,
         // and its `information_schema` shows every database's tables.
         const schemaFilter = schema
            ? ` AND lower(table_schema) = lower(${sqlString(schema)})`
            : dialect === "mysql"
              ? " AND table_schema = DATABASE()"
              : "";
         return {
            sql:
               "SELECT table_catalog, table_schema, table_name, column_name, data_type" +
               precision +
               ` FROM ${catalog}information_schema.columns` +
               ` WHERE lower(table_name) = lower(${sqlString(table)})` +
               schemaFilter +
               " ORDER BY ordinal_position",
            read: (r) => ({
               name: String(field(r, "column_name")),
               type: informationSchemaType(r),
            }),
            tableOf: (r) =>
               ["table_catalog", "table_schema", "table_name"]
                  .map((k) => String(field(r, k)))
                  .join("."),
         };
      }
   }
}

/**
 * The columns of the colocated table `physicalTableName` with their warehouse
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
   const read = catalogRead(args.physicalTableName, args.dialect);
   if (!read) return undefined;
   try {
      const result = await args.run(read.sql, {
         rowLimit: NATIVE_SCHEMA_ROW_LIMIT,
      });
      const rows = result.rows as Record<string, unknown>[];
      const tables = new Set(read.tableOf ? rows.map(read.tableOf) : []);
      if (tables.size > 1) {
         logger.warn(
            "A colocated table's name matched more than one table in the warehouse's catalog; its big-integer columns are answered live",
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
