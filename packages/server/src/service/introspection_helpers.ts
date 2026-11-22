// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What the per-type schema and table listings share: reading a table's
 * columns from its Malloy schema, grouping INFORMATION_SCHEMA rows into
 * tables, the table-name filter, the identifier check a spliced catalog name
 * passes, and the logging and rewrapping of a failed listing.
 *
 * A leaf: it imports no connection module, so the connection-type registry
 * (service/dialects/) and db_utils.ts can both use it.
 */

import type { components } from "../api";
import { BadRequestError, InvalidArgumentError } from "../errors";
import { logger } from "../logger";
import { sqlLiteral } from "./introspection_sql";

type ApiTable = components["schemas"]["Table"];
type ApiColumn = NonNullable<ApiTable["columns"]>[number];

/** A field of a Malloy table schema, as far as columnsOfSchema reads it. */
export interface SchemaField {
   name: string;
   type?: string;
   fields?: SchemaField[];
   elementTypeDef?: { type?: string };
}

/** How deep record members are listed: `delivery.address.zip` is depth 2. */
const MAX_RECORD_DEPTH = 4;

/**
 * A member name a dotted column name can carry as written. A STRUCT key can
 * hold a dot or a space (`{'a.b': 1}`): joined raw, `delivery.a.b` would read
 * as a nested record and does not compile without backticks.
 */
const PLAIN_MEMBER_NAME = /^[A-Za-z_][A-Za-z0-9_]*$/;

/**
 * A table's columns from its Malloy schema: every top-level column, and after
 * each record column (a BigQuery RECORD, a DuckDB STRUCT, a Trino ROW) its
 * members under dotted names (`delivery.city`), nested records included. A
 * repeated record (an array of records) keeps its type, `array`, and its
 * members follow it the same way (`lines.sku`): a member under an `array`
 * column has one value per element, not per row. An array of plain values
 * has no members to list. A member whose name is not a plain identifier
 * (PLAIN_MEMBER_NAME) is not listed, nor anything under it; a top-level
 * column is always listed, as the database names it.
 */
export function columnsOfSchema(
   fields: readonly SchemaField[] | undefined,
   prefix = "",
   depth = 1,
): ApiColumn[] {
   const out: ApiColumn[] = [];
   for (const field of fields ?? []) {
      if (!field.name) continue;
      if (prefix && !PLAIN_MEMBER_NAME.test(field.name)) continue;
      const name = `${prefix}${field.name}`;
      out.push({ name, type: field.type });
      const nested =
         field.type === "record" ||
         (field.type === "array" &&
            field.elementTypeDef?.type === "record_element");
      if (nested && depth <= MAX_RECORD_DEPTH) {
         out.push(...columnsOfSchema(field.fields, `${name}.`, depth + 1));
      }
   }
   return out;
}

/**
 * Build a SQL `AND column IN (...)` fragment for optional table-name filtering.
 * Returns an empty string when `values` is undefined or empty.
 */
export function sqlInFilter(
   columnName: string,
   values?: string[],
   connectionType?: string,
): string {
   if (!values || values.length === 0) return "";
   const escaped = values.map((v) => `'${sqlLiteral(v, connectionType)}'`);
   return `AND ${columnName} IN (${escaped.join(", ")})`;
}

/**
 * A catalog or database name safe to splice into an identifier position
 * (`FROM <catalog>.INFORMATION_SCHEMA.COLUMNS`). Quoting rules differ per
 * dialect, so this REJECTS rather than escapes.
 *
 * The value that matters is the caller's `schemaName` split on its first dot,
 * which `search_database_schema` takes straight from a model-controlled MCP
 * argument. Left raw, `(SELECT c1 AS TABLE_NAME, ... FROM customers) x -- .public`
 * comments out the rest of the query and returns ROW VALUES from a tool whose
 * invariant is that it never returns one. Catalog names from config or SHOW
 * CATALOGS are validated too, for consistency; in the SHOW CATALOGS loop a
 * rejected name is warned and skipped. This shuts out hyphenated Trino catalogs
 * (`hive-prod`); quoting identifiers per dialect would be the real fix.
 *
 * NO HYPHEN: `a--b.public` splices as `FROM a--b.information_schema...`, which
 * runs as `FROM a` with the WHERE commented away. Snowflake, Trino and Unity
 * Catalog need quoting for a hyphenated name anyway, and BigQuery (whose names
 * do carry hyphens) introspects through its client API, never reaching this.
 */
const SAFE_SQL_IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_$]*$/;

export function assertSafeSqlIdentifier(value: string, what: string): string {
   if (!SAFE_SQL_IDENTIFIER.test(value)) {
      throw new InvalidArgumentError(
         `Invalid ${what} "${value}": expected a plain identifier (letters, digits, underscore or dollar, not starting with a digit).`,
      );
   }
   return value;
}

/**
 * Group INFORMATION_SCHEMA.COLUMNS rows into ApiTable objects.
 * Handles both upper-case (Snowflake) and lower-case (Postgres/DuckDB) column names.
 */
export function groupColumnRowsIntoTables(
   rows: unknown[],
   buildResource: (tableName: string) => string,
): ApiTable[] {
   const tableMap = new Map<string, { name: string; type: string }[]>();
   for (const row of rows) {
      const r = row as Record<string, unknown>;
      const tableName = String(r.TABLE_NAME ?? r.table_name ?? "");
      const columnName = String(r.COLUMN_NAME ?? r.column_name ?? "");
      const dataType = String(r.DATA_TYPE ?? r.data_type ?? "").toLowerCase();
      if (!tableName) continue;
      if (!tableMap.has(tableName)) tableMap.set(tableName, []);
      tableMap.get(tableName)!.push({ name: columnName, type: dataType });
   }
   const tables: ApiTable[] = [];
   for (const [tableName, columns] of tableMap) {
      tables.push({ resource: buildResource(tableName), columns });
   }
   return tables;
}

/**
 * Whether a BadRequestError escapes an introspector untouched, and whether it
 * is logged first. Rewrapped as a plain Error it would classify as an internal
 * fault ("try again later"), and an agent would retry an invalid argument
 * forever. Introspectors that never raise one leave it "wrap".
 */
type BadRequestPassThrough = "wrap" | "before-log" | "after-log";

/**
 * Run one introspection call, logging and rewrapping a failure with the
 * connection named. `label` completes both "Error getting <label>" (the log)
 * and "Failed to get <label>: <reason>" (the thrown error).
 */
export async function withIntrospectionFailure<T>(
   label: string,
   fn: () => Promise<T>,
   badRequest: BadRequestPassThrough = "wrap",
): Promise<T> {
   try {
      return await fn();
   } catch (error) {
      if (badRequest === "before-log" && error instanceof BadRequestError) {
         throw error;
      }
      logger.error(`Error getting ${label}`, { error });
      if (badRequest === "after-log" && error instanceof BadRequestError) {
         throw error;
      }
      throw new Error(`Failed to get ${label}: ${(error as Error).message}`, {
         cause: error,
      });
   }
}

export function extractErrorDataFromError(error: unknown): {
   error: string;
   stack?: string;
   task?: unknown;
} {
   const errorMessage = error instanceof Error ? error.message : String(error);
   const errorData: { error: string; stack?: string; task?: unknown } = {
      error: errorMessage,
   };
   if (error instanceof Error && logger.level === "debug") {
      errorData.stack = error.stack;
   }
   if (error && typeof error === "object" && "task" in error) {
      errorData.task = (error as { task?: unknown }).task;
   }
   return errorData;
}
