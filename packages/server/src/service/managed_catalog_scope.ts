// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What SQL on a `managed` DuckLake connection may name: the catalog's own
 * `main` schema, and nothing else.
 *
 * A managed connection's storage secret and file allowlist reach only the
 * `main` schema's files (connection.ts, `storageSecretScope`), but DuckLake
 * answers a good deal without opening a file: a table's columns and types, its
 * row count, unfiltered min and max from file statistics, the catalog listing,
 * and every row of a table small enough that the catalog keeps it inline. When
 * the catalog is also a storage destination, the other schemas hold stored
 * copies of other models' sources, with no gate of this model's. So author and
 * caller SQL on a managed connection is held, before it runs, to references
 * this module can place in `main`:
 *
 * - a table as `t`, `main.t`, `<alias>.t` or `<alias>.main.t` (case-insensitive,
 *   as DuckDB resolves names), whose name is a plain table name
 *   ({@link isPlainTableName}). A name the catalog's `main` lacks falls through
 *   the search path, however it is qualified: to DuckDB's own catalog views
 *   (every one starts with a {@link SYSTEM_RELATION_PREFIXES} prefix), or to a
 *   replacement scan that reads a file. A two-part name whose first part is
 *   the alias reaches the catalog's default schema, `main`; DuckDB refuses it
 *   as ambiguous should a schema of that name exist.
 * - no `DESCRIBE`, `SHOW` or `SUMMARIZE`;
 * - a table function only from {@link ALLOWED_TABLE_FUNCTIONS};
 * - no function call qualified with a catalog or a schema other than `main`,
 *   and no function that reads the catalog ({@link isCatalogFunction}).
 *
 * Anything DuckDB's parser cannot serialize as SELECT statements is refused.
 * Checked against DuckDB 1.5.5. DuckDB and DuckLake offer no attach option or
 * setting that hides a schema, the metadata catalog, file statistics or
 * inlined rows from a read-only attach, so this check is the control.
 */

import { serializeSQL } from "./single_select";

/**
 * The prefixes every relation in DuckDB's `system` catalog (its `main` and
 * `pg_catalog` schemas, which the search path reaches) starts with.
 */
export const SYSTEM_RELATION_PREFIXES: readonly string[] = [
   "duckdb_",
   "pg_",
   "pragma_",
   "sqlite_",
];

/** Table functions that read no catalog or file: they make rows from values. */
export const ALLOWED_TABLE_FUNCTIONS: ReadonlySet<string> = new Set([
   "range",
   "generate_series",
   "unnest",
]);

function hasSystemPrefix(name: string): boolean {
   return SYSTEM_RELATION_PREFIXES.some((prefix) => name.startsWith(prefix));
}

/**
 * Whether `name` (lowercased) can only be a table: not one of DuckDB's own
 * catalog relations ({@link SYSTEM_RELATION_PREFIXES}), and nothing DuckDB
 * would read as a file through a replacement scan (a path, a URL, a glob, or
 * a name with an extension, so no dot at all).
 */
export function isPlainTableName(name: string): boolean {
   return (
      name !== "" && !hasSystemPrefix(name) && !/[./\\:*?[\]{}~]/.test(name)
   );
}

/** Whether scalar function `name` (lowercased) reads the catalog. */
export function isCatalogFunction(name: string): boolean {
   return (
      hasSystemPrefix(name) ||
      name === "current_schemas" ||
      name.startsWith("has_") ||
      name.endsWith("_description") ||
      name === "format_pg_type" ||
      name === "format_type" ||
      name === "get_block_size"
   );
}

const str = (value: unknown): string =>
   typeof value === "string" ? value.toLowerCase() : "";

/** A function call named with no catalog, and no schema but DuckDB's own `main`. */
function unqualified(fn: Record<string, unknown>): boolean {
   const schema = str(fn.schema);
   return !str(fn.catalog) && (schema === "" || schema === "main");
}

/**
 * The first reference in DuckDB's serialized parse (`json_serialize_sql`) that
 * a managed connection named `alias` may not make, described for the author
 * as they wrote it; undefined when every reference is in `main`.
 */
export function managedCatalogRefusalInParse(
   statements: unknown,
   alias: string,
): string | undefined {
   const catalog = alias.toLowerCase();
   let refused: string | undefined;
   const walk = (node: unknown): void => {
      if (refused || node === null || typeof node !== "object") return;
      if (Array.isArray(node)) {
         node.forEach(walk);
         return;
      }
      const record = node as Record<string, unknown>;
      refused = refusalOfNode(record, catalog);
      if (refused) return;
      Object.values(record).forEach(walk);
   };
   walk(statements);
   return refused;
}

function refusalOfNode(
   record: Record<string, unknown>,
   catalog: string,
): string | undefined {
   switch (record.type) {
      case "SHOW_REF":
         return "a DESCRIBE, SHOW or SUMMARIZE";
      case "DELIM_GET":
      case "BOUND_TABLE_REF":
         return "a table reference it cannot place";
      case "BASE_TABLE": {
         const [c, s, t] = [
            str(record.catalog_name),
            str(record.schema_name),
            str(record.table_name),
         ];
         const shown = `'${[c, s, t].filter(Boolean).join(".")}'`;
         // However it is qualified, a name the catalog's `main` lacks falls
         // through the search path (to `system.main`, `system.pg_catalog` or
         // a replacement scan), so the name itself must be one only a table
         // of the catalog can answer to.
         if (!isPlainTableName(t)) return shown;
         if (c) return c === catalog && s === "main" ? undefined : shown;
         if (s) return s === "main" || s === catalog ? undefined : shown;
         return undefined;
      }
      case "TABLE_FUNCTION": {
         const fn = record.function as Record<string, unknown> | undefined;
         const name = str(fn?.function_name);
         if (fn && unqualified(fn) && ALLOWED_TABLE_FUNCTIONS.has(name)) {
            // Its arguments are expressions: walked by the caller.
            return undefined;
         }
         return `the table function '${name || "?"}'`;
      }
   }
   if (
      record.class === "FUNCTION" &&
      typeof record.function_name === "string"
   ) {
      const name = str(record.function_name);
      // DuckDB's parser itself writes some built-ins as `main.<name>`
      // (`list_value` for a list literal): the system catalog's own schema.
      if (!unqualified(record) || isCatalogFunction(name)) {
         return `the function '${name}'`;
      }
   }
   return undefined;
}

/**
 * Why `sql` may not run on the managed connection `alias`, or undefined when
 * every reference it makes is in the catalog's `main` schema
 * ({@link managedCatalogRefusalInParse}). SQL DuckDB's parser does not
 * serialize as SELECT statements is refused.
 */
export async function managedCatalogRefusal(
   sql: string,
   alias: string,
): Promise<string | undefined> {
   const parsed = await serializeSQL(sql);
   if (parsed.error || !Array.isArray(parsed.statements)) {
      return "SQL it cannot read as a SELECT";
   }
   return managedCatalogRefusalInParse(parsed.statements, alias);
}

/** {@link managedCatalogRefusal} of a `.table()` path, read as Malloy places it, after FROM. */
export function managedTablePathRefusal(
   tablePath: string,
   alias: string,
): Promise<string | undefined> {
   return managedCatalogRefusal(`SELECT * FROM ${tablePath}`, alias);
}

/** The author-facing message for a refusal on managed connection `connection`. */
export function managedCatalogMessage(
   what: string,
   connection: string,
   refused: string,
): string {
   return (
      `${what} on managed connection '${connection}' names ${refused}, which ` +
      `it may not: a managed connection reads only its own catalog's tables, ` +
      `in the catalog's main schema.`
   );
}
