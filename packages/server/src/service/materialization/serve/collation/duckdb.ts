// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The DuckDB dialect's collation reader: a DuckDB-dialect source, and a
 * `storage=` or database table in a DuckDB or DuckLake instance. A column's
 * collation is not readable there, so both read the instance: its
 * `default_collation`, and whether a `COLLATE` appears in the SQL or in a
 * definition it holds.
 */

import type { Collation, CollationReader, ProbeIO } from "./probe_io";

/**
 * How many user definitions in the instance name a `COLLATE`: tables, views,
 * and macros (a macro can return a collated value to a view or a query).
 * Attached databases' definitions are listed too.
 */
const DUCKDB_COLLATE_DEFINITIONS =
   "SELECT (SELECT count(*) FROM duckdb_tables() WHERE NOT internal AND sql ILIKE '%collate%') + (SELECT count(*) FROM duckdb_views() WHERE NOT internal AND sql ILIKE '%collate%') + (SELECT count(*) FROM duckdb_functions() WHERE NOT internal AND function_type IN ('macro', 'table_macro') AND (macro_definition IS NULL OR macro_definition ILIKE '%collate%')) AS n";

/**
 * A DuckDB-dialect source compares bytes unless the instance sets a
 * `default_collation`, or a `COLLATE` appears in the source's SQL or in the
 * definition of any table or view the instance holds (a column's collation
 * is not otherwise readable).
 */
async function duckdbCollation(io: ProbeIO, sql: string): Promise<Collation> {
   if (/\bcollate\b/i.test(sql)) return "unknown";
   const [setting] = await io.rows(
      "SELECT current_setting('default_collation') AS c",
   );
   if (String(setting?.c ?? "").trim() !== "") return "unknown";
   const [defs] = await io.rows(DUCKDB_COLLATE_DEFINITIONS);
   return Number(defs?.n ?? 1) === 0 ? "binary" : "unknown";
}

/** The last name of a table path, its quotes off. */
function tableNameOf(path: string): string {
   const parts = path.match(/"(?:[^"]|"")*"|`[^`]*`|[^.]+/g) ?? [path];
   const last = parts[parts.length - 1].trim();
   return /^["`]/.test(last) ? last.slice(1, -1).replace(/""/g, '"') : last;
}

/**
 * A DuckDB or DuckLake table compares bytes unless the instance sets a
 * `default_collation` or the table's definition names a `COLLATE`; a table
 * the catalog does not list is unknown.
 */
async function duckdbTableCollation(
   io: ProbeIO,
   tablePath: string,
): Promise<Collation> {
   const [setting] = await io.rows(
      "SELECT current_setting('default_collation') AS c",
   );
   if (String(setting?.c ?? "").trim() !== "") return "unknown";
   const name = tableNameOf(tablePath).replace(/'/g, "''");
   const defs = await io.rows(
      `SELECT sql FROM duckdb_tables() WHERE table_name = '${name}'`,
   );
   if (defs.length === 0) return "unknown";
   return defs.every(
      (d) => typeof d.sql === "string" && !/\bcollate\b/i.test(d.sql),
   )
      ? "binary"
      : "unknown";
}

/** The DuckDB dialect's reader. */
export const DUCKDB_COLLATION: CollationReader = Object.freeze({
   sourceColumns: duckdbCollation,
   storedTable: duckdbTableCollation,
});
