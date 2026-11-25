// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The DuckDB dialect's collation reader: a DuckDB-dialect source, and a
 * `storage=` or database table in a DuckDB or DuckLake instance. A column's
 * collation is not readable there, so both read the instance: its
 * `default_collation`, and whether a `COLLATE` appears in the SQL or in a
 * definition it holds.
 */

import { DUCKDB_COLLATE_DEFINITIONS } from "../../domain/collation";
import type { Collation, CollationReader, CatalogIO } from "./catalog_io";

/**
 * A DuckDB-dialect source compares bytes unless the instance sets a
 * `default_collation`, or a `COLLATE` appears in the source's SQL or in the
 * definition of any table or view the instance holds (a column's collation
 * is not otherwise readable).
 */
async function duckdbCollation(io: CatalogIO, sql: string): Promise<Collation> {
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
   io: CatalogIO,
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

/** Whether the instance lists the table at `tablePath`, matched without case. */
async function duckdbTableListed(
   io: CatalogIO,
   tablePath: string,
): Promise<boolean> {
   const name = tableNameOf(tablePath).replace(/'/g, "''");
   const [row] = await io.rows(
      `SELECT count(*) AS n FROM duckdb_tables() WHERE lower(table_name) = lower('${name}')`,
   );
   return Number(row?.n ?? 1) !== 0;
}

/** The DuckDB dialect's reader. */
export const DUCKDB_COLLATION: CollationReader & {
   readonly scope: "instance";
} = Object.freeze({
   scope: "instance" as const,
   sourceColumns: duckdbCollation,
   storedTable: duckdbTableCollation,
   tableListed: duckdbTableListed,
});
