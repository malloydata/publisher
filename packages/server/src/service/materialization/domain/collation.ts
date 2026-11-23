// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * How a string column compares, as data and pure rules: the three-way
 * judgement every collation read answers (`Collation`), the Postgres
 * catalog's collation rows judged and identified (shared by the serve
 * side's per-query reads and a build's contract capture), and a DuckDB
 * table's column collations read from its definition, where its catalog
 * does not list them.
 *
 * Pure: the SQL here is text a caller runs.
 */

/**
 * How a string column compares: byte-exact under every operator (`binary`),
 * byte-exact for equality only (`deterministic`), or not known to be either
 * (`unknown`, which fails closed).
 */
export type Collation = "binary" | "deterministic" | "unknown";

/** The normalised identity of a byte-wise collation, on any engine. */
export const BINARY_COLLATION = "binary";

/**
 * A Postgres collation row, judged: the database default by the database's
 * own collation and locale provider (an ICU default is deterministic, never
 * binary, whatever `datcollate` says), any other by its own row. Binary under
 * a libc `C`, `POSIX` or `C.UTF-8`, and under the builtin provider (Postgres 17+:
 * `pg_c_utf8`, `ucs_basic`, `pg_unicode_fast`, or a database created with
 * it), which orders and compares by code point, as UTF-8 bytes do.
 */
export function postgresJudged(
   row: Record<string, unknown> | undefined,
): Collation {
   if (!row) return "unknown";
   // libc `C`, `POSIX`, and `C.UTF-8` (its codeset spelled any way; the
   // locale name itself compares exactly),
   // which glibc orders by code point, as UTF-8 bytes do.
   const binaryName = (c: unknown) =>
      typeof c === "string" &&
      (c === "C" || c === "POSIX" || /^C\.[Uu][Tt][Ff]-?8$/.test(c));
   if (row.name === "default") {
      if (row.dbp === "b") return "binary";
      return row.dbp !== "i" && binaryName(row.dbc)
         ? "binary"
         : "deterministic";
   }
   if (row.det !== true) return "unknown";
   if (row.prov === "b") return "binary";
   return row.prov === "c" && binaryName(row.cc) ? "binary" : "deterministic";
}

/** The columns {@link postgresJudged} reads, from `pg_collation c, pg_database d`. */
export const PG_COLLATION_COLUMNS =
   "c.collname AS name, c.collisdeterministic AS det, c.collcollate AS cc, c.collprovider AS prov, d.datcollate AS dbc, to_jsonb(d) ->> 'datlocprovider' AS dbp";

/**
 * The resolved identity of a Postgres collation, from `pg_collation c` and
 * `pg_database d`: the database default by the database's own provider and
 * locale, any other by its provider, determinism and locale, each with the
 * library version it was recorded under (two clusters on different ICU or
 * libc versions do not match). Never by name, since `default` and a
 * same-named collation mean other things in other databases. An explicit
 * column collation equal to the database default reads as another identity:
 * that over-blocks, never under-blocks.
 */
export const PG_COLLATION_IDENTITY = `CASE WHEN c.collname = 'default' AND c.collnamespace = 'pg_catalog'::regnamespace THEN concat_ws('|', 'db', coalesce(to_jsonb(d) ->> 'datlocprovider', ''), d.datcollate, d.datctype, coalesce(to_jsonb(d) ->> 'daticulocale', ''), coalesce(to_jsonb(d) ->> 'datlocale', ''), coalesce(to_jsonb(d) ->> 'daticurules', ''), coalesce(to_jsonb(d) ->> 'datcollversion', '')) ELSE concat_ws('|', 'coll', c.collprovider, c.collisdeterministic::text, coalesce(c.collcollate, ''), coalesce(c.collctype, ''), coalesce(to_jsonb(c) ->> 'colliculocale', ''), coalesce(to_jsonb(c) ->> 'colllocale', ''), coalesce(to_jsonb(c) ->> 'collicurules', ''), coalesce(to_jsonb(c) ->> 'collversion', '')) END`;

/**
 * One table's column collations as a catalog read finds them: each named
 * column's identity (null: it has none, not a string), and what a column
 * the read did not name holds (`otherwise`; null: unknown).
 */
export interface TableCollations {
   byColumn: ReadonlyMap<string, string | null>;
   otherwise: string | null;
   /**
    * DuckDB: the reading session's `default_collation`, as an identity
    * ({@link duckdbDefaultCollation}), which every column declared with no
    * `COLLATE` compares under. Absent where it is not read.
    */
   defaultCollation?: string;
}

/**
 * A DuckDB session's `default_collation` setting as an identity: unset or
 * empty compares bytes (`binary`), any other reads `duckdb:<name>`
 * (lower-cased).
 */
export function duckdbDefaultCollation(setting: unknown): string {
   const name = typeof setting === "string" ? setting.trim().toLowerCase() : "";
   return name === "" || name === "binary"
      ? BINARY_COLLATION
      : `duckdb:${name}`;
}

/** The collation identity of `column` in `t`, matched exactly then case-blind. */
export function collationOf(t: TableCollations, column: string): string | null {
   if (t.byColumn.has(column)) return t.byColumn.get(column) ?? null;
   const folded = [...t.byColumn.keys()].filter(
      (k) => k.toLowerCase() === column.toLowerCase(),
   );
   if (folded.length === 1) return t.byColumn.get(folded[0]) ?? null;
   return t.otherwise;
}

/**
 * Each column's collation in a DuckDB table, from the table's definition
 * (`duckdb_tables().sql`): DuckDB's catalog lists no column collation, so
 * a column declared `COLLATE <name>` reads `duckdb:<name>` (lower-cased)
 * and every other column `binary`. A definition naming a `COLLATE` this
 * read cannot attribute to one column reads every column as
 * `duckdb:unplaced`, which matches no other. The instance's
 * `default_collation` is the reading session's, not the table's: the
 * catalog read records it beside these (`TableCollations.defaultCollation`).
 */
export function duckdbTableCollations(definition: string): TableCollations {
   const byColumn = new Map<string, string | null>();
   const declared =
      /("(?:[^"]|"")+"|[A-Za-z_][A-Za-z0-9_$]*)\s+[A-Za-z][A-Za-z0-9_]*(?:\s*\([^)]*\))?\s+COLLATE\s+("(?:[^"]|"")+"|[A-Za-z0-9_.]+)/gi;
   let match: RegExpExecArray | null;
   let count = 0;
   while ((match = declared.exec(definition)) !== null) {
      count++;
      byColumn.set(
         unquoted(match[1]),
         `duckdb:${unquoted(match[2]).toLowerCase()}`,
      );
   }
   const total = (definition.match(/\bCOLLATE\b/gi) ?? []).length;
   return total > count
      ? { byColumn: new Map(), otherwise: "duckdb:unplaced" }
      : { byColumn, otherwise: BINARY_COLLATION };
}

/** A name with its double quotes off. */
function unquoted(name: string): string {
   return /^".*"$/.test(name) ? name.slice(1, -1).replace(/""/g, '"') : name;
}

/**
 * A BigQuery column's `collation_name` as an identity: an empty or absent
 * one compares bytes (`binary`), any other reads `bigquery:<name>`.
 */
export function bigqueryColumnCollation(name: unknown): string {
   return typeof name === "string" && name.trim() !== ""
      ? `bigquery:${name.trim()}`
      : BINARY_COLLATION;
}

/**
 * A Postgres column's collation identity as a contract records it: the
 * catalog identity ({@link PG_COLLATION_IDENTITY}) prefixed `postgres:`,
 * or `binary` when the collation is judged binary.
 */
export function postgresColumnCollation(
   identity: string,
   judged: Collation,
): string {
   return judged === "binary" ? BINARY_COLLATION : `postgres:${identity}`;
}

/**
 * How many user definitions in the instance name a `COLLATE`: tables, views,
 * and macros (a macro can return a collated value to a view or a query).
 * Attached databases' definitions are listed too.
 */
export const DUCKDB_COLLATE_DEFINITIONS =
   "SELECT (SELECT count(*) FROM duckdb_tables() WHERE NOT internal AND sql ILIKE '%collate%') + (SELECT count(*) FROM duckdb_views() WHERE NOT internal AND sql ILIKE '%collate%') + (SELECT count(*) FROM duckdb_functions() WHERE NOT internal AND function_type IN ('macro', 'table_macro') AND (macro_definition IS NULL OR macro_definition ILIKE '%collate%')) AS n";
