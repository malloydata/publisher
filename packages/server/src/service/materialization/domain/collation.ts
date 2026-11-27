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
 * The `collation_name` values BigQuery's `INFORMATION_SCHEMA` gives a column
 * with no collation, upper-cased and trimmed: the literal string `'NULL'`
 * (what it reports for an uncollated column), and the empty string. A real
 * NULL reads the same. One list, read by {@link bigqueryNamesNoCollation}
 * and {@link bigqueryCollatedSql}, so a build's contract capture and the
 * serve side's catalog reads agree on which columns are collated.
 */
const BQ_NO_COLLATION = ["", "NULL"] as const;

/** Whether a BigQuery `collation_name` names no collation ({@link BQ_NO_COLLATION}). */
export function bigqueryNamesNoCollation(name: unknown): boolean {
   return (
      typeof name !== "string" ||
      (BQ_NO_COLLATION as readonly string[]).includes(name.trim().toUpperCase())
   );
}

/**
 * {@link bigqueryNamesNoCollation}'s negation as a BigQuery SQL predicate over
 * `column`: true where it names a collation.
 */
export function bigqueryCollatedSql(column: string): string {
   return `UPPER(TRIM(COALESCE(${column}, ''))) NOT IN (${BQ_NO_COLLATION.map((v) => `'${v}'`).join(", ")})`;
}

/**
 * A BigQuery column's `collation_name` as an identity: one that names no
 * collation ({@link bigqueryNamesNoCollation}) compares bytes (`binary`),
 * any other reads `bigquery:<name>`.
 */
export function bigqueryColumnCollation(name: unknown): string {
   return bigqueryNamesNoCollation(name)
      ? BINARY_COLLATION
      : `bigquery:${(name as string).trim()}`;
}

/**
 * A collation identity as a contract records it, read back canonically: a
 * `bigquery:` identity that names no collation (`bigquery:NULL`, which builds
 * of 0.18.0 to 0.18.2 recorded for every uncollated BigQuery string column
 * from the catalog's literal `'NULL'`) is `binary`, which is what such a
 * column compares as and what a capture records for it now. Any other
 * identity is itself.
 */
export function canonicalCollation(identity: string): string {
   const prefix = "bigquery:";
   return identity.startsWith(prefix) &&
      bigqueryNamesNoCollation(identity.slice(prefix.length))
      ? BINARY_COLLATION
      : identity;
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

/**
 * Why a build's catalog read did not prove its strings byte-exact, when it
 * did not: a read that failed or saw too little. Bounded, so a log line
 * carries one of these and never a driver's text.
 */
export type BigQueryBuildUnreadReason =
   /** The build's SQL names a COLLATE, which no catalog read accounts for. */
   | "collate_in_sql"
   /** The connection names no project to read the catalog of. */
   | "no_job_project"
   /** A table the source reads is on another connection. */
   | "table_on_other_connection"
   /** A table path that names no one table (a wildcard, too many segments). */
   | "table_unplaceable"
   /** The project-wide read of the tables' types failed or was denied. */
   | "table_types_unread"
   /** The project-wide read did not list a table the source reads. */
   | "table_not_listed"
   /** A project's regions or collated datasets could not be read. */
   | "project_unread"
   /** A project holds a collated dataset, so an unplaced read may be collated. */
   | "project_collated"
   /** A dataset's own catalog read failed or was denied. */
   | "dataset_unread"
   /** A dataset's own read did not list the table, or not as a table. */
   | "dataset_table_not_listed"
   /** A read threw where none of the above names it. */
   | "read_failed";

/**
 * Where a BigQuery build's failed catalog read was looking, never a
 * credential or a driver's text: the project (the connection's own when the
 * read failed before reaching one), the connection's location (absent: each
 * dataset's location was to be read), and the dataset and table it reached.
 */
export interface BigQueryBuildUnread {
   reason: BigQueryBuildUnreadReason;
   project?: string;
   location?: string;
   dataset?: string;
   table?: string;
   /** The catalog's type for a table read as other than a table. */
   tableType?: string;
}
