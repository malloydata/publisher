// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The Postgres dialect's collation reader. A Postgres column carries its own
 * collation, read with `pg_collation_for` from the source's SQL or from a
 * table's catalog row, and judged by provider and locale (never by name);
 * the database's default collation is read for what a literal or a given
 * compares under. Each collation can also be read as its resolved identity,
 * for a database table judged against its source.
 */

import type { Collation, CollationReader, ProbeIO } from "./probe_io";

/** Postgres rows, which Malloy's Postgres connection reads from a `row` column. */
function pgRows(io: ProbeIO, sql: string): Promise<Record<string, unknown>[]> {
   return io.rows(`SELECT row_to_json(t) AS row FROM (${sql}) t`);
}

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
const PG_COLLATION_COLUMNS =
   "c.collname AS name, c.collisdeterministic AS det, c.collcollate AS cc, c.collprovider AS prov, d.datcollate AS dbc, to_jsonb(d) ->> 'datlocprovider' AS dbp";

/** The Postgres database's default collation, as {@link postgresJudged} reads it. */
async function postgresDefaultCollation(io: ProbeIO): Promise<Collation> {
   const [row] = await pgRows(
      io,
      `SELECT ${PG_COLLATION_COLUMNS} FROM pg_collation c, pg_database d WHERE c.collname = 'default' AND d.datname = current_database()`,
   );
   return postgresJudged(row);
}

/**
 * Malloy's Postgres query SQL with its final stage taken off: a compiled
 * query ends `SELECT row_to_json(finalStage) as row FROM <stage> AS
 * finalStage`, one JSON column with no collation, so the probe reads the
 * stage it wraps instead, whose `probe_value` keeps its column's collation.
 * SQL with no such stage is returned as it is.
 */
export function unfinalizedPostgresSQL(sql: string): string {
   return sql.replace(
      /\bSELECT row_to_json\(finalStage\) as row FROM (__stage\d+) AS finalStage\s*$/,
      "SELECT * FROM $1",
   );
}

/**
 * A Postgres column's collation, read from the source's own SQL
 * (`pg_collation_for`, looked up by its OID): binary under a libc `C` or
 * `POSIX`, deterministic under any other deterministic collation, the
 * database default included.
 */
async function postgresCollation(io: ProbeIO, sql: string): Promise<Collation> {
   const [row] = await pgRows(
      io,
      `SELECT ${PG_COLLATION_COLUMNS} FROM pg_collation c, pg_database d WHERE c.oid = (pg_collation_for((SELECT q.probe_value FROM (${sql}) q LIMIT 1)))::regcollation AND d.datname = current_database()`,
   );
   return postgresJudged(row);
}

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
const PG_COLLATION_IDENTITY = `CASE WHEN c.collname = 'default' AND c.collnamespace = 'pg_catalog'::regnamespace THEN concat_ws('|', 'db', coalesce(to_jsonb(d) ->> 'datlocprovider', ''), d.datcollate, d.datctype, coalesce(to_jsonb(d) ->> 'daticulocale', ''), coalesce(to_jsonb(d) ->> 'datlocale', ''), coalesce(to_jsonb(d) ->> 'daticurules', ''), coalesce(to_jsonb(d) ->> 'datcollversion', '')) ELSE concat_ws('|', 'coll', c.collprovider, c.collisdeterministic::text, coalesce(c.collcollate, ''), coalesce(c.collctype, ''), coalesce(to_jsonb(c) ->> 'colliculocale', ''), coalesce(to_jsonb(c) ->> 'colllocale', ''), coalesce(to_jsonb(c) ->> 'collicurules', ''), coalesce(to_jsonb(c) ->> 'collversion', '')) END`;

/** The identity of the collation `sql`'s `probe_value` compares under. */
async function postgresCollationIdentity(
   io: ProbeIO,
   sql: string,
): Promise<string | undefined> {
   const [row] = await pgRows(
      io,
      `SELECT ${PG_COLLATION_IDENTITY} AS id FROM pg_collation c, pg_database d WHERE c.oid = (pg_collation_for((SELECT q.probe_value FROM (${sql}) q LIMIT 1)))::regcollation AND d.datname = current_database()`,
   );
   return typeof row?.id === "string" ? row.id : undefined;
}

/** The identity of the database's default collation. */
async function postgresDefaultIdentity(
   io: ProbeIO,
): Promise<string | undefined> {
   const [row] = await pgRows(
      io,
      `SELECT ${PG_COLLATION_IDENTITY} AS id FROM pg_collation c, pg_database d WHERE c.collname = 'default' AND c.collnamespace = 'pg_catalog'::regnamespace AND d.datname = current_database()`,
   );
   return typeof row?.id === "string" ? row.id : undefined;
}

/**
 * Every column of a Postgres table, by name: its collation's identity, or
 * null for a column that has none (not a string).
 */
async function postgresColumnIdentities(
   io: ProbeIO,
   tablePath: string,
): Promise<Map<string, string | null>> {
   const path = tablePath.replace(/'/g, "''");
   const columns = await pgRows(
      io,
      `SELECT a.attname AS col, CASE WHEN a.attcollation = 0 THEN NULL ELSE (SELECT ${PG_COLLATION_IDENTITY} FROM pg_collation c, pg_database d WHERE c.oid = a.attcollation AND d.datname = current_database()) END AS id FROM pg_attribute a WHERE a.attrelid = '${path}'::regclass AND a.attnum > 0 AND NOT a.attisdropped`,
   );
   return new Map(
      columns.map((c) => [
         String(c.col),
         typeof c.id === "string" ? c.id : null,
      ]),
   );
}

/**
 * A Postgres table's collatable columns, together: binary when every one is
 * `C`/`POSIX`, deterministic when every one is deterministic, else unknown.
 */
async function postgresTableCollation(
   io: ProbeIO,
   tablePath: string,
): Promise<Collation> {
   const path = tablePath.replace(/'/g, "''");
   const columns = await pgRows(
      io,
      `SELECT ${PG_COLLATION_COLUMNS} FROM pg_attribute a JOIN pg_collation c ON c.oid = a.attcollation, pg_database d WHERE a.attrelid = '${path}'::regclass AND a.attnum > 0 AND NOT a.attisdropped AND d.datname = current_database()`,
   );
   const judged = columns.map(postgresJudged);
   if (judged.includes("unknown")) return "unknown";
   return judged.includes("deterministic") ? "deterministic" : "binary";
}

/** The Postgres dialect's reads: a column's collation, and its identity. */
interface PostgresCollationReader extends CollationReader {
   /** The database's default collation, judged. */
   defaultCollation(io: ProbeIO): Promise<Collation>;
   /**
    * The identity of the collation the `probe_value` of `sql` (a compiled
    * query, its final stage taken off here) compares under.
    */
   collationIdentity(io: ProbeIO, sql: string): Promise<string | undefined>;
   /** The identity of the database's default collation. */
   defaultIdentity(io: ProbeIO): Promise<string | undefined>;
   /** Every column of a table, by name: its collation's identity, or null. */
   columnIdentities(
      io: ProbeIO,
      tablePath: string,
   ): Promise<Map<string, string | null>>;
}

/** The Postgres dialect's reader. */
export const POSTGRES_COLLATION: PostgresCollationReader = Object.freeze({
   sourceColumns: (io: ProbeIO, sql: string) =>
      postgresCollation(io, unfinalizedPostgresSQL(sql)),
   storedTable: postgresTableCollation,
   defaultCollation: postgresDefaultCollation,
   collationIdentity: (io: ProbeIO, sql: string) =>
      postgresCollationIdentity(io, unfinalizedPostgresSQL(sql)),
   defaultIdentity: postgresDefaultIdentity,
   columnIdentities: postgresColumnIdentities,
});
