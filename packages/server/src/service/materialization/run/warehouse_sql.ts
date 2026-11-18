// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The SQL text that differs by warehouse dialect, one family per section: the
 * string literal, the relation and view probes and the view drop, the
 * ownership probe and mark, the incremental refresh's probes and its
 * transaction around a delta, the database swap's transaction, the
 * leftover `name=` view's in-transaction replace, the native-type catalog
 * read, the read cost and the query tag.
 *
 * The other half of the dialect table: `domain/dialect.ts` says whether a
 * dialect can do something, this module says how it is spelled. Text that is
 * the same in every dialect (but for quoting) stays with the statement that
 * runs it; identifiers are quoted by `quoting.ts`.
 *
 * Pure: SQL text in, SQL text out. Each function takes the connection's own
 * `dialectName` and narrows it once to a registered dialect
 * (`registeredDialect`), so its switch names every registered dialect, and a
 * dialect the table does not name takes the switch's `default`, which states
 * that family's answer for it: a refusal (a thrown error), a default spelling,
 * or undefined. Every statement is pinned, per dialect, in
 * `dialect_statements.golden.json`.
 */

import { sqlLiteral } from "../../db_utils";
import { quoteIdentifier, quotePathParts, quoteTablePath } from "../../quoting";
import {
   capabilitiesOf,
   registeredDialect,
   type DialectName,
} from "../domain/dialect";

/**
 * The `default` of a switch over a registered dialect, where `knownDialect` is
 * a dialect the table does not name. It takes `undefined` only, so the switch
 * compiles only while every registered dialect has a case of its own.
 */
function unregistered(knownDialect: undefined): void {
   void knownDialect;
}

// ---------------------------------------------------------------------------
// String literals.
// ---------------------------------------------------------------------------

/**
 * A SQL string for `dialect`, as the marks and their probes write one: a
 * literal, except a MySQL value holding a backslash, which is an expression
 * (see {@link mysqlString}). An unknown dialect doubles the quote.
 */
export function sqlString(value: string, dialect: string): string {
   const knownDialect = registeredDialect(dialect);
   switch (knownDialect) {
      // Backslash is the escape character in these dialects' literals.
      // A BigQuery quoted string cannot hold a raw newline.
      case "standardsql":
         return `'${bigQueryNewlines(value.replace(/\\/g, "\\\\").replace(/'/g, "\\'"))}'`;
      case "databricks":
         return `'${value.replace(/\\/g, "\\\\").replace(/'/g, "\\'")}'`;
      // Snowflake reads backslash escapes and the doubled quote.
      case "snowflake":
         return `'${value.replace(/\\/g, "\\\\").replace(/'/g, "''")}'`;
      case "mysql":
         return mysqlString(value);
      case "sqlserver":
         return `N'${value.replace(/'/g, "''")}'`;
      case "duckdb":
      case "postgres":
      case "trino":
      case "presto":
         return doubledQuoteString(value);
      default:
         unregistered(knownDialect);
         return doubledQuoteString(value);
   }
}

/**
 * Escape a string for a single-quoted SQL literal, as the incremental
 * refresh writes one (its bounds and its target-column probe).
 *
 * Snowflake and BigQuery read `\` in a single-quoted string as an escape
 * character, so a backslash must be doubled there or a value ending in one
 * swallows the closing quote; Postgres (standard_conforming_strings, its
 * default) reads a doubled backslash as two characters. A quote is doubled
 * everywhere except BigQuery, which has no `''` escape (adjacent literals are a
 * syntax error) and takes `\'`; its backslashes are escaped first so they
 * cannot pair with that escape.
 */
export function escapeSqlString(value: string, dialect?: string): string {
   const knownDialect = registeredDialect(dialect);
   if (knownDialect === "standardsql") {
      return bigQueryNewlines(
         value.replace(/\\/g, "\\\\").replace(/'/g, "\\'"),
      );
   }
   const quoted = value.replace(/'/g, "''");
   return knownDialect === "snowflake" ? quoted.replace(/\\/g, "\\\\") : quoted;
}

/**
 * A BigQuery string body with its line breaks escaped (`\n`, `\r`), which a
 * quoted BigQuery string cannot hold raw. Its backslashes are escaped first.
 */
function bigQueryNewlines(escaped: string): string {
   return escaped.replace(/\n/g, "\\n").replace(/\r/g, "\\r");
}

/**
 * A MySQL string that reads the same whatever the session's sql_mode. MySQL
 * reads `\` as an escape unless NO_BACKSLASH_ESCAPES is set, so no one
 * spelling of a backslash in a literal is right in both modes: `'a\\b'` is
 * `a\b` in one and `a\\b` in the other, and a probe written that way misses
 * the table it is looking for. A value holding one is therefore written as
 * `CONCAT('a', CHAR(92 USING utf8mb4), 'b')`, an expression, whose pieces hold
 * no backslash; any other value is a plain literal with its quotes doubled,
 * which both modes read alike.
 */
function mysqlString(value: string): string {
   if (!value.includes("\\")) return doubledQuoteString(value);
   const pieces = value.split("\\").map(doubledQuoteString);
   return `CONCAT(${pieces.join(", CHAR(92 USING utf8mb4), ")})`;
}

/** A SQL string literal with its single quotes doubled, in any dialect. */
function doubledQuoteString(value: string): string {
   return `'${value.replace(/'/g, "''")}'`;
}

/** A column of a result row, whatever case the driver reports it in. */
function field(row: Record<string, unknown>, key: string): unknown {
   if (key in row) return row[key];
   const k = Object.keys(row).find((x) => x.toLowerCase() === key);
   return k === undefined ? undefined : row[k];
}

// ---------------------------------------------------------------------------
// Relation and view probes, and the view drop.
// ---------------------------------------------------------------------------

/** Thrown when a dialect or a name shape cannot be probed. */
class RelationProbeUnsupportedError extends Error {}

/**
 * Whether {@link relationProbeSql} can probe a dialect (Malloy `dialectName`):
 * its `relationProbe` capability. MySQL and Presto are probed by
 * {@link ownedTableProbeSql}'s own queries.
 */
export function relationProbeSupported(dialect: string): boolean {
   return capabilitiesOf(dialect).relationProbe;
}

function quotedPath(segments: string[], dialect: DialectName): string {
   return quoteTablePath(segments.join("."), dialect);
}

/**
 * A query answering what holds the name `segments` names: no row when
 * nothing does, otherwise one row per relation with `kind` (`VIEW` for a
 * view, anything else for a table), `comment` (its comment, or null) and
 * `location`: where the relation actually is, as the warehouse's own catalog
 * spells it, fully qualified (`<database>.<schema>.<name>`, or
 * `<schema>.<name>` on MySQL), whatever schema search or default the name
 * resolved through. table_ownership.ts reads the answer; an ownership mark
 * names the `location` it was written at. Refuses an unknown dialect.
 */
export function relationProbeSql(segments: string[], dialect: string): string {
   const lit = (v: string) => sqlString(v, dialect);
   const name = segments[segments.length - 1];
   const knownDialect = registeredDialect(dialect);
   switch (knownDialect) {
      case "duckdb": {
         const match = (column: string) => duckdbNameMatch(segments, column);
         return (
            `SELECT 'VIEW' AS kind, comment, database_name || '.' || schema_name || '.' || view_name AS location ` +
            `FROM duckdb_views() WHERE NOT internal AND ${match("view_name")} ` +
            `UNION ALL SELECT 'TABLE' AS kind, comment, database_name || '.' || schema_name || '.' || table_name AS location ` +
            `FROM duckdb_tables() WHERE ${match("table_name")}`
         );
      }
      case "postgres":
         // to_regclass resolves the quoted path as the DDL would, search path
         // included, and answers NULL (no row here) when nothing holds it.
         // Wrapped in row_to_json: Malloy's Postgres runSQL returns each row's
         // one `row` column, so a plain select would come back as nulls.
         return (
            `SELECT row_to_json(p) AS "row" FROM (` +
            `SELECT CASE c.relkind WHEN 'v' THEN 'VIEW' ELSE 'TABLE' END AS kind, ` +
            `obj_description(c.oid, 'pg_class') AS comment, ` +
            `current_database() || '.' || n.nspname || '.' || c.relname AS location ` +
            `FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace ` +
            `WHERE c.oid = to_regclass(${lit(quotedPath(segments, knownDialect))})) AS p`
         );
      case "standardsql": {
         if (segments.length < 2) {
            throw new RelationProbeUnsupportedError(
               `a BigQuery name needs a dataset-qualified name, not '${name}'`,
            );
         }
         const schema = `${quotedPath(segments.slice(0, -1), knownDialect)}.INFORMATION_SCHEMA`;
         return (
            `SELECT t.table_type AS kind, o.option_value AS comment, ` +
            `CONCAT(t.table_catalog, '.', t.table_schema, '.', t.table_name) AS location ` +
            `FROM ${schema}.TABLES t LEFT JOIN ${schema}.TABLE_OPTIONS o ` +
            `ON o.table_name = t.table_name AND o.option_name = 'description' ` +
            `WHERE t.table_name = ${lit(name)}`
         );
      }
      case "snowflake": {
         // Quoted identifiers keep their case, so the match is exact.
         const from =
            segments.length === 3
               ? `${quoteIdentifier(segments[0], dialect)}.INFORMATION_SCHEMA.TABLES`
               : "INFORMATION_SCHEMA.TABLES";
         const schema =
            segments.length === 1
               ? "CURRENT_SCHEMA()"
               : lit(segments[segments.length - 2]);
         return (
            `SELECT TABLE_TYPE AS "kind", COMMENT AS "comment", ` +
            `TABLE_CATALOG || '.' || TABLE_SCHEMA || '.' || TABLE_NAME AS "location" FROM ${from} ` +
            `WHERE TABLE_SCHEMA = ${schema} AND TABLE_NAME = ${lit(name)}`
         );
      }
      case "databricks":
      case "trino": {
         // Both fold identifiers to lower case. A one- or two-part name is in
         // the session's catalog, whose information_schema answers bare.
         const from =
            segments.length === 3
               ? `${quoteIdentifier(segments[0], dialect)}.information_schema.tables`
               : "information_schema.tables";
         const schema =
            segments.length > 1
               ? `lower(${lit(segments[segments.length - 2])})`
               : knownDialect === "databricks"
                 ? "current_schema()"
                 : "current_schema";
         if (knownDialect === "databricks") {
            return (
               `SELECT table_type AS kind, comment, ` +
               `concat(table_catalog, '.', table_schema, '.', table_name) AS location FROM ${from} ` +
               `WHERE table_schema = ${schema} AND table_name = lower(${lit(name)})`
            );
         }
         return (
            `SELECT t.table_type AS kind, c.comment AS comment, ` +
            `t.table_catalog || '.' || t.table_schema || '.' || t.table_name AS location FROM ${from} t ` +
            `LEFT JOIN system.metadata.table_comments c ON c.catalog_name = t.table_catalog ` +
            `AND c.schema_name = t.table_schema AND c.table_name = t.table_name ` +
            `WHERE t.table_schema = ${schema} AND t.table_name = lower(${lit(name)})`
         );
      }
      case "sqlserver":
         assertSqlServerShape(segments);
         return (
            `SELECT CASE o.type WHEN 'V' THEN 'VIEW' ELSE 'TABLE' END AS kind, ` +
            `CAST(ep.value AS nvarchar(4000)) AS comment, ` +
            `DB_NAME() + N'.' + SCHEMA_NAME(o.schema_id) + N'.' + o.name AS location FROM sys.objects o ` +
            `LEFT JOIN sys.extended_properties ep ON ep.class = 1 AND ep.major_id = o.object_id ` +
            `AND ep.minor_id = 0 AND ep.name = N'MS_Description' ` +
            `WHERE o.object_id = OBJECT_ID(${lit(quotedPath(segments, knownDialect))})`
         );
      // MySQL and Presto are probed by ownedTableProbeSql's own queries.
      case "mysql":
      case "presto":
         throw unsupportedDialect(dialect);
      default:
         unregistered(knownDialect);
         throw unsupportedDialect(dialect);
   }
}

/**
 * DuckDB: the relation `column` names is the one `segments` names. DuckDB
 * resolves names case-insensitively, and a two-part name as a catalog's
 * `main` schema or a schema of the current catalog: both are matched, so
 * either holder is seen.
 */
function duckdbNameMatch(segments: string[], column: string): string {
   const lit = (v: string) => sqlString(v, "duckdb");
   const n = `lower(${column}) = lower(${lit(segments[segments.length - 1])})`;
   if (segments.length === 1) {
      return `${n} AND database_name = current_database() AND schema_name = current_schema()`;
   }
   if (segments.length === 2) {
      const a = lit(segments[0]);
      return (
         `${n} AND ((lower(database_name) = lower(${a}) AND schema_name = 'main') ` +
         `OR (database_name = current_database() AND lower(schema_name) = lower(${a})))`
      );
   }
   return (
      `${n} AND lower(database_name) = lower(${lit(segments[0])}) ` +
      `AND lower(schema_name) = lower(${lit(segments[1])})`
   );
}

/** The session's current schema, in the dialects whose catalog reads it bare. */
const CURRENT_SCHEMA_SQL: Readonly<Record<"databricks" | "trino", string>> = {
   databricks: "current_schema()",
   trino: "current_schema",
};

/**
 * A query answering the definition of the view `segments` names, as one row
 * with `definition` (no row when no view holds the name). Read with
 * `viewDefinitionReadsOnly` (table_mark_sql.ts). Refuses an unknown dialect.
 */
function viewDefinitionSql(segments: string[], dialect: string): string {
   const lit = (v: string) => sqlString(v, dialect);
   const name = segments[segments.length - 1];
   const knownDialect = registeredDialect(dialect);
   switch (knownDialect) {
      case "duckdb":
         return (
            `SELECT sql AS definition, schema_name AS view_schema FROM duckdb_views() ` +
            `WHERE NOT internal AND ${duckdbNameMatch(segments, "view_name")}`
         );
      case "postgres": {
         const path = lit(quotedPath(segments, knownDialect));
         return (
            `SELECT row_to_json(p) AS "row" FROM (` +
            `SELECT pg_get_viewdef(c.oid) AS definition, ` +
            `c.relnamespace::regnamespace::text AS view_schema FROM pg_class c ` +
            `WHERE c.oid = to_regclass(${path}) AND c.relkind = 'v') AS p`
         );
      }
      case "standardsql": {
         if (segments.length < 2) {
            throw new RelationProbeUnsupportedError(
               `a BigQuery name needs a dataset-qualified name, not '${name}'`,
            );
         }
         const schema = `${quotedPath(segments.slice(0, -1), knownDialect)}.INFORMATION_SCHEMA`;
         return (
            `SELECT view_definition AS definition, table_schema AS view_schema ` +
            `FROM ${schema}.VIEWS ` +
            `WHERE table_name = ${lit(name)}`
         );
      }
      case "snowflake": {
         const from =
            segments.length === 3
               ? `${quoteIdentifier(segments[0], dialect)}.INFORMATION_SCHEMA.VIEWS`
               : "INFORMATION_SCHEMA.VIEWS";
         const schema =
            segments.length === 1
               ? "CURRENT_SCHEMA()"
               : lit(segments[segments.length - 2]);
         return (
            `SELECT VIEW_DEFINITION AS "definition", TABLE_SCHEMA AS "view_schema" ` +
            `FROM ${from} ` +
            `WHERE TABLE_SCHEMA = ${schema} AND TABLE_NAME = ${lit(name)}`
         );
      }
      case "databricks":
      case "trino": {
         const from =
            segments.length === 3
               ? `${quoteIdentifier(segments[0], dialect)}.information_schema.views`
               : "information_schema.views";
         const schema =
            segments.length > 1
               ? `lower(${lit(segments[segments.length - 2])})`
               : CURRENT_SCHEMA_SQL[knownDialect];
         return (
            `SELECT view_definition AS definition, table_schema AS view_schema ` +
            `FROM ${from} ` +
            `WHERE table_schema = ${schema} AND table_name = lower(${lit(name)})`
         );
      }
      case "sqlserver":
         assertSqlServerShape(segments);
         return (
            `SELECT OBJECT_DEFINITION(o.object_id) AS definition, ` +
            `OBJECT_SCHEMA_NAME(o.object_id) AS view_schema FROM sys.objects o ` +
            `WHERE o.type = 'V' AND o.object_id = OBJECT_ID(${lit(quotedPath(segments, knownDialect))})`
         );
      case "mysql":
      case "presto":
         throw unsupportedDialect(dialect);
      default:
         unregistered(knownDialect);
         throw unsupportedDialect(dialect);
   }
}

/**
 * Postgres: the view `viewOid` (an SQL expression for its oid) reads the
 * relation `targetOid` and no other. Its `_RETURN` rule depends on
 * `targetOid`, and on no other table, view, materialized view, partitioned or
 * foreign table besides the view itself: a join or a subquery over another
 * relation is not proven, nor is a view repointed off the target.
 */
function postgresReadsOnlySql(viewOid: string, targetOid: string): string {
   const deps =
      `FROM pg_catalog.pg_depend d JOIN pg_catalog.pg_rewrite r ON r.oid = d.objid ` +
      `WHERE d.classid = 'pg_catalog.pg_rewrite'::regclass ` +
      `AND d.refclassid = 'pg_catalog.pg_class'::regclass ` +
      `AND r.ev_class = ${viewOid}`;
   return (
      `(EXISTS (SELECT 1 ${deps} AND d.refobjid = ${targetOid}) ` +
      `AND NOT EXISTS (SELECT 1 ${deps} AND d.refobjid <> ${targetOid} ` +
      `AND d.refobjid <> ${viewOid} AND EXISTS (SELECT 1 FROM pg_catalog.pg_class rc ` +
      `WHERE rc.oid = d.refobjid AND rc.relkind IN ('r', 'v', 'm', 'p', 'f'))))`
   );
}

/**
 * How to ask whether the view `view` reads the table `table`, which a view's
 * comment cannot say: Postgres's `CREATE OR REPLACE VIEW` and SQL Server's
 * `ALTER VIEW` keep the comment of a view repointed at another table.
 * Postgres and SQL Server answer from their dependency catalogs, one row with
 * a true `reads` when the view reads the table and nothing else; every other
 * dialect hands back the view's definition and schema, which
 * `viewDefinitionReadsOnly` (table_mark_sql.ts) reads.
 */
export function viewReadsSql(
   view: string[],
   table: string[],
   dialect: string,
): { sql: string; answer: "reads" | "definition" } {
   const lit = (v: string) => sqlString(v, dialect);
   const knownDialect = registeredDialect(dialect);
   switch (knownDialect) {
      case "postgres":
         return {
            answer: "reads",
            sql: `SELECT row_to_json(p) AS "row" FROM (SELECT ${postgresReadsOnlySql(
               `to_regclass(${lit(quotedPath(view, knownDialect))})`,
               `to_regclass(${lit(quotedPath(table, knownDialect))})`,
            )} AS reads) AS p`,
         };
      case "sqlserver":
         assertSqlServerShape(view);
         return {
            answer: "reads",
            sql: (() => {
               const v = `OBJECT_ID(${lit(quotedPath(view, knownDialect))})`;
               const t = `OBJECT_ID(${lit(quotedPath(table, knownDialect))})`;
               const deps = `FROM sys.sql_expression_dependencies d WHERE d.referencing_id = ${v}`;
               // Reads the target, and nothing it cannot resolve or that is
               // not the target.
               return (
                  `SELECT CASE WHEN EXISTS (SELECT 1 ${deps} AND d.referenced_id = ${t}) ` +
                  `AND NOT EXISTS (SELECT 1 ${deps} AND (d.referenced_id IS NULL ` +
                  `OR d.referenced_id <> ${t})) THEN 1 ELSE 0 END AS reads`
               );
            })(),
         };
      case "duckdb":
      case "standardsql":
      case "snowflake":
      case "databricks":
      case "trino":
      case "presto":
      case "mysql":
         return { answer: "definition", sql: viewDefinitionSql(view, dialect) };
      default:
         // Refused by viewDefinitionSql.
         unregistered(knownDialect);
         return { answer: "definition", sql: viewDefinitionSql(view, dialect) };
   }
}

/**
 * `DROP VIEW IF EXISTS` for the view `segments` names. Never drops a table.
 * Refuses a dialect {@link relationProbeSupported} does not name.
 */
export function dropViewSql(segments: string[], dialect: string): string {
   const knownDialect = registeredDialect(dialect);
   if (knownDialect === undefined || !relationProbeSupported(knownDialect)) {
      throw unsupportedDialect(dialect);
   }
   if (knownDialect === "sqlserver") assertSqlServerShape(segments);
   return `DROP VIEW IF EXISTS ${quotedPath(segments, knownDialect)}`;
}

/** SQL Server's CREATE VIEW takes a schema, never a database. */
function assertSqlServerShape(segments: string[]): void {
   if (segments.length > 2) {
      throw new RelationProbeUnsupportedError(
         `a SQL Server name takes at most a schema, not '${segments.join(".")}'`,
      );
   }
}

function unsupportedDialect(dialect: string): RelationProbeUnsupportedError {
   return new RelationProbeUnsupportedError(
      `Publisher cannot read a relation's comment in dialect '${dialect}'`,
   );
}

// ---------------------------------------------------------------------------
// The ownership probe and mark.
// ---------------------------------------------------------------------------

/**
 * A query answering what holds the name `segments` names: one row per
 * relation, with `kind` (`VIEW`, or anything else for a table) and `comment`.
 * The shared probe ({@link relationProbeSql}, which refuses an unknown
 * dialect), plus MySQL, and Presto with no comments (a Presto table is never
 * read as marked, only as this package's by its records).
 */
export function ownedTableProbeSql(
   segments: string[],
   dialect: string,
): string {
   const lit = (v: string) => sqlString(v, dialect);
   const name = segments[segments.length - 1];
   const knownDialect = registeredDialect(dialect);
   if (knownDialect === "mysql") {
      const schema =
         segments.length > 1
            ? lit(segments[segments.length - 2])
            : "DATABASE()";
      return (
         `SELECT TABLE_TYPE AS kind, TABLE_COMMENT AS comment, ` +
         `CONCAT(TABLE_SCHEMA, '.', TABLE_NAME) AS location FROM information_schema.TABLES ` +
         `WHERE TABLE_SCHEMA = ${schema} AND TABLE_NAME = ${lit(name)}`
      );
   }
   if (knownDialect === "presto") {
      const schema =
         segments.length > 1
            ? `lower(${lit(segments[segments.length - 2])})`
            : "current_schema";
      const from =
         segments.length === 3
            ? `${quoteTablePath(segments[0], dialect)}.information_schema.tables`
            : "information_schema.tables";
      return (
         `SELECT table_type AS kind, CAST(NULL AS varchar) AS comment, ` +
         `table_catalog || '.' || table_schema || '.' || table_name AS location FROM ${from} ` +
         `WHERE table_schema = ${schema} AND table_name = lower(${lit(name)})`
      );
   }
   return relationProbeSql(segments, dialect);
}

/**
 * The DDL that sets the comment of the table `segments` names to `comment`.
 * An unknown dialect gets `COMMENT ON TABLE`.
 */
export function tableCommentSql(
   segments: string[],
   comment: string,
   dialect: string,
): string {
   const table = quoteTablePath(segments.join("."), dialect);
   const literal = sqlString(comment, dialect);
   const commentOnTable = `COMMENT ON TABLE ${table} IS ${literal}`;
   const knownDialect = registeredDialect(dialect);
   switch (knownDialect) {
      case "standardsql":
         return `ALTER TABLE ${table} SET OPTIONS(description = ${literal})`;
      case "mysql":
         // The table option takes a literal, not the CONCAT a backslash needs
         // (see mysqlString). Publisher's marks are percent-encoded, so none
         // holds one.
         if (comment.includes("\\")) {
            throw new Error(
               "a MySQL table comment cannot hold a backslash: it would read differently under NO_BACKSLASH_ESCAPES",
            );
         }
         return `ALTER TABLE ${table} COMMENT = ${literal}`;
      case "sqlserver": {
         const schema =
            segments.length === 2
               ? sqlString(segments[0], dialect)
               : "SCHEMA_NAME()";
         const name = sqlString(segments[segments.length - 1], dialect);
         const property =
            `@name = N'MS_Description', @value = ${literal}, @level0type = N'SCHEMA', ` +
            `@level0name = @schema, @level1type = N'TABLE', @level1name = ${name}`;
         return [
            `DECLARE @schema sysname = ${schema};`,
            `IF EXISTS (SELECT 1 FROM sys.extended_properties WHERE class = 1 ` +
               `AND major_id = OBJECT_ID(${sqlString(table, dialect)}) AND minor_id = 0 ` +
               `AND name = N'MS_Description')`,
            `   EXEC sys.sp_updateextendedproperty ${property};`,
            "ELSE",
            `   EXEC sys.sp_addextendedproperty ${property};`,
         ].join("\n");
      }
      case "duckdb":
      case "postgres":
      case "snowflake":
      case "databricks":
      case "trino":
      case "presto":
         return commentOnTable;
      default:
         unregistered(knownDialect);
         return commentOnTable;
   }
}

// ---------------------------------------------------------------------------
// The incremental refresh: its probes and the transaction around a delta.
// ---------------------------------------------------------------------------

/**
 * Wrap a probe SELECT so the dialect's driver hands back named columns.
 *
 * Postgres only: `PostgresConnection` rewrites every `runSQL` row as
 * `rows[i].row` (Malloy queries project one `row` JSON column), so a plain
 * `SELECT max(x) AS m` comes back as `[undefined]`. Wrapping in `row_to_json`
 * supplies that column. Every other dialect, an unknown one included, takes
 * the SELECT as it is. Pinned in incremental_apply.spec.ts.
 */
export function probeSelect(dialect: string, innerSelect: string): string {
   const knownDialect = registeredDialect(dialect);
   return knownDialect === "postgres"
      ? `SELECT row_to_json(__p) AS "row" FROM (${innerSelect}) AS __p`
      : innerSelect;
}

/**
 * `sql` bounded to `timeoutMs` on the warehouse itself, for a read that may be
 * given up on (the late-row check): Postgres takes a transaction-local
 * `statement_timeout` (`set_config(…, true)`) in the same simple-protocol
 * call, which holds for that call's implicit transaction alone, because
 * Malloy's Postgres connection cannot cancel a statement in flight; a
 * stricter timeout the session already has is kept. Every other dialect takes `sql` as it
 * is: BigQuery, Snowflake and DuckDB cancel through the run options' abort
 * signal.
 */
export function withStatementTimeout(
   dialect: string,
   sql: string,
   timeoutMs: number,
): string {
   const knownDialect = registeredDialect(dialect);
   const ms = Math.max(1, Math.round(timeoutMs));
   switch (knownDialect) {
      case "postgres":
         return (
            // The smaller of this and the session's own timeout (a
            // connection's statementTimeoutMilliseconds, set when the session
            // opens): a stricter one the connection chose is kept.
            `SELECT set_config('statement_timeout', (SELECT CASE WHEN ` +
            `setting::bigint > 0 AND setting::bigint < ${ms} THEN setting ` +
            `ELSE '${ms}' END FROM pg_settings WHERE name = ` +
            `'statement_timeout'), true); ${sql}`
         );
      case "duckdb":
      case "standardsql":
      case "snowflake":
      case "databricks":
      case "trino":
      case "presto":
      case "mysql":
      case "sqlserver":
         return sql;
      default:
         unregistered(knownDialect);
         return sql;
   }
}

/**
 * A date or timestamp watermark expression as TEXT the bound parser reads
 * (`canonicalBoundValue` in incremental_apply.ts): `YYYY-MM-DD` for a date,
 * `YYYY-MM-DD HH:MM:SS[.f]` in UTC for a timestamp, with no offset or a zero
 * one.
 *
 * Text, not the driver's own value, because drivers disagree about a
 * timestamp: node-postgres hands back a zone-naive one as a Date in the
 * PROCESS's zone, which off UTC would move the bound by hours. Every
 * incremental session is pinned to UTC, so a zoned timestamp's text is UTC.
 * Postgres and Snowflake spell an explicit format, because a cast follows the
 * session's `DateStyle` (Postgres) or the account's `TIMESTAMP_OUTPUT_FORMAT`
 * (Snowflake), which a connection's setup may change; Snowflake converts
 * first, because a `TIMESTAMP_TZ` prints in the offset it was stored with. A
 * dialect with no incremental refresh takes the standard cast.
 */
export function watermarkTextSql(
   dialect: string,
   malloyType: "date" | "timestamp",
   expression: string,
): string {
   const knownDialect = registeredDialect(dialect);
   switch (knownDialect) {
      case "snowflake":
         return malloyType === "date"
            ? `TO_CHAR(${expression}, 'YYYY-MM-DD')`
            : `TO_CHAR(CONVERT_TIMEZONE('UTC', ${expression})::TIMESTAMP_NTZ, ` +
                 `'YYYY-MM-DD HH24:MI:SS')`;
      case "standardsql":
         return `CAST(${expression} AS STRING)`;
      case "postgres":
         // `OF`: the session's offset, `+00` under UTC. Without it a session
         // set to a zone with daylight saving prints the fall-back hour's two
         // instants alike; with it, any zone but UTC is refused and the
         // source is rebuilt rather than started an hour late.
         return malloyType === "date"
            ? `to_char(${expression}, 'YYYY-MM-DD')`
            : `to_char(${expression}, 'YYYY-MM-DD HH24:MI:SSOF')`;
      case "duckdb":
      case "databricks":
      case "trino":
      case "presto":
      case "mysql":
      case "sqlserver":
         return `CAST(${expression} AS VARCHAR)`;
      default:
         unregistered(knownDialect);
         return `CAST(${expression} AS VARCHAR)`;
   }
}

/**
 * The query reading the column names, in ordinal order, of the table whose
 * decoded path is `segments` (one row per column, `column_name`). Undefined
 * when the name gives no container to read the catalog in: every dialect but
 * DuckDB, Postgres and Snowflake (an unknown one included) is read as
 * BigQuery, whose INFORMATION_SCHEMA is per-dataset.
 */
export function targetColumnsProbeSql(
   dialect: string,
   segments: string[],
): string | undefined {
   const alias = quoteIdentifier("column_name", dialect);
   const knownDialect = registeredDialect(dialect);
   if (knownDialect === "duckdb") {
      // `duckdb_columns()`: an attached DuckLake catalog has no
      // `information_schema`, and `DESCRIBE` throws on an absent table where this
      // function's contract needs an empty answer.
      const table = segments[segments.length - 1];
      // A two-segment path is in the catalog's default schema. Named explicitly
      // because two schemas can hold the same table name, and probing the wrong
      // one would describe a table this build never wrote.
      const schema =
         segments.length >= 3 ? segments[segments.length - 2] : "main";
      // The catalog is the FIRST segment (the path is `<destination>.<name>`);
      // counting from the end would misread a longer name's container.
      const database = segments[0];
      return `SELECT column_name AS ${alias}
              FROM duckdb_columns()
              WHERE database_name = '${escapeSqlString(database, dialect)}'
                AND schema_name = '${escapeSqlString(schema, dialect)}'
                AND table_name = '${escapeSqlString(table, dialect)}'
              ORDER BY column_index`;
   } else if (knownDialect === "postgres" || knownDialect === "snowflake") {
      const table = segments[segments.length - 1];
      // An unqualified name lives in the session's default schema, where the CTAS
      // created it. Not "any schema on the search path": two schemas can hold
      // the same table name.
      const schema =
         segments.length >= 2
            ? `'${escapeSqlString(segments[segments.length - 2], dialect)}'`
            : "current_schema()";
      // Snowflake's information_schema is per-DATABASE, so a database-qualified
      // name must probe that database's view; Postgres cannot cross databases,
      // so any leading segment is the current one by construction.
      const infoSchema =
         knownDialect === "snowflake" && segments.length >= 3
            ? `${quoteIdentifier(segments[segments.length - 3], dialect)}.information_schema.columns`
            : "information_schema.columns";
      return probeSelect(
         dialect,
         `SELECT column_name AS ${alias}
           FROM ${infoSchema}
           WHERE table_schema = ${schema}
             AND table_name = '${escapeSqlString(table, dialect)}'
           ORDER BY ordinal_position`,
      );
   }
   // BigQuery's INFORMATION_SCHEMA is per-dataset, so the view itself must be
   // dataset-qualified: an unqualified target has no dataset to look in.
   if (segments.length < 2) return undefined;
   const table = segments[segments.length - 1];
   const container = segments
      .slice(0, -1)
      .map((s) => quoteIdentifier(s, dialect))
      .join(".");
   return `SELECT column_name AS ${alias}
              FROM ${container}.INFORMATION_SCHEMA.COLUMNS
              WHERE table_name = '${escapeSqlString(table, dialect)}'
              ORDER BY ordinal_position`;
}

/**
 * A transaction's keywords. BigQuery spells them out; every other dialect, an
 * unknown one included, takes the bare `BEGIN`, `COMMIT` and `ROLLBACK`.
 */
export function transactionKeywords(dialect: string): {
   begin: string;
   commit: string;
   rollback: string;
} {
   const knownDialect = registeredDialect(dialect);
   return knownDialect === "standardsql"
      ? {
           begin: "BEGIN TRANSACTION",
           commit: "COMMIT TRANSACTION",
           rollback: "ROLLBACK TRANSACTION",
        }
      : { begin: "BEGIN", commit: "COMMIT", rollback: "ROLLBACK" };
}

/**
 * The range replace as ONE statement for Snowflake: an anonymous Snowflake
 * Scripting block in `EXECUTE IMMEDIATE $$…$$` (drivers do not accept a bare
 * block).
 *
 * The EXCEPTION handler is the rollback, INSIDE the statement because a separate
 * `ROLLBACK` call could reach a different pooled session, leaving the open
 * transaction holding locks. RAISE re-throws, so the build still fails and the
 * boundary is not advanced.
 */
export function snowflakeScriptingBlock(statements: string[]): string {
   const body = statements.map((s) => `  ${s};`).join("\n");
   // SQL containing `$$` would end the block early and run the rest as separate
   // statements. Generated deltas never contain it, so refuse rather than escape:
   // planSourceRefresh catches the throw and seeds (plan_error).
   const block = `BEGIN\n  BEGIN TRANSACTION;\n${body}\n  COMMIT;\nEXCEPTION\n  WHEN OTHER THEN\n    ROLLBACK;\n    RAISE;\nEND`;
   if (block.includes("$$")) {
      throw new Error(
         "the delta SQL contains '$$', which cannot be carried inside a " +
            "Snowflake Scripting block",
      );
   }
   return `EXECUTE IMMEDIATE $$\n${block}\n$$`;
}

/**
 * Whether `dialect` runs a transaction as one scripting block whose rollback
 * is its own EXCEPTION handler (Snowflake), rather than a BEGIN, the
 * statements and a COMMIT the caller rolls back on failure.
 */
export function transactsInScriptingBlock(dialect: string): boolean {
   return registeredDialect(dialect) === "snowflake";
}

/**
 * A delta's statements as one transaction: a Snowflake Scripting block on
 * Snowflake (one statement per call, each possibly on a different pooled
 * session), otherwise the statements between {@link transactionKeywords}'
 * BEGIN and COMMIT.
 */
export function inOneTransaction(dialect: string, body: string[]): string[] {
   if (transactsInScriptingBlock(dialect)) {
      return [snowflakeScriptingBlock(body)];
   }
   const transaction = transactionKeywords(dialect);
   return [transaction.begin, ...body, transaction.commit];
}

// ---------------------------------------------------------------------------
// The database swap's transaction.
// ---------------------------------------------------------------------------

/**
 * The transactional swap of `staging` onto `physical` (quoted paths and the
 * rename target already spelled for the dialect), as ONE driver call's text,
 * and the rollback to send as its own call after a failure, if any.
 * `before` runs first, inside the transaction.
 *
 * - Postgres: no BEGIN/COMMIT in the text. The simple query protocol runs a
 *   multi-statement string as one implicit transaction, which a failure rolls
 *   back and ends, so there is no rollback to send.
 * - DuckDB (and any other dialect asked): bracketed by BEGIN TRANSACTION and
 *   COMMIT, with a ROLLBACK after a failure.
 */
export function transactionalSwapSql(
   dialect: string,
   swap: {
      before: string[];
      quotedPhysical: string;
      quotedStaging: string;
      renameTarget: string;
   },
): { sql: string; rollback: string | undefined } {
   const knownDialect = registeredDialect(dialect);
   const explicit = knownDialect !== "postgres";
   const body = [
      ...swap.before,
      `DROP TABLE IF EXISTS ${swap.quotedPhysical}`,
      `ALTER TABLE ${swap.quotedStaging} RENAME TO ${swap.renameTarget}`,
   ];
   const statements = explicit
      ? ["BEGIN TRANSACTION", ...body, "COMMIT"]
      : body;
   return {
      sql: statements.map((s) => `${s};`).join("\n"),
      rollback: explicit ? "ROLLBACK" : undefined,
   };
}

// ---------------------------------------------------------------------------
// The leftover `name=` view's in-transaction replace.
// ---------------------------------------------------------------------------

/**
 * The statements that drop the view `segments` names inside a build's own
 * transaction: a guard that fails the transaction unless the name still
 * holds the view a probe proved, then the drop. On Postgres (the
 * `dependencyGuard` capability) the guard locks the view and reads its
 * dependencies; elsewhere it compares the view's DuckDB definition
 * (`definition`), and without one there is no guard and the answer is
 * undefined. Undefined, too, for a dialect without a transactional swap.
 */
export function replaceViewStatements(
   view: {
      segments: string[];
      targetSegments: string[];
      comment: string;
      definition?: string;
   },
   dialect: string,
): string[] | undefined {
   if (!capabilitiesOf(dialect).transactionalSwap) return undefined;
   const { segments } = view;
   const lit = (v: string) => sqlString(v, dialect);
   const path = quoteTablePath(segments.join("."), dialect);
   const drop = dropViewSql(segments, dialect);
   if (capabilitiesOf(dialect).dependencyGuard) {
      const viewPath = lit(path);
      const targetPath = lit(
         quoteTablePath(view.targetSegments.join("."), dialect),
      );
      return [
         `DO $guard$ BEGIN IF to_regclass(${viewPath}) IS NOT NULL THEN ` +
            `EXECUTE 'LOCK TABLE ' || ${viewPath} || ' IN ACCESS EXCLUSIVE MODE'; END IF; ` +
            `IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c ` +
            `WHERE c.oid = to_regclass(${viewPath}) AND c.relkind = 'v' ` +
            `AND obj_description(c.oid, 'pg_class') = ${lit(view.comment)} ` +
            `AND ${postgresReadsOnlySql("c.oid", `to_regclass(${targetPath})`)}) ` +
            `THEN RAISE EXCEPTION 'the name= view % changed while the build checked it', ${viewPath}; ` +
            `END IF; END $guard$`,
         drop,
      ];
   }
   if (view.definition === undefined) return undefined;
   return [
      `SELECT error(${lit(`the name= view ${path} changed while the build checked it`)}) ` +
         `FROM (SELECT 1 FROM duckdb_views() WHERE NOT internal ` +
         `AND ${duckdbNameMatch(segments, "view_name")} ` +
         `AND sql = ${lit(view.definition)} HAVING count(*) = 0) g`,
      drop,
   ];
}

// ---------------------------------------------------------------------------
// The native-type catalog read.
// ---------------------------------------------------------------------------

/** A column and its warehouse type. */
export interface NativeColumn {
   name: string;
   type: string;
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
 * DuckDB's `DESCRIBE`, Postgres's `format_type`, and
 * `information_schema.columns` for the rest, an unknown dialect included.
 * Undefined for an unqualified BigQuery name, which has no dataset to read.
 */
export function nativeTypeCatalogRead(
   /** The table's path, unquoted and dot-separated, or its parts. */
   physicalTableName: string | readonly string[],
   dialect: string,
):
   | {
        sql: string;
        read: (row: Record<string, unknown>) => NativeColumn;
        tableOf?: (row: Record<string, unknown>) => string;
     }
   | undefined {
   const parts =
      typeof physicalTableName === "string"
         ? physicalTableName.split(".")
         : [...physicalTableName];
   const quoted = quotePathParts(parts, dialect);
   const table = parts[parts.length - 1];
   const knownDialect = registeredDialect(dialect);
   switch (knownDialect) {
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
               `FROM pg_attribute a WHERE a.attrelid = to_regclass(${sqlString(quoted, dialect)}) ` +
               "AND a.attnum > 0 AND NOT a.attisdropped) c ORDER BY c.attnum",
            read: (r) => ({
               name: String(field(r, "name")),
               type: String(field(r, "type")),
            }),
         };
      case "standardsql": {
         if (parts.length < 2) return undefined;
         const dataset = quotePathParts(parts.slice(0, -1), dialect);
         return {
            sql:
               `SELECT column_name, data_type FROM ${dataset}.INFORMATION_SCHEMA.COLUMNS ` +
               `WHERE table_name = ${sqlString(table, dialect)} ORDER BY ordinal_position`,
            read: (r) => ({
               name: String(field(r, "column_name")),
               type: String(field(r, "data_type")),
            }),
         };
      }
      case "snowflake":
      case "databricks":
      case "trino":
      case "presto":
      case "mysql":
      case "sqlserver":
         return informationSchemaCatalogRead(parts, dialect, knownDialect);
      default:
         unregistered(knownDialect);
         return informationSchemaCatalogRead(parts, dialect, knownDialect);
   }
}

/**
 * The precision columns `information_schema.columns` is read with: none on
 * Trino and Presto, whose `data_type` carries them (`decimal(38,0)`).
 */
function informationSchemaPrecision(
   knownDialect: DialectName | undefined,
): string {
   switch (knownDialect) {
      case "trino":
      case "presto":
         return "";
      default:
         return ", numeric_precision, numeric_scale";
   }
}

/**
 * The schema filter for an unqualified name: MySQL resolves one in the
 * connection's database, and its `information_schema` shows every
 * database's tables; elsewhere none.
 */
function unqualifiedSchemaFilter(
   knownDialect: DialectName | undefined,
): string {
   switch (knownDialect) {
      case "mysql":
         return " AND table_schema = DATABASE()";
      default:
         return "";
   }
}

/**
 * {@link nativeTypeCatalogRead} from `information_schema.columns`, for every
 * dialect but DuckDB, Postgres and BigQuery, an unknown one included.
 * `dialect` is the connection's own name, `knownDialect` it narrowed.
 */
function informationSchemaCatalogRead(
   parts: string[],
   dialect: string,
   knownDialect: DialectName | undefined,
): {
   sql: string;
   read: (row: Record<string, unknown>) => NativeColumn;
   tableOf: (row: Record<string, unknown>) => string;
} {
   const table = parts[parts.length - 1];
   const schema = parts.length > 1 ? parts[parts.length - 2] : undefined;
   // A three-part name's catalog (Snowflake's or SQL Server's database,
   // Trino's or Databricks' catalog) holds its own `information_schema`;
   // unqualified, the view is the session's, where a table of the same
   // schema and name would be read instead.
   const catalog =
      parts.length > 2 ? `${quotePathParts(parts.slice(0, -2), dialect)}.` : "";
   const precision = informationSchemaPrecision(knownDialect);
   const schemaFilter = schema
      ? ` AND lower(table_schema) = lower(${sqlString(schema, dialect)})`
      : unqualifiedSchemaFilter(knownDialect);
   return {
      sql:
         "SELECT table_catalog, table_schema, table_name, column_name, data_type" +
         precision +
         ` FROM ${catalog}information_schema.columns` +
         ` WHERE lower(table_name) = lower(${sqlString(table, dialect)})` +
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

/** One table a batched catalog read asks for: a caller's key and its path's parts. */
export interface CatalogTable {
   key: string;
   parts: readonly string[];
}

/**
 * One catalog query for several tables ({@link nativeTypeCatalogBatches}):
 * its SQL, the keys it answers, and how its rows read back as each key's
 * columns. A key the rows do not name is absent (no such table); a key whose
 * name matched more than one table is `ambiguous`.
 */
export interface CatalogBatch {
   sql: string;
   keys: string[];
   columnsOf(
      rows: Record<string, unknown>[],
   ): Map<string, NativeColumn[] | "ambiguous">;
   /**
    * On a DuckDB connection, which may attach a DuckLake catalog: the query
    * that lists its attached catalogs and their types
    * ({@link DUCKDB_CATALOGS_SQL}), and the catalog the batch's one table
    * names (undefined: the session's current one).
    */
   attachedCatalogs?: { sql: string; catalog: string | undefined };
}

/**
 * A DuckDB session's attached catalogs (`database_name`, `type`) and its
 * current one (`current`): a table in a catalog of type `ducklake` holds a
 * 128-bit integer as a DOUBLE.
 */
const DUCKDB_CATALOGS_SQL =
   "SELECT database_name, type, current_database() AS current FROM duckdb_databases()";

/** Tables per catalog query: keeps an `IN (...)` list and its SQL bounded. */
const CATALOG_BATCH_SIZE = 100;

/** `items` in chunks of {@link CATALOG_BATCH_SIZE}. */
function chunks<T>(items: readonly T[]): T[][] {
   const out: T[][] = [];
   for (let i = 0; i < items.length; i += CATALOG_BATCH_SIZE) {
      out.push(items.slice(i, i + CATALOG_BATCH_SIZE));
   }
   return out;
}

/** `items` grouped by `keyOf`, in first-seen order. */
function groupBy<T>(items: readonly T[], keyOf: (t: T) => string): T[][] {
   const groups = new Map<string, T[]>();
   for (const item of items) {
      const k = keyOf(item);
      groups.set(k, [...(groups.get(k) ?? []), item]);
   }
   return [...groups.values()];
}

/**
 * The catalog queries that read several tables' columns with their
 * warehouse types, as few as the dialect allows: one per schema or dataset
 * (`information_schema.columns ... table_name IN (...)` and the like), one
 * for every Postgres table, and DuckDB's in-process `DESCRIBE` per table.
 * Each literal is escaped for the dialect ({@link sqlString}) and each
 * identifier quoted ({@link quotePathParts}). A table the dialect cannot read
 * (an unqualified BigQuery name) is in no batch.
 *
 * The same reads, row for row, as {@link nativeTypeCatalogRead}'s.
 */
export function nativeTypeCatalogBatches(
   tables: readonly CatalogTable[],
   dialect: string,
): CatalogBatch[] {
   const knownDialect = registeredDialect(dialect);
   const lit = (v: string) => sqlString(v, dialect);
   const tableOf = (t: CatalogTable) => t.parts[t.parts.length - 1];
   switch (knownDialect) {
      case "duckdb":
         return tables.map((t) => {
            const single = nativeTypeCatalogRead(t.parts, dialect)!;
            return {
               sql: single.sql,
               keys: [t.key],
               columnsOf: (rows) => new Map([[t.key, rows.map(single.read)]]),
               attachedCatalogs: {
                  sql: DUCKDB_CATALOGS_SQL,
                  catalog: t.parts.length >= 3 ? t.parts[0] : undefined,
               },
            };
         });
      case "postgres":
         return chunks(tables).map((batch) => ({
            sql:
               "SELECT row_to_json(c) AS row FROM (" +
               "SELECT r.k AS key, a.attname AS name, format_type(a.atttypid, a.atttypmod) AS type, a.attnum " +
               `FROM (VALUES ${batch
                  .map(
                     (t) =>
                        `(${lit(t.key)}, to_regclass(${lit(quotePathParts(t.parts, dialect))}))`,
                  )
                  .join(", ")}) AS r(k, oid) ` +
               "JOIN pg_attribute a ON a.attrelid = r.oid " +
               "WHERE a.attnum > 0 AND NOT a.attisdropped) c ORDER BY c.key, c.attnum",
            keys: batch.map((t) => t.key),
            columnsOf: (rows) => {
               const out = new Map<string, NativeColumn[]>();
               for (const r of rows) {
                  const key = String(field(r, "key"));
                  out.set(key, [
                     ...(out.get(key) ?? []),
                     {
                        name: String(field(r, "name")),
                        type: String(field(r, "type")),
                     },
                  ]);
               }
               return out;
            },
         }));
      case "standardsql":
         return groupBy(
            tables.filter((t) => t.parts.length >= 2),
            (t) => JSON.stringify(t.parts.slice(0, -1)),
         ).flatMap((group) =>
            chunks(group).map((batch) => ({
               sql:
                  "SELECT table_name, column_name, data_type FROM " +
                  `${quotePathParts(batch[0].parts.slice(0, -1), dialect)}.INFORMATION_SCHEMA.COLUMNS ` +
                  `WHERE table_name IN (${batch.map((t) => lit(tableOf(t))).join(", ")}) ` +
                  "ORDER BY table_name, ordinal_position",
               keys: batch.map((t) => t.key),
               columnsOf: (rows) => {
                  const out = new Map<string, NativeColumn[]>();
                  for (const t of batch) {
                     const own = rows.filter(
                        (r) => String(field(r, "table_name")) === tableOf(t),
                     );
                     if (own.length === 0) continue;
                     out.set(
                        t.key,
                        own.map((r) => ({
                           name: String(field(r, "column_name")),
                           type: String(field(r, "data_type")),
                        })),
                     );
                  }
                  return out;
               },
            })),
         );
      // Snowflake, Databricks, Trino, Presto, MySQL, SQL Server, and an
      // unknown dialect: `information_schema.columns`.
      default:
         return groupBy(tables, (t) =>
            JSON.stringify(t.parts.slice(0, Math.max(0, t.parts.length - 2))),
         ).flatMap((group) =>
            chunks(group).map((batch) =>
               informationSchemaBatch(batch, dialect, knownDialect),
            ),
         );
   }
}

/**
 * {@link nativeTypeCatalogBatches} from `information_schema.columns`, for
 * tables in one catalog: the rows of every table asked, each matched back by
 * its schema and name as {@link informationSchemaCatalogRead} matches one.
 */
function informationSchemaBatch(
   batch: readonly CatalogTable[],
   dialect: string,
   knownDialect: DialectName | undefined,
): CatalogBatch {
   const lit = (v: string) => sqlString(v, dialect);
   const parts = batch[0].parts;
   const catalog =
      parts.length > 2 ? `${quotePathParts(parts.slice(0, -2), dialect)}.` : "";
   const precision = informationSchemaPrecision(knownDialect);
   const named = batch.map((t) => ({
      key: t.key,
      table: t.parts[t.parts.length - 1],
      schema: t.parts.length > 1 ? t.parts[t.parts.length - 2] : undefined,
   }));
   const condition = named
      .map(
         (t) =>
            `(lower(table_name) = lower(${lit(t.table)})` +
            (t.schema !== undefined
               ? ` AND lower(table_schema) = lower(${lit(t.schema)})`
               : unqualifiedSchemaFilter(knownDialect)) +
            ")",
      )
      .join(" OR ");
   return {
      sql:
         "SELECT table_catalog, table_schema, table_name, column_name, data_type" +
         precision +
         ` FROM ${catalog}information_schema.columns WHERE ${condition}` +
         " ORDER BY table_catalog, table_schema, table_name, ordinal_position",
      keys: batch.map((t) => t.key),
      columnsOf: (rows) => {
         const out = new Map<string, NativeColumn[] | "ambiguous">();
         for (const t of named) {
            const own = rows.filter(
               (r) =>
                  String(field(r, "table_name")).toLowerCase() ===
                     t.table.toLowerCase() &&
                  (t.schema === undefined ||
                     String(field(r, "table_schema")).toLowerCase() ===
                        t.schema.toLowerCase()),
            );
            if (own.length === 0) continue;
            const tables = new Set(
               own.map((r) =>
                  ["table_catalog", "table_schema", "table_name"]
                     .map((k) => String(field(r, k)))
                     .join("."),
               ),
            );
            out.set(
               t.key,
               tables.size > 1
                  ? "ambiguous"
                  : own.map((r) => ({
                       name: String(field(r, "column_name")),
                       type: informationSchemaType(r),
                    })),
            );
         }
         return out;
      },
   };
}

// ---------------------------------------------------------------------------
// Read cost.
// ---------------------------------------------------------------------------

/**
 * How much session history to admit before filtering. Set explicitly because the
 * default is 100 and applies before the `WHERE` — see {@link snowflakeCostSQL}.
 */
const SNOWFLAKE_HISTORY_LIMIT = 10_000;

/**
 * BigQuery's cost columns, selected by the query that already locates the
 * read's result table. They must ride on that query: a script's child job is
 * reachable only through its parent, so a separate lookup keyed on the child
 * id finds nothing.
 */
export const BIGQUERY_COST_COLUMNS = `
   total_slot_time_ms,
   json_extract_string(statistics, '$.query.totalBytesProcessed') AS bytes_processed,
   json_extract_string(statistics, '$.query.totalBytesBilled')    AS bytes_billed,
   json_extract_string(statistics, '$.query.cacheHit')            AS cache_hit`;

/**
 * The database to resolve `INFORMATION_SCHEMA` against.
 *
 * Emitted BARE, not quoted: quoting is case-exact while the connection parameter
 * resolves case-insensitively, so `mydb` against a stored `MYDB` would miss. A
 * name needing quotes takes the `SNOWFLAKE` fallback (measured to return the same
 * rows) rather than being interpolated raw into identifier position.
 */
function snowflakeDatabaseQualifier(
   database: string | null | undefined,
): string {
   // Snowflake's unquoted-identifier rule: a leading digit or `$` needs quotes.
   return typeof database === "string" &&
      /^[A-Za-z_][A-Za-z0-9_$]*$/.test(database)
      ? database
      : "SNOWFLAKE";
}

/**
 * Snowflake: the queries this build's session ran, found by the tag the build
 * set on it.
 *
 * - `INFORMATION_SCHEMA`, not `ACCOUNT_USAGE`: the latter lags up to three hours
 *   and needs elevated grants.
 * - Keyed on the TAG, not `LAST_QUERY_ID()`: the ADBC driver issues its own
 *   `SELECT 1` probes on the session, so the last query is often a probe.
 *   `pickSnowflakeReadRow` (build_read_cost.ts) picks the read out of the
 *   result.
 * - `_BY_SESSION` with an explicit `RESULT_LIMIT`: these table functions cap
 *   output (default 100) BEFORE the `WHERE` runs, so no predicate reaches past
 *   the cap. Measured: past 100 statements, the default form matched zero rows.
 * - The accounting query carries the same tag but is not filtered by text: the
 *   exact-SQL match in `pickSnowflakeReadRow` and `EXECUTION_STATUS` already
 *   exclude it, and a text filter would misfire on a source that models
 *   Snowflake's query history.
 * - Aliases are QUOTED: Snowflake folds bare aliases to uppercase.
 * - The tag is escaped with `sqlLiteral`, not quote-doubling: it is JSON,
 *   so backslashes arrive doubled and Snowflake consumes one as an escape.
 * - Always database-qualified: a database-less connection has no CURRENT
 *   database, so the unqualified form fails with `002004 (42601)`. The
 *   `SNOWFLAKE` shared database exists on every account; a role that cannot read
 *   it loses only the cost, since a failed lookup is treated as no cost.
 */
export function snowflakeCostSQL(
   queryTag: string,
   /** The connection's configured database, absent for a database-less one. */
   database?: string | null,
): string {
   const qualifier = `${snowflakeDatabaseQualifier(database)}.INFORMATION_SCHEMA`;
   return `
      SELECT QUERY_ID       AS "job_id",
             QUERY_TEXT     AS "query_text",
             BYTES_SCANNED  AS "bytes_scanned",
             EXECUTION_TIME AS "execution_time_ms",
             ROWS_PRODUCED  AS "rows_produced"
      FROM TABLE(${qualifier}.QUERY_HISTORY_BY_SESSION(
                 RESULT_LIMIT => ${SNOWFLAKE_HISTORY_LIMIT}))
      WHERE QUERY_TAG = '${sqlLiteral(queryTag, "snowflake")}'
        AND EXECUTION_STATUS = 'SUCCESS'`;
}

// ---------------------------------------------------------------------------
// The query tag.
// ---------------------------------------------------------------------------

/**
 * Snowflake: the statement that sets `tag` as the `QUERY_TAG` of the build's
 * session, sent through the DuckDB passthrough on the secret `secretName`.
 *
 * <b>Escaped with `sqlLiteral` for the snowflake dialect, not by doubling
 * quotes.</b> A property value may contain a backslash — the contract admits
 * printable ASCII except `"` — and JSON renders one as `\\`. Snowflake treats
 * backslash as an escape inside a single-quoted literal, so quote-doubling alone
 * delivers `\` where the JSON needed `\\`, and the stored tag no longer parses:
 * unqueryable in `QUERY_HISTORY`, which is the entire purpose of tagging, and
 * silent because the statement itself succeeds.
 */
export function snowflakeSetQueryTagStatement(
   tag: string,
   secretName: string,
): string {
   const inner = `ALTER SESSION SET QUERY_TAG = '${sqlLiteral(tag, "snowflake")}'`;
   // The outer literal is parsed by DuckDB, which has no backslash escape, so
   // the passthrough's own quote-doubling is the correct level here.
   return `SELECT * FROM snowflake_query('${inner.replace(/'/g, "''")}', '${secretName.replace(/'/g, "''")}')`;
}
