// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The SQL text that differs by warehouse dialect, one family per section: the
 * string literal, the relation and view probes and the view drop, the
 * ownership probe and mark, the incremental refresh's probes and its
 * transaction around a delta, the colocated swap's transaction, the
 * leftover `name=` view's in-transaction replace, the native-type catalog
 * read, the read cost and the query tag.
 *
 * The other half of the dialect table: `domain/dialect.ts` says whether a
 * dialect can do something, this module says how it is spelled. Text that is
 * the same in every dialect (but for quoting) stays with the statement that
 * runs it; identifiers are quoted by `quoting.ts`.
 *
 * Pure: SQL text in, SQL text out. A dialect a family does not name keeps
 * that family's own answer for it, which each function states: a refusal (a
 * thrown error), a default spelling, or undefined. Every statement is pinned,
 * per dialect, in `dialect_statements.golden.json`.
 */

import { sqlLiteral } from "../../db_utils";
import { quoteIdentifier, quoteTablePath } from "../../quoting";
import { capabilitiesOf } from "../domain/dialect";

// ---------------------------------------------------------------------------
// String literals.
// ---------------------------------------------------------------------------

/**
 * A SQL string literal for `dialect`, as the marks and their probes write
 * one. An unknown dialect doubles the quote.
 */
export function sqlString(value: string, dialect: string): string {
   switch (dialect) {
      // Backslash is the escape character in these dialects' literals.
      case "standardsql":
      case "databricks":
         return `'${value.replace(/\\/g, "\\\\").replace(/'/g, "\\'")}'`;
      // Snowflake reads backslash escapes and the doubled quote.
      case "snowflake":
         return `'${value.replace(/\\/g, "\\\\").replace(/'/g, "''")}'`;
      case "sqlserver":
         return `N'${value.replace(/'/g, "''")}'`;
      default:
         return `'${value.replace(/'/g, "''")}'`;
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
   if (dialect === "standardsql") {
      return value.replace(/\\/g, "\\\\").replace(/'/g, "\\'");
   }
   const quoted = value.replace(/'/g, "''");
   return dialect === "snowflake" ? quoted.replace(/\\/g, "\\\\") : quoted;
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

function quotedPath(segments: string[], dialect: string): string {
   return quoteTablePath(segments.join("."), dialect);
}

/**
 * A query answering what holds the name `segments` names: no row when
 * nothing does, otherwise one row per relation with `kind` (`VIEW` for a
 * view, anything else for a table) and `comment` (its comment, or null).
 * table_ownership.ts reads the answer. Refuses an unknown dialect.
 */
export function relationProbeSql(segments: string[], dialect: string): string {
   const lit = (v: string) => sqlString(v, dialect);
   const name = segments[segments.length - 1];
   switch (dialect) {
      case "duckdb": {
         const match = (column: string) => duckdbNameMatch(segments, column);
         return (
            `SELECT 'VIEW' AS kind, comment FROM duckdb_views() WHERE NOT internal AND ${match("view_name")} ` +
            `UNION ALL SELECT 'TABLE' AS kind, comment FROM duckdb_tables() WHERE ${match("table_name")}`
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
            `obj_description(c.oid, 'pg_class') AS comment FROM pg_class c ` +
            `WHERE c.oid = to_regclass(${lit(quotedPath(segments, dialect))})) AS p`
         );
      case "standardsql": {
         if (segments.length < 2) {
            throw new RelationProbeUnsupportedError(
               `a BigQuery name needs a dataset-qualified name, not '${name}'`,
            );
         }
         const schema = `${quotedPath(segments.slice(0, -1), dialect)}.INFORMATION_SCHEMA`;
         return (
            `SELECT t.table_type AS kind, o.option_value AS comment ` +
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
            `SELECT TABLE_TYPE AS "kind", COMMENT AS "comment" FROM ${from} ` +
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
               : dialect === "databricks"
                 ? "current_schema()"
                 : "current_schema";
         if (dialect === "databricks") {
            return (
               `SELECT table_type AS kind, comment FROM ${from} ` +
               `WHERE table_schema = ${schema} AND table_name = lower(${lit(name)})`
            );
         }
         return (
            `SELECT t.table_type AS kind, c.comment AS comment FROM ${from} t ` +
            `LEFT JOIN system.metadata.table_comments c ON c.catalog_name = t.table_catalog ` +
            `AND c.schema_name = t.table_schema AND c.table_name = t.table_name ` +
            `WHERE t.table_schema = ${schema} AND t.table_name = lower(${lit(name)})`
         );
      }
      case "sqlserver":
         assertSqlServerShape(segments);
         return (
            `SELECT CASE o.type WHEN 'V' THEN 'VIEW' ELSE 'TABLE' END AS kind, ` +
            `CAST(ep.value AS nvarchar(4000)) AS comment FROM sys.objects o ` +
            `LEFT JOIN sys.extended_properties ep ON ep.class = 1 AND ep.major_id = o.object_id ` +
            `AND ep.minor_id = 0 AND ep.name = N'MS_Description' ` +
            `WHERE o.object_id = OBJECT_ID(${lit(quotedPath(segments, dialect))})`
         );
      default:
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
const CURRENT_SCHEMA_SQL: Readonly<Record<string, string>> = {
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
   switch (dialect) {
      case "duckdb":
         return (
            `SELECT sql AS definition, schema_name AS view_schema FROM duckdb_views() ` +
            `WHERE NOT internal AND ${duckdbNameMatch(segments, "view_name")}`
         );
      case "postgres": {
         const path = lit(quotedPath(segments, dialect));
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
         const schema = `${quotedPath(segments.slice(0, -1), dialect)}.INFORMATION_SCHEMA`;
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
               : CURRENT_SCHEMA_SQL[dialect];
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
            `WHERE o.type = 'V' AND o.object_id = OBJECT_ID(${lit(quotedPath(segments, dialect))})`
         );
      default:
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
   switch (dialect) {
      case "postgres":
         return {
            answer: "reads",
            sql: `SELECT row_to_json(p) AS "row" FROM (SELECT ${postgresReadsOnlySql(
               `to_regclass(${lit(quotedPath(view, dialect))})`,
               `to_regclass(${lit(quotedPath(table, dialect))})`,
            )} AS reads) AS p`,
         };
      case "sqlserver":
         assertSqlServerShape(view);
         return {
            answer: "reads",
            sql: (() => {
               const v = `OBJECT_ID(${lit(quotedPath(view, dialect))})`;
               const t = `OBJECT_ID(${lit(quotedPath(table, dialect))})`;
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
      default:
         return { answer: "definition", sql: viewDefinitionSql(view, dialect) };
   }
}

/**
 * `DROP VIEW IF EXISTS` for the view `segments` names. Never drops a table.
 * Refuses a dialect {@link relationProbeSupported} does not name.
 */
export function dropViewSql(segments: string[], dialect: string): string {
   if (!relationProbeSupported(dialect)) throw unsupportedDialect(dialect);
   if (dialect === "sqlserver") assertSqlServerShape(segments);
   return `DROP VIEW IF EXISTS ${quotedPath(segments, dialect)}`;
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
   if (dialect === "mysql") {
      const schema =
         segments.length > 1
            ? lit(segments[segments.length - 2])
            : "DATABASE()";
      return (
         `SELECT TABLE_TYPE AS kind, TABLE_COMMENT AS comment FROM information_schema.TABLES ` +
         `WHERE TABLE_SCHEMA = ${schema} AND TABLE_NAME = ${lit(name)}`
      );
   }
   if (dialect === "presto") {
      const schema =
         segments.length > 1
            ? `lower(${lit(segments[segments.length - 2])})`
            : "current_schema";
      const from =
         segments.length === 3
            ? `${quoteTablePath(segments[0], dialect)}.information_schema.tables`
            : "information_schema.tables";
      return (
         `SELECT table_type AS kind, CAST(NULL AS varchar) AS comment FROM ${from} ` +
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
   switch (dialect) {
      case "standardsql":
         return `ALTER TABLE ${table} SET OPTIONS(description = ${literal})`;
      case "mysql":
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
      default:
         return `COMMENT ON TABLE ${table} IS ${literal}`;
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
   return dialect === "postgres"
      ? `SELECT row_to_json(__p) AS "row" FROM (${innerSelect}) AS __p`
      : innerSelect;
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
   if (dialect === "duckdb") {
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
   } else if (dialect === "postgres" || dialect === "snowflake") {
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
         dialect === "snowflake" && segments.length >= 3
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
   return dialect === "standardsql"
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
 * A delta's statements as one transaction: a Snowflake Scripting block on
 * Snowflake (one statement per call, each possibly on a different pooled
 * session), otherwise the statements between {@link transactionKeywords}'
 * BEGIN and COMMIT.
 */
export function inOneTransaction(dialect: string, body: string[]): string[] {
   if (dialect === "snowflake") return [snowflakeScriptingBlock(body)];
   const transaction = transactionKeywords(dialect);
   return [transaction.begin, ...body, transaction.commit];
}

// ---------------------------------------------------------------------------
// The colocated swap's transaction.
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
   const explicit = dialect !== "postgres";
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
               `FROM pg_attribute a WHERE a.attrelid = to_regclass(${doubledQuoteString(quoted)}) ` +
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
               `WHERE table_name = ${doubledQuoteString(table)} ORDER BY ordinal_position`,
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
            ? ` AND lower(table_schema) = lower(${doubledQuoteString(schema)})`
            : dialect === "mysql"
              ? " AND table_schema = DATABASE()"
              : "";
         return {
            sql:
               "SELECT table_catalog, table_schema, table_name, column_name, data_type" +
               precision +
               ` FROM ${catalog}information_schema.columns` +
               ` WHERE lower(table_name) = lower(${doubledQuoteString(table)})` +
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
