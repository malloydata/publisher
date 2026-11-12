// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { quoteIdentifier, quoteTablePath } from "../../quoting";

/**
 * The SQL of the `name=` view (materialization v2, phase 3): a view at the
 * declared `name=` over the physical table a run built, for readers outside
 * Malloy, where the table itself is named by its generation (an orchestrated
 * host's content-addressed name, or a `scope: version` run's versioned name).
 *
 * Every view Publisher makes carries a COMMENT MARK, the string
 * {@link nameViewComment} writes: the orphan sweeper, the table dropper, the
 * tier reclaimer and package delete recognize a Publisher-owned view by it,
 * and the build refuses a `name=` held by anything without it. The mark names
 * the table the view reads, so a reclaim can tell whether the view still
 * reads the table it is dropping.
 *
 * Pure: SQL text in, SQL text out. Every identifier is quoted by the shared
 * helpers in quoting.ts, and every string by {@link sqlString}.
 */

/** The prefix every Publisher-owned `name=` view's comment starts with. */
export const NAME_VIEW_MARK = "malloy-publisher:name-view";

/**
 * The dialects (Malloy `dialectName`) a `name=` view can be made in: each can
 * comment a view and read the comment back, so the view is recognizable.
 * Not MySQL (a view cannot carry a comment) and not Presto (its CREATE VIEW
 * takes no COMMENT): an unmarked view is one no cleanup can recognize, so
 * none is made there.
 */
const NAME_VIEW_DIALECTS: ReadonlySet<string> = new Set([
   "duckdb",
   "postgres",
   "standardsql",
   "snowflake",
   "databricks",
   "trino",
   "sqlserver",
]);

/** Thrown when a dialect or a name shape cannot hold a marked view. */
export class NameViewUnsupportedError extends Error {}

/** Whether a dialect can hold a marked `name=` view. */
export function nameViewDialectSupported(dialect: string): boolean {
   return NAME_VIEW_DIALECTS.has(dialect);
}

/**
 * The comment mark for a view reading `target` (the physical table name, as
 * recorded on the manifest). The target is URI-encoded so the mark carries no
 * quote, backslash or space whatever the table is called, and survives every
 * dialect's string literal and BigQuery's quoted option value.
 */
export function nameViewComment(target: string): string {
   // encodeURIComponent leaves the single quote alone.
   const encoded = encodeURIComponent(target).replace(/'/g, "%27");
   return `${NAME_VIEW_MARK} target=${encoded}`;
}

/**
 * The table a Publisher `name=` view reads, read from its comment, or
 * undefined when the comment is not Publisher's mark. Accepts BigQuery's
 * TABLE_OPTIONS spelling, a double-quoted literal.
 */
export function parseNameViewComment(
   comment: unknown,
): { target: string } | undefined {
   if (typeof comment !== "string") return undefined;
   let text = comment.trim();
   if (text.length >= 2 && text.startsWith('"') && text.endsWith('"')) {
      text = text.slice(1, -1);
   }
   const prefix = `${NAME_VIEW_MARK} target=`;
   if (!text.startsWith(prefix)) return undefined;
   const encoded = text.slice(prefix.length);
   if (encoded === "" || /\s/.test(encoded)) return undefined;
   try {
      return { target: decodeURIComponent(encoded) };
   } catch {
      return undefined;
   }
}

/** Whether a relation's comment is a Publisher `name=` view's mark. */
export function isNameViewComment(comment: unknown): boolean {
   return parseNameViewComment(comment) !== undefined;
}

/** A SQL string literal for `dialect`. */
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

/** The unquoted segments of a dotted table path. */
export function pathSegments(path: string): string[] {
   return path.split(".");
}

function quotedPath(segments: string[], dialect: string): string {
   return quoteTablePath(segments.join("."), dialect);
}

/**
 * A query answering what holds the name `segments` names: no row when
 * nothing does, otherwise one row per relation with `kind` (`VIEW` for a
 * view, anything else for a table) and `comment` (its comment, or null).
 * Read the answer with {@link nameViewSlotOf}.
 */
export function nameViewProbeSql(segments: string[], dialect: string): string {
   const lit = (v: string) => sqlString(v, dialect);
   const name = segments[segments.length - 1];
   switch (dialect) {
      case "duckdb": {
         // DuckDB resolves names case-insensitively, and a two-part name as a
         // catalog's `main` schema or a schema of the current catalog: both
         // are matched, so either holder is seen.
         const match = (column: string) => {
            const n = `lower(${column}) = lower(${lit(name)})`;
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
         };
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
            throw new NameViewUnsupportedError(
               `a BigQuery name= view needs a dataset-qualified name, not '${name}'`,
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

/** What holds a `name=`: nothing, a Publisher view (and the table it reads), or something else. */
export type NameViewSlot =
   | { state: "empty" }
   | { state: "ours"; target: string }
   | { state: "taken"; kind: string };

/** Read {@link nameViewProbeSql}'s rows. */
export function nameViewSlotOf(rows: Record<string, unknown>[]): NameViewSlot {
   if (rows.length === 0) return { state: "empty" };
   let target: string | undefined;
   for (const row of rows) {
      const kind = String(field(row, "kind") ?? "").toUpperCase();
      if (kind !== "VIEW") return { state: "taken", kind: kind || "TABLE" };
      const mark = parseNameViewComment(field(row, "comment"));
      if (!mark) return { state: "taken", kind: "VIEW" };
      target ??= mark.target;
   }
   return { state: "ours", target: target! };
}

/** A column of a result row, whatever case the driver reports it in. */
function field(row: Record<string, unknown>, name: string): unknown {
   if (name in row) return row[name];
   const key = Object.keys(row).find((k) => k.toLowerCase() === name);
   return key === undefined ? undefined : row[key];
}

/**
 * How to make or repoint a view: run `primary` as one call; when it fails,
 * run `rollback` (when given) to close a transaction it opened, then try
 * `fallback` (when given) as one call. Each call makes the view and its mark
 * together, so a reader never sees the view reading one table while marked
 * for another.
 */
export interface NameViewDdl {
   primary: string;
   rollback?: string;
   fallback?: string;
}

/**
 * The DDL that points the view `segments` names at `targetSql` (an already
 * quoted table reference), marked as reading `target`.
 */
export function nameViewReplaceDdl(
   segments: string[],
   targetSql: string,
   target: string,
   dialect: string,
): NameViewDdl {
   const view = quotedPath(segments, dialect);
   const comment = sqlString(nameViewComment(target), dialect);
   const select = `SELECT * FROM ${targetSql}`;
   switch (dialect) {
      case "duckdb":
         // A multi-statement string autocommits each statement: bracketed so
         // the view and its mark change together.
         return {
            primary: [
               "BEGIN TRANSACTION",
               `CREATE OR REPLACE VIEW ${view} AS ${select}`,
               `COMMENT ON VIEW ${view} IS ${comment}`,
               "COMMIT",
            ]
               .map((s) => `${s};`)
               .join("\n"),
            rollback: "ROLLBACK",
         };
      case "postgres": {
         // One call is one implicit transaction. CREATE OR REPLACE keeps the
         // views a reader built over this one, but refuses a column list that
         // is not an extension of the old one; then the view is dropped and
         // made again (which a reader's own dependent view refuses, leaving
         // the old view as it was).
         const create = (orReplace: boolean) =>
            [
               `CREATE ${orReplace ? "OR REPLACE " : ""}VIEW ${view} AS ${select}`,
               `COMMENT ON VIEW ${view} IS ${comment}`,
            ]
               .map((s) => `${s};`)
               .join("\n");
         return {
            primary: create(true),
            fallback: `DROP VIEW IF EXISTS ${view};\n${create(false)}`,
         };
      }
      case "standardsql":
         return {
            primary: `CREATE OR REPLACE VIEW ${view} OPTIONS(description = ${comment}) AS ${select}`,
         };
      case "snowflake":
         return {
            primary: `CREATE OR REPLACE VIEW ${view} COMMENT = ${comment} AS ${select}`,
         };
      case "databricks":
      case "trino":
         return {
            primary: `CREATE OR REPLACE VIEW ${view} COMMENT ${comment} AS ${select}`,
         };
      case "sqlserver": {
         assertSqlServerShape(segments);
         // CREATE VIEW must be alone in its batch, so it runs through EXEC;
         // the view and its MS_Description change in one transaction.
         const lit = (v: string) => sqlString(v, dialect);
         const schema =
            segments.length === 2 ? lit(segments[0]) : "SCHEMA_NAME()";
         const name = lit(segments[segments.length - 1]);
         const property =
            `@name = N'MS_Description', @value = ${comment}, @level0type = N'SCHEMA', ` +
            `@level0name = @schema, @level1type = N'VIEW', @level1name = ${name}`;
         return {
            primary: [
               "SET XACT_ABORT ON;",
               `DECLARE @schema sysname = ${schema};`,
               "BEGIN TRANSACTION;",
               `EXEC(${lit(`CREATE OR ALTER VIEW ${view} AS ${select}`)});`,
               `IF EXISTS (SELECT 1 FROM sys.extended_properties WHERE class = 1 ` +
                  `AND major_id = OBJECT_ID(${lit(view)}) AND minor_id = 0 ` +
                  `AND name = N'MS_Description')`,
               `   EXEC sys.sp_updateextendedproperty ${property};`,
               "ELSE",
               `   EXEC sys.sp_addextendedproperty ${property};`,
               "COMMIT TRANSACTION;",
            ].join("\n"),
         };
      }
      default:
         throw unsupportedDialect(dialect);
   }
}

/** `DROP VIEW IF EXISTS` for the view `segments` names. Never drops a table. */
export function nameViewDropSql(segments: string[], dialect: string): string {
   if (!nameViewDialectSupported(dialect)) throw unsupportedDialect(dialect);
   if (dialect === "sqlserver") assertSqlServerShape(segments);
   return `DROP VIEW IF EXISTS ${quotedPath(segments, dialect)}`;
}

/** SQL Server's CREATE VIEW takes a schema, never a database. */
function assertSqlServerShape(segments: string[]): void {
   if (segments.length > 2) {
      throw new NameViewUnsupportedError(
         `a SQL Server name= view takes at most a schema, not '${segments.join(".")}'`,
      );
   }
}

function unsupportedDialect(dialect: string): NameViewUnsupportedError {
   return new NameViewUnsupportedError(
      `a name= view cannot be marked as Publisher's in dialect '${dialect}'`,
   );
}
