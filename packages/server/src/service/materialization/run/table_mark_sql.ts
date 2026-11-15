// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The SQL of the marks Publisher reads in a warehouse or a storage
 * destination: the ownership mark on a table an auto-run built at a v1
 * `#@ persist name=` (table_ownership.ts), and the mark on a `name=` view an
 * earlier release made over a `#(materialize)` table (leftover_name_views.ts),
 * which Publisher no longer makes but still recognizes, so it can drop this
 * package's own and never touch anyone else's.
 *
 * Pure: SQL text in, SQL text out. Every identifier is quoted by the shared
 * helpers in quoting.ts, and every string by {@link sqlString}.
 */

import { quoteIdentifier, quoteTablePath } from "../../quoting";

/** The prefix of the comment on a `name=` view an earlier release made. */
const NAME_VIEW_MARK = "malloy-publisher:name-view";

/**
 * The dialects (Malloy `dialectName`) whose relations' comments can be read
 * back by {@link relationProbeSql}. MySQL and Presto are probed by
 * table_ownership.ts's own queries.
 */
const PROBE_DIALECTS: ReadonlySet<string> = new Set([
   "duckdb",
   "postgres",
   "standardsql",
   "snowflake",
   "databricks",
   "trino",
   "sqlserver",
]);

/** Thrown when a dialect or a name shape cannot be probed. */
class RelationProbeUnsupportedError extends Error {}

/** Whether {@link relationProbeSql} can probe a dialect. */
export function relationProbeSupported(dialect: string): boolean {
   return PROBE_DIALECTS.has(dialect);
}

/**
 * What a leftover `name=` view's comment says: the table it reads (`target`)
 * and the package that made it (`owner`, `<environment>/<package>`).
 */
interface NameViewMark {
   target: string;
   owner: string;
}

/** The `owner` a mark carries for a package: `<environment>/<package>`. */
export function packageOwner(
   environmentName: string,
   packageName: string,
): string {
   return `${environmentName}/${packageName}`;
}

/**
 * The mark read back from a view's comment, or undefined when the comment is
 * not Publisher's mark: exactly the prefix, then `owner=` and `target=` once
 * each and nothing else. Accepts BigQuery's TABLE_OPTIONS spelling, a
 * double-quoted literal.
 */
export function parseNameViewComment(
   comment: unknown,
): NameViewMark | undefined {
   if (typeof comment !== "string") return undefined;
   let text = comment.trim();
   if (text.length >= 2 && text.startsWith('"') && text.endsWith('"')) {
      text = text.slice(1, -1);
   }
   const [prefix, ...fields] = text.split(" ");
   if (prefix !== NAME_VIEW_MARK || fields.length !== 2) return undefined;
   const values = new Map<string, string>();
   for (const field of fields) {
      const eq = field.indexOf("=");
      if (eq <= 0) return undefined;
      const key = field.slice(0, eq);
      const encoded = field.slice(eq + 1);
      if (encoded === "" || values.has(key)) return undefined;
      try {
         values.set(key, decodeURIComponent(encoded));
      } catch {
         return undefined;
      }
   }
   const target = values.get("target");
   const owner = values.get("owner");
   return target !== undefined && owner !== undefined
      ? { target, owner }
      : undefined;
}

/**
 * The prefix of the ownership mark on a table an auto-run built at the
 * author's `name=` (table_ownership.ts).
 */
export const OWNED_TABLE_MARK = "malloy-publisher:table";

/**
 * The owner a table's ownership mark names (`<environment>/<package>`), or
 * undefined when the comment is not one: exactly the prefix, then `owner=`
 * once. Accepts BigQuery's double-quoted TABLE_OPTIONS spelling.
 */
export function parseOwnedTableComment(comment: unknown): string | undefined {
   if (typeof comment !== "string") return undefined;
   let text = comment.trim();
   if (text.length >= 2 && text.startsWith('"') && text.endsWith('"')) {
      text = text.slice(1, -1);
   }
   const fields = text.split(" ");
   if (fields.length !== 2 || fields[0] !== OWNED_TABLE_MARK) return undefined;
   if (!fields[1].startsWith("owner=")) return undefined;
   const encoded = fields[1].slice("owner=".length);
   if (encoded === "") return undefined;
   try {
      return decodeURIComponent(encoded);
   } catch {
      return undefined;
   }
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
 * table_ownership.ts reads the answer.
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
export function duckdbNameMatch(segments: string[], column: string): string {
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
 * {@link viewDefinitionReadsOnly}.
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
 * Whether a view's definition reads exactly `target` (`SELECT * FROM
 * <target>`, as the warehouse may spell it back: columns expanded,
 * identifiers unquoted or re-cased): one FROM, no join. A name with no
 * schema, on either side, is read in `viewSchema`, the schema the view lives
 * in (where its own unqualified names resolve, and where earlier releases
 * put a bare `name=` view beside its table), so `s2.g1` is not `g1`. A view
 * reading anything else, or more than one relation, is not.
 */
export function viewDefinitionReadsOnly(
   definition: unknown,
   target: string,
   viewSchema?: unknown,
): boolean {
   if (typeof definition !== "string") return false;
   const text = definition
      .toLowerCase()
      .replace(/["`[\]]/g, "")
      .replace(/\s+/g, " ");
   // A comma after the FROM target is a second relation.
   const froms = [...text.matchAll(/\bfrom ([^\s;),]+)\s*(,?)/g)];
   if (froms.length !== 1 || froms[0][2] || /\bjoin\b/.test(text)) {
      return false;
   }
   const schema =
      typeof viewSchema === "string" && viewSchema !== ""
         ? viewSchema.toLowerCase().replace(/["`[\]]/g, "")
         : undefined;
   const qualify = (segments: string[]) =>
      segments.length === 1 && schema ? [schema, ...segments] : segments;
   const read = qualify(froms[0][1].split("."));
   const want = qualify(
      target
         .toLowerCase()
         .replace(/["`[\]]/g, "")
         .split("."),
   );
   const [shorter, longer] =
      read.length <= want.length ? [read, want] : [want, read];
   return shorter.every(
      (segment, i) => segment === longer[longer.length - shorter.length + i],
   );
}

/**
 * Postgres: the view `viewOid` (an SQL expression for its oid) reads the
 * relation `targetOid` and no other. Its `_RETURN` rule depends on
 * `targetOid`, and on no other table, view, materialized view, partitioned or
 * foreign table besides the view itself: a join or a subquery over another
 * relation is not proven, nor is a view repointed off the target.
 */
export function postgresReadsOnlySql(
   viewOid: string,
   targetOid: string,
): string {
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
 * {@link viewDefinitionReadsOnly} reads.
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

/** `DROP VIEW IF EXISTS` for the view `segments` names. Never drops a table. */
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
