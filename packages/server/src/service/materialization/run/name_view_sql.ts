// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   quoteIdentifier,
   quoteRenameTarget,
   quoteTablePath,
} from "../../quoting";

/**
 * The SQL of the `name=` view (materialization v2, phase 3): a view at the
 * declared `name=` over the physical table a run built, for readers outside
 * Malloy, where the table itself is named by its generation (an orchestrated
 * host's content-addressed name, or a `scope: version` run's versioned name).
 *
 * Every view Publisher makes carries a COMMENT MARK, the string
 * {@link nameViewComment} writes: the orphan sweeper, the table dropper, the
 * tier reclaimer and package delete recognize a Publisher-owned view by it,
 * and the build refuses a `name=` held by anything without it, or by another
 * package's view. The mark names the package that made the view and the
 * table it reads, so a reclaim can tell whether the view is still this
 * package's and still reads the table it is dropping.
 *
 * The mark is the warehouse half of ownership: it is what a sweep that sees
 * only the warehouse (the control plane's orphan sweeper, another server) can
 * read. The Publisher half is the run record (`metadata.nameViews`), which
 * says which views a run pointed and so which a drop must settle. Neither is
 * a key: a view is acted on only when both agree.
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
 * What a Publisher `name=` view's comment says: the table it reads (`target`,
 * the physical table name as the manifest records it) and the package that
 * made it (`owner`, `<environment>/<package>`). The owner is what lets one
 * package refuse a `name=` another package's view holds, rather than
 * silently repointing a stable name an outside reader relies on.
 */
export interface NameViewMark {
   target: string;
   owner: string;
}

/** The `owner` a mark carries for a package. */
export function nameViewOwner(
   environmentName: string,
   packageName: string,
): string {
   return `${environmentName}/${packageName}`;
}

/** URI-encoded, the single quote included, so no value needs escaping. */
function encodeMarkValue(value: string): string {
   // encodeURIComponent leaves the single quote alone.
   return encodeURIComponent(value).replace(/'/g, "%27");
}

/**
 * The comment mark for a view. Each value is URI-encoded so the mark carries
 * no quote, backslash or space whatever the table or package is called, and
 * survives every dialect's string literal and BigQuery's quoted option value.
 */
export function nameViewComment(mark: NameViewMark): string {
   return (
      `${NAME_VIEW_MARK} owner=${encodeMarkValue(mark.owner)} ` +
      `target=${encodeMarkValue(mark.target)}`
   );
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
   | { state: "ours"; target: string; owner: string }
   /**
    * `tableOwner`: the package a table's ownership mark names
    * ({@link parseOwnedTableComment}), when it carries one.
    */
   | { state: "taken"; kind: string; tableOwner?: string };

/** Read {@link nameViewProbeSql}'s rows. */
export function nameViewSlotOf(rows: Record<string, unknown>[]): NameViewSlot {
   if (rows.length === 0) return { state: "empty" };
   let found: NameViewMark | undefined;
   for (const row of rows) {
      const kind = String(field(row, "kind") ?? "").toUpperCase();
      if (kind !== "VIEW") {
         const tableOwner = parseOwnedTableComment(field(row, "comment"));
         return {
            state: "taken",
            kind: kind || "TABLE",
            ...(tableOwner !== undefined ? { tableOwner } : {}),
         };
      }
      const mark = parseNameViewComment(field(row, "comment"));
      if (!mark) return { state: "taken", kind: "VIEW" };
      found ??= mark;
   }
   return { state: "ours", ...found! };
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
interface NameViewDdl {
   primary: string;
   rollback?: string;
   fallback?: string;
}

/**
 * The DDL that points the view `segments` names at `targetSql` (an already
 * quoted table reference), carrying `mark`.
 */
export function nameViewReplaceDdl(
   segments: string[],
   targetSql: string,
   mark: NameViewMark,
   dialect: string,
): NameViewDdl {
   const view = quotedPath(segments, dialect);
   const comment = sqlString(nameViewComment(mark), dialect);
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

/**
 * Dialects whose DDL is transactional and reached in one call, so a table
 * can be renamed aside and a view made in its place as one change (as the
 * colocated swap does, colocated_build_strategy.ts).
 */
const TRANSACTIONAL_REPLACE_DIALECTS: ReadonlySet<string> = new Set([
   "duckdb",
   "postgres",
]);

/** Whether a table at a name can be replaced by a view as one transaction. */
function nameViewReplaceIsTransactional(dialect: string): boolean {
   return TRANSACTIONAL_REPLACE_DIALECTS.has(dialect);
}

/**
 * How to put a marked view in place of a TABLE at the same name: a table
 * this package's own committed run wrote there (a self-instructed build
 * names its table `name=` itself), which a build that names its table by
 * generation now replaces with the view reading that generation.
 *
 * On every dialect the old table is kept, renamed to `aside`, until the run
 * that replaced it commits: only then is it dropped (`dropAside`). A run
 * that does not commit drops the view and renames the table back
 * (`restore`), so the name reads what the last committed run left there.
 *
 * - `transactional` (DuckDB, Postgres): `replace` renames the table aside,
 *   makes the view and comments it, as one transaction in one call, and
 *   `restore` drops the view and renames the table back the same way;
 *   `rollback` (when given) closes a transaction a failed call left open. A
 *   reader sees the old table or the view, never neither. On Postgres a
 *   view a reader built over the old table refuses the replacement (it
 *   would follow the table to its aside name and keep reading it): the
 *   table stays as it was.
 * - otherwise: `moveAside` renames the table to `aside`; `create` makes the
 *   view as {@link nameViewReplaceDdl} would; `restore` renames the table
 *   back once the view is dropped. Between the rename and the view a reader
 *   finds nothing at the name (a not-found, never a partial table).
 */
export type NameViewTableReplaceDdl =
   | {
        transactional: true;
        replace: string;
        restore: string;
        rollback?: string;
        dropAside: string;
     }
   | {
        transactional: false;
        moveAside: string;
        create: NameViewDdl;
        restore: string;
        dropAside: string;
     };

/**
 * `ALTER TABLE … RENAME` (`sp_rename` on SQL Server) of the table `from`
 * names to the last segment of `to`, within its own schema.
 */
export function renameTableSql(
   from: string[],
   to: string[],
   dialect: string,
): string {
   if (dialect === "sqlserver") {
      // sp_rename takes the current name as a (bracket-quoted) path and the
      // new one bare: brackets there would become part of the name.
      return `EXEC sp_rename ${sqlString(quotedPath(from, dialect), dialect)}, ${sqlString(to[to.length - 1], dialect)}`;
   }
   return `ALTER TABLE ${quotedPath(from, dialect)} RENAME TO ${quoteRenameTarget(to.join("."), dialect)}`;
}

/**
 * Postgres: fail when any view or rule reads the table `segments` names. A
 * renamed table keeps its dependents, so a reader's view would follow it to
 * its aside name, keep reading the old rows, and refuse its drop.
 */
function postgresNoDependentsGuard(segments: string[]): string {
   const table = sqlString(quotedPath(segments, "postgres"), "postgres");
   return (
      `DO $guard$ BEGIN IF EXISTS (SELECT 1 FROM pg_catalog.pg_depend d ` +
      `JOIN pg_catalog.pg_rewrite r ON r.oid = d.objid ` +
      `WHERE d.classid = 'pg_catalog.pg_rewrite'::regclass ` +
      `AND d.refclassid = 'pg_catalog.pg_class'::regclass ` +
      `AND d.refobjid = ${table}::regclass AND r.ev_class <> d.refobjid) ` +
      `THEN RAISE EXCEPTION 'a view or rule depends on table %', ${table}; ` +
      `END IF; END $guard$`
   );
}

/**
 * The DDL that replaces the table `segments` names with a view reading
 * `targetSql`, carrying `mark`, keeping the old table at `asideSegments`
 * until the replacing run commits.
 */
export function nameViewReplaceTableDdl(
   segments: string[],
   asideSegments: string[],
   targetSql: string,
   mark: NameViewMark,
   dialect: string,
): NameViewTableReplaceDdl {
   if (!nameViewDialectSupported(dialect)) throw unsupportedDialect(dialect);
   if (dialect === "sqlserver") assertSqlServerShape(segments);
   const view = quotedPath(segments, dialect);
   const aside = quotedPath(asideSegments, dialect);
   const moveAside = renameTableSql(segments, asideSegments, dialect);
   const moveBack = renameTableSql(asideSegments, segments, dialect);
   const dropAside = `DROP TABLE IF EXISTS ${aside}`;
   if (nameViewReplaceIsTransactional(dialect)) {
      const replace = [
         ...(dialect === "postgres"
            ? [postgresNoDependentsGuard(segments)]
            : []),
         moveAside,
         `CREATE VIEW ${view} AS SELECT * FROM ${targetSql}`,
         `COMMENT ON VIEW ${view} IS ${sqlString(nameViewComment(mark), dialect)}`,
      ];
      const restore = [`DROP VIEW IF EXISTS ${view}`, moveBack];
      // DuckDB autocommits each statement of a string: bracketed. Postgres
      // runs a multi-statement string as one implicit transaction (an
      // explicit BEGIN would leave an aborted one open on failure).
      const oneCall = (statements: string[]) =>
         (dialect === "duckdb"
            ? ["BEGIN TRANSACTION", ...statements, "COMMIT"]
            : statements
         )
            .map((s) => `${s};`)
            .join("\n");
      return {
         transactional: true,
         replace: oneCall(replace),
         restore: oneCall(restore),
         ...(dialect === "duckdb" ? { rollback: "ROLLBACK" } : {}),
         dropAside,
      };
   }
   return {
      transactional: false,
      moveAside,
      create: nameViewReplaceDdl(segments, targetSql, mark, dialect),
      restore: moveBack,
      dropAside,
   };
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
