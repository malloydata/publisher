/**
 * Split a possibly schema-qualified table name into its schema prefix
 * (including the trailing dot) and the bare table name.
 *
 * Examples:
 *   "my_schema.my_table" -> { schemaPrefix: "my_schema.", bareName: "my_table" }
 *   "my_table"           -> { schemaPrefix: "", bareName: "my_table" }
 */
export function splitTablePath(tableName: string): {
   schemaPrefix: string;
   bareName: string;
} {
   const lastDot = tableName.lastIndexOf(".");
   if (lastDot >= 0) {
      return {
         schemaPrefix: tableName.substring(0, lastDot + 1),
         bareName: tableName.substring(lastDot + 1),
      };
   }
   return { schemaPrefix: "", bareName: tableName };
}

// Dialects whose identifier quote character is a backtick; everything else uses
// the SQL-standard double quote. Keyed by Malloy `dialectName`.
const BACKTICK_DIALECTS = new Set(["standardsql", "mysql", "databricks"]);

/**
 * Quote a single SQL identifier for {@code dialect}, escaping any embedded quote
 * character by doubling it.
 */
export function quoteIdentifier(identifier: string, dialect: string): string {
   if (BACKTICK_DIALECTS.has(dialect)) {
      return "`" + identifier.replace(/`/g, "``") + "`";
   }
   return '"' + identifier.replace(/"/g, '""') + '"';
}

/**
 * Dialect-quote a (possibly container-qualified) table path so it can be inlined
 * into DDL. Each dot segment is quoted independently and rejoined with dots, so
 * a path like {@code my-proj.mydataset.engaged_events_v0} becomes
 * `` `my-proj`.`mydataset`.`engaged_events_v0` `` on BigQuery or
 * {@code "my-proj"."mydataset"."engaged_events_v0"} on Postgres — handling
 * container hierarchies and quote-requiring names (e.g. hyphenated BigQuery
 * project ids) uniformly. The control plane provides the logical (unquoted) path
 * in `physicalTableName`; quoting for the warehouse is the publisher's job.
 */
export function quoteTablePath(tableName: string, dialect: string): string {
   return tableName
      .split(".")
      .map((segment) => quoteIdentifier(segment, dialect))
      .join(".");
}
