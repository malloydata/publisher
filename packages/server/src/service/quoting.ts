/**
 * The bare (unqualified) name of a possibly container-qualified table path:
 * the segment after the last dot, e.g. `my_schema.my_table` -> `my_table` and
 * `my_table` -> `my_table`. Used as the RENAME target, which names a table
 * within its existing schema rather than re-stating the full path.
 */
export function bareTableName(tableName: string): string {
   const lastDot = tableName.lastIndexOf(".");
   return lastDot >= 0 ? tableName.substring(lastDot + 1) : tableName;
}

// Dialects whose identifier quote character is a backtick; everything else uses
// the SQL-standard double quote. Keyed by Malloy `dialectName`. The control
// plane encodes the same fact keyed by connection type
// (`PhysicalTableName.BACKTICK_TYPES` = {bigquery, mysql, databricks}); the two
// must stay byte-compatible. See the conformance table in quoting.spec.ts.
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

/**
 * Whether a table path already carries a dialect quote character, in which case
 * it is treated as canonical SQL that must not be re-quoted. The single
 * definition of "already quoted" shared by every bind site.
 */
export function isQuotedIdentifierPath(tableName: string): boolean {
   return tableName.includes('"') || tableName.includes("`");
}

/**
 * Quote a physical table path for a manifest entry so a Malloy `FROM` resolves
 * it correctly, unless it is already canonical SQL (passed through unchanged).
 * This is the single quoting authority for the two places a physical name is
 * bound into a `FROM`: the serve-side bind ({@link Package.quoteBoundTableNames})
 * and the build-side manifest that stitches chained persist sources together
 * (materialization_service). Both must mirror the CREATE side's
 * {@link quoteTablePath} so a case-folding engine (Snowflake uppercases
 * unquoted identifiers) can resolve the case-preserved table the builder wrote.
 *
 * Passing an already-quoted name through is safe for the names this handles:
 * control-plane physical names are logical/unquoted (never contain a quote),
 * and a self-assigned `#@ persist name=` value is the author's own canonical
 * SQL (they own quoting it for the dialect).
 */
export function quoteManifestTablePath(
   tableName: string,
   dialect: string,
): string {
   return isQuotedIdentifierPath(tableName)
      ? tableName
      : quoteTablePath(tableName, dialect);
}
