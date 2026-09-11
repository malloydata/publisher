// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The bare (unqualified) name of a possibly container-qualified table path:
 * the segment after the last dot, e.g. `my_schema.my_table` -> `my_table` and
 * `my_table` -> `my_table`.
 *
 * Not a rename target on its own: whether a bare target keeps the table in its
 * own container or moves it to the session's is dialect-specific, which is what
 * {@link quoteRenameTarget} decides.
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
 * Dialects whose `ALTER TABLE ... RENAME TO` target must be QUALIFIED, because a
 * bare one is either resolved against the session's current container -- moving
 * the table -- or is resolved that way by some of the dialect's backends. A
 * qualified target naming the table's own container is a no-op for placement, so
 * this set is safe to widen and costly to leave narrow: the failure it prevents
 * is a silent write to the wrong container.
 *
 * Measured, one table per dialect, by renaming `s1.t_staging` to a bare `t` with
 * the session pointed at `s2`:
 *
 * | dialect   | bare target        | qualified target      |
 * | --------- | ------------------ | --------------------- |
 * | snowflake | moves to s2        | accepted, correct     |
 * | mysql     | moves to s2        | accepted, correct     |
 * | postgres  | stays in s1        | syntax error          |
 * | duckdb    | stays in s1        | parser error          |
 * | bigquery  | stays in s1        | "cannot contain dot"  |
 * | trino     | stays in s1 (*)    | accepted, correct     |
 *
 * (*) Trino was measured against the `memory` connector, which ignores the
 * target's container. That does not generalize: the engine's `RenameTableTask`
 * fills a missing schema from the session, and connectors differ on whether they
 * permit a cross-schema rename at all. Since Trino accepts a qualified target and
 * a real deployment never runs on `memory`, it is qualified here rather than
 * relying on one connector's behaviour.
 *
 * The three that reject a qualified target are why this cannot be unconditional.
 * Databricks is not covered by the sweep and stays out, keeping its current
 * behaviour; add it once measured.
 */
const QUALIFIED_RENAME_TARGET_DIALECTS = new Set([
   "snowflake",
   "mysql",
   "trino",
]);

/**
 * Dialect-quote the TARGET of an `ALTER TABLE ... RENAME TO` for a (possibly
 * container-qualified) logical table path: the full path on the dialects that
 * resolve a bare target against the session, and the bare name -- which names
 * the table within its existing container -- on those that require it.
 */
export function quoteRenameTarget(tableName: string, dialect: string): string {
   return QUALIFIED_RENAME_TARGET_DIALECTS.has(dialect)
      ? quoteTablePath(tableName, dialect)
      : quoteIdentifier(bareTableName(tableName), dialect);
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
