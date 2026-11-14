// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The segment after the last dot of a table path (`s.t` -> `t`). Not a rename
 * target on its own: whether a bare target stays in its container is
 * dialect-specific, which {@link quoteRenameTarget} decides.
 */
export function bareTableName(tableName: string): string {
   const lastDot = tableName.lastIndexOf(".");
   return lastDot >= 0 ? tableName.substring(lastDot + 1) : tableName;
}

// Keyed by Malloy `dialectName`; everything else uses double quotes. Must stay
// byte-compatible with the control plane's `PhysicalTableName.BACKTICK_TYPES`
// (keyed by connection type); see the conformance table in quoting.spec.ts.
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
 * Dialect-quote a table path for DDL, segment by segment, so names that need
 * quoting (e.g. hyphenated BigQuery project ids) work at any depth. The control
 * plane sends `physicalTableName` unquoted; quoting is the publisher's job.
 */
export function quoteTablePath(tableName: string, dialect: string): string {
   return tableName
      .split(".")
      .map((segment) => quoteIdentifier(segment, dialect))
      .join(".");
}

/**
 * Dialects whose `ALTER TABLE ... RENAME TO` target must be QUALIFIED, because a
 * bare one resolves (on some or all backends) against the session's container,
 * moving the table. Safe to widen, costly to leave narrow: the failure it
 * prevents is a silent write to the wrong container.
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
 * Dialect-quote the TARGET of an `ALTER TABLE ... RENAME TO`: the full path on
 * dialects that resolve a bare target against the session, the bare name
 * (within the existing container) on those that require it.
 */
export function quoteRenameTarget(tableName: string, dialect: string): string {
   return QUALIFIED_RENAME_TARGET_DIALECTS.has(dialect)
      ? quoteTablePath(tableName, dialect)
      : quoteIdentifier(bareTableName(tableName), dialect);
}

/**
 * Whether a table path already carries a quote character, i.e. is canonical
 * SQL that must not be re-quoted. Shared by every bind site.
 */
export function isQuotedIdentifierPath(tableName: string): boolean {
   return tableName.includes('"') || tableName.includes("`");
}

/**
 * Quote a physical table path for a manifest entry, unless already quoted. The
 * single quoting authority for both `FROM` bind sites (serve-side
 * {@link Package.quoteBoundTableNames} and the build-side chained-persist
 * manifest). Must mirror {@link quoteTablePath} so a case-folding engine
 * (Snowflake) resolves the case-preserved table the builder wrote.
 *
 * Pass-through is safe: control-plane names never contain a quote, and a
 * `#@ persist name=` value is the author's own canonical SQL.
 */
export function quoteManifestTablePath(
   tableName: string,
   dialect: string,
): string {
   return isQuotedIdentifierPath(tableName)
      ? tableName
      : quoteTablePath(tableName, dialect);
}
