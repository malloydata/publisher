// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Names caller and author SQL may not use on a Publisher DuckDB connection:
 * the functions that read the instance's own attachments, secrets and
 * settings, the postgres extension's functions that reach an attached
 * database past the tables a query reads (or open a connection of their own),
 * and the metadata catalog DuckLake attaches beside each catalog. None is
 * something a model or a raw query needs: Malloy reads tables, and Publisher's
 * own reads of these run as internal statements (duckdb_lockdown.ts).
 *
 * Checked against DuckDB 1.5.5's function catalog with postgres, ducklake,
 * httpfs and json loaded. Matched as words, anywhere in the text, as
 * the lockdown's other refused names are: none is a plausible column or table
 * name.
 */
export const INTROSPECTION_NAMES: ReadonlySet<string> = new Set([
   "duckdb_databases",
   "database_list",
   "duckdb_secrets",
   "which_secret",
   "duckdb_settings",
   "postgres_query",
   "postgres_scan",
   "postgres_scan_pushdown",
   "postgres_execute",
   "postgres_attach",
   "postgres_configure_pool",
]);

/** The prefix of the metadata catalog DuckLake attaches for each catalog. */
export const DUCKLAKE_METADATA_PREFIX = "__ducklake_metadata_";

/** Whether `word` (lowercased) is a refused introspection name. */
export function isIntrospectionName(word: string): boolean {
   return (
      INTROSPECTION_NAMES.has(word) || word.startsWith(DUCKLAKE_METADATA_PREFIX)
   );
}

/**
 * The first refused introspection name in `sql`, read as raw words with no
 * regard to comments, quotes or strings (so quoting a name hides nothing), or
 * undefined.
 */
export function introspectionNameIn(sql: string): string | undefined {
   for (const word of sql.toLowerCase().match(/[a-z0-9_$\u0080-￿]+/g) ?? []) {
      if (isIntrospectionName(word)) return word;
   }
   return undefined;
}
