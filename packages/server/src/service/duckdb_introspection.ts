// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Names caller and author SQL may not use on a Publisher DuckDB connection:
 * the functions that read the instance's own attachments, secrets, settings,
 * variables and pragmas, or load the host's cloud credentials, the postgres extension's functions that reach an
 * attached database past the tables a query reads (or open a connection of
 * their own), DuckLake's own functions (its settings, files, snapshots and
 * maintenance), and the metadata catalog DuckLake attaches beside each
 * catalog. None is something a model or a raw query needs: Malloy reads
 * tables, and Publisher's own reads of these run as internal statements
 * (duckdb_lockdown.ts).
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
   "duckdb_variables",
   "current_setting",
   "getvariable",
   "load_aws_credentials",
   "postgres_query",
   "postgres_scan",
   "postgres_scan_pushdown",
   "postgres_execute",
   "postgres_attach",
   "postgres_configure_pool",
   // DuckLake's own functions, as DuckDB 1.5.5 registers them: refused as
   // words anywhere, and every other `ducklake_` name as a call
   // ({@link DUCKLAKE_FUNCTION_PREFIX}).
   "ducklake_add_data_files",
   "ducklake_cleanup_old_files",
   "ducklake_commit",
   "ducklake_current_snapshot",
   "ducklake_delete_orphaned_files",
   "ducklake_expire_snapshots",
   "ducklake_flush_inlined_data",
   "ducklake_last_committed_snapshot",
   "ducklake_list_files",
   "ducklake_merge_adjacent_files",
   "ducklake_options",
   "ducklake_rewrite_data_files",
   "ducklake_scan",
   "ducklake_set_commit_message",
   "ducklake_set_option",
   "ducklake_settings",
   "ducklake_snapshots",
   "ducklake_table_changes",
   "ducklake_table_deletions",
   "ducklake_table_info",
   "ducklake_table_insertions",
]);

/**
 * Prefixes refused wherever a word starts with them: DuckLake's metadata
 * catalogs, and DuckDB's `pragma_` table functions.
 */
export const INTROSPECTION_PREFIXES: readonly string[] = [
   "__ducklake_metadata_",
   "pragma_",
];

/**
 * The prefix of DuckLake's functions, refused wherever a name with it is
 * called. Only as a call: a catalog or table may well be named `ducklake_…`.
 */
export const DUCKLAKE_FUNCTION_PREFIX = "ducklake_";

/** Whether `word` (lowercased) is a refused introspection name. */
export function isIntrospectionName(word: string): boolean {
   return (
      INTROSPECTION_NAMES.has(word) ||
      INTROSPECTION_PREFIXES.some((prefix) => word.startsWith(prefix))
   );
}

/** Whether calling `name` (lowercased) is refused. */
export function isIntrospectionCall(name: string): boolean {
   return (
      isIntrospectionName(name) || name.startsWith(DUCKLAKE_FUNCTION_PREFIX)
   );
}

/**
 * The first refused introspection name in `sql`, read as raw words with no
 * regard to comments, quotes or strings (so quoting a name hides nothing),
 * then any `ducklake_` name written as a call; or undefined. Lexical: DuckDB's
 * own parse ({@link introspectionCallInParse}) catches a call this misses.
 */
export function introspectionNameIn(sql: string): string | undefined {
   const lower = sql.toLowerCase();
   for (const word of lower.match(/[a-z0-9_$\u0080-\uffff]+/g) ?? []) {
      if (isIntrospectionName(word)) return word;
   }
   const call = /(ducklake_[a-z0-9_$]*)"?\s*\(/.exec(lower);
   return call ? call[1] : undefined;
}

/**
 * The first refused name in DuckDB's serialized parse of a statement
 * (`json_serialize_sql`): a function it calls, or a table or catalog it
 * names. Undefined when there is none.
 */
export function introspectionCallInParse(parsed: unknown): string | undefined {
   let found: string | undefined;
   const walk = (node: unknown): void => {
      if (found || node === null || typeof node !== "object") return;
      if (Array.isArray(node)) {
         node.forEach(walk);
         return;
      }
      for (const [key, value] of Object.entries(node)) {
         if (typeof value === "string") {
            const name = value.toLowerCase();
            if (
               (key === "function_name" && isIntrospectionCall(name)) ||
               ((key === "table_name" || key === "catalog_name") &&
                  isIntrospectionName(name))
            ) {
               found = name;
               return;
            }
         }
         walk(value);
      }
   };
   walk(parsed);
   return found;
}
