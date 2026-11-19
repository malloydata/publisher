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
   // DuckDB's own pg_catalog views over a refused function: pg_settings
   // reads duckdb_settings(), pg_database duckdb_databases(), and
   // pg_prepared_statements the connection's prepared statements (with
   // duckdb_prepared_statements, the function it reads). DuckDB 1.5.5's other
   // pg_catalog and information_schema views read only catalog names and
   // types (duckdb_tables, duckdb_columns, duckdb_schemas and their kind).
   "pg_settings",
   "pg_database",
   "pg_prepared_statements",
   "duckdb_prepared_statements",
   // Listings of the instance's own files: the remote files its file cache
   // holds, its spill files, and its extensions with their install paths.
   "duckdb_external_file_cache",
   "duckdb_temporary_files",
   "duckdb_extensions",
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

/**
 * Names that reach DuckDB's logging and profiling, which DuckDB 1.5.5's
 * `lock_configuration` does not cover (a locked session could otherwise log
 * other callers' query text and the signed request headers of the instance's
 * own reads, and read them back), or that run SQL taken from text the gate
 * cannot see through (`query`, `query_table`, `json_execute_serialized_sql`,
 * `import_database`). Checked against `duckdb_functions()` on 1.5.5 with
 * json, icu, excel, ducklake and httpfs loaded.
 *
 * Refused wherever the name appears as a word (in code, a comment, a string
 * literal, at any depth): none is a plausible column or table name, so the
 * gate does not need to know how DuckDB would have read it. One list for the
 * gate on a locked instance and for the load-time author SQL check
 * (duckdb_lockdown.ts), which must refuse them too: a build session runs
 * author SQL before any lock, and a name assembled from text at run time
 * (`query_table('a' || 'b')`) is one no lexical check could see. `query` is the
 * exception, refused only where it is called ({@link CALL_ONLY_NAMES}).
 */
export const STATEMENT_REFUSED_NAMES: ReadonlySet<string> = new Set([
   "enable_logging",
   "disable_logging",
   "truncate_duckdb_logs",
   "duckdb_logs",
   "duckdb_logs_parsed",
   "duckdb_log_contexts",
   "write_log",
   "parse_duckdb_log_message",
   "enable_profiling",
   "disable_profiling",
   "enable_profile",
   "disable_profile",
   "all_profiling_output",
   "duckdb_profiling_settings",
   "query_table",
   "json_execute_serialized_sql",
   "import_database",
]);

/** Refused only as a call: a column or alias named `query` is common. */
export const CALL_ONLY_NAMES: ReadonlySet<string> = new Set(["query"]);

/**
 * Whether `schema` (lowercased) is one of the schemas Postgres keeps its own
 * catalog in: `pg_catalog`, `information_schema`, `pg_toast`, and the
 * per-session `pg_temp_N` and `pg_toast_temp_N`. Postgres reserves the `pg_`
 * prefix for these, so no user schema shares a name with one.
 */
export function isPostgresSystemSchema(schema: string): boolean {
   return (
      schema === "information_schema" ||
      schema === "pg_catalog" ||
      schema === "pg_toast" ||
      /^pg_(toast_)?temp(_\d+)?$/.test(schema)
   );
}

/**
 * Whether `catalog.schema` (lowercased) names a Postgres system schema of an
 * attached database: an attached Postgres alias or a DuckLake catalog's
 * metadata alias. DuckDB's own `information_schema` and `pg_catalog` live in
 * its `system` catalog, which no attachment may be named, and are read
 * unqualified or as `system.<schema>`; every other catalog holding one of
 * these schemas is an attached database.
 */
export function isAttachedSystemSchema(
   catalog: string,
   schema: string,
): boolean {
   return (
      catalog !== "" && catalog !== "system" && isPostgresSystemSchema(schema)
   );
}

/** One part of SQL as {@link attachedSystemSchemaInParts} reads it. */
export type NamePart = { kind: "word" | "dot" | "other"; value: string };

/**
 * The first `catalog.schema` in `parts` that names an attached database's
 * Postgres system schema ({@link isAttachedSystemSchema}): a word, a dot and
 * a word, wherever they stand. Undefined when there is none.
 */
export function attachedSystemSchemaInParts(
   parts: readonly NamePart[],
): string | undefined {
   for (let i = 0; i + 2 < parts.length; i++) {
      const [catalog, dot, schema] = [parts[i], parts[i + 1], parts[i + 2]];
      if (
         catalog.kind === "word" &&
         dot.kind === "dot" &&
         schema.kind === "word" &&
         isAttachedSystemSchema(catalog.value, schema.value)
      ) {
         return `${catalog.value}.${schema.value}`;
      }
   }
   return undefined;
}

/**
 * The first attached Postgres system schema `sql` names as `catalog.schema`
 * (`pg.pg_catalog.pg_settings`, `"PG"."Information_Schema".tables`), read as
 * raw text: double quotes are dropped and string contents read as code, so
 * quoting a name hides nothing. Lexical; DuckDB's own parse
 * ({@link introspectionCallInParse}) catches a reference this misses.
 */
export function attachedSystemSchemaIn(sql: string): string | undefined {
   const lower = sql.toLowerCase();
   const parts: NamePart[] = [];
   // One pass, linear in the text: no backtracking pattern runs on caller SQL.
   for (const match of lower.matchAll(/[a-z0-9_$\u0080-\uffff]+|[^\s"]/g)) {
      const text = match[0];
      parts.push(
         text === "."
            ? { kind: "dot", value: text }
            : /^[a-z0-9_$\u0080-\uffff]/.test(text)
              ? { kind: "word", value: text }
              : { kind: "other", value: text },
      );
   }
   return attachedSystemSchemaInParts(parts);
}

/**
 * The attached Postgres system schema a node of DuckDB's serialized parse
 * reads (a table reference, or a `SHOW`/`DESCRIBE` of one), as
 * `catalog.schema`, or undefined.
 */
export function attachedSystemSchemaOfNode(
   node: Record<string, unknown>,
): string | undefined {
   const { catalog_name: catalog, schema_name: schema } = node;
   if (typeof catalog !== "string" || typeof schema !== "string") {
      return undefined;
   }
   const [c, s] = [catalog.toLowerCase(), schema.toLowerCase()];
   return isAttachedSystemSchema(c, s) ? `${c}.${s}` : undefined;
}

/** Whether `word` (lowercased) is a refused introspection name. */
export function isIntrospectionName(word: string): boolean {
   return (
      INTROSPECTION_NAMES.has(word) ||
      STATEMENT_REFUSED_NAMES.has(word) ||
      INTROSPECTION_PREFIXES.some((prefix) => word.startsWith(prefix))
   );
}

/** Whether calling `name` (lowercased) is refused. */
export function isIntrospectionCall(name: string): boolean {
   return (
      isIntrospectionName(name) ||
      CALL_ONLY_NAMES.has(name) ||
      name.startsWith(DUCKLAKE_FUNCTION_PREFIX)
   );
}

/**
 * The first refused introspection name in `sql`, read as raw words with no
 * regard to comments, quotes or strings (so quoting a name hides nothing),
 * then any `ducklake_` name or `query` written as a call, then an attached database's
 * Postgres system schema ({@link attachedSystemSchemaIn}); or undefined.
 * Lexical: DuckDB's own parse ({@link introspectionCallInParse}) catches a
 * call or a reference this misses.
 */
export function introspectionNameIn(sql: string): string | undefined {
   const lower = sql.toLowerCase();
   let call: string | undefined;
   // One pass over the words, linear in the text: no backtracking pattern
   // runs on caller SQL.
   for (const match of lower.matchAll(/[a-z0-9_$\u0080-\uffff]+/g)) {
      const word = match[0];
      if (isIntrospectionName(word)) return word;
      if (
         call === undefined &&
         (word.startsWith(DUCKLAKE_FUNCTION_PREFIX) ||
            CALL_ONLY_NAMES.has(word))
      ) {
         let i = (match.index ?? 0) + word.length;
         if (lower[i] === '"') i++;
         while (i < lower.length && /\s/.test(lower[i])) i++;
         if (lower[i] === "(") call = word;
      }
   }
   return call ?? attachedSystemSchemaIn(sql);
}

/**
 * The first refused name in DuckDB's serialized parse of a statement
 * (`json_serialize_sql`): a function it calls, a table or catalog it names,
 * or an attached database's Postgres system schema it reads (as
 * `catalog.schema`). Undefined when there is none.
 */
export function introspectionCallInParse(parsed: unknown): string | undefined {
   let found: string | undefined;
   const walk = (node: unknown): void => {
      if (found || node === null || typeof node !== "object") return;
      if (Array.isArray(node)) {
         node.forEach(walk);
         return;
      }
      found = attachedSystemSchemaOfNode(node as Record<string, unknown>);
      if (found) return;
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
