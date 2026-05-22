/**
 * Helpers for the ad-hoc connection SQL endpoints
 * (`/environments/.../connections/.../sqlQuery` and the deprecated
 * `queryData` GET twin) used to bound how much data a single
 * connection query can pull into publisher memory.
 *
 * The Malloy connector layer is not a reliable memory bound here:
 *
 *  - `PostgresConnection.runSQL` ignores `RunSQLOptions.rowLimit`
 *    entirely (only `runSQLStream` honors it).
 *  - `DuckDBCommon.runSQL` reads the full result set, then does
 *    `result.slice(0, rowLimit)` after the fact, so a billion-row
 *    scan OOMs before the slice runs.
 *
 * Until Step 2 swaps the ad-hoc path over to `runSQLStream` with a
 * byte budget, the only way to push a real bound down to the
 * database is to rewrite the user's SQL to wrap it in a `LIMIT N`
 * subquery so the database itself stops producing rows.
 *
 * The wrapping is intentionally conservative: we only wrap SQL we
 * are confident is a single `SELECT` / `WITH ... SELECT`, and pass
 * everything else (DDL, `SHOW`, `DESCRIBE`, `EXPLAIN`, multi-
 * statement, anything containing the DuckDB connector's split
 * sentinel) through unchanged. Those paths get their byte bound
 * from Step 2.
 */

const DUCKDB_SPLIT_SENTINEL = "-- hack: split on this";

/**
 * Wrapper alias used in the rewritten SQL. Surfaced verbatim in DB
 * error messages, so name it for greppability when an operator is
 * staring at a stack trace and wondering where the subquery came
 * from.
 */
export const PUBLISHER_CAP_ALIAS = "__publisher_cap__";

/**
 * Strip leading whitespace, `--` line comments, and `/* ... *\/`
 * block comments. Returns the original string with the leading
 * comment/whitespace prefix removed; trailing comments inside the
 * statement body are left alone (we only care about classifying the
 * first real token).
 */
function stripLeadingCommentsAndWhitespace(sql: string): string {
   let i = 0;
   const n = sql.length;
   while (i < n) {
      const ch = sql.charCodeAt(i);
      // whitespace: space, tab, newline, carriage return, form feed, vtab
      if (
         ch === 0x20 ||
         ch === 0x09 ||
         ch === 0x0a ||
         ch === 0x0d ||
         ch === 0x0b ||
         ch === 0x0c
      ) {
         i += 1;
         continue;
      }
      // `--` line comment runs to end-of-line
      if (ch === 0x2d /* - */ && sql.charCodeAt(i + 1) === 0x2d) {
         const nl = sql.indexOf("\n", i + 2);
         if (nl === -1) return "";
         i = nl + 1;
         continue;
      }
      // `/* ... */` block comment (non-nesting; matches Postgres / DuckDB /
      // BigQuery / Snowflake all of which treat /* */ as non-nested for
      // classification purposes; we don't need to evaluate the SQL).
      if (ch === 0x2f /* / */ && sql.charCodeAt(i + 1) === 0x2a /* * */) {
         const end = sql.indexOf("*/", i + 2);
         if (end === -1) return "";
         i = end + 2;
         continue;
      }
      break;
   }
   return sql.slice(i);
}

/**
 * Scan `sql` for the first unquoted `;`. Returns the index, or -1
 * if there is none. Handles single-quoted strings ('...' with ''
 * escapes), double-quoted identifiers ("..."), and Postgres-style
 * dollar-quoted strings ($tag$...$tag$).
 *
 * We deliberately do NOT try to skip comments inside the body: a
 * comment containing `;` is harmless for our purposes (we only need
 * to detect *real* statement separators, and any `;` we find inside
 * a comment just makes us conservatively refuse to wrap, which is
 * the safe direction).
 */
function indexOfUnquotedSemicolon(sql: string): number {
   const n = sql.length;
   let i = 0;
   while (i < n) {
      const ch = sql[i];
      if (ch === "'") {
         i += 1;
         while (i < n) {
            if (sql[i] === "'") {
               if (sql[i + 1] === "'") {
                  i += 2;
                  continue;
               }
               i += 1;
               break;
            }
            i += 1;
         }
         continue;
      }
      if (ch === '"') {
         i += 1;
         while (i < n && sql[i] !== '"') i += 1;
         i += 1;
         continue;
      }
      if (ch === "$") {
         // Postgres dollar-quoted string: $tag$ ... $tag$ where tag is
         // [A-Za-z_][A-Za-z0-9_]* (or empty for $$ ... $$).
         const tagMatch = /^\$([A-Za-z_][A-Za-z0-9_]*)?\$/.exec(sql.slice(i));
         if (tagMatch) {
            const tag = tagMatch[0];
            const close = sql.indexOf(tag, i + tag.length);
            if (close === -1) return -1;
            i = close + tag.length;
            continue;
         }
      }
      if (ch === ";") return i;
      i += 1;
   }
   return -1;
}

/**
 * True iff `sql` is a single, simple SELECT / WITH statement that
 * we are confident can be wrapped in a `SELECT * FROM (...) LIMIT
 * N` subquery without changing semantics or breaking on the
 * dialect.
 *
 * Conservative on purpose: any false negative just means the
 * statement is passed through unchanged (and its byte size will be
 * bounded by Step 2 instead).
 */
export function isCappableSelect(sql: string): boolean {
   if (typeof sql !== "string" || sql.length === 0) return false;

   // DuckDB connector splits on this sentinel and runs the pieces
   // sequentially; wrapping the whole blob in a single subquery would
   // turn N statements into invalid SQL.
   if (sql.includes(DUCKDB_SPLIT_SENTINEL)) return false;

   const trimmed = stripLeadingCommentsAndWhitespace(sql);
   if (trimmed.length === 0) return false;

   // First token must be SELECT or WITH (case-insensitive). We don't
   // try to classify SELECT-vs-VALUES-vs-TABLE etc; the latter are
   // rare in ad-hoc REST callers and pass-through is the safe default.
   const firstToken = /^[A-Za-z]+/.exec(trimmed)?.[0]?.toUpperCase();
   if (firstToken !== "SELECT" && firstToken !== "WITH") return false;

   // Reject anything with a statement separator followed by more SQL.
   // A trailing `;` (optionally followed by whitespace / comments) is
   // fine and common — strip it before wrapping. Anything else means
   // multi-statement and we refuse to wrap.
   const semi = indexOfUnquotedSemicolon(trimmed);
   if (semi !== -1) {
      const tail = stripLeadingCommentsAndWhitespace(trimmed.slice(semi + 1));
      if (tail.length > 0) return false;
   }

   return true;
}

/**
 * Wrap `sql` in `SELECT * FROM (<sql>) AS __publisher_cap__ LIMIT
 * <limit>`. Strips a single trailing `;` (plus any trailing
 * whitespace / comments) so the wrapped statement parses cleanly on
 * every dialect — Postgres and DuckDB will both reject
 * `SELECT * FROM (SELECT 1;) ...`.
 *
 * Caller is responsible for first checking {@link isCappableSelect}.
 * This function does not validate.
 */
export function wrapWithRowLimit(sql: string, limit: number): string {
   // Defense-in-depth against type-confusion: every operation below
   // (`indexOfUnquotedSemicolon`, `slice`, template-literal interpolation)
   // has subtly different semantics on arrays vs strings, and a non-string
   // `sql` would let a multi-statement payload slip through without ever
   // hitting our `;`-detection. Callers are expected to validate first
   // (and `isCappableSelect` already does), but we re-check here so this
   // helper is safe by contract.
   if (typeof sql !== "string") {
      throw new TypeError(
         `wrapWithRowLimit: sql must be a string (got ${typeof sql})`,
      );
   }
   if (!Number.isInteger(limit) || limit < 0) {
      throw new RangeError(
         `wrapWithRowLimit: limit must be a non-negative integer (got ${limit})`,
      );
   }
   let body = sql;
   const semi = indexOfUnquotedSemicolon(body);
   if (semi !== -1) {
      // isCappableSelect has already verified that anything after the
      // `;` is whitespace / comments only, so it's safe to drop the
      // whole tail.
      body = body.slice(0, semi);
   }
   return `SELECT * FROM (${body}) AS ${PUBLISHER_CAP_ALIAS} LIMIT ${limit}`;
}
