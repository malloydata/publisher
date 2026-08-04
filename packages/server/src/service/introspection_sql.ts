import { Connection } from "@malloydata/malloy";
import { logger } from "../logger";

/**
 * Escape a value for a single-quoted SQL string literal.
 *
 * Lives here rather than in db_utils because gcs_s3_utils needs it too, and
 * gcs_s3_utils cannot import db_utils: db_utils imports IT, so that would be a
 * cycle. db_utils re-exports this name for its existing callers.
 *
 * Doubling the quote is the ANSI rule and is all Postgres (with the default
 * standard_conforming_strings), DuckDB, Trino, Snowflake and BigQuery need.
 * MySQL is the exception: it also treats a backslash as an escape unless
 * NO_BACKSLASH_ESCAPES is set, and the Malloy MySQL driver does not set it, so
 * its call sites pass backslashEscapes. Doing that everywhere would NOT be a
 * safe default: on the ANSI dialects a backslash is an ordinary character, so
 * doubling it corrupts a legitimate name (a Postgres schema `data\\archive`,
 * handed back exactly as this API returned it, would match nothing).
 */
export function sqlLiteral(value: string, backslashEscapes = false): string {
   const escaped = backslashEscapes ? value.replace(/\\/g, "\\\\") : value;
   return escaped.replace(/'/g, "''");
}

/**
 * Row cap for schema-introspection queries.
 *
 * These read metadata rather than user data, and one row is one COLUMN, so a
 * modest warehouse produces far more rows than any query default anticipates:
 * 500 tables averaging 20 columns is 10,000 rows.
 *
 * This exists because several Malloy drivers apply a small default when the
 * caller passes no `rowLimit`, and the defaults are not uniform:
 *
 * - DuckDB, BigQuery and Trino default to **10**.
 * - Postgres has a 1000-row default that its runner does not actually apply.
 * - Snowflake and MySQL apply no default at all.
 *
 * So on DuckDB, BigQuery and Trino an unlimited-looking introspection query was
 * silently truncated to ten rows. It did not fail, it returned a schema that
 * was quietly wrong: a DuckDB database with eight tables reported three, the
 * third missing most of its columns. On Snowflake and MySQL this cap is instead
 * a new upper bound where there was none, which is the safer direction.
 *
 * Any new introspection query must go through {@link runIntrospectionSQL}.
 */
export const INTROSPECTION_ROW_LIMIT = 100_000;

/**
 * Run a schema-introspection query with the cap above rather than whatever the
 * driver would default to, and warn when a result lands exactly on the cap,
 * which is the one case where the answer may still be truncated.
 *
 * Both result shapes are handled: drivers return either `{rows}` or a bare
 * array, and checking only the former would make the truncation warning silently
 * dead for the latter.
 */
export async function runIntrospectionSQL(
   malloyConnection: Connection,
   sql: string,
): Promise<Awaited<ReturnType<Connection["runSQL"]>>> {
   const result = await malloyConnection.runSQL(sql, {
      rowLimit: INTROSPECTION_ROW_LIMIT,
   });
   const rowCount = Array.isArray(result)
      ? result.length
      : (result?.rows?.length ?? 0);
   if (rowCount === INTROSPECTION_ROW_LIMIT) {
      logger.warn(
         "Schema introspection hit the row cap; the result may be truncated",
         { rowLimit: INTROSPECTION_ROW_LIMIT },
      );
   }
   return result;
}
