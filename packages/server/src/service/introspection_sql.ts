import { Connection } from "@malloydata/malloy";
import { logger } from "../logger";

/**
 * Dialects where a backslash escapes inside a single-quoted string.
 *
 * Taken from Malloy's own dialect layer (`stringLiteralStyle`, EscapeStyle
 * Backslash vs Doubled), not from memory: BigQuery, Snowflake, MySQL and
 * Databricks are Backslash; Postgres, DuckDB, MotherDuck, DuckLake and Trino
 * are Doubled.
 *
 * Session settings do not move these in our deployments. The Malloy Postgres
 * driver never touches standard_conforming_strings (on by default since 9.1),
 * the MySQL driver only strips ONLY_FULL_GROUP_BY from sql_mode, the Databricks
 * driver only sets the time zone, and Snowflake has no parameter that disables
 * backslash escapes.
 */
const BACKSLASH_ESCAPE_DIALECTS = new Set([
   "bigquery",
   "databricks",
   "mysql",
   "snowflake",
]);

/**
 * Escape a value for a single-quoted SQL string literal, for the dialect of the
 * connection it will run against.
 *
 * Takes the CONNECTION TYPE rather than a boolean on purpose. The boolean form
 * was set wrong twice in successive rounds of review: first applied to every
 * dialect (which corrupts a legitimate `data\archive` on Postgres, where a
 * backslash is an ordinary character), then applied only to MySQL (which leaves
 * Snowflake and Databricks open, since they escape with a backslash too). A call
 * site that passes the connection it already has cannot express the wrong
 * answer.
 *
 * Quote-doubling is applied for every dialect; it is valid on the backslash
 * dialects as well, so the backslash handling is purely additive.
 *
 * Lives here rather than in db_utils because gcs_s3_utils needs it too, and
 * gcs_s3_utils cannot import db_utils: db_utils imports IT, so that is a cycle.
 */
export function sqlLiteral(value: string, connectionType?: string): string {
   const escaped = BACKSLASH_ESCAPE_DIALECTS.has(
      (connectionType ?? "").toLowerCase(),
   )
      ? value.replace(/\\/g, "\\\\")
      : value;
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
