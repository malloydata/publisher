import { Connection } from "@malloydata/malloy";
import { logger } from "../logger";

/**
 * Dialects where a backslash escapes inside a single-quoted string, AND which
 * also accept `''` as an escaped quote.
 *
 * Taken from Malloy's own dialect layer (`stringLiteralStyle`, EscapeStyle
 * Backslash vs Doubled), not from memory: BigQuery, Snowflake, MySQL and
 * Databricks are Backslash; Postgres, DuckDB, MotherDuck, DuckLake and Trino
 * are Doubled. BigQuery is deliberately absent here; see
 * UNSUPPORTED_LITERAL_DIALECTS below.
 *
 * Session settings do not move these in our deployments. The Malloy Postgres
 * driver never touches standard_conforming_strings (on by default since 9.1),
 * the MySQL driver only strips ONLY_FULL_GROUP_BY from sql_mode, the Databricks
 * driver only sets the time zone, and Snowflake has no parameter that disables
 * backslash escapes.
 */
const BACKSLASH_ESCAPE_DIALECTS = new Set(["databricks", "mysql", "snowflake"]);

/**
 * Dialects this function cannot produce a correct literal for.
 *
 * BigQuery is backslash-only: GoogleSQL does not treat `''` as an escaped
 * quote, and it does not concatenate adjacent string literals either, so the
 * doubling applied unconditionally below would not merely be redundant, it
 * would not parse. Every other escape rule here is additive on top of doubling,
 * which is why BigQuery cannot be expressed as a member of either set.
 *
 * It reaches nothing today: `getSchemasForBigQuery` and `listTablesForBigQuery`
 * both go through the @google-cloud/bigquery client and never build SQL. This
 * exists so that if a BigQuery SQL path is ever added, it throws on the first
 * call instead of silently mis-escaping, and so the gap reads as a decision
 * rather than an omission somebody closes by adding "bigquery" to the set
 * above. If you need it, teach sqlLiteral to skip doubling for backslash-only
 * dialects first.
 */
const UNSUPPORTED_LITERAL_DIALECTS = new Map([
   [
      "bigquery",
      "GoogleSQL does not accept '' as an escaped quote, so this function cannot build a correct BigQuery literal. BigQuery introspection goes through the @google-cloud/bigquery client instead of building SQL.",
   ],
]);

/** Dialects where `''` is the only escape for a quote. */
const DOUBLED_ESCAPE_DIALECTS = new Set([
   "ducklake",
   "duckdb",
   "motherduck",
   "postgres",
   "publisher",
   "trino",
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
 * Quote-doubling is applied for every dialect it supports; MySQL, Snowflake and
 * Databricks accept `''` as well as a backslash, so the backslash handling is
 * purely additive. BigQuery is the one dialect where that is untrue, and it
 * throws rather than being approximated.
 *
 * Lives here rather than in db_utils because gcs_s3_utils needs it too, and
 * gcs_s3_utils cannot import db_utils: db_utils imports IT, so that is a cycle.
 */
export function sqlLiteral(value: string, connectionType?: string): string {
   const dialect = (connectionType ?? "").toLowerCase();
   const unsupported = UNSUPPORTED_LITERAL_DIALECTS.get(dialect);
   if (unsupported) {
      throw new Error(
         `Cannot build a SQL literal for "${dialect}". ${unsupported}`,
      );
   }
   const backslash = BACKSLASH_ESCAPE_DIALECTS.has(dialect);
   // No type at all means the caller is not dialect-aware; doubling is the
   // conservative answer and matches the ANSI majority. A type that IS given
   // but unrecognised is the dangerous case: that is a dialect nobody
   // classified, and it is the one that throws.
   if (dialect && !backslash && !DOUBLED_ESCAPE_DIALECTS.has(dialect)) {
      // Fail LOUDLY on a dialect nobody classified. Both dispatch switches are
      // exhaustive today, so this is unreachable; it exists because the next
      // dialect added would otherwise inherit quote-doubling silently, and
      // doubling alone on a backslash dialect is exploitable rather than merely
      // cosmetic. That classification has now been wrong three times in this
      // file's history, so the failure direction is worth pinning.
      throw new Error(
         `Unclassified SQL dialect "${connectionType}": add it to BACKSLASH_ESCAPE_DIALECTS or DOUBLED_ESCAPE_DIALECTS in introspection_sql.ts before building SQL for it.`,
      );
   }
   const escaped = backslash ? value.replace(/\\/g, "\\\\") : value;
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
