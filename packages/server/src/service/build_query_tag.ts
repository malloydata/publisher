import { sqlLiteral } from "./db_utils";
import type { QueryMetadata } from "./query_metadata";

/**
 * Render a build's query-metadata bag into each warehouse's own tagging grammar,
 * for the `storage=` build path.
 *
 * The Malloy connectors render this bag for every query they issue, but a
 * `storage=` build reads through DuckDB's native query-passthrough, so no
 * connector is in the call path. These functions are the passthrough's
 * equivalent, deliberately matching what the connectors emit so one deployment's
 * query history reads the same whichever path produced the statement.
 *
 * Neither connector exports its renderer — `@malloydata/db-snowflake` exports
 * `SnowflakeConnection` and `buildPoolOptions`, `@malloydata/db-bigquery` exports
 * `BigQueryConnection` — and reaching into either package's `dist/` is not a
 * dependency this repo takes anywhere else. So the grammars are reimplemented
 * here, and pinned by tests that state the grammar rather than compare against
 * an import that cannot be made.
 *
 * Pinned to @malloydata/malloy 0.0.427.
 */

/**
 * Snowflake's `QUERY_TAG` ceiling. The bag arrives already bounded to this on
 * its serialized size ({@link ./query_metadata}'s own budget, which sheds whole
 * properties to fit), so this is a backstop rather than a working limit.
 */
const MAX_QUERY_TAG_LENGTH = 2000;

/** BigQuery label keys and values are at most 63 characters. */
const BQ_MAX_LEN = 63;

/**
 * The bag as a Snowflake `QUERY_TAG` value: JSON, so the properties stay
 * queryable in `QUERY_HISTORY` / `QUERY_ATTRIBUTION_HISTORY` rather than
 * arriving as one opaque string. Case is preserved.
 *
 * Undefined for an empty bag, and undefined rather than TRUNCATED for an
 * over-long one: slicing JSON produces a tag that no consumer can parse, which
 * is indistinguishable from a bug at the point someone tries to read it. An
 * absent tag at least reads as absent. (db-snowflake slices; it can afford to,
 * because the bag it receives was bounded upstream by the same budget that
 * bounds this one.)
 */
export function snowflakeQueryTagValue(
   metadata: QueryMetadata | undefined,
): string | undefined {
   if (metadata === undefined || Object.keys(metadata).length === 0) {
      return undefined;
   }
   const tag = JSON.stringify(metadata);
   return tag.length > MAX_QUERY_TAG_LENGTH ? undefined : tag;
}

/**
 * The statement that tags every subsequent read on this build's Snowflake
 * session, or undefined when there is nothing to tag.
 *
 * Session-scoped rather than per-statement because the passthrough exposes no
 * per-call parameter map: `snowflake_query()` takes SQL and a secret and nothing
 * else. The passthrough reuses ONE session across calls, which is what makes a
 * session-level tag reach the build's read at all.
 *
 * <b>Escaped with {@link sqlLiteral} for the snowflake dialect, not by doubling
 * quotes.</b> A property value may contain a backslash — the contract admits
 * printable ASCII except `"` — and JSON renders one as `\\`. Snowflake treats
 * backslash as an escape inside a single-quoted literal, so quote-doubling alone
 * delivers `\` where the JSON needed `\\`, and the stored tag no longer parses:
 * unqueryable in `QUERY_HISTORY`, which is the entire purpose of tagging, and
 * silent because the statement itself succeeds.
 */
export function snowflakeSetQueryTagSQL(
   metadata: QueryMetadata | undefined,
   secretName: string,
): string | undefined {
   const tag = snowflakeQueryTagValue(metadata);
   if (tag === undefined) return undefined;
   const inner = `ALTER SESSION SET QUERY_TAG = '${sqlLiteral(tag, "snowflake")}'`;
   // The outer literal is parsed by DuckDB, which has no backslash escape, so
   // the passthrough's own quote-doubling is the correct level here.
   return `SELECT * FROM snowflake_query('${inner.replace(/'/g, "''")}', '${secretName.replace(/'/g, "''")}')`;
}

/** BigQuery's label grammar: lowercase, `[a-z0-9_-]`, at most 63 characters. */
function sanitizeBigQueryValue(value: string): string {
   return value
      .toLowerCase()
      .replace(/[^a-z0-9_-]/g, "_")
      .slice(0, BQ_MAX_LEN);
}

/** A key that cannot be made to start with a lowercase letter is dropped. */
function sanitizeBigQueryKey(key: string): string | undefined {
   const sanitized = sanitizeBigQueryValue(key);
   return /^[a-z]/.test(sanitized) ? sanitized : undefined;
}

/**
 * The bag as a value for BigQuery's `@@query_label` system variable:
 * `key:value` pairs joined by commas, which is the form that variable takes.
 * The job's `configuration.labels` then carries them as a map.
 *
 * The separators need no escaping, which is a property of the grammar rather
 * than luck: `,` and `:` are both outside `[a-z0-9_-]`, so sanitizing every key
 * and value removes any that a property could have contributed. The same
 * substitution removes `'` and `\`, so this value cannot disturb the SQL literal
 * it is embedded in either.
 *
 * Undefined when the bag is empty, or when every key was dropped as unusable.
 */
export function bigQueryQueryLabelValue(
   metadata: QueryMetadata | undefined,
): string | undefined {
   if (metadata === undefined) return undefined;
   // Keyed rather than appended, because sanitizing can collide two properties
   // onto one label key and BigQuery REFUSES a list containing a duplicate: the
   // statement errors, the script fails, and the build dies. The bag admits that
   // collision legally — property names are validated case-PRESERVING, so `Team`
   // and `team` are two properties that both render as `team`. Last wins, which
   // is the shape every other drop in this rendering already takes.
   const rendered = new Map<string, string>();
   for (const [key, value] of Object.entries(metadata)) {
      const sanitizedKey = sanitizeBigQueryKey(key);
      if (sanitizedKey === undefined) continue;
      rendered.set(sanitizedKey, sanitizeBigQueryValue(value));
   }
   if (rendered.size === 0) return undefined;
   return [...rendered].map(([key, value]) => `${key}:${value}`).join(",");
}
