// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

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
 * Neither connector exports its renderer, so the grammars are reimplemented
 * here and pinned by tests that state the grammar.
 *
 * The Snowflake statement that sets the tag is warehouse_sql.ts's.
 *
 * Pinned to @malloydata/malloy 0.0.427.
 */

import { errorCodeOf } from "./error_code";
import { snowflakeSetQueryTagStatement } from "./warehouse_sql";
import type { components } from "../../../api";
import { logger } from "../../../logger";
import {
   mergeQueryMetadata,
   type QueryContext,
   type QueryMetadata,
} from "../../query_metadata";
import type { BuildEnvironment } from "./build_upstreams";

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
 * absent tag at least reads as absent.
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
 * The statement escapes the tag for Snowflake (see
 * snowflakeSetQueryTagStatement).
 */
export function snowflakeSetQueryTagSQL(
   metadata: QueryMetadata | undefined,
   secretName: string,
): string | undefined {
   const tag = snowflakeQueryTagValue(metadata);
   if (tag === undefined) return undefined;
   return snowflakeSetQueryTagStatement(tag, secretName);
}

/**
 * The session parameters Malloy's live Snowflake connection sets on every
 * session (`@malloydata/db-snowflake`, `SnowflakeExecutor._setSessionParams`):
 * case-sensitive quoted identifiers, UTC, and weeks starting on Sunday. A
 * `storage=` passthrough build reads Snowflake on a session of its own, which
 * otherwise runs in the account's settings: with
 * `QUOTED_IDENTIFIERS_IGNORE_CASE = TRUE` Malloy's quoted lower-case aliases
 * come back upper-case, and the account's `TIMEZONE` and `WEEK_START` move a
 * day or week boundary from live's. Kept in step with db-snowflake by hand;
 * its output-format settings are left out, since the passthrough reads typed
 * values, not text.
 */
const SNOWFLAKE_SESSION_PARAMETERS = [
   "ALTER SESSION SET QUOTED_IDENTIFIERS_IGNORE_CASE = FALSE",
   "ALTER SESSION SET TIMEZONE = 'UTC'",
   "ALTER SESSION SET WEEK_START = 7",
] as const;

/**
 * The settings a build session must hold before it reads, each with the
 * value Malloy's live session holds: a day or week bucket computed under
 * another is stored as another bucket than live answers.
 * `QUOTED_IDENTIFIERS_IGNORE_CASE` is not among them: the build stores each
 * column under its declared name whatever case the account returns.
 */
const SNOWFLAKE_REQUIRED_SETTINGS = [
   { name: "TIMEZONE", value: "UTC" },
   { name: "WEEK_START", value: "7" },
] as const;

/** `inner` run through the passthrough on `secretName`. */
function throughSnowflake(inner: string, secretName: string): string {
   return `SELECT * FROM snowflake_query('${inner.replace(/'/g, "''")}', '${secretName.replace(/'/g, "''")}')`;
}

/**
 * The statements that give a Snowflake build session Malloy's live session
 * parameters ({@link SNOWFLAKE_SESSION_PARAMETERS}), each through the
 * passthrough on `secretName`, the way the query tag is set.
 */
export function snowflakeSessionParitySQL(secretName: string): string[] {
   return SNOWFLAKE_SESSION_PARAMETERS.map((inner) =>
      throughSnowflake(inner, secretName),
   );
}

/**
 * The reads that check a Snowflake build session holds each required setting
 * ({@link SNOWFLAKE_REQUIRED_SETTINGS}), on the same handle the build reads
 * through: one `SHOW PARAMETERS … IN SESSION` each, with the value it must
 * hold.
 */
export function snowflakeSessionCheckSQL(
   secretName: string,
): { name: string; value: string; sql: string }[] {
   return SNOWFLAKE_REQUIRED_SETTINGS.map(({ name, value }) => ({
      name,
      value,
      sql: throughSnowflake(
         `SHOW PARAMETERS LIKE '${name}' IN SESSION`,
         secretName,
      ),
   }));
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

/**
 * What a build needs to tag its statements: the package-level metadata layer
 * and the run's context. Constant for the run; a source adds only its name.
 */
export interface BuildQueryMetadata {
   packageMaterialization:
      | components["schemas"]["PackageMaterializationConfig"]
      | null;
   context: QueryContext;
}

/**
 * A connection's two metadata layers: the overridable default and the
 * deployment-enforced properties. Fails open: an unreadable connection costs
 * the tags, never the statement.
 */
export function connectionMetadataLayers(
   environment: BuildEnvironment,
   connectionName: string,
): { default: QueryMetadata | null; enforced: QueryMetadata | null } {
   try {
      const connection = environment.getApiConnection(connectionName);
      return {
         default: connection?.queryMetadata ?? null,
         enforced: connection?.queryMetadataEnforced ?? null,
      };
   } catch (error) {
      // Logged because the lost enforced layer has no metric behind it.
      logger.debug("No query-metadata layers for connection", {
         connectionName,
         code: errorCodeOf(error),
      });
      return { default: null, enforced: null };
   }
}

/**
 * The query metadata a run's own statement on `connectionName` carries (an
 * ownership probe, a mark, a drop): the connection's layers under the run's
 * context. Fails open, as a build's does.
 */
export function statementRunOptions(
   environment: BuildEnvironment,
   connectionName: string,
   context: QueryContext,
): { queryMetadata?: QueryMetadata } {
   const layers = connectionMetadataLayers(environment, connectionName);
   const resolved = mergeQueryMetadata({
      connection: layers.default,
      enforced: layers.enforced,
      context,
   });
   return resolved.metadata ? { queryMetadata: resolved.metadata } : {};
}
