// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A `storage=` build's Snowflake source, read through DuckDB's `snowflake`
 * extension, which reuses ONE session across `snowflake_query()` calls: the
 * session aligned with Malloy's live session settings and tagged with the
 * build's query metadata before the read, the tag's cost read back from the
 * session history after it, and the tag cleared before the session is
 * released. Read through passthrough_build.ts.
 *
 * The Malloy connector renders the metadata bag for every query it issues,
 * but the passthrough has no connector in its call path, so the bag is
 * rendered here as the connector renders it (it exports no renderer), pinned
 * by tests that state the grammar, so one deployment's query history reads
 * the same whichever path produced the statement. Pinned to
 * @malloydata/malloy 0.0.427. The statements that set the tag and read the
 * cost are warehouse_sql.ts's.
 *
 * Effects: SQL in Snowflake, through the passthrough on the build session;
 * logs and attribution-skipped counts.
 */

import { errorCodeOf } from "../../../../error_code";
import type { DuckDBConnection } from "@malloydata/db-duckdb";
import type { components } from "../../../../api";
import { logger } from "../../../../logger";
import { recordAttributionSkipped } from "../../../../materialization_metrics";
import { escapeSQL } from "../../../duckdb_session_setup";
import type { QueryMetadata } from "../../../query_metadata";
import {
   pickSnowflakeReadRow,
   resultRows,
   snowflakeReadCost,
   str,
   type BuildReadCost,
} from "../build_read_cost";
import {
   snowflakeCostSQL,
   snowflakeSetQueryTagStatement,
} from "../warehouse_sql";

type ApiConnection = components["schemas"]["Connection"];

/**
 * Snowflake's `QUERY_TAG` ceiling. The bag arrives already bounded to this on
 * its serialized size ({@link ./query_metadata}'s own budget, which sheds whole
 * properties to fit), so this is a backstop rather than a working limit.
 */
const MAX_QUERY_TAG_LENGTH = 2000;

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

/**
 * Give a Snowflake build session the session parameters Malloy's live
 * connection runs with (`snowflakeSessionParitySQL`), before any read, so the
 * stored rows are the rows live computes: the same UTC day and the same
 * Sunday week, and the same column-name case. Each is set the way the query
 * tag is; then `TIMEZONE` and `WEEK_START` are read back on the same handle
 * (`snowflakeSessionCheckSQL`), and a session that does not hold them fails
 * the build: its day and week buckets could differ from live's and be
 * served as exact. `QUOTED_IDENTIFIERS_IGNORE_CASE` is best-effort, logged
 * when refused: the build stores each column under its declared name either
 * way (`canonicalRenames`).
 */
async function alignSnowflakeSession(
   session: DuckDBConnection,
   handle: string,
): Promise<void> {
   for (const sql of snowflakeSessionParitySQL(handle)) {
      try {
         await session.runSQL(sql);
      } catch (error) {
         logger.warn(
            "Could not set a Malloy session setting on the Snowflake build session",
            {
               statement: sql,
               code: errorCodeOf(error),
            },
         );
      }
   }
   for (const check of snowflakeSessionCheckSQL(handle)) {
      let held: string | null = null;
      try {
         held = settingValueOf(await session.runSQL(check.sql), check.name);
      } catch (error) {
         throw new Error(
            `The Snowflake build session's ${check.name} could not be read back, so the build cannot be sure its buckets match live's: ${error instanceof Error ? error.message : String(error)}`,
         );
      }
      if (held !== check.value) {
         throw new Error(
            `The Snowflake build session holds ${check.name} = ${held ?? "(not reported)"}, not Malloy's ${check.value}, so its day and week buckets could differ from live's; the build is refused. Allow ALTER SESSION SET ${check.name} for the build's role.`,
         );
      }
   }
}

/** The `value` a `SHOW PARAMETERS` result reports for `name`, or null. */
function settingValueOf(result: unknown, name: string): string | null {
   for (const row of resultRows(result)) {
      const lower = Object.fromEntries(
         Object.entries(row).map(([k, v]) => [k.toLowerCase(), v]),
      );
      if (String(lower.key ?? "").toUpperCase() === name) {
         return str(lower.value);
      }
   }
   return null;
}

/**
 * Tag every subsequent read on a Snowflake build session with this build's
 * query metadata.
 *
 * Snowflake only: its passthrough reuses ONE session across `snowflake_query()`
 * calls, so a session-level `QUERY_TAG` reaches the following read. BigQuery
 * labels as part of issuing the read (see `issuePassthroughRead`);
 * Postgres has no tag to set.
 *
 * Best-effort: a refused tag costs this build's attribution, never the build.
 */
export async function tagSnowflakeSession(
   session: DuckDBConnection,
   handle: string,
   queryMetadata: QueryMetadata | undefined,
): Promise<void> {
   // Malloy's session settings first, so the build reads as live does.
   await alignSnowflakeSession(session, handle);
   const sql = snowflakeSetQueryTagSQL(queryMetadata, handle);
   if (sql === undefined) return;
   try {
      await session.runSQL(sql);
   } catch (error) {
      logger.warn("Could not tag the Snowflake build session", {
         code: errorCodeOf(error),
      });
      // Untagged, the read is unattributable in the customer's query history AND
      // unfindable by the cost lookup, which keys on that same tag.
      recordAttributionSkipped("tag_failed");
   }
}

/**
 * Ask Snowflake what the build's read just cost, scoped by the tag the build set
 * on its session. Scoped by tag rather than `LAST_QUERY_ID()`, because the ADBC
 * driver issues `SELECT 1` probes around each passthrough call and
 * `LAST_QUERY_ID()` returns a probe.
 *
 * Best-effort: the rows are already captured, so a failure costs a number,
 * never the build.
 */
export async function snowflakeReadCostAfterBuild(
   session: DuckDBConnection,
   handle: string,
   buildSQL: string,
   queryMetadata: QueryMetadata | undefined,
   /**
    * The source connection, whose configured database, if any, the history is
    * read in: `INFORMATION_SCHEMA` is per-database — see
    * {@link snowflakeCostSQL}.
    */
   connection: ApiConnection,
): Promise<BuildReadCost | null> {
   const database = connection.snowflakeConnection?.database;
   // Untagged: nothing to scope the history by, and no attribution was asked for.
   const tag = snowflakeQueryTagValue(queryMetadata);
   if (tag === undefined) return null;
   try {
      const costResult = await session.runSQL(
         passthroughSnowflake(snowflakeCostSQL(tag, database), handle),
      );
      const candidates = resultRows(costResult);
      const row = pickSnowflakeReadRow(candidates, buildSQL);
      if (row === null) {
         // Counted: the likeliest Snowflake miss (QUERY_TEXT must match buildSQL
         // byte for byte after the driver round trip), invisible otherwise.
         recordAttributionSkipped(
            candidates.length === 0
               ? "read_row_not_found"
               : "read_row_ambiguous",
         );
         return null;
      }
      return snowflakeReadCost(row);
   } catch (error) {
      logger.warn("Could not read back what the Snowflake build read cost", {
         code: errorCodeOf(error),
      });
      recordAttributionSkipped("cost_query_failed");
      return null;
   }
}

/** Snowflake-dialect SQL sent through the passthrough, which is the only route to it. */
function passthroughSnowflake(innerSQL: string, handle: string): string {
   return `SELECT * FROM snowflake_query('${escapeSQL(innerSQL)}', '${escapeSQL(handle)}')`;
}

/**
 * Clear the session's `QUERY_TAG` before the session is released.
 *
 * {@link tagSnowflakeSession} may fail, and the driver may hand out a session
 * that was already tagged; a stale tag would misattribute the read, which is
 * worse than no attribution. Best-effort: cleanup must not fail a good build.
 */
export async function clearSnowflakeSessionTag(
   session: DuckDBConnection,
   handle: string,
): Promise<void> {
   try {
      await session.runSQL(
         passthroughSnowflake("ALTER SESSION UNSET QUERY_TAG", handle),
      );
   } catch (error) {
      logger.warn("Could not clear the Snowflake build session's query tag", {
         code: errorCodeOf(error),
      });
   }
}
