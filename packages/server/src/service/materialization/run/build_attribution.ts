// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Attributing a `storage=` build's warehouse read and recovering what it cost:
 * the Snowflake build session aligned with Malloy's live session settings and
 * tagged with the build's query metadata, then the tag's cost read back from
 * the session history and cleared. Read by the build session.
 *
 * Effects: SQL in the source warehouse, through the passthrough on the build
 * session; logs and attribution-skipped counts.
 */

import { errorCodeOf } from "../../../error_code";
import type { DuckDBConnection } from "@malloydata/db-duckdb";
import { logger } from "../../../logger";
import { recordAttributionSkipped } from "../../../materialization_metrics";
import type { FederatedSourceType } from "../../dialects";
import { escapeSQL } from "../../duckdb_session_setup";
import type { QueryMetadata } from "../../query_metadata";
import {
   snowflakeQueryTagValue,
   snowflakeSessionCheckSQL,
   snowflakeSessionParitySQL,
   snowflakeSetQueryTagSQL,
} from "./build_query_tag";
import {
   pickSnowflakeReadRow,
   resultRows,
   snowflakeReadCost,
   str,
   type BuildReadCost,
} from "./build_read_cost";
import { snowflakeCostSQL } from "./warehouse_sql";

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
   sourceType: FederatedSourceType,
   handle: string,
   queryMetadata: QueryMetadata | undefined,
): Promise<void> {
   if (sourceType !== "snowflake") return;
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
   sourceType: FederatedSourceType,
   handle: string,
   buildSQL: string,
   queryMetadata: QueryMetadata | undefined,
   /**
    * The source connection's configured database, if any. `INFORMATION_SCHEMA`
    * is per-database — see {@link snowflakeCostSQL}.
    */
   database: string | null | undefined,
): Promise<BuildReadCost | null> {
   if (sourceType !== "snowflake") return null;
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
   sourceType: FederatedSourceType,
   handle: string | undefined,
): Promise<void> {
   if (sourceType !== "snowflake" || handle === undefined) return;
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
