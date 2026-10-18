// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   type PersistSource,
   Runtime,
} from "@malloydata/malloy";
import {
   existsSync,
   mkdirSync,
   mkdtempSync,
   readdirSync,
   rmSync,
} from "node:fs";
import os from "node:os";
import path from "node:path";
import type { components } from "../api";
import {
   BadRequestError,
   MaterializationEligibilityError,
   StorageCatalogNotFoundError,
} from "../errors";
import { logger } from "../logger";
import { getPartitionedWriteFlushThreshold } from "../config";
import { errMessage } from "../utils";
import { quoteIdentifier, quoteManifestTablePath } from "./quoting";
import { projectToPublicColumns } from "./build_plan";
import {
   bigQueryQueryLabelValue,
   snowflakeQueryTagValue,
   snowflakeSetQueryTagSQL,
} from "./build_query_tag";
import {
   BIGQUERY_COST_COLUMNS,
   bigQueryReadCost,
   pickSnowflakeReadRow,
   snowflakeCostSQL,
   snowflakeReadCost,
   str,
   type BuildReadCost,
} from "./build_read_cost";
import { recordAttributionSkipped } from "../materialization_metrics";
import type { QueryMetadata } from "./query_metadata";
import {
   applySessionResourceLimits,
   attachDuckLakeReadOnlyIfPresent,
   attachDuckLakeReadWrite,
   escapeSQL,
   federateSourceForPassthrough,
   type FederatedSourceType,
} from "./connection";
import type { WatermarkBound } from "./incremental_apply";
import { storageDestinationRoot } from "./connection_config";
import {
   assertServesInDuckDB,
   type ServeBinding,
} from "./materialization_serve_transform";

type ApiConnection = components["schemas"]["Connection"];

/**
 * The process-wide session the build-time servability gate compiles against.
 * Created on first use and never disposed: it holds no attach and no
 * credentials, and a session per build was the largest per-build memory cost.
 */
let sharedGateSession: DuckDBConnection | undefined;
type WireColumn = components["schemas"]["Column"];

/** Source warehouse types the native query-passthrough build supports. */
const PASSTHROUGH_SOURCE_TYPES: readonly FederatedSourceType[] = [
   "bigquery",
   "snowflake",
   "postgres",
];

/**
 * Warehouse types the build can materialize INTO. Wider than
 * `DECLARABLE_STORAGE_DESTINATION_TYPES`, which is what a storage destination may
 * be declared as and admits `ducklake` only — so the `duckdb` branch here is not
 * reachable from configuration today.
 */
const STORAGE_DESTINATION_TYPES = ["ducklake", "duckdb"] as const;

/**
 * The Malloy dialect every storage destination speaks (both types are
 * DuckDB-family): the dialect of a stored table's DDL, DML and identifier
 * quoting, which is NOT the source's.
 */
export const STORAGE_TARGET_DIALECT = "duckdb";

/**
 * Wrap a source-dialect SELECT as a native per-engine query-passthrough call, so
 * the warehouse computes and only result rows enter DuckDB. `handle` is the
 * per-engine reference {@link federateSourceForPassthrough} established (a
 * projectId, a secret name, or an ATTACH alias). Argument order differs per
 * engine, which is why this is centralized.
 */
export function wrapPassthrough(
   sourceType: FederatedSourceType,
   handle: string,
   innerSQL: string,
): string {
   const sql = `'${escapeSQL(innerSQL)}'`;
   const h = `'${escapeSQL(handle)}'`;
   switch (sourceType) {
      case "bigquery":
         // bigquery_query(<project/db>, <sql>) — secret resolved by bq:// scope.
         return `SELECT * FROM bigquery_query(${h}, ${sql})`;
      case "postgres":
         // postgres_query(<attached-db alias>, <sql>).
         return `SELECT * FROM postgres_query(${h}, ${sql})`;
      case "snowflake":
         // snowflake_query(<sql>, <secret name>) — SQL FIRST for this engine.
         return `SELECT * FROM snowflake_query(${sql}, ${h})`;
      default: {
         // Exhaustiveness: a new FederatedSourceType must add a case above.
         const exhaustive: never = sourceType;
         throw new BadRequestError(
            `Unsupported passthrough source type: ${String(exhaustive)}`,
         );
      }
   }
}

/**
 * The SELECT a partitioned insert reads, ordered by the partition columns.
 *
 * DuckDB's partitioned COPY (what a DuckLake insert into a laid-out table
 * plans) charges each appender one vector per column for every partition it
 * has met, before flushing any. Rows in partition order keep that set at one or
 * two partitions; interleaved rows bring in every partition within a few
 * chunks, and a wide, many-partition insert then exceeds `memory_limit` at any
 * flush threshold.
 *
 * Applied only at the top of a passthrough-sourced partitioned INSERT, so what
 * the warehouse runs and is attributed for is unchanged. Sorted by DuckDB
 * (out of core, against the session's spill directory) rather than by the
 * warehouse, because a multi-stream read (BigQuery) re-interleaves a sorted
 * result. Wrapped as a subselect so the SELECT is not parsed.
 */
export function orderByPartitionColumns(
   selectSQL: string,
   partitionColumns: readonly string[],
   dialect: string,
): string {
   if (partitionColumns.length === 0) {
      return selectSQL;
   }
   const order = partitionColumns
      .map((name) => quoteIdentifier(name, dialect))
      .join(", ");
   // A trailing terminator would end the subselect early. The closing paren
   // goes on its own line so a trailing `--` comment cannot swallow it.
   const inner = selectSQL.replace(/[\s;]+$/, "");
   return `SELECT * FROM (\n${inner}\n) AS partitioned_build ORDER BY ${order}`;
}

/**
 * Tag every subsequent read on a Snowflake build session with this build's
 * query metadata.
 *
 * Snowflake only: its passthrough reuses ONE session across `snowflake_query()`
 * calls, so a session-level `QUERY_TAG` reaches the following read. BigQuery
 * labels as part of issuing the read (see {@link issuePassthroughRead});
 * Postgres has no tag to set.
 *
 * Best-effort: a refused tag costs this build's attribution, never the build.
 */
async function tagSnowflakeSession(
   session: DuckDBConnection,
   sourceType: FederatedSourceType,
   handle: string,
   queryMetadata: QueryMetadata | undefined,
): Promise<void> {
   if (sourceType !== "snowflake") return;
   const sql = snowflakeSetQueryTagSQL(queryMetadata, handle);
   if (sql === undefined) return;
   try {
      await session.runSQL(sql);
   } catch (error) {
      logger.warn("Could not tag the Snowflake build session", {
         error: error instanceof Error ? error.message : String(error),
      });
      // Untagged, the read is unattributable in the customer's query history AND
      // unfindable by the cost lookup, which keys on that same tag.
      recordAttributionSkipped("tag_failed");
   }
}

/**
 * Can this connection list the job records the split read depends on?
 *
 * The split locates its result table through the job listing, a permission the
 * direct `bigquery_query()` path never needs. Probing with a one-row listing
 * BEFORE the read means a missing permission costs attribution, not the build:
 * once the read has run, falling back would bill it a second time.
 *
 * Not cached: caching would have to key on credentials and survive rotation.
 */
async function canSplitBigQueryRead(
   session: DuckDBConnection,
   handle: string,
): Promise<boolean> {
   try {
      await session.runSQL(
         `SELECT job_id FROM bigquery_jobs('${escapeSQL(handle)}', maxResults := 1)`,
      );
      return true;
   } catch (error) {
      logger.warn(
         "Cannot list BigQuery jobs for this connection, so this build's " +
            "warehouse read will not be labelled or costed",
         {
            error: error instanceof Error ? error.message : String(error),
         },
      );
      recordAttributionSkipped("job_listing_unavailable");
      return false;
   }
}

/**
 * A build failed AFTER its warehouse read had run and been billed.
 *
 * Everything else this path throws happens before anything is charged. Here the
 * rows live in an anonymous table the build could not locate, so a retry pays
 * for the same read twice: treat it as an operator decision, not something to
 * re-drive automatically. Without the job record there are no rows to capture,
 * so this fails the build rather than degrading to "no cost".
 */
export class BilledReadNotCapturedError extends Error {
   constructor(message: string, options?: { cause?: unknown }) {
      super(message, options);
      this.name = "BilledReadNotCapturedError";
   }
}

const BIGQUERY_LOOKUP_ATTEMPTS = 3;
const BIGQUERY_LOOKUP_BACKOFF_MS = 250;

/**
 * Re-ask for the job record a few times before failing a build whose read has
 * already been billed. The capability probe showed this connection CAN list
 * jobs, so a failure here is likely transient (5xx, quota, rotated credential).
 * Bounded and short: the session still holds federated credentials, and a
 * warehouse that is down should fail the build rather than stall it.
 */
async function withBigQueryLookupRetry<T>(
   work: () => Promise<T>,
   /** Named in the terminal error: the only handle on a read that was paid for. */
   parentJobId: string,
): Promise<T> {
   let lastError: unknown;
   for (let attempt = 1; attempt <= BIGQUERY_LOOKUP_ATTEMPTS; attempt++) {
      try {
         return await work();
      } catch (error) {
         lastError = error;
         if (attempt < BIGQUERY_LOOKUP_ATTEMPTS) {
            logger.warn(
               "Could not read the BigQuery job record for a build's read; retrying",
               {
                  attempt,
                  of: BIGQUERY_LOOKUP_ATTEMPTS,
                  parentJobId,
                  error: error instanceof Error ? error.message : String(error),
               },
            );
            await new Promise((resolve) =>
               setTimeout(resolve, BIGQUERY_LOOKUP_BACKOFF_MS * attempt),
            );
         }
      }
   }
   // Names the parent job: it is an operator's only handle on the billed read,
   // since a script's child job is absent from the default listing.
   throw new BilledReadNotCapturedError(
      `Could not read the BigQuery job record for a build's read (parent job ` +
         `${parentJobId}) after ${BIGQUERY_LOOKUP_ATTEMPTS} attempts, so its ` +
         `rows cannot be captured: ` +
         `${lastError instanceof Error ? lastError.message : String(lastError)}`,
      { cause: lastError },
   );
}

/**
 * What the CTAS reads from, plus the warehouse's id for the read when its shape
 * yields one. `jobId` lets a build's cost be recovered by exact key rather than
 * by matching query text.
 */
interface PassthroughRead {
   /** The SELECT the CTAS wraps. */
   selectSQL: string;
   /** The warehouse's id for the read, null when this shape does not produce one. */
   jobId: string | null;
   /**
    * What the read cost, when the shape that issued it also reported it. Read
    * from the same job record that located the result table: a script's child
    * job is reachable only through its parent, so no later lookup can find it.
    */
   cost: BuildReadCost | null;
}

/**
 * Issue the source-warehouse read for a `storage=` build and return what the
 * CTAS should select from. Two shapes, chosen by whether there is a BigQuery
 * label to apply:
 *
 * <ul>
 *   <li><b>Direct</b> — `wrapPassthrough`'s single rows-returning call, for every
 *       engine and for unlabelled BigQuery. Yields no job id.
 *   <li><b>Split (BigQuery, labelled)</b> — `bigquery_execute` runs the SELECT in
 *       a script that sets `@@query_label` first, then `bigquery_scan` reads the
 *       anonymous result table. `bigquery_query()` takes no labels and cannot run
 *       a script, so labelling requires the split.
 * </ul>
 *
 * The split is not a second scan: reading the result table goes through the
 * Storage Read API and creates no new QUERY job. Gating on the label keeps the
 * unlabelled path unchanged, so only builds that asked to be attributed split.
 */
export async function issuePassthroughRead(
   session: DuckDBConnection,
   sourceType: FederatedSourceType,
   handle: string,
   buildSQL: string,
   queryMetadata: QueryMetadata | undefined,
): Promise<PassthroughRead> {
   const label =
      sourceType === "bigquery"
         ? bigQueryQueryLabelValue(queryMetadata)
         : undefined;
   // Probed BEFORE anything runs: past this point the read is billed, and an
   // unusable split could only be recovered by billing the read twice.
   if (label === undefined || !(await canSplitBigQueryRead(session, handle))) {
      return {
         selectSQL: wrapPassthrough(sourceType, handle, buildSQL),
         jobId: null,
         cost: null,
      };
   }

   // BigQuery creates a parent job for the script and one child per statement;
   // the child that ran the SELECT carries both the label and the cost.
   const script = `SET @@query_label = "${label}";\n${buildSQL};`;
   const executed = await session.runSQL(
      `SELECT * FROM bigquery_execute('${escapeSQL(handle)}', '${escapeSQL(script)}')`,
   );
   const parentJobId = firstColumn(executed, "job_id");
   if (parentJobId === null) {
      throw new BilledReadNotCapturedError(
         "bigquery_execute returned no job_id for the build's read, so its " +
            "result table cannot be located",
      );
   }

   // The result table is anonymous, so its name comes from the job record.
   // Newest first, because the listing order is undocumented and the SELECT is
   // the script's last statement; `SET @@query_label` is not observed to produce
   // a child with a destination table, but that is not guaranteed.
   const located = await withBigQueryLookupRetry(async () => {
      const child = await session.runSQL(`
      SELECT job_id,
             json_extract_string(configuration, '$.query.destinationTable.projectId') AS project,
             json_extract_string(configuration, '$.query.destinationTable.datasetId') AS dataset,
             json_extract_string(configuration, '$.query.destinationTable.tableId')   AS table,
             ${BIGQUERY_COST_COLUMNS}
      FROM (SELECT * FROM bigquery_jobs('${escapeSQL(handle)}',
                                        parentJobId := '${escapeSQL(parentJobId)}')
            WHERE job_type = 'QUERY')
      ORDER BY creation_time DESC`);
      const row = resultRows(child).find((r) => str(r.dataset) && str(r.table));
      if (row === undefined) {
         // Retried: a listing can answer 200 before it enumerates the child.
         throw new Error(
            `no child job with a result table under parent ${parentJobId}`,
         );
      }
      return row;
   }, parentJobId);

   const path = [
      str(located.project) ?? handle,
      str(located.dataset),
      str(located.table),
   ].join(".");
   const jobId = str(located.job_id);
   return {
      selectSQL: `SELECT * FROM bigquery_scan('${escapeSQL(path)}')`,
      jobId,
      cost: bigQueryReadCost(located, jobId, parentJobId),
   };
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
async function snowflakeReadCostAfterBuild(
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
         error: error instanceof Error ? error.message : String(error),
      });
      recordAttributionSkipped("cost_query_failed");
      return null;
   }
}

/** Snowflake-dialect SQL sent through the passthrough, which is the only route to it. */
function passthroughSnowflake(innerSQL: string, handle: string): string {
   return `SELECT * FROM snowflake_query('${escapeSQL(innerSQL)}', '${escapeSQL(handle)}')`;
}

/** Rows from a `runSQL` result, which is either the array itself or wraps one. */
function resultRows(result: unknown): Record<string, unknown>[] {
   if (Array.isArray(result)) return result as Record<string, unknown>[];
   const rows = (result as { rows?: unknown })?.rows;
   return Array.isArray(rows) ? (rows as Record<string, unknown>[]) : [];
}

/** One named column from the first row, or null when it is absent or empty. */
function firstColumn(result: unknown, column: string): string | null {
   const rows = resultRows(result);
   return rows.length > 0 ? str(rows[0][column]) : null;
}

/**
 * Clear the session's `QUERY_TAG` before the session is released.
 *
 * {@link tagSnowflakeSession} may fail, and the driver may hand out a session
 * that was already tagged; a stale tag would misattribute the read, which is
 * worse than no attribution. Best-effort: cleanup must not fail a good build.
 */
async function clearSnowflakeSessionTag(
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
         error: error instanceof Error ? error.message : String(error),
      });
   }
}

/** Narrow a connection's declared type to a supported passthrough source type. */
export function passthroughSourceType(
   sourceConnection: ApiConnection,
): FederatedSourceType {
   const type = sourceConnection.type;
   if ((PASSTHROUGH_SOURCE_TYPES as readonly string[]).includes(type ?? "")) {
      return type as FederatedSourceType;
   }
   throw new BadRequestError(
      `Cannot materialize a '${type}' source into a storage destination: the ` +
         `native query-passthrough build supports source connections of type ` +
         `${PASSTHROUGH_SOURCE_TYPES.join(", ")} only.`,
   );
}

/**
 * A build-scoped DuckDB session on its OWN in-memory instance, plus a disposer
 * that closes it and removes its throwaway working directory.
 *
 * Isolation is load-bearing for multi-tenant safety. `@malloydata/db-duckdb`
 * pools instances in a process-global cache keyed by a share key that EXCLUDES
 * the connection name, so sessions with identical config share one in-memory
 * instance while their lifetimes overlap. Shared, they collide on transient
 * ATTACH aliases: two builds (or two environments with same-named connections
 * holding different credentials) clobber each other's `ATTACH … AS lake`, a
 * cross-tenant read or WRITE.
 *
 * A UNIQUE `workingDirectory` is part of the share key, so each session gets its
 * own instance (catalog, secrets, attaches). `databasePath` stays `:memory:`,
 * not a temp file, because only a `:memory:` primary lets a DuckLake attach
 * auto-initialize a fresh catalog. The working directory also holds the build's
 * spill (`temp_directory`) and nothing else; removal on dispose is best-effort.
 */
export function createIsolatedBuildSession(sessionName: string): {
   session: DuckDBConnection;
   dispose: () => Promise<void>;
   /**
    * The session's private working directory. Returned so the caller can point
    * DuckDB's `temp_directory` at it: it is unique per build and removed by
    * `dispose`, so spill from one build can neither outlive it nor collide with
    * another build's.
    */
   workDir: string;
} {
   const workDir = mkdtempSync(path.join(os.tmpdir(), "malloy-build-"));
   // The disposer that owns removing workDir does not exist until this function
   // returns, so a throw from the constructor would strand the directory with
   // nothing left holding a reference to it.
   let session: DuckDBConnection;
   try {
      // Lifts db-duckdb's default `rowLimit` of 10, which `runSQL` enforces by
      // slicing. `DESCRIBE` and `duckdb_columns()` return one row per COLUMN, so
      // the default would truncate a wide table's schema: the delta path would
      // see a shape mismatch and rebuild forever, and the manifest would declare
      // a short schema.
      session = new DuckDBConnection(sessionName, ":memory:", workDir, {
         rowLimit: Number.MAX_SAFE_INTEGER,
      });
   } catch (err) {
      rmSync(workDir, { recursive: true, force: true });
      throw err;
   }
   const dispose = async () => {
      // `close()` alone does not end the session: the node-api Connection's C++
      // ClientContext keeps the instance refcount above zero, so the database —
      // with the destination ATTACH and federated source credentials — survives
      // (e.g. idle Postgres backends stranded until process exit).
      //
      // db-duckdb cannot disconnect unconditionally (reverted upstream in
      // PR #2793: malloy's translate layer holds weak_ptrs to the
      // Connection). A build session is server-owned and single-use, so it can.
      // Reached through a cast because the handle is library-internal; use an
      // explicit hard-close if db-duckdb adds one.
      const nodeConnection = (
         session as unknown as { connection?: { disconnectSync?: () => void } }
      ).connection;
      try {
         await session.close();
      } catch (err) {
         logger.warn("Failed to close build session (leaked session)", {
            sessionName,
            error: err instanceof Error ? err.message : String(err),
         });
      }
      try {
         nodeConnection?.disconnectSync?.();
      } catch (err) {
         logger.warn("Failed to disconnect build session (leaked connection)", {
            sessionName,
            error: err instanceof Error ? err.message : String(err),
         });
      }
      try {
         rmSync(workDir, { recursive: true, force: true });
      } catch (err) {
         logger.warn("Failed to remove build session working directory", {
            sessionName,
            workDir,
            error: err instanceof Error ? err.message : String(err),
         });
      }
   };
   return { session, dispose, workDir };
}

/**
 * Pin a build session's timezone before any predicate is rendered against it.
 *
 * A `TIMESTAMPTZ` column compared with a naive `TIMESTAMP` literal resolves in
 * the session's zone, and the incremental path renders bounds as UTC text; on a
 * host-local zone the window silently shifts, and a boundary could LEAD the
 * table. Malloy's DuckDB connector already sets UTC; this keeps the property
 * from depending on another package's setup.
 */
export async function pinSessionToUTC(
   session: DuckDBConnection,
): Promise<void> {
   await session.runSQL("SET TimeZone='UTC'");
}

/** Result of building one source into a storage destination. */
export interface StorageBuildResult {
   /** The connection the physical table now lives in (the destination). */
   storageDestinationName: string;
   /** Authoritative DuckDB column schema, captured post-build via DESCRIBE. */
   schema: WireColumn[];
   /**
    * What the source warehouse charged for the read, when its shape reported it.
    * Null means unknown, not free (except on a chained build, which reads no
    * warehouse).
    */
   readCost: BuildReadCost | null;
   /** Present only when the table was advanced IN PLACE rather than rebuilt. */
   refresh?: StorageRefreshOutcome;
   /**
    * For an incremental source that was rebuilt: where the new table reaches,
    * the boundary that lets the NEXT refresh be a delta.
    */
   seededThrough?: WatermarkBound;
   /**
    * For a source that reads a persisted upstream: whether its rows were
    * computed from the upstream's STORED table in the destination (`reused`),
    * or by recomputing the upstream from the source warehouse with it inlined
    * (`recomputed`). Absent for a source with no persisted upstream, whose only
    * build reads the warehouse.
    */
   upstreamReuse?: "reused" | "recomputed";
   /** Why the stored upstream was not read, when `upstreamReuse` is `recomputed`. */
   upstreamRecomputeReason?: string;
}

/** What an in-place refresh did, for the manifest entry to report. */
interface StorageRefreshOutcome {
   /** Absent for a refresh that applied nothing (the watermark had not moved). */
   durationMs?: number;
   /** The boundary now in force — where the next refresh starts. */
   coveredThrough?: WatermarkBound;
   /** `delta` advanced the table; `none` found nothing to apply. */
   refresh: "delta" | "none";
   /** What the delta's own warehouse read cost, when its shape reported it. */
   readCost: BuildReadCost | null;
}

/**
 * A caller's chance to advance the table IN PLACE instead of rebuilding it,
 * handed a session that already holds the destination read-write and the source
 * federated. A callback because the decision is the caller's but the session
 * (whose credentials must not outlive this build) is owned here. Returning
 * undefined means "rebuild", which then runs on the same session.
 */
export interface StorageIncrementalRefresh {
   /**
    * Advance the table in place, or return undefined to have it rebuilt. Runs
    * after the attach and the federation, before the CTAS.
    */
   plan: (deps: {
      session: DuckDBConnection;
      sourceType: FederatedSourceType;
      handle: string;
      /** The destination-qualified, quoted path the DML must name. */
      quotedTablePath: string;
   }) => Promise<StorageRefreshOutcome | undefined>;
   /**
    * Record where the table a REBUILD just wrote reaches, so the next refresh can
    * be a delta, and return that boundary. Runs on the same session after the
    * CTAS, since the boundary is probed from the table. Undefined when there is
    * no boundary (an empty source), so the next refresh rebuilds again.
    */
   afterSeed: (deps: {
      session: DuckDBConnection;
      quotedTablePath: string;
   }) => Promise<WatermarkBound | undefined>;
}

/** See `buildSourceIntoStorage`'s `deps`. */
export interface BuildSessionDeps {
   federate?: typeof federateSourceForPassthrough;
   read?: typeof issuePassthroughRead;
}

/**
 * Build one persist source into a `storage=` destination via native
 * query-passthrough CTAS, on a build-scoped DuckDB session created and disposed
 * here — so the read-write destination attach and the source credentials exist
 * only for the build and NEVER on the serve connection (least privilege).
 *
 * Attach destination read-write → federate source → `CREATE OR REPLACE TABLE`
 * from the passthrough → DESCRIBE the result → dispose. DuckLake's catalog swap
 * is transactional, so the replace is atomic.
 *
 * @returns the destination name and the captured schema, recorded on the
 *   manifest entry for the serve transform.
 */
export async function buildSourceIntoStorage(params: {
   destinationName: string;
   destinationConnection: ApiConnection;
   sourceConnection: ApiConnection;
   /** The source-dialect compiled SELECT (`getSQL` output), verbatim. */
   buildSQL: string;
   /** Logical, unquoted physical table path (may carry a container path). */
   physicalTableName: string;
   /**
    * Columns to lay the destination table out by (`#@ persist partition=`),
    * already resolved against the source's public projection. Empty means a
    * single-statement CTAS.
    */
   partitionColumns?: string[];
   environmentPath: string;
   /**
    * Per-query metadata for the warehouse read, already resolved. Applied here
    * because the read runs inside DuckDB's passthrough, where no Malloy
    * connector is in the call path to attach it.
    */
   queryMetadata?: QueryMetadata;
   /**
    * Offered the chance to advance this table in place before the CTAS runs.
    * Absent for a non-incremental source, which always builds in full.
    */
   incremental?: StorageIncrementalRefresh;
   /**
    * Test seam for federation and the passthrough read; production passes
    * nothing. Lets a spec prove the `finally` closes a proxied source's tunnel
    * without a live warehouse.
    */
   deps?: BuildSessionDeps;
}): Promise<StorageBuildResult> {
   const {
      destinationName,
      destinationConnection,
      sourceConnection,
      buildSQL,
      physicalTableName,
      environmentPath,
      queryMetadata,
   } = params;
   const federate = params.deps?.federate ?? federateSourceForPassthrough;
   const read = params.deps?.read ?? issuePassthroughRead;

   assertSupportedDestination(destinationName, destinationConnection);
   const sourceType = passthroughSourceType(sourceConnection);

   // Its own instance, so its attach and credentials cannot collide with any
   // other connection (see createIsolatedBuildSession).
   const { session, dispose, workDir } = createIsolatedBuildSession(
      `build_${destinationName}`,
   );
   let federatedHandle: string | undefined;
   // A proxied source's tunnel, if any: closed in the finally, because disposing
   // the session does not close a listener opened outside it.
   let federatedClose: (() => Promise<void>) | undefined;
   try {
      // Before the destination attach, which applies (and latches) the limits
      // without a `tempDirectory` for DuckLake: afterwards, spill would go to the
      // shared configured path, outliving `dispose` and colliding between
      // builds. It also bounds memory during the attach itself.
      await applySessionResourceLimits(session, { tempDirectory: workDir });
      await attachDestinationReadWrite(
         session,
         destinationName,
         destinationConnection,
         environmentPath,
      );

      if (destinationConnection.type === "ducklake") {
         // Disable row inlining for this session so rows land as Parquet in the
         // data store, not in the catalog database — in a shared catalog,
         // inlining would push tenant data into the catalog Postgres.
         // Session-scoped; the catalog's persisted options are untouched.
         await session.runSQL("SET ducklake_default_data_inlining_row_limit=0");
      }
      // Before any predicate is rendered (incremental refresh only); see
      // pinSessionToUTC.
      await pinSessionToUTC(session);

      const federated = await federate(
         session,
         sourceType,
         sourceFederationConfig(sourceConnection),
      );
      federatedClose = federated.close;
      federatedHandle = federated.handle;

      await tagSnowflakeSession(
         session,
         sourceType,
         federated.handle,
         queryMetadata,
      );

      // Qualified with the destination catalog: an unqualified name would land
      // in the session's throwaway in-memory catalog and vanish with it.
      // quoteManifestTablePath, NOT quoteTablePath, to match how the serve side
      // renders the path (an already-quoted name passes through). A mismatch
      // surfaces only at run time, past the live fallback, and 400s every query.
      const target = quoteManifestTablePath(
         `${destinationName}.${physicalTableName}`,
         STORAGE_TARGET_DIALECT,
      );

      // After the attach and federation, because the plan probes this
      // destination to decide; a seed then continues on the same session.
      if (params.incremental) {
         // A delta into a laid-out table is the same partitioned COPY as the
         // seed, whose memory scales with width x partitions. The threshold and
         // single appender apply via the session; the ordered read is the
         // seed's alone (see createTableAndDescribe).
         const partitioned = (params.partitionColumns?.length ?? 0) > 0;
         const flushThreshold = getPartitionedWriteFlushThreshold();
         const boundDelta = partitioned && flushThreshold !== undefined;
         if (boundDelta) {
            await session.runSQL(
               `SET partitioned_write_flush_threshold = ${flushThreshold}`,
            );
            await session.runSQL("SET threads = 1");
         }
         let refreshed;
         try {
            refreshed = await params.incremental.plan({
               session,
               sourceType,
               handle: federated.handle,
               quotedTablePath: target,
            });
         } finally {
            if (boundDelta) {
               try {
                  await session.runSQL("RESET threads");
                  await session.runSQL(
                     "RESET partitioned_write_flush_threshold",
                  );
               } catch {
                  // best-effort, as in createTableAndDescribe
               }
            }
         }
         if (refreshed) {
            return {
               storageDestinationName: destinationName,
               // A delta cannot change the shape, but the manifest entry
               // declares the schema on every publication; omitting it would
               // leave the source unservable until its next full build.
               schema: await describeTable(session, target),
               readCost: refreshed.readCost,
               refresh: refreshed,
            };
         }
      }

      const passthrough = await read(
         session,
         sourceType,
         federated.handle,
         buildSQL,
         queryMetadata,
      );

      // The serve transform declares exactly this schema, and the compiler does
      // not type-check a virtual source's declared columns.
      const schema = await createTableAndDescribe(
         session,
         target,
         passthrough.selectSQL,
         params.partitionColumns ?? [],
         { sourceType },
      );
      // Record where the new snapshot reaches, so the next refresh can be a
      // delta. On this session: the attach does not outlive this function.
      const seededThrough = await params.incremental?.afterSeed({
         session,
         quotedTablePath: target,
      });

      return {
         seededThrough,
         storageDestinationName: destinationName,
         schema,
         // BigQuery's labelled read reports its cost; Snowflake's must be asked
         // after the read, while the session still holds the credentials.
         readCost:
            passthrough.cost ??
            (await snowflakeReadCostAfterBuild(
               session,
               sourceType,
               federated.handle,
               buildSQL,
               queryMetadata,
               sourceConnection.snowflakeConnection?.database,
            )),
      };
   } finally {
      await clearSnowflakeSessionTag(session, sourceType, federatedHandle);
      // Releases every secret and attach; nothing federated survives the build.
      await dispose();
      // Then the tunnel, once the attach using it is gone. Best-effort.
      if (federatedClose) {
         await federatedClose().catch((e) =>
            logger.warn(
               `Failed to close the SSH proxy a storage build federated through: ${String(e)}`,
            ),
         );
      }
   }
}

/**
 * Build a CHAINED `storage=` source from its upstreams' already-materialized
 * tables in the destination, instead of recomputing them from the warehouse.
 * The downstream is then a pure function of the parents' STORED rows.
 *
 * `transientModel` (see {@link buildChainedStorageBuildModel}) rebinds every
 * upstream to a virtual source on this destination and re-declares the
 * downstream as a persist source over them. It is compiled against the build
 * session, and the downstream's `getSQL({ virtualMap })` is CTAS'd into the
 * destination; nothing is federated and nothing leaves DuckDB. Session
 * lifecycle mirrors {@link buildSourceIntoStorage}.
 *
 * @returns the destination name and the captured schema.
 */
export async function buildDownstreamIntoStorage(params: {
   destinationName: string;
   destinationConnection: ApiConnection;
   /** Transient rebind model: upstream virtuals + persist-annotated downstream. */
   transientModel: string;
   /** The downstream source's Malloy name, to locate it in the transient plan. */
   downstreamName: string;
   /** destinationName → handle → quoted physical path for the upstream virtuals. */
   virtualMap: Map<string, Map<string, string>>;
   /** Logical, unquoted physical table path for the downstream's own table. */
   physicalTableName: string;
   /**
    * Columns to lay the destination table out by (`#@ persist partition=`),
    * already resolved against the source's public projection. Empty means a
    * single-statement CTAS.
    */
   partitionColumns?: string[];
   environmentPath: string;
}): Promise<StorageBuildResult> {
   const {
      destinationName,
      destinationConnection,
      transientModel,
      downstreamName,
      virtualMap,
      physicalTableName,
      environmentPath,
   } = params;

   assertSupportedDestination(destinationName, destinationConnection);

   const { session, dispose, workDir } = createIsolatedBuildSession(
      `build_${destinationName}`,
   );
   try {
      // Before the destination attach — see buildSourceIntoStorage.
      await applySessionResourceLimits(session, { tempDirectory: workDir });
      await attachDestinationReadWrite(
         session,
         destinationName,
         destinationConnection,
         environmentPath,
      );
      if (destinationConnection.type === "ducklake") {
         // See buildSourceIntoStorage: keep rows out of the catalog database.
         await session.runSQL("SET ducklake_default_data_inlining_row_limit=0");
      }
      // No predicate runs here today (chained sources are excluded from the
      // delta path); pinned so lifting that exclusion stays correct.
      await pinSessionToUTC(session);

      // The upstream virtual sources declare their captured schema, so compiling
      // reads no tables; the CTAS below runs the real SQL.
      const root = "file:///chained-build/";
      const url = `${root}m.malloy`;
      const runtime = new Runtime({
         urlReader: new InMemoryURLReader(new Map([[url, transientModel]])),
         connections: new FixedConnectionMap(
            new Map([[destinationName, session]]),
            destinationName,
         ),
      });
      // The only SHAPE step here: a compile failure means the downstream cannot
      // be expressed over its rebound parents, so the caller may fall back to
      // recomputing. Infrastructure failures (attach, CTAS, DESCRIBE) would fail
      // the same way on fallback, so only shape failures are marked eligible.
      let model;
      try {
         model = await runtime
            .loadModel(new URL(url), { importBaseURL: new URL(root) })
            .getModel();
      } catch (err) {
         throw new MaterializationEligibilityError({
            message: `Chained build model did not compile over the rebound parents: ${errMessage(err)}`,
         });
      }
      // `getBuildPlan`, not `getBuildTargets`: targets render every source's SQL
      // while planning, and the rebound virtual parents cannot render without
      // the `virtualMap`, which that call cannot take.
      const plan = model.getBuildPlan();
      let downstream: PersistSource | undefined;
      for (const ps of Object.values(plan.sources)) {
         if (ps.name === downstreamName) {
            downstream = ps;
            break;
         }
      }
      if (!downstream) {
         // The downstream references something the rebind model lacks (an
         // uncarried parent refinement, a live leaf). The caller falls back.
         throw new MaterializationEligibilityError({
            message:
               `Chained build model did not yield a persist source named ` +
               `'${downstreamName}' (the downstream references something the ` +
               `rebound parents don't provide).`,
         });
      }
      // Projected to public columns so hidden (`except:` / access-restricted)
      // values are not stored at rest.
      const sql = projectToPublicColumns(
         downstream,
         downstream.getSQL({ virtualMap }),
      );

      // Quoted as in buildSourceIntoStorage, to match the serve side.
      const target = quoteManifestTablePath(
         `${destinationName}.${physicalTableName}`,
         STORAGE_TARGET_DIALECT,
      );
      const schema = await createTableAndDescribe(
         session,
         target,
         sql,
         params.partitionColumns ?? [],
      );

      return {
         storageDestinationName: destinationName,
         schema,
         // Reads no source warehouse: here, uniquely, null means "spent nothing".
         readCost: null,
         upstreamReuse: "reused",
      };
   } finally {
      await dispose();
   }
}

/**
 * Build-time servability gate: compile the source's serve shape in DuckDB
 * against the captured (post-build) schema and refuse the build if it does not
 * compile. The served table lives in DuckDB, so this turns a serve-time error
 * into a build-time 422. No SQL runs and no table is read, so an in-memory
 * session with no attach suffices.
 *
 * @throws {MaterializationEligibilityError} (HTTP 422) if the shape can't compile.
 */
export async function assertStorageServeShapeCompiles(params: {
   destinationName: string;
   sourceName: string;
   virtualHandle: string;
   physicalTableName: string;
   schema: WireColumn[];
}): Promise<void> {
   const { destinationName, sourceName, virtualHandle, physicalTableName } =
      params;
   // No `origin`: the gate compiles from the captured schema alone, so it treats
   // a rollup's table and an authored one alike.
   const binding: ServeBinding = {
      sourceName,
      destinationName,
      virtualHandle,
      tablePath: `${destinationName}.${physicalTableName}`,
      schema: params.schema
         .filter((c) => c.name && c.type)
         .map((c) => ({ name: c.name as string, type: c.type as string })),
   };
   // ONE session for the process: this gate neither attaches nor federates, so
   // it has no cross-tenant collision surface, and an instance per build was
   // the bulk of the build path's unreclaimed memory growth. The risk is a
   // shared catalog (earlier declarations leaking into later compiles); the
   // spec pins that refusals still refuse and recompiles see new schemas.
   //
   // Assigned synchronously: an `await` before the assignment would let two
   // concurrent callers each create a session, orphaning one for the life of
   // the process.
   sharedGateSession ??= createIsolatedBuildSession("gate_shared").session;
   // Bounded on every use (idempotent per connection) so the line above stays
   // synchronous. It must be bounded here: it is the longest-lived instance and
   // reaches no connection lookup or attach that would bound it otherwise. No
   // session-owned spill directory: one would accumulate for the process's life,
   // so it uses the configured one.
   await applySessionResourceLimits(sharedGateSession);
   await assertServesInDuckDB(
      sourceName,
      binding,
      new FixedConnectionMap(
         new Map([[destinationName, sharedGateSession]]),
         destinationName,
      ),
   );
}

/** DDL to drop a storage table by its recorded name, catalog-qualified for DuckDB. */
export function dropStorageTableSql(
   destinationName: string,
   physicalTableName: string,
): string {
   // Must name the table the way the build CREATEd it (see the CTAS sites).
   return `DROP TABLE IF EXISTS ${quoteManifestTablePath(
      `${destinationName}.${physicalTableName}`,
      STORAGE_TARGET_DIALECT,
   )}`;
}

/**
 * Drop one materialized table from a `storage=` destination, on an isolated
 * read-write session (the serve attach is read-only; isolation keeps the attach
 * from colliding with another tenant's — see {@link createIsolatedBuildSession}).
 *
 * Exactly one named table, never a pattern or scan, so the caller decides what
 * goes: a name Publisher recorded building, or one an orchestrator assigned.
 */
export async function dropStorageTable(params: {
   destinationName: string;
   destinationConnection: ApiConnection;
   physicalTableName: string;
   environmentPath: string;
}): Promise<void> {
   const {
      destinationName,
      destinationConnection,
      physicalTableName,
      environmentPath,
   } = params;
   // Fail fast (pre-session, pre-attach) on a destination the build can't target.
   assertSupportedDestination(destinationName, destinationConnection);

   const { session, dispose, workDir } = createIsolatedBuildSession(
      `gc_${destinationName}`,
   );
   try {
      // Bounded even for a DROP: the sum of caps across all live instances is
      // what has to fit the container.
      await applySessionResourceLimits(session, { tempDirectory: workDir });
      await attachDestinationReadWrite(
         session,
         destinationName,
         destinationConnection,
         environmentPath,
      );
      await session.runSQL(
         dropStorageTableSql(destinationName, physicalTableName),
      );
   } finally {
      await dispose();
   }
}

/**
 * The tables in one schema of a `storage=` destination, in name order.
 *
 * Read-only, on an isolated instance. A plain DuckDB destination with no
 * database file yet is reported empty rather than created by looking.
 *
 * A DuckLake destination with no catalog yet is a 404
 * ({@link StorageCatalogNotFoundError}), not empty: an empty listing invites a
 * follow-up drop or cleanup, and those attach read-write, creating the catalog.
 */
export async function listStorageTables(params: {
   destinationName: string;
   destinationConnection: ApiConnection;
   schemaName: string;
   environmentPath: string;
}): Promise<string[]> {
   const {
      destinationName,
      destinationConnection,
      schemaName,
      environmentPath,
   } = params;
   assertSupportedDestination(destinationName, destinationConnection);

   const { session, dispose, workDir } = createIsolatedBuildSession(
      `list_${destinationName}`,
   );
   try {
      await applySessionResourceLimits(session, { tempDirectory: workDir });
      if (destinationConnection.type === "ducklake") {
         const cfg = requireDuckLakeConfig(
            destinationName,
            destinationConnection,
         );
         if (
            !(await attachDuckLakeReadOnlyIfPresent(
               session,
               destinationName,
               cfg,
            ))
         ) {
            throw new StorageCatalogNotFoundError(
               `Storage destination '${destinationName}' has no catalog ` +
                  `yet: nothing has been built into it.`,
            );
         }
      } else {
         // Existence is checked by listing the environment's destinations
         // directory, so the request-supplied name is only compared, never used
         // to reach the filesystem before the attach.
         const destinationRoot = storageDestinationRoot(environmentPath);
         const fileName = `${destinationName}.duckdb`;
         if (
            !existsSync(destinationRoot) ||
            !readdirSync(destinationRoot).includes(fileName)
         ) {
            return [];
         }
         const dbPath = path.join(destinationRoot, fileName);
         await session.runSQL(
            `ATTACH '${escapeSQL(dbPath)}' AS ${quoteIdentifier(destinationName, STORAGE_TARGET_DIALECT)} (READ_ONLY)`,
         );
      }
      const rows = resultRows(
         await session.runSQL(
            `SELECT table_name FROM duckdb_tables() ` +
               `WHERE database_name = '${escapeSQL(destinationName)}' ` +
               `AND schema_name = '${escapeSQL(schemaName)}' ` +
               `ORDER BY table_name`,
         ),
      );
      return rows.map((row) => String(row.table_name));
   } finally {
      await dispose();
   }
}

/** What one storage file cleanup reclaimed. */
interface StorageFileCleanupResult {
   expiredSnapshots: number;
   deletedFiles: number;
}

/**
 * Reclaim object-storage bytes in a DuckLake destination.
 *
 * Dropping a DuckLake table only retires it in the catalog; its files stay while
 * any snapshot references them. Two calls, in order:
 *
 *  1. `ducklake_expire_snapshots(older_than)` — forgets snapshots older than the
 *     cutoff (never the latest) and schedules files only they referenced. This
 *     cutoff is the recovery window for a dropped table.
 *  2. `ducklake_cleanup_old_files(older_than)` — deletes files scheduled before
 *     the cutoff, protecting a reader still scanning a just-expired snapshot.
 *
 * NOT `CHECKPOINT` or `ducklake_delete_orphaned_files`: both delete untracked
 * files, which include another catalog's live files on a shared data path and a
 * concurrent build's uncommitted ones. Non-DuckLake destinations are refused.
 */
export async function cleanupStorageFiles(params: {
   destinationName: string;
   destinationConnection: ApiConnection;
   snapshotsOlderThan: Date;
   filesOlderThan: Date;
}): Promise<StorageFileCleanupResult> {
   const {
      destinationName,
      destinationConnection,
      snapshotsOlderThan,
      filesOlderThan,
   } = params;
   if (destinationConnection.type !== "ducklake") {
      throw new BadRequestError(
         `Storage destination '${destinationName}' is type ` +
            `'${destinationConnection.type ?? ""}'; file cleanup applies only to ` +
            `'ducklake' destinations.`,
      );
   }
   const cfg = requireDuckLakeConfig(destinationName, destinationConnection);

   const { session, dispose, workDir } = createIsolatedBuildSession(
      `cleanup_${destinationName}`,
   );
   try {
      await applySessionResourceLimits(session, { tempDirectory: workDir });
      await attachDuckLakeReadWrite(session, destinationName, cfg);
      const catalog = escapeSQL(destinationName);
      const expired = resultRows(
         await session.runSQL(
            `CALL ducklake_expire_snapshots('${catalog}', older_than => ` +
               `TIMESTAMPTZ '${snapshotsOlderThan.toISOString()}')`,
         ),
      );
      const deleted = resultRows(
         await session.runSQL(
            `CALL ducklake_cleanup_old_files('${catalog}', older_than => ` +
               `TIMESTAMPTZ '${filesOlderThan.toISOString()}')`,
         ),
      );
      return { expiredSnapshots: expired.length, deletedFiles: deleted.length };
   } finally {
      await dispose();
   }
}

/** A DuckLake destination's catalog config, or a clean 400 when it has none. */
function requireDuckLakeConfig(
   destinationName: string,
   destinationConnection: ApiConnection,
): components["schemas"]["DucklakeConnection"] {
   const cfg = destinationConnection.ducklakeConnection;
   if (!cfg) {
      throw new BadRequestError(
         `Storage destination '${destinationName}' is type 'ducklake' but ` +
            `has no ducklakeConnection config.`,
      );
   }
   return cfg;
}

/** Where a plain DuckDB destination's database file lives. */
function destinationDatabasePath(
   environmentPath: string,
   destinationName: string,
): string {
   return path.join(
      storageDestinationRoot(environmentPath),
      `${destinationName}.duckdb`,
   );
}

/** Attach the destination connection read-write on the build session. */
async function attachDestinationReadWrite(
   session: DuckDBConnection,
   destinationName: string,
   destinationConnection: ApiConnection,
   environmentPath: string,
): Promise<void> {
   if (destinationConnection.type === "ducklake") {
      const cfg = requireDuckLakeConfig(destinationName, destinationConnection);
      await attachDuckLakeReadWrite(session, destinationName, cfg);
      return;
   }
   // Plain DuckDB: the file lives under the destinations root (as connection
   // assembly derives it) so it cannot collide with a same-named connection.
   // Created here because a build may be the first thing to touch it.
   mkdirSync(storageDestinationRoot(environmentPath), { recursive: true });
   const dbPath = destinationDatabasePath(environmentPath, destinationName);
   await session.runSQL(
      `ATTACH '${escapeSQL(dbPath)}' AS ${quoteIdentifier(destinationName, STORAGE_TARGET_DIALECT)}`,
   );
}

/** Throw a clean 422 unless the destination is a supported storage type. */
function assertSupportedDestination(
   destinationName: string,
   destinationConnection: ApiConnection,
): void {
   const type = destinationConnection.type ?? "";
   if (!(STORAGE_DESTINATION_TYPES as readonly string[]).includes(type)) {
      throw new BadRequestError(
         `Storage destination '${destinationName}' is type '${type}', but ` +
            `'storage=' destinations must be one of ` +
            `${STORAGE_DESTINATION_TYPES.join(", ")}.`,
      );
   }
}

/**
 * The subset of a source connection's config the passthrough federation needs,
 * plus a sanitized `name` base for the secret/alias it creates on the session.
 */
function sourceFederationConfig(sourceConnection: ApiConnection): {
   name: string;
   bigqueryConnection?: components["schemas"]["BigqueryConnection"];
   snowflakeConnection?: components["schemas"]["SnowflakeConnection"];
   postgresConnection?: components["schemas"]["PostgresConnection"];
   proxy?: components["schemas"]["ConnectionProxy"];
} {
   return {
      name: sourceConnection.name ?? "src",
      bigqueryConnection: sourceConnection.bigqueryConnection,
      snowflakeConnection: sourceConnection.snowflakeConnection,
      postgresConnection: sourceConnection.postgresConnection,
      // federatePostgres opens the tunnel; buildSourceIntoStorage closes it.
      proxy: sourceConnection.proxy,
   };
}

/**
 * Build the table and read back its schema.
 *
 * Without partition columns, a CTAS; with them, create-empty / lay out / insert
 * in one transaction, because DuckLake applies a layout only to files written
 * after it is set.
 *
 * On the CTAS path a failed DESCRIBE drops the committed table: the caller
 * records nothing until this returns, so no reclaim or GC would ever find it.
 */
export async function createTableAndDescribe(
   session: DuckDBConnection,
   quotedTablePath: string,
   selectSQL: string,
   partitionColumns: readonly string[] = [],
   options: {
      /**
       * The passthrough engine the SELECT reads from; absent on a chained build.
       * Its presence (not its value) enables the partitioned path's memory
       * bounds — ordered insert, single thread, flush threshold — since the
       * failure they prevent is the writer's, whatever the source.
       */
      sourceType?: FederatedSourceType;
   } = {},
): Promise<WireColumn[]> {
   if (partitionColumns.length === 0) {
      await session.runSQL(
         `CREATE OR REPLACE TABLE ${quotedTablePath} AS (${selectSQL})`,
      );
      return await describeOrDrop(session, quotedTablePath);
   }

   // IN ONE TRANSACTION: the physical name is stable across generations
   // (`selfAssignTableName`), so a rebuild targets the table being SERVED. Run
   // bare, `WITH NO DATA` would serve zero rows until the INSERT lands, and a
   // failed INSERT would lose the previous generation; a ROLLBACK keeps it.
   //
   // The layout only means something because the session disabled inlining
   // (`ducklake_default_data_inlining_row_limit=0`); inlined rows have no files.
   const columns = partitionColumns
      .map((name) => quoteIdentifier(name, STORAGE_TARGET_DIALECT))
      .join(", ");
   // One switch: `off` on the threshold disables all three bounds, which only
   // work together.
   const flushThreshold = getPartitionedWriteFlushThreshold();
   const bounded =
      options.sourceType !== undefined && flushThreshold !== undefined;
   if (bounded) {
      // Bounds the rows each appender holds before flushing to partition files
      // (see getPartitionedWriteFlushThreshold). The session is this build's
      // own instance, so the setting reaches nothing else.
      await session.runSQL(
         `SET partitioned_write_flush_threshold = ${flushThreshold}`,
      );
      // One appender: a sorted result is read in parallel, so with several
      // threads each meets every partition again and the insert runs out of
      // memory. Costs scan parallelism only on the labelled BigQuery split
      // (`bigquery_scan` is multi-stream). `SET threads` is NOT transactional,
      // so the RESET in the finally is what restores it after a rollback.
      await session.runSQL("SET threads = 1");
   }
   let schema: WireColumn[];
   await session.runSQL("BEGIN TRANSACTION");
   try {
      await session.runSQL(
         `CREATE OR REPLACE TABLE ${quotedTablePath} AS (${selectSQL}) WITH NO DATA`,
      );
      await session.runSQL(
         `ALTER TABLE ${quotedTablePath} SET PARTITIONED BY (${columns})`,
      );
      // See orderByPartitionColumns.
      const inserted = bounded
         ? orderByPartitionColumns(
              selectSQL,
              partitionColumns,
              STORAGE_TARGET_DIALECT,
           )
         : selectSQL;
      await session.runSQL(`INSERT INTO ${quotedTablePath} (${inserted})`);
      // Read back INSIDE the transaction, not via `describeOrDrop`: after COMMIT
      // a drop would delete the generation the previous manifest names. Here a
      // failed read-back rolls back and the previous generation survives.
      schema = await describeTable(session, quotedTablePath);
      await session.runSQL("COMMIT");
   } catch (buildErr) {
      // Restores the previous generation. A failed rollback must not replace
      // the error that caused it.
      try {
         await session.runSQL("ROLLBACK");
      } catch (rollbackErr) {
         logger.warn(
            "Failed to roll back a partitioned storage build; the served " +
               "generation may have been replaced",
            {
               table: quotedTablePath,
               error: errMessage(rollbackErr),
               cause: errMessage(buildErr),
            },
         );
      }
      throw buildErr;
   } finally {
      // Best-effort: the session is disposed with the build, so a stuck setting
      // dies with it.
      if (bounded) {
         try {
            await session.runSQL("RESET threads");
         } catch {
            // see above
         }
      }
   }
   return schema;
}

/** Read the built table's schema back, dropping the table if that fails. */
async function describeOrDrop(
   session: DuckDBConnection,
   quotedTablePath: string,
): Promise<WireColumn[]> {
   try {
      return await describeTable(session, quotedTablePath);
   } catch (describeErr) {
      await dropStranded(session, quotedTablePath, describeErr);
      throw describeErr;
   }
}

/**
 * Drop a table a failed build left committed, on the session whose read-write
 * attach created it. Best-effort: a failed drop is logged, never raised over
 * the actual failure.
 */
async function dropStranded(
   session: DuckDBConnection,
   quotedTablePath: string,
   cause: unknown,
): Promise<void> {
   try {
      await session.runSQL(`DROP TABLE IF EXISTS ${quotedTablePath}`);
   } catch (dropErr) {
      logger.warn(
         "Failed to drop a storage table stranded by a failed build " +
            "(physical leak)",
         {
            table: quotedTablePath,
            error: errMessage(dropErr),
            cause: errMessage(cause),
         },
      );
   }
}

async function describeTable(
   session: DuckDBConnection,
   quotedTablePath: string,
): Promise<WireColumn[]> {
   const result = await session.runSQL(`DESCRIBE ${quotedTablePath}`);
   const rows = Array.isArray(result) ? result : (result.rows ?? []);
   const columns: WireColumn[] = [];
   for (const row of rows as Record<string, unknown>[]) {
      const name = row.column_name;
      const type = row.column_type;
      if (typeof name === "string" && typeof type === "string") {
         columns.push({ name, type });
      }
   }
   return columns;
}
