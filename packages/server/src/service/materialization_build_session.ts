// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   type PersistSource,
   Runtime,
} from "@malloydata/malloy";
import { mkdirSync, mkdtempSync, rmSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import type { components } from "../api";
import { BadRequestError, MaterializationEligibilityError } from "../errors";
import { logger } from "../logger";
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
   type BuildReadCost,
} from "./build_read_cost";
import { recordAttributionSkipped } from "../materialization_metrics";
import type { QueryMetadata } from "./query_metadata";
import {
   applySessionResourceLimits,
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
 * Created on first use and deliberately never disposed: it holds no attach and
 * no credentials, and recreating it per build was the single largest source of
 * per-build memory growth.
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
 * The Malloy `dialectName` every storage destination speaks, since both types it
 * may be are DuckDB-family. Named because it is the dialect a stored table's DDL,
 * DML and identifier quoting are all written in, which is NOT the source's — and
 * a bare "duckdb" at each of those sites reads like a default rather than the one
 * fact it is.
 */
export const STORAGE_TARGET_DIALECT = "duckdb";

/**
 * Wrap a source-dialect SELECT as a native per-engine query-passthrough call so
 * a build-scoped DuckDB session can execute it against the source warehouse and
 * stream back only result-sized rows. The compiled SELECT is embedded verbatim
 * as a SQL string literal (single-quote-escaped) — the warehouse computes; the
 * base tables never enter DuckDB. `handle` is the per-engine reference
 * {@link federateSourceForPassthrough} established on the session (a projectId,
 * a secret name, or an ATTACH alias). Argument ORDER differs per engine, which
 * is the whole reason this is centralized.
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
 * Tag every subsequent read on a Snowflake build session with this build's
 * query metadata.
 *
 * Snowflake only, and by capability rather than preference: the passthrough
 * reuses ONE session across `snowflake_query()` calls, so a session-level
 * `QUERY_TAG` reaches the read that follows. BigQuery has no equivalent —
 * `bigquery_query()` accepts no labels parameter and cannot run the script that
 * would set one — so its labelling is part of how the read itself is issued.
 * Postgres has no per-statement tag to set.
 *
 * <b>Best-effort.</b> A build that produces correct rows must not fail because
 * the warehouse refused a tag; the cost is attribution for this one build, which
 * the log line makes diagnosable.
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
 * Can this connection reach the job records the split depends on?
 *
 * The split locates its result table by listing the executed script's child job,
 * which is an API surface the direct `bigquery_query()` path never touches — so
 * turning attribution on would otherwise add a permission requirement to builds
 * that previously needed none, and discover it only AFTER a read had been billed.
 * This asks the question with a one-row listing, before anything runs.
 *
 * <b>A false answer costs attribution, not the build.</b> That is the invariant
 * this whole path is meant to keep, and it is the reason the check exists rather
 * than a try/catch further down: once the read has run, falling back is no longer
 * free — the rows live only in a table this cannot find, so the alternative to
 * failing would be paying for the read a second time.
 *
 * Not cached: it is one listing per build against an API the build is about to
 * use anyway, and caching it would have to key on the connection's credentials
 * and survive their rotation.
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
 * The distinction is the whole point of the type: everything else this path
 * throws happens before anything is charged, and can be retried for the price of
 * a retry. This cannot — the read is paid for, and its rows live in an anonymous
 * table the build could not locate, so retrying pays for the same read twice.
 * A caller with a retry policy should treat it as a decision for an operator
 * rather than something to re-drive automatically.
 *
 * It is not degradable to "no cost". Without the job record there is no result
 * table, so there are no rows to capture — the failure is the build's, not the
 * telemetry's, which is why this path throws where the Snowflake one returns null.
 */
export class BilledReadNotCapturedError extends Error {
   constructor(message: string, options?: { cause?: unknown }) {
      super(message, options);
      this.name = "BilledReadNotCapturedError";
   }
}

/** How many times to re-ask for the job record before giving up on the build. */
const BIGQUERY_LOOKUP_ATTEMPTS = 3;
const BIGQUERY_LOOKUP_BACKOFF_MS = 250;

/**
 * Re-ask for the job record a few times before failing a build whose read has
 * already been billed.
 *
 * The capability probe establishes that this connection CAN list jobs, so a
 * failure here is a transient — a 5xx, a quota trip, a credential rotated between
 * the probe and now — rather than a standing permission answer. Those are worth a
 * retry precisely because the alternative is so expensive: the read is paid for
 * and cannot be re-issued for free.
 *
 * Bounded and short. This runs while the build session still holds federated
 * credentials, and a warehouse that is genuinely down should surface as a failed
 * build rather than a long stall.
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
   // Names the parent job because this is the failure that leaves a read paid for
   // and uncaptured, and the parent is the only handle on it: a script's child
   // job is absent from the default listing. Telling an operator a read was
   // billed without saying which one leaves them nothing to act on.
   throw new BilledReadNotCapturedError(
      `Could not read the BigQuery job record for a build's read (parent job ` +
         `${parentJobId}) after ${BIGQUERY_LOOKUP_ATTEMPTS} attempts, so its ` +
         `rows cannot be captured: ` +
         `${lastError instanceof Error ? lastError.message : String(lastError)}`,
      { cause: lastError },
   );
}

/**
 * What the CTAS reads from, and the warehouse's own id for the read when the
 * shape of that read yielded one.
 *
 * `jobId` is what makes a build's cost recoverable by an EXACT key rather than by
 * matching recorded query text, so it is carried out of here even though the
 * build itself does not need it.
 */
export interface PassthroughRead {
   /** The SELECT the CTAS wraps. */
   selectSQL: string;
   /** The warehouse's id for the read, null when this shape does not produce one. */
   jobId: string | null;
   /**
    * What the read cost, when the shape that issued it also reported it.
    *
    * Read from the SAME job record that located the result table rather than by a
    * later lookup, because a later lookup is not available: a script's child job
    * is absent from the default `bigquery_jobs()` listing and reachable only
    * through its parent, so its id resolves to nothing on its own.
    */
   cost: BuildReadCost | null;
}

/**
 * Issue the source-warehouse read for a `storage=` build and return what the
 * CTAS should select from.
 *
 * Two shapes, and which one runs is decided by whether there is a BigQuery label
 * to apply:
 *
 * <ul>
 *   <li><b>Direct</b> — `wrapPassthrough`'s single rows-returning call. Every
 *       engine, and BigQuery too when there is nothing to label. Returns no job
 *       id: no passthrough hands one back for a call that returns rows.
 *   <li><b>Split (BigQuery, labelled)</b> — `bigquery_execute` runs the SELECT
 *       inside a script that sets `@@query_label` first, then the anonymous result
 *       table that job wrote is read with `bigquery_scan`. `bigquery_query()`
 *       cannot serve this: it takes no labels parameter, and it cannot run a
 *       script (a script returns no result schema), so labelling BigQuery is not
 *       available without splitting the read.
 * </ul>
 *
 * <b>The split is not a second scan.</b> Reading the anonymous result table goes
 * through the Storage Read API and creates no new QUERY job — measured by
 * diffing the project's QUERY job ids across the read. `bigquery_query()` is
 * almost certainly doing the same two steps internally.
 *
 * Gating on the label rather than on the engine keeps the unlabelled path
 * byte-identical to what it was, so the blast radius of the split is exactly the
 * set of builds that asked to be attributed.
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
   // Asked BEFORE anything runs, so an unusable split costs a probe rather than a
   // build. Past this point the read has been issued and billed, and there is no
   // longer a cheap way back: without the job record the result table cannot be
   // located, so the rows cannot be captured at all and re-issuing them through
   // the direct shape would bill the same read twice.
   if (label === undefined || !(await canSplitBigQueryRead(session, handle))) {
      return {
         selectSQL: wrapPassthrough(sourceType, handle, buildSQL),
         jobId: null,
         cost: null,
      };
   }

   // The label is set by a statement of its own, so the script has two: BigQuery
   // creates a parent job for the script and one child per statement, and it is
   // the CHILD that ran the SELECT which carries both the label and the cost.
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

   // The destination table is read from the job record rather than constructed:
   // it is an anonymous table whose name BigQuery chooses.
   //
   // Ordered NEWEST first because the script has more than one statement and
   // nothing documents the order a `parentJobId` listing returns them in. Taking
   // the newest with a destination table takes the SELECT, which is the last
   // statement and the one whose rows this is after. (Measured, `SET
   // @@query_label` produces no such child — but that is behaviour, not a
   // guarantee, and this costs nothing to pin.)
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
         // Inside the retry on purpose: a listing can answer 200 without having
         // enumerated the child yet, which is the same transient as a 5xx and is
         // indistinguishable from it here. Throwing plainly lets the backoff take
         // it; only the LAST attempt becomes a terminal failure.
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
 * on its session.
 *
 * Runs AFTER the read, because the history has to contain it. Scoped by the TAG
 * rather than `LAST_QUERY_ID()`: the session is not exclusively this build's, as
 * the ADBC driver issues `SELECT 1` connection probes around each passthrough
 * call — measured against a live account, `LAST_QUERY_ID()` after a build-shaped
 * read returned a probe rather than the read, which would have reported a probe's
 * zero cost for every Snowflake build.
 *
 * <b>Best-effort.</b> The rows are already built and captured, so a lookup that
 * fails costs a number, never the build.
 */
async function snowflakeReadCostAfterBuild(
   session: DuckDBConnection,
   sourceType: FederatedSourceType,
   handle: string,
   buildSQL: string,
   queryMetadata: QueryMetadata | undefined,
   /**
    * The source connection's configured database, absent when it has none.
    * Snowflake supports that shape, and `INFORMATION_SCHEMA` is per-database, so
    * the lookup has to be told where to resolve it — see {@link snowflakeCostSQL}.
    */
   database: string | null | undefined,
): Promise<BuildReadCost | null> {
   if (sourceType !== "snowflake") return null;
   // Untagged there is nothing to scope the history by — and nothing asked to be
   // attributed in the first place.
   const tag = snowflakeQueryTagValue(queryMetadata);
   if (tag === undefined) return null;
   try {
      const costResult = await session.runSQL(
         passthroughSnowflake(snowflakeCostSQL(tag, database), handle),
      );
      const candidates = resultRows(costResult);
      const row = pickSnowflakeReadRow(candidates, buildSQL);
      if (row === null) {
         // Counted, not just absent. This is the likeliest Snowflake miss —
         // it turns on QUERY_TEXT matching buildSQL byte for byte after a round
         // trip through the driver — and without a signal it is invisible.
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

function str(value: unknown): string | null {
   return typeof value === "string" && value.length > 0 ? value : null;
}

/**
 * Clear the session's `QUERY_TAG` before the session is released.
 *
 * The build session is private and disposed here, so this is not about leaking a
 * tag to a later build. It covers the one case the tag statement is allowed to
 * fail: {@link tagSnowflakeSession} is best-effort, and whether the driver hands
 * this build a session that was ALREADY tagged is the driver's business rather
 * than something this can observe. A failed tag on a pre-tagged session would
 * leave the read attributed to whatever set it last, and wrong attribution is
 * worse than none — nothing about it looks wrong.
 *
 * Best-effort in turn, and last: a build that produced correct rows must not fail
 * on its own cleanup.
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
 * that closes it and removes the throwaway working directory that pins its
 * instance identity.
 *
 * Isolation is load-bearing for multi-tenant safety. `@malloydata/db-duckdb`
 * pools DuckDB instances in a process-global cache keyed by a "share key" that
 * deliberately EXCLUDES the connection name, and it never gives a `:memory:`
 * primary a private instance — so build sessions built with identical default
 * config get pooled onto ONE shared in-memory instance whenever their lifetimes
 * overlap (notably with the long-lived serve connection). Pooled together, they
 * collide on their transient ATTACH aliases: two builds (or two environments
 * with same-named-but-different-credential source/destination connections) both
 * `ATTACH … AS orders_pg` / `AS lake` on the shared instance and one clobbers
 * the other — a cross-tenant read (source) or WRITE (read-write destination).
 * DuckDB itself isolates SEPARATE instances cleanly; the collision is purely an
 * artifact of the shared pool.
 *
 * Fix: give each session a UNIQUE `workingDirectory`, which is part of the share
 * key, so it lands its OWN in-memory instance — its own catalog, secrets, and
 * attaches, torn down on close. Crucially `databasePath` stays exactly
 * `:memory:` (NOT a temp file): a `:memory:` primary lets a DuckLake attach
 * auto-initialize a fresh catalog, whereas a file primary does not. The working
 * directory started out only as a way to make the share key unique — every real
 * path the build uses is absolute — and it is also where the caller points
 * DuckDB's `temp_directory`, so it holds spill for the life of the build and
 * nothing else. Removed on dispose (best-effort; a leftover dir is benign).
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
      // Row limit lifted off db-duckdb's `DEFAULT_QUERY_OPTIONS.rowLimit` of 10,
      // which `runSQL` enforces by slicing the result. Every read this session
      // makes is metadata about the table it just wrote — `duckdb_columns()` and
      // `DESCRIBE` both return one row per COLUMN — so the default silently
      // truncates an 11-column table's shape. Downstream that reads as columns
      // missing from the table: the delta path calls it a shape mismatch and
      // rebuilds in full forever, and the manifest declares a short schema for
      // the serve transform to bind.
      session = new DuckDBConnection(sessionName, ":memory:", workDir, {
         rowLimit: Number.MAX_SAFE_INTEGER,
      });
   } catch (err) {
      rmSync(workDir, { recursive: true, force: true });
      throw err;
   }
   const dispose = async () => {
      // `close()` alone does not end the session. It refcount-decrements the
      // DuckDBInstance, but the node-api Connection holds a C++ ClientContext that
      // keeps the refcount above zero — so the in-memory database survives, and with
      // it the destination's ATTACH and the source's federated credentials. Measured:
      // 2 idle Postgres backends stranded per build, freed only at process exit,
      // where the whole point of a per-build session is that they exist only for
      // the build.
      //
      // db-duckdb cannot do this itself: 0.0.389 disconnected unconditionally and
      // had to be reverted (PR #2793) because malloy's translate layer holds
      // weak_ptrs to the C++ Connection and the language server segfaulted. A build
      // session is different in the way that matters — server-owned, single-use, and
      // nothing outside this function ever sees it — so it can release what a
      // general-purpose connection cannot. Reached through a cast because the handle
      // is library-internal; if db-duckdb ever grows an explicit hard-close, use it.
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
 * A `TIMESTAMP WITH TIME ZONE` column compared against a naive `TIMESTAMP`
 * literal is resolved in the SESSION's zone, and every bound the incremental path
 * renders is UTC text — so a session on the host's local zone selects a window
 * offset by that host's offset. Silently: the delta commits, the rows are simply
 * the wrong ones, and a boundary recorded from a host east of UTC would LEAD the
 * table, the one direction the ledger must never move in.
 *
 * Malloy's DuckDB connector already sets UTC at setup, so this is insurance
 * rather than a fix — issued anyway because a correctness property that lives in
 * another package's setup path is one nothing here would notice losing.
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
    * What the source warehouse charged for the read, when the read's shape
    * reported it. Null is never "free" — see {@link PassthroughRead.cost} for
    * which shapes report and which do not.
    */
   readCost: BuildReadCost | null;
   /**
    * Present only when the table was advanced IN PLACE rather than rebuilt, which
    * is what makes the two outcomes distinguishable from the outside: both leave a
    * correct table and a captured schema, and only this says which one ran.
    */
   refresh?: StorageRefreshOutcome;
   /**
    * Where the table a REBUILD wrote reaches, for an incremental source that was
    * seeded rather than advanced. This is the boundary that turns the NEXT refresh
    * into a delta, so it has to reach the manifest entry.
    */
   seededThrough?: WatermarkBound;
}

/** What an in-place refresh did, for the manifest entry to report. */
export interface StorageRefreshOutcome {
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
 * federated.
 *
 * A callback rather than a parameter block because the decision belongs to the
 * caller and the SESSION belongs here: the plan has to probe the destination
 * before it can choose, and the probes need this session — while the credentials
 * it holds must not outlive the one source's refresh. Returning undefined means
 * "rebuild it", and the seed then proceeds on this same session rather than
 * paying for a second attach and a second credential federation.
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
    * be a delta, and return that boundary for the manifest entry to report. Runs
    * on the same session, after the CTAS, because the boundary is probed from the
    * table itself — and the session holding the destination is gone by the time
    * this function returns.
    *
    * Undefined when the rebuilt table has no boundary to record (an empty source),
    * which leaves the next refresh to rebuild again.
    */
   afterSeed: (deps: {
      session: DuckDBConnection;
      quotedTablePath: string;
   }) => Promise<WatermarkBound | undefined>;
}

/**
 * Build one persist source into a `storage=` destination via native
 * query-passthrough CTAS, on a *build-scoped* DuckDB session that is created,
 * used, and disposed here — so the read-write destination attach and the source
 * credential federation exist ONLY for the build's duration and NEVER on the
 * serve connection (the least-privilege boundary the design requires).
 *
 * Steps: create a private build session → attach the destination read-write
 * → federate the source's credentials on demand → `CREATE OR REPLACE TABLE`
 * (qualified with the destination catalog) whose FROM is the source passthrough
 * → capture the built table's authoritative schema (DESCRIBE) → dispose the
 * session (releasing every credential/attach). DuckLake's catalog swap is
 * transactional, so the replace is atomic. The compiled SELECT (source dialect)
 * is unchanged; only where it executes and where the result lands move.
 *
 * @returns the destination connection name and the captured authoritative
 *   schema, both recorded on the manifest entry for the serve transform.
 */
export async function buildSourceIntoStorage(params: {
   destinationName: string;
   destinationConnection: ApiConnection;
   sourceConnection: ApiConnection;
   /** The source-dialect compiled SELECT (v0's getSQL output), verbatim. */
   buildSQL: string;
   /** Logical, unquoted physical table path (may carry a container path). */
   physicalTableName: string;
   environmentPath: string;
   /**
    * Per-query metadata for the warehouse read, already resolved through the
    * same layering the colocated path uses.
    *
    * It has to be applied HERE rather than by a Malloy connector, which is the
    * whole reason this parameter exists: the read runs inside DuckDB's native
    * query-passthrough, so no connector is in the call path to render the bag
    * onto the statement. Without this, a `storage=` build is the one kind of
    * warehouse work the deployment cannot attribute.
    */
   queryMetadata?: QueryMetadata;
   /**
    * Offered the chance to advance this table in place before the CTAS below
    * runs. Absent for every non-incremental source, which is the common case and
    * leaves this function exactly the full build it was.
    */
   incremental?: StorageIncrementalRefresh;
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

   assertSupportedDestination(destinationName, destinationConnection);
   const sourceType = passthroughSourceType(sourceConnection);

   // A dedicated, disposable build session on its OWN in-memory instance, so its
   // read-write destination attach and federated source credentials cannot be
   // pooled onto — or collide with — any other build/serve connection (see
   // createIsolatedBuildSession).
   const { session, dispose, workDir } = createIsolatedBuildSession(
      `build_${destinationName}`,
   );
   // Visible to the finally, which clears the session tag before release.
   let federatedHandle: string | undefined;
   try {
      // FIRST, before the destination attach: the attach is what carries a
      // DuckLake session into the shared funnel, which applies the limits without
      // a `tempDirectory` and latches them. Running after it therefore lost this
      // build its own disposable directory on exactly the destination type that
      // reaches production, sending spill to the shared configured path instead —
      // outliving `dispose`, and collidable between concurrent builds. Ordering
      // it here also puts the memory bound in effect during the attach itself,
      // and matches `dropStorageTable`.
      await applySessionResourceLimits(session, { tempDirectory: workDir });
      await attachDestinationReadWrite(
         session,
         destinationName,
         destinationConnection,
         environmentPath,
      );

      if (destinationConnection.type === "ducklake") {
         // Write-path: disable DuckLake row inlining for this build session so
         // materialized rows land as Parquet in the data store, NOT inlined into
         // the catalog database. Materializations are generally large, and in a
         // shared/multitenant catalog inlining would push tenant data into the
         // catalog Postgres. Session-scoped (this build only) — it does not
         // mutate the shared catalog's persisted options.
         await session.runSQL("SET ducklake_default_data_inlining_row_limit=0");
      }
      // Before any predicate is rendered against this session. Matters only to an
      // incremental refresh (a full CTAS issues none), but it is cheap and the
      // session it protects is this one — see pinSessionToUTC.
      await pinSessionToUTC(session);

      const federated = await federateSourceForPassthrough(
         session,
         sourceType,
         sourceFederationConfig(sourceConnection),
      );

      await tagSnowflakeSession(
         session,
         sourceType,
         federated.handle,
         queryMetadata,
      );

      // The CTAS target MUST be qualified with the destination catalog (the
      // attach alias) — an unqualified name lands in the build session's own
      // (private, throwaway) in-memory catalog, not the DuckLake/DuckDB store,
      // so the rows would be captured for the schema and then vanish with the
      // session. DuckLake's
      // catalog swap is transactional, so CREATE OR REPLACE is atomic — no
      // staging/rename dance needed on this path.
      // quoteManifestTablePath, NOT quoteTablePath: the serve side renders this same
      // path with the manifest rule, which passes an already-quoted name through
      // unchanged. Quoting unconditionally here would create a table whose NAME
      // contains the author's quote characters while the serve side asks for the
      // unquoted-inner form — and because that mismatch surfaces at shape RUN time,
      // past the compile-time live fallback, every query on the source 400s. For an
      // unquoted name the two functions are identical, so nothing else moves.
      const target = quoteManifestTablePath(
         `${destinationName}.${physicalTableName}`,
         STORAGE_TARGET_DIALECT,
      );

      // Advance in place, if the caller's plan says this refresh can be a bounded
      // delta. Offered here — after the attach and the federation, before the
      // CTAS — because the plan has to probe this destination to decide, and a
      // seed then continues below on the same session.
      if (params.incremental) {
         const refreshed = await params.incremental.plan({
            session,
            sourceType,
            handle: federated.handle,
            quotedTablePath: target,
         });
         if (refreshed) {
            return {
               storageDestinationName: destinationName,
               // Read back even though a delta cannot change the shape (a
               // definitional change re-addresses the source, and a shape that
               // drifted anyway forces a seed): the manifest entry declares this
               // schema to the serve transform on every publication, so an entry
               // that omitted it would leave the source unservable from the tier
               // until its next full build.
               schema: await describeTable(session, target),
               readCost: refreshed.readCost,
               refresh: refreshed,
            };
         }
      }

      const read = await issuePassthroughRead(
         session,
         sourceType,
         federated.handle,
         buildSQL,
         queryMetadata,
      );

      // Capture the authoritative schema from the freshly-built table — the
      // serve transform declares exactly this, and the compiler does not
      // type-check a virtual source's declared columns.
      const schema = await createTableAndDescribe(
         session,
         target,
         read.selectSQL,
      );
      // The table now holds a full snapshot, so record where that snapshot
      // reaches: this is what turns the NEXT refresh into a delta. On this
      // session, because the boundary is probed from the table it just wrote and
      // the destination attach does not outlive this function.
      const seededThrough = await params.incremental?.afterSeed({
         session,
         quotedTablePath: target,
      });

      return {
         seededThrough,
         storageDestinationName: destinationName,
         schema,
         // BigQuery reported its cost while issuing the read, because the shape
         // that carries the label also returns the job. Snowflake's read is
         // unchanged, so it can only be asked once the read has run — which is
         // here, while the session still holds the credentials.
         readCost:
            read.cost ??
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
      // Dispose closes the private instance (releasing every secret + attach —
      // nothing federated or read-write survives the build) and removes its
      // throwaway working directory.
      await dispose();
   }
}

/**
 * Build a CHAINED `storage=` source by reading its already-materialized
 * upstream(s) from the destination store — the "stack on the parent" path —
 * instead of recomputing the upstream from raw against the source
 * warehouse. Reuses the parent's work and is consistent-by-construction
 * (the downstream is a pure function of the parent's STORED rows).
 *
 * The `transientModel` (assembled by the caller from the serve-shape rebind
 * machinery, see {@link buildChainedStorageBuildModel}) rebinds every upstream to
 * a virtual source on THIS destination and re-declares the downstream as a
 * persist source over them. We compile it against the build session (the only
 * connection it references is the attached destination), read the downstream
 * persist source's `getSQL({ virtualMap })` — the exact mirror of the warehouse
 * build's `getSQL`, only the SQL now reads the attached lake tables — and CTAS
 * the result into the destination. No source federation, no passthrough: nothing
 * leaves DuckDB. The `virtualMap` maps each upstream's handle to its (quoted)
 * physical lake path.
 *
 * Session lifecycle mirrors {@link buildSourceIntoStorage} exactly (create →
 * attach read-write → build → dispose), so no read-write attach survives the
 * build, and the same authoritative-schema capture applies.
 *
 * @returns the destination connection name and the captured authoritative schema.
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
      // FIRST, before the destination attach: the attach is what carries a
      // DuckLake session into the shared funnel, which applies the limits without
      // a `tempDirectory` and latches them. Running after it therefore lost this
      // build its own disposable directory on exactly the destination type that
      // reaches production, sending spill to the shared configured path instead —
      // outliving `dispose`, and collidable between concurrent builds. Ordering
      // it here also puts the memory bound in effect during the attach itself,
      // and matches `dropStorageTable`.
      await applySessionResourceLimits(session, { tempDirectory: workDir });
      await attachDestinationReadWrite(
         session,
         destinationName,
         destinationConnection,
         environmentPath,
      );
      if (destinationConnection.type === "ducklake") {
         // Same rationale as buildSourceIntoStorage: keep materialized rows out
         // of the shared catalog database.
         await session.runSQL("SET ducklake_default_data_inlining_row_limit=0");
      }
      // Pinned here as well as on the non-chained path, which is the only other
      // place a build session is created. No predicate or boundary probe runs on
      // this one today — a chained source is excluded from the delta path — so
      // this guards the case that lifting that exclusion creates, in the one
      // function where its absence would not be obvious.
      await pinSessionToUTC(session);

      // Compile the transient rebind model against the build session. Its only
      // connection is the attached destination; the upstream virtual sources
      // declare their captured schema, so compiling reads no tables (schema-on-
      // faith) — the CTAS below runs the real SQL against the attached lake.
      const root = "file:///chained-build/";
      const url = `${root}m.malloy`;
      const runtime = new Runtime({
         urlReader: new InMemoryURLReader(new Map([[url, transientModel]])),
         connections: new FixedConnectionMap(
            new Map([[destinationName, session]]),
            destinationName,
         ),
      });
      // Compiling the transient model is the only SHAPE step in this function:
      // a failure means the downstream cannot be expressed over its rebound
      // parents, which is a legitimate reason to fall back and recompute from
      // raw. Everything around it — the attach, the CTAS, the DESCRIBE — is
      // infrastructure, where falling back would just fail the same way against
      // the same destination. Marking the shape failures lets the caller tell
      // them apart.
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
      const plan = model.getBuildPlan();
      let downstream: PersistSource | undefined;
      for (const ps of Object.values(plan.sources)) {
         if (ps.name === downstreamName) {
            downstream = ps;
            break;
         }
      }
      if (!downstream) {
         // The downstream didn't survive as a persist source — its definition
         // references something the rebind model doesn't provide (a parent
         // refinement not carried, a live leaf). The caller falls back.
         throw new MaterializationEligibilityError({
            message:
               `Chained build model did not yield a persist source named ` +
               `'${downstreamName}' (the downstream references something the ` +
               `rebound parents don't provide).`,
         });
      }
      // The downstream's materialization SQL, over the rebound parents — DuckDB
      // dialect, reading the attached lake tables via the virtualMap. Project to
      // the public columns so a hidden (`except:` / access-restricted) downstream
      // column is not materialized — same rationale as the single-source build:
      // keeping hidden values out of the store at rest.
      const sql = projectToPublicColumns(
         downstream,
         downstream.getSQL({ virtualMap }),
      );

      // Same write/read mirror as the single-source build above.
      const target = quoteManifestTablePath(
         `${destinationName}.${physicalTableName}`,
         STORAGE_TARGET_DIALECT,
      );
      const schema = await createTableAndDescribe(session, target, sql);

      return {
         storageDestinationName: destinationName,
         schema,
         // A chained build reads its parent's already-materialized table out of
         // the destination store, so it touches no source warehouse and there is
         // nothing to account for. Null here means "did not spend", which is the
         // one place in this file where it does.
         readCost: null,
      };
   } finally {
      await dispose();
   }
}

/**
 * Build-time servability gate (the portable-DuckDB eligibility check, deferred
 * from the pre-build pass because it needs the POST-build schema): compile the
 * source's serve-shape in DuckDB against the captured schema and REFUSE the
 * build if it does not compile. The served table lives in DuckDB, so a source
 * authored against a warehouse must have a DuckDB-portable served shape — this
 * turns a serve-time execution error into a build-time refusal (HTTP 422),
 * running as part of stage→validate, the same step that captured the schema.
 *
 * The compile is schema-on-faith (the compiler does not type-check a virtual
 * source's columns and no SQL runs), so a throwaway in-memory DuckDB connection
 * suffices — no attach, no table read, nothing federated.
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
   // No `origin`, and none is needed. This gate compiles a throwaway shape from
   // the CAPTURED schema alone — no author-model lookup, no composite, no
   // refinements — so it asks the same question of a rollup's table as of an
   // authored one: do these columns form a valid DuckDB virtual source. It reads
   // the binding as an opaque (handle, table, destination) triple, which is the
   // property that makes a consumer origin-neutral.
   const binding: ServeBinding = {
      sourceName,
      destinationName,
      virtualHandle,
      tablePath: `${destinationName}.${physicalTableName}`,
      schema: params.schema
         .filter((c) => c.name && c.type)
         .map((c) => ({ name: c.name as string, type: c.type as string })),
   };
   // ONE session for the process, not one per build. The build and GC sessions
   // need their own instance because they ATTACH a destination read-write and
   // federate customer credentials; this gate does neither — it compiles a
   // throwaway serve shape against a captured schema — so it has no cross-tenant
   // collision surface to isolate. A fresh instance per build cost ~5.9MB of RSS
   // per build on the production image, roughly three quarters of the build
   // path's total growth, and it is never reclaimed.
   //
   // The risk of sharing is a shared CATALOG: every compile declares a virtual
   // source into it, so a later shape could in principle pass on declarations
   // left by an earlier one. Pinned in the spec — refusals still refuse after 25
   // successful compiles, a refusal does not poison the session, and the same
   // handle recompiled with a different schema sees the new one.
   // Assigned synchronously, deliberately. An `await` between the check and the
   // assignment lets two concurrent callers both pass the check and both build a
   // session, and the loser is an orphaned connection and temp directory that
   // nothing disposes for the life of the process — and because the limits call
   // is `async` even when nothing is configured, that window would be open on
   // every deployment rather than only opted-in ones.
   sharedGateSession ??= createIsolatedBuildSession("gate_shared").session;
   // Bounded on every use rather than at creation, which is what keeps the line
   // above synchronous: the call is idempotent per connection, so this is one
   // WeakMap hit after the first compile. This is the instance that most needs
   // bounding — process-wide and deliberately never disposed, so the
   // longest-lived DuckDB instance here — and it reaches `assertServesInDuckDB`
   // through a `FixedConnectionMap` that builds a Runtime directly, with no
   // connection lookup and no attach, so neither hook that bounds the other
   // sessions ever sees it.
   //
   // No session-owned spill directory, unlike a build: this session outlives
   // every build, so a directory of its own would accumulate for the life of the
   // process with nothing to remove it. It uses the configured one, which is
   // where an operator who set it wants spill to land.
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
 * Drop one materialized table from a `storage=` destination, on a build-scoped
 * read-write session with its OWN in-memory instance (the SERVE attach is
 * read-only, so GC cannot run there; and an isolated instance keeps this GC's
 * read-write destination attach from colliding with another tenant's same-named
 * destination — see {@link createIsolatedBuildSession}). Mirrors
 * {@link buildSourceIntoStorage}'s lifecycle: attach read-write → drop → dispose.
 *
 * Only the recorded physical table (`physicalTableName`) is dropped — a
 * destination-aware drop of a name the publisher recorded building, never a
 * catalog scan — so GC can never take out a table it did not create.
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
      // Bounded like the build sessions even though a DROP allocates almost
      // nothing. DuckDB does not reserve the limit up front, so this instance
      // costs little while idle; what it carries is a CAP, and it is the sum of
      // the caps across every live instance that has to fit the container.
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

/** Attach the destination connection read-write on the build session. */
async function attachDestinationReadWrite(
   session: DuckDBConnection,
   destinationName: string,
   destinationConnection: ApiConnection,
   environmentPath: string,
): Promise<void> {
   if (destinationConnection.type === "ducklake") {
      const cfg = destinationConnection.ducklakeConnection;
      if (!cfg) {
         throw new BadRequestError(
            `Storage destination '${destinationName}' is type 'ducklake' but ` +
               `has no ducklakeConnection config.`,
         );
      }
      await attachDuckLakeReadWrite(session, destinationName, cfg);
      return;
   }
   // Plain DuckDB destination: attach its database file read-write. The file
   // path is derived the same way connection assembly derives it — under the
   // destinations root, which is where a destination's files live so they cannot
   // collide with a connection of the same name. The directory is created here
   // because a build can be the first thing that ever touches it: on a worker
   // that has not served this destination, nothing else has made it yet.
   const destinationRoot = storageDestinationRoot(environmentPath);
   mkdirSync(destinationRoot, { recursive: true });
   const dbPath = path.join(destinationRoot, `${destinationName}.duckdb`);
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
} {
   return {
      name: sourceConnection.name ?? "src",
      bigqueryConnection: sourceConnection.bigqueryConnection,
      snowflakeConnection: sourceConnection.snowflakeConnection,
      postgresConnection: sourceConnection.postgresConnection,
   };
}

/**
 * Read the authoritative column schema of a just-built table with `DESCRIBE`,
 * mapping DuckDB's `column_name`/`column_type` rows to the wire {@link Column}
 * shape the manifest carries. This is the schema the serve transform declares.
 */
/**
 * CTAS the table, then read back its authoritative schema — dropping the table
 * again if that read-back fails.
 *
 * The window this closes: the CTAS has committed by the time DESCRIBE runs, and
 * the caller records nothing until this function RETURNS. So a DESCRIBE failure
 * propagates past a committed table that no manifest entry names and that
 * `builtThisRun` never saw — leaving it unreachable by the failed-run reclaim
 * and by manifest-driven GC alike, which only drop names they recorded building.
 * Same class the post-build serve-shape gate already closes with its own
 * targeted drop (see materialization_service), and closed the same way.
 *
 * Best-effort: a failed drop is logged, never raised, and never replaces the
 * DESCRIBE error that is the actual failure. The drop runs on the session that
 * created the table, whose read-write attach is still open.
 */
export async function createTableAndDescribe(
   session: DuckDBConnection,
   quotedTablePath: string,
   selectSQL: string,
): Promise<WireColumn[]> {
   await session.runSQL(
      `CREATE OR REPLACE TABLE ${quotedTablePath} AS (${selectSQL})`,
   );
   try {
      return await describeTable(session, quotedTablePath);
   } catch (describeErr) {
      try {
         await session.runSQL(`DROP TABLE IF EXISTS ${quotedTablePath}`);
      } catch (dropErr) {
         logger.warn(
            "Failed to drop a storage table stranded by a schema read-back " +
               "failure (physical leak)",
            {
               table: quotedTablePath,
               error: errMessage(dropErr),
            },
         );
      }
      throw describeErr;
   }
}

export async function describeTable(
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
