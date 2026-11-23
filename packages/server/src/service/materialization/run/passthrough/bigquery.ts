// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A `storage=` build's BigQuery source, read through DuckDB's `bigquery`
 * extension: each read marked with a comment naming the build session, so a
 * cancelled build can find its jobs and cancel them in BigQuery (DuckDB's
 * interrupt stops only its own wait, and BigQuery runs and bills the job to
 * completion); a read that carries query metadata labelled through the
 * split (`bigquery_execute` runs the SELECT in a script that sets
 * `@@query_label`, then `bigquery_scan` reads the anonymous result table),
 * after a probe that this connection can list the job records the split
 * locates its table by; and the labelled read's cost, read from the same job
 * record. Read through passthrough_build.ts.
 *
 * The extension can neither cancel a job nor choose its id or labels
 * (`bigquery_query` and `bigquery_execute` take neither), so the cancel
 * lists its service account's running jobs through BigQuery's API, under
 * the key the read ran with, and cancels the ones that carry this session's
 * marker, which no other build's can. Best-effort and bounded: a cancel
 * never throws into the run, never waits longer than
 * {@link CANCEL_TIMEOUT_MS}.
 *
 * Effects: SQL in BigQuery through the passthrough on the build session;
 * BigQuery's jobs API on a cancel; logs and attribution-skipped counts.
 */

import { errorCodeOf } from "../../../../error_code";
import { randomUUID } from "node:crypto";
import {
   BigQuery,
   type GetJobsOptions,
   type GetJobsResponse,
   type Job,
} from "@google-cloud/bigquery";
import type { DuckDBConnection } from "@malloydata/db-duckdb";
import type { components } from "../../../../api";
import { logger } from "../../../../logger";
import { recordAttributionSkipped } from "../../../../materialization_metrics";
import { escapeSQL } from "../../../duckdb_session_setup";
import type { QueryMetadata } from "../../../query_metadata";
import {
   bigQueryReadCost,
   resultRows,
   str,
   type BuildReadCost,
} from "../build_read_cost";
import { BIGQUERY_COST_COLUMNS } from "../warehouse_sql";

type ApiConnection = components["schemas"]["Connection"];

/** One running BigQuery job, as the cancel needs it. */
export interface BigQueryActiveJob {
   id: string;
   /** The job's SQL text; undefined for a job that is not a query. */
   query: string | undefined;
   cancel(): Promise<unknown>;
}

/** The slice of BigQuery's jobs API the cancel uses. */
export interface BigQueryJobLister {
   /** The caller's own pending and running jobs created at or after `minCreationTime` (epoch ms). */
   activeJobs(minCreationTime: number): Promise<BigQueryActiveJob[]>;
}

/** Makes the lister, only when a cancel needs one. */
export type BigQueryJobListerFactory = (
   projectId: string,
   serviceAccountKeyJson: string | undefined,
) => BigQueryJobLister;

/** The whole cancel, every sweep included. */
const CANCEL_TIMEOUT_MS = 15_000;
/**
 * When each sweep starts, after the abort. A job created a moment before the
 * abort can be missing from the first listing, so the cancel looks again until
 * a sweep finds nothing new after one that cancelled something.
 */
const SWEEP_DELAYS_MS = [0, 1_000, 2_500, 5_000];
/** A build issues a handful of reads; more than this would be a bug. */
const MAX_CANCELLED_JOBS = 20;
/** Listing from before the first read covers a clock skew between hosts. */
const CREATION_TIME_SLACK_MS = 5 * 60_000;
/** Pages of the job listing, at most; a busy account lists many jobs. */
const MAX_LISTING_PAGES = 5;

/**
 * The production lister: `@google-cloud/bigquery` with the connection's own
 * service account key, in the project the read ran in. `allUsers: false`
 * lists only the key's own jobs, which is what the job-listing permission the
 * split read already needs grants.
 *
 * Refuses without a key rather than letting the client fall back to the
 * host's default credentials: the cancel must act as the identity that ran
 * the read, never another one.
 */
export const bigQueryJobLister: BigQueryJobListerFactory = (
   projectId,
   serviceAccountKeyJson,
) => {
   if (!serviceAccountKeyJson) {
      throw new Error(
         "The BigQuery source connection has no service account key, so its " +
            "build's jobs are not cancelled under any other identity",
      );
   }
   const client = new BigQuery({
      projectId,
      credentials: JSON.parse(serviceAccountKeyJson),
   });
   return {
      async activeJobs(minCreationTime) {
         // Paged by hand: the client's autoPaginate stops after one page
         // whenever maxResults is set, so maxApiCalls would bound nothing.
         let query: GetJobsOptions | null | undefined = {
            projection: "full",
            stateFilter: ["pending", "running"],
            allUsers: false,
            minCreationTime: String(minCreationTime),
            maxResults: 200,
            autoPaginate: false,
         };
         const jobs: Job[] = [];
         for (let page = 0; page < MAX_LISTING_PAGES && query; page++) {
            const response: GetJobsResponse = await client.getJobs(query);
            jobs.push(...response[0]);
            query = response[1];
         }
         return jobs.map((job) => ({
            id: String(job.id),
            query: job.metadata?.configuration?.query?.query as
               | string
               | undefined,
            cancel: () => job.cancel(),
         }));
      },
   };
};

/**
 * The BigQuery jobs one build session starts: marks each read, and cancels the
 * marked ones that are still running.
 */
export class BigQueryBuildJobs {
   /** Unique to this session; BigQuery keeps the comment in the job's text. */
   readonly marker = `/* publisher_build:${randomUUID()} */`;
   private firstReadAt: number | undefined;
   /** Marked statements running now ({@link begin}). */
   private inFlight = 0;
   /** Sequence numbers of the last mark and the last settled read. */
   private sequence = 0;
   private lastMarked = 0;
   private lastSettled = 0;
   private cancelling: Promise<void> | undefined;
   private lister: BigQueryJobLister | undefined;

   constructor(
      private readonly makeLister: () => BigQueryJobLister,
      private readonly options: {
         timeoutMs?: number;
         sweepDelaysMs?: readonly number[];
      } = {},
   ) {}

   /** `sql` with this session's marker, which makes the job it starts findable. */
   mark(sql: string): string {
      this.firstReadAt ??= Date.now();
      this.lastMarked = ++this.sequence;
      return `${this.marker}\n${sql}`;
   }

   /**
    * A marked statement starts; call the returned function when it settles.
    * A job is running only while one is in flight, or after a mark whose
    * statement has not run yet.
    */
   begin(): () => void {
      this.inFlight += 1;
      let settled = false;
      return () => {
         if (settled) return;
         settled = true;
         this.inFlight -= 1;
         this.lastSettled = ++this.sequence;
      };
   }

   /** No marked statement running, and nothing marked since the last settled. */
   private idle(): boolean {
      return this.inFlight === 0 && this.lastMarked < this.lastSettled;
   }

   /** Cancel this session's running jobs. Idempotent; never rejects. */
   cancel(): Promise<void> {
      this.cancelling ??= this.cancelBounded();
      return this.cancelling;
   }

   /** The cancel in progress, or nothing when there is none. */
   settled(): Promise<void> {
      return this.cancelling ?? Promise.resolve();
   }

   private async cancelBounded(): Promise<void> {
      // Nothing marked, nothing started: no client, no listing. Every read
      // already settled (an abort after the reads): their jobs are done.
      if (this.firstReadAt === undefined || this.idle()) return;
      const timeoutMs = this.options.timeoutMs ?? CANCEL_TIMEOUT_MS;
      let timer: ReturnType<typeof setTimeout> | undefined;
      let timedOut = false;
      const deadline = new Promise<void>((resolve) => {
         timer = setTimeout(() => {
            timedOut = true;
            resolve();
         }, timeoutMs);
      });
      const cancelled = new Set<string>();
      const started = Date.now();
      let failed = false;
      try {
         await Promise.race([this.sweep(cancelled, () => timedOut), deadline]);
      } catch (error) {
         failed = true;
         logger.warn(
            "Could not cancel a cancelled storage= build's BigQuery jobs",
            { code: errorCodeOf(error) },
         );
      } finally {
         clearTimeout(timer);
      }
      if (timedOut) {
         logger.warn(
            "Gave up cancelling a cancelled storage= build's BigQuery jobs",
            { timeoutMs, cancelled: [...cancelled] },
         );
      } else if (!failed) {
         logger.info("Cancelled a cancelled storage= build's BigQuery jobs", {
            jobs: [...cancelled],
            durationMs: Date.now() - started,
         });
      }
   }

   private async sweep(
      cancelled: Set<string>,
      expired: () => boolean,
   ): Promise<void> {
      this.lister ??= this.makeLister();
      const since = this.firstReadAt! - CREATION_TIME_SLACK_MS;
      let previous = 0;
      for (const at of this.options.sweepDelaysMs ?? SWEEP_DELAYS_MS) {
         if (at > previous) {
            await new Promise((resolve) => setTimeout(resolve, at - previous));
         }
         previous = at;
         if (expired()) return;
         const mine = (await this.lister.activeJobs(since))
            .filter(
               (job) =>
                  job.query?.includes(this.marker) && !cancelled.has(job.id),
            )
            .slice(0, MAX_CANCELLED_JOBS - cancelled.size);
         // Found and cancelled before, nothing new since: done.
         if (mine.length === 0 && cancelled.size > 0) return;
         await Promise.all(
            mine.map(async (job) => {
               cancelled.add(job.id);
               try {
                  await job.cancel();
               } catch (error) {
                  logger.warn("Could not cancel a BigQuery job", {
                     jobId: job.id,
                     code: errorCodeOf(error),
                  });
               }
            }),
         );
         if (cancelled.size >= MAX_CANCELLED_JOBS) return;
      }
   }
}

const jobsBySession = new WeakMap<DuckDBConnection, BigQueryBuildJobs>();

/**
 * `sql` marked for the BigQuery jobs `session` tracks; unchanged on a session
 * that tracks none (any engine but BigQuery, or a build with no cancel signal).
 * Every BigQuery read a build session issues goes through this.
 */
export function markBigQueryRead(
   session: DuckDBConnection,
   sql: string,
): string {
   return jobsBySession.get(session)?.mark(sql) ?? sql;
}

/**
 * Track the BigQuery jobs `session` starts, and cancel them when `signal`
 * aborts. The returned function stops tracking, then waits for a cancel in
 * progress (bounded), so a cancelled build ends after its jobs were cancelled.
 * Nothing is tracked without a signal: nothing could ask for a cancel.
 */
export function trackBigQueryJobs(
   session: DuckDBConnection,
   signal: AbortSignal | undefined,
   makeLister: () => BigQueryJobLister,
   options?: ConstructorParameters<typeof BigQueryBuildJobs>[1],
): () => Promise<void> {
   if (!signal) return async () => {};
   const jobs = new BigQueryBuildJobs(makeLister, options);
   jobsBySession.set(session, jobs);
   // Every statement carrying the marker is a read in flight until it settles:
   // the CTAS over a direct read, the split's script, a delta, a probe.
   const original = session.runSQL;
   const tracked: typeof session.runSQL | undefined =
      typeof original === "function"
         ? (...args) => {
              if (!String(args[0]).includes(jobs.marker)) {
                 return original.apply(session, args);
              }
              const settle = jobs.begin();
              return original.apply(session, args).finally(settle);
           }
         : undefined;
   if (tracked) session.runSQL = tracked;
   const cancel = () => void jobs.cancel();
   signal.addEventListener("abort", cancel, { once: true });
   return async () => {
      signal.removeEventListener("abort", cancel);
      if (tracked && session.runSQL === tracked) session.runSQL = original;
      if (jobsBySession.get(session) === jobs) jobsBySession.delete(session);
      await jobs.settled();
   };
}

/** BigQuery label keys and values are at most 63 characters. */
const BQ_MAX_LEN = 63;

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
            code: errorCodeOf(error),
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
                  code: errorCodeOf(error),
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
 * `sql` prepared for a BigQuery build read: marked for the jobs `session`
 * tracks, and with the `@@query_label` value the split applies when
 * `queryMetadata` renders one and this connection can list the job records
 * the split depends on. The label is probed BEFORE anything runs: past this
 * point the read is billed, and an unusable split could only be recovered by
 * billing the read twice.
 */
export async function prepareBigQueryRead(
   session: DuckDBConnection,
   handle: string,
   sql: string,
   queryMetadata?: QueryMetadata,
): Promise<{ sql: string; label?: string }> {
   const marked = markBigQueryRead(session, sql);
   const label = bigQueryQueryLabelValue(queryMetadata);
   if (label === undefined || !(await canSplitBigQueryRead(session, handle))) {
      return { sql: marked };
   }
   return { sql: marked, label };
}

/**
 * The split read of `buildSQL` under `label`: `bigquery_execute` runs it in a
 * script that sets `@@query_label` first, then `bigquery_scan` reads the
 * anonymous result table the job record names. Returns what the CTAS selects
 * from, the child job's id and what it cost.
 */
export async function splitBigQueryRead(
   session: DuckDBConnection,
   handle: string,
   buildSQL: string,
   label: string,
): Promise<{
   selectSQL: string;
   jobId: string | null;
   cost: BuildReadCost | null;
}> {
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

/** One named column from the first row, or null when it is absent or empty. */
function firstColumn(result: unknown, column: string): string | null {
   const rows = resultRows(result);
   return rows.length > 0 ? str(rows[0][column]) : null;
}

/**
 * Track the BigQuery jobs a build session starts reading `connection` through
 * `handle` (the project the read runs in), and cancel them when `signal`
 * aborts, under the connection's own key; `listerFactory` replaces BigQuery's
 * jobs API in a test.
 */
export function trackBigQueryBuildJobs(
   session: DuckDBConnection,
   signal: AbortSignal | undefined,
   handle: string,
   connection: ApiConnection,
   listerFactory: BigQueryJobListerFactory = bigQueryJobLister,
): () => Promise<void> {
   return trackBigQueryJobs(session, signal, () =>
      listerFactory(
         handle,
         connection.bigqueryConnection?.serviceAccountKeyJson,
      ),
   );
}
