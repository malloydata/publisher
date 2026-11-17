// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Cancel, in BigQuery, the jobs a cancelled `storage=` build started.
 *
 * A `storage=` build reads BigQuery through DuckDB's `bigquery` extension.
 * Interrupting the DuckDB session (`interruptOnAbort`) stops DuckDB's wait,
 * but not the job: BigQuery runs it to completion, and bills it. The extension
 * cannot cancel a job, nor choose its id or labels (`bigquery_query` and
 * `bigquery_execute` take neither), so each read the build issues carries a
 * comment naming this build session ({@link BigQueryBuildJobs.mark}), and on
 * abort the build lists its service account's running jobs through BigQuery's
 * API, under the same key the read ran with, and cancels the ones that carry
 * it.
 *
 * Best-effort and bounded: a cancel never throws into the run, never waits
 * longer than {@link CANCEL_TIMEOUT_MS}, and touches only jobs whose text
 * carries this session's marker, which no other build's can.
 */

import { randomUUID } from "node:crypto";
import { BigQuery, type GetJobsOptions } from "@google-cloud/bigquery";
import type { DuckDBConnection } from "@malloydata/db-duckdb";
import { logger } from "../../../logger";

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
 */
export const bigQueryJobLister: BigQueryJobListerFactory = (
   projectId,
   serviceAccountKeyJson,
) => {
   const client = new BigQuery({
      projectId,
      ...(serviceAccountKeyJson
         ? { credentials: JSON.parse(serviceAccountKeyJson) }
         : {}),
   });
   return {
      async activeJobs(minCreationTime) {
         const options: GetJobsOptions = {
            projection: "full",
            stateFilter: ["pending", "running"],
            allUsers: false,
            minCreationTime: String(minCreationTime),
            maxResults: 200,
            autoPaginate: true,
            maxApiCalls: MAX_LISTING_PAGES,
         };
         const [jobs] = await client.getJobs(options);
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
      return `${this.marker}\n${sql}`;
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
      // Nothing marked, nothing started: no client, no listing.
      if (this.firstReadAt === undefined) return;
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
            { error: error instanceof Error ? error.message : String(error) },
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
                     error:
                        error instanceof Error ? error.message : String(error),
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
   const cancel = () => void jobs.cancel();
   signal.addEventListener("abort", cancel, { once: true });
   return async () => {
      signal.removeEventListener("abort", cancel);
      if (jobsBySession.get(session) === jobs) jobsBySession.delete(session);
      await jobs.settled();
   };
}
