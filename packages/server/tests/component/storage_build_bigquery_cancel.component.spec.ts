// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import * as fs from "fs/promises";
import { mkdirSync, mkdtempSync, rmSync } from "node:fs";
import os from "node:os";
import path from "node:path";

import { BigQuery, type Job } from "@google-cloud/bigquery";
import { storageDestinationRoot } from "../../src/service/connection_config";
import { buildSourceIntoStorage } from "../../src/service/materialization/run/materialization_build_session";

/**
 * F-22, against a REAL BigQuery project: cancelling a `storage=` build stops
 * the BigQuery job its read started, not only DuckDB's wait for it.
 *
 * The read is compute-only (a cross join of two generated arrays, no table),
 * so it processes and bills 0 bytes however long it runs; the extension takes
 * no `maximumBytesBilled`, and needs none here. A unique predicate keeps
 * BigQuery from answering it from cache.
 *
 * Uses the `BQ_PRESTO_TRINO_KEY` service account, as CI sets it up:
 * `GOOGLE_APPLICATION_CREDENTIALS` names the key file and
 * `BIGQUERY_TEST_PROJECT_ID` its project. Absent those this SKIPS. Never point
 * it at a production project.
 */
const hasCredentials = () =>
   !!(
      process.env.GOOGLE_APPLICATION_CREDENTIALS &&
      process.env.BIGQUERY_TEST_PROJECT_ID
   );

/** How long a cancelled job may take to reach DONE with reason `stopped`. */
const STOPPED_WITHIN_MS = 30_000;

async function until<T>(
   probe: () => Promise<T | undefined>,
   timeoutMs: number,
   what: string,
): Promise<T> {
   const deadline = Date.now() + timeoutMs;
   while (Date.now() < deadline) {
      const value = await probe();
      if (value !== undefined) return value;
      await new Promise((resolve) => setTimeout(resolve, 500));
   }
   throw new Error(`timed out after ${timeoutMs} ms waiting for ${what}`);
}

describe.skipIf(!hasCredentials())(
   "F-22: a cancelled storage= build over live BigQuery",
   () => {
      it("cancels the build's BigQuery job", async () => {
         const serviceAccountKeyJson = await fs.readFile(
            process.env.GOOGLE_APPLICATION_CREDENTIALS!,
            "utf-8",
         );
         const projectId = process.env.BIGQUERY_TEST_PROJECT_ID!;
         const client = new BigQuery({
            projectId,
            credentials: JSON.parse(serviceAccountKeyJson),
         });
         const nonce = `cancel_${Date.now()}`;
         const buildSQL =
            `SELECT COUNT(*) AS n ` +
            `FROM UNNEST(GENERATE_ARRAY(1, 100000)) AS a ` +
            `CROSS JOIN UNNEST(GENERATE_ARRAY(1, 100000)) AS b ` +
            `WHERE MOD(a * b + LENGTH('${nonce}'), 7) = 3`;
         const environmentPath = mkdtempSync(
            path.join(os.tmpdir(), `storage-cancel-${nonce}-`),
         );
         mkdirSync(storageDestinationRoot(environmentPath), {
            recursive: true,
         });
         const run = new AbortController();
         let job: Job | undefined;
         try {
            const build = buildSourceIntoStorage({
               destinationName: "lake",
               destinationConnection: { name: "lake", type: "duckdb" } as never,
               sourceConnection: {
                  name: "bq_src",
                  type: "bigquery",
                  bigqueryConnection: {
                     serviceAccountKeyJson,
                     defaultProjectId: projectId,
                  },
               } as never,
               buildSQL,
               physicalTableName: `cancelled_${nonce}`,
               environmentPath,
               signal: run.signal,
            });
            const settled = build.then(
               () => "completed",
               (error: unknown) => error,
            );

            // The read's job, running, found by the text only this build sends.
            job = await until(
               async () => {
                  const [jobs] = await client.getJobs({
                     projection: "full",
                     stateFilter: ["running"],
                     minCreationTime: String(Date.now() - 10 * 60_000),
                  });
                  return jobs.find((job) =>
                     String(
                        job.metadata?.configuration?.query?.query ?? "",
                     ).includes(nonce),
                  );
               },
               60_000,
               "the build's BigQuery job to start",
            );

            const cancelledAt = Date.now();
            run.abort();
            const outcome = await settled;
            const buildEndedMs = Date.now() - cancelledAt;
            expect(outcome).not.toBe("completed");

            const status = await until(
               async () => {
                  // The listed job carries its location, which the lookup needs.
                  const [metadata] = await job.getMetadata();
                  return metadata.status?.state === "DONE"
                     ? metadata.status
                     : undefined;
               },
               STOPPED_WITHIN_MS,
               `job ${job.id} to reach DONE`,
            );
            const stoppedMs = Date.now() - cancelledAt;
            console.log(
               `F-22 live cancel: build ended ${buildEndedMs} ms and job ` +
                  `${job.id} reached DONE ${stoppedMs} ms after the abort`,
            );
            expect(status.errorResult?.reason).toBe("stopped");
            expect(stoppedMs).toBeLessThan(STOPPED_WITHIN_MS);
         } finally {
            run.abort();
            // Directly too, so a regression cannot leave the cross join running.
            await job?.cancel().catch(() => {});
            rmSync(environmentPath, { recursive: true, force: true });
         }
      }, 180_000);
   },
);
