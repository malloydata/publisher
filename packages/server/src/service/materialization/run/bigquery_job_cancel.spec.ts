// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * F-22: a cancelled `storage=` build over BigQuery cancels its BigQuery jobs.
 *
 * DuckDB's interrupt stops the build's wait, not the job, and the extension
 * can set neither a job id nor labels, so each read carries a comment naming
 * its build session and the cancel finds and cancels exactly the jobs that
 * carry it. Nothing here reaches BigQuery: the jobs API is a stub. The live
 * check is tests/component/storage_build_bigquery_cancel.component.spec.ts.
 */

import type { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { mkdirSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import * as sinon from "sinon";
import type { components } from "../../../api";
import { logger } from "../../../logger";
import { storageDestinationRoot } from "../../connection_config";
import {
   BigQueryBuildJobs,
   markBigQueryRead,
   trackBigQueryJobs,
   type BigQueryActiveJob,
   type BigQueryJobLister,
} from "./bigquery_job_cancel";
import {
   buildSourceIntoStorage,
   issuePassthroughRead,
   type BuildSessionDeps,
} from "./materialization_build_session";

type ApiConnection = components["schemas"]["Connection"];

/** A stub of BigQuery's job listing, recording what was cancelled. */
function stubJobs(jobs: () => Array<{ id: string; query?: string }>) {
   const cancelled: string[] = [];
   const listings: number[] = [];
   const lister: BigQueryJobLister = {
      async activeJobs(minCreationTime) {
         listings.push(minCreationTime);
         return jobs().map(
            (job): BigQueryActiveJob => ({
               id: job.id,
               query: job.query,
               cancel: async () => {
                  cancelled.push(job.id);
               },
            }),
         );
      },
   };
   return { lister, cancelled, listings };
}

const FAST = { sweepDelaysMs: [0, 5, 10] };

describe("F-22: a cancelled BigQuery read is marked and cancelled", () => {
   let warn: sinon.SinonStub;
   let info: sinon.SinonStub;
   beforeEach(() => {
      warn = sinon.stub(logger, "warn");
      info = sinon.stub(logger, "info");
   });
   afterEach(() => {
      warn.restore();
      info.restore();
   });

   it("marks each read with a comment unique to its session", () => {
      const a = new BigQueryBuildJobs(() => stubJobs(() => []).lister);
      const b = new BigQueryBuildJobs(() => stubJobs(() => []).lister);
      expect(a.marker).toMatch(/^\/\* publisher_build:[0-9a-f-]{36} \*\/$/);
      expect(a.marker).not.toBe(b.marker);
      expect(a.mark("SELECT 1")).toBe(`${a.marker}\nSELECT 1`);
   });

   it("marks a direct read, and the script of a labelled read, on a tracked session", async () => {
      const statements: string[] = [];
      const session = {
         runSQL: async (sql: string) => {
            statements.push(sql);
            if (sql.includes("maxResults := 1")) return { rows: [] };
            if (sql.includes("bigquery_execute(")) {
               return { rows: [{ job_id: "parent-1" }] };
            }
            return {
               rows: [{ job_id: "c", project: "p", dataset: "d", table: "t" }],
            };
         },
      } as unknown as DuckDBConnection;
      const run = new AbortController();
      const stop = trackBigQueryJobs(
         session,
         run.signal,
         () => stubJobs(() => []).lister,
      );
      try {
         const direct = await issuePassthroughRead(
            session,
            "bigquery",
            "proj",
            "SELECT 1",
            undefined,
         );
         expect(direct.selectSQL).toMatch(
            /bigquery_query\('proj', '\/\* publisher_build:[0-9a-f-]{36} \*\/\nSELECT 1'\)/,
         );
         await issuePassthroughRead(session, "bigquery", "proj", "SELECT 2", {
            team: "growth",
         });
         const script = statements.find((s) => s.includes("bigquery_execute("));
         // In the script's text, which is the parent job's: cancelling the
         // parent cancels the statement it is running.
         expect(script).toMatch(
            /SET @@query_label[^;]*;\n\/\* publisher_build:[0-9a-f-]{36} \*\/\nSELECT 2;/,
         );
      } finally {
         await stop();
      }
   });

   it("leaves an untracked session's read, and another engine's, unmarked", async () => {
      const session = {
         runSQL: async () => ({ rows: [] }),
      } as unknown as DuckDBConnection;
      expect(markBigQueryRead(session, "SELECT 1")).toBe("SELECT 1");
      // No signal: nothing could ask for a cancel, so nothing is tracked.
      const stop = trackBigQueryJobs(session, undefined, () => {
         throw new Error("no lister without a signal");
      });
      expect(markBigQueryRead(session, "SELECT 1")).toBe("SELECT 1");
      await stop();

      const run = new AbortController();
      const stopTracked = trackBigQueryJobs(
         session,
         run.signal,
         () => stubJobs(() => []).lister,
      );
      const read = await issuePassthroughRead(
         session,
         "snowflake",
         "secret",
         "SELECT 1",
         undefined,
      );
      expect(read.selectSQL).not.toContain("publisher_build");
      await stopTracked();
      // Released: the session reads unmarked again.
      expect(markBigQueryRead(session, "SELECT 1")).toBe("SELECT 1");
   });

   it("cancels exactly this session's running jobs on abort", async () => {
      const session = {} as DuckDBConnection;
      const run = new AbortController();
      let marked = "";
      const other = new BigQueryBuildJobs(() => stubJobs(() => []).lister);
      const stub = stubJobs(() => [
         { id: "mine-1", query: marked },
         { id: "other-build", query: other.mark("SELECT 1") },
         { id: "unmarked", query: "SELECT 1" },
         { id: "load-job" },
      ]);
      const before = Date.now();
      const stop = trackBigQueryJobs(
         session,
         run.signal,
         () => stub.lister,
         FAST,
      );
      marked = markBigQueryRead(session, "SELECT 1");
      run.abort();
      await stop();
      expect(stub.cancelled).toEqual(["mine-1"]);
      // Listed from before the first read, not from the whole account history.
      expect(stub.listings[0]).toBeGreaterThan(before - 10 * 60_000);
      expect(stub.listings[0]).toBeLessThanOrEqual(before);
   });

   it("looks again for a job the first listing did not show yet", async () => {
      const session = {} as DuckDBConnection;
      const run = new AbortController();
      let listings = 0;
      let marked = "";
      const stub = stubJobs(() =>
         ++listings < 3 ? [] : [{ id: "late", query: marked }],
      );
      const stop = trackBigQueryJobs(session, run.signal, () => stub.lister, {
         sweepDelaysMs: [0, 5, 10, 15],
      });
      marked = markBigQueryRead(session, "SELECT 1");
      run.abort();
      await stop();
      expect(stub.cancelled).toEqual(["late"]);
      // Cancelled at the third listing; the fourth found nothing new and stopped.
      expect(listings).toBe(4);
   });

   it("calls no API when the session started no BigQuery read", async () => {
      const run = new AbortController();
      let made = 0;
      const stop = trackBigQueryJobs({} as DuckDBConnection, run.signal, () => {
         made += 1;
         return stubJobs(() => []).lister;
      });
      run.abort();
      await stop();
      expect(made).toBe(0);
   });

   it("never throws into the run: a refused listing or cancel is logged", async () => {
      const failing = new BigQueryBuildJobs(
         () => ({
            activeJobs: async () => {
               throw new Error("403 bigquery.jobs.list denied");
            },
         }),
         FAST,
      );
      failing.mark("SELECT 1");
      await expect(failing.cancel()).resolves.toBeUndefined();

      const refusing: BigQueryBuildJobs = new BigQueryBuildJobs(
         () => ({
            activeJobs: async (): Promise<BigQueryActiveJob[]> => [
               {
                  id: "j",
                  query: refusing.marker,
                  cancel: async () => {
                     throw new Error("job already done");
                  },
               },
            ],
         }),
         FAST,
      );
      refusing.mark("SELECT 1");
      await expect(refusing.cancel()).resolves.toBeUndefined();
      expect(warn.callCount).toBeGreaterThanOrEqual(2);
   });

   it("gives up at its timeout when the API does not answer", async () => {
      const hung = new BigQueryBuildJobs(
         () => ({ activeJobs: () => new Promise(() => {}) }),
         { timeoutMs: 50 },
      );
      hung.mark("SELECT 1");
      const started = Date.now();
      await hung.cancel();
      expect(Date.now() - started).toBeLessThan(2_000);
      expect(String(warn.lastCall.args[0])).toMatch(/gave up/i);
   });

   it("buildSourceIntoStorage cancels its BigQuery read's job when the run is cancelled mid-read", async () => {
      const run = new AbortController();
      let readSQL = "";
      const stub = stubJobs(() => [{ id: "the-read", query: readSQL }]);
      const keys: Array<[string, string | undefined]> = [];
      const deps: BuildSessionDeps = {
         federate: async () => ({ handle: "proj", sourceType: "bigquery" }),
         read: async (session, _sourceType, _handle, buildSQL) => {
            // The production read marks the SQL; record what it would send.
            readSQL = markBigQueryRead(session, buildSQL);
            run.abort();
            return { selectSQL: "SELECT 1 AS x", jobId: null, cost: null };
         },
         bigQueryJobs: (projectId, key) => {
            keys.push([projectId, key]);
            return stub.lister;
         },
      };
      const dir = mkdtempSync(join(tmpdir(), "bq-cancel-"));
      mkdirSync(storageDestinationRoot(dir), { recursive: true });
      try {
         await expect(
            buildSourceIntoStorage({
               destinationName: "lake",
               destinationConnection: {
                  name: "lake",
                  type: "duckdb",
               } as ApiConnection,
               sourceConnection: {
                  name: "bq",
                  type: "bigquery",
                  bigqueryConnection: { serviceAccountKeyJson: "{key}" },
               } as ApiConnection,
               buildSQL: "SELECT 1",
               physicalTableName: "t",
               environmentPath: dir,
               signal: run.signal,
               deps,
            }),
         ).rejects.toThrow();
         // Under the read's own project and key, and only the read's job.
         expect(keys).toEqual([["proj", "{key}"]]);
         expect(stub.cancelled).toEqual(["the-read"]);
      } finally {
         rmSync(dir, { recursive: true, force: true });
      }
   });
});
