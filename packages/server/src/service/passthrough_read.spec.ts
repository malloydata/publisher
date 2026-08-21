// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";

import {
   BilledReadNotCapturedError,
   issuePassthroughRead,
} from "./materialization_build_session";

/** A build session that records what it was asked to run and replays canned rows. */
function fakeSession(replies: Record<string, unknown>[][] = []) {
   const issued: string[] = [];
   let next = 0;
   const session = {
      runSQL: async (sql: string) => {
         issued.push(sql);
         return { rows: replies[next++] ?? [] };
      },
   };
   return { session: session as never, issued };
}

/** The capability probe's reply — a listing that succeeds. */
const PROBE_OK = [{ job_id: "any" }];

const BQ_CHILD = [
   {
      job_id: "script_job_abc_0",
      project: "proj",
      dataset: "_anon_ds",
      table: "anon_tbl",
      bytes_processed: "5114816",
      bytes_billed: "10485760",
      total_slot_time_ms: 26,
      cache_hit: "false",
   },
];

describe("issuePassthroughRead — direct shape", () => {
   it("leaves an UNLABELLED BigQuery read exactly as it was", async () => {
      // The split is gated on there being a label, so a deployment that is not
      // tagging keeps byte-for-byte the read it had before.
      const { session, issued } = fakeSession();
      const read = await issuePassthroughRead(
         session,
         "bigquery",
         "proj",
         "SELECT 1",
         undefined,
      );
      expect(read.selectSQL).toBe(
         "SELECT * FROM bigquery_query('proj', 'SELECT 1')",
      );
      expect(read.jobId).toBeNull();
      expect(read.cost).toBeNull();
      // Nothing was executed to produce it.
      expect(issued).toEqual([]);
   });

   it("leaves Snowflake unsplit even WITH metadata", async () => {
      // Snowflake carries its tag on the session, so its read never changes
      // shape; splitting it would buy nothing.
      const { session, issued } = fakeSession();
      const read = await issuePassthroughRead(
         session,
         "snowflake",
         "sf_secret",
         "SELECT 1",
         { cred_run: "r1" },
      );
      expect(read.selectSQL).toStartWith("SELECT * FROM snowflake_query(");
      expect(read.jobId).toBeNull();
      expect(issued).toEqual([]);
   });

   it("leaves Postgres unsplit, having no per-statement tag", async () => {
      const { session } = fakeSession();
      const read = await issuePassthroughRead(
         session,
         "postgres",
         "pg",
         "SELECT 1",
         { cred_run: "r1" },
      );
      expect(read.selectSQL).toStartWith("SELECT * FROM postgres_query(");
   });

   it("stays direct when every property was unusable as a BigQuery key", async () => {
      // A bag that renders to no label leaves nothing to split FOR.
      const { session, issued } = fakeSession();
      const read = await issuePassthroughRead(
         session,
         "bigquery",
         "proj",
         "SELECT 1",
         { "1st": "x" },
      );
      expect(read.selectSQL).toStartWith("SELECT * FROM bigquery_query(");
      expect(issued).toEqual([]);
   });
});

describe("issuePassthroughRead — the split's capability probe", () => {
   /** A session whose FIRST statement throws — i.e. job listing is unavailable. */
   function sessionWithoutJobListing() {
      const issued: string[] = [];
      const session = {
         runSQL: async (sql: string) => {
            issued.push(sql);
            if (sql.includes("bigquery_jobs")) {
               throw new Error("Access Denied: bigquery.jobs.list");
            }
            return { rows: [] };
         },
      };
      return { session: session as never, issued };
   }

   it("falls back to the direct shape rather than failing the build", async () => {
      // Turning attribution on must not add a permission requirement that breaks
      // builds. The cost of not having it is attribution, not the build.
      const { session } = sessionWithoutJobListing();
      const read = await issuePassthroughRead(
         session,
         "bigquery",
         "proj",
         "SELECT a FROM t",
         { cred_run: "r1" },
      );
      expect(read.selectSQL).toStartWith("SELECT * FROM bigquery_query(");
      expect(read.cost).toBeNull();
   });

   it("asks BEFORE issuing the read, so a fallback costs nothing already billed", async () => {
      // Past the read there is no cheap way back: the rows live only in a table
      // this cannot locate, so falling back would mean paying for the read twice.
      const { session, issued } = sessionWithoutJobListing();
      await issuePassthroughRead(session, "bigquery", "proj", "SELECT a", {
         cred_run: "r1",
      });
      expect(issued).toHaveLength(1);
      expect(issued[0]).toContain("bigquery_jobs");
      expect(issued.some((s) => s.includes("bigquery_execute"))).toBe(false);
   });

   it("does not probe at all when there is no label to carry", async () => {
      // No label, no split, nothing to ask about.
      const { session, issued } = sessionWithoutJobListing();
      await issuePassthroughRead(
         session,
         "bigquery",
         "proj",
         "SELECT a",
         undefined,
      );
      expect(issued).toEqual([]);
   });
});

describe("issuePassthroughRead — BigQuery split", () => {
   it("runs the SELECT inside a labelled script, then scans its result table", async () => {
      const { session, issued } = fakeSession([
         PROBE_OK,
         [{ job_id: "job_parent" }],
         BQ_CHILD,
      ]);
      const read = await issuePassthroughRead(
         session,
         "bigquery",
         "proj",
         "SELECT a FROM t",
         { cred_run: "run_1", cred_class: "ops" },
      );

      expect(issued[1]).toContain("bigquery_execute('proj'");
      expect(issued[1]).toContain(
         'SET @@query_label = "cred_run:run_1,cred_class:ops"',
      );
      // The SELECT rides in the same script, which is the only way the label
      // applies to the job that runs it.
      expect(issued[1]).toContain("SELECT a FROM t");
      // The child is reached through its parent — it is not in the default listing.
      expect(issued[2]).toContain("parentJobId := 'job_parent'");
      expect(read.selectSQL).toBe(
         "SELECT * FROM bigquery_scan('proj._anon_ds.anon_tbl')",
      );
   });

   it("reports the cost from the same call that located the table", async () => {
      const { session } = fakeSession([
         PROBE_OK,
         [{ job_id: "job_parent" }],
         BQ_CHILD,
      ]);
      const read = await issuePassthroughRead(
         session,
         "bigquery",
         "proj",
         "SELECT a FROM t",
         { cred_run: "run_1" },
      );
      expect(read.cost).toEqual({
         engine: "bigquery",
         jobId: "script_job_abc_0",
         parentJobId: "job_parent",
         bytesScanned: 5114816,
         bytesBilled: 10485760,
         slotTimeMs: 26,
         executionTimeMs: null,
         cacheHit: false,
      });
   });

   it("keeps billed and scanned apart", async () => {
      // BigQuery's 10MB floor bills a small read well above what it scanned, and
      // refreshes are mostly small reads, so collapsing the two would understate
      // every one of them.
      const { session } = fakeSession([PROBE_OK, [{ job_id: "p" }], BQ_CHILD]);
      const read = await issuePassthroughRead(
         session,
         "bigquery",
         "proj",
         "SELECT 1",
         { cred_run: "r" },
      );
      expect(read.cost?.bytesScanned).toBe(5114816);
      expect(read.cost?.bytesBilled).toBe(10485760);
   });

   it("carries the PARENT id, without which the cost job cannot be looked up", async () => {
      // Measured against a live project: the child of a script is absent from the
      // default bigquery_jobs() listing, so its id alone resolves to nothing.
      const { session } = fakeSession([
         PROBE_OK,
         [{ job_id: "job_parent" }],
         BQ_CHILD,
      ]);
      const read = await issuePassthroughRead(
         session,
         "bigquery",
         "proj",
         "SELECT 1",
         { cred_run: "r" },
      );
      expect(read.cost?.parentJobId).toBe("job_parent");
   });

   it("asks for the NEWEST child first, so the SELECT wins over any sibling", async () => {
      // The script has more than one statement and nothing documents the order a
      // parentJobId listing returns them in. Newest-first takes the last
      // statement, which is the SELECT whose rows this is after.
      const { session, issued } = fakeSession([
         PROBE_OK,
         [{ job_id: "job_parent" }],
         BQ_CHILD,
      ]);
      await issuePassthroughRead(session, "bigquery", "proj", "SELECT 1", {
         cred_run: "r",
      });
      expect(issued[2]).toContain("ORDER BY creation_time DESC");
   });

   it("picks the first child carrying a result table, given that ordering", async () => {
      const { session } = fakeSession([
         PROBE_OK,
         [{ job_id: "job_parent" }],
         // A sibling with no destination table must not be selected over the read.
         [{ job_id: "sibling", dataset: null, table: null }, ...BQ_CHILD],
      ]);
      const read = await issuePassthroughRead(
         session,
         "bigquery",
         "proj",
         "SELECT 1",
         { cred_run: "r" },
      );
      expect(read.selectSQL).toContain("proj._anon_ds.anon_tbl");
      expect(read.jobId).toBe("script_job_abc_0");
   });

   it("retries when the listing succeeds but the child is not enumerated yet", async () => {
      // A 200 with the child missing is the same transient as a 5xx and is
      // indistinguishable from it here — bigquery_execute returning a job id
      // proves the script finished, not that the listing has caught up.
      let calls = 0;
      const session = {
         runSQL: async (sql: string) => {
            if (!sql.includes("parentJobId")) {
               return {
                  rows: sql.includes("bigquery_execute")
                     ? [{ job_id: "job_parent" }]
                     : [{ job_id: "any" }],
               };
            }
            calls++;
            // Two empty listings, then the child appears.
            return { rows: calls < 3 ? [] : BQ_CHILD };
         },
      } as never;
      const read = await issuePassthroughRead(
         session,
         "bigquery",
         "proj",
         "SELECT 1",
         { cred_run: "r" },
      );
      expect(calls).toBe(3);
      expect(read.cost?.bytesScanned).toBe(5114816);
   });

   it("names the parent job when it finally gives up, so the read can be found", async () => {
      // The one failure documented as the operator's decision. A script's child
      // job is absent from the default listing, so the parent is the only handle
      // on a read that was paid for.
      const { session } = fakeSession([
         PROBE_OK,
         [{ job_id: "job_parent" }],
         [],
      ]);
      const error = (await issuePassthroughRead(
         session,
         "bigquery",
         "proj",
         "SELECT 1",
         { cred_run: "r" },
      ).catch((e: Error) => e)) as Error;
      expect(error).toBeInstanceOf(BilledReadNotCapturedError);
      expect(error.message).toContain("job_parent");
   });

   it("retries a failing job lookup before failing a build already billed for", async () => {
      // The probe established that this connection CAN list, so a failure here is
      // transient — and the alternative is expensive, because the read is paid for
      // and cannot be re-issued for free.
      let calls = 0;
      const session = {
         runSQL: async (sql: string) => {
            if (!sql.includes("parentJobId")) {
               return {
                  rows: sql.includes("bigquery_execute")
                     ? [{ job_id: "job_parent" }]
                     : [{ job_id: "any" }],
               };
            }
            calls++;
            if (calls < 3) throw new Error("503 backend error");
            return { rows: BQ_CHILD };
         },
      } as never;
      const read = await issuePassthroughRead(
         session,
         "bigquery",
         "proj",
         "SELECT 1",
         { cred_run: "r" },
      );
      expect(calls).toBe(3);
      expect(read.cost?.bytesScanned).toBe(5114816);
   });

   it("marks a post-read failure as one a retry would pay for twice", async () => {
      // Distinguishable so a retry policy can tell "already billed" from "safe to
      // retry" — the read has run either way.
      const { session } = fakeSession([
         PROBE_OK,
         [{ job_id: "job_parent" }],
         [],
      ]);
      const error = await issuePassthroughRead(
         session,
         "bigquery",
         "proj",
         "SELECT 1",
         { cred_run: "r" },
      ).catch((e: Error) => e);
      expect(error).toBeInstanceOf(BilledReadNotCapturedError);
   });

   it("fails rather than re-running a read that already happened", async () => {
      // The query HAS run and been billed by this point. Falling back to the
      // direct shape would run and bill it a second time; a retry is the
      // operator's decision, not a silent double charge. The message is the
      // retry's terminal one, because an empty listing is now retried first.
      const { session } = fakeSession([
         PROBE_OK,
         [{ job_id: "job_parent" }],
         [],
      ]);
      await expect(
         issuePassthroughRead(session, "bigquery", "proj", "SELECT 1", {
            cred_run: "r",
         }),
      ).rejects.toThrow(/rows cannot be captured/);
   });

   it("fails loudly when the execute reports no job at all", async () => {
      const { session } = fakeSession([PROBE_OK, []]);
      await expect(
         issuePassthroughRead(session, "bigquery", "proj", "SELECT 1", {
            cred_run: "r",
         }),
      ).rejects.toThrow(/no job_id/);
   });

   it("escapes the build SQL into the script it embeds", async () => {
      const { session, issued } = fakeSession([
         PROBE_OK,
         [{ job_id: "job_parent" }],
         BQ_CHILD,
      ]);
      await issuePassthroughRead(
         session,
         "bigquery",
         "proj",
         "SELECT 'it''s' AS x",
         { cred_run: "r" },
      );
      // Doubled once for the DuckDB literal the script rides in. DuckDB has no
      // backslash escape, so a backslash in the compiled SQL passes through to
      // BigQuery unchanged — which is what BigQuery's own literal grammar needs.
      expect(issued[1]).toContain("SELECT ''it''''s'' AS x");
   });
});
