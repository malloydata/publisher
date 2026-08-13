import { describe, expect, it } from "bun:test";

import { issuePassthroughRead } from "./materialization_build_session";

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

describe("issuePassthroughRead — BigQuery split", () => {
   it("runs the SELECT inside a labelled script, then scans its result table", async () => {
      const { session, issued } = fakeSession([
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

      expect(issued[0]).toContain("bigquery_execute('proj'");
      expect(issued[0]).toContain(
         'SET @@query_label = \"cred_run:run_1,cred_class:ops\"',
      );
      // The SELECT rides in the same script, which is the only way the label
      // applies to the job that runs it.
      expect(issued[0]).toContain("SELECT a FROM t");
      // The child is reached through its parent — it is not in the default listing.
      expect(issued[1]).toContain("parentJobId := 'job_parent'");
      expect(read.selectSQL).toBe(
         "SELECT * FROM bigquery_scan('proj._anon_ds.anon_tbl')",
      );
   });

   it("reports the cost from the same call that located the table", async () => {
      const { session } = fakeSession([[{ job_id: "job_parent" }], BQ_CHILD]);
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
      const { session } = fakeSession([[{ job_id: "p" }], BQ_CHILD]);
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
      const { session } = fakeSession([[{ job_id: "job_parent" }], BQ_CHILD]);
      const read = await issuePassthroughRead(
         session,
         "bigquery",
         "proj",
         "SELECT 1",
         { cred_run: "r" },
      );
      expect(read.cost?.parentJobId).toBe("job_parent");
   });

   it("fails rather than re-running a read that already happened", async () => {
      // The query HAS run and been billed by this point. Falling back to the
      // direct shape would run and bill it a second time; a retry is the
      // operator's decision, not a silent double charge.
      const { session } = fakeSession([[{ job_id: "job_parent" }], []]);
      await expect(
         issuePassthroughRead(session, "bigquery", "proj", "SELECT 1", {
            cred_run: "r",
         }),
      ).rejects.toThrow(/no locatable result table/);
   });

   it("fails loudly when the execute reports no job at all", async () => {
      const { session } = fakeSession([[]]);
      await expect(
         issuePassthroughRead(session, "bigquery", "proj", "SELECT 1", {
            cred_run: "r",
         }),
      ).rejects.toThrow(/no job_id/);
   });

   it("escapes the build SQL into the script it embeds", async () => {
      const { session, issued } = fakeSession([
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
      expect(issued[0]).toContain("SELECT ''it''''s'' AS x");
   });
});
