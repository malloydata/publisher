import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as sinon from "sinon";

import { resetMaterializationTelemetryForTesting } from "../materialization_metrics";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "../test_helpers/metrics_harness";
import { lookupBuildCost } from "./build_cost";

describe("lookupBuildCost", () => {
   let harness: MetricsHarness;

   beforeEach(async () => {
      harness = await startMetricsHarness();
      resetMaterializationTelemetryForTesting();
   });
   afterEach(async () => {
      resetMaterializationTelemetryForTesting();
      await harness.shutdown();
      sinon.restore();
   });

   const bq = (rows: unknown[]) => ({
      runner: sinon.stub().resolves({ rows }),
      engine: "bigquery" as const,
      project: "proj",
      sql: "SELECT 1",
      since: new Date("2026-08-12T00:00:00Z"),
   });

   it("returns the cost when exactly one query matches", async () => {
      const cost = await lookupBuildCost(
         bq([
            {
               job_id: "job_abc",
               bytes_processed: 1904884,
               total_slot_time_ms: 33,
               billed: "10485760",
               cache_hit: "false",
            },
         ]),
      );

      expect(cost).toEqual({
         engine: "bigquery",
         jobId: "job_abc",
         bytesScanned: 1904884,
         bytesBilled: 10485760,
         slotTimeMs: 33,
         executionTimeMs: null,
         cacheHit: false,
      });
      expect(
         await harness.collectCounter("publisher_build_cost_lookup_total", {
            engine: "bigquery",
            outcome: "found",
         }),
      ).toBe(1);
   });

   it("keeps billed and scanned as separate numbers", async () => {
      // BigQuery rounds up to a 10MB floor, so a small read bills an order of
      // magnitude above what it scanned. Collapsing the two would understate
      // every refresh, which are mostly small reads.
      const cost = await lookupBuildCost(
         bq([
            {
               job_id: "j",
               bytes_processed: 967654,
               total_slot_time_ms: 23,
               billed: "10485760",
               cache_hit: "false",
            },
         ]),
      );
      expect(cost?.bytesScanned).toBe(967654);
      expect(cost?.bytesBilled).toBe(10485760);
   });

   it("reports NO cost when more than one query matches", async () => {
      // The critical rule. Two identical-SQL reads are where the costs diverge
      // MOST, not least: the second hits the result cache and bills zero. Taking
      // the newest would systematically under-report, and under-reporting build
      // cost flatters the savings figure this data exists to compute. So the
      // answer is no answer.
      const cost = await lookupBuildCost(
         bq([
            {
               job_id: "first",
               bytes_processed: 5_000_000,
               billed: "5000000",
               cache_hit: "false",
            },
            {
               job_id: "second",
               bytes_processed: 0,
               billed: "0",
               cache_hit: "true",
            },
         ]),
      );

      expect(cost).toBeNull();
      expect(
         await harness.collectCounter("publisher_build_cost_lookup_total", {
            engine: "bigquery",
            outcome: "ambiguous",
         }),
      ).toBe(1);
      // And emphatically not the free one.
      expect(
         await harness.collectCounter("publisher_build_cost_lookup_total", {
            engine: "bigquery",
            outcome: "found",
         }),
      ).toBe(0);
   });

   it("reports not_found when nothing matches", async () => {
      expect(await lookupBuildCost(bq([]))).toBeNull();
      expect(
         await harness.collectCounter("publisher_build_cost_lookup_total", {
            engine: "bigquery",
            outcome: "not_found",
         }),
      ).toBe(1);
   });

   it("counts a cached build separately, and still reports its zero cost", async () => {
      // Zero is the CORRECT cost for a cache hit; it is also evidence the
      // rebuild was unnecessary, which is why it gets its own counter.
      const cost = await lookupBuildCost(
         bq([
            {
               job_id: "j",
               bytes_processed: 0,
               billed: "0",
               cache_hit: "true",
               total_slot_time_ms: null,
            },
         ]),
      );
      expect(cost?.cacheHit).toBe(true);
      expect(cost?.bytesBilled).toBe(0);
      expect(cost?.slotTimeMs).toBeNull();
      expect(
         await harness.collectCounter("publisher_build_cache_hit_total"),
      ).toBe(1);
   });

   it("never throws when the warehouse refuses the lookup", async () => {
      // A deployment may restrict query history. That is a permission answer,
      // not a bug — and a build that already succeeded must not fail because
      // its telemetry could not be read.
      const opts = bq([]);
      opts.runner = sinon.stub().rejects(new Error("Access Denied: jobs.list"));

      expect(await lookupBuildCost(opts)).toBeNull();
      expect(
         await harness.collectCounter("publisher_build_cost_lookup_total", {
            engine: "bigquery",
            outcome: "error",
         }),
      ).toBe(1);
   });

   it("widens the window backwards to absorb clock skew", async () => {
      const opts = bq([]);
      await lookupBuildCost(opts);
      const sql = (opts.runner as sinon.SinonStub).firstCall.args[0] as string;
      // `since` is our clock; the warehouse stamps its own. A locally-fast clock
      // would otherwise exclude the very job being looked for.
      expect(sql).toContain("2026-08-11T23:59:00.000Z");
      expect(sql).not.toContain("2026-08-12T00:00:00.000Z");
   });

   it("filters job_type in a subquery so a non-QUERY job cannot break the statement", async () => {
      const opts = bq([]);
      await lookupBuildCost(opts);
      const sql = (opts.runner as sinon.SinonStub).firstCall.args[0] as string;
      // In the outer WHERE, a load/copy job reaches the JSON extraction and
      // fails the whole statement with a cast error.
      expect(sql).toMatch(
         /FROM \(SELECT \* FROM bigquery_jobs[\s\S]*WHERE job_type = 'QUERY'\)/,
      );
   });

   it("escapes quotes in the query text it correlates on", async () => {
      const opts = bq([]);
      opts.sql = "SELECT 'it''s' AS x";
      await lookupBuildCost(opts);
      const sql = (opts.runner as sinon.SinonStub).firstCall.args[0] as string;
      expect(sql).toContain("SELECT ''it''''s'' AS x");
   });

   describe("snowflake", () => {
      const sf = (rows: unknown[]) => ({
         runner: sinon.stub().resolves({ rows }),
         engine: "snowflake" as const,
         sql: "SELECT 1",
         since: new Date("2026-08-12T00:00:00Z"),
      });

      it("reports scanned bytes and execution time, and no billed or slot figure", async () => {
         // Snowflake bills warehouse-seconds, so there is no per-query billed
         // quantity, and EXECUTION_TIME is wall clock — a different thing from
         // BigQuery's aggregate slot time. Neither is invented.
         const cost = await lookupBuildCost(
            sf([
               {
                  job_id: "01b2-c3",
                  bytes_scanned: 4_500_000,
                  execution_time_ms: 812,
                  rows_produced: 66,
               },
            ]),
         );
         expect(cost).toEqual({
            engine: "snowflake",
            jobId: "01b2-c3",
            bytesScanned: 4_500_000,
            bytesBilled: null,
            slotTimeMs: null,
            executionTimeMs: 812,
            cacheHit: false,
         });
      });

      it("infers a result-cache hit from scanning nothing while producing rows", async () => {
         const cost = await lookupBuildCost(
            sf([
               {
                  job_id: "q",
                  bytes_scanned: 0,
                  execution_time_ms: 4,
                  rows_produced: 66,
               },
            ]),
         );
         expect(cost?.cacheHit).toBe(true);
      });

      it("leaves cacheHit null rather than false when an input is missing", async () => {
         // Snowflake has no cache-hit column, so this is inference. An absent
         // input must not read as a positive "this was not cached".
         const cost = await lookupBuildCost(
            sf([
               {
                  job_id: "q",
                  bytes_scanned: null,
                  execution_time_ms: 4,
                  rows_produced: 66,
               },
            ]),
         );
         expect(cost?.cacheHit).toBeNull();
      });

      it("reads the low-latency history view, not the lagging one", async () => {
         const opts = sf([]);
         await lookupBuildCost(opts);
         const sql = (opts.runner as sinon.SinonStub).firstCall
            .args[0] as string;
         // ACCOUNT_USAGE lags by up to three hours, so it cannot answer a
         // question asked right after a build. BY_SESSION cannot be trusted
         // either: the passthrough gives no guarantee the session is reused.
         expect(sql).toContain("INFORMATION_SCHEMA.QUERY_HISTORY");
         expect(sql).not.toContain("ACCOUNT_USAGE");
         expect(sql).not.toContain("QUERY_HISTORY_BY_SESSION");
      });
   });
});
