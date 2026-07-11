import { afterEach, beforeEach, describe, expect, it } from "bun:test";

import {
   recordPackageLoadPhases,
   resetPackageLoadMetricsForTesting,
} from "./package_load_metrics";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "./test_helpers/metrics_harness";

describe("package_load_metrics", () => {
   let harness: MetricsHarness;

   beforeEach(async () => {
      harness = await startMetricsHarness();
      resetPackageLoadMetricsForTesting();
   });

   afterEach(async () => {
      resetPackageLoadMetricsForTesting();
      await harness.shutdown();
   });

   it("records the three phase histograms, labeled by status", async () => {
      recordPackageLoadPhases(
         {
            compileDurationMs: 120,
            schemaFetchDurationMs: 80,
            schemaFetchCount: 4,
         },
         "success",
      );
      recordPackageLoadPhases(
         {
            compileDurationMs: 40,
            schemaFetchDurationMs: 10,
            schemaFetchCount: 1,
         },
         "success",
      );

      const compile = await harness.collectHistogram(
         "malloy_package_load_compile_duration",
         { status: "success" },
      );
      expect(compile.count).toBe(2);
      expect(compile.sum).toBe(160);

      const fetchDuration = await harness.collectHistogram(
         "malloy_package_load_schema_fetch_duration",
         { status: "success" },
      );
      expect(fetchDuration.count).toBe(2);
      expect(fetchDuration.sum).toBe(90);

      const fetches = await harness.collectHistogram(
         "malloy_package_load_schema_fetches",
         { status: "success" },
      );
      expect(fetches.count).toBe(2);
      expect(fetches.sum).toBe(5);
   });

   it("labels phases by terminal status, so failures are separable from successes", async () => {
      recordPackageLoadPhases(
         {
            compileDurationMs: 30,
            schemaFetchDurationMs: 5,
            schemaFetchCount: 1,
         },
         "success",
      );
      recordPackageLoadPhases(
         {
            compileDurationMs: 12,
            schemaFetchDurationMs: 3,
            schemaFetchCount: 1,
         },
         "compilation_error",
      );

      const ok = await harness.collectHistogram(
         "malloy_package_load_compile_duration",
         { status: "success" },
      );
      const failed = await harness.collectHistogram(
         "malloy_package_load_compile_duration",
         { status: "compilation_error" },
      );
      expect(ok.count).toBe(1);
      expect(ok.sum).toBe(30);
      expect(failed.count).toBe(1);
      expect(failed.sum).toBe(12);
   });

   it("resolves the slow-load tail past OTel's default 10s cap", async () => {
      // A 90s load would fall in the +Inf bucket under OTel's default
      // boundaries (top = 10000ms); the explicit buckets must extend further.
      recordPackageLoadPhases(
         {
            compileDurationMs: 90_000,
            schemaFetchDurationMs: 0,
            schemaFetchCount: 0,
         },
         "success",
      );
      const compile = await harness.collectHistogram(
         "malloy_package_load_compile_duration",
      );
      expect(compile.boundaries.some((b) => b > 10_000)).toBe(true);
      expect(compile.boundaries[compile.boundaries.length - 1]).toBe(300_000);
   });
});
