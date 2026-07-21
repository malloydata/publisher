import { afterEach, beforeEach, describe, expect, it } from "bun:test";

import {
   recordChainedStorageBuild,
   recordConnectionDigestSkipped,
   recordDropTables,
   recordEligibilityRefused,
   recordMaterializationRun,
   recordServeShapeTierDrop,
   recordServeShapeTypeFallback,
   recordSourcesOutcome,
   recordStorageBuildFailure,
   recordStorageServeRouting,
   resetMaterializationTelemetryForTesting,
} from "./materialization_metrics";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "./test_helpers/metrics_harness";

describe("materialization_metrics", () => {
   let harness: MetricsHarness;

   beforeEach(async () => {
      harness = await startMetricsHarness();
      // Drop cached instruments so they re-bind to this test's provider.
      resetMaterializationTelemetryForTesting();
   });

   afterEach(async () => {
      resetMaterializationTelemetryForTesting();
      await harness.shutdown();
   });

   it("counts runs labeled by mode and outcome", async () => {
      recordMaterializationRun("auto", "success", 10);
      recordMaterializationRun("auto", "success", 20);
      recordMaterializationRun("orchestrated", "failed", 5);

      expect(
         await harness.collectCounter("publisher_materialization_runs_total", {
            mode: "auto",
            outcome: "success",
         }),
      ).toBe(2);
      expect(
         await harness.collectCounter("publisher_materialization_runs_total", {
            mode: "orchestrated",
            outcome: "failed",
         }),
      ).toBe(1);
   });

   it("adds the source count labeled by outcome, ignoring non-positive counts", async () => {
      recordSourcesOutcome("built", 3);
      recordSourcesOutcome("reused", 2);
      recordSourcesOutcome("built", 0); // guarded: no emission

      expect(
         await harness.collectCounter(
            "publisher_materialization_sources_total",
            { outcome: "built" },
         ),
      ).toBe(3);
      expect(
         await harness.collectCounter(
            "publisher_materialization_sources_total",
            { outcome: "reused" },
         ),
      ).toBe(2);
   });

   it("counts drop-table outcomes (labeled by engine) and connection-digest skips", async () => {
      recordDropTables("success", "storage");
      recordDropTables("failure", "in_warehouse");
      recordConnectionDigestSkipped();

      expect(
         await harness.collectCounter(
            "publisher_materialization_drop_tables_total",
            { outcome: "failure", engine: "in_warehouse" },
         ),
      ).toBe(1);
      expect(
         await harness.collectCounter(
            "publisher_materialization_drop_tables_total",
            { outcome: "success", engine: "storage" },
         ),
      ).toBe(1);
      expect(
         await harness.collectCounter(
            "publisher_materialization_connection_digest_skipped_total",
         ),
      ).toBe(1);
   });

   it("counts storage= serve-routing decisions labeled by outcome", async () => {
      recordStorageServeRouting("storage");
      recordStorageServeRouting("storage");
      recordStorageServeRouting("live_fallback");

      expect(
         await harness.collectCounter("publisher_storage_serve_routing_total", {
            outcome: "storage",
         }),
      ).toBe(2);
      expect(
         await harness.collectCounter("publisher_storage_serve_routing_total", {
            outcome: "live_fallback",
         }),
      ).toBe(1);
   });

   it("counts chained storage builds labeled by outcome", async () => {
      recordChainedStorageBuild("parent_reuse");
      recordChainedStorageBuild("parent_reuse");
      recordChainedStorageBuild("inline_fallback");
      recordChainedStorageBuild("strict_refused");

      expect(
         await harness.collectCounter("publisher_storage_chained_build_total", {
            outcome: "parent_reuse",
         }),
      ).toBe(2);
      expect(
         await harness.collectCounter("publisher_storage_chained_build_total", {
            outcome: "inline_fallback",
         }),
      ).toBe(1);
      expect(
         await harness.collectCounter("publisher_storage_chained_build_total", {
            outcome: "strict_refused",
         }),
      ).toBe(1);
   });

   it("counts storage build failures, eligibility refusals, tier drops, and type fallbacks", async () => {
      recordStorageBuildFailure("lake");
      recordEligibilityRefused("given");
      recordEligibilityRefused("free_parameter");
      recordServeShapeTierDrop(0);
      recordServeShapeTypeFallback("array");

      expect(
         await harness.collectCounter(
            "publisher_storage_build_failures_total",
            { destination: "lake" },
         ),
      ).toBe(1);
      expect(
         await harness.collectCounter(
            "publisher_materialization_eligibility_refused_total",
            { reason: "given" },
         ),
      ).toBe(1);
      expect(
         await harness.collectCounter(
            "publisher_storage_serve_shape_tier_drop_total",
            { tier: "0" },
         ),
      ).toBe(1);
      expect(
         await harness.collectCounter(
            "publisher_storage_serve_shape_type_fallback_total",
            { kind: "array" },
         ),
      ).toBe(1);
   });
});
