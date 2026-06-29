import { afterEach, beforeEach, describe, expect, it } from "bun:test";

import {
   recordConnectionDigestSkipped,
   recordDropTables,
   recordMaterializationRun,
   recordSourcesOutcome,
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

   it("counts drop-table outcomes and connection-digest skips", async () => {
      recordDropTables("success");
      recordDropTables("failure");
      recordConnectionDigestSkipped();

      expect(
         await harness.collectCounter(
            "publisher_materialization_drop_tables_total",
            { outcome: "failure" },
         ),
      ).toBe(1);
      expect(
         await harness.collectCounter(
            "publisher_materialization_connection_digest_skipped_total",
         ),
      ).toBe(1);
   });
});
