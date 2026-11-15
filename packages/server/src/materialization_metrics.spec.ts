// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";

import {
   BUILD_DURATION_BUCKETS_MS,
   BUILD_KINDS,
   BUILD_METHODS,
   BUILD_OUTCOMES,
   BUILD_TIERS,
   FAILURE_CLASSES,
   INCREMENTAL_STEP_REASONS,
   MATERIALIZATION_MODES,
   MATERIALIZATION_OUTCOMES,
   recordBuildDuration,
   recordBuildOutcome,
   recordBuildReadBytes,
   recordIncrementalStep,
   recordStorageTableRetained,
   recordChainedStorageBuild,
   recordConnectionDigestSkipped,
   recordDropTables,
   recordEligibilityRefused,
   recordMaterializationRun,
   recordServeShapeTypeFallback,
   recordManifestBind,
   recordSourceBound,
   recordSourceDeclined,
   ROUTE_LEGS,
   ROUTE_OUTCOMES,
   ROUTE_REASONS,
   BIND_SCOPES,
   BIND_OUTCOMES,
   resetMaterializationTelemetryForTesting,
} from "./materialization_metrics";
import { SOURCE_DECLINE_REASONS } from "./service/materialization/domain/source_decline";
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

   it("counts one build per call, failure_class none unless failed", async () => {
      recordBuildOutcome({
         kind: "persist",
         tier: "colocated",
         outcome: "built",
      });
      recordBuildOutcome({
         kind: "persist",
         tier: "colocated",
         outcome: "reused",
      });
      recordBuildOutcome({
         kind: "rollup",
         tier: "storage",
         outcome: "failed",
         failureClass: "billed_read_lost",
      });

      const points = await harness.collectAttributes(
         "publisher_materialization_build_total",
      );
      expect(points).toHaveLength(3);
      for (const point of points) {
         expect(Object.keys(point).sort()).toEqual([
            "failure_class",
            "kind",
            "outcome",
            "tier",
         ]);
         expect(point.failure_class === "none").toBe(
            point.outcome !== "failed",
         );
      }
      expect(
         await harness.collectCounter("publisher_materialization_build_total", {
            kind: "rollup",
            tier: "storage",
            outcome: "failed",
            failure_class: "billed_read_lost",
         }),
      ).toBe(1);
      expect(
         await harness.collectCounter("publisher_materialization_build_total"),
      ).toBe(3);
   });

   it("times builds by kind, tier and method on the spec's buckets", async () => {
      recordBuildDuration(1234, {
         kind: "materialize",
         tier: "storage",
         method: "delta",
      });
      const histogram = await harness.collectHistogram(
         "publisher_materialization_build_duration_ms",
         { kind: "materialize", tier: "storage", method: "delta" },
      );
      expect(histogram).toMatchObject({ count: 1, sum: 1234 });
      expect(histogram.boundaries).toEqual(BUILD_DURATION_BUCKETS_MS);
      expect(BUILD_DURATION_BUCKETS_MS).toEqual([
         100, 500, 1000, 5000, 15000, 60000, 300000, 900000, 3600000,
      ]);
      expect(
         Object.keys(
            (
               await harness.collectAttributes(
                  "publisher_materialization_build_duration_ms",
               )
            )[0],
         ).sort(),
      ).toEqual(["kind", "method", "tier"]);
   });

   it("records bytes read only when the warehouse reported a positive count", async () => {
      recordBuildReadBytes(5e9, "colocated");
      recordBuildReadBytes(0, "colocated");
      recordBuildReadBytes(null, "storage");
      recordBuildReadBytes(undefined, "storage");

      const histogram = await harness.collectHistogram(
         "publisher_materialization_build_read_bytes",
      );
      expect(histogram).toMatchObject({ count: 1, sum: 5e9 });
      expect(histogram.boundaries).toEqual([
         1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13,
      ]);
      expect(
         await harness.collectAttributes(
            "publisher_materialization_build_read_bytes",
         ),
      ).toEqual([{ tier: "colocated" }]);
   });

   it("keeps every build label to the spec's vocabulary and series bound", () => {
      // Adding a value is a reviewed change: these lists are the contract
      // (materialization v2 plan, "Step 4 specification").
      expect([...BUILD_KINDS]).toEqual([
         "materialize",
         "persist",
         "rollup",
         "view",
         "pair",
      ]);
      expect([...BUILD_TIERS]).toEqual(["colocated", "storage"]);
      expect([...BUILD_METHODS]).toEqual(["full", "delta"]);
      expect([...BUILD_OUTCOMES]).toEqual([
         "built",
         "reused",
         "refused",
         "failed",
         "cancelled",
      ]);
      expect([...FAILURE_CLASSES]).toEqual([
         "none",
         "deterministic",
         "transient",
         "unclassified",
         "billed_read_lost",
      ]);
      expect([...MATERIALIZATION_MODES]).toEqual(["auto", "orchestrated"]);
      expect([...MATERIALIZATION_OUTCOMES]).toEqual([
         "success",
         "partial",
         "failed",
         "cancelled",
      ]);
      expect(INCREMENTAL_STEP_REASONS).toHaveLength(14);

      // build_total: failure_class is none for the four other outcomes, and
      // one of the four failure classes when failed: 5 x 2 x (4 + 4) = 80.
      const failedClasses = FAILURE_CLASSES.length - 1;
      const notFailed = BUILD_OUTCOMES.length - 1;
      expect(
         BUILD_KINDS.length * BUILD_TIERS.length * (notFailed + failedClasses),
      ).toBe(80);
      // build_duration_ms: 5 x 2 x 2 = 20; build_read_bytes: 2.
      expect(
         BUILD_KINDS.length * BUILD_TIERS.length * BUILD_METHODS.length,
      ).toBe(20);
      expect(BUILD_TIERS.length).toBe(2);
   });

   it("counts a retained storage table with no destination label", async () => {
      recordStorageTableRetained();
      expect(
         await harness.collectAttributes(
            "publisher_storage_tables_retained_total",
         ),
      ).toEqual([{}]);
   });

   it("labels an incremental step with its closed reason code", async () => {
      recordIncrementalStep("seed", "no_boundary");
      recordIncrementalStep("delta");
      expect(
         await harness.collectAttributes(
            "publisher_materialization_incremental_step_total",
         ),
      ).toEqual([{ step: "seed", reason: "no_boundary" }, { step: "delta" }]);
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

   it("counts serving's binds by scope, outcome and reason", async () => {
      recordManifestBind("success");
      recordManifestBind("timeout");
      recordSourceBound();
      recordSourceBound();
      recordSourceDeclined("build_plan_unavailable");
      recordSourceDeclined("classification_not_kept");
      const bind = (labels: Record<string, string>) =>
         harness.collectCounter("publisher_served_copy_bind_total", labels);
      expect(
         await bind({ scope: "manifest", outcome: "success", reason: "none" }),
      ).toBe(1);
      expect(
         await bind({ scope: "manifest", outcome: "timeout", reason: "none" }),
      ).toBe(1);
      expect(
         await bind({ scope: "source", outcome: "bound", reason: "none" }),
      ).toBe(2);
      expect(
         await bind({
            scope: "source",
            outcome: "declined",
            reason: "build_plan_unavailable",
         }),
      ).toBe(1);
      expect(
         await bind({
            scope: "source",
            outcome: "declined",
            reason: "classification_not_kept",
         }),
      ).toBe(1);
   });

   it("counts chained storage builds labeled by outcome", async () => {
      recordChainedStorageBuild("parent_reuse");
      recordChainedStorageBuild("parent_reuse");
      recordChainedStorageBuild("inline_fallback");
      recordChainedStorageBuild("strict_shape_fallback");
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
            outcome: "strict_shape_fallback",
         }),
      ).toBe(1);
      expect(
         await harness.collectCounter("publisher_storage_chained_build_total", {
            outcome: "strict_refused",
         }),
      ).toBe(1);
   });

   it("counts eligibility refusals and type fallbacks", async () => {
      recordEligibilityRefused("given");
      recordEligibilityRefused("free_parameter");
      recordServeShapeTypeFallback("array");

      expect(
         await harness.collectCounter(
            "publisher_materialization_eligibility_refused_total",
            { reason: "given" },
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

// Each label of the routing and bind counters is a closed union; adding a
// value is a reviewed change to these lists (Step 4 specification).
describe("the routing and bind label vocabularies", () => {
   it("names exactly the route counter's legs, outcomes and reasons", () => {
      expect([...ROUTE_LEGS]).toEqual(["view", "rollup", "served", "live"]);
      expect([...ROUTE_OUTCOMES]).toEqual(["used", "skipped", "fallback"]);
      expect([...ROUTE_REASONS]).toEqual([
         "NONE",
         "NOT_BUILT",
         "WARNED",
         "MISSING_FIELD",
         "CONTRACT",
         "GRAIN_TOO_COARSE",
         "TIME_ZONE",
         "CROSS_CONNECTION",
         "COUNT_SEMANTICS",
         "STALE",
         "RUN_FAILED",
      ]);
   });

   it("names exactly the bind counter's scopes, outcomes and reasons", () => {
      expect([...BIND_SCOPES]).toEqual(["manifest", "source"]);
      expect([...BIND_OUTCOMES]).toEqual([
         "success",
         "failure",
         "timeout",
         "bound",
         "declined",
      ]);
      expect(["none", ...SOURCE_DECLINE_REASONS]).toEqual([
         "none",
         "build_plan_unavailable",
         "refused",
         "missing_table",
         "compile_failed",
         "cross_connection_unbuilt",
         "column_types",
         "no_declaration",
         "range_not_found",
         "definition_not_parsed",
         "pair_join_not_found",
         "pair_not_compiled",
         "fields_not_carried",
         "read_terms_not_kept",
         "cross_connection_join",
         "inline_join_kept",
         "reroot_not_compiled",
         "classification_not_kept",
      ]);
   });
});
