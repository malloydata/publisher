// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { readFileSync } from "fs";
import * as path from "path";

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
   DROP_OCCASIONS,
   KEY_CHECK_KINDS,
   KEY_CHECK_OUTCOMES,
   recordQueryWalk,
   recordAnnotationFindings,
   recordAttributionSkipped,
   recordAutoLoadOutcome,
   recordBuildDuration,
   recordBuildOutcome,
   recordBuildPlanComputeDuration,
   recordBuildPlanComputeFailed,
   recordBuildReadBytes,
   recordDuplicateTargetSkipped,
   recordIncrementalStep,
   recordContractCapture,
   recordIncompatibleDetected,
   recordKeyCheck,
   recordManifestBindDegraded,
   recordQueryRoute,
   recordQueryRouteDuration,
   recordScheduledFire,
   recordSchedulerTickSkipped,
   recordSharedAddressInstructions,
   recordUpstreamReadUnchecked,
   recordStoredColumnTypeMismatch,
   recordTableCollision,
   recordStorageTableRetained,
   recordChainedStorageBuild,
   recordConnectionDigestSkipped,
   recordDropTables,
   recordEligibilityRefused,
   recordMaterializationRun,
   recordStoredTypeFallback,
   recordManifestBind,
   recordSourceBound,
   recordServedCopyOutcome,
   recordSourceDeclined,
   ROUTE_LEGS,
   routeLegOf,
   ROUTE_OUTCOMES,
   ROUTE_REASONS,
   BIND_SCOPES,
   BIND_OUTCOMES,
   resetMaterializationTelemetryForTesting,
} from "./materialization_metrics";
import {
   SOURCE_DECLINE_REASONS,
   servedCopyOutcomes,
   servesInPart,
} from "./service/materialization/domain/source_decline";
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
      // The duration carries the outcome too, so a failed run's time is
      // read apart from a successful one's.
      const failed = await harness.collectHistogram(
         "publisher_materialization_run_duration_ms",
         { mode: "orchestrated", outcome: "failed" },
      );
      expect(failed).toMatchObject({ count: 1, sum: 5 });
      const succeeded = await harness.collectHistogram(
         "publisher_materialization_run_duration_ms",
         { mode: "auto", outcome: "success" },
      );
      expect(succeeded).toMatchObject({ count: 2, sum: 30 });
   });

   it("counts one build per call, failure_class none unless failed", async () => {
      recordBuildOutcome({
         kind: "persist",
         tier: "database",
         outcome: "built",
      });
      recordBuildOutcome({
         kind: "persist",
         tier: "database",
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
      recordBuildReadBytes(5e9, "database");
      recordBuildReadBytes(0, "database");
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
      ).toEqual([{ tier: "database" }]);
   });

   it("keeps every build label to the spec's vocabulary and series bound", () => {
      // Adding a value is a reviewed change: these lists are the contract
      // (materialization v2 plan, "Step 4 specification").
      expect([...BUILD_KINDS]).toEqual([
         "materialize",
         "persist",
         "rollup",
         "view",
         "query",
         "pair",
      ]);
      expect([...BUILD_TIERS]).toEqual(["database", "storage"]);
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
      expect(INCREMENTAL_STEP_REASONS).toHaveLength(19);

      // build_total: failure_class is none for the four other outcomes, and
      // one of the four failure classes when failed: 6 x 2 x (4 + 4) = 96.
      const failedClasses = FAILURE_CLASSES.length - 1;
      const notFailed = BUILD_OUTCOMES.length - 1;
      expect(
         BUILD_KINDS.length * BUILD_TIERS.length * (notFailed + failedClasses),
      ).toBe(96);
      // build_duration_ms: 6 x 2 x 2 = 24; build_read_bytes: 2.
      expect(
         BUILD_KINDS.length * BUILD_TIERS.length * BUILD_METHODS.length,
      ).toBe(24);
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

   it("labels an annotation finding by outcome in the warning suffixes' words", async () => {
      recordAnnotationFindings([
         { code: "persist-unknown-key", severity: "error" },
         { code: "persist-not-built", severity: "warn" },
      ]);
      expect(
         await harness.collectAttributes("publisher_annotation_findings_total"),
      ).toEqual([
         { code: "persist-unknown-key", outcome: "invalid" },
         { code: "persist-not-built", outcome: "not_built" },
      ]);
   });

   it("counts drop-table outcomes by tier and occasion, and connection-digest skips", async () => {
      recordDropTables("success", "storage", "delete");
      recordDropTables("failure", "database", "reclaim");
      recordConnectionDigestSkipped();

      expect(
         await harness.collectCounter(
            "publisher_materialization_drop_tables_total",
            { outcome: "failure", tier: "database", occasion: "reclaim" },
         ),
      ).toBe(1);
      expect(
         await harness.collectCounter(
            "publisher_materialization_drop_tables_total",
            { outcome: "success", tier: "storage", occasion: "delete" },
         ),
      ).toBe(1);
      expect(DROP_OCCASIONS).toEqual(["delete", "reclaim", "caller"]);
      expect(
         await harness.collectCounter(
            "publisher_materialization_connection_digest_skipped_total",
         ),
      ).toBe(1);
   });

   it("counts a served-copy source by the outcome resolved for it", async () => {
      recordServedCopyOutcome("partial", "inline_join_kept");
      recordServedCopyOutcome("declined", "column_types");
      const bind = (labels: Record<string, string>) =>
         harness.collectCounter("publisher_served_copy_bind_total", labels);
      expect(
         await bind({
            scope: "source",
            outcome: "partial",
            reason: "inline_join_kept",
         }),
      ).toBe(1);
      expect(
         await bind({
            scope: "source",
            outcome: "declined",
            reason: "column_types",
         }),
      ).toBe(1);
   });

   it("counts serving's binds by scope, outcome and reason", async () => {
      recordManifestBind("success");
      recordManifestBind("timeout");
      recordSourceBound();
      recordSourceBound();
      recordSourceDeclined("build_plan_unavailable");
      recordServedCopyOutcome("declined", "classification_not_kept");
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
      recordChainedStorageBuild("strict_refused");

      expect(
         await harness.collectCounter("publisher_storage_chained_build_total", {
            outcome: "parent_reuse",
         }),
      ).toBe(2);
      expect(
         await harness.collectCounter("publisher_storage_chained_build_total", {
            outcome: "strict_refused",
         }),
      ).toBe(1);
   });

   it("counts eligibility refusals and type fallbacks", async () => {
      recordEligibilityRefused("given");
      recordEligibilityRefused("free_parameter");
      recordStoredTypeFallback("array");

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

/**
 * One emission of every instrument `materialization_metrics.ts` declares,
 * read back through the harness. The list is checked against the file's own
 * instrument names, so an instrument added without a case here fails.
 */
const EMISSIONS: Record<string, () => void> = {
   publisher_query_walks_total: () => recordQueryWalk("scope"),
   publisher_materialization_runs_total: () =>
      recordMaterializationRun("auto", "success", 1),
   publisher_materialization_run_duration_ms: () =>
      recordMaterializationRun("orchestrated", "partial", 1),
   publisher_materialization_build_total: () =>
      recordBuildOutcome({
         kind: "persist",
         tier: "database",
         outcome: "built",
      }),
   publisher_materialization_build_duration_ms: () =>
      recordBuildDuration(1, { kind: "view", tier: "storage", method: "full" }),
   publisher_materialization_build_read_bytes: () =>
      recordBuildReadBytes(1_000, "database"),
   publisher_annotation_findings_total: () =>
      recordAnnotationFindings([
         { code: "persist-not-built", severity: "warn" },
      ]),
   publisher_materialization_incremental_step_total: () =>
      recordIncrementalStep("seed", "forced"),
   publisher_materialization_build_plan_compute_duration_ms: () =>
      recordBuildPlanComputeDuration(1, "worker"),
   publisher_materialization_build_plan_compute_failed_total: () =>
      recordBuildPlanComputeFailed(),
   publisher_materialization_auto_load_total: () =>
      recordAutoLoadOutcome("success"),
   publisher_materialization_connection_digest_skipped_total: () =>
      recordConnectionDigestSkipped(),
   publisher_served_copy_bind_total: () => recordSourceBound(),
   publisher_materialization_manifest_bind_degraded_total: () =>
      recordManifestBindDegraded(),
   publisher_materialization_duplicate_target_skipped_total: () =>
      recordDuplicateTargetSkipped(),
   publisher_materialization_shared_address_instructions_total: () =>
      recordSharedAddressInstructions(),
   publisher_materialization_table_collision_total: () =>
      recordTableCollision(),
   publisher_materialization_drop_tables_total: () =>
      recordDropTables("success", "database", "reclaim"),
   publisher_materialization_scheduled_fires_total: () =>
      recordScheduledFire("fired"),
   publisher_materialization_scheduler_ticks_skipped_total: () =>
      recordSchedulerTickSkipped(),
   publisher_query_route_total: () =>
      recordQueryRoute("rollup", "used", "NONE"),
   publisher_query_route_duration_ms: () => recordQueryRouteDuration(1, "live"),
   publisher_materialization_key_check_total: () =>
      recordKeyCheck("parent", "not_unique"),
   publisher_materialization_incompatible_detected_total: () =>
      recordIncompatibleDetected("bind", "column_retyped"),
   publisher_materialization_contract_capture_total: () =>
      recordContractCapture("storage", "unread"),
   publisher_storage_tables_retained_total: () => recordStorageTableRetained(),
   publisher_storage_build_attribution_skipped_total: () =>
      recordAttributionSkipped("tag_failed"),
   publisher_materialization_eligibility_refused_total: () =>
      recordEligibilityRefused("authorize"),
   publisher_storage_serve_shape_type_fallback_total: () =>
      recordStoredTypeFallback("unrecognized"),
   publisher_storage_column_type_mismatch_total: () =>
      recordStoredColumnTypeMismatch("serve", "number"),
   publisher_storage_chained_build_total: () =>
      recordChainedStorageBuild("parent_reuse"),
   publisher_materialization_upstream_recompute_total: () =>
      recordUpstreamReadUnchecked("chained"),
};

/** Every instrument name `materialization_metrics.ts` passes to a lazy constructor. */
function declaredInstruments(): string[] {
   const text = readFileSync(
      path.join(__dirname, "materialization_metrics.ts"),
      "utf8",
   );
   return [...text.matchAll(/lazy(?:Counter|Histogram)\(\s*"([a-z_]+)"/g)].map(
      (m) => m[1],
   );
}

describe("every materialization instrument", () => {
   let harness: MetricsHarness;

   beforeEach(async () => {
      harness = await startMetricsHarness();
      resetMaterializationTelemetryForTesting();
   });

   afterEach(async () => {
      resetMaterializationTelemetryForTesting();
      await harness.shutdown();
   });

   it("has an emission case for each instrument the module declares", () => {
      expect(Object.keys(EMISSIONS).sort()).toEqual(
         declaredInstruments().sort(),
      );
   });

   for (const [name, emit] of Object.entries(EMISSIONS)) {
      it(`emits ${name}`, async () => {
         emit();
         const points = await harness.collectAttributes(name);
         expect(points.length).toBeGreaterThan(0);
      });
   }

   it("labels key checks by kind and outcome from closed lists", async () => {
      for (const kind of KEY_CHECK_KINDS) {
         for (const outcome of KEY_CHECK_OUTCOMES)
            recordKeyCheck(kind, outcome);
      }
      expect(
         await harness.collectCounter(
            "publisher_materialization_key_check_total",
            { kind: "inline_join", outcome: "reused" },
         ),
      ).toBe(1);
      expect(
         await harness.collectAttributes(
            "publisher_materialization_key_check_total",
         ),
      ).toHaveLength(KEY_CHECK_KINDS.length * KEY_CHECK_OUTCOMES.length);
   });

   it("times a route by the leg that answered", async () => {
      recordQueryRouteDuration(4, "materialized");
      recordQueryRouteDuration(6, "materialized");
      expect(
         await harness.collectHistogram("publisher_query_route_duration_ms", {
            leg: "materialized",
         }),
      ).toMatchObject({ count: 2, sum: 10 });
   });
});

// Each label of the routing and bind counters is a closed union; adding a
// value is a reviewed change to these lists (Step 4 specification).
describe("the routing and bind label vocabularies", () => {
   it("names exactly the route counter's legs, outcomes and reasons", () => {
      expect([...ROUTE_LEGS]).toEqual([
         "view",
         "query",
         "rollup",
         "materialized",
         "live",
      ]);
      expect([...ROUTE_OUTCOMES]).toEqual(["used", "skipped", "fallback"]);
      expect([...ROUTE_REASONS]).toEqual([
         "NONE",
         "NOT_BUILT",
         "NOT_SERVED",
         "OPTED_OUT",
         "MISSING_FIELD",
         "SHAPE_MISMATCH",
         "ROW_LEVEL_GATE",
         "GRAIN_TOO_COARSE",
         "TIME_ZONE",
         "CROSS_CONNECTION",
         "COUNT_SEMANTICS",
         "STALE",
         "READ_FAILED",
         "WARNED",
         "CONTRACT",
         "RUN_FAILED",
      ]);
   });

   it("names the served copy's leg `materialized`, as the path report names its answer", () => {
      expect(routeLegOf("served")).toBe("materialized");
      expect(routeLegOf("rollup")).toBe("rollup");
      expect(routeLegOf("live")).toBe("live");
   });

   it("names exactly the bind counter's scopes, outcomes and reasons", () => {
      expect([...BIND_SCOPES]).toEqual(["manifest", "source"]);
      expect([...BIND_OUTCOMES]).toEqual([
         "success",
         "failure",
         "timeout",
         "bound",
         "partial",
         "declined",
      ]);
      expect(["none", ...SOURCE_DECLINE_REASONS]).toEqual([
         "none",
         "build_plan_unavailable",
         "refused",
         "missing_table",
         "unquotable_name",
         "compile_failed",
         "cross_connection_unbuilt",
         "column_types",
         "no_declaration",
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

describe("which source declines still serve the source", () => {
   it("is exactly the served copy's two partial reasons", () => {
      expect(SOURCE_DECLINE_REASONS.filter(servesInPart)).toEqual([
         "cross_connection_join",
         "inline_join_kept",
      ]);
   });
});

describe("one served-copy outcome per source per compile", () => {
   const ids = (...xs: string[]) => new Set(xs);
   it("counts a source with two partial reasons once, while it is re-rooted", () => {
      expect(
         servedCopyOutcomes(
            [
               { sourceID: "s", reason: "inline_join_kept" },
               { sourceID: "s", reason: "inline_join_kept" },
               { sourceID: "s", reason: "cross_connection_join" },
            ],
            ids("s"),
         ),
      ).toEqual([{ outcome: "partial", reason: "inline_join_kept" }]);
   });

   it("declines a source that is partial and then dropped", () => {
      expect(
         servedCopyOutcomes(
            [{ sourceID: "s", reason: "inline_join_kept" }],
            ids(),
         ),
      ).toEqual([{ outcome: "declined", reason: "inline_join_kept" }]);
   });

   it("lets declined beat partial on one source, by its leaving-out reason", () => {
      expect(
         servedCopyOutcomes(
            [
               { sourceID: "s", reason: "cross_connection_join" },
               { sourceID: "s", reason: "classification_not_kept" },
            ],
            ids("s"),
         ),
      ).toEqual([{ outcome: "declined", reason: "classification_not_kept" }]);
   });

   it("keeps sources apart, and counts a problem naming no source on its own", () => {
      expect(
         servedCopyOutcomes(
            [
               { sourceID: "a", reason: "inline_join_kept" },
               { sourceName: "b", reason: "column_types" },
               { reason: "cross_connection_unbuilt" },
            ],
            ids("a"),
         ),
      ).toEqual([
         { outcome: "declined", reason: "cross_connection_unbuilt" },
         { outcome: "partial", reason: "inline_join_kept" },
         { outcome: "declined", reason: "column_types" },
      ]);
   });
});
