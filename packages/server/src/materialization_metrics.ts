/**
 * Centralized telemetry for materialization builds.
 *
 * Operators need to answer "are builds completing, how long do they take, and
 * where are they failing?" without grepping logs. The run counter carries
 * `mode` (`auto`|`orchestrated`) and `outcome` (`success`|`failed`|`cancelled`);
 * the run-duration histogram carries `mode` so a dashboard can render auto-run
 * vs orchestrated build latency side by side.
 *
 * Lazy init for the same reason as {@link ./query_cap_metrics}: instruments
 * created before `setGlobalMeterProvider` bind to a NoOp meter
 * (https://github.com/open-telemetry/opentelemetry-js/issues/3505).
 */

import { metrics, type Counter, type Histogram } from "@opentelemetry/api";

export type MaterializationMode = "auto" | "orchestrated";
export type MaterializationOutcome = "success" | "failed" | "cancelled";
/** Manifest bind outcome: timeout is split out from generic failure on purpose. */
export type ManifestBindOutcome = "success" | "failure" | "timeout";

let runCounter: Counter | null = null;
let runDuration: Histogram | null = null;
let sourcesCounter: Counter | null = null;
let buildPlanComputeDuration: Histogram | null = null;
let autoLoadCounter: Counter | null = null;
let connectionDigestSkipCounter: Counter | null = null;
let manifestBindCounter: Counter | null = null;
let sourceBuildDuration: Histogram | null = null;
let dropTablesCounter: Counter | null = null;

function ensureTelemetry(): { counter: Counter; duration: Histogram } {
   if (runCounter && runDuration) {
      return { counter: runCounter, duration: runDuration };
   }
   const meter = metrics.getMeter("publisher");
   if (!runCounter) {
      runCounter = meter.createCounter("publisher_materialization_runs_total", {
         description:
            "Materialization builds completed. Labels: mode ('auto'|'orchestrated'), outcome ('success'|'failed'|'cancelled').",
      });
   }
   if (!runDuration) {
      runDuration = meter.createHistogram(
         "publisher_materialization_run_duration_ms",
         {
            description:
               "Wall-clock duration of a materialization build. Label: mode ('auto'|'orchestrated').",
            unit: "ms",
         },
      );
   }
   return { counter: runCounter, duration: runDuration };
}

/** Record the outcome and duration of a materialization build. */
export function recordMaterializationRun(
   mode: MaterializationMode,
   outcome: MaterializationOutcome,
   durationMs: number,
): void {
   const { counter, duration } = ensureTelemetry();
   counter.add(1, { mode, outcome });
   duration.record(durationMs, { mode });
}

/**
 * Record how many persist sources a run built vs. reused (carried forward
 * unchanged via skip-if-unchanged). Lets a dashboard show the reuse ratio,
 * the main lever on materialization cost.
 */
export function recordSourcesOutcome(
   outcome: "built" | "reused",
   count: number,
): void {
   if (count <= 0) return;
   if (!sourcesCounter) {
      sourcesCounter = metrics
         .getMeter("publisher")
         .createCounter("publisher_materialization_sources_total", {
            description:
               "Persist sources processed by a materialization run. Label: outcome ('built'|'reused').",
         });
   }
   sourcesCounter.add(count, { outcome });
}

/**
 * Record the wall-clock cost of compiling a package's build plan
 * (`Package.buildPlan`). This recompiles models at load, so it is worth
 * tracking as a discrete cost separate from the build itself.
 */
export function recordBuildPlanComputeDuration(durationMs: number): void {
   if (!buildPlanComputeDuration) {
      buildPlanComputeDuration = metrics
         .getMeter("publisher")
         .createHistogram(
            "publisher_materialization_build_plan_compute_duration_ms",
            {
               description:
                  "Wall-clock duration of compiling a package's build plan (Package.buildPlan).",
               unit: "ms",
            },
         );
   }
   buildPlanComputeDuration.record(durationMs);
}

/**
 * Record an auto-run manifest auto-load attempt. Auto-load is best-effort (a
 * failure does not fail the run), so a failure here is otherwise invisible:
 * the run is MANIFEST_FILE_READY but queries still resolve to the old tables.
 */
export function recordAutoLoadOutcome(outcome: "success" | "failure"): void {
   if (!autoLoadCounter) {
      autoLoadCounter = metrics
         .getMeter("publisher")
         .createCounter("publisher_materialization_auto_load_total", {
            description:
               "Auto-run manifest auto-load attempts. Label: outcome ('success'|'failure').",
         });
   }
   autoLoadCounter.add(1, { outcome });
}

/**
 * Record that a connection digest could not be computed during build-plan
 * compilation because the connection failed to resolve. The resulting buildIds
 * are computed without a digest, so this is a correctness signal, not just noise.
 */
export function recordConnectionDigestSkipped(): void {
   if (!connectionDigestSkipCounter) {
      connectionDigestSkipCounter = metrics
         .getMeter("publisher")
         .createCounter(
            "publisher_materialization_connection_digest_skipped_total",
            {
               description:
                  "Connection digests skipped during build-plan compile because the connection did not resolve.",
            },
         );
   }
   connectionDigestSkipCounter.add(1);
}

/**
 * Record a manifest-bind attempt (publisher binding a configured manifest to a
 * package at load). `timeout` is distinguished from `failure` so operators can
 * tell an unreachable/slow manifest store from a malformed manifest.
 */
export function recordManifestBind(outcome: ManifestBindOutcome): void {
   if (!manifestBindCounter) {
      manifestBindCounter = metrics
         .getMeter("publisher")
         .createCounter("publisher_materialization_manifest_bind_total", {
            description:
               "Manifest bind attempts. Label: outcome ('success'|'failure'|'timeout').",
         });
   }
   manifestBindCounter.add(1, { outcome });
}

/** Record the wall-clock duration of building one persist source's table. */
export function recordSourceBuildDuration(durationMs: number): void {
   if (!sourceBuildDuration) {
      sourceBuildDuration = metrics
         .getMeter("publisher")
         .createHistogram(
            "publisher_materialization_source_build_duration_ms",
            {
               description:
                  "Wall-clock duration of building a single persist source.",
               unit: "ms",
            },
         );
   }
   sourceBuildDuration.record(durationMs);
}

/** Record a best-effort physical-table drop on materialization delete. */
export function recordDropTables(outcome: "success" | "failure"): void {
   if (!dropTablesCounter) {
      dropTablesCounter = metrics
         .getMeter("publisher")
         .createCounter("publisher_materialization_drop_tables_total", {
            description:
               "Physical tables dropped on delete. Label: outcome ('success'|'failure').",
         });
   }
   dropTablesCounter.add(1, { outcome });
}

/** Visible for tests. Drops cached instruments so a fresh MeterProvider can capture emissions. */
export function resetMaterializationTelemetryForTesting(): void {
   runCounter = null;
   runDuration = null;
   sourcesCounter = null;
   buildPlanComputeDuration = null;
   autoLoadCounter = null;
   connectionDigestSkipCounter = null;
   manifestBindCounter = null;
   sourceBuildDuration = null;
   dropTablesCounter = null;
}
