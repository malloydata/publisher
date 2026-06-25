/**
 * Centralized telemetry for materialization builds.
 *
 * Operators need to answer "are builds completing, how long do they take, and
 * where are they failing?" without grepping logs. The run counter carries
 * `mode` (`auto`|`orchestrated`) and `outcome` (`success`|`failed`|`cancelled`);
 * the run-duration histogram carries `mode` so a dashboard can render auto-run
 * vs orchestrated build latency side by side.
 *
 * Instruments are created lazily for the same reason as {@link ./query_cap_metrics}:
 * one created before `setGlobalMeterProvider` binds to a NoOp meter
 * (https://github.com/open-telemetry/opentelemetry-js/issues/3505).
 */

import { metrics, type Counter, type Histogram } from "@opentelemetry/api";

export type MaterializationMode = "auto" | "orchestrated";
export type MaterializationOutcome = "success" | "failed" | "cancelled";
/** Manifest bind outcome: timeout is split out from generic failure on purpose. */
export type ManifestBindOutcome = "success" | "failure" | "timeout";

const resetHooks: (() => void)[] = [];

/**
 * A lazily-created instrument: built from the global meter on first use (so it
 * binds to the real MeterProvider, not the NoOp default) and memoized. Each
 * registers a reset hook so a test can swap in a fresh MeterProvider.
 */
function lazyCounter(name: string, description: string): () => Counter {
   let instrument: Counter | null = null;
   resetHooks.push(() => (instrument = null));
   return () =>
      (instrument ??= metrics
         .getMeter("publisher")
         .createCounter(name, { description }));
}

function lazyHistogram(
   name: string,
   description: string,
   unit: string,
): () => Histogram {
   let instrument: Histogram | null = null;
   resetHooks.push(() => (instrument = null));
   return () =>
      (instrument ??= metrics
         .getMeter("publisher")
         .createHistogram(name, { description, unit }));
}

const runCounter = lazyCounter(
   "publisher_materialization_runs_total",
   "Materialization builds completed. Labels: mode ('auto'|'orchestrated'), outcome ('success'|'failed'|'cancelled').",
);
const runDuration = lazyHistogram(
   "publisher_materialization_run_duration_ms",
   "Wall-clock duration of a materialization build. Label: mode ('auto'|'orchestrated').",
   "ms",
);
const sourcesCounter = lazyCounter(
   "publisher_materialization_sources_total",
   "Persist sources processed by a materialization run. Label: outcome ('built'|'reused').",
);
const buildPlanComputeDuration = lazyHistogram(
   "publisher_materialization_build_plan_compute_duration_ms",
   "Wall-clock duration of compiling a package's build plan (Package.buildPlan).",
   "ms",
);
const autoLoadCounter = lazyCounter(
   "publisher_materialization_auto_load_total",
   "Auto-run manifest auto-load attempts. Label: outcome ('success'|'failure').",
);
const connectionDigestSkipCounter = lazyCounter(
   "publisher_materialization_connection_digest_skipped_total",
   "Connection digests skipped during build-plan compile because the connection did not resolve.",
);
const manifestBindCounter = lazyCounter(
   "publisher_materialization_manifest_bind_total",
   "Manifest bind attempts. Label: outcome ('success'|'failure'|'timeout').",
);
const sourceBuildDuration = lazyHistogram(
   "publisher_materialization_source_build_duration_ms",
   "Wall-clock duration of building a single persist source.",
   "ms",
);
const dropTablesCounter = lazyCounter(
   "publisher_materialization_drop_tables_total",
   "Physical tables dropped on delete. Label: outcome ('success'|'failure').",
);

/** Record the outcome and duration of a materialization build. */
export function recordMaterializationRun(
   mode: MaterializationMode,
   outcome: MaterializationOutcome,
   durationMs: number,
): void {
   runCounter().add(1, { mode, outcome });
   runDuration().record(durationMs, { mode });
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
   sourcesCounter().add(count, { outcome });
}

/**
 * Record the wall-clock cost of compiling a package's build plan
 * (`Package.buildPlan`). This recompiles models at load, so it is worth
 * tracking as a discrete cost separate from the build itself.
 */
export function recordBuildPlanComputeDuration(durationMs: number): void {
   buildPlanComputeDuration().record(durationMs);
}

/**
 * Record an auto-run manifest auto-load attempt. Auto-load is best-effort (a
 * failure does not fail the run), so a failure here is otherwise invisible:
 * the run is MANIFEST_FILE_READY but queries still resolve to the old tables.
 */
export function recordAutoLoadOutcome(outcome: "success" | "failure"): void {
   autoLoadCounter().add(1, { outcome });
}

/**
 * Record that a connection digest could not be computed during build-plan
 * compilation because the connection failed to resolve. The resulting buildIds
 * are computed without a digest, so this is a correctness signal, not just noise.
 */
export function recordConnectionDigestSkipped(): void {
   connectionDigestSkipCounter().add(1);
}

/**
 * Record a manifest-bind attempt (publisher binding a configured manifest to a
 * package at load). `timeout` is distinguished from `failure` so operators can
 * tell an unreachable/slow manifest store from a malformed manifest.
 */
export function recordManifestBind(outcome: ManifestBindOutcome): void {
   manifestBindCounter().add(1, { outcome });
}

/** Record the wall-clock duration of building one persist source's table. */
export function recordSourceBuildDuration(durationMs: number): void {
   sourceBuildDuration().record(durationMs);
}

/** Record a best-effort physical-table drop on materialization delete. */
export function recordDropTables(outcome: "success" | "failure"): void {
   dropTablesCounter().add(1, { outcome });
}

/** Visible for tests. Drops cached instruments so a fresh MeterProvider can capture emissions. */
export function resetMaterializationTelemetryForTesting(): void {
   for (const reset of resetHooks) reset();
}
