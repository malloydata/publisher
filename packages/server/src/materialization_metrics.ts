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

import { type Counter, type Histogram } from "@opentelemetry/api";
import { publisherMeter } from "./telemetry";

export type MaterializationMode = "auto" | "orchestrated";
export type MaterializationOutcome = "success" | "failed" | "cancelled";
/** Manifest bind outcome: timeout is split out from generic failure on purpose. */
export type ManifestBindOutcome = "success" | "failure" | "timeout";
/**
 * Which engine a source build / table drop ran against: the source's own
 * warehouse (colocated, in-warehouse CTAS) or a DuckDB/DuckLake `storage=`
 * destination (federated passthrough CTAS). The two have very different latency
 * and failure profiles, so build/drop metrics are labeled with it rather than
 * pooling the two into one series.
 */
export type StorageBuildEngine = "storage" | "in_warehouse";
/** Why a source was refused materialization into a storage destination. */
export type EligibilityRefusalReason =
   | "free_parameter"
   | "given"
   | "authorize"
   | "not_duckdb_portable"
   | "public_surface_unknown";
/**
 * How a chained `storage=` source (one that reads a storage-materialized
 * upstream) was built: `parent_reuse` = built by reading the upstream's stored
 * lake table ("stack on the parent" — reuses the parent's work and is
 * consistent-by-construction); `inline_fallback` = stacking was ineligible/failed
 * so the upstream was recomputed from raw against the warehouse (non-strict);
 * `strict_refused` = stacking was ineligible under `strictUpstreams`, so
 * the build failed loudly rather than silently recomputing. This is the headline
 * signal for how far the parent-reuse path gets us in practice.
 */
export type ChainedStorageBuildOutcome =
   | "parent_reuse"
   | "inline_fallback"
   | "strict_refused";

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
      (instrument ??= publisherMeter().createCounter(name, { description }));
}

function lazyHistogram(
   name: string,
   description: string,
   unit: string,
): () => Histogram {
   let instrument: Histogram | null = null;
   resetHooks.push(() => (instrument = null));
   return () =>
      (instrument ??= publisherMeter().createHistogram(name, {
         description,
         unit,
      }));
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
const manifestBindDegradedCounter = lazyCounter(
   "publisher_materialization_manifest_bind_degraded_total",
   "Manifest entries bound with an UNQUOTED table path because their connection " +
      "could not be resolved (serve-side bind) or is absent from the build " +
      "(build-side seed). A misconfiguration that breaks the source on a " +
      "case-folding engine (Snowflake); alertable.",
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
const scheduledFireCounter = lazyCounter(
   "publisher_materialization_scheduled_fires_total",
   "Standalone-scheduler attempts to fire a package's materialization.schedule. " +
      "Label: outcome ('fired'|'conflict'|'error').",
);
const storageServeRoutingCounter = lazyCounter(
   "publisher_storage_serve_routing_total",
   "storage= serve routing decisions. Label: outcome ('storage'|'live_fallback').",
);
const storageBuildFailureCounter = lazyCounter(
   "publisher_storage_build_failures_total",
   "storage= build failures (federation/passthrough/attach/CTAS), distinct from " +
      "in-warehouse build failures. Label: destination (connection name).",
);
const eligibilityRefusedCounter = lazyCounter(
   "publisher_materialization_eligibility_refused_total",
   "storage= materialization-eligibility refusals. Label: reason " +
      "('free_parameter'|'given'|'authorize'|'not_duckdb_portable'|" +
      "'public_surface_unknown').",
);
const serveShapeTierDropCounter = lazyCounter(
   "publisher_storage_serve_shape_tier_drop_total",
   "storage serve-shape compile escalations: a refinement tier failed to " +
      "compile and the riskiest category was dropped. Label: tier (the failed " +
      "tier index, 0=full).",
);
const serveShapeTypeFallbackCounter = lazyCounter(
   "publisher_storage_serve_shape_type_fallback_total",
   "Captured DuckDB column types mapped to json in the serve shape (type " +
      "fidelity loss). Label: kind ('array'|'unrecognized').",
);
const chainedStorageBuildCounter = lazyCounter(
   "publisher_storage_chained_build_total",
   "Chained storage= source builds (a source reading a storage-materialized " +
      "upstream). Label: outcome ('parent_reuse'|'inline_fallback'|" +
      "'strict_refused'). The parent_reuse share is the headline signal for how " +
      "far the stack-on-the-parent path gets us vs recompute-from-raw.",
);

/**
 * Record a standalone-scheduler fire attempt. `fired` = a SCHEDULER run started;
 * `conflict` = a materialization was already active (this occurrence coalesces
 * into it); `error` = the fire failed unexpectedly.
 */
export function recordScheduledFire(
   outcome: "fired" | "conflict" | "error",
): void {
   scheduledFireCounter().add(1, { outcome });
}

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
 * compilation because the connection failed to resolve. The resulting sourceEntityIds
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

/**
 * A single manifest entry bound with an unquoted table path because its
 * connection could not be resolved for quoting. Per-entry degradation (the rest
 * of the bind succeeds), distinct from the whole-bind {@link recordManifestBind}
 * outcome — hence its own counter rather than a label on that one.
 */
export function recordManifestBindDegraded(): void {
   manifestBindDegradedCounter().add(1);
}

/**
 * Record the wall-clock duration of building one persist source's table,
 * labeled by engine so the DuckDB passthrough path and the in-warehouse CTAS
 * path (very different latency profiles) don't pool into one series.
 */
export function recordSourceBuildDuration(
   durationMs: number,
   engine: StorageBuildEngine,
): void {
   sourceBuildDuration().record(durationMs, { engine });
}

/** Record a best-effort physical-table drop on materialization delete. */
export function recordDropTables(
   outcome: "success" | "failure",
   engine: StorageBuildEngine,
): void {
   dropTablesCounter().add(1, { outcome, engine });
}

/**
 * Record a `storage=` build failure (federation / passthrough / attach / CTAS),
 * counted separately from in-warehouse build failures because the storage path
 * has its own failure modes and destination axis.
 */
export function recordStorageBuildFailure(destination: string): void {
   storageBuildFailureCounter().add(1, { destination });
}

/**
 * Record a materialization-eligibility refusal (a source that can't be safely
 * materialized into a storage destination). The `given` reason is a security
 * refusal (a frozen given-filtered table would leak rows across tenants); worth
 * tracking how often authors attempt it.
 */
export function recordEligibilityRefused(
   reason: EligibilityRefusalReason,
): void {
   eligibilityRefusedCounter().add(1, { reason });
}

/**
 * Record a serve-shape compile escalation: the refinement tier at `failedTier`
 * did not compile, so the riskiest category was dropped and the shape retried.
 * A systematically-dropping source tells authors which refinements aren't
 * servable from storage.
 */
export function recordServeShapeTierDrop(failedTier: number): void {
   serveShapeTierDropCounter().add(1, { tier: String(failedTier) });
}

/**
 * Record a captured DuckDB column type that could not map onto a Malloy basic
 * type and was carried as json in the serve shape — a type-fidelity loss
 * specific to the storage tier. `array` = a collection/array type; `unrecognized`
 * = an unknown/nested/struct/enum/blob type.
 */
export function recordServeShapeTypeFallback(
   kind: "array" | "unrecognized",
): void {
   serveShapeTypeFallbackCounter().add(1, { kind });
}

/**
 * Record a `storage=` serve-routing decision: `storage` = the query was served
 * from the materialized table via the virtual-source transform; `live_fallback`
 * = the transform was ineligible for this query (a refinement it can't
 * reproduce, an unbound source, mode not `on`) so it was served live. This hit
 * rate is the headline KPI of the storage tier — otherwise the fallback side is
 * only a DEBUG log.
 */
export function recordStorageServeRouting(
   outcome: "storage" | "live_fallback",
): void {
   storageServeRoutingCounter().add(1, { outcome });
}

/**
 * Record how a chained `storage=` source was built (see
 * {@link ChainedStorageBuildOutcome}). Only fired for a source that actually
 * reads a storage-materialized upstream — a single-source build emits nothing
 * here. The parent_reuse : inline_fallback ratio is the spike's key learning.
 */
export function recordChainedStorageBuild(
   outcome: ChainedStorageBuildOutcome,
): void {
   chainedStorageBuildCounter().add(1, { outcome });
}

/** Visible for tests. Drops cached instruments so a fresh MeterProvider can capture emissions. */
export function resetMaterializationTelemetryForTesting(): void {
   for (const reset of resetHooks) reset();
}
