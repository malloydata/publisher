// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Per-load phase timing for package loads.
 *
 * Splits a load into Malloy compile vs. connection schema fetches, which are
 * tuned very differently (worker sizing vs. warehouse round-trips). The worker
 * measures them (see `package_load/rpc_wait_accountant.ts`).
 *
 * Labels: `status` only, never the package name, to bound cardinality;
 * per-package latency belongs to traces.
 *
 * Lazy: instruments created before `setGlobalMeterProvider` bind to a NoOp
 * meter (https://github.com/open-telemetry/opentelemetry-js/issues/3505).
 */

import { lazyHistogram, resetInstrumentsForTesting } from "./telemetry";

/**
 * Load-duration buckets (ms), shared by every load-timing histogram. OTel's
 * defaults stop at 10 s, leaving the slow tail worth seeing in `+Inf`; these
 * reach 5 minutes.
 */
export const LOAD_DURATION_BUCKETS_MS = [
   1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10_000, 30_000, 60_000,
   120_000, 300_000,
];

/** Bucket boundaries for the per-load schema-fetch count. */
const SCHEMA_FETCH_COUNT_BUCKETS = [0, 1, 2, 5, 10, 25, 50, 100, 250, 500];

/**
 * Terminal status of a package load, matching `malloy_package_load_duration`'s
 * `status` label. The phase histograms never see `pool_unavailable`, which
 * fails before the worker returns timings. `policy_rejected` is split from
 * `error` because it is the author's problem (a gate refused what compiled
 * models declared, or publisher.json is unusable), not the server's.
 */
export type PackageLoadStatus =
   | "success"
   | "compilation_error"
   | "policy_rejected"
   | "pool_unavailable"
   | "error";

/** Phase timings a single package load reports (subset of the total). */
export interface PackageLoadPhaseTimings {
   /**
    * Ceiling on Malloy compile CPU (compile region minus schema-fetch wait),
    * including the work the worker does for the main thread.
    */
   compileDurationMs: number;
   /** Wall-clock awaiting proxied connection schema fetches. */
   schemaFetchDurationMs: number;
   /** Number of proxied connection schema fetches the load drove. */
   schemaFetchCount: number;
}

const compileDuration = lazyHistogram(
   "malloy_package_load_compile_duration",
   "Per-load Malloy compile time (compile region minus proxied schema-fetch wait; a ceiling on compile CPU), including the build plan, render-tag results and pre-aggregation companions the worker prepares for the main thread. Label: status.",
   { buckets: LOAD_DURATION_BUCKETS_MS },
);
const schemaFetchDuration = lazyHistogram(
   "malloy_package_load_schema_fetch_duration",
   "Per-load wall-clock awaiting proxied connection schema fetches. Label: status.",
   { buckets: LOAD_DURATION_BUCKETS_MS },
);
const schemaFetches = lazyHistogram(
   "malloy_package_load_schema_fetches",
   "Per-load count of proxied connection schema fetches. Label: status.",
   { unit: "", buckets: SCHEMA_FETCH_COUNT_BUCKETS },
);

/** Record one load's phase breakdown; only for a load the worker returned timings for. */
export function recordPackageLoadPhases(
   timings: PackageLoadPhaseTimings,
   status: PackageLoadStatus,
): void {
   const attrs = { status };
   compileDuration().record(timings.compileDurationMs, attrs);
   schemaFetchDuration().record(timings.schemaFetchDurationMs, attrs);
   schemaFetches().record(timings.schemaFetchCount, attrs);
}

/**
 * The stages of a package load. `malloy_package_load_duration` stops before
 * report discovery and the build plan, so it reads under the time a caller
 * waits; these cover the whole load, main-thread tail included.
 *
 * - `worker`: the package-load worker's job (manifest, compile, extraction) as
 *   the main thread waits for it, with the database scan it overlaps.
 * - `hydrate`: building the main thread's Models from the worker's output,
 *   and checking their persist names.
 * - `render_validation`: render-tag validation of every model.
 * - `discovery`: reading the package's reports.
 * - `lint`: linting them.
 * - `build_plan`: applying the persist build plan the worker derived, or
 *   deriving it here when the worker left it at its soft deadline.
 * - `total`: the load end to end, as `Package.create` runs it.
 *
 * Each is wall-clock time. The main-thread stages yield to the event loop as
 * they go (see `event_loop_yield.ts`), so they include the requests served
 * meanwhile. `Package.reloadAllModels` records none of them.
 */
export type PackageLoadPhase =
   | "worker"
   | "hydrate"
   | "render_validation"
   | "discovery"
   | "lint"
   | "build_plan"
   | "total";

const phaseDuration = lazyHistogram(
   "publisher_package_load_phase_duration_ms",
   "Per-load wall-clock time of each stage of a successful package load, main-thread stages included. Label: phase (worker, hydrate, render_validation, discovery, lint, build_plan, total).",
   { buckets: LOAD_DURATION_BUCKETS_MS },
);

/**
 * Record the stage durations of one successful load. A stage that did not run
 * is left out rather than recorded as zero.
 */
export function recordPackageLoadPhaseDurations(
   durations: Partial<Record<PackageLoadPhase, number>>,
): void {
   for (const [phase, ms] of Object.entries(durations)) {
      if (ms === undefined) continue;
      phaseDuration().record(ms, { phase });
   }
}

/** Test-only: drop cached instruments so a fresh `MeterProvider` captures emissions. */
export function resetPackageLoadMetricsForTesting(): void {
   resetInstrumentsForTesting();
}
