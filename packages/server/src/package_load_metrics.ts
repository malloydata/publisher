// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Per-load phase timing for package loads.
 *
 * `malloy_package_load_duration` already reports the *total* time to load a
 * package, but an operator watching a slow load can't tell whether it's the
 * Malloy compile or the connection schema fetches that dominate — the two are
 * tuned very differently (CPU/worker sizing vs. warehouse round-trips). These
 * histograms split the load into those phases so a single dashboard answers
 * "where did the time go?". The worker measures the phases and reports them on
 * its result; see `package_load/package_load_worker.ts` and
 * `package_load/rpc_wait_accountant.ts`.
 *
 * Labels: `status` only. Deliberately NOT labelled by package name — unlike
 * `malloy_package_load_duration`, which carries `malloy_package_name` — to keep
 * cardinality bounded on deployments with many packages. Per-package latency,
 * when needed, is better answered by exemplars/traces than a label explosion.
 *
 * Lazy init for the same reason as `query_cap_metrics.ts` / `query_timeout.ts`:
 * instruments created before `setGlobalMeterProvider` bind to a NoOp meter
 * (https://github.com/open-telemetry/opentelemetry-js/issues/3505).
 */

import { lazyHistogram, resetInstrumentsForTesting } from "./telemetry";

/**
 * Explicit bucket boundaries (ms) for the load-duration histograms. OTel's
 * default boundaries top out at 10 000 ms, so any load slower than 10 s falls
 * in the `+Inf` bucket and `histogram_quantile` can't resolve the tail — which
 * is exactly the tail (large/slow packages) worth seeing. These extend to
 * 5 minutes while keeping sub-10 ms resolution for the fast common case.
 * Shared with the total `malloy_package_load_duration` histogram so all
 * load-timing histograms bucket consistently.
 */
export const LOAD_DURATION_BUCKETS_MS = [
   1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10_000, 30_000, 60_000,
   120_000, 300_000,
];

/** Bucket boundaries for the per-load schema-fetch count. */
const SCHEMA_FETCH_COUNT_BUCKETS = [0, 1, 2, 5, 10, 25, 50, 100, 250, 500];

/**
 * Terminal status of a package load, mirroring the `status` label on the
 * existing `malloy_package_load_duration` histogram so the phase metrics slice
 * the same way. In practice the phase histograms only ever carry `success`,
 * `compilation_error`, `policy_rejected`, or `error` — a `pool_unavailable`
 * failure happens before the worker returns any timings, so there is nothing to
 * record for it.
 *
 * `policy_rejected` is kept out of the generic `error` bucket because it is the
 * author's problem, not the server's: the models compiled, and a gate refused
 * what they declared (an invalid `#@ persist` incremental declaration), or the
 * package's publisher.json cannot be used as written.
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

/**
 * Record one load's phase breakdown. Call once the worker has returned timings,
 * with the load's terminal `status` (so the phases can be sliced by outcome the
 * same way the total duration is). The terms are meaningless for a load that
 * never produced a worker result (e.g. a pool failure), so don't call it there.
 */
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
 * The stages of a package load, as `publisher_package_load_phase_duration_ms`
 * labels them. `malloy_package_load_duration` is recorded once the models are
 * hydrated and validated, before report discovery and the persist build plan
 * run, so it reads well under the time a caller waits; these cover the whole
 * load, main-thread tail included, without changing what that metric means.
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
 * Record the stage durations of one load that succeeded. Labels: `phase` only,
 * a fixed enumeration, so cardinality stays at seven series whatever the
 * number of packages. A stage that did not run is left out rather than
 * recorded as zero.
 */
export function recordPackageLoadPhaseDurations(
   durations: Partial<Record<PackageLoadPhase, number>>,
): void {
   for (const [phase, ms] of Object.entries(durations)) {
      if (ms === undefined) continue;
      phaseDuration().record(ms, { phase });
   }
}

/**
 * Visible for tests. Drops cached instruments so a fresh `MeterProvider`
 * (installed via `startMetricsHarness`) captures future emissions. Do NOT call
 * from production code.
 */
export function resetPackageLoadMetricsForTesting(): void {
   resetInstrumentsForTesting();
}
