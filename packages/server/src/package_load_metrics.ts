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

import { type Histogram } from "@opentelemetry/api";
import { publisherMeter } from "./telemetry";

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

export type PackageLoadStatus = "success" | "error";

/** Phase timings a single package load reports (subset of the total). */
export interface PackageLoadPhaseTimings {
   /** Ceiling on Malloy compile CPU (compile region minus schema-fetch wait). */
   compileDurationMs: number;
   /** Wall-clock awaiting proxied connection schema fetches. */
   schemaFetchDurationMs: number;
   /** Number of proxied connection schema fetches the load drove. */
   schemaFetchCount: number;
}

let compileDuration: Histogram | null = null;
let schemaFetchDuration: Histogram | null = null;
let schemaFetches: Histogram | null = null;

function ensureInstruments(): void {
   if (compileDuration && schemaFetchDuration && schemaFetches) return;
   const meter = publisherMeter();
   compileDuration ??= meter.createHistogram(
      "malloy_package_load_compile_duration",
      {
         description:
            "Per-load Malloy compile time (compile region minus proxied schema-fetch wait; a ceiling on compile CPU). Label: status.",
         unit: "ms",
         advice: { explicitBucketBoundaries: LOAD_DURATION_BUCKETS_MS },
      },
   );
   schemaFetchDuration ??= meter.createHistogram(
      "malloy_package_load_schema_fetch_duration",
      {
         description:
            "Per-load wall-clock awaiting proxied connection schema fetches. Label: status.",
         unit: "ms",
         advice: { explicitBucketBoundaries: LOAD_DURATION_BUCKETS_MS },
      },
   );
   schemaFetches ??= meter.createHistogram(
      "malloy_package_load_schema_fetches",
      {
         description:
            "Per-load count of proxied connection schema fetches. Label: status.",
         advice: { explicitBucketBoundaries: SCHEMA_FETCH_COUNT_BUCKETS },
      },
   );
}

/**
 * Record one load's phase breakdown. Call for completed loads (`success`);
 * the terms are meaningless for a load that never produced a worker result.
 */
export function recordPackageLoadPhases(
   timings: PackageLoadPhaseTimings,
   status: PackageLoadStatus = "success",
): void {
   ensureInstruments();
   const attrs = { status };
   compileDuration?.record(timings.compileDurationMs, attrs);
   schemaFetchDuration?.record(timings.schemaFetchDurationMs, attrs);
   schemaFetches?.record(timings.schemaFetchCount, attrs);
}

/**
 * Visible for tests. Drops cached instruments so a fresh `MeterProvider`
 * (installed via `startMetricsHarness`) captures future emissions. Do NOT call
 * from production code.
 */
export function resetPackageLoadMetricsForTesting(): void {
   compileDuration = null;
   schemaFetchDuration = null;
   schemaFetches = null;
}
