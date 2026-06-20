/**
 * Centralized telemetry for the two-round materialization protocol.
 *
 * Operators need to answer "are builds completing, how long do the two
 * rounds take, and where are they failing?" without grepping logs. The
 * counter carries `round` (`round1`|`round2`) and `outcome`
 * (`success`|`failed`|`cancelled`); the histogram carries `round` so a
 * dashboard can render Round 1 (compile+plan) vs Round 2 (build) latency
 * side by side.
 *
 * Lazy init for the same reason as {@link ./query_cap_metrics}: instruments
 * created before `setGlobalMeterProvider` bind to a NoOp meter
 * (https://github.com/open-telemetry/opentelemetry-js/issues/3505).
 */

import { metrics, type Counter, type Histogram } from "@opentelemetry/api";

export type MaterializationRound = "round1" | "round2" | "auto";
export type MaterializationOutcome = "success" | "failed" | "cancelled";
/** Manifest bind outcome: timeout is split out from generic failure on purpose. */
export type ManifestBindOutcome = "success" | "failure" | "timeout";

let roundCounter: Counter | null = null;
let roundDuration: Histogram | null = null;
let manifestBindCounter: Counter | null = null;
let sourceBuildDuration: Histogram | null = null;
let dropTablesCounter: Counter | null = null;

function ensureTelemetry(): { counter: Counter; duration: Histogram } {
   if (roundCounter && roundDuration) {
      return { counter: roundCounter, duration: roundDuration };
   }
   const meter = metrics.getMeter("publisher");
   if (!roundCounter) {
      roundCounter = meter.createCounter(
         "publisher_materialization_rounds_total",
         {
            description:
               "Materialization rounds completed. Labels: round ('round1'|'round2'|'auto'), outcome ('success'|'failed'|'cancelled').",
         },
      );
   }
   if (!roundDuration) {
      roundDuration = meter.createHistogram(
         "publisher_materialization_round_duration_ms",
         {
            description:
               "Wall-clock duration of a materialization round. Label: round ('round1'|'round2'|'auto').",
            unit: "ms",
         },
      );
   }
   return { counter: roundCounter, duration: roundDuration };
}

/** Record the outcome and duration of a materialization round. */
export function recordMaterializationRound(
   round: MaterializationRound,
   outcome: MaterializationOutcome,
   durationMs: number,
): void {
   const { counter, duration } = ensureTelemetry();
   counter.add(1, { round, outcome });
   duration.record(durationMs, { round });
}

/**
 * Record a manifest-bind attempt (publisher binding a control-plane manifest to
 * a package at load). `timeout` is distinguished from `failure` so operators can
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
         .createHistogram("publisher_materialization_source_build_duration_ms", {
            description: "Wall-clock duration of building a single persist source.",
            unit: "ms",
         });
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
   roundCounter = null;
   roundDuration = null;
   manifestBindCounter = null;
   sourceBuildDuration = null;
   dropTablesCounter = null;
}
