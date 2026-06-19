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

export type MaterializationRound = "round1" | "round2";
export type MaterializationOutcome = "success" | "failed" | "cancelled";

let roundCounter: Counter | null = null;
let roundDuration: Histogram | null = null;

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
               "Materialization rounds completed. Labels: round ('round1'|'round2'), outcome ('success'|'failed'|'cancelled').",
         },
      );
   }
   if (!roundDuration) {
      roundDuration = meter.createHistogram(
         "publisher_materialization_round_duration_ms",
         {
            description:
               "Wall-clock duration of a materialization round. Label: round ('round1'|'round2').",
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

/** Visible for tests. Drops cached instruments so a fresh MeterProvider can capture emissions. */
export function resetMaterializationTelemetryForTesting(): void {
   roundCounter = null;
   roundDuration = null;
}
