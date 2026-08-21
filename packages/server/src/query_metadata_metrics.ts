// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Telemetry for per-query metadata (see `service/query_metadata.ts`).
 *
 * The resolve path never throws — it clamps a bag that would violate Malloy's
 * contract — so a drop is invisible without a metric: an operator would see
 * correct-looking queries whose warehouse-side attribution is quietly missing a
 * property. The counters make both halves observable: what was attached, and
 * what did not fit.
 *
 * Instruments are created lazily for the same reason as {@link ./query_cap_metrics}:
 * one created before `setGlobalMeterProvider` binds to a NoOp meter
 * (https://github.com/open-telemetry/opentelemetry-js/issues/3505).
 */

import { type Counter } from "@opentelemetry/api";
import { publisherMeter } from "./telemetry";

const resetHooks: (() => void)[] = [];

function lazyCounter(name: string, description: string): () => Counter {
   let instrument: Counter | null = null;
   resetHooks.push(() => (instrument = null));
   return () =>
      (instrument ??= publisherMeter().createCounter(name, { description }));
}

const appliedCounter = lazyCounter(
   "publisher_query_metadata_applied_total",
   "Queries issued with per-query metadata attached. Label: class ('interactive'|'materialize'|'index'|'ops'|'unknown').",
);
const droppedCounter = lazyCounter(
   "publisher_query_metadata_properties_dropped_total",
   "Metadata properties dropped while resolving a bag. Label: reason ('invalid_name'|'invalid_value'|'reserved_name'|'property_cap'|'serialized_cap').",
);

/** One query's metadata reached the connector. */
export function recordQueryMetadataApplied(queryClass: string): void {
   appliedCounter().add(1, { class: queryClass });
}

/** One property did not survive resolution. */
export function recordQueryMetadataDropped(reason: string): void {
   droppedCounter().add(1, { reason });
}

/** Drop memoized instruments so a test can install a fresh MeterProvider. */
export function resetQueryMetadataMetricsForTest(): void {
   for (const reset of resetHooks) reset();
}
