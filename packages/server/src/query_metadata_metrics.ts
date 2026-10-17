// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Telemetry for per-query metadata (see `service/query_metadata.ts`).
 *
 * Resolution never throws; it clamps what would violate Malloy's contract, so
 * a dropped property is invisible without these counters.
 *
 * Lazy: an instrument created before `setGlobalMeterProvider` binds to a NoOp
 * meter (https://github.com/open-telemetry/opentelemetry-js/issues/3505).
 */

import { lazyCounter, resetInstrumentsForTesting } from "./telemetry";

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
   resetInstrumentsForTesting();
}
