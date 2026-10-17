// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Telemetry for row-cap / byte-cap rejections (HTTP 413). The counter's
 * `cap_type` and `source` labels tell an operator which cap fired on which
 * query surface, which a bare 413 status count cannot. Gauges expose the
 * effective caps for utilization panels.
 *
 * Lazily created: instruments made before `setGlobalMeterProvider` bind to
 * NoOp (https://github.com/open-telemetry/opentelemetry-js/issues/3505).
 */

import { type Counter } from "@opentelemetry/api";
import { publisherMeter } from "./telemetry";

import { getMaxQueryRows, getMaxResponseBytes } from "./config";

/**
 * `unserializable` is distinct from `bytes`: the response could not become
 * JSON at all, which may happen under a raised cap (the engine's own string
 * limit can be lower), so alerts must not conflate the two.
 */
export type QueryCapType = "rows" | "bytes" | "unserializable";
export type QueryCapSource = "connection_sql" | "model_query" | "notebook_cell";

let capExceededCounter: Counter | null = null;
let configGaugesInstalled = false;

function ensureCapTelemetry(): Counter {
   if (capExceededCounter && configGaugesInstalled) {
      return capExceededCounter;
   }
   const meter = publisherMeter();
   if (!capExceededCounter) {
      capExceededCounter = meter.createCounter(
         "publisher_query_cap_exceeded_total",
         {
            description:
               "413s for an oversized response. cap_type: rows|bytes exceeded that cap; unserializable could not be turned into JSON at all, so no cap need have been exceeded. source: connection_sql|model_query|notebook_cell.",
         },
      );
   }
   if (!configGaugesInstalled) {
      // Read per scrape so an env-var change shows immediately; a parse
      // failure reports -1 so misconfig is visible.
      meter
         .createObservableGauge("publisher_max_query_rows", {
            description:
               "Current effective PUBLISHER_MAX_QUERY_ROWS cap (0 = disabled, -1 = misconfigured)",
         })
         .addCallback((observation) => {
            try {
               observation.observe(getMaxQueryRows());
            } catch {
               observation.observe(-1);
            }
         });
      meter
         .createObservableGauge("publisher_max_response_bytes", {
            description:
               "Current effective PUBLISHER_MAX_RESPONSE_BYTES cap (0 = disabled, -1 = misconfigured)",
            unit: "By",
         })
         .addCallback((observation) => {
            try {
               observation.observe(getMaxResponseBytes());
            } catch {
               observation.observe(-1);
            }
         });
      configGaugesInstalled = true;
   }
   return capExceededCounter;
}

/**
 * Record one cap-exceeded event. Call BEFORE throwing, so the metric ticks
 * even if a downstream `catch` swallows the error (MCP tools do).
 */
export function recordQueryCapExceeded(
   capType: QueryCapType,
   source: QueryCapSource,
): void {
   ensureCapTelemetry().add(1, { cap_type: capType, source });
}

/** Test-only: drop cached instruments so a fresh MeterProvider captures them. */
export function resetQueryCapTelemetryForTesting(): void {
   capExceededCounter = null;
   configGaugesInstalled = false;
}
