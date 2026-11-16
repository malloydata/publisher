// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Telemetry for row-cap / byte-cap rejections (HTTP 413). The counter's
 * `cap_type` and `source` labels tell an operator which cap fired on which
 * query surface, which a bare 413 status count cannot. Gauges expose the
 * effective caps for utilization panels.
 */

import {
   lazyInstrument,
   publisherMeter,
   resetInstrumentsForTesting,
   suppressibleCounter,
} from "./telemetry";

import { getMaxQueryRows, getMaxResponseBytes } from "./config";

/**
 * `unserializable` is distinct from `bytes`: the response could not become
 * JSON at all, which may happen under a raised cap (the engine's own string
 * limit can be lower), so alerts must not conflate the two.
 */
export type QueryCapType = "rows" | "bytes" | "unserializable";
export type QueryCapSource = "connection_sql" | "model_query" | "notebook_cell";

/** A cap gauge read per scrape, so an env-var change shows immediately; a parse failure reports -1. */
function observeCap(
   name: string,
   description: string,
   read: () => number,
   unit?: string,
): void {
   publisherMeter()
      .createObservableGauge(name, { description, ...(unit ? { unit } : {}) })
      .addCallback((observation) => {
         try {
            observation.observe(read());
         } catch {
            observation.observe(-1);
         }
      });
}

/** The counter, with the cap gauges installed beside it on first use. */
const capExceededCounter = lazyInstrument(() => {
   observeCap(
      "publisher_max_query_rows",
      "Current effective PUBLISHER_MAX_QUERY_ROWS cap (0 = disabled, -1 = misconfigured)",
      getMaxQueryRows,
   );
   observeCap(
      "publisher_max_response_bytes",
      "Current effective PUBLISHER_MAX_RESPONSE_BYTES cap (0 = disabled, -1 = misconfigured)",
      getMaxResponseBytes,
      "By",
   );
   return suppressibleCounter(
      publisherMeter().createCounter("publisher_query_cap_exceeded_total", {
         description:
            "413s for an oversized response. cap_type: rows|bytes exceeded that cap; unserializable could not be turned into JSON at all, so no cap need have been exceeded. source: connection_sql|model_query|notebook_cell.",
      }),
   );
});

/**
 * Record one cap-exceeded event. Call BEFORE throwing, so the metric ticks
 * even if a downstream `catch` swallows the error (MCP tools do).
 */
export function recordQueryCapExceeded(
   capType: QueryCapType,
   source: QueryCapSource,
): void {
   capExceededCounter().add(1, { cap_type: capType, source });
}

/** Test-only: drop cached instruments so a fresh MeterProvider captures them. */
export function resetQueryCapTelemetryForTesting(): void {
   resetInstrumentsForTesting();
}
