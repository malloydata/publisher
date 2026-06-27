/**
 * Centralized telemetry for row-cap / byte-cap rejections (HTTP 413).
 *
 * Without this, operators tuning {@link PUBLISHER_MAX_QUERY_ROWS} /
 * {@link PUBLISHER_MAX_RESPONSE_BYTES} can only see undifferentiated
 * `http_server_requests_total{status_code="413"}` — they can't tell
 * which cap is firing or which query surface is hottest. The counter
 * here carries `cap_type` (`rows` / `bytes`) and `source`
 * (`connection_sql` / `model_query` / `notebook_cell`) so a single
 * dashboard panel can answer "what should I tune and on which
 * endpoint?".
 *
 * Observable gauges expose the current effective caps so dashboards
 * can render `actual_rows_returned / max_rows` utilization without
 * a separate config feed — same pattern the memory governor uses
 * for high/low water bytes and the concurrency middleware uses for
 * its slot cap.
 *
 * Lazy init for the same reason as `query_timeout.ts` /
 * `query_concurrency.ts`: instruments created before
 * `setGlobalMeterProvider` bind to a NoOp meter
 * (https://github.com/open-telemetry/opentelemetry-js/issues/3505).
 * The first throw site initializes the counter; the gauges are
 * installed alongside on the same call so the production hot path
 * is one boolean check after the first 413.
 */

import { type Counter } from "@opentelemetry/api";
import { publisherMeter } from "./telemetry";

import { getMaxQueryRows, getMaxResponseBytes } from "./config";

export type QueryCapType = "rows" | "bytes";
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
               "Queries rejected with 413 because the row or byte cap was exceeded. Labels: cap_type ('rows'|'bytes'), source ('connection_sql'|'model_query'|'notebook_cell').",
         },
      );
   }
   if (!configGaugesInstalled) {
      // Live config readouts so dashboards can render
      // "actual / max" utilization for the row and byte caps the
      // same way `publisher_memory_*_bytes` does for the governor.
      // Read on every scrape so a runtime env-var change is
      // visible without a restart; an env-var parse failure
      // reports -1 so misconfig is visible rather than silently
      // dropped (mirrors `publisher_query_timeout_ms`).
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
 * Record a single 413 cap-exceeded event. Call BEFORE throwing
 * `PayloadTooLargeError` so the metric ticks even if a downstream
 * `catch` swallows the error (MCP tools surface failures as content
 * payloads rather than letting them bubble to the HTTP error
 * mapper).
 *
 * `cap_type` must be one of `rows` / `bytes`; `source` identifies
 * the query surface that detected the overflow.
 */
export function recordQueryCapExceeded(
   capType: QueryCapType,
   source: QueryCapSource,
): void {
   ensureCapTelemetry().add(1, { cap_type: capType, source });
}

/**
 * Visible for tests. Drops the cached instruments so a fresh
 * `MeterProvider` (installed via `startMetricsHarness`) can capture
 * future emissions. Do NOT call from production code.
 */
export function resetQueryCapTelemetryForTesting(): void {
   capExceededCounter = null;
   configGaugesInstalled = false;
}
