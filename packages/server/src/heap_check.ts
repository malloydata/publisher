// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { publisherMeter } from "./telemetry";
import * as v8 from "v8";

import { logger } from "./logger";

/** Structural, so tests can pass a plain stub. */
interface HeapCheckLogger {
   warn: (...args: unknown[]) => unknown;
   info: (...args: unknown[]) => unknown;
}

/**
 * Heap gauges, read live on every scrape, so dashboards can show the
 * configured ceiling beside `PackageMemoryGovernor`'s RSS series. Installed
 * from `checkHeapConfiguration`, after the OTel SDK is up: instruments
 * created earlier bind to a NoOp meter.
 */
let heapGaugesInstalled = false;
function installHeapGauges(): void {
   if (heapGaugesInstalled) return;
   heapGaugesInstalled = true;
   const meter = publisherMeter();
   meter
      .createObservableGauge("publisher_heap_size_limit_bytes", {
         description:
            "V8 heap_size_limit (--max-old-space-size). Compare with PUBLISHER_MAX_MEMORY_BYTES.",
         unit: "By",
      })
      .addCallback((observation) => {
         observation.observe(v8.getHeapStatistics().heap_size_limit);
      });
   meter
      .createObservableGauge("publisher_heap_used_bytes", {
         description:
            "Current V8 used_heap_size in bytes. Watch this alongside publisher_process_rss_bytes; the two diverge under native-allocator pressure (DuckDB, etc.).",
         unit: "By",
      })
      .addCallback((observation) => {
         observation.observe(v8.getHeapStatistics().used_heap_size);
      });
}

/** Test-only: lets a re-installation capture into a fresh MeterProvider. */
export function resetHeapTelemetryForTesting(): void {
   heapGaugesInstalled = false;
}

/**
 * Smallest V8 heap ceiling at which the default row/byte caps are
 * comfortably survivable: below it, one buffered query at the 50 MB byte cap
 * can OOM the process before back-pressure trips.
 */
const MIN_RECOMMENDED_HEAP_BYTES = 2 * 1024 * 1024 * 1024;

/** Test seam: ES modules cannot be stubbed, so the dependencies are injectable. */
export interface CheckHeapOptions {
   getHeapStatistics?: () => Pick<v8.HeapInfo, "heap_size_limit">;
   log?: HeapCheckLogger;
}

/**
 * Warn once at startup when the heap ceiling is below
 * {@link MIN_RECOMMENDED_HEAP_BYTES}. A warning, not an exit: small pods (CI,
 * local dev) are legitimate, and per-request caps already bound memory.
 */
export function checkHeapConfiguration(options: CheckHeapOptions = {}): {
   heapSizeLimitBytes: number;
   warned: boolean;
} {
   installHeapGauges();
   const getStats = options.getHeapStatistics ?? v8.getHeapStatistics;
   const log = options.log ?? logger;
   const stats = getStats();
   const heapSizeLimitBytes = stats.heap_size_limit;
   const limitMiB = Math.round(heapSizeLimitBytes / (1024 * 1024));
   const recommendedMiB = Math.round(
      MIN_RECOMMENDED_HEAP_BYTES / (1024 * 1024),
   );

   if (heapSizeLimitBytes < MIN_RECOMMENDED_HEAP_BYTES) {
      log.warn(
         `V8 heap_size_limit is ${limitMiB} MiB, below the recommended ${recommendedMiB} MiB. ` +
            `Pass --max-old-space-size=${recommendedMiB} (or higher) on the node process to keep ` +
            `the row/byte caps (PUBLISHER_MAX_QUERY_ROWS / PUBLISHER_MAX_RESPONSE_BYTES) within ` +
            `safe margin. With a smaller heap, a single large query can OOM the pod before the ` +
            `memory governor's back-pressure has a chance to trip.`,
         {
            heapSizeLimitBytes,
            recommendedHeapSizeBytes: MIN_RECOMMENDED_HEAP_BYTES,
         },
      );
      return { heapSizeLimitBytes, warned: true };
   }

   log.info(
      `V8 heap_size_limit is ${limitMiB} MiB (>= recommended ${recommendedMiB} MiB).`,
      { heapSizeLimitBytes },
   );
   return { heapSizeLimitBytes, warned: false };
}
