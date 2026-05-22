import { metrics } from "@opentelemetry/api";
import * as v8 from "v8";

import { logger } from "./logger";

/**
 * Subset of the winston logger surface this module needs. Defined
 * structurally so tests can pass a plain object stub and so we
 * don't take a direct dependency on the concrete winston type.
 */
interface HeapCheckLogger {
   warn: (...args: unknown[]) => unknown;
   info: (...args: unknown[]) => unknown;
}

/**
 * Observable gauges for heap configuration so dashboards can render
 * "configured heap ceiling" alongside the RSS / back-pressure
 * timeseries from `PackageMemoryGovernor`. The values are observed
 * on every scrape rather than cached — `v8.getHeapStatistics()` is
 * cheap (a single VM call) and serving the live value avoids stale
 * reads.
 *
 * Lazy init for the same reason as `query_timeout.ts`: instruments
 * captured before `setGlobalMeterProvider` are bound to NoOp. We
 * call `installHeapGauges` from `checkHeapConfiguration` so it
 * runs once at startup, after the OTel SDK is up.
 *
 * Mirrors the governor's `publisher_*` unlabeled style.
 */
let heapGaugesInstalled = false;
function installHeapGauges(): void {
   if (heapGaugesInstalled) return;
   heapGaugesInstalled = true;
   const meter = metrics.getMeter("publisher");
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

/**
 * Visible for tests; production code never calls this. Resets the
 * lazy guard so a re-installation captures into a fresh
 * MeterProvider.
 */
export function resetHeapTelemetryForTesting(): void {
   heapGaugesInstalled = false;
}

/**
 * Minimum V8 heap ceiling (`--max-old-space-size`) the publisher
 * expects in production. Below this the row/byte caps from earlier
 * steps still apply, but a single buffered model query at the
 * default 50 MB byte cap plus the surrounding `Result` allocation
 * can plausibly chew through the remaining headroom and OOM the
 * process before back-pressure trips.
 *
 * 2 GiB is the smallest value at which the defaults are comfortably
 * survivable. Operators running explicitly tuned-down pods (e.g. a
 * lightweight smoke-test deploy) can ignore the warning.
 */
const MIN_RECOMMENDED_HEAP_BYTES = 2 * 1024 * 1024 * 1024;

/**
 * Probe `v8.getHeapStatistics().heap_size_limit` at startup and
 * emit a single structured warning when the process is configured
 * with a heap ceiling below {@link MIN_RECOMMENDED_HEAP_BYTES}.
 *
 * Why warn (not exit):
 * - Smaller pods are legitimate (CI, local dev, smoke tests).
 *   Hard-failing them would be hostile to those workflows.
 * - The earlier steps already bound memory growth per request; this
 *   is a "you probably want to know" signal, not a safety
 *   interlock.
 * - A warning at boot is grep-able in pod logs / dashboards and
 *   surfaces faster than waiting for the first OOMKill.
 *
 * Returns the observed heap limit so the caller (server.ts startup)
 * can also surface it in startup metrics if desired.
 */
/**
 * Test seam: parameters allow injecting a fake heap-stats getter
 * and logger so the bun/sinon "ES modules cannot be stubbed"
 * restriction doesn't force a module-level mock. Production calls
 * (server.ts startup) use the defaults; tests pass in stubs.
 */
export interface CheckHeapOptions {
   getHeapStatistics?: () => Pick<v8.HeapInfo, "heap_size_limit">;
   log?: HeapCheckLogger;
}

export function checkHeapConfiguration(options: CheckHeapOptions = {}): {
   heapSizeLimitBytes: number;
   warned: boolean;
} {
   // Install heap-related observable gauges on the same startup
   // tick. Idempotent — re-calling in tests / warmup paths is
   // safe and re-uses the same instruments.
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
