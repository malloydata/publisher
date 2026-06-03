/**
 * Test helper: spin up an in-memory OpenTelemetry MeterProvider so
 * unit tests can assert that the new guardrails (admission gate,
 * query timeout, query concurrency, heap check) emit the expected
 * counters and gauges.
 *
 * Usage:
 *
 *   const harness = await startMetricsHarness();
 *   // ... exercise production code ...
 *   const sums = await harness.collectCounter("publisher_query_timeout_total");
 *   expect(sums).toBe(1);
 *   await harness.shutdown();
 *
 * The OTel JS API resolves `metrics.getMeter(name)` lazily through a
 * `ProxyMeter`, so registering the MeterProvider here AFTER the
 * production modules have already cached their meter handles works
 * — subsequent `.add()` / observable callbacks route to this
 * harness's provider.
 *
 * The harness drains all of the provider's metrics via
 * `MetricReader.collect()` and reads the cumulative data points
 * directly, bypassing the exporter pipeline so test code doesn't
 * have to thread async callbacks.
 */

import { metrics } from "@opentelemetry/api";
import { MeterProvider, MetricReader } from "@opentelemetry/sdk-metrics";

/**
 * The simplest possible MetricReader implementation: a no-op that
 * exists only so the `MeterProvider` has a reader to satisfy its
 * collection invariants. Tests call `collect()` directly via the
 * harness; the reader never pushes anywhere.
 */
class CollectingMetricReader extends MetricReader {
   protected override async onForceFlush(): Promise<void> {
      // no-op; tests pull via `collect()` on demand
   }
   protected override async onShutdown(): Promise<void> {
      // no-op
   }
}

export interface MetricsHarness {
   readonly provider: MeterProvider;
   /**
    * Force a collection cycle and return the cumulative sum of all
    * data points for the named counter. Returns 0 when the counter
    * has not been emitted yet.
    *
    * `attributeFilter` lets a test scope the sum to a single label
    * set (e.g. only the `environment: "test-env"` data point).
    */
   collectCounter(
      name: string,
      attributeFilter?: Record<string, string | number | boolean>,
   ): Promise<number>;
   /**
    * Force a collection cycle and return the most-recently observed
    * value of the named gauge, or `undefined` if no callback has
    * fired for this gauge yet.
    */
   collectGauge(name: string): Promise<number | undefined>;
   shutdown(): Promise<void>;
}

export async function startMetricsHarness(): Promise<MetricsHarness> {
   const reader = new CollectingMetricReader();
   const provider = new MeterProvider({ readers: [reader] });
   // OTel JS's `setGlobalMeterProvider` silently refuses to
   // overwrite an existing global (`registerGlobal` returns false).
   // Tests that run back-to-back would otherwise route their
   // metrics into a dead provider from the previous test. Disable
   // first to clear the slot.
   metrics.disable();
   metrics.setGlobalMeterProvider(provider);

   return {
      provider,
      async collectCounter(
         name: string,
         attributeFilter?: Record<string, string | number | boolean>,
      ): Promise<number> {
         const result = await reader.collect();
         let total = 0;
         for (const rm of result.resourceMetrics.scopeMetrics) {
            for (const metric of rm.metrics) {
               if (metric.descriptor.name !== name) continue;
               for (const dp of metric.dataPoints) {
                  if (attributeFilter) {
                     const allMatch = Object.entries(attributeFilter).every(
                        ([k, v]) => dp.attributes?.[k] === v,
                     );
                     if (!allMatch) continue;
                  }
                  total += dp.value as number;
               }
            }
         }
         return total;
      },
      async collectGauge(name: string): Promise<number | undefined> {
         const result = await reader.collect();
         for (const rm of result.resourceMetrics.scopeMetrics) {
            for (const metric of rm.metrics) {
               if (metric.descriptor.name !== name) continue;
               // Observable gauges yield one data point per
               // attribute set per collection. Return the last
               // observed value across all data points so unlabeled
               // gauges (the common case in this code base) just
               // work without attribute plumbing.
               const last = metric.dataPoints[metric.dataPoints.length - 1];
               return last?.value as number | undefined;
            }
         }
         return undefined;
      },
      async shutdown(): Promise<void> {
         await reader.shutdown();
         // Clear the global so the next harness's
         // `setGlobalMeterProvider` is accepted.
         metrics.disable();
      },
   };
}
