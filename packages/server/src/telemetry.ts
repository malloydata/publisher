// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   metrics,
   SpanStatusCode,
   trace,
   type Attributes,
   type Counter,
   type Span,
   type Histogram,
   type Meter,
   type UpDownCounter,
} from "@opentelemetry/api";

/**
 * Single source of truth for the OpenTelemetry meter name. Every
 * publisher-emitted metric registers under this name so a typo can't
 * silently split a metric onto a second meter and drop it from the
 * Prometheus scrape.
 */
export const METER_NAME = "publisher";

/**
 * The shared publisher meter. Resolves lazily through the OTel global
 * provider (a `ProxyMeter`), so modules may call this at import time;
 * the real provider can be installed afterward (the production SDK in
 * `instrumentation.ts`, or the in-memory test harness) and instruments
 * still route to it.
 */
export function publisherMeter(): Meter {
   return metrics.getMeter(METER_NAME);
}

/** Histogram buckets for a request duration; the OTel default stops at 10s. */
export const REQUEST_DURATION_BUCKETS_MS = [
   5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 60000,
];

const instrumentResets: (() => void)[] = [];

/**
 * An instrument created from the publisher meter on first use and then
 * memoized. Lazy because one created before the SDK installs its
 * MeterProvider binds to a no-op meter
 * (https://github.com/open-telemetry/opentelemetry-js/issues/3505); tests
 * swap providers and call {@link resetInstrumentsForTesting}.
 */
function lazyInstrument<T>(create: () => T): () => T {
   let instrument: T | null = null;
   instrumentResets.push(() => (instrument = null));
   return () => (instrument ??= create());
}

export function lazyCounter(
   name: string,
   description: string,
   unit?: string,
): () => Counter {
   return lazyInstrument(() =>
      publisherMeter().createCounter(name, {
         description,
         ...(unit ? { unit } : {}),
      }),
   );
}

export function lazyUpDownCounter(
   name: string,
   description: string,
): () => UpDownCounter {
   return lazyInstrument(() =>
      publisherMeter().createUpDownCounter(name, { description }),
   );
}

export function lazyHistogram(
   name: string,
   description: string,
   { unit = "ms", buckets }: { unit?: string; buckets?: number[] } = {},
): () => Histogram {
   return lazyInstrument(() =>
      publisherMeter().createHistogram(name, {
         description,
         // "" means a dimensionless histogram (a count), created with no unit.
         ...(unit ? { unit } : {}),
         ...(buckets ? { advice: { explicitBucketBoundaries: buckets } } : {}),
      }),
   );
}

/** Drop every lazily created instrument so the next use binds to the current provider. */
export function resetInstrumentsForTesting(): void {
   for (const reset of instrumentResets) reset();
}

const tracer = () => trace.getTracer(METER_NAME);

/**
 * Run `fn` inside a span that is active for everything it awaits, so spans the
 * SDK's auto-instrumentation makes (HTTP, database drivers) nest under it. A
 * throw marks the span as an error and is rethrown. Without an exporter
 * configured the tracer is a no-op.
 */
export async function withSpan<T>(
   name: string,
   attributes: Attributes,
   fn: (span: Span) => Promise<T>,
): Promise<T> {
   return tracer().startActiveSpan(name, { attributes }, async (span) => {
      try {
         return await fn(span);
      } catch (error) {
         span.recordException(error as Error);
         span.setStatus({
            code: SpanStatusCode.ERROR,
            message: error instanceof Error ? error.message : String(error),
         });
         throw error;
      } finally {
         span.end();
      }
   });
}
