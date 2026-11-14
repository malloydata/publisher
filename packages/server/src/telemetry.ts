// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { AsyncLocalStorage } from "async_hooks";
import {
   metrics,
   SpanStatusCode,
   trace,
   type Attributes,
   type Counter,
   type Span,
   type Tracer,
   type Histogram,
   type Meter,
} from "@opentelemetry/api";

/**
 * Single source of truth for the OpenTelemetry meter name. Every
 * publisher-emitted metric registers under this name so a typo can't
 * silently split a metric onto a second meter and drop it from the
 * Prometheus scrape.
 */
export const METER_NAME = "publisher";

/**
 * The publisher meter from the global MeterProvider registered right now.
 * `@opentelemetry/api` has no proxy for metrics: called before a provider is
 * registered, this returns a no-op meter, and every instrument created from it
 * stays a no-op for good. Create instruments through {@link lazyCounter},
 * {@link lazyHistogram} or {@link lazyInstrument} rather than at import time.
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
 * `create` run on first use and memoized, so an instrument declared at import
 * time binds to whichever provider is registered when it is first recorded to,
 * not to the no-op meter that preceded the SDK. Tests swap providers and call
 * {@link resetInstrumentsForTesting}.
 */
export function lazyInstrument<T>(create: () => T): () => T {
   let instrument: T | null = null;
   instrumentResets.push(() => (instrument = null));
   return () => (instrument ??= create());
}

/**
 * Set inside work no caller asked for whose metrics would count a caller's
 * request twice: the v1 cutover's shadow comparison, which runs a query a
 * caller already ran ({@link withoutMetrics}).
 */
const metricsSuppressed = new AsyncLocalStorage<true>();

/**
 * Run `fn` with every counter and histogram from this module recording
 * nothing, through everything it awaits. For a second run of a request a
 * caller already made (the shadow comparison): its security, governance,
 * routing and query metrics were counted by the run that answered.
 */
export function withoutMetrics<T>(fn: () => Promise<T>): Promise<T> {
   return metricsSuppressed.run(true, fn);
}

/** Whether the current async context records no metrics. */
export function metricsAreSuppressed(): boolean {
   return metricsSuppressed.getStore() === true;
}

/** A counter that records nothing under {@link withoutMetrics}. */
export function suppressibleCounter(inner: Counter): Counter {
   return {
      add: (value, attributes, context) => {
         if (!metricsAreSuppressed()) inner.add(value, attributes, context);
      },
   };
}

function suppressibleHistogram(inner: Histogram): Histogram {
   return {
      record: (value, attributes, context) => {
         if (!metricsAreSuppressed()) inner.record(value, attributes, context);
      },
   };
}

export function lazyCounter(
   name: string,
   description: string,
   unit?: string,
): () => Counter {
   return lazyInstrument(() =>
      suppressibleCounter(
         publisherMeter().createCounter(name, {
            description,
            ...(unit ? { unit } : {}),
         }),
      ),
   );
}

export function lazyHistogram(
   name: string,
   description: string,
   { unit = "ms", buckets }: { unit?: string; buckets?: number[] } = {},
): () => Histogram {
   return lazyInstrument(() =>
      suppressibleHistogram(
         publisherMeter().createHistogram(name, {
            description,
            // "" means a dimensionless histogram (a count), created with no unit.
            ...(unit ? { unit } : {}),
            ...(buckets
               ? { advice: { explicitBucketBoundaries: buckets } }
               : {}),
         }),
      ),
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
 * throw marks the span as an error and is rethrown. Before a tracer provider is
 * registered the tracer is a no-op.
 */
export async function withSpan<T>(
   name: string,
   attributes: Attributes,
   fn: (span: Span) => Promise<T>,
   spanTracer: Tracer = tracer(),
): Promise<T> {
   return spanTracer.startActiveSpan(name, { attributes }, async (span) => {
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
