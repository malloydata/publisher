// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   createNoopMeter,
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
import { isHypothetical } from "./hypothetical_scope";

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

/** Instruments that record nothing, handed out inside a hypothetical scope. */
const noopMeter = createNoopMeter();
const noopCounter = noopMeter.createCounter("hypothetical");
const noopHistogram = noopMeter.createHistogram("hypothetical");

/**
 * A counter that records nothing for hypothetical work (a what-if), so a
 * proposal that serves no one never moves a production metric.
 */
export function lazyCounter(
   name: string,
   description: string,
   unit?: string,
): () => Counter {
   const instrument = lazyInstrument(() =>
      publisherMeter().createCounter(name, {
         description,
         ...(unit ? { unit } : {}),
      }),
   );
   return () => (isHypothetical() ? noopCounter : instrument());
}

export function lazyHistogram(
   name: string,
   description: string,
   { unit = "ms", buckets }: { unit?: string; buckets?: number[] } = {},
): () => Histogram {
   const instrument = lazyInstrument(() =>
      publisherMeter().createHistogram(name, {
         description,
         // "" means a dimensionless histogram (a count), created with no unit.
         ...(unit ? { unit } : {}),
         ...(buckets ? { advice: { explicitBucketBoundaries: buckets } } : {}),
      }),
   );
   return () => (isHypothetical() ? noopHistogram : instrument());
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
