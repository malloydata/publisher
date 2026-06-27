import { metrics, type Meter } from "@opentelemetry/api";

/**
 * Single source of truth for the OpenTelemetry meter name. Every
 * publisher-emitted metric registers under this name so a typo can't
 * silently split a metric onto a second meter and drop it from the
 * Prometheus scrape.
 */
const METER_NAME = "publisher";

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
