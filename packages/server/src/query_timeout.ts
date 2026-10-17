// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { type Counter } from "@opentelemetry/api";
import { publisherMeter } from "./telemetry";

import { getQueryTimeoutMs } from "./config";
import { QueryTimeoutError } from "./errors";

/**
 * Lazily created so a MeterProvider installed after import (as tests do)
 * still records: OTel's `ProxyMeter` binds earlier instruments to NoOp
 * (https://github.com/open-telemetry/opentelemetry-js/issues/3505).
 */
let queryTimeoutCounter: Counter | null = null;
let timeoutTelemetryInitialized = false;
/** Idempotent installer for the timeout counter and config gauge. */
function ensureTimeoutTelemetry(): Counter {
   if (queryTimeoutCounter && timeoutTelemetryInitialized) {
      return queryTimeoutCounter;
   }
   const meter = publisherMeter();
   if (!queryTimeoutCounter) {
      queryTimeoutCounter = meter.createCounter(
         "publisher_query_timeout_total",
         {
            description:
               "Queries aborted because PUBLISHER_QUERY_TIMEOUT_MS elapsed before the underlying SDK call completed",
         },
      );
   }
   if (!timeoutTelemetryInitialized) {
      // The configured timeout, for dashboards to plot against actual query
      // durations. Read per scrape so an env-var change shows immediately.
      meter
         .createObservableGauge("publisher_query_timeout_ms", {
            description:
               "Current effective PUBLISHER_QUERY_TIMEOUT_MS (0 = disabled)",
            unit: "ms",
         })
         .addCallback((observation) => {
            try {
               observation.observe(getQueryTimeoutMs());
            } catch {
               // A misconfigured env var fails the next request, not the
               // scrape; -1 makes it visible.
               observation.observe(-1);
            }
         });
      timeoutTelemetryInitialized = true;
   }
   return queryTimeoutCounter;
}

/** Test-only: re-trigger lazy init against a fresh MeterProvider. */
export function resetQueryTimeoutTelemetryForTesting(): void {
   queryTimeoutCounter = null;
   timeoutTelemetryInitialized = false;
}

/**
 * Per-query wall-clock guard. Hands `fn` an {@link AbortSignal} that is
 * aborted with {@link PUBLISHER_QUERY_TIMEOUT_REASON} after `timeoutMs`.
 *
 * Contract:
 * - `fn` MUST forward `signal` into the underlying SDK call so the abort
 *   cancels the work rather than just unblocking the awaiter.
 * - `timeoutMs <= 0` opts out (no timer); the signal still carries a client
 *   abort.
 * - A timeout followed by a rejection from `fn` throws
 *   {@link QueryTimeoutError} (504). If `fn` resolves after the timer fired,
 *   the result is returned: a completed query beats a 504. The counter ticks
 *   only when a 504 is thrown.
 * - Any other error is re-thrown unmodified.
 */
export async function runWithQueryTimeout<T>(
   fn: (signal: AbortSignal) => Promise<T>,
   timeoutMs: number,
   /**
    * Aborted when the HTTP client disconnects. Forwarded to `fn`'s signal so
    * an unread query is cancelled at the driver rather than left running
    * after its concurrency slot is released on `close`.
    */
   clientSignal?: AbortSignal,
): Promise<T> {
   // On every call so the timeout gauge exists before the first 504.
   ensureTimeoutTelemetry();

   const ac = new AbortController();
   const onClientGone = () => ac.abort(clientSignal?.reason);
   if (clientSignal?.aborted) onClientGone();
   else clientSignal?.addEventListener("abort", onClientGone, { once: true });
   const detach = () =>
      clientSignal?.removeEventListener("abort", onClientGone);

   if (timeoutMs <= 0) {
      try {
         return await fn(ac.signal);
      } finally {
         detach();
      }
   }

   const reason = PUBLISHER_QUERY_TIMEOUT_REASON;
   let timedOut = false;
   const timer = setTimeout(() => {
      timedOut = true;
      ac.abort(reason);
   }, timeoutMs);
   // Never hold up graceful shutdown for the timer alone.
   timer.unref?.();

   try {
      return await fn(ac.signal);
   } catch (error) {
      if (timedOut) {
         // Tick before throwing: the MCP tool turns errors into content
         // payloads. The `timeout_ms` label separates "the env var was
         // lowered" from "queries got slower".
         ensureTimeoutTelemetry().add(1, { timeout_ms: timeoutMs });
         throw new QueryTimeoutError(
            `Query exceeded PUBLISHER_QUERY_TIMEOUT_MS (${timeoutMs}ms) and was aborted. Refine the query (add a more selective WHERE, lower LIMIT, or simplify joins) or raise the timeout.`,
         );
      }
      throw error;
   } finally {
      clearTimeout(timer);
      detach();
   }
}

/**
 * `AbortSignal.reason` when a publisher-issued query timeout fires, so code
 * holding the signal can tell it from a client cancel or driver abort without
 * matching error messages.
 */
export const PUBLISHER_QUERY_TIMEOUT_REASON = Symbol.for(
   "publisher.queryTimeout",
);
