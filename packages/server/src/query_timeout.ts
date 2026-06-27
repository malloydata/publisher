import { type Counter } from "@opentelemetry/api";
import { publisherMeter } from "./telemetry";

import { getQueryTimeoutMs } from "./config";
import { QueryTimeoutError } from "./errors";

/**
 * Lazy-initialized telemetry. Instruments are created on first use
 * rather than at module load so unit tests that install a real
 * MeterProvider AFTER the module is imported still record their
 * data — OTel JS's `ProxyMeter` binds counters created before
 * `setGlobalMeterProvider` to NoOp instruments (see
 * https://github.com/open-telemetry/opentelemetry-js/issues/3505).
 * In production the first request triggers initialization, which
 * is well after `server.ts` boot and any OTel SDK setup, so the
 * lazy-init has no observable latency cost.
 */
let queryTimeoutCounter: Counter | null = null;
let timeoutTelemetryInitialized = false;
/**
 * Idempotent installer for the timeout counter + config gauge.
 * Called at the top of {@link runWithQueryTimeout} (every path,
 * including the opt-out branch) so the gauge is registered with
 * the OTel SDK as soon as the publisher serves its first query —
 * not just on the first timeout firing. Returns the counter for
 * convenience.
 */
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
      // Observable gauge so dashboards can render the *configured*
      // timeout alongside actual query durations from
      // `malloy_model_query_duration` /
      // `http_server_request_duration_ms`. Read live on each
      // scrape so an env-var change between scrapes is visible
      // without a restart.
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
               // A misconfigured env var should fail the request
               // that observes it, not the metric scrape. Surface
               // as -1 so dashboards reveal the misconfig rather
               // than silently dropping the sample.
               observation.observe(-1);
            }
         });
      timeoutTelemetryInitialized = true;
   }
   return queryTimeoutCounter;
}

/**
 * Visible for tests so they can re-trigger lazy init against a
 * freshly-installed MeterProvider between cases. Do NOT call from
 * production code.
 */
export function resetQueryTimeoutTelemetryForTesting(): void {
   queryTimeoutCounter = null;
   timeoutTelemetryInitialized = false;
}

/**
 * Per-query wall-clock guard. Hands an {@link AbortSignal} to `fn`
 * and arms a `setTimeout` for `timeoutMs`. When the timer fires the
 * signal is aborted with reason `Symbol.for("publisher.queryTimeout")`
 * so downstream catch blocks can distinguish a publisher-issued
 * timeout from a caller cancel or a driver-internal abort.
 *
 * Contract:
 * - `fn` MUST forward `signal` into the underlying SDK call
 *   (`runSQLOptions.abortSignal`, `runnable.run({ abortSignal })`,
 *   etc.) so the abort actually cancels the work — not just unblocks
 *   the awaiter. Failure to forward leaks the query for `timeoutMs`
 *   beyond the 504 response.
 * - `timeoutMs === 0` opts out (no timer is armed); the signal is
 *   still passed for consistency. Use when an operator deliberately
 *   sets `PUBLISHER_QUERY_TIMEOUT_MS=0`.
 * - On timeout AND a subsequent rejection from `fn`, this throws
 *   {@link QueryTimeoutError}. If `fn` happens to resolve cleanly
 *   between "timer fired" and "we entered the catch" (a race that
 *   any driver can win), the success value is returned to the
 *   caller — a query that completed is more useful than a 504 with
 *   an already-materialized result. The timeout counter ticks only
 *   when 504 is actually emitted.
 * - On non-timeout error, the underlying error is re-thrown
 *   unmodified.
 */
export async function runWithQueryTimeout<T>(
   fn: (signal: AbortSignal) => Promise<T>,
   timeoutMs: number,
): Promise<T> {
   // Install telemetry on every call (idempotent) so the
   // `publisher_query_timeout_ms` gauge shows up in `/metrics` as
   // soon as the publisher serves its first query, even if no
   // timeout ever fires. Without this, the gauge would be absent
   // until the first 504 — useless for "tune the timeout BEFORE
   // you get paged" workflows.
   ensureTimeoutTelemetry();

   if (timeoutMs <= 0) {
      // Opt-out path: no timer, no abort. We still pass a never-aborts
      // signal so `fn`'s signature is uniform and forwarding stays
      // mechanical — no per-call branching for "did we get a timeout?".
      const ac = new AbortController();
      return fn(ac.signal);
   }

   const ac = new AbortController();
   const reason = PUBLISHER_QUERY_TIMEOUT_REASON;
   let timedOut = false;
   const timer = setTimeout(() => {
      timedOut = true;
      // `abort(reason)` propagates the reason through `signal.reason`
      // so a downstream catch can `signal.reason === reason` to tell
      // "publisher timeout" from "client disconnect" from "driver
      // internal error" without string-matching error messages.
      ac.abort(reason);
   }, timeoutMs);
   // Match HTTP request lifecycle: don't keep the event loop alive
   // just for the timer. If the process is shutting down and the
   // query has already resolved, we don't want this hanging the
   // graceful-shutdown.
   timer.unref?.();

   try {
      return await fn(ac.signal);
   } catch (error) {
      if (timedOut) {
         // Increment before throwing so the counter ticks even if
         // the controller swallows the error (the MCP tool's catch
         // surfaces failures as content payloads, for instance).
         // Carry the configured timeout as a label so dashboards
         // can pivot a flapping pod between "we tuned the env var
         // down" and "queries got slower".
         ensureTimeoutTelemetry().add(1, { timeout_ms: timeoutMs });
         throw new QueryTimeoutError(
            `Query exceeded PUBLISHER_QUERY_TIMEOUT_MS (${timeoutMs}ms) and was aborted. Refine the query (add a more selective WHERE, lower LIMIT, or simplify joins) or raise the timeout.`,
         );
      }
      throw error;
   } finally {
      clearTimeout(timer);
   }
}

/**
 * Sentinel attached to `AbortSignal.reason` when a publisher-issued
 * query timeout fires. Currently consumed by `runWithQueryTimeout`
 * itself (and by tests verifying the wiring); exported so future
 * call sites composing this signal with their own (e.g. a custom
 * driver wrapper) can write `if (signal.reason === PUBLISHER_QUERY_TIMEOUT_REASON)`
 * to detect "this was the publisher's timeout, not the cap" without
 * coupling to error-message strings. `runWithQueryTimeout`'s own
 * timeout-vs-other-error distinction uses the local `timedOut`
 * flag rather than this symbol, so consumers can rely on the
 * symbol being attached even if the implementation changes.
 */
export const PUBLISHER_QUERY_TIMEOUT_REASON = Symbol.for(
   "publisher.queryTimeout",
);
