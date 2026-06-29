import { type Counter } from "@opentelemetry/api";
import { publisherMeter } from "./telemetry";
import type { NextFunction, Request, RequestHandler, Response } from "express";

import { getMaxConcurrentQueries } from "./config";
import { ServiceUnavailableError } from "./errors";
import { logger } from "./logger";

/**
 * Process-wide query gauge — the only state owned by this module.
 * Incremented when a gated handler accepts a request; decremented
 * exactly once when the response finishes (whether the handler
 * succeeded, errored, or the client disconnected mid-flight).
 *
 * Module-scoped so all gated routes share one slot pool: the
 * concurrency cap bounds aggregate query load, not per-route load.
 */
let active = 0;

/**
 * OpenTelemetry instruments. Lazy-initialized on first use so unit
 * tests that install a real MeterProvider AFTER the module is
 * imported still receive their data — OTel JS's `ProxyMeter`
 * binds counters created before `setGlobalMeterProvider` to NoOp
 * instruments. Module-scoped state so all gated routes share one
 * counter / one slot pool.
 */
let queryConcurrencyRejectionsCounter: Counter | null = null;
let concurrencyTelemetryInitialized = false;
function ensureConcurrencyTelemetry(): Counter {
   if (queryConcurrencyRejectionsCounter && concurrencyTelemetryInitialized) {
      return queryConcurrencyRejectionsCounter;
   }
   const meter = publisherMeter();
   queryConcurrencyRejectionsCounter = meter.createCounter(
      "publisher_query_concurrency_rejections_total",
      {
         description:
            "Queries rejected with 503 because the per-pod PUBLISHER_MAX_CONCURRENT_QUERIES cap was reached",
      },
   );
   if (!concurrencyTelemetryInitialized) {
      // Live count of in-flight queries holding a slot. Observable
      // gauge so a scrape always sees the current value, not the
      // value at the last admission/release event.
      meter
         .createObservableGauge("publisher_query_active_slots", {
            description:
               "In-flight queries currently holding a per-pod concurrency slot",
         })
         .addCallback((observation) => observation.observe(active));

      // Configured cap, exposed so dashboards can render
      // utilization (`active / max`) without needing a separate
      // config feed. Read on every scrape so a runtime env-var
      // change is reflected immediately; the parse is cheap. A
      // `0` cap means "opt-out" (the middleware is a pass-through)
      // and is reported verbatim.
      meter
         .createObservableGauge("publisher_query_max_slots", {
            description:
               "Current effective PUBLISHER_MAX_CONCURRENT_QUERIES (0 = disabled)",
         })
         .addCallback((observation) => {
            try {
               observation.observe(getMaxConcurrentQueries());
            } catch {
               // A misconfigured env var should fail the next
               // request that observes it, not the metric scrape.
               // Surface as -1 so the misconfig is visible.
               observation.observe(-1);
            }
         });
      concurrencyTelemetryInitialized = true;
   }
   return queryConcurrencyRejectionsCounter;
}

/**
 * Visible for tests. Drops the cached instruments so a fresh
 * MeterProvider can capture them on the next request. Do NOT
 * call from production code.
 */
export function resetQueryConcurrencyTelemetryForTesting(): void {
   queryConcurrencyRejectionsCounter = null;
   concurrencyTelemetryInitialized = false;
}

/**
 * Visible for tests / metrics. Don't mutate from outside.
 */
export function getActiveQueryCount(): number {
   return active;
}

/**
 * Visible for tests so a unit test that crashes mid-handler can
 * reset between cases without spinning a fresh module loader.
 */
export function resetActiveQueryCountForTesting(): void {
   active = 0;
}

/**
 * Express middleware that bounds the number of concurrently
 * in-flight query requests per pod.
 *
 * Defense-in-depth on top of the per-request caps from Steps 1–5:
 *   - Row/byte caps bound a single response.
 *   - Memory governor (Step 4) sheds load when RSS crosses the
 *     high-water mark.
 *   - Query timeout (Step 5) prevents one query from monopolising a
 *     slot indefinitely.
 * This middleware caps the *number of slots in flight* at any one
 * moment so a burst of well-behaved but expensive queries can't all
 * land simultaneously and stampede aggregate memory.
 *
 * Behavior:
 *   - When `active >= limit`, the request is rejected with HTTP 503
 *     and the response body identifies the cap so an operator's
 *     grep finds the rationale immediately.
 *   - When admitted, a single-shot decrement is registered on both
 *     `finish` (normal completion) and `close` (client disconnect).
 *     The handler must release exactly once even if both events
 *     fire.
 *   - `limit === 0` opts out (the middleware becomes a pass-through);
 *     `getMaxConcurrentQueries()` is read per-request so config
 *     changes propagate without a server restart. The per-request
 *     read is a single env-var parse — cheap.
 *
 * Failure-mode notes:
 *   - If the response never emits `finish`/`close` for some reason
 *     (e.g. a runtime crash that bypasses Express' normal
 *     teardown), the slot leaks until process restart. This is the
 *     same failure mode as any active-request counter; in practice
 *     `close` always fires on socket teardown.
 *   - We do NOT queue. A backed-up queue would hide load and inflate
 *     p99 latency; failing fast lets the upstream LB retry against
 *     a less-loaded pod.
 */
/**
 * Handle on an acquired concurrency slot. The caller MUST invoke
 * `release()` exactly once when the work is done (success, error,
 * or cancellation). `release()` is idempotent — calling it twice
 * is a no-op rather than a double-decrement, so wrappers that
 * register both `finish` and `close` listeners stay safe.
 */
export interface QuerySlotHandle {
   release: () => void;
}

/**
 * Synchronous slot acquisition shared by the HTTP middleware and
 * the MCP `executeQuery` tool. Throws {@link ServiceUnavailableError}
 * (which controllers map to HTTP 503) when the pod is at its cap;
 * returns a {@link QuerySlotHandle} on success. The `routeLabel`
 * argument is used only for the rejection counter
 * (`publisher_query_concurrency_rejections_total`) so dashboards
 * can identify the hottest surface — keep its cardinality bounded
 * (Express route patterns, fixed strings like `mcp:executeQuery`).
 *
 * Production callers should prefer {@link queryConcurrencyMiddleware}
 * on HTTP routes (it wires the release to `res.finish`/`close`
 * automatically). Direct callers (MCP) take responsibility for
 * release in their own try/finally.
 */
export function tryAcquireQuerySlot(routeLabel: string): QuerySlotHandle {
   // Lazy-init runs on every call so the active/max gauges show up
   // even on pods where the cap is never reached.
   ensureConcurrencyTelemetry();

   const limit = getMaxConcurrentQueries();
   if (limit <= 0) {
      // Opt-out: no slot bookkeeping. Useful for OSS deployments
      // that already have an upstream concurrency bound.
      return { release: () => undefined };
   }

   if (active >= limit) {
      ensureConcurrencyTelemetry().add(1, {
         "http.route": routeLabel,
         limit,
      });
      logger.warn(
         `Rejecting query: ${active}/${limit} slots in use (PUBLISHER_MAX_CONCURRENT_QUERIES).`,
         { route: routeLabel },
      );
      throw new ServiceUnavailableError(
         `Publisher pod is at its maximum concurrent query cap (${limit}). Retry after in-flight queries complete, or raise PUBLISHER_MAX_CONCURRENT_QUERIES.`,
      );
   }

   active += 1;
   let released = false;
   return {
      release: () => {
         if (released) return;
         released = true;
         active = Math.max(0, active - 1);
      },
   };
}

export function queryConcurrencyMiddleware(
   req: Request,
   res: Response,
   next: NextFunction,
): void {
   let handle: QuerySlotHandle;
   try {
      // `req.route?.path` gives the Express-registered pattern
      // (e.g. `/api/v0/environments/:environmentName/.../sqlQuery`)
      // rather than the concrete URL, keeping label cardinality
      // bounded.
      handle = tryAcquireQuerySlot(req.route?.path ?? req.path);
   } catch (error) {
      next(error);
      return;
   }
   // Both events fire on different code paths; we want to release
   // on whichever comes first and ignore the second:
   //   - `finish`: normal completion (response fully flushed).
   //   - `close`: client disconnected before completion (or after,
   //     in some Express/Node versions; hence the idempotency).
   res.on("finish", handle.release);
   res.on("close", handle.release);
   next();
}

/**
 * Convenience for the route-registration call site: produces a
 * single middleware reference so registrations stay readable.
 * Returning a typed `RequestHandler` keeps Express' overloads happy.
 */
export function queryConcurrency(): RequestHandler {
   return queryConcurrencyMiddleware;
}
