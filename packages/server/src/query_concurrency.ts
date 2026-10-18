// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { type Counter } from "@opentelemetry/api";
import { publisherMeter } from "./telemetry";
import type { NextFunction, Request, RequestHandler, Response } from "express";

import { getMaxConcurrentQueries } from "./config";
import { ServiceUnavailableError } from "./errors";
import { logger } from "./logger";

/**
 * In-flight query slots. Module-scoped so all gated routes share one pool:
 * the cap bounds aggregate query load, not per-route load.
 */
let active = 0;

/**
 * Lazily created so a MeterProvider installed after import (as tests do)
 * still receives data: OTel's `ProxyMeter` binds earlier instruments to NoOp.
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
      meter
         .createObservableGauge("publisher_query_active_slots", {
            description:
               "In-flight queries currently holding a per-pod concurrency slot",
         })
         .addCallback((observation) => observation.observe(active));

      // Exposed so dashboards can render utilization (`active / max`). Read
      // per scrape so an env-var change shows immediately.
      meter
         .createObservableGauge("publisher_query_max_slots", {
            description:
               "Current effective PUBLISHER_MAX_CONCURRENT_QUERIES (0 = disabled)",
         })
         .addCallback((observation) => {
            try {
               observation.observe(getMaxConcurrentQueries());
            } catch {
               // A misconfigured env var fails the next request, not the
               // scrape; -1 makes it visible.
               observation.observe(-1);
            }
         });
      concurrencyTelemetryInitialized = true;
   }
   return queryConcurrencyRejectionsCounter;
}

/** Test-only: drop cached instruments so a fresh MeterProvider captures them. */
export function resetQueryConcurrencyTelemetryForTesting(): void {
   queryConcurrencyRejectionsCounter = null;
   concurrencyTelemetryInitialized = false;
}

export function getActiveQueryCount(): number {
   return active;
}

/** Test-only: reset after a test that crashed mid-handler. */
export function resetActiveQueryCountForTesting(): void {
   active = 0;
}

/**
 * Handle on an acquired concurrency slot. The caller MUST call `release()`
 * when the work is done; it is idempotent, so registering it on both
 * `finish` and `close` is safe.
 */
export interface QuerySlotHandle {
   release: () => void;
}

/**
 * Synchronous slot acquisition shared by the HTTP middleware and MCP tools.
 * Throws {@link ServiceUnavailableError} (503) at the cap. `routeLabel` only
 * labels the rejection counter, so keep its cardinality bounded (route
 * patterns, fixed strings like `mcp:executeQuery`). Direct callers release
 * in their own try/finally.
 */
export function tryAcquireQuerySlot(routeLabel: string): QuerySlotHandle {
   // On every call so the gauges exist even on pods that never hit the cap.
   ensureConcurrencyTelemetry();

   const limit = getMaxConcurrentQueries();
   if (limit <= 0) {
      // Opt-out: for deployments with an upstream concurrency bound.
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

/**
 * Express middleware bounding concurrently in-flight query requests per pod,
 * so a burst of expensive queries cannot land at once and stampede aggregate
 * memory. Complements the per-response row/byte caps, the memory governor,
 * and the query timeout.
 *
 * The timeout does not reach every slot holder: compile and the sqlSource
 * routes have no `abortSignal` upstream, so there is nothing to cancel, and
 * releasing their slot early would let the pod run more work than it has
 * slots. They are bounded by the HTTP server timeout (`close` releases the
 * slot); the MCP compile tool releases in a `finally`, so a client that
 * disconnects mid-compile holds its slot until the compile finishes.
 *
 * It does NOT queue: a queue hides load and inflates p99, while failing fast
 * lets the load balancer retry on a less-loaded pod. A response that never
 * emits `finish`/`close` leaks its slot until restart.
 */
export function queryConcurrencyMiddleware(
   req: Request,
   res: Response,
   next: NextFunction,
): void {
   let handle: QuerySlotHandle;
   try {
      // The registered route pattern, not the concrete URL, keeps label
      // cardinality bounded.
      handle = tryAcquireQuerySlot(req.route?.path ?? req.path);
   } catch (error) {
      next(error);
      return;
   }
   // `close` fires on client disconnect (and sometimes after `finish`); the
   // release is idempotent.
   res.on("finish", handle.release);
   res.on("close", handle.release);
   next();
}

/** Typed `RequestHandler` for route registration. */
export function queryConcurrency(): RequestHandler {
   return queryConcurrencyMiddleware;
}
