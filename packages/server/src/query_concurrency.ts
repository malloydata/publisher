// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   lazyCounter,
   lazyInstrument,
   publisherMeter,
   resetInstrumentsForTesting,
} from "./telemetry";
import type { NextFunction, Request, RequestHandler, Response } from "express";

import { getMaxConcurrentQueries } from "./config";
import { ServiceUnavailableError } from "./errors";
import { logger } from "./logger";

/**
 * The pool `publisher_query_active_slots` reports: the one that last took a
 * slot. A process builds one pool (buildApp in main_server.ts), so this is
 * that pool; a spec that builds several sees the one it used last.
 */
let metered: QuerySlots | null = null;
function observe(pool: QuerySlots): void {
   metered = pool;
}

const queryConcurrencyRejections = lazyCounter(
   "publisher_query_concurrency_rejections_total",
   "Queries rejected with 503 because the per-pod PUBLISHER_MAX_CONCURRENT_QUERIES cap was reached",
);

const queryConcurrencyGauges = lazyInstrument(() => {
   const meter = publisherMeter();
   const activeSlots = meter.createObservableGauge(
      "publisher_query_active_slots",
      {
         description:
            "In-flight queries currently holding a per-pod concurrency slot",
      },
   );
   activeSlots.addCallback((observation) =>
      observation.observe(metered?.active ?? 0),
   );
   // Exposed so dashboards can render utilization (`active / max`). Read per
   // scrape so an env-var change shows immediately.
   const maxSlots = meter.createObservableGauge("publisher_query_max_slots", {
      description:
         "Current effective PUBLISHER_MAX_CONCURRENT_QUERIES (0 = disabled)",
   });
   maxSlots.addCallback((observation) => {
      try {
         observation.observe(getMaxConcurrentQueries());
      } catch {
         // A misconfigured env var fails the next request, not the scrape;
         // -1 makes it visible.
         observation.observe(-1);
      }
   });
   return [activeSlots, maxSlots];
});

/** Test-only: kept for component specs; drops every lazily created instrument. */
export const resetQueryConcurrencyTelemetryForTesting =
   resetInstrumentsForTesting;

/**
 * Handle on an acquired concurrency slot. The caller MUST call `release()`
 * when the work is done; it is idempotent, so registering it on both
 * `finish` and `close` is safe.
 */
export interface QuerySlotHandle {
   release: () => void;
}

/** Marks the middleware a QuerySlots hands out, so a route table can tell a gated route. */
const QUERY_SLOT_MIDDLEWARE = Symbol("querySlotMiddleware");

/**
 * In-flight query slots. The composition root builds one pool per process and
 * hands it to every gated REST route, the MCP tools that take a slot, the
 * query use case and retrieval's value lookups, so the cap bounds aggregate
 * query load, not per-route load.
 */
export class QuerySlots {
   #active = 0;

   /** Slots currently held. */
   get active(): number {
      return this.#active;
   }

   /**
    * Synchronous slot acquisition shared by the HTTP middleware and MCP tools.
    * Throws {@link ServiceUnavailableError} (503) at the cap. `routeLabel` only
    * labels the rejection counter, so keep its cardinality bounded (route
    * patterns, fixed strings like `mcp:executeQuery`). Direct callers release
    * in their own try/finally.
    */
   tryAcquire(routeLabel: string): QuerySlotHandle {
      observe(this);
      // On every call so the gauges exist even on pods that never hit the cap.
      queryConcurrencyGauges();

      const limit = getMaxConcurrentQueries();
      if (limit <= 0) {
         // Opt-out: for deployments with an upstream concurrency bound.
         return { release: () => undefined };
      }

      if (this.#active >= limit) {
         queryConcurrencyRejections().add(1, {
            "http.route": routeLabel,
            limit,
         });
         logger.warn(
            `Rejecting query: ${this.#active}/${limit} slots in use (PUBLISHER_MAX_CONCURRENT_QUERIES).`,
            { route: routeLabel },
         );
         throw new ServiceUnavailableError(
            `Publisher pod is at its maximum concurrent query cap (${limit}). Retry after in-flight queries complete, or raise PUBLISHER_MAX_CONCURRENT_QUERIES.`,
         );
      }

      this.#active += 1;
      let released = false;
      return {
         release: () => {
            if (released) return;
            released = true;
            this.#active = Math.max(0, this.#active - 1);
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
    * hands the caller a 503 it can retry. Where several replicas sit behind a
    * load balancer, the retry can land on a less-loaded one; a standalone
    * server has only itself, so its caller retries once in-flight queries
    * complete. A response the handler never ends, and that never emits
    * `finish`/`close`, leaks its slot until restart.
    */
   readonly middleware: RequestHandler = Object.assign(
      (req: Request, res: Response, next: NextFunction): void => {
         let handle: QuerySlotHandle;
         try {
            // The registered route pattern, not the concrete URL, keeps label
            // cardinality bounded.
            handle = this.tryAcquire(req.route?.path ?? req.path);
         } catch (error) {
            next(error);
            return;
         }
         // `close` fires on client disconnect (and sometimes after `finish`);
         // the release is idempotent.
         res.on("finish", handle.release);
         res.on("close", handle.release);
         // Bun 1.3.13 emits neither event on a response whose client went away
         // mid-request: not at the disconnect, and not when the handler later
         // writes its answer to the dead socket, which still reads as open.
         // Released only on those, every abandoned query kept its slot until
         // restart. `end()` is the call the handler makes on every runtime once
         // its work is done, so the slot goes back there too, before the bytes
         // are flushed.
         const end = res.end;
         res.end = function (this: Response, ...args: unknown[]) {
            handle.release();
            return (end as (...a: unknown[]) => Response).apply(this, args);
         } as Response["end"];
         next();
      },
      { [QUERY_SLOT_MIDDLEWARE]: true },
   );
}

/** Whether `handle` is a QuerySlots middleware, i.e. the route is admission-controlled. */
export function isQuerySlotMiddleware(handle: unknown): boolean {
   return (
      typeof handle === "function" &&
      (handle as unknown as Record<symbol, unknown>)[QUERY_SLOT_MIDDLEWARE] ===
         true
   );
}
