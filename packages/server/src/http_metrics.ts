// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `http_server_request_duration_ms` and `http_server_requests_total`, recorded
 * per request. These sit beside the OTel HTTP instrumentation's own
 * `http_server_request_duration` (seconds, semantic-convention labels): theirs
 * is the standard series, these are the documented ones dashboards use.
 */

import type { NextFunction, Request, Response } from "express";
import { isProbePath } from "./logger";
import {
   lazyCounter,
   lazyHistogram,
   REQUEST_DURATION_BUCKETS_MS,
} from "./telemetry";

const httpRequestDuration = lazyHistogram(
   "http_server_request_duration_ms",
   "Duration of HTTP requests in milliseconds",
   { buckets: REQUEST_DURATION_BUCKETS_MS },
);

const httpRequestCount = lazyCounter(
   "http_server_requests_total",
   "Total number of HTTP requests",
);

/** The `http.route` of a request that matched no Express route: static files, the Console, 404s. */
export const UNMATCHED_ROUTE = "unmatched";

/**
 * The matched Express route pattern, never the raw path, so a label value
 * cannot be minted per URL (each static asset, each probe for a missing page).
 */
export function httpRouteLabel(req: Pick<Request, "route">): string {
   const path: unknown = req.route?.path;
   return path === undefined ? UNMATCHED_ROUTE : String(path);
}

export function httpMetricsMiddleware(
   req: Request,
   res: Response,
   next: NextFunction,
) {
   const start = performance.now();

   res.on("finish", () => {
      if (isProbePath(req.path)) return;

      const attrs = {
         "http.method": req.method,
         "http.route": httpRouteLabel(req),
         "http.status_code": res.statusCode,
      };
      httpRequestDuration().record(performance.now() - start, attrs);
      httpRequestCount().add(1, attrs);
   });

   next();
}
