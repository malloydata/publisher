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

export function httpRouteLabel(req: Pick<Request, "route" | "path">): string {
   return req.route?.path ?? req.path;
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
