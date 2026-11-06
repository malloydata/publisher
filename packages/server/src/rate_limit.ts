// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { Request, RequestHandler } from "express";
import rateLimit from "express-rate-limit";

/** Probes and metrics are never limited: they answer the orchestrator. */
function isOperationalPath(req: Request): boolean {
   return req.path === "/metrics" || req.path.startsWith("/health");
}

/**
 * Per-client request rate limiting for the REST app: `perMinute` requests per
 * client per minute (the `rateLimit` setting, `PUBLISHER_RATE_LIMIT`), and `0`,
 * its default, is no limit. The middleware is mounted either way so the limit
 * can be turned on with configuration alone.
 *
 * Clients are keyed by the socket's peer address. Behind a reverse proxy
 * every request arrives from the proxy, so all clients share one bucket:
 * rate-limit at the proxy in that deployment, or leave this off.
 */
export function rateLimitMiddleware(perMinute: number): RequestHandler {
   const enabled = perMinute > 0;
   return rateLimit({
      windowMs: 60_000,
      limit: enabled ? perMinute : Number.MAX_SAFE_INTEGER,
      standardHeaders: "draft-7",
      legacyHeaders: false,
      skip: (req) => !enabled || isOperationalPath(req),
      message: {
         code: 429,
         message: `Too many requests: this client exceeded ${perMinute} requests per minute.`,
      },
   });
}
