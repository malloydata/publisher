// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { Request, RequestHandler } from "express";
import rateLimit from "express-rate-limit";

export const RATE_LIMIT_ENV = "PUBLISHER_RATE_LIMIT";

/**
 * Parse `PUBLISHER_RATE_LIMIT`: the maximum number of requests one client
 * may make per minute. Unset, empty, or `0` means no limit. Anything else
 * that is not a non-negative integer is a startup error rather than a
 * silent default, in line with the other `PUBLISHER_*` switches.
 */
export function parseRateLimit(raw: string | undefined): number | undefined {
   if (raw === undefined || raw.trim() === "") return undefined;
   const n = Number(raw);
   if (!Number.isInteger(n) || n < 0) {
      throw new Error(
         `${RATE_LIMIT_ENV} must be a non-negative integer (requests per minute per client); got "${raw}"`,
      );
   }
   return n === 0 ? undefined : n;
}

/** Probes and metrics are never limited: they answer the orchestrator. */
function isOperationalPath(req: Request): boolean {
   return req.path === "/metrics" || req.path.startsWith("/health");
}

/**
 * Per-client request rate limiting for the REST app. Off unless
 * `PUBLISHER_RATE_LIMIT` is set; the middleware is mounted either way so the
 * limit can be turned on with configuration alone.
 *
 * Clients are keyed by the socket's peer address. Behind a reverse proxy
 * every request arrives from the proxy, so all clients share one bucket:
 * rate-limit at the proxy in that deployment, or leave this off.
 */
export function rateLimitMiddleware(
   perMinute: number | undefined,
): RequestHandler {
   const enabled = perMinute !== undefined;
   return rateLimit({
      windowMs: 60_000,
      limit: perMinute ?? Number.MAX_SAFE_INTEGER,
      standardHeaders: "draft-7",
      legacyHeaders: false,
      skip: (req) => !enabled || isOperationalPath(req),
      message: {
         code: 429,
         message: `Too many requests: this client exceeded ${perMinute} requests per minute.`,
      },
   });
}
