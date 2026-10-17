// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { NextFunction, Request, RequestHandler, Response } from "express";

export const FRAME_ANCESTORS_ENV = "PUBLISHER_FRAME_ANCESTORS";

/**
 * The framing policy for every response, default `'self'` so no page is a
 * clickjacking target unless a deployment opts into embedding.
 *
 * One middleware ahead of every route, so a route added later cannot forget
 * it; a per-route policy leaves the rest of the surface framable while the
 * setting implies full coverage. Unconditional, because the content type is
 * not known until the handler runs.
 *
 * `X-Frame-Options` is deliberately not sent: it cannot list origins, and two
 * policies that must agree eventually disagree with no obvious cause.
 */
export function frameAncestorsMiddleware(
   raw: string | undefined,
): RequestHandler {
   const frameAncestors = parseFrameAncestors(raw);
   return (_req: Request, res: Response, next: NextFunction) => {
      res.setHeader(
         "Content-Security-Policy",
         `frame-ancestors ${frameAncestors}`,
      );
      // `helmet` and some proxies set it; keep one policy in force.
      res.removeHeader("X-Frame-Options");
      next();
   };
}

/**
 * The configured value, or `'self'`. Whitespace-only counts as unset (an empty
 * directive is invalid CSP and some browsers would drop the policy). Otherwise
 * unvalidated: the browser is the only real parser of a source list, and a bad
 * value fails visibly in the client's console.
 */
export function parseFrameAncestors(raw: string | undefined): string {
   if (raw === undefined || raw.trim() === "") return "'self'";
   return raw.trim();
}
