// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { NextFunction, Request, RequestHandler, Response } from "express";

export const FRAME_ANCESTORS_ENV = "PUBLISHER_FRAME_ANCESTORS";

/**
 * The framing policy applied to every document this server returns.
 *
 * Two things were wrong before, and the order they are fixed in matters.
 *
 * FIRST, the policy covered one route. Only in-package `public/` HTML carried
 * `frame-ancestors`; the Console catch-all set no framing header at all, so
 * notebooks, dashboards, models and the Explorer stayed framable from anywhere
 * even on a deployment that had set `PUBLISHER_FRAME_ANCESTORS`. That is worse
 * than a permissive default: setting the variable implied a coverage it did not
 * have, so an operator who did the documented thing got a policy over a corner
 * of the surface and no signal about the rest. It is fixed first, because
 * tightening the default while the knob still lied would have hardened the one
 * route that was already covered and left the rest open under a stricter-looking
 * configuration.
 *
 * SECOND, the default was `*` -- any origin may frame any page. A page with a
 * control worth clicking is then a standing clickjacking target, on a server
 * whose own posture doc says to put it behind a trusted tier. The default is now
 * `'self'`, and embedding is opt-in per deployment.
 *
 * Applied as one middleware ahead of every route rather than per handler, so a
 * route added later inherits it instead of having to remember. The header is
 * cheap and unconditional; narrowing it to HTML would mean predicting the
 * content type before the handler that decides it has run.
 *
 * `X-Frame-Options` is deliberately NOT sent alongside. It is the pre-CSP
 * spelling, it cannot express a list of origins, and where both are present
 * browsers honour `frame-ancestors` -- so sending both means maintaining two
 * policies that must agree, and a deployment that widens one and forgets the
 * other gets the narrower answer with no obvious cause.
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
      // A default Express stack does not set this, but `helmet` and some
      // proxies do. Removing it keeps one policy in force rather than two that
      // can disagree; see the note above.
      res.removeHeader("X-Frame-Options");
      next();
   };
}

/**
 * The configured value, or `'self'`.
 *
 * Whitespace-only is treated as unset rather than as an empty directive: an
 * empty `frame-ancestors` is not valid CSP, and the shape a deployment actually
 * produces is an env var set to nothing by a template that had no value to
 * substitute. Falling back is the safe reading of that; emitting a malformed
 * directive would drop the policy entirely in some browsers.
 *
 * No further validation. The value is a CSP source list whose grammar includes
 * schemes, wildcards and port forms, and a partial validator here would refuse
 * legitimate deployments while still not catching a genuine typo -- the browser
 * is the only real parser. Unlike `PUBLISHER_RATE_LIMIT`, a malformed value
 * fails visibly in the client's console rather than silently changing a limit.
 */
export function parseFrameAncestors(raw: string | undefined): string {
   if (raw === undefined || raw.trim() === "") return "'self'";
   return raw.trim();
}
