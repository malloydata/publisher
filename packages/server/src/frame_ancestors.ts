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

/**
 * One frame-ancestors source as a page-context host pattern, or undefined when
 * it cannot name an embedding origin. Kept: `'self'` (the runtime maps it to
 * its own origin), `scheme://host[:port]` for http(s), and the same with a
 * leading `*.` host label. Dropped: `*`, scheme-only sources, anything with a
 * path, query or credentials, a wildcard anywhere but the leading label, and
 * anything unparseable. Normalized to what a browser reports as
 * `MessageEvent.origin` (lowercase, default port omitted).
 */
export function hostOriginPattern(source: string): string | undefined {
   if (source.toLowerCase() === "'self'") return "'self'";
   const match = /^(https?):\/\/(\*\.)?([^/?#@*\s]+)$/i.exec(source);
   if (!match) return undefined;
   const [, scheme, wildcard, authority] = match;
   try {
      // A real label stands in for the wildcard so URL does the host parsing.
      const url = new URL(`${scheme}://${wildcard ? "w." : ""}${authority}`);
      const host = wildcard ? url.host.slice("w.".length) : url.host;
      return `${url.protocol}//${wildcard ? "*." : ""}${host}`;
   } catch {
      return undefined;
   }
}

/**
 * The page-context `hostOrigins` a frame-ancestors value yields: the sources
 * {@link hostOriginPattern} keeps, deduplicated, in order. Empty when none is
 * usable, which turns the page's host bus off.
 */
export function frameAncestorHostOrigins(raw: string | undefined): string[] {
   const patterns = frameAncestorSources(raw)
      .map(hostOriginPattern)
      .filter((pattern): pattern is string => pattern !== undefined);
   return [...new Set(patterns)];
}

/** The configured sources {@link hostOriginPattern} drops: they stay in the CSP but never become bus origins. */
export function droppedFrameAncestorSources(raw: string | undefined): string[] {
   return frameAncestorSources(raw).filter(
      (source) => hostOriginPattern(source) === undefined,
   );
}

function frameAncestorSources(raw: string | undefined): string[] {
   return parseFrameAncestors(raw).split(/\s+/);
}
