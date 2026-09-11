// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { createHash, timingSafeEqual } from "node:crypto";

/**
 * Request header carrying an authorize bypass, for trusted data-management
 * callers.
 *
 * A header rather than a `QueryRequest` field because a deployment fronting this
 * API generally reuses the same generated request schema for its own inbound
 * body, so a body field able to reach the worker would also be settable by that
 * deployment's external callers. Set on the client at construction, it never
 * passes through body conversion, so no caller body value can reach it.
 *
 * Lowercase because Node lowercases inbound header names.
 */
export const BYPASS_AUTHORIZE_HEADER = "x-publisher-bypass-authorize";

/**
 * Env var holding the shared secret a bypass request must present in
 * {@link BYPASS_AUTHORIZE_HEADER}. Unset means the bypass is unavailable.
 */
export const BYPASS_AUTHORIZE_SECRET_ENV = "PUBLISHER_BYPASS_AUTHORIZE_SECRET";

/**
 * Minimal structural type the reader needs from an HTTP request. Narrower than
 * `express.Request` so tests can pass a bare object.
 */
export interface HeaderCarrier {
   headers: Record<string, string | string[] | undefined>;
}

/**
 * Compare two secrets without leaking their common prefix through timing.
 *
 * `timingSafeEqual` throws on a length mismatch, which would itself be an
 * oracle for the secret's length, so both sides are hashed to a fixed-width
 * digest first and the digests are compared.
 */
const secretsMatch = (presented: string, configured: string): boolean => {
   const digest = (value: string): Buffer =>
      createHash("sha256").update(value, "utf8").digest();
   return timingSafeEqual(digest(presented), digest(configured));
};

/**
 * Read the authorize bypass off the request headers, returning `true` only when
 * the request presents the configured shared secret and `undefined` otherwise.
 *
 * FAIL-CLOSED: with {@link BYPASS_AUTHORIZE_SECRET_ENV} unset or blank there is
 * no value a caller could present, so every bypass request is refused. Header
 * presence alone used to be sufficient, which left the author's `#(authorize)`
 * gates disabled by anything that could reach this port and reduced the
 * protection to whatever the deployment's edge happened to strip.
 *
 * A non-`string` header also denies, which covers a duplicated header without a
 * special case: for a custom header Node joins duplicates into one
 * comma-separated string, not an array (the `string[]` arm of
 * {@link HeaderCarrier} is reachable only for `set-cookie`), and a joined value
 * cannot equal the secret.
 *
 * Deliberately does NOT consult the request body: a `bypassAuthorize` body field
 * is inert, which is what keeps the public request schema from becoming a
 * gate-disabling control.
 */
export const readBypassAuthorize = (req: HeaderCarrier): true | undefined => {
   const configured = process.env[BYPASS_AUTHORIZE_SECRET_ENV]?.trim();
   if (!configured) {
      return undefined;
   }
   const raw = req.headers[BYPASS_AUTHORIZE_HEADER];
   if (typeof raw !== "string") {
      return undefined;
   }
   // Untrimmed: a secret is an exact value, and trimming would let a padded
   // variant authenticate.
   return secretsMatch(raw, configured) ? true : undefined;
};
