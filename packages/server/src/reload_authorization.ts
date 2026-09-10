// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Authorization for the package-reload side of `GET /…/packages/:pkg`.
 *
 * A reload is not a read: it recompiles the package from disk, replaces the
 * served model, and on a package with an install `location` re-fetches over
 * on-disk edits. That is expensive and state-changing, so it needs a caller who
 * was meant to trigger it, while a plain metadata GET stays open.
 *
 * FAIL-CLOSED: with no secret configured a reload over HTTP is refused. That
 * makes the default posture "reads stay open, recompiles do not", and it is a
 * default flip for a deployment that relied on an unauthenticated
 * `?reload=true`. The MCP `reload_package` tool is unaffected — it does not pass
 * through this boundary, and its endpoint binds loopback by default.
 *
 * Kept in a standalone file with no transitive imports, like
 * `query_param_utils.ts`, so unit specs can exercise it without importing
 * `server.ts` (which constructs an `EnvironmentStore` and kicks off async
 * storage init).
 */

import { createHash, timingSafeEqual } from "node:crypto";

/**
 * Request header carrying the reload secret.
 *
 * Lowercase because Node lowercases inbound header names.
 */
export const RELOAD_SECRET_HEADER = "x-publisher-reload-secret";

/** Env var holding the secret a reload request must present. */
export const RELOAD_SECRET_ENV = "PUBLISHER_RELOAD_SECRET";

/** Minimal structural type the check needs, so tests can pass a bare object. */
export interface ReloadHeaderCarrier {
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

/** Why a reload was refused, for the caller's 403 message. */
export type ReloadAuthorization =
   | { authorized: true }
   | { authorized: false; reason: "not-configured" | "bad-secret" };

/**
 * Whether this request may trigger a package reload.
 *
 * Call ONLY when `reload` is true: a plain metadata GET is a read and must not
 * require the secret.
 */
export const authorizeReload = (
   req: ReloadHeaderCarrier,
): ReloadAuthorization => {
   const configured = process.env[RELOAD_SECRET_ENV]?.trim();
   if (!configured) {
      return { authorized: false, reason: "not-configured" };
   }
   const presented = req.headers[RELOAD_SECRET_HEADER];
   if (typeof presented !== "string") {
      return { authorized: false, reason: "bad-secret" };
   }
   // Untrimmed: a secret is an exact value, and trimming would let a padded
   // variant authenticate.
   return secretsMatch(presented, configured)
      ? { authorized: true }
      : { authorized: false, reason: "bad-secret" };
};

/**
 * The 403 message for a refused reload.
 *
 * The two reasons are kept distinct because they call for different operator
 * action: `not-configured` means nobody can reload over HTTP until a secret is
 * set, while `bad-secret` means this caller presented the wrong one. Collapsing
 * them into one message would send an operator hunting a credential bug when the
 * server simply has the feature turned off.
 */
export const reloadDeniedMessage = (
   reason: "not-configured" | "bad-secret",
): string =>
   reason === "not-configured"
      ? `Reload is not enabled on this server. A package reload changes served ` +
        `state, so it requires a shared secret: set ${RELOAD_SECRET_ENV} and ` +
        `send it in the ${RELOAD_SECRET_HEADER} header. Reading package ` +
        `metadata without ?reload=true needs no secret.`
      : `Invalid or missing ${RELOAD_SECRET_HEADER} header. A package reload ` +
        `changes served state, so it requires the secret configured in ` +
        `${RELOAD_SECRET_ENV}. Reading package metadata without ?reload=true ` +
        `needs no secret.`;
