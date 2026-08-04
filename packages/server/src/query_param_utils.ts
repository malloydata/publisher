// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Express query-param normalization helpers.
 *
 * Kept in a standalone file (no transitive imports) so unit specs can
 * exercise them without dragging in `server.ts` — which transitively
 * constructs `EnvironmentStore` and kicks off an async storage init
 * (clone of `malloy-samples`, package downloads, ...). When that init
 * runs in a `bun test` process it races the test runner's exit and
 * leaves a partially-populated `publisher_data/` on disk, which the
 * next process (integration tests) then trips over.
 */

/** Normalize an Express query param into a string[] or undefined. */
export function normalizeQueryArray(value: unknown): string[] | undefined {
   if (value === undefined || value === null) return undefined;
   if (Array.isArray(value)) return value.map(String);
   return [String(value)];
}

/**
 * Parse an Express query param as a non-negative integer, or `undefined` when
 * it is absent or not a finite integer. Degrades a garbage value (`?limit=abc`)
 * to "unset" rather than passing `NaN` down into a SQL `LIMIT`/`OFFSET` bind.
 */
export function parseNonNegativeIntParam(value: unknown): number | undefined {
   if (value === undefined || value === null) return undefined;
   const parsed = parseInt(String(value), 10);
   return Number.isInteger(parsed) && parsed >= 0 ? parsed : undefined;
}

/** Outcome of {@link parseReloadParam}: a usable boolean, or a refusal. */
export type ReloadParam = { ok: true; reload: boolean } | { ok: false };

/**
 * Parse the `reload` query param on a route that honors it.
 *
 * `api-doc.yaml` types `reload` as a boolean, and OpenAPI serializes a boolean
 * as lowercase `true`/`false`, so those two spellings (plus absent) are the
 * whole accepted set. Anything else returns `{ ok: false }` for the caller to
 * answer 400 with.
 *
 * Reading the param as `=== "true"` instead treats every other spelling as "do
 * not reload", so `?reload=1`, `?reload=yes` and `?reload=TRUE` each answer 200
 * without recompiling. That is the same failure the collection-route guard
 * exists to stop, wearing a different disguise: the caller edits a model, sees
 * 200, and queries a model the server never recompiled. An invalid value must
 * not still drive behavior, so it is refused rather than read as false.
 *
 * Coercing `1`/`yes`/`TRUE` would put the guessing back, and a caller that
 * meant to reload is better served by a loud 400 than by a silent no-op. A
 * repeated param (`?reload=true&reload=1`) arrives as an array and is refused
 * for the same reason.
 */
export function parseReloadParam(value: unknown): ReloadParam {
   if (value === undefined || value === null)
      return { ok: true, reload: false };
   if (value === "true") return { ok: true, reload: true };
   if (value === "false") return { ok: true, reload: false };
   return { ok: false };
}

/**
 * The 400 message for a `reload` value {@link parseReloadParam} refused.
 *
 * Lives here, next to the rule it explains, because the modern
 * (`/environments/...`) and legacy (`/projects/...`) route pairs both emit it and
 * a hand-copied second version would drift. `routePath` is the caller's own
 * path, so the suggested fix is one they can paste.
 */
export function invalidReloadMessage(
   value: unknown,
   routePath: string,
): string {
   return (
      `Invalid reload value ${JSON.stringify(value)}: expected "true" or ` +
      `"false". Fix: GET ${routePath}?reload=true.`
   );
}
