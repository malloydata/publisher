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

/** Outcome of {@link parseBooleanParam}: a usable boolean, or a refusal. */
export type BooleanParam = { ok: true; value: boolean } | { ok: false };

/**
 * Parse a boolean query param on a route that honors one.
 *
 * `api-doc.yaml` types every one of these as a boolean, and OpenAPI serializes
 * a boolean as lowercase `true`/`false`, so those two spellings (plus absent)
 * are the whole accepted set. Anything else returns `{ ok: false }` for the
 * caller to answer 400 with.
 *
 * Reading a param as `=== "true"` instead treats every other spelling as
 * `false`, so `?reload=1`, `?reload=yes` and `?reload=TRUE` each answer 200
 * without recompiling. The caller edits a model, sees 200, and queries a model
 * the server never recompiled. An invalid value must not still drive behavior,
 * so it is refused rather than read as false.
 *
 * The same reading is why this is shared rather than written per param. It bit
 * `dropTables` hardest: `DELETE ...\/materializations\/{id}?dropTables=1` read as
 * `false`, so the materialization record went away, its tables stayed on disk,
 * and the response was `204 No Content` -- a destructive request that did half
 * its job and said nothing. `bypass_filters` failed closed rather than open,
 * but silently the same way.
 *
 * Coercing `1`/`yes`/`TRUE` would put the guessing back, and a caller that
 * meant it is better served by a loud 400 than by a silent no-op. A repeated
 * param (`?reload=true&reload=1`) arrives as an array and is refused for the
 * same reason: there is no single value to honor.
 */
export function parseBooleanParam(value: unknown): BooleanParam {
   if (value === undefined || value === null) return { ok: true, value: false };
   if (value === "true") return { ok: true, value: true };
   if (value === "false") return { ok: true, value: false };
   return { ok: false };
}

/**
 * The 400 message for a value {@link parseBooleanParam} refused.
 *
 * Lives here, next to the rule it explains, because every route pair that reads
 * a boolean param emits it -- modern (`/environments/...`) and legacy
 * (`/projects/...`) alike -- and a hand-copied second version would drift.
 * `method` and `routePath` are the caller's own, so the suggested fix is one
 * they can paste.
 */
export function invalidBooleanMessage(
   name: string,
   value: unknown,
   method: string,
   routePath: string,
): string {
   return (
      `Invalid ${name} value ${JSON.stringify(value)}: expected "true" or ` +
      `"false". Fix: ${method} ${routePath}?${name}=true.`
   );
}
