// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Express query-param parsing. Must stay import-free: anything that pulls in
 * `server.ts` starts a storage init that, in a `bun test` process, races the
 * runner's exit and leaves a half-populated `publisher_data/` behind.
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
 * Parse a boolean query param. Only lowercase `true`/`false` (how OpenAPI
 * serializes a boolean) or absent are accepted; anything else, a repeated
 * param included, is `{ ok: false }` for the caller to answer 400.
 *
 * Refused rather than read as `false`: `?reload=1` read as false answers 200
 * without recompiling, and `?dropTables=1` read as false deletes the record
 * but keeps the tables, both silently. Coercing `1`/`yes` would bring the
 * guessing back.
 */
export function parseBooleanParam(value: unknown): BooleanParam {
   if (value === undefined || value === null) return { ok: true, value: false };
   if (value === "true") return { ok: true, value: true };
   if (value === "false") return { ok: true, value: false };
   return { ok: false };
}

/**
 * The 400 message for a value {@link parseBooleanParam} refused, shared by the
 * modern and legacy routes. Names the caller's own method and path so the
 * suggested fix can be pasted.
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
