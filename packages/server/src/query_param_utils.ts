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
