// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The panel's givens after a filter change. `Report` hands back only the
 * givens it declares (`managed`), so a managed name missing from `next` was
 * cleared, and every other name in `current` was never the report's to
 * change: it is kept. The Console's `useGivenUrlParams` applies the same rule
 * to the URL.
 */
export function mergeGivens(
   current: Record<string, string>,
   next: Record<string, string>,
   managed: readonly string[],
): Record<string, string> {
   const owned = new Set(managed);
   const kept = Object.fromEntries(
      Object.entries(current).filter(([name]) => !owned.has(name)),
   );
   return { ...kept, ...next };
}
