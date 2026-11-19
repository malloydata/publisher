// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A rollup's `freshness` group, read strictly: `freshness.window=` and
 * `freshness.fallback=` as on `#@ persist` and `#(materialize)`, where a key
 * outside the group or a value the publisher cannot read is a malformed
 * declaration, which fails the publish and the load (the load-vs-refused
 * policy). Read by both rollup spellings, `#(rollup)` and
 * `#@ preaggregate`.
 *
 * Pure.
 */

import {
   FRESHNESS_FALLBACKS,
   freshnessWindowSeconds,
   isFreshnessFallback,
   type DeclaredFreshness,
} from "../domain/freshness";
import type { RouteTag } from "./declaration";

/** The keys of a `freshness` group. */
export const FRESHNESS_GROUP_KEYS: ReadonlySet<string> = new Set([
   "window",
   "fallback",
]);

/** What a `freshness` group declares, or why it cannot be read. */
type FreshnessGroupRead =
   | { freshness?: DeclaredFreshness }
   | { unknownKeys: string[] }
   | { invalid: string };

/**
 * The `freshness` group at `path` (`["freshness"]`, or
 * `["preaggregate", "freshness"]` for the nested form): nothing when it is
 * not written; its unknown keys; or why a value is unusable (a window that is
 * not a whole number and one unit of `s`, `m`, `h`, `d`, `w`, a fallback
 * outside `live`, `stale_ok`, `fail`, or a group written as a plain value).
 */
export function readFreshnessGroup(
   tag: RouteTag,
   path: readonly string[],
): FreshnessGroupRead {
   if (!tag.has(...path)) return {};
   const group = tag.tag(...path);
   const keys = [...(group?.entries() ?? [])].map(([key]) => key);
   if (keys.length === 0) {
      return {
         invalid:
            '`freshness` takes properties, as freshness.window="24h" or freshness.fallback="live"',
      };
   }
   const unknownKeys = keys.filter((k) => !FRESHNESS_GROUP_KEYS.has(k));
   if (unknownKeys.length > 0) return { unknownKeys };
   const freshness: DeclaredFreshness = {};
   if (keys.includes("window")) {
      const window = tag.text(...path, "window");
      if (
         window === undefined ||
         freshnessWindowSeconds(window) === undefined
      ) {
         return {
            invalid:
               "has a freshness.window= that is not a whole number and a unit " +
               '(s, m, h, d or w), such as freshness.window="24h"',
         };
      }
      freshness.window = window.trim();
   }
   if (keys.includes("fallback")) {
      const fallback = tag.text(...path, "fallback");
      if (!isFreshnessFallback(fallback)) {
         return {
            invalid: `has a freshness.fallback= that is not one of ${FRESHNESS_FALLBACKS.join(", ")}`,
         };
      }
      freshness.fallback = fallback;
   }
   return { freshness };
}

/**
 * One rollup's freshness from the declarations of its measures at one grain
 * and placement, or the field two of them disagree on (windows compare as
 * durations, so `24h` and `1d` agree). A declaration that says nothing about
 * a field leaves it to the others.
 */
export function mergeRollupFreshness(
   declared: readonly (DeclaredFreshness | undefined)[],
): { freshness?: DeclaredFreshness } | { conflict: "window" | "fallback" } {
   const merged: DeclaredFreshness = {};
   for (const d of declared) {
      if (!d) continue;
      if (d.window !== undefined) {
         if (
            merged.window !== undefined &&
            freshnessWindowSeconds(merged.window) !==
               freshnessWindowSeconds(d.window)
         ) {
            return { conflict: "window" };
         }
         merged.window = d.window;
      }
      if (d.fallback !== undefined) {
         if (merged.fallback !== undefined && merged.fallback !== d.fallback) {
            return { conflict: "fallback" };
         }
         merged.fallback = d.fallback;
      }
   }
   return Object.keys(merged).length > 0 ? { freshness: merged } : {};
}

/**
 * A rollup's source layer: `own`, field by field over `base`, its base's own
 * source-level declaration. Undefined when neither declares anything.
 */
export function rollupSourceFreshness(
   own: DeclaredFreshness | undefined,
   base: DeclaredFreshness | undefined,
): DeclaredFreshness | undefined {
   const window = own?.window ?? base?.window;
   const fallback = own?.fallback ?? base?.fallback;
   if (window === undefined && fallback === undefined) return undefined;
   return {
      ...(window !== undefined ? { window } : {}),
      ...(fallback !== undefined ? { fallback } : {}),
   };
}
