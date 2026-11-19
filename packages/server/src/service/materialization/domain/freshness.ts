// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Whether a stored table is fresh enough to read at an instant, and what a
 * query does when it is not (the source's `fallback`).
 *
 * Two invariants:
 *  1. Evaluated per query, never at bind: a table that crosses its window
 *     while the package stays loaded must stop serving. The pin
 *     (`binding.ts`) is the one caller.
 *  2. Aged from `dataAsOf` (the snapshot start the host supplies), not from
 *     build completion: the window bounds the data's age.
 *
 * `fail` degrades to answering live, as `live` does: erroring a query that
 * joins a stale source is not implemented.
 *
 * Pure: the caller passes the instant.
 *
 * One model for every mechanism and mode: a `#@ persist`, `#(materialize)` or
 * rollup table, database or `storage=`, built by a standalone run or a host,
 * carries the same three facts on its manifest entry, and they mean the same
 * thing. A host stamps them on the manifest it distributes; a standalone run
 * stamps them from the declarations ({@link declaredFreshnessFacts}).
 */

/**
 * A table at an instant: within its window, or with none (`fresh`); past it
 * and allowed by `fallback=stale_ok` (`stale_ok`); past it otherwise, so
 * answered live (`stale`).
 */
export type Freshness = "fresh" | "stale_ok" | "stale";

/** The freshness facts of a stored table, as a manifest entry spells them. */
interface FreshnessFacts {
   dataAsOf?: string;
   freshnessWindowSeconds?: number;
   freshnessFallback?: "live" | "stale_ok" | "fail";
}

/**
 * Evaluate a table's facts at `at`. No declared window, a window without an
 * anchor, or an anchor that does not parse is never stale (a control plane
 * that sends none must not trip every query). Age equal to the window is
 * still fresh.
 */
export function freshnessOf(facts: FreshnessFacts, at: Date): Freshness {
   if (facts.freshnessWindowSeconds == null) return "fresh";
   if (facts.dataAsOf == null) return "fresh";
   const dataAsOfMs = Date.parse(facts.dataAsOf);
   if (Number.isNaN(dataAsOfMs)) return "fresh";
   const ageSeconds = (at.getTime() - dataAsOfMs) / 1000;
   if (ageSeconds <= facts.freshnessWindowSeconds) return "fresh";
   return facts.freshnessFallback === "stale_ok" ? "stale_ok" : "stale";
}

/** The `fallback` values a declaration may give. */
export const FRESHNESS_FALLBACKS = ["live", "stale_ok", "fail"] as const;
type FreshnessFallback = (typeof FRESHNESS_FALLBACKS)[number];

/** Whether `value` is a `fallback` a declaration may give. */
export function isFreshnessFallback(
   value: unknown,
): value is FreshnessFallback {
   return (
      typeof value === "string" &&
      (FRESHNESS_FALLBACKS as readonly string[]).includes(value)
   );
}

/** Seconds per unit of a window: `s`, `m`, `h`, `d`, `w`. */
const WINDOW_UNITS: Readonly<Record<string, number>> = {
   s: 1,
   m: 60,
   h: 3600,
   d: 86_400,
   w: 604_800,
};

/**
 * A declared `freshness.window` in seconds: a positive whole number and one
 * unit (`90s`, `30m`, `24h`, `7d`, `2w`). Undefined for any other text, which
 * a standalone run cannot age a table by.
 */
export function freshnessWindowSeconds(window: string): number | undefined {
   const match = /^\s*(\d+)\s*([smhdw])\s*$/.exec(window);
   if (!match) return undefined;
   const count = Number(match[1]);
   if (!Number.isSafeInteger(count) || count <= 0) return undefined;
   return count * WINDOW_UNITS[match[2]];
}

/**
 * Seconds per unit of a duration a `#@ persist` key takes (`lookback=`): the
 * window's letters, and each unit's word, singular or plural. No month or
 * year, whose length varies.
 */
const DURATION_UNITS: Readonly<Record<string, number>> = {
   ...WINDOW_UNITS,
   second: 1,
   seconds: 1,
   minute: 60,
   minutes: 60,
   hour: 3600,
   hours: 3600,
   day: 86_400,
   days: 86_400,
   week: 604_800,
   weeks: 604_800,
};

/** The units {@link durationSeconds} reads, for a message. */
export const DURATION_UNIT_TEXT =
   "seconds, minutes, hours, days or weeks (s, m, h, d or w)";

/**
 * A declared duration in seconds: a positive whole number and one unit,
 * written as a window is (`36h`, `2d`) or in words (`2 days`, `1 hour`,
 * `90 minutes`). Undefined for any other text. Lower case, as every
 * annotation value is case-sensitive.
 */
export function durationSeconds(text: string): number | undefined {
   const match = /^\s*(\d+)\s*([a-z]+)\s*$/.exec(text);
   if (!match || !Object.hasOwn(DURATION_UNITS, match[2])) return undefined;
   const count = Number(match[1]);
   if (!Number.isSafeInteger(count) || count <= 0) return undefined;
   return count * DURATION_UNITS[match[2]];
}

/** A declared freshness policy, as a plan source carries it. */
export interface DeclaredFreshness {
   window?: string;
   fallback?: FreshnessFallback;
}

/** The manifest fields a declaration stamps (no anchor: the run supplies it). */
export interface DeclaredFreshnessFacts {
   freshnessWindowSeconds?: number;
   freshnessFallback?: FreshnessFallback;
}

/**
 * The manifest facts the declarations of one table's sources stamp, or
 * undefined when none declares a window or a fallback. Several sources can
 * share one table (one content address): the narrowest window wins, so no
 * source's table is served past its own window, and `stale_ok` holds only
 * when every source that declares a fallback says so (`live` before `fail`
 * otherwise). A window that does not read as a duration stamps none.
 */
export function declaredFreshnessFacts(
   declarations: readonly (DeclaredFreshness | null | undefined)[],
): DeclaredFreshnessFacts | undefined {
   let window: number | undefined;
   const fallbacks = new Set<FreshnessFallback>();
   let declared = false;
   for (const d of declarations) {
      if (!d) continue;
      if (d.window !== undefined) {
         declared = true;
         const seconds = freshnessWindowSeconds(d.window);
         if (seconds !== undefined) {
            window = window === undefined ? seconds : Math.min(window, seconds);
         }
      }
      if (d.fallback !== undefined) {
         declared = true;
         fallbacks.add(d.fallback);
      }
   }
   if (!declared) return undefined;
   const fallback: FreshnessFallback | undefined = fallbacks.has("live")
      ? "live"
      : fallbacks.has("fail")
        ? "fail"
        : fallbacks.has("stale_ok")
          ? "stale_ok"
          : undefined;
   return {
      ...(window !== undefined ? { freshnessWindowSeconds: window } : {}),
      ...(fallback !== undefined ? { freshnessFallback: fallback } : {}),
   };
}
