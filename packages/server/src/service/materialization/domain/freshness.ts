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
