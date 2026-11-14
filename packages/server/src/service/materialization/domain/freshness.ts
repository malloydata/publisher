// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Whether a persisted source's stored table is fresh enough to read at an
 * instant, and what a query does when it is not (the source's `fallback`).
 *
 * Two invariants:
 *  1. Evaluated per query, never at bind: a table that crosses its window
 *     while the package stays loaded must stop serving.
 *  2. Aged from `dataAsOf` (the snapshot start the host supplies), not from
 *     build completion: the window bounds the data's age.
 *
 * `fail` degrades to answering live, as `live` does: erroring a query that
 * joins a stale source is not implemented.
 *
 * Pure: the caller passes the instant.
 */

import {
   BuildManifest,
   FreshnessAwareManifestEntry,
   FreshnessManifest,
} from "./types";

/**
 * What to do with a single manifest entry at query time.
 *  - `serve_table`: substitute the materialized table (include `{ tableName }`).
 *  - `serve_live`:  drop the entry so `strict:false` falls through to live SQL.
 * `fail` collapses to `serve_live` (see module note).
 */
type FreshnessDecision = "serve_table" | "serve_live";

/**
 * Evaluate one entry against `now`. An entry with no `freshnessWindowSeconds`
 * is un-gated (always serve the table). A fresh entry (age within window) is
 * served. A stale entry serves its table only under `stale_ok`; `live` and
 * `fail` both drop to live.
 */
export function evaluateManifestFreshness(
   entry: FreshnessAwareManifestEntry,
   now: Date,
): FreshnessDecision {
   // No declared window (or a control plane that sends none) ⇒ never stale.
   if (entry.freshnessWindowSeconds == null) return "serve_table";
   // A window without an anchor can't be evaluated; treat as un-gated rather
   // than tripping every query (matches "absent ⇒ never stale").
   if (entry.dataAsOf == null) return "serve_table";
   const dataAsOfMs = Date.parse(entry.dataAsOf);
   if (Number.isNaN(dataAsOfMs)) return "serve_table";
   const ageSeconds = (now.getTime() - dataAsOfMs) / 1000;
   const stale = ageSeconds > entry.freshnessWindowSeconds;
   if (!stale) return "serve_table";
   return entry.freshnessFallback === "stale_ok" ? "serve_table" : "serve_live";
}

/**
 * The instant a currently-fresh, gated entry will cross its window
 * (`dataAsOf + window`), or `null` when the entry has no evaluable window
 * (un-gated). Used to compute the next memo-invalidation instant.
 */
function staleSinceMs(entry: FreshnessAwareManifestEntry): number | null {
   if (entry.freshnessWindowSeconds == null || entry.dataAsOf == null) {
      return null;
   }
   const dataAsOfMs = Date.parse(entry.dataAsOf);
   if (Number.isNaN(dataAsOfMs)) return null;
   return dataAsOfMs + entry.freshnessWindowSeconds * 1000;
}

/**
 * Compute the freshness-filtered Malloy build manifest for `now` from the full
 * retained wire entries.
 *
 * Returns the `sourceEntityId -> { tableName }` map of entries that should
 * substitute their materialized table (fresh, un-gated, or stale-`stale_ok`),
 * plus `nextStaleSince`: the earliest future instant a currently-included,
 * gated entry will cross its window. Because age only ever grows, the
 * fresh→stale transition is one-way, so a single `nextStaleSince` instant is a
 * sufficient memo-invalidation deadline — no per-entry heap is needed. `null`
 * means no included entry has an evaluable window, so the result never changes
 * until the entries are replaced (rebind).
 */
export function filterFreshManifest(
   entries: FreshnessManifest,
   now: Date,
): { manifest: BuildManifest["entries"]; nextStaleSince: number | null } {
   const manifest: BuildManifest["entries"] = {};
   let nextStaleSince: number | null = null;
   const nowMs = now.getTime();

   for (const [sourceEntityId, entry] of Object.entries(entries)) {
      if (evaluateManifestFreshness(entry, now) === "serve_live") continue;
      manifest[sourceEntityId] = { tableName: entry.tableName };

      // Only currently-fresh gated entries can flip later; a kept stale
      // `stale_ok` entry (staleSince already in the past) never changes again.
      const flipAt = staleSinceMs(entry);
      if (flipAt != null && flipAt > nowMs) {
         nextStaleSince =
            nextStaleSince == null ? flipAt : Math.min(nextStaleSince, flipAt);
      }
   }

   return { manifest, nextStaleSince };
}
