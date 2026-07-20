/**
 * Per-query freshness gate for persisted sources (persistence.md §9.3, §14
 * Phase B). A query on a `#@ persist` source may use its materialized table
 * only while that table is within its declared freshness window; otherwise the
 * query does what the source's `fallback` says.
 *
 * Two settled design principles anchor this (do not re-derive them the easy
 * way):
 *
 *  1. Enforce per query, not at bind. Filtering stale entries once when the
 *     manifest is bound would let a table that crosses its window while the
 *     package stays loaded keep serving stale. The serve path re-evaluates
 *     freshness on every query.
 *  2. Clock on `dataAsOf`, not build completion. The window is a deadline on
 *     *data* age; the anchor is the artifact's data-as-of instant (snapshot
 *     start) supplied by the control plane. `age = now - dataAsOf`.
 *
 * Scope: `live`/`stale_ok` are fully publisher-side. `fail` degrades to
 * serve-live in this first cut (the entry is dropped like `live`); the airtight
 * `fail` path — which must *error* a query that joins a stale persist source —
 * is a gated forked-malloy follow-up and is not built here.
 */

import {
   BuildManifest,
   FreshnessAwareManifestEntry,
   FreshnessManifest,
} from "../storage/DatabaseInterface";

/**
 * What to do with a single manifest entry at query time.
 *  - `serve_table`: substitute the materialized table (include `{ tableName }`).
 *  - `serve_live`:  drop the entry so `strict:false` falls through to live SQL.
 *
 * `fail` collapses to `serve_live` in v1 (see module note).
 */
export type FreshnessDecision = "serve_table" | "serve_live";

/**
 * Evaluate one entry against `now`. An entry with no `freshnessWindowSeconds`
 * is un-gated (always serve the table). A fresh entry (age within window) is
 * served. A stale entry serves its table only under `stale_ok`; `live` and
 * `fail` both drop to live in this first cut.
 */
export function evaluateManifestFreshness(
   entry: FreshnessAwareManifestEntry,
   now: Date,
): FreshnessDecision {
   // Un-gated: no declared window ⇒ never stale. Also the rollout-safe default
   // before the control plane starts stamping the freshness fields.
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
