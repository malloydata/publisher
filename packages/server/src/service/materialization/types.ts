// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Materialization domain types: the build protocol's wire shapes (kept in sync
 * with the OpenAPI spec via the generated `api.ts`), the Malloy-facing build
 * manifest, and the freshness-aware manifest the serve path binds. They used to
 * live in `storage/DatabaseInterface.ts`, which still re-exports them for the
 * store; domain code imports them from here.
 */

import { components } from "../../api";

// Wire types for the build protocol.
export type MaterializationStatus =
   components["schemas"]["MaterializationStatus"];
export type BuildPlan = components["schemas"]["BuildPlan"];
export type BuildManifestResult = components["schemas"]["BuildManifest"];
export type ManifestEntry = components["schemas"]["ManifestEntry"];
export type BuildInstruction = components["schemas"]["BuildInstruction"];
export type ManifestReference = components["schemas"]["ManifestReference"];
export type Realization = components["schemas"]["Realization"];
/**
 * One incremental source's `covered_through` boundary as it travels on the
 * wire: reported on `ManifestEntry.ledger`, and returned verbatim in
 * `BuildInstructions.ledger` by a caller that owns the ledger. The store-row
 * counterpart is `IncrementalLedgerEntry` in storage/DatabaseInterface.ts.
 */
export type LedgerEntry = components["schemas"]["LedgerEntry"];

/** How a delta advances the serving table. */
export type IncrementalStrategy = "merge" | "range_replace";

/**
 * One source that failed to build, reported beside `entries` rather than inside
 * one.
 *
 * A failure carries the identifying fields it was instructed with, including the
 * `physicalTableName` it was HEADED for, so a table name here is not evidence a
 * table exists. Keeping these out of `entries` is what lets a consumer that
 * resolves an entry to a real table -- serve bindings, reuse decisions, seeds for
 * a downstream build -- do so without a per-entry check.
 */
export type SourceFailure = components["schemas"]["SourceFailure"];

/**
 * One persist source a run did not attempt because the eligibility gate refused
 * it. The same shape the build plan reports under `refusedSources`; auto-run
 * records its refusals in the run's `metadata.refusedSources`, keyed by sourceID
 * because a refused source may have no content address to key it by.
 */
export type RefusedSource = components["schemas"]["RefusedSource"];

/**
 * Whether a manifest entry records a FAILED source, in the deprecated shape.
 *
 * Manifests at rest (in `publisher.db` and bound manifest files) may hold a
 * failed source INTO `entries`, and this build still mirrors failures there.
 * Its `physicalTableName` names a table the build never created; with stable
 * auto-run names (`selfAssignTableName`) that is the PREVIOUS generation's real
 * table, which would be served as though fresh.
 *
 * Applied at the READ boundaries a persisted manifest enters through:
 * `splitManifestEntries` and `getMostRecentManifestEntries`. Delete this with
 * `ManifestEntry.error`.
 */
export function isLegacyFailedEntry(entry: { error?: string | null }): boolean {
   return Boolean(entry?.error);
}

/**
 * Malloy-facing build manifest: maps a sourceEntityId to the physical table backing
 * that persist source. This is the shape the Malloy runtime consumes when
 * (re)loading models so persist references resolve to materialized tables.
 * Distinct from {@link BuildManifestResult} (the wire build output, which also
 * carries caller bookkeeping like materializedTableId and rowCount).
 */
export interface BuildManifestEntry {
   tableName: string;
}

export interface BuildManifest {
   entries: Record<string, BuildManifestEntry>;
   strict?: boolean;
}

/**
 * A wire manifest entry carrying the control-plane freshness fields alongside
 * the physical `tableName`. This is what the publisher retains at bind so the
 * serve path can re-evaluate `age vs window` per query (see
 * {@link ../freshness}). A `{ tableName }`-only value is structurally
 * assignable here (the freshness fields are optional), so callers that only
 * know a table name — e.g. the post-build auto-load — bind un-gated entries.
 *
 *  - `dataAsOf`   the artifact's data-as-of instant (snapshot start); the age
 *                 anchor. `age = now - dataAsOf`. Absent ⇒ never stale.
 *  - `freshnessWindowSeconds`  the version's effective window. Absent ⇒ un-gated.
 *  - `freshnessFallback`       per-version read-time policy when stale.
 */
export interface FreshnessAwareManifestEntry {
   tableName: string;
   /**
    * The connection the physical table lives on, carried from the wire
    * manifest so the bind step can quote `tableName` for that connection's
    * dialect (see Package.quoteBoundTableNames). Absent on entries whose
    * producer didn't record it; those bind verbatim.
    */
   connectionName?: string;
   dataAsOf?: string;
   freshnessWindowSeconds?: number;
   freshnessFallback?: "live" | "stale_ok" | "fail";
   /**
    * The colocated table's columns as its build captured them (the wire
    * entry's `schema`), in DuckDB's spelling; absent for a table built
    * before the v1 cutover captured one. The served copy binds a v1 colocated
    * table with it (colocated_bindings.ts).
    */
   schema?: { name: string; type: string }[];
}

/**
 * A wire entry's captured `schema` as a bound entry carries it: only columns
 * with both a name and a type, and undefined when none is left.
 */
export function boundSchemaOf(
   schema: readonly { name?: string; type?: string }[] | undefined,
): { schema: { name: string; type: string }[] } | Record<string, never> {
   const columns = (schema ?? []).flatMap((c) =>
      typeof c.name === "string" && typeof c.type === "string"
         ? [{ name: c.name, type: c.type }]
         : [],
   );
   return columns.length > 0 ? { schema: columns } : {};
}

/** Full (unfiltered) manifest map keyed by sourceEntityId. */
export type FreshnessManifest = Record<string, FreshnessAwareManifestEntry>;
