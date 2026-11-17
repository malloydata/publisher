// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The build protocol's types: the wire shapes (generated from the OpenAPI
 * spec into `api.ts`), the Malloy-facing build manifest, and the
 * freshness-aware manifest entry the serve path binds, with the few pure
 * readers of an entry (a legacy failed entry, a bound table's schema).
 *
 * Imported across the module and by the store, which re-exports them from
 * `storage/DatabaseInterface.ts`.
 *
 * Pure.
 */

import { components } from "../../../api";

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

/** Why a valid declaration is not built (`RefusedSource.reason`). */
export type RefusedReason = RefusedSource["reason"];

/**
 * Whether a run manifest entry records a FAILED source, in the deprecated
 * shape.
 *
 * Run manifests at rest (in `publisher.db` and bound manifest files) may hold a
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
    * The connection the physical table lives on, carried from the run
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
    * entry's `schema`), in DuckDB's spelling; absent for a table whose
    * build captured none. The served copy binds a v1 colocated
    * table with it (colocated_bindings.ts).
    */
   schema?: { name: string; type: string }[];
   /**
    * The colocated table's columns with their types as the warehouse's own
    * catalog names them (the wire entry's `nativeSchema`), which the served
    * copy casts a big-integer column to exactly (known limitation 12).
    */
   nativeSchema?: { name: string; type: string }[];
   /**
    * A `#(preaggregate)` rollup's key check (known limitation 29), as the
    * auto-run that wrote or carried this table recorded it: whether the rows
    * it grouped held its one grain dimension once per value. Internal, never
    * on the wire; absent (unknown) on every entry a host's manifest binds.
    */
   keyUniqueness?: KeyUniqueness;
}

/** See {@link FreshnessAwareManifestEntry.keyUniqueness}. */
export interface KeyUniqueness {
   /** The rollup's own key check: absent when it was not checked. */
   keyColumn?: string;
   unique?: boolean;
   /**
    * Each query source's primary key check the rollup's build ran, by the
    * source's sourceID: true when its key was held once per row and never
    * NULL. The join-to-rollup branch reads a join read as a join_many only for a
    * source known true here.
    */
   parentKeys?: Record<string, boolean>;
}

/**
 * One bound colocated table's columns as the bind read them from its
 * warehouse (DuckDB's spelling, as {@link FreshnessAwareManifestEntry.schema}),
 * or why they could not be read, in which case the table answers live.
 */
export type ObservedColumns =
   | {
        /**
         * Each column, `type` absent for one whose warehouse type Malloy
         * reads as no basic type (a struct, an array): never a drift.
         */
        columns: {
           name: string;
           type?: string;
           /** Its type as the warehouse's catalog names it (`numeric(10,2)`). */
           nativeType?: string;
        }[];
        /** The connection's dialect the types were read on. */
        dialect?: string;
     }
   | {
        /** Why, as a fixed class: what a package warning may say. */
        unread: UnreadReason;
        /**
         * The driver's own message, its connection's secrets redacted: for
         * the log alone, never the API.
         */
        detail?: string;
     };

/** Why the bind could not read a database table's columns. */
export type UnreadReason =
   | "connection_failed"
   | "timed_out"
   | "catalog_query_failed"
   | "not_found"
   | "ambiguous"
   | "unparseable"
   | "not_read";

/** Full (unfiltered) manifest map keyed by sourceEntityId. */
export type FreshnessManifest = Record<string, FreshnessAwareManifestEntry>;

/**
 * How loading an auto-run's manifest into its package went
 * (`Environment.loadRunManifest`): the step that failed, when one did.
 */
export type RunManifestLoad =
   | { loaded: true }
   | { loaded: false; step: "reload" | "bind"; error: string };

/** Why a table a run replaced is kept rather than reclaimed, and listed. */
export type KeptTableReason = "author_name" | "destination_missing";

/**
 * A table an auto-run replaced and Publisher keeps, listed on the run record
 * (`metadata.keptTables`) and raised as a package warning
 * (`table-kept-after-rename`) until the table is dropped or the name is
 * reused: one at an author's exact `name=` (`author_name`), which Publisher
 * never drops, or one in a storage destination that is no longer configured
 * (`destination_missing`), which it cannot reach.
 */
export interface KeptTable {
   sourceEntityId: string;
   sourceName?: string;
   /** The kept table. */
   physicalTableName: string;
   connectionName: string;
   storageDestinationName?: string;
   reason: KeptTableReason;
}
