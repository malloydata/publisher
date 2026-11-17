// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What serving may read, and one query's fixed view of it.
 *
 * A {@link Binding} is one stored table as serving reads it, under one source
 * name: its kind and tier, carried from the plan, never inferred from a name.
 * A {@link Snapshot} is every binding a package serves, with the colocated
 * manifest entries Malloy's own build-manifest substitution reads (the
 * colocated rollup and view legs). It is immutable and replaced by one
 * reference swap; a cache keyed by a snapshot's `bindings` array is
 * invalidated exactly when the bindings change.
 *
 * A query pins the snapshot once, before routing ({@link pin}): freshness is
 * evaluated there and nowhere else, so every leg, the run and the path report
 * read the same tables at the same instant, and a rebind mid-query cannot
 * split it. The pinned compiler manifest is never undefined.
 *
 * Made by `serve/bind.ts` and the facade (the snapshot), and, for a chained
 * `storage=` build's upstreams, by the run ({@link deriveServeBindings});
 * pinned by `Model` once per query; read by the three legs, the query
 * pipeline and the path report.
 *
 * Pure: the caller passes the instant.
 */

import { freshnessOf, type Freshness } from "./freshness";
import { storageTablePath } from "./storage_table_path";
import type { BuildManifest, FreshnessManifest, ManifestEntry } from "./types";

/** What a stored table holds, from the plan: never read off a name. */
export type BindingKind =
   | "persist"
   | "materialize"
   | "rollup"
   | "view"
   | "query";

/** Where a stored table lives: the source's own warehouse, or a storage destination. */
type BindingTier = "colocated" | "storage";

/**
 * One stored table as serving reads it, under one source name.
 *
 * A `storage` table is read through `virtual()`, by its destination's alias
 * and {@link virtualHandle}; a `colocated` one (a `#(materialize)` or v1
 * `#@ persist` table in the source's own warehouse) is named in the served
 * copy's text on the connection {@link destinationName} names.
 */
export interface Binding {
   readonly kind: BindingKind;
   readonly tier: BindingTier;
   /** The Malloy source name it rebinds (`source: <sourceName> is ...`). */
   sourceName: string;
   /**
    * The sourceIDs of this package's own sources the table stores, at this
    * content address, from this server's own compile. A model reads a
    * binding only for its own source of that name carrying one of them, so
    * an unrelated same-named source elsewhere is never served from it.
    * Absent, as on a hand-made binding, the name alone decides.
    */
   sourceIDs?: string[];
   /**
    * What the run that wrote or carried the table found of each stored
    * source's primary key, by sourceID: true when no value repeats and none
    * is NULL. Internal: absent on every binding a host's manifest makes.
    */
   primaryKeys?: Record<string, boolean>;
   /**
    * A stored rollup's own key check (known limitation 29), as the run that
    * wrote or carried the table found it: whether its base held the grain's
    * one dimension once per row. Internal, as {@link primaryKeys} is.
    */
   rollupKey?: { keyColumn: string; unique: boolean };
   /**
    * A `storage=` table plan admitted past a stored source's own row-level
    * gate under G3 (`SourceEligibility.gateKept`), marked under every name
    * it is bound by: the router lets a served answer reading it past an
    * entry-point gate, as it does a colocated table's. Set by the bind;
    * absent reads as not kept (fail closed).
    */
   ownGateKept?: boolean;
   /**
    * A database table's columns the bind found a DuckLake catalog holding as
    * a DOUBLE (a 128-bit integer, `storedAsDouble`), each read as the DOUBLE
    * it is: a row-level gate over one is not read from the table
    * (`domain/gate_policy.ts`, `gateColumnTypes`). Set by the bind.
    */
   storedAsDouble?: string[];
   /**
    * The storage destination the table lives in; for a colocated table, the
    * source's own connection.
    */
   destinationName: string;
   /** The `virtual()` handle: the table's content address. */
   virtualHandle: string;
   /**
    * The table's path: unquoted for a storage table (quoted for DuckDB when
    * read); canonical for its connection's dialect for a colocated one.
    */
   tablePath: string;
   /** The stored columns as the build captured them. */
   schema: { name: string; type: string }[];
   /**
    * A colocated table's columns with the warehouse's own types
    * (`ManifestEntry.nativeSchema`): a big-integer column is cast to exactly
    * its type. Absent for a storage table, whose schema is native.
    */
   nativeSchema?: { name: string; type: string }[];
   /** The data-as-of instant the freshness window is aged from. */
   freshAsOf?: string;
   freshnessWindowSeconds?: number;
   freshnessFallback?: "live" | "stale_ok" | "fail";
}

/** Whether a binding is a colocated table. */
export function isColocated(b: Pick<Binding, "tier">): boolean {
   return b.tier === "colocated";
}

/**
 * Every table a package serves, fixed. `entries` are the colocated run
 * manifest entries, freshness facts and key checks included, that Malloy's
 * compiler manifest substitutes; `bindings` are the tables the served copy and the
 * `storage=` legs read.
 */
export interface Snapshot {
   readonly bindings: readonly Binding[];
   readonly entries: Readonly<FreshnessManifest>;
}

/**
 * A snapshot of `bindings` and `entries`. Pass the previous snapshot's
 * `bindings` array when they did not change, so caches keyed by it survive.
 */
export function makeSnapshot(
   bindings: readonly Binding[],
   entries: Readonly<FreshnessManifest> = {},
): Snapshot {
   return Object.freeze({ bindings, entries });
}

/** Nothing bound: every query is answered live. */
export const EMPTY_SNAPSHOT: Snapshot = makeSnapshot(
   Object.freeze([]) as readonly Binding[],
   Object.freeze({}),
);

/**
 * A stored table at the pin's instant: within its window (or with none),
 * past it and allowed (`stale_ok`), past it and answered live (its binding
 * kept for what a report says of it), or not bound.
 */
type TableLookup =
   | { state: "fresh"; binding: Binding }
   | { state: "stale_ok"; binding: Binding }
   | { state: "stale"; binding: Binding }
   | { state: "not_built" };

/** One query's fixed view of a snapshot. */
export interface Pinned {
   readonly snapshot: Snapshot;
   readonly asOf: Date;
   /** The bindings a query may read at {@link asOf}: fresh, or stale and allowed. */
   readonly fresh: readonly Binding[];
   /**
    * The colocated entries a query may read at {@link asOf}, as Malloy's
    * build manifest: never undefined, empty when nothing is bound.
    */
   readonly compilerManifest: BuildManifest;
   /** Whether any colocated entry is bound at all, fresh or not. */
   readonly hasEntries: boolean;
   /** Whether `binding` may be read at {@link asOf}. */
   serves(binding: Binding): boolean;
   /** The binding of `kind` storing `sourceID`, at {@link asOf}. */
   tableFor(sourceID: string, kind: BindingKind): TableLookup;
}

/**
 * The `storage=` table a rollup, persisted view or persisted query is read
 * from, or undefined when its tier is colocated. The binding decides the tier,
 * not the annotation: a host may instruct a source with no `storage=` into a
 * storage destination (a database origin whose destination the control plane
 * chose), and the run's manifest then binds it as a storage table, which is
 * read exactly as a `storage=` annotation's is. With no storage binding, a
 * source annotated `storage=` is still a storage table, not built yet.
 */
export function storageLookupOf(
   pinned: Pinned,
   sourceID: string | undefined,
   kind: BindingKind,
   annotatedStorage: boolean,
): TableLookup | undefined {
   const lookup =
      sourceID === undefined
         ? ({ state: "not_built" } as const)
         : pinned.tableFor(sourceID, kind);
   if (lookup.state !== "not_built" && lookup.binding.tier === "storage") {
      return lookup;
   }
   return annotatedStorage ? lookup : undefined;
}

/** The freshness facts of a binding, as the rule reads them. */
function factsOf(b: Binding) {
   return {
      dataAsOf: b.freshAsOf,
      freshnessWindowSeconds: b.freshnessWindowSeconds,
      freshnessFallback: b.freshnessFallback,
   };
}

/** Whether a table in `state` is read (fresh, or stale and allowed). */
function readable(state: Freshness): boolean {
   return state !== "stale";
}

/**
 * Pin `snapshot` at `asOf`: the one place freshness is evaluated for a query.
 * Evaluated lazily and once each.
 */
export function pin(snapshot: Snapshot, asOf: Date): Pinned {
   const states = new Map<Binding, Freshness>();
   const stateOf = (b: Binding): Freshness => {
      let state = states.get(b);
      if (state === undefined) {
         state = freshnessOf(factsOf(b), asOf);
         states.set(b, state);
      }
      return state;
   };
   let fresh: readonly Binding[] | undefined;
   let compilerManifest: BuildManifest | undefined;
   return Object.freeze({
      snapshot,
      asOf,
      get fresh() {
         fresh ??= Object.freeze(
            snapshot.bindings.filter((b) => readable(stateOf(b))),
         );
         return fresh;
      },
      get compilerManifest() {
         if (!compilerManifest) {
            const entries: BuildManifest["entries"] = {};
            for (const [id, entry] of Object.entries(snapshot.entries)) {
               if (readable(freshnessOf(entry, asOf))) {
                  entries[id] = { tableName: entry.tableName };
               }
            }
            compilerManifest = Object.freeze({
               entries: Object.freeze(entries),
               strict: false,
            }) as BuildManifest;
         }
         return compilerManifest;
      },
      hasEntries: Object.keys(snapshot.entries).length > 0,
      serves: (b: Binding) => readable(stateOf(b)),
      tableFor(sourceID: string, kind: BindingKind): TableLookup {
         const stored = snapshot.bindings.filter(
            (b) => b.kind === kind && (b.sourceIDs ?? []).includes(sourceID),
         );
         if (stored.length === 0) return { state: "not_built" };
         for (const binding of stored) {
            const state = stateOf(binding);
            if (state !== "stale") return { state, binding };
         }
         return { state: "stale", binding: stored[0] };
      },
   });
}

/**
 * Whether a model may serve `binding`: false only when the model's own source
 * of that name carries a sourceID the binding does not name, an unrelated
 * source that happens to share the name. A binding with no `sourceIDs`, a
 * rollup (its name is generated and names nothing in the author's model), and
 * a name the model does not define are left to the checks downstream.
 */
export function bindingServesModelSource(
   binding: Binding,
   contents: Record<string, { sourceID?: unknown } | undefined> | undefined,
): boolean {
   if (binding.kind === "rollup" || !binding.sourceIDs) return true;
   const sourceID = contents?.[binding.sourceName]?.sourceID;
   if (typeof sourceID !== "string" || sourceID.length === 0) return true;
   return binding.sourceIDs.includes(sourceID);
}

/**
 * The primary key checks a local run recorded on a stored entry (its own
 * source's, by sourceID): internal, set only by the committing run or a
 * restore from the store, never by a host's manifest.
 */
export function primaryKeysOf(entry: {
   keyUniqueness?: { parentKeys?: unknown };
}): Record<string, boolean> | undefined {
   const keys = entry.keyUniqueness?.parentKeys;
   if (!keys || typeof keys !== "object") return undefined;
   const out = Object.fromEntries(
      Object.entries(keys).filter(([, v]) => typeof v === "boolean"),
   ) as Record<string, boolean>;
   return Object.keys(out).length > 0 ? out : undefined;
}

/** A rollup's own key check a local run recorded on a stored entry, as {@link primaryKeysOf}. */
function rollupKeyOf(entry: {
   keyUniqueness?: { keyColumn?: unknown; unique?: unknown };
}): Binding["rollupKey"] {
   const k = entry.keyUniqueness;
   return typeof k?.keyColumn === "string" && typeof k.unique === "boolean"
      ? { keyColumn: k.keyColumn, unique: k.unique }
      : undefined;
}

/**
 * The `storage=` bindings a run's manifest entries make: one per name that
 * reads each table (its builder, then its aliases), each with the plan's
 * sourceIDs for it. An entry with no complete column binds nothing. Each is
 * a `rollup` when its entry says so (a manifest travels without its plan),
 * else `persist`; the facade reads the rest of a binding's kind off the plan.
 */
export function deriveServeBindings(
   entries: Record<string, ManifestEntry>,
   aliasesBySourceName: Record<string, string[]>,
   /** A destination's schema by name (see storageTablePath); absent, or answering undefined, is `main`. */
   destinationSchema?: (destinationName: string) => string | undefined,
   /** The package's own sourceIDs for a name at a content address (`planSourceIds`). */
   sourceIDsFor?: (sourceName: string, sourceEntityId: string) => string[],
): Binding[] {
   const bindings: Binding[] = [];
   // Names that OWN a table. An alias never claims one, or the name would be
   // bound twice and declared twice in one served copy.
   const builders = new Set(
      Object.values(entries)
         .map((entry) => entry.sourceName)
         .filter((name): name is string => !!name),
   );
   for (const entry of Object.values(entries)) {
      if (
         !entry.sourceName ||
         !entry.storageDestinationName ||
         !entry.physicalTableName
      ) {
         continue;
      }
      // The wire Column has optional name/type; keep only complete columns.
      const schema = (entry.schema ?? [])
         .filter((c) => c.name && c.type)
         .map((c) => ({ name: c.name as string, type: c.type as string }));
      if (schema.length === 0) continue;
      // Builder first so it wins any ordering downstream; deduplicated because
      // the builder is normally in its own alias group.
      const names = [
         entry.sourceName,
         ...(aliasesBySourceName[entry.sourceName] ?? []).filter(
            (name) => !builders.has(name),
         ),
      ].filter((name, i, all) => all.indexOf(name) === i);
      const primaryKeys = primaryKeysOf(entry as never);
      const rollupKey =
         entry.origin === "preaggregate"
            ? rollupKeyOf(entry as never)
            : undefined;
      for (const sourceName of names) {
         // Set whenever there is a plan to read, even empty: an empty list
         // names no source, so the binding serves nothing (planSourceIds).
         const sourceIDs = sourceIDsFor?.(sourceName, entry.sourceEntityId);
         bindings.push({
            kind: entry.origin === "preaggregate" ? "rollup" : "persist",
            tier: "storage",
            sourceName,
            ...(sourceIDs !== undefined ? { sourceIDs } : {}),
            destinationName: entry.storageDestinationName,
            virtualHandle: entry.sourceEntityId,
            // Qualified with the attach alias: unqualified, it would resolve
            // against the serve session's default catalog.
            tablePath: storageTablePath(
               entry.storageDestinationName,
               entry.physicalTableName,
               destinationSchema?.(entry.storageDestinationName),
            ),
            schema,
            freshAsOf: entry.dataAsOf,
            freshnessWindowSeconds: entry.freshnessWindowSeconds,
            freshnessFallback: entry.freshnessFallback,
            ...(primaryKeys ? { primaryKeys } : {}),
            ...(rollupKey ? { rollupKey } : {}),
         });
      }
   }
   return bindings;
}
