// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What serving may read, and one query's fixed view of it.
 *
 * A {@link Binding} is one stored table as serving reads it, under one source
 * name: its kind and tier, carried from the plan, never inferred from a name.
 * A {@link Snapshot} is every binding a package serves, with the database
 * manifest entries Malloy's own build-manifest substitution reads (the
 * database rollup and view legs). It is immutable and replaced by one
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

import type { TableKind, Tier } from "./vocabulary";
import { isRollupOrigin } from "./table_plans";
import { freshnessOf, type Freshness } from "./freshness";
import { storageTablePath } from "./storage_table_path";
import type {
   BuildManifest,
   FreshnessAwareManifestEntry,
   FreshnessManifest,
   ManifestEntry,
} from "./types";
import type { ColumnContract } from "./column_contract";

/** What a stored table holds, from the plan: never read off a name. */
export type BindingKind = Exclude<TableKind, "pair">;

/** Where a stored table lives: the source's own warehouse, or a storage destination. */
type BindingTier = Tier;

/**
 * One stored table as serving reads it, under one source name.
 *
 * A `storage` table is read through `virtual()`, by its destination's alias
 * and {@link virtualHandle}; a `database` one (a `#(materialize)` or v1
 * `#@ persist` table in the source's own warehouse) is named in the served
 * copy's text on the connection {@link destinationName} names.
 */
export interface Binding {
   readonly kind: BindingKind;
   /**
    * Its stored relation reads the current time, so it holds the rows as of
    * its build (`PackagePlan.asOfBuild`); see `TableRead.asOfBuild`.
    */
   readonly asOfBuild?: boolean;
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
   /** The key column each of {@link primaryKeys} checked, by sourceID. */
   primaryKeyColumns?: Record<string, string>;
   /**
    * A stored rollup's own key check, as the run that
    * wrote or carried the table found it: whether its base held the grain's
    * one dimension once per row. Internal, as {@link primaryKeys} is.
    */
   rollupKey?: { keyColumn: string; unique: boolean };
   /**
    * A `storage=` table plan admitted past a stored source's own row-level
    * gate under own-gate keeping (`SourceEligibility.gateKept`), marked under
    * every name it is bound by: the router lets a served answer reading it past
    * an entry-point gate, as it does a database table's. Set by the bind;
    * absent reads as not kept (fail closed).
    */
   ownGateKept?: boolean;
   /**
    * A `storage=` table every source stored in it holds only ungated rows
    * (`SourceEligibility.ungated`: no gate beneath any of them, joins and
    * derivations included), marked under every name it is bound by: the
    * router lets an answer reading it past a gated entry point's gate, as it
    * does a database table's, and `govern()` grafts and proves that gate on
    * the answer (the ungated-rows rule). Set by the bind; absent reads as not
    * proven (fail closed).
    */
   holdsOnlyUngatedRows?: boolean;
   /**
    * A database table's columns whose type the bind found moved since its
    * build, each with the type the warehouse holds it as now (its native
    * type when known): a column altered in the warehouse, and a 128-bit
    * integer a DuckLake catalog holds as a DOUBLE (`storedAsDouble`). A
    * row-level gate over one is judged by that type, so one now stored
    * inexactly is not read from the table (`domain/gate_policy.ts`,
    * `gateColumnTypes`). Set by the bind.
    */
   retypedColumns?: { name: string; type: string }[];
   /**
    * The storage destination the table lives in; for a database table, the
    * source's own connection.
    */
   destinationName: string;
   /** The `virtual()` handle: the table's content address. */
   virtualHandle: string;
   /**
    * The table's path: unquoted for a storage table (quoted for DuckDB when
    * read); canonical for its connection's dialect for a database one.
    */
   tablePath: string;
   /** The stored columns as the build captured them. */
   schema: { name: string; type: string }[];
   /**
    * A database table's columns with the warehouse's own types
    * (`ManifestEntry.nativeSchema`): a big-integer column is cast to exactly
    * its type. Absent for a storage table, whose schema is native.
    */
   nativeSchema?: { name: string; type: string }[];
   /** The data-as-of instant the freshness window is aged from. */
   freshAsOf?: string;
   freshnessWindowSeconds?: number;
   freshnessFallback?: "live" | "stale_ok" | "fail";
}

/** Whether a binding is a database table. */
export function isDatabase(b: Pick<Binding, "tier">): boolean {
   return b.tier === "database";
}

/**
 * Every table a package serves, fixed. `entries` are the database run
 * manifest entries, freshness facts and key checks included, that Malloy's
 * compiler manifest substitutes; `bindings` are the tables the served copy and the
 * `storage=` legs read.
 */
/**
 * A database table read by Malloy's build-manifest substitution, altered
 * outside its build (its entry's `heldRead`): read instead as a `storage=`
 * table is, through a typed `virtual()` source over the columns it holds now,
 * on its own connection. Its handle is its content address. Undefined when
 * the bind recorded no `heldRead`.
 *
 * The rollup leg hands this binding to the gate policy (`gateTablesRead`),
 * so a gate over a held table is judged on it. It does not set
 * `retypedColumns` (a binding's columns retyped since the build, with the
 * type held now, which the gate policy's `gateColumnTypes` judges a gate
 * column by): that field is not on `Binding` here. The held read's `moved`
 * columns are what it would carry (`{ name, type: nativeType ?? type }`),
 * and whichever change lands second wires the two together. Until then a
 * moved column is read as it is only on a lossless widening, so a gate over
 * a held column never compares a rounded value.
 */
export function heldReadBinding(
   kind: BindingKind,
   sourceEntityId: string,
   entry: FreshnessAwareManifestEntry,
): Binding | undefined {
   const held = entry.heldRead;
   if (!held) return undefined;
   const nativeSchema = held.columns.flatMap((c) =>
      c.nativeType !== undefined ? [{ name: c.name, type: c.nativeType }] : [],
   );
   // What the gate policy judges a gate over a moved column by: the type it
   // holds now, as the bind records for any other drifted database table.
   const retypedColumns = held.columns.flatMap((c) =>
      c.moved ? [{ name: c.name, type: c.nativeType ?? c.type }] : [],
   );
   return {
      kind,
      tier: "database",
      sourceName: sourceEntityId,
      destinationName: held.connectionName,
      virtualHandle: sourceEntityId,
      tablePath: entry.tableName,
      schema: held.columns.map(({ name, type }) => ({ name, type })),
      ...(nativeSchema.length > 0 ? { nativeSchema } : {}),
      ...(retypedColumns.length > 0 ? { retypedColumns } : {}),
   };
}

export interface Snapshot {
   readonly bindings: readonly Binding[];
   readonly entries: Readonly<FreshnessManifest>;
   /**
    * Content addresses whose tables are bound but not served: a failed
    * reload found what they were built from changed upstream. A pin leaves
    * their entries out of the compiler's manifest and their bindings out of
    * {@link Pinned.tableFor}; the served copy keeps them (its files no longer
    * compile against the refetched schemas, so it must not recompile) and
    * the router declines an answer that reads one (`query_serving.ts`), so
    * their queries answer live.
    */
   readonly withdrawn?: ReadonlySet<string>;
   /**
    * The key Malloy files each address's table under, where it is not the
    * address (`PackagePlan.compilerKeys`): what {@link underCompilerKeys}
    * files a manifest handed to Malloy under.
    */
   readonly compilerKeys?: Readonly<Record<string, string>>;
   /**
    * Each served table's column contract, as its build recorded it and the
    * bind held its table to, by content address: what read admission looks
    * a stored column's collation up in (`serve/admission_lookup.ts`).
    */
   readonly contracts?: ReadonlyMap<string, ColumnContract>;
}

/**
 * A snapshot of `bindings` and `entries`. Pass the previous snapshot's
 * `bindings` array when they did not change, so caches keyed by it survive.
 */
export function makeSnapshot(
   bindings: readonly Binding[],
   entries: Readonly<FreshnessManifest> = {},
   withdrawn?: ReadonlySet<string>,
   compilerKeys?: Readonly<Record<string, string>>,
   contracts?: ReadonlyMap<string, ColumnContract>,
): Snapshot {
   return Object.freeze({
      bindings,
      entries,
      ...(withdrawn && withdrawn.size > 0 ? { withdrawn } : {}),
      ...(compilerKeys && Object.keys(compilerKeys).length > 0
         ? { compilerKeys }
         : {}),
      ...(contracts && contracts.size > 0 ? { contracts } : {}),
   });
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
    * The database entries a query may read at {@link asOf}, as Malloy's
    * build manifest: never undefined, empty when nothing is bound.
    */
   readonly compilerManifest: BuildManifest;
   /** Whether any database entry is bound at all, fresh or not. */
   readonly hasEntries: boolean;
   /** Whether `binding` may be read at {@link asOf}. */
   serves(binding: Binding): boolean;
   /** The binding of `kind` storing `sourceID`, at {@link asOf}. */
   tableFor(sourceID: string, kind: BindingKind): TableLookup;
}

/**
 * The `storage=` table a rollup, persisted view or persisted query is read
 * from, or undefined when it is in the source's own database. The binding decides the tier,
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
   const withdrawn = snapshot.withdrawn;
   const servable = (b: Binding): boolean => !withdrawn?.has(b.virtualHandle);
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
               if (!withdrawn?.has(id) && readable(freshnessOf(entry, asOf))) {
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
            (b) =>
               servable(b) &&
               b.kind === kind &&
               (b.sourceIDs ?? []).includes(sourceID),
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
 * Each content address paired with the key Malloy files its table under
 * (`PackagePlan.compilerKeys`, where the two differ), in plan order.
 */
type CompilerKeys =
   | Readonly<Record<string, string>>
   | Iterable<readonly [address: string, key: string]>;

function pairsOf(keys: CompilerKeys): Iterable<readonly [string, string]> {
   return Symbol.iterator in keys
      ? (keys as Iterable<readonly [string, string]>)
      : Object.entries(keys as Readonly<Record<string, string>>);
}

/**
 * Which address fills each compiler key {@link underCompilerKeys} files:
 * the first address, in plan order, with an entry, whose key is neither
 * its own address, `claimed`, nor already a key of `entries`.
 */
function filings(
   entries: Readonly<Record<string, unknown>>,
   keys: CompilerKeys,
   claimed: ReadonlySet<string>,
): Map<string, string> {
   const filed = new Map<string, string>();
   for (const [address, key] of pairsOf(keys)) {
      if (
         key === address ||
         claimed.has(key) ||
         !(address in entries) ||
         key in entries ||
         filed.has(key)
      ) {
         continue;
      }
      filed.set(key, address);
   }
   return filed;
}

/**
 * `manifest`, keyed by Publisher's content addresses, as Malloy must see
 * it: each entry also under the key Malloy files its table under, so
 * Malloy's own lookup finds it (an address folds in more than Malloy's key
 * does: a destination, a `name=`, the upstream column types). Sources
 * folded apart share one key; the first in plan order with an entry fills
 * it, unless the key is an entry's own address or `claimed` (a key the
 * caller decided itself). Every manifest handed to a Malloy runtime, a
 * compile or a package load passes through here; Publisher's own reads of
 * a manifest (freshness, the path report, verdicts) stay on addresses.
 * Unchanged when nothing is filed.
 */
export function underCompilerKeys<M extends BuildManifest | undefined>(
   manifest: M,
   keys: CompilerKeys | undefined,
   claimed: ReadonlySet<string> = new Set(),
): M {
   if (!manifest || !keys) return manifest;
   const filed = filings(manifest.entries, keys, claimed);
   if (filed.size === 0) return manifest;
   const entries = { ...manifest.entries };
   for (const [key, address] of filed) entries[key] = entries[address];
   return { ...manifest, entries };
}

/**
 * The manifest entries a run over {@link underCompilerKeys}`(manifest, keys)`
 * read, by address: a read of a key it filed is a read of the address that
 * filled it.
 */
export function readsUnderCompilerKeys(
   read: ReadonlySet<string>,
   manifest: BuildManifest | undefined,
   keys: CompilerKeys | undefined,
): ReadonlySet<string> {
   if (!manifest || !keys || read.size === 0) return read;
   const filed = filings(manifest.entries, keys, new Set());
   if (![...read].some((k) => filed.has(k))) return read;
   return new Set([...read].map((k) => filed.get(k) ?? k));
}

/**
 * A database table Malloy files under a key that is not its address: sources
 * of one build placed in two tables (foldDistinctTableNames) each have an
 * address of their own, and Malloy files every one under the build's
 * `buildId` (PlannedTable.compilerKey).
 */
export interface KeyedTable {
   readonly tableId: string;
   readonly compilerKey: string;
}

/**
 * `manifest` with `table`'s entry also under its compiler key, so a compile
 * that reads `table`'s source substitutes `table`, not a twin filed under the
 * same key. Unchanged when `manifest` holds no entry at `table`'s address.
 */
export function manifestUnderCompilerKey<M extends BuildManifest | undefined>(
   manifest: M,
   table: KeyedTable | undefined,
): M {
   const own = table ? manifest?.entries[table.tableId] : undefined;
   if (!manifest || !table || !own) return manifest;
   return {
      ...manifest,
      entries: { ...manifest.entries, [table.compilerKey]: own },
   };
}

/**
 * The manifest entries a run read, by address: a read under `table`'s
 * compiler key is a read of `table`, since {@link manifestUnderCompilerKey}
 * put its entry there.
 */
export function readsByAddress(
   read: ReadonlySet<string>,
   table: KeyedTable | undefined,
): ReadonlySet<string> {
   if (!table || !read.has(table.compilerKey)) return read;
   const byAddress = new Set(read);
   byAddress.delete(table.compilerKey);
   byAddress.add(table.tableId);
   return byAddress;
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

/** The key column each of {@link primaryKeysOf}'s checks read, by sourceID. */
export function primaryKeyColumnsOf(entry: {
   keyUniqueness?: { parentKeyColumns?: unknown };
}): Record<string, string> | undefined {
   const columns = entry.keyUniqueness?.parentKeyColumns;
   if (!columns || typeof columns !== "object") return undefined;
   const out = Object.fromEntries(
      Object.entries(columns).filter(([, v]) => typeof v === "string"),
   ) as Record<string, string>;
   return Object.keys(out).length > 0 ? out : undefined;
}

/**
 * Whether `columns` records `sourceID`'s primary key check as one of `key`:
 * a check of another column, or one with no column recorded, says nothing,
 * so a primary key moved since the check reads as unchecked.
 */
export function checkedKeyIs(
   columns: Readonly<Record<string, string>> | undefined,
   sourceID: string,
   key: string | undefined,
): boolean {
   return key !== undefined && columns?.[sourceID] === key;
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
 * `binding` with the key checks `entry` records now (its stored sources'
 * primary keys, and a rollup's own key), in place of the ones it was made
 * with: what a reload that moves a primary key gives a `storage=` binding
 * without binding its entries again.
 */
export function withEntryKeyChecks(
   binding: Binding,
   entry: ManifestEntry,
): Binding {
   const {
      primaryKeys: _was,
      primaryKeyColumns: _wasColumns,
      rollupKey: _wasKey,
      ...rest
   } = binding;
   const primaryKeys = primaryKeysOf(entry as never);
   const primaryKeyColumns = primaryKeyColumnsOf(entry as never);
   const rollupKey = isRollupOrigin(entry.origin)
      ? rollupKeyOf(entry as never)
      : undefined;
   return {
      ...rest,
      ...(primaryKeys ? { primaryKeys } : {}),
      ...(primaryKeyColumns ? { primaryKeyColumns } : {}),
      ...(rollupKey ? { rollupKey } : {}),
   };
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
      const primaryKeyColumns = primaryKeyColumnsOf(entry as never);
      const rollupKey = isRollupOrigin(entry.origin)
         ? rollupKeyOf(entry as never)
         : undefined;
      for (const sourceName of names) {
         // Set whenever there is a plan to read, even empty: an empty list
         // names no source, so the binding serves nothing (planSourceIds).
         const sourceIDs = sourceIDsFor?.(sourceName, entry.sourceEntityId);
         bindings.push({
            kind: isRollupOrigin(entry.origin) ? "rollup" : "persist",
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
            ...(primaryKeyColumns ? { primaryKeyColumns } : {}),
            ...(rollupKey ? { rollupKey } : {}),
         });
      }
   }
   return bindings;
}
