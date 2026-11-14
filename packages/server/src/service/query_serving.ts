// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Serving one query: which compiled form answers it, and the one record of
 * what happened that every reader derives from.
 *
 * `Model.runQueryResults` asks the router (`routeQuery`) over the candidates
 * {@link routeCandidates} lists, then runs the chosen runnable. What the
 * router decided and what the run did are captured in an immutable
 * {@link Answer}: built once from the route ({@link routedAnswer}), replaced
 * (never mutated) when a storage run fails and falls back live
 * ({@link liveFallbackAnswer}), and finished with the manifest entries the
 * answering run read ({@link answeredWith}). `servedFrom`, the routing metrics,
 * the path report, the access record's `path`/`pathUnavailable` and the
 * workload log all read that one record, so no two of them can disagree about
 * how a query was served.
 *
 * Adding a leg. Phase 3's re-rooted served copy, phase 4's persisted-view and
 * join-to-rollup legs plug in here without touching the query path in
 * `model.ts`: add the variant kind to `query_router.ts`'s `VariantKind`, its
 * run-time extras to {@link RoutedExtra}, its candidate to
 * {@link routeCandidates} (with its own logs and metrics), and how it maps onto
 * {@link Answer} in {@link routedAnswer}. The path report and metrics then read
 * it through the record. The query path keeps one rule: route, govern, run the
 * runnable the answer names, and on a run failure ask
 * {@link canDegradeToLive}.
 */

import type {
   GivenValue,
   ModelDef,
   QueryMaterializer,
   VirtualMap,
} from "@malloydata/malloy";
import { getMaterializeGatedSources, getPersistStorageMode } from "../config";
import type { GraftScope } from "./gate_classification";
import { logger } from "../logger";
import { recordStorageServeRouting } from "../materialization_metrics";
import { sourceChain } from "./materialization/plan/source_map";
import { type ServeBinding } from "./materialization/serve/materialization_serve_transform";
import type { StorageBindings } from "./materialization/serve/storage_bindings";
import type { ServedCopyServe } from "./materialization/serve/served_copy_serve";
import { explainDecline } from "./materialization/serve/leg_decline";
import {
   type ApiQueryPath,
   buildQueryPath,
   type MaterializedTableCatalog,
   queryShapeOf,
   type StorageTable,
} from "./materialization/serve/query_path";
import type { BuildManifest } from "./materialization/types";
import type { RollupVariantServe } from "./materialization/serve/rollup_variants";
import type {
   PersistedViewServe,
   ViewTable,
} from "./materialization/serve/view_variants";
import {
   readPreparedQuery,
   resolveStructRefSource,
} from "./governance/ir/prepared_query";
import type { ConsideredVariant, RouteDecision, Variant } from "./query_router";

/**
 * How a query's answer was ultimately produced, once it reached the storage
 * routing decision at all. `storage` = served from the materialized table via
 * the virtual-source transform. `live_fallback` = it routed, then a run-time
 * store failure degraded it to the live warehouse (a success the caller cannot
 * distinguish from a storage hit, which is exactly why it is reported
 * separately; on the error metric, the live retry failed too). Absent means
 * the query never had a storage binding to consider.
 */
export type ServedFrom = "storage" | "live_fallback";

/**
 * What a routed variant carries into the run, by kind: the storage tier needs
 * its virtual map and the bindings that produced the shape; a rollup needs
 * nothing beyond its runnable.
 */
export type RoutedExtra =
   | {
        kind: "storage";
        virtualMap: VirtualMap;
        bindings: ServeBinding[];
        origin: "persist" | "preaggregate";
        tablesRead: ServeBinding[];
        /** Set when the served copy on `virtual` answered, not the serve shape. */
        via?: "served_copy";
        /** The served copy's graft scope, for `govern()` (see ServedCopyServe.loadQuery). */
        graftScope?: GraftScope;
     }
   | {
        kind: "view";
        /** The persisted view's table that answered. */
        table: ViewTable;
        /** For a `storage=` view: how its `virtual()` handle resolves. */
        virtualMap?: VirtualMap;
        /** For a `storage=` view: its binding. */
        binding?: ServeBinding;
        /** The variant's graft scope, for `govern()` (R3). */
        graftScope: GraftScope;
     }
   | {
        kind: "rollup";
        /** The rollup that answered. */
        resolvedMember?: string;
        /** A `#(preaggregate) storage=` rollup: how its table resolves. */
        virtualMap?: VirtualMap;
        /** A `#(preaggregate) storage=` rollup: its binding. */
        binding?: ServeBinding;
        /**
         * Every stored table the answer reads, when that is more than the
         * rollup's (a join leg answered in a storage destination reads the
         * query source's table there too): together they decide whether a
         * failed run may be retried live.
         */
        bindings?: ServeBinding[];
        /**
         * A `#(preaggregate)` rollup's graft scope, for `govern()` (R3): the
         * variant that redeclares the run target over the rollup, gates
         * included. Absent for a v1 `#@ preaggregate` rollup, which no gated
         * entry point reaches.
         */
        graftScope?: GraftScope;
     };

/**
 * Whether a served-copy answer reads stored tables, every one of them
 * colocated (the source's own warehouse): the v1 cutover's contracts at read
 * (G3) apply to it. A `storage=` table waits for the managed-tier review
 * (`materialize.gatedSources`), so an answer reading one, or none, does not.
 */
function readsOnlyColocatedTables(extra: RoutedExtra): boolean {
   return (
      extra.kind === "storage" &&
      extra.via === "served_copy" &&
      extra.tablesRead.length > 0 &&
      extra.tablesRead.every((b) => b.colocated === true)
   );
}

/**
 * One query's serving, as the router decided it and the run carried it out.
 * Immutable: each stage returns a new record. Everything that reports how a
 * query was served derives from it.
 */
export interface Answer {
   /** Each candidate the router considered, and what it decided. */
   readonly considered: readonly ConsideredVariant[];
   /** The tier the router chose: a persisted view, a stored table, a rollup, or live. */
   readonly tier: "view" | "storage" | "rollup" | "live";
   /** For a view answer: the persisted view's table. */
   readonly viewTable: ViewTable | undefined;
   /** The routed runnable, or undefined when live answers (the caller's own compile runs). */
   readonly routedRunnable: QueryMaterializer | undefined;
   /** For a storage answer: how the shape's virtual sources resolve to tables. */
   readonly virtualMap: VirtualMap | undefined;
   /** For a storage answer: the bindings that produced the shape (decide per-binding behaviour from these, never the package-wide set). */
   readonly serveShapeBindings: readonly ServeBinding[];
   /** For a storage answer: the stored tables the query reads. */
   readonly storageTablesRead: readonly ServeBinding[] | undefined;
   /** For a storage answer: `served_copy` when the served copy on `virtual` answered rather than the serve shape. */
   readonly via: "served_copy" | undefined;
   /** For a served-copy, persisted-view or rollup answer: where `govern()` grafts a row-level gate (R3). */
   readonly graftScope: GraftScope | undefined;
   /**
    * The answering variant was let past an entry-point gate because it keeps
    * the contracts (R3): `govern()` must find and enforce that gate on it.
    */
   readonly pastEntryPointGate: boolean;
   /** For a rollup answer: the rollup that answered. */
   readonly rollupMember: string | undefined;
   /** A storage read failed at run time and the query was answered live. */
   readonly runFailed: boolean;
   /** The colocated manifest entries the answering run read. */
   readonly colocatedRead: ReadonlySet<string>;
}

/** The answer before routing, and when nothing could route. */
export const LIVE_ANSWER: Answer = Object.freeze({
   considered: [],
   tier: "live",
   routedRunnable: undefined,
   virtualMap: undefined,
   serveShapeBindings: [],
   storageTablesRead: undefined,
   via: undefined,
   graftScope: undefined,
   pastEntryPointGate: false,
   rollupMember: undefined,
   viewTable: undefined,
   runFailed: false,
   colocatedRead: new Set<string>(),
});

/** The answer the router's decision describes, before anything runs. */
export function routedAnswer(route: RouteDecision<RoutedExtra>): Answer {
   if (route.served !== "variant") {
      return Object.freeze({ ...LIVE_ANSWER, considered: route.considered });
   }
   if (route.extra.kind === "view") {
      return Object.freeze({
         ...LIVE_ANSWER,
         considered: route.considered,
         tier: "view",
         routedRunnable: route.runnable,
         virtualMap: route.extra.virtualMap,
         serveShapeBindings: route.extra.binding ? [route.extra.binding] : [],
         storageTablesRead: route.extra.binding
            ? [route.extra.binding]
            : undefined,
         graftScope: route.extra.graftScope,
         pastEntryPointGate: route.pastEntryPointGate === true,
         viewTable: route.extra.table,
      });
   }
   if (route.extra.kind === "storage") {
      return Object.freeze({
         ...LIVE_ANSWER,
         considered: route.considered,
         tier: "storage",
         routedRunnable: route.runnable,
         virtualMap: route.extra.virtualMap,
         serveShapeBindings: route.extra.bindings,
         storageTablesRead: route.extra.tablesRead,
         via: route.extra.via,
         graftScope: route.extra.graftScope,
         pastEntryPointGate: route.pastEntryPointGate === true,
      });
   }
   return Object.freeze({
      ...LIVE_ANSWER,
      considered: route.considered,
      pastEntryPointGate: route.pastEntryPointGate === true,
      tier: "rollup",
      routedRunnable: route.runnable,
      rollupMember: route.extra.resolvedMember,
      graftScope: route.extra.graftScope,
      // A stored rollup reads through `virtual()`; a run failure on it may
      // degrade to live, since a rollup only accelerates (its binding's
      // `preaggregate` origin never vetoes).
      ...(route.extra.virtualMap
         ? {
              virtualMap: route.extra.virtualMap,
              serveShapeBindings:
                 route.extra.bindings ??
                 (route.extra.binding ? [route.extra.binding] : []),
           }
         : {}),
   });
}

/** The same answer after its storage run failed and the live retry ran. */
export function liveFallbackAnswer(answer: Answer): Answer {
   return Object.freeze({ ...answer, runFailed: true });
}

/** The answer with the manifest entries its answering run read. */
export function answeredWith(
   answer: Answer,
   colocatedRead: ReadonlySet<string>,
): Answer {
   return Object.freeze({ ...answer, colocatedRead });
}

/**
 * `servedFrom` for the answer: a storage hit, a storage run that fell back
 * live (`live_fallback`, a success that must NOT count as a storage hit), or
 * undefined for everything else.
 */
export function servedFromOf(answer: Answer): ServedFrom | undefined {
   if (answer.runFailed) return "live_fallback";
   // A served-copy answer that read only colocated `#(materialize)` tables
   // read the source's own warehouse, as a colocated `#@ persist` answer
   // does, which reports no storage hit; its path report names the table.
   if (
      answer.tier === "storage" &&
      answer.storageTablesRead !== undefined &&
      answer.storageTablesRead.length > 0 &&
      answer.storageTablesRead.every((b) => b.colocated)
   ) {
      return undefined;
   }
   // A `storage=` persisted view or `#(preaggregate)` rollup reads the
   // storage tier too, through `virtual()`.
   return answer.tier === "storage" ||
      ((answer.tier === "view" || answer.tier === "rollup") &&
         answer.virtualMap !== undefined)
      ? "storage"
      : undefined;
}

/**
 * Whether a run-time store failure may be retried against the live warehouse,
 * decided from the bindings that produced the serve shape. Exported for tests.
 *
 * An authored binding declaring anything but `live` vetoes for the whole shape
 * (deliberately coarse). A rollup binding never vetoes and never enables on its
 * own: pre-aggregation has no correctness dimension (docs/preaggregation.md: a
 * stale rollup recomputes from the base with the same answer), so a rollup must
 * not error on a store failure. A rollup must also not be filtered out, or a
 * rollup-only package could never degrade. Empty is false: nothing routed.
 */
export function bindingsAllowDegradeToLive(
   bindings: readonly ServeBinding[],
): boolean {
   return (
      bindings.length > 0 &&
      bindings.every(
         (b) => b.origin === "preaggregate" || b.freshnessFallback === "live",
      )
   );
}

/**
 * Whether a run failure under this answer may be retried live:
 * `freshnessFallback=live` marks the tier as an optimisation, not a
 * dependency. Deliberately narrow: only a storage answer, only per
 * {@link bindingsAllowDegradeToLive} over the bindings that produced THIS
 * shape (a sibling source must not decide), never for a client error (a bad
 * given) or an abort.
 */
export function canDegradeToLive(
   answer: Answer,
   error: unknown,
   opts: { hasLiveRunnable: boolean; aborted: boolean },
): boolean {
   return (
      !!answer.virtualMap &&
      opts.hasLiveRunnable &&
      !opts.aborted &&
      !String((error as { code?: string })?.code ?? "").startsWith(
         "runtime-given-",
      ) &&
      bindingsAllowDegradeToLive(answer.serveShapeBindings)
   );
}

/**
 * Record the serve-routing metric from the router's decision, once per query:
 * a storage hit (with the origin that answered), a storage miss
 * (`live_fallback`: the transform could not answer, whatever answered next),
 * or `blocked_by_row_level_gate`, a row-level gate that vetoed every applicable
 * candidate (storage, rollup, or both) before any was tried. Otherwise nothing
 * when the storage tier was not a candidate.
 */
export function recordRoutingDecision(route: RouteDecision<RoutedExtra>): void {
   // The gate vetoes every applicable candidate at once, before any compile,
   // so it is recorded once per query whichever tiers were candidates: the
   // label's contract (see recordStorageServeRouting) covers both tiers.
   // A served copy that keeps the contracts (R3) may still answer past it.
   if (
      route.served !== "variant" &&
      route.considered.some((c) => c.reason === "row_level_gate")
   ) {
      recordStorageServeRouting("blocked_by_row_level_gate");
      return;
   }
   // The served copy reads the same stored tables, so it counts as the tier.
   const storage =
      route.considered.find(
         (c) => c.kind === "served" && c.outcome === "used",
      ) ?? route.considered.find((c) => c.kind === "storage");
   if (!storage) return;
   if (route.served === "variant" && route.extra.kind === "storage") {
      recordStorageServeRouting("storage", route.extra.origin);
   } else if (storage.reason === "does_not_answer") {
      recordStorageServeRouting("live_fallback");
   }
}

/**
 * The build manifest a run is handed, wrapped to record which entries Malloy
 * looked up and found: the colocated tables it substituted. Malloy looks a
 * persisted source up by its key in exactly one place (`persistedTableFor`,
 * `buildManifest.entries[buildId]`), so a hit is a table the SQL reads, with no
 * reading of the SQL text.
 *
 * Malloy also ENUMERATES the entries before it compiles (it checks every
 * entry's table path), and an enumeration reads each value too. Those reads
 * are not lookups: recording them reported every bound table as having
 * answered, including for a `#@ -persist` reader that recomputes and for a
 * source Malloy never substitutes.
 *
 * The two are told apart by which object they read, not by the order of the
 * reads. Every read of `manifest.entries` is handed a fresh view of the same
 * entries, and a view's reads are lookups only while nothing has enumerated
 * that view. Every enumeration (`Object.entries`, `Object.keys`,
 * `Object.values`, spread, `for…in`, `JSON.stringify`, `Object.assign`) asks
 * for the object's own keys first ([[OwnPropertyKeys]], the `ownKeys` trap),
 * and a keyed lookup never does, whether it reads the value alone or checks
 * `hasOwn` first (Malloy's `safeRecordGet`). So this holds for any enumeration
 * and any lookup form, in any order, sync or async. (The order heuristic this
 * replaced, "a read right after its own key's descriptor is an enumeration",
 * missed `JSON.stringify`, which describes every key before reading any, and
 * would have dropped every real lookup had Malloy guarded it with `hasOwn`, as
 * its own `safeRecordGet` does.) It assumes only that Malloy reaches
 * `entries` afresh for a lookup, as `persistedTableFor` does, rather than
 * enumerating and then looking up through one held reference, or re-reading
 * `entries` for each value of an enumeration (`for (k in m.entries)
 * m.entries[k]`). `query_serving_malloy.spec.ts` runs a real compile against
 * the pinned Malloy and fails loudly if either assumption stops holding.
 */
export function recordingManifest(
   manifest: BuildManifest | undefined,
   read: Set<string>,
): BuildManifest | undefined {
   if (!manifest) return undefined;
   const target = manifest.entries;
   const view = (): BuildManifest["entries"] => {
      let enumerated = false;
      return new Proxy(target, {
         ownKeys(t) {
            enumerated = true;
            return Reflect.ownKeys(t);
         },
         get(t, key, receiver) {
            if (
               !enumerated &&
               typeof key === "string" &&
               Object.hasOwn(t, key)
            ) {
               read.add(key);
            }
            return Reflect.get(t, key, receiver);
         },
      });
   };
   const wrapped = { ...manifest } as BuildManifest;
   Object.defineProperty(wrapped, "entries", {
      get: view,
      enumerable: true,
      configurable: true,
   });
   return wrapped;
}

/** What serving reads from the model that owns the query. */
export interface QueryServingHost {
   readonly modelPath: string;
   /** The model's `storage=` bindings, for the path report. */
   readonly storageServe: StorageBindings;
   /** The served copy on `virtual` (materialization v2), the `served` candidate. */
   readonly servedCopy: ServedCopyServe;
   modelDef(): ModelDef | undefined;
   materializedTables(): MaterializedTableCatalog | undefined;
   /** The discovery surface's names, or undefined when everything is listed. */
   publishedNames(): Set<string> | undefined;
   admitsSourceByName(name: string): boolean;
   /** Every source's `sourceID` chain in the model. */
   sourceChains(): ReadonlyMap<string, readonly string[]>;
   /** The `#(preaggregate)` rollup leg (materialization v2, phase 4). */
   readonly rollupVariants: RollupVariantServe;
   /** The `#@ persist` view leg (materialization v2, phase 4, P14). */
   readonly persistedViews: PersistedViewServe;
}

/**
 * The router's candidates for one query, in catalog order: the served copy on
 * `virtual` (when `materialize.enabled` is on), the `storage=` serve shape,
 * then the pre-aggregation companion. A candidate is listed
 * only when its variant exists for this model (so the gate check runs
 * exactly when one could route), and is inapplicable when it cannot help
 * this request. Each keeps its own logs and metrics.
 */
export function routeCandidates(
   host: QueryServingHost,
   args: {
      queryString: string;
      surfaceGivens: Record<string, GivenValue> | undefined;
      buildManifest: BuildManifest | undefined;
      servedCopyPossible: boolean;
      /**
       * The v1 cutover's contracts at read for colocated tables (G3): the
       * served copy goes past an entry-point gate when every table it reads
       * is colocated, even with `materialize.gatedSources` off, since a
       * colocated table never leaves the author's warehouse.
       */
      colocatedKeepsContracts?: boolean;
      /**
       * The caller's own compile, for the `#(preaggregate)` leg; absent when
       * the leg is off (`materialize.enabled`) or the model has no rollup.
       */
      rollupVariantsFor?: QueryMaterializer;
      /**
       * The caller's own compile, for the persisted-view leg; absent when the
       * leg is off (`materialize.enabled`) or the model has no persisted view.
       */
      persistedViewsFor?: QueryMaterializer;
   },
): Variant<RoutedExtra>[] {
   const { queryString, surfaceGivens, buildManifest, servedCopyPossible } =
      args;
   const candidates: Variant<RoutedExtra>[] = [];
   // A persisted view first (R1: view by exact call, rollup, nearest table,
   // live). Listed only when the model has one; applicable only when the text
   // calls one, so a look-alike or a nest never costs a compile.
   if (args.persistedViewsFor) {
      const authorRunnable = args.persistedViewsFor;
      candidates.push({
         kind: "view",
         applicable: !!host.persistedViews.calledView(queryString),
         // R3, behind the same switch as the served copy (question 14): the
         // source's gates are written on the variant and grafted there.
         keepsContracts: getMaterializeGatedSources(),
         attempt: async () => {
            const answer = await host.persistedViews.attempt({
               queryString,
               givens: surfaceGivens,
               authorRunnable,
               buildManifest,
            });
            return {
               runnable: answer.runnable,
               extra: {
                  kind: "view",
                  table: answer.table,
                  graftScope: answer.graftScope,
                  ...(answer.virtualMap
                     ? { virtualMap: answer.virtualMap }
                     : {}),
                  ...(answer.binding ? { binding: answer.binding } : {}),
               },
            };
         },
         explain: (err) => explainDecline(err, "view"),
         onSkip: (err) => {
            logger.debug("the persisted view does not answer this query", {
               modelPath: host.modelPath,
               error: err instanceof Error ? err.message : String(err),
            });
         },
      });
   }
   // Then `#(preaggregate)` rollups (R1: view, rollup, the nearest table,
   // live): the caller's text compiled where its source's name means the
   // coarsest rollup that reproduces it.
   if (args.rollupVariantsFor) {
      const authorRunnable = args.rollupVariantsFor;
      candidates.push({
         kind: "rollup",
         applicable: true,
         // R3 for rollups, behind the served copy's switch: the run target is
         // redeclared over the rollup with its gates, proven exact on the
         // rollup before it is used (RollupVariantServe), then grafted and
         // re-proven by `govern()`. A `storage=` rollup over a gated base
         // waits for question 14's review (rollupsServeGatedBases).
         keepsContracts: getMaterializeGatedSources(),
         attempt: async () => {
            const answer = await host.rollupVariants.attempt({
               queryString,
               givens: surfaceGivens,
               authorRunnable,
               buildManifest,
            });
            return {
               runnable: answer.runnable,
               extra: {
                  kind: "rollup",
                  resolvedMember: answer.rollupSourceName,
                  ...(answer.graftScope
                     ? { graftScope: answer.graftScope }
                     : {}),
                  ...(answer.virtualMap
                     ? { virtualMap: answer.virtualMap }
                     : {}),
                  ...(answer.binding ? { binding: answer.binding } : {}),
                  ...(answer.bindings ? { bindings: answer.bindings } : {}),
               },
            };
         },
         explain: (err) => explainDecline(err, "rollup"),
         onSkip: (err) => {
            logger.debug("no #(preaggregate) rollup answers this query", {
               modelPath: host.modelPath,
               error: err instanceof Error ? err.message : String(err),
            });
         },
      });
   }
   // The served copy on `virtual` (materialization v2): the author's own
   // files with each stored source re-rooted on its table. Tried first when
   // enabled; a query it cannot answer falls through to the serve shape.
   if (servedCopyPossible) {
      const gatedSources = getMaterializeGatedSources();
      candidates.push({
         kind: "served",
         applicable: true,
         // R3, behind its own switch until the managed-tier review (plan,
         // question 14): a row-level gate is grafted on the served copy.
         // Under the v1 cutover, also for an answer reading only colocated
         // tables (decided per answer, below).
         keepsContracts: gatedSources || !!args.colocatedKeepsContracts,
         ...(gatedSources || !args.colocatedKeepsContracts
            ? {}
            : {
                 keepsContractsFor: (extra: RoutedExtra) =>
                    readsOnlyColocatedTables(extra),
              }),
         attempt: async () => {
            const served = await host.servedCopy.loadQuery(
               queryString,
               surfaceGivens,
            );
            logger.debug("Serving query from the served copy (virtual)", {
               modelPath: host.modelPath,
               tables: served.tablesRead.map((b) => b.sourceName),
            });
            return {
               runnable: served.runnable,
               extra: {
                  kind: "storage",
                  virtualMap: served.virtualMap,
                  bindings: served.bindings,
                  origin: "persist",
                  tablesRead: served.tablesRead,
                  via: "served_copy",
                  graftScope: served.graftScope,
               },
            };
         },
         explain: (err) => explainDecline(err, "served_copy"),
         onSkip: (err) => {
            logger.debug("served copy cannot answer this query", {
               modelPath: host.modelPath,
               error: err instanceof Error ? err.message : String(err),
            });
         },
      });
   }
   return candidates;
}

/**
 * The path report for one answered query, or undefined when it cannot be
 * assembled (logged at debug). Reads the author's compile, the
 * {@link Answer} and the fresh manifest; changes nothing.
 */
export async function queryPathFor(
   host: QueryServingHost,
   answer: Answer,
   args: {
      /** The author's own compile: its names, and no grafted gate filter. */
      runnable: QueryMaterializer;
      sourceName: string | undefined;
      viewName: string | undefined;
      freshManifest: BuildManifest | undefined;
      includeHidden: boolean;
      /** The text the router saw, to name the persisted view it calls. */
      queryString?: string;
   },
): Promise<ApiQueryPath | undefined> {
   try {
      const prepared = await readPreparedQuery(args.runnable);
      const modelDef = prepared.modelDef ?? host.modelDef();
      const struct = resolveStructRefSource<{ as?: string; name?: string }>(
         prepared.structRef,
         modelDef,
      );
      const sourceName =
         args.sourceName ??
         (typeof prepared.structRef === "string"
            ? prepared.structRef
            : (struct?.as ?? ""));
      const shape = queryShapeOf(prepared.query, struct, {
         sourceName,
         viewName: args.viewName,
      });
      const catalog = host.materializedTables();
      const variantRollups = host.rollupVariants.hasRollups()
         ? host.rollupVariants.rollups()
         : [];
      // The persisted view the query calls, if any: its table is the first
      // candidate (R1), whatever the router decided about it.
      const viewTable =
         answer.viewTable ??
         (args.queryString !== undefined
            ? host.persistedViews.reportedTable(
                 args.queryString,
                 args.freshManifest,
              )
            : undefined);
      // Nothing materialized for this model: the answer is live and there is
      // nothing to name, so skip the chain walks and the catalog reads.
      if (
         (catalog?.planned.size ?? 0) === 0 &&
         host.storageServe.serveBindings.length === 0 &&
         host.servedCopy.materializeBindings().length === 0 &&
         variantRollups.length === 0 &&
         viewTable === undefined &&
         answer.considered.length === 0
      ) {
         return {
            query: shape,
            servedBy: { kind: "live" },
            considered: [],
            notConsidered: [],
            warnings: [],
         };
      }
      const now = Date.now();
      const toTable = (b: ServeBinding): StorageTable => ({
         sourceName: b.sourceName,
         sourceIDs: b.sourceIDs,
         origin: b.origin,
         tablePath: b.tablePath,
         freshAsOf: b.freshAsOf,
         ...(b.colocated ? { colocated: true as const } : {}),
      });
      const published = args.includeHidden ? undefined : host.publishedNames();
      return buildQueryPath({
         shape,
         modelPath: host.modelPath,
         chain: sourceChain(struct, modelDef),
         catalog,
         freshColocated: args.freshManifest
            ? new Set(Object.keys(args.freshManifest.entries))
            : undefined,
         // The serve shape's tables, and the `#(materialize)` tables only the
         // served copy reads, so a table on the chain is named whichever
         // candidate answered.
         storage: {
            all: [
               ...host.storageServe.serveBindings,
               ...host.servedCopy.materializeBindings(),
               ...host.rollupVariants.rollupBindings(),
            ].map(toTable),
            fresh: [
               ...host.storageServe.freshServeBindings(now),
               ...host.servedCopy.materializeBindings(now),
               ...host.rollupVariants.rollupBindings(now),
            ].map(toTable),
         },
         rollups: variantRollups.map((r) => ({ ...r, variant: true })),
         // Every `storage=` source carries a package warning when the tier
         // is off or write-only (see storageWarnings).
         storageWarned: getPersistStorageMode() !== "on",
         considered: answer.considered,
         storageTablesRead: answer.storageTablesRead?.map(toTable),
         ...(answer.tier === "rollup"
            ? {
                 rollupTablesRead: answer.serveShapeBindings
                    .filter((b) => b.origin !== "preaggregate")
                    .map(toTable),
              }
            : {}),
         ...(answer.via ? { via: answer.via } : {}),
         servedCopyTables: new Set(
            host.servedCopy.materializeBindings().map((b) => b.tablePath),
         ),
         runFailed: answer.runFailed,
         colocatedRead: answer.colocatedRead,
         ...(viewTable ? { view: viewTable } : {}),
         ...(answer.rollupMember ? { rollupMember: answer.rollupMember } : {}),
         ...(variantRollups.length > 0
            ? {
                 joinedRollupBases: host.rollupVariants.joinedRollupBaseIDs(
                    struct,
                    prepared,
                 ),
              }
            : {}),
         chains: host.sourceChains(),
         // The discovery surface: a name the caller cannot list is never
         // named, unless the caller may see hidden sources.
         ...(args.includeHidden
            ? {}
            : {
                 isVisibleSource: (name: string) =>
                    (published === undefined || published.has(name)) &&
                    host.admitsSourceByName(name),
              }),
      });
   } catch (err) {
      logger.debug("Path report unavailable", {
         modelPath: host.modelPath,
         error: err instanceof Error ? err.message : String(err),
      });
      return undefined;
   }
}
