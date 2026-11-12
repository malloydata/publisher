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
   ModelMaterializer,
   QueryMaterializer,
   VirtualMap,
} from "@malloydata/malloy";
import { getPersistStorageMode } from "../config";
import { logger } from "../logger";
import { recordStorageServeRouting } from "../materialization_metrics";
import { sourceChain } from "./materialization/plan/source_map";
import {
   RollupCoverageError,
   rollupCoverageViolation,
   rollupMeasuresByMember,
} from "./materialization/plan/preaggregation_coverage";
import type { RollupPlan } from "./materialization/plan/preaggregation_synthesis";
import {
   type ServeBinding,
   serveShapeDiagnostics,
} from "./materialization/serve/materialization_serve_transform";
import type { StorageServeShape } from "./materialization/serve/model_serve_shape";
import {
   ServedCopyDecline,
   type ServedCopyServe,
} from "./materialization/serve/served_copy_serve";
import {
   type ApiQueryPath,
   buildQueryPath,
   type MaterializedTableCatalog,
   queryShapeOf,
   type StorageTable,
} from "./materialization/serve/query_path";
import type { BuildManifest } from "./materialization/types";
import {
   RollupSkip,
   type RollupVariantServe,
} from "./materialization/serve/rollup_variants";
import {
   readPreparedQuery,
   resolveStructRefSource,
} from "./governance/ir/prepared_query";
import type {
   ConsideredVariant,
   RouteDecision,
   Variant,
   VariantSkipExplanation,
} from "./query_router";

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
     }
   | {
        kind: "rollup";
        /**
         * The composite member the compiled query resolved to: the rollup that
         * answered, or the base when no rollup's grain covers the query. For a
         * `#(preaggregate)` rollup, the rollup that answered.
         */
        resolvedMember?: string;
        /** A `#(preaggregate) storage=` rollup: how its table resolves. */
        virtualMap?: VirtualMap;
        /** A `#(preaggregate) storage=` rollup: its binding. */
        binding?: ServeBinding;
     };

/**
 * One query's serving, as the router decided it and the run carried it out.
 * Immutable: each stage returns a new record. Everything that reports how a
 * query was served derives from it.
 */
export interface Answer {
   /** Each candidate the router considered, and what it decided. */
   readonly considered: readonly ConsideredVariant[];
   /** The tier the router chose: a stored table, a rollup, or live. */
   readonly tier: "storage" | "rollup" | "live";
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
   /** For a rollup answer: the composite member the compile resolved to. */
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
   rollupMember: undefined,
   runFailed: false,
   colocatedRead: new Set<string>(),
});

/** The answer the router's decision describes, before anything runs. */
export function routedAnswer(route: RouteDecision<RoutedExtra>): Answer {
   if (route.served !== "variant") {
      return Object.freeze({ ...LIVE_ANSWER, considered: route.considered });
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
      });
   }
   return Object.freeze({
      ...LIVE_ANSWER,
      considered: route.considered,
      tier: "rollup",
      routedRunnable: route.runnable,
      rollupMember: route.extra.resolvedMember,
      // A stored rollup reads through `virtual()`; a run failure on it may
      // degrade to live, since a rollup only accelerates (its binding's
      // `preaggregate` origin never vetoes).
      ...(route.extra.virtualMap
         ? {
              virtualMap: route.extra.virtualMap,
              serveShapeBindings: route.extra.binding
                 ? [route.extra.binding]
                 : [],
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
   return answer.tier === "storage" ? "storage" : undefined;
}

/**
 * The manifest a run is handed for this answer: none for the serve shape
 * (it resolves through `virtualMap` and lacks `##! experimental.persistence`),
 * the FULL manifest (rollup entries) only for the pre-aggregation companion,
 * and the manifest without rollup entries for every other runnable.
 */
export function buildManifestFor(
   answer: Answer,
   full: BuildManifest | undefined,
   withoutRollups: BuildManifest | undefined,
): BuildManifest | undefined {
   if (answer.runFailed) return withoutRollups;
   if (answer.virtualMap) return undefined;
   return answer.tier === "rollup" ? full : withoutRollups;
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
   if (route.considered.some((c) => c.reason === "row_level_gate")) {
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
 * persisted source up by its key in exactly one place (`persistedTableFor`),
 * so a hit is a table the SQL reads, with no reading of the SQL text.
 */
export function recordingManifest(
   manifest: BuildManifest | undefined,
   read: Set<string>,
): BuildManifest | undefined {
   if (!manifest) return undefined;
   const entries = new Proxy(manifest.entries, {
      get(target, key, receiver) {
         if (typeof key === "string" && Object.hasOwn(target, key)) {
            read.add(key);
         }
         return Reflect.get(target, key, receiver);
      },
   });
   return { ...manifest, entries };
}

/**
 * A variant's account of a rollup coverage refusal, for the path report: a
 * typed code, and the violation in the caller's own terms. Any other failure
 * (a compile error against the stored form) is left unexplained, so its
 * message, which can name storage paths and synthesized sources, stays in the
 * logs.
 */
function explainCoverageSkip(
   error: unknown,
): VariantSkipExplanation | undefined {
   if (!(error instanceof RollupCoverageError)) return undefined;
   return { code: `rollup_${error.reason}`, callerDetail: error.detail };
}

/** What serving reads from the model that owns the query. */
export interface QueryServingHost {
   readonly modelPath: string;
   readonly storageServe: StorageServeShape;
   /** The served copy on `virtual` (materialization v2), the `served` candidate. */
   readonly servedCopy: ServedCopyServe;
   /** The rollups the model's `#@ preaggregate` declarations describe. */
   rollupPlans(): readonly RollupPlan[];
   modelDef(): ModelDef | undefined;
   materializedTables(): MaterializedTableCatalog | undefined;
   /** The discovery surface's names, or undefined when everything is listed. */
   publishedNames(): Set<string> | undefined;
   admitsSourceByName(name: string): boolean;
   /** Every source's `sourceID` chain in the model. */
   sourceChains(): ReadonlyMap<string, readonly string[]>;
   /** The `#(preaggregate)` rollup leg (materialization v2, phase 4). */
   readonly rollupVariants: RollupVariantServe;
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
      storageRoutingPossible: boolean;
      servedCopyPossible: boolean;
      preaggServeMaterializer: ModelMaterializer | undefined;
      /**
       * The caller's own compile, for the `#(preaggregate)` leg; absent when
       * the leg is off (`materialize.enabled`) or the model has no rollup.
       */
      rollupVariantsFor?: QueryMaterializer;
   },
): Variant<RoutedExtra>[] {
   const {
      queryString,
      surfaceGivens,
      buildManifest,
      storageRoutingPossible,
      servedCopyPossible,
      preaggServeMaterializer,
   } = args;
   const candidates: Variant<RoutedExtra>[] = [];
   // `#(preaggregate)` rollups first (R1: rollup, then the nearest table,
   // then live): the caller's text compiled where its source's name means
   // the coarsest rollup that reproduces it.
   if (args.rollupVariantsFor) {
      const authorRunnable = args.rollupVariantsFor;
      candidates.push({
         kind: "rollup",
         applicable: true,
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
                  ...(answer.virtualMap
                     ? { virtualMap: answer.virtualMap }
                     : {}),
                  ...(answer.binding ? { binding: answer.binding } : {}),
               },
            };
         },
         explain: (err) =>
            err instanceof RollupSkip
               ? { code: err.code, callerDetail: err.callerDetail }
               : undefined,
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
      candidates.push({
         kind: "served",
         applicable: true,
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
               },
            };
         },
         explain: (err) =>
            err instanceof ServedCopyDecline
               ? { code: `served_copy_${err.code}` }
               : undefined,
         onSkip: (err) => {
            logger.debug("served copy cannot answer this query", {
               modelPath: host.modelPath,
               error: err instanceof Error ? err.message : String(err),
            });
         },
      });
   }
   // storage= serve routing: compile against the serve-shape model
   // (materialized sources rebound to virtual sources). If it does not
   // compile, the next candidate (or live) answers.
   if (storageRoutingPossible) {
      candidates.push({
         kind: "storage",
         applicable: true,
         attempt: async () => {
            const shaped = await host.storageServe.loadServeShapeQuery(
               queryString,
               surfaceGivens,
            );
            logger.debug("Serving query from storage tier (virtual-source)", {
               modelPath: host.modelPath,
               origin: shaped.origin,
               // The shape's sources (stale bindings are dropped).
               storageSources: shaped.bindings.map((b) => b.sourceName),
            });
            return {
               runnable: shaped.runnable,
               extra: {
                  kind: "storage",
                  virtualMap: shaped.virtualMap,
                  bindings: shaped.bindings,
                  origin: shaped.origin,
                  tablesRead: shaped.tablesRead,
               },
            };
         },
         explain: explainCoverageSkip,
         onSkip: (shapeErr) => {
            // A rollup refused for this query's aggregates is per-query and
            // expected, not a reason the package never serves from storage.
            if (shapeErr instanceof RollupCoverageError) {
               logger.debug(
                  "storage rollup cannot answer this query; serving live",
                  { modelPath: host.modelPath, error: shapeErr.message },
               );
               return;
            }
            // info, not debug: a fallback is otherwise silent (the query
            // succeeds), so this is how an operator learns why a package
            // never serves from storage. The compiler's "undefined object"
            // reads the same for an unpersisted and a stale source, so both
            // sets are reported.
            const { shapeSources, staleSources } = serveShapeDiagnostics(
               host.storageServe.serveBindings,
               host.storageServe.freshServeBindings(Date.now()),
            );
            logger.info(
               "storage serve-shape ineligible for this query; serving live",
               {
                  modelPath: host.modelPath,
                  error:
                     shapeErr instanceof Error
                        ? shapeErr.message
                        : String(shapeErr),
                  // A name absent from both sets is not materialized at all.
                  shapeSources,
                  // Bound, but withheld by the freshness gate for this query.
                  staleSources,
               },
            );
         },
      });
   }

   // Pre-aggregation serve routing: compile against the synthesized model,
   // where each annotated source is a composite of its rollups and itself.
   // Malloy picks a rollup when one covers the query, so this only offers
   // the choice. Tried only when storage did not route (that runnable
   // resolves through `virtualMap`).
   //
   // Blocked for a row-level-gated entry point as defence in depth: today
   // the build gates refuse to materialize a rollup over a gated base, but
   // that refusal (`referencesAuthorize`) has partial join reach, and a
   // rollup that did build would serve rows pre-aggregated across every
   // tenant. Blocking here keeps the invariant local to this decision.
   //
   // Inapplicable when EVERY rollup is storage-bound: storage rollups are
   // not companion members, so the composite degenerates to the base and
   // the probe would succeed for no gain. A mixed model keeps its
   // colocated members.
   if (preaggServeMaterializer) {
      const plans = host.rollupPlans();
      const everyRollupIsStorageBound =
         plans.length > 0 && plans.every((plan) => !!plan.storage);
      candidates.push({
         kind: "rollup",
         applicable: !everyRollupIsStorageBound,
         attempt: async () => {
            const candidate =
               preaggServeMaterializer.loadRestrictedQuery(queryString);
            // Compile eagerly (Malloy compiles lazily), so a failure falls to
            // live here instead of escaping as a 400 at prepare/run. Probe
            // with exactly what the run uses: the build manifest and the
            // givens. A model `given:` does not cross the companion's
            // `import`, so a given-supplying query must fail HERE and serve
            // live (see preaggregation_seams.spec.ts).
            await candidate.getSQL({
               givens: surfaceGivens,
               buildManifest,
            });
            // Compiling proves only that the chosen member DEFINES every
            // field used; `count()` uses none and resolves to a rollup.
            const compiled = (await candidate.getPreparedQuery())._query;
            const uncovered = rollupCoverageViolation(
               compiled,
               rollupMeasuresByMember([...host.rollupPlans()]),
            );
            if (uncovered) throw new RollupCoverageError(uncovered);
            const resolvedMember = (
               compiled as
                  | { compositeResolvedSourceDef?: { as?: unknown } }
                  | undefined
            )?.compositeResolvedSourceDef?.as;
            return {
               runnable: candidate,
               extra: {
                  kind: "rollup",
                  ...(typeof resolvedMember === "string"
                     ? { resolvedMember }
                     : {}),
               },
            };
         },
         explain: explainCoverageSkip,
         onSkip: (preaggErr) => {
            // Expected and common: the companion imports only the sources it
            // rolls up, and a rollup stores only its declared measures.
            logger.debug(
               "query cannot be served from the pre-aggregation model; serving live",
               {
                  modelPath: host.modelPath,
                  error:
                     preaggErr instanceof Error
                        ? preaggErr.message
                        : String(preaggErr),
               },
            );
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
      const plans = host.rollupPlans();
      const variantRollups = host.rollupVariants.hasRollups()
         ? host.rollupVariants.rollups()
         : [];
      // Nothing materialized for this model: the answer is live and there is
      // nothing to name, so skip the chain walks and the catalog reads.
      if (
         (catalog?.planned.size ?? 0) === 0 &&
         host.storageServe.serveBindings.length === 0 &&
         host.servedCopy.materializeBindings().length === 0 &&
         plans.length === 0 &&
         variantRollups.length === 0 &&
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
         rollups: [
            ...plans.map((p) => ({
               rollupSourceName: p.rollupSourceName,
               baseSourceName: p.baseSourceName,
               ...(p.baseSourceID ? { baseSourceID: p.baseSourceID } : {}),
            })),
            ...variantRollups.map((r) => ({ ...r, variant: true })),
         ],
         // Every `storage=` source carries a package warning when the tier
         // is off or write-only (see storageWarnings).
         storageWarned: getPersistStorageMode() !== "on",
         considered: answer.considered,
         storageTablesRead: answer.storageTablesRead?.map(toTable),
         ...(answer.via ? { via: answer.via } : {}),
         servedCopyTables: new Set(
            host.servedCopy.materializeBindings().map((b) => b.tablePath),
         ),
         runFailed: answer.runFailed,
         colocatedRead: answer.colocatedRead,
         ...(answer.rollupMember ? { rollupMember: answer.rollupMember } : {}),
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
