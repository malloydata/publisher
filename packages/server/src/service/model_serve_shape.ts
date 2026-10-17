// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { lazyCounter } from "../telemetry";
import {
   InMemoryURLReader,
   MalloyConfig,
   type GivenValue,
   type ModelDef,
   type ModelMaterializer,
   type QueryMaterializer,
   Runtime,
   type VirtualMap,
} from "@malloydata/malloy";
import { readFileSync } from "fs";
import { fileURLToPath } from "url";
import { components } from "../api";
import { logger } from "../logger";
import { recordServeShapeTierDrop } from "../materialization_metrics";
import { restrictMalloyConfigToConnections } from "./connection";
import { evaluateManifestFreshness } from "./freshness";
import {
   authorModelLiftContext,
   authorRefinementsFor,
   buildServeShapeModelForBindings,
   buildServeShapeTiers,
   buildVirtualMap,
   documentFlagsForLifts,
   liftDerivedSources,
   narrowSchemaToPublic,
   reachedPersistedSources,
   withholdUnreproducibleCallerScopedJoins,
   type DerivedSourceDef,
   type DerivedSourceLift,
   type RollupShapeGroup,
   type ServeBinding,
   type ServeShapeGiven,
   type SourceLocation,
} from "./materialization_serve_transform";
import { rollupServeBindings } from "./preaggregation_serve_bindings";
import { type RollupPlan } from "./preaggregation_synthesis";

const staleBindingsWithheld = lazyCounter(
   "publisher_storage_bindings_withheld_total",
   "Storage serve bindings withheld from a routed query because the freshness gate found them stale; each one is read live instead. Until now staleness was visible only as a later live read.",
);

type ApiGiven = components["schemas"]["Given"];

/** What the storage serve shape reads from the model it serves. */
export interface ServeShapeHost {
   modelPath(): string;
   modelDef(): ModelDef | undefined;
   givens(): ApiGiven[] | undefined;
   rollupPlans(): RollupPlan[];
   rollupPlansVersion(): number;
   ensureSharedQueryModel(materializer: ModelMaterializer): Promise<void>;
}

/**
 * A model's `storage=` serve routing: the bindings the package pushed, the
 * destination connections they resolve against, and the memoized transient
 * model that rebinds materialized sources onto those destinations.
 */
export class StorageServeShape {
   /**
    * The connections a serve-shape compile may resolve: this environment's
    * storage destinations. Deliberately NOT the config this model itself
    * compiles against — a destination must not be resolvable from the author's
    * model, a notebook cell, or a query, so the two compiles read disjoint name
    * sets. Absent ⇒ no storage serve routing (queries serve live).
    */
   serveDestinationConfig?: () => MalloyConfig;
   /**
    * The package's `storage=` serve bindings, set by the owning Package. Empty
    * ⇒ no serve routing (the common case).
    */
   serveBindings: ServeBinding[] = [];
   /** Memoized serve-shape materializer, keyed by the bound source set. */
   serveShapeCache?: { key: string; materializer: ModelMaterializer };

   constructor(private readonly host: ServeShapeHost) {}

   /**
    * Set the connections a transient serve-shape model compiles against. Pushed
    * by the owning Package from the environment's destination list, resolved on
    * each use so a destination-list change takes effect without a reload.
    */
   public setServeDestinationConfig(provider: () => MalloyConfig): void {
      this.serveDestinationConfig = provider;
      this.invalidateServeShapeCache();
   }

   /**
    * Forget the memoized serve-shape materializer. The memo is keyed on the
    * binding set, so anything else it was compiled against — the destination
    * connections above all — has to invalidate it explicitly.
    */
   public invalidateServeShapeCache(): void {
      this.serveShapeCache = undefined;
   }

   /** Set (or clear) this model's `storage=` serve bindings. */
   public setServeBindings(bindings: ServeBinding[]): void {
      this.serveBindings = bindings;
      this.serveShapeCache = undefined;
   }

   /**
    * The serve bindings that may serve from their materialized table right NOW:
    * fresh, un-gated, or stale-under-`stale_ok`. A stale binding with a
    * `live`/`fail` fallback is dropped, so queries touching it serve live — the
    * same freshness gate the colocated serve applies (`evaluateManifestFreshness`).
    */
   freshServeBindings(now: number): ServeBinding[] {
      const at = new Date(now);
      return this.serveBindings.filter(
         (b) =>
            evaluateManifestFreshness(
               {
                  tableName: b.tablePath,
                  dataAsOf: b.freshAsOf,
                  freshnessWindowSeconds: b.freshnessWindowSeconds,
                  freshnessFallback: b.freshnessFallback,
               },
               at,
            ) === "serve_table",
      );
   }

   /**
    * Compile an untrusted query against the transient serve-shape model that
    * rebinds this package's materialized-into-storage sources to virtual sources
    * on their storage connection.
    *
    * Throwing is the eligibility signal: a query that references a refinement
    * the serve shape does not reproduce, or a source with no binding, fails to
    * compile here and the caller serves it live. The compiled materializer is
    * memoized per fresh binding set.
    */
   async loadServeShapeQuery(
      queryString: string,
      /**
       * The request's given values. Required by the origin probe: compiling
       * resolves every given the shape declares, and a given with no default
       * (every `#(secure)` attribute) would otherwise throw and send exactly the
       * row-level-bounded sources to live.
       */
      givens?: Record<string, GivenValue>,
   ): Promise<{
      runnable: QueryMaterializer;
      virtualMap: VirtualMap;
      /**
       * The fresh bindings the shape was built from, so a per-binding decision
       * reads the set that produced this shape rather than the package-wide one.
       */
      bindings: ServeBinding[];
      /**
       * Whether a pre-aggregation rollup actually ANSWERED this query, read from
       * the probe SQL (a rollup's table appears only if the composite resolver
       * chose it). "The shape has rollup groups" would over-report every query
       * that touched an ordinary stored source in the same package.
       */
      origin: "persist" | "preaggregate";
   }> {
      // Keying the cache on the FRESH subset makes it recompile when a binding
      // crosses its freshness window.
      const freshBindings = this.freshServeBindings(Date.now());
      const withheld = this.serveBindings.length - freshBindings.length;
      if (withheld > 0) staleBindingsWithheld().add(withheld);
      if (freshBindings.length === 0) {
         // The caller catches this and serves live.
         throw new Error("no fresh storage serve bindings for this query");
      }
      const key =
         `v${this.host.rollupPlansVersion()}|` +
         freshBindings
            .map(
               (b) => `${b.sourceName}@${b.destinationName}/${b.virtualHandle}`,
            )
            .sort()
            .join("|");
      // A rollup CANNOT go through serveBindingsWithRefinements: its source name
      // names nothing in the author's model, so schema narrowing intersects to
      // nothing (see preaggregation_serve_bindings.ts).
      const ordinary = freshBindings.filter((b) => b.origin !== "preaggregate");
      const { groups, conflicts } = rollupServeBindings(
         freshBindings,
         this.host.rollupPlans(),
      );
      if (!this.serveShapeCache || this.serveShapeCache.key !== key) {
         for (const conflict of conflicts) {
            // info, not debug: a dropped group is otherwise invisible (rows are
            // correct, only acceleration is lost). Logged on cache miss only,
            // because a conflict belongs to the binding set, not to a query.
            logger.info(
               "Not serving a source's rollups from storage; queries are answered from the base",
               {
                  modelPath: this.host.modelPath(),
                  source: conflict.baseSourceName,
                  reason: conflict.reason,
               },
            );
         }
         this.serveShapeCache = {
            key,
            materializer: await this.compileServeShape(
               this.serveBindingsWithRefinements(ordinary),
               groups,
            ),
         };
      }
      // Rollup members are not in `ordinary`; map their handles explicitly or the
      // composite resolves to nothing at run time.
      const virtualMap = buildVirtualMap([
         ...ordinary,
         ...groups.flatMap((g) => g.members),
      ]);

      await this.host.ensureSharedQueryModel(this.serveShapeCache.materializer);
      const runnable =
         this.serveShapeCache.materializer.loadRestrictedQuery(queryString);
      // Compile eagerly so ineligibility surfaces inside the caller's try: Malloy
      // compiles lazily, and an error at prepare/run would bypass the live
      // fallback.
      const probeSQL = await runnable.getSQL({ virtualMap, givens });
      // Paths quoted exactly as the virtualMap substitutes them, so the match
      // below compares like with like.
      const rollupPaths = [
         ...buildVirtualMap(groups.flatMap((g) => g.members)),
      ].flatMap(([, byHandle]) => [...byHandle.values()]);
      const origin = rollupPaths.some((path) => probeSQL.includes(path))
         ? ("preaggregate" as const)
         : ("persist" as const);
      return { runnable, virtualMap, bindings: freshBindings, origin };
   }

   /**
    * Compile the serve-shape materializer for a binding set, degrading
    * gracefully: one un-carriable refinement would otherwise fail the whole
    * shape and disable storage serving for every source in the package. On
    * failure the riskiest refinement category is dropped and it retries: full →
    * drop views → drop views + joins → base-only. Base-only still carries
    * filters, so a filter that cannot be reproduced fails every tier and the
    * caller serves live rather than unfiltered. The per-query eager compile in
    * {@link loadServeShapeQuery} remains the final net.
    */
   private async compileServeShape(
      enriched: ServeBinding[],
      /**
       * Pre-aggregation groups, passed through every tier UNCHANGED: a rollup's
       * measures are the only reason its member exists, so dropping them would
       * leave a member that compiles and answers nothing.
       */
      rollupGroups: RollupShapeGroup[] = [],
      /**
       * Set on the single re-entry after withholding bindings whose filters do
       * not reproduce; a floor failure on the retry serves live.
       */
      isRetry = false,
   ): Promise<ModelMaterializer> {
      // Every tier keeps the never-thinned kinds, `filter` among them: a
      // source's `where:` is part of what it MEANS, so dropping it would answer
      // with rows the source excludes.
      const tiers = buildServeShapeTiers(rollupGroups);
      // A rollup GROUP counts as something beyond the base, so a pure-rollup
      // shape is still probed and a broken group is caught here rather than as
      // a per-query live fallback for the whole package.
      const nothingToEscalate =
         rollupGroups.length === 0 &&
         !enriched.some((b) => (b.refinements ?? []).length > 0);
      // Derived entry points ride the richest rung only. A failing lift must cost
      // only that lift, not every binding's views: when the richest rung compiles
      // without lifts, they are re-added one at a time in dependency order,
      // keeping each that compiles. Otherwise the lifts were not the cause and
      // the ladder below runs without them.
      let lifts = this.liftedDerivedSources(enriched);
      if (lifts.length > 0) {
         const probe = async (candidate: DerivedSourceLift[]) => {
            const m = this.buildServeShapeMaterializer(
               enriched,
               tiers[0].groups,
               candidate,
            );
            try {
               await m.getModel();
               return m;
            } catch (err) {
               return err instanceof Error ? err : new Error(String(err));
            }
         };
         const withAll = await probe(lifts);
         if (!(withAll instanceof Error)) return withAll;
         const withNone = await probe([]);
         const kept: DerivedSourceLift[] = [];
         let best: ModelMaterializer | undefined;
         if (!(withNone instanceof Error)) {
            best = withNone;
            for (const lift of lifts) {
               const attempt = await probe([...kept, lift]);
               if (attempt instanceof Error) continue;
               kept.push(lift);
               best = attempt;
            }
         }
         if (kept.length < lifts.length) recordServeShapeTierDrop("lifts");
         logger.warn(
            "Storage serve shape failed to compile with its lifted entry points; the ones that do not compile serve live",
            {
               model: this.host.modelPath(),
               dropped: lifts
                  .filter((l) => !kept.includes(l))
                  .map((l) => l.sourceName),
               error: withAll.message,
            },
         );
         if (best) return best;
         lifts = [];
      }
      const lastTier = tiers.length - 1;
      for (let tier = 0; tier <= lastTier; tier++) {
         const { keep, groups } = tiers[tier];
         const shaped =
            tier === 0
               ? enriched
               : enriched.map((b) =>
                    b.refinements
                       ? {
                            ...b,
                            refinements: b.refinements.filter((r) =>
                               keep.has(r.kind),
                            ),
                         }
                       : b,
                 );
         // Lifts (those that survived the probe above) ride tier 0 only.
         const materializer = this.buildServeShapeMaterializer(
            shaped,
            groups,
            tier === 0 ? lifts : [],
         );
         // The floor (virtual bases plus filters) can still fail on a filter that
         // cannot be reproduced. A failure withholds the offending bindings —
         // never thins their filters, which would serve unfiltered.
         if (tier === lastTier) {
            try {
               await materializer.getModel();
               return materializer;
            } catch (err) {
               if (isRetry) return materializer;
               const servable = await this.bindingsWhoseFiltersCompile(
                  enriched,
                  keep,
               );
               const withheld = enriched
                  .filter((b) => !servable.includes(b))
                  .map((b) => b.sourceName);
               if (servable.length === 0 || withheld.length === 0) {
                  // Nothing left to serve, or every binding compiles alone so the
                  // failure is in their COMBINATION (e.g. a duplicate source
                  // name). Either way the model is served live.
                  logger.warn(
                     "Storage serve shape failed at its floor and no binding could be isolated; serving live",
                     {
                        model: this.host.modelPath(),
                        error: err instanceof Error ? err.message : String(err),
                     },
                  );
                  return materializer;
               }
               logger.warn(
                  "Withheld storage serve bindings whose filters cannot be reproduced; those sources serve live",
                  {
                     model: this.host.modelPath(),
                     withheld,
                     stillServed: servable.map((b) => b.sourceName),
                     error: err instanceof Error ? err.message : String(err),
                  },
               );
               // Re-enter the LADDER rather than returning this floor shape, so
               // the survivors keep their joins, views and rollups.
               return await this.compileServeShape(
                  servable,
                  rollupGroups,
                  true,
               );
            }
         }
         // Nothing to escalate means tier 0 IS the floor's shape.
         if (tier === 0 && nothingToEscalate) {
            return materializer;
         }
         try {
            await materializer.getModel();
            return materializer;
         } catch (err) {
            recordServeShapeTierDrop(tier);
            logger.warn(
               "Storage serve shape failed to compile; dropping the riskiest refinement category and retrying",
               {
                  model: this.host.modelPath(),
                  tier,
                  error: err instanceof Error ? err.message : String(err),
               },
            );
         }
      }
      // Unreachable: the last tier returns above. Mirrors that tier (filters
      // kept) so it can never become an unfiltered serve.
      const floor = tiers[tiers.length - 1].keep;
      return this.buildServeShapeMaterializer(
         enriched.map((b) => ({
            ...b,
            refinements: (b.refinements ?? []).filter((r) => floor.has(r.kind)),
         })),
         [],
      );
   }

   /**
    * The bindings whose own filters the serve shape can reproduce, found by
    * compiling each alone at the floor (base plus filters). Only filters survive
    * to this tier, so a failure here is a filter failure; failing bindings are
    * withheld (served live), never thinned (served unfiltered).
    *
    * Known limit: the floor carries no joins, so a filter reaching through a
    * join whose target IS materialized fails this solo probe and is
    * over-withheld. Fails safe. Fixing it means probing each binding with its
    * join-dependency closure (`orderBindingsByJoinDeps` computes that ordering).
    */
   private async bindingsWhoseFiltersCompile(
      enriched: ServeBinding[],
      floorKeep: ReadonlySet<string>,
   ): Promise<ServeBinding[]> {
      const servable: ServeBinding[] = [];
      for (const binding of enriched) {
         const floored = {
            ...binding,
            refinements: (binding.refinements ?? []).filter((r) =>
               floorKeep.has(r.kind),
            ),
         };
         try {
            await this.buildServeShapeMaterializer([floored], []).getModel();
            servable.push(binding);
         } catch {
            // Withheld: its filters do not reproduce.
         }
      }
      return servable;
   }

   /**
    * This model's given surface (imports already collapsed), so the transient
    * shape accepts exactly the givens a live query binds against.
    *
    * A given whose type cannot be rendered is DROPPED rather than guessed: a
    * `where:` reading it then fails to compile and serves live, whereas a
    * guessed type could compile a filter that silently coerces.
    */
   private serveShapeGivens(): ServeShapeGiven[] {
      const out: ServeShapeGiven[] = [];
      for (const given of this.host.givens() ?? []) {
         if (typeof given.name !== "string" || given.name.length === 0)
            continue;
         if (typeof given.type !== "string" || given.type.length === 0)
            continue;
         out.push({
            name: given.name,
            type: given.type,
            defaultText:
               typeof given.default === "string" ? given.default : undefined,
         });
      }
      return out;
   }

   /** Build the transient serve-shape materializer for a set of bindings. */
   private buildServeShapeMaterializer(
      bindings: ServeBinding[],
      rollupGroups: RollupShapeGroup[] = [],
      /**
       * Non-persisted sources to carry over these bindings. Defaulted empty so
       * probes compile the bindings ALONE: a failing lift must not withhold a
       * binding that serves fine on its own.
       */
      derived: DerivedSourceLift[] = [],
   ): ModelMaterializer {
      const { modelText } = buildServeShapeModelForBindings(
         bindings,
         rollupGroups,
         this.serveShapeGivens(),
         derived,
         // The flags of every file whose text the shape carries: a lift's
         // declaration, and a bound source's re-emitted views and joins.
         documentFlagsForLifts(
            [
               ...bindings.map((b) => ({ sourceName: b.sourceName })),
               ...derived,
            ],
            this.authorModelLift(),
         ),
      );
      const root = "file:///storage-serve-shape/";
      const url = `${root}shape.malloy`;
      const runtime = new Runtime({
         urlReader: new InMemoryURLReader(new Map([[url, modelText]])),
         // Narrowed to the destinations THIS shape names, so the shape cannot
         // reach any other warehouse. Rollup members are not in `bindings` but
         // must be included, or the composite fails with "Cannot determine
         // dialect for connection" and silently serves live.
         config: restrictMalloyConfigToConnections(
            this.serveDestinationConfig!(),
            new Set([
               ...bindings.map((binding) => binding.destinationName),
               ...rollupGroups.flatMap((group) =>
                  group.members.map((member) => member.destinationName),
               ),
            ]),
         ),
      });
      return runtime.loadModel(new URL(url), {
         importBaseURL: new URL(root),
      });
   }

   /**
    * The compiled-model facts the serve shape is assembled from: this model's
    * `contents`, a sourceID index over it, and a reader that recovers a
    * declaration's verbatim text from the author's file by location. Shared by
    * the refinement extraction and the derived-source lift so both read ONE view
    * of the model; the file cache lives per call.
    */
   private authorModelLift(): {
      contents: Record<string, DerivedSourceDef & { sourceID?: unknown }>;
      sourceNameById: Map<string, string>;
      liftText: (location: SourceLocation) => string | undefined;
      fileText: (url: string) => string | undefined;
   } {
      return authorModelLiftContext(this.host.modelDef(), (url) => {
         try {
            return readFileSync(fileURLToPath(url), "utf8");
         } catch {
            return undefined;
         }
      });
   }

   /**
    * The non-persisted sources that can be carried onto the shape over the
    * supplied bindings — the entry points a caller's own term lives on. Carried
    * at the richest tier only (see {@link compileServeShape}), so a lift that
    * does not compile costs nothing else.
    */
   private liftedDerivedSources(bindings: ServeBinding[]): DerivedSourceLift[] {
      const lift = this.authorModelLift();
      const { contents, sourceNameById, liftText } = lift;
      // Candidates: the model's namespace plus what those sources derive from
      // (including hidden import dependencies). A source nothing in the
      // namespace reaches is not a candidate, so it cannot withhold the shape.
      const namespace = Object.keys(
         (
            this.host.modelDef() as
               | { contents?: Record<string, unknown> }
               | undefined
         )?.contents ?? {},
      );
      const candidateNames = new Set(namespace);
      for (const name of namespace) {
         for (const reached of reachedPersistedSources(lift, name, () => false)
            .visited) {
            candidateNames.add(reached);
         }
      }
      return liftDerivedSources({
         contents,
         sourceNameById,
         candidateNames,
         // Bases a lift may extend: the FRESH bindings it is handed.
         shapeSourceNames: new Set(bindings.map((b) => b.sourceName)),
         // Candidates are excluded against EVERY binding, including the ones
         // freshness withheld — see `boundSourceNames`.
         boundSourceNames: new Set(this.serveBindings.map((b) => b.sourceName)),
         liftText,
      });
   }

   /**
    * The serve bindings enriched with the refinements to re-declare on each
    * virtual base: dimensions/measures, joins whose target is ALSO materialized,
    * and views. Analytic source-fields are not carried; a query using one serves
    * live. Join and view text is lifted verbatim from the author's files
    * (best-effort; an unreadable file drops that source's joins/views). Views are
    * emitted optimistically and pruned by {@link compileServeShape}.
    */
   private serveBindingsWithRefinements(
      // No default: rollups must never reach this function (their source name
      // resolves to nothing in the author's model), so every caller states
      // which set it means.
      bindings: ServeBinding[],
   ): ServeBinding[] {
      const { contents, sourceNameById, liftText } = this.authorModelLift();
      // Narrow the declared ::Shape to the source's PUBLIC columns. The build
      // materializes every projected column, including `except:`-ed and
      // access-restricted ones; declaring one would expose over storage what
      // live hides. Always applied, so the serve surface never widens.
      //
      // Then drop bindings whose public schema is empty: bindings are pushed to
      // every model in the package, so this model sees bindings for sources it
      // does not define. An empty `type: X__shape is {}` is a parse error that
      // would fail the ENTIRE shape, and dropping them also keeps a source from
      // being served through a model that does not declare it.
      //
      // Done FIRST: everything below decides against the set of sources that
      // will actually be on the shape.
      const onShape = bindings
         .map((b) => ({
            ...b,
            schema: narrowSchemaToPublic(
               b.schema,
               contents?.[b.sourceName]?.fields,
            ),
         }))
         .filter((b) => b.schema.length > 0);
      const { kept, withheld } = withholdUnreproducibleCallerScopedJoins(
         onShape,
         contents,
         sourceNameById,
      );
      if (withheld.length > 0) {
         logger.warn(
            "Withheld storage serve bindings whose caller-scoped join cannot be reproduced; those sources serve live",
            { model: this.host.modelPath(), withheld },
         );
      }
      const materializedSourceNames = new Set(kept.map((b) => b.sourceName));
      // Same assembly the chained build uses for its parents.
      return kept.map((b) => ({
         ...b,
         refinements: authorRefinementsFor(
            b.sourceName,
            { contents, sourceNameById, liftText },
            materializedSourceNames,
         ),
      }));
   }
}
