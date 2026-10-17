// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

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
    * Connection config to compile a transient serve-shape model against, when a
    * query is routed through the `storage=` virtual-source transform. Captured
    * at hydration (fromSerialized) so serve can build a fresh Runtime.
    */
   /**
    * The connections a serve-shape compile may resolve: this environment's
    * storage destinations. Deliberately NOT the config this model itself
    * compiles against — a destination must not be resolvable from the author's
    * model, a notebook cell, or a query, and the serve shape is a separate
    * synthetic model, so the two compiles can and must read disjoint name sets.
    * Absent ⇒ no storage serve routing (queries serve live).
    */
   serveDestinationConfig?: () => MalloyConfig;
   /**
    * The package's `storage=` serve bindings, set by the owning Package when a
    * build/manifest binds materialized-into-storage sources. Empty ⇒ no serve
    * routing (the common case; the serve path is unchanged).
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
      // The reachable connection set is part of what the memoized materializer
      // was compiled against, so a change invalidates it.
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

   /**
    * Set (or clear) this model's `storage=` serve bindings. Invalidates the
    * memoized serve-shape materializer so the next routed query recompiles
    * against the new binding set.
    */
   public setServeBindings(bindings: ServeBinding[]): void {
      this.serveBindings = bindings;
      this.serveShapeCache = undefined;
   }

   /**
    * Compile an untrusted query against the transient serve-shape model that
    * rebinds this package's materialized-into-storage sources to virtual sources
    * on their storage connection. Returns the runnable and the `virtualMap` that
    * binds each virtual handle to its physical table.
    *
    * Throwing is the eligibility signal: a query that references a refinement
    * the serve shape does not reproduce (a measure/dim/join defined on the
    * source in the author's model, or a source with no binding) fails to compile
    * here, and {@link getQueryResults} falls back to serving it live. The
    * compiled materializer is memoized per binding set (the serve-variant cache).
    */
   /**
    * The serve bindings that may serve from their materialized table right NOW:
    * fresh, un-gated, or stale-under-`stale_ok`. A stale binding whose fallback is
    * `live`/`fail` evaluates to `serve_live` and is dropped, so the serve shape
    * omits that source and any query touching it falls through to live — the SAME
    * freshness gate the colocated serve applies (`getFreshBuildManifest`
    * → `evaluateManifestFreshness`). Placement (`storage=`) is orthogonal to
    * freshness: flip a colocated build ↔ `storage=lake` and this behaves identically.
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

   async loadServeShapeQuery(
      queryString: string,
      /**
       * The request's given values, passed to the origin probe below.
       *
       * The probe compiles the query to decide whether a rollup answered it, and
       * compiling resolves every given the shape declares. A given with no
       * DEFAULT — which is every `#(secure)` attribute, since a scalar cannot be
       * secure and a set-valued one is server-set — has nothing to resolve to at
       * that point, so the probe threw and the whole tier fell back to live for
       * exactly the sources that carry a row-level boundary.
       */
      givens?: Record<string, GivenValue>,
   ): Promise<{
      runnable: QueryMaterializer;
      virtualMap: VirtualMap;
      /**
       * The fresh bindings the shape was built from. Returned so a caller making
       * a per-binding decision reads the set that actually produced this shape
       * rather than the package-wide field.
       */
      bindings: ServeBinding[];
      /**
       * Whether a pre-aggregation rollup actually ANSWERED this query, rather than
       * merely being available to. Read from the probe SQL: a rollup's physical
       * table appears in it only if the composite resolver chose that member.
       *
       * Guessing from "the shape contains rollup groups" would over-report every
       * query that touched an ordinary stored source in the same package, which
       * would defeat the reason the distinction is recorded at all.
       */
      origin: "persist" | "preaggregate";
   }> {
      // Gate by freshness first: only bindings that should serve their table now
      // enter the shape. Keying the cache on the FRESH subset means it recompiles
      // when a binding crosses its window (drops out) — the storage analogue of
      // the colocated path's memoized getFreshBuildManifest.
      const freshBindings = this.freshServeBindings(Date.now());
      if (freshBindings.length === 0) {
         // Every bound source is stale past its window with a live/fail fallback —
         // nothing to serve from storage; fall through to live (caller's catch).
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
      // Split by origin. A rollup CANNOT go through serveBindingsWithRefinements:
      // its source name names nothing in the author's model, so the refinement
      // lift finds no fields and the schema narrowing intersects to nothing — see
      // preaggregation_serve_bindings.ts for what each half does to a rollup, and
      // why getting it wrong costs the whole package's storage serving rather than
      // the rollup's.
      const ordinary = freshBindings.filter((b) => b.origin !== "preaggregate");
      const { groups, conflicts } = rollupServeBindings(
         freshBindings,
         this.host.rollupPlans(),
      );
      if (!this.serveShapeCache || this.serveShapeCache.key !== key) {
         for (const conflict of conflicts) {
            // info, matching the storage tier's own fallback line: a dropped group
            // is silent by design — the query succeeds, the rows are correct, only
            // the acceleration is lost — so at debug an operator has no way to tell
            // a package whose rollups never serve from one that has none.
            //
            // Inside the cache-miss branch, not beside it. A conflict is a property
            // of the binding set rather than of a query, so logging it per query
            // would repeat one unchanging line for every request against the
            // package — noise that buries the fallback lines that ARE per query.
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
      // Rollup members are not in `ordinary`, so their handles have to be mapped
      // explicitly or the composite resolves to nothing at run time.
      const virtualMap = buildVirtualMap([
         ...ordinary,
         ...groups.flatMap((g) => g.members),
      ]);

      await this.host.ensureSharedQueryModel(this.serveShapeCache.materializer);
      const runnable =
         this.serveShapeCache.materializer.loadRestrictedQuery(queryString);
      // Compile eagerly so ineligibility (a refinement the serve shape lacks, an
      // unbound source, a bad connection) surfaces HERE — Malloy compiles lazily,
      // so without this the error would escape at prepare/run instead of at the
      // caller's try, defeating the safe fallback. Cheap relative to the run. The
      // serve shape is pure virtual sources, so no buildManifest is needed.
      const probeSQL = await runnable.getSQL({ virtualMap, givens });
      // The rollup members' physical paths, quoted exactly as the virtualMap
      // substitutes them, so this compares like with like rather than re-deriving
      // the quoting and drifting from it.
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
    * gracefully if the richest shape does not compile. A single un-carriable
    * refinement (a join or view that reaches a non-materialized source, a
    * non-portable expression) would otherwise fail the WHOLE shape's compile and
    * disable storage serving for every source in the package. So the shape is
    * validated once (per binding set — the result is cached) and, on failure,
    * the riskiest refinement category is dropped and it retries: full → drop
    * views → drop views + joins → base-only. Base-only carries the virtual
    * sources and their filters, so it compiles for every source whose filters
    * can be reproduced — it is the floor, but no longer a guaranteed one: a
    * filter that cannot be reproduced fails every tier, and the caller then
    * serves live rather than serving unfiltered. Each surviving tier
    * still serves everything it can; the per-query eager compile in
    * {@link loadServeShapeQuery} remains the final net for query-specific
    * ineligibility.
    */
   private async compileServeShape(
      enriched: ServeBinding[],
      /**
       * Pre-aggregation groups, passed through every escalation tier UNCHANGED.
       * The escalation exists to salvage a shape when an author's refinement
       * cannot be reproduced; a rollup's measures are generated from its own plan
       * and are the only reason its member exists, so dropping them would not
       * rescue anything — it would leave a member that compiles and answers
       * nothing.
       */
      rollupGroups: RollupShapeGroup[] = [],
      /**
       * Set on the one re-entry this method makes after withholding bindings
       * whose filters cannot be reproduced. It bounds the recursion to a single
       * extra pass: a floor failure on the retry is answered by serving live
       * rather than by isolating again.
       */
      isRetry = false,
   ): Promise<ModelMaterializer> {
      // The ladder, richest first. Built by {@link buildServeShapeTiers} rather
      // than here so the invariant it must hold — every tier keeps the
      // never-thinned kinds — is assertable in one place. `filter` is one of
      // those: the other kinds are optimizations, while a source's `where:` is
      // part of what the source MEANS, so a tier that dropped it would answer
      // with rows the source excludes.
      const tiers = buildServeShapeTiers(rollupGroups);
      // Skip escalation entirely when nothing beyond the base is carried — but a
      // GROUP is something beyond the base. Reading this off `enriched` alone left
      // the pure-rollup case (no ordinary bindings at all) returning tier 0
      // unprobed, so a broken group was not detected when the shape was built and
      // surfaced instead as a per-query live fallback for everything in the
      // package.
      const nothingToEscalate =
         rollupGroups.length === 0 &&
         !enriched.some((b) => (b.refinements ?? []).length > 0);
      // Derived entry points ride the richest rung only, and are probed there
      // first. A lift the eligibility checks passed can still fail to compile (a
      // field path through a join of its base the shape does not carry), and a
      // lift failure answered by the ordinary ladder would cost every binding its
      // views. So a failing lift costs that lift: when the richest rung compiles
      // without the lifts, they are re-added one at a time, in dependency order,
      // keeping each that compiles. When it does not compile without them either,
      // the lifts were not the cause and the ladder below runs without them.
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
         // Derived entry points ride the richest rung only, and only if they
         // survived the probe above. Every rung below is byte-identical to the
         // shape this package compiled before lifting existed.
         const materializer = this.buildServeShapeMaterializer(
            shaped,
            groups,
            tier === 0 ? lifts : [],
         );
         // The last tier is virtual bases plus their filters. Unlike the tiers
         // above it, it can fail: a filter that cannot be reproduced (one
         // reaching through a join whose target is not materialized, or over a
         // column the source hides) fails every tier including this one. It is
         // therefore PROBED, and a failure is answered by withholding the
         // offending bindings — never by thinning their filters, which is the
         // unfiltered serve this ladder exists to prevent.
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
                  // failure is in their COMBINATION — a duplicate source name is
                  // the reachable one, since the floor carries no joins and so
                  // cannot have an ordering cycle. Either way the model is served
                  // live.
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
               // Re-enter the LADDER rather than returning this floor shape: the
               // survivors did nothing wrong, and rebuilding them here would cost
               // every one of them its joins, views and rollups because a
               // sibling's filter was unservable. Same rationale as the
               // group-dropping rung — a failure should cost its own cause and
               // nothing else.
               return await this.compileServeShape(
                  servable,
                  rollupGroups,
                  true,
               );
            }
         }
         // Nothing to escalate means tier 0 IS the floor's shape, and the loop
         // would otherwise probe a shape it cannot improve on.
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
      // Unreachable: the last tier returns above. Satisfy the type checker.
      // Groups dropped and the never-thinned kinds kept, matching what that last
      // tier is — stripping every refinement here would reintroduce the
      // unfiltered serve this ladder exists to prevent.
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
    * compiling each alone at the floor — base plus filters, the smallest shape a
    * binding can be served from. One compile per binding, on a failure path whose
    * result is cached.
    *
    * Probed at the FLOOR on purpose: a binding whose view cannot be reproduced
    * still compiles here and is kept, because thinning is the right answer for a
    * view and the ladder above already does it. Only filters survive to this
    * tier, so a failure here is a filter failure.
    *
    * Known limit, and the reason that reasoning does not fully generalise: the
    * floor carries no joins, so a filter reaching through a join whose target IS
    * materialized fails this solo probe even though it compiles at the richer
    * tiers. Such a binding is over-withheld and serves live. Fails safe, and no
    * worse than before filters were carried at all, but it is why a source can
    * lose the tier to a sibling it has nothing to do with. Fixing it means
    * probing a binding together with its join-dependency closure rather than
    * alone — `orderBindingsByJoinDeps` already computes that ordering — which is
    * more machinery than the case has so far warranted.
    *
    * Withholding, never thinning: a binding that does not compile is absent from
    * the shape, so queries on it fail to resolve and serve live. Thinning its
    * filters instead would serve it unfiltered, which is the defect this whole
    * mechanism exists to prevent.
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
            // Withheld: its filters do not reproduce, so it cannot be served.
         }
      }
      return servable;
   }

   /**
    * This model's given surface, in the shape the serve-shape model declares.
    *
    * `Model.givens` has already collapsed inheritance from imports, so this is
    * the same surface a live query binds against — which is the point: the
    * transient shape should accept exactly the givens the author's model
    * accepts, no more and no less.
    *
    * A given whose type cannot be rendered is DROPPED rather than guessed. The
    * cost is bounded and safe: a re-emitted `where:` that reads it then fails to
    * compile, the binding is withheld, and the query serves live. Guessing a
    * type would instead compile a filter that silently coerces.
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
       * every probe path — the per-binding filter probe especially — compiles
       * the bindings ALONE: a lift failing there would withhold a binding that
       * serves perfectly well on its own.
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
         // Narrowed to the destinations THIS shape names. The generated text
         // references nothing else, so anything else resolving would only ever be
         // a way to reach a warehouse this query has no business reaching.
         //
         // Rollup members count. They are not in `bindings` — a rollup reaches the
         // shape through its group, under a name nothing queries — so narrowing on
         // `bindings` alone leaves their destination out and the composite fails to
         // compile with "Cannot determine dialect for connection", which reads as
         // an ineligible query and falls back to live.
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
    * The serve bindings enriched with the refinements to re-declare on each
    * virtual base — the source's dimensions/measures (computed from the stored
    * columns), its joins whose target source is ALSO materialized (the join runs
    * over the stored tables), and its views (turtles). All are read from this
    * model's compiled definition. Analytic source-fields are not carried; a
    * query using one falls back to live.
    *
    * Join and view declaration text is lifted verbatim from the author's source
    * files by location (read once per file, best-effort — an unreadable file
    * just drops that source's joins/views, which fall back). The join
    * materialization gate is a `sourceID`-keyed lookup, so it never depends on
    * parsing that text; views are emitted optimistically and pruned by the
    * shape-compile escalation in {@link compileServeShape} if they don't hold.
    */
   /**
    * The compiled-model facts the serve shape is assembled from: this model's
    * `contents`, a sourceID index over it, and a reader that recovers a
    * declaration's verbatim text from the author's file by location.
    *
    * Shared by the refinement extraction and the derived-source lift so the two
    * read ONE view of the model. The file cache lives per call, which is what
    * keeps a package's sources from re-reading the same file once each.
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
    * supplied bindings — the entry points a caller's own term lives on.
    *
    * Carried at the RICHEST tier only (see {@link compileServeShape}). That is
    * what makes this strictly additive: a lift that does not compile costs its
    * own rung and nothing else, and every tier below is the shape this package
    * already got, so a package serving today serves identically if the lift
    * fails.
    */
   private liftedDerivedSources(bindings: ServeBinding[]): DerivedSourceLift[] {
      const lift = this.authorModelLift();
      const { contents, sourceNameById, liftText } = lift;
      // Only what a query against this model can name — its namespace — and
      // what those sources derive from. The lift context also holds the
      // model's hidden dependencies (sources an import brought in without
      // naming them), so a base reached through an import is still carried
      // when a namespace source needs it; one nothing in the namespace
      // reaches is not a candidate, and cannot withhold the shape.
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

   private serveBindingsWithRefinements(
      // No default. This function resolves a binding back into the AUTHOR's model
      // — by name, for its field list — which a rollup cannot survive: its source
      // name names nothing there, so the fields come back undefined and the
      // binding is dropped. Defaulting to `this.serveBindings` would hand the
      // whole set, rollups included, to any future caller that omitted the
      // argument. Every caller states which set it means.
      bindings: ServeBinding[],
   ): ServeBinding[] {
      const { contents, sourceNameById, liftText } = this.authorModelLift();
      // Narrow the declared ::Shape to the source's PUBLIC columns: the build
      // materializes every projected column (incl. `except:`-ed /
      // access-restricted ones), so the captured schema can be wider than the
      // source's public surface. Declaring a hidden column would expose it over
      // storage when live hides it — always applied (even with no refinements),
      // so the serve surface never widens the source's.
      //
      // Then drop any binding whose public schema is empty. Bindings are pushed
      // to EVERY model in the package, so a model receives bindings for sources
      // it doesn't define (defined in a sibling model) — those have no field list
      // here, hence an empty narrowed schema. An empty `type: X__shape is {}` is
      // a Malloy parse error that would fail the ENTIRE serve-shape model
      // (breaking the base-only-always-compiles fallback invariant) and silently
      // drop storage serving for this model's own sources too. Omitting them
      // sends queries on an undefined source to live (where this model refuses
      // them anyway) and keeps a source from being served through a model that
      // doesn't declare it.
      //
      // Done FIRST, because everything below decides against the set of sources
      // that will actually be on the shape: a join is re-emitted only when its
      // target is in it, and a caller-scoped joiner is withheld when its target
      // is not.
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
      // What each source adds to its table, re-declared on the binding — the
      // same assembly the chained build uses for its parents.
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
