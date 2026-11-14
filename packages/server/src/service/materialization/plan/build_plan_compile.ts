// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Compiling a package's build plan on the main thread, against the package's
 * live connections: the build path (MaterializationService) and a load whose
 * plan the package-load worker left at its soft deadline. Kept apart from
 * `build_plan.ts` because it compiles through `Model.getModelRuntime`, and the
 * worker, which imports `build_plan.ts`, must not load `./model`.
 */

import { MODEL_FILE_SUFFIX } from "../../../constants";
import { logger } from "../../../logger";
import { recordConnectionDigestSkipped } from "../../../materialization_metrics";
import {
   collectModelBuildPlan,
   deriveBuildPlanOutcome,
   emptyBuildPlanParts,
   resolveConnectionDigests,
   resolvePackageConnections,
   type BuildPlanOutcome,
   type BuildPlanPackage,
   type CompiledBuildPlan,
} from "./build_plan";
import { refuseCollidingMaterialize } from "./eligibility_collect";
import {
   joinOneKey,
   joinOneKeysOf,
   joinedKeyNeedsOf,
   parentKeyNeedsOf,
   storedKeyChecksOf,
   type JoinedKeyNeed,
   type ParentKeyNeed,
   type RollupVariantPlan,
} from "./rollup_plan";
import {
   compileParentKeyChecks,
   parentKeyCheckCompiler,
} from "./rollup_compile";
import { SourceMap } from "../text/source_map";
import { createEventLoopYielder } from "../../event_loop_yield";
import { Model } from "../../model";
import { keyedOverlay } from "../../overlay_key";
import {
   compiledFileText,
   loadLeavingOutCrossConnectionJoins,
} from "./cross_connection_joins";

export async function compilePackageBuildPlan(
   pkg: BuildPlanPackage,
   signal?: AbortSignal,
): Promise<CompiledBuildPlan> {
   const parts = emptyBuildPlanParts();
   // Every join_one of every model, for the rollup key checks a build runs.
   const joinOneKeys = new Set<string>();
   // Every source keyed on its primary key by a join, with the model that
   // names it, for the query source key checks a rollup's build runs.
   const parentKeyNeeds: {
      need: ParentKeyNeed;
      compile: (text: string) => Promise<string>;
   }[] = [];
   // Every source a join reaches on a column other than its primary key, with
   // the model that names it, for the check of a rollup's base's own key.
   const joinedKeyNeeds: {
      need: JoinedKeyNeed;
      compile: (text: string) => Promise<string>;
   }[] = [];
   // Every declared primary key, by sourceID, for the stored-table key checks.
   const primaryKeys = new Map<string, string>();
   const yielder = createEventLoopYielder();

   for (const modelPath of pkg.getModelPaths()) {
      // Only `.malloy` models declare persist sources. Skip `.malloynb`
      // notebooks: getModel() parses a model file as a flat model and throws on
      // the notebook's `>>>` cell delimiter, which would abort the entire
      // package build plan and silently drop every persist source in it.
      if (!modelPath.endsWith(MODEL_FILE_SUFFIX)) continue;
      // Each compile below is a long synchronous stretch; give the event loop
      // a turn between models so requests are not held for the whole plan.
      // Checked after the yield, so a cancel that lands during it is seen.
      await yielder.maybeYield();
      if (signal?.aborted) throw new Error("Build cancelled");

      const load = async (files?: ReadonlyMap<string, string>) => {
         const { runtime, modelURL, importBaseURL } =
            await Model.getModelRuntime(
               pkg.getPackagePath(),
               modelPath,
               pkg.getMalloyConfig(),
               files ? { overlay: files } : undefined,
            );
         // Held onto (rather than chained straight into `.getModel()`)
         // because the gate classification needs the SAME live materializer
         // that compiled this model — see `classifyPersistSourceGate`'s doc.
         const materializer = runtime.loadModel(modelURL, { importBaseURL });
         return {
            runtime,
            materializer,
            malloyModel: await materializer.getModel(),
         };
      };
      let loaded: Awaited<ReturnType<typeof load>>;
      // The build view's files, when the model joins sources across
      // connections (#6982): its builds are planned from them.
      let buildView: Map<string, string> | undefined;
      try {
         loaded = await load();
      } catch (first) {
         const view = await loadLeavingOutCrossConnectionJoins({
            firstError: first,
            readText: (url) => compiledFileText(url),
            load: (files) => load(keyedOverlay(files)),
         });
         if (!view) throw first;
         loaded = view.result;
         buildView = keyedOverlay(view.overlay);
      }
      const { runtime, materializer, malloyModel } = loaded;
      const map = SourceMap.of(malloyModel._modelDef);
      for (const key of joinOneKeysOf(map)) {
         joinOneKeys.add(key);
      }
      for (const node of map.nodes()) {
         const key = (node.def as { primaryKey?: unknown }).primaryKey;
         if (typeof key === "string" && !primaryKeys.has(node.sourceID)) {
            primaryKeys.set(node.sourceID, key);
         }
      }
      for (const need of parentKeyNeedsOf(map)) {
         parentKeyNeeds.push({
            need,
            compile: parentKeyCheckCompiler(materializer),
         });
      }
      for (const need of joinedKeyNeedsOf(map)) {
         joinedKeyNeeds.push({
            need,
            compile: parentKeyCheckCompiler(materializer),
         });
      }
      await collectModelBuildPlan(parts, {
         modelPath,
         packagePath: pkg.getPackagePath(),
         materializer,
         malloyModel,
         runtime,
         getRuntime: (overlay) =>
            Model.getModelRuntime(
               pkg.getPackagePath(),
               modelPath,
               pkg.getMalloyConfig(),
               { overlay: new Map([...(buildView ?? []), ...overlay]) },
            ),
      });
   }

   // A rollup key check no join_one of the package reads is not run: it
   // would scan the base for nothing (known limitation 29).
   for (const plan of Object.values(parts.preaggregatePlans)) {
      const rollup = plan as RollupVariantPlan;
      if (
         rollup.keyCheck &&
         !joinOneKeys.has(
            joinOneKey(rollup.baseSourceID, rollup.keyCheck.keyColumn),
         )
      ) {
         delete rollup.keyCheck;
      }
   }

   // A query source read through a join_many keyed on its primary key, and
   // a rollup's base reached by a join on another column: the rollup's build
   // checks that key unique and never NULL.
   const rollupPlans = Object.values(
      parts.preaggregatePlans,
   ) as RollupVariantPlan[];
   await compileParentKeyChecks(rollupPlans, parentKeyNeeds, joinedKeyNeeds);
   // A `storage=` rollup read that way is read beside the query source's
   // stored table (#1704): each stored source's key is checked over its
   // table at its own build instead.
   const storedKeyChecks = storedKeyChecksOf(
      rollupPlans,
      parentKeyNeeds.map(({ need }) => need),
      (sourceID) => primaryKeys.get(sourceID),
      Object.keys(parts.sources).filter(
         (sourceID) => !parts.preaggregatePlans[sourceID],
      ),
   );

   const connections = await resolvePackageConnections(
      pkg,
      parts.builds.map((b) => b.connectionName),
   );
   const connectionDigests = await resolveConnectionDigests(
      connections,
      parts.builds,
      (connectionName) => {
         // The connection failed to resolve (already warned in
         // resolvePackageConnections). Surface it as a discrete correctness
         // signal rather than skipping silently.
         recordConnectionDigestSkipped();
         logger.warn("Skipping connection digest; connection did not resolve", {
            connectionName,
         });
      },
   );

   // The build path reads this plan directly: a `#(materialize)` source
   // refused for its table name must not build here either.
   return refuseCollidingMaterialize({
      builds: parts.builds,
      sources: parts.sources,
      targets: parts.targets,
      connectionDigests,
      connections,
      sourceModelPaths: parts.sourceModelPaths,
      droppedPersistSources: parts.droppedPersistSources,
      annotationFindings: parts.annotationFindings,
      preaggregatePlans: parts.preaggregatePlans,
      ...(Object.keys(storedKeyChecks).length > 0 ? { storedKeyChecks } : {}),
      materializePlans: parts.materializePlans,
      sourceGateOutcomes: parts.sourceGateOutcomes,
      omittedSources: parts.omittedSources,
   });
}

/**
 * Compile and project a package's build plan (null when the package declares no
 * materializable persist source), plus any `#@ persist` sources dropped from the
 * plan (see `detectDroppedPersistSources`) so the caller can surface a load-time
 * warning. Feeds the read-only `Package.buildPlan` field.
 */
export async function computePackageBuildPlan(
   pkg: BuildPlanPackage,
   signal?: AbortSignal,
): Promise<BuildPlanOutcome> {
   const compiled = await compilePackageBuildPlan(pkg, signal);
   return deriveBuildPlanOutcome(
      compiled,
      pkg.getMaterializationConfig?.() ?? null,
   );
}
