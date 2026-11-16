// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Compiling a package's build plan on the main thread, against the package's
 * live connections: the build path (MaterializationService) and a load whose
 * plan the package-load worker left at its soft deadline. It compiles each
 * model through `GeneratedFiles` over the package's files on disk, where the
 * worker compiles from its own file map.
 */

import { BuildCancelledError } from "../domain/build_cancelled";
import type { KeyCheckCompiler } from "../domain/key_check";
import { modelDefOf, primaryKeyOf } from "../../governance/ir/compiled_model";
import { MODEL_FILE_SUFFIX } from "../../../constants";
import { logger } from "../../../logger";
import { recordConnectionDigestSkipped } from "../../../materialization_metrics";
import {
   collectModelBuildPlan,
   deriveBuildPlanOutcome,
   emptyBuildPlanParts,
   finishBuildPlan,
   resolveConnectionDigests,
   resolvePackageConnections,
   type BuildPlanOutcome,
   type BuildPlanPackage,
   type FinishedBuildPlan,
} from "./build_plan";
import { loadGateText } from "./load_gate_text";
import {
   joinOneKey,
   joinOneKeysOf,
   joinedKeyNeedsOf,
   parentKeyNeedsOf,
   storedKeyChecksOf,
   type JoinedKeyNeed,
   type ParentKeyNeed,
} from "./rollup_plan";
import type { RollupVariantPlan } from "../domain/table_plans";
import {
   compileParentKeyChecks,
   parentKeyCheckCompiler,
} from "./rollup_compile";
import { SourceMap } from "../text/source_map";
import { createEventLoopYielder } from "../../event_loop_yield";
import { packageFiles } from "../text/package_files";

export async function compilePackageBuildPlan(
   pkg: BuildPlanPackage,
   signal?: AbortSignal,
   {
      keepIncrementalUnbuildable = false,
   }: {
      /**
       * Keep a source whose valid incremental declaration this deployment
       * cannot honour (incremental_policy's rules 6, 7 and 12), rather than
       * refuse it alone. For a spec that plans incremental machinery on
       * DuckDB, which no load or run does.
       */
      keepIncrementalUnbuildable?: boolean;
   } = {},
): Promise<FinishedBuildPlan> {
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
   // Each `#(materialize)` uniqueness check's compiler over a run's
   // compiler manifest: this plan is built on the thread that runs builds.
   const keyCheckCompilers = new Map<string, KeyCheckCompiler>();
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
      if (signal?.aborted) throw new BuildCancelledError();

      const authorFiles = packageFiles(
         pkg.getPackagePath(),
         modelPath,
         pkg.getMalloyConfig(),
      );
      // Held onto (rather than chained straight into `.getModel()`)
      // because the gate classification needs the SAME live materializer
      // that compiled this model — see `classifyPersistSourceGate`'s doc.
      const { runtime, materializer } = await authorFiles.loadModel();
      const malloyModel = await materializer.getModel();
      const map = SourceMap.of(modelDefOf(malloyModel));
      for (const key of joinOneKeysOf(map)) {
         joinOneKeys.add(key);
      }
      for (const node of map.nodes()) {
         const key = primaryKeyOf(node.def);
         if (key !== undefined && !primaryKeys.has(node.sourceID)) {
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
         getRuntime: (overlay) => authorFiles.runtime(overlay),
         keyCheckCompilers,
      });
   }

   // A rollup key check no join_one of the package reads is not run: it
   // would scan the base for nothing (known limitation 29).
   const rollupPlans = Object.values(parts.tables).flatMap((table) =>
      table.rollup ? [table.rollup as RollupVariantPlan] : [],
   );
   for (const rollup of rollupPlans) {
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
   await compileParentKeyChecks(rollupPlans, parentKeyNeeds, joinedKeyNeeds);
   // A `storage=` rollup read that way is read beside the query source's
   // stored table: each stored source's key is checked over its
   // table at its own build instead.
   const storedKeyChecks = storedKeyChecksOf(
      rollupPlans,
      parentKeyNeeds.map(({ need }) => need),
      (sourceID) => primaryKeys.get(sourceID),
      Object.keys(parts.tables).filter(
         (sourceID) => !parts.tables[sourceID].rollup,
      ),
   );
   for (const [sourceID, keyColumn] of Object.entries(storedKeyChecks)) {
      parts.tables[sourceID].storedKeyCheck = keyColumn;
   }

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
   // refused for its table name, or a source whose incremental declaration
   // this deployment cannot honour, must not build here either.
   return finishBuildPlan(parts, {
      connectionDigests,
      connections,
      versionScoped: versionOwnsTables(pkg),
      keyCheckCompilers,
      keepIncrementalUnbuildable,
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

/**
 * The load gate's text for a plan a run compiled (plan/load_gate_text.ts),
 * read off the plan the run derives so the run and the load agree: empty
 * when the files carry no invalid materialization annotation.
 */
export function runLoadGateText(
   compiled: FinishedBuildPlan,
   pkg: BuildPlanPackage,
   judgement: { versionScoped: boolean; enforceCollisions: boolean },
): string {
   const derived = deriveBuildPlanOutcome(
      compiled,
      pkg.getMaterializationConfig?.() ?? null,
   );
   return loadGateText(
      {
         plan: derived.plan,
         annotationFindings: derived.annotationFindings,
         incrementalDeclarations: derived.incrementalDeclarations,
         materializePlans: derived.materializePlans,
         modelGateLines: derived.modelGateLines,
      },
      judgement,
   );
}

/**
 * Whether the package's tables are named per version: a published version
 * of a `scope: "version"` package (the run's `ownedVersionOf`). An
 * unversioned package writes an author's `name=` as given whatever its scope.
 */
function versionOwnsTables(pkg: BuildPlanPackage): boolean {
   return (
      pkg.getPackageMetadata?.()?.scope === "version" &&
      pkg.getVersionId?.() !== undefined
   );
}
