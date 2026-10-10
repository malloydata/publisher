// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Compiling a package's build plan on the main thread, against the package's
 * live connections: the build path (MaterializationService) and any load the
 * package-load worker did not derive a plan for. Kept apart from
 * `build_plan.ts` because it compiles through `Model.getModelRuntime`, and the
 * worker, which imports `build_plan.ts`, must not load `./model`.
 */

import { MODEL_FILE_SUFFIX } from "../constants";
import { logger } from "../logger";
import { recordConnectionDigestSkipped } from "../materialization_metrics";
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
import { Model } from "./model";

export async function compilePackageBuildPlan(
   pkg: BuildPlanPackage,
   signal?: AbortSignal,
): Promise<CompiledBuildPlan> {
   const parts = emptyBuildPlanParts();

   for (const modelPath of pkg.getModelPaths()) {
      // Only `.malloy` models declare persist sources. Skip `.malloynb`
      // notebooks: getModel() parses a model file as a flat model and throws on
      // the notebook's `>>>` cell delimiter, which would abort the entire
      // package build plan and silently drop every persist source in it.
      if (!modelPath.endsWith(MODEL_FILE_SUFFIX)) continue;
      if (signal?.aborted) throw new Error("Build cancelled");

      const { runtime, modelURL, importBaseURL } = await Model.getModelRuntime(
         pkg.getPackagePath(),
         modelPath,
         pkg.getMalloyConfig(),
      );
      // Held onto (rather than chained straight into `.getModel()`) because
      // the gate classification needs the SAME live materializer that
      // compiled this model — see `classifyPersistSourceGate`'s doc.
      const materializer = runtime.loadModel(modelURL, { importBaseURL });
      const malloyModel = await materializer.getModel();
      await collectModelBuildPlan(parts, {
         modelPath,
         packagePath: pkg.getPackagePath(),
         materializer,
         malloyModel,
         getRuntime: (overlay) =>
            Model.getModelRuntime(
               pkg.getPackagePath(),
               modelPath,
               pkg.getMalloyConfig(),
               { overlay },
            ),
      });
   }

   const connections = await resolvePackageConnections(
      pkg,
      parts.graphs.map((g) => g.connectionName),
   );
   const connectionDigests = await resolveConnectionDigests(
      connections,
      parts.graphs,
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

   return {
      graphs: parts.graphs,
      sources: parts.sources,
      connectionDigests,
      connections,
      sourceModelPaths: parts.sourceModelPaths,
      droppedPersistSources: parts.droppedPersistSources,
      preaggregatePlans: parts.preaggregatePlans,
      sourceGateOutcomes: parts.sourceGateOutcomes,
   };
}

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
