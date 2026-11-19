// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The run's compiled view of a package's plan ({@link RunPlan}), and the one
 * door from `run/` into `plan/`: the run compiles its plan here
 * ({@link compileRunPlan}) and reads every per-source derivation the plan
 * owns through the names below, so the run depends on the planner in one
 * module and nowhere else.
 *
 * Today a RunPlan is the planner's own compile of the package's files on
 * the thread that builds (its live connections, `PersistSource`s and key
 * check compilers). It becomes the compile of the package's plain-data plan
 * once the package-load worker sends one; the run reads it unchanged.
 */

import type { CompiledBuildPlan } from "../domain/compiled_build_plan";
import {
   compilePackageBuildPlan,
   runLoadGateText,
} from "../plan/build_plan_compile";

/** The run's compiled view of a package's plan. */
export type RunPlan = CompiledBuildPlan;

/** What a run compiles its plan from: the package as loaded. */
type RunPlanPackage = Parameters<typeof compilePackageBuildPlan>[0];

/** A RunPlan as compiled, with the annotation findings the load gate reads. */
type CompiledRunPlan = Awaited<ReturnType<typeof compilePackageBuildPlan>>;

/** Compile `pkg`'s plan for a run on this thread. */
export function compileRunPlan(
   pkg: RunPlanPackage,
   signal?: AbortSignal,
): Promise<CompiledRunPlan> {
   return compilePackageBuildPlan(pkg, signal);
}

/**
 * The load gate's own text over a run's plan, so the run refuses what the
 * package's load refuses; empty when it refuses nothing.
 */
export function runPlanGateText(
   plan: CompiledRunPlan,
   pkg: RunPlanPackage,
   judgement: { versionScoped: boolean; enforceCollisions: boolean },
): string {
   return runLoadGateText(plan, pkg, judgement);
}

// The plan's per-source derivations the run reads: addresses, build SQL,
// annotation fields, stored columns, and the gates a source is held to.
export { buildSQLOf, presentAs } from "../plan/addressing";
export {
   addressSQL,
   collectIncrementalDeclarations,
   compilerBuildId,
   computeSourceEntityId,
   deriveAnnotationFields,
   deriveColumns,
   foldDistinctTableNames,
   planAddresses,
   plannedSources,
   projectToStoredColumns,
   resolveQueryMetadata,
   sharedTables,
   storedColumnNames,
   storedColumnOf,
   tierGateInputs,
   type TableFolds,
} from "../plan/build_plan";
export { generatedNameOwner } from "../plan/build_file_generator";
export {
   assertMaterializationEligible,
   assertPlacementNamed,
   assertTierEligible,
} from "../plan/materialization_eligibility";
export { extensionOverStoredModel } from "../plan/materialize_stored_chain";
export { declaredStorage } from "../plan/persist_source_fields";
