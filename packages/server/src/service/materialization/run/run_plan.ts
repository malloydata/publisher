// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The run's compiled view of a package's plan ({@link RunPlan}), and the one
 * door from `run/` into `plan/`: the run compiles its plan here
 * ({@link compileRunPlan}) and reads every per-source derivation the plan
 * owns through the names below, so the run depends on the planner in one
 * module and nowhere else.
 *
 * A run compiles the package's files on the thread that builds, because a
 * build needs what no plain-data plan carries: each table's live
 * `PersistSource`, the connections, and the key-check compilers. Everything
 * else it reads (build order, digests, folds, addresses) is the
 * {@link PackagePlan} the load derives from the same compile, and the run's
 * load gate reads that plan, so the run and the load judge one record.
 */

import type { Connection as MalloyConnection } from "@malloydata/malloy";
import type { KeyCheckCompiler } from "../domain/key_check";
import type { PackagePlan } from "../domain/package_plan";
import type { TablePlan } from "../domain/table_plans";
import {
   compilePackageBuildPlan,
   runLoadGateText,
} from "../plan/build_plan_compile";
import { derivePackagePlan } from "../plan/build_plan";

/**
 * The run's compiled view of a package's plan: the {@link PackagePlan}'s
 * build data, each planned table with its live source, and the connections
 * and key-check compilers a build runs with.
 */
export type RunPlan = Pick<PackagePlan, "builds" | "connectionDigests"> &
   Partial<
      Pick<
         PackagePlan,
         | "connectionTypes"
         | "versionScoped"
         | "tableFolds"
         | "addresses"
         | "asOfBuild"
      >
   > & {
      /** sourceID -> the table planned for it, with its compiled source. */
      tables: Record<string, TablePlan>;
      connections: Map<string, MalloyConnection>;
      /** Each `#(materialize)` uniqueness check's compiler, by its key. */
      keyCheckCompilers?: ReadonlyMap<string, KeyCheckCompiler>;
   };

/** What a run compiles its plan from: the package as loaded. */
type RunPlanPackage = Parameters<typeof compilePackageBuildPlan>[0];

/** A run's plan, and the package plan the load gate reads beside it. */
interface CompiledRunPlan {
   runPlan: RunPlan;
   packagePlan: ReturnType<typeof derivePackagePlan>;
}

/**
 * Compile `pkg`'s plan for a run on this thread: the package's files
 * compiled once, its {@link PackagePlan} derived from that compile as a load
 * derives it, and the run's view taken from both.
 */
export async function compileRunPlan(
   pkg: RunPlanPackage,
   signal?: AbortSignal,
): Promise<CompiledRunPlan> {
   const compiled = await compilePackageBuildPlan(pkg, signal);
   const packagePlan = derivePackagePlan(
      compiled,
      pkg.getMaterializationConfig(),
   );
   return {
      runPlan: {
         builds: packagePlan.builds,
         connectionDigests: packagePlan.connectionDigests,
         connectionTypes: packagePlan.connectionTypes,
         versionScoped: packagePlan.versionScoped,
         tableFolds: packagePlan.tableFolds,
         addresses: packagePlan.addresses,
         asOfBuild: packagePlan.asOfBuild,
         tables: compiled.tables,
         connections: compiled.connections,
         ...(compiled.keyCheckCompilers
            ? { keyCheckCompilers: compiled.keyCheckCompilers }
            : {}),
      },
      packagePlan,
   };
}

/**
 * The load gate's own text over a run's package plan, so the run refuses
 * what the package's load refuses; empty when it refuses nothing.
 */
export function runPlanGateText(
   packagePlan: CompiledRunPlan["packagePlan"],
   judgement: { versionScoped: boolean; enforceCollisions: boolean },
): string {
   return runLoadGateText(packagePlan, judgement);
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
