// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Compiling a package's build plan on the main thread, against the package's
 * live connections: the build path (MaterializationService), and a load whose
 * plan the package-load worker left at its soft deadline. It compiles each
 * model through `GeneratedFiles` over the package's files on disk, where the
 * worker compiles from its own file map; both finish through the one tail
 * (`finishPackagePlan`, `derivePackagePlan`).
 *
 * Only a run's plan compiles the key checks its builds run
 * ({@link compileRunKeyChecks}); no load reads them.
 */

import { BuildCancelledError } from "../domain/build_cancelled";
import type { KeyCheckCompiler } from "../domain/key_check";
import type { EligibilityRefusals, PackagePlan } from "../domain/package_plan";
import { modelDefOf, primaryKeyOf } from "../../governance/ir/compiled_model";
import { MODEL_FILE_SUFFIX } from "../../../constants";
import { logger } from "../../../logger";
import {
   recordConnectionDigestSkipped,
   recordEligibilityRefused,
} from "../../../materialization_metrics";
import type { PackageAnnotationFinding } from "../../annotation_reader";
import {
   collectModelBuildPlan,
   derivePackagePlan,
   emptyBuildPlanParts,
   finishPackagePlan,
   type BuildPlanPackage,
   type BuildPlanParts,
   type FinishedBuildPlan,
   type UpstreamSchemaReader,
} from "./build_plan";
import { loadGateText } from "./load_gate_text";
import { newCrossFileCompanions } from "./materialize_compile";
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
import type { ModelMaterializer } from "@malloydata/malloy";

/**
 * What a run's key checks read of the package, gathered model by model as its
 * plan is collected ({@link compileRunKeyChecks}).
 */
interface KeyCheckInputs {
   /** Every join_one of every model. */
   joinOneKeys: Set<string>;
   /** Every source keyed on its primary key by a join, with its model's compiler. */
   parentKeyNeeds: {
      need: ParentKeyNeed;
      compile: (text: string) => Promise<string>;
   }[];
   /** Every source a join reaches on a column other than its primary key. */
   joinedKeyNeeds: {
      need: JoinedKeyNeed;
      compile: (text: string) => Promise<string>;
   }[];
   /** Every declared primary key, by sourceID. */
   primaryKeys: Map<string, string>;
   /** Each `#(materialize)` uniqueness check's compiler over a run's manifest. */
   keyCheckCompilers: Map<string, KeyCheckCompiler>;
}

function emptyKeyCheckInputs(): KeyCheckInputs {
   return {
      joinOneKeys: new Set(),
      parentKeyNeeds: [],
      joinedKeyNeeds: [],
      primaryKeys: new Map(),
      keyCheckCompilers: new Map(),
   };
}

/** Add one compiled model's key-check inputs to `into`. */
function addKeyCheckInputs(
   into: KeyCheckInputs,
   malloyModel: Parameters<typeof modelDefOf>[0],
   materializer: ModelMaterializer,
): void {
   const map = SourceMap.of(modelDefOf(malloyModel));
   for (const key of joinOneKeysOf(map)) into.joinOneKeys.add(key);
   for (const node of map.nodes()) {
      const key = primaryKeyOf(node.def);
      if (key !== undefined && !into.primaryKeys.has(node.sourceID)) {
         into.primaryKeys.set(node.sourceID, key);
      }
   }
   for (const need of parentKeyNeedsOf(map)) {
      into.parentKeyNeeds.push({
         need,
         compile: parentKeyCheckCompiler(materializer),
      });
   }
   for (const need of joinedKeyNeedsOf(map)) {
      into.joinedKeyNeeds.push({
         need,
         compile: parentKeyCheckCompiler(materializer),
      });
   }
}

/**
 * The key checks a run's builds run, set on the collected `parts`: a rollup
 * key check no join_one of the package reads is dropped (it would scan the
 * base for nothing), a query source read through a join_many keyed on its
 * primary key, and a rollup's base reached by a join on another column, are
 * checked by the rollup's build, and a `storage=` rollup read that way has
 * each stored source's key checked at its own build instead. Compiles
 * queries, so only a run's plan pays for it: no load reads any of it.
 */
async function compileRunKeyChecks(
   parts: BuildPlanParts,
   inputs: KeyCheckInputs,
): Promise<void> {
   const rollupPlans = Object.values(parts.tables).flatMap((table) =>
      table.rollup ? [table.rollup as RollupVariantPlan] : [],
   );
   for (const rollup of rollupPlans) {
      if (
         rollup.keyCheck &&
         !inputs.joinOneKeys.has(
            joinOneKey(rollup.baseSourceID, rollup.keyCheck.keyColumn),
         )
      ) {
         delete rollup.keyCheck;
      }
   }
   await compileParentKeyChecks(
      rollupPlans,
      inputs.parentKeyNeeds,
      inputs.joinedKeyNeeds,
   );
   const storedKeyChecks = storedKeyChecksOf(
      rollupPlans,
      inputs.parentKeyNeeds.map(({ need }) => need),
      (sourceID) => inputs.primaryKeys.get(sourceID),
      Object.keys(parts.tables).filter(
         (sourceID) => !parts.tables[sourceID].rollup,
      ),
   );
   for (const [sourceID, keyColumn] of Object.entries(storedKeyChecks)) {
      parts.tables[sourceID].storedKeyCheck = keyColumn;
   }
}

/**
 * Collect every `.malloy` model's plan parts on this thread, in model order;
 * with `keyChecks`, also what a run's key checks read.
 */
async function collectPackageParts(
   pkg: BuildPlanPackage,
   signal: AbortSignal | undefined,
   keyChecks: KeyCheckInputs | undefined,
): Promise<BuildPlanParts> {
   const parts = emptyBuildPlanParts();
   const yielder = createEventLoopYielder();
   // One compile of each cross-file parent's file for the whole plan.
   const crossFileCompanions = newCrossFileCompanions();
   for (const modelPath of pkg.getModelPaths()) {
      // Only `.malloy` models declare persist sources. A `.malloynb` notebook
      // parses as a flat model and throws on its `>>>` cell delimiter, which
      // would abort the whole plan.
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
      // The gate classification needs the SAME live materializer that
      // compiled this model (see `classifyPersistSourceGate`).
      const { runtime, materializer } = await authorFiles.loadModel();
      const malloyModel = await materializer.getModel();
      if (keyChecks) addKeyCheckInputs(keyChecks, malloyModel, materializer);
      await collectModelBuildPlan(parts, {
         modelPath,
         packagePath: pkg.getPackagePath(),
         materializer,
         malloyModel,
         runtime,
         getRuntime: (overlay) => authorFiles.runtime(overlay),
         ...(keyChecks
            ? { keyCheckCompilers: keyChecks.keyCheckCompilers }
            : {}),
         crossFileCompanions,
      });
   }
   return parts;
}

/** `pkg`'s reader of its upstream tables' types for `use`, when it has one. */
function upstreamReaderOf(
   pkg: BuildPlanPackage,
   use: "load" | "run",
): { readUpstreamSchemas?: UpstreamSchemaReader } {
   const read = pkg.readUpstreamSchemas?.bind(pkg);
   return read ? { readUpstreamSchemas: (tables) => read(tables, use) } : {};
}

/**
 * A run's build plan, compiled on this thread against the package's live
 * connections, its key checks included. A connection that did not resolve
 * is reported here.
 */
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
   const keyChecks = emptyKeyCheckInputs();
   const parts = await collectPackageParts(pkg, signal, keyChecks);
   await compileRunKeyChecks(parts, keyChecks);
   // The build path reads this plan directly: a `#(materialize)` source
   // refused for its table name, or a source whose incremental declaration
   // this deployment cannot honour, must not build here either.
   const { compiled, digestSkipped } = await finishPackagePlan(parts, {
      getMalloyConnection: (name) => pkg.getMalloyConnection(name),
      versionScoped: versionOwnsTables(pkg),
      keyCheckCompilers: keyChecks.keyCheckCompilers,
      keepIncrementalUnbuildable,
      ...upstreamReaderOf(pkg, "run"),
   });
   reportDigestSkipped(digestSkipped);
   return compiled;
}

/**
 * A load's package plan, derived on this thread when the load worker left it
 * at its soft deadline: the worker's plan, compiled from the files on disk.
 * Reports nothing: the caller reports `digestSkipped` and records the
 * plan's eligibility refusals, as it does the worker's.
 */
export async function computeLoadPlan(
   pkg: BuildPlanPackage,
   signal?: AbortSignal,
): Promise<{
   packagePlan: PackagePlan<PackageAnnotationFinding>;
   digestSkipped: string[];
}> {
   const parts = await collectPackageParts(pkg, signal, undefined);
   const { compiled, digestSkipped } = await finishPackagePlan(parts, {
      getMalloyConnection: (name) => pkg.getMalloyConnection(name),
      versionScoped: versionOwnsTables(pkg),
      ...upstreamReaderOf(pkg, "load"),
   });
   return {
      packagePlan: derivePackagePlan(compiled, pkg.getMaterializationConfig()),
      digestSkipped,
   };
}

/** {@link computeLoadPlan}'s package plan alone. */
export async function computePackageBuildPlan(
   pkg: BuildPlanPackage,
   signal?: AbortSignal,
): Promise<PackagePlan<PackageAnnotationFinding>> {
   return (await computeLoadPlan(pkg, signal)).packagePlan;
}

/**
 * Report connections whose digest could not be read: the connection did not
 * resolve (already warned where it was resolved), so the sources on it are
 * left out of the plan. A discrete correctness signal, not a silent skip.
 */
export function reportDigestSkipped(connectionNames: readonly string[]): void {
   for (const connectionName of connectionNames) {
      recordConnectionDigestSkipped();
      logger.warn("Skipping connection digest; connection did not resolve", {
         connectionName,
      });
   }
}

/** Record each eligibility refusal a derivation counted. */
export function recordEligibilityRefusals(counts: EligibilityRefusals): void {
   for (const [reason, count] of Object.entries(counts)) {
      for (let i = 0; i < (count ?? 0); i++) {
         recordEligibilityRefused(reason as keyof EligibilityRefusals);
      }
   }
}

/**
 * The load gate's text over a package plan a run derived
 * (plan/load_gate_text.ts), so the run refuses what the load refuses: empty
 * when the files carry no invalid materialization annotation. The plan's
 * eligibility refusals are recorded, as every adopted plan's are.
 */
export function runLoadGateText(
   packagePlan: PackagePlan<PackageAnnotationFinding>,
   judgement: { versionScoped: boolean; enforceCollisions: boolean },
): string {
   recordEligibilityRefusals(packagePlan.eligibilityRefused);
   return loadGateText(packagePlan, judgement);
}

/**
 * Whether the package's tables are named per version: a published version
 * of a `scope: "version"` package (the run's `ownedVersionOf`). An
 * unversioned package writes an author's `name=` as given whatever its scope.
 */
function versionOwnsTables(pkg: BuildPlanPackage): boolean {
   return (
      pkg.getPackageMetadata().scope === "version" &&
      pkg.getVersionId() !== undefined
   );
}
