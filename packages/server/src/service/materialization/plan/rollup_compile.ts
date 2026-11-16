// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Compiles the generated `#(preaggregate)` build companion (see rollup_plan.ts),
 * as materialize_compile.ts compiles the `#(materialize)` one: the text is
 * served from an overlay at a URL beside the declaring model, so its `import`
 * resolves while it never shows up in `getModelPaths()`, the API or the
 * package's own compile.
 *
 * **Built over the author's rows, never over a stored base** (pinned in
 * preaggregate_variants.spec.ts). The
 * companion reads the author's files with every source-level `#@` line blanked
 * (positions kept), so Malloy's planner sees no persisted source under a
 * rollup: a rollup never chains onto its base's table. That is what lets a
 * rollup over a `#@ persist storage=` base build at all (the chained storage
 * build lifts a downstream's definition from its file, and a rollup has
 * none), and it reads the same rows the live query reads. The content address
 * is unchanged by the blanking: Malloy keys a target on its fully inlined SQL.
 */

import type {
   Model as MalloyModel,
   ModelMaterializer,
   Runtime,
} from "@malloydata/malloy";
import * as path from "path";
import { getMaterializeEnabled } from "../../../config";
import { pathToFileURL } from "url";
import { logger } from "../../../logger";
import { queryGivenUsageIds } from "../../governance/ir/prepared_query";
import {
   companionOmissions,
   declaredInModel,
   GeneratedFiles,
   type SynthesizedRuntimeFactory,
} from "../text/generated_files";
import {
   planModelRollups,
   parentKeyCheckQuery,
   readsAsJoinMany,
   rollupKeyCheckQuery,
   type JoinedKeyNeed,
   type ParentKeyNeed,
   type RollupRefusal,
} from "./rollup_plan";
import {
   generateRollupBuildFile,
   packageRelativeSourceID,
   ROLLUP_COMPANION_SUFFIX,
} from "../text/read_text";
import type { ParentKeyCheck, RollupVariantPlan } from "../domain/table_plans";
import { SourceMap } from "../text/source_map";
import type { Omission } from "../domain/omitted_sources";

interface GeneratedRollups {
   /** The rollups the companion declares. Never empty. */
   plans: RollupVariantPlan[];
   model: MalloyModel;
   materializer: ModelMaterializer;
   runtime: Runtime;
   text: string;
}

/** Where the rollup companion for `modelPath` lives, as an absolute URL. */
function rollupCompanionURL(packagePath: string, modelPath: string): URL {
   return pathToFileURL(
      `${path.join(packagePath, modelPath)}${ROLLUP_COMPANION_SUFFIX}`,
   );
}

/**
 * Plan, generate and compile the rollup companion for one author model: the
 * rollups of every source the model DECLARES. `generated` is undefined when it
 * declares none, or when the companion does not compile (a publisher bug: the
 * text is generated, so it is logged at `error` and costs that model its
 * rollups and nothing else). `refused` holds the declarations not planned.
 */
export async function tryCompileRollupCompanion(args: {
   packagePath: string;
   modelPath: string;
   modelDef: unknown;
   getRuntime: SynthesizedRuntimeFactory;
}): Promise<{
   generated?: GeneratedRollups;
   refused: RollupRefusal[];
   /** The rollups a failure left out of the plan (see omitted_sources). */
   omitted?: Omission[];
}> {
   const modelFile = path.resolve(args.packagePath, args.modelPath);
   const companionHref = rollupCompanionURL(
      args.packagePath,
      args.modelPath,
   ).href;
   let refused: RollupRefusal[] = [];
   // Named once planned; until then a failure cannot name them.
   let plannedNames: string[] | undefined;
   try {
      const planned = planModelRollups(
         SourceMap.of(args.modelDef),
         {
            packageRelativeSourceID: (id) =>
               packageRelativeSourceID(args.packagePath, id),
            // v1 lines are always planned; `#(preaggregate)` only with its switch.
            readsPreaggregateDeclarations: getMaterializeEnabled(),
         },
         declaredInModel(modelFile, true),
      );
      refused = planned.refused;
      plannedNames = planned.plans.map((p) => p.rollupSourceName);
      const text = generateRollupBuildFile(
         planned.plans,
         path.basename(args.modelPath),
      );
      if (!text) return { refused };
      const url = rollupCompanionURL(args.packagePath, args.modelPath);
      const files = GeneratedFiles.over(args.getRuntime);
      // The shared, checked blanking (persist_blanking.ts): each compiler
      // span must hold its note and the file must lex the same once blanked.
      const overlay = await files.blanked(args.modelDef);
      if (!overlay) {
         // Building over the author's files as they are would chain a rollup
         // onto its base's table; build nothing instead.
         logger.error(
            "A #(preaggregate) companion was not built: a `#@` line of the author's files could not be left out of it",
            { modelPath: args.modelPath },
         );
         return {
            refused,
            omitted: companionOmissions("rollup_blanking", {
               file: companionHref,
               names: plannedNames,
            }),
         };
      }
      overlay.set(url.href, text);
      const { runtime, materializer, model } = await files.compile(
         url,
         overlay,
      );
      await compileRollupKeyChecks(planned.plans, materializer, args.modelPath);
      return {
         generated: {
            plans: planned.plans,
            model,
            materializer,
            runtime,
            text,
         },
         refused,
      };
   } catch (error) {
      logger.error(
         "Failed to plan or compile the generated #(preaggregate) companion",
         {
            modelPath: args.modelPath,
            error: error instanceof Error ? error.message : String(error),
         },
      );
      return {
         refused,
         omitted: companionOmissions("rollup", {
            file: companionHref,
            names: plannedNames,
         }),
      };
   }
}

/**
 * The `compile` {@link compileParentKeyChecks} takes, over the author's model
 * `materializer`: the check's SQL, refused when the check reads a given (as
 * Malloy records it, `givenUsage`: a `where:` that reads one, say). Compiled
 * once at plan time, such a check reads the rows of the given's default (or
 * does not compile without one), not the rows each caller's query reads, so a
 * key it finds unique may repeat for another caller; refused, the
 * join-to-rollup branch reads that source's key as unknown and declines.
 */
export function parentKeyCheckCompiler(materializer: {
   loadQuery(text: string): {
      getPreparedQuery(): Promise<unknown>;
      getSQL(): Promise<string>;
   };
}): (text: string) => Promise<string> {
   return async (text) => {
      const query = materializer.loadQuery(text);
      const givens = queryGivenUsageIds(await query.getPreparedQuery());
      if (givens === undefined || givens.length > 0) {
         throw new Error(
            givens === undefined
               ? "the check's given usage could not be read"
               : "the check reads a given, so it would describe one caller's rows, not each caller's",
         );
      }
      return query.getSQL();
   };
}

/**
 * Attach to each colocated rollup in `plans` the primary key checks of the
 * query sources that read it through a join read as a join_many
 * (`readsAsJoinMany`), each compiled once by its `compile` (the author's
 * model that names the source). To each rollup, colocated or in a storage
 * destination, also attach the check of its base's own primary key when a
 * join reaches the base on another column (`joinedKeys`, see
 * {@link JoinedKeyNeed}): the join-to-rollup branch serves a count, sum or
 * average of the base through such a join only when that key is unique and
 * never NULL. A check that does not compile is left off, logged: the branch
 * then reads that source's key as unknown and declines.
 */
export async function compileParentKeyChecks(
   plans: Iterable<RollupVariantPlan>,
   needs: readonly {
      need: ParentKeyNeed;
      compile: (text: string) => Promise<string>;
   }[],
   joinedKeys: readonly {
      need: JoinedKeyNeed;
      compile: (text: string) => Promise<string>;
   }[] = [],
): Promise<void> {
   const compiled = new Map<string, Promise<string | undefined>>();
   const compileOnce = (
      sourceID: string,
      name: string,
      keyColumn: string,
      compile: (text: string) => Promise<string>,
   ): Promise<string | undefined> => {
      let sql = compiled.get(sourceID);
      if (sql === undefined) {
         sql = compile(parentKeyCheckQuery(name, keyColumn)).catch((error) => {
            logger.warn(
               "A source's primary key check did not compile; queries it answers through a #(preaggregate) rollup read through a join are answered live",
               {
                  source: name,
                  keyColumn,
                  error: error instanceof Error ? error.message : String(error),
               },
            );
            return undefined;
         });
         compiled.set(sourceID, sql);
      }
      return sql;
   };
   for (const plan of plans) {
      // A `#@ preaggregate` (v1) plan is never read through a join.
      if (plan.variant !== true) continue;
      const checks: ParentKeyCheck[] = [];
      const seen = new Set<string>();
      // A query source's key is checked over its rows where the rollup is
      // colocated; in a destination, over its stored table there
      // (`storedKeyChecksOf`).
      for (const { need, compile } of plan.storage === undefined ? needs : []) {
         if (seen.has(need.sourceID) || !readsAsJoinMany(plan, need)) continue;
         seen.add(need.sourceID);
         const text = await compileOnce(
            need.sourceID,
            need.name,
            need.keyColumn,
            compile,
         );
         if (text === undefined) continue;
         checks.push({
            sourceID: need.sourceID,
            keyColumn: need.keyColumn,
            connectionName: need.connectionName,
            sql: text,
         });
      }
      for (const { need, compile } of joinedKeys) {
         if (need.sourceID !== plan.baseSourceID || seen.has(need.sourceID)) {
            continue;
         }
         seen.add(need.sourceID);
         const text = await compileOnce(
            need.sourceID,
            need.name,
            need.keyColumn,
            compile,
         );
         if (text === undefined) continue;
         checks.push({
            sourceID: need.sourceID,
            keyColumn: need.keyColumn,
            connectionName: need.connectionName,
            sql: text,
         });
      }
      if (checks.length > 0) plan.parentKeyChecks = checks;
      else delete plan.parentKeyChecks;
   }
}

/**
 * Compile each rollup's key check (known limitation 29, see
 * `rollupKeyCheckQuery`) against the companion, onto its plan. A check that
 * does not compile is left off, logged: its rollup's join_one is then never
 * known unique, and the join-to-rollup branch declines it.
 */
async function compileRollupKeyChecks(
   plans: RollupVariantPlan[],
   materializer: ModelMaterializer,
   modelPath: string,
): Promise<void> {
   for (const plan of plans) {
      const text = rollupKeyCheckQuery(plan);
      const keyColumn = plan.grainDimensions[0];
      if (text === undefined || keyColumn === undefined) continue;
      try {
         const sql = await materializer.loadQuery(text).getSQL();
         plan.keyCheck = { keyColumn, sql };
      } catch (error) {
         logger.warn("A #(preaggregate) rollup's key check did not compile", {
            modelPath,
            rollup: plan.rollupSourceName,
            error: error instanceof Error ? error.message : String(error),
         });
      }
   }
}
