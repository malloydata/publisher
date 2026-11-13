// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Compiles the generated `#(preaggregate)` build companion (see rollup_plan.ts),
 * as materialize_compile.ts compiles the `#(materialize)` one: the text is
 * served from an overlay at a URL beside the declaring model, so its `import`
 * resolves while it never shows up in `getModelPaths()`, the API or the
 * package's own compile.
 *
 * **Built over the author's rows, never over a stored base (#6293).** The
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
import { fileURLToPath, pathToFileURL } from "url";
import { logger } from "../../../logger";
import { overlayKey } from "../../overlay_key";
import { persistBlankedFiles } from "../persist_blanking";
import type { SynthesizedRuntimeFactory } from "./preaggregation_compile";
import {
   generateRollupBuildFile,
   planModelRollups,
   ROLLUP_COMPANION_SUFFIX,
   rollupKeyCheckQuery,
   type RollupRefusal,
   type RollupVariantPlan,
} from "./rollup_plan";
import { splitSourceID } from "./persisted_view";
import { SourceMap } from "./source_map";

export interface GeneratedRollups {
   /** The rollups the companion declares. Never empty. */
   plans: RollupVariantPlan[];
   model: MalloyModel;
   materializer: ModelMaterializer;
   runtime: Runtime;
   text: string;
}

/** Where the rollup companion for `modelPath` lives, as an absolute URL. */
export function rollupCompanionURL(
   packagePath: string,
   modelPath: string,
): URL {
   return pathToFileURL(
      `${path.join(packagePath, modelPath)}${ROLLUP_COMPANION_SUFFIX}`,
   );
}

/**
 * The companion URL of the file a `sourceID` was declared in, or undefined
 * for a declaration that is not a local file.
 */
export function rollupCompanionURLOf(declaringURL: string): URL | undefined {
   try {
      return pathToFileURL(
         `${fileURLToPath(declaringURL)}${ROLLUP_COMPANION_SUFFIX}`,
      );
   } catch {
      return undefined;
   }
}

/**
 * A `sourceID` with its file made relative to the package
 * (`orders@models/orders.malloy`), so a rollup's name does not depend on
 * where the package is checked out.
 */
export function packageRelativeSourceID(
   packagePath: string,
   sourceID: string,
): string {
   const at = splitSourceID(sourceID);
   if (!at) return sourceID;
   try {
      const file = fileURLToPath(at.url);
      const relative = path
         .relative(path.resolve(packagePath), file)
         .split(path.sep)
         .join("/");
      return `${at.name}@${relative}`;
   } catch {
      return sourceID;
   }
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
}): Promise<{ generated?: GeneratedRollups; refused: RollupRefusal[] }> {
   const modelFile = path.resolve(args.packagePath, args.modelPath);
   let refused: RollupRefusal[] = [];
   try {
      const planned = planModelRollups(
         SourceMap.of(args.modelDef),
         {
            packageRelativeSourceID: (id) =>
               packageRelativeSourceID(args.packagePath, id),
         },
         (declaringURL) => {
            try {
               return path.resolve(fileURLToPath(declaringURL)) === modelFile;
            } catch {
               return true;
            }
         },
      );
      refused = planned.refused;
      const text = generateRollupBuildFile(
         planned.plans,
         path.basename(args.modelPath),
      );
      if (!text) return { refused };
      const url = rollupCompanionURL(args.packagePath, args.modelPath);
      // The shared, checked blanking (persist_blanking.ts): each compiler
      // span must hold its note and the file must lex the same once blanked.
      const { runtime: reader } = await args.getRuntime(new Map());
      const overlay = await persistBlankedFiles(args.modelDef, async (href) => {
         try {
            const read = await reader.urlReader.readURL(new URL(href));
            return typeof read === "string" ? read : read.contents;
         } catch {
            return undefined;
         }
      });
      if (!overlay) {
         // Building over the author's files as they are would chain a rollup
         // onto its base's table (#6293); build nothing instead.
         logger.error(
            "A #(preaggregate) companion was not built: a `#@` line of the author's files could not be left out of it",
            { modelPath: args.modelPath },
         );
         return { refused };
      }
      overlay.set(overlayKey(url.href), text);
      const { runtime, importBaseURL } = await args.getRuntime(overlay);
      const materializer = runtime.loadModel(url, { importBaseURL });
      const model = await materializer.getModel();
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
      return { refused };
   }
}

/**
 * Compile each rollup's key check (known limitation 29, see
 * `rollupKeyCheckQuery`) against the companion, onto its plan. A check that
 * does not compile is left off, logged: its rollup's join_one is then never
 * known unique, and the join-to-rollup leg keeps declining what it declined
 * before.
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
