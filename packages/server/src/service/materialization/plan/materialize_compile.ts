// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Compiles the generated `#(materialize)` build companion (see
 * build_file_generator.ts), as preaggregation_compile.ts compiles the rollup
 * companion: the text is served from an overlay at a URL beside the author's
 * model, so its `import` resolves while it never shows up in
 * `getModelPaths()`, the API or the package's own compile, and cannot go stale
 * against the author's edits.
 */

import type {
   Model as MalloyModel,
   ModelMaterializer,
   Runtime,
} from "@malloydata/malloy";
import * as path from "path";
import { fileURLToPath, pathToFileURL } from "url";
import { logger } from "../../../logger";
import {
   generateBuildFile,
   MATERIALIZE_COMPANION_SUFFIX,
   planModelMaterialize,
   type MaterializePlan,
   type MaterializeRefusal,
} from "./build_file_generator";
import type { SynthesizedRuntimeFactory } from "./preaggregation_compile";

export interface GeneratedMaterialize {
   /** The sources the generator planned. Never empty. */
   plans: MaterializePlan[];
   /** Declarations the generator refused, for the operator-facing log. */
   refused: MaterializeRefusal[];
   /** The compiled companion: its build targets are the generated sources. */
   model: MalloyModel;
   materializer: ModelMaterializer;
   runtime: Runtime;
   /** The generated text, for logging and tests. */
   text: string;
}

/** Where the generated companion for `modelPath` lives, as an absolute URL. */
export function materializeCompanionURL(
   packagePath: string,
   modelPath: string,
): URL {
   // pathToFileURL, not `file://${path}`: on Windows the path is `D:\…`, and
   // the overlay key must match the URL Malloy normalizes to.
   return pathToFileURL(
      `${path.join(packagePath, modelPath)}${MATERIALIZE_COMPANION_SUFFIX}`,
   );
}

/**
 * Plan, generate and compile the build companion for one author model, or
 * undefined when it declares no plannable `#(materialize)`. Refusals are
 * logged here; the eligibility gates judge the generated sources like any
 * persist source once they are in the plan.
 */
async function compileMaterializeCompanion(args: {
   packagePath: string;
   modelPath: string;
   contents: Record<string, unknown>;
   getRuntime: SynthesizedRuntimeFactory;
}): Promise<GeneratedMaterialize | undefined> {
   const modelFile = path.join(args.packagePath, args.modelPath);
   const { plans, refused } = planModelMaterialize(
      args.contents,
      (declaringURL) => {
         try {
            return (
               path.resolve(fileURLToPath(declaringURL)) ===
               path.resolve(modelFile)
            );
         } catch {
            // Not a local file (a host, another scheme): planned, as before.
            return true;
         }
      },
   );
   for (const refusal of refused) {
      logger.warn("#(materialize) declaration not built", {
         modelPath: args.modelPath,
         sourceName: refusal.sourceName,
         reason: refusal.reason,
      });
   }
   if (plans.length === 0) return undefined;

   const text = generateBuildFile(
      plans,
      path.basename(args.modelPath),
      (declaringURL) => {
         let declaringFile: string;
         try {
            declaringFile = fileURLToPath(declaringURL);
         } catch {
            return undefined;
         }
         if (path.resolve(declaringFile) === path.resolve(modelFile)) {
            return undefined;
         }
         return path
            .relative(path.dirname(modelFile), declaringFile)
            .split(path.sep)
            .join("/");
      },
   );
   if (!text) return undefined;

   const url = materializeCompanionURL(args.packagePath, args.modelPath);
   const { runtime, importBaseURL } = await args.getRuntime(
      new Map([[url.href, text]]),
   );
   const materializer = runtime.loadModel(url, { importBaseURL });
   const model = await materializer.getModel();
   return { plans, refused, model, materializer, runtime, text };
}

/**
 * {@link compileMaterializeCompanion}, with a compile failure logged and
 * swallowed, so a generated file that does not compile costs that model its
 * generated builds and nothing else. A failure is a publisher bug (the text
 * is generated), hence `error` level.
 */
export async function tryCompileMaterializeCompanion(
   args: Parameters<typeof compileMaterializeCompanion>[0],
): Promise<GeneratedMaterialize | undefined> {
   try {
      return await compileMaterializeCompanion(args);
   } catch (error) {
      logger.error("Failed to compile the generated #(materialize) companion", {
         modelPath: args.modelPath,
         error: error instanceof Error ? error.message : String(error),
      });
      return undefined;
   }
}
