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
 * Plan, generate and compile the build companion for one author model.
 * `generated` is undefined when the model declares no plannable
 * `#(materialize)`, or when the companion does not compile: a failure is a
 * publisher bug (the text is generated), so it is logged at `error` level and
 * costs that model its generated builds and nothing else. `refused` holds the
 * declarations the generator would not plan, for the caller to report; the
 * eligibility gates judge the generated sources like any persist source once
 * they are in the plan.
 */
export async function tryCompileMaterializeCompanion(args: {
   packagePath: string;
   modelPath: string;
   contents: Record<string, unknown>;
   getRuntime: SynthesizedRuntimeFactory;
}): Promise<{
   generated?: GeneratedMaterialize;
   refused: MaterializeRefusal[];
}> {
   const modelFile = path.resolve(args.packagePath, args.modelPath);
   let refused: MaterializeRefusal[] = [];
   try {
      const planned = planModelMaterialize(args.contents, (declaringURL) => {
         try {
            return path.resolve(fileURLToPath(declaringURL)) === modelFile;
         } catch {
            // Not a local file (a host, another scheme): planned, as before.
            return true;
         }
      });
      refused = planned.refused;
      const text = generateBuildFile(
         planned.plans,
         path.basename(args.modelPath),
      );
      if (!text) return { refused };
      const url = materializeCompanionURL(args.packagePath, args.modelPath);
      const { runtime, importBaseURL } = await args.getRuntime(
         new Map([[url.href, text]]),
      );
      const materializer = runtime.loadModel(url, { importBaseURL });
      const model = await materializer.getModel();
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
         "Failed to plan or compile the generated #(materialize) companion",
         {
            modelPath: args.modelPath,
            error: error instanceof Error ? error.message : String(error),
         },
      );
      return { refused };
   }
}
