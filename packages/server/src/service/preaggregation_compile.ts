// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Compiles the synthesized pre-aggregation model (see preaggregation_synthesis.ts).
 *
 * Both the build leg and the serve leg compile through this one function, which
 * is what makes their `sourceEntityId`s agree; two call sites doing "roughly
 * this" would break that identity.
 *
 * The synthesized text is never written to disk. It is served from an overlay at
 * a URL inside the package directory, so its `import` of the author's model
 * resolves while it never shows up in `getModelPaths()`, the API, or the
 * package's own compile, and cannot go stale against the author's edits.
 */

import type {
   Model as MalloyModel,
   ModelMaterializer,
   Runtime,
} from "@malloydata/malloy";
import * as path from "path";
import { logger } from "../logger";
import {
   planModelPreaggregation,
   synthesizePreaggregationModel,
   type RollupPlan,
} from "./preaggregation_synthesis";

/** Appended to an author model path to name its synthesized companion. */
const SYNTHESIZED_SUFFIX = ".preagg.malloy";

/** See `compileSynthesizedPreaggregation`'s `getRuntime`. */
export type SynthesizedRuntimeFactory = (
   overlay: ReadonlyMap<string, string>,
) => Promise<{ runtime: Runtime; importBaseURL: URL }>;

export interface SynthesizedPreaggregation {
   /** The rollups the author's declarations asked for. Never empty. */
   plans: RollupPlan[];
   /** The compiled synthesized model: its build plan is the rollups to build. */
   model: MalloyModel;
   /**
    * The same model as a materializer, which is what the serve leg needs: a query
    * loaded from it compiles against the composite and so routes to a rollup when
    * one covers it.
    */
   materializer: ModelMaterializer;
   /** The runtime the synthesized model compiled in, which plans its build. */
   runtime: Runtime;
   /** The generated text, for logging. */
   text: string;
}

/** Where the synthesized companion for `modelPath` lives, as an absolute URL. */
function synthesizedModelURL(packagePath: string, modelPath: string): URL {
   return new URL(
      `file://${path.join(packagePath, modelPath)}${SYNTHESIZED_SUFFIX}`,
   );
}

/**
 * Plan, synthesize and compile the rollups for one author model.
 *
 * `contents` is the author model's compiled `modelDef.contents`. Returns
 * `undefined` when the model declares no usable `#@ preaggregate`, so the common
 * case skips the extra compile.
 *
 * Assumes the publish/load gate has already run: an unusable declaration is
 * skipped by the planner, not reported here, so this is never a second opinion
 * about what is legal.
 */
async function compileSynthesizedPreaggregation(args: {
   packagePath: string;
   modelPath: string;
   contents: Record<string, unknown>;
   /**
    * Builds the runtime the synthesized model compiles in, with `overlay`
    * served ahead of the package's files. A caller on the main thread passes
    * `Model.getModelRuntime` (with any build manifest); the package-load
    * worker passes its own, because it compiles against proxied connections
    * and must not load `./model`.
    */
   getRuntime: SynthesizedRuntimeFactory;
}): Promise<SynthesizedPreaggregation | undefined> {
   const plans = planModelPreaggregation(args.contents);
   if (plans.length === 0) return undefined;

   // The import is by basename: the synthesized model sits beside the author's,
   // so a bare filename resolves however the package is nested.
   const text = synthesizePreaggregationModel(
      plans,
      path.basename(args.modelPath),
   );
   if (!text) return undefined;

   const synthesizedURL = synthesizedModelURL(args.packagePath, args.modelPath);
   const { runtime, importBaseURL } = await args.getRuntime(
      new Map([[synthesizedURL.href, text]]),
   );
   const materializer = runtime.loadModel(synthesizedURL, { importBaseURL });
   const model = await materializer.getModel();
   return { plans, model, materializer, runtime, text };
}

/**
 * {@link compileSynthesizedPreaggregation}, with a compile failure logged and
 * swallowed: pre-aggregation is an optimization, so a failure must cost latency
 * only. The build leg plans no rollup and the serve leg serves live, as if the
 * feature were off.
 *
 * A failure here is a publisher bug (the gate already accepted every
 * declaration, so the failing text is generated), hence `error` level.
 */
export async function tryCompileSynthesizedPreaggregation(
   args: Parameters<typeof compileSynthesizedPreaggregation>[0],
): Promise<SynthesizedPreaggregation | undefined> {
   try {
      return await compileSynthesizedPreaggregation(args);
   } catch (error) {
      logger.error("Failed to compile synthesized pre-aggregation model", {
         modelPath: args.modelPath,
         error: error instanceof Error ? error.message : String(error),
      });
      return undefined;
   }
}
