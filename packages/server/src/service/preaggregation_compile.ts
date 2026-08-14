/**
 * Compiling the synthesized pre-aggregation model — the one seam both legs share.
 *
 * Synthesis produces a second Malloy model per author model (see
 * preaggregation_synthesis.ts). Two callers need it compiled: the build plan, so
 * rollups are built, and the serve path, so queries route. Both come through
 * here, which is what makes their `sourceEntityId`s agree — the identity property
 * the whole mechanism rests on. Two call sites each doing "roughly this" is the
 * one way to break it, so there is exactly one.
 *
 * The synthesized text is never written to disk. It is served to the compiler
 * from an overlay at a URL inside the real package directory, so its `import` of
 * the author's model resolves against the real neighbours while nothing is added
 * to the package tree. A model on disk would be worse in every direction: it
 * would appear in `getModelPaths()`, be listed by the API, be compiled as a model
 * in its own right, and go stale against the author's edits.
 */

import type {
   Model as MalloyModel,
   ModelMaterializer,
} from "@malloydata/malloy";
import * as path from "path";
import type { BuildManifest } from "../storage/DatabaseInterface";
import { logger } from "../logger";
import { Model, type ModelConnectionInput } from "./model";
import {
   planModelPreaggregation,
   synthesizePreaggregationModel,
   type RollupPlan,
} from "./preaggregation_synthesis";

/**
 * The suffix that turns an author model path into its synthesized companion's.
 * Inside the package directory (so imports resolve) and not a `.malloy` file, so
 * that if it ever DID reach disk it would not be picked up as a model.
 */
const SYNTHESIZED_SUFFIX = ".preagg.malloy";

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
   /** The generated text, for logging. */
   text: string;
}

/** Where the synthesized companion for `modelPath` lives, as an absolute URL. */
export function synthesizedModelURL(
   packagePath: string,
   modelPath: string,
): URL {
   return new URL(
      `file://${path.join(packagePath, modelPath)}${SYNTHESIZED_SUFFIX}`,
   );
}

/**
 * Plan, synthesize and compile the rollups for one author model.
 *
 * `contents` is the author model's compiled `modelDef.contents`. Returns
 * `undefined` when the model declares no usable `#@ preaggregate`, so a caller
 * skips the extra compile entirely rather than compiling a model that adds
 * nothing — which is every model in almost every package.
 *
 * Assumes the publish/load gate has already run: an unusable declaration is
 * skipped by the planner rather than reported here, so this never becomes a
 * second, quieter opinion about what is legal.
 */
export async function compileSynthesizedPreaggregation(args: {
   packagePath: string;
   modelPath: string;
   malloyConfig: ModelConnectionInput;
   contents: Record<string, unknown>;
   buildManifest?: BuildManifest["entries"];
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
   const { runtime, importBaseURL } = await Model.getModelRuntime(
      args.packagePath,
      args.modelPath,
      args.malloyConfig,
      {
         buildManifest: args.buildManifest,
         overlay: new Map([[synthesizedURL.href, text]]),
      },
   );
   const materializer = runtime.loadModel(synthesizedURL, { importBaseURL });
   const model = await materializer.getModel();
   return { plans, model, materializer, text };
}

/**
 * {@link compileSynthesizedPreaggregation}, with a compile failure logged and
 * swallowed.
 *
 * Both callers want this: pre-aggregation is an optimization, so a rollup that
 * will not compile must cost latency and nothing else. The build leg then plans
 * no rollup and the serve leg serves live — the same outcome as the feature being
 * off, which is the only safe way for a cache to fail.
 *
 * A failure here is a publisher bug rather than an authoring error: the gate has
 * already accepted every declaration, so anything that fails now is text this
 * code generated. Hence `error`, and hence the generated text in the log — it is
 * the only copy, and without it the report is unactionable.
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
