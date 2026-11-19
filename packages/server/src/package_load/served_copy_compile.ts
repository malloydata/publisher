// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A served copy's compile on the load worker (served_copy_serve.ts's
 * `compileElsewhere`): this thread writes the re-rooted files, a worker
 * compiles them as the inline pass would, and the `modelDef` that comes back
 * is hydrated here in a runtime over the copy's own connections, which a
 * row-level graft reloads through.
 *
 * The worker is used only when one is ready and idle now
 * (`PackageLoadPool.compileServedCopy`), so a query waiting on a copy never
 * waits behind a package load; otherwise, and when the job itself fails, the
 * pass compiles inline. Either way the compile is the same, so the answer is.
 */

import {
   MalloyError,
   type MalloyConfig,
   type ModelDef,
   type ModelMaterializer,
} from "@malloydata/malloy";
import { logger } from "../logger";
import { makeHydrationRuntime } from "../service/model_runtime";
import type { GraftRuntime } from "../service/gate_classification";
import type { PackageLoadPool } from "./package_load_pool";
import type { SerializedError } from "./protocol";

/**
 * The connection an unnamed lookup resolves to, as every package load and
 * the package's own connection map name it.
 */
const DEFAULT_CONNECTION = "duckdb";

/**
 * A compile failure as the inline compile throws it: a `MalloyError` with its
 * problems (what containment places on a re-root, and what the
 * cross-connection and left-out-column steps read), else an `Error` of the
 * same name and message.
 */
export function servedCopyCompileError(serialized: SerializedError): Error {
   const error = serialized.malloyProblems
      ? new MalloyError(
           serialized.message,
           serialized.malloyProblems as MalloyError["problems"],
        )
      : new Error(serialized.message);
   if (!serialized.malloyProblems) error.name = serialized.name;
   if (serialized.stack) error.stack = serialized.stack;
   return error;
}

/**
 * One pass of a served copy compiled on `pool`, hydrated here; undefined
 * when no worker is free now or the job failed, and the pass compiles inline.
 */
export async function compileServedCopyOnWorker(
   pool: PackageLoadPool | null,
   args: {
      packagePath: string;
      modelPath: string;
      config: MalloyConfig;
      files: ReadonlyMap<string, string>;
   },
): Promise<
   | { materializer: ModelMaterializer; def: unknown; runtime: GraftRuntime }
   | undefined
> {
   const pending = pool?.compileServedCopy({
      packagePath: args.packagePath,
      // The worker's schema cache is scoped by this: the package directory
      // is one environment's package, so no other environment shares it.
      environmentName: `served-copy\u0000${args.packagePath}`,
      modelPath: args.modelPath,
      malloyConfig: args.config,
      defaultConnectionName: DEFAULT_CONNECTION,
      files: args.files,
   });
   if (!pending) return undefined;
   let result: Awaited<typeof pending>;
   try {
      result = await pending;
   } catch (error) {
      logger.warn(
         "A served copy's compile on the load worker failed; compiling it here",
         {
            modelPath: args.modelPath,
            error: error instanceof Error ? error.message : String(error),
         },
      );
      return undefined;
   }
   if (result.error) throw servedCopyCompileError(result.error);
   const def = result.modelDef as ModelDef;
   const runtime = makeHydrationRuntime(args.packagePath, args.config);
   return {
      materializer: runtime._loadModelFromModelDef(def),
      def,
      runtime,
   };
}
