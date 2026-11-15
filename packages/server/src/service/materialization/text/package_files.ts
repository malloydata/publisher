// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The Compiler port over a package's files on disk: {@link GeneratedFiles}
 * whose runtime factory is the server's model runtime, compiling as the
 * package's `modelPath` compiles, with a set of connections. Used on the main
 * thread (the build plan, the served copy, the rollup and view legs); the
 * load worker hands `GeneratedFiles` its own factory instead and never loads
 * this module, nor what the model runtime imports.
 *
 * With `generated_files.ts`, the module's only caller of `createModelRuntime`.
 */

import {
   createModelRuntime,
   type ModelConnectionInput,
} from "../../model_runtime";
import { GeneratedFiles } from "./generated_files";

/**
 * Generated files over `packagePath`'s files, compiled as `modelPath` would
 * be, with `config`'s connections.
 */
export function packageFiles(
   packagePath: string,
   modelPath: string,
   config: ModelConnectionInput,
): GeneratedFiles {
   return GeneratedFiles.over(async (overlay) => {
      const { runtime, importBaseURL, modelURL } = await createModelRuntime(
         packagePath,
         modelPath,
         config,
         overlay.size > 0 ? { overlay } : undefined,
      );
      return { runtime, importBaseURL, modelURL };
   });
}
