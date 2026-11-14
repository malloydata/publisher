// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The generated build companion for v1 `#@ persist` (materialization v2's
 * D3, first half; phase 3 pull request 6d).
 *
 * Malloy plans persisted sources only in a file that carries
 * `##! experimental.persistence`, and refuses a non-empty build manifest on
 * one that does not. An author's file needs the flag only for that: under the
 * v1 cutover the served copy reads every table, and the live compile takes
 * no manifest. So for a model file that declares `#@ persist` without the
 * flag, the planner compiles a build copy of the file with the flag as its
 * first line, served over the file's own URL (the #1644 build-copy
 * mechanism's overlay), and asks it for build targets: every `sourceID` is the
 * author's, so the plan keys as before, and a file that has the flag is
 * planned from itself, unchanged. A v1 source's address must not move
 * between the two (`v1_build_companion_corpus.spec.ts`).
 */

import * as path from "path";
import { pathToFileURL } from "url";
import type { Model as MalloyModel, Runtime } from "@malloydata/malloy";
import type { SynthesizedRuntimeFactory } from "./preaggregation_compile";

/** The flag line the build copy opens with. */
export const PERSISTENCE_FLAG_LINE = "##! experimental.persistence";

/**
 * Whether `text` may declare a v1 `#@ persist`: a `#@` line naming `persist`
 * anywhere on it, as Malloy reads the key (`#@ name=t persist` is one). A
 * false positive costs one compile that plans nothing; a false negative
 * would plan the file's sources nowhere and report them nowhere.
 */
export function declaresV1Persist(text: string): boolean {
   return /^\s*#@.*\bpersist\b/m.test(text);
}

/**
 * The build copy's text: the author's file with the flag as its first line.
 * Every declaration moves down one line; nothing an address reads does.
 */
export function v1BuildCompanionText(text: string): string {
   return `${PERSISTENCE_FLAG_LINE}\n${text}`;
}

/**
 * The model and runtime to ask for build targets for a model file that lacks
 * the flag: its build copy, compiled over the file's own URL. Undefined when
 * the file declares no `#@ persist`, cannot be read, or its copy does not
 * compile (the file then plans no persisted source, as before).
 *
 * The copy is made from the text the author's model compiled from, read
 * through `getRuntime`'s reader: a package's build view (joins across
 * connections left out, #6982) or a worker's file map, not necessarily the
 * file on disk. The file on disk would put the left-out joins back, and the
 * copy would never compile.
 */
export async function compileV1BuildCompanion(args: {
   packagePath: string;
   modelPath: string;
   getRuntime: SynthesizedRuntimeFactory;
   /**
    * Called when the file's text cannot be read, or it declares `#@ persist`
    * but its build copy does not compile: the undefineds that may leave
    * declared sources out of the plan.
    */
   onCompileFailed?: (error: unknown) => void;
}): Promise<{ model: MalloyModel; runtime: Runtime } | undefined> {
   const file = path.resolve(args.packagePath, args.modelPath);
   const url = pathToFileURL(file);
   let text: string;
   try {
      const { runtime } = await args.getRuntime(new Map());
      const read = await runtime.urlReader.readURL(url);
      text = typeof read === "string" ? read : read.contents;
   } catch (error) {
      args.onCompileFailed?.(error);
      return undefined;
   }
   if (!declaresV1Persist(text)) return undefined;
   try {
      const { runtime, importBaseURL } = await args.getRuntime(
         new Map([[url.href, v1BuildCompanionText(text)]]),
      );
      const model = await runtime.loadModel(url, { importBaseURL }).getModel();
      return { model, runtime };
   } catch (error) {
      args.onCompileFailed?.(error);
      return undefined;
   }
}
