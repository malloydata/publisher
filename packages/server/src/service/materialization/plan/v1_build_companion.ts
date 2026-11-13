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

import * as fs from "fs";
import * as path from "path";
import { pathToFileURL } from "url";
import type { Model as MalloyModel, Runtime } from "@malloydata/malloy";
import type { SynthesizedRuntimeFactory } from "./preaggregation_compile";

/** The flag line the build copy opens with. */
export const PERSISTENCE_FLAG_LINE = "##! experimental.persistence";

/** Whether `text` declares a v1 `#@ persist` anywhere. */
export function declaresV1Persist(text: string): boolean {
   return /^\s*#@\s*persist\b/m.test(text);
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
 */
export async function compileV1BuildCompanion(args: {
   packagePath: string;
   modelPath: string;
   getRuntime: SynthesizedRuntimeFactory;
   readText?: (file: string) => string | undefined;
}): Promise<{ model: MalloyModel; runtime: Runtime } | undefined> {
   const file = path.resolve(args.packagePath, args.modelPath);
   const text = (
      args.readText ??
      ((f: string) => {
         try {
            return fs.readFileSync(f, "utf8");
         } catch {
            return undefined;
         }
      })
   )(file);
   if (text === undefined || !declaresV1Persist(text)) return undefined;
   const url = pathToFileURL(file);
   try {
      const { runtime, importBaseURL } = await args.getRuntime(
         new Map([[url.href, v1BuildCompanionText(text)]]),
      );
      const model = await runtime.loadModel(url, { importBaseURL }).getModel();
      return { model, runtime };
   } catch {
      return undefined;
   }
}
