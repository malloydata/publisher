// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import * as path from "path";
import { pathToFileURL } from "url";
import {
   computeSourceContentSha,
   UNREADABLE_SHA,
} from "../service/package_revision";
import {
   servedContentShas,
   type PackageWorkflows,
} from "../service/skills/package_workflows";
import { urlKey } from "./job_files";

/** A walk, and the sha of the declaration it read, which the source content sha covers. */
export interface Walked {
   walk: PackageWorkflows;
   declarationSha?: string;
}

/** The source content sha of what the package serves: each model's text and each file the walk read, by the shas read at load, so nothing is read again. */
export function servedContentSha(
   packagePath: string,
   models: readonly { modelPath: string }[],
   fileDigests: Readonly<Record<string, string>>,
   notebookShas: ReadonlyMap<string, string>,
   walked: Walked,
): string {
   return computeSourceContentSha(
      new Map([
         ...modelContentShas(packagePath, models, fileDigests, notebookShas),
         ...servedContentShas(walked.walk),
      ]),
      // The declaration is hashed whole, so an edit to a refused set or workflow moves it too.
      walked.declarationSha,
   );
}

/** The sha256 of the text each model's compile read, `UNREADABLE_SHA` for a model whose file was never read. */
function modelContentShas(
   packagePath: string,
   models: readonly { modelPath: string }[],
   fileDigests: Readonly<Record<string, string>>,
   notebookShas: ReadonlyMap<string, string>,
): Map<string, string> {
   return new Map(
      models.map(({ modelPath }) => {
         const key = urlKey(
            pathToFileURL(path.join(packagePath, modelPath)).href,
         );
         return [
            modelPath,
            fileDigests[key] ?? notebookShas.get(key) ?? UNREADABLE_SHA,
         ];
      }),
   );
}
