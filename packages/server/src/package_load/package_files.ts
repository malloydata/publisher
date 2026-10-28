// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import path from "path";
import recursive from "recursive-readdir";
import { MODEL_FILE_SUFFIX, NOTEBOOK_FILE_SUFFIX } from "../constants";
import { ignoreDotfiles } from "../utils";

/**
 * Every file in the package, package-relative, sorted. `recursive-readdir`
 * yields libuv completion order, and this order becomes `listModels()` order,
 * so sorting keeps it stable across reloads and servers.
 */
export async function listPackageFiles(packagePath: string): Promise<string[]> {
   const files = await recursive(packagePath, [ignoreDotfiles]);
   return (
      files
         .map((full: string) =>
            path.relative(packagePath, full).replace(/\\/g, "/"),
         )
         // Codepoint order: localeCompare follows LANG/LC_ALL, which varies by server.
         .sort((a, b) => (a < b ? -1 : a > b ? 1 : 0))
   );
}

/** The model and notebook files a load compiles. */
export function filterModelPaths(allRelative: string[]): string[] {
   return allRelative.filter(
      (p) => p.endsWith(MODEL_FILE_SUFFIX) || p.endsWith(NOTEBOOK_FILE_SUFFIX),
   );
}
