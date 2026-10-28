// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import * as crypto from "crypto";
import * as fs from "fs";
import path from "path";
import recursive from "recursive-readdir";
import {
   MODEL_FILE_SUFFIX,
   NOTEBOOK_FILE_SUFFIX,
   PACKAGE_MANIFEST_NAME,
} from "../constants";
import { isUnderPublicDir } from "../service/report";
import { computeSourceContentSha } from "../service/package_revision";
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

/**
 * Path, size and mtime of every file a model can read data from, since a
 * cached ModelDef holds those files' schemas. Skipped: model and markdown
 * sources, `*.styles.json` (re-read on every load), `publisher.json` (read
 * whole by {@link treeFingerprint}) and `public/`, a data app's static
 * assets, which a model does not read data from.
 */
export async function dataFilesDigest(
   packagePath: string,
   allRelative: string[],
): Promise<string> {
   const dataFiles = allRelative.filter(
      (p) =>
         !p.endsWith(MODEL_FILE_SUFFIX) &&
         !p.endsWith(NOTEBOOK_FILE_SUFFIX) &&
         !p.toLowerCase().endsWith(".md") &&
         !p.endsWith(".styles.json") &&
         p !== PACKAGE_MANIFEST_NAME &&
         !isUnderPublicDir(p),
   );
   const stats = await Promise.all(
      dataFiles.map((p) =>
         fs.promises.stat(path.join(packagePath, p)).catch(() => undefined),
      ),
   );
   const hash = crypto.createHash("sha1");
   dataFiles.forEach((p, i) =>
      hash.update(`${p}\0${stats[i]?.size}\0${stats[i]?.mtimeMs}\n`),
   );
   return hash.digest("hex");
}

/**
 * What `reload_package`'s ifChanged compares: the content of everything a load
 * reads as text (models, notebooks, markdown such as skills and retrieval
 * prompts, styles, the whole `publisher.json`) and the data-file digest, so
 * the compile cache and ifChanged agree on what a change is. Private to the
 * server: `sourceContentSha` stays the public receipt.
 */
export async function treeFingerprint(packagePath: string): Promise<string> {
   const allFiles = await listPackageFiles(packagePath);
   const textFiles = allFiles.filter(
      (p) =>
         p === PACKAGE_MANIFEST_NAME ||
         p.endsWith(".styles.json") ||
         p.toLowerCase().endsWith(".md") ||
         filterModelPaths([p]).length > 0,
   );
   return `${computeSourceContentSha(packagePath, textFiles)}:${await dataFilesDigest(packagePath, allFiles)}`;
}
