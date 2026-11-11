// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Load a test package into an environment from a tree a test writes: the
 * fixture loader for specs whose subject is what a loaded package does
 * (compile, reports, gates), not how it got there. The tree is written at
 * `<environment>/<name>/` and registered there in place, as a watched
 * environment's package is; a copy already loaded under the name is unloaded
 * first, so a second call replaces it.
 */

import * as fs from "fs";
import * as path from "path";
import type { Environment } from "../service/environment";
import type { Package } from "../service/package";

export async function installTestPackage(
   env: Environment,
   packageName: string,
   write: (packagePath: string) => Promise<void>,
): Promise<Package> {
   const packagePath = path.join(env.getEnvironmentPath(), packageName);
   await env.unloadPackage(packageName);
   await fs.promises.rm(packagePath, { recursive: true, force: true });
   await fs.promises.mkdir(packagePath, { recursive: true });
   await write(packagePath);
   const loaded = await env.addPackage(packageName);
   if (!loaded) {
      throw new Error(`Test package ${packageName} did not load`);
   }
   return loaded;
}
