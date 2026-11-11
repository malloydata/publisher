// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { PackageNotFoundError } from "../../errors";
import { assertSafeEnvironmentPath } from "../../path_safety";
import type { PackageFetchOptions, PackageSource } from "./types";
import { extractZipInto } from "./zip";

/** A `./`, `../`, `~/`, or absolute (POSIX or Windows) location. */
export function isLocalPath(location: string): boolean {
   return (
      location.startsWith("./") ||
      location.startsWith("../") ||
      location.startsWith("~/") ||
      location.startsWith("/") ||
      path.isAbsolute(location)
   );
}

/**
 * Absolute on-disk path for a local package `location`.
 *
 * `~/` (POSIX form only) expands to the home directory. Anything still
 * relative resolves against `anchorDir`, the directory holding the active
 * config, so a config and the packages it points at can be moved or
 * committed together. Absolute locations are returned untouched.
 */
export function resolvePackageLocation(
   location: string,
   anchorDir: string,
   homeDir?: string,
): string {
   let expanded = location;
   if (location.startsWith("~/")) {
      // Lazily: os.homedir() can throw in a distroless container, and a
      // location without `~` must never fail on that.
      const home = homeDir ?? os.homedir();
      if (!home) {
         throw new Error(
            `Cannot expand "~" in location "${location}": home directory is not set`,
         );
      }
      expanded = path.join(home, location.slice(2));
   }
   return path.isAbsolute(expanded) ? expanded : path.join(anchorDir, expanded);
}

/**
 * A local directory, copied; or a local .zip, extracted. `location` is the
 * already-resolved, operator-supplied path.
 */
export class LocalPackageSource implements PackageSource {
   async fetch(
      environmentPath: string,
      absoluteTargetPath: string,
      { environmentName, packageName }: PackageFetchOptions,
   ): Promise<void> {
      // `environmentPath` is the operator-supplied mount source and may
      // legitimately be a relative path that resolves outside the
      // server root; only the target is asserted.
      assertSafeEnvironmentPath(absoluteTargetPath);
      const notFound = () =>
         new PackageNotFoundError(
            `Package ${packageName} for environment ${environmentName} not found in "${environmentPath}"`,
         );
      const source = await fs.promises.stat(environmentPath).catch((error) => {
         if ((error as NodeJS.ErrnoException).code === "ENOENT") {
            throw notFound();
         }
         throw error;
      });
      if (environmentPath.endsWith(".zip") && source.isFile()) {
         // Extract straight into the server-owned target: nothing is written
         // beside the archive, so a read-only mount works.
         await extractZipInto(environmentPath, absoluteTargetPath);
      } else if (source.isDirectory()) {
         await fs.promises.rm(absoluteTargetPath, {
            recursive: true,
            force: true,
         });
         await fs.promises.mkdir(absoluteTargetPath, { recursive: true });
         await fs.promises.cp(environmentPath, absoluteTargetPath, {
            recursive: true,
         });
      } else {
         throw notFound();
      }
   }
}
