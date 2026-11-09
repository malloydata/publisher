// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import extract from "extract-zip";
import * as fs from "fs";
import * as path from "path";
import { BadRequestError } from "../../errors";
import { formatDuration, logger } from "../../logger";
import { assertSafeEnvironmentPath } from "../../path_safety";

/**
 * Extract a downloaded archive into a sibling directory named for it
 * (`<dir>/pkg.zip` -> `<dir>/pkg`), replacing whatever is there, and return
 * that directory. Only for an archive the server downloaded itself.
 */
export async function unzipEnvironment(absoluteEnvironmentPath: string) {
   assertSafeEnvironmentPath(absoluteEnvironmentPath);
   // Extracts to a sibling directory named for the archive. The check is in
   // the one shape CodeQL's js/path-injection accepts as a barrier (an
   // unconditional startsWith on the resolved value it later sinks).
   const archiveDir = path.resolve(path.dirname(absoluteEnvironmentPath));
   const unzippedEnvironmentPath = path.resolve(
      archiveDir,
      path.basename(absoluteEnvironmentPath, ".zip"),
   );
   if (!unzippedEnvironmentPath.startsWith(archiveDir + path.sep)) {
      throw new BadRequestError(
         `Refusing to unzip "${absoluteEnvironmentPath}": target escapes its directory`,
      );
   }
   await extractZipInto(absoluteEnvironmentPath, unzippedEnvironmentPath);
   return unzippedEnvironmentPath;
}

/**
 * Extract `archivePath` into `targetDir`, replacing whatever is there.
 *
 * `targetDir` is removed first, so it must be a directory the server owns.
 * Never pass a path derived from an operator-supplied location: a sibling of
 * an operator's zip is the operator's directory, not ours.
 */
export async function extractZipInto(archivePath: string, targetDir: string) {
   const startedAt = Date.now();
   logger.info(`Detected zip file at "${archivePath}". Unzipping...`);
   await fs.promises.rm(targetDir, {
      recursive: true,
      force: true,
   });
   await fs.promises.mkdir(targetDir, { recursive: true });

   // Stream-extract off the main thread (yauzl via extract-zip): a
   // synchronous inflate of a large archive blocks the event loop long
   // enough to fail Kubernetes liveness probes.
   let entryCount = 0;
   let totalUncompressedBytes = 0;
   try {
      await extract(archivePath, {
         dir: path.resolve(targetDir),
         onEntry: (entry) => {
            // extract-zip 2.0.1 never validates a symlink entry's target, so a
            // crafted archive can write outside `dir` (CVE-2026-19693,
            // CVE-2026-56876, no upstream fix). Throwing here cancels the extract.
            if (
               ((entry.externalFileAttributes >>> 16) & 0o170000) ===
               0o120000
            ) {
               throw new BadRequestError(
                  `Refusing to unzip "${archivePath}": entry "${entry.fileName}" is a symbolic link`,
               );
            }
            entryCount += 1;
            totalUncompressedBytes += entry.uncompressedSize ?? 0;
         },
      });
   } catch (error) {
      // Leave no partial extract behind for a later load to pick up.
      await fs.promises.rm(targetDir, {
         recursive: true,
         force: true,
      });
      throw error;
   }

   const mib = (totalUncompressedBytes / (1024 * 1024)).toFixed(1);
   logger.info(
      `Unzipped "${archivePath}" -> "${targetDir}" ` +
         `(${entryCount} entries, ${mib} MiB uncompressed) in ` +
         `${formatDuration(Date.now() - startedAt)}`,
   );
}
