// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import * as fs from "fs";
import type { simpleGit } from "simple-git";
import { logger } from "../../logger";
import {
   assertSafeEnvironmentPath,
   safeJoinUnderRoot,
} from "../../path_safety";
import { extractErrorDataFromError } from "../db_utils";
import {
   CloneProgressReporter,
   cloneProgressLabel,
   GIT_CLONE_OPTIONS,
   stripGitProgressNoise,
} from "./clone_progress";
import { parseGitHubUrl } from "./github_url";
import type { PackageFetchOptions, PackageSource } from "./types";

/** A `https://github.com/...` or `git@github.com:...` location. */
export function isGitHubURL(location: string): boolean {
   return (
      location.startsWith("https://github.com/") ||
      location.startsWith("git@github.com:")
   );
}

/**
 * A shallow clone of a GitHub repository, with the package subdirectory (the
 * last path segment after the repo) hoisted to the target's root.
 */
export class GitPackageSource implements PackageSource {
   /** `git` is the simple-git factory; a test injects a fake. */
   constructor(private readonly git: typeof simpleGit) {}

   async fetch(
      githubUrl: string,
      absoluteDirPath: string,
      opts?: Pick<PackageFetchOptions, "progress">,
   ): Promise<void> {
      const progressContext = opts?.progress;
      assertSafeEnvironmentPath(absoluteDirPath);
      // Clone the whole repo, then hoist the package subdirectory (if any).
      const githubInfo = parseGitHubUrl(githubUrl);
      if (!githubInfo) {
         throw new Error(`Invalid GitHub URL: ${githubUrl}`);
      }
      const { owner, repoName, packagePath } = githubInfo;
      // `packagePath` is captured with a leading `/`; strip it so the
      // value is a relative segment usable with `safeJoinUnderRoot`.
      const cleanPackagePath = (
         packagePath?.replace("/tree/main", "") || ""
      ).replace(/^\/+/, "");

      await fs.promises.rm(absoluteDirPath, {
         recursive: true,
         force: true,
      });
      await fs.promises.mkdir(absoluteDirPath, { recursive: true });
      const repoUrl = `https://github.com/${owner}/${repoName}`;
      const reporter = new CloneProgressReporter(
         cloneProgressLabel(`${owner}/${repoName}`, progressContext),
      );

      await new Promise<void>((resolve, reject) => {
         this.git({
            progress: (event) => reporter.onProgress(event),
         }).clone(repoUrl, absoluteDirPath, GIT_CLONE_OPTIONS, (err) => {
            reporter.done();
            if (err) {
               err.message = stripGitProgressNoise(err.message);
               if (err.stack) {
                  // V8 captures the message into the stack at construction,
                  // so the spew survives there unless stripped too.
                  err.stack = stripGitProgressNoise(err.stack);
               }
               const errorData = extractErrorDataFromError(err);
               logger.error(
                  `Failed to clone GitHub repository "${repoUrl}"`,
                  errorData,
               );
               reject(err);
               return;
            }
            resolve();
         });
      });

      if (!cleanPackagePath) {
         logger.info(
            `Successfully cloned entire repository to: ${absoluteDirPath}`,
         );
         return;
      }

      // Replace the clone with the subdirectory's contents, e.g.
      // <dir>/imdb/publisher.json becomes <dir>/publisher.json.
      const packageFullPath = safeJoinUnderRoot(
         absoluteDirPath,
         cleanPackagePath,
      );

      const packageExists = await fs.promises
         .access(packageFullPath)
         .then(() => true)
         .catch(() => false);

      if (!packageExists) {
         throw new Error(
            `Package path "${cleanPackagePath}" does not exist in the cloned repository.`,
         );
      }

      const dirContents = await fs.promises.readdir(absoluteDirPath);
      for (const entry of dirContents) {
         if (entry !== cleanPackagePath.replace(/^\/+/, "").split("/")[0]) {
            await fs.promises.rm(safeJoinUnderRoot(absoluteDirPath, entry), {
               recursive: true,
               force: true,
            });
         }
      }

      const packageContents = await fs.promises.readdir(packageFullPath);
      for (const entry of packageContents) {
         await fs.promises.rename(
            safeJoinUnderRoot(packageFullPath, entry),
            safeJoinUnderRoot(absoluteDirPath, entry),
         );
      }

      await fs.promises.rm(packageFullPath, { recursive: true, force: true });
   }
}
