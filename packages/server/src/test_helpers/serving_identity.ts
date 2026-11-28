// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { createHash } from "crypto";
import fs from "fs";
import path from "path";
import { pathToFileURL } from "url";
import { urlKey } from "../package_load/job_files";
import type { LoadPackageOutcome } from "../package_load/package_load_pool";
import { servedContentSha } from "../package_load/served_content_sha";
import { boundedDeclaration } from "../service/package_revision";
import {
   readPackageWorkflows,
   workflowsToWire,
} from "../service/skills/package_workflows";

/**
 * What a load worker ships for a package's serving identity, for specs that
 * hand a Package an outcome without a pool: the walk of `declared` (the
 * manifest's `skillSets` and `workflows`) over `packagePath`, and the worker's
 * own source content sha over it and the digest of each of `modelPaths` as read
 * from disk. The walk goes through a structured clone, as it does across the
 * worker boundary.
 */
export function servingIdentityOutcome(
   packagePath: string,
   declared: { skillSets?: unknown; workflows?: unknown },
   modelPaths: readonly string[],
   bundledSkillNames: readonly string[] = [],
): LoadPackageOutcome {
   const declaration = boundedDeclaration(declared);
   const walk = readPackageWorkflows(
      packagePath,
      declaration.value,
      bundledSkillNames,
   );
   // A model the load never read has no digest, which the worker's function turns into the unreadable sha.
   const fileDigests: Record<string, string> = {};
   for (const modelPath of modelPaths) {
      try {
         fileDigests[
            urlKey(pathToFileURL(path.join(packagePath, modelPath)).href)
         ] = createHash("sha256")
            .update(fs.readFileSync(path.join(packagePath, modelPath)))
            .digest("hex");
      } catch {
         // Left out, as a worker leaves out a file it did not read.
      }
   }
   return {
      packageMetadata: {
         workflowsRead: structuredClone({ walk: workflowsToWire(walk) }),
      },
      models: modelPaths.map((modelPath) => ({
         modelPath,
         modelType: "model" as const,
      })),
      sourceContentSha: servedContentSha(
         packagePath,
         modelPaths.map((modelPath) => ({ modelPath })),
         fileDigests,
         new Map(),
         {
            walk,
            ...(declaration.sha !== undefined && {
               declarationSha: declaration.sha,
            }),
         },
      ),
   } as unknown as LoadPackageOutcome;
}
