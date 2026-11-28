// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { createHash } from "crypto";
import fs from "fs";
import path from "path";
import type { LoadPackageOutcome } from "../package_load/package_load_pool";
import {
   boundedDeclaration,
   computeSourceContentSha,
   UNREADABLE_SHA,
} from "../service/package_revision";
import {
   readPackageWorkflows,
   servedContentShas,
   workflowsToWire,
} from "../service/skills/package_workflows";

/**
 * What a load worker ships for a package's serving identity, for specs that
 * hand a Package an outcome without a pool: the walk of `declared` (the
 * manifest's `skillSets` and `workflows`) over `packagePath`, and the source
 * content sha over it and the sha of each of `modelPaths` as read from disk.
 * The walk goes through a structured clone, as it does across the worker
 * boundary.
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
   const modelShas = new Map(
      modelPaths.map((modelPath): [string, string] => {
         try {
            return [
               modelPath,
               createHash("sha256")
                  .update(fs.readFileSync(path.join(packagePath, modelPath)))
                  .digest("hex"),
            ];
         } catch {
            return [modelPath, UNREADABLE_SHA];
         }
      }),
   );
   return {
      packageMetadata: {
         workflowsRead: structuredClone({ walk: workflowsToWire(walk) }),
      },
      models: modelPaths.map((modelPath) => ({
         modelPath,
         modelType: "model" as const,
      })),
      sourceContentSha: computeSourceContentSha(
         new Map([...modelShas, ...servedContentShas(walk)]),
         declaration.sha,
      ),
   } as unknown as LoadPackageOutcome;
}
