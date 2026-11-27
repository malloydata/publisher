// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { createHash } from "crypto";
import fs from "fs";
import path from "path";
import type { LoadPackageOutcome } from "../package_load/package_load_pool";
import {
   boundedDeclaration,
   UNREADABLE_SHA,
} from "../service/package_revision";
import {
   readPackageWorkflows,
   workflowsToWire,
} from "../service/skills/package_workflows";

/**
 * What a load worker ships for a package's serving identity, for specs that
 * hand a Package an outcome without a pool: the walk of `declared` over
 * `packagePath`, the declaration's sha, and the sha of each of `modelPaths` as
 * read from disk. The walk goes through a structured clone, as it does across
 * the worker boundary.
 */
export function servingIdentityOutcome(
   packagePath: string,
   declared: unknown,
   modelPaths: readonly string[],
): LoadPackageOutcome {
   const declaration = boundedDeclaration(declared);
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
         workflowsRead: structuredClone({
            ...(declaration.sha !== undefined && {
               declarationSha: declaration.sha,
            }),
            walk: workflowsToWire(
               readPackageWorkflows(packagePath, declaration.value),
            ),
         }),
      },
      models: modelPaths.map((modelPath) => ({
         modelPath,
         modelType: "model" as const,
      })),
      modelShas,
   } as unknown as LoadPackageOutcome;
}
