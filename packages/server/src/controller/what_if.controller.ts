// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { GivenValue } from "@malloydata/malloy";
import { components } from "../api";
import { BadRequestError } from "../errors";
import type { PackageLookup } from "../service/environment_ports";
import {
   parseEnvironmentName,
   parsePackageName,
   VersionRef,
} from "../service/identity";
import {
   parseWhatIfQueries,
   runWhatIf,
} from "../service/materialization/what_if";
import { parseOverlayFiles } from "../service/request_overlay";
import { withSpan } from "../telemetry";

type ApiWhatIf = components["schemas"]["WhatIf"];

/**
 * `POST …/packages/{pkg}/build-plan/what-if`: each query's path report under a
 * proposed change to the package's materialization annotations, once built
 * (materialization v2, T3). See `service/materialization/what_if.ts`.
 */
export class WhatIfController {
   constructor(private readonly environmentStore: PackageLookup) {}

   public async whatIf(
      environmentName: string,
      packageName: string,
      body: unknown,
      versionId: VersionRef = VersionRef.LATEST,
   ): Promise<ApiWhatIf> {
      if (!body || typeof body !== "object" || Array.isArray(body)) {
         throw new BadRequestError(
            "The body must be an object with `queries`, and optionally `files` and `givens`.",
         );
      }
      const { files, queries, givens } = body as Record<string, unknown>;
      // Checked before anything loads: an unsafe path never reaches the disk.
      const proposal = parseOverlayFiles(files) ?? [];
      const parsedQueries = parseWhatIfQueries(queries);
      if (
         givens !== undefined &&
         (givens === null ||
            typeof givens !== "object" ||
            Array.isArray(givens))
      ) {
         throw new BadRequestError("`givens` must be an object.");
      }
      const environment = await this.environmentStore.getEnvironment(
         parseEnvironmentName(environmentName),
         false,
      );
      const current = await environment.getPackage(
         parsePackageName(packageName),
         false,
         { versionId },
      );
      return withSpan(
         "publisher.materialization.what_if",
         { "malloy.package": packageName },
         () =>
            runWhatIf(environment, current, packageName, {
               files: proposal,
               queries: parsedQueries,
               ...(givens !== undefined
                  ? { givens: givens as Record<string, GivenValue> }
                  : {}),
            }),
      );
   }
}
