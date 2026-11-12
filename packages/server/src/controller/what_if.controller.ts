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
import { runHypothetical } from "../hypothetical_scope";
import { withSpan } from "../telemetry";

type ApiWhatIf = components["schemas"]["WhatIf"];

/**
 * `POST …/packages/{pkg}/build-plan/what-if`: each query's path report under a
 * proposed change to the package's materialization annotations, once built
 * (materialization v2, T3). See `service/materialization/what_if.ts`.
 */
export class WhatIfController {
   constructor(private readonly environmentStore: PackageLookup) {}

   /**
    * Runs as hypothetical work (hypothetical_scope.ts): nothing here serves
    * anyone, so the throwaway load, bind and routes write no operator log and
    * move no metric, the request overlay count included.
    */
   public async whatIf(
      environmentName: string,
      packageName: string,
      body: unknown,
      versionId: VersionRef = VersionRef.LATEST,
      options: { includeHiddenFilesAndSources?: boolean } = {},
   ): Promise<ApiWhatIf> {
      return runHypothetical(() =>
         this.answer(environmentName, packageName, body, versionId, options),
      );
   }

   private async answer(
      environmentName: string,
      packageName: string,
      body: unknown,
      versionId: VersionRef,
      options: { includeHiddenFilesAndSources?: boolean },
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
               includeHiddenFilesAndSources:
                  !!options.includeHiddenFilesAndSources,
               ...(givens !== undefined
                  ? { givens: givens as Record<string, GivenValue> }
                  : {}),
            }),
      );
   }
}
