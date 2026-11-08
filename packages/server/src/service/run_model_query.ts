// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The query pipeline REST and MCP share: find the model a request names, then
 * run the query under the timeout and read its render tags. What each surface
 * decides for itself (its refusals and their wording, which shape it sends,
 * the authorize bypass, the concurrency slot) stays with the caller.
 */

import type { GivenValue } from "@malloydata/malloy";
import { validateRenderTags } from "@malloydata/render-validator";
import { getQueryTimeoutMs } from "../config";
import { runWithQueryTimeout } from "../query_timeout";
import type { QueryAudit } from "./access_record";
import type { Environment } from "./environment";
import type { PackageLookup } from "./environment_ports";
import type { FilterParams } from "./filter";
import type { Model } from "./model";
import { QueryPrivileges } from "./model_query_request";
import type { Package } from "./package";
import type { ModelQueryMetadataInput } from "./query_metadata";
import { filterPublisherOwnedRenderLogs } from "./report_manifest";
import {
   type EnvironmentName,
   parseEnvironmentName,
   parsePackageName,
   tryParseModelPath,
   type VersionRef,
} from "./identity";

/**
 * The environment a query names, admitted for query traffic: shed load (503)
 * before any disk / DB work rather than crash partway through a query.
 * Loaded packages bypass the package-load admission gate, so this is the only
 * guard for query traffic on a hot pod.
 */
export async function admitQueryEnvironment(
   environmentStore: PackageLookup,
   environmentName: EnvironmentName,
): Promise<Environment> {
   const environment = await environmentStore.getEnvironment(
      environmentName,
      false,
   );
   environment.assertCanAdmitQuery();
   return environment;
}

/**
 * The environment, admitted for query traffic, and the package version and
 * model a request names. `model` is undefined when the package serves no
 * such file; how to refuse that is the caller's.
 *
 * The names arrive as the request carried them (the access record logs them
 * so) and are parsed here, in the order the lookups have always checked
 * them: the environment, its admission, then the package. A model path no
 * package could serve is no model, not a malformed request, as on the query
 * path it always was.
 */
export async function resolveQueryModel(
   environmentStore: PackageLookup,
   names: {
      environmentName: string;
      packageName: string;
      modelPath: string;
      version?: VersionRef;
   },
): Promise<{ environment: Environment; pkg: Package; model?: Model }> {
   const environment = await admitQueryEnvironment(
      environmentStore,
      parseEnvironmentName(names.environmentName),
   );
   const pkg = await environment.getPackage(
      parsePackageName(names.packageName),
      false,
      { versionId: names.version },
   );
   const modelPath = tryParseModelPath(names.modelPath);
   return {
      environment,
      pkg,
      model: modelPath === undefined ? undefined : pkg.getModel(modelPath),
   };
}

export interface ModelQueryRequest {
   sourceName?: string;
   queryName?: string;
   query?: string;
   filterParams?: FilterParams;
   bypassFilters?: boolean;
   givens?: Record<string, GivenValue>;
   queryMetadataInput: ModelQueryMetadataInput;
   /** The shape the caller sends, so exactly that one is built and byte-capped. */
   shape: "compact" | "full";
   bypassAuthorize?: boolean;
   includeHiddenFilesAndSources?: boolean;
   /** Aborted when the client goes away: cancels the query at the driver. */
   clientSignal?: AbortSignal;
}

/** Run a query under PUBLISHER_QUERY_TIMEOUT_MS and read the render tags it carries. */
export async function runModelQuery(
   audit: QueryAudit,
   pkg: Package,
   modelPath: string,
   model: Model,
   request: ModelQueryRequest,
) {
   const outcome = await runWithQueryTimeout(
      (abortSignal) =>
         model.runQuery(
            audit,
            {
               sourceName: request.sourceName,
               queryName: request.queryName,
               query: request.query,
               filterParams: request.filterParams,
               givens: request.givens,
               abortSignal,
               queryMetadataInput: request.queryMetadataInput,
               responseShape: request.shape,
            },
            QueryPrivileges.of({
               bypassFilters: request.bypassFilters,
               bypassAuthorize: request.bypassAuthorize,
               includeHiddenFilesAndSources:
                  request.includeHiddenFilesAndSources,
            }),
         ),
      getQueryTimeoutMs(),
      request.clientSignal,
   );
   // Render-tag validation reads the FULL Malloy result: the tags live in its
   // schema annotations, which the flat rows do not carry.
   const renderLogs = filterPublisherOwnedRenderLogs(
      validateRenderTags(outcome.result),
      pkg.getModel(modelPath)?.carriesReportTag() ?? false,
   );
   return { ...outcome, renderLogs };
}
