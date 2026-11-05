// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { validateRenderTags } from "@malloydata/render-validator";
import { components } from "../api";
import { getQueryTimeoutMs } from "../config";
import { API_PREFIX } from "../constants";
import { BadRequestError, ModelNotFoundError } from "../errors";
import { type QueryAudit, withAccessRecord } from "../service/access_record";
import {
   buildQueryMetadataInput,
   parseQueryClass,
   parseSuppliedQueryMetadata,
   type QueryClass,
   type QueryMetadata,
} from "../service/query_metadata";
import { runWithQueryTimeout } from "../query_timeout";
import { filterPublisherOwnedRenderLogs } from "../service/report_manifest";
import { EnvironmentStore } from "../service/environment_store";
import type { FilterParams } from "../service/filter";
import type { GivenValue } from "@malloydata/malloy";

type ApiQuery = components["schemas"]["QueryResult"];

export class QueryController {
   private environmentStore: EnvironmentStore;

   constructor(environmentStore: EnvironmentStore) {
      this.environmentStore = environmentStore;
   }

   /**
    * Answer a query and emit its access record (`access_record.ts`), on
    * success and on refusal alike. `identity` is what only the HTTP boundary
    * knows: the caller from the deployment's header and the trace id.
    */
   public async getQuery(
      environmentName: string,
      packageName: string,
      modelPath: string,
      sourceName: string,
      queryName: string,
      query: string,
      compactJson: boolean = false,
      filterParams?: FilterParams,
      bypassFilters?: boolean,
      givens?: Record<string, GivenValue>,
      metadata?: {
         queryMetadata?: unknown;
         queryClass?: unknown;
         versionId?: unknown;
      },
      bypassAuthorize?: boolean,
      includeHiddenFilesAndSources?: boolean,
      clientSignal?: AbortSignal,
      identity: { caller?: string; traceId?: string } = {},
   ): Promise<ApiQuery> {
      return withAccessRecord(
         {
            environment: environmentName,
            package: packageName,
            modelPath,
            ...(queryName ? { queryName } : {}),
            ...(sourceName ? { requestedSource: sourceName } : {}),
            adHoc: Boolean(query),
            ...(identity.caller ? { caller: identity.caller } : {}),
            ...(identity.traceId ? { traceId: identity.traceId } : {}),
         },
         clientSignal,
         (audit) =>
            this.runQuery(
               audit,
               environmentName,
               packageName,
               modelPath,
               sourceName,
               queryName,
               query,
               compactJson,
               filterParams,
               bypassFilters,
               givens,
               metadata,
               bypassAuthorize,
               includeHiddenFilesAndSources,
               clientSignal,
            ),
      );
   }

   private async runQuery(
      audit: QueryAudit,
      environmentName: string,
      packageName: string,
      modelPath: string,
      sourceName: string,
      queryName: string,
      query: string,
      compactJson: boolean = false,
      filterParams?: FilterParams,
      bypassFilters?: boolean,
      givens?: Record<string, GivenValue>,
      /** Unvalidated: a bad bag becomes a 400 here, not a connector refusal. */
      metadata?: {
         queryMetadata?: unknown;
         queryClass?: unknown;
         versionId?: unknown;
      },
      /**
       * Skip `#(authorize)` gates for this request. Set from the
       * `x-publisher-bypass-authorize` request header only, never from the body.
       * Orthogonal to {@link bypassFilters} — see {@link Model.getQueryResults}.
       */
      bypassAuthorize?: boolean,
      /** Lift the package's surface for this request (see {@link Model.getQueryResults}). */
      includeHiddenFilesAndSources?: boolean,
      /** Aborted when the HTTP client disconnects: cancels the query with it. */
      clientSignal?: AbortSignal,
   ): Promise<ApiQuery> {
      let requestMetadata: QueryMetadata | undefined;
      let queryClass: QueryClass | undefined;
      try {
         requestMetadata = parseSuppliedQueryMetadata(metadata?.queryMetadata);
         queryClass = parseQueryClass(metadata?.queryClass);
      } catch (error) {
         throw new BadRequestError((error as Error).message);
      }

      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      // Shed load (503) before any disk / DB work rather than crash partway
      // through a query. Loaded packages bypass the package-load admission
      // gate, so this is the only guard for query traffic on a hot pod.
      environment.assertCanAdmitQuery();
      const p = await environment.getPackage(packageName, false, {
         versionId: metadata?.versionId,
      });
      const version = p.getVersionId();
      if (version) audit.version = version;
      const model = p.getModel(modelPath);

      if (!model) {
         // Worded exactly as Model.assertQueryBoundaryEarly's generic refusal,
         // so a 404 cannot tell a gated hidden file from a missing one.
         throw new ModelNotFoundError(`No queryable model "${modelPath}".`);
      } else {
         const {
            result,
            serializedResult,
            rowLimit,
            rowLimitSource,
            queryCorrelationId,
            servedFrom,
            executionTimeMs,
            queryCostBytes,
         } = await runWithQueryTimeout(
            (abortSignal) =>
               model.getQueryResults(
                  audit,
                  sourceName,
                  queryName,
                  query,
                  filterParams,
                  bypassFilters,
                  givens,
                  abortSignal,
                  buildQueryMetadataInput({
                     environmentName,
                     environment,
                     pkg: p,
                     request: requestMetadata,
                     queryClass,
                     // The version that answered (latest when none was
                     // named), not the raw request value; undefined for a
                     // package with no versions.
                     version: p.getVersionId(),
                     noLayersLogMessage:
                        "No query-metadata layers for connection",
                  }),
                  // Serialize and byte-cap exactly the shape this request
                  // receives, not the other one.
                  compactJson ? "compact" : "full",
                  bypassAuthorize,
                  includeHiddenFilesAndSources,
               ),
            getQueryTimeoutMs(),
            clientSignal,
         );
         const renderLogs = filterPublisherOwnedRenderLogs(
            validateRenderTags(result),
            p.getModel(modelPath)?.carriesReportTag() ?? false,
         );
         const api = {
            // Serialized by the model: re-stringifying here would copy the
            // payload, and an oversized one would surface as a bare 500 rather
            // than the byte cap's 413.
            result: serializedResult,
            resource: `${API_PREFIX}/environments/${environmentName}/packages/${packageName}/models/${modelPath}/query`,
            renderLogs: renderLogs.length > 0 ? renderLogs : undefined,
            // The row cap the database applied and whether it was the query's
            // own or the server default, so a caller can tell a complete result
            // from a truncated one (it depends on server config).
            queryRowLimit: rowLimit,
            queryRowLimitSource: rowLimitSource,
            queryCorrelationId,
            // Reports only the `storage=` tier: a colocated `#@ persist` hit and
            // a declined transform both read `null`, like a live query. See the
            // field's description in `api-doc.yaml`.
            servedFrom,
            executionTimeMs,
            queryCostBytes,
         } as ApiQuery;
         return api;
      }
   }
}
