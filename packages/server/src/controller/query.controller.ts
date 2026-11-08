// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { components } from "../api";
import { API_PREFIX } from "../constants";
import type { AccessIdentity } from "../service/access_record";
import { VersionRef } from "../service/identity";
import type { FilterParams } from "../service/filter";
import type { GivenValue } from "@malloydata/malloy";
import { type QueryUseCase, REST_QUERY_OPTIONS } from "../usecases/query";

type ApiQuery = components["schemas"]["QueryResult"];

/** What a query request asks for, and of whom. */
export interface QueryRequest {
   environmentName: string;
   packageName: string;
   modelPath: string;
   sourceName?: string;
   queryName?: string;
   query?: string;
   compactJson?: boolean;
   filterParams?: FilterParams;
   bypassFilters?: boolean;
   givens?: Record<string, GivenValue>;
   /** Unvalidated: a bad bag becomes a 400 here, not a connector refusal. */
   queryMetadata?: unknown;
   queryClass?: unknown;
   /** The published version to query; absent, empty or `latest` means `latest`. */
   versionId?: unknown;
   /**
    * Skip `#(authorize)` gates for this request. Set from the
    * `x-publisher-bypass-authorize` request header only, never from the body.
    * Orthogonal to {@link bypassFilters} — see {@link Model.runQuery}.
    */
   bypassAuthorize?: boolean;
   /** Lift the package's surface for this request (see {@link Model.runQuery}). */
   includeHiddenFilesAndSources?: boolean;
   /** Aborted when the HTTP client disconnects: cancels the query with it. */
   clientSignal?: AbortSignal;
   /** The caller from the deployment's header and the trace id, for the access record. */
   identity: AccessIdentity;
}

/**
 * The REST adapter over {@link QueryUseCase}: runs it with
 * {@link REST_QUERY_OPTIONS} (the bypass header honored, the slot held by the
 * route's middleware, REST's 404 wording) and shapes the `QueryResult`.
 */
export class QueryController {
   constructor(private readonly queryUseCase: QueryUseCase) {}

   /**
    * Answer a query and emit its access record (`access_record.ts`), on
    * success and on refusal alike.
    */
   public async getQuery(request: QueryRequest): Promise<ApiQuery> {
      const { environmentName, packageName, modelPath } = request;
      return this.queryUseCase.run(
         { ...request, version: VersionRef.parse(request.versionId) },
         {
            ...REST_QUERY_OPTIONS,
            // Serialize and byte-cap exactly the shape this request receives.
            shape: request.compactJson === true ? "compact" : "full",
         },
         ({
            result: {
               serializedResult,
               rowLimit,
               rowLimitSource,
               queryCorrelationId,
               servedFrom,
               executionTimeMs,
               queryCostBytes,
               renderLogs,
            },
         }) =>
            ({
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
            }) as ApiQuery,
      );
   }

   /** Record a request a route refused before {@link getQuery} was called, so it is not the one request without a record. */
   public recordRefusal(
      environmentName: string,
      packageName: string,
      modelPath: string,
      body: { sourceName?: string; queryName?: string; query?: string },
      identity: AccessIdentity,
      error: Error,
   ): Promise<void> {
      return this.queryUseCase.recordRefusal(
         {
            environmentName,
            packageName,
            modelPath,
            sourceName: body.sourceName,
            queryName: body.queryName,
            query: body.query,
            identity,
         },
         error,
      );
   }
}
