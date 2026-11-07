// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { components } from "../api";
import { API_PREFIX } from "../constants";
import { BadRequestError, ModelNotFoundError } from "../errors";
import {
   type AccessIdentity,
   type AccessRecordBase,
   type QueryAudit,
   recordRefusal,
   withAccessRecord,
} from "../service/access_record";
import {
   buildQueryMetadataInput,
   parseQueryClass,
   parseSuppliedQueryMetadata,
   type QueryClass,
   type QueryMetadata,
} from "../service/query_metadata";
import { resolveQueryModel, runModelQuery } from "../service/run_model_query";
import { EnvironmentStore } from "../service/environment_store";
import type { FilterParams } from "../service/filter";
import type { GivenValue } from "@malloydata/malloy";

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
    * Orthogonal to {@link bypassFilters} — see {@link Model.getQueryResults}.
    */
   bypassAuthorize?: boolean;
   /** Lift the package's surface for this request (see {@link Model.getQueryResults}). */
   includeHiddenFilesAndSources?: boolean;
   /** Aborted when the HTTP client disconnects: cancels the query with it. */
   clientSignal?: AbortSignal;
   /** The caller from the deployment's header and the trace id, for the access record. */
   identity: AccessIdentity;
}

function accessBase(
   environmentName: string,
   packageName: string,
   modelPath: string,
   sourceName: string | undefined,
   queryName: string | undefined,
   query: string | undefined,
   identity: AccessIdentity,
): AccessRecordBase {
   return {
      environment: environmentName,
      package: packageName,
      modelPath,
      ...(queryName ? { queryName } : {}),
      ...(sourceName ? { requestedSource: sourceName } : {}),
      adHoc: Boolean(query),
      ...identity,
   };
}

export class QueryController {
   private environmentStore: EnvironmentStore;

   constructor(environmentStore: EnvironmentStore) {
      this.environmentStore = environmentStore;
   }

   /**
    * Answer a query and emit its access record (`access_record.ts`), on
    * success and on refusal alike.
    */
   public async getQuery(request: QueryRequest): Promise<ApiQuery> {
      return withAccessRecord(
         accessBase(
            request.environmentName,
            request.packageName,
            request.modelPath,
            request.sourceName,
            request.queryName,
            request.query,
            request.identity,
         ),
         request.clientSignal,
         (audit) => this.runQuery(audit, request),
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
      return recordRefusal(
         accessBase(
            environmentName,
            packageName,
            modelPath,
            body.sourceName,
            body.queryName,
            body.query,
            identity,
         ),
         error,
      );
   }

   private async runQuery(
      audit: QueryAudit,
      {
         environmentName,
         packageName,
         modelPath,
         sourceName,
         queryName,
         query,
         compactJson = false,
         filterParams,
         bypassFilters,
         givens,
         queryMetadata,
         queryClass: rawQueryClass,
         versionId,
         bypassAuthorize,
         includeHiddenFilesAndSources,
         clientSignal,
      }: QueryRequest,
   ): Promise<ApiQuery> {
      let requestMetadata: QueryMetadata | undefined;
      let queryClass: QueryClass | undefined;
      try {
         requestMetadata = parseSuppliedQueryMetadata(queryMetadata);
         queryClass = parseQueryClass(rawQueryClass);
      } catch (error) {
         throw new BadRequestError((error as Error).message);
      }

      const { environment, pkg, model } = await resolveQueryModel(
         this.environmentStore,
         environmentName,
         packageName,
         modelPath,
         versionId,
      );
      const version = pkg.getVersionId();
      if (version) audit.version = version;
      if (!model) {
         // Worded exactly as Model.assertQueryBoundaryEarly's generic refusal,
         // so a 404 cannot tell a gated hidden file from a missing one.
         throw new ModelNotFoundError(`No queryable model "${modelPath}".`);
      }
      const {
         serializedResult,
         rowLimit,
         rowLimitSource,
         queryCorrelationId,
         servedFrom,
         executionTimeMs,
         queryCostBytes,
         renderLogs,
      } = await runModelQuery(audit, pkg, modelPath, model, {
         sourceName,
         queryName,
         query,
         filterParams,
         bypassFilters,
         givens,
         queryMetadataInput: buildQueryMetadataInput({
            environmentName,
            environment,
            pkg,
            request: requestMetadata,
            queryClass,
            // The version that answered (latest when none was named), not the
            // raw request value; undefined for a package with no versions.
            version,
            noLayersLogMessage: "No query-metadata layers for connection",
         }),
         // Serialize and byte-cap exactly the shape this request receives.
         shape: compactJson ? "compact" : "full",
         bypassAuthorize,
         includeHiddenFilesAndSources,
         clientSignal,
      });
      return {
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
   }
}
