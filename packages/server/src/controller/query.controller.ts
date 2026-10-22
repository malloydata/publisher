// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { validateRenderTags } from "@malloydata/render-validator";
import { components } from "../api";
import { getQueryTimeoutMs } from "../config";
import { API_PREFIX } from "../constants";
import { BadRequestError, ConflictError, ModelNotFoundError } from "../errors";
import {
   decodeQueryJobId,
   encodeQueryJobId,
   QueryJobNotFoundError,
   snowflakeQueryJobClient,
   submittingConnection,
} from "../service/snowflake_query_jobs";
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
type ApiQueryJob = components["schemas"]["QueryJob"];
type ApiQueryJobStatus = components["schemas"]["QueryJobStatus"];

/** One row range of a finished query job, rows read as they are sent. */
export interface QueryJobPage {
   totalRows: number;
   offset: number;
   columns: string[];
   rows: AsyncIterable<Record<string, unknown>>;
}

export class QueryController {
   private environmentStore: EnvironmentStore;

   constructor(environmentStore: EnvironmentStore) {
      this.environmentStore = environmentStore;
   }

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
      /** Unvalidated: a bad bag becomes a 400 here, not a connector refusal. */
      metadata?: {
         queryMetadata?: unknown;
         queryClass?: unknown;
         versionId?: string;
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
      const p = await environment.getPackage(packageName, false);
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
                     // Always undefined: the route 501s any versionId first.
                     version: metadata?.versionId,
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

   /**
    * Submit a model query as a Snowflake query job (see
    * `service/snowflake_query_jobs.ts`). Every gate runs exactly as for
    * {@link getQuery}; only the run differs: the compiled SQL is submitted
    * with `asyncExec`, and this answers once Snowflake has accepted it.
    */
   public async submitQueryJob(
      environmentName: string,
      packageName: string,
      modelPath: string,
      sourceName: string,
      queryName: string,
      query: string,
      filterParams?: FilterParams,
      bypassFilters?: boolean,
      givens?: Record<string, GivenValue>,
      metadata?: { queryMetadata?: unknown; queryClass?: unknown },
      bypassAuthorize?: boolean,
      includeHiddenFilesAndSources?: boolean,
      clientSignal?: AbortSignal,
   ): Promise<ApiQueryJob> {
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
      environment.assertCanAdmitQuery();
      const p = await environment.getPackage(packageName, false);
      const model = p.getModel(modelPath);
      if (!model) {
         throw new ModelNotFoundError(`No queryable model "${modelPath}".`);
      }

      let submitted: { queryId: string; connectionName: string } | undefined;
      const { queryCorrelationId } = await runWithQueryTimeout(
         (abortSignal) =>
            model.getQueryResults(
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
                  noLayersLogMessage: "No query-metadata layers for connection",
               }),
               "compact",
               bypassAuthorize,
               includeHiddenFilesAndSources,
               (connectionName) => {
                  const apiConnection =
                     environment.getApiConnection(connectionName);
                  if (
                     apiConnection.type !== "snowflake" ||
                     !apiConnection.snowflakeConnection
                  ) {
                     throw new BadRequestError(
                        `Query jobs run on Snowflake connections; "${connectionName}" is ${apiConnection.type}.`,
                     );
                  }
                  return submittingConnection(
                     connectionName,
                     snowflakeQueryJobClient(
                        environmentName,
                        connectionName,
                        apiConnection.snowflakeConnection,
                     ),
                     (queryId) => (submitted = { queryId, connectionName }),
                  );
               },
            ),
         getQueryTimeoutMs(),
         clientSignal,
      );
      if (!submitted) {
         // Answered from storage, so no statement reached the warehouse.
         throw new BadRequestError(
            "This query is not answered by the warehouse, so it cannot run as a query job.",
         );
      }
      const jobId = encodeQueryJobId({
         queryId: submitted.queryId,
         environment: environmentName,
         package: packageName,
         model: modelPath,
         connection: submitted.connectionName,
      });
      return {
         jobId,
         state: "running",
         snowflakeQueryId: submitted.queryId,
         queryCorrelationId,
         resource: `${API_PREFIX}/environments/${environmentName}/packages/${packageName}/models/${modelPath}/query-jobs/${jobId}`,
      };
   }

   private async queryJob(
      environmentName: string,
      packageName: string,
      modelPath: string,
      jobId: string,
   ) {
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      const p = await environment.getPackage(packageName, false);
      if (!p.getModel(modelPath)) {
         throw new ModelNotFoundError(`No queryable model "${modelPath}".`);
      }
      const claims = decodeQueryJobId(jobId, {
         environment: environmentName,
         package: packageName,
         model: modelPath,
      });
      const apiConnection = environment.getApiConnection(claims.connection);
      if (!apiConnection.snowflakeConnection) {
         throw new QueryJobNotFoundError("No such query job.");
      }
      const client = snowflakeQueryJobClient(
         environmentName,
         claims.connection,
         apiConnection.snowflakeConnection,
      );
      return { claims, client };
   }

   public async getQueryJob(
      environmentName: string,
      packageName: string,
      modelPath: string,
      jobId: string,
   ): Promise<ApiQueryJobStatus> {
      const { claims, client } = await this.queryJob(
         environmentName,
         packageName,
         modelPath,
         jobId,
      );
      const status = await client.status(claims.queryId);
      return {
         jobId,
         snowflakeQueryId: claims.queryId,
         ...status,
         ...(status.state === "succeeded"
            ? { totalRows: (await client.result(claims.queryId)).totalRows }
            : {}),
      };
   }

   /** Rows `offset` to `offset + limit` of a finished job; 409 until it has succeeded. */
   public async getQueryJobPage(
      environmentName: string,
      packageName: string,
      modelPath: string,
      jobId: string,
      offset: number,
      limit: number,
   ): Promise<QueryJobPage> {
      const { claims, client } = await this.queryJob(
         environmentName,
         packageName,
         modelPath,
         jobId,
      );
      const status = await client.status(claims.queryId);
      if (status.state !== "succeeded") {
         throw new ConflictError(
            `Query job is ${status.state}${status.error ? `: ${status.error.message}` : ""}.`,
         );
      }
      const result = await client.result(claims.queryId);
      const start = Math.min(offset, result.totalRows);
      const end = Math.min(offset + limit, result.totalRows) - 1;
      return {
         totalRows: result.totalRows,
         offset: start,
         columns: result.columns,
         rows: end >= start ? result.rows(start, end) : emptyRows(),
      };
   }

   public async cancelQueryJob(
      environmentName: string,
      packageName: string,
      modelPath: string,
      jobId: string,
   ): Promise<{ jobId: string; cancelRequested: true }> {
      const { claims, client } = await this.queryJob(
         environmentName,
         packageName,
         modelPath,
         jobId,
      );
      await client.cancel(claims.queryId);
      return { jobId, cancelRequested: true };
   }
}

async function* emptyRows(): AsyncIterable<Record<string, unknown>> {}
