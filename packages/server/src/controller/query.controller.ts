import { validateRenderTags } from "@malloydata/render-validator";
import { components } from "../api";
import { getQueryTimeoutMs } from "../config";
import { API_PREFIX } from "../constants";
import { BadRequestError, ModelNotFoundError } from "../errors";
import { logger } from "../logger";
import {
   mintCorrelationId,
   parseQueryClass,
   parseSuppliedQueryMetadata,
   type QueryClass,
   type QueryMetadata,
} from "../service/query_metadata";
import { runWithQueryTimeout } from "../query_timeout";
import { EnvironmentStore } from "../service/environment_store";
import type { FilterParams } from "../service/filter";
import type { GivenValue } from "@malloydata/malloy";

type ApiQuery = components["schemas"]["QueryResult"];

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
      /**
       * The request's per-query metadata fields, unvalidated — this controller is
       * the boundary that turns a bad bag into a 400 rather than letting the
       * connector refuse the statement at dispatch.
       */
      metadata?: {
         queryMetadata?: unknown;
         queryClass?: unknown;
         versionId?: string;
      },
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
      // Shed load before any disk / DB work so the publisher returns 503
      // immediately under memory pressure instead of starting a query and
      // crashing partway through. Already-loaded packages bypass the
      // package-load admission gate, so this is the only thing protecting
      // query traffic on a hot pod.
      environment.assertCanAdmitQuery();
      const p = await environment.getPackage(packageName, false);
      const model = p.getModel(modelPath);

      if (!model) {
         throw new ModelNotFoundError(`${modelPath} does not exist`);
      } else {
         const {
            result,
            serializeResult,
            serializeCompactResult,
            rowLimit,
            rowLimitSource,
            queryCorrelationId,
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
                  {
                     request: requestMetadata,
                     queryClass,
                     environment: environmentName,
                     // Always undefined today: the route 501s any versionId
                     // before this runs. Wired so that lifting that rejection
                     // is the whole change.
                     version: metadata?.versionId,
                     // Minted here because this is the boundary that returns
                     // it; a path with nowhere to put it does not mint one.
                     correlationId: mintCorrelationId(),
                     // The environment owns the connection configs, so the
                     // default and enforced layers are read here rather than
                     // from the model.
                     connectionMetadata: (connectionName) => {
                        try {
                           const connection =
                              environment.getApiConnection(connectionName);
                           return {
                              default: connection.queryMetadata,
                              enforced: connection.queryMetadataEnforced,
                           };
                        } catch (error) {
                           // Failing open is right — a tag must not fail a
                           // query — but this is the one drop with no metric
                           // behind it, and what it costs is the ENFORCED
                           // layer. Log it so it is diagnosable.
                           logger.debug(
                              "No query-metadata layers for connection",
                              { connectionName, error },
                           );
                           return null;
                        }
                     },
                  },
               ),
            getQueryTimeoutMs(),
         );
         const renderLogs = validateRenderTags(result);
         return {
            // Both shapes go through the model's guarded serializers rather than
            // being stringified here. Two reasons: when a byte cap is set those
            // bytes have already been built to measure them, so this is free; and
            // a payload too large to serialize reports the same 413 the cap does
            // instead of throwing a bare 500 out of this expression. Each is
            // called only on the branch that sends it, so neither request pays
            // for the payload it does not use.
            result: compactJson ? serializeCompactResult() : serializeResult(),
            resource: `${API_PREFIX}/environments/${environmentName}/packages/${packageName}/models/${modelPath}/query`,
            renderLogs: renderLogs.length > 0 ? renderLogs : undefined,
            // The cap the database applied. A caller counting the rows it got
            // back cannot otherwise tell a complete result from one the limit
            // cut off: a query with no LIMIT of its own silently gets the
            // server default, which is well under the hard ceiling, so nothing
            // raises. Deriving it client-side is not possible, since it depends
            // on server config and on the query's own LIMIT.
            queryRowLimit: rowLimit,
            // Which of the two that cap was. Without it a caller cannot tell a
            // deliberate `limit:`/`top:` from the silently-applied default, and
            // so cannot reproduce the MCP envelope's _limit_hit.
            queryRowLimitSource: rowLimitSource,
            queryCorrelationId,
         } as ApiQuery;
      }
   }
}
