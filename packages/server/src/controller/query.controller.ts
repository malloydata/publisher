import { validateRenderTags } from "@malloydata/render-validator";
import { components } from "../api";
import { getQueryTimeoutMs } from "../config";
import { API_PREFIX } from "../constants";
import { ModelNotFoundError } from "../errors";
import { runWithQueryTimeout } from "../query_timeout";
import { EnvironmentStore } from "../service/environment_store";
import type { FilterParams } from "../service/filter";
import type { GivenValue } from "@malloydata/malloy";

type ApiQuery = components["schemas"]["QueryResult"];

// Replacer function to handle BigInt serialization
function bigIntReplacer(_key: string, value: unknown): unknown {
   if (typeof value === "bigint") {
      return Number(value);
   }
   return value;
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
   ): Promise<ApiQuery> {
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
         const { result, compactResult } = await runWithQueryTimeout(
            (abortSignal) =>
               model.getQueryResults(
                  sourceName,
                  queryName,
                  query,
                  filterParams,
                  bypassFilters,
                  givens,
                  abortSignal,
               ),
            getQueryTimeoutMs(),
         );
         const renderLogs = validateRenderTags(result);
         return {
            result: compactJson
               ? JSON.stringify(compactResult, bigIntReplacer)
               : JSON.stringify(result),
            resource: `${API_PREFIX}/environments/${environmentName}/packages/${packageName}/models/${modelPath}/query`,
            renderLogs: renderLogs.length > 0 ? renderLogs : undefined,
         } as ApiQuery;
      }
   }
}
