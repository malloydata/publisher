import { validateRenderTags } from "@malloydata/render-validator";
import { components } from "../api";
import { API_PREFIX } from "../constants";
import { ModelNotFoundError } from "../errors";
import { EnvironmentStore } from "../service/environment_store";
import type { FilterParams } from "../service/filter";

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
   ): Promise<ApiQuery> {
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      const p = await environment.getPackage(packageName, false);
      const model = p.getModel(modelPath);

      if (!model) {
         throw new ModelNotFoundError(`${modelPath} does not exist`);
      } else {
         const { result, compactResult } = await model.getQueryResults(
            sourceName,
            queryName,
            query,
            filterParams,
            bypassFilters,
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
