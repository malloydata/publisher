import { components } from "../api";
import { API_PREFIX } from "../constants";
import { ModelNotFoundError } from "../errors";
import { ProjectStore } from "../service/project_store";

type ApiQuery = components["schemas"]["QueryResult"];

export class QueryController {
   private projectStore: ProjectStore;

   constructor(projectStore: ProjectStore) {
      this.projectStore = projectStore;
   }

   public async getQuery(
      projectName: string,
      packageName: string,
      modelPath: string,
      sourceName: string,
      queryName: string,
      query: string,
   ): Promise<ApiQuery> {
      const project = await this.projectStore.getProject(projectName, false);
      const p = await project.getPackage(packageName, false);
      const model = p.getModel(modelPath);

      if (!model) {
         throw new ModelNotFoundError(`${modelPath} does not exist`);
      } else {
         const { result } = await model.getQueryResults(
            sourceName,
            queryName,
            query,
         );
         return {
            result: JSON.stringify(result),
            resource: `${API_PREFIX}/projects/${projectName}/packages/${packageName}/models/${modelPath}/query`,
         } as ApiQuery;
      }
   }
}
