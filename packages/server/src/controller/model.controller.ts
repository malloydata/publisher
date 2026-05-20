import { components } from "../api";
import { ModelNotFoundError } from "../errors";
import { EnvironmentStore } from "../service/environment_store";
import type { FilterParams } from "../service/filter";

type ApiNotebook = components["schemas"]["Notebook"];
type ApiModel = components["schemas"]["Model"];
type ApiCompiledModel = components["schemas"]["CompiledModel"];
type ApiRawNotebook = components["schemas"]["RawNotebook"];
export class ModelController {
   private environmentStore: EnvironmentStore;

   constructor(environmentStore: EnvironmentStore) {
      this.environmentStore = environmentStore;
   }

   public async listModels(
      environmentName: string,
      packageName: string,
   ): Promise<ApiModel[]> {
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      const p = await environment.getPackage(packageName, false);
      return p.listModels();
   }

   public async listNotebooks(
      environmentName: string,
      packageName: string,
   ): Promise<ApiNotebook[]> {
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      const p = await environment.getPackage(packageName, false);
      return p.listNotebooks();
   }

   public async getModel(
      environmentName: string,
      packageName: string,
      modelPath: string,
   ): Promise<ApiCompiledModel> {
      try {
         const environment = await this.environmentStore.getEnvironment(
            environmentName,
            false,
         );
         const p = await environment.getPackage(packageName, false);
         const model = p.getModel(modelPath);
         if (!model) {
            throw new ModelNotFoundError(`${modelPath} does not exist`);
         }
         if (model.getType() === "notebook") {
            throw new ModelNotFoundError(`${modelPath} is a notebook`);
         }
         return await model.getModel();
      } catch (error) {
         // Re-throw ModelNotFoundError as-is
         if (error instanceof ModelNotFoundError) {
            throw error;
         }
         // Wrap other errors with more context
         throw new Error(
            `Failed to get model ${modelPath} from package ${packageName} in environment ${environmentName}: ${error}`,
         );
      }
   }

   public async getNotebook(
      environmentName: string,
      packageName: string,
      notebookPath: string,
   ): Promise<ApiRawNotebook> {
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      const p = await environment.getPackage(packageName, false);
      const model = p.getModel(notebookPath);
      if (!model) {
         throw new ModelNotFoundError(`${notebookPath} does not exist`);
      }
      if (model.getType() === "model") {
         throw new ModelNotFoundError(`${notebookPath} is a model`);
      }

      return model.getNotebook();
   }

   public async executeNotebookCell(
      environmentName: string,
      packageName: string,
      notebookPath: string,
      cellIndex: number,
      filterParams?: FilterParams,
      bypassFilters?: boolean,
   ): Promise<{
      type: "code" | "markdown";
      text: string;
      queryName?: string;
      result?: string;
      newSources?: string[];
   }> {
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      const p = await environment.getPackage(packageName, false);
      const model = p.getModel(notebookPath);
      if (!model) {
         throw new ModelNotFoundError(`${notebookPath} does not exist`);
      }
      if (model.getType() === "model") {
         throw new ModelNotFoundError(`${notebookPath} is a model`);
      }

      return model.executeNotebookCell(cellIndex, filterParams, bypassFilters);
   }
}
