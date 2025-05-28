import { components } from "../api";
import { ModelNotFoundError } from "../errors";
import { ProjectStore } from "../service/project_store";

type ApiModel = components["schemas"]["Model"];
type ApiCompiledModel = components["schemas"]["CompiledModel"];
type ApiCompiledNotebook = components["schemas"]["CompiledNotebook"];
export type ListModelsFilterEnum =
   components["parameters"]["ListModelsFilterEnum"];
export class ModelController {
   private projectStore: ProjectStore;

   constructor(projectStore: ProjectStore) {
      this.projectStore = projectStore;
   }

   public async listModels(
      projectName: string,
      packageName: string,
      filter: ListModelsFilterEnum,
   ): Promise<ApiModel[]> {
      const project = await this.projectStore.getProject(projectName, false);
      const p = await project.getPackage(packageName, false);
      return p
         .listModels()
         .filter((model: ApiModel) => filter === "all" || model.type == filter);
   }

   public async getModel(
      projectName: string,
      packageName: string,
      modelPath: string,
   ): Promise<ApiCompiledModel> {
      const project = await this.projectStore.getProject(projectName, false);
      const p = await project.getPackage(packageName, false);
      const model = p.getModel(modelPath);
      if (!model) {
         throw new ModelNotFoundError(`${modelPath} does not exist`);
      }
      if (model.getType() === "notebook") {
         throw new ModelNotFoundError(`${modelPath} is a notebook`);
      }
      return model.getModel();
   }

   public async getNotebook(
      projectName: string,
      packageName: string,
      notebookPath: string,
   ): Promise<ApiCompiledNotebook> {
      const project = await this.projectStore.getProject(projectName, false);
      const p = await project.getPackage(packageName, false);
      const model = p.getModel(notebookPath);
      if (!model) {
         throw new ModelNotFoundError(`${notebookPath} does not exist`);
      }
      if (model.getType() === "model") {
         throw new ModelNotFoundError(`${notebookPath} is a model`);
      }

      return model.getNotebook();
   }
}
