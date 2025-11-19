import { components } from "../api";
import { ModelNotFoundError } from "../errors";
import { ProjectStore } from "../service/project_store";

type ApiNotebook = components["schemas"]["Notebook"];
type ApiModel = components["schemas"]["Model"];
type ApiCompiledModel = components["schemas"]["CompiledModel"];
type ApiRawNotebook = components["schemas"]["RawNotebook"];
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
   ): Promise<ApiModel[]> {
      const project = await this.projectStore.getProject(projectName, false);
      const p = await project.getPackage(packageName, false);
      return p.listModels();
   }

   public async listNotebooks(
      projectName: string,
      packageName: string,
   ): Promise<ApiNotebook[]> {
      const project = await this.projectStore.getProject(projectName, false);
      const p = await project.getPackage(packageName, false);
      return p.listNotebooks();
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
   ): Promise<ApiRawNotebook> {
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

   public async executeNotebookCell(
      projectName: string,
      packageName: string,
      notebookPath: string,
      cellIndex: number,
   ): Promise<{
      type: "code" | "markdown";
      text: string;
      queryName?: string;
      result?: string;
      newSources?: string[];
   }> {
      const project = await this.projectStore.getProject(projectName, false);
      const p = await project.getPackage(packageName, false);
      const model = p.getModel(notebookPath);
      if (!model) {
         throw new ModelNotFoundError(`${notebookPath} does not exist`);
      }
      if (model.getType() === "model") {
         throw new ModelNotFoundError(`${notebookPath} is a model`);
      }

      return model.executeNotebookCell(cellIndex);
   }
}
