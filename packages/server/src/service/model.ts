
import {
   FixedConnectionMap,
   ModelDef,
   ModelMaterializer,
   NamedModelObject,
   NamedQuery,
   QueryMaterializer,
   Result,
   Runtime,
   StructDef,
   TurtleDef,
   isSourceDef,
} from "@malloydata/malloy";
import {
   MalloySQLParser,
   MalloySQLStatementType,
} from "@malloydata/malloy-sql";
import malloyPackage from "@malloydata/malloy/package.json";
import { DataStyles } from "@malloydata/render";
import * as fs from "fs/promises";
import * as path from "path";
import { components } from "../api";
import { HackyDataStylesAccumulator } from "../data_styles";
import {
   BadRequestError,
   ModelCompilationError,
   ModelNotFoundError,
} from "../errors";
import {
   URL_READER,
   MODEL_FILE_SUFFIX,
   NOTEBOOK_FILE_SUFFIX,
   getWorkingDirectory,
} from "../utils";

type ApiCompiledModel = components["schemas"]["CompiledModel"];
type ApiNotebookCell = components["schemas"]["NotebookCell"];
type ApiSource = components["schemas"]["Source"];
type ApiView = components["schemas"]["View"];
type ApiQuery = components["schemas"]["Query"];
export type ApiConnection = components["schemas"]["Connection"];

const MALLOY_VERSION = malloyPackage.version;

export type ModelType = "model" | "notebook";

interface RunnableNotebookCell {
   type: "code" | "markdown";
   text: string;
   runnable?: QueryMaterializer;
}

export class Model {
   private packageName: string;
   private modelPath: string;
   private dataStyles: DataStyles;
   private modelType: ModelType;
   private modelMaterializer: ModelMaterializer | undefined;
   private modelDef: ModelDef | undefined;
   private sources: ApiSource[] | undefined;
   private queries: ApiQuery[] | undefined;
   private runnableNotebookCells: RunnableNotebookCell[] | undefined;
   private compilationError: string | undefined;

   constructor(
      packageName: string,
      modelPath: string,
      dataStyles: DataStyles,
      modelType: ModelType,
      modelMaterializer: ModelMaterializer | undefined,
      modelDef: ModelDef | undefined,
      sources: ApiSource[] | undefined,
      queries: ApiQuery[] | undefined,
      runnableNotebookCells: RunnableNotebookCell[] | undefined,
      compilationError: string | undefined,
   ) {
      this.packageName = packageName;
      this.modelPath = modelPath;
      this.dataStyles = dataStyles;
      this.modelType = modelType;
      this.modelDef = modelDef;
      this.modelMaterializer = modelMaterializer;
      this.sources = sources;
      this.queries = queries;
      this.runnableNotebookCells = runnableNotebookCells;
      this.compilationError = compilationError;
   }

   public static async create(
      packageName: string,
      modelPath: string,
      connections: FixedConnectionMap,
   ): Promise<Model> {
      // getModelRuntime might throw a ModelNotFoundError. It's the callers responsibility
      // to pass a valid model path or handle the error.
      const { runtime, modelURL, importBaseURL, dataStyles, modelType } =
         await Model.getModelRuntime(packageName, modelPath, connections);

      try {
         const { modelMaterializer, runnableNotebookCells } =
            await Model.getModelMaterializer(
               runtime,
               importBaseURL,
               modelURL,
               modelPath,
            );

         let modelDef = undefined;
         let sources = undefined;
         let queries = undefined;
         if (modelMaterializer) {
            modelDef = (await modelMaterializer.getModel())._modelDef;
            sources = Model.getSources(modelPath, modelDef);
            queries = Model.getQueries(modelPath, modelDef);
         }

         return new Model(
            packageName,
            modelPath,
            dataStyles,
            modelType,
            modelMaterializer,
            modelDef,
            sources,
            queries,
            runnableNotebookCells,
            undefined,
         );
      } catch (error) {
         return new Model(
            packageName,
            modelPath,
            dataStyles,
            modelType,
            undefined,
            undefined,
            undefined,
            undefined,
            undefined,
            (error as Error).message,
         );
      }
   }

   public getPath(): string {
      return this.modelPath;
   }

   public getType(): ModelType {
      return this.modelType;
   }

   public getSources(): ApiSource[] | undefined {
      return this.sources;
   }

   public getQueries(): ApiQuery[] | undefined {
      return this.sources;
   }

   public async getModel(): Promise<ApiCompiledModel> {
      if (this.compilationError) {
         throw new ModelCompilationError(this.compilationError);
      }

      if (this.modelType === "model") {
         return this.getStandardModel();
      } else if (this.modelType === "notebook") {
         return this.getNotebookModel();
      } else {
         throw new ModelNotFoundError(
            `${this.modelPath} is not a valid model name.  Model files must end in .malloy or .malloynb.`,
         );
      }
   }

   public async getQueryResults(
      sourceName?: string,
      queryName?: string,
      query?: string,
   ): Promise<{
      queryResults: Result;
      modelDef: ModelDef;
      dataStyles: DataStyles;
   }> {
      if (this.compilationError) {
         throw new ModelCompilationError(this.compilationError);
      }

      let runnable: QueryMaterializer;
      if (!this.modelMaterializer || !this.modelDef)
         throw new BadRequestError("Model has no queryable entities.");
      if (!sourceName && !queryName && query) {
         runnable = this.modelMaterializer.loadQuery("\n" + query);
      } else if (queryName && !query) {
         runnable = this.modelMaterializer.loadQuery(
            `\nrun: ${sourceName ? sourceName + "->" : ""}${queryName}`,
         );
      } else {
         throw new BadRequestError(
            "Invalid query request. Query OR queryName must be defined.",
         );
      }
      const rowLimit =
         runnable instanceof QueryMaterializer
            ? (await runnable.getPreparedResult()).resultExplore.limit
            : undefined;
      return {
         queryResults: await runnable.run({ rowLimit }),
         modelDef: this.modelDef,
         dataStyles: this.dataStyles,
      };
   }

   private getStandardModel(): ApiCompiledModel {
      return {
         type: "source",
         packageName: this.packageName,
         modelPath: this.modelPath,
         malloyVersion: MALLOY_VERSION,
         dataStyles: JSON.stringify(this.dataStyles),
         modelDef: JSON.stringify(this.modelDef),
         sources: this.sources,
         queries: this.queries,
      } as ApiCompiledModel;
   }

   private async getNotebookModel(): Promise<ApiCompiledModel> {
      const notebookCells: ApiNotebookCell[] = await Promise.all(
         (this.runnableNotebookCells as RunnableNotebookCell[]).map(
            async (cell) => {
               let queryName: string | undefined = undefined;
               let queryResult: string | undefined = undefined;
               if (cell.runnable) {
                  try {
                     const rowLimit = cell.runnable
                        ? (await cell.runnable.getPreparedResult())
                           .resultExplore.limit
                        : undefined;
                     const result = await cell.runnable.run({ rowLimit });
                     const query = (await cell.runnable.getPreparedQuery())
                        ._query;
                     queryName = (query as NamedQuery).as || query.name;
                     queryResult =
                        result?._queryResult &&
                        JSON.stringify(result?._queryResult);
                  } catch (error) {
                     if (
                        !(error as Error).message.includes(
                           "Model has no queries",
                        )
                     ) {
                        // When the notebook only executed the source it will have no
                        // queries. That's fine, but throw other types of errors.
                        throw error;
                     }
                  }
               }
               return {
                  type: cell.type,
                  text: cell.text,
                  queryName: queryName,
                  queryResult: queryResult,
               } as ApiNotebookCell;
            },
         ),
      );

      return {
         type: "notebook",
         packageName: this.packageName,
         modelPath: this.modelPath,
         malloyVersion: MALLOY_VERSION,
         dataStyles: JSON.stringify(this.dataStyles),
         modelDef: JSON.stringify(this.modelDef),
         sources: this.modelDef && this.sources,
         queries: this.modelDef && this.queries,
         notebookCells,
      } as ApiCompiledModel;
   }

   static async getModelRuntime(
      packageName: string,
      modelPath: string,
      connections: FixedConnectionMap,
   ): Promise<{
      runtime: Runtime;
      modelURL: URL;
      importBaseURL: URL;
      dataStyles: DataStyles;
      modelType: ModelType;
   }> {
      const workingDirectory = getWorkingDirectory();
      const packagePath = path.join(workingDirectory, packageName);
      const fullModelPath = path.join(packagePath, modelPath);
      try {
         if (!(await fs.stat(fullModelPath)).isFile()) {
            throw new ModelNotFoundError(`${modelPath} is not a file.`);
         }
      } catch (error) {
         throw new ModelNotFoundError(`${modelPath} does not exist.`);
      }

      let modelType: ModelType;
      if (modelPath.endsWith(MODEL_FILE_SUFFIX)) {
         modelType = "model";
      } else if (modelPath.endsWith(NOTEBOOK_FILE_SUFFIX)) {
         modelType = "notebook";
      } else {
         throw new ModelNotFoundError(
            `${modelPath} is not a valid model name.  Model files must end in .malloy or .malloynb.`,
         );
      }

      const importBaseURL = new URL(
         "file://" + path.dirname(fullModelPath) + "/",
      );
      const modelURL = new URL("file://" + fullModelPath);
      const urlReader = new HackyDataStylesAccumulator(URL_READER);

      const runtime = new Runtime({ urlReader, connections });
      const dataStyles = urlReader.getHackyAccumulatedDataStyles();
      return { runtime, modelURL, importBaseURL, dataStyles, modelType };
   }

   private static getQueries(
      modelPath: string,
      modelDef: ModelDef,
   ): ApiQuery[] {
      const isNamedQuery = (object: NamedModelObject): object is NamedQuery =>
         object.type === "query";
      return Object.values(modelDef.contents)
         .filter(isNamedQuery)
         .map((queryObj: NamedQuery) => ({
            name: queryObj.as || queryObj.name,
            annotations: queryObj?.annotation?.blockNotes
               ?.filter((note) => note.at.url.includes(modelPath))
               .map((note) => note.text),
         }));
   }

   private static getSources(
      modelPath: string,
      modelDef: ModelDef,
   ): ApiSource[] {
      return Object.values(modelDef.contents)
         .filter((obj) => isSourceDef(obj))
         .map(
            (sourceObj) =>
               ({
                  name: sourceObj.as || sourceObj.name,
                  annotations: (sourceObj as StructDef).annotation?.blockNotes
                     ?.filter((note) => note.at.url.includes(modelPath))
                     .map((note) => note.text),
                  views: (sourceObj as StructDef).fields
                     .filter((turtleObj) => turtleObj.type === "turtle")
                     .filter((turtleObj) =>
                        // TODO(kjnesbit): Fix non-reduce views. Filter out
                        // non-reduce views, i.e., indexes. Need to discuss with Will.
                        (turtleObj as TurtleDef).pipeline
                           .map((stage) => stage.type)
                           .every((type) => type == "reduce"),
                     )
                     .map(
                        (turtleObj) =>
                           ({
                              name: turtleObj.as || turtleObj.name,
                              annotations: turtleObj?.annotation?.blockNotes
                                 ?.filter((note) =>
                                    note.at.url.includes(modelPath),
                                 )
                                 .map((note) => note.text),
                           }) as ApiView,
                     ),
               }) as ApiSource,
         );
   }

   static async getModelMaterializer(
      runtime: Runtime,
      importBaseURL: URL,
      modelURL: URL,
      modelPath: string,
   ): Promise<{
      modelMaterializer: ModelMaterializer | undefined;
      runnableNotebookCells: RunnableNotebookCell[] | undefined;
   }> {
      if (modelPath.endsWith(MODEL_FILE_SUFFIX)) {
         const modelMaterializer = await Model.getStandardModelMaterializer(
            runtime,
            importBaseURL,
            modelURL,
            modelPath,
         );
         return {
            modelMaterializer,
            runnableNotebookCells: undefined,
         };
      } else if (modelPath.endsWith(NOTEBOOK_FILE_SUFFIX)) {
         const { modelMaterializer: mm, runnableNotebookCells: rnc } =
            await Model.getNotebookModelMaterializer(
               runtime,
               importBaseURL,
               modelURL,
               modelPath,
            );
         return {
            modelMaterializer: mm,
            runnableNotebookCells: rnc,
         };
      } else {
         throw new Error(
            `${modelPath} is not a valid model name.  Model files must end in .malloy or .malloynb.`,
         );
      }
   }

   private static async getStandardModelMaterializer(
      runtime: Runtime,
      importBaseURL: URL,
      modelURL: URL,
      modelPath: string,
   ): Promise<ModelMaterializer> {
      const mm = runtime.loadModel(modelURL, { importBaseURL });
      if (!mm) {
         throw new Error(`Invalid model ${modelPath}.`);
      }
      return mm;
   }

   private static async getNotebookModelMaterializer(
      runtime: Runtime,
      importBaseURL: URL,
      modelURL: URL,
      modelPath: string,
   ): Promise<{
      modelMaterializer: ModelMaterializer | undefined;
      runnableNotebookCells: RunnableNotebookCell[];
   }> {
      let fileContents = undefined;
      let parse = undefined;

      try {
         fileContents = await fs.readFile(modelURL, "utf8");
      } catch {
         throw new ModelNotFoundError("Model not found: " + modelPath);
      }

      try {
         parse = MalloySQLParser.parse(fileContents, modelPath);
      } catch {
         throw new Error("Could not parse model: " + modelPath);
      }

      const runnableNotebookCells: RunnableNotebookCell[] = [];
      let mm: ModelMaterializer | undefined = undefined;
      parse.statements.forEach((stmt) => {
         if (stmt.type === MalloySQLStatementType.MALLOY) {
            if (!mm) {
               mm = runtime.loadModel(stmt.text, { importBaseURL });
            } else {
               mm = mm.extendModel(stmt.text, { importBaseURL });
            }
            const runnable = mm.loadFinalQuery();
            runnableNotebookCells.push({
               type: "code",
               text: stmt.text,
               runnable: runnable,
            });
         } else if (stmt.type === MalloySQLStatementType.MARKDOWN) {
            runnableNotebookCells.push({ type: "markdown", text: stmt.text });
         }
      });

      return {
         modelMaterializer: mm,
         runnableNotebookCells: runnableNotebookCells,
      };
   }
}
