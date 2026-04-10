import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   Annotation,
   API,
   Connection,
   FixedConnectionMap,
   isSourceDef,
   MalloyError,
   ModelDef,
   modelDefToModelInfo,
   ModelMaterializer,
   NamedModelObject,
   NamedQueryDef,
   QueryData,
   QueryMaterializer,
   Runtime,
   StructDef,
   TurtleDef,
} from "@malloydata/malloy";
import * as Malloy from "@malloydata/malloy-interfaces";
import {
   MalloySQLParser,
   MalloySQLStatementType,
} from "@malloydata/malloy-sql";
import malloyPackage from "@malloydata/malloy/package.json";
import { DataStyles } from "@malloydata/render";
import { metrics } from "@opentelemetry/api";
import * as fs from "fs/promises";
import * as path from "path";
import { fileURLToPath } from "url";
import { components } from "../api";
import {
   MODEL_FILE_SUFFIX,
   NOTEBOOK_FILE_SUFFIX,
   ROW_LIMIT,
} from "../constants";
import { HackyDataStylesAccumulator } from "../data_styles";
import {
   BadRequestError,
   ModelCompilationError,
   ModelNotFoundError,
} from "../errors";
import { logger } from "../logger";
import { URL_READER } from "../utils";
import {
   buildMalloyParamClause,
   getSourceParams,
   validateRequiredParams,
} from "../utils/source_params";

type ApiCompiledModel = components["schemas"]["CompiledModel"];
type ApiNotebookCell = components["schemas"]["NotebookCell"];
type ApiRawNotebook = components["schemas"]["RawNotebook"];
// @ts-expect-error TODO: Fix missing Source type in API
type ApiSource = components["schemas"]["Source"];
type ApiView = components["schemas"]["View"];
type ApiQuery = components["schemas"]["Query"];
export type ApiConnection = components["schemas"]["Connection"];
export type SnowflakeConnection = components["schemas"]["SnowflakeConnection"];
export type PostgresConnection = components["schemas"]["PostgresConnection"];
export type BigqueryConnection = components["schemas"]["BigqueryConnection"];
export type TrinoConnection = components["schemas"]["TrinoConnection"];

const MALLOY_VERSION = malloyPackage.version;

export type ModelType = "model" | "notebook";

interface RunnableNotebookCell {
   type: "code" | "markdown";
   text: string;
   runnable?: QueryMaterializer;
   newSources?: Malloy.SourceInfo[];
   queryInfo?: Malloy.QueryInfo;
   /** MM from *before* this cell — used to recompile with params */
   priorModelMaterializer?: ModelMaterializer;
   cellCompilationError?: Error;
}

export class Model {
   private packageName: string;
   private modelPath: string;
   private dataStyles: DataStyles;
   private modelType: ModelType;
   private modelMaterializer: ModelMaterializer | undefined;
   private modelDef: ModelDef | undefined;
   private modelInfo: Malloy.ModelInfo | undefined;
   private sources: ApiSource[] | undefined;
   private queries: ApiQuery[] | undefined;
   private sourceInfos: Malloy.SourceInfo[] | undefined;
   private runnableNotebookCells: RunnableNotebookCell[] | undefined;
   private compilationError: MalloyError | Error | undefined;
   private runtime: Runtime | undefined;
   private importBaseURL: URL | undefined;
   private meter = metrics.getMeter("publisher");
   private queryExecutionHistogram = this.meter.createHistogram(
      "malloy_model_query_duration",
      {
         description: "How long it takes to execute a Malloy model query",
         unit: "ms",
      },
   );

   constructor(
      packageName: string,
      modelPath: string,
      dataStyles: DataStyles,
      modelType: ModelType,
      modelMaterializer: ModelMaterializer | undefined,
      modelDef: ModelDef | undefined,
      // TODO(jjs) - remove these
      sources: ApiSource[] | undefined,
      queries: ApiQuery[] | undefined,
      sourceInfos: Malloy.SourceInfo[] | undefined,
      runnableNotebookCells: RunnableNotebookCell[] | undefined,
      compilationError: MalloyError | Error | undefined,
      runtime?: Runtime,
      importBaseURL?: URL,
   ) {
      this.packageName = packageName;
      this.modelPath = modelPath;
      this.dataStyles = dataStyles;
      this.modelType = modelType;
      this.modelDef = modelDef;
      this.modelMaterializer = modelMaterializer;
      this.sources = sources;
      this.queries = queries;
      this.sourceInfos = sourceInfos;
      this.runnableNotebookCells = runnableNotebookCells;
      this.compilationError = compilationError;
      this.runtime = runtime;
      this.importBaseURL = importBaseURL;
      this.modelInfo = this.modelDef
         ? modelDefToModelInfo(this.modelDef)
         : undefined;
   }

   public static async create(
      packageName: string,
      packagePath: string,
      modelPath: string,
      connections: Map<string, Connection>,
   ): Promise<Model> {
      // getModelRuntime might throw a ModelNotFoundError. It's the callers responsibility
      // to pass a valid model path or handle the error.
      const { runtime, modelURL, importBaseURL, dataStyles, modelType } =
         await Model.getModelRuntime(packagePath, modelPath, connections);

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
         const sourceInfos: Malloy.SourceInfo[] = [];
         if (modelMaterializer) {
            modelDef = (await modelMaterializer.getModel())._modelDef;
            sources = Model.getSources(modelPath, modelDef);
            queries = Model.getQueries(modelPath, modelDef);

            // Collect sourceInfos from imported models first
            // This follows the same pattern as notebook imports handling
            const imports = modelDef.imports || [];
            const importedSourceNames = new Set<string>();
            for (const importLocation of imports) {
               try {
                  const modelString = await runtime.urlReader.readURL(
                     new URL(importLocation.importURL),
                  );
                  const importedModelDef = (
                     await runtime
                        .loadModel(modelString as string, { importBaseURL })
                        .getModel()
                  )._modelDef;
                  const importedModelInfo =
                     modelDefToModelInfo(importedModelDef);
                  const importedSources = importedModelInfo.entries.filter(
                     (entry) => entry.kind === "source",
                  ) as Malloy.SourceInfo[];
                  for (const source of importedSources) {
                     if (!importedSourceNames.has(source.name)) {
                        sourceInfos.push(source);
                        importedSourceNames.add(source.name);
                     }
                  }
               } catch (importError) {
                  // Log but don't fail if we can't load an import's sourceInfo
                  logger.warn("Failed to load sourceInfo from import", {
                     importURL: importLocation.importURL,
                     error: importError,
                  });
               }
            }

            // Add locally-defined sources (not already added from imports)
            const localModelInfo = modelDefToModelInfo(modelDef);
            const localSources = localModelInfo.entries.filter(
               (entry) => entry.kind === "source",
            ) as Malloy.SourceInfo[];
            for (const source of localSources) {
               if (!importedSourceNames.has(source.name)) {
                  sourceInfos.push(source);
               }
            }
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
            sourceInfos.length > 0 ? sourceInfos : undefined,
            runnableNotebookCells,
            undefined,
            runtime,
            importBaseURL,
         );
      } catch (error) {
         let computedError = error;
         if (error instanceof Error && error.stack) {
            logger.error("Error stack", error.stack);
         }

         if (error instanceof MalloyError) {
            const problems = error.problems;
            for (const problem of problems) {
               logger.error("Problem", problem);
            }
            computedError = new ModelCompilationError(error);
         }
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
            undefined,
            computedError as Error,
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

   public getSourceInfos(): Malloy.SourceInfo[] | undefined {
      return this.sourceInfos;
   }

   public getQueries(): ApiQuery[] | undefined {
      return this.sources;
   }

   public async getModel(): Promise<ApiCompiledModel> {
      if (this.compilationError) {
         throw this.compilationError;
      }

      if (this.modelType === "model") {
         return this.getStandardModel();
      } else {
         throw new ModelNotFoundError(
            `${this.modelPath} is not a valid model name.  Model files must end in .malloy.`,
         );
      }
   }

   public getNotebookError(): MalloyError | Error | undefined {
      return this.compilationError;
   }

   public async getNotebook(): Promise<ApiRawNotebook> {
      if (this.compilationError) {
         throw this.compilationError;
      }
      if (this.modelType === "notebook") {
         return this.getNotebookModel();
      } else {
         throw new ModelNotFoundError(
            `${this.modelPath} is not a valid notebook name.  Notebook files must end in .malloynb.`,
         );
      }
   }

   public async getQueryResults(
      sourceName?: string,
      queryName?: string,
      query?: string,
      sourceParameters?: Record<string, unknown>,
   ): Promise<{
      result: Malloy.Result;
      compactResult: QueryData;
      modelInfo: Malloy.ModelInfo;
      dataStyles: DataStyles;
   }> {
      const startTime = performance.now();
      if (this.compilationError) {
         // Re-throw MalloyError and ModelCompilationError as-is (they map to 400/424)
         if (
            this.compilationError instanceof MalloyError ||
            this.compilationError instanceof ModelCompilationError
         ) {
            throw this.compilationError;
         }
         // For other compilation errors, wrap as BadRequestError (400)
         throw new BadRequestError(
            `Model compilation failed: ${this.compilationError.message}`,
         );
      }
      let runnable: QueryMaterializer;
      if (!this.modelMaterializer || !this.modelDef || !this.modelInfo)
         throw new BadRequestError("Model has no queryable entities.");

      // Validate and build source parameter clause if params are provided
      const params = sourceParameters ?? {};
      const declaredParams = getSourceParams(this.sourceInfos, sourceName);
      if (declaredParams.length > 0 || Object.keys(params).length > 0) {
         validateRequiredParams(declaredParams, params);
      }
      const paramClause = buildMalloyParamClause(params, declaredParams);

      // Wrap loadQuery calls in try-catch to handle query parsing errors
      try {
         if (!sourceName && !queryName && query) {
            runnable = this.modelMaterializer.loadQuery("\n" + query);
         } else if (queryName && !query) {
            runnable = this.modelMaterializer.loadQuery(
               `\nrun: ${sourceName ? sourceName + paramClause + "->" : ""}${queryName}`,
            );
         } else {
            const endTime = performance.now();
            const executionTime = endTime - startTime;
            this.queryExecutionHistogram.record(executionTime, {
               "malloy.model.path": this.modelPath,
               "malloy.model.query.name": queryName,
               "malloy.model.query.source": sourceName,
               "malloy.model.query.query": query,
               "malloy.model.query.status": "error",
            });
            throw new BadRequestError(
               "Invalid query request. (Query AND !sourceName) OR (queryName AND sourceName) must be defined.",
            );
         }
      } catch (error) {
         // Re-throw BadRequestError as-is
         if (error instanceof BadRequestError) {
            throw error;
         }
         // Re-throw MalloyError as-is (maps to 400)
         if (error instanceof MalloyError) {
            throw error;
         }
         // For other query parsing errors, wrap as BadRequestError
         const errorMessage =
            error instanceof Error ? error.message : String(error);
         logger.error("Query parsing error", {
            error,
            errorMessage,
            projectName: this.packageName,
            modelPath: this.modelPath,
            query,
            queryName,
            sourceName,
         });
         throw new BadRequestError(`Invalid query: ${errorMessage}`);
      }

      const rowLimit =
         (await runnable.getPreparedResult()).resultExplore.limit || ROW_LIMIT;
      const endTime = performance.now();
      const executionTime = endTime - startTime;

      let queryResults;
      try {
         queryResults = await runnable.run({ rowLimit });
      } catch (error) {
         // Record error metrics
         const errorEndTime = performance.now();
         const errorExecutionTime = errorEndTime - startTime;
         this.queryExecutionHistogram.record(errorExecutionTime, {
            "malloy.model.path": this.modelPath,
            "malloy.model.query.name": queryName,
            "malloy.model.query.source": sourceName,
            "malloy.model.query.query": query,
            "malloy.model.query.status": "error",
         });

         // Re-throw Malloy errors as-is (they will be handled by error handler)
         if (error instanceof MalloyError) {
            throw error;
         }

         // For other runtime errors (like divide by zero), throw as BadRequestError
         const errorMessage =
            error instanceof Error ? error.message : String(error);
         logger.error("Query execution error", {
            error,
            errorMessage,
            projectName: this.packageName,
            modelPath: this.modelPath,
            query,
            queryName,
            sourceName,
         });
         throw new BadRequestError(`Query execution failed: ${errorMessage}`);
      }

      this.queryExecutionHistogram.record(executionTime, {
         "malloy.model.path": this.modelPath,
         "malloy.model.query.name": queryName,
         "malloy.model.query.source": sourceName,
         "malloy.model.query.query": query,
         "malloy.model.query.rows_limit": rowLimit,
         "malloy.model.query.rows_total": queryResults.totalRows,
         "malloy.model.query.connection": queryResults.connectionName,
         "malloy.model.query.status": "success",
      });
      return {
         result: API.util.wrapResult(queryResults),
         compactResult: queryResults.data.value,
         modelInfo: this.modelInfo,
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
         modelInfo: JSON.stringify(
            this.modelDef ? modelDefToModelInfo(this.modelDef) : {},
         ),
         sourceInfos: this.getSourceInfos()?.map((sourceInfo) =>
            JSON.stringify(sourceInfo),
         ),
         sources: this.sources,
         queries: this.queries,
      } as ApiCompiledModel;
   }

   private async getNotebookModel(): Promise<ApiRawNotebook> {
      // Return raw cell contents without executing them
      const notebookCells: ApiNotebookCell[] = (
         this.runnableNotebookCells as RunnableNotebookCell[]
      ).map((cell) => {
         logger.debug("cell.queryInfo", cell.queryInfo);
         return {
            type: cell.type,
            text: cell.text,
            newSources: cell.newSources?.map((source) =>
               JSON.stringify(source),
            ),
            queryInfo: cell.queryInfo
               ? JSON.stringify(cell.queryInfo)
               : undefined,
         } as ApiNotebookCell;
      });

      // Collect all annotations from the inherits chain
      const allAnnotations: string[] = [];
      if (this.modelDef) {
         // Traverse the inherits chain to collect all annotations
         // Type as Annotation to handle the inherits chain properly
         let currentAnnotation: Annotation | undefined =
            this.modelDef.annotation;

         while (currentAnnotation) {
            if (currentAnnotation.notes) {
               allAnnotations.push(
                  ...currentAnnotation.notes.map((note) => note.text),
               );
            }
            // Navigate to the inherited annotation if it exists
            currentAnnotation = currentAnnotation.inherits;
         }
      }

      return {
         type: "notebook",
         packageName: this.packageName,
         modelPath: this.modelPath,
         malloyVersion: MALLOY_VERSION,
         modelInfo: JSON.stringify(
            this.modelDef ? modelDefToModelInfo(this.modelDef) : {},
         ),
         sources: this.modelDef && this.sources,
         queries: this.modelDef && this.queries,
         annotations: allAnnotations,
         notebookCells,
      } as ApiRawNotebook;
   }

   public async executeNotebookCell(
      cellIndex: number,
      sourceParameters?: Record<string, unknown>,
   ): Promise<{
      type: "code" | "markdown";
      text: string;
      queryName?: string;
      result?: string;
      newSources?: string[];
   }> {
      if (this.compilationError) {
         throw this.compilationError;
      }

      if (!this.runnableNotebookCells) {
         throw new BadRequestError("No notebook cells available");
      }

      if (cellIndex < 0 || cellIndex >= this.runnableNotebookCells.length) {
         throw new BadRequestError(
            `Cell index ${cellIndex} out of range (0-${this.runnableNotebookCells.length - 1})`,
         );
      }

      const cell = this.runnableNotebookCells[cellIndex];

      if (cell.type === "markdown") {
         return {
            type: cell.type,
            text: cell.text,
         };
      }

      // If the cell failed initial compilation (e.g. missing required source
      // params) and sourceParameters were provided, try to recompile with
      // the params injected into the cell text.
      if (cell.cellCompilationError) {
         if (sourceParameters && Object.keys(sourceParameters).length > 0) {
            return this.executeNotebookCellWithParams(cell, sourceParameters);
         }
         throw new BadRequestError(
            `Cell ${cellIndex} failed to compile: ${cell.cellCompilationError.message}. ` +
               `If this cell uses a parameterized source, provide sourceParameters.`,
         );
      }

      // For code cells, execute the runnable if available
      let queryName: string | undefined = undefined;
      let queryResult: string | undefined = undefined;

      if (cell.runnable) {
         try {
            const rowLimit =
               (await cell.runnable.getPreparedResult()).resultExplore.limit ||
               ROW_LIMIT;
            const result = await cell.runnable.run({ rowLimit });
            const query = (await cell.runnable.getPreparedQuery())._query;
            queryName = (query as NamedQueryDef).as || query.name;
            queryResult =
               result?._queryResult &&
               this.modelInfo &&
               JSON.stringify(API.util.wrapResult(result));
         } catch (error) {
            // Re-throw execution errors so the client knows about them
            if (error instanceof MalloyError) {
               throw error;
            }
            const errorMessage =
               error instanceof Error ? error.message : String(error);
            if (errorMessage.trim() === "Model has no queries.") {
               return {
                  type: "code",
                  text: cell.text,
               };
            } else {
               logger.error("Error message: ", errorMessage);
            }
            logger.debug("Cell content: ", cellIndex, cell.type, cell.text);
            throw new BadRequestError(`Cell execution failed: ${errorMessage}`);
         }
      }

      return {
         type: cell.type,
         text: cell.text,
         queryName: queryName,
         result: queryResult,
         newSources: cell.newSources?.map((source) => JSON.stringify(source)),
      };
   }

   /**
    * Recompile and execute a notebook cell that failed initial compilation,
    * injecting source parameter values into source invocations in the cell
    * text.  Uses the stored ModelMaterializer to rebuild the query in context.
    */
   /**
    * Recompile and execute a notebook cell that failed initial compilation,
    * injecting source parameter values into source invocations in the cell
    * text.  Uses the stored prior ModelMaterializer to rebuild the query
    * from the correct model context (before this cell was added).
    */
   private async executeNotebookCellWithParams(
      cell: RunnableNotebookCell,
      sourceParameters: Record<string, unknown>,
   ): Promise<{
      type: "code" | "markdown";
      text: string;
      queryName?: string;
      result?: string;
      newSources?: string[];
   }> {
      if (!this.runtime || !this.importBaseURL) {
         throw new BadRequestError(
            "Cannot recompile notebook cell: missing runtime context",
         );
      }

      // Inject parameter values into source invocations in the cell text.
      // For each known parameterized source, replace bare source references
      // in `run:` statements with parameterized invocations.
      const allSourceInfos = this.sourceInfos ?? [];
      const sourcesByName = new Map(allSourceInfos.map((s) => [s.name, s]));

      let modifiedText = cell.text;
      for (const [sourceName, sourceInfo] of sourcesByName) {
         if (!sourceInfo.parameters || sourceInfo.parameters.length === 0)
            continue;

         const paramClause = buildMalloyParamClause(
            sourceParameters,
            sourceInfo.parameters,
         );
         if (!paramClause) continue;

         // Match `run: sourceName ->` or `run: sourceName` at end of line,
         // and inject the param clause after the source name.
         const pattern = new RegExp(`(run:\\s+${sourceName})\\s*(->|$)`, "gm");
         modifiedText = modifiedText.replace(pattern, `$1${paramClause} $2`);
      }

      // Rebuild: extend the prior model (before this cell) with modified text
      let recompiledMM: ModelMaterializer;
      if (cell.priorModelMaterializer) {
         recompiledMM = cell.priorModelMaterializer.extendModel(modifiedText, {
            importBaseURL: this.importBaseURL,
         });
      } else {
         recompiledMM = this.runtime.loadModel(modifiedText, {
            importBaseURL: this.importBaseURL,
         });
      }

      const runnable = recompiledMM.loadFinalQuery();

      try {
         const rowLimit =
            (await runnable.getPreparedResult()).resultExplore.limit ||
            ROW_LIMIT;
         const result = await runnable.run({ rowLimit });
         const query = (await runnable.getPreparedQuery())._query;
         const queryName = (query as NamedQueryDef).as || query.name;
         const queryResult =
            result?._queryResult &&
            this.modelInfo &&
            JSON.stringify(API.util.wrapResult(result));

         return {
            type: cell.type,
            text: cell.text,
            queryName,
            result: queryResult || undefined,
            newSources: cell.newSources?.map((source) =>
               JSON.stringify(source),
            ),
         };
      } catch (error) {
         if (error instanceof MalloyError) throw error;
         const errorMessage =
            error instanceof Error ? error.message : String(error);
         throw new BadRequestError(
            `Cell execution with parameters failed: ${errorMessage}`,
         );
      }
   }

   static async getModelRuntime(
      packagePath: string,
      modelPath: string,
      connections: Map<string, Connection>,
   ): Promise<{
      runtime: Runtime;
      modelURL: URL;
      importBaseURL: URL;
      dataStyles: DataStyles;
      modelType: ModelType;
   }> {
      const fullModelPath = path.join(packagePath, modelPath);
      try {
         if (!(await fs.stat(fullModelPath)).isFile()) {
            throw new ModelNotFoundError(`${modelPath} is not a file.`);
         }
      } catch {
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

      const modelURL = new URL(`file://${fullModelPath}`);
      const baseUrl = new URL(".", modelURL);
      const fileUrl = new URL(baseUrl.pathname, "file:");
      const workingDirectory = fileURLToPath(fileUrl);
      const importBaseURL = new URL(baseUrl.pathname + "/", "file:");
      const urlReader = new HackyDataStylesAccumulator(URL_READER);

      const duckdbConnection = connections.get("duckdb") as DuckDBConnection;
      await duckdbConnection.runSQL(
         `SET FILE_SEARCH_PATH='${workingDirectory}';`,
      );

      const runtime = new Runtime({
         urlReader,
         connections: new FixedConnectionMap(connections, "duckdb"),
      });
      const dataStyles = urlReader.getHackyAccumulatedDataStyles();
      return { runtime, modelURL, importBaseURL, dataStyles, modelType };
   }

   private static getQueries(
      modelPath: string,
      modelDef: ModelDef,
   ): ApiQuery[] {
      const isNamedQuery = (
         object: NamedModelObject,
      ): object is NamedQueryDef => object.type === "query";
      return Object.values(modelDef.contents)
         .filter(isNamedQuery)
         .map((queryObj: NamedQueryDef) => ({
            name: queryObj.as || queryObj.name,
            // What to do when the source is not a string?
            sourceName:
               typeof queryObj.structRef === "string"
                  ? queryObj.structRef
                  : undefined,
            annotations: queryObj?.annotation?.blockNotes
               ?.filter((note: { at: { url: string } }) =>
                  note.at.url.includes(modelPath),
               )
               .map((note: { text: string }) => note.text),
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

      let mm: ModelMaterializer | undefined = undefined;
      const oldImports: string[] = [];
      const oldSources: Record<string, Malloy.SourceInfo> = {};
      // First generate the sequence of ModelMaterializers (lazy — no
      // compilation happens here).  Track the MM *before* each statement
      // so failed cells can be recompiled with different text later.
      const priorMMs: (ModelMaterializer | undefined)[] = [];
      const mms = parse.statements.map((stmt) => {
         priorMMs.push(mm);
         if (stmt.type === MalloySQLStatementType.MALLOY) {
            if (!mm) {
               mm = runtime.loadModel(stmt.text, { importBaseURL });
            } else {
               mm = mm.extendModel(stmt.text, { importBaseURL });
            }
         }
         return mm;
      });
      const runnableNotebookCells: RunnableNotebookCell[] = (
         await Promise.all(
            parse.statements.map(async (stmt, index) => {
               if (stmt.type === MalloySQLStatementType.MALLOY) {
                  // Get the Materializer for the current cell/statement.
                  const localMM = mms[index];
                  if (!localMM) {
                     throw new Error("Model materializer is undefined");
                  }

                  // Compilation is wrapped per-cell so that a single cell
                  // with missing source parameters (or other errors) does not
                  // prevent the rest of the notebook from loading.
                  try {
                     return await Model.compileNotebookCell(
                        stmt.text,
                        localMM,
                        runtime,
                        importBaseURL,
                        oldImports,
                        oldSources,
                     );
                  } catch (cellError) {
                     logger.warn(
                        `Notebook cell ${index} compilation failed, deferring to execution time`,
                        { error: cellError },
                     );
                     return {
                        type: "code",
                        text: stmt.text,
                        priorModelMaterializer: priorMMs[index],
                        cellCompilationError:
                           cellError instanceof Error
                              ? cellError
                              : new Error(String(cellError)),
                     } as RunnableNotebookCell;
                  }
               } else if (stmt.type === MalloySQLStatementType.MARKDOWN) {
                  return {
                     type: "markdown",
                     text: stmt.text,
                  } as RunnableNotebookCell;
               } else {
                  return undefined;
               }
            }),
         )
      ).filter((cell) => cell !== undefined);

      return {
         modelMaterializer: mm,
         runnableNotebookCells: runnableNotebookCells,
      };
   }

   /**
    * Compiles a single notebook cell: resolves imports, extracts new sources,
    * builds the runnable, and extracts query info.  Throws on compilation
    * failure (caller decides how to handle).
    */
   private static async compileNotebookCell(
      cellText: string,
      localMM: ModelMaterializer,
      runtime: Runtime,
      importBaseURL: URL,
      oldImports: string[],
      oldSources: Record<string, Malloy.SourceInfo>,
   ): Promise<RunnableNotebookCell> {
      const currentModelDef = (await localMM.getModel())._modelDef;
      let newSources: Malloy.SourceInfo[] = [];
      const newImports = currentModelDef.imports?.slice(oldImports.length);
      if (newImports) {
         await Promise.all(
            newImports.map(async (importLocation) => {
               const modelString = await runtime.urlReader.readURL(
                  new URL(importLocation.importURL),
               );
               const importModel = (
                  await runtime
                     .loadModel(modelString as string, { importBaseURL })
                     .getModel()
               )._modelDef;
               const importModelInfo = modelDefToModelInfo(importModel);
               newSources = importModelInfo.entries
                  .filter((entry) => entry.kind === "source")
                  .filter(
                     (source) => !(source.name in oldSources),
                  ) as Malloy.SourceInfo[];
               oldImports.push(importLocation.importURL.toString());
            }),
         );
      }
      const currentModelInfo = modelDefToModelInfo(currentModelDef);
      newSources = newSources.concat(
         currentModelInfo.entries
            .filter((entry) => entry.kind === "source")
            .filter(
               (source) => !(source.name in oldSources),
            ) as Malloy.SourceInfo[],
      );

      for (const source of newSources) {
         oldSources[source.name] = source;
      }

      const runnable = localMM.loadFinalQuery();

      let queryInfo: Malloy.QueryInfo | undefined = undefined;
      try {
         const preparedQuery = await runnable.getPreparedQuery();
         const query = preparedQuery._query as NamedQueryDef;
         const queryName = query.as || query.name;
         const anonymousQuery =
            currentModelInfo.anonymous_queries[
               currentModelInfo.anonymous_queries.length - 1
            ];

         if (anonymousQuery) {
            queryInfo = {
               name: queryName,
               schema: anonymousQuery.schema,
               annotations: anonymousQuery.annotations,
               definition: anonymousQuery.definition,
               code: anonymousQuery.code,
               location: anonymousQuery.location,
            } as Malloy.QueryInfo;
         }
      } catch (_error) {
         // No query in this cell — that's fine for source-definition-only cells
      }

      return {
         type: "code",
         text: cellText,
         runnable,
         modelMaterializer: localMM,
         newSources,
         queryInfo,
      } as RunnableNotebookCell;
   }

   public getModelType(): ModelType {
      return this.modelType;
   }

   public async getFileText(packagePath: string): Promise<string> {
      const fullPath = path.join(packagePath, this.modelPath);
      try {
         return await fs.readFile(fullPath, "utf8");
      } catch {
         throw new ModelNotFoundError(
            `Model file not found: ${this.modelPath}`,
         );
      }
   }
}
