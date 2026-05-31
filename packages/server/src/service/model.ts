import {
   Annotation,
   API,
   Connection,
   FixedConnectionMap,
   GivenValue,
   isSourceDef,
   MalloyConfig,
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
import { DataStyles } from "@malloydata/render";
import { metrics } from "@opentelemetry/api";
import * as fs from "fs/promises";
import { createRequire } from "module";
import * as path from "path";
import { components } from "../api";
import { deserializeError } from "../package_load/package_load_pool";
import type {
   SerializedModel,
   SerializedNotebookCell,
} from "../package_load/protocol";
import {
   getDefaultQueryRowLimit,
   getMaxQueryRows,
   getMaxResponseBytes,
} from "../config";
import { MODEL_FILE_SUFFIX, NOTEBOOK_FILE_SUFFIX } from "../constants";
import { HackyDataStylesAccumulator } from "../data_styles";
import {
   BadRequestError,
   ModelCompilationError,
   ModelNotFoundError,
   PayloadTooLargeError,
} from "../errors";
import { logger } from "../logger";
import { BuildManifest } from "../storage/DatabaseInterface";
import { URL_READER } from "../utils";
import {
   buildFilterClause,
   FilterValidationError,
   injectFilterRefinement,
   parseFilters,
   type FilterDefinition,
   type FilterParams,
} from "./filter";
import { malloyGivenToApi, type MalloyGiven } from "./given";
import {
   assertWithinModelResponseLimits,
   resolveModelQueryRowLimit,
} from "./model_limits";

type ApiCompiledModel = components["schemas"]["CompiledModel"];
type ApiNotebookCell = components["schemas"]["NotebookCell"];
type ApiRawNotebook = components["schemas"]["RawNotebook"];
type ApiSource = components["schemas"]["Source"];
type ApiFilter = components["schemas"]["Filter"];
type ApiGiven = components["schemas"]["Given"];
type ApiView = components["schemas"]["View"];
type ApiQuery = components["schemas"]["Query"];
export type ApiConnection = components["schemas"]["Connection"];
export type SnowflakeConnection = components["schemas"]["SnowflakeConnection"];
export type PostgresConnection = components["schemas"]["PostgresConnection"];
export type BigqueryConnection = components["schemas"]["BigqueryConnection"];
export type TrinoConnection = components["schemas"]["TrinoConnection"];

const MALLOY_VERSION = (
   createRequire(import.meta.url)("@malloydata/malloy/package.json") as {
      version: string;
   }
).version;

export type ModelType = "model" | "notebook";
type ModelConnectionInput = MalloyConfig | Map<string, Connection>;

interface RunnableNotebookCell {
   type: "code" | "markdown";
   text: string;
   runnable?: QueryMaterializer;
   /** Retained so we can rebuild the query with filter refinements at execution time. */
   modelMaterializer?: ModelMaterializer;
   newSources?: Malloy.SourceInfo[];
   queryInfo?: Malloy.QueryInfo;
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
   /** Parsed #(filter) definitions keyed by source name. */
   private filterMap: Map<string, FilterDefinition[]>;
   /** Givens declared on the model, in declaration order. Malloy's
    *  `Model.givens` already collapses inheritance; we just stash the list
    *  for surfacing on the compiled-model response. */
   private givens: ApiGiven[] | undefined;
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
      filterMap?: Map<string, FilterDefinition[]>,
      givens?: ApiGiven[],
      /**
       * Precomputed `modelDefToModelInfo(modelDef)`. The package-load
       * worker emits it as part of `SerializedModel` so we don't
       * re-derive it on every package load. Callers that build a
       * `Model` from a raw `modelDef` (e.g. test fixtures via
       * `Model.create`) can omit this and let the constructor
       * derive it lazily.
       */
      modelInfo?: Malloy.ModelInfo,
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
      this.filterMap = filterMap ?? new Map();
      this.givens = givens;
      this.modelInfo =
         modelInfo ??
         (this.modelDef ? modelDefToModelInfo(this.modelDef) : undefined);

      // One-time deprecation notice per Model instance. Surfaces only when
      // the model declares `#(filter)` annotations so operators migrating
      // toward `given:` see a clear pointer in the server log without
      // spamming for models that have already moved over.
      if (this.filterMap.size > 0) {
         logger.warn(
            `Model "${packageName}/${modelPath}" uses deprecated #(filter) annotations. Migrate to given: — see https://github.com/malloydata/publisher/blob/main/docs/givens.md`,
            {
               packageName,
               modelPath,
               filterSourceCount: this.filterMap.size,
            },
         );
      }
   }

   /**
    * Get the parsed filter definitions for a given source name.
    * Returns an empty array if no filters are declared.
    */
   public getFilters(sourceName: string): FilterDefinition[] {
      return this.filterMap.get(sourceName) ?? [];
   }

   /**
    * Best-effort extraction of a source name from an ad-hoc Malloy query string.
    * Matches patterns like `run: source_name -> ...` or `source_name -> ...`.
    */
   private extractSourceName(query?: string): string | undefined {
      if (!query) return undefined;
      const runMatch = query.match(/run\s*:\s*(\w+)\s*->/);
      const arrowMatch = query.match(/^\s*(\w+)\s*->/m);
      return runMatch?.[1] ?? arrowMatch?.[1];
   }

   /**
    * Compile a single model in-process. Kept as a library entry point
    * for test fixtures and any future caller that needs an ad-hoc
    * `Model` from a `.malloy` / `.malloynb` file. Production package
    * loads (`Package.create`) and reloads (`Package.reloadAllModels`)
    * route through the package-load worker pool and dispatch through
    * {@link Model.fromSerialized} instead — neither calls this on the
    * main thread.
    */
   public static async create(
      packageName: string,
      packagePath: string,
      modelPath: string,
      malloyConfig: ModelConnectionInput,
      options?: { buildManifest?: BuildManifest["entries"] },
   ): Promise<Model> {
      // getModelRuntime might throw a ModelNotFoundError. It's the callers responsibility
      // to pass a valid model path or handle the error.
      const { runtime, modelURL, importBaseURL, dataStyles, modelType } =
         await Model.getModelRuntime(
            packagePath,
            modelPath,
            malloyConfig,
            options,
         );

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
         let filterMap: Map<string, FilterDefinition[]> | undefined;
         let givens: ApiGiven[] | undefined;
         const sourceInfos: Malloy.SourceInfo[] = [];
         if (modelMaterializer) {
            const compiledModel = await modelMaterializer.getModel();
            modelDef = compiledModel._modelDef;
            // Malloy's `Model.givens` already collapses inheritance from imports
            // and applies any `finalizeGivens` runtime config. Just read it.
            const malloyGivens = Array.from(
               compiledModel.givens.values(),
            ) as MalloyGiven[];
            givens =
               malloyGivens.length > 0
                  ? (malloyGivens.map(malloyGivenToApi) as ApiGiven[])
                  : undefined;
            const sourceResult = Model.getSources(modelPath, modelDef, givens);
            sources = sourceResult.sources;
            filterMap = sourceResult.filterMap;
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
            filterMap,
            givens,
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

   /**
    * Construct a `Model` from a worker-compiled `SerializedModel`. All
    * the heavy compile work (parse, type check, IR build, sourceInfo
    * extraction, per-cell notebook compile) already ran inside a
    * `worker_threads` worker; this factory just rewraps the wire data
    * into a live `Model`.
    *
    * Hydrates the `ModelMaterializer` (and, for notebooks, the
    * per-cell materializers + runnables) **eagerly** via
    * `Runtime._loadModelFromModelDef` /
    * `ModelMaterializer._loadQueryFromQueryDef`. These are constant-time
    * wraps around the worker's pre-compiled `modelDef` / `queryDef` —
    * no parse, no type-check, no schema fetch — so doing it here at
    * package-load time costs microseconds per model and keeps the
    * resulting `Model` interchangeable with one produced by
    * `Model.create` (no lazy-init branches in the hot path).
    */
   public static fromSerialized(
      packageName: string,
      _packagePath: string,
      malloyConfig: ModelConnectionInput,
      data: SerializedModel,
   ): Model {
      const modelDef = data.modelDef as ModelDef | undefined;
      const modelInfo = data.modelInfo as Malloy.ModelInfo | undefined;
      const dataStyles = (data.dataStyles ?? {}) as DataStyles;
      const sources = data.sources as ApiSource[] | undefined;
      const queries = data.queries as ApiQuery[] | undefined;
      const sourceInfos = data.sourceInfos as Malloy.SourceInfo[] | undefined;
      const givens = data.givens as ApiGiven[] | undefined;
      const filterMap = data.filterMap
         ? new Map(data.filterMap as Array<[string, FilterDefinition[]]>)
         : undefined;

      // No modelDef → either an empty notebook (no MALLOY statements)
      // or a corrupt worker payload. Build a Model with no materializer;
      // downstream getQueryResults / executeNotebookCell will throw a
      // clean BadRequestError if a caller tries to run a query. We
      // still preserve markdown cells for an all-markdown notebook so
      // `getNotebook()` can serve raw text.
      if (!modelDef) {
         return new Model(
            packageName,
            data.modelPath,
            dataStyles,
            data.modelType,
            undefined,
            undefined,
            sources,
            queries,
            sourceInfos,
            data.modelType === "notebook"
               ? hydrateMarkdownOnlyCells(data.notebookCells)
               : undefined,
            undefined,
            filterMap,
            givens,
            modelInfo,
         );
      }

      const runtime = makeHydrationRuntime(malloyConfig);
      const modelMaterializer = runtime._loadModelFromModelDef(modelDef);
      const runnableNotebookCells =
         data.modelType === "notebook"
            ? hydrateNotebookCells(runtime, data.notebookCells)
            : undefined;

      return new Model(
         packageName,
         data.modelPath,
         dataStyles,
         data.modelType,
         modelMaterializer,
         modelDef,
         sources,
         queries,
         sourceInfos,
         runnableNotebookCells,
         undefined, // compilationError
         filterMap,
         givens,
         modelInfo,
      );
   }

   /**
    * Build a Model representing a compilation failure (no modelDef,
    * no materializer). Matches the shape `Model.create` returns when
    * it catches a `MalloyError`, so the rest of the system handles
    * both paths uniformly (the iteration loop in `Package.create`
    * reads `compilationError` via a structural cast).
    */
   public static fromCompilationError(
      packageName: string,
      modelPath: string,
      modelType: ModelType,
      error: Error,
   ): Model {
      return new Model(
         packageName,
         modelPath,
         {} as DataStyles,
         modelType,
         undefined,
         undefined,
         undefined,
         undefined,
         undefined,
         undefined,
         error,
      );
   }

   /** Look up the deserialized error helper for callers (e.g. Package.create). */
   public static deserializeCompilationError = deserializeError;

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
      return this.queries;
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
      filterParams?: FilterParams,
      bypassFilters?: boolean,
      givens?: Record<string, GivenValue>,
      // Optional caller-supplied abort signal. Plumbed straight into
      // `runnable.run` so a publisher-issued query timeout (see
      // `runWithQueryTimeout`) actually cancels the work in flight
      // instead of just unblocking the awaiter. Pass `undefined` to
      // keep the legacy "no timeout" behavior — useful for
      // background callers (materialization, tests) that own their
      // own deadline.
      abortSignal?: AbortSignal,
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

      // Wrap loadQuery calls in try-catch to handle query parsing errors
      try {
         let queryString: string;
         if (!sourceName && !queryName && query) {
            queryString = "\n" + query;
         } else if (queryName && !query) {
            queryString = `\nrun: ${sourceName ? sourceName + "->" : ""}${queryName}`;
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

         // Inject source filter predicates unless bypassed
         if (!bypassFilters) {
            const effectiveSource = sourceName ?? this.extractSourceName(query);
            if (effectiveSource) {
               const filters = this.getFilters(effectiveSource);
               if (filters.length > 0) {
                  const filterClause = buildFilterClause(
                     filters,
                     filterParams ?? {},
                  );
                  queryString = injectFilterRefinement(
                     queryString,
                     filterClause,
                     effectiveSource,
                  );
               }
            }
         }

         runnable = this.modelMaterializer.loadQuery(queryString);
      } catch (error) {
         // Re-throw BadRequestError as-is
         if (error instanceof BadRequestError) {
            throw error;
         }
         // Source filter validation errors are client errors (400)
         if (error instanceof FilterValidationError) {
            throw new BadRequestError(error.message);
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
            environmentName: this.packageName,
            modelPath: this.modelPath,
            query,
            queryName,
            sourceName,
         });
         throw new BadRequestError(`Invalid query: ${errorMessage}`);
      }

      const maxRows = getMaxQueryRows();
      const maxBytes = getMaxResponseBytes();
      const rowLimit = resolveModelQueryRowLimit(
         (await runnable.getPreparedResult({ givens })).resultExplore.limit,
         { defaultLimit: getDefaultQueryRowLimit(), maxRows },
      );
      const endTime = performance.now();
      const executionTime = endTime - startTime;

      let queryResults;
      try {
         queryResults = await runnable.run({ rowLimit, givens, abortSignal });
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
            environmentName: this.packageName,
            modelPath: this.modelPath,
            query,
            queryName,
            sourceName,
         });
         throw new BadRequestError(`Query execution failed: ${errorMessage}`);
      }

      const wrappedResult = API.util.wrapResult(queryResults);
      // Best-effort byte check: we've already buffered `queryResults` and
      // built `wrappedResult` by the time we get here, so this surfaces
      // oversize responses with a clean HTTP 413 instead of letting the
      // controller transmit a half-megabyte payload — it is not OOM
      // prevention. True prevention requires streaming `Result`
      // construction, which is out of scope for this step. The row cap
      // above is the primary OOM defense.
      const serializedBytes =
         maxBytes > 0
            ? Buffer.byteLength(JSON.stringify(wrappedResult), "utf8")
            : 0;
      assertWithinModelResponseLimits(
         queryResults.totalRows,
         serializedBytes,
         { maxRows, maxBytes },
         "model_query",
      );
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
         result: wrappedResult,
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
         // `this.modelInfo` is precomputed once at construction (either
         // by the worker or in the Model.create constructor); don't
         // re-run `modelDefToModelInfo` on every API hit.
         modelInfo: JSON.stringify(this.modelInfo ?? {}),
         sourceInfos: this.getSourceInfos()?.map((sourceInfo) =>
            JSON.stringify(sourceInfo),
         ),
         sources: this.sources,
         queries: this.queries,
         givens: this.givens,
      } as ApiCompiledModel;
   }

   private async getNotebookModel(): Promise<ApiRawNotebook> {
      // Return raw cell contents without executing them
      const notebookCells: ApiNotebookCell[] = (
         this.runnableNotebookCells as RunnableNotebookCell[]
      ).map((cell) => {
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
         modelInfo: JSON.stringify(this.modelInfo ?? {}),
         sources: this.modelDef && this.sources,
         queries: this.modelDef && this.queries,
         annotations: allAnnotations,
         notebookCells,
      } as ApiRawNotebook;
   }

   public async executeNotebookCell(
      cellIndex: number,
      filterParams?: FilterParams,
      bypassFilters?: boolean,
      givens?: Record<string, GivenValue>,
      // See `getQueryResults`: forwarded into `runnable.run` so the
      // publisher's wall-clock timeout actually cancels the query.
      abortSignal?: AbortSignal,
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

      // For code cells, execute the runnable if available
      let queryName: string | undefined = undefined;
      let queryResult: string | undefined = undefined;

      if (cell.runnable) {
         try {
            let runnableToExecute = cell.runnable;

            // If filters need to be applied, rebuild the query with a refinement
            if (!bypassFilters && cell.modelMaterializer) {
               const effectiveSource = this.extractSourceName(cell.text);
               if (effectiveSource) {
                  const filters = this.getFilters(effectiveSource);
                  if (filters.length > 0) {
                     const filterClause = buildFilterClause(
                        filters,
                        filterParams ?? {},
                     );
                     if (filterClause) {
                        const refinedQuery = injectFilterRefinement(
                           cell.text,
                           filterClause,
                           effectiveSource,
                        );
                        runnableToExecute =
                           cell.modelMaterializer.loadQuery(refinedQuery);
                     }
                  }
               }
            }

            const cellMaxRows = getMaxQueryRows();
            const cellMaxBytes = getMaxResponseBytes();
            const rowLimit = resolveModelQueryRowLimit(
               (await runnableToExecute.getPreparedResult({ givens }))
                  .resultExplore.limit,
               {
                  defaultLimit: getDefaultQueryRowLimit(),
                  maxRows: cellMaxRows,
               },
            );
            const result = await runnableToExecute.run({
               rowLimit,
               givens,
               abortSignal,
            });
            const query = (await runnableToExecute.getPreparedQuery())._query;
            queryName = (query as NamedQueryDef).as || query.name;
            queryResult =
               result?._queryResult &&
               this.modelInfo &&
               JSON.stringify(API.util.wrapResult(result));
            // Same caveat as `getQueryResults`: by the time we measure
            // bytes the response has already been buffered and stringified,
            // so this is loud-failure detection (clean 413 instead of
            // partial transmission), not OOM prevention. The row cap above
            // is the primary defense.
            if (result?._queryResult && queryResult) {
               assertWithinModelResponseLimits(
                  result.totalRows,
                  Buffer.byteLength(queryResult, "utf8"),
                  { maxRows: cellMaxRows, maxBytes: cellMaxBytes },
                  "notebook_cell",
               );
            }
         } catch (error) {
            if (error instanceof FilterValidationError) {
               throw new BadRequestError(error.message);
            }
            if (error instanceof MalloyError) {
               throw error;
            }
            // Surface PayloadTooLargeError as-is so the error middleware
            // maps it to HTTP 413; without this it would get swallowed
            // into a generic 400 BadRequestError below.
            if (error instanceof PayloadTooLargeError) {
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

   static async getModelRuntime(
      packagePath: string,
      modelPath: string,
      malloyConfig: ModelConnectionInput,
      options?: { buildManifest?: BuildManifest["entries"] },
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
      const importBaseURL = baseUrl;
      const urlReader = new HackyDataStylesAccumulator(URL_READER);

      // Request runtimes borrow the cached package MalloyConfig. The package
      // owns release; callers must not release this runtime per request.
      const runtime = new Runtime({
         urlReader,
         config: Model.toMalloyConfig(malloyConfig),
         buildManifest: options?.buildManifest
            ? { entries: options.buildManifest, strict: false }
            : undefined,
      });
      const dataStyles = urlReader.getHackyAccumulatedDataStyles();
      return { runtime, modelURL, importBaseURL, dataStyles, modelType };
   }

   private static toMalloyConfig(input: ModelConnectionInput): MalloyConfig {
      if (input instanceof MalloyConfig) {
         return input;
      }

      const malloyConfig = new MalloyConfig({ connections: {} });
      malloyConfig.wrapConnections(
         () => new FixedConnectionMap(input, "duckdb"),
      );
      return malloyConfig;
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
      givens?: ApiGiven[],
   ): {
      sources: ApiSource[];
      filterMap: Map<string, FilterDefinition[]>;
   } {
      const filterMap = new Map<string, FilterDefinition[]>();

      const sources = Object.values(modelDef.contents)
         .filter((obj) => isSourceDef(obj))
         .map((sourceObj) => {
            const sourceName = sourceObj.as || sourceObj.name;
            const annotations = (sourceObj as StructDef).annotation?.blockNotes
               ?.filter((note) => note.at.url.includes(modelPath))
               .map((note) => note.text);

            // Parse #(filter) from ALL annotations, traversing the inherits
            // chain so that filters on a base source (e.g. `recalls`) are
            // picked up by an extending source (`manufacturer_recalls is
            // recalls extend {}`).  The Malloy compiler stores the base
            // source's annotations in `annotation.inherits`.
            //
            // The chain goes child → parent, so we collect child-first.
            // parseFilters uses "last wins" dedup, so we reverse to put
            // parent annotations first and child annotations last (winning).
            const collectedAnnotations: string[][] = [];
            let curAnnotation: Annotation | undefined = (sourceObj as StructDef)
               .annotation;
            while (curAnnotation) {
               if (curAnnotation.blockNotes) {
                  collectedAnnotations.push(
                     curAnnotation.blockNotes.map((note) => note.text),
                  );
               }
               curAnnotation = curAnnotation.inherits;
            }
            const allAnnotations = collectedAnnotations.reverse().flat();
            let filters: ApiFilter[] | undefined;
            if (allAnnotations.length > 0) {
               try {
                  const parsed = parseFilters(allAnnotations);
                  if (parsed.length > 0) {
                     filterMap.set(sourceName, parsed);
                     const structFields = (sourceObj as StructDef).fields;
                     filters = parsed.map((f) => {
                        const field = structFields.find(
                           (fd) => (fd.as || fd.name) === f.dimension,
                        );
                        return {
                           name: f.name,
                           dimension: f.dimension,
                           type: f.type,
                           implicit: f.implicit,
                           required: f.required,
                           dimensionType: field?.type as string | undefined,
                        };
                     });
                  }
               } catch (err) {
                  logger.warn(
                     `Failed to parse filter annotations on source "${sourceName}"`,
                     { error: err },
                  );
               }
            }

            const views = (sourceObj as StructDef).fields
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
                           ?.filter((note) => note.at.url.includes(modelPath))
                           .map((note) => note.text),
                     }) as ApiView,
               );

            return {
               name: sourceName,
               annotations,
               views,
               filters,
               // Malloy exposes givens at the model level, not per-source.
               // First pass: surface the full model-level list on every source
               // — matches how filter introspection already collapses
               // inheritance into the per-source list. Refine to view-scoped
               // filtering if a customer asks.
               givens,
            } as ApiSource;
         });

      return { sources, filterMap };
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
      // First generate the sequence of ModelMaterializers.
      // This has to happen sync, since mm.getModel() is async and
      // may execute out-of-order.
      const mms = parse.statements.map((stmt) => {
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
                     // This can't happen because the to be in this branch there stmt must be
                     // MalloySQLStatementType.MALLOY and we must have a model materializer.
                     throw new Error("Model materializer is undefined");
                  }
                  // Pull available sources from the current model.
                  // Add any of then that are new into newSources and then add them to oldSources.
                  const currentModelDef = (await localMM.getModel())._modelDef;
                  let newSources: Malloy.SourceInfo[] = [];
                  const newImports = currentModelDef.imports?.slice(
                     oldImports.length,
                  );
                  if (newImports) {
                     await Promise.all(
                        newImports.map(async (importLocation) => {
                           const modelString = await runtime.urlReader.readURL(
                              new URL(importLocation.importURL),
                           );
                           const importModel = (
                              await runtime
                                 .loadModel(modelString as string, {
                                    importBaseURL,
                                 })
                                 .getModel()
                           )._modelDef;
                           const importModelInfo =
                              modelDefToModelInfo(importModel);
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

                  // Extract QueryInfo from the runnable
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
                     // If we can't extract query info (e.g., no query in cell), that's okay
                     // This can happen for cells that only define sources
                  }

                  return {
                     type: "code",
                     text: stmt.text,
                     runnable: runnable,
                     modelMaterializer: localMM,
                     newSources,
                     queryInfo,
                  } as RunnableNotebookCell;
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

// ──────────────────────────────────────────────────────────────────────
// Helpers for hydrating worker-compiled models on the main thread
// ──────────────────────────────────────────────────────────────────────

/**
 * Minimal subset of `Runtime` we use here. The `_` methods are
 * marked `@internal` in Malloy but are the only API for constructing
 * a materializer / query materializer from an existing `modelDef` /
 * queryDef — the public `loadModel(url)` path always recompiles.
 */
type HydrationRuntime = Runtime & {
   _loadModelFromModelDef(modelDef: ModelDef): ModelMaterializer;
};
type HydrationMaterializer = ModelMaterializer & {
   _loadQueryFromQueryDef(query: unknown): QueryMaterializer;
};

function makeHydrationRuntime(
   malloyConfig: ModelConnectionInput,
): HydrationRuntime {
   const urlReader = new HackyDataStylesAccumulator(URL_READER);
   const config =
      malloyConfig instanceof MalloyConfig
         ? malloyConfig
         : (() => {
              const c = new MalloyConfig({ connections: {} });
              c.wrapConnections(
                 () => new FixedConnectionMap(malloyConfig, "duckdb"),
              );
              return c;
           })();
   return new Runtime({ urlReader, config }) as HydrationRuntime;
}

/**
 * Build the live `RunnableNotebookCell[]` from worker-emitted
 * per-cell data. Each MALLOY cell is hydrated via
 * `Runtime._loadModelFromModelDef` (for the cell's scope) and
 * `ModelMaterializer._loadQueryFromQueryDef` (for the cell's
 * runnable) — no recompile.
 */
function hydrateNotebookCells(
   runtime: HydrationRuntime,
   notebookCells: SerializedNotebookCell[] | undefined,
): RunnableNotebookCell[] {
   if (!notebookCells) return [];
   return notebookCells.map((sc): RunnableNotebookCell => {
      if (sc.type === "markdown") {
         return { type: "markdown", text: sc.text };
      }
      const cellModelDef = sc.cellModelDef as ModelDef | undefined;
      let modelMaterializer: ModelMaterializer | undefined;
      let runnable: QueryMaterializer | undefined;
      if (cellModelDef) {
         modelMaterializer = runtime._loadModelFromModelDef(cellModelDef);
         if (sc.cellQueryDef !== undefined) {
            try {
               runnable = (
                  modelMaterializer as HydrationMaterializer
               )._loadQueryFromQueryDef(sc.cellQueryDef);
            } catch (error) {
               // Hydration shouldn't fail for a queryDef the worker
               // already prepared, but if Malloy's internal shape
               // drifts we'd rather drop the runnable than crash the
               // whole notebook. The cell remains markdown-runnable.
               logger.warn("Failed to hydrate notebook cell queryDef", {
                  error,
               });
            }
         }
      }
      return {
         type: "code",
         text: sc.text,
         runnable,
         modelMaterializer,
         newSources: sc.newSources as Malloy.SourceInfo[] | undefined,
         queryInfo: sc.queryInfo as Malloy.QueryInfo | undefined,
      };
   });
}

/**
 * For an all-markdown notebook (no MALLOY statements → no
 * `modelDef`), we still want to preserve the cell list so
 * `getNotebook()` can serve raw text. This skips materializer
 * hydration (there's nothing to hydrate) and returns markdown-only
 * cells.
 */
function hydrateMarkdownOnlyCells(
   notebookCells: SerializedNotebookCell[] | undefined,
): RunnableNotebookCell[] | undefined {
   if (!notebookCells) return undefined;
   return notebookCells.map((sc): RunnableNotebookCell => {
      if (sc.type === "markdown") return { type: "markdown", text: sc.text };
      // A code cell without a hydratable scope — surface text only.
      return { type: "code", text: sc.text };
   });
}
