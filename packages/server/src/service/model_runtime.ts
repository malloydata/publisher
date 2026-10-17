// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   FixedConnectionMap,
   MalloyConfig,
   type Connection,
   type ModelDef,
   type ModelMaterializer,
   type NamedQueryDef,
   type QueryMaterializer,
   Runtime,
} from "@malloydata/malloy";
import * as Malloy from "@malloydata/malloy-interfaces";
import { MalloySQLStatementType } from "@malloydata/malloy-sql";
import { DataStyles } from "@malloydata/render";
import * as fs from "fs/promises";
import { MODEL_FILE_SUFFIX, NOTEBOOK_FILE_SUFFIX } from "../constants";
import { HackyDataStylesAccumulator } from "../data_styles";
import { ModelNotFoundError } from "../errors";
import { logger } from "../logger";
import type { SerializedNotebookCell } from "../package_load/protocol";
import { safeJoinUnderRoot } from "../path_safety";
import { BuildManifest } from "../storage/DatabaseInterface";
import { URL_READER } from "../utils";
import { parseMalloynb } from "./deprecated/malloynb";
import { modelInfoOf } from "./model_info";
import { type RunnableNotebookCell } from "./model_notebook_cell";

export type ModelType = "model" | "notebook";

export type ModelConnectionInput = MalloyConfig | Map<string, Connection>;

/**
 * `overlay` maps an absolute `file://` URL to text served INSTEAD of reading
 * that path from disk. It lets pre-aggregation's synthesized (never written)
 * model live at a URL inside the package directory, so its `import` of the
 * author's model resolves. `modelPath` must still exist: it anchors
 * `importBaseURL`.
 */
export async function createModelRuntime(
   packagePath: string,
   modelPath: string,
   malloyConfig: ModelConnectionInput,
   options?: {
      buildManifest?: BuildManifest["entries"];
      overlay?: ReadonlyMap<string, string>;
   },
): Promise<{
   runtime: Runtime;
   modelURL: URL;
   importBaseURL: URL;
   dataStyles: DataStyles;
   modelType: ModelType;
   /** The bytes the compiler read for the model's own `url`, first read
    *  winning; undefined until a compile has read it, and for any other url. */
   compiledTextFor: (url: URL) => string | undefined;
}> {
   // Contain the caller-supplied model path inside the package directory;
   // a path that resolves outside it is reported as a missing model, which
   // is all a caller is entitled to learn.
   let fullModelPath: string;
   try {
      fullModelPath = safeJoinUnderRoot(packagePath, modelPath);
   } catch {
      throw new ModelNotFoundError(`${modelPath} does not exist.`);
   }
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
   const overlay = options?.overlay;
   const inner =
      overlay && overlay.size > 0
         ? {
              readURL: async (url: URL) =>
                 overlay.get(url.href) ?? (await URL_READER.readURL(url)),
           }
         : URL_READER;
   // Only the model's own text: the Runtime lives as long as the Model, and imports' text is never asked for.
   let modelText: string | undefined;
   const urlReader = new HackyDataStylesAccumulator({
      readURL: async (url: URL) => {
         const contents = await inner.readURL(url);
         if (
            modelText === undefined &&
            url.toString() === modelURL.toString()
         ) {
            modelText =
               typeof contents === "string" ? contents : contents.contents;
         }
         return contents;
      },
   });

   // Request runtimes borrow the cached package MalloyConfig. The package
   // owns release; callers must not release this runtime per request.
   const runtime = new Runtime({
      urlReader,
      config: toMalloyConfig(malloyConfig),
      buildManifest: options?.buildManifest
         ? { entries: options.buildManifest, strict: false }
         : undefined,
   });
   const dataStyles = urlReader.getHackyAccumulatedDataStyles();
   return {
      runtime,
      modelURL,
      importBaseURL,
      dataStyles,
      modelType,
      compiledTextFor: (url: URL) =>
         url.toString() === modelURL.toString() ? modelText : undefined,
   };
}

function toMalloyConfig(input: ModelConnectionInput): MalloyConfig {
   if (input instanceof MalloyConfig) {
      return input;
   }

   const malloyConfig = new MalloyConfig({ connections: {} });
   malloyConfig.wrapConnections(() => new FixedConnectionMap(input, "duckdb"));
   return malloyConfig;
}

export async function createModelMaterializer(
   runtime: Runtime,
   importBaseURL: URL,
   modelURL: URL,
   modelPath: string,
): Promise<{
   modelMaterializer: ModelMaterializer | undefined;
   runnableNotebookCells: RunnableNotebookCell[] | undefined;
}> {
   if (modelPath.endsWith(MODEL_FILE_SUFFIX)) {
      const modelMaterializer = await getStandardModelMaterializer(
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
         await getNotebookModelMaterializer(
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

async function getStandardModelMaterializer(
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

async function getNotebookModelMaterializer(
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
      parse = parseMalloynb(fileContents, modelPath);
   } catch {
      throw new Error("Could not parse model: " + modelPath);
   }

   let mm: ModelMaterializer | undefined = undefined;
   const oldImports: string[] = [];
   const oldSources: Record<string, Malloy.SourceInfo> = {};
   // Build the materializer chain synchronously: mm.getModel() is async and
   // may resolve out of order.
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
               const localMM = mms[index];
               if (!localMM) {
                  // Unreachable: a MALLOY statement always set `mm` above.
                  throw new Error("Model materializer is undefined");
               }
               // newSources: sources first visible at this cell.
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
                        const importModelInfo = modelInfoOf(importModel);
                        newSources = importModelInfo.entries
                           .filter((entry) => entry.kind === "source")
                           .filter(
                              (source) => !(source.name in oldSources),
                           ) as Malloy.SourceInfo[];
                        oldImports.push(importLocation.importURL.toString());
                     }),
                  );
               }
               const currentModelInfo = modelInfoOf(currentModelDef);
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
               } catch (error) {
                  // A cell that only defines sources has no query to describe, so this is routine.
                  logger.debug("No query info for a .malloynb cell", {
                     error,
                  });
               }

               return {
                  type: "code",
                  text: stmt.text,
                  runnable: runnable,
                  modelMaterializer: localMM,
                  modelDef: currentModelDef,
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

// ──────────────────────────────────────────────────────────────────────
// Helpers for hydrating worker-compiled models on the main thread
// ──────────────────────────────────────────────────────────────────────

/**
 * The `_` methods are `@internal` in Malloy but are the only way to build a
 * materializer from an existing modelDef/queryDef without recompiling.
 */
export type HydrationRuntime = Runtime & {
   _loadModelFromModelDef(modelDef: ModelDef): ModelMaterializer;
};
export type HydrationMaterializer = ModelMaterializer & {
   _loadQueryFromQueryDef(query: unknown): QueryMaterializer;
};

export function makeHydrationRuntime(
   malloyConfig: ModelConnectionInput,
   buildManifest?: BuildManifest["entries"],
): HydrationRuntime {
   const urlReader = new HackyDataStylesAccumulator(URL_READER);
   const config = toMalloyConfig(malloyConfig);
   // Malloy substitutes a persisted source for its materialized table at
   // getSQL time only when the runtime carries the manifest; without it the
   // hydrated model recomputes from base tables. `strict: false` serves live
   // any source absent from the manifest.
   return new Runtime({
      urlReader,
      config,
      buildManifest: buildManifest
         ? { entries: buildManifest, strict: false }
         : undefined,
   }) as HydrationRuntime;
}

/** Build live `RunnableNotebookCell[]` from worker-emitted cell data, without recompiling. */
export function hydrateNotebookCells(
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
               // Only possible if Malloy's internal shape drifts; drop the
               // runnable rather than fail the whole notebook.
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
         modelDef: cellModelDef,
         newSources: sc.newSources as Malloy.SourceInfo[] | undefined,
         queryInfo: sc.queryInfo as Malloy.QueryInfo | undefined,
      };
   });
}

/**
 * Cells for a notebook with no `modelDef` (all markdown), so `getNotebook()`
 * can still serve the raw text.
 */
export function hydrateMarkdownOnlyCells(
   notebookCells: SerializedNotebookCell[] | undefined,
): RunnableNotebookCell[] | undefined {
   if (!notebookCells) return undefined;
   return notebookCells.map((sc): RunnableNotebookCell => {
      if (sc.type === "markdown") return { type: "markdown", text: sc.text };
      return { type: "code", text: sc.text };
   });
}
