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
 * that path from disk; anything not in the map reads from disk as usual. It
 * exists for pre-aggregation, whose synthesized model is generated per load
 * and never written to disk, and which must resolve its `import` of the
 * author's model relative to the real package directory — so the synthesized
 * text needs a URL inside that directory without a file behind it. `modelPath`
 * still has to exist: it is what anchors `importBaseURL`.
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
 * Minimal subset of `Runtime` we use here. The `_` methods are
 * marked `@internal` in Malloy but are the only API for constructing
 * a materializer / query materializer from an existing `modelDef` /
 * queryDef — the public `loadModel(url)` path always recompiles.
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
   // Thread the package's bound build manifest into the *serve* runtime. Malloy
   // substitutes a persisted source for its materialized table at query
   // (getSQL) time, gated on `prepareResultOptions.buildManifest`; without this
   // the hydrated model always recomputes from the base tables even though the
   // manifest was bound at load. `strict: false` keeps serving live for any
   // source whose sourceEntityId is absent from the manifest.
   return new Runtime({
      urlReader,
      config,
      buildManifest: buildManifest
         ? { entries: buildManifest, strict: false }
         : undefined,
   }) as HydrationRuntime;
}

/**
 * Build the live `RunnableNotebookCell[]` from worker-emitted
 * per-cell data. Each MALLOY cell is hydrated via
 * `Runtime._loadModelFromModelDef` (for the cell's scope) and
 * `ModelMaterializer._loadQueryFromQueryDef` (for the cell's
 * runnable) — no recompile.
 */
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
         modelDef: cellModelDef,
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
export function hydrateMarkdownOnlyCells(
   notebookCells: SerializedNotebookCell[] | undefined,
): RunnableNotebookCell[] | undefined {
   if (!notebookCells) return undefined;
   return notebookCells.map((sc): RunnableNotebookCell => {
      if (sc.type === "markdown") return { type: "markdown", text: sc.text };
      // A code cell without a hydratable scope — surface text only.
      return { type: "code", text: sc.text };
   });
}
