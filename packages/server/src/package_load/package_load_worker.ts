/**
 * Package-load worker entry point.
 *
 * Runs inside a worker_threads `Worker`. Owns **no native connection
 * state of any kind** — every connection lookup, schema fetch, SQL
 * block schema, and non-file URL read is proxied back to the main
 * thread via correlated RPC messages. The worker is intentionally
 * pure-CPU: only Malloy parser / type-checker / IR-builder runs here.
 *
 * Why no DuckDB in the worker?
 * ----------------------------
 * DuckDB's native bindings cannot be safely loaded into more than one
 * isolate of the same Node/Bun process (we hit Bun crash 0x20131 when
 * the worker isolate and the main isolate both `dlopen` duckdb-native).
 * Even if Bun fixes that, holding a duckdb handle in the worker
 * duplicates the in-memory DB state and adds native-module load
 * latency to every worker spawn. Database probing (`readDatabases`)
 * stays on the main thread where it can reuse the package's existing
 * DuckDB connection (see PR #772).
 *
 * Boundary
 * --------
 *   1. Worker: read `publisher.json` (package manifest).
 *   2. Worker: compile every `.malloy` and `.malloynb` via Malloy.
 *      All `lookupConnection(name)` / schema-fetch calls Malloy makes
 *      during compile are proxied to the main thread's live
 *      connection pool over RPC.
 *   3. Worker: for each model, capture the structured-clonable fields
 *      the main-thread `Model` constructor needs: `modelDef`,
 *      `sourceInfos`, `sources`, `queries`, `filterMap`, `givens`,
 *      dataStyles, plus per-cell `modelDef` + `queryDef` for
 *      notebooks (so per-cell materializers / runnables can be
 *      hydrated on the main thread via `Runtime._loadModelFromModelDef`
 *      / `ModelMaterializer._loadQueryFromQueryDef` — no recompile).
 *   4. Main thread: probe embedded `.parquet` / `.csv` databases
 *      against the package's existing DuckDB connection.
 *
 * Per-model compile failures are returned in-band on
 * `SerializedModel.compilationError`; the rest of the package keeps
 * loading. Whole-package failures (e.g. manifest missing) come back
 * as `LoadPackageError`.
 *
 * Bundled separately by `build.ts` as `dist/package_load_worker.mjs`
 * so `new Worker(...)` can load it without dragging in the entire
 * server module graph.
 */
import {
   contextOverlay,
   type BuildManifestEntry,
   type Connection,
   type FetchSchemaOptions,
   type LookupConnection,
   MalloyConfig,
   MalloyError,
   type ModelDef,
   type ModelMaterializer,
   modelDefToModelInfo,
   type NamedModelObject,
   type NamedQueryDef,
   type Query,
   Runtime,
   type SQLSourceDef,
   type SQLSourceRequest,
   type StructDef,
   type TableSourceDef,
   type TurtleDef,
   isSourceDef,
} from "@malloydata/malloy";
import * as Malloy from "@malloydata/malloy-interfaces";
import {
   MalloySQLParser,
   MalloySQLStatementType,
} from "@malloydata/malloy-sql";
import * as fs from "fs";
import * as path from "path";
import { parentPort, threadId } from "node:worker_threads";
import recursive from "recursive-readdir";
import { fileURLToPath, pathToFileURL } from "url";

import {
   MODEL_FILE_SUFFIX,
   NOTEBOOK_FILE_SUFFIX,
   PACKAGE_MANIFEST_NAME,
} from "../constants";
import { HackyDataStylesAccumulator } from "../data_styles";
import { annotationTexts, type AnnotationsDef } from "../service/annotations";
import { parseFilters, type FilterDefinition } from "../service/filter";
import {
   malloyGivenToApi,
   type MalloyGiven,
   type MalloyGivenApi,
} from "../service/given";
import { ignoreDotfiles } from "../utils";
import type {
   ConnectionMetadata,
   ConnectionMetadataRequest,
   ConnectionMetadataResponse,
   LoadPackageError,
   LoadPackageRequest,
   LoadPackageResult,
   MainToWorkerMessage,
   ReadUrlRequest,
   ReadUrlResponse,
   SchemaForSqlRequest,
   SchemaForSqlResponse,
   SchemaForTablesRequest,
   SchemaForTablesResponse,
   SerializedError,
   SerializedModel,
   SerializedNotebookCell,
} from "./protocol";

if (!parentPort) {
   throw new Error(
      "package_load_worker.ts must be loaded inside a worker_threads Worker",
   );
}

const port = parentPort;

// ──────────────────────────────────────────────────────────────────────
// RPC plumbing for worker → main calls
// ──────────────────────────────────────────────────────────────────────

let nextRpcId = 0;
const pendingRpc = new Map<
   string,
   { resolve: (value: unknown) => void; reject: (err: Error) => void }
>();

function newRpcId(): string {
   nextRpcId += 1;
   return `w${threadId}-rpc-${nextRpcId}`;
}

function callMain<T>(send: (requestId: string) => void): Promise<T> {
   const requestId = newRpcId();
   return new Promise<T>((resolve, reject) => {
      pendingRpc.set(requestId, {
         resolve: (value) => resolve(value as T),
         reject,
      });
      send(requestId);
   });
}

function dispatchMainResponse(message: MainToWorkerMessage): void {
   if (
      message.type === "schema-for-tables-response" ||
      message.type === "schema-for-sql-response" ||
      message.type === "read-url-response" ||
      message.type === "connection-metadata-response"
   ) {
      const pending = pendingRpc.get(message.requestId);
      if (!pending) return;
      pendingRpc.delete(message.requestId);
      pending.resolve(message);
      return;
   }
   if (message.type === "rpc-error") {
      const pending = pendingRpc.get(message.requestId);
      if (!pending) return;
      pendingRpc.delete(message.requestId);
      pending.reject(deserializeError(message.error));
      return;
   }
}

// ──────────────────────────────────────────────────────────────────────
// Proxy connection: stand-in for non-duckdb connections at compile time
// ──────────────────────────────────────────────────────────────────────

class ProxyConnection {
   public readonly name: string;
   public readonly dialectName: string;
   private readonly digest: string;
   private readonly jobId: string;

   constructor(metadata: ConnectionMetadata, jobId: string) {
      this.name = metadata.name;
      this.dialectName = metadata.dialectName;
      this.digest = metadata.digest;
      this.jobId = jobId;
   }

   getDigest(): string {
      return this.digest;
   }

   async fetchSchemaForTables(
      tables: Record<string, string>,
      options: FetchSchemaOptions,
   ): Promise<{
      schemas: Record<string, TableSourceDef>;
      errors: Record<string, string>;
   }> {
      const response = await callMain<SchemaForTablesResponse>((requestId) => {
         const req: SchemaForTablesRequest = {
            type: "schema-for-tables",
            requestId,
            jobId: this.jobId,
            connectionName: this.name,
            tables,
            options: serializeFetchOptions(options),
         };
         port.postMessage(req);
      });
      return { schemas: response.schemas, errors: response.errors };
   }

   async fetchSchemaForSQLStruct(
      sentence: SQLSourceRequest,
      options: FetchSchemaOptions,
   ): Promise<
      | { structDef: SQLSourceDef; error?: undefined }
      | { error: string; structDef?: undefined }
   > {
      const response = await callMain<SchemaForSqlResponse>((requestId) => {
         const req: SchemaForSqlRequest = {
            type: "schema-for-sql",
            requestId,
            jobId: this.jobId,
            connectionName: this.name,
            sentence: sentence as unknown,
            options: serializeFetchOptions(options),
         };
         port.postMessage(req);
      });
      if (response.error !== undefined) return { error: response.error };
      if (response.structDef === undefined) {
         return { error: "Empty SQL schema response from main thread" };
      }
      return { structDef: response.structDef };
   }

   // Compile path never calls these on a non-duckdb connection (the
   // worker doesn't execute non-duckdb SQL). We throw rather than no-op
   // so a misrouted call surfaces loudly.
   async runSQL(): Promise<never> {
      throw new Error(
         `ProxyConnection(${this.name}): runSQL is not available in package-load workers`,
      );
   }
   isPool(): false {
      return false;
   }
   canPersist(): false {
      return false;
   }
   canStream(): false {
      return false;
   }
   async close(): Promise<void> {
      /* no-op */
   }
   async idle(): Promise<void> {
      /* no-op */
   }
   async estimateQueryCost(): Promise<never> {
      throw new Error(
         `ProxyConnection(${this.name}): estimateQueryCost not available in package-load workers`,
      );
   }
   async fetchMetadata(): Promise<Record<string, unknown>> {
      return {};
   }
   async fetchTableMetadata(): Promise<Record<string, unknown>> {
      return {};
   }
}

function serializeFetchOptions(options: FetchSchemaOptions): {
   refreshTimestamp?: number;
   modelAnnotation?: AnnotationsDef;
} {
   const out: {
      refreshTimestamp?: number;
      modelAnnotation?: AnnotationsDef;
   } = {};
   if (options.refreshTimestamp !== undefined) {
      out.refreshTimestamp = options.refreshTimestamp;
   }
   if (options.modelAnnotations !== undefined) {
      out.modelAnnotation = options.modelAnnotations;
   }
   return out;
}

// ──────────────────────────────────────────────────────────────────────
// URLReader: file:// → fs; everything else proxies to main thread
// ──────────────────────────────────────────────────────────────────────

function makeWorkerUrlReader(jobId: string): {
   readURL: (url: URL) => Promise<string>;
} {
   return {
      readURL: async (url: URL): Promise<string> => {
         if (url.protocol === "file:") {
            const filePath = fileURLToPath(url);
            return fs.promises.readFile(filePath, "utf8");
         }
         const response = await callMain<ReadUrlResponse>((requestId) => {
            const req: ReadUrlRequest = {
               type: "read-url",
               requestId,
               jobId,
               url: url.toString(),
            };
            port.postMessage(req);
         });
         return response.contents;
      },
   };
}

// ──────────────────────────────────────────────────────────────────────
// MalloyConfig assembly inside the worker
// ──────────────────────────────────────────────────────────────────────

/**
 * Build the package's MalloyConfig inside the worker. Every
 * connection name — including `"duckdb"` — resolves to a
 * {@link ProxyConnection} that RPCs schema fetches back to the main
 * thread. The worker never holds a native connection handle, which
 * keeps it pure-CPU and avoids dlopen'ing duckdb in a second isolate.
 *
 * Concurrent `lookupConnection(name)` calls for the same name are
 * deduped via `inflight` — Malloy's compile pipeline can fan out
 * many schema fetches that all hit `lookupConnection` before any
 * resolve, and we don't want to N-multiply the metadata RPC.
 */
function buildWorkerMalloyConfig(job: LoadPackageRequest): MalloyConfig {
   const proxies = new Map<string, ProxyConnection>();
   const inflight = new Map<string, Promise<ProxyConnection>>();

   const config = new MalloyConfig(
      { connections: {} },
      { config: contextOverlay({ rootDirectory: job.packagePath }) },
   );
   config.wrapConnections(
      (_base: LookupConnection<Connection>): LookupConnection<Connection> => ({
         lookupConnection: async (name?: string): Promise<Connection> => {
            const effectiveName = name ?? job.defaultConnectionName ?? "duckdb";
            const cached = proxies.get(effectiveName);
            if (cached) return cached as unknown as Connection;
            let pending = inflight.get(effectiveName);
            if (!pending) {
               pending = (async () => {
                  const response = await callMain<ConnectionMetadataResponse>(
                     (requestId) => {
                        const req: ConnectionMetadataRequest = {
                           type: "connection-metadata",
                           requestId,
                           jobId: job.requestId,
                           connectionName: effectiveName,
                        };
                        port.postMessage(req);
                     },
                  );
                  const proxy = new ProxyConnection(
                     response.metadata,
                     job.requestId,
                  );
                  proxies.set(effectiveName, proxy);
                  inflight.delete(effectiveName);
                  return proxy;
               })();
               inflight.set(effectiveName, pending);
            }
            return (await pending) as unknown as Connection;
         },
      }),
   );
   return config;
}

// ──────────────────────────────────────────────────────────────────────
// Filesystem helpers (replicated from service/package.ts so the worker
// stays decoupled from the main-thread service module graph)
// ──────────────────────────────────────────────────────────────────────

async function readPackageMetadata(
   packagePath: string,
): Promise<{ name?: string; description?: string }> {
   const manifestPath = path.join(packagePath, PACKAGE_MANIFEST_NAME);
   const contents = await fs.promises.readFile(manifestPath, "utf8");
   const parsed = JSON.parse(contents) as {
      name?: string;
      description?: string;
   };
   return { name: parsed.name, description: parsed.description };
}

async function listPackageFiles(packagePath: string): Promise<string[]> {
   const files = await recursive(packagePath, [ignoreDotfiles]);
   return files.map((full: string) =>
      path.relative(packagePath, full).replace(/\\/g, "/"),
   );
}

function filterModelPaths(allRelative: string[]): string[] {
   return allRelative.filter(
      (p) => p.endsWith(MODEL_FILE_SUFFIX) || p.endsWith(NOTEBOOK_FILE_SUFFIX),
   );
}

// ──────────────────────────────────────────────────────────────────────
// Model compile (mirrors service/model.ts but produces SerializedModel)
// ──────────────────────────────────────────────────────────────────────

interface ApiSourceWire {
   name: string;
   annotations?: string[];
   views?: { name: string; annotations?: string[] }[];
   filters?: unknown[];
   givens?: unknown[];
}
interface ApiQueryWire {
   name: string;
   sourceName?: string;
   annotations?: string[];
}
type ApiGivenWire = MalloyGivenApi;

async function collectImportedSourceInfos(
   modelDef: ModelDef,
   runtime: Runtime,
   importBaseURL: URL,
): Promise<{
   sourceInfos: Malloy.SourceInfo[];
   importedNames: Set<string>;
}> {
   const sourceInfos: Malloy.SourceInfo[] = [];
   const importedNames = new Set<string>();
   const imports = modelDef.imports ?? [];
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
         const importedInfo = modelDefToModelInfo(importedModelDef);
         const importedSources = importedInfo.entries.filter(
            (entry) => entry.kind === "source",
         ) as Malloy.SourceInfo[];
         for (const source of importedSources) {
            if (!importedNames.has(source.name)) {
               sourceInfos.push(source);
               importedNames.add(source.name);
            }
         }
      } catch {
         // Best-effort, matches the in-process Model.create behaviour
         // of warning-and-skipping when an import can't be loaded.
      }
   }
   return { sourceInfos, importedNames };
}

function appendLocalSourceInfos(
   modelDef: ModelDef,
   target: Malloy.SourceInfo[],
   importedNames: Set<string>,
): void {
   const localInfo = modelDefToModelInfo(modelDef);
   const localSources = localInfo.entries.filter(
      (entry) => entry.kind === "source",
   ) as Malloy.SourceInfo[];
   for (const source of localSources) {
      if (!importedNames.has(source.name)) target.push(source);
   }
}

function extractSources(
   modelDef: ModelDef,
   givens: ApiGivenWire[] | undefined,
): { sources: ApiSourceWire[]; filterMap: Map<string, FilterDefinition[]> } {
   const filterMap = new Map<string, FilterDefinition[]>();
   const sources: ApiSourceWire[] = Object.values(modelDef.contents)
      .filter((obj) => isSourceDef(obj))
      .map((sourceObj) => {
         const sourceName =
            (sourceObj as StructDef).as || (sourceObj as StructDef).name;
         const annotations = annotationTexts(
            (sourceObj as StructDef).annotations,
         );

         const collected: string[][] = [];
         let cur = (sourceObj as StructDef).annotations;
         while (cur) {
            if (cur.blockNotes) {
               collected.push(cur.blockNotes.map((note) => note.text));
            }
            cur = cur.inherits;
         }
         const allAnnotations = collected.reverse().flat();
         let filters: unknown[] | undefined;
         if (allAnnotations.length > 0) {
            try {
               const parsed = parseFilters(allAnnotations);
               if (parsed.length > 0) {
                  filterMap.set(sourceName, parsed);
                  const fields = (sourceObj as StructDef).fields;
                  filters = parsed.map((f) => {
                     const field = fields.find(
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
            } catch {
               /* parse errors are warnings; matches in-process */
            }
         }

         const views = (sourceObj as StructDef).fields
            .filter((f) => f.type === "turtle")
            .filter((turtle) =>
               (turtle as TurtleDef).pipeline
                  .map((stage) => stage.type)
                  .every((type) => type === "reduce"),
            )
            .map((turtle) => ({
               name: turtle.as || turtle.name,
               annotations: annotationTexts(turtle.annotations),
            }));

         return {
            name: sourceName,
            annotations,
            views,
            filters,
            givens,
         } as ApiSourceWire;
      });

   return { sources, filterMap };
}

function extractQueries(modelDef: ModelDef): ApiQueryWire[] {
   const isNamedQuery = (obj: NamedModelObject): obj is NamedQueryDef =>
      obj.type === "query";
   return Object.values(modelDef.contents)
      .filter(isNamedQuery)
      .map((q) => ({
         name: q.as || q.name,
         sourceName: typeof q.structRef === "string" ? q.structRef : undefined,
         annotations: annotationTexts(q.annotations),
      }));
}

function buildRuntimeForModel(
   job: LoadPackageRequest,
   malloyConfig: MalloyConfig,
   jobId: string,
): { runtime: Runtime; urlReader: HackyDataStylesAccumulator } {
   const urlReader = new HackyDataStylesAccumulator(makeWorkerUrlReader(jobId));
   const runtime = new Runtime({
      urlReader,
      config: malloyConfig,
      buildManifest:
         job.buildManifest !== undefined && job.buildManifest !== null
            ? {
                 entries: job.buildManifest as Record<
                    string,
                    BuildManifestEntry
                 >,
                 strict: false,
              }
            : undefined,
   });
   return { runtime, urlReader };
}

async function compileMalloyModel(
   job: LoadPackageRequest,
   malloyConfig: MalloyConfig,
   modelPath: string,
): Promise<SerializedModel> {
   const compileStart = performance.now();
   const fullPath = path.join(job.packagePath, modelPath);
   // `pathToFileURL` produces a valid URL on every platform; the
   // naïve `file://${fullPath}` template parses host=`D:` on Windows.
   const modelURL = pathToFileURL(fullPath);
   const importBaseURL = new URL(".", modelURL);

   const { runtime, urlReader } = buildRuntimeForModel(
      job,
      malloyConfig,
      job.requestId,
   );
   const mm = runtime.loadModel(modelURL, { importBaseURL });
   const compiled = await mm.getModel();
   const modelDef = compiled._modelDef;

   const malloyGivens = Array.from(compiled.givens.values());
   const givens =
      malloyGivens.length > 0
         ? malloyGivens.map((g) => malloyGivenToApi(g as MalloyGiven))
         : undefined;

   const { sourceInfos, importedNames } = await collectImportedSourceInfos(
      modelDef,
      runtime,
      importBaseURL,
   );
   appendLocalSourceInfos(modelDef, sourceInfos, importedNames);

   const { sources, filterMap } = extractSources(modelDef, givens);
   const queries = extractQueries(modelDef);

   return {
      modelPath,
      modelType: "model",
      modelDef,
      modelInfo: modelDefToModelInfo(modelDef),
      sourceInfos,
      sources,
      queries,
      filterMap: Array.from(filterMap.entries()),
      givens,
      dataStyles: urlReader.getHackyAccumulatedDataStyles(),
      compileDurationMs: performance.now() - compileStart,
   };
}

async function compileNotebookModel(
   job: LoadPackageRequest,
   malloyConfig: MalloyConfig,
   modelPath: string,
): Promise<SerializedModel> {
   const compileStart = performance.now();
   const fullPath = path.join(job.packagePath, modelPath);
   // See compileMalloyModel above: `pathToFileURL` is the only
   // cross-platform way to build a file URL from an OS path.
   const modelURL = pathToFileURL(fullPath);
   const importBaseURL = new URL(".", modelURL);

   const { runtime, urlReader } = buildRuntimeForModel(
      job,
      malloyConfig,
      job.requestId,
   );

   const fileContents = await fs.promises.readFile(modelURL, "utf8");
   const parse = MalloySQLParser.parse(fileContents, modelPath);

   // Build the extendModel chain synchronously so per-cell materializers
   // line up with statement order. Matches the in-process flow in
   // Model.getNotebookModelMaterializer.
   let mm: ModelMaterializer | undefined;
   const perCellMM: (ModelMaterializer | undefined)[] = parse.statements.map(
      (stmt) => {
         if (stmt.type === MalloySQLStatementType.MALLOY) {
            mm =
               mm === undefined
                  ? runtime.loadModel(stmt.text, { importBaseURL })
                  : mm.extendModel(stmt.text, { importBaseURL });
         }
         return mm;
      },
   );

   const oldImports: string[] = [];
   const oldSources: Record<string, Malloy.SourceInfo> = {};
   const notebookCells: SerializedNotebookCell[] = [];
   for (let i = 0; i < parse.statements.length; i++) {
      const stmt = parse.statements[i];
      if (stmt.type === MalloySQLStatementType.MARKDOWN) {
         notebookCells.push({ type: "markdown", text: stmt.text });
         continue;
      }
      if (stmt.type !== MalloySQLStatementType.MALLOY) continue;

      const localMM = perCellMM[i];
      if (!localMM) {
         // Shouldn't happen for a MALLOY statement, but guard rather
         // than crash a whole notebook compile on one corrupt cell.
         continue;
      }
      const currentModelDef = (await localMM.getModel())._modelDef;

      // newSources via the import chain — mirrors in-process logic.
      let newSources: Malloy.SourceInfo[] = [];
      const newImports = currentModelDef.imports?.slice(oldImports.length);
      if (newImports) {
         for (const importLocation of newImports) {
            try {
               const modelString = await runtime.urlReader.readURL(
                  new URL(importLocation.importURL),
               );
               const importModel = (
                  await runtime
                     .loadModel(modelString as string, { importBaseURL })
                     .getModel()
               )._modelDef;
               const importInfo = modelDefToModelInfo(importModel);
               newSources = importInfo.entries
                  .filter((e) => e.kind === "source")
                  .filter(
                     (s) => !(s.name in oldSources),
                  ) as Malloy.SourceInfo[];
               oldImports.push(importLocation.importURL.toString());
            } catch {
               // Same best-effort policy as the in-process path.
            }
         }
      }
      const currentInfo = modelDefToModelInfo(currentModelDef);
      newSources = newSources.concat(
         currentInfo.entries
            .filter((e) => e.kind === "source")
            .filter((s) => !(s.name in oldSources)) as Malloy.SourceInfo[],
      );
      for (const s of newSources) oldSources[s.name] = s;

      // Capture the per-cell final-query queryDef so the main thread can
      // hydrate a QueryMaterializer via
      // `ModelMaterializer._loadQueryFromQueryDef` without a recompile.
      const runnable = localMM.loadFinalQuery();
      let cellQueryDef: Query | undefined;
      let queryInfo: Malloy.QueryInfo | undefined;
      try {
         const prepared = await runnable.getPreparedQuery();
         cellQueryDef = prepared._query;
         const queryName =
            (prepared._query as NamedQueryDef).as ||
            (prepared._query as NamedQueryDef).name;
         const anonymous =
            currentInfo.anonymous_queries[
               currentInfo.anonymous_queries.length - 1
            ];
         if (anonymous) {
            queryInfo = {
               name: queryName,
               schema: anonymous.schema,
               annotations: anonymous.annotations,
               definition: anonymous.definition,
               code: anonymous.code,
               location: anonymous.location,
            } as Malloy.QueryInfo;
         }
      } catch {
         // Some cells (source-only) have no final query; that's fine.
      }

      notebookCells.push({
         type: "code",
         text: stmt.text,
         cellModelDef: currentModelDef,
         cellQueryDef,
         newSources,
         queryInfo,
      });
   }

   // Aggregate (notebook-level) artifacts — derived from the final mm
   // if any MALLOY statements were present. If the notebook is all
   // markdown, modelDef stays undefined and the main thread treats
   // this as a notebook with no compiled content.
   let finalModelDef: ModelDef | undefined;
   let finalSources: ApiSourceWire[] | undefined;
   let finalQueries: ApiQueryWire[] | undefined;
   let finalSourceInfos: Malloy.SourceInfo[] | undefined;
   let finalFilterMap: Map<string, FilterDefinition[]> | undefined;
   let finalGivens: ApiGivenWire[] | undefined;
   if (mm) {
      const compiled = await mm.getModel();
      finalModelDef = compiled._modelDef;
      const malloyGivens = Array.from(compiled.givens.values());
      finalGivens =
         malloyGivens.length > 0
            ? malloyGivens.map((g) => malloyGivenToApi(g as MalloyGiven))
            : undefined;
      const collected = await collectImportedSourceInfos(
         finalModelDef,
         runtime,
         importBaseURL,
      );
      appendLocalSourceInfos(
         finalModelDef,
         collected.sourceInfos,
         collected.importedNames,
      );
      finalSourceInfos = collected.sourceInfos;
      const extracted = extractSources(finalModelDef, finalGivens);
      finalSources = extracted.sources;
      finalFilterMap = extracted.filterMap;
      finalQueries = extractQueries(finalModelDef);
   }

   return {
      modelPath,
      modelType: "notebook",
      modelDef: finalModelDef,
      modelInfo: finalModelDef ? modelDefToModelInfo(finalModelDef) : undefined,
      sourceInfos: finalSourceInfos,
      sources: finalSources,
      queries: finalQueries,
      filterMap: finalFilterMap
         ? Array.from(finalFilterMap.entries())
         : undefined,
      givens: finalGivens,
      notebookCells,
      dataStyles: urlReader.getHackyAccumulatedDataStyles(),
      compileDurationMs: performance.now() - compileStart,
   };
}

async function compileOneModel(
   job: LoadPackageRequest,
   malloyConfig: MalloyConfig,
   modelPath: string,
): Promise<SerializedModel> {
   try {
      if (modelPath.endsWith(MODEL_FILE_SUFFIX)) {
         return await compileMalloyModel(job, malloyConfig, modelPath);
      }
      if (modelPath.endsWith(NOTEBOOK_FILE_SUFFIX)) {
         return await compileNotebookModel(job, malloyConfig, modelPath);
      }
      return {
         modelPath,
         modelType: "model",
         compilationError: {
            name: "Error",
            message: `Unknown model file suffix: ${modelPath}`,
         },
      };
   } catch (error) {
      const modelType: SerializedModel["modelType"] = modelPath.endsWith(
         NOTEBOOK_FILE_SUFFIX,
      )
         ? "notebook"
         : "model";
      return {
         modelPath,
         modelType,
         compilationError: serializeError(error),
      };
   }
}

// ──────────────────────────────────────────────────────────────────────
// The actual load-package job
// ──────────────────────────────────────────────────────────────────────

async function loadPackage(
   job: LoadPackageRequest,
): Promise<LoadPackageResult> {
   const loadStart = performance.now();

   const packageMetadata = await readPackageMetadata(job.packagePath);
   const malloyConfig = buildWorkerMalloyConfig(job);

   const allFiles = await listPackageFiles(job.packagePath);
   const modelPaths = filterModelPaths(allFiles);
   const models = await Promise.all(
      modelPaths.map((modelPath) =>
         compileOneModel(job, malloyConfig, modelPath),
      ),
   );

   return {
      type: "load-package-result",
      requestId: job.requestId,
      packageMetadata,
      models,
      loadDurationMs: performance.now() - loadStart,
   };
}

// ──────────────────────────────────────────────────────────────────────
// Error serialization
// ──────────────────────────────────────────────────────────────────────

function serializeError(error: unknown): SerializedError {
   if (error instanceof MalloyError) {
      return {
         name: error.name,
         message: error.message,
         stack: error.stack,
         malloyProblems: error.problems as unknown[],
         isCompilationError: true,
      };
   }
   if (error instanceof Error) {
      return {
         name: error.name,
         message: error.message,
         stack: error.stack,
      };
   }
   return { name: "Error", message: String(error) };
}

function deserializeError(serialized: SerializedError): Error {
   const err = new Error(serialized.message);
   err.name = serialized.name;
   if (serialized.stack) err.stack = serialized.stack;
   return err;
}

// ──────────────────────────────────────────────────────────────────────
// Message dispatcher
// ──────────────────────────────────────────────────────────────────────

let shuttingDown = false;
const inFlightJobs = new Set<string>();

port.on("message", (message: MainToWorkerMessage) => {
   if (message.type === "shutdown") {
      shuttingDown = true;
      maybeExit();
      return;
   }
   if (message.type === "load-package") {
      if (shuttingDown) {
         const errMsg: LoadPackageError = {
            type: "load-package-error",
            requestId: message.requestId,
            error: {
               name: "ShuttingDown",
               message: "Package-load worker is shutting down",
            },
         };
         port.postMessage(errMsg);
         return;
      }
      inFlightJobs.add(message.requestId);
      void runJob(message);
      return;
   }
   dispatchMainResponse(message);
});

async function runJob(job: LoadPackageRequest): Promise<void> {
   try {
      const result = await loadPackage(job);
      port.postMessage(result);
   } catch (error) {
      const errMsg: LoadPackageError = {
         type: "load-package-error",
         requestId: job.requestId,
         error: serializeError(error),
      };
      port.postMessage(errMsg);
   } finally {
      inFlightJobs.delete(job.requestId);
      maybeExit();
   }
}

function maybeExit(): void {
   if (shuttingDown && inFlightJobs.size === 0 && pendingRpc.size === 0) {
      // Give the postMessage queue a tick to flush before exit so the
      // last result actually reaches the parent.
      setImmediate(() => process.exit(0));
   }
}

// Announce readiness — the pool waits for this before dispatching jobs
// to a newly-spawned worker so we don't race the worker's module-init
// time.
port.postMessage({ type: "ready" });
