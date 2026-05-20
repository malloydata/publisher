/**
 * Compile worker entry point.
 *
 * Runs inside a worker_threads `Worker`. Owns no DuckDB / native
 * connection state — every schema lookup is proxied back to the
 * main thread via {@link SchemaForTablesRequest} / {@link
 * SchemaForSqlRequest}. The point of this file is to take the
 * dominant CPU cost of a Malloy compile (parser, type checker, IR
 * builder, sourceInfo extraction) off the main event loop so the
 * Kubernetes liveness probe on `/health/liveness` never gets parked
 * behind a multi-second compile.
 *
 * Contract:
 *   - Receives {@link CompileJobRequest} messages from the parent
 *     port. Dispatches one compile per message.
 *   - Proxies schema and URL-reader operations back to the parent
 *     via correlated RPC requests; awaits matching responses.
 *   - Sends back exactly one {@link CompileJobResult} or {@link
 *     CompileJobError} per job.
 *   - Honours a graceful {@link ShutdownRequest} so the pool can
 *     drain on SIGTERM.
 *
 * This file is bundled separately by build.ts and shipped as
 * `dist/compile_worker.mjs`.
 */
import {
   contextOverlay,
   MalloyConfig,
   MalloyError,
   Runtime,
   isSourceDef,
   modelDefToModelInfo,
   type Annotation,
   type Connection,
   type FetchSchemaOptions,
   type LookupConnection,
   type ModelDef,
   type ModelMaterializer,
   type NamedModelObject,
   type NamedQueryDef,
   type SQLSourceDef,
   type SQLSourceRequest,
   type StructDef,
   type TableSourceDef,
   type TurtleDef,
} from "@malloydata/malloy";
import * as Malloy from "@malloydata/malloy-interfaces";
import * as fs from "fs";
import { parentPort, threadId } from "node:worker_threads";
import { fileURLToPath } from "url";
import { parseFilters, type FilterDefinition } from "../service/filter";
import type {
   CompileJobError,
   CompileJobRequest,
   CompileJobResult,
   ConnectionMetadata,
   ConnectionMetadataRequest,
   ConnectionMetadataResponse,
   MainToWorkerMessage,
   ReadUrlRequest,
   ReadUrlResponse,
   SchemaForSqlRequest,
   SchemaForSqlResponse,
   SchemaForTablesRequest,
   SchemaForTablesResponse,
   SerializedError,
} from "./protocol";

if (!parentPort) {
   throw new Error(
      "compile_worker.ts must be loaded inside a worker_threads Worker",
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
// Stub InfoConnection that proxies schema fetches to the main thread
// ──────────────────────────────────────────────────────────────────────

/**
 * Minimal `Connection` implementation that satisfies Malloy's compile
 * pipeline. Only the methods called during compile are implemented
 * meaningfully; the rest throw, since the worker never executes SQL.
 *
 * Holds the `jobId` so the main thread can route the schema RPC to
 * the right environment-side `MalloyConfig`.
 */
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
      if (response.error !== undefined) {
         return { error: response.error };
      }
      if (response.structDef === undefined) {
         return { error: "Empty SQL schema response from main thread" };
      }
      return { structDef: response.structDef };
   }

   // Compile path never calls these. We intentionally throw rather
   // than silently no-op so a misrouted query in a worker surfaces
   // as a loud error rather than a wrong-answer bug.
   async runSQL(): Promise<never> {
      throw new Error(
         `ProxyConnection(${this.name}): runSQL is not available in compile workers`,
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
         `ProxyConnection(${this.name}): estimateQueryCost not available in compile workers`,
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
   modelAnnotation?: Annotation;
} {
   const out: { refreshTimestamp?: number; modelAnnotation?: Annotation } = {};
   if (options.refreshTimestamp !== undefined) {
      out.refreshTimestamp = options.refreshTimestamp;
   }
   if (options.modelAnnotation !== undefined) {
      out.modelAnnotation = options.modelAnnotation;
   }
   return out;
}

// ──────────────────────────────────────────────────────────────────────
// URLReader: try fs first, fall back to main-thread RPC for non-file URLs
// ──────────────────────────────────────────────────────────────────────

function makeWorkerUrlReader(jobId: string) {
   return {
      readURL: async (url: URL): Promise<string> => {
         if (url.protocol === "file:") {
            const filePath = fileURLToPath(url);
            return fs.promises.readFile(filePath, "utf8");
         }
         // Non-file URL — delegate to main so semantics stay
         // identical to the in-process URL_READER (e.g. allow
         // future https:// imports).
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

function buildWorkerMalloyConfig(job: CompileJobRequest): MalloyConfig {
   // Connections are resolved lazily on first lookup via a metadata
   // RPC back to the main thread — see ConnectionMetadataRequest.
   // We never enumerate the connection list upfront; the package
   // layer doesn't always have one (e.g. environment-wrapped
   // connections appear only when Malloy compiles a `table('...')`
   // reference that names them).
   //
   // Concurrent lookups for the same name are deduped via
   // `inflight` — Malloy's compile pipeline can fan-out multiple
   // schema fetches that all hit `lookupConnection(name)` before
   // any of them resolve, and we don't want to N-multiply the RPC.
   const proxies = new Map<string, ProxyConnection>();
   const inflight = new Map<string, Promise<ProxyConnection>>();
   const config = new MalloyConfig(
      { connections: {} },
      {
         config: contextOverlay({ rootDirectory: job.packagePath }),
      },
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
            const proxy = await pending;
            return proxy as unknown as Connection;
         },
      }),
   );
   return config;
}

// ──────────────────────────────────────────────────────────────────────
// The actual compile — mirrors Model.create's in-process flow but
// only the parts that produce data shippable across postMessage.
// ──────────────────────────────────────────────────────────────────────

async function compile(job: CompileJobRequest): Promise<CompileJobResult> {
   const compileStart = performance.now();

   const malloyConfig = buildWorkerMalloyConfig(job);
   const urlReader = makeWorkerUrlReader(job.requestId);

   const runtime = new Runtime({
      urlReader,
      config: malloyConfig,
      // job.buildManifest is wire-typed as `unknown` because the
      // worker protocol doesn't depend on Malloy types — assert
      // the shape Malloy's Runtime expects here. We only pass the
      // wrapper when the caller actually supplied entries.
      buildManifest:
         job.buildManifest !== undefined && job.buildManifest !== null
            ? {
                 entries: job.buildManifest as Record<
                    string,
                    import("@malloydata/malloy").BuildManifestEntry
                 >,
                 strict: false,
              }
            : undefined,
   });

   const modelURL = new URL(`file://${job.packagePath}/${job.modelPath}`);
   const importBaseURL = new URL(".", modelURL);

   const mm: ModelMaterializer = runtime.loadModel(modelURL, {
      importBaseURL,
   });

   const compiledModel = await mm.getModel();
   const modelDef = compiledModel._modelDef as ModelDef;

   // Givens — converted to API shape here so the main thread can
   // stash them on the Model without needing Malloy's MalloyGiven
   // type (which has non-serializable methods).
   const malloyGivens = Array.from(compiledModel.givens.values());
   const givens: ApiGivenWire[] | undefined =
      malloyGivens.length > 0
         ? malloyGivens.map((g) => malloyGivenToWire(g))
         : undefined;

   // Imported sourceInfos — mirrors Model.create line 199–242. We
   // collect them here so the main thread doesn't have to recompile
   // imports just to fill in the response.
   const sourceInfos: Malloy.SourceInfo[] = [];
   const importedSourceNames = new Set<string>();
   const imports = modelDef.imports ?? [];
   for (const importLocation of imports) {
      try {
         const modelString = await urlReader.readURL(
            new URL(importLocation.importURL),
         );
         const importedModelDef = (
            await runtime.loadModel(modelString, { importBaseURL }).getModel()
         )._modelDef;
         const importedInfo = modelDefToModelInfo(importedModelDef);
         const importedSources = importedInfo.entries.filter(
            (entry) => entry.kind === "source",
         ) as Malloy.SourceInfo[];
         for (const source of importedSources) {
            if (!importedSourceNames.has(source.name)) {
               sourceInfos.push(source);
               importedSourceNames.add(source.name);
            }
         }
      } catch {
         // Best-effort, matches the in-process Model.create behaviour
         // of warning-and-skipping when an import can't be loaded.
      }
   }
   const localInfo = modelDefToModelInfo(modelDef);
   const localSources = localInfo.entries.filter(
      (entry) => entry.kind === "source",
   ) as Malloy.SourceInfo[];
   for (const source of localSources) {
      if (!importedSourceNames.has(source.name)) {
         sourceInfos.push(source);
      }
   }

   const { sources, filterMap } = extractSources(job.modelPath, modelDef);
   const queries = extractQueries(job.modelPath, modelDef);
   const filterMapEntries: Array<[string, FilterDefinition[]]> = Array.from(
      filterMap.entries(),
   );

   return {
      type: "compile-result",
      requestId: job.requestId,
      modelDef,
      sourceInfos,
      sources,
      queries,
      filterMap: filterMapEntries,
      givens,
      // dataStyles: the in-process HackyDataStylesAccumulator is fed
      // by the URLReader. We don't reuse it here — main thread will
      // accumulate its own when it builds the lazy materializer.
      dataStyles: {},
      compileDurationMs: performance.now() - compileStart,
   };
}

/**
 * Wire-friendly mirror of the publisher's `ApiGiven`. Inlined here so
 * the worker doesn't import the OpenAPI-generated `components` map
 * (which would drag the whole api.ts surface into the worker bundle).
 * Kept structurally identical to `ApiGiven` so the main thread can
 * type-assert it without conversion.
 */
interface ApiGivenWire {
   name: string;
   type: string;
   annotations?: string[];
}

interface MalloyGivenLike {
   name: string;
   type: { type: string; filterType?: string };
   getTaglines(regex: RegExp): string[];
}

function malloyGivenToWire(given: MalloyGivenLike): ApiGivenWire {
   const t = given.type;
   const renderedType =
      t.type === "filter expression" ? `filter<${t.filterType}>` : t.type;
   return {
      name: given.name,
      type: renderedType,
      annotations: given.getTaglines(/^#\(/),
   };
}

// ──────────────────────────────────────────────────────────────────────
// extractSources / extractQueries — direct copies of the static
// helpers in Model.ts. Inlined here to keep the worker independent
// of the main-thread module graph (smaller bundle, fewer imports of
// things that pull in DuckDB or AWS SDK by transitive include).
// ──────────────────────────────────────────────────────────────────────

interface ApiSource {
   name: string;
   annotations?: string[];
   views?: { name: string; annotations?: string[] }[];
   filters?: unknown[];
}
interface ApiQuery {
   name: string;
   sourceName?: string;
   annotations?: string[];
}

function extractSources(
   modelPath: string,
   modelDef: ModelDef,
): { sources: ApiSource[]; filterMap: Map<string, FilterDefinition[]> } {
   const filterMap = new Map<string, FilterDefinition[]>();
   const sources: ApiSource[] = Object.values(modelDef.contents)
      .filter((obj) => isSourceDef(obj))
      .map((sourceObj) => {
         const sourceName =
            (sourceObj as StructDef).as || (sourceObj as StructDef).name;
         const annotations = (sourceObj as StructDef).annotation?.blockNotes
            ?.filter((note) => note.at.url.includes(modelPath))
            .map((note) => note.text);

         const collected: string[][] = [];
         let cur: Annotation | undefined = (sourceObj as StructDef).annotation;
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
               // Mirrors the in-process behaviour: filter parse
               // errors are warnings, not fatal compile failures.
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
               annotations: turtle?.annotation?.blockNotes
                  ?.filter((note) => note.at.url.includes(modelPath))
                  .map((note) => note.text),
            }));

         return {
            name: sourceName,
            annotations,
            views,
            filters,
         } as ApiSource;
      });

   return { sources, filterMap };
}

function extractQueries(modelPath: string, modelDef: ModelDef): ApiQuery[] {
   const isNamedQuery = (obj: NamedModelObject): obj is NamedQueryDef =>
      obj.type === "query";
   return Object.values(modelDef.contents)
      .filter(isNamedQuery)
      .map((q) => ({
         name: q.as || q.name,
         sourceName: typeof q.structRef === "string" ? q.structRef : undefined,
         annotations: q?.annotation?.blockNotes
            ?.filter((note: { at: { url: string } }) =>
               note.at.url.includes(modelPath),
            )
            .map((note: { text: string }) => note.text),
      }));
}

// ──────────────────────────────────────────────────────────────────────
// Error serialization
// ──────────────────────────────────────────────────────────────────────

function serializeError(error: unknown): SerializedError {
   if (error instanceof MalloyError) {
      // MalloyError is what Malloy throws for compile failures
      // (parse / type / unresolved-reference errors). Flagging
      // `isCompilationError` lets the main thread re-wrap it as a
      // `ModelCompilationError` so callers' instanceof checks for
      // that type still fire after a worker-side compile.
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
      // Don't exit until in-flight jobs finish. Once empty we exit
      // via the explicit process.exit() below; until then we just
      // keep servicing message responses.
      maybeExit();
      return;
   }
   if (message.type === "compile") {
      if (shuttingDown) {
         const errMsg: CompileJobError = {
            type: "compile-error",
            requestId: message.requestId,
            error: {
               name: "ShuttingDown",
               message: "Compile worker is shutting down",
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

async function runJob(job: CompileJobRequest): Promise<void> {
   try {
      const result = await compile(job);
      port.postMessage(result);
   } catch (error) {
      const errMsg: CompileJobError = {
         type: "compile-error",
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

// Announce readiness — the pool waits for this before dispatching
// jobs to a newly-spawned worker so we don't race the worker's
// module-init time.
port.postMessage({ type: "ready" });
