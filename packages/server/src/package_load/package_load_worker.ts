// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

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
 * DuckDB connection.
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
   isSourceDef,
   type BuildManifestEntry,
   type Connection,
   type FetchSchemaOptions,
   type LookupConnection,
   MalloyConfig,
   type ModelDef,
   type ModelMaterializer,
   type NamedQueryDef,
   type Query,
   Runtime,
   type SQLSourceDef,
   type SQLSourceRequest,
   type TableSourceDef,
} from "@malloydata/malloy";
import * as Malloy from "@malloydata/malloy-interfaces";
import {
   MalloySQLParser,
   MalloySQLStatementType,
} from "@malloydata/malloy-sql";
import * as fs from "fs";
import * as path from "path";
import { AsyncLocalStorage } from "node:async_hooks";
import { parentPort, threadId, workerData } from "node:worker_threads";
import recursive from "recursive-readdir";
import { fileURLToPath, pathToFileURL } from "url";

import {
   MODEL_FILE_SUFFIX,
   NOTEBOOK_FILE_SUFFIX,
   installRecordPath,
   PACKAGE_MANIFEST_NAME,
} from "../constants";
import {
   recordAuthorizeAdmitAllGate,
   recordRowLevelGateRejected,
} from "../authorize_metrics";
import { HackyDataStylesAccumulator } from "../data_styles";
import { PackageManifestError } from "../errors";
import { deserializeError, serializeError } from "./error_wire";
import { translatorMalloyError } from "../service/translator_error";
import {
   assertNoLegacyStringGate,
   assertNoMisplacedAuthorizeAnnotations,
   findLegacyStringGates,
   validateAuthorizeProbes,
   type AuthorizeMap,
   type AuthorizeOwnNotesMap,
   type MisplacedAuthorizeAnnotation,
} from "../service/authorize";
import {
   assertAuthorizeGrammarValid,
   assertNoRetiredRouteMarkers,
   collectRetiredRouteMarkers,
   computeGivenDeclaredTypes,
} from "../service/gate_classification";
import {
   validateSourceLineGateGivenUsage,
   type ExpandableRefSummary,
} from "../service/gate_dimension";
import { modelInfoOf } from "../service/model_info";
import { type FilterDefinition } from "../service/filter";
import {
   PackageMaterializationConfig,
   PackageScope,
   materializationWithQueryMetadata,
   parsePackageMaterialization,
   queryMetadataParseWarnings,
   resolveExplores,
   resolvePackageQueryMetadata,
   resolvePackageScope,
} from "../service/package_manifest";
import {
   type PackageRetrievalSettings,
   readPackageRetrieval,
} from "../service/package_retrieval";
import {
   collectSourceInfos,
   extractQueriesFromModelDef,
   extractSourcesFromModelDef,
} from "../service/source_extraction";
import {
   malloyGivenToApi,
   type MalloyGiven,
   type MalloyGivenApi,
   attachSuggestGivenNames,
   gateGivenSource,
   suggestGivenLookup,
} from "../service/given";
import { errMessage, ignoreDotfiles } from "../utils";
import { RpcWaitAccountant } from "./rpc_wait_accountant";
import {
   setEligibilityRefusalSink,
   type EligibilityRefusalReason,
} from "../materialization_metrics";
import { logger } from "../logger";
import {
   tryCompileSynthesizedPreaggregation,
   type SynthesizedPreaggregation,
} from "../service/preaggregation_compile";
import { SchemaCache } from "./schema_cache";
import { renderTagTargets } from "../service/render_tag_targets";
import {
   collectModelBuildPlan,
   deriveBuildPlanOutcome,
   emptyBuildPlanParts,
   mergeBuildPlanParts,
   resolveConnectionDigests,
   resolvePackageConnections,
   type BuildPlanParts,
   type WirePackageMaterialization,
} from "../service/build_plan";
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
   SerializedModel,
   SerializedNotebookCell,
   WorkerBuildPlan,
   WorkerLogEntry,
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

// Per-load phase timing: brackets the wall-clock spent awaiting proxied schema
// fetches so `loadPackage` can separate connection I/O from compile CPU. The
// pool runs one load per worker at a time, so a single module-level instance
// is safe; it's still keyed by jobId to shrug off a straggler fetch from a
// prior, reused-worker load. See {@link RpcWaitAccountant}.
const schemaWait = new RpcWaitAccountant();

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

/**
 * Schemas this worker has fetched through the main thread, kept across loads
 * within the pool's PACKAGE_LOAD_SCHEMA_CACHE_BYTES of serialized schema (see
 * getPackageLoadSchemaCacheBytes). A 50-model package's 37 tables come to
 * 61 KB, the widest (108 columns) to 6.8 KB, so the default holds a few
 * thousand typical tables per worker. Heap use is a few times the serialized
 * size. The pool always sends the budget; without one, nothing is cached.
 */
const schemaCache = new SchemaCache<TableSourceDef | SQLSourceDef>(
   (workerData as { schemaCacheBytes?: number } | null)?.schemaCacheBytes ?? 0,
);

/** What a job collects while it runs in this worker (see runJob). */
interface JobContext {
   logs: WorkerLogEntry[];
   refusals: Partial<Record<EligibilityRefusalReason, number>>;
}

/**
 * The job whose code is running, carried across its awaits. Scoping per job
 * keeps what one job logs or refuses out of another's, however the jobs on
 * this worker interleave.
 */
const jobContext = new AsyncLocalStorage<JobContext>();

setEligibilityRefusalSink(() => jobContext.getStore()?.refusals);

// The logger here writes only to this thread's stdout: OpenTelemetry's log
// export is set up on the main thread alone. A job's log calls are captured
// and sent back with its result for the main thread to log (replayWorkerLogs
// in the pool); outside a job they write as before.
for (const level of ["error", "warn", "info", "debug"] as const) {
   const write = logger[level].bind(logger) as (...args: unknown[]) => unknown;
   (logger as unknown as Record<string, unknown>)[level] = (
      message: unknown,
      ...meta: unknown[]
   ) => {
      const job = jobContext.getStore();
      if (!job) return write(message, ...meta);
      job.logs.push({
         level,
         message: String(message),
         meta: cloneableLogMeta(meta[0]),
      });
      return logger;
   };
}

/** `meta` as plain data, so it survives the message to the main thread. */
function cloneableLogMeta(meta: unknown): Record<string, unknown> | undefined {
   if (meta === undefined || meta === null) return undefined;
   try {
      const plain = JSON.parse(JSON.stringify(meta)) as unknown;
      return typeof plain === "object" && plain !== null
         ? (plain as Record<string, unknown>)
         : { meta: plain };
   } catch {
      return { meta: String(meta) };
   }
}

class ProxyConnection {
   public readonly name: string;
   public readonly dialectName: string;
   private readonly digest: string;
   private readonly jobId: string;
   /** Prefix of this connection's schema-cache keys. */
   private readonly cacheScope: string;

   constructor(
      metadata: ConnectionMetadata,
      jobId: string,
      environmentScope: string,
   ) {
      this.name = metadata.name;
      this.dialectName = metadata.dialectName;
      this.digest = metadata.digest;
      this.jobId = jobId;
      // The environment is part of the key, not only the digest: a digest can
      // be a configured fingerprint, and two environments' connections must
      // never share what one of them can see. The generation retires the
      // entries when the main thread replaces the connection.
      this.cacheScope = [
         environmentScope,
         this.name,
         this.digest,
         metadata.generation,
      ].join("\0");
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
      // A refresh is the main thread's to decide: its connection's own cache
      // knows when it fetched each schema, and this one does not.
      if (options.refreshTimestamp !== undefined) {
         return this.fetchSchemaForTablesFromMain(tables, options);
      }
      const cached: Record<string, TableSourceDef> = {};
      const joined: [string, string, Promise<unknown>][] = [];
      const missing: Record<string, string> = {};
      for (const [key, tablePath] of Object.entries(tables)) {
         const cacheKey = this.tableCacheKey(tablePath);
         const hit = schemaCache.get(cacheKey);
         const pending =
            hit === undefined ? schemaCache.joinInFlight(cacheKey) : undefined;
         if (hit) {
            cached[key] = hit as TableSourceDef;
         } else if (pending) {
            joined.push([key, tablePath, pending]);
         } else {
            missing[key] = tablePath;
         }
      }
      let fetched: {
         schemas: Record<string, TableSourceDef>;
         errors: Record<string, string>;
      } = { schemas: {}, errors: {} };
      if (Object.keys(missing).length > 0) {
         const request = this.fetchSchemaForTablesFromMain(missing, options);
         for (const [key, tablePath] of Object.entries(missing)) {
            schemaCache.trackInFlight(
               this.tableCacheKey(tablePath),
               request.then((r) => r.schemas[key]),
            );
         }
         fetched = await request;
         this.cacheTableSchemas(missing, fetched.schemas);
      }
      // A table another request was already fetching. When that fetch did not
      // produce it, ask for it alone, so the error is this table's own.
      const unanswered: Record<string, string> = {};
      for (const [key, tablePath, pending] of joined) {
         const schema = await pending;
         if (schema) {
            schemaCache.noteJoinedHit();
            cached[key] = schema as TableSourceDef;
         } else {
            unanswered[key] = tablePath;
         }
      }
      if (Object.keys(unanswered).length > 0) {
         const retried = await this.fetchSchemaForTablesFromMain(
            unanswered,
            options,
         );
         this.cacheTableSchemas(unanswered, retried.schemas);
         Object.assign(fetched.schemas, retried.schemas);
         Object.assign(fetched.errors, retried.errors);
      }
      return {
         schemas: { ...cached, ...fetched.schemas },
         errors: fetched.errors,
      };
   }

   private cacheTableSchemas(
      requested: Record<string, string>,
      schemas: Record<string, TableSourceDef>,
   ): void {
      for (const [key, schema] of Object.entries(schemas)) {
         const tablePath = requested[key];
         if (tablePath !== undefined) {
            schemaCache.set(this.tableCacheKey(tablePath), schema);
         }
      }
   }

   private tableCacheKey(tablePath: string): string {
      return `${this.cacheScope}\0table\0${tablePath}`;
   }

   private async fetchSchemaForTablesFromMain(
      tables: Record<string, string>,
      options: FetchSchemaOptions,
   ): Promise<{
      schemas: Record<string, TableSourceDef>;
      errors: Record<string, string>;
   }> {
      schemaWait.noteStart(this.jobId);
      try {
         const response = await callMain<SchemaForTablesResponse>(
            (requestId) => {
               const req: SchemaForTablesRequest = {
                  type: "schema-for-tables",
                  requestId,
                  jobId: this.jobId,
                  connectionName: this.name,
                  tables,
                  options: serializeFetchOptions(options),
               };
               port.postMessage(req);
            },
         );
         return { schemas: response.schemas, errors: response.errors };
      } finally {
         schemaWait.noteSettle(this.jobId);
      }
   }

   async fetchSchemaForSQLStruct(
      sentence: SQLSourceRequest,
      options: FetchSchemaOptions,
   ): Promise<
      | { structDef: SQLSourceDef; error?: undefined }
      | { error: string; structDef?: undefined }
   > {
      const cacheKey = `${this.cacheScope}\0sql\0${sentence.connection}\0${sentence.selectStr}`;
      // See fetchSchemaForTables: a refresh is the main thread's to decide.
      if (options.refreshTimestamp !== undefined) {
         return this.fetchSchemaForSQLStructFromMain(sentence, options);
      }
      const hit = schemaCache.get(cacheKey);
      if (hit) return { structDef: hit as SQLSourceDef };
      const pending = await schemaCache.joinInFlight(cacheKey);
      if (pending) {
         schemaCache.noteJoinedHit();
         return { structDef: pending as SQLSourceDef };
      }
      const request = this.fetchSchemaForSQLStructFromMain(sentence, options);
      schemaCache.trackInFlight(
         cacheKey,
         request.then((r) => r.structDef),
      );
      const result = await request;
      if (result.structDef !== undefined) {
         schemaCache.set(cacheKey, result.structDef);
      }
      return result;
   }

   private async fetchSchemaForSQLStructFromMain(
      sentence: SQLSourceRequest,
      options: FetchSchemaOptions,
   ): Promise<
      | { structDef: SQLSourceDef; error?: undefined }
      | { error: string; structDef?: undefined }
   > {
      schemaWait.noteStart(this.jobId);
      let response: SchemaForSqlResponse;
      try {
         response = await callMain<SchemaForSqlResponse>((requestId) => {
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
      } finally {
         schemaWait.noteSettle(this.jobId);
      }
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
} {
   const out: {
      refreshTimestamp?: number;
   } = {};
   if (options.refreshTimestamp !== undefined) {
      out.refreshTimestamp = options.refreshTimestamp;
   }
   return out;
}

// ──────────────────────────────────────────────────────────────────────
// URLReader: file:// → fs; everything else proxies to main thread
// ──────────────────────────────────────────────────────────────────────

function makeWorkerUrlReader(job: LoadPackageRequest): {
   readURL: (url: URL) => Promise<string>;
} {
   return {
      readURL: async (url: URL): Promise<string> => {
         if (url.protocol === "file:") {
            const filePath = fileURLToPath(url);
            if (
               job.replacement &&
               path.resolve(filePath) ===
                  path.resolve(job.packagePath, job.replacement.modelPath)
            ) {
               return job.replacement.source;
            }
            return fs.promises.readFile(filePath, "utf8");
         }
         const response = await callMain<ReadUrlResponse>((requestId) => {
            const req: ReadUrlRequest = {
               type: "read-url",
               requestId,
               jobId: job.requestId,
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
                     job.environmentName,
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

/**
 * Parse publisher.json.
 *
 * Takes `modelPaths` because the discovery surface has a second source that is
 * a fact about the tree rather than about the manifest text: a package whose
 * root holds an `index.malloy` and declares no `explores` takes its surface
 * from that file. See {@link resolveExplores}.
 */
/** The install location the server recorded for the package, if any. */
async function readInstallRecord(
   packagePath: string,
): Promise<string | undefined> {
   try {
      const raw = await fs.promises.readFile(
         installRecordPath(
            path.dirname(packagePath),
            path.basename(packagePath),
         ),
         "utf8",
      );
      const record: unknown = JSON.parse(raw);
      const location =
         typeof record === "object" && record !== null
            ? (record as { location?: unknown }).location
            : undefined;
      return typeof location === "string" && location !== ""
         ? location
         : undefined;
   } catch {
      return undefined;
   }
}

async function readPackageMetadata(
   packagePath: string,
   modelPaths: readonly string[],
): Promise<{
   name?: string;
   description?: string;
   location?: string;
   explores?: string[];
   queryableSources?: "declared" | "all";
   manifestLocation?: string | null;
   materialization?: PackageMaterializationConfig | null;
   scope?: PackageScope;
   manifestWarnings?: string[];
   retrieval?: PackageRetrievalSettings;
}> {
   const manifestPath = path.join(packagePath, PACKAGE_MANIFEST_NAME);
   const contents = await fs.promises.readFile(manifestPath, "utf8");
   let parsed: {
      name?: string;
      description?: string;
      location?: unknown;
      explores?: string[];
      queryableSources?: unknown;
      manifestLocation?: unknown;
      materialization?: unknown;
      scope?: unknown;
      queryMetadata?: unknown;
      retrieval?: unknown;
   };
   try {
      parsed = JSON.parse(contents);
   } catch (error) {
      // A syntax error is the author's to fix like any other bad manifest, so
      // it is a PackageManifestError (424), not a bare SyntaxError the pool
      // would report as a worker outage.
      throw new PackageManifestError(
         `Invalid ${PACKAGE_MANIFEST_NAME}: it is not valid JSON ` +
            `(${error instanceof Error ? error.message : String(error)}). ` +
            `The package is not served until the file parses. Fix: make it ` +
            `valid JSON; a trailing comma or an unquoted key is the usual cause.`,
      );
   }
   if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
      throw new PackageManifestError(
         `Invalid ${PACKAGE_MANIFEST_NAME}: expected a JSON object, got ` +
            `${JSON.stringify(parsed)}. Fix: { "name": "my-package" }.`,
      );
   }
   // Scope has two homes (canonical `materialization.scope`, deprecated root);
   // an invalid value or a conflict between the two throws and fails the load,
   // and the deprecation rides back as a warning.
   const scope = resolvePackageScope(parsed.scope, parsed.materialization);
   // Query metadata has two homes as well, migrating the other way (canonical
   // root, deprecated `materialization.queryMetadata`). A conflict warns rather
   // than throws — see resolvePackageQueryMetadata.
   const queryMetadata = resolvePackageQueryMetadata(
      parsed.queryMetadata,
      parsed.materialization,
   );
   // The discovery surface has two sources as well: an explicit `explores`,
   // and the `index.malloy` convention, which fills in only where the manifest
   // is silent. The result is a plain path list either way -- nothing
   // downstream can tell which source produced it, and nothing should.
   const explores = resolveExplores({
      declaredExplores: parsed.explores,
      declaredQueryableSources: parsed.queryableSources,
      modelPaths,
   });
   const manifestWarnings = [
      ...scope.warnings,
      ...queryMetadata.warnings,
      ...explores.warnings,
      // Report what the WINNING home could not keep. Reading the envelope alone
      // would say nothing about a malformed property declared at the root, which
      // is the home authors are being moved to.
      ...queryMetadataParseWarnings(
         queryMetadata.queryMetadata,
         queryMetadata.home,
      ),
   ];
   return {
      name: parsed.name,
      description: parsed.description,
      // Where the package was installed from, from the server's own record
      // outside the package directory (see Environment.writePackageManifest).
      // Nothing inside the tree is read as one, neither a `location` in
      // publisher.json nor a record file shipped with the content: a reload
      // re-fetches from this value, so only the server may set it.
      location: await readInstallRecord(packagePath),
      explores: explores.explores,
      // Default + invalid fall back to "declared" (fail-safe: queryable ==
      // discoverable). Only an explicit "all" opts out of the query boundary.
      queryableSources: parsed.queryableSources === "all" ? "all" : "declared",
      // URI (gs://, s3://, file://, or local path) of the control-plane-computed
      // build manifest. The main thread fetches + binds it after load.
      manifestLocation:
         typeof parsed.manifestLocation === "string"
            ? parsed.manifestLocation
            : null,
      // Package-level Malloy Persistence policy; surfaced to the control plane,
      // which owns scheduling. `schedule`/`freshness` are for the control plane;
      // `queryMetadata` is the publisher's own package-level layer.
      materialization: materializationWithQueryMetadata(
         parsePackageMaterialization(parsed.materialization),
         queryMetadata.queryMetadata,
      ),
      // Package-level persist scope mode; defaults to "package".
      scope: scope.scope,
      manifestWarnings:
         manifestWarnings.length > 0 ? manifestWarnings : undefined,
      // How this package is searched and indexed. Validated here so a bad key
      // or an unreadable prompt file stops the load with a message naming it,
      // and read here so a prompt edit takes effect on reload.
      retrieval: await readPackageRetrieval(packagePath, parsed.retrieval),
   };
}

/**
 * Every file in the package, package-relative, sorted.
 *
 * Sorted because `recursive-readdir` pushes each path from inside its
 * `fs.stat` callback, so the order is libuv completion order — not readdir
 * order, and not stable between two servers on identical bytes or between two
 * reloads of one server. This order becomes `Package.models` insertion order
 * and therefore `listModels()`, so anything downstream keyed on "first model
 * wins" was deciding by a race. Nothing should be keyed that way, but a stable
 * listing costs one line and removes the class.
 */
async function listPackageFiles(packagePath: string): Promise<string[]> {
   const files = await recursive(packagePath, [ignoreDotfiles]);
   return (
      files
         .map((full: string) =>
            path.relative(packagePath, full).replace(/\\/g, "/"),
         )
         // Codepoint order: localeCompare with no locale follows the runtime's
         // collation (LANG/LC_ALL), so it does not deliver the cross-server
         // stability the comment above promises.
         .sort((a, b) => (a < b ? -1 : a > b ? 1 : 0))
   );
}

function filterModelPaths(allRelative: string[]): string[] {
   return allRelative.filter(
      (p) => p.endsWith(MODEL_FILE_SUFFIX) || p.endsWith(NOTEBOOK_FILE_SUFFIX),
   );
}

// `resolveExplores` normalizes each entry through `normalizeModelPath`
// (shared, from ../constants) at parse time, so the keys stored in
// Package.models are already normalized, and the API publish/update path
// normalizes its `explores` input through the same helper — so on-disk and
// API-written manifests share one representation.

// explores validation is intentionally NOT done here. The worker is the
// shared load path (startup, reload, AND publish), but the policy differs by
// context — strict-reject at publish, warn-and-fail-safe at load — so it lives
// on the main thread (see Package.getInvalidExplores / loadViaWorker and the
// publish path in package.controller). An unmatched entry simply lists nothing
// (listModels filters by set membership), which is the fail-safe outcome.

// ──────────────────────────────────────────────────────────────────────
// Model compile (mirrors service/model.ts but produces SerializedModel)
// ──────────────────────────────────────────────────────────────────────

interface ApiSourceWire {
   name: string;
   annotations?: string[];
   views?: { name: string; annotations?: string[] }[];
   filters?: unknown[];
   givens?: unknown[];
   authorize?: string[];
   accessFilter?: string[];
}
interface ApiQueryWire {
   name: string;
   sourceName?: string;
   annotations?: string[];
}
type ApiGivenWire = MalloyGivenApi;

// Source / query introspection is shared with the in-process path; see
// service/source_extraction.ts. The worker has no logger, so a filter parse
// failure is swallowed silently (no `onParseError` callback) — matching the
// prior worker-local behavior.
function extractSources(
   modelDef: ModelDef,
   givens: ApiGivenWire[] | undefined,
): {
   sources: ApiSourceWire[];
   filterMap: Map<string, FilterDefinition[]>;
   authorizeMap: AuthorizeMap;
   misplacedAuthorize: MisplacedAuthorizeAnnotation[];
   authorizeOwnNotes: AuthorizeOwnNotesMap;
   attributedAuthorizeOwnNotes: AuthorizeOwnNotesMap;
} {
   const {
      sources,
      filterMap,
      authorizeMap,
      misplacedAuthorize,
      authorizeOwnNotes,
      attributedAuthorizeOwnNotes,
   } = extractSourcesFromModelDef(modelDef, givens);
   return {
      sources: sources as unknown as ApiSourceWire[],
      filterMap,
      authorizeMap,
      misplacedAuthorize,
      authorizeOwnNotes,
      attributedAuthorizeOwnNotes,
   };
}

/**
 * Collects `validateAuthorizeProbes`'s non-fatal `onRowLevelGateUnexpressible`
 * findings as plain strings for the wire
 * (`SerializedModel.authorizeWarnings`) — the worker has no logger (see
 * `extractSources`'s doc above), so these ride to the main thread, which does,
 * to be logged once per compiled model.
 */
function authorizeWarningCollector(): {
   onRowLevelGateUnexpressible: (sourceName: string, detail: string) => void;
   warnings: string[];
} {
   const warnings: string[] = [];
   return {
      warnings,
      onRowLevelGateUnexpressible: (sourceName, detail) => {
         warnings.push(
            `Row-level #(access_filter) gate not expressible at entry point "${sourceName}"; every query against it will be denied: ${detail}`,
         );
      },
   };
}

function extractQueries(modelDef: ModelDef): {
   queries: ApiQueryWire[];
   misplacedAuthorize: MisplacedAuthorizeAnnotation[];
} {
   const { queries, misplacedAuthorize } = extractQueriesFromModelDef(modelDef);
   return {
      queries: queries as unknown as ApiQueryWire[],
      misplacedAuthorize,
   };
}

/**
 * Wrap a URLReader so the text it hands the compiler is kept, keyed by URL.
 *
 * This is the only place the bytes a compile actually consumed exist. Reading
 * the same file again later can disagree with the IR built from it -- see
 * `SerializedModel.modelSourceText`, which is what this feeds -- so the text
 * has to be taken here rather than recovered afterwards.
 *
 * First read wins: the compiler reads a file once per compile, and if it ever
 * read twice the first is the one the coordinates came from.
 */
function captureReadText(inner: { readURL: (url: URL) => Promise<string> }): {
   reader: { readURL: (url: URL) => Promise<string> };
   textFor: (url: URL) => string | undefined;
} {
   const texts = new Map<string, string>();
   return {
      reader: {
         readURL: async (url: URL): Promise<string> => {
            const contents = await inner.readURL(url);
            const key = url.toString();
            if (!texts.has(key)) texts.set(key, contents);
            return contents;
         },
      },
      textFor: (url: URL) => texts.get(url.toString()),
   };
}

function buildRuntimeForModel(
   job: LoadPackageRequest,
   malloyConfig: MalloyConfig,
): {
   runtime: Runtime;
   urlReader: HackyDataStylesAccumulator;
   textFor: (url: URL) => string | undefined;
} {
   const { reader, textFor } = captureReadText(makeWorkerUrlReader(job));
   const urlReader = new HackyDataStylesAccumulator(reader);
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
   return { runtime, urlReader, textFor };
}

/**
 * Per-load state for the work this worker does on the main thread's behalf:
 * the build plan, render-tag results and pre-aggregation companions. Each
 * model's share is done right after its own compile, so no compiled model is
 * held past it.
 */
interface LoadContext {
   /**
    * Past this (a `performance.now()` time) the work is skipped and left to
    * the main thread; see LoadPackageRequest.mainThreadWorkBudgetMs.
    */
   softDeadline: number;
   /** Each `.malloy` model's plan parts, when the request asked for a plan. */
   planParts?: Map<string, BuildPlanParts>;
   /** The plan stopped being collected: past the deadline, or a model failed. */
   planAbandoned: boolean;
   /** Why collecting a model's plan parts threw, failing the plan. */
   planError?: string;
   /** Summed time spent collecting plan parts. */
   planMs: number;
}

async function compileMalloyModel(
   job: LoadPackageRequest,
   malloyConfig: MalloyConfig,
   modelPath: string,
   load: LoadContext,
): Promise<SerializedModel> {
   const compileStart = performance.now();
   const fullPath = path.join(job.packagePath, modelPath);
   // `pathToFileURL` produces a valid URL on every platform; the
   // naïve `file://${fullPath}` template parses host=`D:` on Windows.
   const modelURL = pathToFileURL(fullPath);
   const importBaseURL = new URL(".", modelURL);

   const { runtime, urlReader, textFor } = buildRuntimeForModel(
      job,
      malloyConfig,
   );
   const mm = runtime.loadModel(modelURL, { importBaseURL });
   const compiled = await mm.getModel();
   const modelDef = compiled._modelDef;

   const malloyGivens = Array.from(compiled.givens.values());
   const givens =
      malloyGivens.length > 0
         ? malloyGivens.map((g) => malloyGivenToApi(g as MalloyGiven))
         : undefined;

   // Every source this file can resolve, attributed to nothing but itself.
   // See collectSourceInfos: the old import walk pulled in every source of
   // every imported FILE, including names a selective import never brought
   // into this namespace.
   const sourceInfos = collectSourceInfos(modelDef);

   const {
      sources,
      filterMap,
      authorizeMap,
      misplacedAuthorize,
      authorizeOwnNotes,
      attributedAuthorizeOwnNotes,
   } = extractSources(modelDef, givens);
   // Now that each source's EFFECTIVE gate is known, say which givens each
   // `suggest` needs in its request. In place, so the copies on `sources` see it.
   attachSuggestGivenNames(
      givens,
      suggestGivenLookup(
         modelDef,
         (name) => gateGivenSource(sources, name),
         new Set((givens ?? []).map((given) => given.name)),
      ),
   );
   const queryResult = extractQueries(modelDef);
   const queries = queryResult.queries;
   // See the identical check in `Model.create`.
   assertNoRetiredRouteMarkers(collectRetiredRouteMarkers(modelDef));
   // A `#(authorize)` annotation in a position nothing enforces (a top-level
   // `query:` statement, or a field inside a `source:` rather than the
   // `source:` line itself) fails OPEN — see
   // `assertNoMisplacedAuthorizeAnnotations`'s doc. Checked before
   // `validateAuthorizeProbes` below, same order as `Model.create`.
   assertNoMisplacedAuthorizeAnnotations([
      ...misplacedAuthorize,
      ...queryResult.misplacedAuthorize,
   ]);
   // The string form is refused outright — see `findLegacyStringGates`'s doc.
   // Checked before `validateAuthorizeProbes`, same order as `Model.create`.
   // Presence-based `authorizeOwnNotes` (not the attributed map) — see
   // `extractSourcesFromModelDef`'s doc for why this refusal must not narrow.
   const legacyStringGates = findLegacyStringGates(authorizeOwnNotes);
   legacyStringGates.forEach(() =>
      recordRowLevelGateRejected("legacy_string_gate"),
   );
   assertNoLegacyStringGate(legacyStringGates);
   // The body grammar — see `assertAuthorizeGrammarValid`'s doc, same order
   // as `Model.create`.
   assertAuthorizeGrammarValid(
      modelDef,
      authorizeMap,
      authorizeOwnNotes,
      computeGivenDeclaredTypes(givens),
      (_sourceName, route) => recordAuthorizeAdmitAllGate(route),
      attributedAuthorizeOwnNotes,
   );
   const authorizeWarningCollection = authorizeWarningCollector();
   // Validate both gate routes at compile time (shared with Model.create).
   // Throws on an unknown given / source-field reference or a rejected
   // row-level shape; compileOneModel's catch turns it into this model's
   // compilationError. A gate INHERITED at an entry point that can't express
   // it does not throw — see `validateAuthorizeProbes`'s doc comment for what
   // it validates.
   await validateAuthorizeProbes(mm, {
      authorizeMap,
      authorizeOwnNotes: attributedAuthorizeOwnNotes,
      onRowLevelGateRejected: recordRowLevelGateRejected,
      onRowLevelGateUnexpressible:
         authorizeWarningCollection.onRowLevelGateUnexpressible,
      // G4/W1/W2 for the SOURCE-LINE form, run at EVERY entry point whose
      // probe compiled (see `validateAuthorizeProbes`'s doc on this
      // callback) -- `sourceName` may be an inheritor, not only the
      // DECLARING source. `modelDef.contents[sourceName]` is the same
      // struct the probe was grafted onto, so `refSummary` is already
      // resolved against it either way. The worker has no logger (see this
      // function's doc), so a warning rides the same wire channel as
      // `onRowLevelGateUnexpressible` above.
      onOwnRowLevelConditionCompiled: (sourceName, condition, route) => {
         const struct = modelDef.contents[sourceName];
         if (!struct || !isSourceDef(struct)) return;
         validateSourceLineGateGivenUsage(
            sourceName,
            route,
            struct,
            condition.refSummary as ExpandableRefSummary | undefined,
            condition.e,
            modelDef,
            (cause, detail) => {
               recordRowLevelGateRejected(cause);
               authorizeWarningCollection.warnings.push(
                  `#(${route}) gate warning on "${sourceName}" (${cause}): ${detail}`,
               );
            },
         );
      },
   });

   const compileDurationMs = performance.now() - compileStart;
   const { renderTagResults, preaggregateCompanion } =
      await prepareForMainThread(job, malloyConfig, load, {
         modelPath,
         materializer: mm,
         model: compiled,
         importBaseURL,
         queries,
         sources,
      });

   return {
      modelPath,
      modelType: "model",
      modelDef,
      modelInfo: modelInfoOf(modelDef),
      sourceInfos,
      // `sources`/`queries` ship complete (authorize + filter enforcement and
      // join resolution read the full set); the Model's discovery accessors
      // filter them down to the export closure (`modelDef.exports`) to match
      // `modelInfo`/`sourceInfos`.
      sources,
      queries,
      filterMap: Array.from(filterMap.entries()),
      givens,
      // The bytes this compile read, so a consumer slicing a
      // DocumentLocation out of them is cutting the same snapshot the
      // coordinates were computed against. See SerializedModel.
      modelSourceText: textFor(modelURL),
      dataStyles: urlReader.getHackyAccumulatedDataStyles(),
      compileDurationMs,
      problems: job.collectProblems ? compiled.problems : undefined,
      authorizeWarnings:
         authorizeWarningCollection.warnings.length > 0
            ? authorizeWarningCollection.warnings
            : undefined,
      renderTagResults,
      preaggregateCompanion,
   };
}

/**
 * Do one compiled model's share of what this worker does for the main thread:
 * prepare its render-tag results, compile its pre-aggregation companion
 * against the job's build manifest, and collect its build-plan parts. Past the
 * load's soft deadline none of it is done, and the main thread does it as it
 * did before; that degrades a slow load rather than failing it on the job
 * timeout. Never throws: a failure here fails at most the plan, not the model.
 */
async function prepareForMainThread(
   job: LoadPackageRequest,
   malloyConfig: MalloyConfig,
   load: LoadContext,
   model: {
      modelPath: string;
      materializer: ModelMaterializer;
      model: Awaited<ReturnType<ModelMaterializer["getModel"]>>;
      importBaseURL: URL;
      queries: Parameters<typeof renderTagTargets>[0];
      sources: Parameters<typeof renderTagTargets>[1];
   },
): Promise<{
   renderTagResults?: { label: string; result: Malloy.Result }[];
   preaggregateCompanion?: { modelDef?: unknown };
}> {
   if (performance.now() >= load.softDeadline) {
      load.planAbandoned = true;
      return {};
   }
   const { modelPath, materializer, importBaseURL } = model;

   // Prepared here rather than on the main thread, where compiling every
   // annotated view of a large model blocks the event loop (see
   // SerializedModel.renderTagResults).
   const renderTagResults: { label: string; result: Malloy.Result }[] = [];
   for (const target of renderTagTargets(model.queries, model.sources)) {
      try {
         const prepared = await materializer
            .loadQuery(target.queryString)
            .getPreparedResult();
         // Without its SQL, which the renderer's tag check never reads and
         // which is close to half of each result's size.
         const { sql: _sql, ...result } = prepared.toStableResult();
         renderTagResults.push({
            label: target.label,
            result: result as Malloy.Result,
         });
      } catch {
         // A view or query that fails to prepare is reported by the normal
         // compile path; the render-tag check skips it.
      }
   }

   // The companion the main thread serves rollups through, so compiled
   // against the job's build manifest like the model itself.
   const runtimeWith =
      (buildManifest: unknown) =>
      async (
         overlay: ReadonlyMap<string, string>,
      ): Promise<{ runtime: Runtime; importBaseURL: URL }> => {
         const files = makeWorkerUrlReader(job);
         return {
            runtime: new Runtime({
               urlReader: {
                  readURL: (url: URL) => {
                     const text = overlay.get(url.href);
                     return text !== undefined
                        ? Promise.resolve(text)
                        : files.readURL(url);
                  },
               },
               config: malloyConfig,
               buildManifest:
                  buildManifest !== undefined && buildManifest !== null
                     ? {
                          entries: buildManifest as Record<
                             string,
                             BuildManifestEntry
                          >,
                          strict: false,
                       }
                     : undefined,
            }),
            importBaseURL,
         };
      };
   const companion: SynthesizedPreaggregation | null | undefined =
      job.withPreaggregateCompanions
         ? ((await tryCompileSynthesizedPreaggregation({
              packagePath: job.packagePath,
              modelPath,
              contents: model.model._modelDef.contents as Record<
                 string,
                 unknown
              >,
              getRuntime: runtimeWith(job.buildManifest),
           })) ?? null)
         : undefined;

   if (load.planParts && !load.planAbandoned) {
      if (performance.now() >= load.softDeadline) {
         load.planAbandoned = true;
      } else {
         const start = performance.now();
         try {
            const parts = emptyBuildPlanParts();
            await collectModelBuildPlan(parts, {
               modelPath,
               packagePath: job.packagePath,
               materializer,
               malloyModel: model.model,
               // The plan describes the canonical build, so it reads the
               // companion compiled without a manifest: the one above when
               // the job has none, a fresh one otherwise.
               getRuntime: runtimeWith(undefined),
               synthesized:
                  job.buildManifest === undefined ? companion : undefined,
            });
            load.planParts.set(modelPath, parts);
         } catch (error) {
            load.planError ??= `${modelPath}: ${errMessage(error)}`;
         } finally {
            load.planMs += performance.now() - start;
         }
      }
   }

   return {
      renderTagResults,
      preaggregateCompanion:
         companion === undefined
            ? undefined
            : companion
              ? { modelDef: companion.model._modelDef }
              : {},
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

   const { runtime, urlReader } = buildRuntimeForModel(job, malloyConfig);
   const authorizeWarningCollection = authorizeWarningCollector();

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

      // Sources this cell added, imports included: the cell's namespace minus
      // what earlier cells already surfaced. `collectSourceInfos` reads the
      // accumulated `contents`, so an `import { … }` contributes exactly the
      // names it selected and re-loading the imported file is unnecessary.
      const currentInfo = modelInfoOf(currentModelDef);
      const newSources = collectSourceInfos(currentModelDef).filter(
         (s) => !(s.name in oldSources),
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
   let finalProblems: unknown[] | undefined;
   if (mm) {
      const compiled = await mm.getModel();
      finalProblems = compiled.problems;
      finalModelDef = compiled._modelDef;
      const malloyGivens = Array.from(compiled.givens.values());
      finalGivens =
         malloyGivens.length > 0
            ? malloyGivens.map((g) => malloyGivenToApi(g as MalloyGiven))
            : undefined;
      finalSourceInfos = collectSourceInfos(finalModelDef);
      const extracted = extractSources(finalModelDef, finalGivens);
      finalSources = extracted.sources;
      // See the identical step in `compileMalloyModel` above.
      attachSuggestGivenNames(
         finalGivens,
         suggestGivenLookup(
            finalModelDef,
            (name) => gateGivenSource(extracted.sources, name),
            new Set((finalGivens ?? []).map((given) => given.name)),
         ),
      );
      finalFilterMap = extracted.filterMap;
      const finalQueryResult = extractQueries(finalModelDef);
      finalQueries = finalQueryResult.queries;
      // See the identical check in `compileMalloyModel` above.
      assertNoRetiredRouteMarkers(collectRetiredRouteMarkers(finalModelDef));
      // See the identical check in `compileMalloyModel` above.
      assertNoMisplacedAuthorizeAnnotations([
         ...extracted.misplacedAuthorize,
         ...finalQueryResult.misplacedAuthorize,
      ]);
      // See the identical check in `compileMalloyModel` above.
      const finalLegacyStringGates = findLegacyStringGates(
         extracted.authorizeOwnNotes,
      );
      finalLegacyStringGates.forEach(() =>
         recordRowLevelGateRejected("legacy_string_gate"),
      );
      assertNoLegacyStringGate(finalLegacyStringGates);
      // See the identical check in `compileMalloyModel` above.
      assertAuthorizeGrammarValid(
         finalModelDef,
         extracted.authorizeMap,
         extracted.authorizeOwnNotes,
         computeGivenDeclaredTypes(finalGivens),
         (_sourceName, route) => recordAuthorizeAdmitAllGate(route),
         extracted.attributedAuthorizeOwnNotes,
      );
      // Validate #(authorize) at compile time (shared with Model.create). See
      // `validateAuthorizeProbes`'s doc comment for what it validates.
      //
      // `const` (not the outer `let finalModelDef`) so the narrowed
      // non-undefined type survives into the closure below.
      const finalCompiledModelDef: ModelDef = finalModelDef;
      await validateAuthorizeProbes(mm, {
         authorizeMap: extracted.authorizeMap,
         authorizeOwnNotes: extracted.attributedAuthorizeOwnNotes,
         onRowLevelGateRejected: recordRowLevelGateRejected,
         onRowLevelGateUnexpressible:
            authorizeWarningCollection.onRowLevelGateUnexpressible,
         // See the identical check in `compileMalloyModel` above.
         onOwnRowLevelConditionCompiled: (sourceName, condition, route) => {
            const struct = finalCompiledModelDef.contents[sourceName];
            if (!struct || !isSourceDef(struct)) return;
            validateSourceLineGateGivenUsage(
               sourceName,
               route,
               struct,
               condition.refSummary as ExpandableRefSummary | undefined,
               condition.e,
               finalCompiledModelDef,
               (cause, detail) => {
                  recordRowLevelGateRejected(cause);
                  authorizeWarningCollection.warnings.push(
                     `#(${route}) gate warning on "${sourceName}" (${cause}): ${detail}`,
                  );
               },
            );
         },
      });
   }

   return {
      modelPath,
      modelType: "notebook",
      modelDef: finalModelDef,
      modelInfo: finalModelDef ? modelInfoOf(finalModelDef) : undefined,
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
      problems: job.collectProblems ? finalProblems : undefined,
      authorizeWarnings:
         authorizeWarningCollection.warnings.length > 0
            ? authorizeWarningCollection.warnings
            : undefined,
   };
}

async function compileOneModel(
   job: LoadPackageRequest,
   malloyConfig: MalloyConfig,
   modelPath: string,
   load: LoadContext,
): Promise<SerializedModel> {
   try {
      if (modelPath.endsWith(MODEL_FILE_SUFFIX)) {
         return await compileMalloyModel(job, malloyConfig, modelPath, load);
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
      // A model that fails to compile fails a load that wants a plan, so
      // the remaining models' plan parts would be wasted.
      load.planAbandoned = true;
      const modelType: SerializedModel["modelType"] = modelPath.endsWith(
         NOTEBOOK_FILE_SUFFIX,
      )
         ? "notebook"
         : "model";
      return {
         modelPath,
         modelType,
         // Classified here, before serializing: the check reads the stack, and
         // a plain Error crosses as a bare Error that the main thread answers
         // as an outage. As a MalloyError it crosses as a compile problem,
         // the same as Model.create's in-process path.
         compilationError: serializeError(
            translatorMalloyError(error) ?? error,
         ),
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

   // The file listing runs BEFORE the manifest read: the discovery surface can
   // be defaulted from a root index.malloy, so parsing the manifest needs to
   // know what is on disk. Both stay in the setup region excluded from the
   // compile timing below.
   const allFiles = await listPackageFiles(job.packagePath);
   const modelPaths = filterModelPaths(allFiles);

   // Resolved against the ON-DISK tree, before the replacement fixup below. A
   // replacement can introduce a modelPath that is not on disk, and a /compile
   // preview proposing an index.malloy must not re-curate the whole package
   // for the span of one request.
   const packageMetadata = await readPackageMetadata(
      job.packagePath,
      modelPaths,
   );
   const malloyConfig = buildWorkerMalloyConfig(job);

   const replacementMatchedExisting = job.replacement
      ? modelPaths.includes(job.replacement.modelPath)
      : undefined;
   if (job.replacement && !replacementMatchedExisting) {
      modelPaths.push(job.replacement.modelPath);
   }

   // Bracket the compile region: only work from here on is compilation +
   // proxied schema fetches. The setup above (manifest read + file listing)
   // is excluded so it can't inflate the compile figure — it stays in the
   // derivable remainder (loadDuration - compile - schemaFetch). Begin the
   // schema-fetch accounting HERE, at the region boundary, not at load start:
   // that keeps `compileDurationMs = compileRegion - schemaFetchWait` exact
   // regardless of whether any fetch ever happens during setup (none do
   // today, but this stops that assumption from silently mattering).
   const compileRegionStart = performance.now();
   const schemaCacheHitsAtStart = schemaCache.hits;
   schemaWait.begin(job.requestId);
   const load: LoadContext = {
      softDeadline: loadStart + job.mainThreadWorkBudgetMs,
      planParts: job.computeBuildPlan
         ? new Map<string, BuildPlanParts>()
         : undefined,
      planAbandoned: false,
      planMs: 0,
   };
   const models = await Promise.all(
      modelPaths.map((modelPath) =>
         compileOneModel(job, malloyConfig, modelPath, load),
      ),
   );

   const compileEnd = performance.now();
   const schemaFetchDurationMs = schemaWait.waitMs;
   const schemaFetchCount = schemaWait.fetches;
   const schemaCacheHits = schemaCache.hits - schemaCacheHitsAtStart;
   const buildPlan = await finishWorkerBuildPlan(
      malloyConfig,
      modelPaths,
      load,
      packageMetadata.materialization ?? null,
   );
   const loadEnd = performance.now();
   return {
      type: "load-package-result",
      requestId: job.requestId,
      packageMetadata,
      models,
      replacementMatchedExisting,
      loadDurationMs: loadEnd - loadStart,
      timings: {
         // Compile-region wall minus the schema-fetch wait it contains — a
         // conservative ceiling on compile CPU (clamped: rounding can nudge
         // it slightly negative).
         compileDurationMs: Math.max(
            0,
            compileEnd - compileRegionStart - schemaFetchDurationMs,
         ),
         schemaFetchDurationMs,
         schemaFetchCount,
         schemaCacheHits,
      },
      buildPlan,
   };
}

/**
 * Merge the models' plan parts, in model order, into the package's persist
 * build plan: the derivation `computePackageBuildPlan` runs on the main thread
 * after compiling the package a second time. Undefined when no plan was asked
 * for or collecting it was abandoned, leaving the main thread to derive it as
 * before.
 */
async function finishWorkerBuildPlan(
   malloyConfig: MalloyConfig,
   modelPaths: string[],
   load: LoadContext,
   materializationConfig: WirePackageMaterialization | null,
): Promise<WorkerBuildPlan | undefined> {
   if (!load.planParts || load.planAbandoned) return undefined;
   const start = performance.now();
   const refusals = (): Partial<Record<EligibilityRefusalReason, number>> => ({
      ...(jobContext.getStore()?.refusals ?? {}),
   });
   try {
      if (load.planError) throw new Error(load.planError);
      const parts = emptyBuildPlanParts();
      for (const modelPath of modelPaths) {
         if (!modelPath.endsWith(MODEL_FILE_SUFFIX)) continue;
         const own = load.planParts.get(modelPath);
         if (!own) throw new Error(`Model ${modelPath} did not compile`);
         mergeBuildPlanParts(parts, own);
      }
      const connections = await resolvePackageConnections(
         {
            getMalloyConnection: (name: string) =>
               malloyConfig.connections.lookupConnection(name),
         },
         parts.graphs.map((g) => g.connectionName),
      );
      const digestSkipped: string[] = [];
      const connectionDigests = await resolveConnectionDigests(
         connections,
         parts.graphs,
         (name) => digestSkipped.push(name),
      );
      const outcome = deriveBuildPlanOutcome(
         { ...parts, connectionDigests },
         materializationConfig,
      );
      return {
         ok: true,
         outcome,
         durationMs: load.planMs + performance.now() - start,
         digestSkipped,
         eligibilityRefused: refusals(),
      };
   } catch (error) {
      return {
         ok: false,
         error: errMessage(error),
         durationMs: load.planMs + performance.now() - start,
         eligibilityRefused: refusals(),
      };
   }
}

// ──────────────────────────────────────────────────────────────────────
// Error serialization
// ──────────────────────────────────────────────────────────────────────

// serializeError/deserializeError: ./error_wire

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
   const context: JobContext = { logs: [], refusals: {} };
   try {
      const result = await jobContext.run(context, () => loadPackage(job));
      port.postMessage({ ...result, logs: context.logs });
   } catch (error) {
      const errMsg: LoadPackageError = {
         type: "load-package-error",
         requestId: job.requestId,
         error: serializeError(error),
         logs: context.logs,
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
