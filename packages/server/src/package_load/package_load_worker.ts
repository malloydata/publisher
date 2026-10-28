// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Package-load worker entry point (see protocol.ts for the boundary).
 *
 * Owns no native connection state: every connection lookup, schema fetch and
 * non-file URL read is proxied to the main thread over RPC, so only the Malloy
 * compiler runs here. DuckDB in particular cannot be loaded into a second
 * isolate of one process (Bun crash 0x20131).
 *
 * Bundled separately by `build.ts` as `dist/package_load_worker.mjs` so
 * `new Worker(...)` does not load the whole server module graph.
 */
import {
   contextOverlay,
   isSourceDef,
   type BuildManifestEntry,
   type CacheManager,
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
import { MalloySQLStatementType } from "@malloydata/malloy-sql";
import { parseMalloynb } from "../service/deprecated/malloynb";
import {
   declaredDeprecatedManifestKeys,
   type DeprecatedManifestKey,
} from "../service/deprecated/manifest_keys";
import * as crypto from "crypto";
import * as fs from "fs";
import * as path from "path";
import { AsyncLocalStorage } from "node:async_hooks";
import { parentPort, threadId, workerData } from "node:worker_threads";
import { fileURLToPath, pathToFileURL } from "url";

import {
   MODEL_FILE_SUFFIX,
   NOTEBOOK_FILE_SUFFIX,
   installRecordPath,
   normalizeModelPath,
   PACKAGE_MANIFEST_NAME,
} from "../constants";
import { isReportEligiblePath, readReportTagOfText } from "../service/report";
import {
   type GateFindingCounts,
   recordAuthorizeAdmitAllGate,
   recordRowLevelGateRejected,
   setGateFindingSink,
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
import { boundedManifestValue } from "../service/package_revision";
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
import { errMessage } from "../utils";
import { RpcWaitAccountant } from "./rpc_wait_accountant";
import { acceptBundledExperimentalDialects } from "../service/experimental_dialects";
import {
   setEligibilityRefusalSink,
   type EligibilityRefusalReason,
} from "../materialization_metrics";
import { logger } from "../logger";
import {
   tryCompileSynthesizedPreaggregation,
   type SynthesizedPreaggregation,
   type SynthesizedRuntimeFactory,
} from "../service/preaggregation_compile";
import { type InFlightAnswer, SchemaCache } from "./schema_cache";
import { PackageCompileCache } from "./compile_cache";
import {
   dataFilesDigest,
   filterModelPaths,
   listPackageFiles,
} from "./package_files";
import { plainLogEntry } from "./worker_log";
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
   WorkerLogMessage,
} from "./protocol";

if (!parentPort) {
   throw new Error(
      "package_load_worker.ts must be loaded inside a worker_threads Worker",
   );
}

// This thread has its own dialect registry; see experimental_dialects.ts.
acceptBundledExperimentalDialects();

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

// Module-level is safe because the pool runs one load per worker at a time.
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

type CachedSchema = TableSourceDef | SQLSourceDef;

/**
 * Schemas fetched through the main thread, kept across loads within
 * PACKAGE_LOAD_SCHEMA_CACHE_BYTES of serialized schema (heap use is a few
 * times that). Without a budget from the pool, nothing is cached.
 */
const schemaCache = new SchemaCache<CachedSchema>(
   (workerData as { schemaCacheBytes?: number } | null)?.schemaCacheBytes ?? 0,
);

/** Each package's compile cache, kept across its loads until it is unloaded. */
const compileCaches = new Map<string, PackageCompileCache>();

function compileCacheKey(environmentName: string, packageName: string): string {
   return `${environmentName}\0${packageName}`;
}

function compileCacheFor(job: LoadPackageRequest): PackageCompileCache {
   const key = compileCacheKey(job.environmentName, job.packageName);
   let cache = compileCaches.get(key);
   if (!cache) {
      cache = new PackageCompileCache(
         `${job.environmentName}/${job.packageName}`,
      );
      compileCaches.set(key, cache);
   }
   return cache;
}

/** What a job carries while it runs in this worker (see runJob). */
interface JobContext {
   jobId: string;
   packagePath: string;
   refusals: Partial<Record<EligibilityRefusalReason, number>>;
}

/** The running job, carried across awaits so jobs' logs and refusals never mix. */
const jobContext = new AsyncLocalStorage<JobContext>();

setEligibilityRefusalSink(() => jobContext.getStore()?.refusals);

/** The gate findings of the model being compiled, carried across awaits. */
const modelGateFindings = new AsyncLocalStorage<GateFindingCounts>();

setGateFindingSink(() => modelGateFindings.getStore());

// OpenTelemetry log export exists only on the main thread, so a job's log
// entries are intercepted at `write` (where every call arrives as winston's
// built entry) and forwarded (see the pool's handleWorkerLog). Outside a job,
// entries write to this thread's stdout.
const writeEntry = logger.write.bind(logger) as (entry: unknown) => boolean;
(logger as unknown as { write: (entry: unknown) => boolean }).write = (
   entry: unknown,
) => {
   const job = jobContext.getStore();
   if (!job) return writeEntry(entry);
   if (logger.isLevelEnabled(String((entry as { level?: unknown }).level))) {
      const { level, message, meta } = plainLogEntry(entry);
      const msg: WorkerLogMessage = {
         type: "log",
         jobId: job.jobId,
         packagePath: job.packagePath,
         level,
         message,
         meta,
      };
      port.postMessage(msg);
   }
   return true;
};

/** What a refresh fetched for one table. */
type RefreshedTable = { schema?: TableSourceDef; error?: string };

class ProxyConnection {
   public readonly name: string;
   public readonly dialectName: string;
   private readonly digest: string;
   private readonly jobId: string;
   /** Prefix of this connection's schema-cache keys. */
   private readonly cacheScope: string;
   /** Set on a schema refresh: every fetch of the job goes to the warehouse once, and overwrites the cached entry. */
   private readonly refreshSchemaAt: number | undefined;
   private readonly refreshedTables = new Map<
      string,
      Promise<RefreshedTable>
   >();
   private readonly refreshedSql = new Map<
      string,
      ReturnType<ProxyConnection["fetchSchemaForSQLStructFromMain"]>
   >();

   constructor(
      metadata: ConnectionMetadata,
      jobId: string,
      environmentScope: string,
      refreshSchemaAt: number | undefined,
   ) {
      this.refreshSchemaAt = refreshSchemaAt;
      this.name = metadata.name;
      this.dialectName = metadata.dialectName;
      this.digest = metadata.digest;
      this.jobId = jobId;
      // The environment is in the key because a digest can be a configured
      // fingerprint, and two environments must never share what one can see.
      // The generation retires entries when the connection is replaced.
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

   /**
    * A refresh fetches each table once per job: the models that import it
    * join that fetch instead of each asking the warehouse again.
    */
   private async refreshTables(
      tables: Record<string, string>,
      options: FetchSchemaOptions,
   ): Promise<{
      schemas: Record<string, TableSourceDef>;
      errors: Record<string, string>;
   }> {
      const schemas: Record<string, TableSourceDef> = {};
      const errors: Record<string, string> = {};
      const answers: [string, Promise<RefreshedTable>][] = [];
      const missing: Record<string, string> = {};
      for (const [key, tablePath] of Object.entries(tables)) {
         const known = this.refreshedTables.get(tablePath);
         if (known) answers.push([key, known]);
         else missing[key] = tablePath;
      }
      if (Object.keys(missing).length > 0) {
         const request = this.fetchSchemaForTablesFromMain(missing, options);
         for (const [key, tablePath] of Object.entries(missing)) {
            const answer = request.then(
               (r): RefreshedTable =>
                  r.schemas[key]
                     ? { schema: r.schemas[key] }
                     : {
                          error:
                             r.errors[key] ??
                             "No schema returned for the table",
                       },
               (error: unknown): RefreshedTable => ({
                  error: error instanceof Error ? error.message : String(error),
               }),
            );
            this.refreshedTables.set(tablePath, answer);
            answers.push([key, answer]);
         }
         const refreshed = await request;
         this.cacheTableSchemas(missing, refreshed.schemas);
      }
      for (const [key, answer] of answers) {
         const { schema, error } = await answer;
         if (schema) schemas[key] = schema;
         else errors[key] = error ?? "No schema returned for the table";
      }
      return { schemas, errors };
   }

   private withRefresh(options: FetchSchemaOptions): FetchSchemaOptions {
      return options.refreshTimestamp === undefined &&
         this.refreshSchemaAt !== undefined
         ? { ...options, refreshTimestamp: this.refreshSchemaAt }
         : options;
   }

   async fetchSchemaForTables(
      tables: Record<string, string>,
      options: FetchSchemaOptions,
   ): Promise<{
      schemas: Record<string, TableSourceDef>;
      errors: Record<string, string>;
   }> {
      options = this.withRefresh(options);
      // A refresh is the main thread's to decide, since only its cache knows
      // when each schema was fetched. Its answer replaces the entries.
      if (options.refreshTimestamp !== undefined) {
         return this.refreshTables(tables, options);
      }
      const schemas: Record<string, TableSourceDef> = {};
      const errors: Record<string, string> = {};
      const joined: [string, Promise<InFlightAnswer<CachedSchema>>][] = [];
      const missing: Record<string, string> = {};
      for (const [key, tablePath] of Object.entries(tables)) {
         const cacheKey = this.tableCacheKey(tablePath);
         const hit = schemaCache.get(cacheKey);
         const pending =
            hit === undefined ? schemaCache.joinInFlight(cacheKey) : undefined;
         if (hit) {
            schemas[key] = hit as TableSourceDef;
         } else if (pending) {
            joined.push([key, pending]);
         } else {
            missing[key] = tablePath;
         }
      }
      if (Object.keys(missing).length > 0) {
         const request = this.fetchSchemaForTablesFromMain(missing, options);
         for (const [key, tablePath] of Object.entries(missing)) {
            schemaCache.trackInFlight(
               this.tableCacheKey(tablePath),
               request.then((r) => ({
                  value: r.schemas[key],
                  error: r.errors[key],
               })),
            );
         }
         const fetched = await request;
         this.cacheTableSchemas(missing, fetched.schemas);
         Object.assign(schemas, fetched.schemas);
         Object.assign(errors, fetched.errors);
      }
      // A table already being fetched takes that fetch's outcome, error
      // included (errors are per table), rather than asking again.
      for (const [key, pending] of joined) {
         const answer = await pending;
         if (answer.value) {
            schemaCache.noteJoinedHit();
            schemas[key] = answer.value as TableSourceDef;
         } else {
            errors[key] = answer.error ?? "No schema returned for the table";
         }
      }
      return { schemas, errors };
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
      options = this.withRefresh(options);
      const cacheKey = `${this.cacheScope}\0sql\0${sentence.connection}\0${sentence.selectStr}`;
      // See fetchSchemaForTables: a refresh is the main thread's to decide.
      if (options.refreshTimestamp !== undefined) {
         let refreshed = this.refreshedSql.get(cacheKey);
         if (refreshed === undefined) {
            refreshed = this.fetchSchemaForSQLStructFromMain(
               sentence,
               options,
            ).then((answer) => {
               if (answer.structDef !== undefined) {
                  schemaCache.set(cacheKey, answer.structDef);
               }
               return answer;
            });
            this.refreshedSql.set(cacheKey, refreshed);
         }
         return refreshed;
      }
      const hit = schemaCache.get(cacheKey);
      if (hit) return { structDef: hit as SQLSourceDef };
      const pending = schemaCache.joinInFlight(cacheKey);
      if (pending) {
         const answer = await pending;
         if (answer.value) {
            schemaCache.noteJoinedHit();
            return { structDef: answer.value as SQLSourceDef };
         }
         return { error: answer.error ?? "No schema returned for the SQL" };
      }
      const request = this.fetchSchemaForSQLStructFromMain(sentence, options);
      schemaCache.trackInFlight(
         cacheKey,
         request.then((r) => ({ value: r.structDef, error: r.error })),
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

   // Compilation never executes SQL; throw so a misrouted call surfaces loudly.
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
 * Build the package's MalloyConfig in the worker. Every connection name,
 * `"duckdb"` included, resolves to a {@link ProxyConnection}. Concurrent
 * lookups of one name share one metadata RPC, since a compile fans out many
 * lookups before any resolves.
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
                     job.refreshSchemaAt,
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
// Filesystem helpers
// ──────────────────────────────────────────────────────────────────────

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

/**
 * Which declared `explores` entries are reports, read from each file's text
 * before the compile because the manifest is resolved first. Never throws.
 */
async function declaredReportPaths(
   packagePath: string,
   declared: unknown,
   modelPaths: readonly string[],
): Promise<Set<string>> {
   const reports = new Set<string>();
   if (!Array.isArray(declared)) return reports;
   const known = new Set(modelPaths);
   for (const entry of declared) {
      if (typeof entry !== "string") continue;
      const modelPath = normalizeModelPath(entry);
      if (!known.has(modelPath) || !isReportEligiblePath(modelPath)) continue;
      try {
         const text = await fs.promises.readFile(
            path.join(packagePath, modelPath),
            "utf8",
         );
         if (readReportTagOfText(text).status === "report")
            reports.add(modelPath);
      } catch {
         // Unreadable: not a report as far as the advice goes.
      }
   }
   return reports;
}

/**
 * Parse publisher.json. Takes `modelPaths` because a root `index.malloy`
 * supplies the discovery surface when `explores` is absent (see
 * {@link resolveExplores}).
 */
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
   deprecatedManifestKeys?: DeprecatedManifestKey[];
   retrieval?: PackageRetrievalSettings;
   agents?: unknown;
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
      agents?: unknown;
   };
   try {
      parsed = JSON.parse(contents);
   } catch (error) {
      // A PackageManifestError (424), not a bare SyntaxError the pool would
      // report as a worker outage.
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
   // an invalid value or a conflict throws.
   const scope = resolvePackageScope(parsed.scope, parsed.materialization);
   // Query metadata migrates the other way (canonical root, deprecated
   // `materialization.queryMetadata`); a conflict only warns.
   const queryMetadata = resolvePackageQueryMetadata(
      parsed.queryMetadata,
      parsed.materialization,
   );
   // An explicit `explores`, or the `index.malloy` convention where the
   // manifest is silent; downstream cannot tell which, and should not.
   const explores = resolveExplores({
      declaredExplores: parsed.explores,
      declaredQueryableSources: parsed.queryableSources,
      modelPaths,
      reportPaths: await declaredReportPaths(
         packagePath,
         parsed.explores,
         modelPaths,
      ),
   });
   const manifestWarnings = [
      ...scope.warnings,
      ...queryMetadata.warnings,
      ...explores.warnings,
      // Warn about what the WINNING home could not keep, which may be the root.
      ...queryMetadataParseWarnings(
         queryMetadata.queryMetadata,
         queryMetadata.home,
      ),
   ];
   return {
      name: parsed.name,
      description: parsed.description,
      // Only from the server's own record outside the package directory (see
      // Environment.writePackageManifest), never from package content: a
      // reload re-fetches from this value, so only the server may set it.
      location: await readInstallRecord(packagePath),
      explores: explores.explores,
      // Fail-safe: anything but an explicit "all" keeps the query boundary.
      queryableSources: parsed.queryableSources === "all" ? "all" : "declared",
      // URI of the control-plane build manifest; the main thread binds it.
      manifestLocation:
         typeof parsed.manifestLocation === "string"
            ? parsed.manifestLocation
            : null,
      // Persistence policy: `schedule`/`freshness` are for the control plane,
      // `queryMetadata` for this server.
      materialization: materializationWithQueryMetadata(
         parsePackageMaterialization(parsed.materialization),
         queryMetadata.queryMetadata,
      ),
      scope: scope.scope,
      manifestWarnings:
         manifestWarnings.length > 0 ? manifestWarnings : undefined,
      deprecatedManifestKeys: declaredDeprecatedManifestKeys(parsed),
      // Validated and read here so a bad key or unreadable prompt file fails
      // the load by name, and a prompt edit takes effect on reload.
      retrieval: await readPackageRetrieval(packagePath, parsed.retrieval),
      agents: boundedManifestValue(parsed.agents),
   };
}

// `explores` is not validated here: the policy differs by context (reject at
// publish, warn at load), so it lives on the main thread
// (Package.getInvalidExplores). An unmatched entry simply lists nothing.

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

// A filter parse failure is not logged here; the main thread logs it when it
// builds the model's filters.
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

/** Collects non-fatal authorize findings for `SerializedModel.authorizeWarnings`. */
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
 * Wrap a URLReader so the text it hands the compiler is kept, keyed by URL:
 * the only copy of the bytes the IR was built from (see
 * `SerializedModel.modelSourceText`). First read wins.
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

/** A Runtime for the job: its connections and its build manifest. */
function jobRuntime(
   job: LoadPackageRequest,
   malloyConfig: MalloyConfig,
   urlReader: { readURL: (url: URL) => Promise<string> },
   cacheManager: CacheManager,
): Runtime {
   return new Runtime({
      urlReader,
      config: malloyConfig,
      cacheManager,
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
}

function buildRuntimeForModel(
   job: LoadPackageRequest,
   malloyConfig: MalloyConfig,
   cacheManager: CacheManager,
): {
   runtime: Runtime;
   urlReader: HackyDataStylesAccumulator;
   textFor: (url: URL) => string | undefined;
} {
   // A cache hit still reads every file of the closure (Malloy re-reads to
   // check its key), so textFor and the data styles see what a miss would.
   const { reader, textFor } = captureReadText(makeWorkerUrlReader(job));
   const urlReader = new HackyDataStylesAccumulator(reader);
   const runtime = jobRuntime(job, malloyConfig, urlReader, cacheManager);
   return { runtime, urlReader, textFor };
}

/**
 * Per-load state for the work done on the main thread's behalf (build plan,
 * render-tag results, companions). Each model's share runs right after its
 * compile, so no compiled model is held past it.
 */
interface LoadContext {
   /**
    * Past this (a `performance.now()` time) the work is left to the main
    * thread; see LoadPackageRequest.mainThreadWorkBudgetMs.
    */
   softDeadline: number;
   /** Each `.malloy` model's plan parts, when the request asked for a plan. */
   planParts?: Map<string, BuildPlanParts>;
   /** Each model's position in the package's model order. */
   modelOrder: Map<string, number>;
   /** A model failed in a load that wants a plan, which fails anyway; stop the work. */
   failed: boolean;
   /**
    * The first model, in model order, whose plan parts could not be
    * collected. Earlier models keep collecting so the reported failure is the
    * one the main thread's model-by-model derivation would report.
    */
   planError?: { order: number; message: string };
   /** When the first model's plan collection started. */
   planStart?: number;
   /** A model reached its plan parts past the soft deadline; the main thread derives the plan. */
   planLeftToMainThread: boolean;
}

/** What a load derives from a compiled model, all of it a function of the ModelDef. */
interface ModelDerivation {
   givens: MalloyGivenApi[] | undefined;
   sourceInfos: ReturnType<typeof collectSourceInfos>;
   sources: ReturnType<typeof extractSources>["sources"];
   filterMap: Array<[string, FilterDefinition[]]>;
   queries: ReturnType<typeof extractQueries>["queries"];
   authorizeWarnings?: string[];
   modelInfo: ReturnType<typeof modelInfoOf>;
   /** The gate findings deriving it counted, replayed when it is reused. */
   gateFindings?: GateFindingCounts;
}

/**
 * Each cached ModelDef's derivation, with a digest of the text it was compiled
 * from. A cache hit returns the identical ModelDef, so the derivation (about
 * as costly as the translation it replaces) is reused rather than redone.
 */
const derivations = new WeakMap<
   ModelDef,
   { textDigest: string; derivation: ModelDerivation }
>();

function textDigest(text: string | undefined): string {
   return crypto
      .createHash("sha1")
      .update(text ?? "")
      .digest("hex");
}

function addGateFindings(
   into: GateFindingCounts | undefined,
   from: GateFindingCounts | undefined,
): void {
   if (!into || !from) return;
   for (const [cause, n] of Object.entries(from.rejected)) {
      const key = cause as keyof GateFindingCounts["rejected"];
      into.rejected[key] = (into.rejected[key] ?? 0) + (n ?? 0);
   }
   for (const [route, n] of Object.entries(from.admitAll)) {
      into.admitAll[route] = (into.admitAll[route] ?? 0) + n;
   }
}

function reusableDerivation(
   modelDef: ModelDef,
   modelSourceText: string | undefined,
): ModelDerivation | undefined {
   const entry = derivations.get(modelDef);
   return entry?.textDigest === textDigest(modelSourceText)
      ? entry.derivation
      : undefined;
}

/**
 * Run the model's checks and extract what the main thread serves. Throws as
 * the checks do; only a derivation that passed is kept for reuse.
 */
async function deriveModel(
   mm: ModelMaterializer,
   compiled: Awaited<ReturnType<ModelMaterializer["getModel"]>>,
   modelSourceText: string | undefined,
): Promise<ModelDerivation> {
   const modelDef = compiled._modelDef;
   const malloyGivens = Array.from(compiled.givens.values());
   const givens =
      malloyGivens.length > 0
         ? malloyGivens.map((g) => malloyGivenToApi(g as MalloyGiven))
         : undefined;

   // Every source this file's namespace can resolve; see collectSourceInfos.
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
   // The checks below run in the same order as `Model.create`. A misplaced
   // `#(authorize)` would fail OPEN, so it is refused outright.
   assertNoMisplacedAuthorizeAnnotations([
      ...misplacedAuthorize,
      ...queryResult.misplacedAuthorize,
   ]);
   // Presence-based `authorizeOwnNotes`, not the attributed map: this refusal
   // must not narrow (see `extractSourcesFromModelDef`).
   const legacyStringGates = findLegacyStringGates(authorizeOwnNotes);
   legacyStringGates.forEach(() =>
      recordRowLevelGateRejected("legacy_string_gate"),
   );
   assertNoLegacyStringGate(legacyStringGates);
   assertAuthorizeGrammarValid(
      modelDef,
      authorizeMap,
      authorizeOwnNotes,
      computeGivenDeclaredTypes(givens),
      (_sourceName, route) => recordAuthorizeAdmitAllGate(route),
      attributedAuthorizeOwnNotes,
   );
   const authorizeWarningCollection = authorizeWarningCollector();
   // Throws on a bad reference or rejected row-level shape, which becomes this
   // model's compilationError; an unexpressible inherited gate only warns.
   await validateAuthorizeProbes(mm, {
      authorizeMap,
      authorizeOwnNotes: attributedAuthorizeOwnNotes,
      onRowLevelGateRejected: recordRowLevelGateRejected,
      onRowLevelGateUnexpressible:
         authorizeWarningCollection.onRowLevelGateUnexpressible,
      // Runs at EVERY entry point whose probe compiled, so `sourceName` may be
      // an inheritor; `modelDef.contents[sourceName]` is the struct the probe
      // was grafted onto. Warnings join the authorize warnings.
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

   const derivation: ModelDerivation = {
      givens,
      sourceInfos,
      sources,
      filterMap: Array.from(filterMap.entries()),
      queries,
      authorizeWarnings:
         authorizeWarningCollection.warnings.length > 0
            ? authorizeWarningCollection.warnings
            : undefined,
      modelInfo: modelInfoOf(modelDef),
      gateFindings: structuredClone(modelGateFindings.getStore()),
   };
   derivations.set(modelDef, {
      textDigest: textDigest(modelSourceText),
      derivation,
   });
   return derivation;
}

async function compileMalloyModel(
   job: LoadPackageRequest,
   malloyConfig: MalloyConfig,
   modelPath: string,
   load: LoadContext,
): Promise<SerializedModel> {
   const compileStart = performance.now();
   const fullPath = path.join(job.packagePath, modelPath);
   // Not a `file://` template, which parses host=`D:` on Windows.
   const modelURL = pathToFileURL(fullPath);
   const importBaseURL = new URL(".", modelURL);

   const modelCache = compileCacheFor(job).forModel(modelURL, {
      translate: job.collectProblems === true,
   });
   const { runtime, urlReader, textFor } = buildRuntimeForModel(
      job,
      malloyConfig,
      modelCache,
   );
   const mm = runtime.loadModel(modelURL, { importBaseURL });
   const compiled = await mm.getModel();
   const modelDef = compiled._modelDef;

   const modelSourceText = textFor(modelURL);
   const reused = modelCache.hit
      ? reusableDerivation(modelDef, modelSourceText)
      : undefined;
   if (reused)
      addGateFindings(modelGateFindings.getStore(), reused.gateFindings);
   const {
      givens,
      sourceInfos,
      sources,
      filterMap,
      queries,
      authorizeWarnings,
      modelInfo,
   } = reused ?? (await deriveModel(mm, compiled, modelSourceText));

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
      modelInfo,
      sourceInfos,
      // `sources`/`queries` ship complete (authorize + filter enforcement and
      // join resolution read the full set); the Model's discovery accessors
      // filter them down to the export closure (`modelDef.exports`) to match
      // `modelInfo`/`sourceInfos`.
      sources,
      queries,
      filterMap,
      givens,
      // The bytes this compile read, so a consumer slicing a
      // DocumentLocation out of them is cutting the same snapshot the
      // coordinates were computed against. See SerializedModel.
      modelSourceText,
      dataStyles: urlReader.getHackyAccumulatedDataStyles(),
      compileDurationMs,
      problems: job.collectProblems ? compiled.problems : undefined,
      compileCacheHit: modelCache.hit,
      derivationReused: reused !== undefined,
      authorizeWarnings,
      renderTagResults,
      preaggregateCompanion,
   };
}

/**
 * Do one compiled model's share of the main thread's work: render-tag
 * results, pre-aggregation companion, build-plan parts. Past the soft
 * deadline (checked before each target and the companion) the rest is left to
 * the main thread, so a slow package loads rather than hitting the job limit.
 * Plan parts are still collected after a companion that started in time,
 * since they need no recompile. Never throws: a failure fails at most the plan.
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
   renderTagResults?: { label: string; result?: Malloy.Result }[];
   preaggregateCompanion?: { modelDef?: unknown };
}> {
   if (load.failed) return {};
   const pastDeadline = (): boolean => performance.now() >= load.softDeadline;
   const { modelPath, materializer, importBaseURL } = model;

   // On the main thread these prepares would block the event loop (see
   // SerializedModel.renderTagResults).
   const renderTagResults: { label: string; result?: Malloy.Result }[] = [];
   const targets = renderTagTargets(model.queries, model.sources);
   // Build the shared QueryModel once, as Model.ensureSharedQueryModel does:
   // Malloy reuses it only once it exists, otherwise each prepare builds one.
   if (targets.length > 0 && !load.failed && !pastDeadline()) {
      try {
         void model.model.queryModel;
      } catch {
         // The prepares below build their own.
      }
   }
   for (const target of targets) {
      if (load.failed || pastDeadline()) break;
      try {
         const prepared = await materializer
            .loadQuery(target.queryString)
            .getPreparedResult();
         // Without its SQL: the tag check never reads it, and it is about
         // half of each result's size.
         const { sql: _sql, ...result } = prepared.toStableResult();
         renderTagResults.push({
            label: target.label,
            result: result as Malloy.Result,
         });
      } catch {
         // A view or query that fails to prepare is reported by the normal
         // compile path; the render-tag check skips it.
         renderTagResults.push({ label: target.label });
      }
   }

   const order = load.modelOrder.get(modelPath) ?? 0;
   let wantsPlan =
      load.planParts !== undefined &&
      !load.failed &&
      !load.planLeftToMainThread &&
      !(load.planError && load.planError.order < order);
   if (wantsPlan && pastDeadline()) {
      load.planLeftToMainThread = true;
      wantsPlan = false;
   }

   // The runtime the companion compiles in: the model's own, reading the
   // synthesized model's text from the overlay.
   const companionRuntime: SynthesizedRuntimeFactory = async (overlay) => {
      const files = makeWorkerUrlReader(job);
      return {
         runtime: jobRuntime(
            job,
            malloyConfig,
            {
               readURL: (url: URL) => {
                  const text = overlay.get(url.href);
                  return text !== undefined
                     ? Promise.resolve(text)
                     : files.readURL(url);
               },
            },
            compileCacheFor(job).manager,
         ),
         importBaseURL,
      };
   };
   // One compile serves both the main thread and the plan, since a build
   // manifest is read only at query prepare. Undefined when not wanted or
   // left to the main thread; null when there is no usable rollup.
   const companion: SynthesizedPreaggregation | null | undefined =
      (job.withPreaggregateCompanions || wantsPlan) &&
      !load.failed &&
      !pastDeadline()
         ? ((await tryCompileSynthesizedPreaggregation({
              packagePath: job.packagePath,
              modelPath,
              contents: model.model._modelDef.contents as Record<
                 string,
                 unknown
              >,
              getRuntime: companionRuntime,
           })) ?? null)
         : undefined;

   // Rechecked, since another model may have left the plan to the main
   // thread meanwhile. The deadline is not rechecked: the parts need no
   // recompile now, and abandoning them would make the main thread compile
   // every model.
   if (wantsPlan && load.planLeftToMainThread) {
      wantsPlan = false;
   }
   if (wantsPlan && companion !== undefined && !load.failed) {
      load.planStart ??= performance.now();
      try {
         const parts = emptyBuildPlanParts();
         await collectModelBuildPlan(parts, {
            modelPath,
            packagePath: job.packagePath,
            materializer,
            malloyModel: model.model,
            getRuntime: companionRuntime,
            synthesized: companion,
         });
         load.planParts?.set(modelPath, parts);
      } catch (error) {
         if (!load.planError || order < load.planError.order) {
            load.planError = {
               order,
               message: `${modelPath}: ${errMessage(error)}`,
            };
         }
      }
   } else if (wantsPlan) {
      // The deadline passed while the companion was being decided.
      load.planLeftToMainThread = true;
   }

   return {
      renderTagResults,
      preaggregateCompanion:
         !job.withPreaggregateCompanions || companion === undefined
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
   const modelURL = pathToFileURL(fullPath);
   const importBaseURL = new URL(".", modelURL);

   const { runtime, urlReader } = buildRuntimeForModel(
      job,
      malloyConfig,
      compileCacheFor(job).manager,
   );
   const authorizeWarningCollection = authorizeWarningCollector();

   const fileContents = await fs.promises.readFile(modelURL, "utf8");
   const parse = parseMalloynb(fileContents, modelPath);

   // Sequential, so per-cell materializers line up with statement order.
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
         // Guard rather than fail the whole notebook on one corrupt cell.
         continue;
      }
      const currentModelDef = (await localMM.getModel())._modelDef;

      // Sources this cell added, imports included: its namespace minus what
      // earlier cells surfaced. A selective import contributes only its names.
      const currentInfo = modelInfoOf(currentModelDef);
      const newSources = collectSourceInfos(currentModelDef).filter(
         (s) => !(s.name in oldSources),
      );
      for (const s of newSources) oldSources[s.name] = s;

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
         // Source-only cells have no final query.
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

   // An all-markdown notebook has no modelDef.
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
      // `const` so the narrowed type survives into the closure below.
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
   const gateFindings: GateFindingCounts = { rejected: {}, admitAll: {} };
   const model = await modelGateFindings.run(gateFindings, () =>
      compileOrReportModel(job, malloyConfig, modelPath, load),
   );
   return { ...model, gateFindings };
}

async function compileOrReportModel(
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
      // That load fails anyway; stop the remaining main-thread work.
      if (job.computeBuildPlan) load.failed = true;
      const modelType: SerializedModel["modelType"] = modelPath.endsWith(
         NOTEBOOK_FILE_SUFFIX,
      )
         ? "notebook"
         : "model";
      return {
         modelPath,
         modelType,
         // Classified before serializing (the check reads the stack), so it
         // crosses as a compile problem rather than an outage.
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

   // Listed BEFORE the manifest read, which needs to know whether a root
   // index.malloy exists. Both are excluded from the compile timing.
   const allFiles = await listPackageFiles(job.packagePath);
   const modelPaths = filterModelPaths(allFiles);

   // Resolved against the ON-DISK tree, before the replacement: a /compile
   // preview proposing an index.malloy must not re-curate the package.
   const packageMetadata = await readPackageMetadata(
      job.packagePath,
      modelPaths,
   );
   const malloyConfig = buildWorkerMalloyConfig(job);
   compileCacheFor(job).beginLoad(
      `${job.connectionsEpoch}\0${await dataFilesDigest(job.packagePath, allFiles)}`,
   );

   const replacementMatchedExisting = job.replacement
      ? modelPaths.includes(job.replacement.modelPath)
      : undefined;
   if (job.replacement && !replacementMatchedExisting) {
      modelPaths.push(job.replacement.modelPath);
   }

   // The compile region starts here; schema-fetch accounting begins at the
   // same boundary so `compileDurationMs = compileRegion - schemaFetchWait`
   // holds exactly.
   const compileRegionStart = performance.now();
   const schemaCacheHitsAtStart = schemaCache.hits;
   schemaWait.begin(job.requestId);
   const load: LoadContext = {
      softDeadline: loadStart + job.mainThreadWorkBudgetMs,
      planParts: job.computeBuildPlan
         ? new Map<string, BuildPlanParts>()
         : undefined,
      modelOrder: new Map(modelPaths.map((modelPath, i) => [modelPath, i])),
      failed: false,
      planLeftToMainThread: false,
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
   const compileCacheHits = models.filter((m) => m.compileCacheHit).length;
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
         // Clamped: rounding can nudge it slightly negative.
         compileDurationMs: Math.max(
            0,
            compileEnd - compileRegionStart - schemaFetchDurationMs,
         ),
         schemaFetchDurationMs,
         schemaFetchCount,
         schemaCacheHits,
         compileCacheHits,
      },
      buildPlan,
   };
}

/**
 * Merge the models' plan parts, in model order, into the package build plan,
 * as `computePackageBuildPlan` does. Undefined when not asked for, when a
 * model failed, or when the plan was left to the main thread.
 */
async function finishWorkerBuildPlan(
   malloyConfig: MalloyConfig,
   modelPaths: string[],
   load: LoadContext,
   materializationConfig: WirePackageMaterialization | null,
): Promise<WorkerBuildPlan | undefined> {
   if (!load.planParts || load.failed || load.planLeftToMainThread) {
      return undefined;
   }
   const start = performance.now();
   const refusals = (): Partial<Record<EligibilityRefusalReason, number>> => ({
      ...(jobContext.getStore()?.refusals ?? {}),
   });
   const digestSkipped: string[] = [];
   try {
      if (load.planError) throw new Error(load.planError.message);
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
         durationMs: performance.now() - (load.planStart ?? start),
         digestSkipped,
         eligibilityRefused: refusals(),
      };
   } catch (error) {
      return {
         ok: false,
         error: errMessage(error),
         durationMs: performance.now() - (load.planStart ?? start),
         digestSkipped,
         eligibilityRefused: refusals(),
      };
   }
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
   if (message.type === "drop-compile-cache") {
      compileCaches.delete(
         compileCacheKey(message.environmentName, message.packageName),
      );
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
   const context: JobContext = {
      jobId: job.requestId,
      packagePath: job.packagePath,
      refusals: {},
   };
   try {
      const result = await jobContext.run(context, () => loadPackage(job));
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
      // A tick for the postMessage queue to flush before exit.
      setImmediate(() => process.exit(0));
   }
}

// The pool dispatches nothing to a new worker until this arrives.
port.postMessage({ type: "ready" });
