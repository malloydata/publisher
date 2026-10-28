// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Main-thread half of the package-load worker pool.
 *
 * Malloy compile is synchronous JavaScript; a large package can hold the event
 * loop long enough to fail the Kubernetes liveness probe and restart the pod.
 * This pool runs the CPU-bound bulk of `Package.create` in `worker_threads`
 * workers instead. The pool is REQUIRED: there is no in-process fallback, and
 * `PACKAGE_LOAD_WORKERS=0` throws at boot. Pool failures reject `loadPackage()`,
 * which `Package.loadViaWorker` rewraps as a 503.
 *
 * - Workers spawn lazily, up to `PACKAGE_LOAD_WORKERS`, and take jobs only
 *   after their `ready` handshake.
 * - One job per worker: compile does not overlap inside one event loop, so a
 *   second job only doubles both latencies. Excess jobs queue here. A worker's
 *   proxy RPCs back to this thread are part of its job, routed by `jobId` to
 *   that job's live `MalloyConfig`.
 * - A worker that fails to spawn, or exits mid-job, fails its jobs and is
 *   pruned; the next dispatch respawns.
 * - A job past its limit terminates its worker rather than leave a zombie
 *   compile burning CPU, and the worker is retired so no queued load goes to it
 *   while it exits.
 */
import type {
   FetchSchemaOptions,
   InfoConnection,
   LookupConnection,
   MalloyConfig,
   ModelDef,
   SQLSourceDef,
   TableSourceDef,
} from "@malloydata/malloy";
import * as Malloy from "@malloydata/malloy-interfaces";
import { Worker } from "node:worker_threads";
import { dirname, join } from "path";
import { fileURLToPath, pathToFileURL } from "url";

import { logger } from "../logger";
import type {
   PackageMaterializationConfig,
   PackageScope,
} from "../service/package_manifest";
import type { PackageRetrievalSettings } from "../service/package_retrieval";
import type { DeprecatedManifestKey } from "../service/deprecated/manifest_keys";
import type {
   ConnectionMetadataRequest,
   ConnectionMetadataResponse,
   DropCompileCacheRequest,
   LoadPackageError,
   LoadPackageRequest,
   LoadPackageResult,
   MainToWorkerMessage,
   ReadUrlRequest,
   ReadUrlResponse,
   RpcErrorResponse,
   SchemaForSqlRequest,
   SchemaForSqlResponse,
   SchemaForTablesRequest,
   SchemaForTablesResponse,
   SerializedModel,
   WorkerLogMessage,
   WorkerToMainMessage,
} from "./protocol";

// ──────────────────────────────────────────────────────────────────────
// Configuration
// ──────────────────────────────────────────────────────────────────────

/**
 * The configured pool size (`PACKAGE_LOAD_WORKERS`, default 1). Throws on 0 or
 * a non-positive / non-numeric value: compiling on the main event loop is what
 * fails the liveness probe, so there is deliberately no way to disable the pool.
 */
export function getPackageLoadWorkerCount(): number {
   const raw = process.env.PACKAGE_LOAD_WORKERS;
   if (raw === undefined) return 1;
   const parsed = Number.parseInt(raw, 10);
   if (!Number.isFinite(parsed) || parsed < 1) {
      throw new Error(
         `PACKAGE_LOAD_WORKERS must be a positive integer (got ${JSON.stringify(raw)}). ` +
            `Refusing to start without a package-load worker pool.`,
      );
   }
   return parsed;
}

/** Default for {@link getPackageLoadSchemaCacheBytes}. */
export const DEFAULT_PACKAGE_LOAD_SCHEMA_CACHE_BYTES = 16 * 1024 * 1024;

/**
 * Serialized bytes of schema each worker caches across loads
 * (`PACKAGE_LOAD_SCHEMA_CACHE_BYTES`; see `schema_cache.ts`). Unset or empty
 * takes the default and 0 disables it; any other non-integer refuses to start
 * rather than be silently ignored.
 */
export function getPackageLoadSchemaCacheBytes(): number {
   const raw = process.env.PACKAGE_LOAD_SCHEMA_CACHE_BYTES;
   if (raw === undefined || raw.trim() === "") {
      return DEFAULT_PACKAGE_LOAD_SCHEMA_CACHE_BYTES;
   }
   const parsed = Number(raw.trim());
   if (!Number.isInteger(parsed) || parsed < 0) {
      throw new Error(
         `PACKAGE_LOAD_SCHEMA_CACHE_BYTES must be a non-negative integer number of bytes (got ${JSON.stringify(raw)}).`,
      );
   }
   return parsed;
}

/**
 * Compile budget for one load-package job, so a hung worker cannot leak a
 * Promise forever. Generous enough for cold starts. Overridable via
 * `PACKAGE_LOAD_JOB_TIMEOUT_MS`.
 */
const PACKAGE_LOAD_JOB_TIMEOUT_MS = (() => {
   const raw = process.env.PACKAGE_LOAD_JOB_TIMEOUT_MS;
   if (raw === undefined) return 120_000;
   const parsed = Number.parseInt(raw, 10);
   if (!Number.isFinite(parsed) || parsed <= 0) return 120_000;
   return parsed;
})();

/**
 * The share of a job's timeout the worker spends on work for the main thread,
 * render-tag results, pre-aggregation companions and the build plan, before
 * leaving the rest to it (see LoadPackageRequest.mainThreadWorkBudgetMs).
 */
const MAIN_THREAD_WORK_BUDGET_SHARE = 0.75;

/**
 * How many times `PACKAGE_LOAD_JOB_TIMEOUT_MS` the pool lets a job run. The
 * timeout bounds the compile; the main-thread work interleaved with it starts
 * no new piece past MAIN_THREAD_WORK_BUDGET_SHARE, so a job whose compile fits
 * the timeout finishes within twice it.
 */
const JOB_LIMIT_FACTOR = 2;

/**
 * How long a freshly-spawned worker has to send its `ready` handshake (e.g.
 * if its bundle or a native module fails to load). Separate from the job
 * timeout so a slow start does not eat the job's budget.
 */
const WORKER_SPAWN_TIMEOUT_MS = 30_000;

// ──────────────────────────────────────────────────────────────────────
// Internal types
// ──────────────────────────────────────────────────────────────────────

interface JobContext {
   jobId: string;
   pw: PoolWorker;
   connections: LookupConnection<InfoConnection>;
   urlReader: PackageLoadUrlReader;
   resolve: (result: LoadPackageResult) => void;
   reject: (err: Error) => void;
   timeout: ReturnType<typeof setTimeout>;
}

interface QueuedJob {
   request: LoadPackageJob;
   resolve: (result: LoadPackageOutcome) => void;
   reject: (err: Error) => void;
}

interface PoolWorker {
   id: number;
   worker: Worker;
   ready: Promise<void>;
   inFlight: Set<string>; // job ids; at most one
   exited: boolean;
   /**
    * Stopping and not yet exited: terminated after a job timeout, or failed to
    * start or errored. It takes no new job, but holds its slot until it exits,
    * so the pool never runs more than `maxWorkers` threads.
    */
   retiring: boolean;
}

/** The URL-reader shape the pool dispatches a worker's `read-url` requests to. */
export interface PackageLoadUrlReader {
   readURL(
      url: URL,
   ): Promise<string | { contents: string; invalidationKey?: unknown }>;
}

export interface LoadPackageJob {
   packagePath: string;
   packageName: string;
   /** See {@link LoadPackageRequest.environmentName}. */
   environmentName: string;
   /**
    * The live MalloyConfig. It stays on this thread, which answers the
    * worker's proxy RPCs against `config.connections`.
    */
   malloyConfig: MalloyConfig;
   /** Default connection name (passed verbatim to the worker). */
   defaultConnectionName: string | null;
   /** Custom URLReader for non-file:// imports; falls back to fs in the worker. */
   urlReader?: PackageLoadUrlReader;
   /** Optional buildManifest passed through to Malloy Runtime. */
   buildManifest?: unknown;
   /** Optional in-memory model replacement for a package compile dry-run. */
   replacement?: { modelPath: string; source: string };
   /** Return non-fatal compiler diagnostics for dry-run reporting. */
   collectProblems?: boolean;
   /** See {@link LoadPackageRequest.computeBuildPlan}. */
   computeBuildPlan?: boolean;
   /** See {@link LoadPackageRequest.withPreaggregateCompanions}. */
   withPreaggregateCompanions?: boolean;
   /**
    * See {@link LoadPackageRequest.connectionsEpoch}; defaults to
    * `malloyConfig`'s identity.
    */
   connectionsEpoch?: number;
   /** See {@link LoadPackageRequest.refreshSchemaAt}. */
   refreshSchemaAt?: number;
}

/**
 * `LoadPackageResult` with `modelDef` / `sourceInfos` restored to their Malloy
 * types. `filterMap` stays the wire array; `Model.fromSerialized` builds a
 * `Map` only for models it uses.
 */
export interface LoadPackageOutcome {
   packageMetadata: {
      name?: string;
      description?: string;
      location?: string;
      explores?: string[];
      queryableSources?: "declared" | "all";
      manifestLocation?: string | null;
      materialization?: PackageMaterializationConfig | null;
      scope?: PackageScope;
      /** See {@link LoadPackageResult.packageMetadata.manifestWarnings}. */
      manifestWarnings?: string[];
      /** Deprecated publisher.json keys the manifest declares, at any value. */
      deprecatedManifestKeys?: DeprecatedManifestKey[];
      /** See {@link LoadPackageResult.packageMetadata.retrieval}. */
      retrieval?: PackageRetrievalSettings;
      /** See {@link LoadPackageResult.packageMetadata.agents}. */
      agents?: unknown;
   };
   replacementMatchedExisting?: boolean;
   models: Array<
      Omit<SerializedModel, "modelDef" | "sourceInfos"> & {
         modelDef?: ModelDef;
         sourceInfos?: Malloy.SourceInfo[];
      }
   >;
   loadDurationMs: number;
   /** Per-load phase timing breakdown (see {@link LoadPackageResult.timings}). */
   timings: LoadPackageResult["timings"];
   /** See {@link LoadPackageResult.buildPlan}. */
   buildPlan?: LoadPackageResult["buildPlan"];
}

// ──────────────────────────────────────────────────────────────────────
// Path to the bundled worker script
// ──────────────────────────────────────────────────────────────────────

/**
 * Locate the worker entrypoint: the bundled `package_load_worker.mjs` in a
 * build, or the `.ts` source under Bun (dev and `bun test`). `pathToFileURL`,
 * not a `file://` template, so a Windows `D:\` path is not parsed as a host.
 */
function resolveWorkerScript(): URL {
   const thisFile = fileURLToPath(import.meta.url);
   const thisDir = dirname(thisFile);
   const distCandidate = join(thisDir, "package_load_worker.mjs");
   if (thisFile.endsWith(".mjs") || thisFile.endsWith(".js")) {
      return pathToFileURL(distCandidate);
   }
   const tsCandidate = join(thisDir, "package_load_worker.ts");
   return pathToFileURL(tsCandidate);
}

// ──────────────────────────────────────────────────────────────────────
// The pool
// ──────────────────────────────────────────────────────────────────────

export class PackageLoadPool {
   private readonly workers: PoolWorker[] = [];
   private readonly queue: QueuedJob[] = [];
   private readonly jobs = new Map<string, JobContext>();
   private readonly maxWorkers: number;
   private nextWorkerId = 0;
   private nextJobId = 0;
   private shuttingDown = false;
   private readonly workerScript: URL;
   private readonly schemaCacheBytes: number;
   private readonly mainThreadWorkBudgetMs: number;
   private readonly jobTimeoutMs: number;
   /** How long a job may run before it is terminated (see JOB_LIMIT_FACTOR). */
   private readonly jobLimitMs: number;

   constructor(
      maxWorkers: number,
      workerScript?: URL,
      options: {
         /** See LoadPackageRequest.mainThreadWorkBudgetMs. */
         mainThreadWorkBudgetMs?: number;
         /** Overrides PACKAGE_LOAD_JOB_TIMEOUT_MS. */
         jobTimeoutMs?: number;
      } = {},
   ) {
      if (!Number.isFinite(maxWorkers) || maxWorkers < 1) {
         throw new Error(
            `PackageLoadPool requires maxWorkers >= 1 (got ${maxWorkers}). ` +
               `The in-process compile fallback was removed in favour of a hard ` +
               `dependency on the worker pool; see getPackageLoadWorkerCount().`,
         );
      }
      this.maxWorkers = maxWorkers;
      this.workerScript = workerScript ?? resolveWorkerScript();
      this.schemaCacheBytes = getPackageLoadSchemaCacheBytes();
      this.jobTimeoutMs = options.jobTimeoutMs ?? PACKAGE_LOAD_JOB_TIMEOUT_MS;
      this.jobLimitMs = JOB_LIMIT_FACTOR * this.jobTimeoutMs;
      this.mainThreadWorkBudgetMs =
         options.mainThreadWorkBudgetMs ??
         MAIN_THREAD_WORK_BUDGET_SHARE * this.jobTimeoutMs;
   }

   get size(): number {
      return this.workers.filter((w) => !w.exited).length;
   }

   /**
    * Submit a package for off-thread load. Per-model compile failures come
    * back inside the outcome (`model.compilationError`); the Promise rejects
    * only for whole-package or pool failures.
    */
   async loadPackage(request: LoadPackageJob): Promise<LoadPackageOutcome> {
      if (this.shuttingDown) {
         throw new Error("PackageLoadPool is shutting down");
      }
      return new Promise<LoadPackageOutcome>((resolve, reject) => {
         this.queue.push({ request, resolve, reject });
         this.tryDispatch();
      });
   }

   /** Tell every worker to drop the package's compile cache. */
   dropCompileCache(environmentName: string, packageName: string): void {
      const message: DropCompileCacheRequest = {
         type: "drop-compile-cache",
         environmentName,
         packageName,
      };
      for (const pw of this.workers) this.postToWorker(pw, message);
   }

   /**
    * Drain every in-flight job and terminate the workers. Safe to
    * call multiple times. Awaits each worker's `exit` event.
    */
   async shutdown(): Promise<void> {
      if (this.shuttingDown) return;
      this.shuttingDown = true;
      // Fail any queued jobs that never got dispatched.
      const queued = this.queue.splice(0, this.queue.length);
      for (const qj of queued) {
         qj.reject(new Error("PackageLoadPool: shutdown before dispatch"));
      }
      const exits = this.workers.map(
         (pw) =>
            new Promise<void>((resolve) => {
               if (pw.exited) {
                  resolve();
                  return;
               }
               pw.worker.once("exit", () => resolve());
               const msg: { type: "shutdown" } = { type: "shutdown" };
               pw.worker.postMessage(msg);
               // Hard ceiling — if a worker won't exit, terminate it.
               setTimeout(() => {
                  if (!pw.exited) {
                     void pw.worker.terminate().finally(() => resolve());
                  }
               }, 10_000);
            }),
      );
      await Promise.all(exits);
   }

   // ────────────────────────────────────────────────────────────────
   // Internals: dispatch / queue
   // ────────────────────────────────────────────────────────────────

   /** Drain the queue while there is both a queued job and an available worker. */
   private tryDispatch(): void {
      if (this.shuttingDown) return;
      while (this.queue.length > 0) {
         const worker = this.findIdleOrSpawnable();
         if (!worker) return; // saturated; wait for a job to finish
         const qj = this.queue.shift();
         if (!qj) return;
         void this.dispatchToWorker(worker, qj);
      }
   }

   /** An idle worker, a newly spawned one below `maxWorkers`, or null when saturated. */
   private findIdleOrSpawnable(): PoolWorker | null {
      const alive = this.workers.filter((w) => !w.exited);
      const idle = alive.find((w) => !w.retiring && w.inFlight.size === 0);
      if (idle) return idle;
      if (alive.length < this.maxWorkers) {
         const pw = this.spawnWorker();
         this.workers.push(pw);
         return pw;
      }
      return null;
   }

   private async dispatchToWorker(
      pw: PoolWorker,
      qj: QueuedJob,
   ): Promise<void> {
      // Reserve the worker before waiting for it to become ready. A spawned
      // worker takes a moment to start, and until the job is in `inFlight`
      // the dispatcher sees it as idle, so every job of a burst would land on
      // the first worker spawned and run there concurrently.
      this.nextJobId += 1;
      const jobId = `job-${this.nextJobId}`;
      pw.inFlight.add(jobId);
      try {
         await pw.ready;
      } catch (err) {
         // Spawn failed: reject this job and let the next one find or spawn
         // another worker.
         pw.inFlight.delete(jobId);
         qj.reject(err as Error);
         this.tryDispatch();
         return;
      }

      const timeout = setTimeout(() => {
         this.handleJobTimeout(pw, jobId, qj.request.packagePath);
      }, this.jobLimitMs);

      this.jobs.set(jobId, {
         jobId,
         pw,
         connections: qj.request.malloyConfig.connections,
         urlReader: qj.request.urlReader ?? defaultUrlReader,
         resolve: (result) => {
            clearTimeout(timeout);
            qj.resolve(adaptResult(result));
         },
         reject: (err) => {
            clearTimeout(timeout);
            qj.reject(err);
         },
         timeout,
      });

      const message: LoadPackageRequest = {
         type: "load-package",
         requestId: jobId,
         packagePath: qj.request.packagePath,
         packageName: qj.request.packageName,
         environmentName: qj.request.environmentName,
         defaultConnectionName: qj.request.defaultConnectionName,
         buildManifest: qj.request.buildManifest,
         replacement: qj.request.replacement,
         collectProblems: qj.request.collectProblems,
         computeBuildPlan: qj.request.computeBuildPlan,
         withPreaggregateCompanions: qj.request.withPreaggregateCompanions,
         mainThreadWorkBudgetMs: this.mainThreadWorkBudgetMs,
         connectionsEpoch:
            qj.request.connectionsEpoch ??
            connectionGeneration(qj.request.malloyConfig),
         refreshSchemaAt: qj.request.refreshSchemaAt,
      };
      pw.worker.postMessage(message);
   }

   /**
    * Terminate the worker and reject the caller. Never fall back to compiling
    * on this thread: the worker would keep burning CPU on the same compile.
    */
   private handleJobTimeout(
      pw: PoolWorker,
      jobId: string,
      packagePath: string,
   ): void {
      logger.error(
         `PackageLoadPool: job ${jobId} (${packagePath}) timed out after ${this.jobLimitMs}ms; terminating worker #${pw.id}`,
      );
      // Before the caller hears of the timeout: a load it retries must not be
      // sent to this worker, which is about to exit and would fail it.
      pw.retiring = true;
      this.failJob(
         jobId,
         new Error(
            `Package-load worker timed out after ${this.jobLimitMs}ms (package=${packagePath})`,
         ),
      );
      void pw.worker.terminate();
      // The exit handler fails any residual jobs and redispatches.
   }

   private spawnWorker(): PoolWorker {
      this.nextWorkerId += 1;
      const id = this.nextWorkerId;
      logger.info(
         `PackageLoadPool: spawning worker #${id} (script=${this.workerScript.toString()})`,
      );
      const worker = new Worker(this.workerScript, {
         name: `malloy-package-load-worker-${id}`,
         workerData: { schemaCacheBytes: this.schemaCacheBytes },
      });

      let readyResolve!: () => void;
      let readyReject!: (err: Error) => void;
      let readySettled = false;
      const ready = new Promise<void>((resolve, reject) => {
         readyResolve = () => {
            if (readySettled) return;
            readySettled = true;
            resolve();
         };
         readyReject = (err: Error) => {
            if (readySettled) return;
            readySettled = true;
            reject(err);
         };
      });
      // Callers handle rejection, but Node still tracks the original promise.
      ready.catch(() => {});

      const spawnTimer = setTimeout(() => {
         // Retired before the rejection reaches a waiting load, so the loads
         // queued behind it go to a new worker rather than to this one.
         pw.retiring = true;
         readyReject(
            new Error(
               `Package-load worker #${id} failed to become ready within ${WORKER_SPAWN_TIMEOUT_MS}ms`,
            ),
         );
         void worker.terminate();
      }, WORKER_SPAWN_TIMEOUT_MS);

      const pw: PoolWorker = {
         id,
         worker,
         ready,
         inFlight: new Set(),
         exited: false,
         retiring: false,
      };

      worker.on("message", (msg: WorkerToMainMessage) => {
         this.handleWorkerMessage(pw, msg, readyResolve, spawnTimer);
      });

      worker.on("error", (err) => {
         logger.error(`PackageLoadPool: worker #${id} errored`, {
            error: err,
         });
         clearTimeout(spawnTimer);
         // An uncaught error ends the worker; until its exit event it would
         // look idle and take every queued load. See spawnTimer above.
         pw.retiring = true;
         readyReject(err);
      });

      worker.on("exit", (code) => {
         pw.exited = true;
         clearTimeout(spawnTimer);
         readyReject(
            new Error(
               `Package-load worker #${id} exited before becoming ready (code=${code})`,
            ),
         );
         logger.warn(
            `PackageLoadPool: worker #${id} exited (code=${code}, inFlight=${pw.inFlight.size})`,
         );
         for (const jobId of Array.from(pw.inFlight)) {
            this.failJob(
               jobId,
               new Error(
                  `Package-load worker #${id} exited unexpectedly (code=${code})`,
               ),
            );
         }
         pw.inFlight.clear();
         const idx = this.workers.indexOf(pw);
         if (idx >= 0) this.workers.splice(idx, 1);
         this.tryDispatch();
      });

      return pw;
   }

   // ────────────────────────────────────────────────────────────────
   // Internals: message dispatch
   // ────────────────────────────────────────────────────────────────

   private handleWorkerMessage(
      pw: PoolWorker,
      msg: WorkerToMainMessage,
      markReady: () => void,
      spawnTimer: ReturnType<typeof setTimeout>,
   ): void {
      switch (msg.type) {
         case "ready":
            clearTimeout(spawnTimer);
            markReady();
            return;
         case "load-package-result":
            this.completeJob(pw, msg);
            return;
         case "load-package-error":
            this.errorJob(pw, msg);
            return;
         case "connection-metadata":
            this.answerWorker(pw, msg, this.handleConnectionMetadata(pw, msg));
            return;
         case "schema-for-tables":
            this.answerWorker(pw, msg, this.handleSchemaForTables(pw, msg));
            return;
         case "schema-for-sql":
            this.answerWorker(pw, msg, this.handleSchemaForSql(pw, msg));
            return;
         case "read-url":
            this.answerWorker(pw, msg, this.handleReadUrl(pw, msg));
            return;
         case "log":
            handleWorkerLog(msg);
            return;
         default: {
            const exhaustive: never = msg;
            void exhaustive;
            return;
         }
      }
   }

   private completeJob(pw: PoolWorker, msg: LoadPackageResult): void {
      const ctx = this.jobs.get(msg.requestId);
      if (!ctx) return;
      this.jobs.delete(msg.requestId);
      pw.inFlight.delete(msg.requestId);
      ctx.resolve(msg);
      this.tryDispatch();
   }

   private errorJob(pw: PoolWorker, msg: LoadPackageError): void {
      const ctx = this.jobs.get(msg.requestId);
      if (!ctx) return;
      this.jobs.delete(msg.requestId);
      pw.inFlight.delete(msg.requestId);
      ctx.reject(deserializeError(msg.error));
      this.tryDispatch();
   }

   /**
    * Send a worker the answer to one of its requests. The worker may have been
    * terminated meanwhile, and posting to it throws (`InvalidStateError` under
    * Bun 1.3); unawaited, that would be an unhandled rejection that ends the
    * process. So an exited worker is skipped and a throw is logged.
    */
   private postToWorker(pw: PoolWorker, message: MainToWorkerMessage): void {
      if (pw.exited) return;
      try {
         pw.worker.postMessage(message);
      } catch (error) {
         logger.warn(
            `PackageLoadPool: dropped an answer to worker #${pw.id}, which has stopped`,
            { error: error instanceof Error ? error.message : String(error) },
         );
      }
   }

   /**
    * Run a request handler that nothing awaits, logging a failure instead of
    * leaving it to end the process as an unhandled rejection.
    */
   private answerWorker(
      pw: PoolWorker,
      msg: { type: string; requestId: string },
      handling: Promise<void>,
   ): void {
      handling.catch((error: unknown) => {
         logger.error(
            `PackageLoadPool: answering worker #${pw.id}'s ${msg.type} request failed`,
            {
               requestId: msg.requestId,
               error: error instanceof Error ? error.message : String(error),
            },
         );
      });
   }

   /** Fail a job out-of-band (timeout, worker exit). */
   private failJob(jobId: string, error: Error): void {
      const ctx = this.jobs.get(jobId);
      if (!ctx) return;
      this.jobs.delete(jobId);
      ctx.pw.inFlight.delete(jobId);
      ctx.reject(error);
   }

   private async handleConnectionMetadata(
      pw: PoolWorker,
      msg: ConnectionMetadataRequest,
   ): Promise<void> {
      const ctx = this.jobs.get(msg.jobId);
      const reply = (
         response: ConnectionMetadataResponse | RpcErrorResponse,
      ): void => {
         this.postToWorker(pw, response as MainToWorkerMessage);
      };
      if (!ctx) {
         reply({
            type: "rpc-error",
            requestId: msg.requestId,
            ok: false,
            error: { name: "Error", message: `Unknown jobId ${msg.jobId}` },
         });
         return;
      }
      try {
         const conn = await ctx.connections.lookupConnection(
            msg.connectionName,
         );
         reply({
            type: "connection-metadata-response",
            requestId: msg.requestId,
            ok: true,
            metadata: {
               name: msg.connectionName,
               dialectName: conn.dialectName,
               digest:
                  typeof conn.getDigest === "function"
                     ? conn.getDigest()
                     : msg.connectionName,
               generation: connectionGeneration(conn),
            },
         });
      } catch (error) {
         reply({
            type: "rpc-error",
            requestId: msg.requestId,
            ok: false,
            error: serializeError(error),
         });
      }
   }

   private async handleSchemaForTables(
      pw: PoolWorker,
      msg: SchemaForTablesRequest,
   ): Promise<void> {
      const ctx = this.jobs.get(msg.jobId);
      const reply = (
         response: SchemaForTablesResponse | RpcErrorResponse,
      ): void => {
         this.postToWorker(pw, response as MainToWorkerMessage);
      };
      if (!ctx) {
         reply({
            type: "rpc-error",
            requestId: msg.requestId,
            ok: false,
            error: { name: "Error", message: `Unknown jobId ${msg.jobId}` },
         });
         return;
      }
      try {
         const conn = await ctx.connections.lookupConnection(
            msg.connectionName,
         );
         const result = await conn.fetchSchemaForTables(
            msg.tables,
            buildFetchOptions(msg.options),
         );
         reply({
            type: "schema-for-tables-response",
            requestId: msg.requestId,
            ok: true,
            schemas: result.schemas as Record<string, TableSourceDef>,
            errors: result.errors,
         });
      } catch (error) {
         reply({
            type: "rpc-error",
            requestId: msg.requestId,
            ok: false,
            error: serializeError(error),
         });
      }
   }

   private async handleSchemaForSql(
      pw: PoolWorker,
      msg: SchemaForSqlRequest,
   ): Promise<void> {
      const ctx = this.jobs.get(msg.jobId);
      const reply = (
         response: SchemaForSqlResponse | RpcErrorResponse,
      ): void => {
         this.postToWorker(pw, response as MainToWorkerMessage);
      };
      if (!ctx) {
         reply({
            type: "rpc-error",
            requestId: msg.requestId,
            ok: false,
            error: { name: "Error", message: `Unknown jobId ${msg.jobId}` },
         });
         return;
      }
      try {
         const conn = await ctx.connections.lookupConnection(
            msg.connectionName,
         );
         const result = await conn.fetchSchemaForSQLStruct(
            msg.sentence as Parameters<
               InfoConnection["fetchSchemaForSQLStruct"]
            >[0],
            buildFetchOptions(msg.options),
         );
         if (result.error !== undefined) {
            reply({
               type: "schema-for-sql-response",
               requestId: msg.requestId,
               ok: true,
               error: result.error,
            });
         } else {
            reply({
               type: "schema-for-sql-response",
               requestId: msg.requestId,
               ok: true,
               structDef: result.structDef as SQLSourceDef,
            });
         }
      } catch (error) {
         reply({
            type: "rpc-error",
            requestId: msg.requestId,
            ok: false,
            error: serializeError(error),
         });
      }
   }

   private async handleReadUrl(
      pw: PoolWorker,
      msg: ReadUrlRequest,
   ): Promise<void> {
      const ctx = this.jobs.get(msg.jobId);
      const reply = (response: ReadUrlResponse | RpcErrorResponse): void => {
         this.postToWorker(pw, response as MainToWorkerMessage);
      };
      if (!ctx) {
         reply({
            type: "rpc-error",
            requestId: msg.requestId,
            ok: false,
            error: { name: "Error", message: `Unknown jobId ${msg.jobId}` },
         });
         return;
      }
      try {
         const raw = await ctx.urlReader.readURL(new URL(msg.url));
         const contents = typeof raw === "string" ? raw : raw.contents;
         reply({
            type: "read-url-response",
            requestId: msg.requestId,
            ok: true,
            contents,
         });
      } catch (error) {
         reply({
            type: "rpc-error",
            requestId: msg.requestId,
            ok: false,
            error: serializeError(error),
         });
      }
   }
}

function buildFetchOptions(options: {
   refreshTimestamp?: number;
}): FetchSchemaOptions {
   const out: FetchSchemaOptions = {};
   if (options.refreshTimestamp !== undefined) {
      out.refreshTimestamp = options.refreshTimestamp;
   }
   return out;
}

const connectionGenerations = new WeakMap<object, number>();
let lastConnectionGeneration = 0;

/** A number unique to `conn`'s instance (see ConnectionMetadata.generation). */
export function connectionGeneration(conn: object): number {
   let generation = connectionGenerations.get(conn);
   if (generation === undefined) {
      lastConnectionGeneration += 1;
      generation = lastConnectionGeneration;
      connectionGenerations.set(conn, generation);
   }
   return generation;
}

/** Packages whose schemas were refreshed: the epoch they compile under while `base` holds, and the refresh not yet handed to a load. */
const refreshedPackages = new Map<
   string,
   { epoch: number; base?: number; refreshAt?: number }
>();

/**
 * Give the package a fresh connections epoch, so its next load misses the
 * compile cache, and arm one schema refresh for that load (see
 * {@link settlePackageSchemaRefresh}).
 */
export function refreshPackageConnectionsEpoch(
   environmentName: string,
   packageName: string,
): void {
   refreshedPackages.set(`${environmentName}\0${packageName}`, {
      epoch: connectionGeneration({}),
      refreshAt: Date.now(),
   });
}

/** The refresh armed for the package's next load, if any. */
export function armedPackageSchemaRefresh(
   environmentName: string,
   packageName: string,
): number | undefined {
   return refreshedPackages.get(`${environmentName}\0${packageName}`)
      ?.refreshAt;
}

/**
 * Disarm `refreshAt` once a worker answered the load that carried it. A pool
 * failure never gets here, so the retry still refreshes.
 */
export function settlePackageSchemaRefresh(
   environmentName: string,
   packageName: string,
   refreshAt: number | undefined,
): void {
   const refreshed = refreshedPackages.get(
      `${environmentName}\0${packageName}`,
   );
   if (refreshed && refreshed.refreshAt === refreshAt) {
      refreshed.refreshAt = undefined;
   }
}

/** `base` (the environment config's generation), or the epoch a refresh gave the package while `base` is unchanged. */
export function packageConnectionsEpoch(
   environmentName: string,
   packageName: string,
   base: number,
): number {
   const key = `${environmentName}\0${packageName}`;
   const refreshed = refreshedPackages.get(key);
   if (refreshed === undefined) return base;
   refreshed.base ??= base;
   if (refreshed.base === base) return refreshed.epoch;
   // The connections changed since: the new base is already fresh.
   if (refreshed.refreshAt === undefined) refreshedPackages.delete(key);
   return base;
}

/**
 * Log an entry a job wrote in the worker on this thread, whose logger reaches
 * the OpenTelemetry log pipeline (see WorkerLogMessage), naming the package
 * the job loads.
 */
export function handleWorkerLog(msg: WorkerLogMessage): void {
   // As one entry rather than `log(level, message, meta)`, which reads a `%`
   // in the message as a format directive and then drops the meta.
   logger.log({
      packagePath: msg.packagePath,
      ...msg.meta,
      level: msg.level,
      message: msg.message,
   });
}

/** Restore the wire-typed model fields to their Malloy types (no copy). */
function adaptResult(result: LoadPackageResult): LoadPackageOutcome {
   return {
      packageMetadata: result.packageMetadata,
      replacementMatchedExisting: result.replacementMatchedExisting,
      models: result.models.map((m) => ({
         ...m,
         modelDef: m.modelDef as ModelDef | undefined,
         sourceInfos: m.sourceInfos as Malloy.SourceInfo[] | undefined,
      })),
      loadDurationMs: result.loadDurationMs,
      timings: result.timings,
      buildPlan: result.buildPlan,
   };
}

export { deserializeError, serializeError } from "./error_wire";
import { deserializeError, serializeError } from "./error_wire";

const defaultUrlReader = {
   readURL: async (url: URL): Promise<string> => {
      const { promises: fs } = await import("fs");
      const { fileURLToPath } = await import("url");
      const filePath =
         url.protocol === "file:" ? fileURLToPath(url) : url.toString();
      return fs.readFile(filePath, "utf8");
   },
};

// ──────────────────────────────────────────────────────────────────────
// Singleton accessor
// ──────────────────────────────────────────────────────────────────────

let singleton: PackageLoadPool | null = null;

export function getPackageLoadPool(): PackageLoadPool {
   if (singleton === null) {
      const n = getPackageLoadWorkerCount();
      singleton = new PackageLoadPool(n);
      logger.info(
         `Malloy package-load worker pool enabled (size=${n}). ` +
            `Override with PACKAGE_LOAD_WORKERS=N (N >= 1).`,
      );
   }
   return singleton;
}

/** Drop a package's worker compile caches, if a pool was ever started. */
export function dropPackageCompileCache(
   environmentName: string,
   packageName: string,
): void {
   singleton?.dropCompileCache(environmentName, packageName);
}

/** Test-only: replace the singleton (and shut down the previous one). */
export async function __setPackageLoadPoolForTests(
   pool: PackageLoadPool | null,
): Promise<void> {
   if (singleton && singleton !== pool) {
      await singleton.shutdown();
   }
   singleton = pool;
}
