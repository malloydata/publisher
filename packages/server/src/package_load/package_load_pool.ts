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
 *   while it exits. * - A served copy's pass (`compileServedCopy`) takes only a worker that is
 *   ready and idle now, with nothing queued, and never queues or spawns: a
 *   query may be waiting on it, and its caller compiles inline instead.
 */
import {
   upstreamTableKey,
   type UpstreamSchemaReader,
   type UpstreamSchemaReading,
} from "../service/materialization/domain/upstream_schemas";
import type {
   Connection,
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
import { recordUnhandledRejection } from "../process_safety";
import { ImportOutsidePackageError } from "../utils";
import { Settings, settings } from "../settings";
import type {
   PackageMaterializationConfig,
   PackageScope,
} from "../service/package_manifest";
import type { PackageRetrievalSettings } from "../service/package_retrieval_settings";
import type { DeprecatedManifestKey } from "../service/deprecated/manifest_keys";
import type {
   CompileServedCopyRequest,
   CompileServedCopyResult,
   ConnectionMetadataRequest,
   ConnectionMetadataResponse,
   LoadPackageError,
   LoadPackageRequest,
   LoadPackageResult,
   ForgetSchemasMessage,
   MainToWorkerMessage,
   ReadUrlRequest,
   ReadUrlResponse,
   RpcErrorResponse,
   SchemaForSqlRequest,
   SchemaForSqlResponse,
   UpstreamSchemasRequest,
   UpstreamSchemasResponse,
   SchemaForTablesRequest,
   SchemaForTablesResponse,
   SerializedModel,
   WorkerLogMessage,
   WorkerToMainMessage,
} from "./protocol";
import {
   checkCompileServedCopyResult,
   checkLoadPackageResult,
} from "./protocol_decode";
import {
   connectionTypeOf,
   isManagedConnection,
} from "../service/connection_type";
import { SchemaRefetches } from "./schema_refetch";
import { sqlKey } from "@malloydata/malloy";

// ──────────────────────────────────────────────────────────────────────
// Configuration
// ──────────────────────────────────────────────────────────────────────

/**
 * The configured pool size (`packageLoad.workers`, `PACKAGE_LOAD_WORKERS`,
 * default 1). There is deliberately no way to turn the pool off: compiling on
 * the main event loop is what fails the liveness probe.
 */
export function getPackageLoadWorkerCount(): number {
   return settings.get("packageLoad.workers");
}

export { DEFAULT_PACKAGE_LOAD_SCHEMA_CACHE_BYTES } from "../constants";

/**
 * Serialized bytes of schema each worker caches across loads
 * (`packageLoad.schemaCacheBytes`; see `schema_cache.ts`); 0 disables it.
 */
export function getPackageLoadSchemaCacheBytes(): number {
   return settings.get("packageLoad.schemaCacheBytes");
}

/**
 * The share of a job's timeout the worker spends on work for the main thread,
 * render-tag results, pre-aggregation companions and the build plan, before
 * leaving the rest to it (see LoadPackageRequest.mainThreadWorkBudgetMs).
 */
const MAIN_THREAD_WORK_BUDGET_SHARE = 0.75;

/**
 * How many times `packageLoad.jobTimeoutMs` the pool lets a job run. The
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
   /** Settles the job with its result message, checked by kind. */
   resolve: (result: LoadPackageResult | CompileServedCopyResult) => void;
   reject: (err: Error) => void;
   timeout: ReturnType<typeof setTimeout>;
   /**
    * When a refreshing load was requested, on `performance.now()`: a refetch
    * begun since answers it (see SchemaRefetches).
    */
   refreshRequestedAt?: number;
   /** See {@link LoadPackageJob.upstreamSchemas}. */
   upstreamSchemas?: UpstreamSchemaReader;
}

/**
 * How long a background job may wait behind foreground jobs before a free
 * worker takes it anyway, so sustained user load never starves a connection
 * edit's re-derivation (whose readers would otherwise answer 503 forever).
 */
export const BACKGROUND_JOB_MAX_WAIT_MS = 30_000;

/**
 * Which queued job a free worker takes: the oldest background job once it has
 * waited {@link BACKGROUND_JOB_MAX_WAIT_MS}, otherwise the oldest foreground
 * job, or the oldest background job when no foreground job waits (see
 * LoadPackageJob.background). A job without `enqueuedAt` never ages.
 */
export function nextQueuedJobIndex(
   queue: readonly {
      request: { background?: boolean };
      enqueuedAt?: number;
   }[],
   now: number = Date.now(),
   maxBackgroundWaitMs: number = BACKGROUND_JOB_MAX_WAIT_MS,
): number {
   const background = queue.findIndex((q) => q.request.background);
   const waitedSince =
      background >= 0 ? queue[background].enqueuedAt : undefined;
   if (waitedSince !== undefined && now - waitedSince >= maxBackgroundWaitMs) {
      return background;
   }
   const foreground = queue.findIndex((q) => !q.request.background);
   return foreground >= 0 ? foreground : 0;
}

type QueuedJob =
   | {
        kind: "load";
        request: LoadPackageJob;
        /** See JobContext.refreshRequestedAt. */
        refreshRequestedAt?: number;
        /** When it was queued, for {@link nextQueuedJobIndex}'s aging. */
        enqueuedAt?: number;
        resolve: (result: LoadPackageOutcome) => void;
        reject: (err: Error) => void;
     }
   | {
        kind: "served-copy";
        request: CompileServedCopyJob;
        enqueuedAt?: number;
        resolve: (result: CompileServedCopyResult) => void;
        reject: (err: Error) => void;
     };

interface PoolWorker {
   id: number;
   worker: Worker;
   ready: Promise<void>;
   inFlight: Set<string>; // job ids; at most one
   exited: boolean;
   /** Its `ready` handshake arrived: a job dispatched to it starts now. */
   isReady: boolean;
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
   /**
    * Reads the catalog types of the tables a build reads, for the address
    * fingerprint (materialization's `readUpstreamSchemas`). Absent, every
    * table reads as `unread: "no_catalog"`: a caller that compiles no
    * materialization (a dry run, a test) need not pass it.
    */
   upstreamSchemas?: UpstreamSchemaReader;
   /** Optional buildManifest passed through to Malloy Runtime. */
   buildManifest?: unknown;
   /** Optional in-memory model replacement for a package compile dry-run. */
   replacement?: { modelPath: string; source: string };
   /** More in-memory files for a package compile dry-run (a request's `files` overlay). */
   overlay?: { modelPath: string; source: string }[];
   /** Return non-fatal compiler diagnostics for dry-run reporting. */
   collectProblems?: boolean;
   /** See {@link LoadPackageRequest.computeBuildPlan}. */
   computeBuildPlan?: boolean;
   /** See {@link LoadPackageRequest.publishedVersion}. */
   publishedVersion?: boolean;
   /** See {@link LoadPackageRequest.withEntities}. */
   withEntities?: boolean;
   /**
    * Background work (a connection edit's build-plan re-derivation): stays on
    * this thread's queue behind every foreground job, so a publish or reload
    * waits for at most the background job already running, never a backlog.
    */
   background?: boolean;
   /**
    * An explicit refresh (a reload, a publish): refetch every schema the load
    * reads, here and in every worker's cache (see
    * {@link LoadPackageRequest.refreshTimestamp}).
    */
   refreshSchemas?: boolean;
}

/** One pass of a model's served copy to compile (see CompileServedCopyRequest). */
export interface CompileServedCopyJob {
   packagePath: string;
   /** See {@link CompileServedCopyRequest.environmentName}. */
   environmentName: string;
   modelPath: string;
   /**
    * The served copy's connections. They stay on this thread, which answers
    * the worker's proxy RPCs against them.
    */
   malloyConfig: MalloyConfig;
   defaultConnectionName: string | null;
   /** The re-rooted files, by overlay key. */
   files: ReadonlyMap<string, string>;
   /** Never background work: a query may be waiting on it. */
   background?: undefined;
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
   /** Refreshing loads' refetches, shared while they overlap. */
   private readonly tableRefetches: SchemaRefetches<{
      schema?: TableSourceDef;
      error?: string;
   }>;
   private readonly sqlRefetches: SchemaRefetches<{
      structDef?: SQLSourceDef;
      error?: string;
   }>;

   constructor(
      maxWorkers: number,
      workerScript?: URL,
      options: {
         /** See LoadPackageRequest.mainThreadWorkBudgetMs. */
         mainThreadWorkBudgetMs?: number;
         /** Overrides `packageLoad.jobTimeoutMs`. */
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
      // The compile budget of one job, so a hung worker cannot leak a Promise
      // forever. Generous enough for cold starts.
      this.jobTimeoutMs =
         options.jobTimeoutMs ?? settings.get("packageLoad.jobTimeoutMs");
      this.jobLimitMs = JOB_LIMIT_FACTOR * this.jobTimeoutMs;
      // A refreshing load can wait in the queue before its job runs.
      const refetchWindowMs = this.jobLimitMs + this.jobTimeoutMs;
      this.tableRefetches = new SchemaRefetches(refetchWindowMs);
      this.sqlRefetches = new SchemaRefetches(refetchWindowMs);
      this.mainThreadWorkBudgetMs =
         options.mainThreadWorkBudgetMs ??
         MAIN_THREAD_WORK_BUDGET_SHARE * this.jobTimeoutMs;
   }

   get size(): number {
      return this.workers.filter((w) => !w.exited).length;
   }

   /** Test-only: whether shutdown() has been called. */
   get isShutDownForTests(): boolean {
      return this.shuttingDown;
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
         this.queue.push({
            kind: "load",
            request,
            resolve,
            reject,
            enqueuedAt: Date.now(),
            // When it is asked for, not when a worker takes it, so loads
            // queued behind one worker share each other's refetches.
            ...(request.refreshSchemas
               ? { refreshRequestedAt: performance.now() }
               : {}),
         });
         this.tryDispatch();
      });
   }

   /**
    * Compile one pass of a served copy on a worker that is ready and idle
    * now, or undefined when none is (every worker busy or starting, or a job
    * already queued): the caller then compiles it on this thread, so a query
    * waiting on it never waits behind a package load or a worker's start.
    * The compile's own failure comes back in-band; the Promise rejects only
    * for a failure of the job (a timeout, a worker exit, a malformed result).
    */
   compileServedCopy(
      request: CompileServedCopyJob,
   ): Promise<CompileServedCopyResult> | undefined {
      if (this.shuttingDown || this.queue.length > 0) return undefined;
      const idle = this.workers.find(
         (w) => w.isReady && !w.exited && !w.retiring && w.inFlight.size === 0,
      );
      if (!idle) return undefined;
      return new Promise<CompileServedCopyResult>((resolve, reject) => {
         void this.dispatchToWorker(idle, {
            kind: "served-copy",
            request,
            resolve,
            reject,
         });
      });
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
         const [qj] = this.queue.splice(nextQueuedJobIndex(this.queue), 1);
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

      if (qj.kind === "served-copy") {
         this.jobs.set(jobId, {
            jobId,
            pw,
            connections: qj.request.malloyConfig.connections,
            urlReader: defaultUrlReader,
            resolve: (result) => {
               clearTimeout(timeout);
               qj.resolve(checkCompileServedCopyResult(result));
            },
            reject: (err) => {
               clearTimeout(timeout);
               qj.reject(err);
            },
            timeout,
         });
         const message: CompileServedCopyRequest = {
            type: "compile-served-copy",
            requestId: jobId,
            packagePath: qj.request.packagePath,
            environmentName: qj.request.environmentName,
            modelPath: qj.request.modelPath,
            defaultConnectionName: qj.request.defaultConnectionName,
            files: [...qj.request.files],
         };
         pw.worker.postMessage(message);
         return;
      }
      this.jobs.set(jobId, {
         jobId,
         pw,
         connections: qj.request.malloyConfig.connections,
         urlReader: qj.request.urlReader ?? defaultUrlReader,
         ...(qj.request.upstreamSchemas
            ? { upstreamSchemas: qj.request.upstreamSchemas }
            : {}),
         resolve: (result) => {
            clearTimeout(timeout);
            const checked = checkLoadPackageResult(result);
            this.forgetElsewhere(pw, checked.refreshedSchemaKeys);
            qj.resolve(adaptResult(checked));
         },
         reject: (err) => {
            clearTimeout(timeout);
            qj.reject(err);
         },
         timeout,
         ...(qj.refreshRequestedAt !== undefined
            ? { refreshRequestedAt: qj.refreshRequestedAt }
            : {}),
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
         overlay: qj.request.overlay,
         collectProblems: qj.request.collectProblems,
         computeBuildPlan: qj.request.computeBuildPlan,
         ...(qj.request.publishedVersion ? { publishedVersion: true } : {}),
         withEntities: qj.request.withEntities,
         mainThreadWorkBudgetMs: this.mainThreadWorkBudgetMs,
         ...(qj.request.refreshSchemas
            ? { refreshTimestamp: nextRefreshTimestamp() }
            : {}),
      };
      pw.worker.postMessage(message);
   }

   /**
    * Tell every worker but `refreshed`, the one that ran a refreshing load,
    * to drop its entries for the schemas that load refetched, so none of them
    * compiles the package (or another reading the same table) against the
    * columns from before. A load under way in another worker may still use
    * what it already read.
    */
   private forgetElsewhere(
      refreshed: PoolWorker,
      keys: string[] | undefined,
   ): void {
      if (!keys || keys.length === 0) return;
      const message: ForgetSchemasMessage = { type: "forget-schemas", keys };
      for (const pw of this.workers) {
         if (pw !== refreshed) this.postToWorker(pw, message);
      }
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
         // The worker reads its settings (the logger's, today) from these
         // rather than from its copy of the environment, which no flag reaches.
         workerData: {
            schemaCacheBytes: this.schemaCacheBytes,
            settings: Settings.current().toWorkerData(),
         },
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
         isReady: false,
         retiring: false,
      };
      void ready.then(
         () => {
            pw.isReady = true;
         },
         () => {},
      );

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
         case "compile-served-copy-result":
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
         case "upstream-schemas":
            this.answerWorker(pw, msg, this.handleUpstreamSchemas(pw, msg));
            return;
         case "read-url":
            this.answerWorker(pw, msg, this.handleReadUrl(pw, msg));
            return;
         case "log":
            handleWorkerLog(msg);
            return;
         case "unhandled-rejection":
            recordUnhandledRejection(
               { message: msg.message, stack: msg.stack },
               { workerId: pw.id },
            );
            return;
         default: {
            const exhaustive: never = msg;
            void exhaustive;
            return;
         }
      }
   }

   private completeJob(
      pw: PoolWorker,
      msg: LoadPackageResult | CompileServedCopyResult,
   ): void {
      const ctx = this.jobs.get(msg.requestId);
      if (!ctx) return;
      this.jobs.delete(msg.requestId);
      pw.inFlight.delete(msg.requestId);
      // The message is structured-clone data typed only by the union; the
      // job's resolve checks its protocol version and envelope before
      // anything casts into it.
      try {
         ctx.resolve(msg);
      } catch (error) {
         logger.error(
            `PackageLoadPool: worker #${pw.id} sent a result this server cannot read`,
            {
               requestId: msg.requestId,
               error: error instanceof Error ? error.message : String(error),
            },
         );
         ctx.reject(error as Error);
         this.tryDispatch();
         return;
      }
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
               connectionType: connectionTypeOf(conn),
               ...(isManagedConnection(conn) ? { managed: true } : {}),
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

   /**
    * Read the native column types of a worker's plan's upstream tables
    * through the job's connections; a refreshing load reads them again.
    */
   private async handleUpstreamSchemas(
      pw: PoolWorker,
      msg: UpstreamSchemasRequest,
   ): Promise<void> {
      const ctx = this.jobs.get(msg.jobId);
      const reply = (
         response: UpstreamSchemasResponse | RpcErrorResponse,
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
         const read: UpstreamSchemaReader =
            ctx.upstreamSchemas ??
            (async ({ tables }) =>
               Object.fromEntries(
                  tables.map((t) => [
                     upstreamTableKey(t),
                     { unread: "no_catalog" } as UpstreamSchemaReading,
                  ]),
               ));
         const readings = await read({
            tables: msg.tables,
            // The job's own connections, on this thread: real ones, which
            // run SQL (the lookup's type is the compile's narrower view).
            connection: async (name) =>
               (await ctx.connections.lookupConnection(
                  name,
               )) as unknown as Connection,
            refresh: ctx.refreshRequestedAt !== undefined,
         });
         reply({
            type: "upstream-schemas-response",
            requestId: msg.requestId,
            ok: true,
            readings,
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
         const options = buildFetchOptions(msg.options);
         const result =
            options.refreshTimestamp !== undefined &&
            ctx.refreshRequestedAt !== undefined
               ? await this.refetchTables(
                    conn,
                    msg.tables,
                    options,
                    ctx.refreshRequestedAt,
                 )
               : await conn.fetchSchemaForTables(msg.tables, options);
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

   /**
    * A refreshing load's table fetch: each table refetched once across the
    * refreshing loads that overlap (see SchemaRefetches). Keyed by table
    * path, as the connection caches it.
    */
   private async refetchTables(
      conn: InfoConnection,
      tables: Record<string, string>,
      options: FetchSchemaOptions,
      requestedAt: number,
   ): Promise<{
      schemas: Record<string, unknown>;
      errors: Record<string, string>;
   }> {
      const answers = await this.tableRefetches.fetch(
         conn,
         Object.values(tables),
         requestedAt,
         async (paths) => {
            const wanted = new Set(paths);
            const asked = Object.entries(tables).filter(([, p]) =>
               wanted.has(p),
            );
            const read = await conn.fetchSchemaForTables(
               Object.fromEntries(asked),
               options,
            );
            const byPath = new Map<
               string,
               { schema?: TableSourceDef; error?: string }
            >();
            for (const [key, p] of asked) {
               if (byPath.has(p)) continue;
               byPath.set(p, {
                  schema: read.schemas[key] as TableSourceDef | undefined,
                  error: read.errors[key],
               });
            }
            return byPath;
         },
      );
      const schemas: Record<string, unknown> = {};
      const errors: Record<string, string> = {};
      for (const [key, p] of Object.entries(tables)) {
         const answer = answers.get(p);
         if (answer?.schema) schemas[key] = answer.schema;
         else errors[key] = answer?.error ?? "No schema returned for the table";
      }
      return { schemas, errors };
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
         const sentence = msg.sentence as Parameters<
            InfoConnection["fetchSchemaForSQLStruct"]
         >[0];
         const options = buildFetchOptions(msg.options);
         const requestedAt = ctx.refreshRequestedAt;
         let result: { structDef?: unknown; error?: string };
         if (
            options.refreshTimestamp !== undefined &&
            requestedAt !== undefined
         ) {
            const key = sqlKey(sentence.connection, sentence.selectStr);
            const answers = await this.sqlRefetches.fetch(
               conn,
               [key],
               requestedAt,
               async () =>
                  new Map([
                     [
                        key,
                        (await conn.fetchSchemaForSQLStruct(
                           sentence,
                           options,
                        )) as {
                           structDef?: SQLSourceDef;
                           error?: string;
                        },
                     ],
                  ]),
            );
            result = answers.get(key) ?? {
               error: "No schema returned for the SQL",
            };
         } else {
            result = await conn.fetchSchemaForSQLStruct(sentence, options);
         }
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

let lastRefreshTimestamp = 0;

/**
 * A refreshing load's stamp, strictly increasing: a connection refetches an
 * entry only when the stamp is greater than the entry's own. It is not what
 * forces the refetch, since SchemaRefetches drops the entry first; it decides
 * only for a connection with no cache to drop from, where an entry stored
 * under a clock later stepped back could still be judged current.
 */
function nextRefreshTimestamp(): number {
   lastRefreshTimestamp = Math.max(Date.now() + 1, lastRefreshTimestamp + 1);
   return lastRefreshTimestamp;
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
function connectionGeneration(conn: object): number {
   let generation = connectionGenerations.get(conn);
   if (generation === undefined) {
      lastConnectionGeneration += 1;
      generation = lastConnectionGeneration;
      connectionGenerations.set(conn, generation);
   }
   return generation;
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

// The worker reads `file:` URLs itself, confined to the package, and asks
// here only for any other scheme. A package's model reads nothing but its own
// files, so every such read is refused (reading the URL as a path would
// resolve it against the server's working directory).
const defaultUrlReader = {
   readURL: async (_url: URL): Promise<string> => {
      throw new ImportOutsidePackageError();
   },
};

// ──────────────────────────────────────────────────────────────────────
// Singleton accessor
// ──────────────────────────────────────────────────────────────────────

let singleton: PackageLoadPool | null = null;

/** Test-only: what makes the pool when there is none; see the setter below. */
let poolFactoryForTests: (() => PackageLoadPool) | undefined;

/**
 * The pool, when one is running; never makes one. For work that uses a
 * worker only when one is already free (a served copy's compile).
 */
export function currentPackageLoadPool(): PackageLoadPool | null {
   return singleton;
}

export function getPackageLoadPool(): PackageLoadPool {
   if (singleton === null && poolFactoryForTests) {
      singleton = poolFactoryForTests();
   }
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

/**
 * Test-only: replace the singleton, and shut down the previous one unless
 * `keep` holds it (a test helper that reuses pools; see freshLoadPool).
 */
export async function __setPackageLoadPoolForTests(
   pool: PackageLoadPool | null,
   keep?: ReadonlySet<PackageLoadPool>,
): Promise<void> {
   if (singleton && singleton !== pool && !keep?.has(singleton)) {
      await singleton.shutdown();
   }
   singleton = pool;
}

/**
 * Test-only: make the pool, when there is none, with `factory` rather than a
 * new one. It runs at the first load after the pool was cleared, so it sees
 * the settings that load runs under.
 */
export function __setPackageLoadPoolFactoryForTests(
   factory: (() => PackageLoadPool) | undefined,
): void {
   poolFactoryForTests = factory;
}
