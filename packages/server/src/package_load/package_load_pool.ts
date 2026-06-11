/**
 * Main-thread half of the package-load worker pool.
 *
 * Why this exists: Malloy compile is pure JavaScript that runs on the
 * V8 main event loop. A large package can hold the loop for many
 * seconds — long enough to time out the Kubernetes `/health/liveness`
 * probe (timeout=5s, see worker/main.tf) and trigger a pod restart in
 * the middle of an otherwise routine package load. This pool moves the
 * CPU-bound bulk of `Package.create` (manifest read, every
 * `.malloy`/`.malloynb` compile) into `worker_threads` workers, leaving
 * the main loop free to serve `/health/liveness` and other API traffic.
 *
 * Public surface
 * --------------
 *   getPackageLoadPool()       — lazy singleton; respects PACKAGE_LOAD_WORKERS
 *   pool.loadPackage({...})    — submit one package for off-thread load
 *   pool.shutdown()            — graceful drain (called from health.ts)
 *
 * The pool is REQUIRED. There is no in-process compile fallback —
 * `PACKAGE_LOAD_WORKERS=0` throws at boot. Running Malloy compile on
 * the main event loop is what tripped the K8s liveness probe in the
 * first place; we don't want a knob that re-enables that. Pool-
 * infrastructure failures (worker crash, job timeout, pool shutting
 * down) reject `loadPackage()`; `Package.loadViaWorker` then rewraps
 * them as `ServiceUnavailableError` so the HTTP layer responds 503.
 *
 * Concurrency model
 * -----------------
 *   - Workers are spawned lazily on first use, up to N (configurable
 *     via PACKAGE_LOAD_WORKERS).
 *   - Each worker emits {type:'ready'} once its module has initialized;
 *     the pool waits for that before dispatching to a newly-spawned
 *     worker.
 *   - **Per-worker active job cap = 1.** Compile is CPU-bound and
 *     doesn't multiplex meaningfully inside a single event loop, so
 *     loading two large packages on the same worker just doubles each
 *     of their latencies rather than overlapping their work. Excess
 *     jobs queue at the pool layer and pick a worker the moment one
 *     becomes idle.
 *   - Schema/URL/connection-metadata RPCs from a worker back to the
 *     main thread (over MessagePort) are not counted against the
 *     active-job cap — they're sub-RPCs of the worker's currently
 *     active load-package job.
 *
 * Failure modes
 * -------------
 *   - **Worker spawn fails.** `pw.ready` rejects on `worker.error`,
 *     `worker.exit`, or after `WORKER_SPAWN_TIMEOUT_MS`. Pending jobs
 *     waiting on that worker get a clean rejection and are re-queued
 *     to whichever worker becomes available next.
 *   - **Worker dies mid-job.** `worker.exit` fails every in-flight job
 *     on that worker; the pool prunes the dead worker, and the next
 *     `loadPackage()` lazily respawns up to `maxWorkers`.
 *   - **Job timeout.** `PACKAGE_LOAD_JOB_TIMEOUT_MS` (default 120s) caps a
 *     single package load. On timeout we **terminate the worker** and
 *     reject the caller — we do not silently fall back, and we do not
 *     leave a zombie compile burning CPU on the worker (that's the
 *     exact scenario PR-#767's job-timeout-without-terminate had).
 *
 * Schema-fetch RPC routing
 * ------------------------
 *   When a worker proxies `fetchSchemaForTables` (or other connection
 *   metadata calls) back to the main thread, the message includes a
 *   `jobId`. The pool keeps a `jobId → JobContext` map so it knows
 *   which live `MalloyConfig` to dispatch the request against. The
 *   mapping is installed when the job is submitted and removed when
 *   it resolves.
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

import { ModelCompilationError } from "../errors";
import { logger } from "../logger";
import type {
   ConnectionMetadataRequest,
   ConnectionMetadataResponse,
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
   SerializedError,
   SerializedModel,
   WorkerToMainMessage,
} from "./protocol";

// ──────────────────────────────────────────────────────────────────────
// Configuration
// ──────────────────────────────────────────────────────────────────────

/**
 * Returns the configured worker pool size. The pool is REQUIRED — no
 * in-process fallback exists. Defaults to 1 worker when the env var
 * is unset; throws when set to 0 or to a non-positive / non-numeric
 * value so we fail fast at boot rather than silently degrading.
 *
 * Operators set `PACKAGE_LOAD_WORKERS=N` (N ≥ 1) to tune the pool
 * size for their workload. Setting it to 0 to "disable" the pool is
 * unsupported by design — running Malloy compile on the main event
 * loop is what tripped the K8s liveness probe in the first place
 * and we don't want a config knob that re-enables that footgun.
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

/**
 * Wall-clock cap for a single load-package job. If a worker hangs we
 * don't want to leak a Promise forever. Twice the K8s liveness
 * threshold (5s × 15 fails = 75s) gives comfortable margin against
 * false timeouts during cold starts while still failing fast on real
 * hangs. Overridable via `PACKAGE_LOAD_JOB_TIMEOUT_MS`.
 */
const PACKAGE_LOAD_JOB_TIMEOUT_MS = (() => {
   const raw = process.env.PACKAGE_LOAD_JOB_TIMEOUT_MS;
   if (raw === undefined) return 120_000;
   const parsed = Number.parseInt(raw, 10);
   if (!Number.isFinite(parsed) || parsed <= 0) return 120_000;
   return parsed;
})();

/**
 * How long a freshly-spawned worker has to send its `ready` handshake.
 * If the worker can't load its bundle (missing `package_load_worker.mjs`,
 * native module init failure, etc.), this fires and the pool fails
 * the pending jobs on that worker without waiting forever. Distinct
 * from the per-job timeout so a slow-to-start worker doesn't poison
 * the job timeout budget.
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
   inFlight: Set<string>; // job ids — capped at 1 by per-worker active job rule
   exited: boolean;
}

/**
 * Minimal URL-reader shape the pool dispatches to. Intentionally
 * narrower than Malloy's `URLReader` (which can also return
 * `{contents, invalidationKey}`) — the pool only needs the string
 * payload to forward back to the worker.
 */
export interface PackageLoadUrlReader {
   readURL(
      url: URL,
   ): Promise<string | { contents: string; invalidationKey?: unknown }>;
}

export interface LoadPackageJob {
   packagePath: string;
   packageName: string;
   /**
    * The live MalloyConfig. We don't ship it across the worker
    * boundary; we hold it on the main side and answer the worker's
    * proxy RPCs (for non-duckdb connections) against
    * `config.connections`. The worker has its own DuckDB connection
    * for embedded-database probes.
    */
   malloyConfig: MalloyConfig;
   /** Default connection name (passed verbatim to the worker). */
   defaultConnectionName: string | null;
   /** Custom URLReader for non-file:// imports; falls back to fs in the worker. */
   urlReader?: PackageLoadUrlReader;
   /** Optional buildManifest passed through to Malloy Runtime. */
   buildManifest?: unknown;
}

/**
 * Adapter result the pool surfaces to callers. Same wire shape as
 * `LoadPackageResult` but with the heavy `modelDef` / `sourceInfos`
 * fields restored to their Malloy types so the consumer (currently
 * `Package.createViaWorker`) doesn't need to repeat the cast at
 * every call site. The protocol module uses `unknown` for these so
 * it stays decoupled from the full Malloy type surface; this is
 * where we re-tighten.
 *
 * `filterMap` stays as the wire array; `Model.fromSerialized` will
 * rebuild a `Map` on demand. Keeping it array-shaped here means we
 * don't allocate a Map for models the caller never looks at.
 */
export interface LoadPackageOutcome {
   packageMetadata: {
      name?: string;
      description?: string;
      explores?: string[];
      queryableSources?: "declared" | "all";
   };
   models: Array<
      Omit<SerializedModel, "modelDef" | "sourceInfos"> & {
         modelDef?: ModelDef;
         sourceInfos?: Malloy.SourceInfo[];
      }
   >;
   loadDurationMs: number;
}

// ──────────────────────────────────────────────────────────────────────
// Path to the bundled worker script
// ──────────────────────────────────────────────────────────────────────

/**
 * Locate the worker explore. In production builds it's bundled to
 * `dist/package_load_worker.mjs` next to `server.mjs`. In dev
 * (bun --watch) the source `.ts` next to this file loads directly —
 * Bun supports `new Worker(<ts-url>)`. The .ts fallback also keeps
 * `bun test` working without a build step.
 *
 * Uses `pathToFileURL` rather than a `file://${path}` template so the
 * URL stays valid on Windows, where absolute paths look like
 * `D:\foo\bar` (template form would yield `file://D:\foo\bar`, which
 * parses as host `D:` rather than a path).
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

   constructor(maxWorkers: number, workerScript?: URL) {
      if (!Number.isFinite(maxWorkers) || maxWorkers < 1) {
         throw new Error(
            `PackageLoadPool requires maxWorkers >= 1 (got ${maxWorkers}). ` +
               `The in-process compile fallback was removed in favour of a hard ` +
               `dependency on the worker pool; see getPackageLoadWorkerCount().`,
         );
      }
      this.maxWorkers = maxWorkers;
      this.workerScript = workerScript ?? resolveWorkerScript();
   }

   get size(): number {
      return this.workers.filter((w) => !w.exited).length;
   }

   /**
    * Submit a package for off-thread load. Returns when the worker
    * has produced a {@link LoadPackageOutcome}. Per-package compile
    * failures are surfaced **inside** the outcome (as
    * `model.compilationError`), not as a rejected Promise — the
    * Promise only rejects for whole-package errors (manifest missing,
    * worker crash, RPC timeout, etc.) so the caller can distinguish
    * "this package is broken; tell the user" from "the worker pool
    * itself failed and we have a bigger problem".
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

   /**
    * Greedy dispatcher: keep draining the queue while there's both
    * a queued job and an available worker. Called from `loadPackage`
    * (on submit) and from job completion / worker exit (to give the
    * next job a worker that just freed up).
    */
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

   /**
    * Return an idle worker, or spawn a new one if we're below
    * `maxWorkers`. With per-worker active=1, "idle" means
    * `inFlight.size === 0`. Returns null when the pool is saturated.
    */
   private findIdleOrSpawnable(): PoolWorker | null {
      const alive = this.workers.filter((w) => !w.exited);
      const idle = alive.find((w) => w.inFlight.size === 0);
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
      try {
         await pw.ready;
      } catch (err) {
         // Spawn failed; this worker is dead. Reject this job and
         // try the next queued one — a different worker may already
         // be alive, or `findIdleOrSpawnable` will lazily spawn one.
         qj.reject(err as Error);
         this.tryDispatch();
         return;
      }

      this.nextJobId += 1;
      const jobId = `job-${this.nextJobId}`;

      const timeout = setTimeout(() => {
         this.handleJobTimeout(pw, jobId, qj.request.packagePath);
      }, PACKAGE_LOAD_JOB_TIMEOUT_MS);

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

      // Register inFlight BEFORE postMessage so the next concurrent
      // dispatcher pass sees this worker as busy (fixes Sha-Bang #1's
      // 4/1 skew where simultaneous callers all tiebreak to alive[0]).
      pw.inFlight.add(jobId);

      const message: LoadPackageRequest = {
         type: "load-package",
         requestId: jobId,
         packagePath: qj.request.packagePath,
         packageName: qj.request.packageName,
         defaultConnectionName: qj.request.defaultConnectionName,
         buildManifest: qj.request.buildManifest,
      };
      pw.worker.postMessage(message);
   }

   /**
    * On job timeout we DON'T silently fall back — that lets the
    * worker keep burning CPU on the doomed compile while the caller
    * also re-runs it on the main thread (the exact 2× CPU situation
    * the pool exists to avoid). Instead: terminate the worker,
    * reject the caller, and let the next call respawn lazily.
    */
   private handleJobTimeout(
      pw: PoolWorker,
      jobId: string,
      packagePath: string,
   ): void {
      logger.error(
         `PackageLoadPool: job ${jobId} (${packagePath}) timed out after ${PACKAGE_LOAD_JOB_TIMEOUT_MS}ms; terminating worker #${pw.id}`,
      );
      this.failJob(
         jobId,
         new Error(
            `Package-load worker timed out after ${PACKAGE_LOAD_JOB_TIMEOUT_MS}ms (package=${packagePath})`,
         ),
      );
      void pw.worker.terminate();
      // The exit handler will mark the worker exited, fail any
      // residual jobs on it, and trigger tryDispatch.
   }

   private spawnWorker(): PoolWorker {
      this.nextWorkerId += 1;
      const id = this.nextWorkerId;
      logger.info(
         `PackageLoadPool: spawning worker #${id} (script=${this.workerScript.toString()})`,
      );
      const worker = new Worker(this.workerScript, {
         name: `malloy-package-load-worker-${id}`,
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
      // Silence unhandled-rejection on `ready` itself — every caller
      // that consumes it (via dispatchToWorker) handles rejection
      // explicitly, but Node still tracks the original promise.
      ready.catch(() => {});

      const spawnTimer = setTimeout(() => {
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
      };

      worker.on("message", (msg: WorkerToMainMessage) => {
         this.handleWorkerMessage(pw, msg, readyResolve, spawnTimer);
      });

      worker.on("error", (err) => {
         logger.error(`PackageLoadPool: worker #${id} errored`, {
            error: err,
         });
         clearTimeout(spawnTimer);
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
         // Fail any jobs that were running on this worker so callers
         // don't strand waiting for a result that will never come.
         for (const jobId of Array.from(pw.inFlight)) {
            this.failJob(
               jobId,
               new Error(
                  `Package-load worker #${id} exited unexpectedly (code=${code})`,
               ),
            );
         }
         pw.inFlight.clear();
         // Prune the dead worker so size accounting is honest and
         // the next dispatch will see a free slot to respawn into.
         const idx = this.workers.indexOf(pw);
         if (idx >= 0) this.workers.splice(idx, 1);
         // Give queued jobs a chance to dispatch elsewhere.
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
            void this.handleConnectionMetadata(pw, msg);
            return;
         case "schema-for-tables":
            void this.handleSchemaForTables(pw, msg);
            return;
         case "schema-for-sql":
            void this.handleSchemaForSql(pw, msg);
            return;
         case "read-url":
            void this.handleReadUrl(pw, msg);
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
      // Worker is idle now — give it the next queued job (if any).
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
         pw.worker.postMessage(response as MainToWorkerMessage);
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
         pw.worker.postMessage(response as MainToWorkerMessage);
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
         pw.worker.postMessage(response as MainToWorkerMessage);
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
         pw.worker.postMessage(response as MainToWorkerMessage);
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

/**
 * Hydrate the wire-typed serialized model fields back into their
 * full Malloy types. Cheap (one pass, no copy); `filterMap` is
 * deliberately left as the wire array — `Model.fromSerialized`
 * rebuilds the `Map` lazily for the models it actually uses.
 */
function adaptResult(result: LoadPackageResult): LoadPackageOutcome {
   return {
      packageMetadata: result.packageMetadata,
      models: result.models.map((m) => ({
         ...m,
         modelDef: m.modelDef as ModelDef | undefined,
         sourceInfos: m.sourceInfos as Malloy.SourceInfo[] | undefined,
      })),
      loadDurationMs: result.loadDurationMs,
   };
}

function serializeError(error: unknown): SerializedError {
   if (error instanceof Error) {
      return {
         name: error.name,
         message: error.message,
         stack: error.stack,
      };
   }
   return { name: "Error", message: String(error) };
}

/**
 * Reconstitute an Error from a serialized payload. When the original
 * was a Malloy compile error we re-wrap as `ModelCompilationError` so
 * downstream `instanceof` checks (which decide e.g. HTTP 424 vs 500)
 * keep firing across the worker boundary.
 */
export function deserializeError(serialized: SerializedError): Error {
   const err = new Error(serialized.message);
   err.name = serialized.name;
   if (serialized.stack) err.stack = serialized.stack;
   if (serialized.malloyProblems) {
      (err as unknown as { problems: unknown }).problems =
         serialized.malloyProblems;
   }
   if (serialized.isCompilationError) {
      // ModelCompilationError's ctor expects a MalloyError-shaped
      // input but only reads `.message` at runtime. Cast through to
      // satisfy the nominal type without losing data.
      const wrapped = new ModelCompilationError(
         err as unknown as ConstructorParameters<
            typeof ModelCompilationError
         >[0],
      );
      if (serialized.stack) wrapped.stack = serialized.stack;
      return wrapped;
   }
   return err;
}

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

/** Test-only: replace the singleton (and shut down the previous one). */
export async function __setPackageLoadPoolForTests(
   pool: PackageLoadPool | null,
): Promise<void> {
   if (singleton && singleton !== pool) {
      await singleton.shutdown();
   }
   singleton = pool;
}
