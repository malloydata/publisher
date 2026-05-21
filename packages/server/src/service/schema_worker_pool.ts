/**
 * Long-lived pool of {@link Worker} threads that perform DuckDB schema
 * introspection off the main event loop.
 *
 * Why a dedicated pool (not the libuv worker pool, not setImmediate):
 *
 *  - `@malloydata/db-duckdb` opens DuckDB via a native addon. Every
 *    DuckDBConnection allocates its own native thread pool sized to
 *    the host's CPU count (not the cgroup's CPU share). Concurrent
 *    schema introspection on the main thread compounded into the
 *    466-leaked-Bun-Pool-threads / 90GB-VmSize OOM signature seen on
 *    `worker-76b49bdb89-8bsv4`.
 *
 *  - Owning the DuckDBConnection inside a worker isolates the native
 *    pool to *that* worker. Per-pool sizing → predictable thread
 *    budget. Worker exit → native threads die with it. No leak across
 *    package loads.
 *
 *  - The schema-introspection case is uniquely suited to workers:
 *    inputs and outputs are plain JSON (structured-cloneable), and
 *    the work touches no environment connections, so we don't need
 *    cross-thread IPC for live Snowflake/BigQuery handles. This is
 *    why we tackle schema first — model compile (which *does* need
 *    live env connections) is a much bigger lift, tracked separately.
 */
import { Worker } from "worker_threads";

import { logger } from "../logger";

type ColumnInfo = { type: string; name: string };

/**
 * Public-facing schema-row shape. Mirrors the original synchronous
 * `getDatabaseInfo` return so callers in `package.ts` are unchanged.
 */
export interface SchemaResult {
   name: string;
   rowCount: number;
   columns: ColumnInfo[];
}

interface WorkerSlot {
   worker: Worker;
   /** Whether the worker is currently handling a request. */
   busy: boolean;
}

interface PendingRequest {
   id: number;
   packagePath: string;
   databasePath: string;
   resolve: (value: SchemaResult) => void;
   reject: (reason: Error) => void;
}

const DEFAULT_POOL_SIZE = 2;

export class SchemaWorkerPool {
   private readonly workers: WorkerSlot[] = [];
   private readonly queue: PendingRequest[] = [];
   /** id → pending request currently executing. */
   private readonly inFlight = new Map<number, PendingRequest>();
   /** Maps a worker index to the id of the request it's running. */
   private readonly workerCurrentId = new Map<number, number>();
   private nextId = 1;
   private stopped = false;

   constructor(
      private readonly workerUrl: URL,
      private readonly size: number = DEFAULT_POOL_SIZE,
   ) {}

   public start(): void {
      if (this.workers.length > 0) return;
      for (let i = 0; i < this.size; i++) {
         this.workers.push(this.spawn(i));
      }
      logger.info(`SchemaWorkerPool started (size=${this.size})`);
   }

   public async stop(): Promise<void> {
      this.stopped = true;
      // Fail any queued/in-flight work so callers don't hang on shutdown.
      const shutdownError = new Error("SchemaWorkerPool stopped");
      for (const req of this.queue.splice(0)) req.reject(shutdownError);
      for (const req of this.inFlight.values()) req.reject(shutdownError);
      this.inFlight.clear();
      await Promise.all(
         this.workers.map(async (slot) => {
            try {
               await slot.worker.terminate();
            } catch {
               // Best-effort: terminate failures shouldn't block shutdown.
            }
         }),
      );
      this.workers.length = 0;
   }

   /**
    * Submit one schema-introspection job. Resolves with the schema
    * description; rejects if the worker returns an error or crashes.
    *
    * Concurrent calls beyond the pool size are queued FIFO; once a
    * worker frees up the next queued request is dispatched.
    */
   public submit(
      packagePath: string,
      databasePath: string,
   ): Promise<SchemaResult> {
      if (this.stopped) {
         return Promise.reject(new Error("SchemaWorkerPool stopped"));
      }
      if (this.workers.length === 0) {
         return Promise.reject(
            new Error("SchemaWorkerPool.submit called before start()"),
         );
      }
      return new Promise<SchemaResult>((resolve, reject) => {
         const req: PendingRequest = {
            id: this.nextId++,
            packagePath,
            databasePath,
            resolve,
            reject,
         };
         this.queue.push(req);
         this.drain();
      });
   }

   /**
    * Try to assign queued requests to idle workers. Cheap; called
    * after every enqueue and after every worker completes a request.
    */
   private drain(): void {
      for (let i = 0; i < this.workers.length; i++) {
         if (this.queue.length === 0) return;
         const slot = this.workers[i];
         if (slot.busy) continue;
         const req = this.queue.shift()!;
         slot.busy = true;
         this.inFlight.set(req.id, req);
         this.workerCurrentId.set(i, req.id);
         slot.worker.postMessage({
            id: req.id,
            packagePath: req.packagePath,
            databasePath: req.databasePath,
         });
      }
   }

   private spawn(index: number): WorkerSlot {
      const worker = new Worker(this.workerUrl);
      const slot: WorkerSlot = { worker, busy: false };

      worker.on(
         "message",
         (msg: {
            id: number;
            ok: boolean;
            result?: SchemaResult;
            error?: { message: string; stack?: string };
         }) => {
            const req = this.inFlight.get(msg.id);
            if (!req) {
               logger.warn("SchemaWorkerPool: response for unknown request", {
                  id: msg.id,
                  workerIndex: index,
               });
               return;
            }
            this.inFlight.delete(msg.id);
            this.workerCurrentId.delete(index);
            slot.busy = false;
            if (msg.ok && msg.result) {
               req.resolve(msg.result);
            } else {
               const err = new Error(msg.error?.message ?? "Unknown error");
               if (msg.error?.stack) err.stack = msg.error.stack;
               req.reject(err);
            }
            this.drain();
         },
      );

      // Lifecycle: `error` fires first (if it fires) and reports the
      // crash; `exit` always fires next and is the single point where
      // we replace the slot. Splitting it this way avoids a class of
      // bugs where `error` respawns the slot and then `exit` respawns
      // it again, leaking the worker created in between (alive Worker
      // instance with a DuckDB connection — exactly what this pool
      // exists to prevent).
      worker.on("error", (err) => {
         const inFlightId = this.workerCurrentId.get(index);
         if (inFlightId !== undefined) {
            const req = this.inFlight.get(inFlightId);
            if (req) {
               this.inFlight.delete(inFlightId);
               req.reject(err);
            }
            this.workerCurrentId.delete(index);
         }
         logger.error("SchemaWorkerPool: worker errored", {
            workerIndex: index,
            error: err,
         });
         // Don't respawn here — `exit` will, after the worker has
         // fully torn down its native resources.
      });

      worker.on("exit", (code) => {
         if (this.stopped) return;
         // If `error` already fired, workerCurrentId is empty and this
         // is a no-op. If the worker exited without firing `error`
         // (e.g. process.exit inside the worker, or a clean exit while
         // mid-request), reject any in-flight request so the caller
         // doesn't hang forever.
         const inFlightId = this.workerCurrentId.get(index);
         if (inFlightId !== undefined) {
            const req = this.inFlight.get(inFlightId);
            if (req) {
               this.inFlight.delete(inFlightId);
               req.reject(new Error(`SchemaWorker exited with code ${code}`));
            }
            this.workerCurrentId.delete(index);
         }
         if (code !== 0) {
            logger.warn("SchemaWorkerPool: worker exited unexpectedly", {
               workerIndex: index,
               code,
            });
         } else {
            // A clean exit while the pool is still running is also
            // unexpected — workers are supposed to live as long as
            // the pool does. Respawn so capacity isn't silently lost.
            logger.info("SchemaWorkerPool: worker exited cleanly, respawning", {
               workerIndex: index,
            });
         }
         this.workers[index] = this.spawn(index);
         this.drain();
      });

      return slot;
   }
}

/**
 * Process-wide singleton. Constructed lazily so importing this module
 * doesn't spawn workers in test environments that never call
 * `getSchemaWorkerPool()`.
 *
 * The worker URL is resolved from `import.meta.url`, which lets Bun
 * load `schema_worker.ts` directly in dev and the bundled
 * `schema_worker.mjs` in prod (see `build.ts`).
 */
let singleton: SchemaWorkerPool | null = null;

export function getSchemaWorkerPool(): SchemaWorkerPool {
   if (!singleton) {
      const url = resolveWorkerUrl();
      const size = Number(process.env.PUBLISHER_SCHEMA_WORKER_POOL_SIZE) || 2;
      singleton = new SchemaWorkerPool(url, size);
      singleton.start();
   }
   return singleton;
}

function resolveWorkerUrl(): URL {
   // In dev (`bun --watch src/server.ts`), import.meta.url points at
   // `.../src/service/schema_worker_pool.ts` and the worker is the
   // sibling `.ts` file.
   //
   // In prod, this module gets inlined into `dist/server.mjs`, so
   // `import.meta.url` resolves to `dist/server.mjs`. Bun's bundler
   // nests outputs by their path relative to the common entrypoint
   // root (./src), so schema_worker lands at
   // `dist/service/schema_worker.mjs` — one directory below
   // server.mjs.
   const base = new URL(import.meta.url);
   const isBundled = base.pathname.endsWith(".mjs");
   return new URL(
      isBundled ? "./service/schema_worker.mjs" : "./schema_worker.ts",
      base,
   );
}
