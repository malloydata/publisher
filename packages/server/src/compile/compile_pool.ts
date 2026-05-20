/**
 * Main-thread half of the compile worker pool.
 *
 * Why this exists: Malloy compile is pure JavaScript and runs on the
 * V8 main thread. A large package can hold the event loop for many
 * seconds, which is long enough to time out the Kubernetes
 * `/health/liveness` probe (timeout=5s, see worker/main.tf). Moving
 * compile into `worker_threads` keeps the main loop responsive
 * regardless of compile cost.
 *
 * Public surface:
 *   - `getCompilePool()` — lazy singleton; respects MALLOY_COMPILE_WORKERS
 *   - `pool.compile({...})` — submit one model for off-thread compile
 *   - `pool.shutdown()` — graceful drain (called from health.ts)
 *
 * Lifecycle:
 *   - Workers are spawned lazily on first use, up to N (configurable).
 *   - Each worker emits {type:'ready'} once initialized; the pool
 *     waits for that before dispatching to a newly-spawned worker.
 *   - Jobs are dispatched to the worker with the fewest in-flight
 *     jobs (least-busy load balancing). A worker can hold multiple
 *     jobs concurrently because compile interleaves on `await`s.
 *   - On worker `exit` (uncaught error, OOM), the pool fails any
 *     in-flight jobs on that worker with a clean error, then respawns
 *     lazily on the next request.
 *
 * Schema-fetch RPC routing:
 *   - When a worker proxies `fetchSchemaForTables` back to the main
 *     thread, the message includes a `jobId`. The pool keeps a
 *     `jobId → MalloyConfig` map for in-flight jobs so it knows which
 *     live connection to dispatch the request to. The mapping is
 *     installed when the job is submitted and removed when it
 *     resolves.
 */
import type {
   Annotation,
   FetchSchemaOptions,
   InfoConnection,
   LookupConnection,
   MalloyConfig,
   ModelDef,
   SQLSourceDef,
   TableSourceDef,
} from "@malloydata/malloy";
import * as Malloy from "@malloydata/malloy-interfaces";
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import { Worker } from "node:worker_threads";
import { ModelCompilationError } from "../errors";
import { logger } from "../logger";
import type { FilterDefinition } from "../service/filter";
import type {
   CompileJobError,
   CompileJobRequest,
   CompileJobResult,
   ConnectionMetadataRequest,
   ConnectionMetadataResponse,
   MainToWorkerMessage,
   ReadUrlRequest,
   ReadUrlResponse,
   RpcErrorResponse,
   SchemaForSqlRequest,
   SchemaForSqlResponse,
   SchemaForTablesRequest,
   SchemaForTablesResponse,
   SerializedError,
   WorkerToMainMessage,
} from "./protocol";

// ──────────────────────────────────────────────────────────────────────
// Configuration
// ──────────────────────────────────────────────────────────────────────

/**
 * Returns the configured worker pool size, or 0 if disabled.
 *
 * Defaults to 0 (off) so the change ships dark and only takes effect
 * where Terraform / operator config opts in via
 * `MALLOY_COMPILE_WORKERS=N`. This lets us land the feature, validate
 * it in staging, and roll out per cluster without surprising any
 * environment that's happy with the in-process compile path.
 */
export function getCompileWorkerCount(): number {
   const raw = process.env.MALLOY_COMPILE_WORKERS;
   if (raw === undefined) return 0;
   const parsed = Number.parseInt(raw, 10);
   if (!Number.isFinite(parsed) || parsed < 0) return 0;
   return parsed;
}

// Wall-clock cap for a single job — if a worker hangs, we don't
// want to leak a Promise forever. Twice the K8s liveness threshold
// (5s × 15 fails = 75s) gives us a comfortable margin against false
// timeouts during cold starts while still failing fast on real hangs.
const COMPILE_JOB_TIMEOUT_MS = Number.parseInt(
   process.env.MALLOY_COMPILE_JOB_TIMEOUT_MS ?? "120000",
   10,
);

// ──────────────────────────────────────────────────────────────────────
// Worker-side stub of the job request, plus the resolved connection
// lookup the pool services schema RPCs against.
// ──────────────────────────────────────────────────────────────────────

interface JobContext {
   jobId: string;
   connections: LookupConnection<InfoConnection>;
   urlReader: CompileUrlReader;
   resolve: (result: CompileJobResult) => void;
   reject: (err: Error) => void;
   timeout: ReturnType<typeof setTimeout>;
}

interface PoolWorker {
   id: number;
   worker: Worker;
   ready: Promise<void>;
   inFlight: Set<string>; // job ids
   exited: boolean;
}

/**
 * Minimal URL-reader shape the pool dispatches to. Intentionally
 * narrower than Malloy's `URLReader` (which can also return
 * `{contents, invalidationKey}`) — the pool only needs the string
 * payload to forward back to the worker.
 */
export interface CompileUrlReader {
   readURL(
      url: URL,
   ): Promise<string | { contents: string; invalidationKey?: unknown }>;
}

export interface CompileRequest {
   packagePath: string;
   modelPath: string;
   /**
    * The live MalloyConfig. We don't ship it across the worker
    * boundary; we hold it on the main side and answer the worker's
    * proxy RPCs against `config.connections`.
    */
   malloyConfig: MalloyConfig;
   /** Default connection name (passed verbatim to the worker). */
   defaultConnectionName: string | null;
   /** Custom URLReader for non-file:// imports; falls back to fs in the worker. */
   urlReader?: CompileUrlReader;
   /** Optional buildManifest passed through to Malloy Runtime. */
   buildManifest?: unknown;
}

export interface CompileOutcome {
   modelDef: ModelDef;
   sourceInfos: Malloy.SourceInfo[];
   sources: unknown[];
   queries: unknown[];
   filterMap: Map<string, FilterDefinition[]>;
   /** Pre-converted API-shape givens or undefined if the model declared none. */
   givens?: unknown[];
   compileDurationMs: number;
}

// ──────────────────────────────────────────────────────────────────────
// Path to the bundled worker script
// ──────────────────────────────────────────────────────────────────────

/**
 * Locate the worker entrypoint. In production builds it's bundled to
 * `dist/compile_worker.mjs` next to `server.mjs`. In dev (bun --watch)
 * the source `.ts` next to this file is loaded directly — Bun supports
 * `new Worker(<ts-url>)`. Falling back to the source path also keeps
 * `bun test` happy without a build step.
 */
function resolveWorkerScript(): URL {
   const thisFile = fileURLToPath(import.meta.url);
   const thisDir = dirname(thisFile);
   // dist layout: dist/compile_worker.mjs sibling to dist/server.mjs.
   // The bundler emits compile_pool.ts inlined into server.mjs (it's
   // not a separate entry), but compile_worker.mjs IS a separate entry
   // so `new Worker(...)` can load it. See build.ts.
   const distCandidate = join(thisDir, "compile_worker.mjs");
   if (thisFile.endsWith(".mjs") || thisFile.endsWith(".js")) {
      return new URL(`file://${distCandidate}`);
   }
   // Dev / test: load the .ts directly.
   const tsCandidate = join(thisDir, "compile_worker.ts");
   return new URL(`file://${tsCandidate}`);
}

// ──────────────────────────────────────────────────────────────────────
// The pool
// ──────────────────────────────────────────────────────────────────────

export class CompileWorkerPool {
   private readonly workers: PoolWorker[] = [];
   private readonly maxWorkers: number;
   private nextWorkerId = 0;
   private readonly jobs = new Map<string, JobContext>();
   private nextJobId = 0;
   private shuttingDown = false;
   private readonly workerScript: URL;

   constructor(maxWorkers: number, workerScript?: URL) {
      this.maxWorkers = maxWorkers;
      this.workerScript = workerScript ?? resolveWorkerScript();
   }

   get enabled(): boolean {
      return this.maxWorkers > 0;
   }

   get size(): number {
      return this.workers.filter((w) => !w.exited).length;
   }

   async compile(request: CompileRequest): Promise<CompileOutcome> {
      if (!this.enabled) {
         throw new Error(
            "CompileWorkerPool.compile called while disabled (MALLOY_COMPILE_WORKERS=0)",
         );
      }
      if (this.shuttingDown) {
         throw new Error("CompileWorkerPool is shutting down");
      }

      const worker = await this.acquireWorker();
      this.nextJobId += 1;
      const jobId = `job-${this.nextJobId}`;

      return new Promise<CompileOutcome>((resolve, reject) => {
         const timeout = setTimeout(() => {
            this.failJob(
               jobId,
               new Error(
                  `Compile job timed out after ${COMPILE_JOB_TIMEOUT_MS}ms (model=${request.modelPath})`,
               ),
            );
         }, COMPILE_JOB_TIMEOUT_MS);

         this.jobs.set(jobId, {
            jobId,
            connections: request.malloyConfig.connections,
            urlReader: request.urlReader ?? defaultUrlReader,
            resolve: (result) => {
               clearTimeout(timeout);
               resolve(adaptResult(result));
            },
            reject: (err) => {
               clearTimeout(timeout);
               reject(err);
            },
            timeout,
         });

         worker.inFlight.add(jobId);

         const message: CompileJobRequest = {
            type: "compile",
            requestId: jobId,
            packagePath: request.packagePath,
            modelPath: request.modelPath,
            defaultConnectionName: request.defaultConnectionName,
            buildManifest: request.buildManifest,
         };
         worker.worker.postMessage(message);
      });
   }

   /**
    * Drain every in-flight job and terminate the workers. Safe to
    * call multiple times. Awaits each worker's `exit` event.
    */
   async shutdown(): Promise<void> {
      if (this.shuttingDown) return;
      this.shuttingDown = true;
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
   // Internals
   // ────────────────────────────────────────────────────────────────

   private async acquireWorker(): Promise<PoolWorker> {
      // Prune any dead workers from the front so size accounting is
      // honest; spawn lazily up to maxWorkers.
      this.workers.splice(
         0,
         this.workers.length,
         ...this.workers.filter((w) => !w.exited),
      );
      if (this.workers.length < this.maxWorkers) {
         const pw = this.spawnWorker();
         this.workers.push(pw);
         await pw.ready;
         return pw;
      }
      // Least-busy
      const alive = this.workers.filter((w) => !w.exited);
      let best = alive[0];
      for (const candidate of alive) {
         if (candidate.inFlight.size < best.inFlight.size) {
            best = candidate;
         }
      }
      await best.ready;
      return best;
   }

   private spawnWorker(): PoolWorker {
      this.nextWorkerId += 1;
      const id = this.nextWorkerId;
      logger.info(
         `CompileWorkerPool: spawning worker #${id} (script=${this.workerScript.toString()})`,
      );
      const worker = new Worker(this.workerScript, {
         // Worker stderr/stdout flow through the parent's by default;
         // we don't override.
         name: `malloy-compile-worker-${id}`,
      });
      let readyResolve!: () => void;
      const ready = new Promise<void>((r) => (readyResolve = r));

      const pw: PoolWorker = {
         id,
         worker,
         ready,
         inFlight: new Set(),
         exited: false,
      };

      worker.on("message", (msg: WorkerToMainMessage) => {
         this.handleWorkerMessage(pw, msg, readyResolve);
      });

      worker.on("error", (err) => {
         logger.error(`CompileWorkerPool: worker #${id} errored`, {
            error: err,
         });
      });

      worker.on("exit", (code) => {
         pw.exited = true;
         logger.warn(
            `CompileWorkerPool: worker #${id} exited (code=${code}, inFlight=${pw.inFlight.size})`,
         );
         // Fail any jobs that were running on this worker — don't
         // strand callers waiting for a result that will never come.
         for (const jobId of pw.inFlight) {
            this.failJob(
               jobId,
               new Error(
                  `Compile worker #${id} exited unexpectedly (code=${code})`,
               ),
            );
         }
         pw.inFlight.clear();
      });

      return pw;
   }

   private handleWorkerMessage(
      pw: PoolWorker,
      msg: WorkerToMainMessage,
      markReady: () => void,
   ): void {
      switch (msg.type) {
         case "ready":
            markReady();
            return;
         case "compile-result":
            this.completeJob(pw, msg);
            return;
         case "compile-error":
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

   private completeJob(pw: PoolWorker, msg: CompileJobResult): void {
      const ctx = this.jobs.get(msg.requestId);
      if (!ctx) return;
      this.jobs.delete(msg.requestId);
      pw.inFlight.delete(msg.requestId);
      ctx.resolve(msg);
   }

   private errorJob(pw: PoolWorker, msg: CompileJobError): void {
      const ctx = this.jobs.get(msg.requestId);
      if (!ctx) return;
      this.jobs.delete(msg.requestId);
      pw.inFlight.delete(msg.requestId);
      ctx.reject(deserializeError(msg.error));
   }

   private failJob(jobId: string, error: Error): void {
      const ctx = this.jobs.get(jobId);
      if (!ctx) return;
      this.jobs.delete(jobId);
      ctx.reject(error);
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
   modelAnnotation?: Annotation;
}): FetchSchemaOptions {
   const out: FetchSchemaOptions = {};
   if (options.refreshTimestamp !== undefined) {
      out.refreshTimestamp = options.refreshTimestamp;
   }
   if (options.modelAnnotation !== undefined) {
      out.modelAnnotation = options.modelAnnotation;
   }
   return out;
}

// Translate the worker's flat-array filterMap into a Map for use sites.
function adaptResult(result: CompileJobResult): CompileOutcome {
   return {
      modelDef: result.modelDef as ModelDef,
      sourceInfos: result.sourceInfos as Malloy.SourceInfo[],
      sources: result.sources,
      queries: result.queries,
      filterMap: new Map(
         result.filterMap.map(([k, v]) => [k, v as FilterDefinition[]]),
      ),
      givens: result.givens,
      compileDurationMs: result.compileDurationMs,
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

function deserializeError(serialized: SerializedError): Error {
   // Compilation errors retain their identity so callers that
   // catch(MalloyError) / catch(ModelCompilationError) keep working
   // when they live on the main thread. The MalloyError ctor takes
   // a non-trivial argument shape, so we construct a plain Error
   // with `.problems` attached (downstream code reads `.message`
   // and `.problems` on both shapes).
   const err = new Error(serialized.message);
   err.name = serialized.name;
   if (serialized.stack) err.stack = serialized.stack;
   if (serialized.malloyProblems) {
      (err as unknown as { problems: unknown }).problems =
         serialized.malloyProblems;
   }
   if (serialized.isCompilationError) {
      // ModelCompilationError's ctor expects a MalloyError-shaped
      // input but at runtime only reads `.message`. We pass our
      // reconstituted Error (with `.problems` attached) — the cast
      // narrows the constructor's nominal type without losing data.
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

let singleton: CompileWorkerPool | null = null;

export function getCompilePool(): CompileWorkerPool {
   if (singleton === null) {
      const n = getCompileWorkerCount();
      singleton = new CompileWorkerPool(n);
      if (n > 0) {
         logger.info(
            `Malloy compile worker pool enabled (size=${n}). Set MALLOY_COMPILE_WORKERS=0 to disable.`,
         );
      } else {
         logger.info(
            "Malloy compile worker pool DISABLED (MALLOY_COMPILE_WORKERS=0). Compile runs on the main event loop.",
         );
      }
   }
   return singleton;
}

/** Test-only: replace the singleton (and shut down the previous one). */
export async function __setCompilePoolForTests(
   pool: CompileWorkerPool | null,
): Promise<void> {
   if (singleton && singleton !== pool) {
      await singleton.shutdown();
   }
   singleton = pool;
}
