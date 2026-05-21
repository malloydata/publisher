/**
 * Wire protocol between the main thread (CompileWorkerPool) and the
 * compile worker threads. Messages flow in both directions over the
 * worker_threads MessagePort:
 *
 *   main ──▶ worker:  CompileJobRequest      (start a compile)
 *   worker ──▶ main:  CompileJobResult       (success)
 *   worker ──▶ main:  CompileJobError        (failure)
 *
 *   worker ──▶ main:  SchemaForTablesRequest (proxy schema fetch)
 *   worker ──▶ main:  SchemaForSqlRequest    (proxy SQL block schema)
 *   main ──▶ worker:  SchemaForTablesResponse / SchemaForSqlResponse
 *
 *   main ──▶ worker:  ShutdownRequest        (graceful drain & exit)
 *
 * The protocol intentionally uses plain structured-clonable POJOs so
 * `parentPort.postMessage` and `worker.postMessage` can transfer them
 * via V8's structured clone — much cheaper than JSON.stringify for
 * the multi-MB `modelDef` payloads that come back from compile.
 *
 * All requests are correlated by an opaque `requestId` string so the
 * receiver can match responses without relying on FIFO ordering.
 */

import type {
   Annotation,
   SQLSourceDef,
   TableSourceDef,
} from "@malloydata/malloy";

// ──────────────────────────────────────────────────────────────────────
// Direction: main ──▶ worker (compile job)
// ──────────────────────────────────────────────────────────────────────

/**
 * Connection metadata the worker needs to construct a stub
 * `InfoConnection`. Resolved lazily — the worker asks the main thread
 * for these on the first `lookupConnection(name)` call (see
 * {@link ConnectionMetadataRequest}). We don't ship the full list
 * upfront because the caller layer doesn't always know it; Malloy
 * sees connection names only as `connection.table('...')`
 * references inside the model.
 */
export interface ConnectionMetadata {
   name: string;
   dialectName: string;
   digest: string;
}

export interface CompileJobRequest {
   type: "compile";
   requestId: string;
   /** Absolute path to the package directory on disk. */
   packagePath: string;
   /**
    * Path of the model file relative to `packagePath`. Required for
    * file-backed compiles. Omit when supplying {@link inlineSource}.
    */
   modelPath?: string;
   /**
    * Inline Malloy source string to compile in place of reading a file.
    * Used by call sites that synthesize Malloy on the fly (e.g. the
    * per-database schema probe in {@link Package.getDatabaseInfo}). When
    * set, the worker calls `runtime.loadModel(inlineSource, {…})` instead
    * of resolving a file:// URL. `modelPath` should be omitted; the
    * worker will use a synthetic in-memory model id derived from
    * `requestId` for source-info display purposes.
    */
   inlineSource?: string;
   /**
    * Base URL used to resolve `import "…"` statements inside the model.
    * Only meaningful with {@link inlineSource}. For file-backed compiles
    * the worker derives the importBaseURL from the modelPath.
    */
   importBaseURL?: string;
   /** Name of the default connection (e.g. "duckdb"), or null. */
   defaultConnectionName: string | null;
   /** Optional row-build manifest passed through to the Runtime. */
   buildManifest?: unknown;
}

// ──────────────────────────────────────────────────────────────────────
// Direction: worker ──▶ main (compile result)
// ──────────────────────────────────────────────────────────────────────

/**
 * Wire shape of a successful compile. Mirrors the fields the
 * server's `Model` constructor needs to fully describe a `.malloy`
 * file without holding a `ModelMaterializer` reference.
 *
 * The materializer itself is intentionally NOT shipped back — it
 * binds to a Runtime that holds live native connection handles and
 * cannot cross a worker_threads boundary. The main thread builds
 * its own materializer lazily on the first query (see
 * `Model.ensureMaterializer`).
 */
export interface CompileJobResult {
   type: "compile-result";
   requestId: string;
   /** Whatever `await modelMaterializer.getModel()`._modelDef returned. */
   modelDef: unknown;
   /** Source-info entries (from imports + local sources). */
   sourceInfos: unknown[];
   /** Pre-extracted API source descriptors. */
   sources: unknown[];
   /** Pre-extracted API query descriptors. */
   queries: unknown[];
   /** Parsed `#(filter)` map, keyed by source name. */
   filterMap: Array<[string, unknown[]]>;
   /** Givens declared on the model, already in API shape so the main
    *  thread can stash them on the `Model` without further conversion. */
   givens?: unknown[];
   /** Accumulated dataStyles (from HackyDataStylesAccumulator). */
   dataStyles: unknown;
   /** Wall-clock ms inside the worker for the actual compile. */
   compileDurationMs: number;
}

export interface CompileJobError {
   type: "compile-error";
   requestId: string;
   /** Serialized error — the main thread reconstructs an Error. */
   error: SerializedError;
}

/**
 * Error wire-shape. We cannot transfer Error instances directly
 * across postMessage cleanly (Bun/Node behaviour diverges on stack
 * propagation), so we ship a structured payload and reconstitute on
 * the main thread.
 */
export interface SerializedError {
   name: string;
   message: string;
   stack?: string;
   /** Set when the error originated as a Malloy `MalloyError`. */
   malloyProblems?: unknown[];
   /** Set when the error originated as `ModelCompilationError`. */
   isCompilationError?: boolean;
}

// ──────────────────────────────────────────────────────────────────────
// Direction: worker ──▶ main (proxy connection metadata)
// ──────────────────────────────────────────────────────────────────────

export interface ConnectionMetadataRequest {
   type: "connection-metadata";
   requestId: string;
   jobId: string;
   connectionName: string;
}

export interface ConnectionMetadataResponse {
   type: "connection-metadata-response";
   requestId: string;
   ok: true;
   metadata: ConnectionMetadata;
}

// ──────────────────────────────────────────────────────────────────────
// Direction: worker ──▶ main (proxy schema fetches)
// ──────────────────────────────────────────────────────────────────────

export interface SchemaForTablesRequest {
   type: "schema-for-tables";
   requestId: string;
   /** Job this RPC belongs to (so main routes to the right config). */
   jobId: string;
   connectionName: string;
   tables: Record<string, string>;
   options: {
      refreshTimestamp?: number;
      modelAnnotation?: Annotation;
   };
}

export interface SchemaForTablesResponse {
   type: "schema-for-tables-response";
   requestId: string;
   ok: true;
   schemas: Record<string, TableSourceDef>;
   errors: Record<string, string>;
}

export interface SchemaForSqlRequest {
   type: "schema-for-sql";
   requestId: string;
   jobId: string;
   connectionName: string;
   sentence: unknown;
   options: {
      refreshTimestamp?: number;
      modelAnnotation?: Annotation;
   };
}

export interface SchemaForSqlResponse {
   type: "schema-for-sql-response";
   requestId: string;
   ok: true;
   structDef?: SQLSourceDef;
   error?: string;
}

export interface RpcErrorResponse {
   type: "rpc-error";
   requestId: string;
   ok: false;
   error: SerializedError;
}

// ──────────────────────────────────────────────────────────────────────
// Direction: worker ──▶ main (file read for imports)
// ──────────────────────────────────────────────────────────────────────

/**
 * Workers read most files directly via fs (they run in the same
 * filesystem namespace). This RPC exists for the rare case where the
 * package URL reader has host-specific behaviour (e.g. virtual files,
 * remote URLs) — we delegate back to the main thread's URL reader so
 * compile semantics stay identical to the in-process path.
 */
export interface ReadUrlRequest {
   type: "read-url";
   requestId: string;
   jobId: string;
   url: string;
}

export interface ReadUrlResponse {
   type: "read-url-response";
   requestId: string;
   ok: true;
   contents: string;
   invalidationKey?: string | number | null;
}

// ──────────────────────────────────────────────────────────────────────
// Lifecycle
// ──────────────────────────────────────────────────────────────────────

export interface ShutdownRequest {
   type: "shutdown";
}

export interface ReadyMessage {
   type: "ready";
}

// ──────────────────────────────────────────────────────────────────────
// Union types for routing
// ──────────────────────────────────────────────────────────────────────

export type MainToWorkerMessage =
   | CompileJobRequest
   | ConnectionMetadataResponse
   | SchemaForTablesResponse
   | SchemaForSqlResponse
   | ReadUrlResponse
   | RpcErrorResponse
   | ShutdownRequest;

export type WorkerToMainMessage =
   | CompileJobResult
   | CompileJobError
   | ConnectionMetadataRequest
   | SchemaForTablesRequest
   | SchemaForSqlRequest
   | ReadUrlRequest
   | ReadyMessage;
