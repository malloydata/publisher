/**
 * Wire protocol between the main thread (`PackageLoadPool`) and a
 * package-load worker thread.
 *
 * Boundary
 * --------
 * The worker performs the **CPU-bound bulk of `Package.create`** off
 * the main event loop:
 *
 *   1. Read `publisher.config.json` (cheap, but already on the worker
 *      side of the boundary so the main thread isn't blocked).
 *   2. Compile every `.malloy` / `.malloynb` (the Malloy parser,
 *      type-checker, and IR-builder — the dominant CPU cost).
 *   3. Return a structured-clonable POJO carrying every `modelDef`,
 *      `sourceInfos`, dataStyles, etc. that the main thread needs to
 *      reconstitute a live `Package`.
 *
 * Embedded database probing (`.parquet` / `.csv` schema + row count)
 * stays on the main thread — it reuses the package's existing DuckDB
 * connection (PR #772) and the probe queries are async-IO-bound, not
 * CPU-bound. Keeping all native-DB handles on the main thread also
 * sidesteps Bun crash 0x20131 where duckdb-native cannot be loaded
 * into more than one isolate of the same process.
 *
 * The main thread reconstitutes by:
 *   - Building a fresh `MalloyConfig` against its own connection pool
 *     (live native handles can't cross the worker boundary).
 *   - Lazy-hydrating each model's `ModelMaterializer` from `modelDef`
 *     via `Runtime._loadModelFromModelDef` on first query — NO
 *     recompile. This is what closes the loop on PR #767's original
 *     "first-query recompile on main thread" gap.
 *
 * Per-model compile failures are returned in-band on
 * `SerializedModel.compilationError` so a single bad model doesn't
 * abort the rest of the package load. The main thread decides
 * whether/when to surface as a fatal `Package.create` error (today
 * it throws on the first error; `Package.reloadAllModels` keeps the
 * failed models as placeholders in the package's model map).
 *
 * Whole-package failures (manifest missing, FS errors, worker
 * crashes) come back as `LoadPackageError`. The pool main-thread
 * half (`PackageLoadPool.loadPackage`) rejects with a deserialised
 * Error; `Package.loadViaWorker` then rewraps any non-compile
 * failure as `ServiceUnavailableError` so the HTTP layer responds
 * 503 (transient, retryable) — there is no in-process fallback.
 *
 * Direction summary
 * -----------------
 *   main  ──▶ worker:  LoadPackageRequest         (start)
 *   worker ──▶ main:   LoadPackageResult          (success)
 *   worker ──▶ main:   LoadPackageError           (whole-package failure)
 *
 *   worker ──▶ main:   ConnectionMetadataRequest  (proxy non-duckdb lookups)
 *   worker ──▶ main:   SchemaForTablesRequest     (proxy schema fetch)
 *   worker ──▶ main:   SchemaForSqlRequest        (proxy SQL block schema)
 *   worker ──▶ main:   ReadUrlRequest             (proxy non-file URL reads)
 *   main  ──▶ worker:  *Response                  (correlated by requestId)
 *
 *   main  ──▶ worker:  ShutdownRequest            (graceful drain)
 *   worker ──▶ main:   ReadyMessage               (post-init handshake)
 *
 * The protocol uses plain structured-clonable POJOs so the
 * `postMessage` transfer goes through V8's structured clone — much
 * cheaper than `JSON.stringify` for the multi-MB modelDef payloads.
 */

import type { SQLSourceDef, TableSourceDef } from "@malloydata/malloy";

// ──────────────────────────────────────────────────────────────────────
// Direction: main ──▶ worker (load-package job)
// ──────────────────────────────────────────────────────────────────────

/**
 * Connection metadata the worker needs to construct a stub
 * `InfoConnection`. Resolved lazily — the worker asks the main thread
 * on the first `lookupConnection(name)` call (see
 * {@link ConnectionMetadataRequest}). We don't ship the full list
 * upfront because Malloy only references connections by name as it
 * encounters `<connection>.table('...')` / `<connection>.sql('...')`
 * inside the model.
 */
export interface ConnectionMetadata {
   name: string;
   dialectName: string;
   digest: string;
}

export interface LoadPackageRequest {
   type: "load-package";
   requestId: string;
   /** Absolute path to the package directory on disk. */
   packagePath: string;
   /** Logical package name (used in metric labels + log fields). */
   packageName: string;
   /**
    * Default connection name (passed verbatim to the worker; today
    * always `"duckdb"` for embedded packages, but kept configurable
    * to mirror Malloy's own surface).
    */
   defaultConnectionName: string | null;
   /** Optional row-build manifest passed through to Malloy Runtime. */
   buildManifest?: unknown;
}

// ──────────────────────────────────────────────────────────────────────
// Direction: worker ──▶ main (load-package result)
// ──────────────────────────────────────────────────────────────────────

/**
 * Wire shape for one compiled model in the package. Mirrors the
 * data a main-thread `Model` constructor needs without holding a
 * `ModelMaterializer` reference (that binds to live native
 * connection handles and can't cross the worker boundary).
 *
 * `compilationError` is set when this single model failed to
 * compile but the rest of the package is fine; the main thread
 * decides whether to abort `Package.create`.
 */
export interface SerializedModel {
   /** Path relative to the package root, forward-slash normalized. */
   modelPath: string;
   modelType: "model" | "notebook";
   /** Set when the model compiled successfully. Wire-typed as
    *  `unknown` so the protocol module doesn't drag in the full
    *  Malloy type surface; cast to `ModelDef` on receipt. */
   modelDef?: unknown;
   /**
    * Precomputed `modelDefToModelInfo(modelDef)`. Shipped from the
    * worker so the main-thread `Model` constructor doesn't pay the
    * derivation cost on every package load and every subsequent
    * `getModel()` / `getNotebook()` API hit can stringify a cached
    * object instead of recomputing.
    */
   modelInfo?: unknown;
   sourceInfos?: unknown[];
   sources?: unknown[];
   queries?: unknown[];
   filterMap?: Array<[string, unknown[]]>;
   givens?: unknown[];
   /** Notebook (.malloynb) only — per-cell pre-extracted info. */
   notebookCells?: SerializedNotebookCell[];
   /** Accumulated dataStyles from sibling `.styles.json` files. */
   dataStyles?: unknown;
   /** Wall-clock ms spent compiling this single model in the worker. */
   compileDurationMs?: number;
   /** Set when the model failed to compile. */
   compilationError?: SerializedError;
}

export interface SerializedNotebookCell {
   type: "code" | "markdown";
   /** Raw cell text. */
   text: string;
   /**
    * Per-cell ModelDef captured at the cell's point in the
    * `extendModel` chain. The main thread hydrates a per-cell
    * `ModelMaterializer` from this via
    * `Runtime._loadModelFromModelDef`, so cell-level filter
    * refinement can compile new queries against the correct scope
    * without ever recompiling the .malloynb itself.
    */
   cellModelDef?: unknown;
   /**
    * The final-query QueryDef for this cell, captured during the
    * worker's compile. Main thread hydrates a `QueryMaterializer`
    * via `ModelMaterializer._loadQueryFromQueryDef` — no recompile.
    */
   cellQueryDef?: unknown;
   newSources?: unknown[];
   queryInfo?: unknown;
}

export interface LoadPackageResult {
   type: "load-package-result";
   requestId: string;
   packageMetadata: {
      name?: string;
      description?: string;
      explores?: string[];
   };
   models: SerializedModel[];
   /** Wall-clock ms inside the worker for the full package load. */
   loadDurationMs: number;
}

export interface LoadPackageError {
   type: "load-package-error";
   requestId: string;
   error: SerializedError;
}

/**
 * Error wire-shape. We cannot transfer `Error` instances directly
 * across `postMessage` cleanly (Bun/Node behaviour diverges on stack
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
// Direction: worker ──▶ main (proxy schema fetches for non-duckdb)
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
// Direction: worker ──▶ main (file read for non-file URLs)
// ──────────────────────────────────────────────────────────────────────

/**
 * Workers read most files directly via `fs` (they share the host's
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
   | LoadPackageRequest
   | ConnectionMetadataResponse
   | SchemaForTablesResponse
   | SchemaForSqlResponse
   | ReadUrlResponse
   | RpcErrorResponse
   | ShutdownRequest;

export type WorkerToMainMessage =
   | LoadPackageResult
   | LoadPackageError
   | ConnectionMetadataRequest
   | SchemaForTablesRequest
   | SchemaForSqlRequest
   | ReadUrlRequest
   | ReadyMessage;
