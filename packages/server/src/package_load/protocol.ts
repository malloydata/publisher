// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Wire protocol between the main thread (`PackageLoadPool`) and a
 * package-load worker thread.
 *
 * The worker does the CPU-bound bulk of `Package.create` off the main event
 * loop: read the manifest, compile every `.malloy` / `.malloynb`, and return
 * a structured-clonable POJO with everything the main thread needs to
 * reconstitute a live `Package`. The main thread builds its own
 * `MalloyConfig` (native handles cannot cross the boundary) and hydrates each
 * model from `modelDef` on first query, never recompiling.
 *
 * Embedded database probing stays on the main thread: it is IO-bound, and
 * duckdb-native cannot be loaded into more than one isolate of a process
 * (Bun crash 0x20131).
 *
 * Per-model compile failures come back in-band on
 * `SerializedModel.compilationError`; whole-package failures come back as
 * `LoadPackageError`, which `Package.loadViaWorker` rewraps as a 503 (there
 * is no in-process fallback).
 *
 *   main  ──▶ worker:  LoadPackageRequest, ShutdownRequest,
 *                      DropCompileCacheRequest,
 *                      *Response (correlated by requestId)
 *   worker ──▶ main:   LoadPackageResult | LoadPackageError, ReadyMessage,
 *                      WorkerLogMessage, and the proxy requests
 *                      (connection metadata, schema fetches, non-file URLs)
 *
 * Messages are structured-cloned, which is cheaper than JSON for multi-MB
 * modelDefs and PRESERVES INTERNAL ALIASING. `#(authorize)` enforcement
 * depends on that: an inherited gate is recognized by its note object being
 * shared by reference, so a JSON transport would turn every such check into a
 * fail-closed deny.
 */

import type { GateFindingCounts } from "../authorize_metrics";
import type { EligibilityRefusalReason } from "../materialization_metrics";
import type { BuildPlanOutcome } from "../service/build_plan";
import type { SQLSourceDef, TableSourceDef } from "@malloydata/malloy";
import type {
   PackageMaterializationConfig,
   PackageScope,
} from "../service/package_manifest";
import type { PackageRetrievalSettings } from "../service/package_retrieval";
import type { DeprecatedManifestKey } from "../service/deprecated/manifest_keys";

// ──────────────────────────────────────────────────────────────────────
// Direction: main ──▶ worker (load-package job)
// ──────────────────────────────────────────────────────────────────────

/**
 * Connection metadata the worker needs to construct a stub `InfoConnection`,
 * requested lazily on the first `lookupConnection(name)` (see
 * {@link ConnectionMetadataRequest}).
 */
export interface ConnectionMetadata {
   name: string;
   dialectName: string;
   digest: string;
   /**
    * Identifies the main thread's connection instance, so the worker's schema
    * cache is dropped when an edit replaces it. The digest alone does not
    * change on every edit.
    */
   generation: number;
}

export interface LoadPackageRequest {
   type: "load-package";
   requestId: string;
   /** Absolute path to the package directory on disk. */
   packagePath: string;
   /** Logical package name (used in metric labels + log fields). */
   packageName: string;
   /**
    * The environment the package loads into. Scopes the worker's schema
    * cache, so environments never share schemas.
    */
   environmentName: string;
   /** Default connection name, passed verbatim to Malloy. */
   defaultConnectionName: string | null;
   /** Optional row-build manifest passed through to Malloy Runtime. */
   buildManifest?: unknown;
   /**
    * In-memory replacement for a package compile dry-run: this text is read
    * instead of the named file (or added, if the path is new), without
    * touching disk.
    */
   replacement?: { modelPath: string; source: string };
   /** Include non-fatal compiler diagnostics in SerializedModel results. */
   collectProblems?: boolean;
   /**
    * Derive the package's persist build plan from this compile and return it
    * as `LoadPackageResult.buildPlan`. Only meaningful without a
    * `buildManifest`: the plan describes the canonical build, not SQL a bound
    * manifest has rewritten.
    */
   computeBuildPlan?: boolean;
   /**
    * How long into the job the worker keeps doing optional work for the main
    * thread (render-tag results, pre-aggregation companions, build plan).
    * Checked before each render-tag target and each companion; past it, the
    * remaining work (and the plan, once any model's parts are missing) is left
    * to the main thread. The pool's jobLimitMs adds room for it.
    */
   mainThreadWorkBudgetMs: number;
   /**
    * Compile each model's pre-aggregation companion for the main thread to
    * hydrate (see SerializedModel.preaggregateCompanion). Set by the loads
    * that serve the package; a compile preview does not serve it.
    */
   withPreaggregateCompanions?: boolean;
   /**
    * Identifies the connections the package compiles against, and moves on a
    * schema refresh; the worker empties the package's compile cache when it
    * changes.
    */
   connectionsEpoch: number;
   /**
    * Set on the one load that follows a schema refresh: every schema the job
    * asks for is refetched from the warehouse (as a `refreshTimestamp` fetch),
    * past the main thread's connection cache too, and replaces the worker's entry.
    */
   refreshSchemaAt?: number;
}

// ──────────────────────────────────────────────────────────────────────
// Direction: worker ──▶ main (load-package result)
// ──────────────────────────────────────────────────────────────────────

/**
 * Wire shape for one compiled model: what a main-thread `Model` constructor
 * needs, minus the `ModelMaterializer`, which binds to native handles.
 */
export interface SerializedModel {
   /** Path relative to the package root, forward-slash normalized. */
   modelPath: string;
   modelType: "model" | "notebook";
   /** Set when the model compiled. A `ModelDef`, wire-typed `unknown`. */
   modelDef?: unknown;
   /** Precomputed `modelDefToModelInfo(modelDef)`, so the main thread does not derive it. */
   modelInfo?: unknown;
   sourceInfos?: unknown[];
   sources?: unknown[];
   queries?: unknown[];
   filterMap?: Array<[string, unknown[]]>;
   givens?: unknown[];
   /** Notebook (.malloynb) only — per-cell pre-extracted info. */
   notebookCells?: SerializedNotebookCell[];
   /**
    * The model file's text EXACTLY as this compile read it, so `get_context`
    * can slice view definitions at the IR's `DocumentLocation` coordinates.
    * Re-reading the file is not equivalent: a stale package's coordinates
    * belong to the pre-edit text. `.malloy` models only.
    */
   modelSourceText?: string;
   /** Accumulated dataStyles from sibling `.styles.json` files. */
   dataStyles?: unknown;
   /** Wall-clock ms spent compiling this single model in the worker. */
   compileDurationMs?: number;
   /** Non-fatal compiler diagnostics emitted while compiling this model. */
   problems?: unknown[];
   /** This model's compile was served from the worker's compile cache. */
   compileCacheHit?: boolean;
   /** What the load derived from the ModelDef was reused from an earlier load. */
   derivationReused?: boolean;
   /** Gate findings the compile counted, for the main thread to record (the worker's meter is a no-op). */
   gateFindings?: GateFindingCounts;
   /** Set when the model failed to compile. */
   compilationError?: SerializedError;
   /**
    * Non-fatal `#(authorize)` load-time findings, logged by the main thread:
    * a gate inherited (judged by shared note object, not by text) at an entry
    * point where it cannot be expressed. The model still serves; that entry
    * point denies every request (`Model.resolveGateShape`).
    */
   authorizeWarnings?: string[];
   /**
    * Prepared `Malloy.Result`s for the model's render-tag targets, for the
    * main thread's tag validation (the renderer cannot load in the worker,
    * and preparing is the expensive part). One entry per target reached, in
    * order; `result` absent means it failed to prepare. The main thread
    * prepares any targets after the last entry itself.
    */
   renderTagResults?: { label: string; result?: unknown }[];
   /**
    * The model's pre-aggregation companion, for the main thread to hydrate
    * rather than compile (see Model.buildPreaggregateServeModel). `{}` when
    * there is none or it failed; absent when not attempted, and the main
    * thread compiles it.
    */
   preaggregateCompanion?: { modelDef?: unknown };
}

export interface SerializedNotebookCell {
   type: "code" | "markdown";
   /** Raw cell text. */
   text: string;
   /**
    * The cell's ModelDef at its point in the `extendModel` chain, hydrated by
    * the main thread so cell-level queries compile in the right scope.
    */
   cellModelDef?: unknown;
   /** The cell's final QueryDef, hydrated by the main thread without recompiling. */
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
      location?: string;
      explores?: string[];
      queryableSources?: "declared" | "all";
      manifestLocation?: string | null;
      materialization?: PackageMaterializationConfig | null;
      scope?: PackageScope;
      /** Manifest-shape deprecations the load tolerated (e.g. a root-level `scope`). */
      manifestWarnings?: string[];
      /** Deprecated publisher.json keys the manifest declares, at any value. */
      deprecatedManifestKeys?: DeprecatedManifestKey[];
      /** The manifest's `retrieval` block, validated, with prompt files read. */
      retrieval?: PackageRetrievalSettings;
      /** The manifest's `agents` value, unvalidated: the main thread validates it and warns instead of throwing. */
      agents?: unknown;
   };
   models: SerializedModel[];
   /** Whether the replacement path exactly matched an enumerated package file. */
   replacementMatchedExisting?: boolean;
   /** Wall-clock ms inside the worker for the full package load. */
   loadDurationMs: number;
   /**
    * Per-load timing breakdown (subset of {@link loadDurationMs}; the
    * remainder is worker setup + post-compile extraction). See
    * `rpc_wait_accountant.ts` for how compile time is separated from I/O.
    */
   timings: {
      /**
       * Compile-region wall-clock minus time awaiting proxied schema fetches:
       * a ceiling on compile CPU. Includes the work prepared for the main
       * thread, not the build plan's final derivation.
       */
      compileDurationMs: number;
      /** Wall-clock awaiting proxied connection schema fetches (union). */
      schemaFetchDurationMs: number;
      /** Number of proxied connection schema fetches the load drove. */
      schemaFetchCount: number;
      /** Schema requests answered from the worker's own cache. */
      schemaCacheHits?: number;
      /** Models whose compile was served from the package's compile cache. */
      compileCacheHits?: number;
   };
   /**
    * Present when `computeBuildPlan` was set and every model compiled and
    * reached its companion before `mainThreadWorkBudgetMs`; otherwise the
    * main thread derives the plan.
    */
   buildPlan?: WorkerBuildPlan;
}

/**
 * A log entry written while a job ran in the worker, forwarded as written so
 * it reaches the main thread's OpenTelemetry pipeline even if the job later
 * times out or crashes.
 */
export interface WorkerLogMessage {
   type: "log";
   jobId: string;
   /** The package the job loads, added to the entry's meta. */
   packagePath: string;
   level: string;
   message: string;
   /** The entry's other fields, an error's message and stack among them. */
   meta: Record<string, unknown>;
}

/**
 * The package build plan derived from this compile, so the main thread need
 * not compile again. `durationMs` is a ceiling (other compiles interleave). A
 * failure fails the plan, not the load.
 */
export type WorkerBuildPlan =
   | {
        ok: true;
        outcome: BuildPlanOutcome;
        durationMs: number;
        /** Connections whose digest could not be read (see resolveConnectionDigests). */
        digestSkipped: string[];
        /** Eligibility refusals counted while deriving it, by reason. */
        eligibilityRefused: Partial<Record<EligibilityRefusalReason, number>>;
     }
   | {
        ok: false;
        error: string;
        durationMs: number;
        /** Connections whose digest could not be read before it failed. */
        digestSkipped: string[];
        /** Refusals counted before the derivation failed. */
        eligibilityRefused: Partial<Record<EligibilityRefusalReason, number>>;
     };

export interface LoadPackageError {
   type: "load-package-error";
   requestId: string;
   error: SerializedError;
}

/**
 * Error wire shape: `Error` instances do not cross `postMessage` reliably
 * (Bun and Node differ on the stack), so a payload is reconstituted.
 */
export interface SerializedError {
   name: string;
   message: string;
   stack?: string;
   /** Set when the error originated as a Malloy `MalloyError`. */
   malloyProblems?: unknown[];
   /** Set when the error originated as `ModelCompilationError`. */
   isCompilationError?: boolean;
   /** Set when the error originated as `PackageManifestError`. */
   isManifestError?: boolean;
   /** Errno fields, so the receiver can recognize a refused filesystem access. */
   errno?: SerializedErrno;
}

export interface SerializedErrno {
   code: string;
   syscall?: string;
   path?: string;
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
 * Workers read most files directly via `fs`; this RPC delegates non-file URLs
 * to the main thread's URL reader so compile semantics stay identical.
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

/** Drop a package's compile cache: the package was unloaded. */
export interface DropCompileCacheRequest {
   type: "drop-compile-cache";
   environmentName: string;
   packageName: string;
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
   | ShutdownRequest
   | DropCompileCacheRequest;

export type WorkerToMainMessage =
   | LoadPackageResult
   | LoadPackageError
   | ConnectionMetadataRequest
   | SchemaForTablesRequest
   | SchemaForSqlRequest
   | ReadUrlRequest
   | WorkerLogMessage
   | ReadyMessage;
