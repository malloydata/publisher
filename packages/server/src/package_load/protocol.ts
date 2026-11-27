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
 *   main  ──▶ worker:  LoadPackageRequest, CompileServedCopyRequest,
 *                      ShutdownRequest, *Response (correlated by requestId)
 *   worker ──▶ main:   LoadPackageResult | LoadPackageError,
 *                      CompileServedCopyResult, ReadyMessage,
 *                      WorkerLogMessage, and the proxy requests
 *                      (connection metadata, schema fetches, non-file URLs)
 *
 * Messages are structured-cloned, which is cheaper than JSON for multi-MB
 * modelDefs and PRESERVES INTERNAL ALIASING. `#(authorize)` enforcement
 * depends on that: an inherited gate is recognized by its note object being
 * shared by reference, so a JSON transport would turn every such check into a
 * fail-closed deny.
 */

import type { SerializedNotebookCell } from "./notebook_cell_wire";
import type { PackageAnnotationFinding } from "../service/annotation_reader";
import type {
   EligibilityRefusals,
   PackagePlan,
} from "../service/materialization/domain/package_plan";
import type { SQLSourceDef, TableSourceDef } from "@malloydata/malloy";
import type {
   PackageMaterializationConfig,
   PackageScope,
} from "../service/package_manifest";
import type { PackageRetrievalSettings } from "../service/package_retrieval_settings";
import type { ApiPackageWarning } from "../service/classification";
import type { DeprecatedManifestKey } from "../service/deprecated/manifest_keys";
import type { ModelEntitiesPlan } from "../service/entities/plan";
import type {
   UpstreamSchemaReadings,
   UpstreamTable,
} from "../service/materialization/domain/upstream_schemas";
import type {
   TargetContainer,
   TargetContainerReadings,
} from "../service/materialization/domain/target_containers";
import type { PackageWorkflowsWire } from "../service/skills/package_workflows";

/**
 * Version of this wire protocol. The worker stamps it on every
 * {@link LoadPackageResult}, and the pool refuses a result carrying any other
 * value (see `checkLoadPackageResult`), so a worker bundle built from another
 * revision fails its load loudly instead of being hydrated through a blind
 * cast. Bump it whenever a change to these shapes would make an older or newer
 * peer misread a message.
 *
 * 2: `SerializedModel.entities`, the model's entities plan
 * (`LoadPackageRequest.withEntities`).
 *
 * 3: `SerializedModel.governsAccess`.
 *
 * 4: every field of a build plan's outcome is sent, so
 * the pool reads none as missing.
 *
 * 5: the served-copy compile job ({@link CompileServedCopyRequest}).
 *
 * 6: the build plan is sent as its `PackagePlan` (its tables, builds and
 * addresses as data), and the pre-aggregation companion fields are gone.
 *
 * 7: a refreshing load (`LoadPackageRequest.refreshTimestamp`), the schema
 * keys it refetched (`LoadPackageResult.refreshedSchemaKeys`), and
 * {@link ForgetSchemasMessage}.
 *
 * 8: the upstream-schema read ({@link UpstreamSchemasRequest}), and a build
 * plan's `upstreamSchemas`.
 *
 * 9: the target-container read ({@link TargetContainersRequest}).
 *
 * 10: a served-copy compile's `virtualMap`
 * ({@link CompileServedCopyRequest.virtualMap}).
 *
 * 11: the skills walk ({@link LoadPackageRequest.readWorkflows}): the walk as
 * wire data, or why it could not finish (`unavailable`), the whole `workflows`
 * value's refusal (`manifestRefusal`) and each listed file's `binary` flag
 * ({@link WorkflowsRead}, `workflowsRead`), and the content shas of the serving
 * identity (`modelShas`), which replace `packageMetadata.workflows`.
 */
export const PACKAGE_LOAD_PROTOCOL_VERSION = 11;

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
   /**
    * The connection's configured type (`postgres`, `duckdb`, `ducklake`, ...),
    * which the build plan's `storage=` eligibility reads; absent when the main
    * thread recorded none.
    */
   connectionType?: string;
   /** The schemas SQL on the connection may name; present only when it is configured `managed`. */
   managedSchemas?: string[];
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
   /**
    * More in-memory files for a package compile dry-run (a request's `files`
    * overlay), read the same way as `replacement`.
    */
   overlay?: { modelPath: string; source: string }[];
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
    * thread (render-tag results, entities, build plan). Checked before each
    * render-tag target and each model's plan parts; past it, the
    * remaining work (and the plan, once any model's parts are missing) is left
    * to the main thread. The pool's jobLimitMs adds room for it.
    */
   mainThreadWorkBudgetMs: number;
   /**
    * The package is a published version (its version owns its runs' tables
    * when its scope is `"version"`), so its tables take readable names whose
    * hash covers the version, which decides which sources share a table.
    * Absent: unversioned, whose `name=`s are written as given.
    */
   publishedVersion?: boolean;
   /**
    * Plan each `.malloy` model's entities for the main thread to resolve (see
    * SerializedModel.entities). Set by the loads that compute the package's
    * entities: a load that serves it, and the package-scope compile.
    */
   withEntities?: boolean;
   /**
    * Set by an explicit refresh (a reload, a publish): every table and SQL
    * schema the load reads is fetched again, once, with this stamp as its
    * `refreshTimestamp`, replacing the worker's cached entry and the main
    * thread connection's. Absent, the load reads cached schemas.
    */
   refreshTimestamp?: number;
   /**
    * Walk the package's workflows and skill sets, and hash the files its
    * serving identity covers, in this job (`workflowsRead`, `modelShas`): the
    * main thread then only adopts them. Set by the loads that install a
    * package; every other job skips the walk.
    */
   readWorkflows?: true;
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
   /** Set when the model failed to compile. */
   compilationError?: SerializedError;
   /**
    * Non-fatal gate findings at load (`GateLoadWarning`), logged by the main
    * thread and carried on the package's warnings: a gate inherited (judged
    * by shared note object, not by text) at an entry point where it cannot be
    * expressed, so that entry point denies every request
    * (`Model.resolveGateShape`), or a source-line gate that reads no given or
    * negates a membership test. The model still serves.
    */
   gateWarnings?: { sourceName: string; code: string; message: string }[];
   /**
    * Prepared `Malloy.Result`s for the model's render-tag targets, for the
    * main thread's tag validation (the renderer cannot load in the worker,
    * and preparing is the expensive part). One entry per target reached, in
    * order; `result` absent means it failed to prepare. The main thread
    * prepares any targets after the last entry itself.
    */
   renderTagResults?: { label: string; result?: unknown }[];
   /**
    * SHA-256 of `renderTagResults` as JSON, set only when they cover every
    * render-tag target. The results are the whole input of the tag check, so
    * the main thread keys its findings cache on this digest and skips the
    * check for a model it has already judged (ms2data/service#7795). It
    * covers what model text alone would miss: a table whose schema changed
    * under the same text prepares different results.
    */
   renderTagResultsDigest?: string;
   /**
    * The model's entities, planned before its sources' queryability is known
    * (`service/entities/plan.ts`); the main thread resolves it against the
    * query route. Absent when not asked for (`withEntities`), when the model
    * did not compile, or when the work budget ran out first: the main thread
    * then plans it itself.
    */
   entities?: ModelEntitiesPlan;
   /**
    * Whether `modelDef` governs access (`governsAccess` in
    * governance/ir/array_query.ts), answered here so the main thread never
    * walks a model that governs nothing; it notes this on the hydrated
    * `modelDef`. Absent when the model did not compile.
    */
   governsAccess?: boolean;
}

export type { SerializedNotebookCell } from "./notebook_cell_wire";

/** The package's workflows as the worker walked them: what the serving identity installs and hashes, with no file left for the main thread to read. */
export interface WorkflowsRead {
   /** The sha256 of the manifest's canonical `workflows` declaration; absent when it declares none. */
   declarationSha?: string;
   walk: PackageWorkflowsWire;
}

export interface LoadPackageResult {
   type: "load-package-result";
   requestId: string;
   /** The sender's {@link PACKAGE_LOAD_PROTOCOL_VERSION}. */
   protocolVersion: number;
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
      /** Findings with a code the manifest raised (a key an earlier release read). */
      manifestFindings?: ApiPackageWarning[];
      /** The manifest's workflows, read; present only for a job with `readWorkflows`. */
      workflowsRead?: WorkflowsRead;
   };
   models: SerializedModel[];
   /** Whether the replacement path exactly matched an enumerated package file. */
   replacementMatchedExisting?: boolean;
   /**
    * A refreshing load's refetched schema-cache keys, which the pool tells
    * the other workers to forget (see {@link ForgetSchemasMessage}).
    */
   refreshedSchemaKeys?: string[];
   /**
    * The sha256 of every `file:` text this load's compiles read, failed
    * compiles' included, by normalized URL (`job_files.ts` `urlKey`). A
    * reload keeps a file's last good model only when every file that model
    * read is unchanged since it compiled, so a recompile that fails on an
    * author's edit is never mistaken for one that failed on a dropped table.
    */
   fileDigests?: Record<string, string>;
   /**
    * The sha256 of each model file's text this load's compile read, by model
    * path, `UNREADABLE_SHA` for one it could not read; present only for a job
    * with `readWorkflows`, and for every model it compiled.
    */
   modelShas?: Map<string, string>;
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
      /** Translations the load stored for its models' imports (#1566). */
      translationsStored?: number;
      /** Imports answered from a translation the load had stored. */
      translationsReused?: number;
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
 * An unhandled promise rejection caught in the worker, reported so the main
 * thread logs it and counts it in `publisher_unhandled_rejection_total`. The
 * worker's own logger has no job context in a rejection listener (Bun's
 * AsyncLocalStorage is empty there), so a log written in place would carry no
 * job and reach only the worker's stdout.
 */
export interface WorkerUnhandledRejectionMessage {
   type: "unhandled-rejection";
   message: string;
   stack?: string;
}

/**
 * The package build plan derived from this compile, so the main thread need
 * not compile again. `durationMs` is a ceiling (other compiles interleave). A
 * failure fails the plan, not the load.
 */
export type WorkerBuildPlan =
   | {
        ok: true;
        /** Its eligibility refusals are counted on it (`eligibilityRefused`). */
        packagePlan: PackagePlan<PackageAnnotationFinding>;
        durationMs: number;
        /** Connections whose digest could not be read (see resolveConnectionDigests). */
        digestSkipped: string[];
     }
   | {
        ok: false;
        error: string;
        durationMs: number;
        /** Connections whose digest could not be read before it failed. */
        digestSkipped: string[];
        /** Refusals counted before the derivation failed. */
        eligibilityRefused: EligibilityRefusals;
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
// The served-copy compile job (main ──▶ worker ──▶ main)
// ──────────────────────────────────────────────────────────────────────

/**
 * Compile one pass of a model's served copy (served_copy_serve.ts): the
 * package's model over the re-rooted files, read in place of the package's
 * own. The main thread writes the files and hydrates the `modelDef` that
 * comes back; connections are proxied, as for a package load, against the
 * served copy's own connections (the package's, and its storage
 * destinations under their aliases).
 */
export interface CompileServedCopyRequest {
   type: "compile-served-copy";
   requestId: string;
   /** Absolute path to the package directory on disk. */
   packagePath: string;
   /** Scopes the worker's schema cache, as {@link LoadPackageRequest.environmentName}. */
   environmentName: string;
   /** The model the copy is of, relative to the package. */
   modelPath: string;
   /** The connection an unnamed lookup resolves to. */
   defaultConnectionName: string | null;
   /** The re-rooted files, each keyed by its overlay key (`overlay_key.ts`). */
   files: [string, string][];
   /**
    * The copy's `virtualMap` (connection -> handle -> table path), which a
    * `.sql()` source's `%{}` of a re-rooted source reads at compile time.
    */
   virtualMap?: [string, [string, string][]][];
}

/**
 * A served-copy compile's outcome: the compiled `modelDef`, or the compile
 * failure, in-band so the main thread reads it as the inline compile's
 * failure (a `MalloyError` keeps its problems). A failure of the job itself
 * (a timeout, a crash) rejects instead.
 */
export interface CompileServedCopyResult {
   type: "compile-served-copy-result";
   requestId: string;
   /** The sender's {@link PACKAGE_LOAD_PROTOCOL_VERSION}. */
   protocolVersion: number;
   /** Set when the copy compiled. A `ModelDef`, wire-typed `unknown`. */
   modelDef?: unknown;
   /** Set when it did not. */
   error?: SerializedError;
   /** Wall-clock ms the compile took in the worker. */
   compileDurationMs: number;
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

// ──────────────────────────────────────────────────────────────────────
// Direction: worker ──▶ main (the build plan's upstream column types)
// ──────────────────────────────────────────────────────────────────────

/**
 * Read the native column types of the tables a build plan's sources read,
 * through the main thread's connections (a worker runs no SQL), refetched
 * under a refreshing load.
 */
export interface UpstreamSchemasRequest {
   type: "upstream-schemas";
   requestId: string;
   jobId: string;
   tables: UpstreamTable[];
}

export interface UpstreamSchemasResponse {
   type: "upstream-schemas-response";
   requestId: string;
   ok: true;
   readings: UpstreamSchemaReadings;
}

/**
 * Read whether the containers a build plan's database tables are placed in
 * exist, through the main thread's connections (a worker runs no SQL).
 */
export interface TargetContainersRequest {
   type: "target-containers";
   requestId: string;
   jobId: string;
   containers: TargetContainer[];
}

export interface TargetContainersResponse {
   type: "target-containers-response";
   requestId: string;
   ok: true;
   readings: TargetContainerReadings;
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

/**
 * Drop these schema-cache entries: another worker refetched them for a
 * refreshing load, so this worker's copies may be stale.
 */
export interface ForgetSchemasMessage {
   type: "forget-schemas";
   keys: string[];
}

export interface ReadyMessage {
   type: "ready";
}

// ──────────────────────────────────────────────────────────────────────
// Union types for routing
// ──────────────────────────────────────────────────────────────────────

export type MainToWorkerMessage =
   | LoadPackageRequest
   | CompileServedCopyRequest
   | ConnectionMetadataResponse
   | SchemaForTablesResponse
   | SchemaForSqlResponse
   | UpstreamSchemasResponse
   | TargetContainersResponse
   | ReadUrlResponse
   | RpcErrorResponse
   | ShutdownRequest
   | ForgetSchemasMessage;

export type WorkerToMainMessage =
   | LoadPackageResult
   | CompileServedCopyResult
   | LoadPackageError
   | ConnectionMetadataRequest
   | SchemaForTablesRequest
   | SchemaForSqlRequest
   | UpstreamSchemasRequest
   | TargetContainersRequest
   | ReadUrlRequest
   | WorkerLogMessage
   | WorkerUnhandledRejectionMessage
   | ReadyMessage;
