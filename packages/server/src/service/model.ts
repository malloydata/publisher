// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   EMPTY_BUILD_MANIFEST,
   GivenValue,
   isSourceDef,
   MalloyConfig,
   MalloyError,
   Annotations,
   ModelDef,
   ModelMaterializer,
   NamedQueryDef,
   QueryData,
   QueryMaterializer,
} from "@malloydata/malloy";
import * as Malloy from "@malloydata/malloy-interfaces";
import { DataStyles } from "@malloydata/render";
import { lazyCounter, lazyHistogram, withSpan } from "../telemetry";
import { recordStorageServeRouting } from "../materialization_metrics";
import { recordVariantRouting, routeQuery } from "./query_router";
import {
   answeredWith,
   type Answer,
   canDegradeToLive,
   LIVE_ANSWER,
   liveFallbackAnswer,
   queryPathFor,
   type QueryServingHost,
   recordingManifest,
   recordRoutingDecision,
   routeCandidates,
   routedAnswer,
   type RoutedExtra,
   type ServedFrom,
   servedFromOf,
} from "./query_serving";
import * as fs from "fs/promises";
import { createRequire } from "module";
import * as path from "path";
import { fileURLToPath } from "url";
import { components } from "../api";
import {
   getMaxQueryRows,
   getMaxResponseBytes,
   getQueryMetadataMode,
} from "../config";
import {
   GIVENS_DOCS_URL,
   INDEX_MODEL_NAME,
   NOTEBOOK_FILE_SUFFIX,
} from "../constants";
import {
   AccessDeniedError,
   BadRequestError,
   InvalidQueryRequestError,
   ConnectionError,
   ModelCompilationError,
   ModelNotFoundError,
   NotQueryableError,
   OffSurfaceError,
   PayloadTooLargeError,
} from "../errors";
import { getMaterializeEnabled, getPersistStorageMode } from "../config";
import { keyedOverlay } from "./overlay_key";
import {
   compiledFileText,
   loadLeavingOutCrossConnectionJoins,
   type LeftOutDefinition,
} from "./materialization/plan/cross_connection_joins";
import { type RenderTagTarget, renderTagTargets } from "./render_tag_targets";
import { modelInfoOf } from "./model_info";
import { logger } from "../logger";
import {
   accessPathOf,
   summarizeQueryAccess,
   type QueryAudit,
} from "./access_record";
import { type ServeBinding } from "./materialization/serve/materialization_serve_transform";
import { deserializeError } from "../package_load/package_load_pool";
import type { SerializedModel } from "../package_load/protocol";
import { decodeSerializedModel } from "../package_load/protocol_decode";
import { BuildManifest } from "./materialization/domain/types";
import {
   modelAnnotations,
   ownModelAnnotations,
   ownModelNoteObjects,
   ownModelNotes,
} from "./annotations";
import {
   composeDeclaredQueryMetadata,
   type ReadableTag,
} from "./materialization/plan/build_plan";
import {
   assertNoAuthorizeTagLike,
   assertNoCallerAuthorizeAnnotation,
   assertNoCallerClassificationAnnotation,
} from "./authorize";
import {
   buildReportManifest,
   compileTileGivens,
   factsReportTag,
   readReportModelFacts,
   type ServedReportManifest,
   type ReportModelFacts,
} from "./report_manifest";
import { isReportEligiblePath } from "./report";
import {
   docNotesAboveArtifact,
   isNotebookReaderError,
   parseNotebookText,
   readNotebookCells,
   type NotebookReaderError,
} from "./notebook";
import {
   recordNotebookCellExecution,
   type NotebookFormat,
} from "../notebook_metrics";
import {
   buildFilterClause,
   FILTER_ANNOTATION_PREFIX,
   filterHasValue,
   FilterValidationError,
   injectFilterRefinement,
   type FilterDefinition,
   type FilterParams,
} from "./filter";
import { assertFilterGivensParse, gateGivenSource } from "./given";
import {
   deprecatedPersistAnnotations,
   type DeprecatedPersistAnnotation,
} from "./deprecated/persist_annotations";
import { filterPublisherOwnedRenderLogs } from "./report_manifest";
import {
   docCommentTitleAndDescription,
   motlyTag,
   parseRouteTag,
   readAutorun,
   readStartingGivens,
   routeHasKey,
   tagText,
} from "./motly";
import {
   assertWithinModelRowLimit,
   type QueryRowLimitSource,
} from "./model_limits";
import {
   isGivenBindingFailure,
   mapRunError,
   prepareAndRun,
   serializeWithinCaps,
   type PreparedRun,
} from "./query_execution";
import { onlyParseFailures } from "./compile_restriction";
import { createEventLoopYielder } from "./event_loop_yield";
import {
   buildDerivationBaseMap,
   buildJoinBaseMap,
   extractRunTargetSourceName,
   extractRunTargetSourceNames,
   malloyIdentifiers,
   stripMalloyCommentsAndLiterals,
} from "./query_text";
import { type CallerRegion } from "./caller_joins";
import {
   mergeQueryMetadata,
   type ModelQueryMetadataInput,
   type QueryMetadata,
} from "./query_metadata";
import {
   validateModelPreaggregation,
   type PreaggregateViolation,
} from "./materialization/plan/preaggregation_validation";
import {
   QueryGovernor,
   REQUEST_CHAIN_MAX_NAMES,
   type GovernorHost,
   type GovernRequest,
} from "./governance/query_governor";
import { type SiblingModelDefResolver } from "./filter_binding_guard";
// Aliased with an `Impl` suffix because `Model` declares private wrapper
// methods of the same names; without the alias, converting one of them to an
// arrow-function property would recurse instead of delegating.
import { extractCheckedModel } from "./model_load_checks";
import {
   recordAuthorizeGuardRejection,
   recordRowLevelGateDecision,
   type AuthorizeGuardField,
} from "../authorize_metrics";
import { translatorMalloyError } from "./translator_error";
import {
   createModelMaterializer,
   createModelRuntime,
   hydrateMarkdownOnlyCells,
   hydrateNotebookCells,
   makeHydrationRuntime,
   type HydrationRuntime,
   type ModelConnectionInput,
   type ModelType,
} from "./model_runtime";
import {
   declaredGivensPerCell,
   tileQueryInfo,
   type CellDeclaredGivens,
   type RunnableNotebookCell,
   type NotebookCellRunResult,
} from "./model_notebook_cell";
import {
   compileErrorOf,
   multipleRunStatementsError,
   queryCompileError,
   runStatementCount,
} from "./model_query_compile";
import { quotedIdentifier } from "./malloy_text";
import { runReadsPublished, scrubHiddenIds } from "./model_projection";
import { compileOnlyDestinationConfig } from "./connection";
import {
   StorageBindings,
   type SourceTypeMismatches,
} from "./materialization/serve/storage_bindings";
import type { MaterializePlan } from "./materialization/plan/build_file_generator";
import { RollupVariantServe } from "./materialization/serve/rollup_variants";
import {
   resultShapeOf,
   ServedCopyServe,
   type ServedCopyProblem,
} from "./materialization/serve/served_copy_serve";
import { type ApiQueryPath } from "./materialization/serve/query_path";
import { PersistedViewServe } from "./materialization/serve/view_variants";
import { sourceChain } from "./materialization/text/source_map";
import type { ViewServeVerdict } from "./materialization/serve/serve_verdict";

export type { ModelConnectionInput, ModelType } from "./model_runtime";
export {
   bindingsAllowDegradeToLive,
   recordRoutingDecision,
   type RoutedExtra,
} from "./query_serving";
export type {
   NotebookCellRunResult,
   RunnableNotebookCell,
} from "./model_notebook_cell";

export type { ModelQueryMetadataInput } from "./query_metadata";
import {
   ModelPolicy,
   type OffSurfaceContext,
   type QueryBoundaryPolicy,
} from "./model_policy";
import { QueryPrivileges, type QueryRequest } from "./model_query_request";
import { annotationNearMisses, authoredNotes } from "./annotation_near_miss";
import { type AnnotationFinding } from "./annotation_reader";
export { QueryPrivileges } from "./model_query_request";
export type { QueryRequest } from "./model_query_request";

type ApiCompiledModel = components["schemas"]["CompiledModel"];
type ApiNotebookCell = components["schemas"]["NotebookCell"];
type ApiRawNotebook = components["schemas"]["RawNotebook"];
type ApiSource = components["schemas"]["Source"];
type ApiGiven = components["schemas"]["Given"];
type ApiQuery = components["schemas"]["Query"];
export type ApiConnection = components["schemas"]["Connection"];
export type SnowflakeConnection = components["schemas"]["SnowflakeConnection"];
export type PostgresConnection = components["schemas"]["PostgresConnection"];
export type BigqueryConnection = components["schemas"]["BigqueryConnection"];
export type TrinoConnection = components["schemas"]["TrinoConnection"];

const MALLOY_VERSION = (
   createRequire(import.meta.url)("@malloydata/malloy/package.json") as {
      version: string;
   }
).version;

export type { OffSurfaceContext, QueryBoundaryPolicy } from "./model_policy";
export { ModelPolicy, PackageModelPolicy } from "./model_policy";

/** What {@link Model.runQuery} answers. */
export interface ModelQueryResults {
   result: Malloy.Result;
   /**
    * The JSON of the `responseShape` asked for, built once. Callers should
    * send this string: it is the bytes the byte cap measured, and a payload
    * too large to serialize surfaces here as a 413 rather than a bare 500.
    */
   serializedResult: string;
   compactResult: QueryData;
   modelInfo: Malloy.ModelInfo;
   dataStyles: DataStyles;
   /** Row cap pushed into the SQL: the query's own LIMIT, else the default. */
   rowLimit: number;
   /** Which of those two the cap came from. */
   rowLimitSource: QueryRowLimitSource;
   /**
    * The `query_id` property attached to this query's statements, which is the
    * caller's join key into the backend's own query record. Null when no
    * metadata was attached.
    */
   queryCorrelationId: string | null;
   /**
    * How the answer was produced once the query reached the storage routing
    * decision; null when it never had a storage binding to consider. See
    * {@link ServedFrom} — `live_fallback` is a SUCCESS answered by the live
    * warehouse, so a caller must not read it as a storage hit.
    */
   servedFrom: ServedFrom | null;
   /**
    * The path report (materialization v2, D1): what answered the query and
    * why. Undefined only when it could not be assembled; reporting never fails
    * a query.
    */
   path?: ApiQueryPath;
   /**
    * Wall-clock ms from query handling through execution, excluding
    * serialization and transport; the span the query histogram records.
    */
   executionTimeMs: number;
   /**
    * Warehouse bytes this query SCANNED, when the backend reports it
    * (BigQuery today) — not bytes billed. Null means "not reported",
    * including a storage-served query; never read it as zero cost without
    * checking `servedFrom`. Named after Malloy's `runStats.queryCostBytes`,
    * which it carries verbatim.
    */
   queryCostBytes: number | null;
}

/**
 * A non-fatal render-tag finding from {@link Model.validateRenderTags}: an issue
 * that affects only how a field renders, never whether the model compiles or a
 * query runs. `subject` is the query or view it sits on (e.g. `by_carrier` or
 * `flights -> by_carrier`). `severity` is the renderer's own: `error` for a tag
 * malformed for its field, `warn` for one that is well-formed but inert where it
 * sits and therefore silently ignored.
 */
export interface RenderTagWarning {
   subject: string;
   message: string;
   severity: "error" | "warn";
}

/** The one empty result {@link Model.preaggregateViolations} hands back, so its
 *  identity contract holds on the branch that never reaches the memo. */
const NO_PREAGGREGATE_VIOLATIONS: readonly Readonly<PreaggregateViolation>[] =
   Object.freeze([]);

const queryExecutionHistogram = lazyHistogram(
   "malloy_model_query_duration",
   "How long it takes to execute a Malloy model query",
);
/**
 * Warehouse bytes SCANNED by model queries, summed — not bytes billed (BigQuery
 * bills a 10MB minimum per query). A counter because the question is a sum over
 * a period; a histogram would need buckets spanning KB to TB.
 *
 * Only advances for backends that report the figure inline — today BigQuery
 * (`totalBytesProcessed`). Absent is not zero: Snowflake has bytes only in
 * `QUERY_HISTORY` (an extra round trip) and Postgres has none, so a sum across
 * connections understates every non-BigQuery one.
 */
const queryScannedBytesCounter = lazyCounter(
   "malloy_model_query_scanned_bytes",
   "Warehouse bytes scanned by Malloy model queries, where the backend reports them. NOT bytes billed: BigQuery bills a 10MB minimum per query, so spend exceeds this on small reads.",
   "By",
);

/** What a {@link Model} is built from; only the first four are always known. */
export interface ModelState {
   packageName: string;
   modelPath: string;
   dataStyles: DataStyles;
   modelType: ModelType;
   modelMaterializer?: ModelMaterializer;
   modelDef?: ModelDef;
   sources?: ApiSource[];
   queries?: ApiQuery[];
   sourceInfos?: Malloy.SourceInfo[];
   runnableNotebookCells?: RunnableNotebookCell[];
   compilationError?: MalloyError | Error;
   filterMap?: Map<string, FilterDefinition[]>;
   givens?: ApiGiven[];
   /** Precomputed `modelInfoOf(modelDef)` from the package-load worker;
    *  derived here when omitted. */
   modelInfo?: Malloy.ModelInfo;
   /**
    * Who decides this model's curation, query boundary, sibling resolution and
    * freshness. A Package always passes its {@link PackageModelPolicy} view;
    * omitted means {@link ModelPolicy.standalone}, which is what a Model built
    * directly (outside any Package) has always meant.
    */
   policy?: ModelPolicy;
   /**
    * The runtime that produced {@link modelMaterializer}, retained so a
    * row-level gate can graft (see {@link Model.gateRuntime}). Absent: every
    * row-level gate denies.
    */
   gateRuntime?: HydrationRuntime;
   /**
    * The model file's absolute path, which tells a source this file declares
    * from one an import brought in. Absent: no source counts as declared here.
    */
   modelFile?: string;
}

export class Model {
   private packageName: string;
   private modelPath: string;
   private dataStyles: DataStyles;
   private modelType: ModelType;
   private modelMaterializer: ModelMaterializer | undefined;
   private modelDef: ModelDef | undefined;
   /** See {@link ModelState.modelFile}. */
   private modelFile: string | undefined;
   /** See {@link declaredDeprecatedPersistAnnotations}. */
   private deprecatedPersistAnnotationsMemo:
      | DeprecatedPersistAnnotation[]
      | undefined;
   private modelInfo: Malloy.ModelInfo | undefined;
   private storageServe = new StorageBindings({
      modelPath: () => this.modelPath,
      modelDef: () => this.modelDef,
   });
   /**
    * The served copy on `virtual` (materialization v2, phase 3): the author's
    * own files with each stored source re-rooted on its table. A router
    * candidate only when `materialize.enabled` (`PUBLISHER_MATERIALIZE`) is on.
    */
   /** The author sourceIDs a `#(materialize)` build stores (see setServeBindings). */
   private materializeSourceIDs: ReadonlySet<string> = new Set();
   /**
    * The `#@ persist` view leg (materialization v2, phase 4, P14): a router
    * candidate only when `materialize.enabled` is on and a source this model
    * sees persists a view.
    */
   private persistedViews = new PersistedViewServe({
      modelPath: () => this.modelPath,
      modelDef: () => this.modelDef,
      modelMaterializer: () => this.modelMaterializer,
      serveDestinationConfig: () => this.storageServe.serveDestinationConfig,
      catalog: () => this.policy.materializedTables(),
   });
   private servedCopy = this.newServedCopy();
   /** What {@link setServedCopyContext} last set, for a dry-run copy. */
   private servedCopyContext:
      | Parameters<ServedCopyServe["setContext"]>
      | undefined;
   /**
    * The `#(preaggregate)` rollup leg (materialization v2, phase 4): a router
    * candidate only when `materialize.enabled` is on and a source this model
    * sees declares a rollup.
    */
   private rollupVariants = new RollupVariantServe({
      modelPath: () => this.modelPath,
      modelDef: () => this.modelDef,
      serveDestinationConfig: () => this.storageServe.serveDestinationConfig,
      catalog: () => this.policy.materializedTables(),
      // A joined source's `storage=` rollup is read through a join only from
      // a query source the served copy reads in the same destination.
      joinRollupBasis: async () =>
         this.servedCopyEnabled()
            ? this.servedCopy.joinRollupBasis()
            : undefined,
   });
   /** {@link sourceChains}, keyed by the model it was read from. */
   private sourceChainCache:
      | {
           modelDef: ModelDef | undefined;
           chains: ReadonlyMap<string, readonly string[]>;
        }
      | undefined;
   /**
    * The model file's text as the compile that produced {@link modelDef} read
    * it: shipped by the worker on a package load, read by `Model.create`
    * in-process.
    *
    * Held rather than re-read because it is only meaningful paired with THIS
    * compile's `DocumentLocation` coordinates; after a failed reload the file
    * on disk is newer than the served IR.
    */
   private compiledSourceText: string | undefined;
   /**
    * For a model whose files join sources across connections (#6982), loaded
    * from its build view: the definitions left out. Undefined otherwise.
    */
   private crossConnectionLeftOut: LeftOutDefinition[] | undefined;
   /** {@link getReportModelFacts}, once read; boxed so `undefined` is cached too. */
   private reportModelFacts:
      | { value: ReportModelFacts | undefined }
      | undefined;
   /** {@link carriesReportTag}, once read. */
   private reportTagCarried: boolean | undefined;
   /**
    * Per materializer, settles once {@link ensureSharedQueryModel} has built
    * its QueryModel. Weak, so a grafted materializer evicted from its cache
    * takes its entry with it.
    */
   private sharedQueryModels = new WeakMap<ModelMaterializer, Promise<void>>();
   private compiledReportFacts:
      | Promise<ReportModelFacts | undefined>
      | undefined;
   /** {@link compiledReportFacts} once settled, for the sync notebook reader. */
   private settledCompiledReportFacts: ReportModelFacts | undefined;
   private sources: ApiSource[] | undefined;
   private queries: ApiQuery[] | undefined;
   private sourceInfos: Malloy.SourceInfo[] | undefined;
   private runnableNotebookCells: RunnableNotebookCell[] | undefined;
   private compilationError: MalloyError | Error | undefined;
   /** Why the reader refused this served notebook's cells; the model itself still compiled and serves. */
   private notebookReaderRefusal: NotebookReaderError | undefined;
   /** A served notebook's own notes as the reader collected them (see `NotebookReadResult.annotations`). */
   private notebookAnnotations: string[] | undefined;
   /** The tile layout of a served linear `.malloy` report; undefined for a `.malloynb`. */
   private notebookLayout: ServedReportManifest | undefined;
   /** Parsed #(filter) definitions keyed by source name. */
   private filterMap: Map<string, FilterDefinition[]>;
   /** Givens declared on the model, in declaration order. */
   private givens: ApiGiven[] | undefined;
   /** Indexed like {@link runnableNotebookCells}; `undefined` = no modelDef, filter as before. */
   private notebookCellDeclaredGivens: (CellDeclaredGivens | undefined)[] = [];
   /**
    * Memo for {@link getDeclaredQueryMetadata}. `undefined` = not yet computed,
    * `null` = computed and nothing declared.
    */
   private declaredQueryMetadataMemo: QueryMetadata | null | undefined;
   /** Memo for {@link getDeclaredSourceQueryMetadata}. */
   private declaredSourceQueryMetadataMemo:
      | { sourceName: string; queryMetadata: QueryMetadata }[]
      | undefined;
   /** Memo for {@link preaggregateViolations}. */
   private preaggregateViolationsMemo:
      | readonly Readonly<PreaggregateViolation>[]
      | undefined;
   /**
    * Curation, query boundary, sibling resolution and freshness, decided by
    * this model's owner and fixed at construction (see {@link ModelPolicy}).
    * A Package's policy is a live view the Package fills; a standalone one
    * is uncurated, admits every declared name and serves live.
    */
   private readonly policy: ModelPolicy;
   /** {@link buildDerivationBaseMap} over the boundary's `derivationText`,
    *  memoized per boundary object rather than computed per query. */
   private derivationBasesMemo?: {
      boundary: QueryBoundaryPolicy;
      bases: Map<string, Set<string>> | undefined;
   };
   /**
    * {@link ownCuratedSourceNames}, and {@link curatedBasesOf}'s answer for a
    * name asked with no query or derivation context, per boundary object and
    * discovery-curation setting: everything they read besides this model's
    * fixed compiled state. The entities ask the route once per source, so
    * rebuilding these per call made a listing quadratic in the source count.
    * A package installs a new boundary object for every path whenever it
    * re-decides its boundaries, and a reload builds new Model instances, so a
    * stale entry is never read.
    */
   private curationMemo?: {
      boundary: QueryBoundaryPolicy;
      discoveryCuration: boolean;
      ownCuratedSourceNames: ReadonlySet<string>;
      curatedBasesOf: Map<string, ReadonlySet<string> | undefined>;
   };
   /** Whether discovery accessors curate to the `export {}` closure: true
    *  only when the owning package declares `explores` in publisher.json. */
   private get discoveryCurationEnabled(): boolean {
      return this.policy.discoveryCurationEnabled();
   }
   /**
    * Resolves the `ModelDef` the owning Package independently compiled for
    * the file a `file://` URL names. `undefined` outside a Package, which is
    * safe: every guard treats "unresolvable" as deny. See
    * `SiblingModelDefResolver` in `./filter_binding_guard` for why it exists.
    */
   private get siblingModelDefResolver(): SiblingModelDefResolver | undefined {
      return this.policy.siblingModelDefResolver();
   }
   /** The query boundary this file enforces; see {@link QueryBoundaryPolicy}. */
   private get queryBoundary(): QueryBoundaryPolicy {
      return this.policy.queryBoundary();
   }
   /** {@link buildDerivationBaseMap} over `queryBoundary.derivationText`,
    *  computed once per boundary rather than per query. */
   /** The {@link curationMemo} for the boundary and curation now in force. */
   private get curation(): NonNullable<Model["curationMemo"]> {
      const boundary = this.queryBoundary;
      const discoveryCuration = this.discoveryCurationEnabled;
      const memo = this.curationMemo;
      if (
         memo?.boundary === boundary &&
         memo.discoveryCuration === discoveryCuration
      )
         return memo;
      // A notebook or dashboard can see hidden sources through its imports, so
      // nothing it sees is curated by being visible; only the package surface
      // admits (and derivesFromCurated for its own declarations).
      const ownCuratedSourceNames: ReadonlySet<string> =
         this.isNotebook() || this.isGridReport()
            ? new Set()
            : new Set(
                 (this.getSources() ?? [])
                    .map((s) => s.name)
                    .filter((n): n is string => n !== undefined),
              );
      this.curationMemo = {
         boundary,
         discoveryCuration,
         ownCuratedSourceNames,
         curatedBasesOf: new Map(),
      };
      return this.curationMemo;
   }
   private get fileDerivationBases(): Map<string, Set<string>> | undefined {
      const boundary = this.queryBoundary;
      if (this.derivationBasesMemo?.boundary !== boundary) {
         this.derivationBasesMemo = {
            boundary,
            bases: boundary.derivationText
               ? buildDerivationBaseMap(
                    stripMalloyCommentsAndLiterals(boundary.derivationText),
                 )
               : undefined,
         };
      }
      return this.derivationBasesMemo.bases;
   }
   constructor(state: ModelState) {
      this.packageName = state.packageName;
      this.modelPath = state.modelPath;
      this.dataStyles = state.dataStyles;
      this.modelType = state.modelType;
      this.modelDef = state.modelDef;
      this.modelFile = state.modelFile;
      this.modelMaterializer = state.modelMaterializer;
      this.sources = state.sources;
      this.queries = state.queries;
      this.sourceInfos = state.sourceInfos;
      this.runnableNotebookCells = state.runnableNotebookCells;
      this.compilationError = state.compilationError;
      this.filterMap = state.filterMap ?? new Map();
      this.givens = state.givens;
      this.policy = state.policy ?? ModelPolicy.standalone();
      this.governor = new QueryGovernor(this.governorHost(state.gateRuntime));
      this.notebookCellDeclaredGivens = declaredGivensPerCell(
         this.runnableNotebookCells ?? [],
      );
      this.modelInfo =
         state.modelInfo ??
         (this.modelDef ? modelInfoOf(this.modelDef) : undefined);

      // One-time `#(filter)` deprecation notice per Model instance.
      if (this.filterMap.size > 0) {
         logger.warn(
            `Model "${this.packageName}/${this.modelPath}" uses deprecated #(filter) annotations. Migrate to given: — see ${GIVENS_DOCS_URL}`,
            {
               packageName: this.packageName,
               modelPath: this.modelPath,
               filterSourceCount: this.filterMap.size,
            },
         );
      }
   }

   /**
    * The sources THIS file declares deprecated `#(filter)` annotations on: the
    * annotation is written in this file, on the source's own statement. Unlike
    * `filterMap`, which also holds every source that inherits a filter (an
    * `extend` of a filtered source) and every filtered source an import brings
    * in, this names each declaration exactly once, in the file to change.
    */
   public declaredFilterSourceNames(): string[] {
      if (!this.modelDef) return [];
      const names: string[] = [];
      for (const entry of Object.values(this.modelDef.contents)) {
         if (!isSourceDef(entry)) continue;
         const own = entry.annotations?.blockNotes ?? [];
         const declares = own.some(
            (note) =>
               note.text.trim().startsWith(FILTER_ANNOTATION_PREFIX) &&
               note.at !== undefined &&
               this.declaredHere(note.at.url),
         );
         if (declares) names.push(entry.as || entry.name);
      }
      return names.sort();
   }

   /**
    * Whether a document URL is this model's own file. Compared as resolved
    * paths: a suffix match on {@link modelPath} would take
    * `sub/models/a.malloy`, imported here, for `models/a.malloy`.
    */
   private declaredHere(url: string): boolean {
      if (this.modelFile === undefined) return false;
      try {
         return path.resolve(fileURLToPath(url)) === this.modelFile;
      } catch {
         return false;
      }
   }

   /**
    * The deprecated v1 `#@ preaggregate` annotations THIS file declares, each
    * once, at the measure it is written on.
    */
   public declaredDeprecatedPersistAnnotations(): DeprecatedPersistAnnotation[] {
      if (!this.modelDef) return [];
      // Read per package request, and the model never changes: walk it once.
      return (this.deprecatedPersistAnnotationsMemo ??=
         deprecatedPersistAnnotations(this.modelDef, (url) =>
            this.declaredHere(url),
         ));
   }

   /** The parsed filter definitions for a source; empty when none. */
   public getFilters(sourceName: string): FilterDefinition[] {
      return this.filterMap.get(sourceName) ?? [];
   }

   /**
    * Effective `#(authorize)` expressions gating a source; empty means
    * unrestricted. Introspection only: use it to decide THAT a source is
    * gated, never to reconstruct the gate. Enforcement re-derives the
    * condition from the IR, and an unattributable expression appears as a
    * `"false"` sentinel.
    */
   public getAuthorize(sourceName: string): string[] {
      return (
         this.sources?.find((source) => source.name === sourceName)
            ?.authorize ?? []
      );
   }

   /**
    * Effective `#(access_filter)` expressions filtering a source's rows — the
    * mirror of {@link getAuthorize} for the filter route ONLY. Same
    * introspection-only caveats apply.
    */
   public getAccessFilter(sourceName: string): string[] {
      return (
         this.sources?.find((source) => source.name === sourceName)
            ?.accessFilter ?? []
      );
   }

   /**
    * Whether the model declares any `#(authorize)` gate at all, on any
    * source. Lets callers cheaply skip authorize work for ungated models
    * without compiling a probe.
    */
   public hasAuthorize(): boolean {
      return this.sources?.some((s) => (s.authorize?.length ?? 0) > 0) ?? false;
   }

   /**
    * This model's authorization: every gate a query passes after the query
    * boundary (see {@link QueryGovernor}). Built once in the constructor,
    * after the state it reads is set.
    */
   private readonly governor: QueryGovernor;

   /**
    * This model's {@link QueryGovernor}: the authorization `/compile` runs
    * its gates through (the query route reaches it inside {@link runQuery}).
    */
   public governance(): QueryGovernor {
      return this.governor;
   }

   /** What {@link governor} reads from this model; see {@link GovernorHost}. */
   private governorHost(
      gateRuntime: HydrationRuntime | undefined,
   ): GovernorHost {
      return {
         packageName: this.packageName,
         modelPath: this.modelPath,
         gateRuntime,
         modelDef: () => this.modelDef,
         modelMaterializer: () => this.modelMaterializer,
         sources: () => this.sources,
         queries: () => this.queries,
         givens: () => this.givens,
         filterMap: () => this.filterMap,
         runnableNotebookCells: () => this.runnableNotebookCells,
         siblingModelDefResolver: () => this.siblingModelDefResolver,
         queryBoundary: () => this.queryBoundary,
         isNotebook: () => this.isNotebook(),
         notebookFormat: () => this.notebookFormat(),
         isCuratedSource: (name) => this.isCuratedSource(name),
         isCuratedQuery: (name) => this.isCuratedQuery(name),
         derivesFromCurated: (name, query, derivationContext) =>
            this.derivesFromCurated(name, query, derivationContext),
         notQueryable: (refusal, explainable, refused, name) =>
            this.notQueryable(refusal, explainable, refused, name),
         assertQueryBoundaryEarly: (
            sourceName,
            queryName,
            query,
            derivationContext,
         ) =>
            this.assertQueryBoundaryEarly(
               sourceName,
               queryName,
               query,
               derivationContext,
            ),
         assertQueryBoundaryCompiled: (
            compiledSource,
            query,
            derivationContext,
         ) =>
            this.assertQueryBoundaryCompiled(
               compiledSource,
               query,
               derivationContext,
            ),
         ensureSharedQueryModel: (materializer) =>
            this.ensureSharedQueryModel(materializer),
      };
   }

   /** See {@link QueryGovernor.assertAuthorized}. */
   public assertAuthorized(
      ...args: Parameters<QueryGovernor["assertAuthorized"]>
   ): Promise<void> {
      return this.governor.assertAuthorized(...args);
   }

   /** See {@link QueryGovernor.assertAuthorizedForAllSources}. */
   public assertAuthorizedForAllSources(
      ...args: Parameters<QueryGovernor["assertAuthorizedForAllSources"]>
   ): Promise<void> {
      return this.governor.assertAuthorizedForAllSources(...args);
   }

   /** See {@link QueryGovernor.assertAuthorizedFromCompiledRunnable}. */
   public assertAuthorizedFromCompiledRunnable(
      ...args: Parameters<QueryGovernor["assertAuthorizedFromCompiledRunnable"]>
   ): Promise<void> {
      return this.governor.assertAuthorizedFromCompiledRunnable(...args);
   }

   /** See {@link QueryGovernor.queryHadRowLevelFilterAttached}. */
   public queryHadRowLevelFilterAttached(runnable: QueryMaterializer): boolean {
      return this.governor.queryHadRowLevelFilterAttached(runnable);
   }

   /** See {@link QueryGovernor.assertDocumentJoinsQueryable}. */
   public assertDocumentJoinsQueryable(
      ...args: Parameters<QueryGovernor["assertDocumentJoinsQueryable"]>
   ): Promise<void> {
      return this.governor.assertDocumentJoinsQueryable(...args);
   }

   /** See {@link QueryGovernor.assertTextNameVisible}. */
   public assertTextNameVisible(query: string, definitions: string): void {
      this.governor.assertTextNameVisible(query, definitions);
   }

   /** See {@link QueryGovernor.assertAuthorizedForText}. */
   public assertAuthorizedForText(
      ...args: Parameters<QueryGovernor["assertAuthorizedForText"]>
   ): Promise<void> {
      return this.governor.assertAuthorizedForText(...args);
   }

   /** See {@link QueryGovernor.assertAuthorizedForRunnable}. */
   public assertAuthorizedForRunnable(
      ...args: Parameters<QueryGovernor["assertAuthorizedForRunnable"]>
   ): Promise<void> {
      return this.governor.assertAuthorizedForRunnable(...args);
   }

   /**
    * Compile a single model in-process (test fixtures and ad-hoc callers).
    * Production package loads go through the worker pool and
    * {@link Model.fromSerialized} instead.
    *
    * `options.policy` defaults to {@link ModelPolicy.standalone}: a model
    * compiled on its own belongs to no Package, so nothing curates it.
    */
   public static async create(
      packageName: string,
      packagePath: string,
      modelPath: string,
      malloyConfig: ModelConnectionInput,
      options?: {
         buildManifest?: BuildManifest["entries"];
         /** Files read instead of the package's, by `file://` URL (see `createModelRuntime`). */
         overlay?: ReadonlyMap<string, string>;
         policy?: ModelPolicy;
      },
   ): Promise<Model> {
      const policy = options?.policy ?? ModelPolicy.standalone();
      // getModelRuntime might throw a ModelNotFoundError. It's the callers responsibility
      // to pass a valid model path or handle the error.
      const {
         runtime,
         modelURL,
         importBaseURL,
         dataStyles,
         modelType,
         compiledTextFor,
      } = await Model.getModelRuntime(
         packagePath,
         modelPath,
         malloyConfig,
         options,
      );

      try {
         const loadedModel = await Model.getModelMaterializer(
            runtime,
            importBaseURL,
            modelURL,
            modelPath,
         );
         let modelMaterializer = loadedModel.modelMaterializer;
         const runnableNotebookCells = loadedModel.runnableNotebookCells;
         let gateRuntime = runtime;
         let compiled:
            | Awaited<ReturnType<ModelMaterializer["getModel"]>>
            | undefined;
         let crossConnectionLeftOut: LeftOutDefinition[] | undefined;
         try {
            compiled = await modelMaterializer?.getModel();
         } catch (first) {
            // A file that joins sources across connections (#6982) loads
            // from its build view behind the materialize switch.
            const view =
               modelType === "model"
                  ? await loadLeavingOutCrossConnectionJoins({
                       firstError: first,
                       readText: (url) =>
                          compiledFileText(url, options?.overlay),
                       load: async (files) => {
                          const rt = await Model.getModelRuntime(
                             packagePath,
                             modelPath,
                             malloyConfig,
                             {
                                ...options,
                                overlay: new Map([
                                   ...(options?.overlay ?? []),
                                   ...keyedOverlay(files),
                                ]),
                             },
                          );
                          const mm = rt.runtime.loadModel(rt.modelURL, {
                             importBaseURL: rt.importBaseURL,
                          });
                          return { rt, mm, model: await mm.getModel() };
                       },
                    })
                  : undefined;
            if (!view) throw first;
            modelMaterializer = view.result.mm;
            gateRuntime = view.result.rt.runtime;
            compiled = view.result.model;
            crossConnectionLeftOut = view.leftOut;
         }
         const checked =
            modelMaterializer && compiled
               ? await extractCheckedModel(
                    modelMaterializer,
                    compiled,
                    (sourceName, err) =>
                       logger.warn(
                          `Failed to parse filter annotations on source "${sourceName}"`,
                          { error: err },
                       ),
                 )
               : undefined;
         for (const warning of checked?.warnings ?? []) {
            logger.warn(warning.message, {
               packageName,
               modelPath,
               sourceName: warning.sourceName,
            });
         }
         const modelDef = compiled?._modelDef;
         const sources = checked?.sources as ApiSource[] | undefined;
         const queries = checked?.queries as ApiQuery[] | undefined;
         const filterMap = checked?.filterMap;
         const givens = checked?.givens as ApiGiven[] | undefined;
         const sourceInfos = checked?.sourceInfos;

         const model = new Model({
            packageName,
            modelPath,
            dataStyles,
            modelType,
            modelMaterializer,
            modelDef,
            modelFile: path.resolve(packagePath, modelPath),
            sources,
            queries,
            sourceInfos,
            runnableNotebookCells,
            filterMap,
            givens,
            policy,
            // Cast: `_loadModelFromModelDef` is internal and undeclared on the
            // public `Runtime` type.
            gateRuntime: gateRuntime as HydrationRuntime,
         });
         model.compiledSourceText = compiledTextFor(modelURL);
         model.crossConnectionLeftOut = crossConnectionLeftOut;
         return model;
      } catch (thrown) {
         if (thrown instanceof Error && thrown.stack) {
            logger.error("Error stack", thrown.stack);
         }
         // The translator's plain Error is the author's mistake too, as it is
         // on the worker path (package_load_worker.ts compileOneModel).
         const error = translatorMalloyError(thrown) ?? thrown;
         let computedError = error;

         if (error instanceof MalloyError) {
            const problems = error.problems;
            for (const problem of problems) {
               logger.error("Problem", problem);
            }
            computedError = new ModelCompilationError(error);
         }
         return new Model({
            packageName,
            modelPath,
            dataStyles,
            modelType,
            compilationError: computedError as Error,
            policy,
         });
      }
   }

   /**
    * Construct a `Model` from a worker-compiled `SerializedModel`.
    *
    * Hydrates materializers (and notebook cell runnables) eagerly: the wraps
    * are constant-time over pre-compiled IR, and eager hydration keeps the
    * result interchangeable with `Model.create` (no lazy-init branches).
    */
   public static fromSerialized(
      packageName: string,
      packagePath: string,
      malloyConfig: ModelConnectionInput,
      data: SerializedModel,
      options: {
         /** The owning Package's view for this model, or an explicit standalone one. */
         policy: ModelPolicy;
         buildManifest?: BuildManifest["entries"];
         // A package compile hydrates every model on every call, for a package
         // that already logged these warnings when it loaded.
         skipAuthorizeWarningLog?: boolean;
      },
   ): Model {
      // Envelope check before the casts below; throws a
      // PackageLoadProtocolError naming the malformed field.
      decodeSerializedModel(data);
      const modelDef = data.modelDef as ModelDef | undefined;
      const modelInfo = data.modelInfo as Malloy.ModelInfo | undefined;
      const dataStyles = (data.dataStyles ?? {}) as DataStyles;
      const sources = data.sources as ApiSource[] | undefined;
      const queries = data.queries as ApiQuery[] | undefined;
      const sourceInfos = data.sourceInfos as Malloy.SourceInfo[] | undefined;
      const givens = data.givens as ApiGiven[] | undefined;
      const filterMap = data.filterMap
         ? new Map(data.filterMap as Array<[string, FilterDefinition[]]>)
         : undefined;

      // Non-fatal `#(authorize)` findings from the worker, which has no logger.
      for (const warning of options.skipAuthorizeWarningLog
         ? []
         : (data.authorizeWarnings ?? [])) {
         logger.warn(warning, { packageName, modelPath: data.modelPath });
      }

      // No modelDef: an empty notebook or a corrupt payload. Queries throw a
      // clean BadRequestError downstream; markdown cells are kept so
      // `getNotebook()` can serve them.
      if (!modelDef) {
         return new Model({
            packageName,
            modelPath: data.modelPath,
            dataStyles,
            modelType: data.modelType,
            sources,
            queries,
            sourceInfos,
            runnableNotebookCells:
               data.modelType === "notebook"
                  ? hydrateMarkdownOnlyCells(data.notebookCells)
                  : undefined,
            filterMap,
            givens,
            modelInfo,
            policy: options.policy,
         });
      }

      const runtime = makeHydrationRuntime(malloyConfig, options.buildManifest);
      const modelMaterializer = runtime._loadModelFromModelDef(modelDef);
      const runnableNotebookCells =
         data.modelType === "notebook"
            ? hydrateNotebookCells(runtime, data.notebookCells)
            : undefined;

      const model = new Model({
         packageName,
         modelPath: data.modelPath,
         dataStyles,
         modelType: data.modelType,
         modelMaterializer,
         modelDef,
         modelFile: path.resolve(packagePath, data.modelPath),
         sources,
         queries,
         sourceInfos,
         runnableNotebookCells,
         filterMap,
         givens,
         modelInfo,
         policy: options.policy,
         // `malloyConfig` is deliberately not captured for serve shapes: that
         // would put a storage destination in reach of the author's namespace
         // (see Package.setServeDestinationConfig). The gate runtime must be
         // the SAME one that hydrated `modelMaterializer`, so grafts share this
         // model's given identities.
         gateRuntime: runtime,
      });
      // Paired with `modelDef` above: the coordinates in that IR index this
      // text and no other revision of the file.
      model.compiledSourceText = data.modelSourceText;
      model.crossConnectionLeftOut = data.crossConnectionLeftOut as
         | LeftOutDefinition[]
         | undefined;
      model.preparedRenderTagResults = data.renderTagResults as
         | { label: string; result?: Malloy.Result }[]
         | undefined;
      return model;
   }

   /**
    * Build a materializer's Malloy `QueryModel` once, on the base model it
    * holds, so every query compiled against it reuses it: this Model's own
    * materializer, a row-level grafted one, or the storage serve shape.
    *
    * Malloy reuses the base model's `QueryModel` for a query that does not
    * modify the model, but only if one already exists; otherwise every prepare
    * builds one over every source and drops it. A materializer's `ModelDef` is
    * fixed, so building once is the same answer.
    *
    * Call it just before compiling, so a query the 404/403 gates reject builds
    * nothing. A failure is logged and not retried (it would fail identically).
    */
   private ensureSharedQueryModel(
      materializer: ModelMaterializer | undefined,
   ): Promise<void> {
      if (!materializer) return Promise.resolve();
      let built = this.sharedQueryModels.get(materializer);
      if (!built) {
         built = (async () => {
            try {
               const base = await materializer.getModel();
               void base.queryModel;
            } catch (error) {
               logger.warn(
                  "Could not build a shared QueryModel; each query builds its own",
                  {
                     packageName: this.packageName,
                     modelPath: this.modelPath,
                     error:
                        error instanceof Error ? error.message : String(error),
                  },
               );
            }
         })();
         this.sharedQueryModels.set(materializer, built);
      }
      return built;
   }

   /**
    * Build a Model representing a compilation failure, the same shape
    * `Model.create` returns when it catches a `MalloyError`.
    */
   public static fromCompilationError(
      packageName: string,
      modelPath: string,
      modelType: ModelType,
      error: Error,
      options: {
         /** The owning Package's view for this model. */
         policy: ModelPolicy;
         // The text that failed, when it is not the file on disk: a package
         // compile's what-if replacement. Without it, a reader of this model's
         // text judges the saved file instead of the edit.
         sourceText?: string;
      },
   ): Model {
      const model = new Model({
         packageName,
         modelPath,
         dataStyles: {} as DataStyles,
         modelType,
         compilationError: error,
         policy: options.policy,
      });
      model.compiledSourceText = options.sourceText;
      return model;
   }

   /** Look up the deserialized error helper for callers (e.g. Package.create). */
   public static deserializeCompilationError = deserializeError;

   public getPath(): string {
      return this.modelPath;
   }

   public getType(): ModelType {
      return this.modelType;
   }

   /** The givens this model surfaces, as `CompiledModel.givens` reports them. */
   public getGivens(): readonly ApiGiven[] {
      return this.givens ?? [];
   }

   /**
    * This file's own model-level `queryMetadata` declaration
    * (`## materialization.queryMetadata.*` or `## queryMetadata.*`), or null.
    * Exposed for the publish gate, which otherwise sees only persist sources'
    * declarations. Memoized: called per package inside `listPackages`, and a
    * reload replaces the `Model`, so no invalidation is needed.
    */
   public getDeclaredQueryMetadata(): QueryMetadata | null {
      // `undefined` means "not computed"; `null` is a computed answer of "none".
      if (this.declaredQueryMetadataMemo === undefined) {
         this.declaredQueryMetadataMemo = composeDeclaredQueryMetadata({
            modelTag: this.safeModelFileTag(),
         });
      }
      return this.declaredQueryMetadataMemo;
   }

   /**
    * Each top-level source's OWN `#@ queryMetadata.*`, for the sources that
    * declare one — including sources that persist nothing, which the publish
    * gate would otherwise never validate. Memoized like
    * {@link getDeclaredQueryMetadata}.
    */
   public getDeclaredSourceQueryMetadata(): {
      sourceName: string;
      queryMetadata: QueryMetadata;
   }[] {
      if (this.declaredSourceQueryMetadataMemo === undefined) {
         const contents = (this.modelDef?.contents ?? {}) as Record<
            string,
            unknown
         >;
         this.declaredSourceQueryMetadataMemo = Object.keys(contents).flatMap(
            (sourceName) => {
               const queryMetadata = composeDeclaredQueryMetadata({
                  sourceTag: this.safeSourceTag(sourceName),
               });
               return queryMetadata ? [{ sourceName, queryMetadata }] : [];
            },
         );
      }
      return this.declaredSourceQueryMetadataMemo;
   }

   /**
    * Restrict a list of named objects to the model's re-export closure
    * (`modelDef.exports`) for discovery, consistent with Malloy's `modelInfo`.
    * Only active when the package declares `explores`. `this.sources` and
    * `this.queries` stay complete for enforcement; queryability is decided
    * separately ({@link assertQueryBoundaryEarly}).
    */
   private curateForDiscovery<T extends { name?: string }>(
      items: T[] | undefined,
   ): T[] | undefined {
      if (!items) return items;
      if (!this.discoveryCurationEnabled) return items;
      const exports = this.modelDef?.exports;
      if (!Array.isArray(exports)) {
         // Fails open (failing closed would blank the listing), but loudly:
         // `exports` is non-optional, so the IR shape changed.
         logger.warn(
            "Discovery curation skipped: modelDef.exports is not an array",
            { modelPath: this.modelPath, packageName: this.packageName },
         );
         return items;
      }
      const exported = new Set(exports);
      return items.filter(
         (item) => item.name !== undefined && exported.has(item.name),
      );
   }

   /**
    * The freshness-filtered build manifest for this query, or undefined (no
    * override; serves live). Evaluated per call so a table that crosses its
    * window is gated on the very next query.
    */
   private resolveFreshBuildManifest(): BuildManifest | undefined {
      const entries = this.policy.freshBuildManifestEntries();
      return entries ? { entries, strict: false } : undefined;
   }

   /**
    * `manifest` with the rollups' own entries removed, for a run that reads
    * the author's model rather than a rollup variant: a legacy `.malloynb`
    * notebook cell, which still compiles with the build manifest (its cells
    * are not routed through the served copy), and the persisted-view leg's
    * variant. Malloy refuses a non-empty `buildManifest` against a model
    * without `##! experimental.persistence`, which a model declaring only
    * rollups need not carry, so a rollup entry there would 400 every query
    * (guard 3 of the plan's D3 list, kept past 6g for these two readers).
    * Undefined when nothing survives, which `prepareAndRun` hands Malloy as
    * EMPTY_BUILD_MANIFEST, never as undefined (that would substitute from the
    * Runtime's unfiltered manifest, #1721).
    */
   private withoutPreaggregateEntries(
      manifest: BuildManifest | undefined,
   ): BuildManifest | undefined {
      if (!manifest) return undefined;
      // The `sourceEntityId`s of the rollups pre-aggregation synthesized
      // (read per query: the build plan is computed after the models load).
      const preaggregateIds = this.policy.preaggregateEntityIds();
      if (!preaggregateIds || preaggregateIds.size === 0) return manifest;
      const entries = Object.fromEntries(
         Object.entries(manifest.entries).filter(
            ([sourceEntityId]) => !preaggregateIds.has(sourceEntityId),
         ),
      );
      return Object.keys(entries).length > 0
         ? { ...manifest, entries }
         : undefined;
   }

   /**
    * `manifest`, or nothing when `modelDef` lacks `##! experimental.persistence`.
    * For a legacy `.malloynb` cell, the one run left that compiles the
    * author's own model with the build manifest: each cell's imports decide
    * its flag, and Malloy refuses a non-empty manifest on a model without it,
    * so a table bound for another model would fail every such cell. A model
    * without the flag has no persisted source for Malloy to substitute, so
    * nothing it reads is lost. Presence only, decided as Malloy decides it.
    * (The query path's form of this, guard 2, retired with 6g: its live
    * compile takes no manifest.)
    */
   private manifestIfPersistent(
      manifest: BuildManifest | undefined,
      modelDef: ModelDef | undefined,
   ): BuildManifest | undefined {
      if (!manifest || !modelDef) return undefined;
      let persistent = this.persistenceFlagMemo.get(modelDef);
      if (persistent === undefined) {
         persistent = routeHasKey(
            new Annotations(modelAnnotations(modelDef)),
            "!",
            "experimental",
            "persistence",
         );
         this.persistenceFlagMemo.set(modelDef, persistent);
      }
      return persistent ? manifest : undefined;
   }

   /** {@link manifestIfPersistent}'s answer for each model it was read from. */
   private readonly persistenceFlagMemo = new WeakMap<ModelDef, boolean>();

   /** `includeHidden` lists sources the file does not export, which `queryableSources: "all"` still serves. */
   public getSources(includeHidden = false): ApiSource[] | undefined {
      return includeHidden
         ? this.sources
         : this.curateForDiscovery(this.sources);
   }

   public getSourceInfos(
      includeHidden = false,
   ): Malloy.SourceInfo[] | undefined {
      return includeHidden
         ? this.sourceInfos
         : this.curateForDiscovery(this.sourceInfos);
   }

   public getQueries(): ApiQuery[] | undefined {
      return this.curateForDiscovery(this.queries);
   }

   /**
    * The compiled model IR, or undefined when the model failed to compile —
    * the only place a field's EXPRESSION is available (`sourceInfos` and
    * `sources` are expression-free).
    *
    * Deliberately NOT curated: callers should join onto the curated
    * `getSourceInfos()` and use this only to look up fields it admitted.
    */
   public getModelDef(): ModelDef | undefined {
      return this.modelDef;
   }

   /** Memo for {@link annotationNearMisses}; a model's IR never changes. */
   private nearMissFindings: AnnotationFinding[] | undefined;

   /**
    * The annotations this model file wrote that do nothing: near misses of a
    * route Publisher owns, and prefixes Malloy cannot route (see
    * `annotation_near_miss.ts`). Empty when the model did not compile.
    */
   public annotationNearMisses(): AnnotationFinding[] {
      if (this.nearMissFindings === undefined) {
         this.nearMissFindings = this.modelDef
            ? annotationNearMisses(authoredNotes(this.modelDef))
            : [];
      }
      return this.nearMissFindings;
   }

   /** The compiled query of each notebook cell that runs one, with the model state its query compiled in. */
   public async preparedCellQueries(): Promise<
      Array<{ query: unknown; modelDef: ModelDef | undefined }>
   > {
      const out: Array<{ query: unknown; modelDef: ModelDef | undefined }> = [];
      for (const cell of this.runnableNotebookCells ?? []) {
         if (!cell.runnable) continue;
         let prepared: { _query?: unknown; _modelDef?: ModelDef };
         try {
            prepared =
               (await cell.runnable.getPreparedQuery()) as typeof prepared;
         } catch {
            // A cell that does not compile fails when it runs, not here.
            continue;
         }
         out.push({
            query: prepared._query,
            modelDef: prepared._modelDef ?? cell.modelDef,
         });
      }
      return out;
   }

   /**
    * The model file's text as this model's compile read it, or undefined when there is
    * none (a `.malloynb`, a compile failure, or an unreadable file). A compile
    * failure does carry text when it is a package compile's what-if
    * replacement, because that text is not on disk to fall back to.
    *
    * The only safe input for slicing a `DocumentLocation` out of: the ranges in
    * {@link getModelDef}'s IR index THIS text. Callers must not fall back to
    * reading the file, which can be newer than the compile.
    */
   public getCompiledSourceText(): string | undefined {
      return this.compiledSourceText;
   }

   /**
    * The facts report discovery reads off this model, or undefined when the
    * model failed to compile.
    *
    * Deliberately uncurated: `explores` curation shapes the discovery surface
    * agents see, and these facts are the raw material the manifest and the
    * lint are built from. See `service/report_manifest.ts`.
    */
   public getReportModelFacts(): ReportModelFacts | undefined {
      // Memoized: every input is fixed for this Model's life (a reload builds
      // new Models), and `carriesReportTag` asks on every query.
      return (this.reportModelFacts ??= {
         value: this.deriveReportModelFacts(),
      }).value;
   }

   private deriveReportModelFacts(): ReportModelFacts | undefined {
      if (!this.modelDef) return undefined;
      // `this.givens` is the model's surfaced given list — the same surface
      // `filterGivensToModelSurface` enforces at query time, so a control the
      // manifest advertises is one a query will accept.
      return readReportModelFacts(
         this.modelPath,
         this.modelDef,
         (this.givens ?? [])
            .map((given) => given.name)
            .filter((name): name is string => name !== undefined),
         // The EFFECTIVE gates per source on both routes, inheritance already
         // resolved by the extraction, so a suggest over a gated source learns
         // which givens its gates read. See `gateGivenSource`.
         new Map(
            (this.sources ?? []).flatMap((source) =>
               source.name
                  ? [
                       [
                          source.name,
                          gateGivenSource(this.sources ?? [], source.name) ??
                             [],
                       ] as const,
                    ]
                  : [],
            ),
         ),
      );
   }

   /**
    * {@link getReportModelFacts} plus what each layout tile's compiled query
    * reads (`compiledTileGivens`), for the manifest discovery serves.
    *
    * The static walk misses a given read through a joined source's `where:`, a
    * dimension defined with `$X`, or a gate, and cannot resolve a refinement;
    * Malloy's `givenUsage` has all of those. About a millisecond per tile, and
    * cached for this Model's life. A tile that fails to compile keeps the
    * static answer.
    */
   public getCompiledReportModelFacts(): Promise<ReportModelFacts | undefined> {
      this.compiledReportFacts ??= this.compileReportModelFacts().then(
         (facts) => (this.settledCompiledReportFacts = facts),
      );
      return this.compiledReportFacts;
   }

   private async compileReportModelFacts(): Promise<
      ReportModelFacts | undefined
   > {
      const facts = this.getReportModelFacts();
      const materializer = this.modelMaterializer;
      if (!facts || !materializer) return facts;
      let tiles: string[];
      try {
         tiles = (buildReportManifest(facts)?.tiles ?? []).flatMap((tile) =>
            tile.kind === "query" ? [tile.query] : [],
         );
      } catch {
         // Discovery builds it again and reports the throw.
         return facts;
      }
      const compiled = await compileTileGivens(
         tiles,
         materializer,
         this.modelDef?.givens,
         (source) => gateGivenSource(this.sources ?? [], source),
      );
      return { ...facts, compiledTileGivens: compiled };
   }

   /**
    * True when this model's curated discovery surface is empty (no sources or
    * named queries in its export closure), typically an import-only model.
    * Drives the load-time warning (Package.emptyDiscoveryWarnings) for a
    * listed model that would render blank.
    */
   public hasEmptyDiscoverySurface(): boolean {
      // No curation (no `explores`) ⇒ legacy listings include imported sources.
      if (!this.discoveryCurationEnabled) return false;
      if (this.modelType !== "model" || this.isNotebook() || !this.modelDef) {
         return false;
      }
      return (
         (this.getSources()?.length ?? 0) === 0 &&
         (this.getQueries()?.length ?? 0) === 0
      );
   }

   /**
    * File-level half of the query boundary: refuse a file that is not an
    * entry point. Inert when there is no surface or under `"all"`. Notebooks
    * and discovered dashboards always pass; they are always listed, and what
    * they may read is held to the surface by the name-level checks.
    *
    * Public because the model GET applies the same rule: a file nobody can
    * query is not shown either.
    */
   public assertFileOnSurface(): void {
      if (!this.fileOnSurface()) {
         throw this.notQueryable(
            `No queryable model "${this.modelPath}".`,
            true,
            "model",
         );
      }
   }

   /** No boundary: `queryableSources: "all"`, or no `explores` declared, so
    *  everything compiled stays queryable. */
   private boundaryInert(): boolean {
      const { mode, exploresDeclared } = this.queryBoundary;
      return mode === "all" || !exploresDeclared;
   }

   /** {@link assertFileOnSurface}'s test: no boundary, or this file is an entry point. */
   private fileOnSurface(): boolean {
      if (this.boundaryInert()) return true;
      return this.queryBoundary.isQueryEntryPoint || this.isNotebook();
   }

   /**
    * Whether the query boundary admits a query that names `name` as its source
    * in this file, by `sourceName` or as `run: name -> …`: the query route's
    * queryability rule, before any gate (`#(authorize)`, `#(access_filter)`)
    * is asked. True everywhere when the package has no boundary.
    *
    * The one rule: {@link assertQueryBoundaryEarly} admits a named source by
    * {@link sourceAdmission} after {@link assertFileOnSurface}, as this does,
    * and the entities behind `GET …/entities` report a source queryable by
    * asking this.
    */
   public admitsSourceByName(name: string): boolean {
      return (
         this.fileOnSurface() &&
         (this.boundaryInert() || this.sourceAdmission(name) !== undefined)
      );
   }

   /**
    * How the boundary admits a source named in a query to this file, which is
    * on the surface: `curated` when it is on the surface itself (cleared),
    * `report` when this served report builds it on the surface (deferred to
    * the compiled check, which settles it the same way), undefined when it is
    * refused.
    */
   private sourceAdmission(name: string): "curated" | "report" | undefined {
      if (this.isCuratedSource(name)) return "curated";
      if (this.isReportSourceOnSurface(name)) return "report";
      return undefined;
   }

   /**
    * A boundary refusal. Explains itself ({@link OffSurfaceError}) only when
    * `explainable` (the target is real) AND no gate is findable anywhere in
    * this model; otherwise it is the generic 404, indistinguishable from a
    * missing name. Unreadable IR counts as gated, so doubt lands generic.
    *
    * `refused` says what the fix has to move: this whole file, or one source or
    * query this file can already see. `name` is the refused source, when the
    * caller knows it: a source a served report declares itself gets a fix
    * aimed at its base rather than at the source.
    */
   private notQueryable(
      refusal: string,
      explainable: boolean,
      refused: "model" | "source" | "query",
      name?: string,
   ): NotQueryableError {
      const surface = this.queryBoundary.offSurface;
      if (explainable && surface && !this.governor.hasAnyAuthorizeNote()) {
         const reason =
            (refused === "source" &&
               name !== undefined &&
               this.reportSourceReason(surface, name)) ||
            this.offSurfaceReason(surface, refused);
         return new OffSurfaceError(`${refusal} ${reason}`);
      }
      return new NotQueryableError(refusal);
   }

   /**
    * The explanation for a source a served report declares in its own file
    * and that is refused, or undefined when `name` is not one. Exporting a
    * report's own source from index.malloy is impossible (index.malloy cannot
    * import a report), so the fix names the base the source is built on: that
    * base is what is missing from the surface. A source built straight on a
    * table has no base to publish, so it is told to build on a served source.
    */
   private reportSourceReason(
      surface: OffSurfaceContext,
      name: string,
   ): string | undefined {
      if (!this.queryBoundary.layout || !this.fileDerivationBases?.has(name)) {
         return undefined;
      }
      const exporter = surface.indexModel
         ? `"${INDEX_MODEL_NAME}"`
         : `a listed model`;
      const surfaceIs = surface.indexModel
         ? `this package's published surface, "${INDEX_MODEL_NAME}"`
         : `this package's published surface, the models publisher.json ` +
           `"explores" lists (${JSON.stringify(surface.files)})`;
      const base = this.offSurfaceBase(name);
      // offSurfaceBase stops at the first name with no declared base in this
      // file. When that is a source this model can see (one it imports), it is
      // the unserved base. Otherwise the chain ends at a table or connection.
      if (base !== name && this.declaresSource(base)) {
         return (
            `"${name}" is declared in this report on "${base}", which is not ` +
            `on ${surfaceIs}. A report's own source is queryable only when ` +
            `it is built on a source the surface serves. Fix: name "${base}" ` +
            `in the export { ... } of ${exporter}, importing the file that ` +
            `declares it if needed.`
         );
      }
      const served = surface.indexModel
         ? `a source "${INDEX_MODEL_NAME}" exports`
         : `a source exported by a model publisher.json "explores" lists ` +
           `(${JSON.stringify(surface.files)})`;
      return (
         `"${name}" is declared in this report and is not built on ${served}. ` +
         `Fix: build it with extend on ${served}.`
      );
   }

   /** The sentence an explained refusal appends: which files are the surface,
    *  and the edit that puts the refused thing on it. */
   private offSurfaceReason(
      surface: OffSurfaceContext,
      refused: "model" | "source" | "query",
   ): string {
      const where = surface.indexModel
         ? `It is not on this package's published surface, "${INDEX_MODEL_NAME}": ` +
           `only what that file exports is queryable, and only through it.`
         : `It is not on this package's published surface, the models ` +
           `publisher.json "explores" lists (${JSON.stringify(surface.files)}): ` +
           `only what those files export is queryable, and only through them.`;
      let fix: string;
      if (refused === "model") {
         fix = surface.indexModel
            ? `import "${this.modelPath}" in "${INDEX_MODEL_NAME}", name the ` +
              `sources you want in that file's export { ... }, and address ` +
              `the query to "${INDEX_MODEL_NAME}".`
            : // `explores` is deprecated: fix through a listed file.
              `import "${this.modelPath}" in a listed model, name the sources ` +
              `you want in that model's export { ... }, and address the ` +
              `query to that model.`;
      } else if (!this.isNotebook() && !this.isGridReport()) {
         // This file is on the surface and already sees the name, so the only
         // edit is to export it here.
         fix = `name the ${refused} in the export { ... } of "${this.modelPath}".`;
      } else {
         // A notebook or dashboard admits nothing of its own beyond sources it
         // builds on the surface (a refused one is explained by
         // reportSourceReason), so a surface file must export the name, which
         // may first need to import where it is declared. Dashboard tiles keep
         // running against the
         // dashboard file, so only a notebook caller is told to re-address.
         const exporter = surface.indexModel
            ? `"${INDEX_MODEL_NAME}"`
            : `a listed model`;
         const readdress = this.isGridReport()
            ? ""
            : surface.indexModel
              ? `, and address the query to "${INDEX_MODEL_NAME}"`
              : `, and address the query to that model`;
         fix =
            `name the ${refused} in the export { ... } of ${exporter}, ` +
            `importing the file that declares it if needed${readdress}.`;
      }
      return `${where} Fix: ${fix}`;
   }

   private declaresSource(name: string): boolean {
      return this.sources?.some((s) => s.name === name) ?? false;
   }

   /**
    * Stable identity of the definition `name` resolves to IN THIS MODEL's
    * namespace: declaring file plus position. This makes the package-wide
    * union safe: keyed on bare names, model A's exported `customers` would
    * admit model B's DIFFERENT, hidden `customers`.
    *
    * With `view`, the identity of that view on the source `name` resolves to,
    * so a view inherited by an extending source has its declaration's
    * identity in every model that can see it.
    *
    * Undefined when Malloy attached no location; callers then fall back to
    * this model's own export closure (fail closed).
    */
   public definitionIdentity(
      name: string | undefined,
      view?: string,
   ): string | undefined {
      if (!name) return undefined;
      const named = this.modelDef?.contents?.[name];
      const def =
         view === undefined
            ? named
            : named && isSourceDef(named)
              ? named.fields.find(
                   (field) => field.type === "turtle" && field.name === view,
                )
              : undefined;
      const location = (
         def as
            | {
                 location?: {
                    url?: string;
                    range?: { start?: { line?: number; character?: number } };
                 };
              }
            | undefined
      )?.location;
      const url = location?.url;
      if (!url) return undefined;
      const start = location?.range?.start;
      return `${url}#${start?.line ?? -1}:${start?.character ?? -1}`;
   }

   /**
    * True when `name` is admitted by the package-wide curated map: some listed
    * model exported `name`, AND this model resolves `name` to that same
    * declaration. See {@link definitionIdentity}.
    */
   private admittedByPackage(
      name: string,
      curated: ReadonlyMap<string, ReadonlySet<string>> | undefined,
   ): boolean {
      const identities = curated?.get(name);
      if (!identities) return false;
      const mine = this.definitionIdentity(name);
      return mine !== undefined && identities.has(mine);
   }

   /** True if `name` is a directly queryable source under the "declared"
    *  boundary: exported by THIS model, or exported by a sibling listed model
    *  and resolved here to that same declaration. */
   private isCuratedSource(name: string): boolean {
      if (this.ownCuratedSourceNames().has(name)) return true;
      return this.admittedByPackage(
         name,
         this.queryBoundary.packageCuratedSources,
      );
   }

   /** Named-query counterpart of {@link isCuratedSource}. */
   private isCuratedQuery(name: string): boolean {
      // A notebook's or dashboard's own queries admit nothing by name (a
      // dashboard's is deferred to the compiled check).
      if (
         !this.isNotebook() &&
         !this.isGridReport() &&
         (this.getQueries() ?? []).some((q) => q.name === name)
      )
         return true;
      return this.admittedByPackage(
         name,
         this.queryBoundary.packageCuratedQueries,
      );
   }

   /**
    * Query boundary, step 1 of 2 — the PRE-compilation gate for "queryable ==
    * discoverable": a top-level target must be on the package's discovery
    * surface. This is the *what* axis; `#(authorize)` is the orthogonal *who*
    * axis, and both must pass. Denials are a 404 ({@link notQueryable}). Inert
    * unless `explores` is declared and mode is "declared". A notebook's own
    * view admits nothing; only the package surface counts.
    *
    * Runs before compilation so compile errors cannot probe a denied target
    * (schema oracle). It only POSITIVELY denies; anything it cannot pin is
    * "deferred" to {@link assertQueryBoundaryCompiled}. "cleared" (explicit
    * curated names) skips the backstop: an exported named query may read
    * hidden sources internally, which is the author's deliberate exposure.
    */
   public assertQueryBoundaryEarly(
      sourceName?: string,
      queryName?: string,
      query?: string,
      /** More author text whose declarations count as derivation edges: the
       *  notebook cells before this one. Never caller text. */
      derivationContext?: string,
   ): "cleared" | "deferred" {
      // No surface or "all": everything compiled stays queryable.
      if (this.boundaryInert()) return "cleared";

      // File-level: named by the request URL, so nothing to resolve or evade.
      this.assertFileOnSurface();

      // A named query/view: admit on its own name, or on the named curated
      // source it runs against.
      if (queryName && !query) {
         // The query-name fast path must NOT fire when a source is also named
         // (`run: <source>-><query>`): a hidden source's view could collide
         // with an exported top-level query's name, and clearing skips the
         // compiled backstop.
         if (!sourceName && this.isCuratedQuery(queryName)) return "cleared";
         const admission = sourceName
            ? this.sourceAdmission(sourceName)
            : undefined;
         if (admission === "curated") return "cleared";
         if (admission === "report") return "deferred";
         // A dashboard's own named query (a single-query dashboard, or a
         // control's `suggest { query= }`) is decided by the compiled check, by
         // the source it reads.
         if (
            !sourceName &&
            this.isGridReport() &&
            (this.queries ?? []).some((q) => q.name === queryName)
         ) {
            return "deferred";
         }
         // The refusal names the caller's own words.
         if (sourceName) {
            throw this.notQueryable(
               `No queryable source "${sourceName}".`,
               this.declaresSource(sourceName),
               "source",
               sourceName,
            );
         }
         throw this.notQueryable(
            `No queryable query "${queryName}".`,
            (this.queries ?? []).some((q) => q.name === queryName),
            "query",
         );
      }

      // An explicitly-named source: admit iff curated, or a served report's
      // own source over the surface (settled again by the compiled check).
      if (sourceName) {
         const admission = this.sourceAdmission(sourceName);
         if (admission === "curated") return "cleared";
         if (admission === "report") return "deferred";
         throw this.notQueryable(
            `No queryable source "${sourceName}".`,
            this.declaresSource(sourceName),
            "source",
            sourceName,
         );
      }

      // Ad-hoc text: deny a model-declared, non-curated target that does not
      // derive from a curated one. Every `run:` is read, since Malloy compiles
      // (and reports errors for) every statement. Anything else defers.
      for (const target of query ? extractRunTargetSourceNames(query) : []) {
         if (
            !this.isCuratedSource(target) &&
            !this.derivesFromCurated(target, query, derivationContext) &&
            this.declaresSource(target)
         ) {
            // Not the name: in a gated model this must not reveal which hidden
            // names are real.
            throw this.notQueryable(
               "Query target is not queryable.",
               true,
               "source",
               target,
            );
         }
      }
      return "deferred";
   }

   /**
    * True when `name` is a source this served report (grid or linear) declares
    * in its own file, built through its own `extend` declarations, possibly
    * chained, on sources the package surface serves. Such a source is
    * queryable by name when the query is addressed to this report: its tiles
    * and cells already run against it, and ad-hoc text over it is admitted by
    * {@link derivesFromCurated}, so naming it is the same read. It inherits
    * its base's `#(authorize)` and access filters, which the authorize gate
    * applies after the boundary. False for every other file: an ordinary model
    * curates by its export closure, and a `.malloynb` has no report text.
    */
   private isReportSourceOnSurface(name: string): boolean {
      return (
         this.queryBoundary.layout !== undefined &&
         this.fileDerivationBases?.has(name) === true &&
         this.declaresSource(name) &&
         this.derivesFromCurated(name)
      );
   }

   /**
    * Whether `compileError` for ad-hoc `query` may be shown although
    * {@link queryTextSourcesQueryable} cannot vouch for the text, because no
    * run target can be read off it (`run` with no colon, SQL, a bare `run:`).
    *
    * A grammar failure says nothing about the model, so the only way the
    * answer could depend on a hidden name is a check that reads names before
    * the compile and refuses on one. Three do, and this is false whenever one
    * could act, so a hidden name and a missing one still get the same answer:
    *   - the run-target check, which has no target to read here (zero names);
    *   - the caller-join check, which refuses a hidden join base before the
    *     compile and lets a missing one through to it, so every join base
    *     must be curated or derived from curated sources in the text;
    *   - the lock checks, which read every name in the text and answer
    *     differently for a gated one: false when anything is gated, using the
    *     same test {@link notQueryable} applies before it will explain a
    *     refusal.
    * Where nothing is gated, the boundary already says a hidden source is real
    * (see {@link OffSurfaceError}), so there is no existence to protect.
    */
   private parseFailureNamesNothing(
      query: string,
      compileError: MalloyError,
   ): boolean {
      return (
         onlyParseFailures(compileError.problems) &&
         extractRunTargetSourceNames(query).length === 0 &&
         [...buildJoinBaseMap(query).values()].every((bases) =>
            [...bases].every(
               (b) =>
                  this.isCuratedSource(b) || this.derivesFromCurated(b, query),
            ),
         ) &&
         !this.governor.declaresAnyGate() &&
         !this.governor.hasAnyAuthorizeNote()
      );
   }

   /**
    * Whether the compiler's problems for ad-hoc `query` may be shown (400) or
    * the text keeps the boundary's 404. Every source the text NAMES, not just
    * run targets, must be curated or derive from curated: Malloy compiles
    * every statement, so a never-run declaration over a hidden source would
    * still leak its existence in the problems. No readable run target keeps
    * the 404.
    */
   private queryTextSourcesQueryable(query: string): boolean {
      const queryable = (name: string): boolean =>
         this.isCuratedSource(name) || this.derivesFromCurated(name, query);
      const runTargets = extractRunTargetSourceNames(query);
      if (runTargets.length === 0) return false;
      if (!runTargets.every(queryable)) return false;
      // Each caller-declared source's base chain must ground in curated too.
      for (const declared of buildDerivationBaseMap(query).keys()) {
         if (!queryable(declared)) return false;
      }
      return true;
   }

   /**
    * Query boundary, step 2 of 2 — the POST-compilation backstop for
    * "deferred" admissions, over the compiled run target
    * ({@link resolveAuthorizeSourceFromRunnable}). Admit iff it is curated or
    * derives from a curated source (composing over a queryable source is
    * queryable). FAIL CLOSED otherwise, including an unresolvable target.
    */
   public assertQueryBoundaryCompiled(
      compiledSource: string | undefined,
      query?: string,
      derivationContext?: string,
   ): void {
      const { mode, exploresDeclared } = this.queryBoundary;
      if (mode === "all" || !exploresDeclared) return;
      this.assertFileOnSurface();
      if (compiledSource) {
         if (this.isCuratedSource(compiledSource)) return;
         // A dashboard's own file supplies derivation edges even with no
         // request text.
         if (
            (query || derivationContext || this.fileDerivationBases) &&
            this.derivesFromCurated(compiledSource, query, derivationContext)
         )
            return;
      }
      throw this.notQueryable(
         "Query target is not queryable.",
         compiledSource !== undefined,
         "source",
         compiledSource,
      );
   }

   /**
    * Boundary re-check for /compile's authorize-denial conversion
    * (`denyHiddenAsNotQueryable` in service/environment.ts). /compile is exempt
    * from the boundary; this runs only after an authorize denial, on the
    * COMPILED run target (surface syntax misses `query:` indirection and
    * derivations), so the 403 is masked exactly where the query surface
    * answers 404.
    */
   public async assertCompiledTargetQueryable(
      runnable: { getPreparedQuery(): Promise<unknown> },
      query?: string,
   ): Promise<void> {
      const { mode, exploresDeclared } = this.queryBoundary;
      if (mode === "all" || !exploresDeclared) return;
      this.assertQueryBoundaryCompiled(
         await this.governor.resolveAuthorizeSourceFromRunnable(runnable),
         query,
      );
   }

   /**
    * The query boundary for one notebook cell: the same two checks as the
    * query route, with every code cell up to this one as derivation context,
    * so `source: big is orders extend {...}` in an earlier cell still counts
    * as derived from `orders`. Inert with no surface and under `"all"`.
    */
   private async assertNotebookCellOnSurface(
      cellIndex: number,
      runnable: { getPreparedQuery(): Promise<unknown> },
   ): Promise<void> {
      const { mode, exploresDeclared } = this.queryBoundary;
      if (mode === "all" || !exploresDeclared) return;
      const cells = this.runnableNotebookCells ?? [];
      const cellText = cells[cellIndex]?.text ?? "";
      const context = cells
         .slice(0, cellIndex + 1)
         .filter((c) => c.type === "code")
         .map((c) => c.text)
         .join("\n");
      const early = this.assertQueryBoundaryEarly(
         undefined,
         undefined,
         cellText,
         context,
      );
      if (early === "deferred") {
         this.assertQueryBoundaryCompiled(
            await this.governor.resolveAuthorizeSourceFromRunnable(runnable),
            cellText,
            context,
         );
      }
   }

   /**
    * Whether the query route would refuse this request for being off the
    * surface, for the load-time dashboard lint. Runs the same two checks as
    * {@link runQuery}, so the lint and the query cannot disagree.
    *
    * Returns undefined when the request would pass, or when it does not
    * compile: a tile that fails to compile is a different finding, and a
    * missing target must not be reported as a hidden one. On a refusal,
    * `source` is the source the query reads, when it can be read and the
    * query route's own refusal would name it.
    */
   public async surfaceRefusal(request: {
      queryName?: string;
      query?: string;
   }): Promise<{ source?: string } | undefined> {
      const { mode, exploresDeclared } = this.queryBoundary;
      if (mode === "all" || !exploresDeclared) return undefined;
      const text =
         request.query ??
         (request.queryName
            ? `run: ${quotedIdentifier(request.queryName)}`
            : undefined);
      if (!text || !this.modelMaterializer) return undefined;
      let compiledSource: string | undefined;
      try {
         const runnable = this.modelMaterializer.loadRestrictedQuery(
            "\n" + text,
         );
         await runnable.getPreparedQuery();
         compiledSource =
            await this.governor.resolveAuthorizeSourceFromRunnable(runnable);
      } catch (error) {
         // Only a compile failure is someone else's finding; anything else
         // makes lintDashboards report the lint incomplete.
         if (
            error instanceof MalloyError ||
            error instanceof ModelCompilationError
         ) {
            return undefined;
         }
         throw error;
      }
      try {
         const early = this.assertQueryBoundaryEarly(
            undefined,
            request.query ? undefined : request.queryName,
            request.query,
         );
         if (early === "deferred") {
            this.assertQueryBoundaryCompiled(compiledSource, request.query);
         }
         return undefined;
      } catch (error) {
         // Name the source only where the query route would (an explained
         // refusal); in a gated model that would give away what the 404 hides.
         if (error instanceof OffSurfaceError) {
            return {
               source: compiledSource && this.offSurfaceBase(compiledSource),
            };
         }
         if (error instanceof NotQueryableError) return {};
         throw error;
      }
   }

   /**
    * The source to name in a refusal's fix. For a source this dashboard
    * declares (`source: staged is orders_staging extend {...}`), exporting it
    * from index.malloy is impossible, so follow its declared bases to the one
    * that is off the surface. Otherwise the name itself.
    */
   private offSurfaceBase(name: string): string {
      const seen = new Set<string>();
      let current = name;
      while (!seen.has(current)) {
         seen.add(current);
         const bases = this.fileDerivationBases?.get(current);
         const next = bases
            ? Array.from(bases).find((base) => !this.isCuratedSource(base))
            : undefined;
         if (!next) return current;
         current = next;
      }
      return current;
   }

   /** A `.malloynb`, or a `.malloy` the package discovered as a served notebook. */
   public isNotebook(): boolean {
      return (
         this.modelPath.endsWith(NOTEBOOK_FILE_SUFFIX) ||
         this.queryBoundary.layout === "linear"
      );
   }

   /** The file format this notebook is written in; `malloy` for a served notebook. */
   private notebookFormat(): NotebookFormat {
      return this.modelPath.endsWith(NOTEBOOK_FILE_SUFFIX)
         ? "malloynb"
         : "malloy";
   }

   /**
    * Whether this compiled file carries a model-level `## artifact` tag
    * (`report.ts`), so the renderer's "unknown tag" findings for the tags
    * Publisher owns are dropped. For a check that runs before discovery has
    * said; discovery's map is the answer after it.
    */
   public carriesReportTag(): boolean {
      // Memoized, like the facts it reads: the query paths ask on every query.
      return (this.reportTagCarried ??= this.deriveCarriesReportTag());
   }

   private deriveCarriesReportTag(): boolean {
      if (!isReportEligiblePath(this.modelPath)) return false;
      const facts = this.getReportModelFacts();
      return facts !== undefined && factsReportTag(facts).status !== "none";
   }

   private isGridReport(): boolean {
      return this.queryBoundary.layout === "grid";
   }

   /** The names this file curates by exporting them; see {@link curation}. */
   private ownCuratedSourceNames(): ReadonlySet<string> {
      return this.curation.ownCuratedSourceNames;
   }

   /**
    * True if `name` is PROVABLY a composition over the curated surface, by
    * walking the text's own derivation declarations.
    *
    * Uses {@link buildDerivationBaseMap} over
    * {@link stripMalloyCommentsAndLiterals} text (as
    * {@link requestChainProvesUngated} does): it follows `query:` hops (the
    * pre-aggregate idiom `query: agg is <curated> -> {…}`, `source: blended is
    * agg extend {…}`), and stripping prevents edges forged in literals or
    * comments.
    *
    * The quantifier is inverted from the authorize gate: an extra edge here
    * would widen ADMISSION, so a name is admitted only if EVERY declared base
    * proves out; a forged or shadowing edge can only add an obligation.
    *
    * Fails closed on a name with no declared base, a chain longer than
    * {@link REQUEST_CHAIN_MAX_NAMES}, and a cycle.
    */
   private derivesFromCurated(
      name: string,
      query?: string,
      derivationContext?: string,
   ): boolean {
      return this.curatedBasesOf(name, query, derivationContext) !== undefined;
   }

   /**
    * The curated names `name` is, or derives from: `{name}` when it is
    * curated itself, else the union over every declared base, each of which
    * must prove out ({@link derivesFromCurated} is "this is defined").
    * Undefined when the walk proves nothing.
    *
    * Public, for the entities: a served report's own source is reported in
    * the place of the served source it is built on, and this names it.
    */
   public curatedBasesOf(
      name: string,
      query?: string,
      derivationContext?: string,
   ): ReadonlySet<string> | undefined {
      // With nothing but this file's own edges, the answer is a property of
      // the boundary in force, so it is asked once per name.
      if (!query && !derivationContext) {
         const memo = this.curation.curatedBasesOf;
         if (memo.has(name)) return memo.get(name);
         const answer = this.walkCuratedBases(
            name,
            this.fileDerivationBases ?? new Map(),
         );
         memo.set(name, answer);
         return answer;
      }
      // A dashboard's own declarations are edges too, merged (never replacing)
      // so an extra edge only adds an obligation.
      const basesOf = query
         ? buildDerivationBaseMap(stripMalloyCommentsAndLiterals(query))
         : new Map<string, Set<string>>();
      const contextBases = derivationContext
         ? buildDerivationBaseMap(
              stripMalloyCommentsAndLiterals(derivationContext),
           )
         : undefined;
      for (const [derived, bases] of [
         ...(this.fileDerivationBases ?? []),
         ...(contextBases ?? []),
      ]) {
         const into = basesOf.get(derived);
         if (into) for (const base of bases) into.add(base);
         else basesOf.set(derived, new Set(bases));
      }
      return this.walkCuratedBases(name, basesOf);
   }

   /** {@link curatedBasesOf}'s walk over the derivation edges `basesOf`, which
    *  it only reads. */
   private walkCuratedBases(
      name: string,
      basesOf: ReadonlyMap<string, ReadonlySet<string>>,
   ): ReadonlySet<string> | undefined {
      const own = this.ownCuratedSourceNames();
      const packageCurated = this.queryBoundary.packageCuratedSources;
      // Only positive results are memoized: a name proven curated is proven
      // wherever it appears, while a failure may be the local verdict of the
      // in-progress cycle guard rather than a property of the name.
      const proven = new Map<string, Set<string>>();
      const inProgress = new Set<string>();
      const proves = (current: string): Set<string> | undefined => {
         if (
            own.has(current) ||
            this.admittedByPackage(current, packageCurated)
         )
            return new Set([current]);
         const known = proven.get(current);
         if (known) return known;
         // A back-edge or an over-long chain grounds nothing: deny.
         if (inProgress.has(current)) return undefined;
         if (inProgress.size >= REQUEST_CHAIN_MAX_NAMES) return undefined;
         const bases = basesOf.get(current);
         if (!bases || bases.size === 0) return undefined;
         inProgress.add(current);
         const roots = new Set<string>();
         let ok = true;
         for (const base of bases) {
            const reached = proves(base);
            if (!reached) {
               ok = false;
               break;
            }
            for (const root of reached) roots.add(root);
         }
         inProgress.delete(current);
         if (!ok) return undefined;
         proven.set(current, roots);
         return roots;
      };
      return proves(name);
   }

   /**
    * Every `#@ preaggregate` declaration in this model that cannot take effect.
    *
    * Read off `modelDef.contents`, so it reaches even a source nothing
    * references. Returned rather than thrown so the Package can report every
    * bad declaration at once.
    *
    * Memoized: `/status` reaches this for every package on every poll, and a
    * reload replaces the `Model`. Callers share one array, so it is `readonly`
    * down to the element (by type, not a per-element freeze).
    */
   public preaggregateViolations(): readonly Readonly<PreaggregateViolation>[] {
      // Shared, so the identity contract holds for a failed compile too.
      if (!this.modelDef) return NO_PREAGGREGATE_VIOLATIONS;
      if (this.preaggregateViolationsMemo === undefined) {
         this.preaggregateViolationsMemo = validateModelPreaggregation(
            this.modelDef.contents as Record<string, unknown>,
            this.modelDef.modelID,
         );
      }
      return this.preaggregateViolationsMemo;
   }

   /**
    * Compile-time renderer-tag validation, run on the main thread: the
    * renderer mutates DOM globals at import, which would destabilize the
    * pure-CPU package-load worker.
    *
    * Prepares each annotated named query and source view compile-only and runs
    * the renderer's headless `validateRenderTags`. Findings are returned, each
    * naming its target, as non-fatal warnings with the renderer's severity:
    * `error` for a tag malformed for its field, `warn` for one well-formed but
    * inert where it sits (e.g. `# colspan` outside columns mode), which is
    * invisible at query time, so load is the only place an author sees it. The
    * owning Package drops the ones a grid report's tiles explain, then logs
    * and surfaces the rest.
    */
   public async validateRenderTags(): Promise<RenderTagWarning[]> {
      const mm = this.modelMaterializer;
      if (!mm) {
         return [];
      }
      // Dynamic import: the validator is heavy, so load it only when needed.
      const { validateRenderTags } = await import(
         "@malloydata/render-validator"
      );

      // The worker prepares targets in order up to its soft deadline; the rest
      // are prepared here. Taken once.
      const fromWorker = this.preparedRenderTagResults ?? [];
      this.preparedRenderTagResults = undefined;
      const remaining = renderTagTargets(this.queries, this.sources).slice(
         fromWorker.length,
      );
      const prepared = [
         ...fromWorker.filter(
            (entry): entry is { label: string; result: Malloy.Result } =>
               entry.result !== undefined,
         ),
         ...(remaining.length > 0
            ? await this.prepareRenderTagTargets(mm, remaining)
            : []),
      ];

      const findings: RenderTagWarning[] = [];
      // Nothing below does I/O; yield so a large model does not hold the
      // event loop for ~1 s.
      const yielder = createEventLoopYielder();
      for (const { label, result } of prepared) {
         await yielder.maybeYield();
         // No severity filter: an inert `warn` tag is exactly the finding an
         // author cannot see otherwise, and an unknown future severity is
         // surfaced as `warn` below rather than dropped. Publisher-owned tags
         // (`# artifact`, `# drill`) are removed first, as on the query paths.
         // Tile layout tags (`# colspan` and the rest) are kept whatever this
         // file is: whether one is read depends on whether a grid report names
         // this view as a tile, which may be another file, so the package
         // decides once its reports are known (dropGridTileLayoutFindings).
         const logs = filterPublisherOwnedRenderLogs(
            validateRenderTags(result),
            false,
         );
         // Not logged here: the package logs what survives its tile filter,
         // so a tile's layout tag does not log a warning on every load.
         for (const e of logs) {
            findings.push({
               subject: label,
               message: e.message,
               severity: e.severity === "error" ? "error" : "warn",
            });
         }
      }
      return findings;
   }

   /**
    * Results the package-load worker prepared for the render-tag check, held
    * until {@link validateRenderTags} reads them.
    */
   private preparedRenderTagResults:
      | { label: string; result?: Malloy.Result }[]
      | undefined;

   /**
    * Prepare render-tag targets compile-only: the ones the worker did not
    * reach. A target that fails to prepare is reported by the normal compile
    * path, so it is skipped rather than masked here.
    */
   private async prepareRenderTagTargets(
      mm: ModelMaterializer,
      targets: RenderTagTarget[],
   ): Promise<{ label: string; result: Malloy.Result }[]> {
      // One QueryModel for every prepare below, not one each.
      await this.ensureSharedQueryModel(mm);
      const yielder = createEventLoopYielder();
      const prepared: { label: string; result: Malloy.Result }[] = [];
      for (const target of targets) {
         await yielder.maybeYield();
         try {
            const result = await mm
               .loadQuery(target.queryString)
               .getPreparedResult();
            prepared.push({
               label: target.label,
               result: result.toStableResult(),
            });
         } catch {
            continue;
         }
      }
      return prepared;
   }

   public async getModel(): Promise<ApiCompiledModel> {
      if (this.compilationError) {
         throw this.compilationError;
      }

      if (this.modelType === "model") {
         return this.getStandardModel();
      } else {
         throw new ModelNotFoundError(
            `${this.modelPath} is not a valid model name.  Model files must end in .malloy.`,
         );
      }
   }

   /**
    * The error this model failed to compile with, if any. A failed model is kept
    * in the package (as a placeholder) so listings can show it as broken rather
    * than omit it.
    */
   public getCompilationError(): MalloyError | Error | undefined {
      return this.compilationError;
   }

   public getNotebookError(): MalloyError | Error | undefined {
      return this.getCompilationError() ?? this.notebookReaderError();
   }

   /**
    * A notebook's listing title and description: `## title="…"`, then the
    * `#"` doc comment (the dashboard's chain), then the notebook's first
    * markdown heading. No title when nothing resolves (the caller shows the
    * filename).
    */
   public getNotebookListing(): { title?: string; description?: string } {
      if (!this.isNotebook()) return {};
      if (!this.modelPath.endsWith(NOTEBOOK_FILE_SUFFIX)) {
         return this.getServedNotebookListing();
      }
      // This notebook's own `##` only: a title belongs to one document, so an
      // imported model's `## title=` or `#"` doc comment must not become it.
      const annotations = this.modelDef ? ownModelNotes(this.modelDef) : [];
      // Both fields together, so the line the title falls back to is not also
      // printed as the description; see `docCommentTitleAndDescription`.
      const doc = docCommentTitleAndDescription(
         annotations,
         tagText(motlyTag(annotations), "title"),
      );
      return {
         title: doc.title ?? this.firstMarkdownHeading(),
         description: doc.description,
      };
   }

   /**
    * A served notebook's listing: `title` in the artifact tag, then the first
    * line of the `"` notes above the artifact line, then the first markdown
    * heading when no prose precedes it. Notes below the tag are cells, not
    * description.
    */
   private getServedNotebookListing(): {
      title?: string;
      description?: string;
   } {
      const notes = this.modelDef ? ownModelNoteObjects(this.modelDef) : [];
      const artifact = motlyTag(notes.map((note) => note.text))?.tag(
         "artifact",
      );
      const doc = docCommentTitleAndDescription(
         docNotesAboveArtifact(notes),
         tagText(artifact, "title"),
      );
      return {
         title: doc.title ?? this.firstMarkdownHeading(),
         description: doc.description,
      };
   }

   /**
    * Attach the cells of a served notebook, which is only known to be one after
    * discovery, and recompute the givens each cell may name.
    */
   public setNotebookCells(cells: RunnableNotebookCell[]): void {
      this.runnableNotebookCells = cells;
      this.notebookCellDeclaredGivens = declaredGivensPerCell(cells);
   }

   /**
    * Read a served notebook's cells out of `text`, the text this model compiled,
    * and attach them. A reader refusal becomes this notebook's error and leaves
    * it with no cells; the model itself keeps serving. `ok` when its cells were
    * read, `refused` when the reader refused them, `broken` when it did not
    * compile and so has none.
    */
   public attachServedNotebookCells(text: string): "ok" | "refused" | "broken" {
      this.notebookReaderRefusal = undefined;
      this.notebookAnnotations = undefined;
      this.notebookLayout = undefined;
      const modelDef = this.modelDef;
      const materializer = this.modelMaterializer;
      if (this.compilationError || !modelDef || !materializer) {
         this.setNotebookCells([]);
         return "broken";
      }
      const refuse = (error: NotebookReaderError) => {
         this.notebookReaderRefusal = error;
         this.setNotebookCells([]);
         return "refused" as const;
      };
      const parse = parseNotebookText(text);
      if (isNotebookReaderError(parse)) return refuse(parse);
      const read = readNotebookCells(parse, modelDef, text);
      if (read.error) return refuse(read.error);
      this.notebookAnnotations = read.annotations;
      const layout = this.readNotebookLayout();
      this.notebookLayout = layout;
      // A linear report's prose and queries are its tiles, in tile order; the
      // file's own cells are only its definitions (`report.ts`).
      const cells: RunnableNotebookCell[] = read.cells
         .filter((cell) => cell.kind === "definition")
         .map(
            (cell): RunnableNotebookCell => ({
               type: "code",
               kind: "definition",
               text: cell.text,
               markdown: cell.markdown,
               proseLines: cell.proseLines,
               codeLine: cell.codeLine,
               caption: cell.caption,
               modelDef,
            }),
         );
      if (layout) {
         for (const tile of layout.tiles ?? []) {
            if (tile.kind === "text") {
               cells.push({
                  type: "markdown",
                  kind: "markdown",
                  text: tile.markdown,
               });
               continue;
            }
            // malloy-text: a report tile's query, the author's Malloy, run as written
            const text = `run: ${tile.query}`;
            let queryInfo: Promise<Malloy.QueryInfo | undefined> | undefined;
            cells.push({
               type: "code",
               kind: "query",
               text,
               proseLines: [],
               runnable: materializer.loadQuery(text),
               modelMaterializer: materializer,
               modelDef,
               deriveQueryInfo: () =>
                  (queryInfo ??= tileQueryInfo(materializer, text)),
            });
         }
      }
      this.setNotebookCells(cells);
      return "ok";
   }

   /** The manifest of a served linear `.malloy` report, which lists its tiles. */
   private readNotebookLayout(): ServedReportManifest | undefined {
      // Discovery settles the compiled facts first, so both routes serve the same tile `givenNames`.
      const facts =
         this.settledCompiledReportFacts ?? this.getReportModelFacts();
      const manifest = facts && buildReportManifest(facts);
      return manifest?.layout === "linear" && manifest.tiles
         ? manifest
         : undefined;
   }

   /** Why the reader refused this served notebook's cells, if it did. */
   public getNotebookReaderRefusal(): NotebookReaderError | undefined {
      return this.notebookReaderRefusal;
   }

   /** The reader's refusal as the error a notebook that did not compile fails with. */
   private notebookReaderError(): ModelCompilationError | undefined {
      const refusal = this.notebookReaderRefusal;
      return refusal
         ? new ModelCompilationError({
              message: `${this.modelPath}: ${refusal.message}`,
           })
         : undefined;
   }

   /**
    * The text of the notebook's first markdown heading, at any level, only if
    * no prose precedes it (a notebook opening with a paragraph is not naming
    * itself).
    */
   private firstMarkdownHeading(): string | undefined {
      for (const cell of this.runnableNotebookCells ?? []) {
         if (cell.type !== "markdown") continue;
         for (const line of cell.text.split("\n")) {
            const trimmed = line.trim();
            if (trimmed.length === 0) continue;
            const heading = /^#{1,6}\s+(.+)$/.exec(trimmed);
            return heading ? heading[1] : undefined;
         }
      }
      return undefined;
   }

   public async getNotebook(): Promise<ApiRawNotebook> {
      if (this.compilationError) {
         throw this.compilationError;
      }
      if (this.isNotebook()) {
         const readerError = this.notebookReaderError();
         if (readerError) throw readerError;
         return this.getNotebookModel();
      } else {
         throw new ModelNotFoundError(
            `${this.modelPath} is not a valid notebook name.  Notebook files must end in .malloynb, or be a .malloy report with a model-level ## artifact { layout=linear tiles=[…] } tag.`,
         );
      }
   }

   /** The served copy reading this model's state. */
   private newServedCopy(): ServedCopyServe {
      return new ServedCopyServe({
         // The author's compile of a query, reading no table: what a query
         // over a table with a column left out is checked against.
         authorResultShape: async (queryString, givens) => {
            const materializer = this.modelMaterializer;
            if (!materializer) return undefined;
            return resultShapeOf(
               await materializer
                  .loadRestrictedQuery(queryString)
                  .getPreparedResult({
                     buildManifest: EMPTY_BUILD_MANIFEST,
                     givens: givens as never,
                  }),
            );
         },
         ensureSharedQueryModel: (materializer) =>
            this.ensureSharedQueryModel(materializer),
         modelPath: () => this.modelPath,
         modelDef: () => this.modelDef,
         // Storage tables only while the storage tier serves; colocated tables
         // (on the source's own connection) need no destination.
         serveDestinationConfig: () =>
            getPersistStorageMode() === "on"
               ? this.storageServe.serveDestinationConfig
               : undefined,
      });
   }

   /** See {@link StorageBindings.setServeDestinationConfig}. */
   public setServeDestinationConfig(provider: () => MalloyConfig): void {
      this.storageServe.setServeDestinationConfig(provider);
      this.servedCopy.invalidate();
      this.rollupVariants.invalidate();
      this.persistedViews.invalidate();
   }

   /**
    * The package directory and connections this model's served copy compiles
    * with. Set by the owning package with its bindings.
    */
   public setServedCopyContext(
      packagePath: string,
      malloyConfig: ModelConnectionInput,
      materializePlans: Readonly<Record<string, MaterializePlan>> = {},
      declarationOf?: (sourceID: string) => unknown,
   ): void {
      this.materializeSourceIDs = new Set(Object.keys(materializePlans));
      this.servedCopyContext = [
         packagePath,
         malloyConfig,
         materializePlans,
         declarationOf,
      ];
      this.servedCopy.setContext(...this.servedCopyContext);
      this.persistedViews.setContext(packagePath, malloyConfig);
      this.rollupVariants.setContext(packagePath, malloyConfig);
   }

   /**
    * Whether the router may try the persisted-view leg: a model file, the
    * materialize switch on, and a persisted view on a source it sees.
    */
   private persistedViewsEnabled(): boolean {
      return (
         this.modelType === "model" &&
         !this.isNotebook() &&
         getMaterializeEnabled() &&
         this.persistedViews.hasViews()
      );
   }

   /**
    * The persisted views' generated build sources this model can see, by
    * `sourceID`, with the source and view each stores: the package's serve
    * verdict reads a view's reach through its source.
    */
   public persistedViewBuilds(): {
      buildSourceID: string;
      sourceID: string;
      sourceName: string;
      viewName: string;
   }[] {
      if (!getMaterializeEnabled()) return [];
      return this.persistedViews.plans().map((p) => ({
         buildSourceID: p.buildSourceID,
         sourceID: p.sourceID,
         sourceName: p.sourceName,
         viewName: p.viewName,
      }));
   }

   /**
    * Whether the router may try the served copy: a model file. It reads a
    * colocated table on the source's own connection, and a storage table
    * only while `PERSIST_STORAGE_MODE=on` (its host withholds the
    * destinations otherwise). Warnings and the compile at bind follow the
    * same rule, so a package never warns about a copy that is not routed.
    */
   private servedCopyEnabled(): boolean {
      // A notebook or report is not a model file the served copy can load.
      return this.modelType === "model" && !this.isNotebook();
   }

   /**
    * Whether the router may try the `#(preaggregate)` rollup leg: a model file
    * and a rollup declared on a source it sees.
    */
   private rollupVariantsEnabled(): boolean {
      return (
         this.modelType === "model" &&
         !this.isNotebook() &&
         // v1 `#@ preaggregate` rollups are read here too (G4).
         this.rollupVariants.hasRollups()
      );
   }

   /** Compile the served copy now, when it is enabled, so its warnings exist. */
   public async prepareServedCopy(): Promise<void> {
      if (!this.servedCopyEnabled()) return;
      await this.servedCopy.prepare();
   }

   /**
    * The definitions this model's build view leaves out because they join
    * sources across connections (#6982); empty for any other model.
    */
   public crossConnectionLeftOutDefinitions(): readonly LeftOutDefinition[] {
      return this.crossConnectionLeftOut ?? [];
   }

   /**
    * The sources this model's served copy answers from storage, by author
    * `sourceID`: undefined when the served copy is off for this model, and
    * `pending` before it has compiled.
    */
   public servedCopySourceIDs(): ReadonlySet<string> | "pending" | undefined {
      if (!this.servedCopyEnabled()) return undefined;
      return this.servedCopy.rerootedSourceIDs() ?? "pending";
   }

   /** Why this model's served copy leaves sources unserved (empty when off). */
   public servedCopyProblems(): readonly ServedCopyProblem[] {
      if (!this.servedCopyEnabled()) return [];
      return this.servedCopy.problems();
   }

   /** Drop every compiled leg (the destinations or the model changed). */
   public invalidateServeShapeCache(): void {
      this.servedCopy.invalidate();
      this.rollupVariants.invalidate();
      this.persistedViews.invalidate();
   }

   /** See {@link StorageBindings.setServeBindings}. */
   public setServeBindings(bindings: ServeBinding[]): void {
      // A `#(materialize)` table is presented under its author's source name,
      // and only the served copy reads it, so it is kept off the storage
      // bindings the path report and the type check read.
      const materialized = this.materializeSourceIDs;
      // A persisted view's table holds the view's result, and a
      // `#(preaggregate)` rollup's its partials, not a source's rows: only
      // their own legs read them.
      const views = this.persistedViews.buildSourceIDs();
      const rollups = this.rollupVariants.buildSourceIDs(bindings);
      // A colocated `#(materialize)` table is read on its own connection by
      // the served copy alone; no other leg reads one.
      const colocated = bindings.filter((b) => b.colocated);
      const stored =
         colocated.length === 0
            ? bindings
            : bindings.filter((b) => !b.colocated);
      const others =
         views.size === 0 && rollups.size === 0
            ? stored
            : stored.filter(
                 (b) =>
                    !(b.sourceIDs ?? []).some(
                       (id) => views.has(id) || rollups.has(id),
                    ),
              );
      this.storageServe.setServeBindings(
         materialized.size === 0
            ? others
            : others.filter(
                 (b) => !(b.sourceIDs ?? []).some((id) => materialized.has(id)),
              ),
      );
      // Colocated tables, v1 `#@ persist` and `#(materialize)` alike, are
      // read by the served copy alone.
      this.servedCopy.setBindings(
         colocated.length === 0 ? others : [...others, ...colocated],
      );
      this.rollupVariants.setBindings(stored);
      this.persistedViews.setBindings(stored);
   }

   /** See {@link StorageBindings.storedColumnTypeMismatches}. */
   public storedColumnTypeMismatches(): readonly SourceTypeMismatches[] {
      return this.storageServe.storedColumnTypeMismatches();
   }

   /**
    * The source names this model keeps a storage serve binding for: the
    * package pushes every binding to every model, and a model drops one that
    * materializes a same-named source declared elsewhere (see
    * {@link StorageBindings.setServeBindings}).
    */
   public servedStorageSourceNames(): string[] {
      return [
         ...this.storageServe.serveBindings,
         ...this.persistedViews.viewBindings(),
      ].map((b) => b.sourceName);
   }

   /**
    * Whether each named view of the given stored sources compiles against a
    * serve shape over `bindings`, without touching this model's own shape
    * (ms2data/service#7090). The shape is a throwaway: it compiles against the
    * same destinations a served query would, and needs no built table, since a
    * virtual source declares its columns. Empty when no destination is
    * configured, since then nothing serves from storage either way.
    */
   /**
    * The v1 cutover's preview (dry run): the sources this model's served copy
    * would re-root over `bindings` (declared from the plan's output columns,
    * so no table is read), and why it leaves the others as written. Compiled
    * in a served copy of its own, with the cutover's per-column types, and
    * discarded. Undefined when this model has no served copy to compile.
    */
   public async previewServedCopy(bindings: ServeBinding[]): Promise<
      | {
           rerooted: ReadonlySet<string>;
           throughBase: ReadonlySet<string>;
           gateDeclined: ReadonlySet<string>;
           problems: readonly ServedCopyProblem[];
        }
      | undefined
   > {
      if (
         this.modelType !== "model" ||
         this.isNotebook() ||
         !this.modelDef ||
         !this.servedCopyContext ||
         bindings.length === 0
      ) {
         return undefined;
      }
      const destinations = this.storageServe.serveDestinationConfig;
      const copy = new ServedCopyServe({
         modelPath: () => this.modelPath,
         modelDef: () => this.modelDef,
         // Compile-only, as the view preview: a destination's catalog need
         // not exist before the first build.
         serveDestinationConfig: () =>
            destinations && getPersistStorageMode() === "on"
               ? () => compileOnlyDestinationConfig(destinations())
               : undefined,
      });
      copy.setContext(...this.servedCopyContext);
      copy.setBindings(bindings);
      await copy.prepare();
      return {
         rerooted: copy.rerootedSourceIDs() ?? new Set<string>(),
         throughBase: copy.servedThroughBaseSourceIDs(),
         gateDeclined: copy.gateDeclinedSourceIDs(),
         problems: copy.problems(),
      };
   }

   public async previewStorageViews(
      bindings: ServeBinding[],
   ): Promise<Map<string, ViewServeVerdict[]>> {
      const out = new Map<string, ViewServeVerdict[]>();
      const destinations = this.storageServe.serveDestinationConfig;
      if (
         !destinations ||
         bindings.length === 0 ||
         !this.modelDef ||
         !this.servedCopyContext ||
         !this.servedCopyEnabled()
      ) {
         return out;
      }
      // The same identity filter the model's own bindings pass (#1001).
      const kept = new StorageBindings({
         modelPath: () => this.modelPath,
         modelDef: () => this.modelDef,
         quiet: true,
      });
      kept.setServeBindings(bindings);
      const stored = kept.serveBindings.filter(
         (b) => b.origin !== "preaggregate",
      );
      if (stored.length === 0) return out;
      // A throwaway served copy over these bindings, compile-only: before the
      // first build a DuckLake destination's catalog does not exist yet, and
      // the dialect must not depend on reaching it.
      const copy = new ServedCopyServe({
         modelPath: () => this.modelPath,
         modelDef: () => this.modelDef,
         quiet: true,
         serveDestinationConfig: () => () =>
            compileOnlyDestinationConfig(destinations()),
      });
      copy.setContext(...this.servedCopyContext);
      copy.setBindings(stored);
      await copy.prepare();
      const problems = copy.problems();
      // A view reading a given with no default serves once the caller
      // supplies it; the preview compiles with a placeholder in its place, so
      // it judges the stored columns, not the request.
      const placeholders = placeholderGivens(this.givens);
      for (const binding of stored) {
         const def = this.modelDef.contents?.[binding.sourceName] as
            | {
                 fields?: readonly {
                    name?: string;
                    as?: string;
                    type?: string;
                 }[];
              }
            | undefined;
         const views = (def?.fields ?? [])
            .filter((f) => f.type === "turtle")
            .map((f) => f.as ?? f.name)
            .filter((name): name is string => typeof name === "string");
         // Why the copy left this source as written, when it did.
         const left = problems.find(
            (p) =>
               p.sourceName === binding.sourceName &&
               (p.sourceID === undefined ||
                  (binding.sourceIDs ?? [p.sourceID]).includes(p.sourceID)),
         );
         const verdicts: ViewServeVerdict[] = [];
         for (const view of views) {
            try {
               await copy.loadQuery(
                  `run: ${quotedIdentifier(binding.sourceName)} -> ${quotedIdentifier(view)}`,
                  placeholders,
               );
               verdicts.push({ view, verdict: "serves" });
            } catch (error) {
               verdicts.push({
                  view,
                  verdict: "live",
                  detail: compileErrorLine(
                     left?.detail ??
                        (error instanceof Error
                           ? error.message
                           : String(error)),
                  ),
               });
            }
         }
         // By the plan's sourceID: two models' same-named stored sources are
         // different sources with different views.
         out.set(binding.sourceIDs?.[0] ?? binding.sourceName, verdicts);
      }
      return out;
   }

   /**
    * Refuse a request whose value for one of this model's `filter<T>` givens
    * does not parse, with a 400 naming the given and the parser's reason. Run
    * by a query and a notebook cell after their authorize gate, so a denied
    * caller still gets its 403. /compile checks the submitted text's own givens
    * instead (see Environment.compileSource).
    */
   public assertFilterGivens(
      givens: Record<string, GivenValue> | undefined,
   ): void {
      assertFilterGivensParse(this.givens, givens);
   }

   /**
    * Run a query against this model: a named view on a source, a named
    * query, or ad-hoc Malloy text. Traced as one `publisher.query` span.
    *
    * `privileges` relaxes governance checks and defaults to none; only an
    * adapter that decided the caller holds one passes it.
    */
   public async runQuery(
      // Receives what the query learned for the access record; see `QueryAudit`.
      audit: QueryAudit,
      request: QueryRequest,
      privileges: QueryPrivileges = QueryPrivileges.none,
   ): Promise<ModelQueryResults> {
      return withSpan(
         "publisher.query",
         {
            "malloy.package": this.packageName,
            "malloy.model.path": this.modelPath,
         },
         async (span) => {
            const result = await this.runQueryResults(
               audit,
               request,
               privileges,
            );
            if (result.servedFrom)
               span.setAttribute("publisher.served_from", result.servedFrom);
            return result;
         },
      );
   }

   private async runQueryResults(
      audit: QueryAudit,
      request: QueryRequest,
      privileges: QueryPrivileges,
   ): Promise<ModelQueryResults> {
      // A stored table is read only by a candidate that reports it in the
      // path report: the served copy, the rollup leg or the view leg. The
      // live compile reads none (see liveBuildManifest).
      const servedCopy = this.servedCopy;
      const {
         sourceName,
         queryName,
         query,
         filterParams,
         givens,
         abortSignal,
         queryMetadataInput,
      } = request;
      const responseShape = request.responseShape ?? "full";
      const { bypassFilters, bypassAuthorize, includeHiddenFilesAndSources } =
         privileges;
      const startTime = performance.now();
      if (this.compilationError) {
         // Re-throw MalloyError and ModelCompilationError as-is (they map to 400/424)
         if (
            this.compilationError instanceof MalloyError ||
            this.compilationError instanceof ModelCompilationError
         ) {
            throw this.compilationError;
         }
         // For other compilation errors, wrap as BadRequestError (400)
         throw new BadRequestError(
            `Model compilation failed: ${this.compilationError.message}`,
         );
      }

      let runnable: QueryMaterializer;
      let liveRunnable: QueryMaterializer | undefined;
      // Hoisted so the row-level recompile after the try block reuses the
      // SAME caller text.
      let queryString: string;
      // How this query was served: the router's decision, then what the run
      // did. Replaced, never mutated (see query_serving.ts); servedFrom, the
      // routing metrics, the path report and the access record all derive
      // from it.
      let answer: Answer = LIVE_ANSWER;
      if (!this.modelMaterializer || !this.modelDef || !this.modelInfo)
         throw new BadRequestError("Model has no queryable entities.");

      // Resolved once, before routing, so prepare, the pre-aggregation probe
      // and run all agree on the same freshness substitution.
      const buildManifest = this.resolveFreshBuildManifest();
      // Without rollup entries, for the persisted-view leg's variant.
      const viewBuildManifest = this.withoutPreaggregateEntries(buildManifest);
      // Without persisted-view entries too: only the view leg's variant
      // imports a view's build source (it carries the persistence flag).
      // The live compile reads no table at all: an explicit empty manifest,
      // since with none Malloy falls back to the manifest the model's runtime
      // was reloaded with, which no freshness window filters and no path
      // report names.
      const liveBuildManifest = EMPTY_BUILD_MANIFEST;
      // See filterGivensToModelSurface. Resolved from the caller's own compiled
      // query, before routing or any graft replaces it.
      let querySurfaceGivens: Record<string, GivenValue> | undefined;

      // Query boundary FIRST (404), before authorize and compilation, so a
      // non-queryable source's schema cannot be probed. "deferred" is settled
      // by the compiled backstop below.
      const boundary = includeHiddenFilesAndSources
         ? "cleared"
         : this.assertQueryBoundaryEarly(sourceName, queryName, query);
      // The caller's own text; its joins are checked as extra run targets.
      // Text carrying an annotation is left to the forgery rejecter below.
      const callerRegion: CallerRegion | undefined =
         !sourceName && !queryName && query
            ? { kind: "query", text: query }
            : undefined;
      // The advisory pre-compile checks (`OracleGuard`): a refused caller hears
      // the gate's 403 or the boundary's 404 before any compile error could
      // describe the source. Never the authority; `govern` below is.
      const { earlySource } = await this.governor.oracle.beforeQueryCompile({
         sourceName,
         queryName,
         query,
         givens,
         bypassAuthorize,
         includeHiddenFilesAndSources,
         callerRegion,
      });

      // Wrap loadQuery calls in try-catch to handle query parsing errors
      try {
         // Before any compile: caller text may not declare an authorize gate
         // (it would override the author's). Names are also quoted below;
         // checking them too is defence in depth.
         for (const [field, callerText] of [
            ["query", query],
            ["source_name", sourceName],
            ["query_name", queryName],
         ] as const satisfies readonly (readonly [
            AuthorizeGuardField,
            string | undefined,
         ])[]) {
            if (!callerText) continue;
            try {
               // A name is interpolated mid-line, so it is never lexed as text of its own.
               if (field === "query")
                  assertNoCallerAuthorizeAnnotation(callerText);
               else assertNoAuthorizeTagLike(callerText);
            } catch (err) {
               // The only signal a rejection emits: the catch below skips the
               // query histogram for a BadRequestError.
               recordAuthorizeGuardRejection(field);
               throw err;
            }
         }
         if (query) assertNoCallerClassificationAnnotation(query);
         if (!sourceName && !queryName && query) {
            queryString = "\n" + query;
         } else if (queryName && !query) {
            // NAMES, not Malloy text: quoted so each lexes as one identifier.
            // Unquoted, a newline would open a second statement (Malloy runs
            // the LAST `run:`), and getFilters() would miss the raw name and
            // inject no filter.
            queryString = `\nrun: ${
               sourceName ? `${quotedIdentifier(sourceName)} -> ` : ""
            }${quotedIdentifier(queryName)}`;
         } else {
            const endTime = performance.now();
            const executionTime = endTime - startTime;
            queryExecutionHistogram().record(
               executionTime,
               this.queryMetricAttributes({
                  environment: queryMetadataInput?.environment,
                  queryName,
                  sourceName,
                  status: "rejected",
               }),
            );
            throw new InvalidQueryRequestError(
               "Invalid query request. (Query AND !sourceName) OR (queryName AND sourceName) must be defined.",
            );
         }

         // Only controls how the protected source is resolved for filter
         // injection; both forms are untrusted and compile restricted.
         const isAdHocQuery = !sourceName && !queryName && !!query;

         // Inject source filter predicates unless bypassed. For ad-hoc queries
         // resolve the run target through any alias/extend/chain so a protected
         // source can't be read unfiltered under a different name.
         let injectedFilters: FilterDefinition[] | undefined;
         let injectedFilterSourceName: string | undefined;
         if (!bypassFilters) {
            const effectiveSource = isAdHocQuery
               ? this.governor.resolveFilterSource(query)
               : sourceName;
            if (effectiveSource) {
               const filters = this.getFilters(effectiveSource);
               if (filters.length > 0) {
                  const filterClause = buildFilterClause(
                     filters,
                     filterParams ?? {},
                  );
                  queryString = injectFilterRefinement(
                     queryString,
                     filterClause,
                  );
                  // Only filters with a value were injected; checking the
                  // others would deny a derived source that legitimately
                  // `except:`s an unused filter's dimension.
                  injectedFilters = filters.filter((f) =>
                     filterHasValue(f, filterParams ?? {}),
                  );
                  injectedFilterSourceName = effectiveSource;
               }
            }
         }

         // Restricted mode keeps untrusted text inside the model's surface: it
         // rejects `import`, raw `connection.table(...)`/`.sql(...)`, raw-SQL
         // functions, and `##!` flags. Only author notebook cells use the
         // unrestricted `loadQuery`.
         await this.ensureSharedQueryModel(this.modelMaterializer);
         runnable = this.modelMaterializer.loadRestrictedQuery(queryString);
         // Kept for the run-time freshnessFallback retry below.
         liveRunnable = runnable;
         querySurfaceGivens = this.governor.filterGivensToModelSurface(
            givens,
            await this.governor.givenNamesReadByQuery(runnable, givens),
         );

         // An injected `#(filter)` reads its dimension by NAME, so a caller's
         // own `rename:`/`except:` can rebind it to another column; check
         // against the struct actually compiled.
         if (
            injectedFilters &&
            injectedFilters.length > 0 &&
            injectedFilterSourceName
         ) {
            // Prepare first, unguarded, so a genuine compile failure surfaces
            // as itself rather than as the bind check's fail-closed 403.
            await runnable.getPreparedQuery();
            await this.governor.assertFilterAnnotationsBindToDeclaringSource(
               runnable,
               injectedFilterSourceName,
               injectedFilters,
            );
         }

         // Routing eligibility is decided BEFORE routing (see
         // `queryEntryPointHasRowLevelGate`): a candidate that cannot keep a
         // gate is vetoed by it, and one that keeps the contracts grafts and
         // proves the gate on its own compile.
         //
         // `hasBindings` counts a storage table only while the storage tier
         // serves, and a colocated table always.
         const servedCopyPossible =
            this.servedCopyEnabled() && servedCopy.hasBindings();
         const route = await routeQuery<RoutedExtra>(
            routeCandidates(this.servingHost(servedCopy), {
               queryString,
               surfaceGivens: querySurfaceGivens,
               buildManifest,
               servedCopyPossible,
               // G3: a colocated table is read past a row-level gate (it
               // never leaves the author's warehouse).
               colocatedKeepsContracts: true,
               ...(this.rollupVariantsEnabled()
                  ? { rollupVariantsFor: runnable }
                  : {}),
               ...(this.persistedViewsEnabled()
                  ? { persistedViewsFor: runnable }
                  : {}),
            }),
            async () => {
               // Cheap short-circuit for gate-free models. Safe, unlike
               // `hasAuthorize()`: see `hasAnyAuthorizeNote`.
               if (bypassAuthorize || !this.governor.hasAnyAuthorizeNote()) {
                  return false;
               }
               const veto = await this.governor.rowLevelGateVeto(
                  runnable,
                  callerRegion,
               );
               // Only an entry-point gate can be grafted on a routed compile
               // (the served copy, R3); a caller join's gate, or one the walk
               // could not decide, keeps every routed shape away.
               return veto === "none"
                  ? false
                  : veto === "entry_point"
                    ? "entry_point"
                    : true;
            },
            { recordMetrics: false },
         );
         recordRoutingDecision(route);
         answer = routedAnswer(route);
         if (answer.routedRunnable) runnable = answer.routedRunnable;
      } catch (error) {
         // Re-throw BadRequestError as-is
         if (error instanceof BadRequestError) {
            throw error;
         }
         // Re-throw AccessDeniedError as-is (403), e.g. from the `#(filter)`
         // binding check inside this block.
         if (error instanceof AccessDeniedError) {
            throw error;
         }
         // Source filter validation errors are client errors (400)
         if (error instanceof FilterValidationError) {
            throw new BadRequestError(error.message);
         }
         // Re-throw MalloyError as-is (maps to 400)
         if (error instanceof MalloyError) {
            throw error;
         }
         // For other query parsing errors, wrap as BadRequestError
         const errorMessage =
            error instanceof Error ? error.message : String(error);
         logger.warn("Query parsing error", {
            error,
            errorMessage,
            packageName: this.packageName,
            modelPath: this.modelPath,
            query,
            queryName,
            sourceName,
         });
         throw new BadRequestError(`Invalid query: ${errorMessage}`);
      }

      // Caller text that does not compile is settled here, before the compiled
      // backstop turns an unresolved target into a bare 404. Every pre-compile
      // gate has already run, so when every source the text names is queryable
      // the caller gets the compiler's problems as a 400; otherwise the 404
      // (except a grammar-only failure naming nothing in an ungated model, see
      // parseFailureNamesNothing). An unbound given is left to the run path,
      // which answers opaquely when a gate reads it. Skipped when routed (same
      // text, a second compile).
      if (
         !sourceName &&
         !queryName &&
         query &&
         liveRunnable &&
         runnable === liveRunnable
      ) {
         const compileError = await compileErrorOf(liveRunnable);
         if (compileError && !isGivenBindingFailure(compileError)) {
            if (
               boundary === "deferred" &&
               !this.queryTextSourcesQueryable(query) &&
               !this.parseFailureNamesNothing(query, compileError)
            ) {
               // Explain the refusal (`OffSurfaceError`, ungated only) when a
               // run target is a real model source off the surface; anything
               // else keeps the plain form.
               const offSurface = extractRunTargetSourceNames(query).find(
                  (t) =>
                     this.declaresSource(t) &&
                     !this.isCuratedSource(t) &&
                     !this.derivesFromCurated(t, query),
               );
               throw this.notQueryable(
                  "Query target is not queryable.",
                  offSurface !== undefined,
                  "source",
                  offSurface,
               );
            }
            queryExecutionHistogram().record(
               performance.now() - startTime,
               this.queryMetricAttributes({
                  environment: queryMetadataInput?.environment,
                  queryName,
                  sourceName,
                  status: "error",
                  servedFrom: servedFromOf(answer),
               }),
            );
            throw queryCompileError(compileError, query);
         }
      }

      // Counted off the compile in hand, but refused only after every gate, so
      // the count cannot reveal whether a hidden or locked source exists.
      const runCount =
         !sourceName && !queryName && query
            ? await runStatementCount(runnable)
            : undefined;

      const compiledSource =
         await this.governor.resolveAuthorizeSourceFromRunnable(runnable);

      // The AUTHORITATIVE governance, on the compiled run target (the early
      // checks cannot resolve named-query, multi-statement or caller-declared
      // forms): the compiled boundary backstop (404) for a "deferred"
      // admission (see assertQueryBoundaryEarly), then every gate (403), the
      // row-level graft, the bind guards and the ceiling. MUST run
      // unconditionally and its runnable MUST replace `runnable`: for a
      // row-level gate the replacement IS the enforcement.
      //
      // Do NOT guard on hasAuthorize(): it misses inherited gates. The recompile
      // stays restricted, as the original compile was.
      const governRequest: GovernRequest = {
         givens,
         bypassAuthorize,
         boundary:
            boundary === "deferred" ? { compiledSource, query } : undefined,
         callerRegion,
         recompile: (mm: ModelMaterializer) =>
            mm.loadRestrictedQuery(queryString),
         // The caller's OWN text, not the synthesized/injected `queryString`.
         callerQueryText: query,
         ceiling: {
            audit,
            source: () => compiledSource ?? sourceName,
            // Read on the compile that answers: the served copy is the
            // author's text with its classifications kept (the load's checks
            // decline a copy that does not keep them), and the only compile
            // that has a join a cross-connection build view leaves out.
            ...(answer.tier === "storage" &&
            answer.via === "served_copy" &&
            answer.routedRunnable
               ? { answeredBy: "served_copy" as const }
               : {}),
         },
         // The served copy is the author's text, gates included, as are a
         // persisted view's and a `#(preaggregate)` rollup's variants: a
         // row-level gate is grafted and proven on its own compile, so the
         // routed answer keeps it (R3), and an answer let past an entry-point
         // gate is re-proven there. Any other routed shape carries no
         // annotations and is never reached past a row-level gate (the
         // router's veto).
         graftScope: answer.graftScope,
         routedPastEntryPointGate: answer.pastEntryPointGate,
      };
      const { runnable: governed, decision } = await this.governor.govern(
         governRequest,
         // The LIVE compile keeps the caller's joins; a routed one does not.
         { runnable, live: liveRunnable },
      );
      runnable = governed;
      if (runCount !== undefined && runCount > 1) {
         queryExecutionHistogram().record(
            performance.now() - startTime,
            this.queryMetricAttributes({
               environment: queryMetadataInput?.environment,
               queryName,
               sourceName,
               status: "error",
               servedFrom: servedFromOf(answer),
            }),
         );
         // An unsupplied row-level gate given denies at prepare; the count must
         // not answer first.
         throw (
            this.governor.unboundGateDenial(
               runnable,
               givens,
               compiledSource ?? sourceName ?? "unknown",
               decision.grafts.flatMap((graft) => graft.givenNames),
            ) ?? multipleRunStatementsError(runCount)
         );
      }
      // After the gate, so a denied caller gets its 403 rather than a 400 about
      // a value; before prepare, which is where Malloy would parse it.
      this.assertFilterGivens(givens);
      // No post-hoc gate check is possible on a serve-shape or v1
      // `#@ preaggregate` runnable (it carries no annotations): their
      // correctness rests entirely on `queryEntryPointHasRowLevelGate`. The
      // served copy, a persisted view and a `#(preaggregate)` rollup are the
      // exceptions (R3): each keeps the author's gates on the run target's
      // own `sourceID`, and `govern()` above grafted and proved them on its
      // own compile.

      const maxRows = getMaxQueryRows();
      const maxBytes = getMaxResponseBytes();
      // A run on `virtualMap` (the served copy, a `storage=` view or rollup)
      // takes no manifest; a colocated rollup reads the full one (its
      // entries), a colocated persisted view the one without rollups, and
      // the live compile none.
      const effectiveBuildManifest = answer.runFailed
         ? liveBuildManifest
         : answer.virtualMap
           ? undefined
           : answer.tier === "rollup"
             ? buildManifest
             : answer.tier === "view"
               ? viewBuildManifest
               : liveBuildManifest;

      // Prepare INSIDE the run try/catch, so a given-binding throw at prepare
      // gets the same error mapping as run instead of escaping as a 500.
      let executionTime = 0;
      let run: PreparedRun;
      // Passed whether or not the query routes. The serve shape declares this
      // model's given surface (`serveShapeGivens`), and the givens MUST be
      // passed: a materialized source's re-emitted `where:` may read one, and
      // dropping it would read the artifact unfiltered (every caller's rows).
      const effectiveGivens = querySurfaceGivens;
      // The manifest entries the run that answers reads (recordingManifest).
      const runRead = new Set<string>();
      try {
         // Prepare yields the connection name, so its metadata layer resolves
         // before the statement is issued.
         run = await prepareAndRun(
            governed,
            {
               givens: effectiveGivens,
               buildManifest: recordingManifest(
                  effectiveBuildManifest,
                  runRead,
               ),
               virtualMap: answer.virtualMap,
            },
            {
               abortSignal,
               maxRows,
               usage: Model.statementUsage(
                  queryMetadataInput,
                  this.packageName,
               ),
               queryMetadata: (connectionName) =>
                  this.resolveQueryMetadata(
                     queryMetadataInput,
                     connectionName,
                     // The gate's run-target answer (compiled, else surface), so
                     // a `queryName` or derived query is tagged with the source
                     // it actually reads.
                     compiledSource ?? earlySource,
                  ),
            },
         );
         // AFTER run(), so it includes the warehouse round trip.
         executionTime = performance.now() - startTime;
         answer = answeredWith(answer, runRead);
      } catch (error) {
         // `freshnessFallback=live` marks the tier as an optimisation, not a
         // dependency, so a store failing UNDER a routed query degrades to live.
         // Deliberately narrow:
         //  - only when the query actually routed to storage;
         //  - only per {@link bindingsAllowDegradeToLive} over the bindings that
         //    produced THIS shape (a sibling source must not decide);
         //  - never for a client error (bad given) or an abort;
         //  - the retry re-supplies the REAL givens, or the live source could
         //    serve unfiltered rows.
         const degradeToLive = canDegradeToLive(answer, error, {
            hasLiveRunnable: !!liveRunnable,
            aborted: !!abortSignal?.aborted,
         });
         // Both the original failure and a failure OF THE RETRY end here, so a
         // retry failure still gets the error metric and mapping. Typed on the
         // CONST: control-flow narrowing of `never` requires an explicit
         // annotation.
         const failQuery: (err: unknown) => never = (err) => {
            const errorEndTime = performance.now();
            const errorExecutionTime = errorEndTime - startTime;
            queryExecutionHistogram().record(
               errorExecutionTime,
               this.queryMetricAttributes({
                  environment: queryMetadataInput?.environment,
                  queryName,
                  sourceName,
                  status: "error",
                  servedFrom: servedFromOf(answer),
               }),
            );

            // Bad givens surface as Malloy `runtime-given-*` errors, mapped to
            // a 400 (the prefix is a pinned coupling to Malloy's error codes;
            // also in environment.ts). But a grafted gate's givens bind in the
            // same prepare/run, and Malloy's message names the missing one,
            // which `docs/authorize.md` promises a denied caller never sees. So
            // when a gate applied and a name it reads went unsupplied, refuse
            // with the opaque 403, ahead of the `MalloyError` rethrow.
            const gateDenial = isGivenBindingFailure(err)
               ? this.governor.unboundGateDenial(
                    runnable,
                    givens,
                    compiledSource ?? sourceName ?? "unknown",
                    this.governor.authorizeReferencedGivenNames,
                 )
               : undefined;
            if (gateDenial) {
               logger.debug("Gate given unbound; denying opaquely", {
                  packageName: this.packageName,
                  modelPath: this.modelPath,
                  error: err instanceof Error ? err.message : String(err),
               });
               throw gateDenial;
            }

            return mapRunError(err, {
               packageName: this.packageName,
               modelPath: this.modelPath,
               failurePrefix: "Query execution failed",
               // A connection-classified failure (exhausted pool) keeps its own
               // status rather than reading as a bad query.
               passThrough: [ConnectionError],
               logFailure: (errorMessage) =>
                  logger.error("Query execution error", {
                     error: err,
                     errorMessage,
                     packageName: this.packageName,
                     modelPath: this.modelPath,
                     query,
                     queryName,
                     sourceName,
                  }),
            });
         };
         if (!degradeToLive) failQuery(error);
         logger.warn(
            "Storage-served query failed at run time; falling back to live (freshnessFallback=live)",
            {
               modelPath: this.modelPath,
               error: error instanceof Error ? error.message : String(error),
            },
         );
         recordStorageServeRouting("runtime_live_fallback");
         // From here the query is the live retry: a retry failure's error
         // metric reads `live_fallback`, not a storage error the tier never
         // answered.
         answer = liveFallbackAnswer(answer);
         // The retry runs the LIVE compile, which `governed` is not (it is the
         // serve shape), so it is governed in its own right here: the same
         // bind, on the runnable that will execute. A gated entry point
         // reaches here only through the served copy (R3), whose answer's
         // graft scope is the served copy's: the retry grafts on the author's
         // own scope, or the graft would recompile against the stored table
         // that just failed. Outside the retry's try, so a denial answers
         // 403, not a mapped run error.
         const { runnable: governedLive } = await this.governor.govern(
            {
               ...governRequest,
               graftScope: undefined,
               routedPastEntryPointGate: false,
               boundary: undefined,
               // Checked on live already, unless the served copy answered:
               // then the retry, which runs live, is held to live's ceiling.
               ceiling: governRequest.ceiling?.answeredBy
                  ? { ...governRequest.ceiling, answeredBy: undefined }
                  : undefined,
               bypassAlreadyNoted: true,
            },
            { runnable: liveRunnable!, live: liveRunnable },
         );
         const retryRead = new Set<string>();
         try {
            // A fresh prepare re-derives the row cap from the live shape (a
            // failure at storage prepare would leave it 0, an EMPTY answer).
            // `liveBuildManifest`: this runs on the author's model.
            run = await prepareAndRun(
               governedLive,
               {
                  givens: querySurfaceGivens,
                  // The retry is the run that answers: record its reads alone.
                  buildManifest: recordingManifest(
                     liveBuildManifest,
                     retryRead,
                  ),
               },
               {
                  abortSignal,
                  maxRows,
                  usage: Model.statementUsage(
                     queryMetadataInput,
                     this.packageName,
                  ),
                  // Re-resolved: the first bag was for the STORAGE connection.
                  // The correlation id rides in the input, so it is kept.
                  queryMetadata: (connectionName) =>
                     this.resolveQueryMetadata(
                        queryMetadataInput,
                        connectionName,
                        compiledSource ?? earlySource,
                     ),
               },
            );
         } catch (retryError) {
            failQuery(retryError);
         }
         // NOT a storage hit: servedFromOf reads `live_fallback`.
         answer = answeredWith(answer, retryRead);
         executionTime = performance.now() - startTime;
      }
      const {
         result: queryResults,
         rowLimit,
         rowLimitSource,
         queryMetadata: appliedQueryMetadata,
      } = run;

      // A gate that matched nothing is a normal 200 with zero rows,
      // indistinguishable from an empty source, so record it.
      if (decision.grafts.length > 0 && queryResults.totalRows === 0) {
         const first = decision.grafts[0];
         recordRowLevelGateDecision(
            "empty_after_filter",
            first?.callerJoinPath ? "caller_join" : "entry_point",
         );
      }
      // Row cap checked before wrapping (an overflow is maxRows+1 rows, and the
      // wrap builds a larger object graph); then ONE serialization of the shape
      // the caller asked for, measured against the byte cap and returned so
      // nothing stringifies it again. The byte check is best-effort (a clean
      // 413), not OOM prevention; the row cap is. Known gap: MCP's own
      // envelope (mcp/query_envelope.ts) re-stringifies the compact rows
      // unguarded.
      const { wrapped: wrappedResult, serialized: serializedResult } =
         serializeWithinCaps(queryResults, {
            maxRows,
            maxBytes,
            source: "model_query",
            compact: responseShape === "compact",
         });
      const metricAttributes = this.queryMetricAttributes({
         environment: queryMetadataInput?.environment,
         queryName,
         sourceName,
         status: "success",
         connection: queryResults.connectionName,
         servedFrom: servedFromOf(answer),
      });
      queryExecutionHistogram().record(executionTime, metricAttributes);
      // Only when reported, so the counter is absent rather than a false zero.
      const queryCostBytes = queryResults.runStats?.queryCostBytes;
      if (queryCostBytes !== undefined) {
         queryScannedBytesCounter().add(queryCostBytes, metricAttributes);
      }
      // Never fails the query: an unreadable prepared query yields a record
      // with no fields, and a thrown summary yields none.
      try {
         audit.summary = await summarizeQueryAccess({
            // The storage serve shape carries no annotations; the live one does.
            runnable: liveRunnable ?? runnable,
            modelDef: this.modelDef,
            enforced: decision.enforced,
            apiSources: this.sources,
            modelGivens: this.givens,
            source: compiledSource ?? sourceName,
            givens,
            bypassAuthorize,
            grafts: decision.grafts,
            rowCount: queryResults.totalRows,
            // A named view on a source; a model-level named query has no source.
            ...(sourceName && queryName ? { view: queryName } : {}),
         });
      } catch (err) {
         logger.debug("Access summary unavailable", {
            modelPath: this.modelPath,
            error: (err as Error).message,
         });
      }
      // Counted once the run answered, so a variant whose run failed and fell
      // back live counts as skipped (`run_failed`), not used.
      recordVariantRouting(answer.considered, answer.runFailed);
      const path = await queryPathFor(this.servingHost(servedCopy), answer, {
         // The author's own compile: its names, and no grafted gate filter.
         runnable: liveRunnable ?? runnable,
         sourceName: compiledSource ?? sourceName,
         viewName: sourceName && queryName ? queryName : undefined,
         freshManifest: buildManifest,
         includeHidden: !!includeHiddenFilesAndSources,
         ...(this.persistedViewsEnabled() ? { queryString } : {}),
      });
      const servedFrom = servedFromOf(answer);
      audit.servedFrom = servedFrom ?? null;
      audit.executionTimeMs = Math.round(executionTime);
      if (queryCostBytes !== undefined) audit.scannedBytes = queryCostBytes;
      const recordedPath = path ? accessPathOf(path) : undefined;
      if (recordedPath) audit.path = recordedPath;
      // No report at all (it could not be assembled), as against a report
      // that says live with nothing considered, which records no `path`.
      if (!path) audit.pathUnavailable = true;
      return {
         result: wrappedResult,
         serializedResult,
         compactResult: queryResults.data.value,
         modelInfo: this.modelInfo,
         dataStyles: this.dataStyles,
         // The cap pushed into the SQL, so a caller can tell a complete result
         // from one silently cut at DEFAULT_QUERY_ROW_LIMIT.
         rowLimit,
         // The author's own limit, or the silent default (only the latter
         // suggests rows were left behind).
         rowLimitSource,
         // From the bag actually attached, not the one supplied.
         queryCorrelationId: appliedQueryMetadata?.query_id ?? null,
         // Per-query attribution the aggregate histogram cannot give;
         // `servedFrom` is the only way to tell a storage answer from a live one.
         servedFrom: servedFrom ?? null,
         ...(path ? { path } : {}),
         executionTimeMs: Math.round(executionTime),
         // Null is "not reported", not zero.
         queryCostBytes: queryCostBytes ?? null,
      };
   }

   /**
    * What serving (query_serving.ts) reads from this model: its storage serve
    * shape, rollup plans, catalog and discovery surface. Built per call so it
    * always reads the current state.
    */
   private servingHost(
      servedCopy: ServedCopyServe = this.servedCopy,
   ): QueryServingHost {
      return {
         modelPath: this.modelPath,
         storageServe: this.storageServe,
         servedCopy,
         rollupVariants: this.rollupVariants,
         persistedViews: this.persistedViews,
         modelDef: () => this.modelDef,
         materializedTables: () => this.policy.materializedTables(),
         publishedNames: () => this.publishedNames(),
         admitsSourceByName: (name) => this.admitsSourceByName(name),
         sourceChains: () => this.sourceChains(),
      };
   }

   /** Every source's `sourceID` chain in this model, for the path report's `notConsidered`. */
   private sourceChains(): ReadonlyMap<string, readonly string[]> {
      const modelDef = this.modelDef;
      const cached = this.sourceChainCache;
      if (cached && cached.modelDef === modelDef) return cached.chains;
      const chains = new Map<string, readonly string[]>();
      for (const entry of Object.values(modelDef?.contents ?? {})) {
         if (!isSourceDef(entry)) continue;
         const chain = sourceChain(entry, modelDef);
         if (chain[0] && !chains.has(chain[0])) chains.set(chain[0], chain);
      }
      this.sourceChainCache = { modelDef, chains };
      return chains;
   }

   /**
    * The label set for `queryExecutionHistogram`, shared by every recording
    * site so success and error paths cannot drift.
    *
    * Query text, row count and row limit are deliberately absent: each is
    * unbounded, or chosen by the caller (a cardinality leak); they stay on the
    * request log.
    */
   private queryMetricAttributes(args: {
      environment?: string;
      queryName?: string;
      sourceName?: string;
      /** `rejected`: a request malformed before anything compiled, kept out of `error` so the error rate means failed queries. */
      status: "success" | "error" | "rejected";
      connection?: string;
      servedFrom?: ServedFrom;
   }): Record<string, string | number | undefined> {
      return {
         "malloy.model.path": this.modelPath,
         "malloy.package": this.packageName,
         "malloy.model.query.name": args.queryName,
         "malloy.model.query.source": args.sourceName,
         "malloy.model.query.status": args.status,
         ...(args.environment
            ? { "malloy.environment": args.environment }
            : {}),
         ...(args.connection
            ? { "malloy.model.query.connection": args.connection }
            : {}),
         // Only for queries that reached the storage routing decision.
         ...(args.servedFrom
            ? { "malloy.model.query.served_from": args.servedFrom }
            : {}),
      };
   }

   /** The usage hook for a statement, when its caller says which connections are metered. */
   private static statementUsage(
      input: ModelQueryMetadataInput | undefined,
      packageName: string,
   ):
      | {
           metered: (connectionName: string) => boolean;
           environment: string;
           package: string;
        }
      | undefined {
      if (!input?.meteredConnection || !input.environment) return undefined;
      return {
         metered: input.meteredConnection,
         environment: input.environment,
         package: packageName,
      };
   }

   /**
    * The per-query metadata for one model query: the executing connection's
    * default, the caller's request override, and the server's context (which
    * package, which model, which class of work), merged most-specific-wins.
    *
    * The author-declared layers are included, composed as the build path
    * composes them ({@link composeDeclaredQueryMetadata}): they describe the
    * SOURCE (team, cost centre, tier), which is as true of a query as of a
    * build. Build-identifying properties (`run_id`, `trigger`, `source`,
    * `class`) come from the per-statement context layer, so a served query is
    * never mistaken for a build.
    *
    * Fails open: an unreadable connection config or an unparseable annotation
    * contributes no layer rather than failing the query.
    */
   private resolveQueryMetadata(
      input: ModelQueryMetadataInput | undefined,
      connectionName: string | undefined,
      sourceName?: string,
   ): QueryMetadata | undefined {
      // Skip the assembly entirely when off (the default). Read per statement:
      // the mode may change under a running server.
      if (getQueryMetadataMode() === "off") return undefined;

      let connectionLayers: {
         default?: QueryMetadata | null;
         enforced?: QueryMetadata | null;
      } | null = null;
      if (connectionName && input?.connectionMetadata) {
         try {
            connectionLayers = input.connectionMetadata(connectionName);
         } catch {
            connectionLayers = null;
         }
      }
      const resolved = mergeQueryMetadata({
         connection: connectionLayers?.default,
         enforced: connectionLayers?.enforced,
         model: composeDeclaredQueryMetadata({
            packageDeclaration: input?.packageDeclaration,
            modelTag: this.safeModelFileTag(),
            sourceTag: this.safeSourceTag(sourceName),
         }),
         request: input?.request,
         context: {
            queryClass: input?.queryClass ?? "interactive",
            environment: input?.environment,
            package: this.packageName,
            model: this.modelPath,
            version: input?.version,
            correlationId: input?.correlationId,
         },
      });
      if (resolved.drops.length > 0) {
         logger.warn("Dropped query-metadata properties for a query", {
            modelPath: this.modelPath,
            drops: resolved.drops,
         });
      }
      return resolved.metadata;
   }

   /**
    * The model file's own `##` tag, or undefined if absent or unparseable.
    *
    * The file's OWN notes, not the folded import lineage: a tag is not a gate,
    * and folding would attribute every importer's traffic to a shared
    * include's team (and report its warning against the wrong file).
    */
   private safeModelFileTag(): ReadableTag | undefined {
      if (!this.modelDef) return undefined;
      try {
         return parseRouteTag(
            new Annotations(ownModelAnnotations(this.modelDef)),
         ).tag as ReadableTag;
      } catch {
         return undefined;
      }
   }

   /**
    * The run-target source's own `#@` tag, or undefined when no source could be
    * resolved, the name is not a top-level source, or the annotation fails to
    * parse.
    *
    * Reads the source's OWN annotations, never its derivation base: unlike a
    * gate (which an extension must not shed), inheriting a cost label would
    * attribute `a`'s traffic to `b`'s team.
    */
   private safeSourceTag(
      sourceName: string | undefined,
   ): ReadableTag | undefined {
      if (!sourceName || !this.modelDef) return undefined;
      try {
         const entry = this.modelDef.contents?.[sourceName];
         // `contents` also holds named queries, whose `#@` is not a source's.
         if (!entry || !isSourceDef(entry)) return undefined;
         const def = entry as unknown as { annotations?: unknown };
         if (!def.annotations) return undefined;
         return parseRouteTag(new Annotations(def.annotations), "@")
            .tag as ReadableTag;
      } catch {
         return undefined;
      }
   }

   private getStandardModel(): ApiCompiledModel {
      const published = this.publishedNames();
      const keep = <T extends { name?: string }>(items: T[] | undefined) =>
         published && items
            ? items.filter((item) => item.name && published.has(item.name))
            : items;
      return {
         type: "source",
         packageName: this.packageName,
         path: this.modelPath,
         // Deprecated alias of `path`, not in the spec.
         modelPath: this.modelPath,
         malloyVersion: MALLOY_VERSION,
         dataStyles: JSON.stringify(this.dataStyles),
         modelDef: JSON.stringify(this.publishedModelDef(published)),
         // Precomputed and export-curated by Malloy; `keep` adds a dashboard's
         // surface rule.
         modelInfo: JSON.stringify(
            this.modelInfo
               ? {
                    ...this.modelInfo,
                    entries: keep(this.modelInfo.entries),
                    anonymous_queries: this.publishedRuns(published),
                 }
               : {},
         ),
         sourceInfos: keep(this.getSourceInfos())?.map((sourceInfo) =>
            JSON.stringify(sourceInfo),
         ),
         sources: keep(this.getSources()),
         queries: keep(this.getQueries()),
         givens: this.givens,
      } as ApiCompiledModel;
   }

   /**
    * The names this file's model GET may show, or undefined when the package
    * curates nothing (then every name is public).
    *
    * For an ordinary file, its export closure, the same set
    * {@link curateForDiscovery} lists. For a dashboard, which admits nothing of
    * its own (as does a served notebook), only the exported names that trace
    * to the package surface: a source it may read, or a query over one.
    */
   /** {@link publishedNames}, for a reader that applies the same rule to its own view of the file. */
   public getPublishedNames(): Set<string> | undefined {
      return this.publishedNames();
   }

   private publishedNames(): Set<string> | undefined {
      if (!this.discoveryCurationEnabled) return undefined;
      const exports = this.modelDef?.exports;
      if (!Array.isArray(exports)) return undefined;
      const names = new Set<string>(exports);
      const { mode, exploresDeclared } = this.queryBoundary;
      if (
         !(this.isGridReport() || this.isNotebook()) ||
         mode === "all" ||
         !exploresDeclared
      ) {
         return names;
      }
      const readable = (source: string) =>
         this.isCuratedSource(source) || this.derivesFromCurated(source);
      const admitted = new Set<string>();
      for (const name of names) {
         const query = this.queries?.find((q) => q.name === name);
         const source = query ? query.sourceName : name;
         if (source && readable(source)) admitted.add(name);
      }
      return admitted;
   }

   /**
    * `modelDef` with every top-level list of named definitions limited to
    * `published`: a whole-file import carries unexported sources in `contents`,
    * which the response must not name. Other keys are kept.
    */
   private publishedModelDef(
      published: Set<string> | undefined,
   ): ModelDef | undefined {
      if (!published || !this.modelDef) return this.modelDef;
      // `sourceRegistry` is keyed `name@file` and lists every source the file
      // can see; `queryList` holds the file's top-level `run:` statements, each
      // with the source it reads inline. `modelAnnotations`, `dependencies`
      // and `imports` are keyed by file, not by source, and are kept.
      const runsPublished = (query: unknown) =>
         runReadsPublished(query, published);
      return {
         ...this.modelDef,
         contents: Object.fromEntries(
            Object.entries(this.modelDef.contents ?? {})
               .filter(([name]) => published.has(name))
               .map(([name, def]) => [name, scrubHiddenIds(def, published)]),
         ),
         exports: (this.modelDef.exports ?? []).filter((name) =>
            published.has(name),
         ),
         sourceRegistry: Object.fromEntries(
            Object.entries(this.modelDef.sourceRegistry ?? {}).filter(([key]) =>
               published.has(key.split("@")[0]),
            ),
         ),
         queryList: (this.modelDef.queryList ?? []).filter(runsPublished),
         // Every name the file uses, with where it is defined: editor
         // go-to-definition data that names hidden sources and their fields.
         // Nothing that reads this response uses it.
         references: [],
      };
   }

   /**
    * `modelInfo.anonymous_queries` limited to the runs `publishedModelDef`
    * keeps in `queryList`: each run's schema lists every column it returns.
    */
   private publishedRuns(
      published: Set<string> | undefined,
   ): Malloy.ModelInfo["anonymous_queries"] | undefined {
      const runs = this.modelInfo?.anonymous_queries;
      if (!published || !runs) return runs;
      const queryList = this.modelDef?.queryList ?? [];
      // Index-aligned with `queryList`; when they disagree, show none.
      if (queryList.length !== runs.length) return [];
      return runs.filter((_, k) => runReadsPublished(queryList[k], published));
   }

   /**
    * Whether the model GET may return this file's text. Not when the text
    * names a source the file does not publish, whether it declares it, runs
    * it, or builds on it: the text would show that name and how it is used.
    * Reads identifiers outside comments and string literals, so an import
    * path or a note does not count, and reads a backticked name whole. A
    * dashboard's text is always returned, because its editor saves through
    * it; the notebook editor fetches a notebook's text with
    * `includeHiddenFilesAndSources`.
    *
    * A notebook exports nothing the surface serves, yet `contents` lists
    * every source it imports. So a name the notebook may read (one the
    * surface serves, or one its cells build on one) is not hidden, and
    * neither is one of its own `given:` declarations.
    */
   public showsFileText(text: string): boolean {
      if (this.isGridReport()) return true;
      const published = this.publishedNames();
      if (!published) return true;
      const readable = this.isNotebook() ? this.notebookReadable() : undefined;
      const givens = new Set((this.givens ?? []).map((given) => given.name));
      const unpublished = new Set(
         Object.keys(this.modelDef?.contents ?? {}).filter(
            (name) =>
               !published.has(name) && !givens.has(name) && !readable?.(name),
         ),
      );
      if (unpublished.size === 0) return true;
      return !malloyIdentifiers(text).some((name) => unpublished.has(name));
   }

   /**
    * Serialize a notebook cell's `newSources` to JSON strings, embedding the
    * model-level `givens` on each SourceInfo (as `Source.givens` in the API
    * spec) so a client can render given inputs without a second round trip.
    * No `givens` key when the model declares none.
    */
   private serializeNewSources(
      newSources: Malloy.SourceInfo[] | undefined,
      cellIndex: number,
   ): string[] | undefined {
      // An import cell reports every source it brings in, schema and all, so
      // under a surface only the ones this notebook may read are shown.
      const readable = this.notebookReadable(cellIndex);
      return newSources
         ?.filter((source) => !readable || readable(source.name))
         .map((source) =>
            JSON.stringify(
               this.givens && this.givens.length > 0
                  ? { ...source, givens: this.givens }
                  : source,
            ),
         );
   }

   /**
    * Under an active surface, a predicate for the source names a notebook may
    * read and so show: what the surface publishes, and what the code cells up
    * to `cellIndex` (every cell when omitted) derive from it. Undefined when
    * nothing is curated.
    */
   private notebookReadable(
      cellIndex?: number,
   ): ((name: string) => boolean) | undefined {
      const { mode, exploresDeclared } = this.queryBoundary;
      if (mode === "all" || !exploresDeclared) return undefined;
      const context = (this.runnableNotebookCells ?? [])
         .slice(0, cellIndex === undefined ? undefined : cellIndex + 1)
         .filter((c) => c.type === "code")
         .map((c) => c.text)
         .join("\n");
      return (name) =>
         this.isCuratedSource(name) ||
         this.derivesFromCurated(name, undefined, context);
   }

   /**
    * Whether the notebook GET may show a cell's `queryInfo`. Its schema lists
    * every column the cell's query returns, so a cell the query route would
    * refuse (`run: secret -> { select: * }`) would otherwise describe the
    * hidden source here. Asks the same check running the cell does.
    */
   private async showsCellQueryInfo(
      cellIndex: number,
      cell: RunnableNotebookCell,
   ): Promise<boolean> {
      // Nothing to check an unhydrated cell against, so under a surface it shows nothing.
      if (!cell.runnable) return this.notebookReadable(cellIndex) === undefined;
      try {
         await this.assertNotebookCellOnSurface(cellIndex, cell.runnable);
         return true;
      } catch (error) {
         if (error instanceof NotQueryableError) return false;
         throw error;
      }
   }

   /**
    * Which of `modelInfo.anonymous_queries` a cell whose `queryInfo` is shown
    * describes. A served notebook's query cell names its run; a `.malloynb`'s
    * model info is its last code cell's compile, whose final run is that cell's
    * `queryInfo`. A run no shown cell describes is withheld.
    */
   private shownRunIndexes(
      cells: readonly RunnableNotebookCell[],
      shownCells: ReadonlySet<number>,
   ): Set<number> {
      const shown = new Set<number>();
      if (this.notebookFormat() === "malloy") {
         cells.forEach((cell, index) => {
            if (cell.queryIndex !== undefined && shownCells.has(index))
               shown.add(cell.queryIndex);
         });
         return shown;
      }
      const last = cells.findLastIndex(
         (cell) => cell.type === "code" && cell.modelDef !== undefined,
      );
      const runs = cells[last]?.modelDef?.queryList.length ?? 0;
      if (
         last >= 0 &&
         shownCells.has(last) &&
         runs > 0 &&
         runs === (this.modelInfo?.anonymous_queries?.length ?? 0)
      ) {
         shown.add(runs - 1);
      }
      return shown;
   }

   private async getNotebookModel(): Promise<ApiRawNotebook> {
      // Return raw cell contents without executing them
      const cells = this.runnableNotebookCells ?? [];
      const notebookCells: ApiNotebookCell[] = [];
      const shownCells = new Set<number>();
      for (const [index, cell] of cells.entries()) {
         const shown =
            (cell.queryInfo !== undefined ||
               cell.deriveQueryInfo !== undefined) &&
            (await this.showsCellQueryInfo(index, cell));
         if (shown) shownCells.add(index);
         const queryInfo = shown
            ? (cell.queryInfo ?? (await cell.deriveQueryInfo?.()))
            : undefined;
         notebookCells.push({
            type: cell.type,
            kind: cell.kind,
            text: cell.text,
            markdown: cell.markdown,
            proseLines: cell.proseLines,
            codeLine: cell.codeLine,
            caption: cell.caption,
            newSources: this.serializeNewSources(cell.newSources, index),
            queryInfo: queryInfo ? JSON.stringify(queryInfo) : undefined,
         } as ApiNotebookCell);
      }

      // The notebook's own `##` tags, not its imports' (a shared include must
      // not configure every importer). A served notebook's `"` notes below the
      // tag are its markdown cells, not annotations.
      const served = this.notebookFormat() === "malloy";
      const allAnnotations = served
         ? (this.notebookAnnotations ?? [])
         : this.modelDef
           ? ownModelNotes(this.modelDef)
           : [];

      // A served notebook declares `title`/`autorun`/`givens` in its artifact tag.
      const ownTag = motlyTag(allAnnotations);
      const notebookTag = served ? ownTag?.tag("artifact") : ownTag;

      // No `as` cast, so a field rename in `api-doc.yaml` fails to typecheck.
      // Not export-curated (a notebook cannot be imported), but under a surface
      // it shows only what its cells may read.
      const readable = this.notebookReadable(notebookCells.length - 1);
      const shownSource = (name: string | undefined) =>
         !readable || (name !== undefined && readable(name));
      const shownRuns = readable
         ? this.shownRunIndexes(cells, shownCells)
         : undefined;
      const notebook: ApiRawNotebook = {
         type: "notebook",
         format: this.notebookFormat(),
         packageName: this.packageName,
         modelPath: this.modelPath,
         malloyVersion: MALLOY_VERSION,
         modelInfo: JSON.stringify(
            this.modelInfo
               ? {
                    ...this.modelInfo,
                    entries: this.modelInfo.entries?.filter((entry) =>
                       entry.kind === "source" ? shownSource(entry.name) : true,
                    ),
                    // The same surface filter each cell's `queryInfo` gets.
                    anonymous_queries: shownRuns
                       ? this.modelInfo.anonymous_queries?.filter((_, k) =>
                            shownRuns.has(k),
                         )
                       : this.modelInfo.anonymous_queries,
                 }
               : {},
         ),
         sources:
            this.modelDef &&
            this.sources?.filter((source) => shownSource(source.name)),
         queries:
            this.modelDef &&
            this.queries?.filter(
               (query) => !readable || shownSource(query.sourceName),
            ),
         annotations: allAnnotations,
         // Derived here so notebook `##` and dashboard artifact tags arrive as
         // the same fields with the same defaults. Known gap: a malformed `##`
         // line (including an `@env.` on the same line) silently falls back to
         // defaults in a `.malloynb`, which has no lint channel.
         autorun: readAutorun(notebookTag),
         startingGivens: readStartingGivens(
            notebookTag,
            (name) => (this.givens ?? []).find((g) => g.name === name)?.type,
         ),
         notebookCells,
         ...(this.notebookLayout && {
            dashboard: {
               packageName: this.packageName,
               name: this.notebookLayout.name,
               path: this.modelPath,
               layout: this.notebookLayout.layout,
               title: this.notebookLayout.title,
               description: this.notebookLayout.description,
               tiles: this.notebookLayout.tiles,
               dashboardColumns: this.notebookLayout.dashboardColumns,
               startingGivens: this.notebookLayout.startingGivens,
               autorun: this.notebookLayout.autorun,
               givens: this.notebookLayout.givens,
            },
         }),
      };
      return notebook;
   }

   public async executeNotebookCell(
      audit: QueryAudit,
      cellIndex: number,
      filterParams?: FilterParams,
      bypassFilters?: boolean,
      givens?: Record<string, GivenValue>,
      // Cancels the query on timeout (see `runQuery`).
      abortSignal?: AbortSignal,
      // Tagged like any query, but with no correlation id: the cell response
      // has no field to return it on.
      queryMetadataInput?: ModelQueryMetadataInput,
   ): Promise<NotebookCellRunResult> {
      const started = performance.now();
      const cell = this.runnableNotebookCells?.[cellIndex];
      // A served run that names no cell (out of range, or refused by the reader) is `none`, never `.malloynb`'s `code`.
      const kind =
         this.notebookFormat() === "malloy"
            ? (cell?.kind ?? "none")
            : cell?.type === "markdown"
              ? "markdown"
              : "code";
      try {
         const result = await this.runNotebookCell(
            audit,
            cellIndex,
            filterParams,
            bypassFilters,
            givens,
            abortSignal,
            queryMetadataInput,
         );
         recordNotebookCellExecution(
            this.notebookFormat(),
            kind,
            "ok",
            performance.now() - started,
         );
         return result;
      } catch (error) {
         recordNotebookCellExecution(
            this.notebookFormat(),
            kind,
            error instanceof AccessDeniedError
               ? "denied"
               : error instanceof NotQueryableError
                 ? "not_queryable"
                 : error instanceof BadRequestError ||
                     error instanceof MalloyError
                   ? "bad_request"
                   : "error",
            performance.now() - started,
         );
         throw error;
      }
   }

   private async runNotebookCell(
      audit: QueryAudit,
      cellIndex: number,
      filterParams: FilterParams | undefined,
      bypassFilters: boolean | undefined,
      givens: Record<string, GivenValue> | undefined,
      abortSignal: AbortSignal | undefined,
      queryMetadataInput: ModelQueryMetadataInput | undefined,
   ): Promise<NotebookCellRunResult> {
      if (this.compilationError) {
         throw this.compilationError;
      }
      const readerError = this.notebookReaderError();
      if (readerError) throw readerError;

      if (!this.runnableNotebookCells) {
         throw new BadRequestError("No notebook cells available");
      }

      if (cellIndex < 0 || cellIndex >= this.runnableNotebookCells.length) {
         throw new BadRequestError(
            `Cell index ${cellIndex} out of range (0-${this.runnableNotebookCells.length - 1})`,
         );
      }

      const cell = this.runnableNotebookCells[cellIndex];

      if (cell.type === "markdown") {
         return {
            type: cell.type,
            kind: cell.kind,
            text: cell.text,
         };
      }

      let queryName: string | undefined = undefined;
      let queryResult: string | undefined = undefined;

      if (cell.runnable) {
         // The package surface first (a hidden gated source must 404, not 403)
         // and outside the try (whose catch would turn the 404 into a 400).
         await this.assertNotebookCellOnSurface(cellIndex, cell.runnable);
         try {
            let runnableToExecute = cell.runnable;
            // The text the executing runnable was built from, updated if a
            // `#(filter)` refinement rebuilds it.
            let textToExecute = cell.text;

            // See `resolveNotebookCellGraftScope`.
            const { graftScope, usesOwnScope } =
               await this.governor.resolveNotebookCellGraftScope(
                  cellIndex,
                  cell.runnable,
               );
            if (!graftScope) {
               // Lets an operator tell "nowhere to attach a graft" from "the
               // gate's condition failed" behind the same 403.
               logger.debug(
                  "Notebook cell has no graft scope to attach a row-level gate against (no earlier code cell, and this cell has no compiled model of its own)",
                  { modelPath: this.modelPath, cellIndex },
               );
            }

            // Whether a `#(filter)` refinement will rebuild the runnable,
            // computed up front (no compile) to decide the pre-refinement gate.
            const effectiveSource =
               !bypassFilters && cell.modelMaterializer
                  ? extractRunTargetSourceName(cell.text)
                  : undefined;
            const cellFilters = effectiveSource
               ? this.getFilters(effectiveSource)
               : [];

            // Pre-refinement gate on the UNREFINED runnable, so a denied
            // caller gets the 403 rather than a 400 from a broken refinement
            // or bad `filterParams` (`buildFilterClause` throws, so this runs
            // before it). Row-level gates are deferred to the authoritative
            // bind below. Skipped with no refinement: the bind would see the
            // same runnable.
            if (cell.modelMaterializer && cellFilters.length > 0) {
               await this.governor.oracle.beforeCellRefinement(
                  cell.runnable,
                  givens ?? {},
                  graftScope,
               );
            }

            const filterClause =
               cellFilters.length > 0
                  ? buildFilterClause(cellFilters, filterParams ?? {})
                  : undefined;

            if (filterClause && cell.modelMaterializer) {
               textToExecute = injectFilterRefinement(cell.text, filterClause);
               runnableToExecute =
                  cell.modelMaterializer.loadQuery(textToExecute);
               // The same fail-closed `#(filter)` bind check as
               // `runQuery`, over the injected filters only.
               const injectedCellFilters = cellFilters.filter((f) =>
                  filterHasValue(f, filterParams ?? {}),
               );
               if (injectedCellFilters.length > 0 && effectiveSource) {
                  // Prepare first so a compile failure surfaces as itself,
                  // not as the bind check's fail-closed 403.
                  await runnableToExecute.getPreparedQuery();
                  await this.governor.assertFilterAnnotationsBindToDeclaringSource(
                     runnableToExecute,
                     effectiveSource,
                     injectedCellFilters,
                  );
               }
            }

            // Read before the bind below grafts gates onto the runnable.
            const cellReadGivenNames =
               await this.governor.givenNamesReadByQuery(
                  runnableToExecute,
                  givens,
               );

            // The AUTHORITATIVE governance, on the runnable that actually
            // executes (after refinement, so the graft composes with the
            // `#(filter)` refinement instead of dropping it). Cells are
            // author-curated, so the recompile uses `loadQuery`, not
            // `loadRestrictedQuery`. The boundary was settled above
            // (`assertNotebookCellOnSurface`), and a cell has no caller text.
            // What governance returned is the only thing this cell may run.
            const textForRecompile = textToExecute;
            // Own scope: graft the compiled queryDef (read off
            // `runnableToExecute`, so the refinement survives) with its
            // paired proof; see `ownScopeQueryDefBinder`.
            const ownScopeBinder =
               cell.modelMaterializer && usesOwnScope
                  ? this.governor.ownScopeQueryDefBinder(
                       (
                          (await runnableToExecute.getPreparedQuery()) as {
                             _query: unknown;
                          }
                       )._query,
                       graftScope!,
                    )
                  : undefined;
            const { runnable: cellGoverned, decision: cellDecision } =
               await this.governor.govern(
                  {
                     givens,
                     ...(cell.modelMaterializer
                        ? {
                             recompile: ownScopeBinder
                                ? ownScopeBinder.recompile
                                : (mm: ModelMaterializer) =>
                                     mm.loadQuery(textForRecompile),
                             proveGraft: ownScopeBinder?.prove,
                             graftScope,
                          }
                        : // No materializer to recompile against, so a
                          // row-level gate denies.
                          { bypassAuthorize: false }),
                     ceiling: {
                        audit,
                        source: async (governed) =>
                           (await this.governor.resolveAuthorizeSourceFromRunnable(
                              governed,
                           )) ?? extractRunTargetSourceName(cell.text),
                     },
                  },
                  { runnable: runnableToExecute },
               );
            runnableToExecute = cellGoverned;
            // After the gate, as on the query path; before prepare.
            this.assertFilterGivens(givens);

            const cellMaxRows = getMaxQueryRows();
            const cellMaxBytes = getMaxResponseBytes();
            // The freshness-filtered manifest minus rollups (a notebook has no
            // companion), and none for a cell whose model lacks the
            // persistence flag (see manifestIfPersistent).
            const buildManifest = this.manifestIfPersistent(
               this.withoutPreaggregateEntries(
                  this.resolveFreshBuildManifest(),
               ),
               cell.modelDef ?? this.modelDef,
            );
            // See filterGivensToModelSurface.
            const cellSurfaceGivens = this.governor.filterGivensToModelSurface(
               givens,
               cellReadGivenNames,
               this.notebookCellDeclaredGivens[cellIndex],
            );
            const cellRunnable = cellGoverned;
            const { result } = await prepareAndRun(
               cellRunnable,
               { givens: cellSurfaceGivens, buildManifest },
               {
                  abortSignal,
                  maxRows: cellMaxRows,
                  usage: Model.statementUsage(
                     queryMetadataInput,
                     this.packageName,
                  ),
                  queryMetadata: async (connectionName) => {
                     // The compiled run target, as in runQuery.
                     const cellCompiledSource =
                        await this.governor.resolveAuthorizeSourceFromRunnable(
                           cellRunnable,
                        );
                     return this.resolveQueryMetadata(
                        queryMetadataInput,
                        connectionName,
                        cellCompiledSource ??
                           extractRunTargetSourceName(cell.text),
                     );
                  },
               },
            );
            const query = (await runnableToExecute.getPreparedQuery())._query;
            queryName = (query as NamedQueryDef).as || query.name;
            try {
               audit.summary = await summarizeQueryAccess({
                  runnable: cellRunnable,
                  modelDef: this.modelDef,
                  enforced: cellDecision.enforced,
                  apiSources: this.sources,
                  modelGivens: this.givens,
                  source:
                     (await this.governor.resolveAuthorizeSourceFromRunnable(
                        cellRunnable,
                     )) ?? extractRunTargetSourceName(cell.text),
                  givens,
                  bypassAuthorize: false,
                  grafts: cellDecision.grafts,
                  rowCount: result?.totalRows ?? 0,
               });
            } catch (err) {
               logger.debug("Access summary unavailable", {
                  modelPath: this.modelPath,
                  error: (err as Error).message,
               });
            }
            // As in runQuery: only when reported, absent is not zero.
            const cellScannedBytes = result?.runStats?.queryCostBytes;
            if (cellScannedBytes !== undefined) {
               audit.scannedBytes = cellScannedBytes;
            }
            // As in runQuery.
            if (
               result?._queryResult &&
               result.totalRows === 0 &&
               cellDecision.grafts.length > 0
            ) {
               recordRowLevelGateDecision("empty_after_filter");
            }
            // Same caps and ordering as runQuery.
            if (result?._queryResult) {
               if (this.modelInfo) {
                  queryResult = serializeWithinCaps(result, {
                     maxRows: cellMaxRows,
                     maxBytes: cellMaxBytes,
                     source: "notebook_cell",
                  }).serialized;
               } else {
                  assertWithinModelRowLimit(
                     result.totalRows,
                     cellMaxRows,
                     "notebook_cell",
                  );
               }
            }
         } catch (error) {
            // The gate runs inside this try; keep its 403 from becoming a 400.
            if (error instanceof AccessDeniedError) {
               throw error;
            }
            if (error instanceof FilterValidationError) {
               throw new BadRequestError(error.message);
            }
            // Same error mapping as runQuery.
            mapRunError(error, {
               packageName: this.packageName,
               modelPath: this.modelPath,
               failurePrefix: "Cell execution failed",
               // Keep the 413.
               passThrough: [PayloadTooLargeError],
               benignMessage: "Model has no queries.",
               logFailure: (errorMessage) =>
                  logger.error("Error message: ", errorMessage),
            });
            return {
               type: "code",
               kind: cell.kind,
               text: cell.text,
               markdown: cell.markdown,
               proseLines: cell.proseLines,
               codeLine: cell.codeLine,
               caption: cell.caption,
            };
         }
      }

      return {
         type: cell.type,
         kind: cell.kind,
         text: cell.text,
         markdown: cell.markdown,
         proseLines: cell.proseLines,
         codeLine: cell.codeLine,
         caption: cell.caption,
         queryName: queryName,
         result: queryResult,
         newSources: this.serializeNewSources(cell.newSources, cellIndex),
      };
   }

   /** See {@link createModelRuntime}. */
   static getModelRuntime(
      ...args: Parameters<typeof createModelRuntime>
   ): ReturnType<typeof createModelRuntime> {
      return createModelRuntime(...args);
   }

   /** See {@link createModelMaterializer}. */
   static getModelMaterializer(
      ...args: Parameters<typeof createModelMaterializer>
   ): ReturnType<typeof createModelMaterializer> {
      return createModelMaterializer(...args);
   }

   public async getFileText(packagePath: string): Promise<string> {
      const fullPath = path.join(packagePath, this.modelPath);
      try {
         return await fs.readFile(fullPath, "utf8");
      } catch {
         throw new ModelNotFoundError(
            `Model file not found: ${this.modelPath}`,
         );
      }
   }
}

/**
 * A value for each given with no default, for a compile that only asks
 * whether a query compiles: `null`, or an empty filter for a `filter<T>`
 * given, whose SQL needs a filter literal (as model_info.ts does for the same
 * reason).
 */
function placeholderGivens(
   givens:
      | readonly { name?: string; type?: string; default?: unknown }[]
      | undefined,
): Record<string, GivenValue> {
   const out: Record<string, GivenValue> = {};
   for (const given of givens ?? []) {
      if (!given.name || given.default !== undefined) continue;
      out[given.name] = /^filter\b/i.test(given.type ?? "") ? "" : null;
   }
   return out;
}

/** The first line of an error message, which is where Malloy says what failed. */
function firstLine(message: string): string {
   return message.split("\n")[0].trim();
}

/**
 * The line of a compile error that says what is wrong: past Malloy's
 * "Error(s) compiling model:" and "FILE:" header lines, without the `line N:`
 * prefix. The whole first line when there is no such line.
 */
function compileErrorLine(message: string): string {
   const said = message
      .split("\n")
      .map((l) => l.trim())
      .find(
         (l) =>
            l !== "" &&
            !l.startsWith("Error(s) compiling model") &&
            !l.startsWith("FILE:"),
      );
   return said ? said.replace(/^line \d+: /, "") : firstLine(message);
}
