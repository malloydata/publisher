// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
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
   type FilterCondition,
   type SourceDef,
   type VirtualMap,
} from "@malloydata/malloy";
import * as Malloy from "@malloydata/malloy-interfaces";
import { DataStyles } from "@malloydata/render";
import { lazyCounter, lazyHistogram, withSpan } from "../telemetry";
import { recordStorageServeRouting } from "../materialization_metrics";
import * as fs from "fs/promises";
import { createRequire } from "module";
import * as path from "path";
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
   ConnectionError,
   ModelCompilationError,
   ModelNotFoundError,
   NotQueryableError,
   OffSurfaceError,
   PayloadTooLargeError,
} from "../errors";
import { getPersistStorageMode } from "../config";
import {
   planModelPreaggregation,
   type RollupPlan,
} from "./preaggregation_synthesis";
import { tryCompileSynthesizedPreaggregation } from "./preaggregation_compile";
import { type RenderTagTarget, renderTagTargets } from "./render_tag_targets";
import { modelInfoOf } from "./model_info";
import { logger } from "../logger";
import {
   type ServeBinding,
   serveShapeDiagnostics,
} from "./materialization_serve_transform";
import { deserializeError } from "../package_load/package_load_pool";
import type { SerializedModel } from "../package_load/protocol";
import { BuildManifest } from "../storage/DatabaseInterface";
import {
   annotationTexts,
   ownLevelNotes,
   ownModelAnnotations,
   ownModelNoteObjects,
   ownModelNotes,
   type AnnotationNote,
} from "./annotations";
import { composeDeclaredQueryMetadata, type ReadableTag } from "./build_plan";
import {
   assertNoAuthorizeTagLike,
   assertNoCallerAuthorizeAnnotation,
   assertNoLegacyStringGate,
   assertNoMisplacedAuthorizeAnnotations,
   containsAuthorizeAnnotationTag,
   hasCallerAuthorizeAnnotation,
   findLegacyStringGates,
   referencedGivenNames,
   validateAuthorizeProbes,
   type RowLevelGateRejectionCause,
} from "./authorize";
import { ACCESS_FILTER_ROUTE, AUTHORIZE_ROUTE } from "./authorize_routes";
import {
   buildReportManifest,
   compileTileGivens,
   factsReportTag,
   readReportModelFacts,
   type ServedReportManifest,
   type ReportModelFacts,
} from "./report_manifest";
import { isReportEligiblePath, type ReportLayout } from "./report";
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
   validateSourceLineGateGivenUsage,
   type ExpandableRefSummary,
} from "./gate_dimension";
import {
   buildFilterClause,
   FILTER_ANNOTATION_PREFIX,
   filterHasValue,
   FilterValidationError,
   injectFilterRefinement,
   parseFilterAnnotation,
   type FilterDefinition,
   type FilterParams,
} from "./filter";
import {
   assertFilterGivensParse,
   gateGivenSource,
   malloyGivenToApi,
   type MalloyGiven,
} from "./given";
import { filterPublisherOwnedRenderLogs } from "./report_manifest";
import {
   docCommentTitleAndDescription,
   motlyTag,
   readAutorun,
   readStartingGivens,
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
   buildSourceAliasMap,
   collectIdentifierNames,
   extractRunTargetSourceName,
   extractRunTargetSourceNames,
   malloyIdentifiers,
   stripMalloyCommentsAndLiterals,
} from "./query_text";
import {
   CallerJoinWalkError,
   collectCallerJoins,
   derivationTerminals,
   locateCallerJoin,
   resolveCallerJoin,
   type CallerJoin,
   type CallerJoinPath,
   type CallerJoinResolution,
   type CallerRegion,
   type CallerSource,
   type PreparedQueryIr,
} from "./caller_joins";
import {
   mergeQueryMetadata,
   type ModelQueryMetadataInput,
   type QueryMetadata,
} from "./query_metadata";
import {
   validateModelPreaggregation,
   type PreaggregateViolation,
} from "./preaggregation_validation";
import { derivedStructsReachable } from "./gate_registry_walk";
import {
   assertFilterDimensionBindsToDeclaringSource,
   assertGraftedGateBindsToDeclaringSource,
   assertInheritedSourceFiltersBind,
   findFilterAnnotationDeclaringSource,
   type DocumentLocationLike,
   type SiblingModelDefResolver,
   type StageLike,
} from "./filter_binding_guard";
// Aliased with an `Impl` suffix because `Model` declares private wrapper
// methods of the same names; without the alias, converting one of them to an
// arrow-function property would recurse instead of delegating.
import {
   assertAuthorizeGrammarValid,
   assertNoRetiredRouteMarkers,
   collectEntryPointGates as collectEntryPointGatesImpl,
   collectRetiredRouteMarkers,
   computeGivenDeclaredTypes,
   createGateClassificationDeps,
   resolveGateShape as resolveGateShapeImpl,
   resolveGraftTarget as resolveGraftTargetImpl,
   type GateClassificationDeps,
   type GateEntry,
   type GraftScope,
   type RowLevelGraftEntry,
} from "./gate_classification";
import { collectSourceInfos } from "./source_extraction";
import {
   recordAuthorizeAdmitAllGate,
   recordAuthorizeBypass,
   recordAuthorizeGuardRejection,
   recordLockDecision,
   recordRowLevelGateDecision,
   recordRowLevelGateRejected,
   type AuthorizeBypassEntryPoint,
   type AuthorizeGateSite,
   type AuthorizeGuardField,
} from "../authorize_metrics";
import { decideLock } from "./authorize_lock";
import { translatorMalloyError } from "./translator_error";
import {
   createModelMaterializer,
   createModelRuntime,
   hydrateMarkdownOnlyCells,
   hydrateNotebookCells,
   makeHydrationRuntime,
   type HydrationMaterializer,
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
   quoteMalloyIdentifier,
   runStatementCount,
} from "./model_query_compile";
import {
   extractApiQueries,
   extractApiSources,
   runReadsPublished,
   scrubHiddenIds,
} from "./model_projection";
import { StorageServeShape } from "./model_serve_shape";

export type { ModelConnectionInput, ModelType } from "./model_runtime";
export type {
   NotebookCellRunResult,
   RunnableNotebookCell,
} from "./model_notebook_cell";

/** One caller join the live query reaches, and what it resolves to. */
type ResolvedCallerJoin = {
   callerJoin: CallerJoin;
   compiledModelDef: ModelDef;
   resolution: CallerJoinResolution;
};

/**
 * The shared tail of every gate resolution that is not a row filter: a
 * `#(authorize)` lock is DECIDED, and anything else — or a lock that does not
 * admit this caller — is a 403 naming only the source.
 *
 * Shared by `assertAuthorized`'s pre-compile gate and `probeEntryPointGates`
 * so both reach the same answer; a divergence between them would be a schema
 * oracle.
 */
function denyUnlessAdmitted(
   resolution:
      | {
           shape: "lock";
           condition: { e?: unknown };
           givenNamesById: ReadonlyMap<string, string>;
        }
      | { shape: "rejected"; cause?: RowLevelGateRejectionCause },
   givens: Record<string, GivenValue>,
   label: string,
   site: AuthorizeGateSite = "entry_point",
): void {
   if (resolution.shape === "lock") {
      const outcome = decideLock(
         resolution.condition.e,
         (id) => resolution.givenNamesById.get(id),
         givens,
      );
      if (outcome === "admit") {
         recordLockDecision("admitted", site);
         return;
      }
      // Both refusals are the same 403 to the caller; only the label differs,
      // so an operator can alert on a gate that could not be decided without
      // firing on one that is working.
      recordLockDecision(
         outcome === "unresolvable" ? "denied_unresolvable" : "denied_by_lock",
         site,
      );
      throw new AccessDeniedError(`Access denied for source "${label}".`);
   }
   recordLockDecision("denied_unresolvable", site);
   // Also booked on the row-level counter: a gate refused at shape resolution
   // is a fail-closed rejection, and `denied_by_gate` must count all of them.
   // `cause` is a finer label, not a substitute.
   recordRowLevelGateDecision("denied_by_gate", site);
   if (resolution.cause) recordRowLevelGateRejected(resolution.cause);
   throw new AccessDeniedError(`Access denied for source "${label}".`);
}

export type { ModelQueryMetadataInput } from "./query_metadata";

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

/** The package's published surface, as an explained refusal describes it:
 *  `indexModel` when the package is curated by its root index.malloy rather
 *  than an explicit "explores" list, and the surface files either way. */
export interface OffSurfaceContext {
   indexModel: boolean;
   files: readonly string[];
}

/**
 * How a query's answer was ultimately produced, once it reached the storage
 * routing decision at all. `storage` = served from the materialized table via
 * the virtual-source transform. `live_fallback` = it routed, then a run-time
 * store failure degraded it to the live warehouse — a success the caller cannot
 * distinguish from a storage hit, which is exactly why it is reported
 * separately. Absent means the query never had a storage binding to consider.
 */
export type ServedFrom = "storage" | "live_fallback";

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

/**
 * Budget for a walk over a request's own derivation declarations. Exceeding it
 * fails the proof (and so denies), so it only has to exceed any real chain.
 *
 * {@link Model.requestChainProvesUngated} counts total names visited, while
 * {@link Model.derivesFromCurated} counts stack depth (its every-base proof
 * recurses). Both deny on exhaustion, so the divergence can only over-deny.
 * Bounding depth leaves O(edges x depth) work, which is not a DoS lever:
 * `every` short-circuits on the first failing base and positives are memoized.
 */
const REQUEST_CHAIN_MAX_NAMES = 64;

/**
 * Whether a run-time store failure may be retried against the live warehouse,
 * decided from the bindings that produced the serve shape. Exported for tests.
 *
 * An authored binding declaring anything but `live` vetoes for the whole shape
 * (deliberately coarse). A rollup binding never vetoes and never enables on its
 * own: pre-aggregation has no correctness dimension (docs/preaggregation.md: a
 * stale rollup recomputes from the base with the same answer), so a rollup must
 * not error on a store failure. A rollup must also not be filtered out, or a
 * rollup-only package could never degrade. Empty is false: nothing routed.
 */
export function bindingsAllowDegradeToLive(
   bindings: readonly ServeBinding[],
): boolean {
   return (
      bindings.length > 0 &&
      bindings.every(
         (b) => b.origin === "preaggregate" || b.freshnessFallback === "live",
      )
   );
}

/** The one empty result {@link Model.preaggregateViolations} hands back, so its
 *  identity contract holds on the branch that never reaches the memo. */
const NO_PREAGGREGATE_VIOLATIONS: readonly Readonly<PreaggregateViolation>[] =
   Object.freeze([]);

/** The one sentence a gated model answers a hidden, hidden-and-locked or absent name with. */
const GENERIC_NOT_QUERYABLE = "Query target is not queryable.";

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

export class Model {
   private packageName: string;
   private modelPath: string;
   private dataStyles: DataStyles;
   private modelType: ModelType;
   private modelMaterializer: ModelMaterializer | undefined;
   private modelDef: ModelDef | undefined;
   private modelInfo: Malloy.ModelInfo | undefined;
   private storageServe = new StorageServeShape({
      modelPath: () => this.modelPath,
      modelDef: () => this.modelDef,
      givens: () => this.givens,
      rollupPlans: () => this.preaggregateRollupPlans,
      rollupPlansVersion: () => this.preaggregatePlansVersion,
      ensureSharedQueryModel: (materializer) =>
         this.ensureSharedQueryModel(materializer),
   });
   /**
    * The synthesized pre-aggregation model for this model, compiled once per load
    * by the owning Package (see Package.pushPreaggregateServeModels) and reused by
    * every query. Undefined when the model declares no usable `#@ preaggregate`
    * or when synthesis failed — in both cases the serve path is untouched.
    *
    * Per load rather than per query: rollup freshness is handled underneath by
    * the per-query build manifest (a stale rollup recomputes from the base with
    * the same answer), so nothing per-query keys it.
    */
   private preaggregateServeMaterializer?: ModelMaterializer;
   /**
    * The rollups this model's `#@ preaggregate` declarations describe.
    *
    * Held apart from {@link preaggregateServeMaterializer} because storage-tier
    * rollup serving needs only these plans, so a companion that fails to
    * compile must not also disable lake serving.
    */
   private preaggregateRollupPlans: RollupPlan[] = [];
   /**
    * Bumped whenever {@link preaggregateRollupPlans} is replaced, and folded into
    * the serve-shape cache key.
    *
    * Clearing the cache is not enough: an in-flight query that read the old
    * (possibly empty) plans can write its shape after the clear. Keying on the
    * version means such a write is never read again.
    */
   private preaggregatePlansVersion = 0;
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
   /** Given names (`$NAME`) referenced by any authorize gate reachable in this
    *  model, own or inherited (the walk {@link assertAuthorizedForAllSources}
    *  runs). Computed at construction for {@link filterGivensToModelSurface}. */
   private authorizeReferencedGivenNames: Set<string> = new Set();
   /** Whether discovery accessors curate to the `export {}` closure. Pushed
    *  down by the owning Package (see Package.applyDiscoveryPolicyToModels):
    *  true only when the package declares `explores` in publisher.json. */
   private discoveryCurationEnabled = false;
   /**
    * Resolves the `ModelDef` the owning Package independently compiled for
    * the file a `file://` URL names — pushed down by
    * `Package.applySiblingModelResolverToModels`. `undefined` outside a
    * Package, which is safe: every guard treats "unresolvable" as deny. See
    * `SiblingModelDefResolver` in `./filter_binding_guard` for why it exists.
    */
   private siblingModelDefResolver: SiblingModelDefResolver | undefined;
   /** Per-package query-boundary policy, pushed down by the owning Package
    *  (see `Package.applyQueryBoundaryToModels`). Defaults are inert so a
    *  Model outside a Package, or before the policy lands, never denies. */
   private queryBoundary: {
      mode: "declared" | "all";
      exploresDeclared: boolean;
      isQueryEntryPoint: boolean;
      /** The PACKAGE-wide export closure (union over explores-listed models),
       *  keyed name -> DEFINITION IDENTITIES ({@link definitionIdentity}), never
       *  the bare name: a name is admitted only when THIS model resolves it to
       *  the very definition a listed model exported, so a same-named source in
       *  another file is not admitted by the collision. Absent when inert. */
      packageCuratedSources?: ReadonlyMap<string, ReadonlySet<string>>;
      packageCuratedQueries?: ReadonlyMap<string, ReadonlySet<string>>;
      /** Which files are the surface, for an explained refusal
       *  ({@link notQueryable}). */
      offSurface?: OffSurfaceContext;
      /** This file is a served report with this layout: an entry point that,
       *  like a `.malloynb`, admits nothing of its own, since a grid report's
       *  tiles and a linear report's cells read only the package surface. A
       *  linear report keeps `modelType` "model". */
      layout?: ReportLayout;
      /** The served report's own compiled file text, so a source it declares
       *  on top of a surface source (`source: big is orders extend {...}`) can
       *  be proven to derive from it. Author text, not caller text. */
      derivationText?: string;
   } = { mode: "all", exploresDeclared: false, isQueryEntryPoint: true };
   /** {@link buildDerivationBaseMap} over `queryBoundary.derivationText`,
    *  computed once per policy rather than per query. */
   private fileDerivationBases?: Map<string, Set<string>>;
   /** Per-query freshness resolver, pushed down by the owning Package (see
    *  Package.wireFreshnessResolvers): the freshness-filtered build manifest,
    *  passed as Malloy's per-query `buildManifest` override so a persist source
    *  routes to its table only within its freshness window. Undefined means no
    *  override. */
   private freshnessResolver?: () => BuildManifest["entries"] | undefined;
   /** See {@link setPreaggregateEntityIdResolver}. */
   private preaggregateEntityIdResolver?: () => ReadonlySet<string>;
   /** Entry-point gates per declared source name — see
    *  {@link computeEntryPointGatesBySource}. The one answer that
    *  `sources[].authorize`, {@link getAuthorize} and the early gate all read. */
   private entryPointGatesBySource: Map<string, GateEntry[]> = new Map();
   /**
    * The runtime that produced this model, retained ONLY so a row-level gate
    * can be enforced by grafting its condition onto a copied `ModelDef` (see
    * `buildGraftedMaterializer`). Set by `setGateRuntime` from `Model.create`
    * and `fromSerialized`; without it `authorizeAndBindRunnable` denies any
    * row-level gate (fail closed).
    *
    * Held explicitly rather than read off `ModelMaterializer`'s `protected
    * runtime`: a Malloy rename of that field would silently turn every
    * row-level gate into a fleet-wide deny.
    */
   private gateRuntime: HydrationRuntime | undefined;
   /**
    * This model's {@link GateClassificationDeps}, built once (lazily, so
    * `this.givens` is settled) and held for the instance's life: a reload
    * constructs a fresh `Model`, so the cache never outlives its model.
    *
    * `gateShapeCache` holds the lifted condition too, so grafting reuses the
    * one probe compile per `(cacheScope, graftTarget, filterText)`. It is
    * uncapped, unlike {@link graftedMaterializerCache}, because every key
    * component comes from the model's own shape, never from a request.
    */
   private gateClassificationDepsCache: GateClassificationDeps | undefined;
   /**
    * Grafted `ModelMaterializer`s, memoized per `cacheScope` + sorted set of
    * `(graftTarget, filterText)` pairs (see `getOrBuildGraftedMaterializer`).
    * A graft never depends on a caller's givens or query text (those bind at
    * `run()`), so requests hitting the same gate set share the expensive
    * `structuredClone`. `cacheScope` is in the key so a graft built against
    * one notebook cell's modelDef is never served for another scope.
    */
   private graftedMaterializerCache: Map<string, ModelMaterializer> = new Map();
   /**
    * Entry cap for {@link graftedMaterializerCache}. Each entry retains a full
    * `structuredClone(modelDef)` (~1.35x the serialized ModelDef), so uncapped
    * a large gated notebook could pin hundreds of MiB. 32 covers a normal
    * package; a miss costs latency (one clone + reload), never correctness.
    */
   private static readonly GRAFTED_MATERIALIZER_CACHE_MAX = 32;
   /**
    * `QueryMaterializer`s that `authorizeAndBindRunnable` returned with a
    * row-level filter attached. Keyed by object identity because `Model` is
    * shared across concurrent requests and a per-instance flag would race.
    * Used to keep a filtered query off the storage-serve tier (which cannot
    * bind the filter's given) and to record `empty_after_filter`.
    */
   private rowLevelFilteredRunnables = new WeakMap<
      QueryMaterializer,
      readonly RowLevelGraftEntry[]
   >();
   constructor(
      packageName: string,
      modelPath: string,
      dataStyles: DataStyles,
      modelType: ModelType,
      modelMaterializer: ModelMaterializer | undefined,
      modelDef: ModelDef | undefined,
      sources: ApiSource[] | undefined,
      queries: ApiQuery[] | undefined,
      sourceInfos: Malloy.SourceInfo[] | undefined,
      runnableNotebookCells: RunnableNotebookCell[] | undefined,
      compilationError: MalloyError | Error | undefined,
      filterMap?: Map<string, FilterDefinition[]>,
      givens?: ApiGiven[],
      /** Precomputed `modelInfoOf(modelDef)` from the package-load worker;
       *  derived here when omitted. */
      modelInfo?: Malloy.ModelInfo,
   ) {
      this.packageName = packageName;
      this.modelPath = modelPath;
      this.dataStyles = dataStyles;
      this.modelType = modelType;
      this.modelDef = modelDef;
      this.modelMaterializer = modelMaterializer;
      this.sources = sources;
      this.queries = queries;
      this.sourceInfos = sourceInfos;
      this.runnableNotebookCells = runnableNotebookCells;
      this.compilationError = compilationError;
      this.filterMap = filterMap ?? new Map();
      this.givens = givens;
      this.notebookCellDeclaredGivens = declaredGivensPerCell(
         this.runnableNotebookCells ?? [],
      );
      // `collectEntryPointGates` is the single definition of what gates a
      // source as an entry point: it follows the `inherits`/registry chain AND
      // a query-source's derivation base. The extractor's `sources[].authorize`
      // sees only `inherits`, so a derived source (`source: laundered is
      // locked_src -> {...}`) would read as ungated. Computed here because
      // both load paths (in-process and worker) reach the constructor.
      try {
         this.entryPointGatesBySource = this.computeEntryPointGatesBySource();
      } catch {
         this.entryPointGatesBySource = new Map();
      }
      // Make introspection agree with enforcement: downstream enforcers read
      // `sources[].authorize`/`accessFilter`, and the narrower extraction-time
      // value would report a gated source as unrestricted. Mutated in place so
      // getSources()/getAuthorize()/the early gate share one answer; split by
      // route so the two wire fields cannot disagree.
      for (const source of this.sources ?? []) {
         if (!source.name) continue;
         const gates = this.entryPointGatesBySource.get(source.name);
         const exprsForRoute = (route: string): string[] | undefined => {
            const exprs = gates
               ?.filter((g) => g.route === route)
               .flatMap((g) => g.exprs);
            return exprs && exprs.length > 0 ? exprs : undefined;
         };
         const lockExprs = exprsForRoute(AUTHORIZE_ROUTE);
         if (lockExprs) source.authorize = lockExprs;
         const filterExprs = exprsForRoute(ACCESS_FILTER_ROUTE);
         if (filterExprs) source.accessFilter = filterExprs;
      }
      // A malformed gate reachable only through a join/derivation must not
      // throw out of the constructor.
      try {
         this.authorizeReferencedGivenNames =
            this.computeAuthorizeReferencedGivenNames();
      } catch {
         this.authorizeReferencedGivenNames = new Set();
      }
      this.modelInfo =
         modelInfo ?? (this.modelDef ? modelInfoOf(this.modelDef) : undefined);

      // One-time `#(filter)` deprecation notice per Model instance.
      if (this.filterMap.size > 0) {
         logger.warn(
            `Model "${packageName}/${modelPath}" uses deprecated #(filter) annotations. Migrate to given: — see ${GIVENS_DOCS_URL}`,
            {
               packageName,
               modelPath,
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
      const ownFile = (url: string): boolean => {
         try {
            return decodeURIComponent(new URL(url).pathname).endsWith(
               `/${this.modelPath}`,
            );
         } catch {
            return false;
         }
      };
      const names: string[] = [];
      for (const entry of Object.values(this.modelDef.contents)) {
         if (!isSourceDef(entry)) continue;
         const own = entry.annotations?.blockNotes ?? [];
         const declares = own.some(
            (note) =>
               note.text.trim().startsWith(FILTER_ANNOTATION_PREFIX) &&
               note.at !== undefined &&
               ownFile(note.at.url),
         );
         if (declares) names.push(entry.as || entry.name);
      }
      return names.sort();
   }

   /** The parsed filter definitions for a source; empty when none. */
   public getFilters(sourceName: string): FilterDefinition[] {
      return this.filterMap.get(sourceName) ?? [];
   }

   /**
    * Retain the runtime a row-level `#(access_filter)` gate grafts through — see
    * {@link gateRuntime}. A setter rather than a constructor parameter because
    * only the compiled-model path needs it.
    */
   private setGateRuntime(runtime: HydrationRuntime): void {
      this.gateRuntime = runtime;
   }

   /**
    * The graft scope for the query path and the compile-time probe backstop:
    * this model's cumulative (modelDef, modelMaterializer), scoped `"model"`.
    * `undefined` for a compilation-failure placeholder (callers fail closed).
    *
    * A notebook cell never uses this (see {@link resolveNotebookCellGraftScope}):
    * the cumulative model already holds the cell's own declarations, so
    * recompiling the cell against it fails with "Cannot redefine".
    */
   private defaultGraftScope(): GraftScope | undefined {
      if (!this.modelDef || !this.modelMaterializer) return undefined;
      return {
         modelDef: this.modelDef,
         materializer: this.modelMaterializer,
         cacheScope: "model",
      };
   }

   /**
    * The EARLIER-scope half of a notebook cell's row-level graft: the nearest
    * earlier code cell's own (modelDef, modelMaterializer), skipping markdown.
    * Never this cell's own scope nor the cumulative {@link defaultGraftScope}
    * ("Cannot redefine"). `undefined` when no earlier code cell exists;
    * {@link resolveNotebookCellGraftScope} then falls back to
    * {@link selfGraftScopeForCell}.
    *
    * `cacheScope` is keyed on the SUPPLYING cell's index, so later cells
    * sharing one earlier cell share a cache entry, and cells with different
    * earlier cells never collide on a same-named source.
    */
   private graftScopeForCell(cellIndex: number): GraftScope | undefined {
      const cells = this.runnableNotebookCells;
      if (!cells) return undefined;
      for (let i = cellIndex - 1; i >= 0; i--) {
         const prior = cells[i];
         if (
            prior.type === "code" &&
            prior.modelDef &&
            prior.modelMaterializer
         ) {
            return {
               modelDef: prior.modelDef,
               materializer: prior.modelMaterializer,
               cacheScope: `cell:${i}`,
            };
         }
      }
      return undefined;
   }

   /**
    * The {@link GraftScope} built from `cellIndex`'s OWN post-declaration
    * model: the fallback for a cell that declares a gated source and runs it
    * in the same cell, so no earlier scope holds it. Safe only because the
    * paired bind (`_loadQueryFromQueryDef` in `executeNotebookCell`) repoints
    * the compiled queryDef and never recompiles the cell's text, so "Cannot
    * redefine" cannot arise.
    */
   private selfGraftScopeForCell(cellIndex: number): GraftScope | undefined {
      const cell = this.runnableNotebookCells?.[cellIndex];
      if (!cell?.modelDef || !cell.modelMaterializer) return undefined;
      return {
         modelDef: cell.modelDef,
         materializer: cell.modelMaterializer,
         cacheScope: `cell-self:${cellIndex}`,
      };
   }

   /**
    * The {@link GraftScope} for a notebook cell's row-level gate and, via
    * `usesOwnScope`, how the caller must bind against it. Decided by where the
    * RUN TARGET resolves, never by the cell's index (a later cell can declare
    * and run its own gated source just like cell 0):
    *  - the target resolves against the EARLIER scope: use it and recompile the
    *    cell text normally (the earlier scope lacks this cell's declarations,
    *    so redeclaring succeeds);
    *  - otherwise: the cell's OWN scope (`usesOwnScope: true`); the caller must
    *    repoint the compiled queryDef, not recompile.
    *
    * Reachability uses {@link resolveGraftTarget}, the same check
    * {@link resolveGateShape} applies later, so the two cannot disagree.
    */
   private async resolveNotebookCellGraftScope(
      cellIndex: number,
      runnable: QueryMaterializer,
   ): Promise<{ graftScope: GraftScope | undefined; usesOwnScope: boolean }> {
      // A served notebook's query cell is a bare `run:` over the whole compiled file, so it declares nothing to collide with.
      if (this.isNotebook() && this.notebookFormat() === "malloy") {
         return { graftScope: this.defaultGraftScope(), usesOwnScope: false };
      }
      const selfScope = this.selfGraftScopeForCell(cellIndex);
      const earlierScope = this.graftScopeForCell(cellIndex);
      if (!earlierScope) return { graftScope: selfScope, usesOwnScope: true };

      const { struct, modelDef } = await this.resolveRunTargetStruct(runnable);
      if (
         struct &&
         modelDef &&
         this.resolveGraftTarget(struct, modelDef, earlierScope.modelDef)
      ) {
         return { graftScope: earlierScope, usesOwnScope: false };
      }
      return { graftScope: selfScope, usesOwnScope: true };
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
    * Filter caller-supplied givens down to the ones safe to forward to the
    * real query's `getPreparedResult`/`run`. A caller may supply a value only
    * so a gate carried in from a derivation base can see it, and Malloy does
    * not flatten a `given:` declared more than one import hop away (see
    * `docs/authorize.md`), so forwarding it throws "unknown given".
    *
    * Dropping is safe by invariant: the gate is grafted and evaluated by the
    * same `run()` against this filtered set, and an accepted gate can only
    * reference a given on this model's own surface (`resolveGateShape` rejects
    * `unreachable_given` and re-checks this on every accepted classification).
    *
    * Only drops a name both off the surface ({@link givens}) and referenced by
    * a reachable gate ({@link authorizeReferencedGivenNames}). Any other
    * unknown name still reaches the query and fails closed via Malloy's
    * "unknown given", rather than silently binding a declared default
    * (over-exposure).
    *
    * Dropping is safe only while nothing the query compiles reads the name.
    * When something does (a same-named `where:` given declared elsewhere in
    * the query's closure), dropping would bind that declaration's default, so
    * a name in `queryReadNames` ({@link givenNamesReadByQuery}) is forwarded
    * and Malloy decides: it 400s a name off the entry surface.
    *
    * `cellDeclared` (a notebook cell's declared set) also drops a surface name
    * the cell's closure never declares, i.e. one only a later cell imports.
    * Nothing the cell runs can reference such a name, so no default can bind.
    * Match by id as well as name: an aliased import (`import { T is TENANT }`)
    * surfaces `T` while the registry keeps the declaration name `TENANT`.
    * Do NOT instead scope the gate-only rule to the cell's surface: a name
    * declared deep in the cell would be dropped while its `where:` still reads
    * it, binding the default where the cell must 400.
    */
   private filterGivensToModelSurface(
      givens: Record<string, GivenValue> | undefined,
      queryReadNames: ReadonlySet<string>,
      cellDeclared?: CellDeclaredGivens,
   ): Record<string, GivenValue> | undefined {
      if (!givens) return givens;
      const surfaceNames = new Set((this.givens ?? []).map((g) => g.name));
      const filtered: Record<string, GivenValue> = {};
      for (const [name, value] of Object.entries(givens)) {
         const authorizeOnly =
            this.isGateOnlyGivenName(name) && !queryReadNames.has(name);
         const outOfCellScope =
            cellDeclared !== undefined &&
            surfaceNames.has(name) &&
            !cellDeclared.names.has(name) &&
            !this.cellDeclaresGivenId(name, cellDeclared);
         if (!authorizeOnly && !outOfCellScope) filtered[name] = value;
      }
      return filtered;
   }

   private isGateOnlyGivenName(name: string): boolean {
      return (
         !(this.givens ?? []).some((g) => g.name === name) &&
         this.authorizeReferencedGivenNames.has(name)
      );
   }

   /**
    * The given names `runnable`'s compiled query actually reads. Malloy's
    * `_query.givenUsage` says which given ids the query uses (an unused field
    * or join that reads one is not counted); the IR walk maps each id to every
    * name it is spelled with, following name-string `structRef`s (a named
    * query, a same-document source) through the prepared model. A given bound
    * as a source argument is always counted. Pass the runnable BEFORE any gate
    * graft, so a gate's own `$NAME` is not counted as a read.
    *
    * Two over-refusals (a 400 rather than a default) are accepted: an aliased
    * id also spelled by an unused field counts that spelling, because
    * `givenUsage` keeps one `at` per id and matching on it would drop a live
    * read; and an unused join's source argument counts, as in
    * persist_dynamic_terms.ts, because `activeJoins` misses caller joins.
    *
    * Only computed when a supplied given is a drop candidate. A
    * `getPreparedQuery()` throw yields the empty set (drop): the same compile
    * fails at run, and throwing here would put Malloy's compile text ahead of
    * the boundary 404 and the gate 403. A used id the walk cannot name, or IR
    * with no `givenUsage`, forwards every candidate.
    */
   private async givenNamesReadByQuery(
      runnable: { getPreparedQuery(): Promise<unknown> },
      givens: Record<string, GivenValue> | undefined,
   ): Promise<Set<string>> {
      const candidates = Object.keys(givens ?? {}).filter((name) =>
         this.isGateOnlyGivenName(name),
      );
      const names = new Set<string>();
      if (candidates.length === 0) return names;
      let prepared: {
         _query?: { givenUsage?: { id: string }[] };
         _modelDef?: ModelDef;
      };
      try {
         prepared = (await runnable.getPreparedQuery()) as typeof prepared;
      } catch {
         return names;
      }
      const usage = prepared._query?.givenUsage;
      if (!usage) {
         for (const name of candidates) names.add(name);
         return names;
      }
      const usedIds = new Set(usage.map((g) => g.id));
      const namedIds = new Set<string>();
      const modelDef = prepared._modelDef ?? this.modelDef;
      // Separate visited sets: a struct seen outside a source argument must
      // still be read again when an argument reaches it.
      const seen = { plain: new Set<object>(), argument: new Set<object>() };
      const worklist: Array<[unknown, boolean]> = [[prepared._query, false]];
      while (worklist.length > 0) {
         const [value, inArgument] = worklist.pop()!;
         if (value === null || typeof value !== "object") continue;
         const visited = inArgument ? seen.argument : seen.plain;
         if (visited.has(value)) continue;
         visited.add(value);
         if (Array.isArray(value)) {
            for (const item of value) worklist.push([item, inArgument]);
            continue;
         }
         const node = value as Record<string, unknown>;
         if (
            node.node === "given" &&
            typeof node.refName === "string" &&
            typeof node.id === "string"
         ) {
            // A source argument binds while the source is built, so
            // `givenUsage` never lists it (see persist_dynamic_terms.ts).
            if (inArgument || usedIds.has(node.id)) names.add(node.refName);
            namedIds.add(node.id);
         }
         for (const [key, child] of Object.entries(node)) {
            const resolved =
               key === "structRef" && typeof child === "string"
                  ? modelDef?.contents[child]
                  : child;
            worklist.push([
               resolved,
               inArgument || key === "arguments" || key === "sourceArguments",
            ]);
         }
      }
      // A used id the walk never met is unnamed, so forward every candidate,
      // unless the model surfaces it: a default chain's ids appear nowhere in
      // the query IR, and a surfaced given is never a drop candidate by name.
      for (const entry of Object.values(modelDef?.contents ?? {})) {
         if (entry.type === "given") namedIds.add(entry.id);
      }
      if ([...usedIds].some((id) => !namedIds.has(id))) {
         for (const name of candidates) names.add(name);
      }
      return names;
   }

   // The id/name OR is safe only because Malloy refuses a second given of one
   // name across a notebook's extendModel chain, so it never picks between two.
   private cellDeclaresGivenId(
      surfaceName: string,
      cellDeclared: CellDeclaredGivens,
   ): boolean {
      const entry = this.modelDef?.contents[surfaceName];
      return entry?.type === "given" && cellDeclared.ids.has(entry.id);
   }

   /**
    * Compute {@link authorizeReferencedGivenNames} via
    * {@link collectEntryPointGates} rooted at every top-level source — the
    * traversal {@link assertAuthorizedForAllSources} runs per query.
    */
   private computeAuthorizeReferencedGivenNames(): Set<string> {
      const names = new Set<string>();
      const addExprs = (exprs: string[]) => {
         for (const expr of exprs) {
            for (const name of referencedGivenNames(expr)) names.add(name);
         }
      };
      for (const gates of this.entryPointGatesBySource.values()) {
         for (const entry of gates) {
            addExprs(entry.exprs);
         }
      }
      return names;
   }

   /**
    * Every top-level source's entry-point gates, keyed by the name a query would
    * enter through (`as || name` — the same key `extractSourcesFromModelDef`
    * uses, so a rename resolves the same way on both sides).
    *
    * `treatAsOwnGate` is true: a gate the source declares itself is probed
    * ambient-first while an inherited one stays `selfContained` (see
    * {@link collectEntryPointGates}). Flattening every gate to ambient-first
    * would let an inherited gate's `$LEVEL` be satisfied by an entry model that
    * declares its own `LEVEL`.
    */
   private computeEntryPointGatesBySource(): Map<string, GateEntry[]> {
      const byName = new Map<string, GateEntry[]>();
      const modelDef = this.modelDef;
      if (!modelDef) return byName;
      for (const entry of Object.values(modelDef.contents)) {
         if (!isSourceDef(entry)) continue;
         const name = (entry as { as?: string }).as ?? entry.name;
         byName.set(
            name,
            this.collectEntryPointGates(entry, modelDef, new Set(), true),
         );
      }
      return byName;
   }

   /**
    * Whether the model declares any `#(authorize)` gate at all, on any
    * source. Lets callers cheaply skip authorize work for ungated models
    * without compiling a probe.
    */
   public hasAuthorize(): boolean {
      return this.sources?.some((s) => (s.authorize?.length ?? 0) > 0) ?? false;
   }

   /** Memoized {@link hasAnyAuthorizeNote}; `undefined` until first asked. */
   private anyAuthorizeNote: boolean | undefined;

   /**
    * Whether this model carries an `#(authorize)` annotation ANYWHERE, so an
    * ungated deployment with pre-aggregation skips a per-query live compile
    * (see `getQueryResults`'s routing pre-check).
    *
    * Deliberately wider than {@link hasAuthorize}, which reads only top-level
    * sources' extracted gates and can miss an inherited one. This sweeps the
    * IR — sources, fields and `annotations.inherits` chains across `contents`
    * u `sourceRegistry` — and must be a superset of what
    * {@link collectEntryPointGates} reaches. That requires following two links
    * that land outside both collections ({@link derivedStructsReachable}):
    *  - a `query_source`'s base imported from another file is an INLINE
    *    `SourceDef` on `query.structRef`, not a name in `contents`;
    *  - a composite's resolved member (`query.compositeResolvedSourceDef`) is
    *    synthesized and in neither collection.
    * Unreadable IR returns `true`, so inexactness errs toward walking.
    */
   private hasAnyAuthorizeNote(): boolean {
      if (this.anyAuthorizeNote !== undefined) return this.anyAuthorizeNote;
      this.anyAuthorizeNote = ((): boolean => {
         const modelDef = this.modelDef;
         if (!modelDef) return false;
         try {
            for (const struct of this.reachableStructsForNoteSweep(modelDef)) {
               // `annotationTexts` (whole chain), not `ownLevelNoteTexts`: a
               // stray annotation on the deriving statement demotes the gate
               // to `annotations.inherits`.
               if (
                  containsAuthorizeAnnotationTag(
                     annotationTexts(struct.annotations) ?? [],
                  )
               ) {
                  return true;
               }
               for (const field of struct.fields) {
                  if (
                     containsAuthorizeAnnotationTag(
                        annotationTexts(field.annotations) ?? [],
                     )
                  ) {
                     return true;
                  }
               }
            }
            return false;
         } catch {
            return true;
         }
      })();
      return this.anyAuthorizeNote;
   }

   /**
    * Every struct {@link hasAnyAuthorizeNote} sweeps: top-level sources,
    * non-reference `sourceRegistry` entries, and everything reachable from
    * them through a derivation hop.
    */
   private reachableStructsForNoteSweep(modelDef: ModelDef): SourceDef[] {
      const structs: SourceDef[] = [];
      for (const obj of Object.values(modelDef.contents)) {
         if (isSourceDef(obj)) structs.push(obj);
      }
      for (const value of Object.values(modelDef.sourceRegistry ?? {})) {
         const entry = value.entry;
         if (entry.type === "source_registry_reference") continue;
         if (isSourceDef(entry)) structs.push(entry);
      }
      structs.push(...derivedStructsReachable(structs, modelDef));
      return structs;
   }

   /**
    * Runtime authorize gate. Throws `AccessDeniedError` (403) when a gate on
    * `sourceName` denies or cannot be classified; a row-filter gate is deferred
    * to `authorizeAndBindRunnable`, which grafts it. No gate = unrestricted.
    *
    * Fail closed. The 403 names only the source, never the expression, so
    * gate logic is not leaked.
    */
   public async assertAuthorized(
      sourceName: string | undefined,
      givens: Record<string, GivenValue>,
      bypassAuthorize = false,
      /** The scope a row-level gate classifies against; defaults to the
       *  model-wide scope (a notebook cell passes its own). */
      graftScope: GraftScope | undefined = this.defaultGraftScope(),
   ): Promise<void> {
      if (bypassAuthorize) {
         this.noteAuthorizeBypass("source", sourceName);
         return;
      }
      // Gated from the same entry-point walk the compiled backstop uses. Any
      // disagreement is a schema oracle: this runs BEFORE compilation, so a
      // wrongly admitted caller would see compile errors (`'no_such_field' is
      // not defined`) the backstop would have denied.
      const gates = sourceName
         ? this.entryPointGatesBySource.get(sourceName)
         : undefined;
      if (gates) {
         for (const entry of gates) {
            // A row-level gate is enforceable only against a runnable
            // (`authorizeAndBindRunnable`). Deferring it here is not fail-open:
            // every caller either is `authorizeAndBindRunnable` (which enforces
            // this gate later in the same call) or is a pre-compile fast path
            // that an unconditional backstop always follows. A missing
            // `modelDef` rejects like an unresolvable gate.
            const resolution = this.modelDef
               ? await this.resolveGateShape(entry, this.modelDef, graftScope)
               : ({ shape: "rejected", cause: undefined } as const);
            if (resolution.shape === "row_level") continue;
            denyUnlessAdmitted(resolution, givens, entry.label);
         }
         return;
      }
      // Not a declared top-level source: nothing gates it here. A caller
      // cannot reach the warehouse through an inline source without restricted
      // mode refusing it first (see `getQueryResults`); a notebook cell's
      // inline source is author-written.
   }

   /**
    * Counter + audit line for one skipped gate evaluation. Both places a
    * bypass can be honoured ({@link assertAuthorized} and
    * {@link authorizeAndBindRunnable}) funnel here, so this is the complete
    * audit surface. Identifiers go on the log line, not counter labels (see
    * {@link recordAuthorizeBypass}); the router's audit line carries
    * environment/organization and joins on package + model.
    */
   private noteAuthorizeBypass(
      entryPoint: AuthorizeBypassEntryPoint,
      sourceName: string | undefined,
   ): void {
      recordAuthorizeBypass(entryPoint);
      logger.info("authorize bypass", {
         entryPoint,
         sourceName: sourceName ?? "(query)",
         modelPath: this.modelPath,
         packageName: this.packageName,
      });
   }

   /**
    * Gate a compiled query on its ENTRY POINT: the run target's own
    * `#(authorize)`, gates it carries from what it derives from (`extend` /
    * query-source derivation), and, for a composite, the member branch Malloy
    * resolved. Semantics are AND across all collected gates and within each
    * source's expression list.
    *
    * A join the model AUTHOR wrote is NOT gated: a gate means "who may query
    * THIS source", so an author who joins sensitive data into an ungated source
    * has published it. A join the CALLER wrote is gated like an extra run
    * target when `options.callerRegion` locates caller text (`./caller_joins`).
    *
    * Runs unconditionally — never guarded by {@link hasAuthorize}, which misses
    * gates inherited from a non-top-level base.
    */
   public async assertAuthorizedForAllSources(
      runnable: { getPreparedQuery(): Promise<unknown> },
      givens: Record<string, GivenValue>,
      bypassAuthorize = false,
      options?: { checkOnly?: boolean; callerRegion?: CallerRegion },
   ): Promise<void> {
      // Check-only: there is no runnable to swap in, so a row-level gate
      // denies here rather than admitting unfiltered.
      await this.authorizeAndBindRunnable(
         runnable as QueryMaterializer,
         givens,
         {
            bypassAuthorize,
            checkOnly: options?.checkOnly,
            compileCallerText: options?.callerRegion?.text,
            callerJoins: options?.callerRegion
               ? {
                    runnable: runnable as QueryMaterializer,
                    region: options.callerRegion,
                 }
               : undefined,
         },
      );
   }

   /**
    * The entry-point gate walk shared by {@link assertAuthorizedForAllSources}
    * and {@link authorizeAndBindRunnable}: gate the run target's own source
    * name ({@link assertAuthorized}), then collect every gate reachable from
    * the compiled run-target `SourceDef` as an entry point (see
    * {@link assertAuthorizedForAllSources} for what that covers). This is the
    * audited answer to "which gate applies"; callers differ only in how they
    * enforce it.
    *
    * Joined sources are NOT collected: an author join carries no gate, and a
    * caller join is gated by {@link probeCallerJoinGates}.
    */
   private async collectAuthorizeEntryPointGates(
      runnable: { getPreparedQuery(): Promise<unknown> },
      givens: Record<string, GivenValue>,
      /** The scope a row-level gate grafts against; also forwarded to
       *  `assertAuthorized` so both classify in the same scope. */
      graftScope: GraftScope | undefined,
      options: {
         /** Skip the gate keyed on the run target's own SOURCE NAME, keeping
          *  only the gates readable from the compiled ModelDef. Set by
          *  {@link assertAuthorizedFromCompiledRunnable} for a `/compile` of a
          *  path with no cached Model, where the source name would resolve
          *  against an unrelated file. */
         skipOwnSourceGate?: boolean;
         /** The caller's text at `/compile` append scope; see the fold below. */
         callerText?: string;
      } = {},
   ): Promise<{
      entryPointGates: GateEntry[];
      modelDef: ModelDef | undefined;
      /** The run target's own compiled struct — see `resolveRunTargetStruct`. */
      struct: SourceDef | undefined;
   }> {
      const { skipOwnSourceGate = false, callerText } = options;
      const ownSourceName =
         await this.resolveAuthorizeSourceFromRunnable(runnable);
      // Skipped when this Model is unrelated to the target file (see
      // `assertAuthorizedFromCompiledRunnable`).
      if (!skipOwnSourceGate) {
         await this.assertAuthorized(ownSourceName, givens, false, graftScope);
      }

      const { struct, modelDef, compositeResolvedSourceDef } =
         await this.resolveRunTargetStruct(runnable);
      const seen = new Set<SourceDef>();
      // `struct` IS the run target — its own gate stays ambient-first
      // (`treatAsOwnGate: true`). Not deduped against `assertAuthorized`:
      // every reachable gate is evaluated independently.
      let entryPointGates = this.collectEntryPointGates(
         struct,
         modelDef,
         seen,
         true,
      );
      // A query-local `-> { join_one: ... }` is a caller join, gated by
      // `probeCallerJoinGates` rather than collected here.
      if (compositeResolvedSourceDef && modelDef) {
         // A composite run target resolves to exactly one member branch per
         // query; gate what is reachable from THAT branch, not every member
         // (a locked sibling must not deny an open branch). The branch stands
         // in for the run target, so its gate is ambient-first too.
         //
         // Not a fail-open gap: Malloy's `_resolveCompositeSources` always
         // resolves a composite run target to a concrete member or fails the
         // compile, so `undefined` only means "not a composite".
         entryPointGates.push(
            ...this.collectEntryPointGates(
               compositeResolvedSourceDef,
               modelDef,
               seen,
               true,
               undefined,
               // Identity-subtract `struct`'s (the composite's) own notes —
               // Malloy copies them onto the resolved member's own
               // `blockNotes` by reference; see `collectEntryPointGates`'s
               // `excludeNotes` doc.
               struct ? ownLevelNotes(struct.annotations) : [],
            ),
         );
      }
      // Fold in THIS model's on-disk gates for `ownSourceName`. The walk above
      // read the struct compiled from caller-submitted text (the `/compile`
      // backstop), so a caller who strips `#(authorize)` from that text would
      // otherwise escape; `entryPointGatesBySource` comes from the on-disk
      // model, which caller text cannot edit. Skipped under
      // `skipOwnSourceGate`, where `this` is unrelated to `ownSourceName`.
      //
      // Deduped by CONTENT, not struct identity: a fresh compile never shares
      // identity with `this.modelDef`, so identity dedup would graft the same
      // gate twice. The on-disk entry REPLACES its walk twin because only it
      // resolves as a graft target in the "model" scope; an append-scope
      // `/compile` twin comes from an ephemeral ModelDef and would be rejected,
      // denying a caller who supplied every given. On the ordinary query path
      // both twins resolve to the same target, so the swap is unobservable.
      if (!skipOwnSourceGate && ownSourceName) {
         const onDiskGates = this.entryPointGatesBySource.get(ownSourceName);
         if (onDiskGates) {
            // A separator no label or expression can contain, so
            // `{label: "a b", exprs: []}` cannot key like `{label: "a",
            // exprs: ["b"]}`. Not \0, which makes tools treat this file as
            // binary.
            const SEP = "\x1f";
            const keyOf = (entry: GateEntry): string =>
               `${entry.label}${SEP}${entry.route}${SEP}${entry.exprs.join(SEP)}${SEP}${entry.selfContained}`;
            const byKey = new Map(
               entryPointGates.map((entry) => [keyOf(entry), entry]),
            );
            for (const entry of onDiskGates) {
               byKey.set(keyOf(entry), entry);
            }
            entryPointGates = Array.from(byKey.values());
         }
      }
      if (callerText && !skipOwnSourceGate && ownSourceName) {
         entryPointGates = this.foldRequestDeclaredTwins(
            ownSourceName,
            callerText,
            entryPointGates,
         );
      }
      return { entryPointGates, modelDef, struct };
   }

   /**
    * A run target the caller's own text declared (`source: mine is gated
    * extend {}`, or a derivation through a model named query) has no
    * `modelDef.contents` key to graft onto, so its walk entries classify
    * `rejected` even when the inherited gate admits the caller. Swap each for
    * its on-disk twin on a base the text provably derives from; every other
    * entry stays, so an unexplained gate still denies. Runs on `/query` and
    * `/compile` alike: the landing proof after it decides whether a graft
    * lands, and a twin from a sibling base with identical gate text can graft on
    * the wrong base and be refused there (over-denial, never fail-open).
    */
   private foldRequestDeclaredTwins(
      ownSourceName: string,
      callerText: string,
      walked: GateEntry[],
   ): GateEntry[] {
      if (this.entryPointGatesBySource.has(ownSourceName)) return walked;
      const proof = this.requestChainGates(ownSourceName, callerText);
      if (!proof.proven) return walked;
      return this.swapForOnDiskTwins(
         walked,
         proof.gatesByBase.flatMap(([, gates]) => gates),
      );
   }

   /** Each `walked` entry replaced by the first unused `twins` entry with the same route and expressions. */
   private swapForOnDiskTwins(
      walked: GateEntry[],
      twins: GateEntry[],
   ): GateEntry[] {
      const used = new Set<GateEntry>();
      return walked.map((entry) => {
         const twin = twins.find(
            (t) =>
               !used.has(t) &&
               t.route === entry.route &&
               t.exprs.length === entry.exprs.length &&
               t.exprs.every((expr, i) => expr === entry.exprs[i]),
         );
         if (!twin) return entry;
         used.add(twin);
         return twin;
      });
   }

   /**
    * Gate only from the compiled runnable's own ModelDef, for a /compile of a
    * new path with no cached Model. The prepared query still carries imported
    * sources and their inherited gates, so this closes the missing-model
    * fail-open without borrowing an unrelated file's gate. Check-only: a
    * row-level gate denies.
    */
   public async assertAuthorizedFromCompiledRunnable(
      runnable: { getPreparedQuery(): Promise<unknown> },
      givens: Record<string, GivenValue>,
   ): Promise<void> {
      await this.authorizeAndBindRunnable(
         runnable as QueryMaterializer,
         givens,
         {
            skipOwnSourceGate: true,
            checkOnly: true,
         },
      );
   }

   /**
    * The opaque 403 a row-level gate gives when a given it reads went
    * unsupplied, or undefined when none did or no gate was grafted. Opaque on
    * purpose: Malloy's own failure names the missing given, and
    * `docs/authorize.md` promises a denied caller is never told which one.
    * `fallbackLabel` names the source when no caller join's graft owns the
    * missing given.
    *
    * Shared by the prepare-time failure handler and the run-statement count,
    * which must yield to this denial rather than answer 400 first.
    *
    * `gateGivenNames`: the prepare handler passes every name any gate reads (a
    * binding failure already confirmed one is missing); the count passes only
    * this query's own grafts' names, so another source's gate cannot turn its
    * 400 into a 403.
    */
   private unboundGateDenial(
      runnable: QueryMaterializer,
      givens: Record<string, unknown> | undefined,
      fallbackLabel: string,
      gateGivenNames: Iterable<string>,
   ): AccessDeniedError | undefined {
      if (
         !this.queryHadRowLevelFilterAttached(runnable) ||
         ![...gateGivenNames].some((name) => !(name in (givens ?? {})))
      ) {
         return undefined;
      }
      // A caller join's graft answers under the caller's own alias.
      const unbound = this.rowLevelFilteredRunnables
         .get(runnable)
         ?.find(
            (graft) =>
               graft.callerJoinPath &&
               graft.givenNames.some((name) => !(name in (givens ?? {}))),
         );
      recordRowLevelGateDecision(
         "denied_by_gate",
         unbound ? "caller_join" : "entry_point",
      );
      return new AccessDeniedError(
         `Access denied for source "${unbound?.label ?? fallbackLabel}".`,
      );
   }

   /**
    * Whether `runnable` (a value {@link authorizeAndBindRunnable} returned) has
    * a row-level `#(access_filter)` filter attached; see
    * {@link rowLevelFilteredRunnables}.
    */
   public queryHadRowLevelFilterAttached(runnable: QueryMaterializer): boolean {
      return this.rowLevelFilteredRunnables.has(runnable);
   }

   /**
    * Collect and evaluate every entry-point gate on `runnable`, as
    * {@link authorizeAndBindRunnable} does before grafting: a lock or rejected
    * gate throws now, and each `row_level` gate is returned (deferred) for the
    * caller to graft.
    *
    * Has no bypass concept, so calling it twice per request (pre- and
    * post-refinement) never double-counts a bypass audit.
    */
   private async probeEntryPointGates(
      runnable: QueryMaterializer,
      givens: Record<string, GivenValue>,
      graftScope: GraftScope | undefined,
      options: { skipOwnSourceGate?: boolean; callerText?: string } = {},
   ): Promise<RowLevelGraftEntry[]> {
      const { entryPointGates, modelDef } =
         await this.collectAuthorizeEntryPointGates(
            runnable,
            givens,
            graftScope,
            options,
         );

      // Evaluate each gate independently — never dedup by expression text: a
      // non-deterministic gate (e.g. random()) must not be evaluated once and
      // reused.
      const rowLevel: RowLevelGraftEntry[] = [];
      for (const entry of entryPointGates) {
         const resolution = modelDef
            ? await this.resolveGateShape(entry, modelDef, graftScope)
            : ({ shape: "rejected", cause: undefined } as const);
         if (resolution.shape === "row_level") {
            rowLevel.push({
               label: entry.label,
               graftTarget: resolution.graftTarget,
               filterText: resolution.filterText,
               condition: resolution.condition,
               givenNames: resolution.givenNames,
            });
            continue;
         }
         denyUnlessAdmitted(resolution, givens, entry.label);
      }
      return rowLevel;
   }

   /**
    * Whether `runnable`'s entry point carries any gate, deciding whether to
    * attempt storage-serve routing at all.
    *
    * Security-relevant, not a performance pre-filter: the serve-shape model
    * carries no annotation bytes, so once routed, nothing downstream —
    * including {@link authorizeAndBindRunnable} — can discover a gate this
    * missed. A false negative is never undone.
    *
    * ROUTE-BLIND ON PURPOSE: `gates` is tested for length, never route. An
    * `#(access_filter)` would serve unfiltered rows and an `#(authorize)` lock
    * would never be decided; narrowing to one route is the bypass.
    *
    * Never calls `assertAuthorized`: a routing decision must not deny or
    * double-count metrics. Uses the same traversal
    * ({@link collectEntryPointGates}) as the authoritative check, over the
    * LIVE struct, and fails closed on any exception.
    */
   private async queryEntryPointHasRowLevelGate(
      runnable: {
         getPreparedQuery(): Promise<unknown>;
      },
      /** Where caller text sits, so a caller join into a gated source counts. */
      callerRegion?: CallerRegion,
   ): Promise<boolean> {
      try {
         const { struct, modelDef, compositeResolvedSourceDef } =
            await this.resolveRunTargetStruct(runnable);
         // "Cannot tell", like the catch below: `resolveRunTargetStruct`
         // swallows a live compile failure as `modelDef: undefined`, while the
         // annotation-free serve shape may still compile and answer from
         // unfiltered rows.
         if (!modelDef) return true;
         const seen = new Set<SourceDef>();
         const gates = this.collectEntryPointGates(
            struct,
            modelDef,
            seen,
            true,
         );
         if (compositeResolvedSourceDef) {
            gates.push(
               ...this.collectEntryPointGates(
                  compositeResolvedSourceDef,
                  modelDef,
                  seen,
                  true,
                  undefined,
                  // Identity-subtract `struct`'s own notes — see
                  // `collectAuthorizeEntryPointGates`'s identical exclusion
                  // and `collectEntryPointGates`'s `excludeNotes` doc.
                  struct ? ownLevelNotes(struct.annotations) : [],
               ),
            );
         }
         if (gates.length > 0) return true;
         if (!callerRegion) return false;
         // The serve shape carries no annotation for a caller join's gate
         // either, so the same route-blind question is asked of each one.
         const callerJoins = await this.resolvedCallerJoins(
            runnable,
            callerRegion,
         );
         if (!callerJoins) return true;
         const gated = (name: string) =>
            (this.entryPointGatesBySource.get(name)?.length ?? 0) > 0;
         return callerJoins.some(
            ({ resolution, compiledModelDef }) =>
               resolution.kind === "unproven" ||
               Array.from(resolution.names).some(gated) ||
               resolution.callerSources.some(
                  (source) =>
                     !source.chain.proven ||
                     source.chain.terminals.some(gated) ||
                     this.collectEntryPointGates(
                        source.struct as unknown as SourceDef,
                        compiledModelDef,
                        new Set<SourceDef>(),
                        true,
                     ).length > 0,
               ),
         );
      } catch {
         // Fail closed: a false positive costs a live query; a false negative
         // serves unfiltered rows with nothing downstream to catch it.
         return true;
      }
   }

   /**
    * Deny a request that enters a gated source through a derivation the
    * REQUEST ITSELF declared, when the entry-point walk collected no gate for
    * the entry point actually being run.
    *
    * The dimension form of `#(authorize)` is enforced by grafting `where:
    * \`<name>\`` onto the entry BY NAME, so a request-declared `source: mine is
    * X extend { except: authorized }` (or an `accept:` omitting it) presents IR
    * with no gate, which would also un-block storage/pre-aggregation routing.
    *
    * The run target comes from the COMPILED query
    * ({@link resolveAuthorizeSourceFromRunnable}): Malloy runs the LAST `run:`,
    * reads around comments and resolves `query:` indirection, so a text scan
    * would err in both directions.
    *
    * The default is DENY: an ephemeral entry point (not a `modelDef.contents`
    * key) is admitted only when {@link requestChainProvesUngated} positively
    * proves a chain to model-declared ungated sources. Asking the inverse
    * question made every divergence between the scan and Malloy's grammar
    * (comments, string literals, backtick names, parenthesised bases,
    * non-ASCII names) a full bypass; with deny as default it costs an
    * over-denial.
    *
    * The derivation chain must be read from text because the IR does not keep
    * one: an ad-hoc `source: mine is X extend {…}` compiles to a self-reference
    * with no `referenceID`, identical for `except: authorized`, `extend {}` and
    * an ungated sibling. "Reads the same relation as a gated source" is wrong
    * too: ungated siblings usually read the same table. The scan is hardened
    * ({@link stripMalloyCommentsAndLiterals}, Unicode identifiers, parameter
    * lists, parenthesised bases) and over-collects edges, which is safe.
    *
    * Known safe over-denials: derivations over `compose(...)` or a
    * parameterised source. Raw `connection.table(...)`/`.sql(...)` never
    * reaches here (restricted mode refuses it).
    *
    * A MODEL-declared derivation that drops the gate field keeps its documented
    * fail-open (`docs/authorize.md`); this does not narrow it. Request-declared
    * shapes that KEEP the gate field already deny (no graft target for an
    * ephemeral entry), which is why this is the cannot-attach 403 rather than
    * zero rows.
    */
   private async assertRequestDeclaredEntryPointIsNotLaundered(
      runnable: QueryMaterializer,
      query: string,
      givens: Record<string, GivenValue>,
      graftScope: GraftScope | undefined,
   ): Promise<void> {
      // Nothing to launder unless this model actually declares a gate.
      if (!this.declaresAnyGate()) return;
      const entryPoint =
         await this.resolveAuthorizeSourceFromRunnable(runnable);
      // An unresolvable target did not compile and returns no rows; the
      // compiler's diagnostic is the right answer, not a 403.
      if (!entryPoint) return;
      // A key here means the author declared the source, and its gates were
      // already folded in by name.
      if (this.entryPointGatesBySource.has(entryPoint)) return;
      // Ephemeral: denied unless the chain is positively proven.
      const proof = this.requestChainGates(entryPoint, query);
      if (proof.proven) {
         // Every gated base reached must be a lock this caller satisfies: a
         // lock is decided, not attached, so the alias has nothing to strip.
         // A row filter has no graft target on an ephemeral entry, so it is
         // refused rather than silently dropped.
         for (const [base, gates] of proof.gatesByBase) {
            for (const entry of gates) {
               const resolution = this.modelDef
                  ? await this.resolveGateShape(
                       entry,
                       this.modelDef,
                       graftScope,
                    )
                  : ({ shape: "rejected", cause: undefined } as const);
               if (resolution.shape === "row_level") {
                  this.denyLaunderedEntryPoint(
                     entryPoint,
                     "row_filter_not_graftable",
                     base,
                  );
               }
               // Labelled with the ephemeral entry point, not the gated base.
               denyUnlessAdmitted(resolution, givens, entryPoint);
            }
         }
         return;
      }
      this.denyLaunderedEntryPoint(entryPoint, proof.reason, proof.at);
   }

   /** {@link requestChainProvesUngated} over `text`, plus the on-disk gates of each gated base it reached. */
   private requestChainGates(
      entryPoint: string,
      text: string,
   ):
      | {
           proven: true;
           gatesByBase: ReadonlyArray<readonly [string, GateEntry[]]>;
        }
      | { proven: false; reason: "chain_not_established"; at: string } {
      const proof = this.requestChainProvesUngated(
         entryPoint,
         buildDerivationBaseMap(text),
      );
      if (!proof.proven) return proof;
      return {
         proven: true,
         gatesByBase: proof.gatedBases.map(
            (base) =>
               [base, this.entryPointGatesBySource.get(base) ?? []] as const,
         ),
      };
   }

   /**
    * Decide, before compiling, every lock reachable from an entry point THIS
    * REQUEST declared — `/compile` at file and package scope, where the text
    * is the author's whole file and is not compiled in restricted mode.
    *
    * Throws on a refused lock. A source the text declares but never runs has
    * no runnable, so this walk is the only check it gets. Unbounded on purpose
    * (every declared name may be a root, so a cap would skip locked aliases);
    * `seen` bounds the work to the text's edge count.
    */
   private async assertLocksOnRequestDeclaredBases(
      entryPoint: string,
      query: string,
      givens: Record<string, GivenValue>,
      bypassAuthorize?: boolean,
   ): Promise<void> {
      if (!this.declaresAnyGate()) return;
      const basesOf = buildDerivationBaseMap(query);
      const seen = new Set<string>();
      // With no run target, every name the text declares is a root.
      const worklist = entryPoint ? [entryPoint] : [...basesOf.keys()];
      for (let i = 0; i < worklist.length; i++) {
         const name = worklist[i];
         if (seen.has(name)) continue;
         seen.add(name);
         if (this.entryPointGatesBySource.has(name)) {
            try {
               // Names a source the request's own text derives from, so
               // reporting it back tells the caller nothing they did not write.
               await this.assertAuthorized(name, givens, bypassAuthorize);
            } catch (error) {
               // Convert here, on the base actually gated: a hidden one must be
               // a 404, not a 403 naming it, and callers' conversions cannot
               // see through this alias.
               if (error instanceof AccessDeniedError) {
                  this.refuseGatedNameAsBoundary(name);
               }
               throw error;
            }
            continue;
         }
         for (const base of basesOf.get(name) ?? []) worklist.push(base);
      }
   }

   /** The one refusal for a request-declared entry point, and its audit line. */
   private denyLaunderedEntryPoint(
      entryPoint: string,
      reason: string,
      at: string,
   ): never {
      recordRowLevelGateDecision("denied_by_gate");
      logger.debug(
         "Request-declared entry point in a gated model is not provably ungated; denying",
         { modelPath: this.modelPath, entryPoint, reason, at },
      );
      // Names the compiled entry point the request itself declared — never the
      // gate's column, nor which gated source it may have reached.
      throw new AccessDeniedError(`Access denied for source "${entryPoint}".`);
   }

   /** Memo for {@link declaresAnyGate}. */
   private declaresAnyGateMemo?: boolean;

   /**
    * Whether ANY source in this model carries an entry-point gate. Not
    * {@link hasAuthorize} (misses inherited gates), and not
    * `entryPointGatesBySource.size > 0` (every source has a key).
    */
   private declaresAnyGate(): boolean {
      this.declaresAnyGateMemo ??= Array.from(
         this.entryPointGatesBySource.values(),
      ).some((gates) => gates.length > 0);
      return this.declaresAnyGateMemo;
   }

   /**
    * Whether the request's own declarations establish that `entryPoint` — an
    * EPHEMERAL entry point, already known not to be a model-declared source —
    * derives only from model-declared UNGATED sources.
    *
    * This is a PROOF, and its absence denies. Every name reached must resolve
    * to a model-declared source (a terminal; gated ones are returned for the
    * caller to decide) or to a readable declaration to keep following. Any
    * other name — unreadable grammar, or never declared — ends the walk
    * unproven. That direction makes the `./query_text` patterns non-critical:
    * a divergence from Malloy's grammar costs an over-denial, never a
    * laundered read. Over-collected edges can likewise only deny.
    */
   private requestChainProvesUngated(
      entryPoint: string,
      basesOf: Map<string, Set<string>>,
   ):
      | { proven: true; gatedBases: readonly string[] }
      | {
           proven: false;
           reason: "chain_not_established";
           at: string;
           gatedBases?: undefined;
        } {
      // A chain past the name budget is not a real derivation, and stops on
      // the deny side; a model-declared source ends its branch without
      // following the AUTHOR's own derivations, which keeps the documented
      // model-authored fail-open intact.
      const chain = derivationTerminals(
         entryPoint,
         basesOf,
         (name) => this.entryPointGatesBySource.has(name),
         REQUEST_CHAIN_MAX_NAMES,
         (name) => this.modelQuerySource(name),
      );
      if (!chain.proven) {
         return {
            proven: false,
            reason: "chain_not_established",
            at: chain.at,
         };
      }
      // A gated base does not disprove the chain: an admitting LOCK leaves
      // nothing to launder. The caller denies anything else.
      return {
         proven: true,
         gatedBases: chain.terminals.filter(
            (name) => (this.entryPointGatesBySource.get(name)?.length ?? 0) > 0,
         ),
      };
   }

   private modelQuerySource(name: string): string | undefined {
      return this.queries?.find((q) => q.name === name)?.sourceName;
   }

   /**
    * Every caller-written join the compiled `runnable` reaches, each resolved
    * to the model-declared sources it reads (see `./caller_joins`).
    * `undefined` when the query does not compile: its own execution fails on
    * that same compile, and {@link assertCallerJoinBasesEarly} is what keeps
    * the compiler's diagnostics from answering first for a gated source.
    * Throws {@link CallerJoinWalkError} for a walk it cannot complete.
    */
   private resolvedCallerJoins(
      runnable: { getPreparedQuery(): Promise<unknown> },
      region: CallerRegion,
   ): Promise<ResolvedCallerJoin[] | undefined> {
      // One walk per request, shared by the routing, boundary and authorize
      // passes, which all read the same live runnable.
      const cached = this.callerJoinResolutions.get(runnable);
      if (cached && cached.region === region) return cached.result;
      const result = this.computeCallerJoins(runnable, region);
      this.callerJoinResolutions.set(runnable, { region, result });
      return result;
   }

   private callerJoinResolutions = new WeakMap<
      object,
      {
         region: CallerRegion;
         result: Promise<ResolvedCallerJoin[] | undefined>;
      }
   >();

   private async computeCallerJoins(
      runnable: { getPreparedQuery(): Promise<unknown> },
      region: CallerRegion,
   ): Promise<ResolvedCallerJoin[] | undefined> {
      let prepared: PreparedQueryIr;
      try {
         prepared = (await runnable.getPreparedQuery()) as PreparedQueryIr;
      } catch {
         return undefined;
      }
      const compiledModelDef = prepared._modelDef;
      if (!compiledModelDef) {
         throw new CallerJoinWalkError("the compiled query carries no model");
      }
      const authorUrls = this.authorUrls();
      const context = {
         isModelSource: (name: string) =>
            this.entryPointGatesBySource.has(name),
         isModelQuery: (name: string) =>
            (this.modelDef?.contents[name] as { type?: string } | undefined)
               ?.type === "query",
         modelQuerySource: (name: string) => this.modelQuerySource(name),
         authorUrls,
      };
      return collectCallerJoins(prepared, region, authorUrls).map(
         (callerJoin) => ({
            callerJoin,
            compiledModelDef,
            resolution: resolveCallerJoin(
               callerJoin,
               compiledModelDef,
               region,
               context,
            ),
         }),
      );
   }

   private authorUrlsMemo?: ReadonlySet<string>;

   /**
    * The URLs of this model's own files, read off the on-disk `ModelDef` only:
    * a location under any other URL is caller-written.
    */
   private authorUrls(): ReadonlySet<string> {
      if (this.authorUrlsMemo) return this.authorUrlsMemo;
      const urls = new Set<string>();
      const modelDef = this.modelDef;
      if (modelDef) {
         urls.add(modelDef.modelID);
         for (const url of Object.keys(modelDef.modelAnnotations ?? {})) {
            urls.add(url);
         }
         for (const imported of modelDef.imports ?? []) {
            urls.add(imported.importURL);
         }
         const walk = (tree: ModelDef["dependencies"] | undefined) => {
            for (const [url, deps] of Object.entries(tree ?? {})) {
               urls.add(url);
               walk(deps);
            }
         };
         walk(modelDef.dependencies);
         for (const entry of Object.values(modelDef.contents)) {
            const url = (entry as { location?: { url?: string } } | undefined)
               ?.location?.url;
            if (url) urls.add(url);
         }
      }
      this.authorUrlsMemo = urls;
      return urls;
   }

   /**
    * The gates a caller-declared source reached by a caller join carries, read
    * the way a request-declared run target's are: its compiled struct first,
    * and only when that finds none, the text chain as the laundering proof —
    * every base a model source, and any gated one an admitting lock.
    */
   private async decideCallerSource(
      source: CallerSource,
      compiledModelDef: ModelDef,
      alias: string,
      givens: Record<string, GivenValue>,
      graftScope: GraftScope | undefined,
   ): Promise<Awaited<ReturnType<Model["resolveGateShape"]>>[]> {
      const gates = this.collectEntryPointGates(
         source.struct as unknown as SourceDef,
         compiledModelDef,
         new Set<SourceDef>(),
         true,
      );
      if (gates.length > 0) {
         // The inherited gate of a source the caller declared has no contents
         // key to graft onto; its on-disk twin does.
         const twins = source.chain.proven
            ? source.chain.terminals.flatMap(
                 (base) => this.entryPointGatesBySource.get(base) ?? [],
              )
            : [];
         const swapped = this.swapForOnDiskTwins(gates, twins);
         return Promise.all(
            swapped.map((entry, i) =>
               entry === gates[i]
                  ? this.resolveGateShape(entry, compiledModelDef, graftScope)
                  : this.resolveGateShape(
                       entry,
                       this.modelDef ?? compiledModelDef,
                       graftScope,
                    ),
            ),
         );
      }
      if (!source.chain.proven) {
         this.denyCallerJoin(alias, "chain_not_established", source.chain.at);
      }
      for (const base of source.chain.terminals) {
         for (const entry of this.entryPointGatesBySource.get(base) ?? []) {
            const shape = this.modelDef
               ? await this.resolveGateShape(entry, this.modelDef, graftScope)
               : ({ shape: "rejected", cause: undefined } as const);
            if (shape.shape === "row_level") {
               this.denyCallerJoin(alias, "row_filter_not_graftable", base);
            }
            denyUnlessAdmitted(shape, givens, alias, "caller_join");
         }
      }
      return [];
   }

   /**
    * Decide every gate a caller-written join reaches, as if the joined source
    * were an extra run target: a lock is decided here and refuses with a 403
    * naming the caller's own alias; a row filter comes back as a graft for
    * that join, attached and proven alongside the run target's. A composite
    * join's row filter is refused, since which member reaches SQL is not known
    * here. A join whose chain cannot be proven, and a walk that cannot be
    * completed, deny. A no-op in a model that declares no gate.
    */
   private async probeCallerJoinGates(
      runnable: QueryMaterializer,
      region: CallerRegion,
      givens: Record<string, GivenValue>,
      graftScope: GraftScope | undefined,
   ): Promise<RowLevelGraftEntry[]> {
      if (!this.declaresAnyGate()) return [];
      let resolved:
         | Awaited<ReturnType<Model["resolvedCallerJoins"]>>
         | undefined;
      try {
         resolved = await this.resolvedCallerJoins(runnable, region);
      } catch (err) {
         this.denyCallerJoin(
            (await this.resolveAuthorizeSourceFromRunnable(runnable)) ??
               "query",
            "walk_incomplete",
            err instanceof Error ? err.message : String(err),
         );
      }
      if (!resolved) return [];
      const grafts: RowLevelGraftEntry[] = [];
      for (const { callerJoin, compiledModelDef, resolution } of resolved) {
         const alias = callerJoin.alias;
         if (resolution.kind === "unproven") {
            this.denyCallerJoin(alias, "chain_not_established", resolution.at);
         }
         const shapes: {
            at: string;
            shape: Awaited<ReturnType<Model["resolveGateShape"]>>;
         }[] = [];
         for (const name of resolution.names) {
            for (const entry of this.entryPointGatesBySource.get(name) ?? []) {
               shapes.push({
                  at: name,
                  shape: this.modelDef
                     ? await this.resolveGateShape(
                          entry,
                          this.modelDef,
                          graftScope,
                       )
                     : ({ shape: "rejected", cause: undefined } as const),
               });
            }
         }
         for (const source of resolution.callerSources) {
            for (const shape of await this.decideCallerSource(
               source,
               compiledModelDef,
               alias,
               givens,
               graftScope,
            )) {
               shapes.push({ at: source.key, shape });
            }
         }
         for (const { at, shape } of shapes) {
            if (shape.shape === "row_level") {
               if (resolution.composite) {
                  this.denyCallerJoin(alias, "row_filter_not_graftable", at);
               }
               grafts.push({
                  label: alias,
                  graftTarget: shape.graftTarget,
                  filterText: shape.filterText,
                  condition: shape.condition,
                  givenNames: shape.givenNames,
                  callerJoinPath: callerJoin.path,
               });
               continue;
            }
            denyUnlessAdmitted(shape, givens, alias, "caller_join");
         }
      }
      return grafts;
   }

   /** The one refusal for a caller join, naming only the caller's own alias. */
   private denyCallerJoin(alias: string, reason: string, at: string): never {
      recordRowLevelGateDecision("denied_by_gate", "caller_join");
      logger.debug("Caller join is not provably allowed; denying", {
         modelPath: this.modelPath,
         alias,
         reason,
         at,
      });
      throw new AccessDeniedError(`Access denied for source "${alias}".`);
   }

   /**
    * The query boundary (the *what* axis, 404) for caller-written joins: a
    * join may reach only what `run:` could. A join to an exported author query
    * passes on that query's name; any other passes only when every source it
    * reaches is curated (a composite on its own name, as a composite run
    * target is), and one whose chain cannot be proven, or that the walk cannot
    * complete, is refused. Runs before any authorize gate and under
    * `bypassAuthorize`, like the run target's own backstop. A no-op when the
    * boundary is inert.
    */
   private async assertCallerJoinsQueryable(
      runnable: QueryMaterializer,
      region: CallerRegion,
   ): Promise<void> {
      const { mode, exploresDeclared } = this.queryBoundary;
      if (mode === "all" || !exploresDeclared) return;
      const refusal = () =>
         this.notQueryable("Query target is not queryable.", true, "source");
      let resolved: Awaited<ReturnType<Model["resolvedCallerJoins"]>>;
      try {
         resolved = await this.resolvedCallerJoins(runnable, region);
      } catch {
         throw refusal();
      }
      if (!resolved) throw refusal();
      for (const { resolution } of resolved) {
         if (resolution.kind === "unproven") throw refusal();
         if (resolution.boundaryUnprovenAt !== undefined) throw refusal();
         if (
            resolution.curatedQuery &&
            this.isCuratedQuery(resolution.curatedQuery)
         ) {
            continue;
         }
         for (const name of resolution.boundaryNames) {
            if (!this.isCuratedSource(name)) throw refusal();
         }
      }
   }

   /**
    * The query boundary for the joins a document's own text writes, the same
    * two checks the query route runs on a caller join: `text` read before
    * compiling, then the compiled `runnable` once it exists. A document that
    * passes both runs on the query route. A no-op when the boundary is inert.
    */
   public async assertDocumentJoinsQueryable(
      text: string,
      runnable?: { getPreparedQuery(): Promise<unknown> },
   ): Promise<void> {
      if (!runnable) {
         await this.assertCallerJoinBasesEarly(text, {}, "boundary");
         return;
      }
      await this.assertCallerJoinsQueryable(runnable as QueryMaterializer, {
         kind: "query",
         text,
      });
   }

   /**
    * The boundary's refusal for a gated name reached through an alias or a join,
    * which the caller did not write. In a gated model its words are the generic
    * sentence, since `No queryable source "<name>".` beside `Query target is not
    * queryable.` would confirm the name is a real, locked source.
    */
   private refuseGatedNameAsBoundary(name: string): void {
      try {
         this.assertQueryBoundaryEarly(name, undefined, undefined);
      } catch (error) {
         if (
            error instanceof NotQueryableError &&
            !(error instanceof OffSurfaceError) &&
            this.hasAnyAuthorizeNote()
         ) {
            throw new NotQueryableError(GENERIC_NOT_QUERYABLE);
         }
         throw error;
      }
   }

   /**
    * Throws the generic 404 for `query` (with the document's `definitions`
    * ahead of it) when a run target, or the base chain of a source the text
    * declares, is not queryable, in a model where that 404 is the only answer:
    * a hidden source, a hidden gated one and an absent one then read the same,
    * so neither a compile problem nor a document that comes back confirms a
    * name. An ungated model explains a hidden source already, so there the
    * compile problem is shown. A no-op when the boundary is inert.
    */
   public assertTextNameVisible(query: string, definitions: string): void {
      const { mode, exploresDeclared } = this.queryBoundary;
      if (mode === "all" || !exploresDeclared) return;
      if (!this.hasAnyAuthorizeNote()) return;
      const text = `${definitions}\n${query}`;
      const queryable = (name: string): boolean =>
         this.isCuratedSource(name) || this.derivesFromCurated(name, text);
      if (
         extractRunTargetSourceNames(text).every(queryable) &&
         [...buildDerivationBaseMap(text).keys()].every(queryable)
      ) {
         return;
      }
      throw new NotQueryableError(GENERIC_NOT_QUERYABLE);
   }

   /**
    * Decide, before compiling, what a caller-written join's TEXT reaches: the
    * boundary (`phase: "boundary"`) or every lock (`phase: "locks"`), so a
    * refused caller hears 404 or 403 rather than the compiler's opinion of a
    * column on that source. Never the authority: the compiled checks decide,
    * a chain this cannot follow just stops, and a join in a declaration the
    * query never runs is read too — an over-denial, never an admission.
    */
   private async assertCallerJoinBasesEarly(
      query: string,
      givens: Record<string, GivenValue>,
      phase: "boundary" | "locks",
      /** Locked names this request already decided; each one decided here is added. */
      decided: Set<string> = new Set(),
      /** The request lifted the surface: a refusal stays the lock's 403. */
      includeHiddenFilesAndSources = false,
   ): Promise<void> {
      const joins = buildJoinBaseMap(query);
      if (joins.size === 0) return;
      if (phase === "boundary") {
         const { mode, exploresDeclared } = this.queryBoundary;
         if (mode === "all" || !exploresDeclared) return;
      } else if (!this.declaresAnyGate() || !this.modelDef) {
         return;
      }
      const derivations = buildDerivationBaseMap(query);
      for (const [alias, bases] of joins) {
         const seen = new Set<string>();
         const worklist = [...bases];
         for (let i = 0; i < worklist.length; i++) {
            const name = worklist[i];
            if (seen.has(name)) continue;
            seen.add(name);
            if (seen.size > REQUEST_CHAIN_MAX_NAMES) break;
            if (!this.entryPointGatesBySource.has(name)) {
               for (const base of derivations.get(name) ?? []) {
                  worklist.push(base);
               }
               // A named-query hop is this join's to decide, under its alias,
               // the same way the compiled boundary check treats it: an
               // exported query admits on its own name, so only a
               // non-curated one follows the hop to its source.
               const querySource = this.queries?.find(
                  (q) => q.name === name,
               )?.sourceName;
               if (
                  querySource &&
                  (phase !== "boundary" || !this.isCuratedQuery(name))
               ) {
                  worklist.push(querySource);
               }
               continue;
            }
            if (phase === "boundary") {
               if (!this.isCuratedSource(name)) {
                  throw this.notQueryable(
                     "Query target is not queryable.",
                     true,
                     "source",
                  );
               }
               continue;
            }
            if (decided.has(name)) continue;
            try {
               for (const entry of this.entryPointGatesBySource.get(name) ??
                  []) {
                  const resolution = await this.resolveGateShape(
                     entry,
                     this.modelDef!,
                     this.defaultGraftScope(),
                  );
                  if (resolution.shape === "row_level") continue;
                  denyUnlessAdmitted(resolution, givens, alias, "caller_join");
               }
            } catch (error) {
               // A 403 naming a hidden source over this join's alias confirms
               // it exists; convert on the source actually gated, the same
               // way the other early lock passes do.
               if (
                  error instanceof AccessDeniedError &&
                  !includeHiddenFilesAndSources
               ) {
                  this.refuseGatedNameAsBoundary(name);
               }
               throw error;
            }
            decided.add(name);
         }
      }
   }

   /**
    * Authorize a compiled runnable and return THE RUNNABLE TO EXECUTE — the
    * one authoritative gate entry point. A row-level gate has no admit/deny
    * answer, only a set of rows, so it is enforced by recompiling the caller's
    * UNMODIFIED query against a model whose entry source carries the condition
    * as a `where:`. Appending `+ {where: ...}` to the query text would leak: it
    * binds to the caller's last stage and a projection can neutralize it.
    *
    * Locks and rejected gates are decided by {@link probeEntryPointGates};
    * row-level gates are collected and grafted together. With none,
    * `runnable` is returned unchanged.
    *
    * A row-level gate with no `options.recompile` denies (nothing to rewrite),
    * except for a `checkOnly` caller whose gate is decidable (see below).
    *
    * Otherwise all conditions graft onto ONE copied `ModelDef`
    * ({@link getOrBuildGraftedMaterializer}), `recompile` produces the runnable,
    * and {@link assertGateLanded} proves each condition is present before it is
    * trusted. Any failure denies; the 403 names nothing about the gate (a gate
    * reading `childtable.name` reveals a relationship), details go to `debug`.
    */
   public async authorizeAndBindRunnable(
      runnable: QueryMaterializer,
      givens: Record<string, GivenValue>,
      options?: {
         recompile?: (
            materializer: ModelMaterializer,
            grafts: ReadonlyArray<{
               graftTarget: string;
               condition: FilterCondition;
            }>,
         ) => QueryMaterializer;
         /**
          * Replaces {@link assertGateLanded} where the IR proof would be
          * vacuous: the notebook own-scope queryDef graft mutates the object
          * the prover would read. Supplied with `recompile` by
          * {@link ownScopeQueryDefBinder} so the two cannot drift.
          */
         proveGraft?: (recompiled: QueryMaterializer) => Promise<void>;
         bypassAuthorize?: boolean;
         /** The scope a row-level gate grafts against; defaults to
          *  {@link defaultGraftScope}. A notebook cell must pass its own. */
         graftScope?: GraftScope;
         /** See {@link assertAuthorizedFromCompiledRunnable}. */
         skipOwnSourceGate?: boolean;
         /**
          * The caller's appended text at `/compile` append scope; see {@link foldRequestDeclaredTwins}.
          * Kept apart from `callerQueryText`, whose presence arms the launder check.
          */
         compileCallerText?: string;
         /**
          * The CALLER-written query text, read by
          * {@link assertRequestDeclaredEntryPointIsNotLaundered} and
          * {@link foldRequestDeclaredTwins}. Absent for a notebook cell, whose
          * text is the author's and keeps the author-side fail-open.
          */
         callerQueryText?: string;
         /**
          * The caller will NEVER execute this runnable (`/compile`). Only such
          * a caller may get back an ungrafted runnable when a gate resolved.
          */
         checkOnly?: boolean;
         /**
          * Where caller-written text sits, and the LIVE compiled query to find
          * its joins in — never a storage serve shape or pre-aggregation
          * runnable, which re-emit author joins with no annotations. Absent
          * for text the author wrote (a notebook cell, a named query, a
          * whole-file `/compile`). See {@link probeCallerJoinGates}.
          */
         callerJoins?: { runnable: QueryMaterializer; region: CallerRegion };
      },
   ): Promise<QueryMaterializer> {
      // Bypass skips the gate walk but not the source-name resolution (the
      // audit trail) or the inherited-filter bind check: a misbound `where:` is
      // a wrong answer, not an authorization decision.
      if (options?.bypassAuthorize) {
         this.noteAuthorizeBypass(
            "runnable",
            await this.resolveAuthorizeSourceFromRunnable(runnable),
         );
         await this.assertNoMisboundInheritedFilters(
            runnable,
            new Set(),
            "entry_point",
            options?.callerJoins?.region,
         );
         return runnable;
      }

      const graftScope = options?.graftScope ?? this.defaultGraftScope();
      const entryPointRowLevel = await this.probeEntryPointGates(
         runnable,
         givens,
         graftScope,
         {
            skipOwnSourceGate: options?.skipOwnSourceGate,
            callerText: options?.compileCallerText ?? options?.callerQueryText,
         },
      );

      // Keyed on the RUN TARGET's own gates, never on the caller-join grafts
      // added below: a harmless caller join into any row-gated source must not
      // skip this check for a laundered run target.
      if (entryPointRowLevel.length === 0) {
         // No gate collected does not mean ungated when the request declared
         // the entry point itself. Skipped under `skipOwnSourceGate`, where
         // `entryPointGatesBySource` is the wrong file's namespace.
         if (options?.callerQueryText && !options?.skipOwnSourceGate) {
            await this.assertRequestDeclaredEntryPointIsNotLaundered(
               runnable,
               options.callerQueryText,
               givens,
               graftScope,
            );
         }
      }

      const callerJoinRowLevel = options?.callerJoins
         ? await this.probeCallerJoinGates(
              options.callerJoins.runnable,
              options.callerJoins.region,
              givens,
              graftScope,
           )
         : [];
      // A graft can only be proven on the runnable the walk read; a routed
      // serve shape is a different compile.
      if (
         callerJoinRowLevel.length > 0 &&
         options?.callerJoins?.runnable !== runnable
      ) {
         this.denyCallerJoin(
            callerJoinRowLevel[0].label,
            "routed_past_caller_join_gate",
            callerJoinRowLevel[0].graftTarget,
         );
      }
      const rowLevel = [...entryPointRowLevel, ...callerJoinRowLevel];
      const siteOf = (graft: RowLevelGraftEntry): AuthorizeGateSite =>
         graft.callerJoinPath ? "caller_join" : "entry_point";
      // Before any exit, `checkOnly` included: `/compile` must refuse what the
      // query route refuses. A grafted runnable is checked again below.
      await this.assertNoMisboundInheritedFilters(
         runnable,
         new Set(),
         rowLevel.length > 0 ? siteOf(rowLevel[0]) : undefined,
         options?.callerJoins?.region,
      );
      if (rowLevel.length === 0) return runnable;

      if (!options?.recompile) {
         // Without `recompile` the filter cannot attach, so a caller that will
         // RUN this must be denied. A `checkOnly` caller runs nothing, and the
         // query path answers a gated source with filtered rows, not a 403, so
         // denying `/compile` would only make gated sources un-authorable.
         // Admitted only when the gate is decidable without running: it reads
         // no given, or the caller supplied every given it reads. Missing
         // givens still deny, so an anonymous caller learns nothing.
         const decidable = (g: (typeof rowLevel)[number]) =>
            g.givenNames.length === 0 ||
            g.givenNames.every((name) => Object.hasOwn(givens, name));
         if (!options?.checkOnly || !rowLevel.every(decidable)) {
            recordRowLevelGateDecision("denied_by_gate", siteOf(rowLevel[0]));
            throw new AccessDeniedError(
               `Access denied for source "${rowLevel[0].label}".`,
            );
         }
         return runnable;
      }

      try {
         // `graftScope` is defined here: `row_level` requires one.
         // Deduped for the graft only; every site is still proven below.
         const grafts = Array.from(
            new Map(
               rowLevel.map((g) => [
                  `${g.graftTarget}\u0000${g.filterText}`,
                  g,
               ]),
            ).values(),
         );
         const graftedMaterializer = this.getOrBuildGraftedMaterializer(
            grafts,
            graftScope!,
         );
         await this.ensureSharedQueryModel(graftedMaterializer);
         const recompiled = options.recompile(graftedMaterializer, grafts);
         if (options.proveGraft) {
            // The own-scope binder proves the run target only.
            if (callerJoinRowLevel.length > 0) {
               throw new Error("a caller-join graft has no prover here");
            }
            await options.proveGraft(recompiled);
         } else {
            await this.assertGateLanded(recompiled, rowLevel);
         }
         // `assertGateLanded` proves only that the condition is present; a
         // `rename:`/`except:` misbind still satisfies that, because Malloy
         // resolves field references late. This proves it reads the field it
         // was written against (`./filter_binding_guard`), for caller-join
         // grafts too (located by `callerJoinPath`).
         await this.assertGraftedGatesBind(recompiled, rowLevel, graftScope!);
         // Covers the other misbindable shapes: a plain author `where:` and a
         // joined source's own filter. Grafted conditions (run target and
         // caller joins) are excluded as `alreadyProven`: this walk's
         // location-based classification cannot place their synthetic
         // compile location.
         await this.assertNoMisboundInheritedFilters(
            recompiled,
            new Set(rowLevel.map((r) => r.condition)),
            siteOf(rowLevel[0]),
            options?.callerJoins?.region,
         );
         this.rowLevelFilteredRunnables.set(recompiled, rowLevel);
         return recompiled;
      } catch (err) {
         recordRowLevelGateDecision("denied_by_gate", siteOf(rowLevel[0]));
         logger.debug("Row-level authorize attach failed; denying", {
            modelPath: this.modelPath,
            error: err instanceof Error ? err.message : String(err),
         });
         throw new AccessDeniedError(
            `Access denied for source "${rowLevel[0].label}".`,
         );
      }
   }

   /**
    * Resolve the run-target `SourceDef`/`ModelDef`, the composite member it
    * resolved to, and two signals `./filter_binding_guard` uses to identify a
    * condition parsed from THIS request's own text:
    *
    * - `compiledUrl`: the synthetic `internal://` URL of the request's text.
    *   `undefined` for a named run, whose URL is the model file's — a misbind
    *   baked into a declared query must not claim freshness by sharing it.
    * - `queryLocation`: `prepared._query.location`. Needed because `query: q2
    *   is X extend { where: … }` can leave the struct carrying X's location,
    *   so q2's own filter sits outside the struct's span.
    *
    * `undefined`s mean "no further gate to check", not deny; the own-source
    * gate stays the authoritative deny for an unresolvable target.
    */
   private async resolveRunTargetStruct(runnable: {
      getPreparedQuery(): Promise<unknown>;
   }): Promise<{
      struct: SourceDef | undefined;
      modelDef: ModelDef | undefined;
      compositeResolvedSourceDef: SourceDef | undefined;
      compiledUrl: string | undefined;
      queryLocation: DocumentLocationLike | undefined;
      pipeline: readonly StageLike[] | undefined;
   }> {
      try {
         const prepared = (await runnable.getPreparedQuery()) as {
            _query?: {
               structRef?: unknown;
               compositeResolvedSourceDef?: SourceDef;
               location?: DocumentLocationLike;
               pipeline?: readonly StageLike[];
            };
            _modelDef?: ModelDef;
         };
         const modelDef = prepared._modelDef ?? this.modelDef;
         if (!modelDef)
            return {
               struct: undefined,
               modelDef: undefined,
               compositeResolvedSourceDef: undefined,
               compiledUrl: undefined,
               queryLocation: undefined,
               pipeline: undefined,
            };
         const structRef = prepared._query?.structRef;
         const struct =
            typeof structRef === "string"
               ? modelDef.contents[structRef]
               : structRef;
         return {
            struct:
               struct && typeof struct === "object"
                  ? (struct as SourceDef)
                  : undefined,
            modelDef,
            compositeResolvedSourceDef:
               prepared._query?.compositeResolvedSourceDef,
            compiledUrl:
               typeof prepared._query?.location?.url === "string" &&
               prepared._query.location.url.startsWith("internal://")
                  ? prepared._query.location.url
                  : undefined,
            queryLocation: prepared._query?.location,
            pipeline: prepared._query?.pipeline,
         };
      } catch {
         // Not fail-open: if getPreparedQuery() throws here, execution's own
         // getPreparedResult()/run() (same compilation) throws too, so no data
         // is returned. Safety depends on execution sharing this compilation.
         return {
            struct: undefined,
            modelDef: undefined,
            compositeResolvedSourceDef: undefined,
            compiledUrl: undefined,
            queryLocation: undefined,
            pipeline: undefined,
         };
      }
   }

   /** See `./gate_classification`'s {@link collectEntryPointGates}. */
   private collectEntryPointGates(
      struct: SourceDef | undefined,
      modelDef: ModelDef | undefined,
      seen: Set<SourceDef> = new Set(),
      treatAsOwnGate = false,
      entryPointStruct: SourceDef | undefined = struct,
      excludeNotes: readonly AnnotationNote[] = [],
   ): GateEntry[] {
      return collectEntryPointGatesImpl(
         struct,
         modelDef,
         seen,
         treatAsOwnGate,
         entryPointStruct,
         excludeNotes,
      );
   }

   /**
    * This model's {@link GateClassificationDeps} (see
    * {@link gateClassificationDepsCache}).
    */
   private gateClassificationDeps(): GateClassificationDeps {
      this.gateClassificationDepsCache ??= createGateClassificationDeps(
         this.givens ?? [],
         this.modelPath,
      );
      return this.gateClassificationDepsCache;
   }

   /** See `./gate_classification`'s {@link resolveGateShape}. */
   private async resolveGateShape(
      entry: GateEntry,
      originModelDef: ModelDef,
      graftScope: GraftScope | undefined,
   ): Promise<
      | {
           shape: "row_level";
           graftTarget: string;
           filterText: string;
           condition: FilterCondition;
           givenNames: readonly string[];
        }
      | {
           shape: "lock";
           condition: FilterCondition;
           givenNames: readonly string[];
           givenNamesById: ReadonlyMap<string, string>;
        }
      | { shape: "rejected"; cause?: RowLevelGateRejectionCause }
   > {
      const result = await resolveGateShapeImpl(
         entry,
         originModelDef,
         graftScope,
         this.gateClassificationDeps(),
      );
      // Widen `authorizeReferencedGivenNames`: a source-line gate's givens are
      // only knowable post-lift, and the text scan at construction misses
      // them. Without this, omitting such a given shows Malloy's raw error
      // naming it instead of the opaque 403. Runs before the graft executes.
      if (result.shape !== "rejected") {
         for (const name of result.givenNames) {
            this.authorizeReferencedGivenNames.add(name);
         }
      }
      return result;
   }

   /** See `./gate_classification`'s {@link resolveGraftTarget}. */
   private resolveGraftTarget(
      struct: SourceDef,
      originModelDef: ModelDef,
      graftModelDef: ModelDef,
   ): string | undefined {
      return resolveGraftTargetImpl(struct, originModelDef, graftModelDef);
   }

   /**
    * Build (or reuse, see {@link graftedMaterializerCache}) the
    * `ModelMaterializer` every row-level gate on this run target grafts onto.
    */
   private getOrBuildGraftedMaterializer(
      grafts: ReadonlyArray<{
         graftTarget: string;
         filterText: string;
         condition: FilterCondition;
      }>,
      graftScope: GraftScope,
   ): ModelMaterializer {
      // Distinct separators (U+0000 within a pair, U+0001 between pairs) so
      // two different graft sets can never produce the same key and share a
      // materializer.
      const key =
         `${graftScope.cacheScope}\u0000` +
         grafts
            .map((g) => `${g.graftTarget}\u0000${g.filterText}`)
            .sort()
            .join("\u0001");
      const materializer = this.graftedMaterializerCache.get(key);
      if (materializer) {
         // Re-insert so eviction is LRU, not FIFO (which would evict the hot
         // model-wide scope).
         this.graftedMaterializerCache.delete(key);
         this.graftedMaterializerCache.set(key, materializer);
         return materializer;
      }
      const built = this.buildGraftedMaterializer(grafts, graftScope.modelDef);
      this.graftedMaterializerCache.set(key, built);
      // Evict after inserting, so the entry just built is never dropped.
      while (
         this.graftedMaterializerCache.size >
         Model.GRAFTED_MATERIALIZER_CACHE_MAX
      ) {
         const oldest = this.graftedMaterializerCache.keys().next();
         if (oldest.done) break;
         this.graftedMaterializerCache.delete(oldest.value);
      }
      return built;
   }

   /**
    * Deep-copy `modelDef`, append each grafted condition to its target's
    * `filterList`, and reload the copy through {@link gateRuntime}.
    *
    * `modelDef` itself is NEVER mutated (concurrent requests with other gate
    * sets keep compiling against it).
    *
    * Safe ONLY because it is scoped to the gates THIS request reaches (entry
    * point and caller joins). An AUTHOR join inside another source keeps its
    * own `filterList` array from model load, so it stays unfiltered (one author
    * path, a join through a named query over an imported source, is filtered —
    * the safe direction). Do not graft every gated source up front: that fires
    * gates on author joins that must not fire.
    */
   private buildGraftedMaterializer(
      grafts: ReadonlyArray<{
         graftTarget: string;
         condition: FilterCondition;
      }>,
      modelDef: ModelDef,
   ): ModelMaterializer {
      if (!this.gateRuntime) {
         throw new Error(
            "no retained runtime to graft a row-level gate through",
         );
      }
      const copy = structuredClone(modelDef);
      for (const { graftTarget, condition } of grafts) {
         const target = copy.contents[graftTarget];
         if (!target || !isSourceDef(target)) {
            throw new Error(
               `graft target "${graftTarget}" is not a source in this model`,
            );
         }
         // Spread-assign a NEW array; never `push`. Malloy shares `filterList`
         // arrays across its spread copies and `structuredClone` preserves
         // that aliasing, so pushing would land the gate on author join copies.
         target.filterList = [...(target.filterList ?? []), condition];
         this.graftIntoNamedQuerySnapshots(
            copy,
            graftTarget,
            target,
            condition,
         );
      }
      return this.gateRuntime._loadModelFromModelDef(copy);
   }

   /**
    * Append `condition` to any NAMED QUERY in `copy` whose stored run target
    * is a pre-graft snapshot of `target`.
    *
    * A model-level `query: tile is gated -> {…}` over an IMPORTED `gated`
    * stores an inlined copy of the struct as its `structRef`, which Malloy
    * reuses verbatim; grafting `contents[graftTarget]` alone never reaches it,
    * and the prover would then deny an authorized caller. This is the
    * single-query dashboard shape (`docs/dashboards.md`).
    *
    * Each limit is load-bearing:
    *  - match `sourceID` ONLY, never `referenceID`: join copies carry the
    *    base's `referenceID`, so matching it would leak the gate onto joins.
    *    The one exception is a snapshot with no `sourceID` at all, an inline
    *    `gated extend { … }`, matched by {@link resolveGraftTarget}, the same
    *    resolution the request path used to choose `graftTarget`;
    *  - APPEND to the snapshot; never repoint `structRef` at the name, which
    *    would drop the reference's `sourceArguments`;
    *  - NO recursion into `fields` (joins), the `pipeline`, or a composite's
    *    `sources[]` (composite member snapshots fail closed).
    *
    * Idempotent by `condition.code`. For a gated `query_source` the filter
    * applies inside the stored query's run target, so its aggregates are
    * caller-rows, not all-rows.
    */
   private graftIntoNamedQuerySnapshots(
      copy: ModelDef,
      graftTarget: string,
      target: SourceDef,
      condition: FilterCondition,
   ): void {
      const sourceID = target.sourceID;
      // No `sourceID` means no safe way to tell this struct from another of
      // the same name in an inheritance chain — skip rather than guess.
      if (!sourceID) return;
      for (const [key, value] of Object.entries(copy.contents)) {
         if (key === graftTarget) continue;
         const named = value as unknown as {
            type?: string;
            structRef?: unknown;
         };
         if (named?.type !== "query") continue;
         const snapshot = named.structRef;
         if (!snapshot || typeof snapshot !== "object") continue;
         const struct = snapshot as SourceDef;
         // An inline extension (`query: q is gated extend { … } -> v`) stores
         // a new struct with NO `sourceID` of its own, so the identity match
         // above never reaches it. Match it the way the request path chose
         // this graft target for that same struct (`resolveGraftTarget`),
         // so the two decisions cannot disagree. The bind guards that run
         // after the graft still prove the condition reads `graftTarget`'s
         // own column on this struct.
         if (struct.sourceID === undefined) {
            if (resolveGraftTargetImpl(struct, copy, copy) !== graftTarget)
               continue;
            // Identity, not `code`: a `where:` the query's author wrote with
            // the same text must not stand in for the gate's own condition.
            if (struct.filterList?.includes(condition)) continue;
            struct.filterList = [...(struct.filterList ?? []), condition];
            continue;
         }
         if (struct.sourceID !== sourceID) continue;
         if (struct.filterList?.some((f) => f.code === condition.code))
            continue;
         // Spread-assign, never `push` — same aliasing reason as the graft
         // site above.
         struct.filterList = [...(struct.filterList ?? []), condition];
      }
   }

   /**
    * The grafted-gate half of the filter-binding guard (see
    * `./filter_binding_guard`). Each entry's `graftTarget` names its declaring
    * struct in `graftScope.modelDef.contents`; the struct it must bind against
    * is the executed run target, or for a `callerJoinPath` entry the join
    * re-located on THIS recompile by `locateCallerJoin`.
    */
   private async assertGraftedGatesBind(
      recompiled: QueryMaterializer,
      rowLevel: ReadonlyArray<{
         graftTarget: string;
         condition: FilterCondition;
         label: string;
         callerJoinPath?: CallerJoinPath;
      }>,
      graftScope: GraftScope,
   ): Promise<void> {
      // Resolved lazily: a caller-join-only `rowLevel` never reads it.
      let executedStruct: SourceDef | undefined;
      let preparedForCallerJoins: PreparedQueryIr | undefined;
      for (const { graftTarget, condition, callerJoinPath } of rowLevel) {
         const entryPointStruct = graftScope.modelDef.contents[graftTarget];
         let site: SourceDef | undefined;
         if (callerJoinPath) {
            preparedForCallerJoins ??=
               (await recompiled.getPreparedQuery()) as PreparedQueryIr;
            const located = locateCallerJoin(
               preparedForCallerJoins,
               callerJoinPath,
            ) as SourceDef | undefined;
            // A query-source join's own struct is its output columns; bind against the inner base the graft landed on.
            site =
               located && condition.code
                  ? this.resolveFilterListLandingStruct(
                       located,
                       graftScope.modelDef,
                       condition.code,
                       0,
                    )
                  : undefined;
            if (!site) {
               throw new Error(
                  "a caller join's row-security gate could not be re-located on the recompiled query",
               );
            }
         } else {
            if (executedStruct === undefined) {
               const { struct, compositeResolvedSourceDef } =
                  await this.resolveRunTargetStruct(recompiled);
               executedStruct = compositeResolvedSourceDef ?? struct;
               // Unlike `assertNoMisboundInheritedFilters`, this IS the
               // authoritative proof for a grafted gate, so unresolvable
               // must deny rather than skip.
               if (!executedStruct) {
                  throw new Error(
                     "a row-security gate's executed source could not be resolved",
                  );
               }
            }
            site = executedStruct;
         }
         assertGraftedGateBindsToDeclaringSource(
            isSourceDef(entryPointStruct) ? entryPointStruct : undefined,
            site,
            condition,
            graftScope.modelDef,
            this.siblingModelDefResolver,
         );
      }
   }

   /**
    * Fail-closed guard against a `rename:`/`except:`/`accept:` derivation
    * freeing up a name a filter still reads by TEXT, silently rebinding it to
    * a different column. Hands the run-target struct to
    * {@link assertInheritedSourceFiltersBind}, which walks every inherited
    * `filterList` entry and every reachable joined source.
    *
    * An unresolvable struct no-ops: every caller has its own authoritative
    * deny for that case. `alreadyProven` skips grafted conditions
    * {@link assertGraftedGatesBind} already proved by note identity (their
    * synthetic `.at` defeats this walk's location-based classification).
    */
   private async assertNoMisboundInheritedFilters(
      runnable: {
         getPreparedQuery(): Promise<unknown>;
      },
      alreadyProven: ReadonlySet<FilterCondition> = new Set(),
      site: AuthorizeGateSite = "entry_point",
      callerRegion?: CallerRegion,
   ): Promise<void> {
      const {
         struct,
         modelDef,
         compositeResolvedSourceDef,
         compiledUrl,
         queryLocation,
         pipeline,
      } = await this.resolveRunTargetStruct(runnable);
      const target = compositeResolvedSourceDef ?? struct;
      if (!target) return;
      try {
         assertInheritedSourceFiltersBind(
            target,
            modelDef,
            {
               callerText:
                  callerRegion?.kind === "span"
                     ? {
                          url: callerRegion.url,
                          fromLine: callerRegion.fromLine,
                       }
                     : callerRegion && compiledUrl
                       ? { url: compiledUrl, fromLine: 0 }
                       : undefined,
               queryLocation,
               virtualUrl:
                  callerRegion?.kind === "span" ? callerRegion.url : undefined,
            },
            this.siblingModelDefResolver,
            undefined,
            undefined,
            alreadyProven,
            struct,
            pipeline,
         );
      } catch (err) {
         recordRowLevelGateDecision("denied_by_gate", site);
         logger.debug("Inherited source filter binding check failed; denying", {
            modelPath: this.modelPath,
            error: err instanceof Error ? err.message : String(err),
         });
         // The MODEL alias, never the raw struct name: on a table source
         // `name` is the physical table identifier (`duckdb:orgtable`).
         const named = struct ?? target;
         const label = (named as { as?: string }).as || named.name;
         throw new AccessDeniedError(`Access denied for source "${label}".`);
      }
   }

   /**
    * Prove every grafted condition landed on the recompiled query's run target
    * (or the caller join it was grafted for), turning a graft that silently
    * failed to attach into a REFUSAL instead of a leak. Matches `code`
    * against `filterList`, recursing into a `query_source`'s base where the
    * graft actually sits, bounded to {@link MAX_GATE_PROOF_DEPTH}; anything
    * not found is absent. Throws if any condition is absent.
    *
    * It inspects the IR, NOT the SQL: it cannot catch Malloy keeping
    * `filterList` in the IR but no longer honoring it in SQL generation. Do
    * not describe it as proof of that.
    */
   private async assertGateLanded(
      recompiled: QueryMaterializer,
      grafts: ReadonlyArray<{
         condition: FilterCondition;
         callerJoinPath?: CallerJoinPath;
      }>,
   ): Promise<void> {
      const prepared = (await recompiled.getPreparedQuery()) as {
         _query?: {
            structRef?: unknown;
            compositeResolvedSourceDef?: unknown;
         };
         _modelDef?: ModelDef;
      };
      const modelDef = prepared._modelDef ?? this.modelDef;
      // MIRROR the compiler's precedence (`compositeResolvedSourceDef ??
      // structRef`), never a union: checking the struct that does not
      // generate SQL would let unfiltered rows pass behind a green proof.
      const structRef =
         prepared._query?.compositeResolvedSourceDef ??
         prepared._query?.structRef;
      const resolvedRef =
         typeof structRef === "string"
            ? modelDef?.contents[structRef]
            : structRef;
      // A string `structRef` names a `contents` entry; an object one IS it.
      const struct =
         resolvedRef && typeof resolvedRef === "object"
            ? (resolvedRef as SourceDef)
            : undefined;
      for (const { condition, callerJoinPath } of grafts) {
         // A caller join's graft is proven on that join, found again by the
         // path the walk recorded; the run target's on the run target.
         const site = callerJoinPath
            ? (locateCallerJoin(prepared as PreparedQueryIr, callerJoinPath) as
                 | SourceDef
                 | undefined)
            : struct;
         if (
            !condition.code ||
            !this.filterListContainsCode(site, modelDef, condition.code, 0)
         ) {
            throw new Error(
               "a row-level gate condition did not land on the recompiled query",
            );
         }
      }
   }

   /**
    * The bind pair for a notebook cell that must graft against its OWN scope:
    * a `recompile` that grafts the cell's compiled queryDef, and the `prove`
    * that checks it. Returned together because one without the other is a
    * silent fail-OPEN.
    *
    * The queryDef is grafted rather than the text recompiled because the
    * grafted clone, loaded from a ModelDef, has no base URL, so a relative
    * `import` in the cell would fail to resolve. When the queryDef holds an
    * inlined run-target struct the condition is appended to it, on a deep
    * clone: `cell.runnable` is memoized, and mutating it would leak one
    * caller's filter into the next caller's query.
    */
   private ownScopeQueryDefBinder(
      queryDef: unknown,
      scope: GraftScope,
   ): {
      recompile: (
         materializer: ModelMaterializer,
         grafts: ReadonlyArray<{
            graftTarget: string;
            condition: FilterCondition;
         }>,
      ) => QueryMaterializer;
      prove: (recompiled: QueryMaterializer) => Promise<void>;
   } {
      let graftedStruct: SourceDef | undefined;
      let expectedCodes: string[] = [];
      let appliedGrafts: ReadonlyArray<{ condition: FilterCondition }> = [];
      return {
         recompile: (materializer, grafts) => {
            const clone = structuredClone(queryDef) as { structRef?: unknown };
            expectedCodes = [];
            appliedGrafts = grafts;
            graftedStruct = undefined;
            const snapshot = clone.structRef;
            if (typeof snapshot === "string") {
               // A NAME resolves against the grafted clone, which already
               // carries the condition; `assertGateLanded` stays honest.
               return (
                  materializer as HydrationMaterializer
               )._loadQueryFromQueryDef(clone);
            }
            if (!snapshot || typeof snapshot !== "object") {
               // Unprovable shape: deny rather than run ungated.
               throw new Error(
                  "own-scope run target is neither a named source nor an inlined struct",
               );
            }
            const struct = snapshot as SourceDef;
            for (const { graftTarget, condition } of grafts) {
               const target = scope.modelDef.contents[graftTarget];
               // `sourceID` identity, never name: two structs in one
               // inheritance chain share a name, and appending a condition
               // lifted in `graftTarget`'s field space to a renamed or
               // projected relative binds it to the wrong column.
               if (
                  !target ||
                  !isSourceDef(target) ||
                  !target.sourceID ||
                  struct.sourceID !== target.sourceID
               ) {
                  throw new Error(
                     `own-scope run target does not match graft target "${graftTarget}"`,
                  );
               }
               if (!struct.filterList?.some((f) => f.code === condition.code)) {
                  struct.filterList = [...(struct.filterList ?? []), condition];
               }
               expectedCodes.push(condition.code ?? "");
            }
            graftedStruct = struct;
            return (
               materializer as HydrationMaterializer
            )._loadQueryFromQueryDef(clone);
         },
         /**
          * Re-reading the condition off `graftedStruct` is VACUOUS (this binder
          * just wrote it); the load-bearing check is object IDENTITY: the
          * struct the materializer executes must be the one grafted, so a
          * `_loadQueryFromQueryDef` that copies its argument denies. A NAME
          * run target defers to {@link assertGateLanded}. SQL-level honoring
          * is pinned by `tests/component/authorize/import_hop.spec.ts`.
          */
         prove: async (recompiled) => {
            if (!graftedStruct) {
               await this.assertGateLanded(recompiled, appliedGrafts);
               return;
            }
            const prepared = (await recompiled.getPreparedQuery()) as {
               _query?: { structRef?: unknown };
            };
            if (prepared._query?.structRef !== graftedStruct) {
               throw new Error(
                  "the grafted run target is not the one the query will execute",
               );
            }
            for (const code of expectedCodes) {
               if (!graftedStruct.filterList?.some((f) => f.code === code)) {
                  throw new Error(
                     "a row-level gate condition did not land on the recompiled query",
                  );
               }
            }
         },
      };
   }

   /** Depth bound for {@link assertGateLanded}'s query-source base recursion. */
   private static readonly MAX_GATE_PROOF_DEPTH = 8;

   private filterListContainsCode(
      struct: SourceDef | undefined,
      modelDef: ModelDef | undefined,
      code: string,
      depth: number,
   ): boolean {
      return (
         this.resolveFilterListLandingStruct(struct, modelDef, code, depth) !==
         undefined
      );
   }

   /**
    * The struct in `struct`'s query-source chain whose OWN `filterList`
    * carries `code`: the landing site a grafted condition must bind against
    * (a `query_source`'s visible struct is its output columns, not the inner
    * base). `undefined` beyond {@link MAX_GATE_PROOF_DEPTH} (fail closed).
    */
   private resolveFilterListLandingStruct(
      struct: SourceDef | undefined,
      modelDef: ModelDef | undefined,
      code: string,
      depth: number,
   ): SourceDef | undefined {
      if (!struct || depth > Model.MAX_GATE_PROOF_DEPTH) return undefined;
      if (struct.filterList?.some((f) => f.code === code)) return struct;
      const duck = struct as unknown as {
         type: string;
         query?: {
            structRef?: SourceDef | string;
            compositeResolvedSourceDef?: SourceDef | string;
         };
      };
      if (duck.type === "query_source" && modelDef) {
         // Same mirrored compiler precedence as `assertGateLanded`.
         const ref =
            duck.query?.compositeResolvedSourceDef ?? duck.query?.structRef;
         const base = typeof ref === "string" ? modelDef.contents[ref] : ref;
         if (base && isSourceDef(base)) {
            return this.resolveFilterListLandingStruct(
               base,
               modelDef,
               code,
               depth + 1,
            );
         }
      }
      return undefined;
   }

   /**
    * Gate ad-hoc `/compile` text before compiling. Caller text (`append`
    * scope) has every locked name that appears anywhere in it decided, after
    * the last `run:` target and the locks its caller-written joins reach — a
    * caller-owned fragment has no legitimate need to name a base it may not
    * read. At file and package scope (`wholeFile`) the text IS the author's
    * file, so only the last `run:` target is checked, plus a derivation walk
    * from it ({@link assertLocksOnRequestDeclaredBases}); a mention that is
    * not what Malloy will run must still compile.
    *
    * Takes no bypass argument, deliberately: `/compile` returns schema and SQL,
    * and no caller needs to compile through a gate.
    */
   public async assertAuthorizedForText(
      text: string,
      givens: Record<string, GivenValue>,
      /** `wholeFile`: file or package scope, where the text is the author's file. */
      options?: { wholeFile?: boolean },
   ): Promise<void> {
      const target = extractRunTargetSourceName(text);
      await this.assertAuthorized(target, givens);
      // Text carrying an annotation is left to the forgery rejecter, whose
      // refusal is the specific one.
      if (hasCallerAuthorizeAnnotation(text)) return;
      if (options?.wholeFile) {
         // `/compile` answers WITH the compiler's diagnostics, so a lock not
         // decided first makes them readable for a source the caller is
         // refused. Text with no `run:` resolves no target and is walked anyway.
         await this.assertLocksOnRequestDeclaredBases(
            target ?? "",
            text,
            givens,
         );
         return;
      }
      // Locks only: `/compile` is exempt from the query boundary.
      const decided = new Set(target ? [target] : []);
      await this.assertCallerJoinBasesEarly(text, givens, "locks", decided);
      await this.assertLocksOnAppearingNames(text, givens, false, decided);
   }

   /**
    * Pre-decide every locked source, and every named query over one, whose
    * name appears in `text` and is not already in `decided`. See
    * {@link collectIdentifierNames} for why the scan is the union of the raw
    * and the stripped text. Parens, compose, joins and chains never have to be
    * recognized; a field that merely shares a locked name is refused too, and
    * only for a caller that lock already refuses.
    */
   private async assertLocksOnAppearingNames(
      text: string,
      givens: Record<string, GivenValue>,
      bypassAuthorize: boolean,
      decided: Set<string>,
      /** The request lifted the surface: a refusal stays the lock's 403. */
      includeHiddenFilesAndSources = false,
   ): Promise<void> {
      if (!this.declaresAnyGate()) return;
      for (const name of collectIdentifierNames(text)) {
         const asQuery = this.queries?.find((q) => q.name === name)?.sourceName;
         const source =
            (this.entryPointGatesBySource.get(name)?.length ?? 0) > 0
               ? name
               : asQuery &&
                   (this.entryPointGatesBySource.get(asQuery)?.length ?? 0) > 0
                 ? asQuery
                 : undefined;
         if (!source || decided.has(source)) continue;
         decided.add(source);
         try {
            await this.assertAuthorized(source, givens, bypassAuthorize);
         } catch (error) {
            // A 403 that names a hidden source confirms it exists. Convert on
            // the source actually gated, the same way the derivation walk does:
            // a hidden one is a 404, and a curated one keeps the 403.
            if (
               error instanceof AccessDeniedError &&
               !includeHiddenFilesAndSources
            ) {
               this.refuseGatedNameAsBoundary(source);
            }
            throw error;
         }
      }
   }

   /**
    * The `/compile` backstop once a runnable exists: the same entry-point rule
    * as the query path ({@link assertAuthorizedForAllSources}), resolved from
    * the compiled `structRef`, so named-query and multi-statement indirection
    * cannot dodge it. No bypass argument (see {@link assertAuthorizedForText}).
    */
   public async assertAuthorizedForRunnable(
      runnable: { getPreparedQuery(): Promise<unknown> },
      givens: Record<string, GivenValue>,
      /** Where the caller's appended text sits, at `/compile` append scope. */
      callerRegion?: CallerRegion,
   ): Promise<void> {
      await this.assertAuthorizedForAllSources(runnable, givens, false, {
         checkOnly: true,
         callerRegion,
      });
   }

   /**
    * Resolve the source a compiled query reads from its `structRef` — the
    * authoritative answer, immune to how a request names the query.
    * Undefined if it cannot be determined.
    */
   private async resolveAuthorizeSourceFromRunnable(runnable: {
      getPreparedQuery(): Promise<unknown>;
   }): Promise<string | undefined> {
      try {
         const prepared = (await runnable.getPreparedQuery()) as {
            _query?: { structRef?: unknown };
         };
         const structRef = prepared._query?.structRef;
         if (typeof structRef === "string") return structRef;
         if (structRef && typeof structRef === "object") {
            const s = structRef as { as?: string; name?: string };
            return s.as || s.name;
         }
      } catch {
         // Can't resolve — caller simply has no name to gate on here.
      }
      return undefined;
   }

   /**
    * Resolve the run target of an ad-hoc query to the model-defined source
    * whose `#(filter)`s apply, following derivation declarations: the filter
    * belongs to the source, not the name it is read under. Undefined when the
    * target does not derive from a protected source.
    *
    * Scans {@link stripMalloyCommentsAndLiterals} text, because a missed name is
    * served unfiltered with no post-compile backstop: in raw text a comment can
    * erase a derivation edge, and a declaration or `run:` forged inside a
    * literal or comment can replace a base or re-point the target.
    *
    * Limitation: a derivation through a named `query:` is not followed (this
    * resolves a single source name).
    */
   private resolveFilterSource(query?: string): string | undefined {
      if (!query) return undefined;
      const scanned = stripMalloyCommentsAndLiterals(query);
      const target = extractRunTargetSourceName(scanned);
      if (!target) return undefined;

      const aliasOf = buildSourceAliasMap(scanned);

      // Walk the derivation chain until we hit a protected source or run out.
      let current: string | undefined = target;
      const seen = new Set<string>();
      while (current && !seen.has(current)) {
         if (this.filterMap.has(current)) return current;
         seen.add(current);
         current = aliasOf.get(current);
      }
      return undefined;
   }

   /**
    * Span equality stands in for object identity: Malloy doesn't guarantee
    * the same struct object survives from `modelDef.contents` to a query's
    * compiled IR, so reference equality under-matches the unmodified case.
    */
   private sameDeclarationLocation(
      a: { location?: unknown },
      b: { location?: unknown },
   ): boolean {
      const la = a.location as
         | { url: string; range: { start: unknown; end: unknown } }
         | undefined;
      const lb = b.location as
         | { url: string; range: { start: unknown; end: unknown } }
         | undefined;
      if (!la || !lb) return false;
      return (
         la.url === lb.url &&
         JSON.stringify(la.range.start) === JSON.stringify(lb.range.start) &&
         JSON.stringify(la.range.end) === JSON.stringify(lb.range.end)
      );
   }

   /**
    * `!field.e` closes the `except: name; dimension: name is other` shape —
    * name equality alone can't tell that apart from a genuinely untouched column.
    */
   private fieldIsUnaliased(struct: SourceDef, name: string): boolean {
      const field = struct.fields?.find(
         (f) => ((f as { as?: string }).as ?? f.name) === name,
      );
      return !!field && field.name === name && !(field as { e?: unknown }).e;
   }

   /**
    * Fail-closed guard for `#(filter)` injection (see `./filter_binding_guard`).
    * `declaringSourceName` is the source `getFilters` resolved the definitions
    * from; `runnable` is the compiled query the injected `where:` will run
    * against, so this reads the struct Malloy will execute.
    *
    * `declaringSourceName` may be a DERIVED name: `filterMap` stores an entry
    * under every inheriting derivation, and comparing a derived struct to
    * itself is vacuous. So each filter's true declaring struct is re-resolved
    * with {@link findFilterAnnotationDeclaringSource} (also via
    * {@link siblingModelDefResolver}). Without a resolver (outside a Package),
    * the `sameDeclarationLocation`/`fieldIsUnaliased` pair covers the one case
    * of running `candidateStruct` directly; `fieldIsUnaliased` catches a
    * same-name redeclare.
    */
   private async assertFilterAnnotationsBindToDeclaringSource(
      runnable: { getPreparedQuery(): Promise<unknown> },
      declaringSourceName: string,
      filters: readonly FilterDefinition[],
   ): Promise<void> {
      try {
         const { struct, modelDef, compositeResolvedSourceDef } =
            await this.resolveRunTargetStruct(runnable);
         const executedStruct = compositeResolvedSourceDef ?? struct;
         // The runnable's own compiled `modelDef`, not `this.modelDef`: for a
         // notebook the latter can lack an imported base source and deny a
         // legitimate inheritance.
         if (!executedStruct || !modelDef) {
            throw new Error("a #(filter) run target could not be resolved");
         }
         const candidateStruct = modelDef.contents[declaringSourceName];
         if (!candidateStruct || !isSourceDef(candidateStruct)) {
            throw new Error(
               `a #(filter) declaring source "${declaringSourceName}" could not be resolved`,
            );
         }
         for (const filter of filters) {
            const declaringStruct =
               findFilterAnnotationDeclaringSource(
                  candidateStruct,
                  modelDef,
                  filter,
                  parseFilterAnnotation,
                  this.siblingModelDefResolver,
               ) ??
               (this.sameDeclarationLocation(candidateStruct, executedStruct) &&
               this.fieldIsUnaliased(candidateStruct, filter.dimension)
                  ? candidateStruct
                  : undefined);
            if (!declaringStruct) {
               throw new Error(
                  "a #(filter) dimension's declaring source could not be resolved",
               );
            }
            assertFilterDimensionBindsToDeclaringSource(
               declaringStruct,
               executedStruct,
               filter.dimension,
            );
         }
      } catch (err) {
         logger.debug("#(filter) annotation binding check failed; denying", {
            modelPath: this.modelPath,
            error: err instanceof Error ? err.message : String(err),
         });
         throw new AccessDeniedError(
            `Access denied for source "${declaringSourceName}".`,
         );
      }
   }

   /**
    * Compile a single model in-process (test fixtures and ad-hoc callers).
    * Production package loads go through the worker pool and
    * {@link Model.fromSerialized} instead.
    */
   public static async create(
      packageName: string,
      packagePath: string,
      modelPath: string,
      malloyConfig: ModelConnectionInput,
      options?: { buildManifest?: BuildManifest["entries"] },
   ): Promise<Model> {
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
         const { modelMaterializer, runnableNotebookCells } =
            await Model.getModelMaterializer(
               runtime,
               importBaseURL,
               modelURL,
               modelPath,
            );

         let modelDef = undefined;
         let sources = undefined;
         let queries = undefined;
         let filterMap: Map<string, FilterDefinition[]> | undefined;
         let givens: ApiGiven[] | undefined;
         const sourceInfos: Malloy.SourceInfo[] = [];
         if (modelMaterializer) {
            const compiledModel = await modelMaterializer.getModel();
            modelDef = compiledModel._modelDef;
            // Malloy's `Model.givens` already collapses import inheritance.
            const malloyGivens = Array.from(
               compiledModel.givens.values(),
            ) as MalloyGiven[];
            givens =
               malloyGivens.length > 0
                  ? (malloyGivens.map(malloyGivenToApi) as ApiGiven[])
                  : undefined;
            const sourceResult = extractApiSources(modelDef, givens);
            sources = sourceResult.sources;
            filterMap = sourceResult.filterMap;
            const queryResult = extractApiQueries(modelDef);
            queries = queryResult.queries;

            // A retired route marker (e.g. `#(partition)`) is enforced by
            // nothing. Checked first so it gets its own message.
            assertNoRetiredRouteMarkers(collectRetiredRouteMarkers(modelDef));

            // A misplaced `#(authorize)` (on a `query:` or a field) is
            // enforced by nothing and would fail OPEN. The checks below run
            // before `validateAuthorizeProbes` so each mistake gets its own
            // message.
            assertNoMisplacedAuthorizeAnnotations([
               ...sourceResult.misplacedAuthorize,
               ...queryResult.misplacedAuthorize,
            ]);
            // The legacy string form is refused outright.
            const legacyStringGates = findLegacyStringGates(
               sourceResult.authorizeOwnNotes,
            );
            legacyStringGates.forEach(() =>
               recordRowLevelGateRejected("legacy_string_gate"),
            );
            assertNoLegacyStringGate(legacyStringGates);
            assertAuthorizeGrammarValid(
               modelDef,
               sourceResult.authorizeMap,
               sourceResult.authorizeOwnNotes,
               computeGivenDeclaredTypes(givens),
               (_sourceName, route) => recordAuthorizeAdmitAllGate(route),
               sourceResult.attributedAuthorizeOwnNotes,
            );
            // Load-time probe compile of every entry point's gate (shared with
            // the worker), so unknown givens and fields fail at load, not on
            // first request. `const` keeps the narrowed type in the closures.
            const compiledModelDef: ModelDef = modelDef;
            await validateAuthorizeProbes(modelMaterializer, {
               authorizeMap: sourceResult.authorizeMap,
               authorizeOwnNotes: sourceResult.attributedAuthorizeOwnNotes,
               onRowLevelGateRejected: recordRowLevelGateRejected,
               // An inherited gate not expressible at this entry point: not
               // fatal (every query denies), so warn rather than lose it.
               onRowLevelGateUnexpressible: (sourceName, detail) =>
                  logger.warn(
                     "Row-level #(access_filter) gate not expressible at this entry point; every query against it will be denied",
                     { packageName, modelPath, sourceName, detail },
                  ),
               // Source-line gate given-usage checks, at every entry point
               // whose probe compiled (`sourceName` may be an inheritor).
               // `refSummary` is resolved against `contents[sourceName]`.
               onOwnRowLevelConditionCompiled: (
                  sourceName,
                  condition,
                  route,
               ) => {
                  const struct = compiledModelDef.contents[sourceName];
                  if (!struct || !isSourceDef(struct)) return;
                  validateSourceLineGateGivenUsage(
                     sourceName,
                     route,
                     struct,
                     condition.refSummary as ExpandableRefSummary | undefined,
                     condition.e,
                     compiledModelDef,
                     (cause, detail) => {
                        recordRowLevelGateRejected(cause);
                        logger.warn(`#(${route}) gate warning`, {
                           packageName,
                           modelPath,
                           sourceName,
                           cause,
                           detail,
                        });
                     },
                  );
               },
            });

            // Every source this file can resolve: its own declarations plus
            // exactly the names an `import { … }` selected (shared with the
            // worker so the paths cannot drift).
            sourceInfos.push(...collectSourceInfos(modelDef));
         }

         const model = new Model(
            packageName,
            modelPath,
            dataStyles,
            modelType,
            modelMaterializer,
            modelDef,
            sources,
            queries,
            sourceInfos.length > 0 ? sourceInfos : undefined,
            runnableNotebookCells,
            undefined,
            filterMap,
            givens,
         );
         // Cast: `_loadModelFromModelDef` is internal and undeclared on the
         // public `Runtime` type.
         model.setGateRuntime(runtime as HydrationRuntime);
         model.compiledSourceText = compiledTextFor(modelURL);
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
         return new Model(
            packageName,
            modelPath,
            dataStyles,
            modelType,
            undefined,
            undefined,
            undefined,
            undefined,
            undefined,
            undefined,
            computedError as Error,
         );
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
      _packagePath: string,
      malloyConfig: ModelConnectionInput,
      data: SerializedModel,
      options?: {
         buildManifest?: BuildManifest["entries"];
         // A package compile hydrates every model on every call, for a package
         // that already logged these warnings when it loaded.
         skipAuthorizeWarningLog?: boolean;
      },
   ): Model {
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
      for (const warning of options?.skipAuthorizeWarningLog
         ? []
         : (data.authorizeWarnings ?? [])) {
         logger.warn(warning, { packageName, modelPath: data.modelPath });
      }

      // No modelDef: an empty notebook or a corrupt payload. Queries throw a
      // clean BadRequestError downstream; markdown cells are kept so
      // `getNotebook()` can serve them.
      if (!modelDef) {
         return new Model(
            packageName,
            data.modelPath,
            dataStyles,
            data.modelType,
            undefined,
            undefined,
            sources,
            queries,
            sourceInfos,
            data.modelType === "notebook"
               ? hydrateMarkdownOnlyCells(data.notebookCells)
               : undefined,
            undefined,
            filterMap,
            givens,
            modelInfo,
         );
      }

      const runtime = makeHydrationRuntime(
         malloyConfig,
         options?.buildManifest,
      );
      const modelMaterializer = runtime._loadModelFromModelDef(modelDef);
      const runnableNotebookCells =
         data.modelType === "notebook"
            ? hydrateNotebookCells(runtime, data.notebookCells)
            : undefined;

      const model = new Model(
         packageName,
         data.modelPath,
         dataStyles,
         data.modelType,
         modelMaterializer,
         modelDef,
         sources,
         queries,
         sourceInfos,
         runnableNotebookCells,
         undefined, // compilationError
         filterMap,
         givens,
         modelInfo,
      );
      // `malloyConfig` is deliberately not captured for serve shapes: that
      // would put a storage destination in reach of the author's namespace
      // (see Package.setServeDestinationConfig). The gate runtime must be the
      // SAME one that hydrated `modelMaterializer`, so grafts share this
      // model's given identities.
      model.setGateRuntime(runtime);
      // Paired with `modelDef` above: the coordinates in that IR index this
      // text and no other revision of the file.
      model.compiledSourceText = data.modelSourceText;
      model.preparedRenderTagResults = data.renderTagResults as
         | { label: string; result?: Malloy.Result }[]
         | undefined;
      model.preparedPreaggregateCompanion = data.preaggregateCompanion as
         | { modelDef?: ModelDef }
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
      // The text that failed, when it is not the file on disk: a package
      // compile's what-if replacement. Without it, a reader of this model's
      // text judges the saved file instead of the edit.
      sourceText?: string,
   ): Model {
      const model = new Model(
         packageName,
         modelPath,
         {} as DataStyles,
         modelType,
         undefined,
         undefined,
         undefined,
         undefined,
         undefined,
         undefined,
         error,
      );
      model.compiledSourceText = sourceText;
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

   /** Set by the owning Package; see {@link curateForDiscovery}. */
   public setDiscoveryCuration(enabled: boolean): void {
      this.discoveryCurationEnabled = enabled;
   }

   /**
    * Set by the owning Package; see {@link freshnessResolver}.
    */
   public setFreshnessResolver(
      resolver: () => BuildManifest["entries"] | undefined,
   ): void {
      this.freshnessResolver = resolver;
   }

   /**
    * The freshness-filtered build manifest for this query, or undefined (no
    * override; serves live). Evaluated per call so a table that crosses its
    * window is gated on the very next query.
    */
   private resolveFreshBuildManifest(): BuildManifest | undefined {
      const entries = this.freshnessResolver?.();
      return entries ? { entries, strict: false } : undefined;
   }

   /**
    * Set by the owning Package (see Package.wireFreshnessResolvers). Supplies the
    * `sourceEntityId`s of the rollups pre-aggregation synthesized, which
    * {@link withoutPreaggregateEntries} strips from the manifest. A resolver
    * rather than a value because the build plan it reads is computed AFTER the
    * models are wired.
    */
   public setPreaggregateEntityIdResolver(
      resolver: () => ReadonlySet<string>,
   ): void {
      this.preaggregateEntityIdResolver = resolver;
   }

   /**
    * `manifest` with pre-aggregation's own rollup entries removed: only the
    * companion model sees rollups. Malloy refuses a non-empty `buildManifest`
    * against a model without `##! experimental.persistence`, which a
    * `#@ preaggregate` model need not carry, so passing rollup entries to the
    * author's model would 400 every query. Undefined when nothing survives.
    */
   private withoutPreaggregateEntries(
      manifest: BuildManifest | undefined,
   ): BuildManifest | undefined {
      if (!manifest) return undefined;
      const preaggregateIds = this.preaggregateEntityIdResolver?.();
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

   public getSources(): ApiSource[] | undefined {
      return this.curateForDiscovery(this.sources);
   }

   public getSourceInfos(): Malloy.SourceInfo[] | undefined {
      return this.curateForDiscovery(this.sourceInfos);
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

   /** Set by the owning Package; see {@link assertQueryBoundaryEarly}. */
   public setQueryBoundary(policy: {
      mode: "declared" | "all";
      exploresDeclared: boolean;
      isQueryEntryPoint: boolean;
      packageCuratedSources?: ReadonlyMap<string, ReadonlySet<string>>;
      packageCuratedQueries?: ReadonlyMap<string, ReadonlySet<string>>;
      offSurface?: OffSurfaceContext;
      layout?: ReportLayout;
      derivationText?: string;
   }): void {
      this.queryBoundary = policy;
      this.fileDerivationBases = policy.derivationText
         ? buildDerivationBaseMap(
              stripMalloyCommentsAndLiterals(policy.derivationText),
           )
         : undefined;
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
      const { mode, exploresDeclared, isQueryEntryPoint } = this.queryBoundary;
      if (mode === "all" || !exploresDeclared) return;
      if (!isQueryEntryPoint && !this.isNotebook()) {
         throw this.notQueryable(
            `No queryable model "${this.modelPath}".`,
            true,
            "model",
         );
      }
   }

   /** Set by the owning Package; see {@link siblingModelDefResolver}. */
   public setSiblingModelDefResolver(
      resolver: SiblingModelDefResolver | undefined,
   ): void {
      this.siblingModelDefResolver = resolver;
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
      if (explainable && surface && !this.hasAnyAuthorizeNote()) {
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
      const { mode, exploresDeclared } = this.queryBoundary;
      // No surface or "all": everything compiled stays queryable.
      if (mode === "all" || !exploresDeclared) return "cleared";

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
         if (sourceName && this.isCuratedSource(sourceName)) return "cleared";
         if (sourceName && this.isReportSourceOnSurface(sourceName)) {
            return "deferred";
         }
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
         if (this.isCuratedSource(sourceName)) return "cleared";
         if (this.isReportSourceOnSurface(sourceName)) return "deferred";
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
         !this.declaresAnyGate() &&
         !this.hasAnyAuthorizeNote()
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
         await this.resolveAuthorizeSourceFromRunnable(runnable),
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
            await this.resolveAuthorizeSourceFromRunnable(runnable),
            cellText,
            context,
         );
      }
   }

   /**
    * Whether the query route would refuse this request for being off the
    * surface, for the load-time dashboard lint. Runs the same two checks as
    * {@link getQueryResults}, so the lint and the query cannot disagree.
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
            ? `run: ${quoteMalloyIdentifier(request.queryName)}`
            : undefined);
      if (!text || !this.modelMaterializer) return undefined;
      let compiledSource: string | undefined;
      try {
         const runnable = this.modelMaterializer.loadRestrictedQuery(
            "\n" + text,
         );
         await runnable.getPreparedQuery();
         compiledSource =
            await this.resolveAuthorizeSourceFromRunnable(runnable);
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

   private ownCuratedSourceNames(): Set<string> {
      // A notebook or dashboard can see hidden sources through its imports, so
      // nothing it sees is curated by being visible; only the package surface
      // admits (and derivesFromCurated for its own declarations).
      if (this.isNotebook() || this.isGridReport()) return new Set();
      return new Set(
         (this.getSources() ?? [])
            .map((s) => s.name)
            .filter((n): n is string => n !== undefined),
      );
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
      const own = this.ownCuratedSourceNames();
      const packageCurated = this.queryBoundary.packageCuratedSources;
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
      // Only positive results are memoized: a name proven curated is proven
      // wherever it appears, while a `false` may be the local verdict of the
      // in-progress cycle guard rather than a property of the name.
      const proven = new Set<string>();
      const inProgress = new Set<string>();
      const proves = (current: string): boolean => {
         if (
            own.has(current) ||
            this.admittedByPackage(current, packageCurated)
         )
            return true;
         if (proven.has(current)) return true;
         // A back-edge or an over-long chain grounds nothing: deny.
         if (inProgress.has(current)) return false;
         if (inProgress.size >= REQUEST_CHAIN_MAX_NAMES) return false;
         const bases = basesOf.get(current);
         if (!bases || bases.size === 0) return false;
         inProgress.add(current);
         const ok = Array.from(bases).every((base) => proves(base));
         inProgress.delete(current);
         if (ok) proven.add(current);
         return ok;
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
         );
      }
      return this.preaggregateViolationsMemo;
   }

   /**
    * Compile this model's synthesized pre-aggregation companion and keep it for
    * the serve path, or clear it when there is nothing to synthesize.
    *
    * Called by the owning Package. Never throws: a failure leaves the serve
    * path serving live. Notebooks are skipped.
    */
   public async buildPreaggregateServeModel(
      packagePath: string,
      malloyConfig: ModelConnectionInput,
      buildManifest?: BuildManifest["entries"],
   ): Promise<void> {
      this.preaggregateServeMaterializer = undefined;
      this.preaggregateRollupPlans = [];
      // The serve shape embeds these plans but is keyed on bindings, which a
      // model edit need not change, so it must be dropped explicitly.
      this.storageServe.invalidateServeShapeCache();
      if (!this.modelDef || this.modelType !== "model") return;
      // Planned independently of the compile below, so a companion that fails
      // to compile still leaves the storage tier able to serve these rollups.
      this.preaggregateRollupPlans = planModelPreaggregation(
         this.modelDef.contents as Record<string, unknown>,
      );
      this.preaggregatePlansVersion += 1;
      // The first build after hydration loads the worker-compiled companion
      // (the manifest is read only at prepare time, so this is equivalent).
      // Taken once; later rebuilds compile here.
      const prepared = this.preparedPreaggregateCompanion;
      this.preparedPreaggregateCompanion = undefined;
      if (prepared) {
         this.preaggregateServeMaterializer = prepared.modelDef
            ? makeHydrationRuntime(
                 malloyConfig,
                 buildManifest,
              )._loadModelFromModelDef(prepared.modelDef)
            : undefined;
         return;
      }
      const synthesized = await tryCompileSynthesizedPreaggregation({
         packagePath,
         modelPath: this.modelPath,
         contents: this.modelDef.contents as Record<string, unknown>,
         getRuntime: (overlay) =>
            Model.getModelRuntime(packagePath, this.modelPath, malloyConfig, {
               buildManifest,
               overlay,
            }),
      });
      this.preaggregateServeMaterializer = synthesized?.materializer;
   }

   /**
    * The pre-aggregation companion the package-load worker compiled for this
    * model, held until {@link buildPreaggregateServeModel} reads it.
    */
   private preparedPreaggregateCompanion: { modelDef?: ModelDef } | undefined;

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

   /** See {@link StorageServeShape.setServeDestinationConfig}. */
   public setServeDestinationConfig(provider: () => MalloyConfig): void {
      this.storageServe.setServeDestinationConfig(provider);
   }

   /** See {@link StorageServeShape.invalidateServeShapeCache}. */
   public invalidateServeShapeCache(): void {
      this.storageServe.invalidateServeShapeCache();
   }

   /** See {@link StorageServeShape.setServeBindings}. */
   public setServeBindings(bindings: ServeBinding[]): void {
      this.storageServe.setServeBindings(bindings);
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
    */
   public async getQueryResults(
      ...args: Parameters<Model["runQueryResults"]>
   ): ReturnType<Model["runQueryResults"]> {
      return withSpan(
         "publisher.query",
         {
            "malloy.package": this.packageName,
            "malloy.model.path": this.modelPath,
         },
         async (span) => {
            const result = await this.runQueryResults(...args);
            if (result.servedFrom)
               span.setAttribute("publisher.served_from", result.servedFrom);
            return result;
         },
      );
   }

   private async runQueryResults(
      sourceName?: string,
      queryName?: string,
      query?: string,
      filterParams?: FilterParams,
      bypassFilters?: boolean,
      givens?: Record<string, GivenValue>,
      // Passed to `runnable.run` so a query timeout (`runWithQueryTimeout`)
      // cancels the work in flight. `undefined` means no timeout.
      abortSignal?: AbortSignal,
      /**
       * Per-query metadata inputs from the request boundary (see
       * {@link ModelQueryMetadataInput}). Omitting it still tags the query with
       * the server context; what is lost is the caller's own properties, the
       * connection default, and the correlation id.
       */
      queryMetadataInput?: ModelQueryMetadataInput,
      /**
       * Which shape the caller will send, so exactly that one is built and
       * measured: `"compact"` (the `compactJson` rows) or `"full"` (the wrapped
       * result). Every production caller passes it.
       */
      responseShape: "full" | "compact" = "full",
      /**
       * Skip `#(authorize)` gate evaluation — the private data-management path
       * (the router's `dataManagementQuery`). Disables ONLY authorize gates;
       * `where:`, `#(filter)` and {@link bypassFilters} are untouched. Never
       * settable from the ingress-exposed surface: `/private/**` is not routed
       * externally and the router pins an M2M identity before forwarding.
       */
      bypassAuthorize = false,
      /**
       * Lift the package's surface for this request (like `queryableSources:
       * "all"`), for package editors; the gateway decides who that is. Gates
       * still apply: this lifts curation, never the lock.
       */
      includeHiddenFilesAndSources = false,
   ): Promise<{
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
   }> {
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
      // Set when this query is routed through the `storage=` serve-shape
      // transform; threaded into prepare + run so the virtual sources resolve to
      // their physical tables. Undefined ⇒ served live (the default path).
      let serveVirtualMap: VirtualMap | undefined;
      // The bindings that actually PRODUCED the serve shape. Per-binding
      // decisions must read this, not the package-wide set, or a sibling
      // source's value would decide this query's behavior.
      let serveShapeBindings: ServeBinding[] = [];
      // How the answer was produced, for the query histogram. `live_fallback`
      // must NOT count as a storage hit, or the hit rate would rise while the
      // tier is down.
      let servedFrom: ServedFrom | undefined;
      // Set when the query runs against the pre-aggregation companion, the only
      // runnable that may see rollup manifest entries.
      let preaggRouted = false;
      if (!this.modelMaterializer || !this.modelDef || !this.modelInfo)
         throw new BadRequestError("Model has no queryable entities.");

      // Resolved once, before routing, so prepare, the pre-aggregation probe
      // and run all agree on the same freshness substitution.
      const buildManifest = this.resolveFreshBuildManifest();
      // Without rollup entries, for every runnable that is NOT the companion.
      const liveBuildManifest = this.withoutPreaggregateEntries(buildManifest);
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
      const readCallerJoinText =
         !!callerRegion && !hasCallerAuthorizeAnnotation(callerRegion.text);
      // Under `includeHiddenFilesAndSources` there is no boundary to check a join against.
      if (readCallerJoinText && !includeHiddenFilesAndSources) {
         await this.assertCallerJoinBasesEarly(
            callerRegion.text,
            givens ?? {},
            "boundary",
         );
      }

      // Early authorize gate from surface syntax, BEFORE compilation, so a
      // denied caller cannot learn a gated source's columns from "field not
      // found" errors. The authoritative compiled-source gate below still runs.
      const surfaceName = extractRunTargetSourceName(query);
      const earlySource =
         sourceName ||
         (queryName
            ? this.queries?.find((q) => q.name === queryName)?.sourceName
            : undefined) ||
         // A run target can be a declared QUERY (`run: locked_q + { … }`):
         // resolve it to its source, or nothing gates it early and the
         // refinement's compile errors leak the source.
         (surfaceName && !this.sources?.some((s) => s.name === surfaceName)
            ? this.queries?.find((q) => q.name === surfaceName)?.sourceName
            : undefined) ||
         surfaceName;
      if (earlySource) {
         await this.assertAuthorized(
            earlySource,
            givens ?? {},
            bypassAuthorize,
         );
      }
      // Each lock is decided (and booked) once before compile.
      const decided = new Set(earlySource ? [earlySource] : []);
      // Not under `earlySource`: a join needs no readable run target.
      if (readCallerJoinText && !bypassAuthorize) {
         await this.assertCallerJoinBasesEarly(
            callerRegion.text,
            givens ?? {},
            "locks",
            decided,
            includeHiddenFilesAndSources,
         );
      }
      // Every other locked name the text mentions, wherever it sits: the
      // pre-compile oracle guard, never the authority.
      if (query && !hasCallerAuthorizeAnnotation(query)) {
         await this.assertLocksOnAppearingNames(
            query,
            givens ?? {},
            bypassAuthorize,
            decided,
            includeHiddenFilesAndSources,
         );
      }

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
         if (!sourceName && !queryName && query) {
            queryString = "\n" + query;
         } else if (queryName && !query) {
            // NAMES, not Malloy text: quoted so each lexes as one identifier.
            // Unquoted, a newline would open a second statement (Malloy runs
            // the LAST `run:`), and getFilters() would miss the raw name and
            // inject no filter.
            queryString = `\nrun: ${
               sourceName ? `${quoteMalloyIdentifier(sourceName)} -> ` : ""
            }${quoteMalloyIdentifier(queryName)}`;
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
            throw new BadRequestError(
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
               ? this.resolveFilterSource(query)
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
         querySurfaceGivens = this.filterGivensToModelSurface(
            givens,
            await this.givenNamesReadByQuery(runnable, givens),
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
            await this.assertFilterAnnotationsBindToDeclaringSource(
               runnable,
               injectedFilterSourceName,
               injectedFilters,
            );
         }

         // Routing eligibility is decided BEFORE routing: the serve shape
         // carries no annotation bytes, so once `runnable` is swapped nothing
         // downstream can discover a gate this pre-check missed (see
         // `queryEntryPointHasRowLevelGate`). It IS the gate keeping a gated
         // query off the storage tier.
         //
         // Skipped under `bypassAuthorize` (no filter to lose; never settable
         // from ingress) and when routing is impossible anyway, since it costs
         // an entry-point walk and possibly a probe compile.
         const storageRoutingPossible =
            getPersistStorageMode() === "on" &&
            this.storageServe.serveBindings.length > 0 &&
            !!this.storageServe.serveDestinationConfig;
         // Captured once so the guard and the use below see the same companion.
         const preaggServeMaterializer = this.preaggregateServeMaterializer;
         const routingBlockedByRowLevelGate =
            (storageRoutingPossible || !!preaggServeMaterializer) &&
            !bypassAuthorize &&
            // Cheap short-circuit for gate-free models. Safe, unlike
            // `hasAuthorize()`: see `hasAnyAuthorizeNote`.
            this.hasAnyAuthorizeNote() &&
            (await this.queryEntryPointHasRowLevelGate(runnable, callerRegion));
         // Recorded once here, for the decision that blocks both tiers.
         if (routingBlockedByRowLevelGate) {
            recordStorageServeRouting("blocked_by_row_level_gate");
         }

         // storage= serve routing: try compiling against the serve-shape model
         // (materialized sources rebound to virtual sources). If it does not
         // compile, keep the original runnable and serve live.
         if (storageRoutingPossible && !routingBlockedByRowLevelGate) {
            try {
               const shaped = await this.storageServe.loadServeShapeQuery(
                  queryString,
                  querySurfaceGivens,
               );
               runnable = shaped.runnable;
               serveVirtualMap = shaped.virtualMap;
               serveShapeBindings = shaped.bindings;
               servedFrom = "storage";
               // `servedFrom` names the TIER, so a lake-served rollup is
               // "storage"; its origin rides the metric (a new enum value would
               // break generated clients).
               recordStorageServeRouting("storage", shaped.origin);
               logger.debug(
                  "Serving query from storage tier (virtual-source)",
                  {
                     modelPath: this.modelPath,
                     origin: shaped.origin,
                     // The shape's sources (stale bindings are dropped).
                     storageSources: shaped.bindings.map((b) => b.sourceName),
                  },
               );
            } catch (shapeErr) {
               recordStorageServeRouting("live_fallback");
               // info, not debug: a fallback is otherwise silent (the query
               // succeeds), so this is how an operator learns why a package
               // never serves from storage. The compiler's "undefined object"
               // reads the same for an unpersisted and a stale source, so both
               // sets are reported.
               const { shapeSources, staleSources } = serveShapeDiagnostics(
                  this.storageServe.serveBindings,
                  this.storageServe.freshServeBindings(Date.now()),
               );
               logger.info(
                  "storage serve-shape ineligible for this query; serving live",
                  {
                     modelPath: this.modelPath,
                     error:
                        shapeErr instanceof Error
                           ? shapeErr.message
                           : String(shapeErr),
                     // A name absent from both sets is not materialized at all.
                     shapeSources,
                     // Bound, but withheld by the freshness gate for this query.
                     staleSources,
                  },
               );
            }
         }

         // Pre-aggregation serve routing: compile against the synthesized model,
         // where each annotated source is a composite of its rollups and itself.
         // Malloy picks a rollup when one covers the query, so this only offers
         // the choice. Skipped when storage already routed (that runnable
         // resolves through `virtualMap`).
         //
         // Blocked for a row-level-gated entry point as defence in depth: today
         // the build gates refuse to materialize a rollup over a gated base, but
         // that refusal (`referencesAuthorize`) has partial join reach, and a
         // rollup that did build would serve rows pre-aggregated across every
         // tenant. Blocking here keeps the invariant local to this decision.
         //
         // Also skipped when EVERY rollup is storage-bound: storage rollups are
         // not companion members, so the composite degenerates to the base and
         // the probe would succeed for no gain. A mixed model keeps its
         // colocated members.
         const everyRollupIsStorageBound =
            this.preaggregateRollupPlans.length > 0 &&
            this.preaggregateRollupPlans.every((plan) => !!plan.storage);
         if (
            preaggServeMaterializer &&
            !routingBlockedByRowLevelGate &&
            !serveVirtualMap &&
            !everyRollupIsStorageBound
         ) {
            try {
               const candidate =
                  preaggServeMaterializer.loadRestrictedQuery(queryString);
               // Compile eagerly (Malloy compiles lazily), so a failure falls to
               // live here instead of escaping as a 400 at prepare/run. Probe
               // with exactly what the run uses: the build manifest and the
               // givens. A model `given:` does not cross the companion's
               // `import`, so a given-supplying query must fail HERE and serve
               // live (see preaggregation_seams.spec.ts).
               await candidate.getSQL({
                  givens: querySurfaceGivens,
                  buildManifest,
               });
               runnable = candidate;
               preaggRouted = true;
            } catch (preaggErr) {
               // Expected and common: the companion imports only the sources it
               // rolls up.
               logger.debug(
                  "query does not compile against the pre-aggregation model; serving live",
                  {
                     modelPath: this.modelPath,
                     error:
                        preaggErr instanceof Error
                           ? preaggErr.message
                           : String(preaggErr),
                  },
               );
            }
         }
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
                  servedFrom,
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
         await this.resolveAuthorizeSourceFromRunnable(runnable);

      // Boundary backstop (404) BEFORE the authorize gate (403), for "deferred"
      // admissions only (see assertQueryBoundaryEarly).
      if (boundary === "deferred") {
         this.assertQueryBoundaryCompiled(compiledSource, query);
         // The LIVE compile keeps the caller's joins; a routed one does not.
         if (callerRegion && liveRunnable) {
            await this.assertCallerJoinsQueryable(liveRunnable, callerRegion);
         }
      }

      // The AUTHORITATIVE gate, on the compiled run target (the early gate
      // cannot resolve named-query, multi-statement or caller-declared forms).
      // MUST run unconditionally and its result MUST be assigned back to
      // `runnable`: for a row-level gate the reassignment IS the enforcement.
      //
      // Do NOT guard on hasAuthorize(): it misses inherited gates. The recompile
      // stays restricted, as the original compile was.
      runnable = await this.authorizeAndBindRunnable(runnable, givens ?? {}, {
         recompile: (mm) => mm.loadRestrictedQuery(queryString),
         bypassAuthorize,
         // The caller's OWN text, not the synthesized/injected `queryString`.
         callerQueryText: query,
         callerJoins:
            callerRegion && liveRunnable
               ? { runnable: liveRunnable, region: callerRegion }
               : undefined,
      });
      if (runCount !== undefined && runCount > 1) {
         queryExecutionHistogram().record(
            performance.now() - startTime,
            this.queryMetricAttributes({
               environment: queryMetadataInput?.environment,
               queryName,
               sourceName,
               status: "error",
               servedFrom,
            }),
         );
         // An unsupplied row-level gate given denies at prepare; the count must
         // not answer first.
         throw (
            this.unboundGateDenial(
               runnable,
               givens,
               compiledSource ?? sourceName ?? "unknown",
               (this.rowLevelFilteredRunnables.get(runnable) ?? []).flatMap(
                  (graft) => graft.givenNames,
               ),
            ) ?? multipleRunStatementsError(runCount)
         );
      }
      // After the gate, so a denied caller gets its 403 rather than a 400 about
      // a value; before prepare, which is where Malloy would parse it.
      this.assertFilterGivens(givens);
      // No post-hoc gate check is possible on a routed runnable (the serve
      // shape carries no annotations): storage routing's correctness rests
      // entirely on `queryEntryPointHasRowLevelGate`.

      const maxRows = getMaxQueryRows();
      const maxBytes = getMaxResponseBytes();
      // The serve shape resolves through `virtualMap` and lacks `##!
      // experimental.persistence`, so it gets no manifest. Only the companion
      // sees the FULL manifest (rollup entries).
      const effectiveBuildManifest = serveVirtualMap
         ? undefined
         : preaggRouted
           ? buildManifest
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
      try {
         // Prepare yields the connection name, so its metadata layer resolves
         // before the statement is issued.
         run = await prepareAndRun(
            runnable,
            {
               givens: effectiveGivens,
               buildManifest: effectiveBuildManifest,
               virtualMap: serveVirtualMap,
            },
            {
               abortSignal,
               maxRows,
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
         const canDegradeToLive =
            !!serveVirtualMap &&
            !!liveRunnable &&
            !abortSignal?.aborted &&
            !String((error as { code?: string })?.code ?? "").startsWith(
               "runtime-given-",
            ) &&
            bindingsAllowDegradeToLive(serveShapeBindings);
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
                  servedFrom,
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
               ? this.unboundGateDenial(
                    runnable,
                    givens,
                    compiledSource ?? sourceName ?? "unknown",
                    this.authorizeReferencedGivenNames,
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
         if (!canDegradeToLive) failQuery(error);
         logger.warn(
            "Storage-served query failed at run time; falling back to live (freshnessFallback=live)",
            {
               modelPath: this.modelPath,
               error: error instanceof Error ? error.message : String(error),
            },
         );
         recordStorageServeRouting("runtime_live_fallback");
         try {
            // A fresh prepare re-derives the row cap from the live shape (a
            // failure at storage prepare would leave it 0, an EMPTY answer).
            // `liveBuildManifest`: this runs on the author's model.
            run = await prepareAndRun(
               liveRunnable!,
               { givens: querySurfaceGivens, buildManifest: liveBuildManifest },
               {
                  abortSignal,
                  maxRows,
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
         // NOT a storage hit.
         servedFrom = "live_fallback";
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
      if (
         this.queryHadRowLevelFilterAttached(runnable) &&
         queryResults.totalRows === 0
      ) {
         const first = this.rowLevelFilteredRunnables.get(runnable)?.[0];
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
         servedFrom,
         rowsLimit: rowLimit,
      });
      queryExecutionHistogram().record(executionTime, metricAttributes);
      // Only when reported, so the counter is absent rather than a false zero.
      const queryCostBytes = queryResults.runStats?.queryCostBytes;
      if (queryCostBytes !== undefined) {
         queryScannedBytesCounter().add(queryCostBytes, metricAttributes);
      }
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
         executionTimeMs: Math.round(executionTime),
         // Null is "not reported", not zero.
         queryCostBytes: queryCostBytes ?? null,
      };
   }

   /**
    * The label set for `queryExecutionHistogram`, shared by every recording
    * site so success and error paths cannot drift.
    *
    * Query text and row count are deliberately absent: both are unbounded
    * label values (a cardinality leak); they stay on the request log.
    * `rows_limit` is bounded only by convention (the default plus authored
    * limits).
    */
   private queryMetricAttributes(args: {
      environment?: string;
      queryName?: string;
      sourceName?: string;
      /** `rejected`: a request malformed before anything compiled, kept out of `error` so the error rate means failed queries. */
      status: "success" | "error" | "rejected";
      connection?: string;
      servedFrom?: ServedFrom;
      rowsLimit?: number;
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
         ...(args.rowsLimit === undefined
            ? {}
            : { "malloy.model.query.rows_limit": args.rowsLimit }),
         // Only for queries that reached the storage routing decision.
         ...(args.servedFrom
            ? { "malloy.model.query.served_from": args.servedFrom }
            : {}),
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
         return new Annotations(ownModelAnnotations(this.modelDef)).parseAsTag()
            .tag as ReadableTag;
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
         return new Annotations(def.annotations).parseAsTag("@")
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
      cellIndex: number,
      filterParams?: FilterParams,
      bypassFilters?: boolean,
      givens?: Record<string, GivenValue>,
      // Cancels the query on timeout (see `getQueryResults`).
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
               await this.resolveNotebookCellGraftScope(
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
               await this.probeEntryPointGates(
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
               // `getQueryResults`, over the injected filters only.
               const injectedCellFilters = cellFilters.filter((f) =>
                  filterHasValue(f, filterParams ?? {}),
               );
               if (injectedCellFilters.length > 0 && effectiveSource) {
                  // Prepare first so a compile failure surfaces as itself,
                  // not as the bind check's fail-closed 403.
                  await runnableToExecute.getPreparedQuery();
                  await this.assertFilterAnnotationsBindToDeclaringSource(
                     runnableToExecute,
                     effectiveSource,
                     injectedCellFilters,
                  );
               }
            }

            // Read before the bind below grafts gates onto the runnable.
            const cellReadGivenNames = await this.givenNamesReadByQuery(
               runnableToExecute,
               givens,
            );

            // The AUTHORITATIVE bind, on the runnable that actually executes
            // (after refinement, so the graft composes with the `#(filter)`
            // refinement instead of dropping it). Cells are author-curated, so
            // the recompile uses `loadQuery`, not `loadRestrictedQuery`.
            if (cell.modelMaterializer) {
               const textForRecompile = textToExecute;
               // Own scope: graft the compiled queryDef (read off
               // `runnableToExecute`, so the refinement survives) with its
               // paired proof; see `ownScopeQueryDefBinder`.
               const ownScopeBinder = usesOwnScope
                  ? this.ownScopeQueryDefBinder(
                       (
                          (await runnableToExecute.getPreparedQuery()) as {
                             _query: unknown;
                          }
                       )._query,
                       graftScope!,
                    )
                  : undefined;
               runnableToExecute = await this.authorizeAndBindRunnable(
                  runnableToExecute,
                  givens ?? {},
                  {
                     recompile: ownScopeBinder
                        ? ownScopeBinder.recompile
                        : (mm) => mm.loadQuery(textForRecompile),
                     proveGraft: ownScopeBinder?.prove,
                     graftScope,
                  },
               );
            } else {
               await this.assertAuthorizedForAllSources(
                  runnableToExecute,
                  givens ?? {},
               );
            }
            // After the gate, as on the query path; before prepare.
            this.assertFilterGivens(givens);

            const cellMaxRows = getMaxQueryRows();
            const cellMaxBytes = getMaxResponseBytes();
            // The freshness-filtered manifest minus rollups (a notebook has no
            // companion).
            const buildManifest = this.withoutPreaggregateEntries(
               this.resolveFreshBuildManifest(),
            );
            // See filterGivensToModelSurface.
            const cellSurfaceGivens = this.filterGivensToModelSurface(
               givens,
               cellReadGivenNames,
               this.notebookCellDeclaredGivens[cellIndex],
            );
            const cellRunnable = runnableToExecute;
            const { result } = await prepareAndRun(
               cellRunnable,
               { givens: cellSurfaceGivens, buildManifest },
               {
                  abortSignal,
                  maxRows: cellMaxRows,
                  queryMetadata: async (connectionName) => {
                     // The compiled run target, as in getQueryResults.
                     const cellCompiledSource =
                        await this.resolveAuthorizeSourceFromRunnable(
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
            // As in getQueryResults.
            if (
               result?._queryResult &&
               result.totalRows === 0 &&
               this.queryHadRowLevelFilterAttached(runnableToExecute)
            ) {
               recordRowLevelGateDecision("empty_after_filter");
            }
            // Same caps and ordering as getQueryResults.
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
            // Same error mapping as getQueryResults.
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
