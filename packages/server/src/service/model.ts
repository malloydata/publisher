// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   EMPTY_BUILD_MANIFEST,
   GivenValue,
   isSourceDef,
   MalloyConfig,
   MalloyError,
   Annotations,
   type Connection,
   ModelDef,
   ModelMaterializer,
   QueryMaterializer,
} from "@malloydata/malloy";
import * as Malloy from "@malloydata/malloy-interfaces";
import { DataStyles } from "@malloydata/render";
import { withSpan } from "../telemetry";
import {
   recordServedCopyOutcome,
   recordStoredColumnTypeMismatch,
} from "../materialization_metrics";
import type { QueryServingHost } from "./query_serving";
import { makeLegs, PersistedQueryServe } from "./materialization/serve/leg";
import {
   ModelServing,
   settledCompileError,
   type ModelQueryResults,
   type ModelServingHost,
} from "./model_serving";
import { createRequire } from "module";
import * as path from "path";
import { fileURLToPath, pathToFileURL } from "url";
import { components } from "../api";
import { getQueryMetadataMode } from "../config";
import {
   GIVENS_DOCS_URL,
   INDEX_MODEL_NAME,
   NOTEBOOK_FILE_SUFFIX,
} from "../constants";
import {
   BadRequestError,
   ModelCompilationError,
   ModelNotFoundError,
   NotQueryableError,
   OffSurfaceError,
} from "../errors";
import { getMaterializeEnabled, getPersistStorageMode } from "../config";
import { storageServes } from "./materialization/domain/placement";
import {
   renderTagFindingsCache,
   renderTagFindingsKey,
} from "./render_tag_findings_cache";
import { type RenderTagTarget, renderTagTargets } from "./render_tag_targets";
import { modelInfoOf } from "./model_info";
import { logger } from "../logger";
import { readPackageURL } from "../utils";
import { type QueryAudit } from "./access_record";
import {
   type Binding,
   EMPTY_SNAPSHOT,
   makeSnapshot,
   pin,
   type Pinned,
   type Snapshot,
} from "./materialization/domain/binding";
import { servedCopyOutcomes } from "./materialization/domain/source_decline";
import {
   currentPackageLoadPool,
   deserializeError,
} from "../package_load/package_load_pool";
import { compileServedCopyOnWorker } from "../package_load/served_copy_compile";
import type { SerializedModel } from "../package_load/protocol";
import { decodeSerializedModel } from "../package_load/protocol_decode";
import { BuildManifest } from "./materialization/domain/types";
import {
   ownModelAnnotations,
   ownModelNoteObjects,
   ownModelNotes,
} from "./annotations";
import {
   composeDeclaredQueryMetadata,
   type ReadableTag,
} from "./materialization/plan/build_plan";
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
import { FILTER_ANNOTATION_PREFIX, type FilterDefinition } from "./filter";
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
   tagText,
} from "./motly";
import { onlyParseFailures, restrictedRejections } from "./compile_restriction";
import { isMalloynbPath } from "./deprecated/malloynb";
import {
   assertNoCallerAuthorizeAnnotation,
   assertNoCallerClassificationAnnotation,
   hasCallerAuthorizeAnnotation,
} from "./authorize";
import { buildFilterClause, FilterValidationError } from "./filter";
import { compileErrorOf } from "./model_query_compile";
import {
   createEventLoopYielder,
   type EventLoopYielder,
} from "./event_loop_yield";
import {
   buildDerivationBaseMap,
   buildJoinBaseMap,
   extractRunTargetSourceNames,
   lastRunStatementText,
   malloyIdentifiers,
   stripMalloyCommentsAndLiterals,
} from "./query_text";
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
} from "./governance/query_governor";
import { type SiblingModelDefResolver } from "./filter_binding_guard";
// Aliased with an `Impl` suffix because `Model` declares private wrapper
// methods of the same names; without the alias, converting one of them to an
// arrow-function property would recurse instead of delegating.
import { extractCheckedModel, type GateLoadWarning } from "./model_load_checks";
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
   tileQueryInfo,
   type RunnableNotebookCell,
} from "./model_notebook_cell";
import { quotedIdentifier } from "./malloy_text";
import { runReadsPublished, scrubHiddenIds } from "./model_projection";
import { compileOnlyDestinationConfig } from "./connection";
import {
   modelStorageTables,
   ownBindings,
   type SourceTypeMismatches,
   STORED_COLUMN_TYPE_MISMATCH_CODE,
} from "./materialization/serve/storage_bindings";
import type { MaterializePlan } from "./materialization/domain/table_plans";
import { RollupVariantServe } from "./materialization/serve/rollup_variants";
import { recordScopeFacts } from "./materialization/serve/scope_facts";
import {
   planSourceRollups,
   rollupGateEnforcedAtRead,
} from "./materialization/plan/rollup_plan";
import {
   planModelPersistedQueries,
   planModelPersistedViews,
} from "./materialization/plan/persisted_view";
import {
   chainBuildText,
   resultShapeOf,
   ServedCopyServe,
   type ServedCopyProblem,
} from "./materialization/serve/served_copy_serve";
import type {
   ChainBuildRequest,
   ChainBuildText,
} from "./materialization/domain/chain_build";

import { PersistedViewServe } from "./materialization/serve/view_variants";
import { sourceChain } from "./materialization/text/source_map";
import type {
   PersistedResultOf,
   ViewServeVerdict,
} from "./materialization/serve/serve_verdict";

export type { ModelConnectionInput, ModelType } from "./model_runtime";
export type { ModelQueryResults } from "./model_serving";
export type { RunnableNotebookCell } from "./model_notebook_cell";

export type { ModelQueryMetadataInput } from "./query_metadata";
import {
   ModelPolicy,
   type OffSurfaceContext,
   type QueryBoundaryPolicy,
} from "./model_policy";
import { QueryPrivileges, type QueryRequest } from "./model_query_request";
import { annotationNearMisses, authoredNotes } from "./annotation_near_miss";
import {
   type ColumnContract,
   parseContract,
} from "./materialization/domain/column_contract";
import { type AnnotationFinding } from "./annotation_reader";
export { QueryPrivileges } from "./model_query_request";
export type { QueryRequest } from "./model_query_request";

type ApiCompiledModel = components["schemas"]["CompiledModel"];
type ApiNotebookCell = components["schemas"]["NotebookCell"];
type ApiReportCells = components["schemas"]["ReportCells"];
type ApiSource = components["schemas"]["Source"];
type ApiGiven = components["schemas"]["Given"];
type ApiQuery = components["schemas"]["Query"];
/**
 * Why the query route refuses a query for every caller that sends it as the
 * Console sends a `.malloynb` cell ({@link Model.consoleCellRefusal}):
 * `off_surface` is the boundary's 404; `caller_annotation` an annotation only
 * the model author may write (400); `required_filter` a required `#(filter)`
 * with no value (400); `restricted_mode` a construct restricted mode refuses
 * (400); and `compile_error` any other compile failure of the text on its own
 * (400).
 */
export interface QueryTextRefusal {
   status: 400 | 404;
   reason:
      | "off_surface"
      | "caller_annotation"
      | "required_filter"
      | "restricted_mode"
      | "compile_error";
   /** The compiler's message; on a 404, restricted mode's, when it also refused the text. */
   problem?: string;
   /** The refused source, only where the query route's refusal names it. */
   source?: string;
   /** Whether a `.malloynb`'s own cells declare {@link source}. */
   declared?: boolean;
   /** The named query the text runs, when the refused source is the one it reads. */
   via?: string;
}

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
   /**
    * The connections a stored table resolves against: this environment's
    * storage destinations, resolved on each use so a list change takes
    * effect without a reload. Deliberately NOT the config this model itself
    * compiles against, so a destination is never resolvable from the
    * author's model, a notebook cell or a query. Absent: nothing serves from
    * storage.
    */
   private serveDestinationConfig?: () => MalloyConfig;
   /**
    * Bindings this model serves in place of its package's (see
    * {@link setServeBindings}): a model driven outside a package.
    */
   private bindingsOverride:
      | { bindings: readonly Binding[]; over?: Snapshot; snapshot?: Snapshot }
      | undefined;
   /**
    * What reads and keeps this model's string capabilities
    * (`serve/source_capability.ts`), set by the package beside the served
    * copy's context; absent, {@link refreshStringCapabilities} reads none.
    */
   private stringCapabilityReader:
      | ((
           runtime: object,
           args: {
              modelDef: ModelDef;
              load: (query: string) => QueryMaterializer;
              connection: (name: string) => Promise<Connection>;
              givens?: Record<string, unknown>;
           },
        ) => Promise<void>)
      | undefined;

   /** Set what {@link refreshStringCapabilities} reads through. */
   public setStringCapabilityReader(
      reader: NonNullable<Model["stringCapabilityReader"]>,
   ): void {
      this.stringCapabilityReader = reader;
   }

   /** The contracts of the tables set directly ({@link setServeBindings}). */
   private contractsOverride: ReadonlyMap<string, ColumnContract> | undefined;
   /** {@link storageTables}, for the snapshot bindings it was read from. */
   private storageTablesMemo:
      | {
           bindings: readonly Binding[];
           modelDef: ModelDef | undefined;
           tables: ReturnType<typeof modelStorageTables>;
        }
      | undefined;
   /** Tables already reported for a stored column type mismatch. */
   private readonly typeMismatchesLogged = new Set<string>();
   /** Sources already reported as served live for a name clash. */
   private readonly shadowedSourcesLogged = new Set<string>();
   /**
    * The `#@ persist` view leg: a router candidate only when
    * `materialize.enabled` is on and a source this model sees persists a
    * view.
    */
   private persistedViews = new PersistedViewServe({
      modelPath: () => this.modelPath,
      modelDef: () => this.modelDef,
      modelMaterializer: () => this.modelMaterializer,
      serveDestinationConfig: () => this.servingDestinationConfig(),
      catalog: () => this.policy.materializedTables(),
      planModelPersistedViews,
      planModelPersistedQueries,
   });
   /**
    * The `#@ persist` named-query leg: a router candidate only when
    * `materialize.enabled` is on and this model sees a persisted query.
    */
   private persistedQueries = new PersistedQueryServe(
      {
         modelPath: () => this.modelPath,
         modelDef: () => this.modelDef,
         modelMaterializer: () => this.modelMaterializer,
         serveDestinationConfig: () => this.servingDestinationConfig(),
         planModelPersistedViews,
         planModelPersistedQueries,
      },
      this.persistedViews,
   );
   /**
    * The served copy on `virtual`: the author's own files with each stored
    * source re-rooted on its table.
    */
   private servedCopy = this.newServedCopy();
   /** What {@link setServedCopyContext} last set, for a dry-run copy. */
   private servedCopyContext:
      | Parameters<ServedCopyServe["setContext"]>
      | undefined;
   /**
    * The rollup leg: a router candidate when this model sees a rollup; a
    * `#(rollup)` one is read only with `materialize.enabled` on, a v1
    * `#@ preaggregate` one either way.
    */
   private rollupVariants = new RollupVariantServe({
      modelPath: () => this.modelPath,
      modelDef: () => this.modelDef,
      serveDestinationConfig: () => this.servingDestinationConfig(),
      catalog: () => this.policy.materializedTables(),
      // A joined source's `storage=` rollup is read through a join only from
      // a query source the served copy reads in the same destination.
      joinRollupBasis: async (pinned) =>
         this.servedCopyEnabled()
            ? this.servedCopy.joinRollupBasis(pinned)
            : undefined,
      planSourceRollups,
      rollupGateEnforcedAtRead,
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
    * The non-fatal gate findings this model's load made, which the package
    * carries as warnings (`row-level-gate-*`).
    */
   private gateLoadWarningList: readonly GateLoadWarning[] = [];
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
   /** Why the reader refused this linear report's cells; the model itself still compiled and serves. */
   private notebookReaderRefusal: NotebookReaderError | undefined;
   /** Parsed #(filter) definitions keyed by source name. */
   private filterMap: Map<string, FilterDefinition[]>;
   /** Givens declared on the model, in declaration order. */
   private givens: ApiGiven[] | undefined;
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
      /** {@link showsCellQueryInfo}'s verdict per notebook cell, which a
       *  `.malloynb` reaches through a compile of the cell's `run:`. */
      cellQueryInfoShown: WeakMap<RunnableNotebookCell, Promise<boolean>>;
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
         cellQueryInfoShown: new WeakMap(),
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
         siblingModelDefResolver: () => this.siblingModelDefResolver,
         queryBoundary: () => this.queryBoundary,
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
         const modelMaterializer = loadedModel.modelMaterializer;
         const runnableNotebookCells = loadedModel.runnableNotebookCells;
         const compiled = await modelMaterializer?.getModel();
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
               code: warning.code,
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
            gateRuntime: runtime as HydrationRuntime,
         });
         model.compiledSourceText = compiledTextFor(modelURL);
         model.gateLoadWarningList = checked?.warnings ?? [];
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

      // Non-fatal gate findings from the worker, which has no logger.
      const gateWarnings = (data.gateWarnings ?? []) as GateLoadWarning[];
      for (const warning of options.skipAuthorizeWarningLog
         ? []
         : gateWarnings) {
         logger.warn(warning.message, {
            packageName,
            modelPath: data.modelPath,
            sourceName: warning.sourceName,
            code: warning.code,
         });
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

      const runtime = makeHydrationRuntime(
         packagePath,
         malloyConfig,
         options.buildManifest,
      );
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
      model.preparedRenderTagResults = data.renderTagResults as
         | { label: string; result?: Malloy.Result }[]
         | undefined;
      model.preparedRenderTagResultsDigest = data.renderTagResultsDigest;
      model.gateLoadWarningList = gateWarnings;
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
    * `manifest` with the rollups' own entries removed, for a run that reads
    * the author's model rather than a rollup variant: the persisted-view
    * leg's variant. Malloy refuses a non-empty `buildManifest` against a model
    * without `##! experimental.persistence`, which a model declaring only
    * rollups need not carry, so a rollup entry there would 400 every query
    * (guard 3 of the plan's D3 list, kept past 6g for this reader).
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
   /** The non-fatal gate findings this model's load made (`row-level-gate-*`). */
   public gateLoadWarnings(): readonly GateLoadWarning[] {
      return this.gateLoadWarningList;
   }

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
    * The query boundary for one notebook cell, as the query route would
    * decide it. A `.malloynb` cell runs as its `runQuery` alone, the text
    * the reports read serves and the SDK sends, so that text is put through
    * the route's boundary checks: see {@link assertMalloynbRunOnSurface}.
    * Inert with no surface and under `"all"`.
    */
   private async assertNotebookCellOnSurface(cellIndex: number): Promise<void> {
      const { mode, exploresDeclared } = this.queryBoundary;
      if (mode === "all" || !exploresDeclared) return;
      const cells = this.runnableNotebookCells ?? [];
      await this.assertMalloynbRunOnSurface(malloynbRunQuery(cells[cellIndex]));
   }

   /**
    * The boundary checks the query route runs on a `.malloynb` cell's `run:`
    * sent as `query` on the notebook's path: the early check on its text, the
    * boundary for the joins it writes, then, on its restricted compile, the
    * compiled run target and those joins again. So a source the cells declare
    * is not on the surface, and neither is a run that joins a hidden source,
    * in its source or in its own `-> { … }`. A run that does not compile there
    * is refused, since the route does not run it either. The stages are
    * {@link malloynbRunStages}, which the load-time cell check shares.
    */
   private async assertMalloynbRunOnSurface(
      query: string | undefined,
   ): Promise<void> {
      const refusal = () =>
         this.notQueryable("Query target is not queryable.", true, "source");
      if (!query || !this.modelMaterializer) throw refusal();
      const outcome = await this.malloynbRunStages(
         this.modelMaterializer,
         query,
      );
      if (outcome.stage === "boundary") throw outcome.error;
      if (outcome.stage === "compile") {
         if (
            outcome.error instanceof MalloyError ||
            outcome.error instanceof ModelCompilationError
         ) {
            throw refusal();
         }
         throw outcome.error;
      }
   }

   /**
    * DEPRECATED (`.malloynb` only; deleted with the format). The query
    * route's stages for a `.malloynb` cell's `run:` sent as `query` on this
    * file, in the route's order (`ModelServing.run`), stopping at the first
    * that refuses: the boundary on the text and the joins it writes, the
    * joins skipped as the route skips them for text with an `authorize`
    * spelling when `beforeCompile` models the stage that refuses it; then
    * `beforeCompile`, the route's prepare-stage checks for a caller that wants
    * them; then the restricted compile; then, when the early check deferred,
    * the boundary on the compiled run target and those joins again. A
    * boundary refusal comes back as its error, with the compiled run target
    * when that check refused it. Shared by the reports read
    * ({@link assertMalloynbRunOnSurface}) and the load-time cell check
    * ({@link consoleCellRefusal}), so the two ask in one order. With
    * `shareQueryModel`, the compile builds the materializer's shared
    * QueryModel first ({@link ensureSharedQueryModel}), which the model then
    * keeps; the load-time check passes false, so a notebook nobody opens
    * keeps none. A compile without it still reuses one already built.
    */
   private async malloynbRunStages<T>(
      materializer: ModelMaterializer,
      query: string,
      beforeCompile?: () => T | undefined,
      shareQueryModel = true,
   ): Promise<
      | { stage: "boundary"; error: NotQueryableError; compiledSource?: string }
      | { stage: "prepare"; value: T }
      | {
           stage: "compile";
           error: unknown;
           runnable: QueryMaterializer;
           boundary: "cleared" | "deferred";
        }
      | { stage: "admitted" }
   > {
      let boundary: "cleared" | "deferred";
      try {
         boundary = this.assertQueryBoundaryEarly(undefined, undefined, query);
         // The route reads the joins of text that carries an `authorize`
         // spelling only after the prepare stage has refused it
         // (`OracleGuard.beforeQueryCompile`), so a caller modeling that stage
         // skips them here, and hears the annotation's 400 as the route does.
         if (!(beforeCompile && hasCallerAuthorizeAnnotation(query))) {
            await this.governor.assertDocumentJoinsQueryable(query);
         }
      } catch (error) {
         if (error instanceof NotQueryableError) {
            return { stage: "boundary", error };
         }
         throw error;
      }
      const prepared = beforeCompile?.();
      if (prepared !== undefined) return { stage: "prepare", value: prepared };
      if (shareQueryModel) await this.ensureSharedQueryModel(materializer);
      const runnable = materializer.loadRestrictedQuery("\n" + query);
      try {
         await runnable.getPreparedQuery();
      } catch (error) {
         return { stage: "compile", error, runnable, boundary };
      }
      if (boundary !== "deferred") return { stage: "admitted" };
      const compiledSource =
         await this.governor.resolveAuthorizeSourceFromRunnable(runnable);
      try {
         this.assertQueryBoundaryCompiled(compiledSource, query);
      } catch (error) {
         if (error instanceof NotQueryableError) {
            return { stage: "boundary", error, compiledSource };
         }
         throw error;
      }
      try {
         await this.governor.assertDocumentJoinsQueryable(query, runnable);
      } catch (error) {
         if (error instanceof NotQueryableError) {
            return { stage: "boundary", error };
         }
         throw error;
      }
      return { stage: "admitted" };
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
    * DEPRECATED (`.malloynb` only; deleted with the format). Each code cell
    * the query route refuses for every reader in the Console, sent as the
    * SDK's `Report` sends it, the `runQuery` the reports read serves
    * ({@link malloynbRunQuery}): its 1-based
    * position among the notebook's cells, markdown included, and the refusal
    * ({@link consoleCellRefusal}). Empty for any other file, and for a
    * notebook with no compiled model. Hands the event loop a turn between
    * cells through `yielder`, which a caller checking several notebooks
    * shares.
    */
   public async malloynbCellRefusals(
      yielder: EventLoopYielder = createEventLoopYielder(),
   ): Promise<({ cell: number } & QueryTextRefusal)[]> {
      if (!isMalloynbPath(this.modelPath)) return [];
      const refusals: ({ cell: number } & QueryTextRefusal)[] = [];
      let declared: ReadonlySet<string> | undefined;
      const declaredNames = () => (declared ??= this.notebookDeclaredNames());
      for (const [index, cell] of (
         this.runnableNotebookCells ?? []
      ).entries()) {
         // The Console runs code cells only (the SDK's `cellRuns`); a markdown
         // cell keeps its place in the count, and has no `runQuery`.
         const query = malloynbRunQuery(cell);
         if (query === undefined) continue;
         await yielder.maybeYield();
         const refusal = await this.consoleCellRefusal(query, declaredNames);
         if (refusal) refusals.push({ cell: index + 1, ...refusal });
      }
      return refusals;
   }

   /**
    * How the query route answers ad-hoc `query` on this file for every caller
    * that sends it as the Console sends a `.malloynb` cell: no privilege such
    * as `includeHiddenFilesAndSources`, and no `filterParams`. Undefined when
    * some such caller could get rows. It runs the route's checks that read
    * only the text and the model, in the route's order (`ModelServing.run`),
    * through the stages the reports read shares ({@link malloynbRunStages}):
    * the boundary on the text and its joins (404); the caller-annotation
    * guards (400); a required `#(filter)` left without a value (400); the
    * restricted compile, settled by the route's own
    * {@link settledCompileError} (400, or the boundary's 404); then the
    * boundary on the compiled run target and its joins (404). The checks that
    * read a caller's givens (`#(authorize)`, row-level gates) are left out:
    * their answer is the caller's. Undefined, too, for a file with no
    * compiled model. `declaredNames` gives the sources the notebook's cells
    * declare, which a caller checking every cell builds once.
    */
   public async consoleCellRefusal(
      query: string,
      declaredNames: () => ReadonlySet<string> = () =>
         this.notebookDeclaredNames(),
   ): Promise<QueryTextRefusal | undefined> {
      const materializer = this.modelMaterializer;
      if (this.compilationError || !materializer || !this.modelDef) {
         return undefined;
      }
      const outcome = await this.malloynbRunStages(
         materializer,
         query,
         () => this.preparedRefusal(query),
         false,
      );
      if (outcome.stage === "boundary") {
         return this.offSurfaceRefusal(
            outcome.error,
            query,
            declaredNames,
            outcome.compiledSource,
         );
      }
      if (outcome.stage === "prepare") return outcome.value;
      if (outcome.stage === "admitted") return undefined;
      const settled = await settledCompileError(
         outcome.runnable,
         query,
         outcome.boundary,
         (text, compileError) =>
            this.unqueryableTextRefusal(text, compileError),
      );
      if (!settled) {
         // An unbound given is the run path's, and so the reader's. Anything
         // that is not a compile failure at all is not this check's to judge.
         if (await compileErrorOf(outcome.runnable)) return undefined;
         throw outcome.error;
      }
      const errors = settled.compileError.problems.filter(
         (problem) => problem.severity === "error",
      );
      const restricted = restrictedRejections(errors);
      if (settled.offSurface) {
         return {
            ...this.offSurfaceRefusal(settled.refusal, query, declaredNames),
            ...(restricted.length > 0
               ? { problem: restricted[0].message }
               : {}),
         };
      }
      return {
         status: 400,
         reason: restricted.length > 0 ? "restricted_mode" : "compile_error",
         problem:
            (restricted[0] ?? errors[0])?.message ??
            firstLine(settled.compileError.message),
      };
   }

   /**
    * `ModelServing.prepare`'s refusals of `query` before it compiles: the
    * guards `assertNoCallerAnnotations` runs on query text, then `#(filter)`
    * injection with no `filterParams`, as the Console sends a cell.
    */
   private preparedRefusal(query: string): QueryTextRefusal | undefined {
      try {
         assertNoCallerAuthorizeAnnotation(query);
         assertNoCallerClassificationAnnotation(query);
      } catch (error) {
         if (!(error instanceof BadRequestError)) throw error;
         return {
            status: 400,
            reason: "caller_annotation",
            problem: firstSentence(error.message),
         };
      }
      const filterSource = this.governor.resolveFilterSource(query);
      if (!filterSource) return undefined;
      try {
         buildFilterClause(this.getFilters(filterSource), {});
      } catch (error) {
         if (!(error instanceof FilterValidationError)) throw error;
         return {
            status: 400,
            reason: "required_filter",
            problem: error.message,
         };
      }
      return undefined;
   }

   /** Every source a `source:` statement in one of the notebook's code cells declares. */
   private notebookDeclaredNames(): ReadonlySet<string> {
      const names = new Set<string>();
      for (const cell of this.runnableNotebookCells ?? []) {
         if (cell.type !== "code") continue;
         for (const name of buildDerivationBaseMap(cell.text).keys()) {
            names.add(name);
         }
      }
      return names;
   }

   /**
    * A boundary refusal as a {@link QueryTextRefusal}, naming the source only
    * where the query route's own refusal does (an {@link OffSurfaceError}),
    * so a gated model's 404 gives nothing away here either. `compiledSource`
    * is the run target the compiled check refused, which a named query
    * reaches through its own name: then that query is `via`. Rethrows
    * anything else.
    */
   private offSurfaceRefusal(
      error: unknown,
      query: string,
      declaredNames: () => ReadonlySet<string>,
      compiledSource?: string,
   ): QueryTextRefusal {
      if (!(error instanceof NotQueryableError)) throw error;
      if (!(error instanceof OffSurfaceError)) {
         return { status: 404, reason: "off_surface" };
      }
      const named = extractRunTargetSourceNames(query).find(
         (name) =>
            !this.isCuratedSource(name) &&
            !this.derivesFromCurated(name, query),
      );
      const source = compiledSource ?? named;
      if (source === undefined) return { status: 404, reason: "off_surface" };
      const declared = declaredNames().has(source);
      const target = extractRunTargetSourceNames(query)[0];
      const via =
         compiledSource !== undefined &&
         target !== undefined &&
         target !== compiledSource &&
         !this.declaresSource(target)
            ? target
            : undefined;
      return {
         status: 404,
         reason: "off_surface",
         source,
         declared,
         ...(via ? { via } : {}),
      };
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

   /** A `.malloynb`, or a linear `.malloy` report. */
   public isNotebook(): boolean {
      return (
         this.modelPath.endsWith(NOTEBOOK_FILE_SUFFIX) ||
         this.queryBoundary.layout === "linear"
      );
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
      // The worker prepares targets in order up to its soft deadline; the rest
      // are prepared here. Taken once.
      const fromWorker = this.preparedRenderTagResults ?? [];
      this.preparedRenderTagResults = undefined;
      const digest = this.preparedRenderTagResultsDigest;
      this.preparedRenderTagResultsDigest = undefined;
      const targets = renderTagTargets(this.queries, this.sources);
      // Only a digest of every target's result says what the findings are.
      const cacheKey =
         digest !== undefined && fromWorker.length === targets.length
            ? renderTagFindingsKey(digest)
            : undefined;
      if (cacheKey !== undefined) {
         const cached = renderTagFindingsCache.get(cacheKey);
         if (cached !== undefined) return cached;
      }
      // Dynamic import: the validator is heavy, so load it only when needed.
      const { validateRenderTags } = await import(
         "@malloydata/render-validator"
      );

      const remaining = targets.slice(fromWorker.length);
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
      if (cacheKey !== undefined) {
         renderTagFindingsCache.set(cacheKey, findings);
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

   /** SerializedModel.renderTagResultsDigest, taken with the results. */
   private preparedRenderTagResultsDigest: string | undefined;

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
    * A linear report's listing: `title` in the artifact tag, then the first
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
    * Attach the cells of a linear report, which is only known to be one after
    * discovery.
    */
   public setNotebookCells(cells: RunnableNotebookCell[]): void {
      this.runnableNotebookCells = cells;
   }

   /**
    * Read a linear report's cells out of `text`, the text this model compiled,
    * and attach them. A reader refusal becomes this notebook's error and leaves
    * it with no cells; the model itself keeps serving. `ok` when its cells were
    * read, `refused` when the reader refused them, `broken` when it did not
    * compile and so has none.
    */
   public attachServedNotebookCells(text: string): "ok" | "refused" | "broken" {
      this.notebookReaderRefusal = undefined;
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
      const layout = this.readNotebookLayout();
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

   /** Why the reader refused this linear report's cells, if it did. */
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

   /**
    * A `.malloynb` read as cells, for the reports read. Anything else is
    * refused: a `.malloy` report's tiles are in its manifest.
    */
   public async getNotebook(): Promise<ApiReportCells> {
      if (!this.modelPath.endsWith(NOTEBOOK_FILE_SUFFIX)) {
         throw new ModelNotFoundError(
            `${this.modelPath} is not a .malloynb. Only a .malloynb is read as cells; a .malloy report's tiles are in its manifest.`,
         );
      }
      if (this.compilationError) {
         throw this.compilationError;
      }
      return this.getNotebookModel();
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
         // Database tables (on the source's own connection) need no
         // destination.
         serveDestinationConfig: () => this.servingDestinationConfig(),
         pinned: () => this.pinNow(),
         // Off this thread when a load worker is free; inline otherwise.
         compileElsewhere: (args) =>
            compileServedCopyOnWorker(currentPackageLoadPool(), args),
      });
   }

   /**
    * Every table this model serves now: its package's snapshot, or the
    * bindings set on it directly ({@link setServeBindings}) beside the
    * package's database entries. Replaced by reference, never edited.
    */
   private servingSnapshot(): Snapshot {
      const pkg = this.policy.snapshot() ?? EMPTY_SNAPSHOT;
      const override = this.bindingsOverride;
      if (!override) return pkg;
      if (override.over !== pkg || !override.snapshot) {
         override.over = pkg;
         // Each table's contract: the package's, the database entries',
         // and those set beside the bindings.
         const contracts = new Map(pkg.contracts ?? []);
         for (const [id, entry] of Object.entries(pkg.entries)) {
            const contract = parseContract(entry.contract);
            if (contract && !contracts.has(id)) contracts.set(id, contract);
         }
         for (const [id, c] of this.contractsOverride ?? []) {
            contracts.set(id, c);
         }
         override.snapshot = makeSnapshot(
            override.bindings,
            pkg.entries,
            undefined,
            undefined,
            contracts,
         );
      }
      return override.snapshot;
   }

   /** This model's tables pinned now, for a caller outside a query. */
   private pinNow(): Pinned {
      return pin(this.servingSnapshot(), new Date());
   }

   /**
    * The storage destinations a leg may read through: none unless the
    * storage tier serves (`PERSIST_STORAGE_MODE=on`). Every leg (served
    * copy, rollup, persisted view, persisted query) reads it here, so under
    * `write-only` and `off` none answers from a storage table.
    */
   private servingDestinationConfig(): (() => MalloyConfig) | undefined {
      return storageServes(getPersistStorageMode())
         ? this.serveDestinationConfig
         : undefined;
   }

   /** Set the storage destinations, pushed by the owning package. */
   public setServeDestinationConfig(provider: () => MalloyConfig): void {
      // The same provider again changes nothing: it is read live, and a
      // destination list swap invalidates explicitly (invalidateCompiledLegs).
      if (provider === this.serveDestinationConfig) return;
      // Replaced, not first set (a reload's new model): what failed over the
      // old destinations may compile over these.
      if (this.serveDestinationConfig) this.servedCopy.forgetFailures();
      this.serveDestinationConfig = provider;
      this.servedCopy.invalidate();
      this.rollupVariants.invalidate();
      this.persistedViews.invalidate();
      this.persistedQueries.invalidate();
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
      // The same context again (a rebind, a republish) keeps the served
      // copy: its compile is keyed on its bindings, and this model's own text
      // never changes. The other legs are reset as before, since their caches
      // are not keyed on the database entries a bind can change alone.
      const current = this.servedCopyContext;
      const unchanged =
         !!current &&
         current[0] === packagePath &&
         current[1] === malloyConfig &&
         current[2] === materializePlans &&
         current[3] === declarationOf;
      this.servedCopyContext = [
         packagePath,
         malloyConfig,
         materializePlans,
         declarationOf,
      ];
      if (!unchanged) this.servedCopy.setContext(...this.servedCopyContext);
      this.persistedViews.setContext(packagePath, malloyConfig);
      this.persistedQueries.setContext(packagePath, malloyConfig);
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
    * Whether the router may try the persisted-query leg: as for views, with
    * a persisted named query this model sees.
    */
   private persistedQueriesEnabled(): boolean {
      return (
         this.modelType === "model" &&
         !this.isNotebook() &&
         getMaterializeEnabled() &&
         this.persistedQueries.hasQueries()
      );
   }

   /**
    * The persisted views' generated build sources this model can see, by
    * `sourceID`, with the source and view each stores, and the persisted
    * named queries' with the query (and its run target): the package's serve
    * verdict reads a view's reach through its source.
    */
   public persistedViewBuilds(): (PersistedResultOf & {
      buildSourceID: string;
   })[] {
      if (!getMaterializeEnabled()) return [];
      return [
         ...this.persistedViews.plans().map((p) => ({
            buildSourceID: p.buildSourceID,
            sourceID: p.sourceID,
            sourceName: p.sourceName,
            viewName: p.viewName,
         })),
         ...this.persistedQueries.plans().map((p) => ({
            buildSourceID: p.buildSourceID,
            queryName: p.queryName,
            ...(p.sourceID !== undefined ? { sourceID: p.sourceID } : {}),
            ...(p.sourceName !== undefined ? { sourceName: p.sourceName } : {}),
         })),
      ];
   }

   /**
    * Whether the router may try the served copy: a model file. It reads a
    * database table on the source's own connection, and a storage table
    * only while `PERSIST_STORAGE_MODE=on` (its host withholds the
    * destinations otherwise). Warnings and the compile at bind follow the
    * same rule, so a package never warns about a copy that is not routed.
    */
   private servedCopyEnabled(): boolean {
      // A notebook or report is not a model file the served copy can load.
      return this.modelType === "model" && !this.isNotebook();
   }

   /**
    * Whether the router may try the `#(rollup)` rollup leg: a model file
    * and a rollup declared on a source it sees.
    */
   private rollupVariantsEnabled(): boolean {
      return (
         this.modelType === "model" &&
         !this.isNotebook() &&
         // v1 `#@ preaggregate` rollups are read here too.
         this.rollupVariants.hasRollups()
      );
   }

   /**
    * Read this model's tables now, so their warnings exist before any query:
    * the stored columns' types, and, when it is enabled, the served copy's
    * compile.
    */
   public async prepareServedCopy(): Promise<void> {
      await this.refreshStringCapabilities();
      await this.prepareServedCopyOver(this.servingSnapshot());
   }

   /**
    * Read how each of this model's sources compares its strings live, and
    * keep it with the model's runtime, for read admission's lookups
    * (`serve/source_capability.ts`). Every bind commit calls it, so a
    * collation altered in the warehouse is seen at the next bind. Never
    * throws; a model with no compile reads nothing.
    */
   public async refreshStringCapabilities(): Promise<void> {
      const refresh = this.stringCapabilityReader;
      const materializer = this.modelMaterializer;
      const load =
         this.modelType === "model" && !this.isNotebook() && materializer
            ? (query: string) => materializer.loadQuery(query)
            : undefined;
      const modelDef = this.modelDef;
      const runtime = (
         this.modelMaterializer as unknown as
            | {
                 runtime?: {
                    connections: {
                       lookupConnection(name?: string): Promise<Connection>;
                    };
                 };
              }
            | undefined
      )?.runtime;
      if (!refresh || !load || !modelDef || !runtime) return;
      try {
         await refresh(runtime, {
            modelDef,
            load,
            connection: (name) => runtime.connections.lookupConnection(name),
            givens: placeholderGivens(this.givens),
         });
      } catch (err) {
         logger.warn(
            "Could not read how this model's sources compare strings",
            {
               modelPath: this.modelPath,
               error: err instanceof Error ? err.message : String(err),
            },
         );
      }
   }

   /**
    * Record, for read admission's lookups, what each named source's gates
    * and row scope read over the tables `snapshot` binds, and its gate and
    * composite flags (`serve/scope_facts.ts`): every bind commit, so a
    * query pinned to `snapshot` walks none of it. The served copy's lane is
    * the one compiled last, over the same bindings when its compile ran.
    */
   public recordScopeFactsOver(snapshot: Snapshot): void {
      const lane = this.servedCopyEnabled()
         ? this.servedCopy.scopeLane()
         : undefined;
      const runtime = (
         this.modelMaterializer as unknown as { runtime?: object } | undefined
      )?.runtime;
      const modelDef = this.modelDef;
      if (!runtime || !modelDef || this.modelType !== "model") return;
      try {
         recordScopeFacts(runtime, {
            modelDef,
            snapshot,
            ...(lane ? { lane } : {}),
         });
      } catch (err) {
         logger.warn("Could not record this model's scope facts", {
            modelPath: this.modelPath,
            error: err instanceof Error ? err.message : String(err),
         });
      }
   }

   /**
    * {@link prepareServedCopy} over `snapshot`, the tables a bind is about
    * to swap in (`PackageMaterialization.bindCompiled`): the copy is then
    * current the moment the swap makes `snapshot` the one this model reads.
    */
   public async prepareServedCopyOver(snapshot: Snapshot): Promise<void> {
      const pinned = pin(snapshot, new Date());
      this.storageTables(pinned.snapshot);
      if (!this.servedCopyEnabled()) {
         this.recordScopeFactsOver(snapshot);
         return;
      }
      await this.servedCopy.prepare(pinned);
      this.recordScopeFactsOver(snapshot);
      // Once per source per model per snapshot it compiles (a load or a
      // rebind): each source the copy does not serve whole, `declined`
      // beating `partial`, and `partial` only while it is still re-rooted.
      for (const { outcome, reason } of servedCopyOutcomes(
         // A storage table dropped for its database twin serves the source.
         this.servedCopy.problems().filter((p) => !p.servedFromDatabase),
         this.servedCopy.rerootedSourceIDs() ?? new Set(),
      )) {
         recordServedCopyOutcome(outcome, reason);
      }
   }

   /**
    * The sources this model's served copy answers from storage, by author
    * `sourceID`: undefined when the served copy is off for this model, and
    * `pending` until it has compiled over the bindings bound now. A copy
    * compiled over a previous bind's bindings is pending too, so a verdict
    * read between a bind and its compile never judges the new tables by it.
    */
   public servedCopySourceIDs(): ReadonlySet<string> | "pending" | undefined {
      if (!this.servedCopyEnabled()) return undefined;
      const ids = this.servedCopy.rerootedSourceIDs();
      if (ids && this.servedCopy.isCurrent()) return ids;
      // Nothing else may compile it: a freshness window that lapses changes
      // the bindings a query reads with no bind, and a model nobody queries
      // would stay pending until the next bind.
      this.refreshServedCopy();
      return "pending";
   }

   /** The compile {@link refreshServedCopy} started, while it runs. */
   private servedCopyRefresh?: Promise<void>;

   /**
    * Compile the served copy over the bindings fresh now, in the background,
    * once at a time: a status poll that finds it pending asks again only
    * after the compile it started has landed (and then finds it current).
    */
   private refreshServedCopy(): void {
      if (this.servedCopyRefresh) return;
      const refresh = this.prepareServedCopy()
         .catch(() => {
            // The compile records its own failure; nothing escapes a poll.
         })
         .finally(() => {
            if (this.servedCopyRefresh === refresh) {
               this.servedCopyRefresh = undefined;
            }
         });
      this.servedCopyRefresh = refresh;
   }

   /**
    * The sources this model's served copy re-rooted on a database table and
    * on no storage table, by author `sourceID`: undefined when the served
    * copy is off for this model or has yet to compile.
    */
   public servedCopyDatabaseOnlyIDs(): ReadonlySet<string> | undefined {
      if (!this.servedCopyEnabled()) return undefined;
      return this.servedCopy.rerootedOnDatabaseOnly();
   }

   /**
    * The sources this model's served copy leaves as written because they
    * extend a source it re-rooted, by author `sourceID` (empty when off or
    * not yet compiled).
    */
   public servedCopyThroughBaseIDs(): ReadonlySet<string> {
      if (!this.servedCopyEnabled()) return new Set();
      return this.servedCopy.servedThroughBaseSourceIDs();
   }

   /**
    * Why this model's served copy leaves sources unserved: empty when off,
    * and withheld while the copy is pending ({@link servedCopySourceIDs}),
    * so a package's warnings never carry a previous bind's problems while
    * its verdicts say pending.
    */
   public servedCopyProblems(): readonly ServedCopyProblem[] {
      if (!this.servedCopyEnabled()) return [];
      if (!this.servedCopy.isCurrent()) {
         this.refreshServedCopy();
         return [];
      }
      return this.servedCopy.problems();
   }

   /** Drop every compiled leg (the destinations or the model changed). */
   public invalidateCompiledLegs(): void {
      this.servedCopy.forgetFailures();
      this.servedCopy.invalidate();
      this.rollupVariants.invalidate();
      this.persistedViews.invalidate();
      this.persistedQueries.invalidate();
   }

   /**
    * Serve this model from `bindings` in place of its package's (the
    * package's database entries still apply): a model driven outside a
    * package, as a test or a tool drives one. A copy is taken, so a caller
    * that edits a binding and sets it again compiles over the edit.
    */
   public setServeBindings(
      bindings: readonly Binding[],
      /** Their tables' column contracts, by content address; kept when absent. */
      contracts?: ReadonlyMap<string, ColumnContract>,
   ): void {
      this.bindingsOverride = { bindings: Object.freeze([...bindings]) };
      if (contracts) this.contractsOverride = contracts;
   }

   /**
    * This model's own `#@ persist` `storage=` tables in `snapshot`, read once
    * per snapshot: a binding that stores a same-named source declared
    * elsewhere is dropped (that source serves live here,
    * malloydata/publisher#1001), and the stored columns whose type does not
    * serve as declared are found, each logged once per table per model.
    */
   private storageTables(
      snapshot: Snapshot = this.servingSnapshot(),
   ): ReturnType<typeof modelStorageTables> {
      const memo = this.storageTablesMemo;
      if (
         memo &&
         memo.bindings === snapshot.bindings &&
         memo.modelDef === this.modelDef
      ) {
         return memo.tables;
      }
      const tables = modelStorageTables(
         snapshot.bindings,
         this.modelDef,
         this.servedCopyContext?.[0],
      );
      this.storageTablesMemo = {
         bindings: snapshot.bindings,
         modelDef: this.modelDef,
         tables,
      };
      if (tables.checkFailed !== undefined) {
         // Reporting only: the served copy decides each column on its own.
         logger.debug("Could not check stored column types", {
            modelPath: this.modelPath,
            error: tables.checkFailed,
         });
      }
      for (const source of tables.shadowed) {
         if (this.shadowedSourcesLogged.has(source)) continue;
         this.shadowedSourcesLogged.add(source);
         logger.info(
            "Serving a source live: its storage table materializes a same-named source declared elsewhere",
            { modelPath: this.modelPath, source },
         );
      }
      for (const finding of tables.findings) {
         const key = `${finding.binding.sourceName}\u0000${finding.binding.virtualHandle}`;
         if (this.typeMismatchesLogged.has(key)) continue;
         this.typeMismatchesLogged.add(key);
         for (const m of finding.mismatches) {
            recordStoredColumnTypeMismatch("serve", m.declaredType);
         }
         logger.warn(
            "Stored column types do not match the declared Malloy types; casting where faithful, serving the rest live",
            {
               code: STORED_COLUMN_TYPE_MISMATCH_CODE,
               modelPath: this.modelPath,
               source: finding.binding.sourceName,
               cast: finding.casts,
               live: finding.unservable,
            },
         );
      }
      return tables;
   }

   /** The stored columns of this model's tables that do not serve as declared. */
   public storedColumnTypeMismatches(): readonly SourceTypeMismatches[] {
      return this.storageTables().mismatches;
   }

   /**
    * The source names this model serves a `storage=` table for: its own
    * `#@ persist` sources' (see {@link storageTables}) and its persisted
    * views'.
    */
   public servedStorageSourceNames(): string[] {
      const pinned = this.pinNow();
      return [
         ...this.storageTables(pinned.snapshot).kept,
         ...this.persistedViews.viewBindings(pinned, "bound"),
         ...this.persistedQueries.queryBindings(pinned, "bound"),
      ].map((b) => b.sourceName);
   }

   /**
    * The text a chained `storage=` build of a downstream this model declares
    * compiles from, with each stored upstream the request carries re-rooted
    * as this model's served copy re-roots it (`chainBuildText` in
    * serve/served_copy_serve.ts).
    * Undefined when this model has no served copy or does not declare the
    * downstream.
    */
   public chainBuildText(
      request: ChainBuildRequest,
   ): ChainBuildText | undefined {
      if (
         this.modelType !== "model" ||
         this.isNotebook() ||
         !this.modelDef ||
         !this.servedCopyContext
      ) {
         return undefined;
      }
      return chainBuildText(request, {
         modelPath: this.modelPath,
         modelDef: this.modelDef,
         context: this.servedCopyContext,
      });
   }

   /**
    * The v1 cutover's preview (dry run): the sources this model's served copy
    * would re-root over `bindings` (declared from the plan's output columns,
    * so no table is read), and why it leaves the others as written. Compiled
    * in a served copy of its own, with the cutover's per-column types, and
    * discarded. Undefined when this model has no served copy to compile.
    */
   public async previewServedCopy(bindings: Binding[]): Promise<
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
      const destinations = this.serveDestinationConfig;
      // Declared from the plan, never read, so no window applies.
      const pinned = pin(makeSnapshot(bindings), new Date(0));
      const copy = new ServedCopyServe({
         modelPath: () => this.modelPath,
         modelDef: () => this.modelDef,
         // Compile-only, as the view preview: a destination's catalog need
         // not exist before the first build.
         serveDestinationConfig: () =>
            destinations && storageServes(getPersistStorageMode())
               ? () => compileOnlyDestinationConfig(destinations())
               : undefined,
         pinned: () => pinned,
      });
      copy.setContext(...this.servedCopyContext);
      await copy.prepare();
      return {
         rerooted: copy.rerootedSourceIDs() ?? new Set<string>(),
         throughBase: copy.servedThroughBaseSourceIDs(),
         gateDeclined: copy.gateDeclinedSourceIDs(),
         problems: copy.problems(),
      };
   }

   public async previewStorageViews(
      bindings: Binding[],
   ): Promise<Map<string, ViewServeVerdict[]>> {
      const out = new Map<string, ViewServeVerdict[]>();
      const destinations = this.serveDestinationConfig;
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
      const stored = ownBindings(
         bindings.filter((b) => b.kind !== "rollup"),
         this.modelDef,
         this.servedCopyContext[0],
      ).kept;
      if (stored.length === 0) return out;
      const pinned = pin(makeSnapshot(stored), new Date(0));
      // A throwaway served copy over these bindings, compile-only: before the
      // first build a DuckLake destination's catalog does not exist yet, and
      // the dialect must not depend on reaching it.
      const copy = new ServedCopyServe({
         modelPath: () => this.modelPath,
         modelDef: () => this.modelDef,
         quiet: true,
         serveDestinationConfig: () => () =>
            compileOnlyDestinationConfig(destinations()),
         pinned: () => pinned,
      });
      copy.setContext(...this.servedCopyContext);
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
            const result = await this.serving.run(audit, request, privileges);
            if (result.servedFrom)
               span.setAttribute("publisher.served_from", result.servedFrom);
            return result;
         },
      );
   }

   /** This model's query pipeline (model_serving.ts), made on first use. */
   private get serving(): ModelServing {
      this.servingMemo ??= new ModelServing(this.servingPort());
      return this.servingMemo;
   }
   private servingMemo: ModelServing | undefined;

   /** What the query pipeline reads from this model; each read is live. */
   private servingPort(): ModelServingHost {
      return {
         packageName: this.packageName,
         modelPath: this.modelPath,
         governor: this.governor,
         compilationError: () => this.compilationError,
         modelMaterializer: () => this.modelMaterializer,
         modelDef: () => this.modelDef,
         modelInfo: () => this.modelInfo,
         dataStyles: () => this.dataStyles,
         sources: () => this.sources,
         givens: () => this.givens,
         snapshot: () => this.servingSnapshot(),
         reportIncompatible: (handles) =>
            this.policy.reportIncompatible?.(handles),
         servedCopy: () => this.servedCopy,
         legs: (pinned, servedCopy) =>
            makeLegs(pinned, {
               ...(this.servedCopyEnabled() ? { servedCopy } : {}),
               ...(this.rollupVariantsEnabled()
                  ? { rollupVariants: this.rollupVariants }
                  : {}),
               ...(this.persistedViewsEnabled()
                  ? { persistedViews: this.persistedViews }
                  : {}),
               ...(this.persistedQueriesEnabled()
                  ? { persistedQueries: this.persistedQueries }
                  : {}),
            }),
         servingHost: (servedCopy) => this.servingHost(servedCopy),
         persistedViewsEnabled: () =>
            this.persistedViewsEnabled() || this.persistedQueriesEnabled(),
         withoutPreaggregateEntries: (manifest) =>
            this.withoutPreaggregateEntries(manifest),
         ensureSharedQueryModel: (materializer) =>
            this.ensureSharedQueryModel(materializer),
         getFilters: (sourceName) => this.getFilters(sourceName),
         assertFilterGivens: (givens) => this.assertFilterGivens(givens),
         assertQueryBoundaryEarly: (sourceName, queryName, query) =>
            this.assertQueryBoundaryEarly(sourceName, queryName, query),
         unqueryableTextRefusal: (query, compileError) =>
            this.unqueryableTextRefusal(query, compileError),
         resolveQueryMetadata: (input, connectionName, sourceName) =>
            this.resolveQueryMetadata(input, connectionName, sourceName),
      };
   }

   /**
    * Caller text that did not compile, under a "deferred" boundary: when a
    * source it names is not queryable (and the failure is not a grammar-only
    * one naming nothing in an ungated model, see parseFailureNamesNothing),
    * the boundary's 404, explained (`OffSurfaceError`, ungated only) when a
    * run target is a real model source off the surface; anything else keeps
    * the plain form. Undefined when the compiler's problems may be shown.
    */
   private unqueryableTextRefusal(
      query: string,
      compileError: MalloyError,
   ): Error | undefined {
      if (
         this.queryTextSourcesQueryable(query) ||
         this.parseFailureNamesNothing(query, compileError)
      ) {
         return undefined;
      }
      const offSurface = extractRunTargetSourceNames(query).find(
         (t) =>
            this.declaresSource(t) &&
            !this.isCuratedSource(t) &&
            !this.derivesFromCurated(t, query),
      );
      return this.notQueryable(
         "Query target is not queryable.",
         offSurface !== undefined,
         "source",
         offSurface,
      );
   }

   /**
    * What serving (query_serving.ts) reads from this model: its own storage
    * tables, its legs, its catalog and discovery surface. Built per call so
    * it always reads the current state.
    */
   private servingHost(
      servedCopy: ServedCopyServe = this.servedCopy,
   ): QueryServingHost {
      return {
         modelPath: this.modelPath,
         storageTables: (snapshot) => this.storageTables(snapshot).kept,
         servedCopy,
         rollupVariants: this.rollupVariants,
         persistedViews: this.persistedViews,
         persistedQueries: this.persistedQueries,
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
    * its own (as does a linear report), only the exported names that trace
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
      // under a surface only the ones a query on this notebook may name are shown.
      const readable = this.notebookQueryable(cellIndex);
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
    * Under an active surface, a predicate for the source names the reports
    * read lists with their schema for a `.malloynb`: the ones a query on its
    * path may name. A cell runs as its `run:` statement alone, which the query
    * route admits only over what the surface publishes, so a source its cells
    * declare is withheld, as a hidden one is. Undefined when nothing is
    * curated.
    */
   private notebookQueryable(
      cellIndex?: number,
   ): ((name: string) => boolean) | undefined {
      const readable = this.notebookReadable(cellIndex);
      if (!readable) return readable;
      return (name) => this.admitsSourceByName(name);
   }

   /**
    * Whether the reports read may show a `.malloynb` cell's `queryInfo`. Its
    * schema lists every column the cell's query returns, so a cell the query
    * route would refuse (`run: secret -> { select: * }`) would otherwise
    * describe the hidden source here. Asks {@link assertNotebookCellOnSurface},
    * which runs the query route's own boundary checks on the cell's `run:`,
    * its joins included. Decided once per cell for the boundary in force,
    * since the cells and the model are fixed for this Model's life; a read
    * that fails is not remembered.
    */
   private showsCellQueryInfo(
      cellIndex: number,
      cell: RunnableNotebookCell,
   ): Promise<boolean> {
      const memo = this.curation.cellQueryInfoShown;
      const known = memo.get(cell);
      if (known) return known;
      const shown = this.decideCellQueryInfo(cellIndex, cell);
      memo.set(cell, shown);
      shown.catch(() => {
         if (memo.get(cell) === shown) memo.delete(cell);
      });
      return shown;
   }

   /** {@link showsCellQueryInfo}, decided afresh. */
   private async decideCellQueryInfo(
      cellIndex: number,
      cell: RunnableNotebookCell,
   ): Promise<boolean> {
      // Nothing to check an unhydrated cell against, so under a surface it shows nothing.
      if (!cell.runnable) return this.notebookReadable(cellIndex) === undefined;
      try {
         await this.assertNotebookCellOnSurface(cellIndex);
         return true;
      } catch (error) {
         if (error instanceof NotQueryableError) return false;
         throw error;
      }
   }

   /**
    * Which of `modelInfo.anonymous_queries` a cell whose `queryInfo` is shown
    * describes. A `.malloynb`'s model info is its last code cell's compile,
    * whose final run is that cell's `queryInfo`. A run no shown cell
    * describes is withheld.
    */
   private shownRunIndexes(
      cells: readonly RunnableNotebookCell[],
      shownCells: ReadonlySet<number>,
   ): Set<number> {
      const shown = new Set<number>();
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

   private async getNotebookModel(): Promise<ApiReportCells> {
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
            runQuery: isMalloynbPath(this.modelPath)
               ? malloynbRunQuery(cell)
               : undefined,
         } as ApiNotebookCell);
      }

      // The notebook's own `##` tags, not its imports' (a shared include must
      // not configure every importer).
      const allAnnotations = this.modelDef ? ownModelNotes(this.modelDef) : [];

      const notebookTag = motlyTag(allAnnotations);

      // No `as` cast, so a field rename in `api-doc.yaml` fails to typecheck.
      // Not export-curated (a notebook cannot be imported), but under a surface
      // it shows only what a query on it may name.
      const readable = this.notebookQueryable(notebookCells.length - 1);
      const shownSource = (name: string | undefined) =>
         !readable || (name !== undefined && readable(name));
      const shownRuns = readable
         ? this.shownRunIndexes(cells, shownCells)
         : undefined;
      const notebook: ApiReportCells = {
         type: "notebook",
         format: "malloynb",
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
      };
      return notebook;
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
         return await readPackageURL(packagePath, pathToFileURL(fullPath));
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

/** The first sentence of a message, up to its first ". ". */
function firstSentence(message: string): string {
   const end = message.indexOf(". ");
   return end < 0 ? message : message.slice(0, end + 1);
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

/**
 * What a `.malloynb` code cell runs: the `runQuery` the reports read serves,
 * which the SDK sends as `query` on the notebook's path, and the text the
 * cell's `queryInfo` screen and the load-time cell check
 * ({@link Model.malloynbCellRefusals}) check. Undefined for a markdown cell
 * and for a code cell with no `run:`.
 */
function malloynbRunQuery(
   cell: Pick<RunnableNotebookCell, "type" | "text"> | undefined,
): string | undefined {
   return cell?.type === "code" ? lastRunStatementText(cell.text) : undefined;
}
