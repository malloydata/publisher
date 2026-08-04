// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   API,
   Connection,
   FixedConnectionMap,
   GivenValue,
   InMemoryURLReader,
   isSourceDef,
   MalloyConfig,
   MalloyError,
   Annotations,
   ModelDef,
   modelDefToModelInfo,
   ModelMaterializer,
   NamedQueryDef,
   QueryData,
   QueryMaterializer,
   Runtime,
   type FilterCondition,
   type SourceDef,
   type VirtualMap,
} from "@malloydata/malloy";
import * as Malloy from "@malloydata/malloy-interfaces";
import {
   MalloySQLParser,
   MalloySQLStatementType,
} from "@malloydata/malloy-sql";
import { DataStyles } from "@malloydata/render";
import { publisherMeter } from "../telemetry";
import {
   recordServeShapeTierDrop,
   recordStorageServeRouting,
} from "../materialization_metrics";
import * as fs from "fs/promises";
import { readFileSync } from "fs";
import { createRequire } from "module";
import * as path from "path";
import { fileURLToPath } from "url";
import { components } from "../api";
import {
   getDefaultQueryRowLimit,
   getMaxQueryRows,
   getMaxResponseBytes,
   getQueryMetadataMode,
} from "../config";
import { MODEL_FILE_SUFFIX, NOTEBOOK_FILE_SUFFIX } from "../constants";
import { HackyDataStylesAccumulator } from "../data_styles";
import {
   AccessDeniedError,
   BadRequestError,
   ModelCompilationError,
   ModelNotFoundError,
   NotQueryableError,
   PayloadTooLargeError,
} from "../errors";
import { getPersistStorageMode } from "../config";
import { logger } from "../logger";
import { restrictMalloyConfigToConnections } from "./connection";
import {
   buildServeShapeModelForBindings,
   buildVirtualMap,
   extractJoins,
   extractRefinements,
   extractViews,
   narrowSchemaToPublic,
   sliceSourceRange,
   type ServeBinding,
   type SourceLocation,
} from "./materialization_serve_transform";
import { evaluateManifestFreshness } from "./freshness";
import { deserializeError } from "../package_load/package_load_pool";
import type {
   SerializedModel,
   SerializedNotebookCell,
} from "../package_load/protocol";
import { BuildManifest } from "../storage/DatabaseInterface";
import { URL_READER } from "../utils";
import {
   annotationTexts,
   ownLevelNotes,
   ownModelAnnotations,
   ownModelNotes,
   type AnnotationNote,
} from "./annotations";
import { composeDeclaredQueryMetadata, type ReadableTag } from "./build_plan";
import {
   assertAtMostOneAuthorizeGate,
   assertNoCallerAuthorizeAnnotation,
   assertNoLegacyStringGate,
   assertNoMisplacedAuthorizeAnnotations,
   containsAuthorizeAnnotationTag,
   findLegacyStringGates,
   findMultipleAuthorizeGates,
   referencedGivenNames,
   validateAuthorizeProbes,
   type AuthorizeMap,
   type MisplacedAuthorizeAnnotation,
   type RowLevelGateRejectionCause,
} from "./authorize";
import { readDashboardModelFacts, type DashboardModelFacts } from "./dashboard";
import {
   validateSourceLineGateGivenUsage,
   type ExpandableRefSummary,
} from "./gate_dimension";
import {
   buildFilterClause,
   FilterValidationError,
   injectFilterRefinement,
   type FilterDefinition,
   type FilterParams,
} from "./filter";
import { malloyGivenToApi, type MalloyGiven } from "./given";
import {
   docCommentTitleAndDescription,
   motlyTag,
   readAutorun,
   readStartingGivens,
   tagText,
} from "./motly";
import {
   assertWithinModelByteLimit,
   assertWithinModelRowLimit,
   type QueryRowLimitSource,
   queryRowLimitSource,
   resolveModelQueryRowLimit,
   stringifyQueryResponse,
} from "./model_limits";
import { bigIntReplacer } from "../json_utils";
import {
   buildDerivationBaseMap,
   buildSourceAliasMap,
   extractRunTargetSourceName,
   stripMalloyCommentsAndLiterals,
} from "./query_text";
import {
   mergeQueryMetadata,
   type QueryClass,
   type QueryMetadata,
} from "./query_metadata";
import {
   validateModelPreaggregation,
   type PreaggregateViolation,
} from "./preaggregation_validation";
import { derivedStructsReachable } from "./gate_registry_walk";
// Aliased with an `Impl` suffix: `Model` declares its own private methods of
// these same names (thin per-instance wrappers, see each one's doc) — an
// unaliased import would only work today because a method body's unqualified
// call resolves to this module-level import, never to the method itself
// (that needs `this.`). Aliasing removes the name COLLISION outright, rather
// than leaving a trap where converting one of those methods to an
// arrow-function class property would recurse instead of delegating.
import {
   collectEntryPointGates as collectEntryPointGatesImpl,
   createGateClassificationDeps,
   resolveGateShape as resolveGateShapeImpl,
   resolveGraftTarget as resolveGraftTargetImpl,
   type GateClassificationDeps,
   type GateEntry,
   type GraftScope,
} from "./gate_classification";
import {
   extractQueriesFromModelDef,
   extractSourcesFromModelDef,
} from "./source_extraction";
import {
   recordAuthorizeBypass,
   recordAuthorizeGuardRejection,
   recordRowLevelGateDecision,
   recordRowLevelGateRejected,
   type AuthorizeBypassEntryPoint,
   type AuthorizeGuardField,
} from "../authorize_metrics";
import { safeJoinUnderRoot } from "../path_safety";

/**
 * What a request boundary contributes to a model query's per-query metadata: the
 * caller's own properties and class, the environment the query runs in, and a
 * reader for the executing connection's default (the controller owns the
 * environment, so it supplies the lookup rather than the model reaching for it).
 */
export interface ModelQueryMetadataInput {
   request?: QueryMetadata;
   queryClass?: QueryClass;
   environment?: string;
   version?: string;
   /**
    * The id this query is correlated by, minted by the boundary that returns it
    * (see `mintCorrelationId`). Omitted by callers with no response field to
    * carry it, which is why it is not minted here.
    */
   correlationId?: string;
   /**
    * The executing connection's two metadata layers: its overridable default and
    * the properties the deployment enforces. Supplied by the controller, which
    * owns the environment; the model only knows the connection by name.
    */
   connectionMetadata?: (connectionName: string) => {
      default?: QueryMetadata | null;
      enforced?: QueryMetadata | null;
   } | null;
   /**
    * The package's declared bag — the least specific author-declared layer.
    * Supplied by the controller, which owns the package; the model knows only
    * its own file and its package's NAME.
    */
   packageDeclaration?: QueryMetadata | null;
}

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

export type ModelType = "model" | "notebook";

/**
 * How a query's answer was ultimately produced, once it reached the storage
 * routing decision at all. `storage` = served from the materialized table via
 * the virtual-source transform. `live_fallback` = it routed, then a run-time
 * store failure degraded it to the live warehouse — a success the caller cannot
 * distinguish from a storage hit, which is exactly why it is reported
 * separately. Absent means the query never had a storage binding to consider.
 */
export type ServedFrom = "storage" | "live_fallback";
export type ModelConnectionInput = MalloyConfig | Map<string, Connection>;

interface RunnableNotebookCell {
   type: "code" | "markdown";
   text: string;
   runnable?: QueryMaterializer;
   /** Retained so we can rebuild the query with filter refinements at execution time. */
   modelMaterializer?: ModelMaterializer;
   /**
    * This cell's own compiled modelDef — the model state AFTER this cell's
    * own declarations, i.e. what `modelMaterializer` above was hydrated
    * from. Retained for TWO graft roles (see `Model.graftScopeForCell` /
    * `Model.selfGraftScopeForCell` / `Model.resolveNotebookCellGraftScope`):
    *  - as a LATER cell's EARLIER scope: a cell's text compiles against
    *    whatever existed BEFORE its own declarations, so the correct graft
    *    scope for recompiling a LATER cell's text is an EARLIER cell's own
    *    (modelDef, modelMaterializer) pair, never that later cell's own —
    *    which already contains whatever it itself just declared, so
    *    grafting it and recompiling that cell's own `source:`+`run:` text
    *    against it fails with "Cannot redefine".
    *  - as THIS cell's OWN fallback scope, when no earlier cell can cover
    *    its gate at all (this cell declares AND runs its own gated source):
    *    bound by repointing this cell's already-compiled queryDef via
    *    `_loadQueryFromQueryDef`, never by recompiling text against it — the
    *    same "Cannot redefine" collision would occur if it were recompiled,
    *    but the repoint mechanism sidesteps it entirely by never
    *    re-parsing any text.
    */
   modelDef?: ModelDef;
   newSources?: Malloy.SourceInfo[];
   queryInfo?: Malloy.QueryInfo;
}

/**
 * Backtick-quote a Malloy identifier for safe interpolation into a `run:`
 * query string. Escapes backslashes and backticks (in that order) so a name
 * that needs Malloy quoting (hyphen, space, reserved word, leading digit) or
 * contains an embedded backtick cannot break out of the quotes. Mirrors
 * malloy's internal `identifierCode` / `escapeIdentifier` (to_stable.ts), which
 * is not exported.
 */
function quoteMalloyIdentifier(name: string | undefined): string {
   return "`" + (name ?? "").replace(/\\/g, "\\\\").replace(/`/g, "\\`") + "`";
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

/**
 * Whether `err` is Malloy refusing to BIND a given: the runtime form carries
 * a `runtime-given-*` code on the error itself, while a prepare-time failure
 * arrives as a `MalloyError` whose `problems` carry a `compiler-given-*` one.
 * Duck-typed on `.code` for the same reason every other check here is —
 * neither error class is root-exported.
 */
function isGivenBindingFailure(err: unknown): boolean {
   const isGivenCode = (code: unknown): boolean =>
      typeof code === "string" &&
      (code.startsWith("runtime-given-") || code.startsWith("compiler-given-"));
   if (isGivenCode((err as { code?: unknown })?.code)) return true;
   const problems = (err as { problems?: unknown })?.problems;
   return (
      Array.isArray(problems) &&
      problems.some((p) => isGivenCode((p as { code?: unknown })?.code))
   );
}

/**
 * Name budget for {@link Model.requestChainProvesUngated}'s walk over a
 * request's own derivation declarations. Exceeding it fails the proof (and so
 * denies), which is why it only has to be larger than any real chain.
 */
const REQUEST_CHAIN_MAX_NAMES = 64;

export class Model {
   private packageName: string;
   private modelPath: string;
   private dataStyles: DataStyles;
   private modelType: ModelType;
   private modelMaterializer: ModelMaterializer | undefined;
   private modelDef: ModelDef | undefined;
   private modelInfo: Malloy.ModelInfo | undefined;
   /**
    * Connection config to compile a transient serve-shape model against, when a
    * query is routed through the `storage=` virtual-source transform. Captured
    * at hydration (fromSerialized) so serve can build a fresh Runtime.
    */
   /**
    * The connections a serve-shape compile may resolve: this environment's
    * storage destinations. Deliberately NOT the config this model itself
    * compiles against — a destination must not be resolvable from the author's
    * model, a notebook cell, or a query, and the serve shape is a separate
    * synthetic model, so the two compiles can and must read disjoint name sets.
    * Absent ⇒ no storage serve routing (queries serve live).
    */
   private serveDestinationConfig?: () => MalloyConfig;
   /**
    * The package's `storage=` serve bindings, set by the owning Package when a
    * build/manifest binds materialized-into-storage sources. Empty ⇒ no serve
    * routing (the common case; the serve path is unchanged).
    */
   private serveBindings: ServeBinding[] = [];
   /** Memoized serve-shape materializer, keyed by the bound source set. */
   private serveShapeCache?: { key: string; materializer: ModelMaterializer };
   /**
    * The synthesized pre-aggregation model for this model, compiled once per load
    * by the owning Package (see Package.pushPreaggregateServeModels) and reused by
    * every query. Undefined when the model declares no usable `#@ preaggregate`
    * or when synthesis failed — in both cases the serve path is untouched.
    *
    * Compiled per load rather than per query because there is nothing per-query
    * to key it on. Unlike the storage tier, whose shape must recompile as
    * bindings cross their freshness window, a rollup's freshness is handled
    * underneath this by the per-query build manifest: while the table is fresh it
    * substitutes, and when it is not, the composite member simply recomputes the
    * rollup from the base. Same answer either way, which is what lets one
    * compiled model serve every query.
    */
   private preaggregateServeMaterializer?: ModelMaterializer;
   private sources: ApiSource[] | undefined;
   private queries: ApiQuery[] | undefined;
   private sourceInfos: Malloy.SourceInfo[] | undefined;
   private runnableNotebookCells: RunnableNotebookCell[] | undefined;
   private compilationError: MalloyError | Error | undefined;
   /** Parsed #(filter) definitions keyed by source name. */
   private filterMap: Map<string, FilterDefinition[]>;
   /** Givens declared on the model, in declaration order. Malloy's
    *  `Model.givens` already collapses inheritance; we just stash the list
    *  for surfacing on the compiled-model response. */
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
   /** Given names (`$NAME`) referenced by any authorize gate reachable
    *  anywhere in this model -- every top-level source's own gate, and every
    *  gate a top-level source carries in from what it derives from (the same
    *  walk {@link assertAuthorizedForAllSources} runs at request time).
    *  Computed once at construction; see {@link filterGivensToModelSurface},
    *  the only consumer. */
   private authorizeReferencedGivenNames: Set<string> = new Set();
   /** Whether discovery accessors curate to the `export {}` closure. Pushed
    *  down by the owning Package (see Package.applyDiscoveryPolicyToModels):
    *  true only when the package declares `explores` in publisher.json.
    *  Defaults to false (legacy listings) so a Model created outside a
    *  Package matches pre-opt-in behavior. */
   private discoveryCurationEnabled = false;
   /** Per-package query-boundary policy, pushed down by the owning Package
    *  (see `Package.applyQueryBoundaryToModels`). Defaults are inert (mode
    *  "all" / not declared) so a Model created outside a Package — or before
    *  the policy is applied — never spuriously denies. */
   private queryBoundary: {
      mode: "declared" | "all";
      exploresDeclared: boolean;
      isQueryEntryPoint: boolean;
      /** The PACKAGE-wide export closure (union over explores-listed models),
       *  so a source declared queryable by its own file stays queryable when
       *  addressed through a model that imports it. Keyed name -> the set of
       *  DEFINITION IDENTITIES ({@link definitionIdentity}) curated under that
       *  name, never the bare name: a name is admitted only when THIS model
       *  resolves it to the very definition a listed model exported, so a
       *  same-named source in another file is not admitted by the collision.
       *  Absent when the boundary is inert. See
       *  Package.applyQueryBoundaryToModels. */
      packageCuratedSources?: ReadonlyMap<string, ReadonlySet<string>>;
      packageCuratedQueries?: ReadonlyMap<string, ReadonlySet<string>>;
   } = { mode: "all", exploresDeclared: false, isQueryEntryPoint: true };
   /** Per-query freshness resolver, pushed down by the owning Package (see
    *  Package.wireFreshnessResolvers). Returns the freshness-filtered build
    *  manifest for the serve path — threaded into Malloy's per-query
    *  `buildManifest` override so a persist source only routes to its
    *  materialized table while within its declared freshness window. Undefined
    *  (or returning undefined) means no override: the runtime-baked manifest
    *  applies, which serves live when unbound. */
   private freshnessResolver?: () => BuildManifest["entries"] | undefined;
   /** See {@link setPreaggregateEntityIdResolver}. */
   private preaggregateEntityIdResolver?: () => ReadonlySet<string>;
   /** Entry-point gates per declared source name — see
    *  {@link computeEntryPointGatesBySource}. The one answer that
    *  `sources[].authorize`, {@link getAuthorize} and the early gate all read. */
   private entryPointGatesBySource: Map<string, GateEntry[]> = new Map();
   /**
    * The runtime that produced this model, retained ONLY so a row-level
    * `#(authorize)` gate can be enforced by grafting its compiled condition
    * onto a deep-copied `ModelDef` and reloading that copy — see
    * `buildGraftedMaterializer`. Set once, right after construction, by
    * `setGateRuntime` from both `Model.create` and `fromSerialized`; a
    * `Model` built any other way (a test fixture that hand-constructs one)
    * simply has no retained runtime, so `authorizeAndBindRunnable` denies any
    * row-level gate it finds rather than throwing — fail closed, not a crash.
    *
    * Threaded explicitly rather than read off `ModelMaterializer`'s own
    * `protected runtime` field on purpose: reaching across that visibility
    * boundary would work today, but a Malloy internal rename of that field
    * would silently disable grafting on the next dependency bump. It still
    * fails closed in that scenario (every row-level gate would deny instead
    * of filtering) — but a silent, fleet-wide outage on a routine Malloy bump
    * is exactly the failure mode worth spending an explicit field to avoid.
    */
   private gateRuntime: HydrationRuntime | undefined;
   /**
    * This model's own {@link GateClassificationDeps} — built ONCE, via
    * {@link createGateClassificationDeps}, and reused for the life of THIS
    * MODEL INSTANCE. Correct to hold onto that long because a `Model`
    * instance is never mutated and reused across a package reload; a reload
    * always constructs a fresh `Model` (and therefore a fresh deps struct,
    * with a fresh, empty `gateShapeCache`) rather than patching this one.
    * `gateShapeCache`'s entries hold the LIFTED condition alongside the
    * classification (not just the classification itself), which is what
    * makes a row-level gate's SECOND consumer — grafting it onto a copied
    * `ModelDef` — free too: the expensive part is the probe compile, and the
    * cache is what limits it to exactly one per gate per `(cacheScope,
    * graftTarget, filterText)` on this model instance, ever.
    * `gateShapeCache` is uncapped, unlike {@link graftedMaterializerCache},
    * and for a reason rather than by oversight: every component of its key
    * is derived from the model's own shape, never from a caller's givens or
    * query text, so the entry count is bounded by the model itself and no
    * request pattern can grow it.
    *
    * Built lazily (not at construction) purely so `this.givens` is settled
    * first; see {@link createGateClassificationDeps}'s doc for why the cache
    * and the given-declared-type/default maps must be minted together here
    * rather than assembled from three separately-cached fields.
    */
   private gateClassificationDepsCache: GateClassificationDeps | undefined;
   /**
    * Grafted `ModelMaterializer`s, memoized per `cacheScope` + SORTED set of
    * `(graftTarget, filterText)` pairs — see `getOrBuildGraftedMaterializer`.
    * A graft depends only on WHICH source carries WHICH conditions, never on
    * a caller's givens or query text (those bind later, at `run()`), so every
    * request that hits the same gate set IN THE SAME SCOPE reuses the same
    * graft. This is the expensive step (`structuredClone` scales with model
    * size), so caching it is what keeps a row-level gate affordable under
    * load rather than merely correct. `cacheScope` (see {@link GraftScope})
    * is part of the key for the same reason as `gateShapeCache`'s: a graft
    * built against a notebook cell's own modelDef must never be served for a
    * different cell (or the model-wide model) that happens to name its graft
    * target identically.
    */
   private graftedMaterializerCache: Map<string, ModelMaterializer> = new Map();
   /**
    * Entry cap for {@link graftedMaterializerCache}.
    *
    * Each entry retains a full `structuredClone(modelDef)`, costing roughly
    * 1.35x the serialized ModelDef -- so uncapped, on a large gated package
    * whose notebook multiplies the scopes by its cell count, this is a
    * several-hundred-MiB step function per `Model` instance that nothing ever
    * releases. 32 holds every graft set a normal package produces (one per
    * gated entry point, plus a notebook's per-cell scopes). A miss costs one
    * clone and one model reload, never correctness -- the graft is rebuilt from
    * the same inputs -- so evicting too eagerly is a latency question, not a
    * safety one.
    */
   private static readonly GRAFTED_MATERIALIZER_CACHE_MAX = 32;
   /**
    * Identifies a `QueryMaterializer` `authorizeAndBindRunnable` returned
    * because it attached a row-level filter — an object-identity `WeakSet`
    * rather than a field on `Model`, since `Model` is shared across
    * concurrently in-flight requests and a per-instance flag would race.
    * Consulted by the query and notebook paths to (a) keep a filtered query
    * off the storage-serve tier, which cannot bind the given the filter
    * depends on, and (b) record `empty_after_filter` without re-deriving
    * whether a filter actually attached.
    */
   private rowLevelFilteredRunnables: WeakSet<QueryMaterializer> =
      new WeakSet();
   private meter = publisherMeter();
   private queryExecutionHistogram = this.meter.createHistogram(
      "malloy_model_query_duration",
      {
         description: "How long it takes to execute a Malloy model query",
         unit: "ms",
      },
   );
   /**
    * Warehouse bytes SCANNED by model queries, summed — not bytes billed. The
    * backend reports what the query processed; BigQuery then bills a 10MB minimum
    * per query, so spend runs above this on small reads. Named for what it holds
    * rather than for what it is used for: a metric name is the one surface that
    * cannot be corrected after the fact without renaming the series, and `cost`
    * would have implied money.
    *
    * A counter rather than a histogram: the question is "how much did this cost
    * over a period", which is a sum, and bytes as a histogram VALUE would need
    * bucket boundaries spanning kilobytes to terabytes to say anything useful.
    *
    * Only advances for backends that report the figure — today BigQuery, whose
    * job metadata carries `totalBytesProcessed` in the same response the driver
    * already fetches. **Absent is not zero.** Snowflake exposes bytes only in
    * `QUERY_HISTORY`, keyed by query id, so getting it inline would cost an extra
    * round trip per query; it belongs to a deferred join on the query tag
    * instead. Postgres has no equivalent. A dashboard summing this across
    * connections silently understates every non-BigQuery one.
    */
   private queryScannedBytesCounter = this.meter.createCounter(
      "malloy_model_query_scanned_bytes",
      {
         description:
            "Warehouse bytes scanned by Malloy model queries, where the backend reports them. NOT bytes billed: BigQuery bills a 10MB minimum per query, so spend exceeds this on small reads.",
         unit: "By",
      },
   );

   constructor(
      packageName: string,
      modelPath: string,
      dataStyles: DataStyles,
      modelType: ModelType,
      modelMaterializer: ModelMaterializer | undefined,
      modelDef: ModelDef | undefined,
      // TODO(jjs) - remove these
      sources: ApiSource[] | undefined,
      queries: ApiQuery[] | undefined,
      sourceInfos: Malloy.SourceInfo[] | undefined,
      runnableNotebookCells: RunnableNotebookCell[] | undefined,
      compilationError: MalloyError | Error | undefined,
      filterMap?: Map<string, FilterDefinition[]>,
      givens?: ApiGiven[],
      /**
       * Precomputed `modelDefToModelInfo(modelDef)`. The package-load
       * worker emits it as part of `SerializedModel` so we don't
       * re-derive it on every package load. Callers that build a
       * `Model` from a raw `modelDef` (e.g. test fixtures via
       * `Model.create`) can omit this and let the constructor
       * derive it lazily.
       */
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
      // One walk, both consumers. `collectEntryPointGates` is the single
      // definition of "what gates this source as an entry point" — it follows
      // the `inherits`/registry chain AND a query-source's derivation base.
      // Resolving the same question a second way is what let a derived source
      // (`source: laundered is locked_src -> {...}`) report as ungated while the
      // late walk gated it: introspection and the early gate read
      // `sources[].authorize`, which is extracted from the `inherits` chain
      // alone and cannot see `query.structRef`. So the walk's answer is recorded
      // here, keyed by source name, and it is what both read.
      //
      // The constructor is the right place because BOTH load paths reach it: the
      // in-process `Model.create` and the worker pool, which serializes `sources`
      // over the wire and would otherwise keep the extractor's narrower answer.
      // It costs nothing extra — computeAuthorizeReferencedGivenNames already
      // walked every top-level source, and now does it once for both results.
      try {
         this.entryPointGatesBySource = this.computeEntryPointGatesBySource();
      } catch {
         this.entryPointGatesBySource = new Map();
      }
      // Make introspection agree with enforcement. `sources[].authorize` is
      // serialized to the API and read by downstream enforcers, so leaving the
      // narrower value there reports a gated source as unrestricted — the more
      // dangerous of the two possible errors. Mutating in place (rather than at
      // the API boundary) keeps getSources()/getAuthorize()/the early gate on one
      // answer instead of three.
      for (const source of this.sources ?? []) {
         if (!source.name) continue;
         const exprs = this.entryPointGatesBySource
            .get(source.name)
            ?.flatMap((g) => g.exprs);
         if (exprs && exprs.length > 0) source.authorize = exprs;
      }
      // Guarded defensively: a malformed gate reachable only through a
      // join/derivation must not throw out of the constructor
      // (gateExprsForOwnAnnotations already fails closed per struct, so this
      // can only throw on something unrelated).
      try {
         this.authorizeReferencedGivenNames =
            this.computeAuthorizeReferencedGivenNames();
      } catch {
         this.authorizeReferencedGivenNames = new Set();
      }
      this.modelInfo =
         modelInfo ??
         (this.modelDef ? modelDefToModelInfo(this.modelDef) : undefined);

      // One-time deprecation notice per Model instance. Surfaces only when
      // the model declares `#(filter)` annotations so operators migrating
      // toward `given:` see a clear pointer in the server log without
      // spamming for models that have already moved over.
      if (this.filterMap.size > 0) {
         logger.warn(
            `Model "${packageName}/${modelPath}" uses deprecated #(filter) annotations. Migrate to given: — see https://github.com/malloydata/publisher/blob/main/docs/givens.md`,
            {
               packageName,
               modelPath,
               filterSourceCount: this.filterMap.size,
            },
         );
      }
   }

   /**
    * Get the parsed filter definitions for a given source name.
    * Returns an empty array if no filters are declared.
    */
   public getFilters(sourceName: string): FilterDefinition[] {
      return this.filterMap.get(sourceName) ?? [];
   }

   /**
    * Retain the runtime a row-level `#(authorize)` gate grafts through — see
    * {@link gateRuntime}. Called once by each construction path
    * (`Model.create`, `fromSerialized`) right after `new Model(...)`, rather
    * than threaded as a constructor parameter: the constructor already has
    * two call shapes (a compiled model, a compilation-failure placeholder)
    * and most of its parameters are read unconditionally, while this is
    * needed only on the compiled-model path and only for one feature.
    */
   private setGateRuntime(runtime: HydrationRuntime): void {
      this.gateRuntime = runtime;
   }

   /**
    * The graft scope every non-notebook caller uses — this model's own
    * cumulative (modelDef, modelMaterializer), scoped `"model"`. `undefined`
    * only when this `Model` has no compiled model at all (a
    * compilation-failure placeholder), matching every other row-level code
    * path's existing fail-closed posture for that case.
    *
    * {@link GraftScope} (`./gate_classification`) is built here, per graft
    * consumer:
    *  - the query path and the compile-time probe backstop use this method;
    *  - a notebook cell instead passes the model as resolved by
    *    {@link resolveNotebookCellGraftScope} — either an EARLIER cell's own
    *    scope ({@link graftScopeForCell}) or, when no earlier scope covers
    *    the run target, this cell's OWN post-declaration scope
    *    ({@link selfGraftScopeForCell}). Never this method's model-wide
    *    cumulative scope for a notebook cell: it already carries everything
    *    every cell (including this one) has declared, so recompiling a cell
    *    that declares its own source (`source: local2 is gated extend {…}`
    *    then `run: local2 -> …`) against it tries to redeclare a name the
    *    cumulative model already has, and fails with "Cannot redefine
    *    'local2'" — the same failure a cell's OWN scope has for its OWN
    *    declarations, which is why that fallback binds by repointing a
    *    compiled queryDef instead of recompiling text (see
    *    `executeNotebookCell`).
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
    * The EARLIER-scope half of a notebook cell's row-level graft: the model
    * AS OF THAT CELL — the nearest EARLIER code cell's own (modelDef,
    * modelMaterializer) pair, walking back over any markdown cell in
    * between. Never this cell's own scope (see
    * {@link RunnableNotebookCell.modelDef}'s doc) and never this model's
    * cumulative {@link defaultGraftScope}, for the same "Cannot redefine"
    * reason.
    *
    * `undefined` for the first code cell in a notebook, or a code cell
    * preceded only by markdown (nothing EARLIER carries a code cell's
    * modelDef/modelMaterializer to graft against). That is not, by itself,
    * a reason to deny: {@link resolveNotebookCellGraftScope} — the only
    * caller — falls back to {@link selfGraftScopeForCell} whenever this
    * returns `undefined`, OR whenever it returns a scope that does not
    * actually carry the cell's run target (a LATER cell that declares and
    * runs its own gated source has an earlier cell, but not one that
    * declared that source). A row-level gate genuinely has nowhere to graft
    * only when NEITHER scope covers it — see
    * `resolveNotebookCellGraftScope`'s doc for the full decision.
    *
    * `cacheScope` is keyed on the SUPPLYING cell's index (not the requesting
    * cell's), so two different later cells that share the same nearest
    * earlier code cell correctly share one cached classification/graft,
    * while two cells whose nearest earlier cell differs never collide even
    * if both happen to declare a same-named source.
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
    * model — the fallback {@link resolveNotebookCellGraftScope} uses
    * whenever the nearest earlier code cell cannot cover a cell's row-level
    * gate at all: a cell that both DECLARES a gated source and RUNS it in
    * the SAME cell (`#(authorize) "…"` + `source: gated is …` then
    * `run: gated -> …`, all in one cell). That source exists nowhere
    * earlier to graft against — grafting THIS scope instead works only
    * because the bind mechanism this scope is paired with
    * (`_loadQueryFromQueryDef` in `executeNotebookCell`) never recompiles
    * the cell's own text, so the "Cannot redefine" collision
    * {@link graftScopeForCell}'s doc describes for an EARLIER-scope graft
    * never arises here.
    *
    * `undefined` only when the cell itself has no compiled
    * (modelDef, modelMaterializer) pair of its own — should not happen for
    * any cell reaching this method, since only a cell with both ever
    * attempts a row-level bind at all.
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
    * The {@link GraftScope} for a notebook cell's row-level gate — and,
    * via `usesOwnScope`, HOW the caller must bind against it. Two scopes
    * exist for a cell: the nearest EARLIER code cell's own
    * ({@link graftScopeForCell}) and this cell's OWN post-declaration one
    * ({@link selfGraftScopeForCell}). Which one is correct depends on where
    * the RUN TARGET actually resolves — never on the cell's INDEX. A
    * cell-0-only test would miss a LATER cell that declares and runs its
    * own gated source: that cell has an earlier code cell, but the source
    * it declares does not exist there either, so the earlier scope is just
    * as unusable as it is for cell 0 — the failure is the same shape, only
    * the cell index differs.
    *
    *  - If the run target resolves as a graft target against the EARLIER
    *    scope, use it: the recompile step stays the ordinary one
    *    (`mm.loadQuery(cellText)`), because the earlier scope does not yet
    *    hold whatever name this cell's own text declares, so redeclaring it
    *    during recompile succeeds. This is the `local2` case: a cell
    *    that declares `local2 is gated extend {}` and runs it, where `gated`
    *    (and its gate) was declared in an EARLIER cell.
    *  - Otherwise — no earlier code cell at all (the first code cell, or one
    *    preceded only by markdown), or the run target simply isn't reachable
    *    from whatever the earlier scope holds — fall back to this cell's OWN
    *    scope (`usesOwnScope: true`). The caller must NOT recompile this
    *    cell's text against it (see {@link selfGraftScopeForCell}'s doc for
    *    why that throws); it must repoint the cell's already-compiled
    *    queryDef instead.
    *
    * Reuses {@link resolveGraftTarget} itself to decide reachability — the
    * exact check {@link resolveGateShape} would otherwise fail on later, per
    * gate entry — so the two can never disagree about what the earlier scope
    * does or doesn't cover.
    */
   private async resolveNotebookCellGraftScope(
      cellIndex: number,
      runnable: QueryMaterializer,
   ): Promise<{ graftScope: GraftScope | undefined; usesOwnScope: boolean }> {
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
    * Effective authorize expressions gating a source: its own `#(authorize)`.
    * Empty array means unrestricted. Reads the per-source list surfaced on
    * `sources` (which rides the worker serialization boundary), so it works
    * for both freshly-created and deserialized models.
    *
    * The value is the annotation's expression as authored — introspection
    * only. Use it to decide THAT a source is gated, never to reconstruct
    * the gate: enforcement re-derives the condition from the recompiled IR
    * (`./gate_classification`), and `gate_registry_walk.ts` substitutes a
    * `"false"` sentinel where it cannot attribute an expression, so this
    * text is not always literally what the author wrote.
    */
   public getAuthorize(sourceName: string): string[] {
      return (
         this.sources?.find((source) => source.name === sourceName)
            ?.authorize ?? []
      );
   }

   /**
    * Filter caller-supplied givens down to the ones safe to forward to the
    * REAL query's `getPreparedResult`/`run`. A caller may legitimately need
    * to supply a value only so a gate carried in from a derivation base can
    * see it. Malloy's own given-resolution doesn't flatten a `given:`
    * declared more than one import hop away into the entry model's namespace
    * (see `docs/authorize.md`), so passing such an authorize-only name
    * straight through to the real query throws ("givens: unknown given").
    *
    * A gate is NOT evaluated separately from the query any more — it is
    * grafted onto the run target and evaluated by the same `run()`, against
    * this FILTERED set. What makes dropping safe is therefore an invariant,
    * not an ordering: an ACCEPTED gate can only reference a given on this
    * model's own surface, because `resolveGateShape` rejects
    * (`unreachable_given`) any given absent from `givenDeclaredTypes` — which
    * is that same surface (`computeGivenDeclaredTypes(this.givens)`). A name
    * this method drops is off the surface by definition, so no gate that
    * survived classification reads it. `resolveGateShape` re-checks that
    * invariant on every accepted classification and denies if it ever fails,
    * rather than leaving it to hold by coincidence.
    *
    * Only drops a name that is BOTH absent from this model's own given
    * surface ({@link givens}) AND referenced by an authorize gate reachable
    * in this model ({@link authorizeReferencedGivenNames}) — i.e. a name
    * that could only have been supplied for gate evaluation. A name that no
    * gate references is left untouched even when the model doesn't surface
    * it, so a genuinely unknown / typo'd / legitimately-needed-but-unsurfaced
    * `where:` given still reaches the real query and fails closed via
    * Malloy's own "unknown given" error, instead of being silently swallowed
    * and falling back to its declared default (over-exposure).
    */
   private filterGivensToModelSurface(
      givens: Record<string, GivenValue> | undefined,
   ): Record<string, GivenValue> | undefined {
      if (!givens) return givens;
      const surfaceNames = new Set((this.givens ?? []).map((g) => g.name));
      const filtered: Record<string, GivenValue> = {};
      for (const [name, value] of Object.entries(givens)) {
         const authorizeOnly =
            !surfaceNames.has(name) &&
            this.authorizeReferencedGivenNames.has(name);
         if (!authorizeOnly) filtered[name] = value;
      }
      return filtered;
   }

   /**
    * Compute {@link authorizeReferencedGivenNames}: every given name (`$NAME`)
    * referenced by an authorize gate expression reachable anywhere in this
    * model. Walks, for every top-level source in `modelDef.contents`, every
    * gate reached from it via {@link collectEntryPointGates} — the exact
    * same unified traversal {@link assertAuthorizedForAllSources} runs
    * per-query, just rooted at every top-level source instead of one run
    * target. Runs once at construction, not per request.
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
    * `treatAsOwnGate` is true: each source here IS an entry point, so a gate it
    * declares itself is probed ambient-first while one carried in from a
    * derivation base or an `extend` ancestor stays `selfContained` — see
    * {@link collectEntryPointGates}. Preserving that per-gate flag is the second
    * half of why this replaced the name-lookup path: the old early gate flattened
    * every gate to ambient-first, so an inherited gate referencing `$LEVEL` was
    * satisfied by an entry model that happened to declare its own `LEVEL`.
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
    * Whether this model carries an `#(authorize)` annotation ANYWHERE — the
    * cheap short-circuit for a per-query gate walk, and deliberately NOT the
    * same predicate as {@link hasAuthorize}.
    *
    * The difference is the whole point. `hasAuthorize` reads top-level sources'
    * OWN effective gates off the extracted `sources` list, which is why
    * `getQueryResults` warns against guarding the authoritative gate on it: a
    * gate a source only INHERITS can be missing from it, and guarding on that
    * re-opens the inherited-gate bypass. This asks a strictly wider question, of
    * the IR rather than the extract: does any authorize-routed note exist across
    * `contents` u `sourceRegistry` — on a source, on one of its fields, or
    * anywhere up an `annotations.inherits` chain?
    *
    * It has to be a superset of everything {@link collectEntryPointGates} can
    * reach, and `contents` u `sourceRegistry` alone is NOT that. Two of the five
    * links the walk follows land on structs in neither collection, so the sweep
    * has to follow them itself:
    *
    *  - a `query_source`'s base (`query.structRef`) is an INLINE `SourceDef`,
    *    not a string into `contents`, whenever the base arrived through an
    *    import. `import { gated }` in a middle file, `source: qs is gated ->
    *    {...}`, then `import { qs }` locally: `contents` holds only `qs`, and
    *    `gated`'s gate lives on the inline ref.
    *  - a composite's resolved member (`query.compositeResolvedSourceDef`) is a
    *    synthesized struct that is never in either collection.
    *
    * Both were missed by an earlier version of this sweep, which therefore
    * answered `false` for a genuinely gated entry point and skipped the walk —
    * handing storage and pre-aggregation routing back the dependence on a BUILD
    * path's refusal that the guard exists to remove. {@link
    * derivedStructsReachable} follows both, transitively.
    *
    * With those, `false` genuinely means "no gate is findable" — and unreadable
    * IR returns `true`, so every remaining inexactness falls on the side of
    * walking anyway.
    *
    * It exists so a deployment with pre-aggregation enabled and no gates
    * anywhere does not start paying a live compile per query for a case it
    * cannot hit; see `getQueryResults`'s routing pre-check.
    */
   private hasAnyAuthorizeNote(): boolean {
      if (this.anyAuthorizeNote !== undefined) return this.anyAuthorizeNote;
      this.anyAuthorizeNote = ((): boolean => {
         const modelDef = this.modelDef;
         if (!modelDef) return false;
         try {
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
            for (const struct of structs) {
               // `annotationTexts` (whole chain), not `ownLevelNoteTexts`: this
               // has to see a gate demoted to `annotations.inherits` by a stray
               // annotation on the deriving statement, which is the shape the
               // inherited-gate hole lives in.
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
    * Runtime authorize gate. Throws `AccessDeniedError` (403) when a gate on
    * `sourceName` cannot even be classified/grafted; a gate that CAN be
    * expressed as a row filter is deferred (see the entry-point loop below)
    * rather than evaluated here, since it has no whole-source admit/deny
    * answer — enforcing it means recompiling against a grafted materializer,
    * which is `authorizeAndBindRunnable`'s job, not this probe-only one's.
    * No in-scope gate = unrestricted.
    *
    * Fail closed: any failure to classify or graft a gate denies. (Expression
    * well-formedness was already validated at model load; see authorize.ts.)
    * The 403 message names only the source, never the expression, so gate logic
    * is not leaked to the caller.
    */
   public async assertAuthorized(
      sourceName: string | undefined,
      _givens: Record<string, GivenValue>,
      bypassAuthorize = false,
      /**
       * The graft scope a row-level gate found here would classify/lift
       * against — see {@link GraftScope}. Defaults to this model's own
       * cumulative scope, correct for every caller except
       * `collectAuthorizeEntryPointGates`, which forwards whatever scope the
       * runnable it was given belongs to (a notebook cell's own, for a
       * notebook cell's runnable).
       */
      graftScope: GraftScope | undefined = this.defaultGraftScope(),
   ): Promise<void> {
      if (bypassAuthorize) {
         this.noteAuthorizeBypass("source", sourceName);
         return;
      }
      // A named, declared source is gated from the entry-point walk — the SAME
      // walk, and so the same answer, the compiled backstop reaches. Before
      // this, the two disagreed in both directions: the walk followed a
      // query-source's derivation base and this did not, and this probed every
      // gate ambient-first while the walk isolated inherited ones (both gates
      // classify the compiled shape now, so neither probes at all).
      // Either disagreement is a schema oracle, not just an inconsistency: this
      // gate runs BEFORE compilation, so a source it wrongly admits gets its
      // compile errors (`'no_such_field' is not defined`) returned to a caller the
      // backstop would have denied.
      const gates = sourceName
         ? this.entryPointGatesBySource.get(sourceName)
         : undefined;
      if (gates) {
         for (const entry of gates) {
            // A gate is a row FILTER now, only enforceable where a runnable
            // exists to recompile against the grafted materializer
            // (`authorizeAndBindRunnable`). Deferring the admit case here is
            // safe rather than fail-open: every caller of `assertAuthorized`
            // that reaches a named source either IS `authorizeAndBindRunnable`
            // itself (via the shared `collectAuthorizeEntryPointGates` helper,
            // which re-derives and enforces this exact gate a few lines
            // further down the SAME call) or is a best-effort PRE-compile fast
            // path (`getQueryResults`' early gate, `assertAuthorizedForText`)
            // that an unconditional authoritative backstop always runs after.
            // Denying here instead would be wrong in the opposite direction:
            // every gated query would be refused before the graft ever got a
            // chance to run. A missing `modelDef` cannot even attempt a
            // classification, so it rejects the same as an unresolvable one.
            const resolution = this.modelDef
               ? await this.resolveGateShape(entry, this.modelDef, graftScope)
               : ({ shape: "rejected", cause: undefined } as const);
            if (resolution.shape === "row_level") continue;
            // Same decision counter `authorizeAndBindRunnable` books for its
            // own fail-closed refusals: a rejection is a rejection wherever
            // the gate was resolved, and an operator reading
            // `publisher_authorize_row_level_total{decision=
            // "denied_by_gate"}` must not have a whole class of them (every
            // gate refused at SHAPE resolution) silently missing. `cause` is
            // the separate, finer rejection label, not a substitute for it.
            recordRowLevelGateDecision("denied_by_gate");
            if (resolution.cause) recordRowLevelGateRejected(resolution.cause);
            throw new AccessDeniedError(
               `Access denied for source "${entry.label}".`,
            );
         }
         return;
      }
      // Not a declared top-level source (an ad-hoc inline source, a name we
      // could not resolve, or a model with no modelDef): nothing gates it —
      // `#(authorize)` is declared only on a `source:`, and a caller cannot
      // reach the warehouse through one of these without going through
      // restricted mode first (see `getQueryResults`, which rejects inline
      // `duckdb.sql(...)`/`connection.table(...)` before any gate runs), or,
      // for a notebook cell, without the model author having written it
      // themselves.
   }

   /**
    * Counter + audit line for one skipped gate evaluation.
    *
    * Every authorize bypass in the process funnels through here, because both
    * places a bypass can be honoured short-circuit into it: the surface-name
    * gate ({@link assertAuthorized}) and {@link authorizeAndBindRunnable},
    * which is what actually ENFORCES a gate — by grafting it — and which
    * {@link assertAuthorizedForAllSources} is only a check-shaped wrapper
    * around. So this is the complete audit surface: a bypass that does not
    * appear here did not happen.
    *
    * The identifiers live on the log line rather than the counter labels; see
    * {@link recordAuthorizeBypass} for why. `Model` knows its package and path
    * but not its environment or organization — the router's own audit line on
    * the private endpoint carries those, and the two join on package + model.
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
    * Gate a compiled query on its ENTRY POINT — the source the query runs
    * against. That means the run target's own `#(authorize)`, the gate it
    * carries from the source it derives from (`extend` / query-source
    * derivation, see `./gate_registry_walk`'s `ancestorGateExprs`), and, when
    * the run target is a composite, the one member branch Malloy resolved.
    *
    * Joined sources are NOT gated. Reaching a gated source through `join_*` —
    * at any depth, aliased, cross-file, query-local, or as a composite member —
    * does not bring its gate along. This is deliberate (Q16): authorization is
    * evaluated once, at the entry point, so a gate means "who may query THIS
    * source", not "who may read every byte transitively beneath it". The
    * consequence is that an author who joins sensitive data into an ungated
    * source has published it — the gate belongs on the source callers enter
    * through.
    *
    * Where several gates are collected (a derivation chain, a resolved
    * composite branch), semantics are AND across them: any one failing denies
    * the query, while each source's own expression list stays an OR
    * disjunction.
    *
    * Runs UNCONDITIONALLY — NOT guarded by {@link hasAuthorize}, which only
    * inspects top-level `modelDef.contents` sources and so misses a gate
    * carried in from a derivation base that is not itself top-level. The probe
    * is a cheap no-op for a genuinely ungated model (empty expr list), so there
    * is nothing to save by skipping it.
    */
   public async assertAuthorizedForAllSources(
      runnable: { getPreparedQuery(): Promise<unknown> },
      givens: Record<string, GivenValue>,
      bypassAuthorize = false,
      options?: { checkOnly?: boolean },
   ): Promise<void> {
      // No `recompile`: this method has no runnable to swap in, so it is the
      // right shape only for a caller that wants a check, not a rewritten
      // query to run (the compiled-source backstop used by `/compile` via
      // `assertAuthorizedForRunnable`). A gate therefore denies here rather
      // than admitting unfiltered — see `authorizeAndBindRunnable`, which
      // this delegates to.
      await this.authorizeAndBindRunnable(
         runnable as QueryMaterializer,
         givens,
         {
            bypassAuthorize,
            checkOnly: options?.checkOnly,
         },
      );
   }

   /**
    * The entry-point gate walk shared by {@link assertAuthorizedForAllSources}
    * and {@link authorizeAndBindRunnable}: resolve the run target's own source
    * name and gate it ({@link assertAuthorized}), then resolve the compiled
    * run-target `SourceDef` and collect every gate reachable from it as an
    * entry point (its own annotations, a query-source derivation base, and —
    * when the run target or that base is a composite — the one member branch
    * Malloy resolved). This is the audited answer to "which gate applies";
    * callers differ only in HOW they enforce what comes back (a probe here,
    * a probe-or-graft there), never in what they collect.
    *
    * Gate a compiled query on its ENTRY POINT — the source the query runs
    * against. That means the run target's own `#(authorize)`, the gate it
    * carries from the source it derives from (`extend` / query-source
    * derivation, see `./gate_registry_walk`'s `ancestorGateExprs`), and, when
    * the run target is a composite, the one member branch Malloy resolved.
    *
    * Joined sources are NOT gated. Reaching a gated source through `join_*` —
    * at any depth, aliased, cross-file, query-local, or as a composite member —
    * does not bring its gate along. This is deliberate (Q16): authorization is
    * evaluated once, at the entry point, so a gate means "who may query THIS
    * source", not "who may read every byte transitively beneath it". The
    * consequence is that an author who joins sensitive data into an ungated
    * source has published it — the gate belongs on the source callers enter
    * through.
    *
    * Where several gates are collected (a derivation chain, a resolved
    * composite branch), semantics are AND across them: any one failing denies
    * the query, while each source's own expression list stays an OR
    * disjunction.
    *
    * Runs UNCONDITIONALLY — NOT guarded by {@link hasAuthorize}, which only
    * inspects top-level `modelDef.contents` sources and so misses a gate
    * carried in from a derivation base that is not itself top-level. The probe
    * is a cheap no-op for a genuinely ungated model (empty expr list), so there
    * is nothing to save by skipping it.
    */
   private async collectAuthorizeEntryPointGates(
      runnable: { getPreparedQuery(): Promise<unknown> },
      givens: Record<string, GivenValue>,
      /** The scope a row-level gate found here grafts against — see
       *  {@link GraftScope}. Forwarded to `assertAuthorized` so its own
       *  deferred classification of `runnable`'s OWN source uses the SAME
       *  scope the caller (`authorizeAndBindRunnable`) uses for everything
       *  else it collects here. */
      graftScope: GraftScope | undefined,
      /** Skip the gate keyed on the run target's own SOURCE NAME, keeping
       *  only the gates readable from the compiled ModelDef. Set by
       *  {@link assertAuthorizedFromCompiledRunnable} for a `/compile` of a
       *  path with no cached Model, where the source name would resolve
       *  against an unrelated file. */
      skipOwnSourceGate = false,
   ): Promise<{
      entryPointGates: GateEntry[];
      modelDef: ModelDef | undefined;
   }> {
      const ownSourceName =
         await this.resolveAuthorizeSourceFromRunnable(runnable);
      // Skipped by `assertAuthorizedFromCompiledRunnable`, whose whole point
      // is to gate from the compiled ModelDef WITHOUT borrowing a gate that
      // only the cached Model's source name would supply.
      if (!skipOwnSourceGate) {
         await this.assertAuthorized(ownSourceName, givens, false, graftScope);
      }

      const { struct, modelDef, compositeResolvedSourceDef } =
         await this.resolveRunTargetStruct(runnable);
      const seen = new Set<SourceDef>();
      // `struct` IS the run target itself — its own gate stays ambient-first
      // (`treatAsOwnGate: true`), matching `assertAuthorized` above. Do NOT
      // dedup this against that call: AND-across-sources evaluates every
      // reachable source's gate independently (see the comment below).
      let entryPointGates = this.collectEntryPointGates(
         struct,
         modelDef,
         seen,
         true,
      );
      // Sources joined LOCALLY inside the query's own `-> { join_one: ... }`
      // refinement are NOT gated, for the same reason no other join is.
      if (compositeResolvedSourceDef && modelDef) {
         // The run target itself may be a composite source (`compose(a, b)`).
         // Malloy resolves it to exactly one concrete member branch per query
         // (surfaced as Query.compositeResolvedSourceDef), based on which
         // fields the query references — so gate everything reachable from
         // that RESOLVED branch, not every member. Gating every member would
         // deny access through an open branch just because a sibling branch
         // happens to be locked. This branch stands in for the run target
         // ITSELF (just the concrete resolved shape), so its own gate stays
         // ambient-first too (`treatAsOwnGate: true`) — same reasoning as the
         // `struct` walk above.
         //
         // `compositeResolvedSourceDef && modelDef` is not a fail-open gap for
         // a runnable composite RUN TARGET: Malloy's composite resolver
         // (`_resolveCompositeSources` in `@malloydata/malloy`'s
         // `composite-source-utils.ts`) marks composite resolution as
         // required as soon as the top-level source is itself a composite,
         // before any field-usage logic runs — so it always resolves to a
         // concrete member (even one no query field discriminates, it just
         // picks the first candidate — see the "no field forcing a choice"
         // test in authorize_integration.spec.ts) or the compile fails
         // outright (no member satisfies the query). The `undefined` case
         // here is only ever "the run target genuinely isn't a composite" —
         // not "a composite run target resolved to nothing". (A composite
         // reached via a JOIN is not gated at all — joins are not traced.)
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
      // Fold in THIS model's own on-disk gates for `ownSourceName` — the
      // struct walk above just read `runnable`'s own compiled struct, which
      // reflects whatever text the caller submitted (the `/compile` file/
      // append backstop recompiles caller-edited text against `this`, the
      // authoritative on-disk gateModel, but the run target's STRUCT comes
      // from that edited text). A caller who strips the `#(authorize)`
      // annotation from submitted text compiles a struct with no gate of its
      // own, so the walk alone finds nothing — this is what closes that gap.
      // `entryPointGatesBySource` is computed once from `this.modelDef` (the
      // on-disk model), which caller text cannot edit, so it still finds the
      // gate; `resolveGateShape`'s graft resolves it directly (`entry.struct`
      // is already a `this.modelDef.contents` entry, so `findContentsKey`'s
      // identity match succeeds regardless of what the walk's own `struct`
      // came from), and a row-level gate found this way still denies
      // /compile's no-`recompile` callers same as one the walk found itself.
      // Skipped when `skipOwnSourceGate` is set — that flag means `this`
      // gateModel is unrelated to `ownSourceName` (a new path with no cached
      // Model of its own), so `entryPointGatesBySource` would resolve against
      // the wrong file's namespace.
      //
      // Deduped by CONTENT (`label` + `exprs` + `selfContained`), not struct
      // identity: for an ordinary (non-`/compile`) query, `struct` above is
      // resolved off the runnable's OWN prepared query
      // (`resolveRunTargetStruct`'s `prepared._modelDef`), a fresh compile
      // that does not share object identity with `this.modelDef`'s own
      // `contents` entry even when it describes the identical gate — a
      // struct-identity dedup let a still-intact gate double-apply here
      // (`org_id in $GROUPS` grafted twice, once per copy).
      // A content-equal on-disk entry REPLACES its walk twin here rather
      // than being skipped by it: the on-disk entry's `struct` is a
      // `this.modelDef.contents` value, so `resolveGraftTarget` resolves it
      // by object identity against the "model" graft scope. The walk's
      // entry, for an append-scope `/compile`, comes from that request's
      // own fresh, ephemeral `ModelDef` (see `resolveRunTargetStruct`'s
      // `prepared._modelDef`) — it matches neither by object identity, nor
      // by `sourceID` (minted against the synthetic `__compile_check.malloy`
      // URL, not the on-disk file), nor by annotation-note identity (a
      // separate parse), so `resolveGraftTarget` finds nothing and
      // `resolveGateShapeImpl` returns a causeless `rejected` — denying a
      // caller who supplied every given the gate names. Swapping in the
      // on-disk twin is behavior-preserving on the ordinary query path:
      // there both twins already resolve to the same `graftTarget` (the
      // walk's entry matches by same-URL `sourceID`), so which twin is
      // kept is unobservable.
      if (!skipOwnSourceGate && ownSourceName) {
         const onDiskGates = this.entryPointGatesBySource.get(ownSourceName);
         if (onDiskGates) {
            const keyOf = (entry: GateEntry): string =>
               `${entry.label} ${entry.exprs.join(" ")} ${entry.selfContained}`;
            const byKey = new Map(
               entryPointGates.map((entry) => [keyOf(entry), entry]),
            );
            for (const entry of onDiskGates) {
               byKey.set(keyOf(entry), entry);
            }
            entryPointGates = Array.from(byKey.values());
         }
      }
      return { entryPointGates, modelDef };
   }

   /**
    * Gate only from the compiled runnable's own ModelDef. Used when /compile
    * targets a brand-new path that has no cached Model to provide the
    * source-name gate. The prepared query still carries the imported source
    * definitions and their inherited authorize annotations, so this closes the
    * missing-model fail-open without borrowing an unrelated file-level gate.
    *
    * No `recompile`, for the same reason {@link assertAuthorizedForAllSources}
    * passes none: this is a check, not a query rewrite, so a gate denies here
    * rather than admitting unfiltered.
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
    * Whether `runnable` (a value {@link authorizeAndBindRunnable} returned) has
    * a row-level `#(authorize)` filter attached. Object-identity keyed
    * ({@link rowLevelFilteredRunnables}) rather than a field on `Model`, which
    * is shared across concurrently in-flight requests. Consulted by the query
    * and notebook paths to keep a filtered query off the storage-serve tier
    * and to know whether an empty result is `empty_after_filter`.
    */
   public queryHadRowLevelFilterAttached(runnable: QueryMaterializer): boolean {
      return this.rowLevelFilteredRunnables.has(runnable);
   }

   /**
    * Collect and evaluate every entry-point gate on `runnable`, exactly like
    * {@link authorizeAndBindRunnable} does BEFORE it attempts a graft: every
    * `rejected`-classified gate throws immediately, and every `row_level`
    * gate is DEFERRED — collected and returned rather than evaluated or
    * denied — because there is nothing to enforce it against without a
    * `recompile` step, which is `authorizeAndBindRunnable`'s job, not this
    * one's.
    *
    * Factored out so `executeNotebookCell` can run a PRE-refinement gate
    * call on `cell.runnable` that gets the SAME deferred-row-level
    * treatment `authorizeAndBindRunnable` gives its post-refinement bind,
    * without denying a row-level gate outright (which a raw
    * `authorizeAndBindRunnable` call with no `recompile` would do — correct
    * for `/compile`'s backstop, wrong for a probe that is never the
    * authoritative enforcement point). Never calls `noteAuthorizeBypass`:
    * this helper has no bypass concept of its own, so a caller that invokes
    * it twice (once pre-, once post-refinement, both through
    * `authorizeAndBindRunnable`) never double-counts a bypass audit — only
    * `authorizeAndBindRunnable`'s own top-level `bypassAuthorize` check
    * ever fires that counter, and it short-circuits before reaching here.
    */
   private async probeEntryPointGates(
      runnable: QueryMaterializer,
      givens: Record<string, GivenValue>,
      graftScope: GraftScope | undefined,
      skipOwnSourceGate = false,
   ): Promise<
      Array<{
         label: string;
         graftTarget: string;
         filterText: string;
         condition: FilterCondition;
         givenNames: readonly string[];
      }>
   > {
      const { entryPointGates, modelDef } =
         await this.collectAuthorizeEntryPointGates(
            runnable,
            givens,
            graftScope,
            skipOwnSourceGate,
         );

      // Evaluate each collected gate independently — do NOT dedup by expression
      // text. Two distinct entry-point shapes with identical gate text must each
      // be evaluated, or a non-deterministic gate (e.g. one referencing
      // random()) would be under-enforced (evaluated once, reused). Probes are
      // ~microsecond one-row DuckDB queries, so there is nothing worth deduping.
      // (Cycles/repeat structs are already pruned in collectEntryPointGates
      // by struct identity, so the list holds no literal duplicates.)
      const rowLevel: Array<{
         label: string;
         graftTarget: string;
         filterText: string;
         condition: FilterCondition;
         givenNames: readonly string[];
      }> = [];
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
         // Booked here as well as in `authorizeAndBindRunnable` — see the
         // identical call in `assertAuthorized`: a gate refused at SHAPE
         // resolution is still a fail-closed rejection, and leaving it out
         // would make the decision counter under-report every one of them.
         recordRowLevelGateDecision("denied_by_gate");
         if (resolution.cause) recordRowLevelGateRejected(resolution.cause);
         throw new AccessDeniedError(
            `Access denied for source "${entry.label}".`,
         );
      }
      return rowLevel;
   }

   /**
    * Whether `runnable`'s entry point carries any `#(authorize)` gate — used
    * to decide whether to attempt storage-serve routing at all.
    *
    * This IS the security-relevant check for that decision, not a mere
    * performance pre-filter: the serve-shape model a routed query compiles
    * against (`buildServeShapeModel` in `materialization_serve_transform.ts`)
    * carries no `#(authorize)` annotation bytes at all, so once `runnable` is
    * swapped for the shape's runnable, nothing downstream — including the
    * authoritative walk inside {@link authorizeAndBindRunnable} — can ever
    * discover a gate this call missed. There is no post-hoc undo that can
    * catch a false negative here: `queryHadRowLevelFilterAttached` only sees
    * what the authoritative walk finds, and the authoritative walk runs
    * against whichever struct `runnable` resolves to AT THAT POINT — the
    * annotation-free shape, if routing already happened. A miss here is
    * therefore not "storage routing attempted and then undone" — it is
    * "storage routing attempted and never undone."
    *
    * Every gate is a row filter now (see `authorize.ts`'s module doc), so
    * every gate found here blocks routing — there is no shape left that is
    * safe to route around a `#(authorize)` annotation. Collecting the gate
    * list is therefore the whole check: unlike before, nothing here needs to
    * classify or resolve a graft for any of them.
    *
    * Deliberately does NOT call `assertAuthorized`: this must never itself
    * evaluate or deny a gate (a routing decision must
    * not be able to deny a query the authoritative gate below would have
    * admitted), and must never double-count a gate's metrics.
    *
    * Walks the SAME entry-point traversal ({@link collectEntryPointGates})
    * the authoritative check uses, over the LIVE (unshaped) struct — not a
    * cheaper approximation of it — because that traversal is the only thing
    * standing between a gated entry point and the storage tier. Any
    * exception during the walk fails closed (see the `catch` below): "cannot
    * tell" must block routing, not admit it.
    */
   private async queryEntryPointHasRowLevelGate(runnable: {
      getPreparedQuery(): Promise<unknown>;
   }): Promise<boolean> {
      try {
         const { struct, modelDef, compositeResolvedSourceDef } =
            await this.resolveRunTargetStruct(runnable);
         // The SAME "cannot tell" the catch below refuses, not a "no gate
         // here": `resolveRunTargetStruct` SWALLOWS a `getPreparedQuery()`
         // throw and reports it as `{struct: undefined, modelDef: undefined}`
         // rather than rethrowing, so a failed compile of the LIVE query
         // lands here and never reaches that catch. Returning false would
         // admit exactly what the catch exists to block: the live compile
         // failed so this walk found nothing, while the serve shape's own
         // (different, annotation-free) compile can still succeed and answer
         // from frozen, unfiltered rows with nothing left downstream to
         // discover the gate.
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
         return gates.length > 0;
      } catch {
         // Cannot tell whether the entry point carries a row-level gate — and,
         // once this returns false, nothing downstream can catch a wrong
         // guess (see this method's doc: the shape this decision routes to
         // carries no `#(authorize)` annotation bytes at all, so a walk of it
         // can never discover what this walk missed). Fail closed: block
         // storage routing rather than risk serving frozen, unfiltered rows.
         // The cost of a false positive here is a live query where a routed
         // one would have been cheaper and still correct — the cost of a
         // false negative is a security bypass, so it is not a close call.
         return true;
      }
   }

   /**
    * Deny a request that enters a gated source through a derivation the
    * REQUEST ITSELF declared, when the entry-point walk collected no gate for
    * the entry point actually being run.
    *
    * ## What it is defending
    *
    * The dimension form of `#(authorize)` is discovered as an annotated boolean
    * field on the entry struct's own `fields` and enforced by grafting
    * `where: \`<name>\`` onto that entry BY NAME. A request that declares its
    * own derivation dropping that field — `source: mine is X extend { except:
    * authorized }`, or an `accept:` list that omits it — therefore presents IR
    * with no gate anywhere on it, and nothing downstream catches it: the query
    * boundary admits a derivation over a curated source, and a zero-gate answer
    * also un-blocks storage / pre-aggregation routing. Under the retired string
    * form the annotation sat on the `source:` line and survived a field-level
    * `except:`/`accept:`, so the same text failed CLOSED there. This is the
    * dimension form's own regression, reachable by anyone who can post a query.
    *
    * ## Why the run target comes from the COMPILED query
    *
    * {@link resolveAuthorizeSourceFromRunnable} — not a scan of the request
    * text — decides what is being run, for the same reason the compiled
    * boundary backstop does: Malloy executes the LAST `run:`, reads around
    * comments, and resolves a `query:` indirection, so any of those defeats a
    * first-match text scan in BOTH directions. Reading the first `run:` misses
    * a laundered derivation that a later `run:` actually executes, and it also
    * DENIES a query whose laundered alias is declared but never run (the
    * executed target being some ungated source) — a false positive on text the
    * author published. The compiled target has neither failure mode.
    *
    * ## The default is DENY
    *
    * When the compiled entry point is not a `modelDef.contents` key — the
    * request declared it — this denies unless the scan POSITIVELY establishes a
    * complete chain to model-declared ungated sources
    * ({@link requestChainProvesUngated}). It does not ask "did I find a
    * derivation reaching a gated source"; it asks "did I prove this reaches
    * only ungated ones", and treats every other answer as a denial.
    *
    * That inversion is the substance of this defence, and it is what three
    * earlier rounds got wrong. Asking the question the other way round meant an
    * UNREADABLE declaration read as "not a request-declared derivation" and was
    * admitted, so every divergence between the scan and Malloy's grammar was a
    * full bypass rather than a false positive — and they kept coming: a
    * declaration hidden in a comment or forged in a string literal, then a
    * backtick-quoted name whose contents blanked every declaration after it
    * (an innocuous `` dimension: `q'` is 1 `` was enough), then a parenthesised
    * base, then a non-ASCII name. Each was found after the previous round
    * shipped. With the default inverted, the same class of miss costs an
    * over-denial on a legitimate derivation instead.
    *
    * ## Why the DERIVATION chain is still read from the text
    *
    * Because Malloy does not keep one. Measured against `@malloydata/malloy`:
    * an ad-hoc `source: mine is X extend { … }` compiles to a struct whose
    * `sourceRegistry` entry is a `source_registry_reference` to `mine` ITSELF,
    * whose `referenceID` is unset, and for which `resolveDeclaredSource`
    * answers `{kind: "none"}` — identically for `except: authorized`, for a
    * trivial `extend {}`, and for a derivation of an UNGATED sibling. The IR
    * cannot tell those three apart, so "this entry point's ancestry reaches a
    * gated source" is not a readable property and the decision cannot be made
    * on the IR alone.
    *
    * The coarser IR-only property that IS readable — "the compiled entry point
    * is ephemeral (no `modelDef.contents` key) and reads the same relation as
    * some gated source" — was implemented and measured, and it is wrong: a
    * gated source and the ungated siblings an author deliberately publishes
    * beside it normally read the SAME table, so it denies the documented Q16
    * contract (`source: j is plain extend { join_one: base_locked … }` — joins
    * are not gated) and ad-hoc composition over an ungated source
    * (`source: mine is plain extend { measure: … }`). Both are pinned in
    * `authorize_integration.spec.ts`. So the defence is text-shaped by
    * necessity, not by preference.
    *
    * The scan is still hardened, because a miss now costs a legitimate
    * derivation rather than a leak and that is worth minimising too: comments
    * and string-literal bodies are blanked and backtick spans skipped whole
    * before the scan ({@link stripMalloyCommentsAndLiterals}), the pattern
    * reads Unicode identifiers, an optional parameter list and a parenthesised
    * base, and the map over-collects (every base per name, `query:` included)
    * — which is safe in the same direction, since an extra edge adds a name
    * that must also be proven.
    *
    * Known over-denials, all of the safe kind: a derivation over
    * `compose(...)` or a parameterised source resolves to a base name the walk
    * cannot place, so it denies. Both need an experimental flag to compile at
    * all. A request-declared source over a raw `connection.table(...)` /
    * `.sql(...)` never reaches here — restricted-mode compilation refuses it
    * first.
    *
    * ## The line, and what it costs
    *
    * A MODEL-declared derivation that drops the gate field keeps its
    * documented fail-open (`docs/authorize.md`): the request text declares no
    * alias for it, so it never enters the walk. That is the accepted
    * author-discipline gap, and this does not narrow it.
    *
    * Denying adds no restriction to request-declared shapes that KEEP the gate
    * field: those ALREADY denied — including the trivial `extend {}` and an
    * `accept:` list naming the gate dimension — because
    * {@link resolveGraftTarget} has no `modelDef.contents` key for an ephemeral
    * entry point to graft onto. This makes the gate-DROPPING shapes agree with
    * them instead of being the one spelling that succeeded. It is also why the
    * answer is the cannot-attach 403 and not a 200 with zero rows: there is no
    * gate left to evaluate, so supplying the right givens changes nothing.
    */
   private async assertRequestDeclaredEntryPointIsNotLaundered(
      runnable: QueryMaterializer,
      query: string,
   ): Promise<void> {
      // Nothing to launder unless this model actually declares a gate.
      if (!this.declaresAnyGate()) return;
      const entryPoint =
         await this.resolveAuthorizeSourceFromRunnable(runnable);
      // An unresolvable target is a query that did not compile: it returns no
      // rows, and the compiler's own diagnostic is the right answer. Turning it
      // into a 403 would hide an authoring error behind an access denial while
      // protecting nothing — a caller naming the `internal` gate dimension, or
      // redefining it, lands exactly here.
      if (!entryPoint) return;
      // A MODEL-declared entry point. `entryPointGatesBySource` is keyed by
      // `as ?? name` over every source in `modelDef.contents`, so having a key
      // at all IS "the author declared this source" — and its gates were folded
      // in by name, which makes the empty walk result that brought us here
      // authoritative for it.
      if (this.entryPointGatesBySource.has(entryPoint)) return;
      // Ephemeral: an entry point THIS REQUEST declared. Denied unless the scan
      // POSITIVELY establishes a complete chain to model-declared ungated
      // sources — see this method's doc for why the default is deny.
      const proof = this.requestChainProvesUngated(
         entryPoint,
         buildDerivationBaseMap(stripMalloyCommentsAndLiterals(query)),
      );
      if (proof.proven) return;
      recordRowLevelGateDecision("denied_by_gate");
      logger.debug(
         "Request-declared entry point in a gated model is not provably ungated; denying",
         {
            modelPath: this.modelPath,
            entryPoint,
            reason: proof.reason,
            at: proof.at,
         },
      );
      // Names the compiled entry point the request itself declared — never the
      // gate's column, nor which gated source it may have reached.
      throw new AccessDeniedError(`Access denied for source "${entryPoint}".`);
   }

   /** Memo for {@link declaresAnyGate}. */
   private declaresAnyGateMemo?: boolean;

   /**
    * Whether ANY source in this model carries an entry-point gate.
    *
    * Read off {@link entryPointGatesBySource}, not {@link hasAuthorize}: that
    * one reads top-level sources' OWN gates off the extracted `sources` list
    * and so misses a gate a source only inherits. Note also that
    * `entryPointGatesBySource.size > 0` is NOT this predicate — it is keyed for
    * every declared source, gated or not.
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
    * either to a model-declared source (the terminal this is looking for:
    * ungated proves that branch, gated disproves the whole chain) or to a
    * declaration the scan could read and can keep following. A name that is
    * neither — because the declaration used grammar the pattern cannot read,
    * or because the text never declared it at all — ends the walk with nothing
    * proven, which denies.
    *
    * That direction is the entire point of this function, and it is what makes
    * the pattern pair in `./query_text` non-critical: a divergence between
    * those patterns and Malloy's grammar now costs an over-denial on a
    * legitimate derivation, instead of admitting a laundered read of a gated
    * source. Three rounds of this defence were lost to exactly such
    * divergences — a comment or a literal that hid a declaration, a
    * backtick-quoted name whose contents blanked the declarations after it, a
    * parenthesised base, a non-ASCII name — each of which read as "no
    * derivation declared here" and was therefore admitted.
    *
    * Over-collection in the base map is safe in the same direction: an extra
    * edge adds a name that must also be proven, so a false edge can only deny.
    */
   private requestChainProvesUngated(
      entryPoint: string,
      basesOf: Map<string, Set<string>>,
   ):
      | { proven: true; reason?: undefined; at?: undefined }
      | {
           proven: false;
           reason: "reaches_gated_source" | "chain_not_established";
           at: string;
        } {
      const seen = new Set<string>();
      const worklist = [entryPoint];
      for (let i = 0; i < worklist.length; i++) {
         const name = worklist[i];
         if (seen.has(name)) continue;
         seen.add(name);
         // A chain this long is not a real derivation; stop rather than walk a
         // caller-sized graph, and stop on the deny side.
         if (seen.size > REQUEST_CHAIN_MAX_NAMES) {
            return { proven: false, reason: "chain_not_established", at: name };
         }
         const modelGates = this.entryPointGatesBySource.get(name);
         if (modelGates !== undefined) {
            if (modelGates.length > 0) {
               return {
                  proven: false,
                  reason: "reaches_gated_source",
                  at: name,
               };
            }
            // A model-declared ungated source: this branch is proven, and the
            // walk stops here rather than following the AUTHOR's own
            // derivations, which is what keeps the documented model-authored
            // fail-open intact.
            continue;
         }
         const bases = basesOf.get(name);
         if (!bases || bases.size === 0) {
            return { proven: false, reason: "chain_not_established", at: name };
         }
         for (const base of bases) worklist.push(base);
      }
      return { proven: true };
   }

   /**
    * Authorize a compiled runnable and return THE RUNNABLE TO EXECUTE — the
    * one authoritative entry point for both the probe-only gate
    * ({@link assertAuthorizedForAllSources}, which delegates here with no
    * `recompile`) and the row-level gate, which cannot be enforced by a
    * boolean probe at all: `#(authorize) "org_id in $GROUPS"` has no
    * whole-source admit/deny answer, only a set of rows, so enforcing it
    * means recompiling the caller's UNMODIFIED query text against a model
    * whose entry source carries the condition as a `where:` — the only
    * mechanism that doesn't leak, since appending `+ {where: ...}` to the
    * query text resolves against the caller's own last pipeline stage and
    * can be neutralized by a caller-controlled projection.
    *
    * `bypassAuthorize` short-circuits exactly like the probe-only path does,
    * before any gate is even collected.
    *
    * Every `rejected` gate is handled by {@link probeEntryPointGates} —
    * unchanged from before that method was factored out of this one. Every
    * `row_level` gate it returns is held instead of evaluated inline, because
    * there is nothing to evaluate it AGAINST until every row-level gate on
    * this run target is known: they all graft onto the model at once, below.
    *
    * With no row-level gate collected — this run target carries no gate at
    * all — `runnable` is returned UNCHANGED, so this path is byte-identical
    * to calling the probe-only gate directly.
    *
    * A row-level gate with no `options.recompile` denies — there is no
    * boolean this method can fall back to reporting, and no runnable it can
    * rewrite, so "cannot apply the gate" must refuse rather than admit
    * unfiltered. This is what gives `/compile` its row-level 403 for free: it
    * has nothing to swap in (compiling a gated source is itself a schema+SQL
    * oracle, and there is no row filtering for `/compile` to do), so it calls
    * this method with no `recompile` and inherits the deny.
    *
    * Otherwise: every row-level condition collected for this run target is
    * grafted onto ONE copied, reloaded `ModelDef` ({@link
    * getOrBuildGraftedMaterializer}), `options.recompile` is handed that
    * grafted materializer to produce the runnable to execute, and — before
    * trusting that runnable — {@link assertGateLanded} proves each grafted
    * condition is actually present on the compiled result. Any failure along
    * this path (the graft, the recompile, or the landing proof) denies; the
    * caught error is logged at `debug` with detail, but the thrown
    * `AccessDeniedError` names nothing about the gate — no column, no join —
    * since a gate reading `childtable.name` names a relationship the caller
    * may not otherwise see.
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
          * Replaces {@link assertGateLanded} for a caller whose bind mechanism
          * makes the IR proof vacuous — currently only the notebook own-scope
          * queryDef graft, which mutates the very object the prover would read
          * back. Supplied together with `recompile` by
          * {@link ownScopeQueryDefBinder} so the two cannot drift apart.
          */
         proveGraft?: (recompiled: QueryMaterializer) => Promise<void>;
         bypassAuthorize?: boolean;
         /** The scope a row-level gate on `runnable` grafts against — see
          *  {@link GraftScope}. Defaults to this model's own cumulative
          *  scope ({@link defaultGraftScope}), which is correct for every
          *  caller except a notebook cell, which must pass its OWN per-cell
          *  scope (see `executeNotebookCell`, `graftScopeForCell`). */
         graftScope?: GraftScope;
         /** See {@link assertAuthorizedFromCompiledRunnable}. */
         skipOwnSourceGate?: boolean;
         /**
          * The request's own submitted query text, when it has any — read by
          * {@link assertRequestDeclaredEntryPointIsNotLaundered} for the
          * derivation chain the IR does not keep. Supplied by the ad-hoc query
          * path, the only one that compiles text a CALLER wrote. Deliberately
          * absent for a notebook cell: a cell's text is the AUTHOR's (the
          * request carries only a `cellIndex`), so a source declared there is
          * model content and keeps the author-side fail-open.
          */
         callerQueryText?: string;
         /**
          * The caller will NEVER execute this runnable — it wants a decision,
          * not a query to run (`/compile`). Only such a caller may be handed
          * back an ungrafted runnable when a gate resolved; see the
          * `recompile` branch below.
          */
         checkOnly?: boolean;
      },
   ): Promise<QueryMaterializer> {
      // Returns BEFORE the entry-point walk below, so this books at most one
      // `runnable` emission and never two. It still skips the walk, which is
      // the expensive part (resolveRunTargetStruct + collectEntryPointGates)
      // — but NOT the source-name resolution, which is the only thing that
      // tells an investigator what a bypass actually read. An ad-hoc query is
      // exactly the case where the caller-side name is unavailable and this
      // is the sole record of the target.
      if (options?.bypassAuthorize) {
         this.noteAuthorizeBypass(
            "runnable",
            await this.resolveAuthorizeSourceFromRunnable(runnable),
         );
         return runnable;
      }

      const graftScope = options?.graftScope ?? this.defaultGraftScope();
      const rowLevel = await this.probeEntryPointGates(
         runnable,
         givens,
         graftScope,
         options?.skipOwnSourceGate ?? false,
      );

      if (rowLevel.length === 0) {
         // No gate was collected for the entry point actually being run. When
         // the request declared that entry point itself, that is not the same
         // as "the run target is ungated" — see
         // {@link assertRequestDeclaredEntryPointIsNotLaundered}.
         //
         // Skipped under `skipOwnSourceGate` for the same reason the on-disk
         // fold is: that flag means `this` gateModel is unrelated to the path
         // being served, so `entryPointGatesBySource` — which the check reads
         // to find the gated relations — is the wrong file's namespace.
         if (options?.callerQueryText && !options?.skipOwnSourceGate) {
            await this.assertRequestDeclaredEntryPointIsNotLaundered(
               runnable,
               options.callerQueryText,
            );
         }
         return runnable;
      }

      if (!options?.recompile) {
         // No `recompile` means no way to attach the filter, so returning
         // `runnable` to a caller that will RUN it serves unfiltered rows.
         // A `checkOnly` caller runs nothing, and for it a refusal here was
         // strictly harsher than the query path: since every gate became a
         // row filter, a query against a gated source answers with FILTERED
         // rows, never a 403. Denying `/compile` therefore protected nothing
         // the query path protects while making a gated source
         // un-authorable — the same class of breakage as gating `/compile`
         // on the query boundary (see `environment.ts`'s note on the QA
         // session where every per-file compile 404'd).
         //
         // Admitted only when the gate is decided WITHOUT running it: a gate
         // that references NO given (`authorized is true`, `authorized is 1
         // = 1`) is decided by construction — `/compile` executes nothing, so
         // there is no caller value left to wait on — or one whose every
         // given the caller actually supplied — the author's own authoring
         // loop. A gate whose givens are missing still denies, so an
         // anonymous caller learns nothing new about a gated source.
         const decidable = (g: (typeof rowLevel)[number]) =>
            g.givenNames.length === 0 ||
            g.givenNames.every((name) => Object.hasOwn(givens, name));
         if (!options?.checkOnly || !rowLevel.every(decidable)) {
            recordRowLevelGateDecision("denied_by_gate");
            throw new AccessDeniedError(
               `Access denied for source "${rowLevel[0].label}".`,
            );
         }
         return runnable;
      }

      try {
         // `rowLevel` is non-empty only when `resolveGateShape` classified a
         // gate as `row_level`, which requires a defined `graftScope` (see
         // its own `if (!graftScope) return { shape: "rejected" }`) — so
         // `graftScope` is never actually undefined here.
         const graftedMaterializer = this.getOrBuildGraftedMaterializer(
            rowLevel,
            graftScope!,
         );
         const recompiled = options.recompile(graftedMaterializer, rowLevel);
         if (options.proveGraft) {
            await options.proveGraft(recompiled);
         } else {
            await this.assertGateLanded(recompiled, rowLevel);
         }
         this.rowLevelFilteredRunnables.add(recompiled);
         return recompiled;
      } catch (err) {
         recordRowLevelGateDecision("denied_by_gate");
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
    * Resolve the run-target `SourceDef` and its `ModelDef`, for walking joined
    * sources. `prepared._modelDef` is the modelDef the query actually compiled
    * against (falls back to `this.modelDef`); a string `structRef` resolves
    * through `modelDef.contents`. Also surfaces `compositeResolvedSourceDef` —
    * when the run target is itself a composite source (`compose(a, b)`), this
    * is the ONE concrete member branch Malloy resolved the query against (see
    * {@link assertAuthorizedForAllSources}). Returns `undefined`s if these
    * can't be resolved — callers treat that as "no further gate to check"
    * rather than denying, since {@link assertAuthorizedForAllSources}'s
    * own-source gate above is still the authoritative deny for an unresolvable
    * target.
    */
   private async resolveRunTargetStruct(runnable: {
      getPreparedQuery(): Promise<unknown>;
   }): Promise<{
      struct: SourceDef | undefined;
      modelDef: ModelDef | undefined;
      compositeResolvedSourceDef: SourceDef | undefined;
   }> {
      try {
         const prepared = (await runnable.getPreparedQuery()) as {
            _query?: {
               structRef?: unknown;
               compositeResolvedSourceDef?: SourceDef;
            };
            _modelDef?: ModelDef;
         };
         const modelDef = prepared._modelDef ?? this.modelDef;
         if (!modelDef)
            return {
               struct: undefined,
               modelDef: undefined,
               compositeResolvedSourceDef: undefined,
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
         };
      } catch {
         // Not fail-open: if getPreparedQuery() throws here, execution's own
         // getPreparedResult()/run() (same compilation) throws too, so no data
         // is returned. Safety depends on execution sharing this compilation.
         return {
            struct: undefined,
            modelDef: undefined,
            compositeResolvedSourceDef: undefined,
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
    * The {@link GateClassificationDeps} every per-instance gate-classification
    * wrapper below supplies to `./gate_classification`'s free functions —
    * see {@link gateClassificationDepsCache}'s doc for why this model builds
    * exactly one, lazily, and reuses it for its own life rather than
    * reassembling one from separately-cached fields on each call. A one-shot
    * build-time caller instead calls {@link createGateClassificationDeps}
    * itself, once, so it neither leaks entries into nor inherits them from a
    * request-serving `Model`'s.
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
      | { shape: "rejected"; cause?: RowLevelGateRejectionCause }
   > {
      const result = await resolveGateShapeImpl(
         entry,
         originModelDef,
         graftScope,
         this.gateClassificationDeps(),
      );
      // Widen `authorizeReferencedGivenNames` with whatever THIS
      // classification resolved — a source-line gate's given names are only
      // knowable post-lift, via the compiled condition's own `refSummary`.
      // (`computeAuthorizeReferencedGivenNames` still captures names up front
      // for every gate, but by TEXT-scanning `entry.exprs`, so it cannot see a
      // name that only surfaces in the compiled condition.) Without this, a
      // source-line field-reference gate's opaque-403 backstop
      // (`authorizeReferencedGivenNames`, `model.ts`'s "Gate given unbound;
      // denying opaquely" check) never learns the given it reads, and a
      // caller who omits it sees Malloy's raw compile error naming the given
      // instead. Additive and idempotent (a `Set`), and always runs BEFORE
      // the graft this same request builds is ever executed, cache hit or
      // miss — safe to widen unconditionally.
      if (result.shape === "row_level") {
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
    * Build (or reuse) the `ModelMaterializer` every row-level gate on this run
    * target grafts onto, keyed on `graftScope.cacheScope` + the sorted set of
    * `(graftTarget, filterText)` pairs in {@link graftedMaterializerCache}.
    * The graft depends only on WHICH source carries WHICH conditions — never
    * on the caller's givens or query text, which bind later, at `run()` —
    * so every request that hits the same gate set IN THE SAME SCOPE reuses
    * the same grafted materializer instead of paying the deep copy again.
    */
   private getOrBuildGraftedMaterializer(
      grafts: ReadonlyArray<{
         graftTarget: string;
         filterText: string;
         condition: FilterCondition;
      }>,
      graftScope: GraftScope,
   ): ModelMaterializer {
      // Joined on U+0001, distinct from the U+0000 separator inside each
      // pair -- joining on "" would let two DIFFERENT graft sets produce the
      // same key whenever a graftTarget/filterText boundary in one set lines
      // up with a different boundary in another, which would serve one gate
      // set's cached materializer for another's request. `cacheScope` is
      // prefixed with its own U+0000 separator for the same reason
      // `resolveGateShape`'s cache key uses one -- see {@link GraftScope}.
      const key =
         `${graftScope.cacheScope}\u0000` +
         grafts
            .map((g) => `${g.graftTarget}\u0000${g.filterText}`)
            .sort()
            .join("\u0001");
      const materializer = this.graftedMaterializerCache.get(key);
      if (materializer) {
         // Re-insert so Map iteration order is least-recently-used first;
         // that ordering is what makes the eviction below an LRU rather
         // than a FIFO, which would evict the hot model-wide scope in
         // favour of whichever notebook cell ran last.
         this.graftedMaterializerCache.delete(key);
         this.graftedMaterializerCache.set(key, materializer);
         return materializer;
      }
      const built = this.buildGraftedMaterializer(grafts, graftScope.modelDef);
      this.graftedMaterializerCache.set(key, built);
      // Evict AFTER inserting, so the entry just built is never the one
      // dropped. Each held entry is a multi-MiB ModelDef clone -- see
      // GRAFTED_MATERIALIZER_CACHE_MAX for the measurement.
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
    * The deep copy (`structuredClone`) is what makes this safe across
    * concurrent requests: `modelDef` itself is NEVER mutated, so a request
    * whose gate set differs — or one with no row-level gate at all — keeps
    * compiling against the original, untouched model.
    *
    * P0 — this graft is safe ONLY because it is scoped to the gates
    * {@link collectAuthorizeEntryPointGates} collected for THIS run target.
    * Appending to a source's `filterList` propagates into every join copy
    * compiled from it afterward — grafting a source that is not on the run
    * target's own entry-point ancestry would leak the filter into (or out of)
    * an unrelated query through that propagation. An ungated parent joining a
    * gated child collects no gate for the child, so nothing is grafted here
    * and the child's gate correctly does not fire through the join. Do not
    * "simplify" this by grafting every gated source in the model up front —
    * that reintroduces exactly the leak this scoping exists to prevent.
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
         // ARRAYS by reference across its spread copies (malloy-element.js,
         // named-source.js, dynamic-space.js) and `structuredClone` preserves
         // that aliasing, so pushing would land this gate on join copies of
         // the source too — the exact leak the P0 note above forbids.
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
    * A model-level `query: tile is gated -> {…}` stores its own
    * `NamedQueryDef.structRef`, and when `gated` came from an `import` that
    * `structRef` is an INLINED copy of the struct rather than a name (see this
    * file's `authorize_import_hop` spec header). `query-reference.js` reuses
    * that stored def verbatim, so grafting `contents[graftTarget]` alone never
    * reaches it and the gate silently fails to attach — which the prover then
    * correctly turns into a denial of an authorized caller. This is the
    * documented single-query dashboard shape (`docs/dashboards.md`), so it is
    * not a corner case.
    *
    * Deliberately narrow, and each limit is load-bearing rather than caution:
    *  - `sourceID` ONLY, never `referenceID` — a join copy carries the base's
    *    `referenceID` (named-source.js), so matching that would graft the gate
    *    onto joins and leak it into unrelated queries.
    *  - APPEND to the snapshot; never repoint `structRef` at the name, which
    *    would drop the reference site's own `sourceArguments`
    *    (query_model_impl.js) and silently run a parameterized source on its
    *    declaration defaults.
    *  - NO recursion — not into `fields` (joins, per the P0 note on
    *    {@link buildGraftedMaterializer}), not into the query's `pipeline`,
    *    and not into a `CompositeSourceDef`'s `sources[]`. Composite member
    *    snapshots are a separate, currently fail-CLOSED case.
    *
    * Idempotent by `condition.code`, so re-grafting a cached clone cannot
    * stack the same filter twice.
    *
    * Known behaviour change, and the reason this is called out in review: for
    * a gated `query_source` the filter now applies INSIDE the stored query's
    * own run target, so aggregates over it change from all-rows to
    * caller-rows. That is the correct number, but it IS a change.
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
         if (struct.sourceID !== sourceID) continue;
         if (struct.filterList?.some((f) => f.code === condition.code))
            continue;
         // Spread-assign, never `push` — same aliasing reason as the graft
         // site above.
         struct.filterList = [...(struct.filterList ?? []), condition];
      }
   }

   /**
    * Prove every grafted condition actually landed on the recompiled query's
    * run target — the backstop that turns a graft which silently failed to
    * attach into a REFUSAL instead of a leak.
    *
    * Read what this does and does not cover. It inspects the recompiled
    * query's IR, NOT the SQL that executes. So it catches the graft failing
    * to reach the run target (the import-hop snapshot bugs), but it CANNOT
    * catch a future Malloy change that keeps `filterList` in the IR and stops
    * honoring it during SQL generation. Do not describe it as proof against
    * "Malloy no longer honoring the graft" — five vacuous tests have already
    * shipped on this feature, and overstating the last backstop is how the
    * sixth gets written.
    * Resolves the recompiled query's run-target `SourceDef` the same way
    * {@link resolveRunTargetStruct} does, then, for each grafted condition,
    * matches its `code` against that struct's own `filterList[].code` —
    * recursing into a `query_source`'s `structRef` base when it isn't there.
    * That recursion is necessary, not defensive: for `source: qs is X ->
    * {…}`, the EXECUTED source's own `filterList` is empty (the filter was
    * consumed inside the inner pipeline), but it IS present on `X`, which is
    * exactly where {@link resolveGraftTarget} put it. Bounded to
    * {@link MAX_GATE_PROOF_DEPTH}; any failure to find a condition — missing,
    * unresolvable base, depth exhausted — is treated as ABSENT.
    *
    * `prepared._modelDef` falls back to `this.modelDef`, matching
    * {@link resolveRunTargetStruct}'s identical fallback — a query-source's
    * recursion above needs SOME modelDef to resolve `query.structRef`
    * through, and a missing one must mean "cannot prove the graft landed"
    * (deny), never "assume it landed" (leak).
    *
    * Throws (denying, via the caller's catch) if any condition is not found.
    */
   private async assertGateLanded(
      recompiled: QueryMaterializer,
      grafts: ReadonlyArray<{ condition: FilterCondition }>,
   ): Promise<void> {
      const prepared = (await recompiled.getPreparedQuery()) as {
         _query?: {
            structRef?: unknown;
            compositeResolvedSourceDef?: unknown;
         };
         _modelDef?: ModelDef;
      };
      const modelDef = prepared._modelDef ?? this.modelDef;
      // MIRROR the compiler's own precedence, never a union of the two.
      // Malloy generates SQL from `compositeResolvedSourceDef ?? structRef`
      // (query_model_impl.js:69, :141-143, query_query.js:608). Reading only
      // `structRef` let the proof pass on a name-resolved struct while a
      // STALE resolved composite was what actually generated SQL — unfiltered
      // rows behind a green proof. Checking "either one carries the filter"
      // would reintroduce exactly that leak from the other side, so this
      // picks the one the compiler picks and checks only that one.
      const structRef =
         prepared._query?.compositeResolvedSourceDef ??
         prepared._query?.structRef;
      const resolvedRef =
         typeof structRef === "string"
            ? modelDef?.contents[structRef]
            : structRef;
      // Same resolution `resolveRunTargetStruct` uses for a live query's run
      // target: a string `structRef` names a `contents` entry, an object one
      // already IS the struct.
      const struct =
         resolvedRef && typeof resolvedRef === "object"
            ? (resolvedRef as SourceDef)
            : undefined;
      for (const { condition } of grafts) {
         if (
            !condition.code ||
            !this.filterListContainsCode(struct, modelDef, condition.code, 0)
         ) {
            throw new Error(
               "a row-level gate condition did not land on the recompiled query",
            );
         }
      }
   }

   /**
    * The bind pair for a notebook cell that must graft against its OWN scope:
    * a `recompile` that grafts the cell's already-compiled queryDef, and the
    * `proveGraft` that checks it. Returned together because supplying one
    * without the other is a silent fail-OPEN — see `prove` below.
    *
    * Why the queryDef is grafted rather than recompiled. A cell that both
    * brings in a gated source and runs it in the SAME cell has no earlier
    * cell to graft against, so its own model is the graft scope. That scope
    * cannot be re-entered by recompiling the cell's text: the grafted clone is
    * loaded from a ModelDef and therefore has no base URL, so a relative
    * `import` in that text fails to resolve outright ("In order to use
    * relative imports, you must compile a file via a URL") — which is exactly
    * what an import-and-run cell contains. `_loadQueryFromQueryDef` binds the
    * already-compiled queryDef instead, translating nothing.
    *
    * The cell's queryDef holds an INLINED copy of the run-target struct (its
    * source arrived unexported through an `import`), so the condition is
    * appended to that copy. The copy is deep-cloned first: `cell.runnable` is
    * memoized across requests, and mutating it in place would leak one
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
               // The cell DECLARES its own gated source, so the run target is
               // a NAME. `_loadQueryFromQueryDef` resolves that name against
               // the grafted clone, which already carries the condition — no
               // queryDef surgery needed, and `assertGateLanded` stays fully
               // honest because resolution goes through `contents`. This is
               // the long-standing own-scope case; leave it exactly as it was.
               return (
                  materializer as HydrationMaterializer
               )._loadQueryFromQueryDef(clone);
            }
            if (!snapshot || typeof snapshot !== "object") {
               // Neither a name nor a struct is a shape we can prove — DENY
               // rather than run ungated.
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
          * Be precise about what this proves, because half of it proves
          * nothing. Re-reading the condition off `graftedStruct` is VACUOUS —
          * it is the array this binder just wrote. The load-bearing half is
          * the object-IDENTITY check: it asserts the struct the materializer
          * will actually execute is the one we grafted, so a future
          * `_loadQueryFromQueryDef` that copies, re-derives, or substitutes
          * its argument turns into a denial instead of an ungated query.
          *
          * When the run target was a NAME, nothing was grafted here and this
          * defers to {@link assertGateLanded}, which is non-vacuous on that
          * path because the name resolves through the grafted `contents`.
          *
          * Neither branch proves Malloy still HONORS `filterList` during SQL
          * generation — no IR-level check can, here or on the ordinary
          * {@link assertGateLanded} path. That property is pinned by the
          * row-count and generated-SQL assertions in
          * `authorize_import_hop.integration.spec.ts`, which is where a
          * behavioural property belongs.
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
      if (!struct || depth > Model.MAX_GATE_PROOF_DEPTH) return false;
      if (struct.filterList?.some((f) => f.code === code)) return true;
      const duck = struct as unknown as {
         type: string;
         query?: {
            structRef?: SourceDef | string;
            compositeResolvedSourceDef?: SourceDef | string;
         };
      };
      if (duck.type === "query_source" && modelDef) {
         // Same mirrored precedence as `assertGateLanded` — see the note
         // there. The inner pipeline generates SQL from the resolved
         // composite when it has one, so proving against the unresolved
         // `structRef` would prove the wrong struct.
         const ref =
            duck.query?.compositeResolvedSourceDef ?? duck.query?.structRef;
         const base = typeof ref === "string" ? modelDef.contents[ref] : ref;
         if (base && isSourceDef(base)) {
            return this.filterListContainsCode(base, modelDef, code, depth + 1);
         }
      }
      return false;
   }

   /**
    * Gate ad-hoc compile/query text by the named source it targets. Resolves the
    * source from surface syntax (`extractRunTargetSourceName`) and applies the
    * gate. An unnamed/inline source resolves to `undefined`, so nothing gates
    * it — the same top-level-only boundary as the query path's early gate.
    * Used by the `/compile` path, which has no runnable to resolve before it
    * decides whether to compile at all.
    *
    * Takes no bypass argument, deliberately. `/compile` returns schema and, with
    * `includeSql`, SQL; no caller needs to compile through a gate, so the
    * parameter is not plumbed here rather than plumbed and defaulted off. Adding
    * it back should require deciding to, not one word in a call site.
    */
   public async assertAuthorizedForText(
      text: string,
      givens: Record<string, GivenValue>,
   ): Promise<void> {
      await this.assertAuthorized(extractRunTargetSourceName(text), givens);
   }

   /**
    * Gate a compiled query by the source it actually reads, resolved from the
    * prepared query's `structRef` (authoritative — survives named-query and
    * multi-statement indirection that surface syntax misses, e.g. the executed
    * `run:` statement isn't the first one), PLUS the gate that run target
    * carries from what it derives from (see assertAuthorizedForAllSources).
    * Used as the `/compile` backstop once a runnable exists, so `/compile`
    * applies the same entry-point rule as the query path — including its
    * "joins are not gated" consequence.
    *
    * No bypass argument, for the same reason as {@link assertAuthorizedForText}.
    */
   public async assertAuthorizedForRunnable(
      runnable: { getPreparedQuery(): Promise<unknown> },
      givens: Record<string, GivenValue>,
   ): Promise<void> {
      await this.assertAuthorizedForAllSources(runnable, givens, false, {
         checkOnly: true,
      });
   }

   /**
    * Resolve the source a compiled query reads, from its prepared query's
    * `structRef`. This is authoritative — it survives named-query indirection
    * and bare `run: <query>` forms that surface-syntax extraction misses — so
    * the authorize gate can't be dodged by how a request names the query.
    * Returns undefined if the source can't be determined.
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
    * Best-effort extraction of a source name from an ad-hoc Malloy query string.
    * Matches patterns like `run: source_name -> ...` or `source_name -> ...`.
    */
   /**
    * Resolve the run target of an ad-hoc query to the model-defined source
    * whose filters apply, following source-derivation declarations so that a
    * filter-protected source carries its filter requirements when read under a
    * derived name. The declared filter belongs to the source, not to the name
    * it is read under. Returns undefined when the run target does not derive
    * from a protected source.
    */
   private resolveFilterSource(query?: string): string | undefined {
      const target = extractRunTargetSourceName(query);
      if (!target || !query) return undefined;

      const aliasOf = buildSourceAliasMap(query);

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
    * Compile a single model in-process. Kept as a library entry point
    * for test fixtures and any future caller that needs an ad-hoc
    * `Model` from a `.malloy` / `.malloynb` file. Production package
    * loads (`Package.create`) and reloads (`Package.reloadAllModels`)
    * route through the package-load worker pool and dispatch through
    * {@link Model.fromSerialized} instead — neither calls this on the
    * main thread.
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
      const { runtime, modelURL, importBaseURL, dataStyles, modelType } =
         await Model.getModelRuntime(
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
            // Malloy's `Model.givens` already collapses inheritance from imports
            // and applies any `finalizeGivens` runtime config. Just read it.
            const malloyGivens = Array.from(
               compiledModel.givens.values(),
            ) as MalloyGiven[];
            givens =
               malloyGivens.length > 0
                  ? (malloyGivens.map(malloyGivenToApi) as ApiGiven[])
                  : undefined;
            const sourceResult = Model.getSources(modelDef, givens);
            sources = sourceResult.sources;
            filterMap = sourceResult.filterMap;
            const queryResult = Model.getQueries(modelDef);
            queries = queryResult.queries;

            // A `#(authorize)` annotation in a position nothing enforces (a
            // top-level `query:` statement, or a field inside a `source:`
            // rather than the `source:` line itself) fails OPEN — see
            // `assertNoMisplacedAuthorizeAnnotations`'s doc. Checked before
            // `validateAuthorizeProbes` below so this specific mistake gets
            // its own message rather than loading as if the gate were fine.
            assertNoMisplacedAuthorizeAnnotations([
               ...sourceResult.misplacedAuthorize,
               ...queryResult.misplacedAuthorize,
            ]);
            // The string form is refused outright — see `findLegacyStringGates`'s
            // doc. Checked before `validateAuthorizeProbes` so an author gets the
            // rewrite instead of the string-form machinery's own error surface.
            const legacyStringGates = findLegacyStringGates(
               sourceResult.authorizeOwnNotes,
            );
            legacyStringGates.forEach(() =>
               recordRowLevelGateRejected("legacy_string_gate"),
            );
            assertNoLegacyStringGate(legacyStringGates);
            // A source may declare at most one `#(authorize)` block — see
            // `findMultipleAuthorizeGates`'s doc. Same check as the
            // package-load worker.
            assertAtMostOneAuthorizeGate(
               findMultipleAuthorizeGates(sourceResult.authorizeOwnNotes),
            );
            // Translation-time validation of #(authorize) annotations (shared
            // with the package-load worker so both compile paths validate
            // identically). Compiling the probe surfaces unknown givens and
            // source-field references at model-load instead of first request.
            // `validateAuthorizeProbes` widens to `authorizeMap` (every entry
            // point, inheritance included) for shape-aware, per-entry-point
            // validation — see its doc comment.
            //
            // `const` (not the outer `let modelDef`) so the narrowed
            // non-undefined type survives into the closures below.
            const compiledModelDef: ModelDef = modelDef;
            await validateAuthorizeProbes(modelMaterializer, {
               authorizeMap: sourceResult.authorizeMap,
               authorizeOwnNotes: sourceResult.attributedAuthorizeOwnNotes,
               onRowLevelGateRejected: recordRowLevelGateRejected,
               // A gate genuinely inherited (not declared) at this entry
               // point — its own gate note objects are shared, by
               // reference, with a base, or it carries no annotation of its
               // own at all — that could not be expressed here; see
               // `validateAuthorizeProbes`'s doc. Not fatal, so surface it as
               // a warning an operator or author can act on rather than
               // losing it silently.
               onRowLevelGateUnexpressible: (sourceName, detail) =>
                  logger.warn(
                     "Row-level #(authorize) gate not expressible at this entry point; every query against it will be denied",
                     { packageName, modelPath, sourceName, detail },
                  ),
               // G4/W1/W2 for the SOURCE-LINE form, run at EVERY entry
               // point whose probe compiled (see `validateAuthorizeProbes`'s
               // doc on this callback) -- `sourceName` may be an inheritor,
               // not only the DECLARING source. `compiledModelDef
               // .contents[sourceName]` is the same struct the probe was
               // grafted onto, so `refSummary` is already resolved against
               // it either way.
               onOwnRowLevelConditionCompiled: (sourceName, condition) => {
                  const struct = compiledModelDef.contents[sourceName];
                  if (!struct || !isSourceDef(struct)) return;
                  validateSourceLineGateGivenUsage(
                     sourceName,
                     struct,
                     condition.refSummary as ExpandableRefSummary | undefined,
                     condition.e,
                     compiledModelDef,
                     (cause, detail) => {
                        recordRowLevelGateRejected(cause);
                        logger.warn("Row-level #(authorize) gate warning", {
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

            // Collect sourceInfos from imported models first
            // This follows the same pattern as notebook imports handling
            const imports = modelDef.imports || [];
            const importedSourceNames = new Set<string>();
            for (const importLocation of imports) {
               try {
                  const modelString = await runtime.urlReader.readURL(
                     new URL(importLocation.importURL),
                  );
                  const importedModelDef = (
                     await runtime
                        .loadModel(modelString as string, { importBaseURL })
                        .getModel()
                  )._modelDef;
                  const importedModelInfo =
                     modelDefToModelInfo(importedModelDef);
                  const importedSources = importedModelInfo.entries.filter(
                     (entry) => entry.kind === "source",
                  ) as Malloy.SourceInfo[];
                  for (const source of importedSources) {
                     if (!importedSourceNames.has(source.name)) {
                        sourceInfos.push(source);
                        importedSourceNames.add(source.name);
                     }
                  }
               } catch (importError) {
                  // Log but don't fail if we can't load an import's sourceInfo
                  logger.warn("Failed to load sourceInfo from import", {
                     importURL: importLocation.importURL,
                     error: importError,
                  });
               }
            }

            // Add locally-defined sources (not already added from imports)
            const localModelInfo = modelDefToModelInfo(modelDef);
            const localSources = localModelInfo.entries.filter(
               (entry) => entry.kind === "source",
            ) as Malloy.SourceInfo[];
            for (const source of localSources) {
               if (!importedSourceNames.has(source.name)) {
                  sourceInfos.push(source);
               }
            }
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
         // Same runtime the materializer above was loaded from — see
         // `setGateRuntime`. Cast for the same reason `fromSerialized`'s
         // `makeHydrationRuntime` casts: `_loadModelFromModelDef` is an
         // internal Malloy method this file's public `Runtime` import doesn't
         // declare, but it is the same underlying `Runtime` instance either way.
         model.setGateRuntime(runtime as HydrationRuntime);
         return model;
      } catch (error) {
         let computedError = error;
         if (error instanceof Error && error.stack) {
            logger.error("Error stack", error.stack);
         }

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
    * Construct a `Model` from a worker-compiled `SerializedModel`. All
    * the heavy compile work (parse, type check, IR build, sourceInfo
    * extraction, per-cell notebook compile) already ran inside a
    * `worker_threads` worker; this factory just rewraps the wire data
    * into a live `Model`.
    *
    * Hydrates the `ModelMaterializer` (and, for notebooks, the
    * per-cell materializers + runnables) **eagerly** via
    * `Runtime._loadModelFromModelDef` /
    * `ModelMaterializer._loadQueryFromQueryDef`. These are constant-time
    * wraps around the worker's pre-compiled `modelDef` / `queryDef` —
    * no parse, no type-check, no schema fetch — so doing it here at
    * package-load time costs microseconds per model and keeps the
    * resulting `Model` interchangeable with one produced by
    * `Model.create` (no lazy-init branches in the hot path).
    */
   public static fromSerialized(
      packageName: string,
      _packagePath: string,
      malloyConfig: ModelConnectionInput,
      data: SerializedModel,
      options?: { buildManifest?: BuildManifest["entries"] },
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

      // Non-fatal `#(authorize)` findings the worker collected (see
      // `SerializedModel.authorizeWarnings`'s doc) — the worker has no
      // logger, so they ride over the wire as strings for this thread, which
      // does, to log once per model hydration.
      for (const warning of data.authorizeWarnings ?? []) {
         logger.warn(warning, { packageName, modelPath: data.modelPath });
      }

      // No modelDef → either an empty notebook (no MALLOY statements)
      // or a corrupt worker payload. Build a Model with no materializer;
      // downstream getQueryResults / executeNotebookCell will throw a
      // clean BadRequestError if a caller tries to run a query. We
      // still preserve markdown cells for an all-markdown notebook so
      // `getNotebook()` can serve raw text.
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
      // The connections a serve shape compiles against are NOT this model's: the
      // owning Package pushes the environment's storage destinations
      // instead (see Package.setServeDestinationConfig). Capturing `malloyConfig`
      // here would put a destination in reach of the author's own namespace.
      // Retain the SAME runtime `modelMaterializer` above hydrated from — see
      // `setGateRuntime`. A row-level gate grafts by copying `modelDef` and
      // reloading it through this runtime, so it has to be the one that
      // shares this model's given identities, not a fresh one.
      model.setGateRuntime(runtime);
      return model;
   }

   /**
    * Build a Model representing a compilation failure (no modelDef,
    * no materializer). Matches the shape `Model.create` returns when
    * it catches a `MalloyError`, so the rest of the system handles
    * both paths uniformly (the iteration loop in `Package.create`
    * reads `compilationError` via a structural cast).
    */
   public static fromCompilationError(
      packageName: string,
      modelPath: string,
      modelType: ModelType,
      error: Error,
   ): Model {
      return new Model(
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
    * This file's own model-level `queryMetadata` declaration, or null if it
    * declares none. Covers both the `## materialization.queryMetadata.*` form
    * and the bare `## queryMetadata.*` one beneath it, since
    * {@link composeDeclaredQueryMetadata} reads the same two layers the build
    * path does.
    *
    * Exposed for the publish gate. That gate walks the package manifest and the
    * build plan's persist sources, so a model file's declaration was only ever
    * visible through a source that inherited it — and a file with NO persist
    * source was invisible entirely. Since a model-file declaration now rides
    * served queries whether or not the file persists anything, it needs a
    * validation path of its own.
    *
    * Memoized, because the caller is not the cold path it looks like:
    * `getPackageMetadata()` runs once per package inside `listPackages`, and
    * several callers invoke it only to read `manifestLocation`. Recomputing
    * would walk the import closure and re-parse every `##` note on each of those.
    * A compiled model's annotations never change — a reload replaces the `Model`
    * object outright — so the memo needs no invalidation.
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
    * declare one.
    *
    * `queryMetadata` is a sibling of `persist` in the `#@` namespace rather than
    * a key inside it, so a source that persists nothing can declare tags and
    * they ride every query against it. The publish gate reads persist sources
    * from the build plan, so those declarations had no validation path — a
    * reserved or malformed name published clean and vanished with only a metric
    * behind it.
    *
    * Memoized for the same reason as {@link getDeclaredQueryMetadata}: the
    * caller runs once per package inside `listPackages`.
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
    * (`modelDef.exports`) for discovery. This mirrors what Malloy's stable
    * `modelDefToModelInfo` already does for `modelInfo`/`sourceInfos` (and what
    * the app renders), so the publisher-extracted `sources`/`queries` stay
    * consistent with `modelInfo` within a single response. A model with no
    * `export { … }` has `exports` = all top-level names, so this is a no-op
    * there. Only active when the package declares `explores` (see
    * `discoveryCurationEnabled`). `this.sources`/`this.queries` stay complete so
    * #(authorize)/filter enforcement and join/extend resolution are unaffected.
    * Whether a non-exported source is also non-*queryable* depends on the
    * package's `queryableSources` policy — see {@link assertQueryBoundaryEarly}.
    */
   private curateForDiscovery<T extends { name?: string }>(
      items: T[] | undefined,
   ): T[] | undefined {
      if (!items) return items;
      if (!this.discoveryCurationEnabled) return items;
      const exports = this.modelDef?.exports;
      if (!Array.isArray(exports)) return items;
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
    * Set by the owning Package (see Package.wireFreshnessResolvers). Supplies
    * the freshness-filtered build manifest the serve path threads into Malloy's
    * per-query `buildManifest` override so stale persist sources fall back per
    * their declared policy. See {@link resolveFreshBuildManifest}.
    */
   public setFreshnessResolver(
      resolver: () => BuildManifest["entries"] | undefined,
   ): void {
      this.freshnessResolver = resolver;
   }

   /**
    * The freshness-filtered build manifest for this query, or undefined when the
    * package is unbound / has no resolver (⇒ no per-query override; the runtime
    * serves live). Evaluated per call so a table that crosses its window while
    * the package stays loaded is gated on the very next query.
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
    * `manifest` with pre-aggregation's own rollup entries removed.
    *
    * A synthesized rollup exists ONLY in the companion model (see
    * preaggregation_synthesis) — the author's model never declares it and can
    * never reference it, so its manifest entry can substitute nothing there. It
    * is not merely useless: Malloy refuses a non-empty `buildManifest` against a
    * model without `##! experimental.persistence`, and a model that declares
    * `#@ preaggregate` has no reason to carry that flag, since the companion
    * declares its own. Passing the full manifest to the author's model therefore
    * turned every query served from it into a 400 the moment a build bound a
    * manifest — which is every query the companion cannot answer, including any
    * naming a source it does not import, and every notebook cell.
    *
    * So the rule is by ORIGIN: only the companion sees the rollups. Returns
    * undefined when nothing survives, matching
    * {@link resolveFreshBuildManifest}'s "no override ⇒ serve live".
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
    * The facts dashboard discovery reads off this model, or undefined when the
    * model failed to compile.
    *
    * Deliberately uncurated HERE: `explores` curation shapes the *discovery*
    * surface agents see, and these facts are the raw material the manifest is
    * built from, so filtering them at this level would lose information the
    * lint needs. The curation decision is made by the caller instead:
    * `Package.discoverDashboards` withholds a dashboard whose entry file is not
    * a query entry point, because its manifest would advertise names the query
    * boundary refuses. See `service/dashboard.ts`.
    */
   public getDashboardModelFacts(): DashboardModelFacts | undefined {
      if (!this.modelDef) return undefined;
      // `this.givens` is the model's surfaced given list — the same surface
      // `filterGivensToModelSurface` enforces at query time, so a control the
      // manifest advertises is one a query will accept.
      return readDashboardModelFacts(
         this.modelPath,
         this.modelDef,
         (this.givens ?? [])
            .map((given) => given.name)
            .filter((name): name is string => name !== undefined),
      );
   }

   /**
    * True when this model's curated discovery surface is empty: its export
    * closure yields no sources and no named queries. The common cause is an
    * import-only model (imports other files, declares/re-exports nothing);
    * an `export {}` that filters everything out, or a genuinely empty file,
    * reads the same to a browser. Legitimate as plumbing, but confusing when
    * the model is *listed* — the page renders blank and an agent's listing
    * comes back []. Used by the load-time warning
    * (Package.emptyDiscoveryWarnings); the fix is to re-export what should be
    * visible (`export { name }`) or unlist the file.
    */
   public hasEmptyDiscoverySurface(): boolean {
      // No curation (no `explores`) ⇒ legacy listings include imported sources.
      if (!this.discoveryCurationEnabled) return false;
      if (this.modelType !== "model" || !this.modelDef) return false;
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
   }): void {
      this.queryBoundary = policy;
   }

   /**
    * Stable identity of the definition `name` resolves to IN THIS MODEL's
    * namespace: the file that declares it plus its position in that file.
    * Malloy resolves a name against the requested model's namespace, so two
    * models resolving a name to the same declaration yield the same key, while
    * two same-named declarations in different files do not.
    *
    * This is what makes the package-wide union safe. Keying the union on bare
    * names would let listed model A's exported `customers` admit the name
    * everywhere, and listed model B — which imports a DIFFERENT, hidden
    * `customers` — would then serve the hidden one, since the gate matched the
    * name while Malloy resolved the definition. Keying on the declaration
    * closes that.
    *
    * Returns undefined when Malloy attached no location (nothing to prove
    * identity with); every caller then falls back to this model's own export
    * closure, i.e. fails closed to the pre-union behavior.
    */
   public definitionIdentity(name: string | undefined): string | undefined {
      if (!name) return undefined;
      const def = this.modelDef?.contents?.[name] as
         | {
              location?: {
                 url?: string;
                 range?: { start?: { line?: number; character?: number } };
              };
           }
         | undefined;
      const url = def?.location?.url;
      if (!url) return undefined;
      const start = def?.location?.range?.start;
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
      if ((this.getQueries() ?? []).some((q) => q.name === name)) return true;
      return this.admittedByPackage(
         name,
         this.queryBoundary.packageCuratedQueries,
      );
   }

   /**
    * Query boundary, step 1 of 2 — the PRE-compilation gate. Enforces the rule
    * "queryable == discoverable": a source is a valid top-level query target
    * only if it is in the package's discovery surface (`explores` files +
    * their `export {}` closure). This is the *what* axis (identity-free);
    * `#(authorize)` is the orthogonal *who* axis, and both must pass. Denials
    * are a generic 404 ({@link NotQueryableError}) so a hidden target is
    * indistinguishable from a non-existent one. Inert unless `explores` is
    * declared AND mode is "declared" (the default). Notebooks are exempt
    * (always public) and never call this.
    *
    * This step runs before compilation so a denied target can't be probed via
    * compile errors (schema oracle), and it only POSITIVELY denies — explicit
    * names it can check, and ad-hoc text whose surface-resolved target is a
    * model-declared non-curated source. Anything it can't pin returns
    * "deferred" for {@link assertQueryBoundaryCompiled} to settle against the
    * compiled run target. "cleared" admissions (explicit curated names) skip
    * the backstop: an exported named query may read hidden sources internally —
    * that is the author's deliberate exposure, and re-deriving its source from
    * the compiled query must not re-deny it.
    */
   public assertQueryBoundaryEarly(
      sourceName?: string,
      queryName?: string,
      query?: string,
   ): "cleared" | "deferred" {
      // Notebooks are always public — they can't be explores and are never
      // gated by the boundary. (/compile does not reach this gate at all; it
      // is exempt from the boundary — see Environment.compileSource.)
      if (this.modelPath.endsWith(NOTEBOOK_FILE_SUFFIX)) return "cleared";
      const { mode, exploresDeclared, isQueryEntryPoint } = this.queryBoundary;
      // No opt-in surface (no explores) or explicitly decoupled ("all") ⇒ the
      // boundary is discovery-only; everything compiled stays queryable.
      if (mode === "all" || !exploresDeclared) return "cleared";

      // File-level: a non-`explores` model file is not a query entry point.
      // This is the robust line — the file is named by the request URL, so
      // there is nothing to resolve and nothing to evade.
      if (!isQueryEntryPoint) {
         throw new NotQueryableError(`No queryable model "${this.modelPath}".`);
      }

      // A named query/view is an author-exported entry point (the author chose
      // to expose it, even if it reads hidden sources internally) — admit it on
      // its own name, or on the explicitly-named curated source it runs against.
      if (queryName && !query) {
         // The exported-query fast-path admits a *pure* named query (`run: q`)
         // on its own name. It must NOT fire when a source is also named: a
         // request is `run: <sourceName>-><queryName>`, so a hidden source
         // could otherwise be reached via a view whose name happens to collide
         // with an exported top-level query (and clearing skips the compiled
         // backstop). With a source prefix, only the named source's curation
         // gates the request.
         if (!sourceName && this.isCuratedQuery(queryName)) return "cleared";
         if (sourceName && this.isCuratedSource(sourceName)) return "cleared";
         throw new NotQueryableError(`No queryable query "${queryName}".`);
      }

      // An explicitly-named source: admit iff curated.
      if (sourceName) {
         if (this.isCuratedSource(sourceName)) return "cleared";
         throw new NotQueryableError(`No queryable source "${sourceName}".`);
      }

      // Ad-hoc text: positively deny only a surface-resolved target that is a
      // model-declared source outside the curated surface (and doesn't derive
      // from a curated one) — pre-compile, so its compile errors can't leak
      // schema. Everything else (inline derivations, multi-statement, forms
      // the regex can't read) defers to the compiled backstop.
      if (query) {
         const target = extractRunTargetSourceName(query);
         if (
            target &&
            !this.isCuratedSource(target) &&
            !this.derivesFromCurated(target, query) &&
            this.sources?.some((s) => s.name === target)
         ) {
            throw new NotQueryableError(`No queryable source "${target}".`);
         }
      }
      return "deferred";
   }

   /**
    * Query boundary, step 2 of 2 — the POST-compilation backstop for
    * "deferred" admissions. `compiledSource` is the run target read off the
    * compiled query's `structRef` (see
    * {@link resolveAuthorizeSourceFromRunnable}) — the source Malloy actually
    * executes, surviving named-query indirection, multi-statement forms (the
    * LAST `run:` wins), and derivation. This inspects compiler *output* only;
    * compilation itself is never altered or restricted.
    *
    * Admit iff the compiled target is curated, or is an ad-hoc alias that
    * derives from a curated source (`source: x is customers extend { … }` →
    * `run: x` — composing over a queryable source is itself queryable). FAIL
    * CLOSED otherwise, including when the target can't be resolved at all.
    * (Raw inline `conn.sql(...)` never reaches here on the query path —
    * restricted-mode compilation rejects it first.)
    */
   public assertQueryBoundaryCompiled(
      compiledSource: string | undefined,
      query?: string,
   ): void {
      // Notebooks are always public (mirrors assertQueryBoundaryEarly).
      if (this.modelPath.endsWith(NOTEBOOK_FILE_SUFFIX)) return;
      const { mode, exploresDeclared, isQueryEntryPoint } = this.queryBoundary;
      if (mode === "all" || !exploresDeclared) return;
      if (!isQueryEntryPoint) {
         throw new NotQueryableError(`No queryable model "${this.modelPath}".`);
      }
      if (compiledSource) {
         if (this.isCuratedSource(compiledSource)) return;
         if (query && this.derivesFromCurated(compiledSource, query)) return;
      }
      throw new NotQueryableError("Query target is not queryable.");
   }

   /**
    * Boundary re-check for /compile's authorize-denial conversion (see
    * `denyHiddenAsNotQueryable` in service/environment.ts). /compile itself is
    * exempt from the boundary; this runs only AFTER an authorize denial, to
    * decide whether the 403 would confirm a hidden source exists. It settles
    * the COMPILED run target — the source Malloy actually executes — because
    * the early text gate resolves only the first `run:` statement, so
    * converting on it alone lets a multi-statement decoy
    * (`run: visible\nrun: hidden_gated`) or a derivation alias keep a 403 that
    * names the hidden source. Same admission rule as the query surface
    * (curated, or derives from curated via the submitted text), so the 403 is
    * masked exactly where the query surface answers 404. No-ops when the
    * boundary is inert.
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

   /** Source names in THIS model's export-curated discovery surface. The
    *  package-wide closure is applied separately and identity-checked (see
    *  {@link isCuratedSource}); it is deliberately not merged in here, so this
    *  stays the one set whose membership needs no identity proof. */
   private ownCuratedSourceNames(): Set<string> {
      return new Set(
         (this.getSources() ?? [])
            .map((s) => s.name)
            .filter((n): n is string => n !== undefined),
      );
   }

   /** True if `name` reaches a curated source by walking the ad-hoc text's
    *  `source: NAME is BASE` derivation declarations — composition over a
    *  queryable source is itself queryable. */
   private derivesFromCurated(name: string, query: string): boolean {
      // Hoisted out of the walk: the own-closure set is the same for every link
      // in the derivation chain, and only the identity check varies by name.
      const own = this.ownCuratedSourceNames();
      const packageCurated = this.queryBoundary.packageCuratedSources;
      const aliasOf = buildSourceAliasMap(query);
      let current: string | undefined = name;
      const seen = new Set<string>();
      while (current && !seen.has(current)) {
         if (
            own.has(current) ||
            this.admittedByPackage(current, packageCurated)
         )
            return true;
         seen.add(current);
         current = aliasOf.get(current);
      }
      return false;
   }

   /**
    * Every `#@ preaggregate` declaration in this model that cannot take effect.
    *
    * Read straight off the compiled `modelDef.contents`, so it reaches an
    * annotation on any source in the model — including one nothing references,
    * where a silently-ignored annotation would otherwise be undetectable.
    *
    * Returned rather than thrown: the owning Package joins these across its
    * models into one rejection, so an author fixing a package sees every bad
    * declaration at once instead of one per publish.
    */
   public preaggregateViolations(): PreaggregateViolation[] {
      if (!this.modelDef) return [];
      return validateModelPreaggregation(
         this.modelDef.contents as Record<string, unknown>,
      );
   }

   /**
    * Compile this model's synthesized pre-aggregation companion and keep it for
    * the serve path, or clear it when there is nothing to synthesize.
    *
    * Called by the owning Package, which supplies the package path, connections
    * and bound manifest a Model does not hold. Never throws: a failure leaves the
    * serve path exactly as it was, which is serving live.
    *
    * Only `.malloy` models are considered. A notebook's cells are compiled
    * individually and it declares no sources of its own to roll up.
    */
   public async buildPreaggregateServeModel(
      packagePath: string,
      malloyConfig: ModelConnectionInput,
      buildManifest?: BuildManifest["entries"],
   ): Promise<void> {
      this.preaggregateServeMaterializer = undefined;
      if (!this.modelDef || this.modelType !== "model") return;
      // Dynamic import to break a module cycle: the helper compiles through
      // Model.getModelRuntime, so importing it at the top of this file would make
      // the two modules import each other.
      const { tryCompileSynthesizedPreaggregation } = await import(
         "./preaggregation_compile"
      );
      const synthesized = await tryCompileSynthesizedPreaggregation({
         packagePath,
         modelPath: this.modelPath,
         malloyConfig,
         contents: this.modelDef.contents as Record<string, unknown>,
         buildManifest,
      });
      this.preaggregateServeMaterializer = synthesized?.materializer;
   }

   /**
    * Compile-time renderer-tag validation, run on the main thread.
    *
    * The renderer (`@malloydata/render`) is a large solid-js bundle that mutates
    * DOM globals at import; loading it inside the package-load worker isolate
    * destabilizes that thread (it is deliberately kept pure-CPU). So validation
    * runs here, after the worker has hydrated this Model, where the renderer is
    * already used at query time (see query.controller.ts / execute_query_tool.ts).
    *
    * Prepares each annotated top-level named query (`run: <name>`) and each
    * annotated source view (`run: <source> -> <view>`) compile-only -- no
    * execution -- to get a stable result schema, then runs the renderer's
    * headless `validateRenderTags`. Targets with no annotations carry no render
    * tags, so they are skipped without compiling. Every finding the renderer
    * reports is logged as a warning naming the offending target; none of them
    * fail the package load. The findings are returned so the owning Package can
    * surface them as non-fatal `warnings` on its response, tagged with the
    * renderer's own severity:
    *
    *   - `error`: the tag is malformed for the field it sits on (e.g. a
    *     child-only `# big_value { sparkline=... }` on a view with no activating
    *     big_value, or `# colspan=abc`). It renders as "[object Object]" or an
    *     inline error at query time.
    *   - `warn`: the tag is well-formed but inert where it sits, so the renderer
    *     ignores it (`# colspan` outside `# dashboard { columns=N }`, or a
    *     colspan wider than the grid, which is clamped). Nothing looks broken at
    *     query time -- the layout just is not what the author wrote -- which is
    *     why load time is the only place this becomes visible.
    *
    * `warn` and `error` are the only severities the renderer emits, so no
    * finding is dropped here.
    */
   public async validateRenderTags(): Promise<RenderTagWarning[]> {
      const mm = this.modelMaterializer;
      if (!mm) {
         return [];
      }
      // Dynamic import (like execute_query_tool.ts): the renderer is heavy and
      // mutates DOM globals on load, so only pull it in when there's a model to
      // validate.
      const { validateRenderTags } = await import(
         "@malloydata/render-validator"
      );

      // Renderable targets: top-level named queries and every view declared on a
      // source. Source views are where render tags like `# big_value` usually
      // live, and they are NOT in `this.queries`.
      const targets: { label: string; queryString: string }[] = [];
      for (const query of this.queries ?? []) {
         // Only an annotated, named query can carry a render tag to validate;
         // skip the rest rather than compiling every query in the package.
         if (!query.name || !query.annotations?.length) {
            continue;
         }
         // Quote the identifier (see quoteMalloyIdentifier) so a name needing
         // Malloy quoting still lexes and cannot break out of the quotes.
         targets.push({
            label: query.name,
            queryString: `run: ${quoteMalloyIdentifier(query.name)}`,
         });
      }
      for (const source of this.sources ?? []) {
         for (const view of source.views ?? []) {
            // Render tags live on the view's own or inherited annotations, not
            // via source-to-view inheritance, so an unannotated view has
            // nothing to validate and need not be compiled. (A model-level
            // `##` tag is the one case this gate doesn't reach, but those are
            // theme/config, not the child-only chart tags this guards against.)
            if (!view.annotations?.length) {
               continue;
            }
            // Quote both identifiers (see quoteMalloyIdentifier): an unquoted
            // name like `gated-source` fails to lex, and the catch below would
            // then silently skip the very view this is meant to validate.
            targets.push({
               label: `${source.name} -> ${view.name}`,
               queryString: `run: ${quoteMalloyIdentifier(source.name)} -> ${quoteMalloyIdentifier(view.name)}`,
            });
         }
      }

      const findings: RenderTagWarning[] = [];
      for (const target of targets) {
         let result: Malloy.Result;
         try {
            const prepared = await mm
               .loadQuery(target.queryString)
               .getPreparedResult();
            result = prepared.toStableResult();
         } catch {
            // A view/query that fails to prepare is reported by the normal
            // compile path; don't mask that with a render-tag error.
            continue;
         }
         // Keep both severities. The renderer reports a tag that is well-formed
         // but inert as `warn`, not `error` -- `# colspan` outside columns mode
         // ("Ignored # colspan ... colspan only applies in columns mode"), or a
         // colspan wider than the grid -- so filtering to error-severity
         // dropped exactly the findings an author cannot otherwise see: the tag
         // parses, the package loads, the query runs, and the layout silently
         // ignores it. `warn` and `error` are the only severities the renderer
         // emits, so this is everything it reports, and `severity` keeps the two
         // apart for the operator.
         const logs = validateRenderTags(result).filter(
            (log) => log.severity === "error" || log.severity === "warn",
         );
         if (logs.length > 0) {
            // An inert tag is not an invalid one, so don't call it "Invalid";
            // the per-finding messages already say which kind each is.
            logger.warn(
               `Render tag findings on '${target.label}': ${logs
                  .map((e) => `[${e.severity}] ${e.message}`)
                  .join("; ")}`,
            );
            for (const e of logs) {
               findings.push({
                  subject: target.label,
                  message: e.message,
                  severity: e.severity === "error" ? "error" : "warn",
               });
            }
         }
      }
      return findings;
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
      return this.getCompilationError();
   }

   /**
    * The parts of a notebook a package listing shows: its human title and
    * description, resolved the way a dashboard's are plus one step a notebook
    * can afford.
    *
    * `## title="…"` then the `#"` doc comment are the dashboard's chain
    * verbatim, so an author who learned one convention has learned both. The
    * third step is the notebook's own: the first markdown heading. A notebook
    * is prose by definition and almost always opens with its title already
    * written, so reading it makes existing notebooks stop surfacing as
    * filenames without anyone editing anything — the same bargain a `Page`
    * makes by reading its `<title>`. A dashboard has no prose to read, which is
    * why the step is here rather than in the shared chain.
    *
    * Returns no title when nothing resolves, leaving the filename to the
    * caller; a title equal to the path would just be noise on the wire.
    */
   public getNotebookListing(): { title?: string; description?: string } {
      if (this.modelType !== "notebook") return {};
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
    * The text of the first markdown heading in the notebook, at any level.
    *
    * Any level, because a notebook that opens with `## Overview` means that as
    * its title just as much as one that writes `# Overview`; heading depth is a
    * typographic choice here, not a statement about what the document is
    * called. Only the first heading is considered, and only if no prose
    * precedes it — a notebook that opens with a paragraph is not naming itself.
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
      if (this.modelType === "notebook") {
         return this.getNotebookModel();
      } else {
         throw new ModelNotFoundError(
            `${this.modelPath} is not a valid notebook name.  Notebook files must end in .malloynb.`,
         );
      }
   }

   /**
    * Set the connections a transient serve-shape model compiles against. Pushed
    * by the owning Package from the environment's destination list, resolved on
    * each use so a destination-list change takes effect without a reload.
    */
   public setServeDestinationConfig(provider: () => MalloyConfig): void {
      this.serveDestinationConfig = provider;
      // The reachable connection set is part of what the memoized materializer
      // was compiled against, so a change invalidates it.
      this.invalidateServeShapeCache();
   }

   /**
    * Forget the memoized serve-shape materializer. The memo is keyed on the
    * binding set, so anything else it was compiled against — the destination
    * connections above all — has to invalidate it explicitly.
    */
   public invalidateServeShapeCache(): void {
      this.serveShapeCache = undefined;
   }

   /**
    * Set (or clear) this model's `storage=` serve bindings. Invalidates the
    * memoized serve-shape materializer so the next routed query recompiles
    * against the new binding set.
    */
   public setServeBindings(bindings: ServeBinding[]): void {
      this.serveBindings = bindings;
      this.serveShapeCache = undefined;
   }

   /**
    * Compile an untrusted query against the transient serve-shape model that
    * rebinds this package's materialized-into-storage sources to virtual sources
    * on their storage connection. Returns the runnable and the `virtualMap` that
    * binds each virtual handle to its physical table.
    *
    * Throwing is the eligibility signal: a query that references a refinement
    * the serve shape does not reproduce (a measure/dim/join defined on the
    * source in the author's model, or a source with no binding) fails to compile
    * here, and {@link getQueryResults} falls back to serving it live. The
    * compiled materializer is memoized per binding set (the serve-variant cache).
    */
   /**
    * The serve bindings that may serve from their materialized table right NOW:
    * fresh, un-gated, or stale-under-`stale_ok`. A stale binding whose fallback is
    * `live`/`fail` evaluates to `serve_live` and is dropped, so the serve shape
    * omits that source and any query touching it falls through to live — the SAME
    * freshness gate the colocated serve applies (`getFreshBuildManifest`
    * → `evaluateManifestFreshness`). Placement (`storage=`) is orthogonal to
    * freshness: flip a colocated build ↔ `storage=lake` and this behaves identically.
    */
   private freshServeBindings(now: number): ServeBinding[] {
      const at = new Date(now);
      return this.serveBindings.filter(
         (b) =>
            evaluateManifestFreshness(
               {
                  tableName: b.tablePath,
                  dataAsOf: b.freshAsOf,
                  freshnessWindowSeconds: b.freshnessWindowSeconds,
                  freshnessFallback: b.freshnessFallback,
               },
               at,
            ) === "serve_table",
      );
   }

   private async loadServeShapeQuery(queryString: string): Promise<{
      runnable: QueryMaterializer;
      virtualMap: VirtualMap;
      /**
       * The fresh bindings the shape was built from. Returned so a caller making
       * a per-binding decision reads the set that actually produced this shape
       * rather than the package-wide field.
       */
      bindings: ServeBinding[];
   }> {
      // Gate by freshness first: only bindings that should serve their table now
      // enter the shape. Keying the cache on the FRESH subset means it recompiles
      // when a binding crosses its window (drops out) — the storage analogue of
      // the colocated path's memoized getFreshBuildManifest.
      const freshBindings = this.freshServeBindings(Date.now());
      if (freshBindings.length === 0) {
         // Every bound source is stale past its window with a live/fail fallback —
         // nothing to serve from storage; fall through to live (caller's catch).
         throw new Error("no fresh storage serve bindings for this query");
      }
      const key = freshBindings
         .map((b) => `${b.sourceName}@${b.destinationName}/${b.virtualHandle}`)
         .sort()
         .join("|");
      if (!this.serveShapeCache || this.serveShapeCache.key !== key) {
         this.serveShapeCache = {
            key,
            materializer: await this.compileServeShape(
               this.serveBindingsWithRefinements(freshBindings),
            ),
         };
      }
      const virtualMap = buildVirtualMap(freshBindings);
      const runnable =
         this.serveShapeCache.materializer.loadRestrictedQuery(queryString);
      // Compile eagerly so ineligibility (a refinement the serve shape lacks, an
      // unbound source, a bad connection) surfaces HERE — Malloy compiles lazily,
      // so without this the error would escape at prepare/run instead of at the
      // caller's try, defeating the safe fallback. Cheap relative to the run. The
      // serve shape is pure virtual sources, so no buildManifest is needed.
      await runnable.getSQL({ virtualMap });
      return { runnable, virtualMap, bindings: freshBindings };
   }

   /**
    * Compile the serve-shape materializer for a binding set, degrading
    * gracefully if the richest shape does not compile. A single un-carriable
    * refinement (a join or view that reaches a non-materialized source, a
    * non-portable expression) would otherwise fail the WHOLE shape's compile and
    * disable storage serving for every source in the package. So the shape is
    * validated once (per binding set — the result is cached) and, on failure,
    * the riskiest refinement category is dropped and it retries: full → drop
    * views → drop views + joins → base-only. Base-only is pure virtual sources
    * and always compiles, so it is the guaranteed floor. Each surviving tier
    * still serves everything it can; the per-query eager compile in
    * {@link loadServeShapeQuery} remains the final net for query-specific
    * ineligibility.
    */
   private async compileServeShape(
      enriched: ServeBinding[],
   ): Promise<ModelMaterializer> {
      // Richest first; each predicate keeps fewer refinement kinds than the last.
      const keepKinds: Array<ReadonlySet<string>> = [
         new Set(["join", "dimension", "measure", "view"]),
         new Set(["join", "dimension", "measure"]),
         new Set(["dimension", "measure"]),
         new Set(),
      ];
      // Skip escalation entirely when nothing beyond the base is carried.
      const hasRefinements = enriched.some(
         (b) => (b.refinements ?? []).length > 0,
      );
      const lastTier = keepKinds.length - 1;
      for (let tier = 0; tier <= lastTier; tier++) {
         const keep = keepKinds[tier];
         const shaped =
            tier === 0
               ? enriched
               : enriched.map((b) =>
                    b.refinements
                       ? {
                            ...b,
                            refinements: b.refinements.filter((r) =>
                               keep.has(r.kind),
                            ),
                         }
                       : b,
                 );
         const materializer = this.buildServeShapeMaterializer(shaped);
         // Base-only (last tier) always compiles; trust it without a probe. And
         // when there are no refinements at all, tier 0 IS the base — skip too.
         if (tier === lastTier || (tier === 0 && !hasRefinements)) {
            return materializer;
         }
         try {
            await materializer.getModel();
            return materializer;
         } catch (err) {
            recordServeShapeTierDrop(tier);
            logger.warn(
               "Storage serve shape failed to compile; dropping the riskiest refinement category and retrying",
               {
                  model: this.modelPath,
                  tier,
                  error: err instanceof Error ? err.message : String(err),
               },
            );
         }
      }
      // Unreachable: the last tier returns above. Satisfy the type checker.
      return this.buildServeShapeMaterializer(
         enriched.map((b) => ({ ...b, refinements: [] })),
      );
   }

   /** Build the transient serve-shape materializer for a set of bindings. */
   private buildServeShapeMaterializer(
      bindings: ServeBinding[],
   ): ModelMaterializer {
      const { modelText } = buildServeShapeModelForBindings(bindings);
      const root = "file:///storage-serve-shape/";
      const url = `${root}shape.malloy`;
      const runtime = new Runtime({
         urlReader: new InMemoryURLReader(new Map([[url, modelText]])),
         // Narrowed to the destinations THESE bindings name. The shape's
         // generated text references nothing else, so anything else resolving
         // would only ever be a way to reach a warehouse this query has no
         // business reaching.
         config: restrictMalloyConfigToConnections(
            this.serveDestinationConfig!(),
            new Set(bindings.map((binding) => binding.destinationName)),
         ),
      });
      return runtime.loadModel(new URL(url), {
         importBaseURL: new URL(root),
      });
   }

   /**
    * The serve bindings enriched with the refinements to re-declare on each
    * virtual base — the source's dimensions/measures (computed from the stored
    * columns), its joins whose target source is ALSO materialized (the join runs
    * over the stored tables), and its views (turtles). All are read from this
    * model's compiled definition. Analytic source-fields are not carried; a
    * query using one falls back to live.
    *
    * Join and view declaration text is lifted verbatim from the author's source
    * files by location (read once per file, best-effort — an unreadable file
    * just drops that source's joins/views, which fall back). The join
    * materialization gate is a `sourceID`-keyed lookup, so it never depends on
    * parsing that text; views are emitted optimistically and pruned by the
    * shape-compile escalation in {@link compileServeShape} if they don't hold.
    */
   private serveBindingsWithRefinements(
      bindings: ServeBinding[] = this.serveBindings,
   ): ServeBinding[] {
      const contents = (
         this.modelDef as
            | {
                 contents?: Record<
                    string,
                    { sourceID?: unknown; fields?: unknown[] }
                 >;
              }
            | undefined
      )?.contents;
      // sourceID -> author source name, for the join materialization gate.
      const sourceNameById = new Map<string, string>();
      for (const [name, def] of Object.entries(contents ?? {})) {
         if (typeof def?.sourceID === "string") {
            sourceNameById.set(def.sourceID, name);
         }
      }
      const materializedSourceNames = new Set(
         bindings.map((b) => b.sourceName),
      );
      // Cache each source file's text (or null when unreadable) across bindings.
      const fileCache = new Map<string, string | null>();
      const liftText = (location: SourceLocation): string | undefined => {
         if (!location?.url?.startsWith("file:")) return undefined;
         if (!fileCache.has(location.url)) {
            try {
               fileCache.set(
                  location.url,
                  readFileSync(fileURLToPath(location.url), "utf8"),
               );
            } catch {
               fileCache.set(location.url, null);
            }
         }
         const text = fileCache.get(location.url);
         return text ? sliceSourceRange(text, location.range) : undefined;
      };
      return (
         bindings
            .map((b) => {
               const fields = contents?.[b.sourceName]?.fields;
               // Narrow the declared ::Shape to the source's PUBLIC columns: the
               // build materializes every projected column (incl. `except:`-ed /
               // access-restricted ones), so the captured schema can be wider
               // than the source's public surface. Declaring a hidden column
               // would expose it over storage when live hides it — always applied
               // (even with no refinements), so the serve surface never widens
               // the source's.
               const schema = narrowSchemaToPublic(b.schema, fields);
               const refinements = [
                  ...extractJoins(fields, {
                     sourceNameById,
                     materializedSourceNames,
                     liftText,
                  }),
                  ...extractRefinements(fields),
                  ...extractViews(fields, liftText),
               ];
               return { ...b, schema, refinements };
            })
            // Drop any binding whose public schema is empty. Bindings are pushed
            // to EVERY model in the package, so a model receives bindings for
            // sources it doesn't define (defined in a sibling model) — those have
            // no field list here, hence an empty narrowed schema. An empty
            // `type: X__shape is {}` is a Malloy parse error that would fail the
            // ENTIRE serve-shape model (breaking the base-only-always-compiles
            // fallback invariant) and silently drop storage serving for this
            // model's own sources too. Omitting them sends queries on an
            // undefined source to live (where this model refuses them anyway) and
            // keeps a source from being served through a model that doesn't
            // declare it.
            .filter((b) => b.schema.length > 0)
      );
   }

   public async getQueryResults(
      sourceName?: string,
      queryName?: string,
      query?: string,
      filterParams?: FilterParams,
      bypassFilters?: boolean,
      givens?: Record<string, GivenValue>,
      // Optional caller-supplied abort signal. Plumbed straight into
      // `runnable.run` so a publisher-issued query timeout (see
      // `runWithQueryTimeout`) actually cancels the work in flight
      // instead of just unblocking the awaiter. Pass `undefined` to
      // keep the legacy "no timeout" behavior — useful for
      // background callers (materialization, tests) that own their
      // own deadline.
      abortSignal?: AbortSignal,
      /**
       * Per-query metadata inputs from the request boundary (see
       * {@link ModelQueryMetadataInput}). Omitting it still tags the query with
       * the server context; what is lost is the caller's own properties, the
       * connection default, and the correlation id.
       */
      queryMetadataInput?: ModelQueryMetadataInput,
      /**
       * Which shape the caller is going to send, so exactly that one is built and
       * measured. `"compact"` is the `compactJson` response (the rows, with the
       * bigint replacer); `"full"` is the wrapped result. Every caller declares
       * one: REST from its `compactJson` flag, and the MCP tool as `"compact"`,
       * because its envelope is built from the compact rows even though it does
       * not send this string verbatim. The default covers the spec call sites,
       * which predate the parameter; no production caller omits it.
       */
      responseShape: "full" | "compact" = "full",
      /**
       * Skip `#(authorize)` gate evaluation for this request — the private
       * data-management path (see the router's `dataManagementQuery`).
       *
       * Disables ONLY expressions collected from `#(authorize)` annotations.
       * The author's own `where:` clauses, `#(filter)` handling, and
       * every other semantic are untouched, and this neither reads nor writes
       * {@link bypassFilters} — that is a separate, deprecated, `#(filter)`-only
       * control. Never settable from the ingress-exposed surface: `/private/**`
       * is not routed externally and the router's controller pins an M2M identity
       * before forwarding it.
       */
      bypassAuthorize = false,
   ): Promise<{
      result: Malloy.Result;
      /**
       * The JSON of whichever shape `responseShape` asked for, built once. A
       * caller that sends that shape should send this string rather than
       * stringify the object itself: it is the same bytes the byte cap measured,
       * and a payload too large to serialize is reported here as the same HTTP
       * 413 the cap produces instead of escaping as a bare 500 from the caller's
       * own `JSON.stringify`.
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
       * Wall-clock milliseconds from the start of query handling through the end
       * of execution — compile, authorize, routing, prepare and the warehouse
       * round trip. It excludes serialization and transport, so it reads lower
       * than a caller's own stopwatch, and it is the same span the query
       * histogram records.
       */
      executionTimeMs: number;
      /**
       * Warehouse bytes this query SCANNED, when the backend reports it (BigQuery
       * today) — not what it is billed, which runs higher on small reads because
       * BigQuery charges a 10MB minimum per query. Null means "not reported",
       * which includes both a backend that cannot say and a storage-served query
       * that touched no warehouse — never read it as zero cost without checking
       * `servedFrom`.
       *
       * Named `cost` rather than `scanned` on purpose, and deliberately unlike
       * the counter above: this field carries `runStats.queryCostBytes` through
       * verbatim, so it keeps the name upstream Malloy gives it and a reader can
       * follow it back to its source. The counter had no such lineage to
       * preserve — it is ours to name — which is why the two land differently.
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
      // Hoisted out of the try block below (it's assigned there) so the
      // row-level authorize recompile, which runs after that try/catch, can
      // still hand this SAME caller text back to `loadRestrictedQuery`.
      let queryString: string;
      // Set when this query is routed through the `storage=` serve-shape
      // transform; threaded into prepare + run so the virtual sources resolve to
      // their physical tables. Undefined ⇒ served live (the default path).
      let serveVirtualMap: VirtualMap | undefined;
      // The bindings that actually PRODUCED the serve shape — the fresh subset,
      // not the package's whole set. Per-binding decisions (freshnessFallback)
      // must read this: `this.serveBindings` is pushed package-wide, so a
      // sibling source's value would otherwise decide this query's behavior.
      let serveShapeBindings: ServeBinding[] = [];
      // How the answer was ultimately produced, for the query histogram. Absent
      // when the query never routed (an off/live deployment's histogram is
      // unchanged); "storage" when served from a materialized table;
      // "live_fallback" when it routed but a run-time store failure degraded it
      // to live — which must NOT count as a storage hit, since the hit rate is
      // the tier's headline KPI and would otherwise rise while the tier is down.
      let servedFrom: ServedFrom | undefined;
      // Set when the query compiled against the pre-aggregation companion model
      // and will run there. Decides which build manifest the run gets: only the
      // companion may see pre-aggregation's own rollup entries (see
      // {@link withoutPreaggregateEntries}).
      let preaggRouted = false;
      if (!this.modelMaterializer || !this.modelDef || !this.modelInfo)
         throw new BadRequestError("Model has no queryable entities.");

      // Per-query freshness gate (persistence.md §9.3): resolve the
      // freshness-filtered manifest once and thread it into both the prepare
      // (for the row limit) and the run so a stale persist source falls back per
      // its declared policy — and prep/run agree on the same substitution.
      //
      // Resolved HERE, above the routing block below, because the pre-aggregation
      // probe has to compile against the same manifest the run will use.
      const buildManifest = this.resolveFreshBuildManifest();
      // The same manifest with pre-aggregation's rollups removed, for every
      // runnable that is NOT the companion. See the method for why.
      const liveBuildManifest = this.withoutPreaggregateEntries(buildManifest);
      // Givens supplied only so a joined source's authorize gate could see
      // them (checked below, against the full unfiltered set) must not reach
      // the real query if this model doesn't itself surface them — see
      // filterGivensToModelSurface. Resolved here for the same reason as
      // `buildManifest`: the pre-aggregation probe needs them.
      const querySurfaceGivens = this.filterGivensToModelSurface(givens);

      // Query boundary FIRST (the *what* axis): reject a target that isn't in
      // the package's queryable surface with a generic 404, before authorize
      // (the *who* axis) and before compilation — so a non-queryable source is
      // indistinguishable from a non-existent one and can't be probed.
      // "deferred" means the early gate couldn't pin the target; the compiled
      // backstop below settles it against the source the query actually runs.
      const boundary = this.assertQueryBoundaryEarly(
         sourceName,
         queryName,
         query,
      );

      // Early fast-path authorize gate (before loadQuery). Resolve the source
      // from surface syntax; gate if it names one. This runs BEFORE compilation
      // so the gate can't be used as a schema oracle — without it, a denied
      // caller probing `run: gated -> { group_by: maybe_field }` would get a
      // Malloy "field not found" vs a 403 and learn the gated source's columns.
      // It does NOT replace the authoritative compiled-source gate below (which
      // always runs and catches named-query / multi-statement forms surface
      // syntax can't resolve); it only fails fast for the common case.
      const surfaceName = extractRunTargetSourceName(query);
      const earlySource =
         sourceName ||
         (queryName
            ? this.queries?.find((q) => q.name === queryName)?.sourceName
            : undefined) ||
         // A run target named in ad-hoc text can be a declared QUERY rather than
         // a source (`run: locked_q + { … }` refines the author's named query).
         // Resolve it to the source that query runs against, the same way the
         // `queryName` param is resolved above — otherwise the name matches no
         // source, nothing gates it, and the refinement's compile errors come
         // back from a source the caller cannot read.
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

      // Wrap loadQuery calls in try-catch to handle query parsing errors
      try {
         // Before any compile: caller text may not declare an authorize gate (it
         // would override the author's — see the function's doc). Every
         // caller-supplied fragment that reaches the compiler is checked, not
         // just the ad-hoc `query`. `sourceName`/`queryName` are quoted as
         // identifiers below, so an annotation smuggled through either one can
         // no longer become syntax — it is part of a name, and the request dies
         // as an unresolvable one. Checking them anyway is defence in depth:
         // it is cheap, it rejects the attempt before compilation rather than
         // after, and it keeps the guard correct if the builder below ever
         // changes shape again.
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
               assertNoCallerAuthorizeAnnotation(callerText);
            } catch (err) {
               // Recorded here, not at the throw: the parse `catch` below
               // rethrows a BadRequestError untouched and never reaches the
               // query histogram, so this is the only signal a rejection emits.
               recordAuthorizeGuardRejection(field);
               throw err;
            }
         }
         if (!sourceName && !queryName && query) {
            queryString = "\n" + query;
         } else if (queryName && !query) {
            // These fields are NAMES, not Malloy text. Quote both as
            // identifiers so a caller-supplied name can only ever lex as one
            // identifier: without this a newline (or a space) in either field
            // opens a SECOND top-level statement and Malloy runs the LAST
            // `run:`, reading a source this request never named. The gates
            // above key off the raw strings by exact match, and the two lookups
            // fail in opposite directions — the curated-source lookup misses
            // and 404s, but getFilters() misses and returns NO filters, so the
            // smuggled statement reads its target unfiltered. Quoting also
            // makes a name that REQUIRES Malloy quoting (hyphen, space,
            // reserved word, leading digit) work on this path for the first
            // time. Empty `sourceName` stays falsy-means-absent.
            queryString = `\nrun: ${
               sourceName ? `${quoteMalloyIdentifier(sourceName)} -> ` : ""
            }${quoteMalloyIdentifier(queryName)}`;
         } else {
            const endTime = performance.now();
            const executionTime = endTime - startTime;
            this.queryExecutionHistogram.record(
               executionTime,
               this.queryMetricAttributes({
                  environment: queryMetadataInput?.environment,
                  queryName,
                  sourceName,
                  status: "error",
               }),
            );
            throw new BadRequestError(
               "Invalid query request. (Query AND !sourceName) OR (queryName AND sourceName) must be defined.",
            );
         }

         // Distinguishes free-form query text from the named `source->view`
         // form. Both are driven by untrusted caller input and compiled in
         // restricted mode below; this flag only controls how the protected
         // source is resolved for filter injection.
         const isAdHocQuery = !sourceName && !queryName && !!query;

         // Inject source filter predicates unless bypassed. For ad-hoc queries
         // resolve the run target through any alias/extend/chain so a protected
         // source can't be read unfiltered under a different name.
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
               }
            }
         }

         // Restricted mode keeps untrusted query text inside the model's curated
         // surface — it rejects `import`, raw `connection.table(...)` /
         // `connection.sql(...)`, raw-SQL functions, and `##!` flags. The
         // model's own definitions are unaffected. Both the ad-hoc `query` text
         // and the `run: source->view` string built from the caller-supplied
         // `sourceName`/`queryName` pair are untrusted, so both compile here;
         // only author-curated notebook cells use the unrestricted `loadQuery`.
         runnable = this.modelMaterializer.loadRestrictedQuery(queryString);
         // Kept so a routed query can still be answered live if the store fails
         // underneath it at RUN time (see the freshnessFallback retry below).
         liveRunnable = runnable;

         // Storage-routing eligibility check: decide it BEFORE attempting
         // routing, not after.
         // The serve-shape's transient model (buildServeShapeModel in
         // materialization_serve_transform.ts, `source: X is <dest>.virtual(…
         // )::<shape>`) carries no `#(authorize)` annotation bytes at all, so
         // once `runnable` is swapped for the shape's runnable below, NOTHING
         // downstream — including the authoritative walk inside
         // `authorizeAndBindRunnable` — can ever discover a row-level gate
         // this pre-check missed: that walk resolves its struct from
         // whichever runnable it is handed, and the shape's struct carries no
         // annotations to find. There is therefore no post-hoc undo that can
         // backstop a wrong answer here (an earlier version of this code
         // carried one; it could never fire on this path, since by the time
         // it ran `runnable` already pointed at the annotation-free shape —
         // it was deleted rather than kept as inert belt-and-braces). This
         // pre-check IS the gate that keeps a row-level-gated query off the
         // storage tier — see its own doc comment.
         //
         // `bypassAuthorize` short-circuits this too: a bypassed query skips
         // every `#(authorize)` gate anyway (`authorizeAndBindRunnable`'s own
         // top-level check, below), so there is no row-level filter for
         // storage routing to lose by running against the shape — only
         // routing eligibility to lose for no benefit. This does not weaken
         // anything for a NON-bypassed caller: `bypassAuthorize` is never
         // settable from the ingress-exposed surface (see its own doc
         // comment on this method), so this only ever short-circuits for the
         // same trusted data-management path that already bypasses the
         // authoritative gate below.
         //
         // Ordered AFTER the routing preconditions rather than before them:
         // this pre-check exists only to veto storage routing, so a
         // deployment that cannot route at all (mode off/write-only, no
         // serve bindings, no destination config) has nothing for it to
         // decide — and it is not free, being a full entry-point walk plus,
         // on a cold `gateShapeCache`, a probe compile. Evaluating it first
         // would put that on every query in every deployment, including the
         // overwhelming majority that never route.
         const storageRoutingPossible =
            getPersistStorageMode() === "on" &&
            this.serveBindings.length > 0 &&
            !!this.serveDestinationConfig;
         // The companion alone, deliberately: the pre-aggregation tier's own
         // precondition is just "a companion was synthesized".
         // `serveVirtualMap` is NOT available at this point — it is assigned
         // inside the storage block below — so folding it in here would read
         // `undefined` and answer the wrong question. Captured into a local
         // rather than re-read below, so the guard and the use cannot disagree
         // about which companion (or none) they are talking about.
         const preaggServeMaterializer = this.preaggregateServeMaterializer;
         const routingBlockedByRowLevelGate =
            (storageRoutingPossible || !!preaggServeMaterializer) &&
            !bypassAuthorize &&
            // Short-circuit BEFORE the walk. The walk is a full entry-point
            // traversal plus, on a cold `gateShapeCache`, a live compile, and a
            // deployment that enables pre-aggregation and declares no gate
            // anywhere would otherwise pay it on every query for a case it
            // cannot hit. This is NOT the `hasAuthorize()` trap warned about
            // further down — see `hasAnyAuthorizeNote`'s doc for why the two
            // predicates differ and why only this one is safe to skip on.
            this.hasAnyAuthorizeNote() &&
            (await this.queryEntryPointHasRowLevelGate(runnable));
         // Recorded HERE, once, rather than in each tier's block below: the
         // pre-aggregation guard runs no compile attempt of its own and calls
         // no routing metric today, so an emit inside each tier would leave
         // this outcome inconsistent between the two (present for storage,
         // absent for pre-aggregation) rather than firing once for the
         // decision that actually blocked both.
         if (routingBlockedByRowLevelGate) {
            recordStorageServeRouting("blocked_by_row_level_gate");
         }

         // storage= serve routing: when enabled and this package has sources
         // materialized into a storage destination, try compiling the query
         // against the transient serve-shape model (materialized sources rebound
         // to virtual sources on their storage connection). If it compiles, we
         // serve from the materialized tables via the virtualMap; if it does not
         // (a refinement the shape lacks, or an unbound source), we keep the
         // original runnable and serve live — safe fallback, no behavior change
         // for anything the transform can't yet reproduce. Off / write-only and
         // packages with no storage bindings skip this entirely — and so does a
         // row-level-gated entry point, per the pre-check just above.
         if (storageRoutingPossible && !routingBlockedByRowLevelGate) {
            try {
               const shaped = await this.loadServeShapeQuery(queryString);
               runnable = shaped.runnable;
               serveVirtualMap = shaped.virtualMap;
               serveShapeBindings = shaped.bindings;
               servedFrom = "storage";
               recordStorageServeRouting("storage");
               logger.info("Serving query from storage tier (virtual-source)", {
                  modelPath: this.modelPath,
                  // The sources in the SHAPE, not the package's whole binding set:
                  // a stale binding is dropped before the shape is built.
                  storageSources: shaped.bindings.map((b) => b.sourceName),
               });
            } catch (shapeErr) {
               recordStorageServeRouting("live_fallback");
               // info, matching the storage-hit line above: the two halves of one
               // routing decision, and this is the half an operator needs. A
               // fallback is silent by design — the query succeeds, the rows are
               // correct, only the tier is lost — so at debug the reason for a
               // package that never serves from storage is unreadable in any
               // deployment running at info. Volume is bounded by the same thing
               // that bounds the hit line: one per routed query, on packages that
               // declare `storage=` at all.
               logger.info(
                  "storage serve-shape ineligible for this query; serving live",
                  {
                     modelPath: this.modelPath,
                     error:
                        shapeErr instanceof Error
                           ? shapeErr.message
                           : String(shapeErr),
                  },
               );
            }
         }

         // Pre-aggregation serve routing: compile the query against the
         // synthesized model, where each annotated source is a composite of its
         // rollups and itself. Malloy then picks a rollup when one covers the
         // query and the base when none does, so this does not decide routing —
         // it only offers the choice. Which is why there is no eligibility test
         // here and no metric for "declined": from out here a covered query and
         // an uncovered one are the same call.
         //
         // Skipped when the storage shape already routed: that runnable resolves
         // through `virtualMap` rather than the build manifest, and recompiling it
         // here would discard that. Composing the two tiers is future work.
         // Gated on the SAME hoisted answer as the storage tier above, rather
         // than on a second walk.
         //
         // This is defence in depth, not a fix for a reachable leak, and the
         // distinction is worth stating because the measurement is easy to get
         // wrong. Measured on this branch: a rollup over a gated base compiles
         // and is PLANNED, but both build gates refuse to materialize it
         // (`assertColocatedPersistNotAuthorizeGated` and
         // `assertMaterializationEligible`, via `referencesAuthorize` finding the
         // base's gate in the rollup's compiled subtree). So no rollup table
         // exists, the composite's rollup member recomputes from the gated base,
         // the gate grafts onto that base, and a covered query already returns
         // correctly filtered rows.
         //
         // What the guard removes is the serve path's dependence on a BUILD
         // path's refusal for its own correctness. That refusal is the blind
         // `referencesAuthorize` walk, whose join reach is documented as partial
         // — an ANNOTATED join leaves no authorize byte in the subtree for it to
         // find. If synthesis ever emitted its base import in a shape that walk
         // cannot see, a rollup would build, and a covered query would then be
         // served from a table pre-aggregated across every tenant, with the gate
         // filtering on a column the rollup does not carry. Nothing downstream
         // would catch it: `preaggRouted` is never reset, so
         // `effectiveBuildManifest` hands the rollup manifest to the runnable
         // regardless. Blocking the tier for a row-level-gated entry point makes
         // the invariant local to this decision.
         if (
            preaggServeMaterializer &&
            !routingBlockedByRowLevelGate &&
            !serveVirtualMap
         ) {
            try {
               const candidate =
                  preaggServeMaterializer.loadRestrictedQuery(queryString);
               // Compile eagerly, for the same reason loadServeShapeQuery does:
               // Malloy compiles LAZILY, so `loadRestrictedQuery` cannot throw
               // here and without this the error escapes at prepare/run instead,
               // past the catch below — which made a query naming any source the
               // companion does not import a hard 400 rather than a live answer.
               // Cheap relative to the run. It must probe with exactly what the
               // run will use — the build manifest its rollup members resolve
               // through, and the givens — because the probe's whole job is to
               // decide routing on the same terms.
               //
               // The givens are the load-bearing half, and not for the reason one
               // would guess. A model-level `given:` does NOT cross the
               // companion's `import`, so the companion surfaces none: probing
               // WITH them makes a given-supplying query fail here and fall to
               // live, which is what we want, while probing without them lets it
               // compile and then fail at RUN with "unknown given … Model
               // surfaces []" — an escaped 400, the same class of bug the eager
               // compile above exists to prevent. Measured both ways; see the
               // givens test in preaggregation_seams.spec.ts.
               await candidate.getSQL({
                  givens: querySurfaceGivens,
                  buildManifest,
               });
               runnable = candidate;
               preaggRouted = true;
            } catch (preaggErr) {
               // Expected, and common: the synthesized model imports only the
               // sources it rolls up, so a query touching anything else does not
               // compile against it. Serving live is the right answer for those,
               // so this is debug rather than a warning.
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
         logger.error("Query parsing error", {
            error,
            errorMessage,
            environmentName: this.packageName,
            modelPath: this.modelPath,
            query,
            queryName,
            sourceName,
         });
         throw new BadRequestError(`Invalid query: ${errorMessage}`);
      }

      // Authoritative authorize gate: resolve the gated source from the
      // COMPILED query — the source Malloy actually runs (the LAST `run:`
      // statement) — not from surface syntax. Surface-syntax resolution alone
      // is both bypassable (first-statement regex vs. last-statement execution:
      // `run: ungated\nrun: gated` would gate `ungated` while running `gated`)
      // and over-restrictive, so this compiled check always runs and is the
      // source of truth; it handles named-query / blank-source / multi-statement
      // forms uniformly. Skip only the redundant re-probe when it's the same
      // source the early gate already cleared. Outside the loadQuery try so
      // AccessDeniedError stays a 403; independent of bypassFilters.
      const compiledSource =
         await this.resolveAuthorizeSourceFromRunnable(runnable);

      // Boundary backstop (the *what* axis, 404) BEFORE the authorize gate
      // (the *who* axis, 403): settle "deferred" early-gate admissions against
      // the compiled run target. Skipped for "cleared" admissions — an
      // explicitly-named exported query may read hidden sources internally
      // (the author's deliberate exposure), and must not be re-denied here.
      if (boundary === "deferred") {
         this.assertQueryBoundaryCompiled(compiledSource, query);
      }

      // Gate the compiled run target's own source PLUS the gate it carries from
      // what it derives from, and — new here — bind a row-level gate's filter
      // onto whatever ends up executing (authorizeAndBindRunnable). This MUST
      // run unconditionally, not just when compiledSource !== earlySource, and
      // its result MUST be assigned back to `runnable`: for a row-level gate,
      // the reassignment IS the enforcement — a caller that keeps the
      // pre-gate `runnable` around and runs THAT instead serves unfiltered rows.
      //
      // The early gate now reads the same entry-point walk this does, so for a
      // NAMED declared source the two agree — but agreeing is not the same as
      // being redundant. Surface syntax cannot resolve every run target: a
      // named-query or multi-statement form, or a source the caller DECLARED in
      // its own ad-hoc text (which does not exist in `modelDef.contents` at
      // early-gate time and so has no entry in `entryPointGatesBySource`), is
      // first identifiable here, from the compiled query. This is the
      // authoritative gate; the early one only fails fast.
      //
      // Do NOT re-add a hasAuthorize() guard here — it reads top-level sources'
      // OWN gates only, so guarding on it re-opens the inherited-gate bypass. The
      // walk is a cheap no-op for an ungated model. When compiledSource is
      // unknown/unresolved, nothing gates it — a `source:` is the only place
      // `#(authorize)` is declared. Note: on this path an ad-hoc inline
      // `duckdb.sql(...)` query is rejected by restricted mode (the raw-SQL
      // ban from loadRestrictedQuery above) before it can run, so the
      // raw-warehouse bypass is closed by restricted mode regardless.
      // `queryString` is the caller's own untrusted text — same one compiled
      // above — so the recompile stays inside restricted mode exactly as the
      // original compile did.
      runnable = await this.authorizeAndBindRunnable(runnable, givens ?? {}, {
         recompile: (mm) => mm.loadRestrictedQuery(queryString),
         bypassAuthorize,
         // The caller's OWN text, not `queryString` — which is the same text
         // after the filter-refinement injection and after the `sourceName` /
         // `queryName` forms have been synthesized into a `run:`. Only text the
         // caller actually wrote can declare the derivation this reads.
         callerQueryText: query,
      });
      // No post-hoc check of `queryHadRowLevelFilterAttached(runnable)` here:
      // when `routingBlockedByRowLevelGate` was false and routing succeeded
      // above, `runnable` at this point is the storage serve-shape's own
      // runnable, whose struct carries no `#(authorize)` annotation bytes —
      // so this walk can never find on it what the pre-check (walking the
      // real, annotated struct) already ruled out. A check here would be
      // unreachable dead code, not a backstop; correctness for storage
      // routing depends entirely on `queryEntryPointHasRowLevelGate` above
      // being sound.

      const maxRows = getMaxQueryRows();
      const maxBytes = getMaxResponseBytes();
      // `buildManifest` / `liveBuildManifest` / `querySurfaceGivens` are resolved
      // above the routing block, which needs them for its compile probe.
      //
      // The serve-shape runnable resolves its tables through `virtualMap`, not
      // the same-connection build manifest, and its transient model carries no
      // `##! experimental.persistence` — so passing a non-empty buildManifest to
      // it errors. When routing through the shape, suppress the manifest.
      //
      // Only the pre-aggregation companion may see the FULL manifest: its rollup
      // entries name sources that exist nowhere else, and the author's model has
      // no reason to carry the persistence flag those entries require.
      const effectiveBuildManifest = serveVirtualMap
         ? undefined
         : preaggRouted
           ? buildManifest
           : liveBuildManifest;

      // Prepare INSIDE the run try/catch: a bad-given / value-type throw at
      // prepare time (getPreparedResult binds the givens) gets the same
      // MalloyError→rethrow / else→400 handling as run, instead of escaping as
      // a 500. `executionTime` is captured after run() returns, so it spans
      // prepare AND the warehouse round trip — which is the span the success
      // histogram has always described itself as recording.
      let rowLimit = 0;
      let rowLimitSource: QueryRowLimitSource = "server_default";
      let executionTime = 0;
      let queryResults;
      let appliedQueryMetadata: QueryMetadata | undefined;
      // Same reason as effectiveBuildManifest: the serve shape is built from
      // given-FREE sources, so it surfaces no `given:` and Malloy rejects any
      // supplied name with "unknown given" — a spurious 400, past the routing
      // fallback, on a query that should just serve from storage. Nothing in the
      // shape can read them; the authorize gate above already saw the full set.
      const effectiveGivens = serveVirtualMap ? undefined : querySurfaceGivens;
      try {
         // The prepared result is also where the executing connection's name
         // comes from, which is what makes the connection's default metadata
         // layer resolvable BEFORE the statement is issued.
         const preparedResult = await runnable.getPreparedResult({
            givens: effectiveGivens,
            buildManifest: effectiveBuildManifest,
            virtualMap: serveVirtualMap,
         });
         const preparedLimit = preparedResult.resultExplore.limit;
         rowLimitSource = queryRowLimitSource(preparedLimit);
         rowLimit = resolveModelQueryRowLimit(preparedLimit, {
            defaultLimit: getDefaultQueryRowLimit(),
            maxRows,
         });
         appliedQueryMetadata = this.resolveQueryMetadata(
            queryMetadataInput,
            preparedResult.connectionName,
            // The run-target source already resolved for the authorize gate, not
            // the raw `sourceName` param: a `queryName` request names exactly one
            // source and must not lose its declared layer for having named it
            // indirectly, and ad-hoc text resolves through the same surface-syntax
            // path. Reusing the gate's answer also keeps one definition of "which
            // source is this query against" rather than a second, weaker one.
            //
            // The COMPILED target specifically, for the reason the authorize gate
            // treats it as the source of truth: `extractRunTargetSourceName`
            // reads the FIRST `run:` and Malloy executes the LAST, so `run:
            // cheap\nrun: expensive` would execute one source while tagging
            // another's team and tier — attribution that is not merely missing
            // but wrong, and wrong in the direction of blaming the cheap query.
            // Falls back to the surface-syntax answer when the compiled one is
            // unresolved, the same degradation the gate accepts.
            compiledSource ?? earlySource,
         );

         queryResults = await runnable.run({
            rowLimit,
            givens: effectiveGivens,
            abortSignal,
            buildManifest: effectiveBuildManifest,
            virtualMap: serveVirtualMap,
            queryMetadata: appliedQueryMetadata,
         });
         // AFTER run(), not before it. Taken above, this stopped at
         // getPreparedResult and so measured compile + authorize + routing +
         // prepare while excluding the warehouse round trip entirely — the one
         // part anyone reading "query duration" is asking about. The live-fallback
         // branch below already recomputed it post-run, so the same field meant
         // two different things depending on which path ran.
         executionTime = performance.now() - startTime;
      } catch (error) {
         // A binding that declares `freshnessFallback=live` is saying the tier is
         // a performance optimisation, not a dependency — so a store that fails
         // UNDER a routed query should degrade to serving live, the same answer
         // the compile-time ladder already gives when the shape can't be built.
         // Without this the store is a hard dependency the moment a query routes:
         // a not-yet-converged rebind or an over-eager GC turns into a user-facing
         // error for a source explicitly marked as safe to serve live.
         //
         // Deliberately narrow:
         //  - only when the query actually routed to storage (`serveVirtualMap`);
         //  - only when every binding THAT PRODUCED THIS SHAPE says `live` —
         //    `fail` (and the `stale_ok` default) keep surfacing, so one
         //    fail-closed source is not degraded by a permissive neighbour.
         //    Read off the shape's own bindings, not the package's: bindings are
         //    pushed package-wide and `freshnessFallback` is per entry, so a
         //    mixed set is normal and a sibling must not decide this query;
         //  - never for a client error (a bad given) or an abort, where a retry
         //    would just reproduce it or defy the caller;
         //  - the retry re-supplies the REAL givens. The storage path suppresses
         //    them because the shape is built from given-free sources; the live
         //    source may filter on them, and running it without them would serve
         //    unfiltered rows.
         const canDegradeToLive =
            !!serveVirtualMap &&
            !!liveRunnable &&
            !abortSignal?.aborted &&
            !String((error as { code?: string })?.code ?? "").startsWith(
               "runtime-given-",
            ) &&
            serveShapeBindings.length > 0 &&
            serveShapeBindings.every((b) => b.freshnessFallback === "live");
         // Both the original failure and a failure OF THE RETRY end here: record
         // the error metric, then map the error. A broad outage takes the source
         // warehouse down alongside the store, so the retry failing is ordinary —
         // and without this it would escape the given-mapping, the MalloyError
         // rethrow and the error metric, turning a clean 400 into an untracked 500.
         // Annotated on the CONST, not just the arrow: control-flow analysis only
         // treats a call as never-returning when the callee has an explicit type
         // annotation, and the fall-through below depends on that narrowing.
         const failQuery: (err: unknown) => never = (err) => {
            // Record error metrics
            const errorEndTime = performance.now();
            const errorExecutionTime = errorEndTime - startTime;
            this.queryExecutionHistogram.record(
               errorExecutionTime,
               this.queryMetricAttributes({
                  environment: queryMetadataInput?.environment,
                  queryName,
                  sourceName,
                  status: "error",
                  servedFrom,
               }),
            );

            // Bad client-supplied givens (unknown name, wrong-typed value, an
            // operator-finalized override, ...) all surface as a Malloy
            // `runtime-given-*` error. Malloy is the single validator; the publisher
            // just maps its rejection to a clean 400. Duck-type on `.code`
            // (MalloyCompileError extends Error, not MalloyError, and isn't
            // root-exported). The `runtime-given-` prefix is a pinned coupling to
            // Malloy's error codes (@malloydata/malloy given_binding.ts / runtime.ts);
            // if they're renamed upstream, update it here (and in environment.ts) —
            // otherwise these fall through to the generic 400 below with a worse
            // message, and the /compile path silently omits `sql`.
            // A gate is grafted INTO the query now, so its own givens are
            // bound by the same prepare/run — and Malloy's failure names the
            // one that could not bind ("Given 'ROLE' has no value and no
            // default. To fix: supply it via `.run({givens: {ROLE: …}})`").
            // Passing that through tells a denied caller the gate's given by
            // name, which `docs/authorize.md` promises never happens and
            // `authorizeReferencedGivenNames` exists to prevent. Refuse
            // opaquely instead whenever a gate applied to this query and one
            // of the names it reads went unsupplied — the same 403 a
            // whole-source gate returned before the graft existed. Checked
            // ahead of the `MalloyError` rethrow below, which is where a
            // PREPARE-time binding failure would otherwise escape.
            if (
               isGivenBindingFailure(err) &&
               this.queryHadRowLevelFilterAttached(runnable) &&
               [...this.authorizeReferencedGivenNames].some(
                  (name) => !(name in (givens ?? {})),
               )
            ) {
               logger.debug("Gate given unbound; denying opaquely", {
                  environmentName: this.packageName,
                  modelPath: this.modelPath,
                  error: err instanceof Error ? err.message : String(err),
               });
               recordRowLevelGateDecision("denied_by_gate");
               throw new AccessDeniedError(
                  `Access denied for source "${compiledSource ?? sourceName ?? "unknown"}".`,
               );
            }

            const givenCode = (err as { code?: string })?.code;
            if (
               typeof givenCode === "string" &&
               givenCode.startsWith("runtime-given-")
            ) {
               logger.debug("Rejected client-supplied given", {
                  environmentName: this.packageName,
                  modelPath: this.modelPath,
                  error: err instanceof Error ? err.message : String(err),
               });
               throw new BadRequestError(
                  err instanceof Error ? err.message : String(err),
               );
            }

            // Re-throw Malloy errors as-is (they will be handled by error handler)
            if (err instanceof MalloyError) {
               throw err;
            }

            // For other runtime errors (like divide by zero), throw as BadRequestError
            const errorMessage =
               err instanceof Error ? err.message : String(err);
            logger.error("Query execution error", {
               error: err,
               errorMessage,
               environmentName: this.packageName,
               modelPath: this.modelPath,
               query,
               queryName,
               sourceName,
            });
            throw new BadRequestError(
               `Query execution failed: ${errorMessage}`,
            );
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
            // Re-derive the row cap from the LIVE shape. `rowLimit` is assigned
            // inside the try above, from the storage prepare — so a store failure
            // that surfaces AT PREPARE (a manifest naming a table that is missing
            // or malformed resolves through `virtualMap` there) leaves it 0, which
            // the connector reads as a hard cap and stops before the first row: a
            // successful, EMPTY answer. Asking the live shape is also the honest
            // limit, since the live shape is what runs.
            // `liveBuildManifest`, not `buildManifest`: this retry runs on the
            // AUTHOR's model, which can neither reference a synthesized rollup
            // nor be assumed to carry `##! experimental.persistence`.
            const livePrepared = await liveRunnable!.getPreparedResult({
               givens: querySurfaceGivens,
               buildManifest: liveBuildManifest,
            });
            const livePreparedLimit = livePrepared.resultExplore.limit;
            rowLimitSource = queryRowLimitSource(livePreparedLimit);
            rowLimit = resolveModelQueryRowLimit(livePreparedLimit, {
               defaultLimit: getDefaultQueryRowLimit(),
               maxRows,
            });
            // Re-resolve rather than reuse: the bag above was resolved against
            // the STORAGE connection, which on this tier is routinely not the
            // one the live shape runs on, so reusing it would stamp another
            // connection's default and enforced layers on this statement. The
            // correlation id rides in the input, so the retry keeps the id the
            // response returns — one API call, one join key, two statements.
            appliedQueryMetadata = this.resolveQueryMetadata(
               queryMetadataInput,
               livePrepared.connectionName,
               // Same compiled run target as the primary path — the retry runs
               // the same query, so it must not tag a different source.
               compiledSource ?? earlySource,
            );
            queryResults = await liveRunnable!.run({
               rowLimit,
               givens: querySurfaceGivens,
               abortSignal,
               buildManifest: liveBuildManifest,
               queryMetadata: appliedQueryMetadata,
            });
         } catch (retryError) {
            failQuery(retryError);
         }
         // The answer came from the live warehouse, so it is NOT a storage hit —
         // `runtime_live_fallback` above is the signal that the tier is degraded.
         servedFrom = "live_fallback";
         executionTime = performance.now() - startTime;
         // Fall through: `queryResults` is set, so the normal post-run path
         // wraps and returns it exactly as a live query would.
      }

      // A row-level gate that applied cleanly and matched nothing is a normal
      // 200 with zero rows — the deliberate readable-but-empty posture, not an
      // error — but it is otherwise indistinguishable from a source that is
      // genuinely empty, so record it. `runnable` here is whatever the gate
      // step above returned; the live-fallback retry never reaches this line
      // with a row-level gate attached, because `canDegradeToLive` requires
      // `serveVirtualMap`, which is only ever set when storage routing
      // succeeded — and `routingBlockedByRowLevelGate` keeps a row-level-gated
      // query out of that block in the first place.
      if (
         this.queryHadRowLevelFilterAttached(runnable) &&
         queryResults.totalRows === 0
      ) {
         recordRowLevelGateDecision("empty_after_filter");
      }
      // Rows first, and above `wrapResult` rather than merely above the
      // serialize. A row overflow is a `maxRows + 1`-row result by construction,
      // and `wrapResult` deep-converts every row into Cell objects, an object
      // graph larger than the JSON string built from it. Checking here means the
      // process does neither for a response it is about to refuse, and the caller
      // is told it exceeded PUBLISHER_MAX_QUERY_ROWS rather than that the response
      // could not be serialized. The notebook path below has the same ordering.
      assertWithinModelRowLimit(queryResults.totalRows, maxRows, "model_query");
      const wrappedResult = API.util.wrapResult(queryResults);
      // Best-effort byte check: we've already buffered `queryResults` and
      // built `wrappedResult` by the time we get here, so this surfaces
      // oversize responses with a clean HTTP 413 instead of letting the
      // controller transmit a half-megabyte payload — it is not OOM
      // prevention. True prevention requires streaming `Result`
      // construction, which is out of scope for this step. The row cap
      // above is the primary OOM defense.
      // One serialization, of the shape this caller asked for, measured here and
      // returned so nothing stringifies it again. `stringifyQueryResponse` runs
      // whether or not `maxBytes` is set: the unserializable case is a 413
      // regardless, and `maxBytes` decides only whether the size is compared and
      // whether the message names a cap.
      //
      // Both neighbouring choices were wrong for opposite reasons. Leaving the
      // caller to stringify is what let an unserializable response escape as the
      // bare 500 this guard replaces. Measuring a shape the caller did not ask
      // for refuses a `compactJson` request on the wrapped result's bytes, which
      // it never receives.
      //
      // `wrapResult` above is NOT conditional, so a `compactJson` request still
      // builds the wrapped object graph; only the second JSON pass is saved.
      // Callers need `result` itself (render-tag validation reads the schema
      // annotations that flat rows drop), so skipping the wrap is a bigger change
      // than this one.
      //
      // MCP asks for `"compact"` and then builds its own envelope from those rows
      // (mcp/query_envelope.ts), serializing them with an unguarded
      // `JSON.stringify` at indent 2 and truncating to 90k characters. Compact
      // rows that serialize while that indented envelope does not still throw a
      // raw RangeError there. Guarding it belongs with that budget; follow-up.
      const serializedResult = stringifyQueryResponse(
         responseShape === "compact" ? queryResults.data.value : wrappedResult,
         queryResults.totalRows,
         maxBytes,
         "model_query",
         responseShape === "compact" ? bigIntReplacer : undefined,
      );
      assertWithinModelByteLimit(serializedResult, maxBytes, "model_query");
      const metricAttributes = this.queryMetricAttributes({
         environment: queryMetadataInput?.environment,
         queryName,
         sourceName,
         status: "success",
         connection: queryResults.connectionName,
         servedFrom,
         rowsLimit: rowLimit,
      });
      this.queryExecutionHistogram.record(executionTime, metricAttributes);
      // Only advanced when the backend actually reported a figure, so the counter
      // stays absent rather than reading a false zero on a backend that cannot
      // report one. A query served from storage never reaches here with a cost:
      // it touched no warehouse, which is the entire point.
      const queryCostBytes = queryResults.runStats?.queryCostBytes;
      if (queryCostBytes !== undefined) {
         this.queryScannedBytesCounter.add(queryCostBytes, metricAttributes);
      }
      return {
         result: wrappedResult,
         serializedResult,
         compactResult: queryResults.data.value,
         modelInfo: this.modelInfo,
         dataStyles: this.dataStyles,
         // The cap actually pushed into the SQL. A caller cannot otherwise tell
         // a complete result from one the row limit cut off: with no LIMIT of
         // its own a query silently gets DEFAULT_QUERY_ROW_LIMIT rows, and that
         // is under maxRows, so assertWithinModelRowLimit raises nothing.
         // Returning the number lets a caller compare it against the row count
         // and say so, with no extra query.
         rowLimit,
         // Whether that cap was the author's own limit:/top: or the silent
         // default. Only the second means rows were probably left behind; a
         // deliberate `top: 10` returning 10 rows is a complete answer.
         rowLimitSource,
         // The id from the bag that was actually attached, not the one supplied:
         // a bag shed under budget pressure sheds context last, but a caller
         // should be told the truth about what it can look up.
         queryCorrelationId: appliedQueryMetadata?.query_id ?? null,
         // Where the answer came from, and how long producing it took. Both are
         // measured here already for the histogram; returning them lets a caller
         // attribute a SINGLE query, which an aggregate histogram cannot do —
         // "this chart is slow" needs the one query, not the p95.
         //
         // `servedFrom` is the only way a caller can tell a storage-served answer
         // from a live one: the rows are identical by design, so without it the
         // whole point of materializing is invisible at the call site.
         servedFrom: servedFrom ?? null,
         // Wall-clock around execution, not the HTTP round trip: it excludes
         // serialization and transport, so it is comparable against the histogram
         // and against the same query on another connection, and it is NOT what a
         // caller's own stopwatch will read.
         executionTimeMs: Math.round(executionTime),
         // What the warehouse will bill for this query, when it says. Null on a
         // backend that does not report it — which is NOT the same as zero, and
         // is why the field is nullable rather than defaulted. A storage-served
         // query legitimately has no warehouse cost, and reports null for the
         // opposite reason: there was no warehouse in the path at all.
         queryCostBytes: queryCostBytes ?? null,
      };
   }

   /**
    * The label set for {@link queryExecutionHistogram}. Every recording site
    * builds its labels here so the success and error paths cannot drift apart.
    *
    * <b>The query text and the returned row count are deliberately absent.</b>
    * Both are unbounded: ad-hoc query text yields a new label value for every
    * distinct query a caller ever sends, and a row count yields one for every
    * distinct result size. A histogram label multiplies by the bucket count, so
    * either one makes this metric grow without limit for as long as the process
    * serves traffic — the classic Prometheus cardinality leak, and the reason
    * it is worth stating here rather than letting someone re-add them. Both
    * remain on the request log, where cardinality costs nothing.
    *
    * `environment` and `package` are what make the metric attributable. Without
    * them the only identity is a bare model path (`report.malloynb`), which is
    * neither unique across packages nor able to answer "whose queries are these".
    *
    * `rows_limit` stays: its values are the server default plus whatever limits
    * authors actually write, which is a small set in practice — but it is the one
    * label here bounded by convention rather than by construction.
    */
   private queryMetricAttributes(args: {
      environment?: string;
      queryName?: string;
      sourceName?: string;
      status: "success" | "error";
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
         // Ships dark: only tag queries that actually reached the storage routing
         // decision. A live query on a deployment with no storage destination gets
         // no new label, so its histogram is byte-for-byte what it was before.
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
    * The author-declared layers ARE included, composed by
    * {@link composeDeclaredQueryMetadata} exactly as the build path composes
    * them. This reverses an earlier decision to omit them, which read
    * `materialization.queryMetadata` as describing only how a persist source is
    * BUILT and reasoned that a live query is a different unit of work.
    *
    * What that reasoning missed is which properties the layer actually carries.
    * The build-identifying properties — `run_id`, `trigger`, `source`, `class` —
    * come from the CONTEXT layer, which is per-statement and never inherited, so
    * a served query cannot be mistaken for a build no matter what the author
    * declared. The declared layer carries the author's own vocabulary (a team, a
    * cost centre, a data tier), which describes the SOURCE and is as true of a
    * query reading it as of the build writing it. Omitting it meant a deployment
    * could attribute its builds and not the interactive traffic that is most of
    * its warehouse bill.
    *
    * The block is named `materialization.queryMetadata` for historical reasons;
    * the name is narrower than the thing it declares.
    *
    * Fails open, like every other metadata path: a connection whose config can't
    * be read contributes no default rather than failing the query, and an
    * unparseable annotation contributes no layer rather than throwing.
    */
   private resolveQueryMetadata(
      input: ModelQueryMetadataInput | undefined,
      connectionName: string | undefined,
      sourceName?: string,
   ): QueryMetadata | undefined {
      // Nothing below is observable when the feature is off: `mergeQueryMetadata`
      // early-returns, so every layer assembled here is discarded. Assembling
      // them anyway made a default deployment — the mode is off unless an
      // operator turns it on — pay an annotation walk and a connection lookup on
      // every query for a bag nobody reads. Read per statement rather than per
      // boot for the same reason `mergeQueryMetadata` reads it there: the mode
      // is allowed to change under a running server.
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
    * The model file's own `##` tag, or undefined if it is absent or fails to
    * parse. Read off `modelDef`, which survives the worker serialization
    * boundary — so this works for a freshly-compiled model and a deserialized
    * one alike, the same reason {@link authorizeReferencedGivenNames} is
    * derived from it in the constructor rather than shipped over the wire.
    *
    * The file's OWN notes, not the folded import lineage. `modelAnnotations`
    * folds deliberately, but only because a file-level `##(authorize)` gate an
    * import could shed would be no gate at all; `annotations.ts` says to read
    * through `ownModelNotes` for everything that is not a policy gate. A tag is
    * not a gate, and folding one would let a shared include attribute every
    * importing file's traffic to the include's team — the same misattribution
    * {@link safeSourceTag} already refuses for a derivation base. It would also
    * report the resulting publish warning against the importer's path, sending
    * an author to a file that does not contain the line.
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
    * Callers pass the source the server ALREADY resolved for the authorize gate,
    * not the raw `sourceName` request param — a `queryName` request names one
    * source indirectly and must not lose its declared layer for it, and ad-hoc
    * text resolves through the same surface-syntax path. What remains
    * unresolvable is a statement with no single run target (some notebook
    * cells); those carry the package and model-file layers only.
    *
    * Reads the named source's OWN annotations and does not walk its derivation
    * base, so `source: a is b extend {…}` inherits nothing from `b`'s
    * declaration. This diverges from `./gate_registry_walk`'s
    * `ancestorGateExprs`, which walks ancestors deliberately because a gate
    * an extension could shed would be no gate at all. A cost label carries no
    * such requirement, and inheriting one would attribute `a`'s traffic to
    * `b`'s team.
    */
   private safeSourceTag(
      sourceName: string | undefined,
   ): ReadableTag | undefined {
      if (!sourceName || !this.modelDef) return undefined;
      try {
         const entry = this.modelDef.contents?.[sourceName];
         // The docstring's "not a top-level source" case, now actually checked.
         // `contents` also holds NAMED QUERIES, and a `#@` on one of those is
         // not a source's declaration — reading it resolved a query's tag as
         // though a source had declared it, and listed the query's name among
         // the package's tagged sources in the publish warnings.
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
      return {
         type: "source",
         packageName: this.packageName,
         modelPath: this.modelPath,
         malloyVersion: MALLOY_VERSION,
         dataStyles: JSON.stringify(this.dataStyles),
         modelDef: JSON.stringify(this.modelDef),
         // `this.modelInfo` is precomputed once at construction (either
         // by the worker or in the Model.create constructor); don't
         // re-run `modelDefToModelInfo` on every API hit.
         modelInfo: JSON.stringify(this.modelInfo ?? {}),
         sourceInfos: this.getSourceInfos()?.map((sourceInfo) =>
            JSON.stringify(sourceInfo),
         ),
         // Discovery surface: an explore lists only its export closure
         // (getSources/getQueries curate); `this.sources` stays complete for
         // enforcement and resolution.
         sources: this.getSources(),
         queries: this.getQueries(),
         givens: this.givens,
      } as ApiCompiledModel;
   }

   /**
    * Serialize a notebook cell's `newSources` to the wire shape (an array
    * of JSON strings), embedding the model-level `givens` on every
    * SourceInfo so consumers iterating `newSources` can render `given:`
    * inputs without a second getModel round-trip. Matches `Source.givens`
    * in the API spec ("Identical to CompiledModel.givens") and how
    * `getSources` already copies the full list onto each CompiledModel
    * source. When the model declares no givens, the SourceInfo is emitted
    * untouched (no empty `givens` key).
    *
    * Shared by `getNotebookModel` (the notebook GET endpoint) and
    * `executeNotebookCell` (the cell-run endpoint) so both surface givens
    * identically.
    */
   private serializeNewSources(
      newSources: Malloy.SourceInfo[] | undefined,
   ): string[] | undefined {
      return newSources?.map((source) =>
         JSON.stringify(
            this.givens && this.givens.length > 0
               ? { ...source, givens: this.givens }
               : source,
         ),
      );
   }

   private async getNotebookModel(): Promise<ApiRawNotebook> {
      // Return raw cell contents without executing them
      const notebookCells: ApiNotebookCell[] = (
         this.runnableNotebookCells as RunnableNotebookCell[]
      ).map((cell) => {
         return {
            type: cell.type,
            text: cell.text,
            newSources: this.serializeNewSources(cell.newSources),
            queryInfo: cell.queryInfo
               ? JSON.stringify(cell.queryInfo)
               : undefined,
         } as ApiNotebookCell;
      });

      // A notebook's own `##` tags, not its imports': `ownModelNotes` does NOT
      // fold the import lineage the way `modelAnnotations` (`./annotations`)
      // does, so a shared include carrying its own `##(filters)` does not
      // configure the filter panel of every notebook that imports it.
      const allAnnotations = this.modelDef ? ownModelNotes(this.modelDef) : [];

      // `allAnnotations` is already this notebook's own `##` only (see above),
      // which is exactly the scope these three describe: `title`, `autorun` and
      // the starting `givens` belong to one document, and reading them off the
      // folded import lineage would let a shared include set them for every
      // notebook importing it.
      const notebookTag = motlyTag(allAnnotations);

      // No `as` cast. The literal used to carry `type`, `modelPath`,
      // `modelInfo`, and `queries`, which `RawNotebook` did not declare, so it
      // needed one — and a blanket cast over an object literal accepts a stale
      // field name after a rename in `api-doc.yaml`, typechecking clean while
      // the client reads undefined. The schema now declares every field this
      // returns, so the next rename fails here instead.
      const notebook: ApiRawNotebook = {
         type: "notebook",
         packageName: this.packageName,
         modelPath: this.modelPath,
         malloyVersion: MALLOY_VERSION,
         modelInfo: JSON.stringify(this.modelInfo ?? {}),
         // Raw-notebook view is uncurated (complete `this.sources`/`this.queries`,
         // not the export-filtered `getSources`/`getQueries`): notebooks can't be
         // imported, so their in-file sources have no internal/import-only role to
         // hide — they're always public. Model files curate; notebooks don't.
         sources: this.modelDef && this.sources,
         queries: this.modelDef && this.queries,
         annotations: allAnnotations,
         // Derived here rather than left to the client, so `## autorun=false`
         // on a notebook and `# artifact { autorun=false }` on a dashboard
         // arrive as the same field with the same default. Same for
         // `## givens { … }` and the artifact tag's `givens { … }`.
         // Known gap, measured: an `@env.` anywhere on the SAME line takes the
         // whole tag, so `## title=@env.X autorun=false` arrives as
         // `autorun=true`, the default, with the flag silently lost. The same
         // holds for an ordinary malformed `##` line, because nothing calls
         // `motlyParseErrors` on a notebook's model-level tags at all, so the
         // parse error it already produces has no reader. Dashboards report both
         // through the package lint; notebooks have no equivalent channel yet.
         // Not closed here: it needs a notebook warnings surface, which is its
         // own change. Filed as a follow-up.
         autorun: readAutorun(notebookTag),
         startingGivens: readStartingGivens(
            notebookTag,
            (name) => (this.givens ?? []).find((g) => g.name === name)?.type,
         ),
         notebookCells,
      };
      return notebook;
   }

   public async executeNotebookCell(
      cellIndex: number,
      filterParams?: FilterParams,
      bypassFilters?: boolean,
      givens?: Record<string, GivenValue>,
      // See `getQueryResults`: forwarded into `runnable.run` so the
      // publisher's wall-clock timeout actually cancels the query.
      abortSignal?: AbortSignal,
      // A notebook cell issues real backend SQL on an interactive path, so it is
      // tagged like any other query. No correlation id: the cell response has no
      // field to hand one back on, and an id nobody can read costs the result
      // cache for nothing.
      queryMetadataInput?: ModelQueryMetadataInput,
   ): Promise<{
      type: "code" | "markdown";
      text: string;
      queryName?: string;
      result?: string;
      newSources?: string[];
   }> {
      if (this.compilationError) {
         throw this.compilationError;
      }

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
            text: cell.text,
         };
      }

      // For code cells, execute the runnable if available
      let queryName: string | undefined = undefined;
      let queryResult: string | undefined = undefined;

      if (cell.runnable) {
         try {
            let runnableToExecute = cell.runnable;
            // The text the runnable that actually executes was built from —
            // starts as the cell's own text and is updated below if a
            // `#(filter)` refinement rebuilds the query, so the authorize gate
            // recompiles against whichever text ends up running.
            let textToExecute = cell.text;

            // The model to graft a row-level gate against, and — via
            // `usesOwnScope` — HOW to bind it. See
            // `resolveNotebookCellGraftScope`'s doc for the full decision;
            // in short: the nearest EARLIER code cell's own scope when this
            // cell's run target resolves there (the `local2` case —
            // `source: local2 is gated extend {…}` then `run: local2 -> …`,
            // where `gated` was declared earlier), else this cell's OWN
            // post-declaration scope — needed whenever no earlier cell
            // covers the gate at all: the first code cell, a cell preceded
            // only by markdown, or a LATER cell that both declares and runs
            // its OWN gated source in one cell. Recompiling a cell's text
            // against a scope that already holds whatever that cell just
            // declared fails with "Cannot redefine" the moment `loadQuery`
            // re-parses the `source:` line — true of the model-wide
            // cumulative scope AND of a cell's own scope alike — which is
            // why the own-scope fallback below binds by repointing a
            // compiled queryDef instead of recompiling any text.
            const { graftScope, usesOwnScope } =
               await this.resolveNotebookCellGraftScope(
                  cellIndex,
                  cell.runnable,
               );
            if (!graftScope) {
               // The one case NEITHER scope can cover: this cell has no
               // compiled (modelDef, modelMaterializer) pair of its own
               // (`selfGraftScopeForCell` returned undefined) and there is no
               // earlier code cell either. An operator seeing an
               // `AccessDeniedError` out of this cell should be able to tell
               // "there was nowhere at all to attach a graft" apart from "the
               // gate's own condition failed" — logged here, cheap (no
               // compile), independent of whether this cell turns out to
               // carry a row-level gate at all. The caller-facing error text
               // is unaffected either way.
               logger.debug(
                  "Notebook cell has no graft scope to attach a row-level gate against (no earlier code cell, and this cell has no compiled model of its own)",
                  { modelPath: this.modelPath, cellIndex },
               );
            }

            // Whether a `#(filter)` refinement will actually rebuild
            // `cell.runnable` into a new (possibly broken) `QueryMaterializer`
            // below — computed up front, before either the pre-refinement
            // gate call or the rebuild itself, so the pre-call can be skipped
            // whenever nothing downstream can change its answer. Cheap: no
            // compile, just a source-name extraction and a filter-map lookup.
            const effectiveSource =
               !bypassFilters && cell.modelMaterializer
                  ? extractRunTargetSourceName(cell.text)
                  : undefined;
            const cellFilters = effectiveSource
               ? this.getFilters(effectiveSource)
               : [];

            // Pre-refinement gate call: probe `cell.runnable` — the
            // UNREFINED query — for an entry-point gate BEFORE the
            // `#(filter)` refinement rebuild below, which recompiles the
            // query and can itself throw if the refined text fails to
            // compile. Without this, a caller a gate would have denied could
            // instead hit a broken refinement first: resolving
            // the broken runnable's source silently swallows the compile
            // failure and returns `undefined`, so no gate is found, and the
            // eventual failure surfaces as a Malloy-worded 400 instead of the
            // 403 the gate would have produced. No data ever escapes either
            // way (the query still never runs), but behavior must stay
            // byte-identical to the prior release, and a security-path error
            // code changing is a regression even when nothing leaked.
            // `probeEntryPointGates` — not `authorizeAndBindRunnable` itself
            // — is used here because a `row_level` gate must be DEFERRED
            // (the same treatment `assertAuthorized` already gives a
            // row-level gate it finds), not denied outright the way
            // `authorizeAndBindRunnable` would with no `recompile` to hand
            // it: the post-refinement call below is the one authoritative
            // enforcement point.
            //
            // Skipped entirely when this cell has no `#(filter)` refinement
            // to apply: with nothing to rebuild `cell.runnable` into, the
            // post-refinement authoritative bind below runs against that SAME
            // unrefined runnable, so this call would evaluate the identical
            // gate a second time for nothing — doubling the probe/graft work
            // for no reason. This is also what keeps behavior byte-identical to
            // the prior release for the common (no `#(filter)` refinement)
            // cell: with no filters on that path, this call never ran before
            // either.
            //
            // Keyed on `cellFilters.length` rather than on a built
            // `filterClause`, and placed BEFORE `buildFilterClause` runs, for
            // the same reason the call exists at all: `buildFilterClause`
            // itself throws `FilterValidationError` on a bad `filterParams`,
            // which the catch below turns into a 400. Building the clause
            // first would let malformed filter params preempt the gate and
            // turn a denied caller's 403 into that 400 — the same
            // security-path error-code regression this call was added to
            // prevent, just one step earlier in the sequence.
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

            // If filters need to be applied, rebuild the query with the
            // refinement computed above.
            if (filterClause && cell.modelMaterializer) {
               textToExecute = injectFilterRefinement(cell.text, filterClause);
               runnableToExecute =
                  cell.modelMaterializer.loadQuery(textToExecute);
            }

            // Authorize gate — only cells that actually run a query touch
            // data, so gate exactly those (a source-def / import cell has no
            // runnable and accesses nothing). Gates the COMPILED cell query's
            // own source (nothing, for an unknown/inline source — a
            // `source:` is the only place `#(authorize)` is declared) PLUS
            // the gate that source carries from what it derives from, and
            // binds a row-level gate's filter onto
            // the runnable that ACTUALLY EXECUTES — not `cell.runnable`
            // (pre-refinement): grafting that would let the recompile step
            // silently drop the `#(filter)` refinement above instead of
            // composing with it. Notebook cells are author-curated, so the
            // recompile uses `loadQuery` — same as the refinement rebuild
            // just above — never the query path's `loadRestrictedQuery`.
            //
            // This is the AUTHORITATIVE bind — it runs AFTER the refinement
            // rebuild (it needs `textToExecute`, which that rebuild may have
            // changed) and its result is what actually executes. The
            // pre-refinement `probeEntryPointGates` call above is a
            // best-effort fast-fail for the common case; this call is what
            // enforcement depends on regardless of whether that one ran or
            // agreed. `AccessDeniedError` staying a 403 (not the generic 400
            // this catch block otherwise wraps everything into) is handled by
            // the explicit rethrow at the top of the catch below, independent
            // of `bypassFilters`.
            if (cell.modelMaterializer) {
               const textForRecompile = textToExecute;
               // `usesOwnScope` selects the recompile strategy (see
               // `resolveNotebookCellGraftScope`'s doc): the ordinary text
               // recompile against an earlier cell's scope, or — when this
               // cell's own source must be the graft target — repointing
               // this cell's own already-compiled queryDef at a grafted
               // clone of its own model via `_loadQueryFromQueryDef`, never
               // re-parsing any text. The queryDef is read off
               // `runnableToExecute` (not `cell.runnable`) so a `#(filter)`
               // refinement rebuild above is still the one that ends up
               // executing.
               // Own-scope binding grafts the cell's own compiled queryDef and
               // carries its own proof; the two arrive together from
               // `ownScopeQueryDefBinder` precisely so neither can be wired up
               // without the other. See its doc for why this cell cannot
               // simply recompile its text.
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

            const cellMaxRows = getMaxQueryRows();
            const cellMaxBytes = getMaxResponseBytes();
            // Per-query freshness gate (see getQueryResults): the same
            // freshness-filtered manifest gates notebook-cell queries — minus
            // pre-aggregation's rollups, which belong to the companion model and
            // are never referenced from a notebook cell. A notebook has no
            // companion (it declares no sources to roll up), so unlike
            // getQueryResults there is no branch here: the live view is the only
            // one that applies.
            const buildManifest = this.withoutPreaggregateEntries(
               this.resolveFreshBuildManifest(),
            );
            // See getQueryResults / filterGivensToModelSurface: the gate
            // above already saw the full unfiltered givens.
            const cellSurfaceGivens = this.filterGivensToModelSurface(givens);
            const preparedCell = await runnableToExecute.getPreparedResult({
               givens: cellSurfaceGivens,
               buildManifest,
            });
            const rowLimit = resolveModelQueryRowLimit(
               preparedCell.resultExplore.limit,
               {
                  defaultLimit: getDefaultQueryRowLimit(),
                  maxRows: cellMaxRows,
               },
            );
            // The compiled run target, preferred over the cell's surface syntax
            // for the reason getQueryResults prefers it: `extractRunTargetSourceName`
            // reads the first `run:` and Malloy executes the last, so a cell
            // holding more than one would tag the wrong source. The prepared
            // query is read again below, so this costs nothing new.
            const cellCompiledSource =
               await this.resolveAuthorizeSourceFromRunnable(runnableToExecute);
            // A notebook cell never takes `getQueryResults`' constant-false
            // short circuit: it always runs, even for a gate proven to admit
            // no row. That is a cost (one zero-scan warehouse round trip),
            // never a correctness gap — the grafted `where: false` is what
            // produces the empty result either way.
            const result = await runnableToExecute.run({
               rowLimit,
               givens: cellSurfaceGivens,
               abortSignal,
               buildManifest,
               queryMetadata: this.resolveQueryMetadata(
                  queryMetadataInput,
                  preparedCell.connectionName,
                  // Same resolution the cell's own filter lookup uses, so a
                  // notebook cell carries the source's declared layer too.
                  cellCompiledSource ?? extractRunTargetSourceName(cell.text),
               ),
            });
            const query = (await runnableToExecute.getPreparedQuery())._query;
            queryName = (query as NamedQueryDef).as || query.name;
            // Same reasoning as getQueryResults: a row-level gate that applied
            // cleanly and matched nothing is a normal empty result, not an
            // error, but worth telling apart from a source that is genuinely
            // empty.
            if (
               result?._queryResult &&
               result.totalRows === 0 &&
               this.queryHadRowLevelFilterAttached(runnableToExecute)
            ) {
               recordRowLevelGateDecision("empty_after_filter");
            }
            // Same ordering as getQueryResults: rows first, so a row overflow is
            // not reported as an unserializable response.
            if (result?._queryResult) {
               assertWithinModelRowLimit(
                  result.totalRows,
                  cellMaxRows,
                  "notebook_cell",
               );
            }
            queryResult =
               result?._queryResult &&
               this.modelInfo &&
               // Same guard as getQueryResults, and here too the string is the
               // payload rather than a throwaway measurement: a cell whose
               // result cannot be serialized would otherwise return the same
               // bare 500.
               stringifyQueryResponse(
                  API.util.wrapResult(result),
                  result.totalRows,
                  cellMaxBytes,
                  "notebook_cell",
               );
            // Same caveat as `getQueryResults`: by the time we measure
            // bytes the response has already been buffered and stringified,
            // so this is loud-failure detection (clean 413 instead of
            // partial transmission), not OOM prevention. The row cap above
            // is the primary defense.
            if (result?._queryResult && queryResult) {
               assertWithinModelByteLimit(
                  queryResult,
                  cellMaxBytes,
                  "notebook_cell",
               );
            }
         } catch (error) {
            // The authorize gate above now runs INSIDE this try (it needs the
            // filter-refined text), so its `AccessDeniedError` must be
            // rethrown here before anything below reshapes it — otherwise it
            // falls through to the generic `BadRequestError` at the bottom of
            // this catch and a 403 silently becomes a 400.
            if (error instanceof AccessDeniedError) {
               throw error;
            }
            if (error instanceof FilterValidationError) {
               throw new BadRequestError(error.message);
            }
            // Bad client-supplied givens (unknown name, wrong-typed value,
            // finalized override, ...) surface as a Malloy `runtime-given-*`
            // error; see getQueryResults. Malloy validates, the publisher maps
            // to 400. Duck-type on `.code` (not a MalloyError, not root-exported).
            const givenCode = (error as { code?: string })?.code;
            if (
               typeof givenCode === "string" &&
               givenCode.startsWith("runtime-given-")
            ) {
               logger.debug("Rejected client-supplied given", {
                  environmentName: this.packageName,
                  modelPath: this.modelPath,
                  error: error instanceof Error ? error.message : String(error),
               });
               throw new BadRequestError(
                  error instanceof Error ? error.message : String(error),
               );
            }
            if (error instanceof MalloyError) {
               throw error;
            }
            // Surface PayloadTooLargeError as-is so the error middleware
            // maps it to HTTP 413; without this it would get swallowed
            // into a generic 400 BadRequestError below.
            if (error instanceof PayloadTooLargeError) {
               throw error;
            }
            const errorMessage =
               error instanceof Error ? error.message : String(error);
            if (errorMessage.trim() === "Model has no queries.") {
               return {
                  type: "code",
                  text: cell.text,
               };
            } else {
               logger.error("Error message: ", errorMessage);
            }
            throw new BadRequestError(`Cell execution failed: ${errorMessage}`);
         }
      }

      return {
         type: cell.type,
         text: cell.text,
         queryName: queryName,
         result: queryResult,
         newSources: this.serializeNewSources(cell.newSources),
      };
   }

   /**
    * `overlay` maps an absolute `file://` URL to text served INSTEAD of reading
    * that path from disk; anything not in the map reads from disk as usual. It
    * exists for pre-aggregation, whose synthesized model is generated per load
    * and never written to disk, and which must resolve its `import` of the
    * author's model relative to the real package directory — so the synthesized
    * text needs a URL inside that directory without a file behind it. `modelPath`
    * still has to exist: it is what anchors `importBaseURL`.
    */
   static async getModelRuntime(
      packagePath: string,
      modelPath: string,
      malloyConfig: ModelConnectionInput,
      options?: {
         buildManifest?: BuildManifest["entries"];
         overlay?: ReadonlyMap<string, string>;
      },
   ): Promise<{
      runtime: Runtime;
      modelURL: URL;
      importBaseURL: URL;
      dataStyles: DataStyles;
      modelType: ModelType;
   }> {
      // Contain the caller-supplied model path inside the package directory;
      // a path that resolves outside it is reported as a missing model, which
      // is all a caller is entitled to learn.
      let fullModelPath: string;
      try {
         fullModelPath = safeJoinUnderRoot(packagePath, modelPath);
      } catch {
         throw new ModelNotFoundError(`${modelPath} does not exist.`);
      }
      try {
         if (!(await fs.stat(fullModelPath)).isFile()) {
            throw new ModelNotFoundError(`${modelPath} is not a file.`);
         }
      } catch {
         throw new ModelNotFoundError(`${modelPath} does not exist.`);
      }

      let modelType: ModelType;
      if (modelPath.endsWith(MODEL_FILE_SUFFIX)) {
         modelType = "model";
      } else if (modelPath.endsWith(NOTEBOOK_FILE_SUFFIX)) {
         modelType = "notebook";
      } else {
         throw new ModelNotFoundError(
            `${modelPath} is not a valid model name.  Model files must end in .malloy or .malloynb.`,
         );
      }

      const modelURL = new URL(`file://${fullModelPath}`);
      const baseUrl = new URL(".", modelURL);
      const importBaseURL = baseUrl;
      const overlay = options?.overlay;
      const urlReader = new HackyDataStylesAccumulator(
         overlay && overlay.size > 0
            ? {
                 readURL: async (url: URL) =>
                    overlay.get(url.href) ?? (await URL_READER.readURL(url)),
              }
            : URL_READER,
      );

      // Request runtimes borrow the cached package MalloyConfig. The package
      // owns release; callers must not release this runtime per request.
      const runtime = new Runtime({
         urlReader,
         config: Model.toMalloyConfig(malloyConfig),
         buildManifest: options?.buildManifest
            ? { entries: options.buildManifest, strict: false }
            : undefined,
      });
      const dataStyles = urlReader.getHackyAccumulatedDataStyles();
      return { runtime, modelURL, importBaseURL, dataStyles, modelType };
   }

   private static toMalloyConfig(input: ModelConnectionInput): MalloyConfig {
      if (input instanceof MalloyConfig) {
         return input;
      }

      const malloyConfig = new MalloyConfig({ connections: {} });
      malloyConfig.wrapConnections(
         () => new FixedConnectionMap(input, "duckdb"),
      );
      return malloyConfig;
   }

   private static getQueries(modelDef: ModelDef): {
      queries: ApiQuery[];
      misplacedAuthorize: MisplacedAuthorizeAnnotation[];
   } {
      // Shared with the package-load worker — see service/source_extraction.ts.
      const { queries, misplacedAuthorize } =
         extractQueriesFromModelDef(modelDef);
      return { queries: queries as unknown as ApiQuery[], misplacedAuthorize };
   }

   private static getSources(
      modelDef: ModelDef,
      givens?: ApiGiven[],
   ): {
      sources: ApiSource[];
      filterMap: Map<string, FilterDefinition[]>;
      authorizeMap: AuthorizeMap;
      misplacedAuthorize: MisplacedAuthorizeAnnotation[];
      authorizeOwnNotes: Map<string, AnnotationNote[]>;
      attributedAuthorizeOwnNotes: Map<string, AnnotationNote[]>;
   } {
      // Shared with the package-load worker — see service/source_extraction.ts.
      // The service path logs filter parse failures; the worker stays silent.
      const {
         sources,
         filterMap,
         authorizeMap,
         misplacedAuthorize,
         authorizeOwnNotes,
         attributedAuthorizeOwnNotes,
      } = extractSourcesFromModelDef(modelDef, givens, (sourceName, err) =>
         logger.warn(
            `Failed to parse filter annotations on source "${sourceName}"`,
            { error: err },
         ),
      );
      return {
         sources: sources as unknown as ApiSource[],
         filterMap,
         authorizeMap,
         misplacedAuthorize,
         authorizeOwnNotes,
         attributedAuthorizeOwnNotes,
      };
   }

   static async getModelMaterializer(
      runtime: Runtime,
      importBaseURL: URL,
      modelURL: URL,
      modelPath: string,
   ): Promise<{
      modelMaterializer: ModelMaterializer | undefined;
      runnableNotebookCells: RunnableNotebookCell[] | undefined;
   }> {
      if (modelPath.endsWith(MODEL_FILE_SUFFIX)) {
         const modelMaterializer = await Model.getStandardModelMaterializer(
            runtime,
            importBaseURL,
            modelURL,
            modelPath,
         );
         return {
            modelMaterializer,
            runnableNotebookCells: undefined,
         };
      } else if (modelPath.endsWith(NOTEBOOK_FILE_SUFFIX)) {
         const { modelMaterializer: mm, runnableNotebookCells: rnc } =
            await Model.getNotebookModelMaterializer(
               runtime,
               importBaseURL,
               modelURL,
               modelPath,
            );
         return {
            modelMaterializer: mm,
            runnableNotebookCells: rnc,
         };
      } else {
         throw new Error(
            `${modelPath} is not a valid model name.  Model files must end in .malloy or .malloynb.`,
         );
      }
   }

   private static async getStandardModelMaterializer(
      runtime: Runtime,
      importBaseURL: URL,
      modelURL: URL,
      modelPath: string,
   ): Promise<ModelMaterializer> {
      const mm = runtime.loadModel(modelURL, { importBaseURL });
      if (!mm) {
         throw new Error(`Invalid model ${modelPath}.`);
      }
      return mm;
   }

   private static async getNotebookModelMaterializer(
      runtime: Runtime,
      importBaseURL: URL,
      modelURL: URL,
      modelPath: string,
   ): Promise<{
      modelMaterializer: ModelMaterializer | undefined;
      runnableNotebookCells: RunnableNotebookCell[];
   }> {
      let fileContents = undefined;
      let parse = undefined;

      try {
         fileContents = await fs.readFile(modelURL, "utf8");
      } catch {
         throw new ModelNotFoundError("Model not found: " + modelPath);
      }

      try {
         parse = MalloySQLParser.parse(fileContents, modelPath);
      } catch {
         throw new Error("Could not parse model: " + modelPath);
      }

      let mm: ModelMaterializer | undefined = undefined;
      const oldImports: string[] = [];
      const oldSources: Record<string, Malloy.SourceInfo> = {};
      // First generate the sequence of ModelMaterializers.
      // This has to happen sync, since mm.getModel() is async and
      // may execute out-of-order.
      const mms = parse.statements.map((stmt) => {
         if (stmt.type === MalloySQLStatementType.MALLOY) {
            if (!mm) {
               mm = runtime.loadModel(stmt.text, { importBaseURL });
            } else {
               mm = mm.extendModel(stmt.text, { importBaseURL });
            }
         }
         return mm;
      });
      const runnableNotebookCells: RunnableNotebookCell[] = (
         await Promise.all(
            parse.statements.map(async (stmt, index) => {
               if (stmt.type === MalloySQLStatementType.MALLOY) {
                  // Get the Materializer for the current cell/statement.
                  const localMM = mms[index];
                  if (!localMM) {
                     // This can't happen because the to be in this branch there stmt must be
                     // MalloySQLStatementType.MALLOY and we must have a model materializer.
                     throw new Error("Model materializer is undefined");
                  }
                  // Pull available sources from the current model.
                  // Add any of then that are new into newSources and then add them to oldSources.
                  const currentModelDef = (await localMM.getModel())._modelDef;
                  let newSources: Malloy.SourceInfo[] = [];
                  const newImports = currentModelDef.imports?.slice(
                     oldImports.length,
                  );
                  if (newImports) {
                     await Promise.all(
                        newImports.map(async (importLocation) => {
                           const modelString = await runtime.urlReader.readURL(
                              new URL(importLocation.importURL),
                           );
                           const importModel = (
                              await runtime
                                 .loadModel(modelString as string, {
                                    importBaseURL,
                                 })
                                 .getModel()
                           )._modelDef;
                           const importModelInfo =
                              modelDefToModelInfo(importModel);
                           newSources = importModelInfo.entries
                              .filter((entry) => entry.kind === "source")
                              .filter(
                                 (source) => !(source.name in oldSources),
                              ) as Malloy.SourceInfo[];
                           oldImports.push(importLocation.importURL.toString());
                        }),
                     );
                  }
                  const currentModelInfo = modelDefToModelInfo(currentModelDef);
                  newSources = newSources.concat(
                     currentModelInfo.entries
                        .filter((entry) => entry.kind === "source")
                        .filter(
                           (source) => !(source.name in oldSources),
                        ) as Malloy.SourceInfo[],
                  );

                  for (const source of newSources) {
                     oldSources[source.name] = source;
                  }

                  const runnable = localMM.loadFinalQuery();

                  // Extract QueryInfo from the runnable
                  let queryInfo: Malloy.QueryInfo | undefined = undefined;
                  try {
                     const preparedQuery = await runnable.getPreparedQuery();
                     const query = preparedQuery._query as NamedQueryDef;
                     const queryName = query.as || query.name;
                     const anonymousQuery =
                        currentModelInfo.anonymous_queries[
                           currentModelInfo.anonymous_queries.length - 1
                        ];

                     if (anonymousQuery) {
                        queryInfo = {
                           name: queryName,
                           schema: anonymousQuery.schema,
                           annotations: anonymousQuery.annotations,
                           definition: anonymousQuery.definition,
                           code: anonymousQuery.code,
                           location: anonymousQuery.location,
                        } as Malloy.QueryInfo;
                     }
                  } catch (_error) {
                     // If we can't extract query info (e.g., no query in cell), that's okay
                     // This can happen for cells that only define sources
                  }

                  return {
                     type: "code",
                     text: stmt.text,
                     runnable: runnable,
                     modelMaterializer: localMM,
                     modelDef: currentModelDef,
                     newSources,
                     queryInfo,
                  } as RunnableNotebookCell;
               } else if (stmt.type === MalloySQLStatementType.MARKDOWN) {
                  return {
                     type: "markdown",
                     text: stmt.text,
                  } as RunnableNotebookCell;
               } else {
                  return undefined;
               }
            }),
         )
      ).filter((cell) => cell !== undefined);

      return {
         modelMaterializer: mm,
         runnableNotebookCells: runnableNotebookCells,
      };
   }

   public getModelType(): ModelType {
      return this.modelType;
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

// ──────────────────────────────────────────────────────────────────────
// Helpers for hydrating worker-compiled models on the main thread
// ──────────────────────────────────────────────────────────────────────

/**
 * Minimal subset of `Runtime` we use here. The `_` methods are
 * marked `@internal` in Malloy but are the only API for constructing
 * a materializer / query materializer from an existing `modelDef` /
 * queryDef — the public `loadModel(url)` path always recompiles.
 */
type HydrationRuntime = Runtime & {
   _loadModelFromModelDef(modelDef: ModelDef): ModelMaterializer;
};
type HydrationMaterializer = ModelMaterializer & {
   _loadQueryFromQueryDef(query: unknown): QueryMaterializer;
};

function makeHydrationRuntime(
   malloyConfig: ModelConnectionInput,
   buildManifest?: BuildManifest["entries"],
): HydrationRuntime {
   const urlReader = new HackyDataStylesAccumulator(URL_READER);
   const config =
      malloyConfig instanceof MalloyConfig
         ? malloyConfig
         : (() => {
              const c = new MalloyConfig({ connections: {} });
              c.wrapConnections(
                 () => new FixedConnectionMap(malloyConfig, "duckdb"),
              );
              return c;
           })();
   // Thread the package's bound build manifest into the *serve* runtime. Malloy
   // substitutes a persisted source for its materialized table at query
   // (getSQL) time, gated on `prepareResultOptions.buildManifest`; without this
   // the hydrated model always recomputes from the base tables even though the
   // manifest was bound at load. `strict: false` keeps serving live for any
   // source whose sourceEntityId is absent from the manifest.
   return new Runtime({
      urlReader,
      config,
      buildManifest: buildManifest
         ? { entries: buildManifest, strict: false }
         : undefined,
   }) as HydrationRuntime;
}

/**
 * Build the live `RunnableNotebookCell[]` from worker-emitted
 * per-cell data. Each MALLOY cell is hydrated via
 * `Runtime._loadModelFromModelDef` (for the cell's scope) and
 * `ModelMaterializer._loadQueryFromQueryDef` (for the cell's
 * runnable) — no recompile.
 */
function hydrateNotebookCells(
   runtime: HydrationRuntime,
   notebookCells: SerializedNotebookCell[] | undefined,
): RunnableNotebookCell[] {
   if (!notebookCells) return [];
   return notebookCells.map((sc): RunnableNotebookCell => {
      if (sc.type === "markdown") {
         return { type: "markdown", text: sc.text };
      }
      const cellModelDef = sc.cellModelDef as ModelDef | undefined;
      let modelMaterializer: ModelMaterializer | undefined;
      let runnable: QueryMaterializer | undefined;
      if (cellModelDef) {
         modelMaterializer = runtime._loadModelFromModelDef(cellModelDef);
         if (sc.cellQueryDef !== undefined) {
            try {
               runnable = (
                  modelMaterializer as HydrationMaterializer
               )._loadQueryFromQueryDef(sc.cellQueryDef);
            } catch (error) {
               // Hydration shouldn't fail for a queryDef the worker
               // already prepared, but if Malloy's internal shape
               // drifts we'd rather drop the runnable than crash the
               // whole notebook. The cell remains markdown-runnable.
               logger.warn("Failed to hydrate notebook cell queryDef", {
                  error,
               });
            }
         }
      }
      return {
         type: "code",
         text: sc.text,
         runnable,
         modelMaterializer,
         modelDef: cellModelDef,
         newSources: sc.newSources as Malloy.SourceInfo[] | undefined,
         queryInfo: sc.queryInfo as Malloy.QueryInfo | undefined,
      };
   });
}

/**
 * For an all-markdown notebook (no MALLOY statements → no
 * `modelDef`), we still want to preserve the cell list so
 * `getNotebook()` can serve raw text. This skips materializer
 * hydration (there's nothing to hydrate) and returns markdown-only
 * cells.
 */
function hydrateMarkdownOnlyCells(
   notebookCells: SerializedNotebookCell[] | undefined,
): RunnableNotebookCell[] | undefined {
   if (!notebookCells) return undefined;
   return notebookCells.map((sc): RunnableNotebookCell => {
      if (sc.type === "markdown") return { type: "markdown", text: sc.text };
      // A code cell without a hydratable scope — surface text only.
      return { type: "code", text: sc.text };
   });
}
