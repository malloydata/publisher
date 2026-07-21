import {
   Annotations,
   API,
   Connection,
   FixedConnectionMap,
   GivenValue,
   isJoined,
   isSourceDef,
   MalloyConfig,
   MalloyError,
   ModelDef,
   modelDefToModelInfo,
   ModelMaterializer,
   NamedQueryDef,
   QueryData,
   QueryMaterializer,
   Runtime,
   type FieldDef,
   type SourceDef,
} from "@malloydata/malloy";
import * as Malloy from "@malloydata/malloy-interfaces";
import {
   MalloySQLParser,
   MalloySQLStatementType,
} from "@malloydata/malloy-sql";
import { DataStyles } from "@malloydata/render";
import { publisherMeter } from "../telemetry";
import * as fs from "fs/promises";
import { createRequire } from "module";
import * as path from "path";
import { components } from "../api";
import {
   getDefaultQueryRowLimit,
   getMaxQueryRows,
   getMaxResponseBytes,
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
import { logger } from "../logger";
import { deserializeError } from "../package_load/package_load_pool";
import type {
   SerializedModel,
   SerializedNotebookCell,
} from "../package_load/protocol";
import { BuildManifest } from "../storage/DatabaseInterface";
import { URL_READER } from "../utils";
import { modelAnnotations } from "./annotations";
import {
   collectAuthorizeExprs,
   evaluateAuthorize,
   referencedGivenNames,
   validateAuthorizeProbes,
} from "./authorize";
import {
   buildFilterClause,
   FilterValidationError,
   injectFilterRefinement,
   type FilterDefinition,
   type FilterParams,
} from "./filter";
import { malloyGivenToApi, type MalloyGiven } from "./given";
import {
   assertWithinModelResponseLimits,
   resolveModelQueryRowLimit,
} from "./model_limits";
import { buildSourceAliasMap, extractRunTargetSourceName } from "./query_text";
import {
   extractQueriesFromModelDef,
   extractSourcesFromModelDef,
} from "./source_extraction";

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
type ModelConnectionInput = MalloyConfig | Map<string, Connection>;

interface RunnableNotebookCell {
   type: "code" | "markdown";
   text: string;
   runnable?: QueryMaterializer;
   /** Retained so we can rebuild the query with filter refinements at execution time. */
   modelMaterializer?: ModelMaterializer;
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
 * A non-fatal render-tag finding from {@link Model.validateRenderTags}: an
 * error-severity issue that affects only how a field renders, never whether the
 * model compiles or a query runs. `target` is the query or view it sits on
 * (e.g. `by_carrier` or `flights -> by_carrier`).
 */
export interface RenderTagWarning {
   target: string;
   message: string;
   severity: "error" | "warn";
}

export class Model {
   private packageName: string;
   private modelPath: string;
   private dataStyles: DataStyles;
   private modelType: ModelType;
   private modelMaterializer: ModelMaterializer | undefined;
   private modelDef: ModelDef | undefined;
   private modelInfo: Malloy.ModelInfo | undefined;
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
   /** Model-wide `##(authorize)` expressions; apply to every query in the
    *  model, including ad-hoc inline sources not declared in the model. */
   private fileLevelAuthorize: string[] = [];
   /** Given names (`$NAME`) referenced by any authorize gate reachable
    *  anywhere in this model -- the file-level gate, every top-level
    *  source's own gate, and every gate reached transitively from a
    *  top-level source via join_* / query-source derivation (the same walk
    *  {@link assertAuthorizedForAllSources} runs at request time). Computed
    *  once at construction; see {@link filterGivensToModelSurface}, the only
    *  consumer. */
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
   } = { mode: "all", exploresDeclared: false, isQueryEntryPoint: true };
   /** Per-query freshness resolver, pushed down by the owning Package (see
    *  Package.wireFreshnessResolvers). Returns the freshness-filtered build
    *  manifest for the serve path — threaded into Malloy's per-query
    *  `buildManifest` override so a persist source only routes to its
    *  materialized table while within its declared freshness window. Undefined
    *  (or returning undefined) means no override: the runtime-baked manifest
    *  applies, which serves live when unbound. */
   private freshnessResolver?: () => BuildManifest["entries"] | undefined;
   private meter = publisherMeter();
   private queryExecutionHistogram = this.meter.createHistogram(
      "malloy_model_query_duration",
      {
         description: "How long it takes to execute a Malloy model query",
         unit: "ms",
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
      // Model-wide ##(authorize) gates, derived from the file-level annotations
      // on the modelDef (which survives the worker boundary). These apply to
      // any query that resolves to a model source (or to no nameable source),
      // model-wide. (Raw-SQL access to the warehouse is closed separately by
      // restricted mode, which rejects inline `duckdb.sql(...)` on the caller
      // query path before any gate runs — see getQueryResults.) A
      // successfully-loaded model has already had these validated; guard the
      // parse defensively so the constructor never throws.
      try {
         this.fileLevelAuthorize = this.modelDef
            ? collectAuthorizeExprs(
                 (modelAnnotations(this.modelDef).notes ?? []).map(
                    (note) => note.text,
                 ),
              )
            : [];
      } catch {
         this.fileLevelAuthorize = [];
      }
      // Guarded the same way as fileLevelAuthorize above: a malformed gate
      // reachable only through a join/derivation must not throw out of the
      // constructor (gateExprsForOwnAnnotations already fails closed per
      // struct, so this can only throw on something unrelated).
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
    * Given name → declared Malloy type, from this model's own given surface
    * ({@link givens}). Passed to {@link evaluateAuthorize}'s self-contained
    * probe fallback so it prefers the gate author's DECLARED type over
    * inferring one from the caller's JS value (e.g. `$LEVEL > 3` compares
    * numerically even if the caller sends `"5"`). Only reaches a given
    * declared within one import hop of this model — a gate on a source
    * reached through a deeper transitive import isn't on this surface, so
    * that case still falls back to inferring from the value.
    */
   private givenDeclaredTypes(): Map<string, string> {
      return new Map(
         (this.givens ?? [])
            .filter((g) => g.name != null && g.type != null)
            .map((g) => [g.name, g.type] as [string, string]),
      );
   }

   /**
    * Effective authorize expressions gating a source: file-level
    * `##(authorize)` followed by the source's own `#(authorize)`, evaluated as
    * one OR disjunction at request time. Empty array means unrestricted. Reads
    * the per-source list surfaced on `sources` (which rides the worker
    * serialization boundary), so it works for both freshly-created and
    * deserialized models.
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
    * to supply a value only so a joined source's `#(authorize)` gate can see
    * it — that gate is evaluated separately, against the FULL unfiltered
    * givens (see `assertAuthorizedForAllSources`, which runs before this
    * filter is applied). Malloy's own given-resolution doesn't flatten a
    * `given:` declared more than one import hop away into the entry model's
    * namespace (see `docs/authorize.md`), so passing such an authorize-only
    * name straight through to the real query throws ("givens: unknown
    * given"). Dropping it here is safe: the query itself can't reference a
    * given it doesn't declare, so there is nothing for the dropped value to
    * have affected.
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
    * model. Walks the file-level gate plus, for every top-level source in
    * `modelDef.contents`, every gate reached from it via
    * {@link collectAllReachableGates} — the exact same unified traversal
    * {@link assertAuthorizedForAllSources} runs per-query, just rooted at
    * every top-level source instead of one run target. Runs once at
    * construction, not per request.
    */
   private computeAuthorizeReferencedGivenNames(): Set<string> {
      const names = new Set<string>();
      const addExprs = (exprs: string[]) => {
         for (const expr of exprs) {
            for (const name of referencedGivenNames(expr)) names.add(name);
         }
      };
      addExprs(this.fileLevelAuthorize);
      const modelDef = this.modelDef;
      if (!modelDef) return names;
      for (const entry of Object.values(modelDef.contents)) {
         if (!isSourceDef(entry)) continue;
         for (const { exprs } of this.collectAllReachableGates(
            entry,
            modelDef,
            new Set(),
         )) {
            addExprs(exprs);
         }
      }
      return names;
   }

   /**
    * Whether the model declares any `#(authorize)` / `##(authorize)` gate at all
    * (file-level or on any source). Lets callers cheaply skip authorize work for
    * ungated models without compiling a probe.
    */
   public hasAuthorize(): boolean {
      return (
         this.fileLevelAuthorize.length > 0 ||
         (this.sources?.some((s) => (s.authorize?.length ?? 0) > 0) ?? false)
      );
   }

   /**
    * Effective authorize expressions for whatever a query runs against:
    *  - a declared model source → its own list (file-level ++ source-level);
    *  - anything else (an ad-hoc inline `duckdb.sql(...)` source, or a source
    *    we couldn't name) → the model-wide file-level `##(authorize)` gates.
    * The second case is what stops a file-level gate from being bypassed by
    * querying the warehouse through raw inline SQL.
    */
   private effectiveAuthorizeFor(sourceName: string | undefined): string[] {
      if (sourceName && this.sources?.some((s) => s.name === sourceName)) {
         return this.getAuthorize(sourceName);
      }
      return this.fileLevelAuthorize;
   }

   /**
    * Runtime authorize gate. Throws `AccessDeniedError` (403) unless at least
    * one in-scope authorize expression evaluates true for the supplied givens.
    * No in-scope expressions = unrestricted.
    *
    * Fail closed: any failure to evaluate the probe — a missing given value, a
    * transient probe error, a missing/non-true result — denies. (Expression
    * well-formedness was already validated at model load; see authorize.ts.)
    * The 403 message names only the source, never the expression, so gate logic
    * is not leaked to the caller.
    */
   public async assertAuthorized(
      sourceName: string | undefined,
      givens: Record<string, GivenValue>,
   ): Promise<void> {
      await this.assertAuthorizedExprs(
         sourceName ?? "(query)",
         this.effectiveAuthorizeFor(sourceName),
         givens,
      );
   }

   /**
    * Core runtime gate, factored out of {@link assertAuthorized} so the
    * joined-source walk ({@link assertAuthorizedForAllSources}) can evaluate a
    * gate it read directly off a join field's own struct annotations, without
    * going through a `this.sources` name lookup (which would miss deep
    * transitively-imported and inline-extend joins — see the design doc).
    *
    * `selfContainedFirst`, passed through to {@link evaluateAuthorize},
    * isolates a joined/derived source's gate from the entry model's own
    * ambient given namespace — see {@link collectAllReachableGates}'s
    * `selfContained` tag. Left `false` (ambient-first) for the run target's
    * OWN source gate ({@link assertAuthorized}'s call), which is correct to
    * evaluate against the entry model's ambient namespace.
    */
   private async assertAuthorizedExprs(
      label: string,
      exprs: string[],
      givens: Record<string, GivenValue>,
      selfContainedFirst = false,
   ): Promise<void> {
      if (exprs.length === 0) return; // unrestricted
      const deny = () => {
         throw new AccessDeniedError(`Access denied for source "${label}".`);
      };
      if (!this.modelMaterializer) deny();
      let passed = false;
      try {
         passed = await evaluateAuthorize(
            this.modelMaterializer!,
            exprs,
            givens,
            this.givenDeclaredTypes(),
            { selfContainedFirst },
         );
      } catch (err) {
         // Fail closed — e.g. a referenced given had no supplied value.
         logger.debug("Authorize probe failed; denying", {
            sourceName: label,
            modelPath: this.modelPath,
            error: err instanceof Error ? err.message : String(err),
         });
         deny();
      }
      if (!passed) deny();
   }

   /**
    * Gate a compiled query against every gate reachable in its struct — the
    * run target's own gate PLUS every joined source's gate, recursed
    * transitively (A→B→C). Closes the join-bypass: `#(authorize)` on a source
    * reached only via `join_*` was previously never enforced (see
    * `docs/authorize.md` "Known limitations" and the design doc's W1).
    *
    * Each joined source's gate is read directly from the join field's OWN
    * struct annotations (the struct spread in Malloy's `join.js` preserves
    * them), not from a `this.sources` name lookup — a name lookup would
    * silently miss a deep transitively-imported source (absent from
    * `modelDef.contents`) or an inline-extend join (`referenceID` cleared),
    * leaving both bypassable. File-level `##(authorize)` still prepends to
    * every joined source, so the permissive-admin idiom keeps working
    * transitively. Semantics are AND across sources: any single reachable
    * gate failing denies the whole query, while each source's own list of
    * expressions stays an OR disjunction. Identical expr-lists are evaluated
    * once.
    *
    * Runs UNCONDITIONALLY — NOT guarded by {@link hasAuthorize}. `hasAuthorize`
    * only inspects `this.sources` (top-level `modelDef.contents` sources), so a
    * gated source reached only through a cross-file/deep-transitive join is
    * invisible to it and the walk below would never run. The own-source probe
    * and the joined-gate walk are both cheap no-ops for a genuinely ungated
    * model (empty expr lists / no joined sources to find), so there is nothing
    * to save by skipping them.
    */
   public async assertAuthorizedForAllSources(
      runnable: { getPreparedQuery(): Promise<unknown> },
      givens: Record<string, GivenValue>,
   ): Promise<void> {
      const ownSourceName =
         await this.resolveAuthorizeSourceFromRunnable(runnable);
      await this.assertAuthorized(ownSourceName, givens);

      const { struct, modelDef, compositeResolvedSourceDef, extendSources } =
         await this.resolveRunTargetStruct(runnable);
      const seen = new Set<SourceDef>();
      // `struct` IS the run target itself — its own gate stays ambient-first
      // (`treatAsOwnGate: true`), matching `assertAuthorized` above. Do NOT
      // dedup this against that call: AND-across-sources evaluates every
      // reachable source's gate independently (see the comment below).
      const joinedGates = this.collectAllReachableGates(
         struct,
         modelDef,
         seen,
         true,
      );
      if (modelDef) {
         // Sources joined LOCALLY inside the query's own `-> { join_one: ...
         // }` refinement live on the pipeline segment's `extendSource`, not on
         // the run target's `struct.fields` — the walk above never sees them
         // from `struct` alone. Walk each the same way as everything else.
         for (const field of extendSources) {
            if (!isJoined(field) || !isSourceDef(field)) continue;
            joinedGates.push(
               ...this.collectAllReachableGates(
                  field as SourceDef,
                  modelDef,
                  seen,
               ),
            );
         }
      }
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
         // reached via a JOIN is a different story — Malloy doesn't surface
         // which member it resolved to there, which is why the `joinedSource
         // .type === "composite"` branch above walks every member
         // conservatively instead.)
         joinedGates.push(
            ...this.collectAllReachableGates(
               compositeResolvedSourceDef,
               modelDef,
               seen,
               true,
            ),
         );
      }
      // Evaluate every reachable source's gate independently — do NOT dedup by
      // expression text. AND-across-sources requires a probe per source: two
      // distinct sources with identical gate text must each be evaluated, or a
      // non-deterministic gate (e.g. one referencing random()) would be
      // under-enforced (evaluated once, reused) — a fail-open relative to
      // per-source enforcement. Probes are ~microsecond one-row DuckDB queries
      // and reachable gated sources are few, so there is nothing worth deduping.
      // (Cycles/repeat structs are already pruned in collectAllReachableGates
      // by struct identity, so joinedGates holds no literal duplicates.)
      for (const { label, exprs, selfContained } of joinedGates) {
         await this.assertAuthorizedExprs(label, exprs, givens, selfContained);
      }
   }

   /**
    * Resolve the run-target `SourceDef` and its `ModelDef`, for walking joined
    * sources. `prepared._modelDef` is the modelDef the query actually compiled
    * against (falls back to `this.modelDef`); a string `structRef` resolves
    * through `modelDef.contents`. Also surfaces `compositeResolvedSourceDef` —
    * when the run target is itself a composite source (`compose(a, b)`), this
    * is the ONE concrete member branch Malloy resolved the query against (see
    * {@link assertAuthorizedForAllSources}). Also surfaces `extendSources` —
    * `FieldDef[]` pulled from every pipeline segment's `extendSource`, i.e.
    * every source joined LOCALLY inside the query's own `-> { join_one: ...
    * }` refinement. Such a join lives on the query pipeline's segment, not on
    * the run target's own `struct.fields`, so {@link collectAllReachableGates}
    * walking `struct.fields` alone would never see it. Returns `undefined`s /
    * an empty array if any of these can't be resolved — callers treat that as
    * "no joins to check" rather than denying, since
    * {@link assertAuthorizedForAllSources}'s own-source gate above is still the
    * authoritative deny for an unresolvable target.
    */
   private async resolveRunTargetStruct(runnable: {
      getPreparedQuery(): Promise<unknown>;
   }): Promise<{
      struct: SourceDef | undefined;
      modelDef: ModelDef | undefined;
      compositeResolvedSourceDef: SourceDef | undefined;
      extendSources: FieldDef[];
   }> {
      try {
         const prepared = (await runnable.getPreparedQuery()) as {
            _query?: {
               structRef?: unknown;
               compositeResolvedSourceDef?: SourceDef;
               pipeline?: { extendSource?: FieldDef[] }[];
            };
            _modelDef?: ModelDef;
         };
         const modelDef = prepared._modelDef ?? this.modelDef;
         if (!modelDef)
            return {
               struct: undefined,
               modelDef: undefined,
               compositeResolvedSourceDef: undefined,
               extendSources: [],
            };
         const structRef = prepared._query?.structRef;
         const struct =
            typeof structRef === "string"
               ? modelDef.contents[structRef]
               : structRef;
         const extendSources = (prepared._query?.pipeline ?? []).flatMap(
            (segment) => segment.extendSource ?? [],
         );
         return {
            struct:
               struct && typeof struct === "object"
                  ? (struct as SourceDef)
                  : undefined,
            modelDef,
            compositeResolvedSourceDef:
               prepared._query?.compositeResolvedSourceDef,
            extendSources,
         };
      } catch {
         // Not fail-open: if getPreparedQuery() throws here, execution's own
         // getPreparedResult()/run() (same compilation) throws too, so no data
         // is returned. Safety depends on execution sharing this compilation.
         return {
            struct: undefined,
            modelDef: undefined,
            compositeResolvedSourceDef: undefined,
            extendSources: [],
         };
      }
   }

   /**
    * Effective authorize exprs read directly off a struct's OWN block
    * annotations (file-level `##(authorize)` ++ its own `#(authorize)`). Used
    * by the joined-source and composite-member walks below, which read a
    * struct's gate straight off its own annotations rather than through a
    * `this.sources` name lookup (a name lookup only covers top-level
    * `modelDef.contents` sources and would miss these).
    *
    * Fails CLOSED: `extractSourcesFromModelDef` only validates authorize
    * annotations for top-level `modelDef.contents` sources at model load, so
    * a malformed gate on a source reachable ONLY through a join or a
    * composite member is never probed there — a parse failure here can't be
    * assumed unreachable/already-validated. Force denial (a single
    * unsatisfiable `"false"` expr) rather than treating the parse failure as
    * "no gate" (fail-open).
    */
   private gateExprsForOwnAnnotations(struct: SourceDef): string[] {
      const ownNotes = (struct.annotations?.blockNotes ?? []).map(
         (note) => note.text,
      );
      try {
         return [
            ...this.fileLevelAuthorize,
            ...collectAuthorizeExprs(ownNotes),
         ];
      } catch {
         return ["false"];
      }
   }

   /**
    * Canonical entry point for "every gate reachable from `struct`" — the
    * single walk every call site in this file uses, replacing what used to
    * be a hand-composed sequence of narrower collectors per call site (the
    * inconsistency between those sequences is what let two derivations
    * launder a locked base's gate away — see the composite-branch and
    * query-local-join call sites in {@link assertAuthorizedForAllSources}).
    * Collects, for `struct` and recursively for everything below:
    *  - its own annotations ({@link gateExprsForOwnAnnotations});
    *  - every joined source (`struct.fields`), including every member of a
    *    joined composite (`compose(a, b)` reached via `join_*`) — Malloy
    *    doesn't surface which branch a JOINED composite resolved to (unlike
    *    a run target's own composite resolution, which the caller handles
    *    by passing the resolved branch in as `struct` directly), so a joined
    *    composite is walked conservatively: every member gated, any one
    *    failing denies the whole query;
    *  - if `struct` is itself query-derived (`source: x is y -> {...}`), the
    *    base it derives from (`query.structRef`, resolved the same way
    *    {@link resolveRunTargetStruct} resolves a run target's structRef) —
    *    a `QuerySourceDef`'s own `.fields`/`.annotations` reflect the
    *    DERIVED shape, not `y`'s gate, so without this the derivation
    *    launders the base's gate away;
    *  - if `struct` is itself query-derived, its own inner-pipeline
    *    `join_one`s (`-> { extend: { join_one: locked ... } ... }`), which
    *    live on the derivation's pipeline segment, not on `struct.fields`
    *    and not reachable via `query.structRef` — mirrors the run target's
    *    own `extendSources` handling in {@link assertAuthorizedForAllSources},
    *    applied to a query-source's `query.pipeline` instead of the run
    *    query's;
    *  - if `struct` is itself query-derived AND its base is a composite
    *    (`source: x is compose(a, b) -> {...}`), the ONE resolved member
    *    branch Malloy picked for THIS derivation (`query.compositeResolvedSourceDef`)
    *    — mirrors the run target's own composite-resolution handling in
    *    {@link assertAuthorizedForAllSources}, applied to a query-source's
    *    own resolution instead of the run query's.
    * Every case above recurses through this SAME function, so a derivation,
    * a join, and a composite member compose uniformly no matter how deep
    * (a query-source over a joined query-source, a chained derivation, a
    * composite member that is itself query-derived, etc).
    *
    * `seen` (struct-identity keyed) is shared across the whole walk by the
    * caller, guarding cycles and repeat structs — a struct already visited
    * anywhere in the walk is not walked again.
    *
    * `QuerySourceDef` isn't re-exported from the package root (same
    * situation as `given.ts`'s `MalloyGiven` duck type), so query-source
    * detection checks `.type` and reaches `.query.structRef`/`.query.pipeline`
    * through a local shape rather than importing the real type.
    *
    * Each returned entry carries `selfContained`, telling the caller which
    * order {@link evaluateAuthorize} should try for that gate (see
    * `assertAuthorizedExprs`'s `selfContainedFirst`). `treatAsOwnGate` — true
    * only for the run target's own struct and its own resolved composite
    * branch (the two call sites in {@link assertAuthorizedForAllSources} that
    * represent the run target ITSELF, not something reached by walking a
    * join/derivation) — marks `struct`'s OWN top-level annotations entry
    * `selfContained: false` (ambient-first, matching {@link assertAuthorized}'s
    * treatment of the same gate). Every other entry — everything reached by
    * recursing into a joined field, a composite member, a query-source's
    * base, or an inner pipeline join, no matter how deep — is tagged
    * `selfContained: true`: those are gates that live in a DIFFERENT source
    * (possibly a different file/given-namespace) than the run target, so
    * evaluating them ambiently against the entry model's own namespace risks
    * a name collision silently granting access off the wrong given (see
    * `evaluateAuthorize`'s `selfContainedFirst` doc).
    */
   private collectAllReachableGates(
      struct: SourceDef | undefined,
      modelDef: ModelDef | undefined,
      seen: Set<SourceDef> = new Set(),
      treatAsOwnGate = false,
   ): { label: string; exprs: string[]; selfContained: boolean }[] {
      if (!struct || !modelDef || seen.has(struct)) return [];
      seen.add(struct);

      const results: {
         label: string;
         exprs: string[];
         selfContained: boolean;
      }[] = [];
      const label = (struct as { as?: string }).as ?? struct.name;
      const ownExprs = this.gateExprsForOwnAnnotations(struct);
      if (ownExprs.length > 0) {
         results.push({
            label,
            exprs: ownExprs,
            selfContained: !treatAsOwnGate,
         });
      }

      for (const field of struct.fields as FieldDef[]) {
         if (!isJoined(field)) continue;
         if (!isSourceDef(field)) {
            // `field` IS a join (Malloy's own `isJoined` — `'join' in sd` —
            // says so) but doesn't have a `SourceDef` shape we can recurse
            // into. That combination should be impossible for a real join
            // field; seeing it means something about Malloy's join/source
            // representation has drifted out from under this duck-typed
            // walk. Silently `continue`-ing here (the old behavior) would
            // fail OPEN: the joined source's gate would never be found or
            // evaluated. Deny loudly instead — this is exactly the
            // join-bypass this walk exists to close.
            logger.error(
               "authorize: joined field failed to resolve to a walkable SourceDef; denying rather than silently skipping its gate (possible Malloy struct-shape drift)",
               {
                  modelPath: this.modelPath,
                  parentSource: label,
                  fieldName: (field as { name?: string }).name,
                  fieldType: (field as { type?: string }).type,
               },
            );
            results.push({
               label: `${label} (unresolvable joined source)`,
               exprs: ["false"],
               selfContained: false,
            });
            continue;
         }
         const joinedSource = field as SourceDef;
         results.push(
            ...this.collectAllReachableGates(joinedSource, modelDef, seen),
         );
         // A JOINED source that is itself a composite (`compose(a, b)`
         // reached via `join_*`) is walked conservatively: unlike a run
         // target's own composite resolution (handled by the caller passing
         // the resolved branch in as `struct` directly, never as a joined
         // field), Malloy doesn't surface which member a JOINED composite
         // resolved to, so every member is gated — any member's gate
         // failing denies the whole query.
         if (joinedSource.type === "composite") {
            const members = (
               joinedSource as SourceDef & { sources: SourceDef[] }
            ).sources;
            for (const member of members) {
               results.push(
                  ...this.collectAllReachableGates(member, modelDef, seen),
               );
            }
         }
      }

      const duck = struct as unknown as {
         type: string;
         query?: {
            structRef?: SourceDef | string;
            compositeResolvedSourceDef?: SourceDef;
            pipeline?: { extendSource?: FieldDef[] }[];
         };
      };
      if (duck.type === "query_source") {
         const ref = duck.query?.structRef;
         const base = typeof ref === "string" ? modelDef.contents[ref] : ref;
         if (base && isSourceDef(base)) {
            results.push(
               ...this.collectAllReachableGates(
                  base as SourceDef,
                  modelDef,
                  seen,
               ),
            );
         }
         // A query-source's own base may itself be a composite
         // (`source: qs is compose(a, b) -> {...}`) — Malloy resolves that
         // composite to exactly one concrete member branch for THIS
         // query-source's derivation (surfaced the same way as a run
         // target's own composite resolution, see
         // assertAuthorizedForAllSources), carried on the query-source's
         // OWN `query.compositeResolvedSourceDef`, not on `query.structRef`
         // (the raw composite) or `query.pipeline`. Without walking it, a
         // query-source derived from a locked composite member laundered
         // that member's gate away. Recursing through this same function
         // means a query-source nested at any depth (query-source over
         // composite over query-source, etc) is covered uniformly.
         const resolved = duck.query?.compositeResolvedSourceDef;
         if (resolved) {
            results.push(
               ...this.collectAllReachableGates(resolved, modelDef, seen),
            );
         }
         const innerJoins = (duck.query?.pipeline ?? []).flatMap(
            (segment) => segment.extendSource ?? [],
         );
         for (const field of innerJoins) {
            if (!isJoined(field) || !isSourceDef(field)) continue;
            results.push(
               ...this.collectAllReachableGates(
                  field as SourceDef,
                  modelDef,
                  seen,
               ),
            );
         }
      }

      return results;
   }

   /**
    * Gate ad-hoc compile/query text by the named source it targets. Resolves the
    * source from surface syntax (`extractRunTargetSourceName`) and applies the
    * gate. An
    * unnamed/inline source resolves to `undefined`, so only the model-wide
    * file-level gate applies — the same top-level-only boundary as the query
    * path's early gate. Used by the `/compile` path, which has no runnable to
    * resolve before it decides whether to compile at all.
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
    * `run:` statement isn't the first one), PLUS every source reached
    * transitively via join_* (see assertAuthorizedForAllSources). Used as the
    * `/compile` backstop once a runnable exists, so `/compile` inherits the
    * same join-bypass protection as the query path.
    */
   public async assertAuthorizedForRunnable(
      runnable: { getPreparedQuery(): Promise<unknown> },
      givens: Record<string, GivenValue>,
   ): Promise<void> {
      await this.assertAuthorizedForAllSources(runnable, givens);
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
            queries = Model.getQueries(modelDef);

            // Translation-time validation of #(authorize) annotations (shared
            // with the package-load worker so both compile paths validate
            // identically). Compiling the probe surfaces unknown givens and
            // source-field references at model-load instead of first request.
            await validateAuthorizeProbes(modelMaterializer, sources ?? []);

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

         return new Model(
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

      return new Model(
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
    * True when this is an import-only model: it imports other files but
    * declares and re-exports nothing of its own, so `modelDef.exports` is
    * empty and its discovery surface lists no sources or queries. Legitimate
    * as plumbing, but confusing when the model is *listed* — the page renders
    * blank. Used by the load-time warning (Package.emptyDiscoveryWarnings);
    * the fix is to re-export what should be visible (`export { name }`).
    */
   public hasEmptyDiscoverySurface(): boolean {
      // No curation (no `explores`) ⇒ legacy listings include imported sources.
      if (!this.discoveryCurationEnabled) return false;
      if (this.modelType !== "model" || !this.modelDef) return false;
      const exports = this.modelDef.exports;
      if (!Array.isArray(exports) || exports.length > 0) return false;
      return (this.modelDef.imports?.length ?? 0) > 0;
   }

   /** Set by the owning Package; see {@link assertQueryBoundaryEarly}. */
   public setQueryBoundary(policy: {
      mode: "declared" | "all";
      exploresDeclared: boolean;
      isQueryEntryPoint: boolean;
   }): void {
      this.queryBoundary = policy;
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
      // gated by the boundary, even when reached via the /compile path.
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

      const curatedSources = this.curatedSourceNames();
      const curatedQueries = new Set(
         (this.getQueries() ?? []).map((q) => q.name).filter(Boolean),
      );

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
         if (!sourceName && curatedQueries.has(queryName)) return "cleared";
         if (sourceName && curatedSources.has(sourceName)) return "cleared";
         throw new NotQueryableError(`No queryable query "${queryName}".`);
      }

      // An explicitly-named source: admit iff curated.
      if (sourceName) {
         if (curatedSources.has(sourceName)) return "cleared";
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
            !curatedSources.has(target) &&
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
         if (this.curatedSourceNames().has(compiledSource)) return;
         if (query && this.derivesFromCurated(compiledSource, query)) return;
      }
      throw new NotQueryableError("Query target is not queryable.");
   }

   /**
    * The /compile-path wrapper: resolve the compiled run target from the
    * materializer and apply the boundary backstop. No-ops when the boundary is
    * inert, so the resolution work is skipped for unaffected packages.
    */
   public async assertQueryBoundaryForRunnable(
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

   /** Source names in the export-curated discovery surface (= the directly
    *  queryable set under the "declared" boundary). */
   private curatedSourceNames(): Set<string> {
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
      const curated = this.curatedSourceNames();
      const aliasOf = buildSourceAliasMap(query);
      let current: string | undefined = name;
      const seen = new Set<string>();
      while (current && !seen.has(current)) {
         if (curated.has(current)) return true;
         seen.add(current);
         current = aliasOf.get(current);
      }
      return false;
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
    * tags, so they are skipped without compiling. Any error-severity finding
    * (e.g. a child-only `# big_value { sparkline=... }` placed on a view with no
    * activating big_value) is logged as a warning naming the offending target;
    * it does not fail the package load. Such a tag still renders as
    * "[object Object]" at query time, so the warning is the operator-facing
    * signal. Lower-severity findings are left for the query-time `renderLogs`
    * surface. The findings are returned so the owning Package can surface them
    * as non-fatal `warnings` on its response.
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
         const errors = validateRenderTags(result).filter(
            (log) => log.severity === "error",
         );
         if (errors.length > 0) {
            logger.warn(
               `Invalid renderer configuration on '${target.label}': ${errors
                  .map((e) => e.message)
                  .join("; ")}`,
            );
            for (const e of errors) {
               findings.push({
                  target: target.label,
                  message: e.message,
                  severity: "error",
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

   public getNotebookError(): MalloyError | Error | undefined {
      return this.compilationError;
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
   ): Promise<{
      result: Malloy.Result;
      compactResult: QueryData;
      modelInfo: Malloy.ModelInfo;
      dataStyles: DataStyles;
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
      if (!this.modelMaterializer || !this.modelDef || !this.modelInfo)
         throw new BadRequestError("Model has no queryable entities.");

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
      const earlySource =
         sourceName ||
         (queryName
            ? this.queries?.find((q) => q.name === queryName)?.sourceName
            : undefined) ||
         extractRunTargetSourceName(query);
      if (earlySource) {
         await this.assertAuthorized(earlySource, givens ?? {});
      }

      // Wrap loadQuery calls in try-catch to handle query parsing errors
      try {
         let queryString: string;
         if (!sourceName && !queryName && query) {
            queryString = "\n" + query;
         } else if (queryName && !query) {
            queryString = `\nrun: ${sourceName ? sourceName + "->" : ""}${queryName}`;
         } else {
            const endTime = performance.now();
            const executionTime = endTime - startTime;
            this.queryExecutionHistogram.record(executionTime, {
               "malloy.model.path": this.modelPath,
               "malloy.model.query.name": queryName,
               "malloy.model.query.source": sourceName,
               "malloy.model.query.query": query,
               "malloy.model.query.status": "error",
            });
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

      // Gate the compiled run target's own source PLUS every source reached
      // transitively via join_* (assertAuthorizedForAllSources) — this MUST
      // run unconditionally, not just when compiledSource !== earlySource:
      // the common `run: joiner -> {...}` case has an ungated top-level source
      // (so compiledSource === earlySource and the own-source re-probe really
      // would be redundant), but a joined source's gate has never been checked
      // yet on this request. The walk runs UNCONDITIONALLY (do NOT re-add a
      // hasAuthorize() guard here — it only sees top-level sources, so guarding
      // on it re-opens the deep-import join bypass); it is a cheap no-op for an
      // ungated model. When compiledSource is unknown/unresolved,
      // the own-source half still applies the model-wide file-level gate via
      // effectiveAuthorizeFor. Note: on this path an ad-hoc inline
      // `duckdb.sql(...)` query is rejected by restricted mode (the raw-SQL
      // ban from loadRestrictedQuery above) before it can run, so the
      // raw-warehouse bypass is closed by restricted mode — not by this gate.
      await this.assertAuthorizedForAllSources(runnable, givens ?? {});

      const maxRows = getMaxQueryRows();
      const maxBytes = getMaxResponseBytes();
      // Per-query freshness gate (persistence.md §9.3): resolve the
      // freshness-filtered manifest once and thread it into both the prepare
      // (for the row limit) and the run so a stale persist source falls back per
      // its declared policy — and prep/run agree on the same substitution.
      const buildManifest = this.resolveFreshBuildManifest();

      // Prepare INSIDE the run try/catch: a bad-given / value-type throw at
      // prepare time (getPreparedResult binds the givens) gets the same
      // MalloyError→rethrow / else→400 handling as run, instead of escaping as
      // a 500. `executionTime` is still captured after prepare and before run,
      // preserving the pre-existing timing recorded by the success histogram.
      let rowLimit = 0;
      let executionTime = 0;
      let queryResults;
      // Givens supplied only so a joined source's authorize gate could see
      // them (checked above, against the full unfiltered set) must not reach
      // the real query if this model doesn't itself surface them — see
      // filterGivensToModelSurface.
      const querySurfaceGivens = this.filterGivensToModelSurface(givens);
      try {
         rowLimit = resolveModelQueryRowLimit(
            (
               await runnable.getPreparedResult({
                  givens: querySurfaceGivens,
                  buildManifest,
               })
            ).resultExplore.limit,
            { defaultLimit: getDefaultQueryRowLimit(), maxRows },
         );
         executionTime = performance.now() - startTime;

         queryResults = await runnable.run({
            rowLimit,
            givens: querySurfaceGivens,
            abortSignal,
            buildManifest,
         });
      } catch (error) {
         // Record error metrics
         const errorEndTime = performance.now();
         const errorExecutionTime = errorEndTime - startTime;
         this.queryExecutionHistogram.record(errorExecutionTime, {
            "malloy.model.path": this.modelPath,
            "malloy.model.query.name": queryName,
            "malloy.model.query.source": sourceName,
            "malloy.model.query.query": query,
            "malloy.model.query.status": "error",
         });

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

         // Re-throw Malloy errors as-is (they will be handled by error handler)
         if (error instanceof MalloyError) {
            throw error;
         }

         // For other runtime errors (like divide by zero), throw as BadRequestError
         const errorMessage =
            error instanceof Error ? error.message : String(error);
         logger.error("Query execution error", {
            error,
            errorMessage,
            environmentName: this.packageName,
            modelPath: this.modelPath,
            query,
            queryName,
            sourceName,
         });
         throw new BadRequestError(`Query execution failed: ${errorMessage}`);
      }

      const wrappedResult = API.util.wrapResult(queryResults);
      // Best-effort byte check: we've already buffered `queryResults` and
      // built `wrappedResult` by the time we get here, so this surfaces
      // oversize responses with a clean HTTP 413 instead of letting the
      // controller transmit a half-megabyte payload — it is not OOM
      // prevention. True prevention requires streaming `Result`
      // construction, which is out of scope for this step. The row cap
      // above is the primary OOM defense.
      const serializedBytes =
         maxBytes > 0
            ? Buffer.byteLength(JSON.stringify(wrappedResult), "utf8")
            : 0;
      assertWithinModelResponseLimits(
         queryResults.totalRows,
         serializedBytes,
         { maxRows, maxBytes },
         "model_query",
      );
      this.queryExecutionHistogram.record(executionTime, {
         "malloy.model.path": this.modelPath,
         "malloy.model.query.name": queryName,
         "malloy.model.query.source": sourceName,
         "malloy.model.query.query": query,
         "malloy.model.query.rows_limit": rowLimit,
         "malloy.model.query.rows_total": queryResults.totalRows,
         "malloy.model.query.connection": queryResults.connectionName,
         "malloy.model.query.status": "success",
      });
      return {
         result: wrappedResult,
         compactResult: queryResults.data.value,
         modelInfo: this.modelInfo,
         dataStyles: this.dataStyles,
      };
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

      const allAnnotations = this.modelDef
         ? new Annotations(modelAnnotations(this.modelDef)).texts()
         : [];

      return {
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
         notebookCells,
      } as ApiRawNotebook;
   }

   public async executeNotebookCell(
      cellIndex: number,
      filterParams?: FilterParams,
      bypassFilters?: boolean,
      givens?: Record<string, GivenValue>,
      // See `getQueryResults`: forwarded into `runnable.run` so the
      // publisher's wall-clock timeout actually cancels the query.
      abortSignal?: AbortSignal,
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

      // Authorize gate — only cells that actually run a query touch data, so
      // gate exactly those (a source-def / import cell has no runnable and
      // accesses nothing). Gates the COMPILED cell query's own source (the
      // model-wide file-level gate for an unknown/inline source) PLUS every
      // source reached transitively via join_* — see
      // assertAuthorizedForAllSources. Before the execution try below so
      // AccessDeniedError stays a 403; independent of bypassFilters.
      if (cell.runnable) {
         await this.assertAuthorizedForAllSources(cell.runnable, givens ?? {});
      }

      // For code cells, execute the runnable if available
      let queryName: string | undefined = undefined;
      let queryResult: string | undefined = undefined;

      if (cell.runnable) {
         try {
            let runnableToExecute = cell.runnable;

            // If filters need to be applied, rebuild the query with a refinement
            if (!bypassFilters && cell.modelMaterializer) {
               const effectiveSource = extractRunTargetSourceName(cell.text);
               if (effectiveSource) {
                  const filters = this.getFilters(effectiveSource);
                  if (filters.length > 0) {
                     const filterClause = buildFilterClause(
                        filters,
                        filterParams ?? {},
                     );
                     if (filterClause) {
                        const refinedQuery = injectFilterRefinement(
                           cell.text,
                           filterClause,
                        );
                        runnableToExecute =
                           cell.modelMaterializer.loadQuery(refinedQuery);
                     }
                  }
               }
            }

            const cellMaxRows = getMaxQueryRows();
            const cellMaxBytes = getMaxResponseBytes();
            // Per-query freshness gate (see getQueryResults): the same
            // freshness-filtered manifest gates notebook-cell queries.
            const buildManifest = this.resolveFreshBuildManifest();
            // See getQueryResults / filterGivensToModelSurface: the gate
            // above already saw the full unfiltered givens.
            const cellSurfaceGivens = this.filterGivensToModelSurface(givens);
            const rowLimit = resolveModelQueryRowLimit(
               (
                  await runnableToExecute.getPreparedResult({
                     givens: cellSurfaceGivens,
                     buildManifest,
                  })
               ).resultExplore.limit,
               {
                  defaultLimit: getDefaultQueryRowLimit(),
                  maxRows: cellMaxRows,
               },
            );
            const result = await runnableToExecute.run({
               rowLimit,
               givens: cellSurfaceGivens,
               abortSignal,
               buildManifest,
            });
            const query = (await runnableToExecute.getPreparedQuery())._query;
            queryName = (query as NamedQueryDef).as || query.name;
            queryResult =
               result?._queryResult &&
               this.modelInfo &&
               JSON.stringify(API.util.wrapResult(result));
            // Same caveat as `getQueryResults`: by the time we measure
            // bytes the response has already been buffered and stringified,
            // so this is loud-failure detection (clean 413 instead of
            // partial transmission), not OOM prevention. The row cap above
            // is the primary defense.
            if (result?._queryResult && queryResult) {
               assertWithinModelResponseLimits(
                  result.totalRows,
                  Buffer.byteLength(queryResult, "utf8"),
                  { maxRows: cellMaxRows, maxBytes: cellMaxBytes },
                  "notebook_cell",
               );
            }
         } catch (error) {
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

   static async getModelRuntime(
      packagePath: string,
      modelPath: string,
      malloyConfig: ModelConnectionInput,
      options?: { buildManifest?: BuildManifest["entries"] },
   ): Promise<{
      runtime: Runtime;
      modelURL: URL;
      importBaseURL: URL;
      dataStyles: DataStyles;
      modelType: ModelType;
   }> {
      const fullModelPath = path.join(packagePath, modelPath);
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
      const urlReader = new HackyDataStylesAccumulator(URL_READER);

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

   private static getQueries(modelDef: ModelDef): ApiQuery[] {
      // Shared with the package-load worker — see service/source_extraction.ts.
      return extractQueriesFromModelDef(modelDef) as ApiQuery[];
   }

   private static getSources(
      modelDef: ModelDef,
      givens?: ApiGiven[],
   ): {
      sources: ApiSource[];
      filterMap: Map<string, FilterDefinition[]>;
   } {
      // Shared with the package-load worker — see service/source_extraction.ts.
      // The service path logs filter parse failures; the worker stays silent.
      const { sources, filterMap } = extractSourcesFromModelDef(
         modelDef,
         givens,
         (sourceName, err) =>
            logger.warn(
               `Failed to parse filter annotations on source "${sourceName}"`,
               { error: err },
            ),
      );
      return { sources: sources as unknown as ApiSource[], filterMap };
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
