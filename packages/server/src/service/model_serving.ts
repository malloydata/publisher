// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The query pipeline: one query against one model, served or live, with its
 * report. `Model.runQuery` hands its request here; the model supplies what it owns through {@link ModelServingHost}.
 *
 * A query runs in six stages, each a method below:
 *
 * 1. **pin**: the model's snapshot pinned once (C1b), so every leg, the run
 *    and the path report read the tables bound at one instant;
 * 2. **prepare**: the query boundary and the advisory pre-compile checks, the
 *    caller-text guards, filter injection, and the caller's own (live) compile;
 * 3. **route**: the legs for the pin (`makeLegs`), tried in order by the
 *    router, into one {@link Answer};
 * 4. **govern**: caller text that does not compile is settled, then
 *    `govern()`, the authority, on whatever answered;
 * 5. **run**: the governed runnable, with the manifest or virtual map the
 *    answer names, retried live when a stored read fails and every binding
 *    it read allows it;
 * 6. **report**: the caps, the metrics, the access record and the path report.
 */

import { DUCKLAKE_CATALOG_UNAVAILABLE } from "./ducklake_catalog_unavailable";
import {
   EMPTY_BUILD_MANIFEST,
   type GivenValue,
   MalloyError,
   type ModelDef,
   type ModelMaterializer,
   type QueryData,
   type QueryMaterializer,
} from "@malloydata/malloy";
import type * as Malloy from "@malloydata/malloy-interfaces";
import type { DataStyles } from "@malloydata/render";
import { lazyCounter, lazyHistogram } from "../telemetry";
import { recordVariantRouting, routeQuery } from "./query_router";
import {
   answerOf,
   queryPathFor,
   type QueryServingHost,
   recordingManifest,
   variantsOf,
} from "./query_serving";
import type { components } from "../api";
import { getMaxQueryRows, getMaxResponseBytes } from "../config";
import {
   AccessDeniedError,
   BadRequestError,
   InvalidQueryRequestError,
   ModelCompilationError,
   PayloadTooLargeError,
} from "../errors";
import { logger } from "../logger";
import {
   accessPathOf,
   summarizeQueryAccess,
   type QueryAudit,
} from "./access_record";
import {
   manifestUnderCompilerKey,
   readsUnderCompilerKeys,
   underCompilerKeys,
   pin,
   type Pinned,
   readsByAddress,
   type Snapshot,
} from "./materialization/domain/binding";
import type { BuildManifest } from "./materialization/domain/types";
import {
   answeredWith,
   type Answer,
   canDegradeToLive,
   isDestinationCatalogUnavailableError,
   isStoredDecimalArithmeticError,
   liveFallbackAnswer,
   type Leg,
   runManifestOf,
   type ServedFrom,
   servedFromOf,
} from "./materialization/serve/leg";
import {
   liveRoundsBigIntegersAsPostgres,
   type ResultRows,
   roundBigIntegersAsLive,
} from "./materialization/domain/live_big_integers";
import {
   type Float4Shape,
   float4ColumnsByHandle,
   float4OutputShape,
   liveWritesFloat4AsPostgres,
   type ResultRows as Float4ResultRows,
   roundFloat4AsLive,
} from "./materialization/domain/live_float4";
import type { Binding } from "./materialization/domain/binding";
import {
   readPreparedQuery,
   resolveStructRef,
} from "./governance/ir/prepared_query";
import type { ServedCopyServe } from "./materialization/serve/served_copy_serve";
import type { ApiQueryPath } from "./materialization/serve/query_path";
import {
   assertNoAuthorizeTagLike,
   assertNoCallerAuthorizeAnnotation,
   assertNoCallerClassificationAnnotation,
} from "./authorize";
import {
   buildFilterClause,
   filterHasValue,
   FilterValidationError,
   injectFilterRefinement,
   type FilterDefinition,
} from "./filter";
import { type QueryRowLimitSource } from "./model_limits";
import {
   isGivenBindingFailure,
   mapRunError,
   prepareAndRun,
   serializeWithinCaps,
   type PreparedRun,
} from "./query_execution";
import { responseBytes } from "./statement_usage";
import { type CallerRegion } from "./caller_joins";
import type { ModelQueryMetadataInput, QueryMetadata } from "./query_metadata";
import type { GovernRequest, QueryGovernor } from "./governance/query_governor";

type GovernedRunnable = Awaited<
   ReturnType<QueryGovernor["govern"]>
>["runnable"];
import {
   recordAuthorizeGuardRejection,
   recordRowLevelGateDecision,
   type AuthorizeGuardField,
} from "../authorize_metrics";
import {
   compileErrorOf,
   multipleRunStatementsError,
   queryCompileError,
   runStatementCount,
} from "./model_query_compile";
import { quotedIdentifier } from "./malloy_text";
import type { QueryPrivileges, QueryRequest } from "./model_query_request";

type ApiSource = components["schemas"]["Source"];
type ApiGiven = components["schemas"]["Given"];

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
    * The path report (D1): what answered the query and why. Undefined only
    * when it could not be assembled; reporting never fails a query.
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

/** What the pipeline reads from the model that owns the query. */
export interface ModelServingHost {
   readonly packageName: string;
   readonly modelPath: string;
   readonly governor: QueryGovernor;
   compilationError(): MalloyError | Error | undefined;
   modelMaterializer(): ModelMaterializer | undefined;
   modelDef(): ModelDef | undefined;
   modelInfo(): Malloy.ModelInfo | undefined;
   dataStyles(): DataStyles;
   sources(): ApiSource[] | undefined;
   givens(): ApiGiven[] | undefined;
   /** Every table the model serves now; pinned once per query. */
   snapshot(): Snapshot;
   /** The model's served copy now (captured once per query). */
   servedCopy(): ServedCopyServe;
   /** The legs for one pin (`makeLegs`), around `servedCopy`. */
   legs(pinned: Pinned, servedCopy: ServedCopyServe): readonly Leg[];
   /** What the path report reads of the model, around `servedCopy`. */
   servingHost(servedCopy: ServedCopyServe): QueryServingHost;
   /** Whether the view or query leg is on (the path report then names the called view or query). */
   persistedViewsEnabled(): boolean;
   /** `manifest` with the rollups' own entries removed. */
   withoutPreaggregateEntries(
      manifest: BuildManifest | undefined,
   ): BuildManifest | undefined;
   ensureSharedQueryModel(materializer: ModelMaterializer): Promise<void>;
   getFilters(sourceName: string): FilterDefinition[];
   assertFilterGivens(givens: Record<string, GivenValue> | undefined): void;
   assertQueryBoundaryEarly(
      sourceName?: string,
      queryName?: string,
      query?: string,
   ): "cleared" | "deferred";
   /**
    * The refusal for caller text that did not compile and names a source the
    * caller may not query (the boundary's 404), or undefined when the
    * compiler's own problems may be shown.
    */
   unqueryableTextRefusal(
      query: string,
      compileError: MalloyError,
   ): Error | undefined;
   resolveQueryMetadata(
      input: ModelQueryMetadataInput | undefined,
      connectionName: string | undefined,
      sourceName?: string,
   ): QueryMetadata | undefined;
}

/** What admission settled before anything compiled. */
interface Admitted {
   /** The boundary's early answer; "deferred" is settled on the compile. */
   readonly boundary: "cleared" | "deferred";
   /** The caller's own text; its joins are checked as extra run targets. */
   readonly callerRegion: CallerRegion | undefined;
   /** The run target the advisory checks resolved before compiling. */
   readonly earlySource: string | undefined;
}

/** The caller's query, compiled live. */
interface Prepared {
   /** The text the router saw: the caller's, with any filter injected. */
   readonly queryString: string;
   /** The caller's own compile; the run-time live retry runs it. */
   readonly liveRunnable: QueryMaterializer;
   /** See filterGivensToModelSurface. */
   readonly surfaceGivens: Record<string, GivenValue> | undefined;
}

/** The query as governance left it. */
interface Governed {
   readonly runnable: GovernedRunnable;
   readonly request: GovernRequest;
   readonly decision: Awaited<ReturnType<QueryGovernor["govern"]>>["decision"];
   readonly compiledSource: string | undefined;
}

/** What the run produced, and the answer as the run left it. */
interface Ran {
   readonly run: PreparedRun;
   readonly answer: Answer;
   readonly executionTime: number;
   /** The caps the run was held to, which the result is held to too. */
   readonly maxRows: number;
   readonly maxBytes: number;
}

/** The query's request fields and privileges, read once. */
interface QueryContext {
   readonly audit: QueryAudit;
   readonly request: QueryRequest;
   readonly privileges: QueryPrivileges;
   readonly startTime: number;
}

export class ModelServing {
   constructor(private readonly host: ModelServingHost) {}

   /** Run one query: pin, prepare, route, govern, run, report. */
   async run(
      audit: QueryAudit,
      request: QueryRequest,
      privileges: QueryPrivileges,
   ): Promise<ModelQueryResults> {
      // A stored table is read only by a leg that reports it in the path
      // report: the served copy, the rollup leg or the view leg. The live
      // compile reads none (see the run stage's manifests).
      const servedCopy = this.host.servedCopy();
      const ctx: QueryContext = {
         audit,
         request,
         privileges,
         startTime: performance.now(),
      };
      this.assertCompiled();
      if (
         !this.host.modelMaterializer() ||
         !this.host.modelDef() ||
         !this.host.modelInfo()
      )
         throw new BadRequestError("Model has no queryable entities.");
      const pinned = this.pin();
      // Without rollup entries, for the persisted-view leg's variant.
      const viewManifest = this.host.withoutPreaggregateEntries(
         pinnedManifest(pinned),
      );
      const admitted = await this.admit(ctx);
      let prepared: Prepared;
      let answer: Answer;
      // Query parsing errors from the compile and the route map to a 400.
      try {
         prepared = await this.prepare(ctx);
         answer = await this.route(ctx, prepared, pinned, admitted, servedCopy);
      } catch (error) {
         throw this.preparationError(ctx, error);
      }
      const governed = await this.govern(ctx, admitted, prepared, answer);
      const ran = await this.execute(ctx, {
         pinned,
         viewManifest,
         admitted,
         prepared,
         answer,
         governed,
      });
      try {
         return await this.report(ctx, {
            pinned,
            prepared,
            governed,
            ran,
            servedCopy,
         });
      } finally {
         // Emitted with the response's size in report(); here only when report
         // threw first (a cap refused the result), so the statement still has
         // its record.
         ran.run.usage?.emit();
      }
   }

   // ---- Stage 1: pin ----------------------------------------------------

   /**
    * Pinned once, before routing (C1b): every leg, the run and the path
    * report read the tables bound now, at this instant, so a rebind or a
    * window crossed mid-query cannot split the query.
    */
   private pin(): Pinned {
      return pin(this.host.snapshot(), new Date());
   }

   // ---- Stage 2: prepare ------------------------------------------------

   /** The compile failure every query on this model answers with. */
   private assertCompiled(): void {
      const compilationError = this.host.compilationError();
      if (compilationError) {
         // Re-throw MalloyError and ModelCompilationError as-is (they map to 400/424)
         if (
            compilationError instanceof MalloyError ||
            compilationError instanceof ModelCompilationError
         ) {
            throw compilationError;
         }
         // For other compilation errors, wrap as BadRequestError (400)
         throw new BadRequestError(
            `Model compilation failed: ${compilationError.message}`,
         );
      }
   }

   /**
    * Query boundary FIRST (404), before authorize and compilation, so a
    * non-queryable source's schema cannot be probed. "deferred" is settled by
    * the compiled backstop in the govern stage. Then the advisory pre-compile
    * checks (`OracleGuard`): a refused caller hears the gate's 403 or the
    * boundary's 404 before any compile error could describe the source. Never
    * the authority; `govern` is.
    */
   private async admit(ctx: QueryContext): Promise<Admitted> {
      const { sourceName, queryName, query, givens } = ctx.request;
      const { bypassAuthorize, includeHiddenFilesAndSources } = ctx.privileges;
      const boundary = includeHiddenFilesAndSources
         ? "cleared"
         : this.host.assertQueryBoundaryEarly(sourceName, queryName, query);
      // Text carrying an annotation is left to the forgery rejecter below.
      const callerRegion: CallerRegion | undefined =
         !sourceName && !queryName && query
            ? { kind: "query", text: query }
            : undefined;
      const { earlySource } =
         await this.host.governor.oracle.beforeQueryCompile({
            sourceName,
            queryName,
            query,
            givens,
            bypassAuthorize,
            includeHiddenFilesAndSources,
            callerRegion,
         });
      return { boundary, callerRegion, earlySource };
   }

   /**
    * The caller's query compiled live: the caller-text guards, the text the
    * router sees, `#(filter)` injection and its bind check.
    */
   private async prepare(ctx: QueryContext): Promise<Prepared> {
      const { givens } = ctx.request;
      assertNoCallerAnnotations(ctx.request);
      let queryString = this.queryStringOf(ctx);
      const injected = this.injectFilters(ctx, queryString);
      queryString = injected.queryString;

      // Restricted mode keeps untrusted text inside the model's surface: it
      // rejects `import`, raw `connection.table(...)`/`.sql(...)`, raw-SQL
      // functions, and `##!` flags. Only author text, never a caller's, uses
      // the unrestricted `loadQuery`.
      const materializer = this.host.modelMaterializer()!;
      await this.host.ensureSharedQueryModel(materializer);
      const runnable = materializer.loadRestrictedQuery(queryString);
      const governor = this.host.governor;
      const surfaceGivens = governor.filterGivensToModelSurface(
         givens,
         await governor.givenNamesReadByQuery(runnable, givens),
      );

      // An injected `#(filter)` reads its dimension by NAME, so a caller's
      // own `rename:`/`except:` can rebind it to another column; check
      // against the struct actually compiled.
      if (
         injected.filters &&
         injected.filters.length > 0 &&
         injected.sourceName
      ) {
         // Prepare first, unguarded, so a genuine compile failure surfaces
         // as itself rather than as the bind check's fail-closed 403.
         await runnable.getPreparedQuery();
         await governor.assertFilterAnnotationsBindToDeclaringSource(
            runnable,
            injected.sourceName,
            injected.filters,
         );
      }
      return { queryString, liveRunnable: runnable, surfaceGivens };
   }

   /** The text a request runs, or the refusal of a malformed request. */
   private queryStringOf(ctx: QueryContext): string {
      const { sourceName, queryName, query, queryMetadataInput } = ctx.request;
      if (!sourceName && !queryName && query) {
         return "\n" + query;
      } else if (queryName && !query) {
         // NAMES, not Malloy text: quoted so each lexes as one identifier.
         // Unquoted, a newline would open a second statement (Malloy runs
         // the LAST `run:`), and getFilters() would miss the raw name and
         // inject no filter.
         return `\nrun: ${
            sourceName ? `${quotedIdentifier(sourceName)} -> ` : ""
         }${quotedIdentifier(queryName)}`;
      }
      const endTime = performance.now();
      const executionTime = endTime - ctx.startTime;
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

   /**
    * Inject source filter predicates unless bypassed. For ad-hoc queries
    * resolve the run target through any alias/extend/chain so a protected
    * source can't be read unfiltered under a different name.
    */
   private injectFilters(
      ctx: QueryContext,
      queryString: string,
   ): {
      queryString: string;
      filters: FilterDefinition[] | undefined;
      sourceName: string | undefined;
   } {
      const { sourceName, queryName, query, filterParams } = ctx.request;
      // Only controls how the protected source is resolved for filter
      // injection; both forms are untrusted and compile restricted.
      const isAdHocQuery = !sourceName && !queryName && !!query;
      let injectedFilters: FilterDefinition[] | undefined;
      let injectedFilterSourceName: string | undefined;
      if (!ctx.privileges.bypassFilters) {
         const effectiveSource = isAdHocQuery
            ? this.host.governor.resolveFilterSource(query)
            : sourceName;
         if (effectiveSource) {
            const filters = this.host.getFilters(effectiveSource);
            if (filters.length > 0) {
               const filterClause = buildFilterClause(
                  filters,
                  filterParams ?? {},
               );
               queryString = injectFilterRefinement(queryString, filterClause);
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
      return {
         queryString,
         filters: injectedFilters,
         sourceName: injectedFilterSourceName,
      };
   }

   /** An error from the compile or the route, as the caller hears it. */
   private preparationError(ctx: QueryContext, error: unknown): Error {
      const { sourceName, queryName, query } = ctx.request;
      // Re-throw BadRequestError as-is
      if (error instanceof BadRequestError) {
         return error;
      }
      // Re-throw AccessDeniedError as-is (403), e.g. from the `#(filter)`
      // binding check inside this block.
      if (error instanceof AccessDeniedError) {
         return error;
      }
      // Source filter validation errors are client errors (400)
      if (error instanceof FilterValidationError) {
         return new BadRequestError(error.message);
      }
      // Re-throw MalloyError as-is (maps to 400)
      if (error instanceof MalloyError) {
         return error;
      }
      // For other query parsing errors, wrap as BadRequestError
      const errorMessage =
         error instanceof Error ? error.message : String(error);
      logger.warn("Query parsing error", {
         error,
         errorMessage,
         packageName: this.host.packageName,
         modelPath: this.host.modelPath,
         query,
         queryName,
         sourceName,
      });
      return new BadRequestError(`Invalid query: ${errorMessage}`);
   }

   // ---- Stage 3: route --------------------------------------------------

   /**
    * Routing eligibility is decided BEFORE routing (see
    * `queryEntryPointHasRowLevelGate`): a leg that cannot keep a gate is
    * vetoed by it, and one that keeps the contracts grafts and proves the gate
    * on its own compile.
    */
   private async route(
      ctx: QueryContext,
      prepared: Prepared,
      pinned: Pinned,
      admitted: Admitted,
      servedCopy: ServedCopyServe,
   ): Promise<Answer> {
      const runnable = prepared.liveRunnable;
      const legs = this.host.legs(pinned, servedCopy);
      const route = await routeQuery(
         variantsOf(
            legs,
            {
               queryString: prepared.queryString,
               givens: prepared.surfaceGivens,
               authorRunnable: runnable,
            },
            pinned,
            { modelPath: this.host.modelPath },
         ),
         async () => {
            // Cheap short-circuit for gate-free models. Safe, unlike
            // `hasAuthorize()`: see `hasAnyAuthorizeNote`.
            const governor = this.host.governor;
            if (
               ctx.privileges.bypassAuthorize ||
               !governor.hasAnyAuthorizeNote()
            ) {
               return false;
            }
            const veto = await governor.rowLevelGateVeto(
               runnable,
               admitted.callerRegion,
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
      return answerOf(route);
   }

   // ---- Stage 4: govern -------------------------------------------------

   /**
    * The AUTHORITATIVE governance, on the compiled run target (the early
    * checks cannot resolve named-query, multi-statement or caller-declared
    * forms): the compiled boundary backstop (404) for a "deferred"
    * admission (see assertQueryBoundaryEarly), then every gate (403), the
    * row-level graft, the bind guards and the ceiling. MUST run
    * unconditionally and its runnable MUST replace the routed one: for a
    * row-level gate the replacement IS the enforcement.
    *
    * Do NOT guard on hasAuthorize(): it misses inherited gates. The recompile
    * stays restricted, as the original compile was.
    */
   private async govern(
      ctx: QueryContext,
      admitted: Admitted,
      prepared: Prepared,
      answer: Answer,
   ): Promise<Governed> {
      const { sourceName, queryName, query, givens } = ctx.request;
      const { bypassAuthorize } = ctx.privileges;
      const { liveRunnable, queryString } = prepared;
      const runnable =
         answer.leg === "live" ? liveRunnable : answer.answer.runnable;
      await this.settleCompileError(ctx, admitted, prepared, answer);

      // Counted off the compile in hand, but refused only after every gate, so
      // the count cannot reveal whether a hidden or locked source exists.
      const runCount =
         !sourceName && !queryName && query
            ? await runStatementCount(runnable)
            : undefined;

      const governor = this.host.governor;
      const compiledSource =
         await governor.resolveAuthorizeSourceFromRunnable(runnable);
      const governRequest = governRequestOf({
         givens,
         ceiling: {
            audit: ctx.audit,
            source: () => compiledSource ?? sourceName,
            // Read on the compile that answers: the served copy is the
            // author's text with its classifications kept (the load's checks
            // decline a copy that does not keep them).
            ...(answer.leg === "served"
               ? { answeredBy: "served_copy" as const }
               : {}),
         },
         // The served copy is the author's text, gates included, as are a
         // persisted view's and a `#(rollup)` rollup's variants: a
         // row-level gate is grafted and proven on its own compile, so the
         // routed answer keeps it (R3), and an answer let past an entry-point
         // gate is re-proven there. Any other routed shape carries no
         // annotations and is never reached past a row-level gate (the
         // router's veto).
         graft: {
            recompile: (mm: ModelMaterializer) =>
               mm.loadRestrictedQuery(queryString),
            graftScope:
               answer.leg === "live" ? undefined : answer.answer.graftScope,
         },
         query: {
            bypassAuthorize,
            boundary:
               admitted.boundary === "deferred"
                  ? { compiledSource, query }
                  : undefined,
            callerRegion: admitted.callerRegion,
            // The caller's OWN text, not the synthesized/injected `queryString`.
            callerQueryText: query,
            routedPastEntryPointGate:
               answer.leg !== "live" && answer.pastEntryPointGate,
            // A rollup's variant writes again the given-reading terms its
            // build left out: re-proven on it.
            reprovesGivenTerms: answer.leg === "rollup",
         },
      });
      const { runnable: governed, decision } = await governor.govern(
         governRequest,
         // The LIVE compile keeps the caller's joins; a routed one does not.
         { runnable, live: liveRunnable },
      );
      if (runCount !== undefined && runCount > 1) {
         queryExecutionHistogram().record(
            performance.now() - ctx.startTime,
            this.queryMetricAttributes({
               environment: ctx.request.queryMetadataInput?.environment,
               queryName,
               sourceName,
               status: "error",
               servedFrom: servedFromOf(answer),
            }),
         );
         // An unsupplied row-level gate given denies at prepare; the count must
         // not answer first.
         throw (
            governor.unboundGateDenial(
               governed,
               givens,
               compiledSource ?? sourceName ?? "unknown",
               decision.grafts.flatMap((graft) => graft.givenNames),
            ) ?? multipleRunStatementsError(runCount)
         );
      }
      // After the gate, so a denied caller gets its 403 rather than a 400 about
      // a value; before prepare, which is where Malloy would parse it.
      this.host.assertFilterGivens(givens);
      // No post-hoc gate check is possible on a serve-shape or v1
      // `#@ preaggregate` runnable (it carries no annotations): their
      // correctness rests entirely on `queryEntryPointHasRowLevelGate`. The
      // served copy, a persisted view and a `#(rollup)` rollup are the
      // exceptions (R3): each keeps the author's gates on the run target's
      // own `sourceID`, and `govern()` above grafted and proved them on its
      // own compile.
      return {
         runnable: governed,
         request: governRequest,
         decision,
         compiledSource,
      };
   }

   /**
    * Caller text that does not compile is settled here, before the compiled
    * backstop turns an unresolved target into a bare 404. Every pre-compile
    * gate has already run, so when every source the text names is queryable
    * the caller gets the compiler's problems as a 400; otherwise the 404
    * (except a grammar-only failure naming nothing in an ungated model, see
    * parseFailureNamesNothing). An unbound given is left to the run path,
    * which answers opaquely when a gate reads it. Skipped when routed (same
    * text, a second compile).
    */
   private async settleCompileError(
      ctx: QueryContext,
      admitted: Admitted,
      prepared: Prepared,
      answer: Answer,
   ): Promise<void> {
      const { sourceName, queryName, query } = ctx.request;
      if (!sourceName && !queryName && query && answer.leg === "live") {
         const settled = await settledCompileError(
            prepared.liveRunnable,
            query,
            admitted.boundary,
            (text, compileError) =>
               this.host.unqueryableTextRefusal(text, compileError),
         );
         if (!settled) return;
         if (settled.offSurface) throw settled.refusal;
         queryExecutionHistogram().record(
            performance.now() - ctx.startTime,
            this.queryMetricAttributes({
               environment: ctx.request.queryMetadataInput?.environment,
               queryName,
               sourceName,
               status: "error",
               servedFrom: servedFromOf(answer),
            }),
         );
         throw settled.refusal;
      }
   }

   // ---- Stage 5: run ----------------------------------------------------

   /**
    * Run the governed runnable with what the answer names: a run on a
    * virtual map (the served copy, a `storage=` view or rollup) takes no
    * manifest; a database rollup reads the full pinned one, a database
    * persisted view the one without rollups, and the live compile an empty
    * one. Prepare runs INSIDE the run's try/catch, so a given-binding throw
    * at prepare gets the same error mapping as run instead of escaping as a
    * 500.
    */
   private async execute(
      ctx: QueryContext,
      args: {
         pinned: Pinned;
         viewManifest: BuildManifest | undefined;
         admitted: Admitted;
         prepared: Prepared;
         answer: Answer;
         governed: Governed;
      },
   ): Promise<Ran> {
      const { abortSignal, queryMetadataInput } = ctx.request;
      const { prepared, governed, admitted } = args;
      let answer = args.answer;
      const buildManifest = pinnedManifest(args.pinned);
      const runManifest = runManifestOf(answer, {
         pinned: buildManifest,
         withoutRollups: args.viewManifest,
         // The live compile reads no table at all: an explicit empty
         // manifest, since with none Malloy falls back to the manifest the
         // model's runtime was reloaded with, which no freshness window
         // filters and no path report names.
         live: EMPTY_BUILD_MANIFEST,
      });
      // A stored answer's table that Malloy files under a key not its address
      // is read under that key, as the leg compiled it; a read of the key is
      // a read of that table.
      const keyed =
         answer.leg !== "live" && !answer.runFailed
            ? answer.answer.keyedTable
            : undefined;
      const keyedTable =
         keyed && runManifest?.entries[keyed.tableId] ? keyed : undefined;
      // As Malloy reads it: each entry also under its compiler key, and the
      // answer's own table under its key over any sibling's.
      const compilerKeys = args.pinned.snapshot.compilerKeys;
      const effectiveBuildManifest = manifestUnderCompilerKey(
         underCompilerKeys(runManifest, compilerKeys),
         keyedTable,
      );
      const maxRows = getMaxQueryRows();
      const maxBytes = getMaxResponseBytes();
      let executionTime = 0;
      let run: PreparedRun;
      // The manifest entries the run that answers reads (recordingManifest).
      const runRead = new Set<string>();
      // Prepare yields the connection name, so its metadata layer resolves
      // before the statement is issued.
      const runOptions = {
         abortSignal,
         maxRows,
         usage: statementUsage(queryMetadataInput, this.host.packageName),
         queryMetadata: (connectionName: string | undefined) =>
            this.host.resolveQueryMetadata(
               queryMetadataInput,
               connectionName,
               // The gate's run-target answer (compiled, else surface), so
               // a `queryName` or derived query is tagged with the source
               // it actually reads.
               governed.compiledSource ?? admitted.earlySource,
            ),
      };
      try {
         // Passed whether or not the query routes. A served answer reads this
         // model's given surface, and the givens MUST be passed: a stored
         // source's re-emitted `where:` may read one, and dropping it would
         // read the table unfiltered (every caller's rows).
         run = await prepareAndRun(
            governed.runnable,
            {
               givens: prepared.surfaceGivens,
               buildManifest: recordingManifest(
                  effectiveBuildManifest,
                  runRead,
               ),
               virtualMap:
                  answer.leg === "live" ? undefined : answer.answer.virtualMap,
            },
            runOptions,
         );
         // AFTER run(), so it includes the warehouse round trip.
         executionTime = performance.now() - ctx.startTime;
         answer = answeredWith(
            answer,
            readsUnderCompilerKeys(
               readsByAddress(runRead, keyedTable),
               runManifest,
               compilerKeys,
            ),
         );
      } catch (error) {
         // `freshnessFallback=live` marks the tier as an optimisation, not a
         // dependency, so a store failing UNDER a routed query degrades to
         // live. Deliberately narrow:
         //  - only when the query read a stored table (a virtual map, or a
         //    database rollup through the build manifest);
         //  - only per `bindingsAllowDegradeToLive` over the bindings that
         //    produced THIS shape (a sibling source must not decide);
         //  - or for DECIMAL arithmetic past 38 digits, whatever the policy
         //    (`isStoredDecimalArithmeticError`): the table is right;
         //  - or for a destination catalog the worker lost, whatever the
         //    policy (`isDestinationCatalogUnavailableError`);
         //  - never for a client error (bad given) or an abort;
         //  - the retry re-supplies the REAL givens, or the live source could
         //    serve unfiltered rows.
         const failed = answer;
         if (
            !canDegradeToLive(failed, error, {
               hasLiveRunnable: !!prepared.liveRunnable,
               aborted: !!abortSignal?.aborted,
            })
         ) {
            this.failQuery(ctx, governed, answer, error);
         }
         logger.warn(
            isStoredDecimalArithmeticError(error)
               ? "Storage-served query's DECIMAL arithmetic exceeded 38 digits; falling back to live"
               : isDestinationCatalogUnavailableError(error)
                 ? "Storage-served query found its destination catalog unavailable; falling back to live"
                 : "Storage-served query failed at run time; falling back to live (freshnessFallback=live)",
            {
               modelPath: this.host.modelPath,
               ...(isDestinationCatalogUnavailableError(error)
                  ? { code: DUCKLAKE_CATALOG_UNAVAILABLE }
                  : {}),
               error: error instanceof Error ? error.message : String(error),
            },
         );
         // From here the query is the live retry: a retry failure's error
         // metric reads `live_fallback`, not a storage error the tier never
         // answered.
         answer = liveFallbackAnswer(failed);
         // Re-resolved per connection: the first bag was for the STORAGE
         // connection. The correlation id rides in the input, so it is kept.
         const retried = await this.retryLive(
            ctx,
            governed,
            prepared,
            answer,
            runOptions,
         );
         run = retried.run;
         // NOT a storage hit: servedFromOf reads `live_fallback`.
         answer = answeredWith(answer, retried.read);
         executionTime = performance.now() - ctx.startTime;
      }
      if (answer.leg !== "live" && !answer.runFailed) {
         try {
            await roundStoredAnswerAsLive(run.result, prepared.liveRunnable, {
               runnable: governed.runnable,
               bindings: answer.answer.shapeBindings,
            });
         } catch (error) {
            // The statement ran: its record goes out, without a response.
            run.usage?.emit();
            throw error;
         }
      }
      return { run, answer, executionTime, maxRows, maxBytes };
   }

   /**
    * The live retry of a stored run that failed. The retry runs the LIVE
    * compile, which the governed runnable is not, so it is governed in its
    * own right here: the same bind, on the runnable that will execute. A
    * gated entry point reaches here only through the served copy (R3), whose
    * answer's graft scope is the served copy's: the retry grafts on the
    * author's own scope, or the graft would recompile against the stored
    * table that just failed. Outside the retry's try, so a denial answers
    * 403, not a mapped run error.
    */
   private async retryLive(
      ctx: QueryContext,
      governed: Governed,
      prepared: Prepared,
      answer: Answer,
      runOptions: Parameters<typeof prepareAndRun>[2],
   ): Promise<{ run: PreparedRun; read: ReadonlySet<string> }> {
      const governRequest = governed.request;
      const { runnable: governedLive } = await this.host.governor.govern(
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
         { runnable: prepared.liveRunnable, live: prepared.liveRunnable },
      );
      const retryRead = new Set<string>();
      try {
         // A fresh prepare re-derives the row cap from the live shape (a
         // failure at storage prepare would leave it 0, an EMPTY answer).
         // The empty manifest: this runs on the author's model.
         const run = await prepareAndRun(
            governedLive,
            {
               givens: prepared.surfaceGivens,
               // The retry is the run that answers: record its reads alone.
               buildManifest: recordingManifest(
                  EMPTY_BUILD_MANIFEST,
                  retryRead,
               ),
            },
            runOptions,
         );
         return { run, read: retryRead };
      } catch (retryError) {
         this.failQuery(ctx, governed, answer, retryError);
      }
   }

   /**
    * Both the original failure and a failure OF THE RETRY end here, so a
    * retry failure still gets the error metric and mapping.
    */
   private failQuery(
      ctx: QueryContext,
      governed: Governed,
      answer: Answer,
      err: unknown,
   ): never {
      const { sourceName, queryName, query, givens, queryMetadataInput } =
         ctx.request;
      const errorEndTime = performance.now();
      const errorExecutionTime = errorEndTime - ctx.startTime;
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

      // Ahead of the `MalloyError` rethrow; see unboundGateGivenDenial.
      const gateDenial = this.unboundGateGivenDenial(
         err,
         governed.runnable,
         givens,
         governed.compiledSource ?? sourceName ?? "unknown",
      );
      if (gateDenial) throw gateDenial;

      return mapRunError(err, {
         packageName: this.host.packageName,
         modelPath: this.host.modelPath,
         failurePrefix: "Query execution failed",
         subject: governed.compiledSource ?? sourceName ?? undefined,
         passThrough: [],
         logFailure: (errorMessage) =>
            logger.error("Query execution error", {
               error: err,
               errorMessage,
               packageName: this.host.packageName,
               modelPath: this.host.modelPath,
               query,
               queryName,
               sourceName,
            }),
      });
   }

   // ---- Stage 6: report -------------------------------------------------

   /**
    * The caps, the metrics, the access record and the path report, all read
    * off the one answer.
    */
   private async report(
      ctx: QueryContext,
      args: {
         pinned: Pinned;
         prepared: Prepared;
         governed: Governed;
         ran: Ran;
         servedCopy: ServedCopyServe;
      },
   ): Promise<ModelQueryResults> {
      const { audit, request, privileges } = ctx;
      const { sourceName, queryName, givens, queryMetadataInput } = request;
      const { prepared, governed, ran } = args;
      const { answer, executionTime } = ran;
      const {
         result: queryResults,
         rowLimit,
         rowLimitSource,
         queryMetadata: appliedQueryMetadata,
      } = ran.run;
      const decision = governed.decision;

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
      // A wrap failure is mapped as a run failure is (error metric, status);
      // a cap's 413 keeps its own answer.
      let serializedWithinCaps: ReturnType<typeof serializeWithinCaps>;
      try {
         serializedWithinCaps = serializeWithinCaps(queryResults, {
            maxRows: ran.maxRows,
            maxBytes: ran.maxBytes,
            source: "model_query",
            compact: (request.responseShape ?? "full") === "compact",
         });
      } catch (err) {
         if (err instanceof PayloadTooLargeError) throw err;
         return this.failQuery(ctx, governed, answer, err);
      }
      const { wrapped: wrappedResult, serialized: serializedResult } =
         serializedWithinCaps;
      // Emitted with this size just before report() returns, once nothing left
      // can fail the answer; runQuery's finally covers every earlier throw.
      const servedBytes = responseBytes(serializedResult);
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
            // The live runnable carries the author's annotations the summary reads.
            runnable: prepared.liveRunnable,
            modelDef: this.host.modelDef(),
            enforced: decision.enforced,
            apiSources: this.host.sources(),
            modelGivens: this.host.givens(),
            source: governed.compiledSource ?? sourceName,
            givens,
            bypassAuthorize: privileges.bypassAuthorize,
            grafts: decision.grafts,
            rowCount: queryResults.totalRows,
            // A named view on a source; a model-level named query has no source.
            ...(sourceName && queryName ? { view: queryName } : {}),
         });
      } catch (err) {
         logger.debug("Access summary unavailable", {
            modelPath: this.host.modelPath,
            error: (err as Error).message,
         });
      }
      // Counted once the run answered, so a variant whose run failed and fell
      // back live counts as skipped (`run_failed`), not used.
      recordVariantRouting(
         answer.considered,
         answer.leg !== "live" && answer.runFailed,
      );
      const path = await queryPathFor(
         this.host.servingHost(args.servedCopy),
         answer,
         {
            // The author's own compile: its names, and no grafted gate filter.
            runnable: prepared.liveRunnable,
            sourceName: governed.compiledSource ?? sourceName,
            viewName: sourceName && queryName ? queryName : undefined,
            pinned: args.pinned,
            includeHidden: !!privileges.includeHiddenFilesAndSources,
            ...(this.host.persistedViewsEnabled()
               ? { queryString: prepared.queryString }
               : {}),
         },
      );
      const servedFrom = servedFromOf(answer);
      audit.servedFrom = servedFrom ?? null;
      audit.executionTimeMs = Math.round(executionTime);
      if (queryCostBytes !== undefined) audit.scannedBytes = queryCostBytes;
      const recordedPath = path ? accessPathOf(path) : undefined;
      if (recordedPath) audit.path = recordedPath;
      // No report at all (it could not be assembled), as against a report
      // that says live with nothing considered, which records no `path`.
      if (!path) audit.pathUnavailable = true;
      ran.run.usage?.emit(servedBytes);
      return {
         result: wrappedResult,
         serializedResult,
         compactResult: queryResults.data.value,
         modelInfo: this.host.modelInfo()!,
         dataStyles: this.host.dataStyles(),
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
         "malloy.model.path": this.host.modelPath,
         "malloy.package": this.host.packageName,
         "malloy.model.query.name": args.queryName,
         "malloy.model.query.source": args.sourceName,
         "malloy.model.query.status": args.status,
         ...(args.environment
            ? { "malloy.environment": args.environment }
            : {}),
         ...(args.connection
            ? { "malloy.model.query.connection": args.connection }
            : {}),
         ...(args.servedFrom
            ? { "malloy.model.query.served_from": args.servedFrom }
            : {}),
      };
   }

   /**
    * The opaque 403 for a run that failed binding its givens, when a gate
    * grafted onto `runnable` reads a given the caller did not supply, else
    * undefined. Bad givens surface as Malloy `runtime-given-*` errors, mapped
    * to a 400 (the prefix is a pinned coupling to Malloy's error codes; also
    * in environment.ts). But a grafted gate's givens bind in the same
    * prepare, before any SQL runs, and Malloy's message names the missing
    * one, which `docs/authorize.md` promises a denied caller never sees.
    */
   private unboundGateGivenDenial(
      err: unknown,
      runnable: QueryMaterializer,
      givens: Record<string, unknown> | undefined,
      label: string,
   ): AccessDeniedError | undefined {
      if (!isGivenBindingFailure(err)) return undefined;
      const governor = this.host.governor;
      const denial = governor.unboundGateDenial(
         runnable,
         givens,
         label,
         governor.authorizeReferencedGivenNames,
      );
      if (denial) {
         logger.debug("Gate given unbound; denying opaquely", {
            packageName: this.host.packageName,
            modelPath: this.host.modelPath,
            error: err instanceof Error ? err.message : String(err),
         });
      }
      return denial;
   }
}

/** The pinned build manifest, or undefined when nothing database is bound. */
function pinnedManifest(pinned: Pinned): BuildManifest | undefined {
   return pinned.hasEntries ? pinned.compilerManifest : undefined;
}

/**
 * How ad-hoc caller `query`, compiled as `runnable`, is answered when it does
 * not compile: the boundary's 404 when the boundary deferred and a source the
 * text names is not queryable (`offSurface`), otherwise the compiler's
 * problems as a 400. Undefined when it compiles, or when the failure is an
 * unbound given, which the run path answers. The query route's settling
 * (`ModelServing.settleCompileError`), shared with the load-time `.malloynb`
 * cell check (`Model.consoleCellRefusal`) so the two cannot settle apart.
 */
export async function settledCompileError(
   runnable: { getPreparedQuery(): Promise<unknown> },
   query: string,
   boundary: "cleared" | "deferred",
   unqueryableTextRefusal: (
      query: string,
      compileError: MalloyError,
   ) => Error | undefined,
): Promise<
   | { refusal: Error; compileError: MalloyError; offSurface: boolean }
   | undefined
> {
   const compileError = await compileErrorOf(runnable);
   if (!compileError || isGivenBindingFailure(compileError)) return undefined;
   const unqueryable =
      boundary === "deferred"
         ? unqueryableTextRefusal(query, compileError)
         : undefined;
   return unqueryable
      ? { refusal: unqueryable, compileError, offSurface: true }
      : {
           refusal: queryCompileError(compileError, query),
           compileError,
           offSurface: false,
        };
}

/**
 * Before any compile: caller text may not declare an authorize gate (it
 * would override the author's). Names are also quoted below; checking them
 * too is defence in depth.
 */
function assertNoCallerAnnotations(request: QueryRequest): void {
   const { query, sourceName, queryName } = request;
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
         if (field === "query") assertNoCallerAuthorizeAnnotation(callerText);
         else assertNoAuthorizeTagLike(callerText);
      } catch (err) {
         // The only signal a rejection emits: the stage's catch skips the
         // query histogram for a BadRequestError.
         recordAuthorizeGuardRejection(field);
         throw err;
      }
   }
   if (query) assertNoCallerClassificationAnnotation(query);
}

/** The usage hook for a statement, when its caller says which connections are metered. */
function statementUsage(
   input: ModelQueryMetadataInput | undefined,
   packageName: string,
):
   | {
        metered: (connectionName: string) => boolean;
        catalog?: (connectionName: string) => string | undefined;
        environment: string;
        package: string;
     }
   | undefined {
   if (!input?.meteredConnection || !input.environment) return undefined;
   return {
      metered: input.meteredConnection,
      catalog: input.meteredCatalog,
      environment: input.environment,
      package: packageName,
   };
}

/**
 * The one request `govern()` judges: the caller's givens, the ceiling over the audit, and how a graft
 * recompiles on the scope it lands on; a query adds its boundary,
 * its caller's text and region, and whether it was routed past a gate.
 */
function governRequestOf(r: {
   givens: GovernRequest["givens"];
   ceiling: NonNullable<GovernRequest["ceiling"]>;
   graft: Pick<GovernRequest, "recompile" | "graftScope">;
   query?: Pick<
      GovernRequest,
      | "bypassAuthorize"
      | "boundary"
      | "callerRegion"
      | "callerQueryText"
      | "routedPastEntryPointGate"
      | "reprovesGivenTerms"
   >;
}): GovernRequest {
   return {
      givens: r.givens,
      ...r.query,
      // Last, so nothing a query passes can replace how the graft recompiles.
      ...r.graft,
      ceiling: r.ceiling,
   };
}

/**
 * A stored answer's values written as live writes them, when the query's
 * live dialect is Postgres: its big integers rounded as live rounds them
 * (`liveRoundsBigIntegersAsPostgres`), and the float4 values a `storage=`
 * table holds written as live's JSON stage writes a float4
 * (`liveWritesFloat4AsPostgres`). A live compile that cannot say its dialect
 * leaves the answer as it ran, and so does a served compile whose IR cannot
 * be read.
 */
async function roundStoredAnswerAsLive(
   result: PreparedRun["result"],
   liveRunnable: QueryMaterializer,
   served: {
      runnable: QueryMaterializer;
      bindings: readonly Binding[];
   },
): Promise<void> {
   let dialect: string;
   try {
      dialect = (await liveRunnable.getPreparedQuery()).dialect;
   } catch {
      return;
   }
   if (liveRoundsBigIntegersAsPostgres(dialect)) {
      roundBigIntegersAsLive(result as unknown as ResultRows);
   }
   if (!liveWritesFloat4AsPostgres(dialect)) return;
   const float4 = float4ColumnsByHandle(served.bindings);
   if (float4.size === 0) return;
   let shape: Float4Shape;
   try {
      const view = await readPreparedQuery(served.runnable);
      const live = await readPreparedQuery(liveRunnable);
      shape = float4OutputShape(
         {
            query: view.query,
            root: resolveStructRef(view.structRef, view.modelDef),
            liveQuery: live.query,
         },
         float4,
      );
   } catch {
      return;
   }
   roundFloat4AsLive(result as unknown as Float4ResultRows, shape);
}
