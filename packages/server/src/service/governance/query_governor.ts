// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `QueryGovernor`: the authorization half of a {@link Model}, moved out of
 * `../model.ts` verbatim. It owns every gate a query passes through after the
 * query boundary admits it — locks, caller joins, the row-level graft and its
 * proof, the bind guards and the `#(carries)` ceiling — and is the only code
 * that mints a {@link GovernedRunnable}.
 *
 * It reads the model it governs through {@link GovernorHost}, never by
 * importing `../model.ts` (lint rule R3); gate classification goes through
 * {@link GateClassificationDeps}.
 */

import type { QueryBoundaryPolicy } from "../model_policy";
import {
   GivenValue,
   isSourceDef,
   ModelDef,
   ModelMaterializer,
   QueryMaterializer,
   type FilterCondition,
   type SourceDef,
} from "@malloydata/malloy";
import { components } from "../../api";
import {
   AccessDeniedError,
   BadRequestError,
   NotQueryableError,
   OffSurfaceError,
} from "../../errors";
import { logger } from "../../logger";
import {
   enforcedGatesOf,
   summarizeQueryAccess,
   type EnforcedGates,
   type QueryAudit,
} from "../access_record";
import { ceilingRefusal, modelDeclaresCarries } from "../classification";
import {
   annotationTexts,
   ownLevelNotes,
   type AnnotationNote,
} from "../annotations";
import {
   containsAuthorizeAnnotationTag,
   referencedGivenNames,
   type RowLevelGateRejectionCause,
} from "../authorize";
import { ACCESS_FILTER_ROUTE, AUTHORIZE_ROUTE } from "../authorize_routes";
import { type NotebookFormat } from "../../notebook_metrics";
import { OracleGuard } from "./oracle_guard";
import { parseFilterAnnotation, type FilterDefinition } from "../filter";
import {
   buildDerivationBaseMap,
   buildJoinBaseMap,
   buildSourceAliasMap,
   collectIdentifierNames,
   extractRunTargetSourceName,
   extractRunTargetSourceNames,
   stripMalloyCommentsAndLiterals,
} from "../query_text";
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
} from "../caller_joins";
import {
   preparedQueryView,
   readPreparedQuery,
   resolveStructRef,
   sqlTargetRef,
   structRefName,
   queryGivenUsageIds,
} from "./ir/prepared_query";
import {
   mintGovernedRunnable,
   type GovernedRunnable,
} from "./governed_runnable";
import { derivedStructsReachable } from "./ir/source_graph";
import {
   arrayQueryJoinRefusal,
   arrayQueryJoins,
   governsAccess,
} from "./ir/array_query";
import {
   assertFilterDimensionBindsToDeclaringSource,
   assertGraftedGateBindsToDeclaringSource,
   assertInheritedSourceFiltersBind,
   findFilterAnnotationDeclaringSource,
   type DocumentLocationLike,
   type SiblingModelDefResolver,
   type StageLike,
} from "../filter_binding_guard";
// Aliased with an `Impl` suffix because `QueryGovernor` declares private wrapper
// methods of the same names; without the alias, converting one of them to an
// arrow-function property would recurse instead of delegating.
import {
   collectEntryPointGates as collectEntryPointGatesImpl,
   createGateClassificationDeps,
   resolveGateShape as resolveGateShapeImpl,
   resolveGraftTarget as resolveGraftTargetImpl,
   type GateClassificationDeps,
   type GateEntry,
   type GraftScope,
   type RowLevelGraftEntry,
} from "../gate_classification";
import {
   recordAuthorizeBypass,
   recordLockDecision,
   recordRowLevelGateDecision,
   recordRowLevelGateRejected,
   type AuthorizeBypassEntryPoint,
   type AuthorizeGateSite,
} from "../../authorize_metrics";
import { decideLock } from "../authorize_lock";
import {
   type HydrationMaterializer,
   type HydrationRuntime,
} from "../model_runtime";
import {
   type CellDeclaredGivens,
   type RunnableNotebookCell,
} from "../model_notebook_cell";

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
      throw AccessDeniedError.forSource(label);
   }
   recordLockDecision("denied_unresolvable", site);
   // Also booked on the row-level counter: a gate refused at shape resolution
   // is a fail-closed rejection, and `denied_by_gate` must count all of them.
   // `cause` is a finer label, not a substitute.
   recordRowLevelGateDecision("denied_by_gate", site);
   if (resolution.cause) recordRowLevelGateRejected(resolution.cause);
   throw AccessDeniedError.forSource(label);
}

/**
 * Whether two {@link EnforcedGates} hold the same gates: per route, the same
 * set of expressions. A gate reached twice (own and inherited, or by two
 * checks) counts once; order does not matter. Givens are read by name from
 * the expression text and bound from the one request, so equal text is the
 * same gate with the same givens.
 */
export function sameGateSet(a: EnforcedGates, b: EnforcedGates): boolean {
   const same = (x: readonly string[], y: readonly string[]) => {
      const xs = new Set(x);
      const ys = new Set(y);
      return xs.size === ys.size && [...xs].every((e) => ys.has(e));
   };
   return (
      same(a.authorize, b.authorize) && same(a.accessFilter, b.accessFilter)
   );
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
export const REQUEST_CHAIN_MAX_NAMES = 64;

/** The one sentence a gated model answers a hidden, hidden-and-locked or absent name with. */
const GENERIC_NOT_QUERYABLE = "Query target is not queryable.";

/**
 * One request to {@link QueryGovernor.govern}: who is asking (givens and
 * privileges), and what the caller wrote.
 */
export interface GovernRequest {
   /** The caller's givens as supplied; gates read `givens ?? {}`. */
   givens: Record<string, GivenValue> | undefined;
   /** Skip the gate walk (never settable from ingress); the bypass is audited. */
   bypassAuthorize?: boolean;
   /**
    * The compiled half of the query boundary, for an admission the early
    * check DEFERRED (see `Model.assertQueryBoundaryEarly`). Absent when the
    * boundary is already settled: cleared early, lifted by privilege, or (a
    * notebook cell) checked by the caller before anything ran.
    */
   boundary?: { compiledSource: string | undefined; query?: string };
   /**
    * Where caller-written text sits. Its joins are found in
    * {@link CompiledQuery.live} and gated like extra run targets.
    */
   callerRegion?: CallerRegion;
   /** The caller's own query text; arms the laundered-entry-point check. */
   callerQueryText?: string;
   /** See `authorizeAndBindRunnable`'s options. */
   recompile?: (
      materializer: ModelMaterializer,
      grafts: ReadonlyArray<{
         graftTarget: string;
         condition: FilterCondition;
      }>,
   ) => QueryMaterializer;
   /** See `authorizeAndBindRunnable`'s options. */
   proveGraft?: (recompiled: QueryMaterializer) => Promise<void>;
   /** See `authorizeAndBindRunnable`'s options. */
   graftScope?: GraftScope;
   /**
    * The router let `compiled.runnable`, a variant that keeps the author's
    * text (the served copy, R3), past a gate on the query's entry point. The
    * veto read the gate off the LIVE compile; this re-proves it on the routed
    * one: the request must carry that variant's own `graftScope` and the
    * `live` compile, and governing must enforce there exactly the gates the
    * live entry point carries (see `sameGateSet`), or the query is denied.
    */
   routedPastEntryPointGate?: boolean;
   /** See `authorizeAndBindRunnable`'s options. */
   bypassAlreadyNoted?: boolean;
   /**
    * The caller will never execute what comes back (`/compile`); see
    * `authorizeAndBindRunnable`'s options.
    */
   checkOnly?: boolean;
   /** See `authorizeAndBindRunnable`'s options. */
   skipOwnSourceGate?: boolean;
   /** See `authorizeAndBindRunnable`'s options. */
   compileCallerText?: string;
   /**
    * The `#(carries)` ceiling: checked unless absent or bypassed. `source`
    * names the entry for a refusal's access record, read off the governed
    * runnable when it needs one.
    */
   ceiling?: {
      audit: QueryAudit;
      source: (
         governed: GovernedRunnable,
      ) => string | undefined | Promise<string | undefined>;
      /**
       * `"served_copy"` when the router answered from the served copy (the
       * author's text re-rooted on stored tables), so the ceiling is read on
       * {@link CompiledQuery.runnable}, the compile that answers: the live
       * compile is a cross-connection model's build view, which lacks the
       * joins it leaves out, so a query through one does not compile there.
       * The live compile is still checked whenever it compiles. Any other
       * routed shape carries no annotations to read a ceiling from, so it
       * stays on the live compile.
       */
      answeredBy?: "served_copy";
   };
}

/** The compiled query {@link QueryGovernor.govern} decides. */
export interface CompiledQuery {
   /** The runnable to govern; what comes back is it, or its grafted recompile. */
   runnable: QueryMaterializer;
   /**
    * The LIVE compile of the caller's text, where caller joins are found and
    * the ceiling reads. Defaults to the governed runnable.
    */
   live?: QueryMaterializer;
}

/** What {@link QueryGovernor.govern} decided, for the access record. */
export interface GovernDecision {
   /**
    * The gates enforcement evaluated (on the bypass: found in force and
    * skipped); undefined when the bypass could not read the run target.
    */
   enforced: EnforcedGates | undefined;
   /** The row-level grafts attached to the governed runnable; empty if none. */
   grafts: readonly RowLevelGraftEntry[];
}

type ApiSource = components["schemas"]["Source"];
type ApiGiven = components["schemas"]["Given"];
type ApiQuery = components["schemas"]["Query"];

/**
 * What a {@link QueryGovernor} reads from the model it governs: the compiled
 * state (read live, since a notebook's cells are attached after construction)
 * and the query boundary's own answers, which stay with the model because
 * discovery curation shares them.
 */
export interface GovernorHost {
   readonly packageName: string;
   readonly modelPath: string;
   /** See `ModelState.gateRuntime`: absent, every row-level gate denies. */
   readonly gateRuntime: HydrationRuntime | undefined;
   modelDef(): ModelDef | undefined;
   modelMaterializer(): ModelMaterializer | undefined;
   sources(): ApiSource[] | undefined;
   queries(): ApiQuery[] | undefined;
   givens(): ApiGiven[] | undefined;
   filterMap(): Map<string, FilterDefinition[]>;
   runnableNotebookCells(): RunnableNotebookCell[] | undefined;
   siblingModelDefResolver(): SiblingModelDefResolver | undefined;
   queryBoundary(): QueryBoundaryPolicy;
   isNotebook(): boolean;
   notebookFormat(): NotebookFormat;
   isCuratedSource(name: string): boolean;
   isCuratedQuery(name: string): boolean;
   derivesFromCurated(
      name: string,
      query?: string,
      derivationContext?: string,
   ): boolean;
   notQueryable(
      refusal: string,
      explainable: boolean,
      refused: "model" | "source" | "query",
      name?: string,
   ): NotQueryableError;
   assertQueryBoundaryEarly(
      sourceName?: string,
      queryName?: string,
      query?: string,
      derivationContext?: string,
   ): "cleared" | "deferred";
   assertQueryBoundaryCompiled(
      compiledSource: string | undefined,
      query?: string,
      derivationContext?: string,
   ): void;
   ensureSharedQueryModel(
      materializer: ModelMaterializer | undefined,
   ): Promise<void>;
}

export class QueryGovernor {
   private get packageName(): string {
      return this.host.packageName;
   }
   private get modelPath(): string {
      return this.host.modelPath;
   }
   private get modelDef(): ModelDef | undefined {
      return this.host.modelDef();
   }
   private get modelMaterializer(): ModelMaterializer | undefined {
      return this.host.modelMaterializer();
   }
   private get sources(): ApiSource[] | undefined {
      return this.host.sources();
   }
   private get queries(): ApiQuery[] | undefined {
      return this.host.queries();
   }
   private get givens(): ApiGiven[] | undefined {
      return this.host.givens();
   }
   private get filterMap(): Map<string, FilterDefinition[]> {
      return this.host.filterMap();
   }
   private get runnableNotebookCells(): RunnableNotebookCell[] | undefined {
      return this.host.runnableNotebookCells();
   }
   private get siblingModelDefResolver(): SiblingModelDefResolver | undefined {
      return this.host.siblingModelDefResolver();
   }
   private get queryBoundary(): QueryBoundaryPolicy {
      return this.host.queryBoundary();
   }
   private isNotebook(): boolean {
      return this.host.isNotebook();
   }
   private notebookFormat(): NotebookFormat {
      return this.host.notebookFormat();
   }
   private isCuratedSource(name: string): boolean {
      return this.host.isCuratedSource(name);
   }
   private isCuratedQuery(name: string): boolean {
      return this.host.isCuratedQuery(name);
   }
   private derivesFromCurated(
      name: string,
      query?: string,
      derivationContext?: string,
   ): boolean {
      return this.host.derivesFromCurated(name, query, derivationContext);
   }
   private notQueryable(
      refusal: string,
      explainable: boolean,
      refused: "model" | "source" | "query",
      name?: string,
   ): NotQueryableError {
      return this.host.notQueryable(refusal, explainable, refused, name);
   }
   private assertQueryBoundaryEarly(
      sourceName?: string,
      queryName?: string,
      query?: string,
      derivationContext?: string,
   ): "cleared" | "deferred" {
      return this.host.assertQueryBoundaryEarly(
         sourceName,
         queryName,
         query,
         derivationContext,
      );
   }
   private ensureSharedQueryModel(
      materializer: ModelMaterializer | undefined,
   ): Promise<void> {
      return this.host.ensureSharedQueryModel(materializer);
   }

   /** Given names (`$NAME`) referenced by any authorize gate reachable in this
    *  model, own or inherited (the walk {@link assertAuthorizedForAllSources}
    *  runs). Computed at construction for {@link filterGivensToModelSurface}. */
   public readonly authorizeReferencedGivenNames: Set<string> = new Set();
   /** Entry-point gates per declared source name — see
    *  {@link computeEntryPointGatesBySource}. The one answer that
    *  `sources[].authorize`, {@link getAuthorize} and the early gate all read. */
   private entryPointGatesBySource: Map<string, GateEntry[]> = new Map();
   /**
    * The runtime that produced this model, retained ONLY so a row-level gate
    * can be enforced by grafting its condition onto a copied `ModelDef` (see
    * `buildGraftedMaterializer`). Passed at construction (`ModelState.gateRuntime`)
    * by `Model.create` and `fromSerialized`; without it `authorizeAndBindRunnable`
    * denies any row-level gate (fail closed).
    *
    * Held explicitly rather than read off `ModelMaterializer`'s `protected
    * runtime`: a Malloy rename of that field would silently turn every
    * row-level gate into a fleet-wide deny.
    */
   private readonly gateRuntime: HydrationRuntime | undefined;
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
    * The gates enforcement evaluated for each runnable
    * {@link authorizeAndBindRunnable} returned (see `EnforcedGates`): what the
    * access record reports, so a record says what was decided rather than
    * what a load-time lookup by name finds.
    */
   private readonly enforcedGatesByRunnable = new WeakMap<
      QueryMaterializer,
      EnforcedGates
   >();

   /**
    * `QueryMaterializer`s that `authorizeAndBindRunnable` returned with a
    * row-level filter attached. Keyed by object identity because `Model` is
    * shared across concurrent requests and a per-instance flag would race.
    * Used to keep a filtered query off the storage-serve tier (which cannot
    * bind the filter's given) and to record `empty_after_filter`.
    */
   private readonly rowLevelFilteredRunnables = new WeakMap<
      QueryMaterializer,
      readonly RowLevelGraftEntry[]
   >();

   /**
    * The advisory pre-compile checks (see {@link OracleGuard}), sequenced
    * over this governor's own gates. Never the authority: {@link govern} is.
    */
   public readonly oracle: OracleGuard;

   constructor(private readonly host: GovernorHost) {
      this.gateRuntime = host.gateRuntime;
      this.oracle = new OracleGuard({
         assertAuthorized: (sourceName, givens, bypassAuthorize, graftScope) =>
            this.assertAuthorized(
               sourceName,
               givens,
               bypassAuthorize,
               graftScope,
            ),
         assertCallerJoinBasesEarly: (
            query,
            givens,
            phase,
            decided,
            includeHiddenFilesAndSources,
         ) =>
            this.assertCallerJoinBasesEarly(
               query,
               givens,
               phase,
               decided,
               includeHiddenFilesAndSources,
            ),
         assertLocksOnAppearingNames: (
            text,
            givens,
            bypassAuthorize,
            decided,
            includeHiddenFilesAndSources,
         ) =>
            this.assertLocksOnAppearingNames(
               text,
               givens,
               bypassAuthorize,
               decided,
               includeHiddenFilesAndSources,
            ),
         assertLocksOnRequestDeclaredBases: (
            entryPoint,
            query,
            givens,
            bypassAuthorize,
         ) =>
            this.assertLocksOnRequestDeclaredBases(
               entryPoint,
               query,
               givens,
               bypassAuthorize,
            ),
         probeEntryPointGates: (runnable, givens, graftScope) =>
            this.probeEntryPointGates(runnable, givens, graftScope),
         sources: () => this.sources,
         queries: () => this.queries,
      });
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
   }

   /**
    * Govern one compiled query, in a fixed sequence, and return the only
    * runnable that may execute plus what was decided:
    *
    *  1. boundary: the compiled query boundary (404) for a deferred
    *     admission, then the boundary for the caller's own joins;
    *  2. locks: every entry-point gate decided ({@link probeEntryPointGates}),
    *     and a request-declared entry point proven not laundered;
    *  3. caller joins: each caller-written join gated as an extra run target;
    *  4. graft + proof: row-level conditions grafted onto one copied model
    *     and proven landed;
    *  5. bind guards: every filter proven to read the field it was written
    *     against;
    *  6. ceiling: the `#(carries)` ceiling (not under the bypass).
    *
    * Steps 2-5 are {@link authorizeAndBindRunnable}, the one minter of a
    * {@link GovernedRunnable}. Any step refuses by throwing. The pre-compile
    * checks ({@link OracleGuard}) are advisory and never replace a step here.
    */
   public async govern(
      request: GovernRequest,
      compiled: CompiledQuery,
   ): Promise<{ runnable: GovernedRunnable; decision: GovernDecision }> {
      const { live } = compiled;
      if (request.boundary) {
         this.host.assertQueryBoundaryCompiled(
            request.boundary.compiledSource,
            request.boundary.query,
         );
         // The LIVE compile keeps the caller's joins; a routed one does not.
         if (request.callerRegion && live) {
            await this.assertCallerJoinsQueryable(live, request.callerRegion);
         }
      }
      if (
         request.routedPastEntryPointGate &&
         !request.bypassAuthorize &&
         !request.graftScope
      ) {
         // Grafting on the author's scope would recompile against the live
         // model and silently answer live under a routed answer record.
         await this.denyRoutedPastGate(compiled.runnable, "no_graft_scope");
      }
      const runnable = await this.authorizeAndBindRunnable(
         compiled.runnable,
         request.givens ?? {},
         {
            recompile: request.recompile,
            proveGraft: request.proveGraft,
            bypassAuthorize: request.bypassAuthorize,
            graftScope: request.graftScope,
            skipOwnSourceGate: request.skipOwnSourceGate,
            compileCallerText: request.compileCallerText,
            callerQueryText: request.callerQueryText,
            checkOnly: request.checkOnly,
            callerJoins:
               request.callerRegion && live
                  ? { runnable: live, region: request.callerRegion }
                  : undefined,
            bypassAlreadyNoted: request.bypassAlreadyNoted,
         },
      );
      if (request.routedPastEntryPointGate && !request.bypassAuthorize) {
         // The routed compile must enforce EXACTLY the gates the live entry
         // point does, by route and expression, own and inherited. One gate
         // found is not the gate set: a routed form that kept the lock but
         // lost the filter would answer rows live refuses. A live set that
         // cannot be read, or is empty, denies too.
         const expected = live
            ? await this.liveEntryPointGateSet(live)
            : undefined;
         const enforced = this.enforcedGatesByRunnable.get(runnable);
         if (!expected || !enforced) {
            await this.denyRoutedPastGate(compiled.runnable, "no_gate_found");
         } else if (!sameGateSet(expected, enforced)) {
            await this.denyRoutedPastGate(
               compiled.runnable,
               "gate_set_differs",
            );
         }
      }
      // The compile that runs: live, or the served copy when it answers.
      await this.assertNoArrayQueryJoin(runnable);
      if (request.ceiling && !request.bypassAuthorize) {
         const source = await request.ceiling.source(runnable);
         for (const target of await this.ceilingTargets(
            request.ceiling,
            compiled,
            runnable,
         )) {
            await this.assertWithinCarriesCeiling(
               request.ceiling.audit,
               target.compile,
               runnable,
               source,
               request.givens,
               request.callerRegion,
               target.servedCopy,
            );
         }
      }
      return {
         runnable,
         decision: {
            enforced: this.enforcedGatesByRunnable.get(runnable),
            grafts: this.rowLevelFilteredRunnables.get(runnable) ?? [],
         },
      };
   }

   /**
    * Deny a routed answer that {@link GovernRequest.routedPastEntryPointGate}
    * could not re-prove; opaque to the caller, like every gate denial.
    */
   private async denyRoutedPastGate(
      routed: QueryMaterializer,
      why: "no_graft_scope" | "no_gate_found" | "gate_set_differs",
   ): Promise<never> {
      const source = await this.resolveAuthorizeSourceFromRunnable(routed);
      recordRowLevelGateDecision("denied_by_gate", "entry_point");
      logger.warn(
         "A routed answer past an entry-point gate could not be re-proven on its own compile; denying",
         { modelPath: this.modelPath, source, why },
      );
      throw AccessDeniedError.forSource(source ?? "unknown");
   }

   /**
    * The gates governing the LIVE compile would evaluate on its entry point:
    * the walk ({@link entryPointGatesInForce}: own, inherited, a derivation's
    * base, a composite's resolved branch) plus this model's on-disk gates for
    * the run target's name, which {@link probeEntryPointGates} folds in the
    * same way. `undefined` (deny) when the walk cannot read the run target or
    * finds no gate, since a routed answer past a gate implies live has one.
    */
   private async liveEntryPointGateSet(
      live: QueryMaterializer,
   ): Promise<EnforcedGates | undefined> {
      const walked = await this.entryPointGatesInForce(live);
      if (!walked) return undefined;
      const name = await this.resolveAuthorizeSourceFromRunnable(live);
      const onDisk = name ? (this.entryPointGatesBySource.get(name) ?? []) : [];
      const gates = enforcedGatesOf([...walked, ...onDisk]);
      return gates.authorize.length + gates.accessFilter.length === 0
         ? undefined
         : gates;
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
         runtime: this.gateRuntime,
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
               runtime: this.gateRuntime,
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
         runtime: this.gateRuntime,
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
   public async resolveNotebookCellGraftScope(
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
   public filterGivensToModelSurface(
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
   public async givenNamesReadByQuery(
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
      const usage = queryGivenUsageIds(prepared);
      if (!usage) {
         for (const name of candidates) names.add(name);
         return names;
      }
      const usedIds = new Set(usage);
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

   /** Memoized {@link hasAnyAuthorizeNote}; `undefined` until first asked. */
   private anyAuthorizeNote: boolean | undefined;

   /**
    * Whether this model carries an `#(authorize)` annotation ANYWHERE, so an
    * ungated deployment with pre-aggregation skips a per-query live compile
    * (see `runQuery`'s routing pre-check).
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
   public hasAnyAuthorizeNote(): boolean {
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
      // mode refusing it first (see `runQuery`); a notebook cell's
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
      // denies here rather than admitting unfiltered. The caller's joins are
      // found in this same runnable (`live`).
      await this.govern(
         {
            givens,
            bypassAuthorize,
            checkOnly: options?.checkOnly,
            compileCallerText: options?.callerRegion?.text,
            callerRegion: options?.callerRegion,
         },
         {
            runnable: runnable as QueryMaterializer,
            live: runnable as QueryMaterializer,
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
      await this.govern(
         { givens, skipOwnSourceGate: true, checkOnly: true },
         { runnable: runnable as QueryMaterializer },
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
   public unboundGateDenial(
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
      return AccessDeniedError.forSource(unbound?.label ?? fallbackLabel);
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
      /** Receives every gate that was evaluated and did not deny. */
      evaluated?: GateEntry[],
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
            evaluated?.push(entry);
            continue;
         }
         denyUnlessAdmitted(resolution, givens, entry.label);
         evaluated?.push(entry);
      }
      return rowLevel;
   }

   /**
    * The entry-point gates the walk finds in force on `runnable`, without
    * evaluating any: what the bypass path skipped, for its access record.
    * `undefined` when the run target cannot be read.
    */
   private async entryPointGatesInForce(
      runnable: QueryMaterializer,
   ): Promise<GateEntry[] | undefined> {
      try {
         const { struct, modelDef, compositeResolvedSourceDef } =
            await this.resolveRunTargetStruct(runnable);
         if (!modelDef) return undefined;
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
                  struct ? ownLevelNotes(struct.annotations) : [],
               ),
            );
         }
         return gates;
      } catch {
         return undefined;
      }
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
   public async queryEntryPointHasRowLevelGate(
      runnable: {
         getPreparedQuery(): Promise<unknown>;
      },
      /** Where caller text sits, so a caller join into a gated source counts. */
      callerRegion?: CallerRegion,
   ): Promise<boolean> {
      return (await this.rowLevelGateVeto(runnable, callerRegion)) !== "none";
   }

   /**
    * {@link queryEntryPointHasRowLevelGate}, saying which gate it found:
    * `entry_point` when only the run target's own chain is gated, so a
    * routed shape that keeps the author's gates (the served copy, R3) can
    * have them grafted and proven by `govern()`; `caller_join` when a caller
    * join reaches a gated source, which `govern()` refuses on any routed
    * compile (`routed_past_caller_join_gate`); `unproven` when the walk could
    * not decide. Only `entry_point` may let a routed shape through.
    */
   public async rowLevelGateVeto(
      runnable: {
         getPreparedQuery(): Promise<unknown>;
      },
      callerRegion?: CallerRegion,
   ): Promise<"none" | "entry_point" | "caller_join" | "unproven"> {
      try {
         const { struct, modelDef, compositeResolvedSourceDef } =
            await this.resolveRunTargetStruct(runnable);
         // "Cannot tell", like the catch below: `resolveRunTargetStruct`
         // swallows a live compile failure as `modelDef: undefined`, while the
         // annotation-free serve shape may still compile and answer from
         // unfiltered rows.
         if (!modelDef) return "unproven";
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
         const entryGated = gates.length > 0;
         if (!callerRegion) return entryGated ? "entry_point" : "none";
         // The serve shape carries no annotation for a caller join's gate
         // either, so the same route-blind question is asked of each one,
         // even when the entry point is gated too: the served copy may answer
         // past an entry-point gate, never past a caller join's.
         const callerJoins = await this.resolvedCallerJoins(
            runnable,
            callerRegion,
         );
         if (!callerJoins) return "unproven";
         const gated = (name: string) =>
            (this.entryPointGatesBySource.get(name)?.length ?? 0) > 0;
         const joinGated = callerJoins.some(
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
         return joinGated ? "caller_join" : entryGated ? "entry_point" : "none";
      } catch {
         // Fail closed: a false positive costs a live query; a false negative
         // serves unfiltered rows with nothing downstream to catch it.
         return "unproven";
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
      /** Receives every gate that was evaluated and admitted. */
      evaluated?: GateEntry[],
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
               evaluated?.push(entry);
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
      throw AccessDeniedError.forSource(entryPoint);
   }

   /** Memo for {@link declaresAnyGate}. */
   private declaresAnyGateMemo?: boolean;

   /**
    * Whether ANY source in this model carries an entry-point gate. Not
    * {@link hasAuthorize} (misses inherited gates), and not
    * `entryPointGatesBySource.size > 0` (every source has a key).
    */
   public declaresAnyGate(): boolean {
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

   /**
    * The compiles the `#(carries)` ceiling is read on, in order. Live (or the
    * governed runnable, when there is no separate live compile) unless the
    * served copy answered. When it did, the served copy's own compile (the
    * routed runnable, before any graft, as the live check reads the live
    * compile before any graft), and live as well whenever live compiles: a
    * live compile that does not compile is a cross-connection model's build
    * view without the join the query goes through, which has nothing to
    * read, and is never what answers. A served-copy claim with no separate
    * routed compile is not one: the live compile is what runs, so it is read.
    */
   private async ceilingTargets(
      ceiling: NonNullable<GovernRequest["ceiling"]>,
      compiled: CompiledQuery,
      governed: GovernedRunnable,
   ): Promise<{ compile: QueryMaterializer; servedCopy: boolean }[]> {
      const { live } = compiled;
      if (
         ceiling.answeredBy !== "served_copy" ||
         !live ||
         compiled.runnable === live
      ) {
         return [{ compile: live ?? governed, servedCopy: false }];
      }
      const served = { compile: compiled.runnable, servedCopy: true };
      let liveCompiles = true;
      try {
         await live.getPreparedQuery();
      } catch {
         liveCompiles = false;
      }
      return liveCompiles
         ? [{ compile: live, servedCopy: false }, served]
         : [served];
   }

   /**
    * Refuse a query that joins a source inside a query on an array, in a
    * model that governs access: no check here can follow what that join
    * reads, so a gate or a `#(carries)` it reaches would pass unseen and the
    * access record would miss it. A model's own such join refuses its load
    * (`Package.formatArrayQueryJoins`), so this is one the caller wrote, in
    * the query or in a source it declares. After the gates, so a caller a
    * gate refuses is refused as one.
    */
   private async assertNoArrayQueryJoin(
      runnable: QueryMaterializer,
   ): Promise<void> {
      if (!governsAccess(this.modelDef)) return;
      let prepared: Awaited<ReturnType<typeof readPreparedQuery>>;
      try {
         prepared = await readPreparedQuery(runnable);
      } catch {
         // No query to read (a compile with no final `run:`): nothing runs.
         return;
      }
      const joins = [
         prepared.query,
         resolveStructRef(prepared.structRef, prepared.modelDef),
         prepared.compositeResolvedSourceDef,
      ].flatMap((root) => arrayQueryJoins(root));
      const refusal = arrayQueryJoinRefusal("This query", joins);
      if (refusal) throw new BadRequestError(refusal);
   }

   /**
    * Refuse a query whose entry carries a `#(carries)` ceiling when it reads a
    * field past it, so a source's list holds for what a caller reads and not
    * only for what the author declared. The entry's ceiling is read off the
    * compiled struct, so a source a caller declares over a carried one keeps
    * it. A `bad_request` record names the reads.
    */
   private async assertWithinCarriesCeiling(
      audit: QueryAudit,
      live: QueryMaterializer,
      bound: QueryMaterializer,
      source: string | undefined,
      givens: Record<string, GivenValue> | undefined,
      region: CallerRegion | undefined,
      /**
       * `live` is the served copy's compile: its own model is the one read
       * (the build view's lacks what it left out), and a ceiling declared
       * only there still counts.
       */
      servedCopy = false,
   ): Promise<void> {
      // Only a model that declares a ceiling pays for reading the query.
      if (!servedCopy && !modelDeclaresCarries(this.modelDef)) return;
      const prepared = await readPreparedQuery(live);
      const modelDef = servedCopy
         ? prepared.modelDef
         : (prepared.modelDef ?? this.modelDef);
      if (
         servedCopy &&
         modelDef &&
         !modelDeclaresCarries(this.modelDef) &&
         !modelDeclaresCarries(modelDef)
      ) {
         return;
      }
      let refusal: string | undefined;
      try {
         const joins = region
            ? ((await this.resolvedCallerJoins(live, region)) ?? []).map(
                 (r) => r.callerJoin.join,
              )
            : [];
         // The served copy's compile without its own model cannot be read
         // against anything: refused rather than read against the build view.
         refusal =
            servedCopy && !modelDef
               ? "this query's source cannot be read completely"
               : ceilingRefusal(modelDef, prepared.query, joins);
      } catch (err) {
         if (!(err instanceof CallerJoinWalkError)) throw err;
         refusal = "this query's joins cannot be read completely";
      }
      if (!refusal) return;
      try {
         audit.summary = await summarizeQueryAccess({
            runnable: live,
            modelDef: this.modelDef,
            enforced: this.enforcedGatesByRunnable.get(bound),
            apiSources: this.sources,
            modelGivens: this.givens,
            source,
            givens,
            bypassAuthorize: false,
            grafts: this.rowLevelFilteredRunnables.get(bound) ?? [],
            rowCount: 0,
         });
      } catch (err) {
         logger.debug("Access summary unavailable", {
            modelPath: this.modelPath,
            error: (err as Error).message,
         });
      }
      throw new BadRequestError(refusal);
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
   ): Promise<Awaited<ReturnType<QueryGovernor["resolveGateShape"]>>[]> {
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
         | Awaited<ReturnType<QueryGovernor["resolvedCallerJoins"]>>
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
            shape: Awaited<ReturnType<QueryGovernor["resolveGateShape"]>>;
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
      throw AccessDeniedError.forSource(alias);
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
      let resolved: Awaited<ReturnType<QueryGovernor["resolvedCallerJoins"]>>;
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
   private async authorizeAndBindRunnable(
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
         /**
          * This request's bypass was already noted by an earlier bind (the
          * degrade-to-live retry re-binds the live runnable): skip the second
          * audit line and counter tick, nothing else.
          */
         bypassAlreadyNoted?: boolean;
      },
   ): Promise<GovernedRunnable> {
      // Bypass skips the gate walk but not the source-name resolution (the
      // audit trail) or the inherited-filter bind check: a misbound `where:` is
      // a wrong answer, not an authorization decision.
      if (options?.bypassAuthorize) {
         if (!options.bypassAlreadyNoted) {
            this.noteAuthorizeBypass(
               "runnable",
               await this.resolveAuthorizeSourceFromRunnable(runnable),
            );
         }
         await this.assertNoMisboundInheritedFilters(
            runnable,
            new Set(),
            "entry_point",
            options?.callerJoins?.region,
         );
         // Nothing was evaluated; the record names the gates the walk found
         // in force and skipped.
         const inForce = await this.entryPointGatesInForce(runnable);
         if (inForce) {
            this.enforcedGatesByRunnable.set(
               runnable,
               enforcedGatesOf(inForce),
            );
         }
         // The explicit bypass: governed by decision, nothing evaluated.
         return mintGovernedRunnable(runnable);
      }

      const graftScope = options?.graftScope ?? this.defaultGraftScope();
      const evaluated: GateEntry[] = [];
      const entryPointRowLevel = await this.probeEntryPointGates(
         runnable,
         givens,
         graftScope,
         {
            skipOwnSourceGate: options?.skipOwnSourceGate,
            callerText: options?.compileCallerText ?? options?.callerQueryText,
         },
         evaluated,
      );
      // Every non-bypass exit: the runnable governance decided, branded.
      const bound = (result: QueryMaterializer): GovernedRunnable => {
         this.enforcedGatesByRunnable.set(result, enforcedGatesOf(evaluated));
         return mintGovernedRunnable(result);
      };

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
               evaluated,
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
      if (rowLevel.length === 0) return bound(runnable);

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
            throw AccessDeniedError.forSource(rowLevel[0].label);
         }
         return bound(runnable);
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
         return bound(recompiled);
      } catch (err) {
         recordRowLevelGateDecision("denied_by_gate", siteOf(rowLevel[0]));
         logger.debug("Row-level authorize attach failed; denying", {
            modelPath: this.modelPath,
            error: err instanceof Error ? err.message : String(err),
         });
         throw AccessDeniedError.forSource(rowLevel[0].label);
      }
   }

   /**
    * Resolve the run-target `SourceDef`/`ModelDef`, the composite member it
    * resolved to, and two signals `./filter_binding_guard` uses to identify a
    * condition parsed from THIS request's own text:
    *
    * - `compiledUrl`: the synthetic `internal://` URL of the request's text,
    *   which locates the caller-written span. `undefined` for a named run,
    *   whose URL is the model file's — a misbind baked into a declared query
    *   must not be taken for caller text by sharing it.
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
         const view = await readPreparedQuery(runnable);
         const modelDef = view.modelDef ?? this.modelDef;
         if (!modelDef)
            return {
               struct: undefined,
               modelDef: undefined,
               compositeResolvedSourceDef: undefined,
               compiledUrl: undefined,
               queryLocation: undefined,
               pipeline: undefined,
            };
         const location = view.location as DocumentLocationLike | undefined;
         return {
            struct: resolveStructRef(view.structRef, modelDef),
            modelDef,
            compositeResolvedSourceDef: view.compositeResolvedSourceDef as
               | SourceDef
               | undefined,
            compiledUrl:
               typeof location?.url === "string" &&
               location.url.startsWith("internal://")
                  ? location.url
                  : undefined,
            queryLocation: location,
            pipeline: view.pipeline as readonly StageLike[] | undefined,
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
      const built = this.buildGraftedMaterializer(grafts, graftScope);
      this.graftedMaterializerCache.set(key, built);
      // Evict after inserting, so the entry just built is never dropped.
      while (
         this.graftedMaterializerCache.size >
         QueryGovernor.GRAFTED_MATERIALIZER_CACHE_MAX
      ) {
         const oldest = this.graftedMaterializerCache.keys().next();
         if (oldest.done) break;
         this.graftedMaterializerCache.delete(oldest.value);
      }
      return built;
   }

   /**
    * Deep-copy `modelDef`, append each grafted condition to its target's
    * `filterList`, and reload the copy through the scope's own runtime
    * ({@link GraftScope.runtime}).
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
      { modelDef, runtime }: GraftScope,
   ): ModelMaterializer {
      if (!runtime) {
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
      return runtime._loadModelFromModelDef(copy);
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
         throw AccessDeniedError.forSource(label);
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
      const prepared = await recompiled.getPreparedQuery();
      const view = preparedQueryView(prepared);
      const modelDef = view.modelDef ?? this.modelDef;
      // MIRROR the compiler's precedence (`compositeResolvedSourceDef ??
      // structRef`), never a union: checking the struct that does not
      // generate SQL would let unfiltered rows pass behind a green proof.
      // A string `structRef` names a `contents` entry; an object one IS it.
      const struct = resolveStructRef(sqlTargetRef(view), modelDef);
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
   public ownScopeQueryDefBinder(
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
            if (
               (await readPreparedQuery(recompiled)).structRef !== graftedStruct
            ) {
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
      if (!struct || depth > QueryGovernor.MAX_GATE_PROOF_DEPTH)
         return undefined;
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
    * Gate ad-hoc `/compile` text before compiling: the advisory
    * {@link OracleGuard.beforeCompileText}. Kept as a method for callers and
    * specs written against it; the compiled backstop
    * ({@link assertAuthorizedForRunnable}) is the authority.
    */
   public async assertAuthorizedForText(
      text: string,
      givens: Record<string, GivenValue>,
      /** `wholeFile`: file or package scope, where the text is the author's file. */
      options?: { wholeFile?: boolean },
   ): Promise<void> {
      await this.oracle.beforeCompileText(text, givens, options);
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
   public async resolveAuthorizeSourceFromRunnable(runnable: {
      getPreparedQuery(): Promise<unknown>;
   }): Promise<string | undefined> {
      try {
         // `as || name`: an empty alias falls through to the name.
         return structRefName((await readPreparedQuery(runnable)).structRef);
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
   public resolveFilterSource(query?: string): string | undefined {
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
   public async assertFilterAnnotationsBindToDeclaringSource(
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
         throw AccessDeniedError.forSource(declaringSourceName);
      }
   }
}
