// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * One package version's build plan over its life: taken from the load worker
 * (or compiled on this thread past its soft deadline), marked stale by a
 * connection edit, re-derived, and waited on by readers that must not see a
 * pre-edit plan. Holds the plan and everything compiled beside it
 * (eligibility, rollups kept past a gate, compiler keys, annotation findings).
 *
 * Effects: logs, and the plan-compute and eligibility metrics.
 */

import type { PackageAnnotationFinding } from "../annotation_reader";
import { components } from "../../api";
import { getPersistCollisionEnforce } from "../../config";
import { BuildPlanRefreshingError } from "../../errors";
import { logger } from "../../logger";
import {
   recordBuildPlanComputeDuration,
   recordBuildPlanComputeFailed,
   recordConnectionDigestSkipped,
   recordEligibilityRefused,
   type EligibilityRefusalReason,
} from "../../materialization_metrics";
import type { LoadPackageOutcome } from "../../package_load/package_load_pool";
import { errMessage } from "../../utils";
import type { Model } from "../model";
import {
   projectRefusals,
   type BuildPlanPackage,
   type ColocatedSourceEligibility,
   type RefusalNotices,
   type RefusedRollup,
   type SourceEligibility,
} from "./plan/build_plan";
import { computePackageBuildPlan } from "./plan/build_plan_compile";
import {
   incrementalPolicySourcesOf,
   loadGateText,
   relaxedCollision,
} from "./plan/load_gate_text";
import type { IncrementalPolicySource } from "./plan/incremental_policy";
import type { MaterializePlan } from "./domain/table_plans";
import type { BuildPlan } from "./domain/types";

type ApiPackage = components["schemas"]["Package"];

/**
 * How long a reader of the build plan waits for a re-derivation after a
 * connection change before it answers 503 with Retry-After instead. Long
 * enough for an ordinary package's recompile, short enough that a caller's
 * own timeout never fires first.
 */
const BUILD_PLAN_REFRESH_WAIT_MS = 10_000;

/** What a {@link PlanLifecycle} reads of the package that holds it. */
interface PlanLifecycleOwner {
   packageName(): string;
   versionId(): string | undefined;
   metadata(): ApiPackage;
   models(): Map<string, Model>;
   buildPlanPackage: BuildPlanPackage;
}

export class PlanLifecycle {
   /**
    * The persist build plan, computed once at load from the unbound models, so
    * it is stable per (package version, connection config). Null when the
    * package declares no persist source, or the plan could not be derived.
    */
   buildPlan: BuildPlan | null = null;
   /**
    * The plan's generated `#(materialize)` build sources, by sourceID, pushed
    * to each model's served copy with the bindings. Kept across a failed
    * re-derivation, like the plan's last good bindings.
    */
   materializePlans: Record<string, MaterializePlan> = {};
   /** The build plan could not be derived (see {@link load}). */
   failed = false;
   /** Rollups over a gated base whose gates are grafted at read (row 1). */
   rollupsKeepingGates = new Set<string>();
   /** The base and grain of each refused rollup, by its sourceID. */
   refusedRollups: Record<string, RefusedRollup> = {};
   refusalNotices: RefusalNotices = {};
   /** See BuildPlanOutcome.compilerKeys. */
   compilerKeys: Record<string, string> = {};
   annotationFindings: PackageAnnotationFinding[] = [];
   /**
    * Which sources may be served from a materialized table, decided at compile
    * where the compiled sources exist. `undefined` means NOT KNOWN (the plan
    * compute failed) and is refused like "nothing is eligible": an unknown
    * answer must not read as consent.
    */
   sourceEligibility: SourceEligibility | undefined = undefined;
   /** The colocated analogue, keyed by sourceEntityId. Same "undefined refuses" rule. */
   colocatedSourceEligibility: ColocatedSourceEligibility | undefined =
      undefined;
   /**
    * Incremental-refresh gate inputs, resolved at compile because the rules
    * read the compiled schema, which the wire plan lacks. Empty when the plan
    * compute failed; safe, as the build only dispatches a delta it can read.
    */
   incrementalPolicySources: IncrementalPolicySource[] = [];
   /**
    * Every connection a persist source builds on, from the last derived plan
    * (including skipped and refused sources), so a connection edit that
    * creates or fixes one re-derives the plan.
    */
   private buildConnectionNames: ReadonlySet<string> = new Set();
   /**
    * The last good plan, kept only for binding while a failed re-derivation
    * leaves {@link buildPlan} null: a host's rebind then matches its entries
    * as it did before the edit. Never handed out as the plan, so no route or
    * run validates against it. Cleared when a plan is derived.
    */
   private servingPlanWhileUnavailable: BuildPlan | null = null;
   /**
    * Connections edited since the plan was last derived. A failed
    * re-derivation keeps colocated eligibility only for sources on other
    * connections: a colocated table lives in, and is read through, its
    * connection, so one the edit changed may now be a different warehouse.
    */
   private connectionsChangedSinceDerived = new Set<string>();
   /**
    * Whether the last failed refresh failed in the load pool (a worker timeout
    * or crash), which a retry can clear, rather than in the plan compute.
    */
   private refreshFailedTransiently = false;
   /**
    * Set when a connection edit makes the plan stale, and resolved once a
    * re-derivation started after the latest edit has finished.
    */
   private stale: { promise: Promise<void>; resolve: () => void } | undefined;
   /** Bumped by each {@link markStale}, so a refresh knows if it is the latest. */
   private staleSeq = 0;
   /** The re-derivation in flight, so refreshes run one at a time. */
   private refreshChain: Promise<boolean> = Promise.resolve(true);

   constructor(
      private readonly owner: PlanLifecycleOwner,
      /** Called after every refresh, derived or not: the plan decides what serves. */
      private readonly onRefreshed: () => void,
   ) {}

   /** The plan bindings are matched against: the current one, else the last good one. */
   servingPlan(): BuildPlan | null {
      return this.buildPlan ?? this.servingPlanWhileUnavailable;
   }

   /**
    * Apply the persist build plan, derived from the unbound models so it
    * reflects the canonical build. The worker normally supplies it; deriving
    * it here recompiles every model on this thread, so that happens only when
    * the worker left it at its soft deadline. Best-effort: a failure is logged
    * and the package serves without a plan, both eligibilities unknown, which
    * refuses every storage AND colocated serve binding until a load succeeds.
    *
    * Returns how long it took, or undefined when it failed.
    */
   async load(
      workerPlan: LoadPackageOutcome["buildPlan"],
   ): Promise<number | undefined> {
      const start = performance.now();
      try {
         // Before a failed plan throws, matching the main-thread derivation,
         // which counts these as it goes.
         for (const [reason, count] of Object.entries(
            workerPlan?.eligibilityRefused ?? {},
         )) {
            for (let i = 0; i < (count ?? 0); i++) {
               recordEligibilityRefused(reason as EligibilityRefusalReason);
            }
         }
         for (const connectionName of workerPlan?.digestSkipped ?? []) {
            recordConnectionDigestSkipped();
            logger.warn(
               "Skipping connection digest; connection did not resolve",
               { connectionName },
            );
         }
         if (workerPlan && !workerPlan.ok) {
            throw new Error(workerPlan.error);
         }
         const outcome = workerPlan
            ? workerPlan.outcome
            : await computePackageBuildPlan(this.owner.buildPlanPackage);
         // A collision this deployment refuses alone, rather than failing
         // the load, is a refusal record like any other.
         const relaxed = projectRefusals(
            outcome.plan,
            outcome.annotationFindings.map((f) =>
               relaxedCollision(f, getPersistCollisionEnforce()),
            ),
            outcome.refusalNotices,
         );
         this.buildPlan = relaxed.plan;
         this.materializePlans = outcome.materializePlans;
         this.rollupsKeepingGates = new Set(outcome.rollupsKeepingGates);
         this.refusedRollups = outcome.refusedRollups;
         this.refusalNotices = relaxed.refusalNotices;
         this.compilerKeys = outcome.compilerKeys;
         this.failed = false;
         this.servingPlanWhileUnavailable = null;
         this.annotationFindings = relaxed.annotationFindings;
         this.sourceEligibility = outcome.sourceEligibility;
         this.colocatedSourceEligibility = outcome.colocatedSourceEligibility;
         this.buildConnectionNames = new Set(outcome.buildConnectionNames);
         // A persisted view's plan entry is its generated build source; its
         // findings name the author's view and the source it is declared on.
         const persistedViews = new Map<
            string,
            { sourceName: string; viewName: string }
         >();
         for (const model of this.owner.models().values()) {
            for (const v of model.persistedViewBuilds() ?? []) {
               // A persisted query takes no incremental key.
               if (v.queryName !== undefined) continue;
               persistedViews.set(v.buildSourceID, {
                  sourceName: v.sourceName,
                  viewName: v.viewName,
               });
            }
         }
         this.incrementalPolicySources = incrementalPolicySourcesOf(
            outcome.plan,
            outcome.incrementalDeclarations,
            outcome.materializePlans,
            persistedViews,
         );
         recordBuildPlanComputeDuration(
            workerPlan ? workerPlan.durationMs : performance.now() - start,
            workerPlan ? "worker" : "main_thread",
         );
         return performance.now() - start;
      } catch (err) {
         this.failed = true;
         // Nothing from the previous load describes this one: a failed
         // re-derivation after a connection edit must not leave the pre-edit
         // plan and eligibility in force, or a run would validate against
         // addresses the build does not compute
         // (build_plan_connection_refresh.spec.ts).
         this.buildPlan = null;
         this.sourceEligibility = undefined;
         this.colocatedSourceEligibility = undefined;
         this.incrementalPolicySources = [];
         this.rollupsKeepingGates = new Set();
         this.refusedRollups = {};
         this.refusalNotices = {};
         this.compilerKeys = {};
         recordBuildPlanComputeFailed();
         logger.warn(
            `Failed to compute build plan for package ${this.owner.packageName()}`,
            {
               packageName: this.owner.packageName(),
               error: errMessage(err),
            },
         );
      }
      return undefined;
   }

   /**
    * Mark the plan stale after a connection edit. Every content address folds
    * in its connection's digest, and the digest reads connection config, so
    * an edit can move the address of every source on it. Until
    * {@link refresh} re-derives it, {@link whenCurrent} holds readers back:
    * the pre-edit plan would hand out old addresses that a run's own compile
    * refuses as "measured under a different definition"
    * (build_plan_connection_refresh.spec.ts).
    */
   markStale(changedConnections: Iterable<string> = []): void {
      for (const name of changedConnections) {
         this.connectionsChangedSinceDerived.add(name);
      }
      this.staleSeq++;
      if (!this.stale) {
         let resolve!: () => void;
         const promise = new Promise<void>((r) => (resolve = r));
         this.stale = { promise, resolve };
      }
   }

   lastRefreshFailedTransiently(): boolean {
      return this.refreshFailedTransiently;
   }

   isStale(): boolean {
      return this.stale !== undefined;
   }

   /**
    * Re-derive the plan if it is stale, with `derive` supplying the outcome
    * (the load worker's, so the recompile stays off this thread; undefined
    * falls back to deriving here). Serialized; a refresh that finds the plan
    * current does nothing, so a burst of edits costs one re-derivation. The
    * waiters are released only by a refresh that started after the latest
    * edit. Resolves true when the plan was derived, false when it failed.
    */
   refresh(
      derive: () => Promise<LoadPackageOutcome["buildPlan"]>,
      {
         retryFailed = false,
         isStale,
      }: {
         retryFailed?: boolean;
         /** Whether the package serves an older model than its files. */
         isStale?: () => boolean;
      } = {},
   ): Promise<boolean> {
      const run = this.refreshChain.then(async () => {
         // A retry of a failed re-derivation runs while the plan is
         // unavailable but not stale (readers get 409, not a wait).
         const retrying = retryFailed && this.failed;
         if (!this.stale && !retrying) return !this.failed;
         const seq = this.staleSeq;
         // A failed re-derivation keeps what serving reads from the last good
         // plan: storage eligibility is keyed by name and does not move with
         // connection config, and colocated eligibility only admits addresses
         // that plan computed. Only run creation is refused (409) until a
         // retry succeeds.
         const lastGoodPlan = this.servingPlan();
         const lastGood = {
            sourceEligibility: this.sourceEligibility,
            colocatedSourceEligibility: this.colocatedSourceEligibility,
            incrementalPolicySources: this.incrementalPolicySources,
            buildConnectionNames: this.buildConnectionNames,
            compilerKeys: this.compilerKeys,
         };
         let workerPlan: LoadPackageOutcome["buildPlan"];
         let poolFailed = false;
         try {
            workerPlan = await derive();
            // The files on disk now carry an invalid annotation, or the
            // package serves an older model than its files: that plan is not
            // this package's, so the re-derivation fails and no run builds it.
            const invalid = workerPlan?.ok
               ? loadGateText(
                    {
                       plan: workerPlan.outcome.plan,
                       annotationFindings:
                          workerPlan.outcome.annotationFindings,
                       incrementalDeclarations:
                          workerPlan.outcome.incrementalDeclarations,
                       materializePlans: workerPlan.outcome.materializePlans,
                       modelGateLines: workerPlan.outcome.modelGateLines,
                    },
                    {
                       enforceCollisions: getPersistCollisionEnforce(),
                       versionScoped: this.versionOwnsTables(),
                    },
                 )
               : "";
            const stale = isStale?.() ?? false;
            if (invalid || stale) {
               workerPlan = failedPlan(
                  invalid
                     ? "The package's files carry an invalid materialization " +
                          `annotation, which its load refuses:\n${invalid}`
                     : "The package's files changed and its last reload " +
                          "refused them, so it serves an older model; reload " +
                          "it once they are fixed.",
               );
            }
         } catch (err) {
            poolFailed = true;
            workerPlan = failedPlan(errMessage(err));
         }
         const derived = (await this.load(workerPlan)) !== undefined;
         this.refreshFailedTransiently = !derived && poolFailed;
         if (derived) {
            this.connectionsChangedSinceDerived.clear();
         } else if (lastGood.sourceEligibility !== undefined) {
            Object.assign(this, lastGood);
            this.colocatedSourceEligibility = withoutChangedConnections(
               lastGood.colocatedSourceEligibility,
               lastGoodPlan,
               this.connectionsChangedSinceDerived,
            );
            this.servingPlanWhileUnavailable = lastGoodPlan;
         }
         if (seq === this.staleSeq && this.stale) {
            this.stale.resolve();
            this.stale = undefined;
         }
         this.onRefreshed();
         return derived;
      });
      this.refreshChain = run.catch(() => false);
      return run;
   }

   /**
    * Resolves once the plan reflects the current connections. Past
    * `timeoutMs` it rejects with BuildPlanRefreshingError, a 503 with
    * Retry-After, rather than hold the caller's request.
    */
   async whenCurrent(
      timeoutMs: number = BUILD_PLAN_REFRESH_WAIT_MS,
   ): Promise<void> {
      const stale = this.stale;
      if (!stale) return;
      let timer: ReturnType<typeof setTimeout> | undefined;
      try {
         await Promise.race([
            stale.promise,
            new Promise<never>((_, reject) => {
               timer = setTimeout(
                  () =>
                     reject(
                        new BuildPlanRefreshingError(
                           `The build plan of package ${this.owner.packageName()} is being re-derived after a connection change. Retry shortly.`,
                        ),
                     ),
                  timeoutMs,
               );
            }),
         ]);
      } finally {
         if (timer) clearTimeout(timer);
      }
   }

   /**
    * The connections this package's persist sources build on, so a
    * connection edit re-derives only the packages it can move. Null when the
    * plan is unavailable (unknown, so any edit re-derives it).
    */
   connectionNames(): ReadonlySet<string> | null {
      if (this.failed) return null;
      return new Set([
         ...Object.values(this.buildPlan?.sources ?? {}).map(
            (source) => source.connectionName,
         ),
         // Sources the plan skipped (their connection did not resolve) or
         // refused: creating or fixing that connection must re-derive.
         ...this.buildConnectionNames,
      ]);
   }

   /** Whether this package's tables are named per version. */
   versionOwnsTables(): boolean {
      return (
         this.owner.metadata().scope === "version" &&
         this.owner.versionId() !== undefined
      );
   }
}

function failedPlan(error: string): LoadPackageOutcome["buildPlan"] {
   return {
      ok: false,
      error,
      durationMs: 0,
      digestSkipped: [],
      eligibilityRefused: {},
   };
}

/**
 * Colocated eligibility from the last good plan, less every address a source on
 * a changed connection builds: those tables were built through the connection
 * as it was, and are read through it as it is now. Refused with a reason, so
 * the bind's log says why they serve live until the plan is re-derived.
 */
function withoutChangedConnections(
   eligibility: ColocatedSourceEligibility | undefined,
   plan: BuildPlan | null,
   changed: ReadonlySet<string>,
): ColocatedSourceEligibility | undefined {
   if (!eligibility || changed.size === 0) return eligibility;
   const dropped = new Set<string>();
   for (const source of Object.values(plan?.sources ?? {})) {
      if (changed.has(source.connectionName))
         dropped.add(source.sourceEntityId);
   }
   if (dropped.size === 0) return eligibility;
   const refused = { ...eligibility.refused };
   for (const address of dropped) {
      if (eligibility.eligibleEntityIds.has(address)) {
         refused[address] =
            "its connection changed and the build plan could not be re-derived yet";
      }
   }
   return {
      eligibleEntityIds: new Set(
         [...eligibility.eligibleEntityIds].filter((a) => !dropped.has(a)),
      ),
      refused,
   };
}
