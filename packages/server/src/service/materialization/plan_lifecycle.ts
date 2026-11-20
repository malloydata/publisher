// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * One package version's build plan over its life: taken from the load worker
 * (or compiled on this thread past its soft deadline), marked stale by a
 * connection edit, re-derived, and waited on by readers that must not see a
 * pre-edit plan. Holds the plan and everything compiled beside it
 * (eligibility, rollups kept past a gate, compiler keys, annotation findings),
 * as one immutable {@link PlanState} replaced on every transition.
 *
 * Effects: logs, and the plan-compute and eligibility metrics.
 */

import type { PackageAnnotationFinding } from "../annotation_reader";
import { components } from "../../api";
import { getPersistCollisionEnforce } from "../../config";
import { errorCodeOf } from "../../error_code";
import { BuildPlanRefreshingError } from "../../errors";
import { logger } from "../../logger";
import {
   recordBuildPlanComputeDuration,
   recordBuildPlanComputeFailed,
} from "../../materialization_metrics";
import type { LoadPackageOutcome } from "../../package_load/package_load_pool";
import { errMessage } from "../../utils";
import type { BuildPlanPackage } from "./plan/build_plan";
import type {
   AsOfBuildSource,
   DatabaseSourceEligibility,
   PackagePlan,
   PersistedViewName,
   RefusalNotices,
   RefusedRollup,
   SourceEligibility,
} from "./domain/package_plan";
import {
   computeLoadPlan,
   recordEligibilityRefusals,
   reportDigestSkipped,
} from "./plan/build_plan_compile";
import {
   incrementalPolicySourcesOf,
   loadGateText,
} from "./plan/load_gate_text";
import type { IncrementalPolicySource } from "./plan/incremental_policy";
import type { MaterializePlan } from "./domain/table_plans";
import type { IncrementalDeclaration } from "./domain/incremental";
import type { BuildPlan } from "./domain/types";
import type { UpstreamSchemaReadings } from "./domain/upstream_schemas";

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
   buildPlanPackage: BuildPlanPackage;
}

/**
 * One derived plan, as the lifecycle holds it: the package plan's parts that
 * serving, the verdicts, the warnings and the load gate read, and the
 * incremental gate's inputs resolved beside them.
 */
interface PlanRecord {
   plan: BuildPlan | null;
   materializePlans: Record<string, MaterializePlan>;
   rollupsKeepingGates: ReadonlySet<string>;
   refusedRollups: Record<string, RefusedRollup>;
   refusalNotices: RefusalNotices;
   compilerKeys: Record<string, string>;
   annotationFindings: PackageAnnotationFinding[];
   /** Undefined: NOT KNOWN, refused like "nothing is eligible". */
   sourceEligibility: SourceEligibility | undefined;
   /** The database analogue, keyed by sourceEntityId. Same rule. */
   databaseSourceEligibility: DatabaseSourceEligibility | undefined;
   incrementalPolicySources: IncrementalPolicySource[];
   incrementalDeclarations: Record<string, IncrementalDeclaration>;
   persistedViews: Record<string, PersistedViewName>;
   /**
    * Every connection a persist source builds on, the skipped and refused
    * sources' included.
    */
   buildConnectionNames: ReadonlySet<string>;
   /** See PackagePlan.asOfBuild. */
   asOfBuild: Readonly<Record<string, AsOfBuildSource>>;
   /** See PackagePlan.upstreamSchemas. */
   upstreamSchemas: UpstreamSchemaReadings;
}

/** A record holding nothing: no plan, both eligibilities unknown. */
const EMPTY_RECORD: PlanRecord = {
   plan: null,
   materializePlans: {},
   rollupsKeepingGates: new Set(),
   refusedRollups: {},
   refusalNotices: {},
   compilerKeys: {},
   annotationFindings: [],
   sourceEligibility: undefined,
   databaseSourceEligibility: undefined,
   incrementalPolicySources: [],
   incrementalDeclarations: {},
   persistedViews: {},
   buildConnectionNames: new Set(),
   asOfBuild: {},
   upstreamSchemas: {},
};

/**
 * The lifecycle's plan, replaced as one value on every transition.
 *
 * The rule for what a failure keeps:
 *  - A failed load keeps nothing: no plan, both eligibilities unknown, which
 *    refuses every stored read until a plan is derived.
 *  - A failed re-derivation keeps the last good record, whole, as `serving`,
 *    its database eligibility less every address a source on an edited
 *    connection builds: that table was built through the connection as it
 *    was. A host's rebind matches against it. Nothing VALIDATES against it
 *    (`current` is null), so no route or run reads the pre-edit plan as the
 *    plan; the warnings and serve verdicts that start from `buildPlan` read
 *    nothing of it either, as stored reads are refused during the outage.
 *    The record is kept whole rather than trimmed to what a rebind reads, so
 *    every transition replaces one value.
 */
interface PlanState {
   /** The plan derived last; null before any load, and after a failure. */
   current: PlanRecord | null;
   /** The last plan derived, kept across failures for {@link serving}. */
   lastGood: PlanRecord | null;
   /** What serving reads: `current`, or the kept record while it is null. */
   serving: PlanRecord | null;
   /** The last derivation failed. */
   failed: boolean;
}

export class PlanLifecycle {
   private state: PlanState = {
      current: null,
      lastGood: null,
      serving: null,
      failed: false,
   };
   /**
    * Connections edited since the plan was last derived, whose database
    * tables a failed re-derivation's serving record leaves out.
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

   /**
    * The persist build plan, computed once at load from the unbound models, so
    * it is stable per (package version, connection config). Null when the
    * package declares no persist source, or the plan could not be derived.
    */
   get buildPlan(): BuildPlan | null {
      return this.state.current?.plan ?? null;
   }
   /** The build plan could not be derived (see {@link load}). */
   get failed(): boolean {
      return this.state.failed;
   }
   /**
    * The plan's generated `#(materialize)` build sources, by sourceID, pushed
    * to each model's served copy with the bindings.
    */
   get materializePlans(): Record<string, MaterializePlan> {
      return this.serving().materializePlans;
   }
   /** Rollups over a gated base whose gates are grafted at read. */
   get rollupsKeepingGates(): ReadonlySet<string> {
      return this.serving().rollupsKeepingGates;
   }
   /** The base and grain of each refused rollup, by its sourceID. */
   get refusedRollups(): Record<string, RefusedRollup> {
      return this.serving().refusedRollups;
   }
   get refusalNotices(): RefusalNotices {
      return this.serving().refusalNotices;
   }
   /** See PackagePlan.compilerKeys. */
   get compilerKeys(): Record<string, string> {
      return this.serving().compilerKeys;
   }
   get annotationFindings(): PackageAnnotationFinding[] {
      return this.serving().annotationFindings;
   }
   /**
    * Which sources may be served from a materialized table, decided at compile
    * where the compiled sources exist. `undefined` means NOT KNOWN (the plan
    * compute failed) and is refused like "nothing is eligible": an unknown
    * answer must not read as consent.
    */
   get sourceEligibility(): SourceEligibility | undefined {
      return this.serving().sourceEligibility;
   }
   /** The database analogue, keyed by sourceEntityId. Same "undefined refuses" rule. */
   get databaseSourceEligibility(): DatabaseSourceEligibility | undefined {
      return this.serving().databaseSourceEligibility;
   }
   /**
    * Incremental-refresh gate inputs, resolved at compile because the rules
    * read the compiled schema, which the wire plan lacks. Empty when the plan
    * compute failed; safe, as the build only dispatches a delta it can read.
    */
   get incrementalPolicySources(): IncrementalPolicySource[] {
      return this.serving().incrementalPolicySources;
   }
   get incrementalDeclarations(): Record<string, IncrementalDeclaration> {
      return this.serving().incrementalDeclarations;
   }
   get persistedViews(): Record<string, PersistedViewName> {
      return this.serving().persistedViews;
   }
   /**
    * The readings of the tables the plan's sources read, as its load read
    * them (PackagePlan.upstreamSchemas): a run reuses them, so it addresses
    * each source as the load did.
    */
   get upstreamSchemas(): UpstreamSchemaReadings {
      return this.serving().upstreamSchemas;
   }
   /** See PackagePlan.asOfBuild. */
   get asOfBuild(): Readonly<Record<string, AsOfBuildSource>> {
      return this.serving().asOfBuild;
   }

   // Test seams, and `Package`'s own setters for a spec that isolates a
   // filter: each replaces the record in force with one changed field.
   set buildPlan(plan: BuildPlan | null) {
      this.patch({ plan });
   }
   set materializePlans(materializePlans: Record<string, MaterializePlan>) {
      this.patch({ materializePlans });
   }
   set buildConnectionNames(buildConnectionNames: ReadonlySet<string>) {
      this.patch({ buildConnectionNames });
   }
   set refusedRollups(refusedRollups: Record<string, RefusedRollup>) {
      this.patch({ refusedRollups });
   }
   set annotationFindings(annotationFindings: PackageAnnotationFinding[]) {
      this.patch({ annotationFindings });
   }
   set sourceEligibility(sourceEligibility: SourceEligibility | undefined) {
      this.patch({ sourceEligibility });
   }
   set databaseSourceEligibility(
      databaseSourceEligibility: DatabaseSourceEligibility | undefined,
   ) {
      this.patch({ databaseSourceEligibility });
   }

   /** The plan bindings are matched against: the current one, else the last good one. */
   servingPlan(): BuildPlan | null {
      return this.state.serving?.plan ?? null;
   }

   /**
    * Apply the persist build plan, derived from the unbound models so it
    * reflects the canonical build. The worker normally supplies it; deriving
    * it here recompiles every model on this thread, so that happens only when
    * the worker left it at its soft deadline. Best-effort: a failure is logged
    * and the package serves without a plan, both eligibilities unknown, which
    * refuses every storage AND database serve binding until a load succeeds.
    *
    * Returns how long it took, or undefined when it failed.
    */
   async load(
      workerPlan: LoadPackageOutcome["buildPlan"],
   ): Promise<number | undefined> {
      const start = performance.now();
      const record = await this.derive(workerPlan);
      this.state = record
         ? { current: record, lastGood: record, serving: record, failed: false }
         : { current: null, lastGood: null, serving: null, failed: true };
      return record ? performance.now() - start : undefined;
   }

   /**
    * The record of `workerPlan` (deriving it on this thread when the worker
    * left it), or undefined when it failed. Reports the derivation's
    * refusals, skipped digests and duration, or its failure.
    */
   private async derive(
      workerPlan: LoadPackageOutcome["buildPlan"],
   ): Promise<PlanRecord | undefined> {
      const start = performance.now();
      try {
         // A derivation's refusals and skipped digests are reported once,
         // here, whichever thread derived it, a failed one's included.
         const derived = workerPlan
            ? workerPlan
            : {
                 ok: true as const,
                 ...(await computeLoadPlan(this.owner.buildPlanPackage)),
              };
         recordEligibilityRefusals(
            derived.ok
               ? derived.packagePlan.eligibilityRefused
               : derived.eligibilityRefused,
         );
         reportDigestSkipped(derived.digestSkipped);
         if (!derived.ok) throw new Error(derived.error);
         const record = this.recordOf(derived.packagePlan);
         recordBuildPlanComputeDuration(
            workerPlan ? workerPlan.durationMs : performance.now() - start,
            workerPlan ? "worker" : "main_thread",
         );
         return record;
      } catch (err) {
         recordBuildPlanComputeFailed();
         logger.warn(
            `Failed to compute build plan for package ${this.owner.packageName()}`,
            {
               packageName: this.owner.packageName(),
               code: errorCodeOf(err),
            },
         );
         return undefined;
      }
   }

   /** The record a derived package plan leaves in force. */
   private recordOf(
      outcome: PackagePlan<PackageAnnotationFinding>,
   ): PlanRecord {
      return {
         plan: outcome.plan,
         materializePlans: outcome.materializePlans,
         rollupsKeepingGates: new Set(outcome.rollupsKeepingGates),
         refusedRollups: outcome.refusedRollups,
         refusalNotices: outcome.refusalNotices,
         compilerKeys: outcome.compilerKeys,
         annotationFindings: outcome.annotationFindings,
         sourceEligibility: outcome.sourceEligibility,
         databaseSourceEligibility: outcome.databaseSourceEligibility,
         // A persisted view's plan entry is its generated build source; its
         // findings name the author's view and the source it is declared on.
         incrementalPolicySources: incrementalPolicySourcesOf(
            outcome.plan,
            outcome.incrementalDeclarations,
            outcome.materializePlans,
            outcome.persistedViews,
         ),
         incrementalDeclarations: outcome.incrementalDeclarations,
         persistedViews: outcome.persistedViews,
         buildConnectionNames: new Set(outcome.buildConnectionNames),
         asOfBuild: outcome.asOfBuild ?? {},
         upstreamSchemas: outcome.upstreamSchemas ?? {},
      };
   }

   /** What serving reads now: the record in force, or none. */
   private serving(): PlanRecord {
      return this.state.serving ?? EMPTY_RECORD;
   }

   /** Replace the record in force with `change` applied. */
   private patch(change: Partial<PlanRecord>): void {
      const record = {
         ...(this.state.current ?? this.state.serving ?? EMPTY_RECORD),
         ...change,
      };
      this.state = {
         ...this.state,
         current: record,
         lastGood: record,
         serving: record,
      };
   }

   /**
    * Mark the plan stale after a connection edit. Every content address folds
    * in its connection's digest, and the digest reads connection config, so
    * an edit can move the address of every source on it. Until
    * {@link refresh} re-derives it, {@link whenCurrent} holds readers back:
    * the pre-edit plan would hand out old addresses that a run's own compile
    * refuses as "measured under a different definition".
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
    * edit. Resolves true when the plan was derived, false when it failed;
    * a failure keeps the last good record for serving ({@link PlanState}).
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
         const retrying = retryFailed && this.state.failed;
         if (!this.stale && !retrying) return !this.state.failed;
         const seq = this.staleSeq;
         let workerPlan: LoadPackageOutcome["buildPlan"];
         let poolFailed = false;
         try {
            workerPlan = await derive();
            // The files on disk now carry an invalid annotation, or the
            // package serves an older model than its files: that plan is not
            // this package's, so the re-derivation fails and no run builds it.
            const invalid = workerPlan?.ok
               ? loadGateText(workerPlan.packagePlan, {
                    enforceCollisions: getPersistCollisionEnforce(),
                    versionScoped: this.versionOwnsTables(),
                 })
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
         const record = await this.derive(workerPlan);
         this.refreshFailedTransiently = !record && poolFailed;
         if (record) {
            this.connectionsChangedSinceDerived.clear();
            this.state = {
               current: record,
               lastGood: record,
               serving: record,
               failed: false,
            };
         } else {
            const { lastGood } = this.state;
            this.state = {
               current: null,
               lastGood,
               serving: lastGood && {
                  ...lastGood,
                  databaseSourceEligibility: withoutChangedConnections(
                     lastGood.databaseSourceEligibility,
                     lastGood.plan,
                     this.connectionsChangedSinceDerived,
                  ),
               },
               failed: true,
            };
         }
         if (seq === this.staleSeq && this.stale) {
            this.stale.resolve();
            this.stale = undefined;
         }
         this.onRefreshed();
         return record !== undefined;
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
      if (this.state.failed) return null;
      const current = this.state.current ?? EMPTY_RECORD;
      return new Set([
         ...Object.values(current.plan?.sources ?? {}).map(
            (source) => source.connectionName,
         ),
         // Sources the plan skipped (their connection did not resolve) or
         // refused: creating or fixing that connection must re-derive.
         ...current.buildConnectionNames,
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
 * Database eligibility from the last good plan, less every address a source on
 * a changed connection builds: those tables were built through the connection
 * as it was, and are read through it as it is now. Refused with a reason, so
 * the bind's log says why they serve live until the plan is re-derived.
 */
function withoutChangedConnections(
   eligibility: DatabaseSourceEligibility | undefined,
   plan: BuildPlan | null,
   changed: ReadonlySet<string>,
): DatabaseSourceEligibility | undefined {
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
