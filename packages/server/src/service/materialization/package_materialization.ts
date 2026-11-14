// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   getDialect,
   isSourceDef,
   type MalloyConfig,
   type SourceDef,
} from "@malloydata/malloy";
import {
   findingSeverity,
   MATERIALIZE_V2_ROUTES,
   type PackageAnnotationFinding,
} from "../annotation_reader";
import { components } from "../../api";
import {
   getMaterializeGatedSources,
   getPersistCollisionEnforce,
   getMaterializeEnabled,
   getPersistStorageMode,
} from "../../config";
import { PACKAGE_MANIFEST_NAME } from "../../constants";
import { BuildPlanRefreshingError } from "../../errors";
import { logger } from "../../logger";
import {
   recordBuildPlanComputeDuration,
   recordBuildPlanComputeFailed,
   recordColocatedBindDropped,
   recordConnectionDigestSkipped,
   recordEligibilityRefused,
   type EligibilityRefusalReason,
} from "../../materialization_metrics";
import type { LoadPackageOutcome } from "../../package_load/package_load_pool";
import { errMessage } from "../../utils";
import type { Model } from "../model";
import type { PackageModelPolicy } from "../model_policy";
import type {
   BuildPlanPackage,
   ColocatedSourceEligibility,
   SourceEligibility,
} from "./plan/build_plan";
import { computePackageBuildPlan } from "./plan/build_plan_compile";
import type { MaterializePlan } from "./plan/build_file_generator";
import {
   incrementalPolicyAdvisories,
   incrementalPolicyRejections,
   type IncrementalPolicySource,
} from "./plan/incremental_policy";
import type {
   PreaggregateViolation,
   PreaggregateViolationCode,
} from "./plan/preaggregation_validation";
import { CronEvaluator } from "./run/cron_evaluator";
import {
   evaluateManifestFreshness,
   filterFreshManifest,
} from "./run/freshness";
import {
   type BoundTable,
   type LiveTableVerdict,
   liveVerdictKey,
   type MaterializedTableCatalog,
   type PlannedTable,
} from "./serve/query_path";
import {
   deriveServeBindings,
   groupAliasesByName,
   planSourceIds,
   type ServeBinding,
} from "./serve/materialization_serve_transform";
import {
   SERVED_COPY_WARNING_CODE,
   type ServedCopyProblem,
} from "./serve/served_copy_serve";
import {
   deriveColocatedServeBindings,
   v1StoredColumnsOf,
} from "./serve/colocated_bindings";
import { namesServedCopyAlias } from "./serve/served_copy_config";
import { STORED_COLUMN_TYPE_MISMATCH_CODE } from "./stored_column_types";
import {
   computeServeVerdicts,
   VERDICT_WARNING_CODES,
   type ServeVerdict,
   type ViewServeVerdict,
} from "./serve/serve_verdict";
import { storageTablePath } from "./run/storage_table_path";
import { persistTableTarget } from "./plan/eligibility_collect";
import { quoteManifestTablePath } from "../quoting";
import { structsReachedFrom } from "../governance/ir/source_graph";
import { CLASSIFICATION_COVERAGE } from "../classification";
import { reachableSourceIDs, SourceMap } from "./plan/source_map";
import type {
   BuildManifest,
   BuildPlan,
   FreshnessManifest,
   ManifestEntry,
} from "./types";

type ApiPackage = components["schemas"]["Package"];
type ApiPackageWarning = NonNullable<ApiPackage["warnings"]>[number];

/**
 * `#@ preaggregate` violations that only warn: refusing them would stop
 * already-published packages loading (see formatInvalidPreaggregatePolicy).
 */
const PREAGG_WARN_ONLY: ReadonlySet<PreaggregateViolationCode> = new Set([
   "non_public_measure",
   "grain_dimension_not_public",
]);

/**
 * Project wire entries to the `sourceEntityId -> { tableName }` map that
 * hydrates models. Freshness fields gate the serve path per query, not hydration.
 */
export function toTableNameManifest(
   entries: FreshnessManifest,
): BuildManifest["entries"] {
   const out: BuildManifest["entries"] = {};
   for (const [sourceEntityId, entry] of Object.entries(entries)) {
      out[sourceEntityId] = { tableName: entry.tableName };
   }
   return out;
}

/**
 * What the environment answers about where a package's tables can be written
 * (P13), read live. `undefined` from either means "not known", which never
 * warns.
 */
export interface DeploymentFacts {
   /** The configured storage destinations, or undefined when the list is not authoritative. */
   destinationNames(): readonly string[] | undefined;
   /** Whether a connection's configured type can create a table. */
   connectionCanHoldTable(connectionName: string): boolean | undefined;
}

/** What a {@link PackageMaterialization} reads of the package that holds it. */
export interface PackageMaterializationOwner {
   packageName(): string;
   versionId(): string | undefined;
   metadata(): ApiPackage;
   /** The models in service now; replaced wholesale on a reload. */
   models(): Map<string, Model>;
   /** The policy those models were built with; replaced with them. */
   modelPolicy(): PackageModelPolicy;
   packagePath(): string;
   malloyConfig(): MalloyConfig;
   /** The package, for a main-thread build-plan compute. */
   buildPlanPackage: BuildPlanPackage;
   /**
    * The sources callers may query, by the model that publishes them, or
    * undefined when every source is queryable. Optional: a spec's owner need
    * not curate.
    */
   publishedSurface?():
      | { modelPath: string; names: string[]; queries?: string[] }[]
      | undefined;
}

/**
 * How long a reader of the build plan waits for a re-derivation after a
 * connection change before it answers 503 with Retry-After instead. Long
 * enough for an ordinary package's recompile, short enough that a caller's
 * own timeout never fires first.
 */
export const BUILD_PLAN_REFRESH_WAIT_MS = 10_000;

/**
 * The code on the warning for a `#@ persist` source a compile-time gate
 * refused (a row-level gate, a free parameter, a given in the persisted query,
 * or a `storage=` source the storage gate refuses). Not counted as an
 * annotation finding: the annotation is well formed.
 */
export const PERSIST_SOURCE_REFUSED_CODE = "persist-source-refused";

/**
 * The code on the warning for a rollup whose build found its join_one key
 * repeated ({@link PackageMaterialization.repeatedJoinOneKeyWarnings}).
 */
export const PREAGGREGATE_JOIN_ONE_KEY_REPEATED_CODE =
   "preaggregate-join-one-key-repeated";

/** The live serve verdicts the path report reads, by table and by source. */
interface LiveVerdicts {
   byTable: ReadonlyMap<string, LiveTableVerdict>;
   bySource: ReadonlyMap<string, LiveTableVerdict>;
}

/**
 * A package's materialization state: its build plan and which sources may be
 * served from a materialized table, the manifest it is bound to (both tiers),
 * the serve routing it pushes onto its models, and the persistence-policy
 * findings a publish and a load judge it by. Held by `Package`, which
 * delegates to it only what callers outside this module use.
 */
export class PackageMaterialization {
   /** The environment's storage destinations for a serve-shape compile; see {@link setServeDestinationConfig}. */
   private serveDestinationConfig?: () => MalloyConfig;
   /** The environment's deployment facts, read live; see {@link setDeploymentFacts}. */
   private deploymentFacts?: DeploymentFacts;
   /** A destination's schema by name, resolved live; see {@link setServeDestinationSchema}. */
   private serveDestinationSchema?: (
      destinationName: string,
   ) => string | undefined;
   // Bound build manifest (sourceEntityId -> { tableName }): routes persist
   // sources to their materialized tables for served queries and the /compile
   // preview alike, so previewed SQL matches executed SQL. Surfaced on /status
   // so the control plane can confirm a worker bound it.
   private buildManifestEntries: BuildManifest["entries"] | undefined;
   // The full wire entries, freshness fields included, so the serve path can
   // re-evaluate age vs window per query. Undefined when serving live.
   private freshnessEntries: FreshnessManifest | undefined;
   // `storage=` serve bindings, pushed onto each Model for the virtual-source
   // serve transform. A storage source serves cross-connection through these,
   // not through the same-connection manifest above.
   private storageServeBindings: ReturnType<typeof deriveServeBindings> = [];
   // Memoized freshness-filtered manifest, recomputed when an entry crosses its
   // window (`validUntil`) or a rebind clears it. Infinity: stable until rebind.
   private freshManifestCache:
      | { manifest: BuildManifest["entries"]; validUntil: number }
      | undefined;
   private manifestBindingStatus: "unbound" | "bound" | "live_fallback" =
      "unbound";
   private manifestEntryCount = 0;
   private boundManifestUri: string | null = null;
   // The persist build plan, computed once at load from the unbound models, so
   // it is stable per (package version, connection config). Null when the
   // package declares no persist source.
   buildPlan: BuildPlan | null = null;
   /**
    * The plan's generated `#(materialize)` build sources, by sourceID, pushed
    * to each model's served copy with the bindings. Kept across a failed
    * re-derivation, like the plan's last good bindings.
    */
   materializePlans: Record<string, MaterializePlan> = {};
   /** The build plan could not be derived at load (see applyLoadBuildPlan). */
   private buildPlanFailed = false;
   /**
    * Set when a connection edit makes the plan stale, and resolved once a
    * re-derivation started after the latest edit has finished (see
    * refreshBuildPlan). Readers that must not see the pre-edit plan wait on it.
    */
   private buildPlanStale:
      | { promise: Promise<void>; resolve: () => void }
      | undefined;
   /** Bumped by each markBuildPlanStale, so a refresh knows if it is the latest. */
   private buildPlanStaleSeq = 0;
   /** The re-derivation in flight, so refreshes run one at a time. */
   private buildPlanRefreshChain: Promise<boolean> = Promise.resolve(true);
   // Keyed on the plan it came from, so a reload recomputes it.
   private preaggregateEntityIdCache:
      | { plan: BuildPlan | null; ids: ReadonlySet<string> }
      | undefined;
   // Keyed on the plan, the bound entries and the live verdicts it came from.
   private tableCatalogCache:
      | {
           plan: BuildPlan | null;
           bound: FreshnessManifest | undefined;
           liveVerdicts: LiveVerdicts | undefined;
           catalog: MaterializedTableCatalog;
        }
      | undefined;
   // Keyed on every fact a verdict reads that is not the clock.
   private liveVerdictCache:
      | {
           key: readonly unknown[];
           verdicts: LiveVerdicts | undefined;
        }
      | undefined;
   // `#@ persist` sources Malloy's build walk did not plan, which
   // would silently serve live. Warned, and refused at build.
   private droppedPersistSources: { name: string; modelPath: string }[] = [];
   /** Rollups over a gated base whose gates are grafted at read (row 1). */
   private rollupsKeepingGates = new Set<string>();
   private rollupsOverStoredBase: { baseName: string; modelPath?: string }[] =
      [];
   private annotationFindings: PackageAnnotationFinding[] = [];
   // Which sources may be served from a materialized table, decided at compile
   // where the compiled sources exist; binding does not recompile. `undefined`
   // means NOT KNOWN (the plan compute failed) and is refused like "nothing is
   // eligible": an unknown answer must not read as consent.
   sourceEligibility: SourceEligibility | undefined = undefined;
   // The colocated (same-connection) analogue, keyed by sourceEntityId. Same
   // "undefined means refuse" rule.
   colocatedSourceEligibility: ColocatedSourceEligibility | undefined =
      undefined;
   // Incremental-refresh gate inputs, resolved at compile because the rules read
   // the compiled schema, which the wire plan lacks. Empty when the plan compute
   // failed; safe, as the build only dispatches a delta it can read.
   incrementalPolicySources: IncrementalPolicySource[] = [];
   /**
    * Every connection a persist source builds on, from the last derived plan
    * (including skipped and refused sources), so a connection edit that
    * creates or fixes one re-derives the plan (see planConnectionNames).
    */
   private buildConnectionNames: ReadonlySet<string> = new Set();
   /**
    * The last good plan, kept only for binding while a failed re-derivation
    * leaves {@link buildPlan} null (see refreshBuildPlan): a host's rebind then
    * matches its entries as it did before the edit. Never handed out by
    * getBuildPlan, so no route or run validates against it. Cleared when a
    * plan is derived.
    */
   private servingPlanWhileUnavailable: BuildPlan | null = null;
   /**
    * Connections edited since the plan was last derived (see
    * markBuildPlanStale). A failed re-derivation keeps colocated eligibility
    * only for sources on connections outside this set: a colocated table lives
    * in, and is read through, its connection, so one the edit changed may now
    * be a different warehouse. Cleared when a plan is derived.
    */
   private connectionsChangedSinceDerived = new Set<string>();
   /**
    * Whether the last failed refresh failed in the load pool (a worker timeout
    * or crash), which a retry can clear, rather than in the plan compute, which
    * fails the same way until the model or a connection changes.
    */
   private lastRefreshFailedTransiently = false;

   constructor(private readonly owner: PackageMaterializationOwner) {}

   /**
    * What the package is bound to, for `Package.getOperationalFields`: the
    * binding's status and size, its manifest, and the sources served from
    * storage. `origin` marks rows (rollups) whose `sourceName` is in no model
    * file.
    */
   bindingState(): {
      manifestBindingStatus: "unbound" | "bound" | "live_fallback";
      manifestEntryCount: number;
      boundManifestUri: string | null;
      storageServeBindings: NonNullable<ApiPackage["storageServeBindings"]>;
   } {
      return {
         manifestBindingStatus: this.manifestBindingStatus,
         manifestEntryCount: this.manifestEntryCount,
         boundManifestUri: this.boundManifestUri,
         storageServeBindings: this.storageServeBindings.map((b) => ({
            origin: b.origin ?? "persist",
            sourceName: b.sourceName,
            storageDestinationName: b.destinationName,
            tablePath: b.tablePath,
         })),
      };
   }

   /** The persist build plan, or null when the package declares no persist source. */
   getBuildPlan(): BuildPlan | null {
      return this.buildPlan;
   }

   /** Whether the build plan could not be derived at load, so `getBuildPlan` says nothing. */
   isBuildPlanUnavailable(): boolean {
      return this.buildPlanFailed;
   }

   /** Legal-but-probably-unintended incremental declarations, as warnings. */
   incrementalPolicyAdvisories(): ApiPackageWarning[] {
      return incrementalPolicyAdvisories(this.incrementalPolicySources);
   }

   /**
    * Apply the persist build plan, derived from the unbound models so it
    * reflects the canonical build. The worker normally supplies it; deriving
    * it here recompiles every model on this thread, so that happens only when
    * the worker left it at its soft deadline. Best-effort: a failure is logged
    * and the package serves without a plan.
    *
    * A failure leaves both eligibilities unknown, which refuses every storage
    * AND colocated serve binding until a load succeeds: serving live is slower,
    * an unexamined binding could be wrong.
    *
    * Returns how long it took, or undefined when it failed.
    */
   async applyLoadBuildPlan(
      workerPlan: LoadPackageOutcome["buildPlan"],
   ): Promise<number | undefined> {
      const buildPlanPhaseStart = performance.now();
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
         const {
            plan,
            droppedPersistSources,
            rollupsOverStoredBase,
            annotationFindings,
            sourceEligibility,
            colocatedSourceEligibility,
            incrementalDeclarations,
            buildConnectionNames,
            materializePlans,
            rollupsKeepingGates,
         } = workerPlan
            ? workerPlan.outcome
            : await computePackageBuildPlan(this.owner.buildPlanPackage);
         this.buildPlan = plan;
         this.materializePlans = materializePlans ?? {};
         this.rollupsKeepingGates = new Set(rollupsKeepingGates ?? []);
         this.buildPlanFailed = false;
         this.servingPlanWhileUnavailable = null;
         this.droppedPersistSources = droppedPersistSources;
         this.rollupsOverStoredBase = rollupsOverStoredBase ?? [];
         // `?? []`: an outcome a worker built before this field existed.
         this.annotationFindings = annotationFindings ?? [];
         this.sourceEligibility = sourceEligibility;
         this.colocatedSourceEligibility = colocatedSourceEligibility;
         // `?? []`: an outcome a worker built before this field existed.
         this.buildConnectionNames = new Set(buildConnectionNames ?? []);
         this.incrementalPolicySources = Object.entries(plan?.sources ?? {})
            .filter(([sourceID]) => incrementalDeclarations[sourceID])
            .map(([sourceID, source]) => ({
               sourceName: source.name,
               modelPath: source.modelPath,
               dialect: source.dialect,
               storageDestination: source.annotationFields?.storage,
               // So the gate can spot two sources resolving to one table.
               sourceEntityId: source.sourceEntityId,
               declaration: incrementalDeclarations[sourceID],
            }));
         recordBuildPlanComputeDuration(
            workerPlan
               ? workerPlan.durationMs
               : performance.now() - buildPlanPhaseStart,
            workerPlan ? "worker" : "main_thread",
         );
         return performance.now() - buildPlanPhaseStart;
      } catch (err) {
         this.buildPlanFailed = true;
         // Nothing from the previous load describes this one: a failed
         // re-derivation after a connection edit must not leave the pre-edit
         // plan and eligibility in force, or a run would validate against
         // addresses the build no longer computes (ms2data/service#6145).
         // Unknown eligibility refuses every new binding (fail closed).
         this.buildPlan = null;
         this.sourceEligibility = undefined;
         this.colocatedSourceEligibility = undefined;
         this.incrementalPolicySources = [];
         this.rollupsOverStoredBase = [];
         this.rollupsKeepingGates = new Set();
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

   /** The bound manifest entries, or undefined when serving live; reused by /compile. */
   getBuildManifestEntries(): BuildManifest["entries"] | undefined {
      return this.buildManifestEntries;
   }

   /**
    * Whether a non-empty colocated manifest is bound. A pure-storage refresh may
    * skip the recompile only if nothing is substituted now or was before.
    */
   hasBoundTableNameManifest(): boolean {
      return (
         this.buildManifestEntries !== undefined &&
         Object.keys(this.buildManifestEntries).length > 0
      );
   }

   /**
    * Whether `storage=` serve bindings exist, so a refresh whose storage entries
    * vanished still clears them.
    */
   hasStorageServeBindings(): boolean {
      return this.storageServeBindings.length > 0;
   }

   /**
    * The freshness-filtered manifest at `now`: a table is used only within its
    * window, else per its `fallback` (`live`/`fail` drop it, `stale_ok` keeps
    * it); an entry with no window always routes. Undefined when unbound.
    * Memoized: fresh→stale is monotonic, so one `validUntil` deadline suffices.
    */
   getFreshBuildManifest(
      now: number = Date.now(),
   ): BuildManifest["entries"] | undefined {
      if (!this.freshnessEntries) return undefined;
      if (this.freshManifestCache && now < this.freshManifestCache.validUntil) {
         return this.freshManifestCache.manifest;
      }
      const { manifest, nextStaleSince } = filterFreshManifest(
         this.freshnessEntries,
         new Date(now),
      );
      this.freshManifestCache = {
         manifest,
         validUntil: nextStaleSince ?? Infinity,
      };
      return manifest;
   }

   /** Install the per-query freshness resolvers for every model; idempotent. */
   wireFreshnessResolvers(): void {
      this.owner.modelPolicy().setFreshnessResolvers({
         manifest: () => this.getFreshBuildManifest(),
         preaggregateEntityIds: () => this.getPreaggregateEntityIds(),
         materializedTables: () => this.materializedTableCatalog(),
      });
   }

   /**
    * The package's materialized tables as the path report reads them: every
    * planned source by `sourceID`, and the colocated tables bound now.
    * Memoized on the plan and the bound entries, both replaced wholesale.
    */
   materializedTableCatalog(): MaterializedTableCatalog {
      const liveVerdicts = this.liveVerdictsByTable();
      const cached = this.tableCatalogCache;
      if (
         cached &&
         cached.plan === this.buildPlan &&
         cached.bound === this.freshnessEntries &&
         cached.liveVerdicts === liveVerdicts
      ) {
         return cached.catalog;
      }
      const planned = new Map<string, PlannedTable>();
      for (const [sourceID, source] of Object.entries(
         this.buildPlan?.sources ?? {},
      )) {
         if (!source.sourceEntityId) continue;
         planned.set(sourceID, {
            sourceID,
            name: source.name,
            sourceEntityId: source.sourceEntityId,
            origin:
               source.origin === "preaggregate" ? "preaggregate" : "persist",
            ...(source.modelPath ? { modelPath: source.modelPath } : {}),
         });
      }
      const bound: Record<string, BoundTable> = {};
      for (const [sourceEntityId, entry] of Object.entries(
         this.freshnessEntries ?? {},
      )) {
         bound[sourceEntityId] = {
            tableName: entry.tableName,
            ...(entry.dataAsOf ? { dataAsOf: entry.dataAsOf } : {}),
            ...(entry.keyUniqueness
               ? { keyUniqueness: entry.keyUniqueness }
               : {}),
         };
      }
      const catalog: MaterializedTableCatalog = {
         planned,
         bound,
         ...(liveVerdicts
            ? {
                 liveVerdicts: liveVerdicts.byTable,
                 sourceVerdicts: liveVerdicts.bySource,
              }
            : {}),
      };
      this.tableCatalogCache = {
         plan: this.buildPlan,
         bound: this.freshnessEntries,
         liveVerdicts,
         catalog,
      };
      return catalog;
   }

   /**
    * The tables no query can be answered from, by their serve verdicts, for
    * the path report: keyed by tier and `sourceEntityId`
    * ({@link liveVerdictKey}), a table only when every planned source sharing
    * it on that tier is `live`. Staleness is left out, since the
    * report decides it per query from the window it reads now, and every other
    * reason reads no clock, so this is memoized on the facts the verdicts read
    * (each replaced wholesale, except the deployment facts, which key it by
    * what they answer for this plan) and costs a query a key compare plus one
    * fact read per planned source.
    */
   private liveVerdictsByTable(): LiveVerdicts | undefined {
      const models = this.owner.models();
      const key = [
         this.buildPlan,
         this.freshnessEntries,
         this.buildManifestEntries,
         this.storageServeBindings,
         this.sourceEligibility,
         this.colocatedSourceEligibility,
         this.rollupsOverStoredBase,
         getPersistStorageMode(),
         models,
         // The deployment facts are read live, not replaced wholesale, so
         // they key the memo by what they answer for this plan.
         this.deploymentSignature(),
         // So is each model's served copy, which compiles after the bind and
         // is dropped on invalidation; a `#(materialize)` verdict reads it.
         this.servedCopyState(models).signature,
      ];
      const cached = this.liveVerdictCache;
      if (cached && cached.key.every((part, i) => part === key[i])) {
         return cached.verdicts;
      }
      let verdicts: LiveVerdicts | undefined;
      if (this.buildPlan) {
         const sources = this.buildPlan.sources ?? {};
         const perTable = new Map<string, ServeVerdict[]>();
         const bySource = new Map<string, LiveTableVerdict>();
         for (const verdict of this.serveVerdicts()) {
            if (
               verdict.verdict === "live" &&
               verdict.reason &&
               verdict.reason !== "stale" &&
               !bySource.has(verdict.sourceID)
            ) {
               bySource.set(verdict.sourceID, {
                  reason: verdict.reason,
                  sourceName: verdict.sourceName,
               });
            }
            const entityId = sources[verdict.sourceID]?.sourceEntityId;
            if (!entityId) continue;
            const key = liveVerdictKey(verdict.tier, entityId);
            const list = perTable.get(key) ?? [];
            list.push(verdict);
            perTable.set(key, list);
         }
         const byTable = new Map<string, LiveTableVerdict>();
         for (const [tableKey, tableVerdicts] of perTable) {
            const live = tableVerdicts.every(
               (v) => v.verdict === "live" && v.reason && v.reason !== "stale",
            );
            if (!live) continue;
            const first = tableVerdicts[0];
            const plan = sources[first.sourceID];
            byTable.set(tableKey, {
               reason: first.reason as string,
               sourceName:
                  plan?.preaggregate?.baseSourceName ?? first.sourceName,
            });
         }
         verdicts = { byTable, bySource };
      }
      this.liveVerdictCache = { key, verdicts };
      return verdicts;
   }

   /**
    * The `sourceEntityId`s pre-aggregation synthesized, read off the plan's
    * `origin` (never the rollup naming convention, which an author's source
    * could share). Memoized on plan identity; the plan is replaced wholesale.
    */
   getPreaggregateEntityIds(): ReadonlySet<string> {
      if (this.preaggregateEntityIdCache?.plan === this.buildPlan) {
         return this.preaggregateEntityIdCache.ids;
      }
      const ids = new Set<string>();
      for (const source of Object.values(this.buildPlan?.sources ?? {})) {
         if (source.origin === "preaggregate" && source.sourceEntityId) {
            ids.add(source.sourceEntityId);
         }
      }
      this.preaggregateEntityIdCache = { plan: this.buildPlan, ids };
      return ids;
   }

   /**
    * Record that `uri`'s manifest is bound. Asserted rather than derived from
    * the entry count (a bind may carry no colocated entries); called after both
    * tiers apply, so it overrides {@link recordManifestBinding}.
    */
   markManifestBound(uri: string): void {
      this.boundManifestUri = uri;
      this.manifestBindingStatus = "bound";
   }

   /**
    * Mark that a configured `manifestLocation` could not be fetched/bound, so
    * the package is serving live despite intending to be materialized-routed.
    */
   markManifestBindFailed(): void {
      this.manifestBindingStatus = "live_fallback";
   }

   /** Store the latest bind's entries; an empty map reverts to live (unbound). */
   recordManifestBinding(entries: FreshnessManifest): void {
      const count = Object.keys(entries).length;
      this.freshnessEntries = count > 0 ? entries : undefined;
      this.buildManifestEntries =
         count > 0 ? toTableNameManifest(entries) : undefined;
      this.manifestEntryCount = count;
      this.manifestBindingStatus = count > 0 ? "bound" : "unbound";
      this.freshManifestCache = undefined;
      if (count === 0) {
         this.boundManifestUri = null;
      }
      this.refreshColocatedServeBindings();
   }

   /**
    * Drop the rollup key checks (known limitation 29) bound for these content
    * addresses: a run is about to write their tables, so what a check found
    * over the rows before no longer describes them. The routing itself is
    * kept; the join-to-rollup leg reads the table as unknown until a run
    * binds a new check.
    */
   forgetKeyUniqueness(sourceEntityIds: Iterable<string>): void {
      const ids = new Set(sourceEntityIds);
      // A stored table's own primary key check, on its storage binding.
      if (
         this.storageServeBindings.some(
            (b) => b.primaryKeys && ids.has(b.virtualHandle),
         )
      ) {
         this.storageServeBindings = this.storageServeBindings.map((b) => {
            if (!b.primaryKeys || !ids.has(b.virtualHandle)) return b;
            const { primaryKeys: _forgotten, ...rest } = b;
            return rest;
         });
         this.pushStorageServeBindingsToModels();
      }
      const entries = this.freshnessEntries;
      if (!entries) return;
      let next: FreshnessManifest | undefined;
      for (const id of ids) {
         const entry = entries[id];
         if (!entry?.keyUniqueness) continue;
         next ??= { ...entries };
         const { keyUniqueness: _forgotten, ...rest } = entry;
         next[id] = rest;
      }
      if (!next) return;
      this.freshnessEntries = next;
      this.tableCatalogCache = undefined;
   }

   /**
    * Restore colocated (same-connection) serve routing on load WITHOUT
    * recompiling: routing is a per-query `buildManifest` override, so setting
    * the entries suffices. An empty map reverts to live.
    *
    * Filtered POSITIVELY against {@link colocatedSourceEligibility}; a miss is
    * dropped, not fatal. This filter covers only this load-time restore path.
    * The three paths through `Package.reloadAllModels` record entries UNFILTERED
    * and rely on the per-query `#(authorize)` graft
    * (`Model.buildGraftedMaterializer`); do not treat this check as a boundary
    * they pass through.
    */
   bindColocatedServeManifest(entries: FreshnessManifest): void {
      const eligibility = this.colocatedSourceEligibility;
      const eligibleEntityIds = eligibility?.eligibleEntityIds;
      const allowed: FreshnessManifest = {};
      let dropped = 0;
      const dialects = this.dialectsByEntityId();
      for (const [sourceEntityId, entry] of Object.entries(entries)) {
         if (eligibleEntityIds?.has(sourceEntityId)) {
            // Malloy pastes `tableName` into `FROM` verbatim, so a table named
            // by a reserved word (`name="order"`) or needing case preserved
            // reaches the warehouse only quoted, as the builder CREATEd it and
            // as `Package.reloadAllModels` binds it. The dialect is the plan's
            // for this content address; a quoted name passes through.
            const dialect = dialects.get(sourceEntityId);
            allowed[sourceEntityId] = dialect
               ? {
                    ...entry,
                    tableName: quoteManifestTablePath(entry.tableName, dialect),
                 }
               : entry;
            continue;
         }
         dropped++;
         const reason =
            eligibility === undefined
               ? "the package build plan could not be computed, so no source was examined for eligibility"
               : (eligibility.refused[sourceEntityId] ??
                 "the source is not in the package build plan, so it was never examined (a `#@ persist` on a non-query-shaped source is dropped)");
         recordColocatedBindDropped(
            eligibility === undefined ? "build_plan_unavailable" : "refused",
         );
         logger.warn(
            "Refusing a colocated serve binding: the source is not eligible to be served from a materialized table",
            {
               packageName: this.owner.packageName(),
               sourceEntityId,
               reason,
            },
         );
      }
      if (eligibility === undefined && dropped > 0) {
         // The whole default tier just reverted to live, not one source.
         logger.warn(
            "Colocated serve bindings dropped package-wide: the build plan never computed, so no source could be examined for eligibility",
            { packageName: this.owner.packageName(), droppedCount: dropped },
         );
      }
      this.recordManifestBinding(allowed);
   }

   /** Each planned source's dialect, by its content address. */
   private dialectsByEntityId(): Map<string, string> {
      const out = new Map<string, string>();
      for (const source of Object.values(this.buildPlan?.sources ?? {})) {
         if (source.sourceEntityId && source.dialect) {
            out.set(source.sourceEntityId, source.dialect);
         }
      }
      return out;
   }

   /**
    * Bind the `storage=` serve bindings a host vouches for, minus any this
    * package's eligibility gate refuses, and push them onto every model.
    *
    * The host knows WHICH table backs a source, not WHETHER a frozen table may
    * serve it: only a compile knows that. A `given`-referencing source binds
    * per query for row-level access, so one shared table would hand every
    * caller the builder's rows (a source can gain a `given` after its build).
    *
    * Eligibility must be POSITIVE: a failed plan compute or a source missing
    * from the plan must not read as consent. Refused bindings are dropped, and
    * that source serves live, which is always correct.
    *
    * A package also binds only the tables its own definitions built: an
    * entry is matched to a plan source by name AND content address
    * ({@link planSourceIds}), never by name alone. An entry at an address no
    * same-named plan source has was built from another definition (another
    * version's, in a `scope: package` package whose versions share tables
    * under unchanged names, or an earlier edit's, or an older release's), so
    * it is dropped and that source serves live. (The colocated tier is keyed
    * by content address already.)
    */
   bindStorageServeBindings(entries: Record<string, ManifestEntry>): void {
      const plan = this.buildPlan ?? this.servingPlanWhileUnavailable;
      const sourceIDsFor = planSourceIds(plan?.sources ?? {});
      const derived = deriveServeBindings(
         plan ? this.ownEntries(entries, sourceIDsFor) : entries,
         groupAliasesByName(Object.values(plan?.sources ?? {})),
         this.serveDestinationSchema,
         sourceIDsFor,
      );
      const eligibility = this.sourceEligibility;
      // Keyed by sourceID (the source map's keying rule): a binding passes when
      // one of the plan sources it materializes was examined and found
      // eligible. A rollup passes under its own synthesized sourceID.
      const eligible = new Set(eligibility?.eligible ?? []);
      // A binding is kept for its ELIGIBLE sources only: two models may declare
      // same-named sources with identical SQL (one table), one gated and
      // refused. Narrowing the binding's sourceIDs keeps the refused one off
      // the table in its own model, where admitting the binding whole would
      // have served it (fails closed).
      const allowed = derived.flatMap((binding) => {
         const ids = binding.sourceIDs ?? [];
         const admitted = ids.filter((id) => eligible.has(id));
         if (admitted.length > 0) {
            return [
               admitted.length === ids.length
                  ? binding
                  : { ...binding, sourceIDs: admitted },
            ];
         }
         const refusal = ids
            .map((id) => eligibility?.refused[id])
            .find((reason) => reason !== undefined);
         const reason =
            eligibility === undefined
               ? "the package build plan could not be computed, so no source was examined for eligibility"
               : (refusal ??
                 (ids.length === 0
                    ? "the table names no source of this package's build plan, so none was examined"
                    : "the source is not in the package build plan, so it was never examined (a `#@ persist` on a non-query-shaped source is dropped)"));
         logger.warn(
            "Refusing a storage serve binding: the source is not eligible to be served from a materialized table",
            {
               packageName: this.owner.packageName(),
               sourceName: binding.sourceName,
               // A generated rollup name means nothing to an operator alone.
               ...(binding.origin === "preaggregate"
                  ? { kind: "pre-aggregation rollup" }
                  : {}),
               reason,
            },
         );
         return [];
      });
      this.storageServeBindings = allowed;
      this.pushStorageServeBindingsToModels();
   }

   /**
    * The entries this package's own sources built: a plan source of the
    * entry's name at its content address. See {@link bindStorageServeBindings}.
    */
   private ownEntries(
      entries: Record<string, ManifestEntry>,
      sourceIDsFor: (sourceName: string, sourceEntityId: string) => string[],
   ): Record<string, ManifestEntry> {
      const kept: Record<string, ManifestEntry> = {};
      for (const [key, entry] of Object.entries(entries)) {
         if (
            !entry.sourceName ||
            sourceIDsFor(entry.sourceName, entry.sourceEntityId).length > 0
         ) {
            kept[key] = entry;
            continue;
         }
         logger.info(
            "Serving a source live: its stored table was built from another definition",
            {
               packageName: this.owner.packageName(),
               ...(this.owner.versionId() !== undefined
                  ? { versionId: this.owner.versionId() }
                  : {}),
               sourceName: entry.sourceName,
            },
         );
      }
      return kept;
   }

   /**
    * A declaration's compiled definition, from the package model that holds it
    * as a node of its own (the model that declares or imports it). A model
    * that reaches a source only through a join (an index importing the file
    * that joins it, #6643) holds the join, not the declaration, so its served
    * copy finds the declaration here.
    */
   private declarationOf(sourceID: string): unknown {
      for (const model of this.owner.models().values()) {
         const def = SourceMap.of(model.getModelDef?.()).node(sourceID)?.def;
         if (def) return def;
      }
      return undefined;
   }

   /**
    * The served copy's bindings for the colocated `#(materialize)` tables the
    * bound manifest holds (colocated_bindings.ts), admitted by the colocated
    * tier's positive eligibility, keyed by content address as that tier is.
    */
   private colocatedServeBindings(): ServeBinding[] {
      const plan = this.buildPlan ?? this.servingPlanWhileUnavailable;
      const sources = plan?.sources ?? {};
      const eligible = this.colocatedSourceEligibility?.eligibleEntityIds;
      // v1 colocated `#@ persist` tables too (the v1 cutover's G1).
      const v1 = true;
      const viewBuilds = this.persistedViewBuildIDs();
      return deriveColocatedServeBindings({
         entries: this.freshnessEntries,
         planSources: sources,
         materializePlans: this.materializePlans,
         eligible: (sourceID) =>
            !!eligible?.has(sources[sourceID]?.sourceEntityId ?? ""),
         ...(v1
            ? {
                 v1Schema: (sourceID: string) =>
                    v1StoredColumnsOf(this.declarationOf(sourceID)),
                 notV1: (sourceID: string) => !!viewBuilds?.has(sourceID),
              }
            : {}),
      });
   }

   /** Every model's persisted views' generated build sources, by sourceID. */
   private persistedViewBuildIDs(): Set<string> {
      const ids = new Set<string>();
      for (const model of this.owner.models().values()) {
         // Optional: a model a spec stands in for need not implement it.
         for (const b of model.persistedViewBuilds?.() ?? []) {
            ids.add(b.buildSourceID);
         }
      }
      return ids;
   }

   /** What {@link pushStorageServeBindingsToModels} last pushed, colocated. */
   private colocatedBindingsSignature = "";

   /**
    * Push the bindings again when the colocated `#(materialize)` tables the
    * served copy would read changed (a manifest bind, a re-derived plan).
    */
   private refreshColocatedServeBindings(): void {
      if (
         colocatedSignature(this.colocatedServeBindings()) !==
         this.colocatedBindingsSignature
      ) {
         this.pushStorageServeBindingsToModels();
      }
   }

   /**
    * Push the current serve bindings onto every loaded model: the storage
    * tier's, and the colocated `#(materialize)` tables only the served copy
    * reads.
    */
   pushStorageServeBindingsToModels(): void {
      const colocated = this.colocatedServeBindings();
      this.colocatedBindingsSignature = colocatedSignature(colocated);
      const bindings =
         colocated.length === 0
            ? this.storageServeBindings
            : [...this.storageServeBindings, ...colocated];
      for (const model of this.owner.models().values()) {
         this.setServedCopyContextOn(model);
         model.setServeBindings(bindings);
         // Compiled at bind so a served copy that does not compile is a
         // package warning before any query; never throws.
         void model.prepareServedCopy?.();
      }
   }

   /**
    * Hand every loaded model the package directory, connections and plans its
    * served copy, rollup and persisted-view legs compile with, before any
    * binding exists. A colocated rollup is read through the build manifest,
    * which a bind sets without pushing bindings, so its leg needs this from
    * load (a package whose only tables are colocated rollups otherwise never
    * routes to them).
    */
   pushServedCopyContextToModels(): void {
      for (const model of this.owner.models().values()) {
         this.setServedCopyContextOn(model);
      }
   }

   private setServedCopyContextOn(model: Model): void {
      // Optional: a model a spec stands in for need not implement it.
      model.setServedCopyContext?.(
         this.owner.packagePath(),
         this.owner.malloyConfig(),
         this.materializePlans,
         (sourceID) => this.declarationOf(sourceID),
      );
   }

   /**
    * How a serve binding names a destination's schema, so it reads the table
    * where the build wrote it (storageTablePath). Absent, every destination is
    * `main`, the pre-existing layout.
    */
   setServeDestinationSchema(
      resolver: (destinationName: string) => string | undefined,
   ): void {
      this.serveDestinationSchema = resolver;
   }

   /**
    * Give the package the environment's deployment facts its publish warnings
    * read (P13): which storage destinations exist, and which connections can
    * hold a table. Read live on every package read, so a destination
    * configured after the load clears its warning with no reload. Absent,
    * nothing is warned.
    */
   setDeploymentFacts(facts: DeploymentFacts): void {
      this.deploymentFacts = facts;
   }

   /**
    * Set the connections serve shapes compile against: the environment's
    * storage destinations. Kept apart from the package's own MalloyConfig so no author
    * model can reach a destination. Absent ⇒ queries serve live.
    */
   setServeDestinationConfig(provider: () => MalloyConfig): void {
      this.serveDestinationConfig = provider;
      this.pushServeDestinationConfigToModels();
   }

   pushServeDestinationConfigToModels(): void {
      if (!this.serveDestinationConfig) return;
      for (const model of this.owner.models().values()) {
         model.setServeDestinationConfig(this.serveDestinationConfig);
      }
   }

   /** Drop every model's memoized serve shape, when its destinations are replaced. */
   invalidateServeShapes(): void {
      for (const model of this.owner.models().values()) {
         model.invalidateServeShapeCache();
      }
   }

   /**
    * Mark the plan stale after a connection edit. Every content address folds
    * in its connection's digest, and the digest reads connection config (host,
    * database, project, role, the setup SQL a statement timeout is applied
    * through, …), so an edit can move the address of every source on it. Until
    * {@link refreshBuildPlan} re-derives it, {@link whenBuildPlanCurrent}
    * holds readers back: the plan computed before the edit would hand out the
    * old addresses, a host would echo ledger entries recorded under them, they
    * would pass create-time validation against that same plan, and the run's
    * own compile would refuse them as "measured under a different definition"
    * (ms2data/service#6145).
    */
   markBuildPlanStale(changedConnections: Iterable<string> = []): void {
      for (const name of changedConnections) {
         this.connectionsChangedSinceDerived.add(name);
      }
      this.buildPlanStaleSeq++;
      if (!this.buildPlanStale) {
         let resolve!: () => void;
         const promise = new Promise<void>((r) => (resolve = r));
         this.buildPlanStale = { promise, resolve };
      }
   }

   /** See {@link lastRefreshFailedTransiently}. */
   lastBuildPlanRefreshFailedTransiently(): boolean {
      return this.lastRefreshFailedTransiently;
   }

   /** Whether a connection edit has made the plan stale and it is not re-derived yet. */
   isBuildPlanStale(): boolean {
      return this.buildPlanStale !== undefined;
   }

   /**
    * Re-derive the plan if it is stale, with `derive` supplying the outcome
    * (the load worker's, so the recompile stays off this thread; undefined
    * falls back to deriving here, as a load does past its soft deadline).
    * Serialized; a refresh that finds the plan current does nothing, so a
    * burst of edits costs one re-derivation per package. The waiters are
    * released only by a refresh that started after the latest edit, so a late
    * edit re-derives again. Resolves true when the plan was derived, false
    * when it failed (the plan is then unavailable, see applyLoadBuildPlan).
    */
   refreshBuildPlan(
      derive: () => Promise<LoadPackageOutcome["buildPlan"]>,
      { retryFailed = false }: { retryFailed?: boolean } = {},
   ): Promise<boolean> {
      const run = this.buildPlanRefreshChain.then(async () => {
         // A retry of a failed re-derivation runs while the plan is
         // unavailable but not stale (readers get 409, not a wait).
         const retrying = retryFailed && this.buildPlanFailed;
         if (!this.buildPlanStale && !retrying) return !this.buildPlanFailed;
         const seq = this.buildPlanStaleSeq;
         // What serving reads from the last good plan. A failed re-derivation
         // keeps it (below): storage eligibility is keyed by name and does not
         // move with connection config, and colocated eligibility only admits
         // addresses the last good plan computed, which is what was serving.
         // So bindings in force, and a host's rebind while the plan is
         // unavailable, keep serving instead of dropping the package to live.
         // Only run creation is refused (409) until a retry succeeds.
         const lastGoodPlan =
            this.buildPlan ?? this.servingPlanWhileUnavailable;
         const lastGood = {
            sourceEligibility: this.sourceEligibility,
            colocatedSourceEligibility: this.colocatedSourceEligibility,
            incrementalPolicySources: this.incrementalPolicySources,
            rollupsOverStoredBase: this.rollupsOverStoredBase,
            buildConnectionNames: this.buildConnectionNames,
         };
         let workerPlan: LoadPackageOutcome["buildPlan"];
         let poolFailed = false;
         try {
            workerPlan = await derive();
         } catch (err) {
            poolFailed = true;
            workerPlan = {
               ok: false,
               error: errMessage(err),
               durationMs: 0,
               digestSkipped: [],
               eligibilityRefused: {},
            };
         }
         const derived =
            (await this.applyLoadBuildPlan(workerPlan)) !== undefined;
         this.lastRefreshFailedTransiently = !derived && poolFailed;
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
         if (seq === this.buildPlanStaleSeq && this.buildPlanStale) {
            this.buildPlanStale.resolve();
            this.buildPlanStale = undefined;
         }
         // The plan's addresses and eligibility decide the colocated tables
         // the served copy reads.
         this.refreshColocatedServeBindings();
         return derived;
      });
      this.buildPlanRefreshChain = run.catch(() => false);
      return run;
   }

   /**
    * Resolves once the plan reflects the current connections (see
    * markBuildPlanStale). Past `timeoutMs` it rejects with
    * BuildPlanRefreshingError, a 503 with Retry-After, rather than hold the
    * caller's request.
    */
   async whenBuildPlanCurrent(
      timeoutMs: number = BUILD_PLAN_REFRESH_WAIT_MS,
   ): Promise<void> {
      const stale = this.buildPlanStale;
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
    * The connections this package's persist sources build on, from the plan,
    * so a connection edit re-derives only the packages it can move. Null when
    * the plan is unavailable (unknown, so any edit re-derives it).
    */
   planConnectionNames(): ReadonlySet<string> | null {
      if (this.buildPlanFailed) return null;
      return new Set([
         ...Object.values(this.buildPlan?.sources ?? {}).map(
            (source) => source.connectionName,
         ),
         // Sources the plan skipped (their connection did not resolve) or
         // refused: creating or fixing that connection must re-derive.
         ...this.buildConnectionNames,
      ]);
   }

   /**
    * One warning per `storage=` base whose `#@ preaggregate storage=` rollups
    * were left out: they can neither build nor serve over a stored base yet
    * (#6293), so the base serves from its stored table and the rollups do not
    * exist.
    */
   rollupsOverStoredBaseWarnings(): ApiPackageWarning[] {
      // With the storage tier off the base itself serves live, which
      // storageWarnings already says; "reads the stored table" would be false.
      if (getPersistStorageMode() === "off") return [];
      return this.rollupsOverStoredBase.map((r) => ({
         ...(r.modelPath ? { model: r.modelPath } : {}),
         subject: r.baseName,
         code: "preaggregate-over-stored-base",
         severity: "warn" as const,
         message:
            `declares '#@ preaggregate storage=' rollups, but is itself ` +
            `persisted with 'storage=', so the rollups are not built: a rollup ` +
            `over a stored base cannot build or serve yet, and queries read the ` +
            `base's stored table instead.` +
            (getMaterializeEnabled()
               ? ` Write the rollup as '#(preaggregate) grain=[…] storage=…' ` +
                 `on the measure, which builds and serves over a stored base.`
               : ""),
      }));
   }

   /**
    * Warnings for `#@ persist` sources Malloy's build walk did not plan (e.g.
    * a filtered pass-through), which would otherwise silently serve
    * live. Independent of {@link buildPlan}: a package whose only persist
    * source is dropped has no plan.
    */
   droppedPersistWarnings(): ApiPackageWarning[] {
      return this.droppedPersistSources.map((d) => ({
         model: d.modelPath,
         subject: d.name,
         code: "persist-not-materializable",
         severity: "warn" as const,
         message:
            `is annotated '#@ persist' but was not recognized as a ` +
            `materializable source, so nothing is materialized and it is served ` +
            `live. Only query/aggregate sources build; a filtered pass-through ` +
            `does not. Persist a query source, or invoke a parameterized source ` +
            `with a bound argument.`,
      }));
   }

   /**
    * The findings on authored `#@` annotations, as package warnings: a
    * malformed one (an unparseable tag, a retired key) is `error`, refused at
    * publish; an unservable one (an unknown key) is `warn`.
    */
   annotationFindingWarnings(): ApiPackageWarning[] {
      return this.annotationFindings.map((finding) => ({
         ...(finding.model ? { model: finding.model } : {}),
         ...(finding.subject ? { subject: finding.subject } : {}),
         message: finding.rewrite
            ? `${finding.message}; ${finding.rewrite}`
            : finding.message,
         code: finding.code,
         severity: findingSeverity(finding),
      }));
   }

   /**
    * One warning per bound `#(preaggregate)` rollup whose last build found its
    * join_one key repeated (known limitation 29's check, `keyUniqueness`
    * false): the data breaks a `join_one` declared on that key, so queries
    * that read it beside other aggregates are answered live, and live itself
    * counts each parent once per repeated row. Recomputed from what is bound,
    * so it clears once a build finds the key unique, or forgets the check.
    */
   repeatedJoinOneKeyWarnings(): ApiPackageWarning[] {
      const warnings: ApiPackageWarning[] = [];
      const bySourceEntity = new Map(
         Object.values(this.buildPlan?.sources ?? {}).flatMap((source) =>
            source.sourceEntityId ? [[source.sourceEntityId, source]] : [],
         ),
      );
      for (const [sourceEntityId, entry] of Object.entries(
         this.freshnessEntries ?? {},
      )) {
         // The rollup's own key check only: an entry may carry just the
         // query sources' primary key checks (`parentKeys`), which say
         // nothing of a join_one keyed on the rollup's grain.
         const check = entry.keyUniqueness;
         if (check?.unique !== false || check.keyColumn === undefined) {
            continue;
         }
         const rollup = bySourceEntity.get(sourceEntityId)?.preaggregate;
         if (!rollup) continue;
         const source = bySourceEntity.get(sourceEntityId);
         warnings.push({
            ...(source?.modelPath ? { model: source.modelPath } : {}),
            subject: rollup.baseSourceName,
            code: PREAGGREGATE_JOIN_ONE_KEY_REPEATED_CODE,
            severity: "warn",
            message:
               `holds \`${check.keyColumn}\` more than once, found by its ` +
               `#(preaggregate) rollup's last build (grain ` +
               `${rollup.grainDimensions.join(", ")}): a join_one keyed on it ` +
               `is not unique in the data, so live repeats each joining row ` +
               `once per match. Queries that read that join_one beside other ` +
               `aggregates are answered live. Fix the data or declare the ` +
               `join as join_many.`,
         });
      }
      return warnings;
   }

   /**
    * Warnings for the `#@ persist` sources the compile-time gates refused,
    * coded {@link PERSIST_SOURCE_REFUSED_CODE}. Not an annotation finding: the
    * annotation is well formed; the source it sits on cannot be materialized.
    * Each carries the gate's message verbatim, so there is one wording to
    * maintain.
    * A rollup's `subject` is its BASE source (its own name is generated), and
    * is omitted when the base is unavailable.
    */
   persistRefusalWarnings(): ApiPackageWarning[] {
      const refused = this.buildPlan?.refusedSources;
      if (!refused) return [];
      return Object.entries(refused).map(([sourceID, source]) => {
         const subject =
            source.tier === "preaggregate"
               ? this.buildPlan?.sources?.[sourceID]?.preaggregate
                    ?.baseSourceName
               : source.name;
         const warning: ApiPackageWarning = {
            message: source.message,
            code: PERSIST_SOURCE_REFUSED_CODE,
            severity: "warn",
         };
         if (source.modelPath) warning.model = source.modelPath;
         if (subject) warning.subject = subject;
         return warning;
      });
   }

   /**
    * Warnings for `storage=` sources the serve path is not honoring, per
    * {@link getPersistStorageMode}: `off` ignores the annotation, `write-only`
    * builds but does not route. `on` warns nothing.
    */
   storageWarnings(): ApiPackageWarning[] {
      const mode = getPersistStorageMode();
      if (mode === "on" || !this.buildPlan?.sources) return [];
      const warnings: ApiPackageWarning[] = [];
      for (const source of Object.values(this.buildPlan.sources)) {
         const storage = source.annotationFields?.storage?.trim();
         if (!storage) continue;
         // A rollup gets its own wording: nobody queries it by name and there is
         // no live reading of it; queries are answered from the base. It is named
         // by its base source and grain (what the author typed), never its
         // generated name. In the default `off` mode this is the only signal.
         // The grain is a SET: the wire carries sorted dimensions, not the
         // authored order.
         const rollup = source.preaggregate;
         const at = rollup
            ? `Measures of \`${rollup.baseSourceName}\` pre-aggregated at grain ` +
              `(${rollup.grainDimensions.join(", ")}) declare `
            : "";
         const message = rollup
            ? mode === "off"
               ? `${at}storage="${storage}", but PERSIST_STORAGE_MODE is off, so ` +
                 `the rollup is not built. Queries that would have used it are ` +
                 `answered from \`${rollup.baseSourceName}\` instead — correct, ` +
                 `but unaccelerated.`
               : `${at}storage="${storage}", and the rollup is built, but ` +
                 `PERSIST_STORAGE_MODE is write-only, so the serve path does not ` +
                 `route to it. Queries are answered from ` +
                 `\`${rollup.baseSourceName}\` — correct, but unaccelerated.`
            : mode === "off"
              ? `declares storage="${storage}" but PERSIST_STORAGE_MODE is off; ` +
                `the annotation is ignored and the source is served live from ` +
                `its own warehouse.`
              : `is materialized into storage "${storage}" but ` +
                `PERSIST_STORAGE_MODE is write-only; the serve path is not ` +
                `routed to the materialized table (served live).`;
         warnings.push({
            model: source.modelPath ?? "",
            subject: rollup ? rollup.baseSourceName : source.name,
            message,
         });
      }
      return warnings;
   }

   /**
    * Warnings for a model whose served copy (materialization v2) leaves
    * sources unserved: it did not compile, a cross-connection join waits for
    * the other side to build, or a source could not be re-rooted. Those
    * sources serve through today's serve shape, or live. Empty when the served
    * copy is off.
    */
   servedCopyWarnings(): ApiPackageWarning[] {
      const warnings: ApiPackageWarning[] = [];
      for (const [modelPath, model] of this.owner.models()) {
         const leftOut = model.crossConnectionLeftOutDefinitions?.() ?? [];
         // What read a left-out join goes with it; nothing leaves silently.
         const readers = leftOut.filter((l) => !l.crossConnection);
         if (readers.length > 0) {
            warnings.push({
               model: modelPath,
               code: SERVED_COPY_WARNING_CODE,
               severity: "warn",
               message:
                  "These definitions read a join left out across connections, so this model loads without them: " +
                  readers
                     .map((l) =>
                        l.declaration
                           ? `\`${l.declaration}.${l.name}\``
                           : `\`${l.name}\``,
                     )
                     .join(", ") +
                  ".",
            });
         }
         for (const l of leftOut) {
            if (!l.crossConnection) continue;
            warnings.push({
               model: modelPath,
               subject: l.declaration ?? l.name,
               code: SERVED_COPY_WARNING_CODE,
               severity: "warn",
               message:
                  `This model joins sources on different connections (the join \`${l.name}\`), ` +
                  "which does not compile live, so it loads without that join and what reads it. " +
                  "Queries that need it are answered once both sides are built into one storage destination.",
            });
         }
         const problems = model.servedCopyProblems?.() ?? [];
         // A join already inlined, but kept as written: its own warning says
         // why, so the cross-connection one does not advise inlining it.
         const keptInline = new Set(
            problems
               .filter((p) => p.reason === "inline_join_kept")
               .map((p) => `${p.sourceName}\u0000${p.join}`),
         );
         for (const p of problems) {
            warnings.push({
               model: modelPath,
               ...(p.sourceName ? { subject: p.sourceName } : {}),
               code: SERVED_COPY_WARNING_CODE,
               severity: "warn",
               message: servedCopyProblemMessage(
                  p,
                  keptInline.has(`${p.sourceName}\u0000${p.join}`),
               ),
            });
            // The coded warning the compile of record's own check raises.
            for (const message of p.coverage ?? []) {
               warnings.push({
                  model: modelPath,
                  ...(p.sourceName ? { subject: p.sourceName } : {}),
                  code: CLASSIFICATION_COVERAGE,
                  severity: "warn",
                  message,
               });
            }
         }
      }
      return warnings;
   }

   /**
    * Warnings for stored columns whose type does not serve as the type Malloy
    * declares for them (ms2data/service#6642), one per source and column. A
    * castable column is read through a cast to its declared type, and a
    * rebuild stores that type; any other is answered live. Read from the
    * bindings bound now, so it follows the manifest rather than the load. Keyed
    * by the source's sourceID, so two models' same-named sources both report,
    * and a source several models import reports once.
    */
   storedColumnTypeWarnings(): ApiPackageWarning[] {
      const warnings: ApiPackageWarning[] = [];
      const seen = new Set<string>();
      for (const [modelPath, model] of this.owner.models()) {
         // Optional: a model a spec stands in for need not implement it.
         for (const {
            sourceName,
            sourceID,
            mismatches,
         } of model.storedColumnTypeMismatches?.() ?? []) {
            for (const m of mismatches) {
               const key = `${sourceID ?? `${modelPath}\u0000${sourceName}`}\u0000${m.column}`;
               if (seen.has(key)) continue;
               seen.add(key);
               warnings.push({
                  model: modelPath,
                  subject: sourceName,
                  code: STORED_COLUMN_TYPE_MISMATCH_CODE,
                  severity: "warn",
                  message:
                     `stores column \`${m.column}\` as ${m.storedType}, not its ` +
                     `declared Malloy type \`${m.declaredType}\`. ` +
                     (m.handling === "cast"
                        ? `It is read through a cast to \`${m.declaredType}\`, ` +
                          `so it still serves from storage; the next build ` +
                          `stores the declared type.`
                        : `No cast reproduces its values faithfully, so queries ` +
                          `that read it are answered live. Give the field a ` +
                          `native cast in the model (for example \`::"double"\`) ` +
                          `so the warehouse returns the declared type.`),
               });
            }
         }
      }
      return warnings;
   }

   /**
    * Each materialized source's serve verdict (materialization v2, D2;
    * ms2data/service#6277): whether queries are answered from its table, and
    * when not, why and what to change. Read from the plan and the binding as
    * they stand now, so it follows each bind. `views` adds per-view verdicts
    * (a preview, see {@link previewServeVerdicts}).
    */
   serveVerdicts(
      views?: ReadonlyMap<string, ViewServeVerdict[]>,
   ): ServeVerdict[] {
      if (!this.buildPlan && this.rollupsOverStoredBase.length === 0) return [];
      try {
         return this.computeVerdicts(views);
      } catch (error) {
         // Reporting only: a verdict that cannot be computed must not fail
         // the dry run, the package read or the status poll that carries it.
         logger.warn("Could not compute serve verdicts", {
            packageName: this.owner.packageName(),
            error: errMessage(error),
         });
         return [];
      }
   }

   private computeVerdicts(
      views?: ReadonlyMap<string, ViewServeVerdict[]>,
   ): ServeVerdict[] {
      const models = this.owner.models();
      const index = this.entryPointIndex(models);
      const persistedViews = new Map<
         string,
         { sourceID: string; sourceName: string; viewName: string }
      >();
      for (const model of models.values()) {
         for (const v of model.persistedViewBuilds?.() ?? []) {
            persistedViews.set(v.buildSourceID, v);
         }
      }
      const servedStorageNames = new Set<string>();
      for (const model of models.values()) {
         for (const name of model.servedStorageSourceNames?.() ?? []) {
            servedStorageNames.add(name);
         }
      }
      const servedCopy = this.servedCopyState(models);
      const materializeIDs = new Set(Object.keys(this.materializePlans));
      const now = new Date();
      const freshStorageNames = new Set(
         this.storageServeBindings
            .filter(
               (b) =>
                  evaluateManifestFreshness(
                     {
                        tableName: b.tablePath,
                        dataAsOf: b.freshAsOf,
                        freshnessWindowSeconds: b.freshnessWindowSeconds,
                        freshnessFallback: b.freshnessFallback,
                     },
                     now,
                  ) === "serve_table",
            )
            .map((b) => b.sourceName),
      );
      const reads = (
         ids: ReadonlySet<string>,
         sourceID: string | undefined,
         sourceName: string,
      ) =>
         sourceID !== undefined
            ? ids.has(sourceID)
            : [...ids].some((id) => id.startsWith(`${sourceName}@`));
      return computeServeVerdicts({
         plan: this.buildPlan,
         storageMode: getPersistStorageMode(),
         reachable: (sourceID, sourceName) =>
            index?.reached === undefined ||
            reads(index.reached, sourceID, sourceName),
         gated: (sourceID, sourceName) => {
            const reaching = (index?.roots ?? []).filter((root) =>
               reads(root.ids, sourceID, sourceName),
            );
            return reaching.length > 0 && reaching.every((root) => root.gated);
         },
         servedCopyKeepsGates: getMaterializeGatedSources(),
         rollupKeepsGates: (sourceID) => this.rollupsKeepingGates.has(sourceID),
         refusedAtBind: (sourceID, source, tier) =>
            this.refusedAtBind(sourceID, source, tier),
         boundEntityIds: new Set(Object.keys(this.buildManifestEntries ?? {})),
         freshEntityIds: new Set(
            Object.keys(this.getFreshBuildManifest(now.getTime()) ?? {}),
         ),
         boundStorageNames: new Set(
            this.storageServeBindings.map((b) => b.sourceName),
         ),
         servedStorageNames,
         freshStorageNames,
         rollupsOverStoredBase: this.rollupsOverStoredBase,
         materialize: (sourceID) => {
            if (!materializeIDs.has(sourceID)) return undefined;
            if (!servedCopy.on) return "off";
            return servedCopy.ids.has(sourceID) || servedCopy.pending
               ? "serves"
               : "not_served";
         },
         persistedViewOf: (sourceID) => persistedViews.get(sourceID),
         views,
         destinationConfigured: (name) => this.destinationConfigured(name),
         connectionCanHoldTable: (name) =>
            this.deploymentFacts?.connectionCanHoldTable(name),
      });
   }

   /** See {@link ServeVerdictFacts.destinationConfigured}. */
   private destinationConfigured(name: string): boolean | undefined {
      const names = this.deploymentFacts?.destinationNames();
      return names === undefined ? undefined : names.includes(name);
   }

   /**
    * What the deployment facts answer for this plan's sources, as a string:
    * the {@link liveVerdictsByTable} memo's key part for facts the
    * environment can change under a loaded package.
    */
   /**
    * What the models' served copies answer from storage, by author
    * `sourceID`: every model's, since a source is re-rooted in each model that
    * reads it. `on` when any model's served copy is enabled, `pending` when one
    * has yet to compile, and a `signature` that changes whenever either does.
    */
   private servedCopyState(models: ReadonlyMap<string, Model>): {
      on: boolean;
      pending: boolean;
      ids: ReadonlySet<string>;
      signature: string;
   } {
      let on = false;
      let pending = false;
      const ids = new Set<string>();
      for (const model of models.values()) {
         const reread = model.servedCopySourceIDs?.();
         if (reread === undefined) continue;
         on = true;
         if (reread === "pending") pending = true;
         else for (const id of reread) ids.add(id);
      }
      const signature = on
         ? `${pending ? "pending" : "compiled"}:${[...ids].sort().join(",")}`
         : "off";
      return { on, pending, ids, signature };
   }

   private deploymentSignature(): string {
      if (!this.deploymentFacts) return "";
      const parts: string[] = [];
      for (const source of Object.values(this.buildPlan?.sources ?? {})) {
         const destination = source.annotationFields?.storage?.trim();
         parts.push(
            destination
               ? `d:${destination}=${this.destinationConfigured(destination)}`
               : `c:${source.connectionName}=${this.deploymentFacts.connectionCanHoldTable(source.connectionName)}`,
         );
      }
      return parts.join("|");
   }

   /**
    * Why the bind refuses a planned source, as {@link bindStorageServeBindings}
    * and {@link bindColocatedServeManifest} decide it, or undefined when it
    * keeps it. Storage requires a positive eligibility, as the bind does.
    * Colocated rollups need not appear in the colocated record, so only an
    * authored source missing from it, or one it names as refused, counts.
    */
   private refusedAtBind(
      sourceID: string,
      source: { name: string; sourceEntityId: string; origin?: string },
      tier: "colocated" | "storage",
   ): string | undefined {
      if (tier === "storage") {
         const eligibility = this.sourceEligibility;
         if (!eligibility || eligibility.eligible.includes(sourceID)) {
            return undefined;
         }
         return (
            eligibility.refused[sourceID] ??
            "the source was never examined for eligibility"
         );
      }
      const eligibility = this.colocatedSourceEligibility;
      if (!eligibility) return undefined;
      const refused = eligibility.refused[source.sourceEntityId];
      if (refused !== undefined) return refused;
      if (
         source.origin !== "preaggregate" &&
         !eligibility.eligibleEntityIds.has(source.sourceEntityId)
      ) {
         return "the source was never examined for eligibility";
      }
      return undefined;
   }

   /**
    * {@link serveVerdicts} with each stored source's named views compiled
    * against a serve shape, before anything is built (ms2data/service#7090):
    * the shape is declared from the plan's output columns, the types the build
    * stores, so it needs no table. Costs one shape compile per model and one
    * compile per view, so it runs only where a caller asks for a preview.
    */
   async previewServeVerdicts(): Promise<ServeVerdict[]> {
      let bindings: ServeBinding[];
      try {
         bindings = this.previewServeBindings();
      } catch (error) {
         logger.warn("Could not declare the preview's serve bindings", {
            packageName: this.owner.packageName(),
            error: errMessage(error),
         });
         bindings = [];
      }
      const views = new Map<string, ViewServeVerdict[]>();
      if (bindings.length > 0) {
         for (const model of this.owner.models().values()) {
            try {
               const found = await model.previewStorageViews(bindings);
               // Keyed by sourceID: a model that imports the source reports
               // the same views as the one that declares it.
               for (const [sourceID, verdicts] of found) {
                  if (!views.has(sourceID)) views.set(sourceID, verdicts);
               }
            } catch (error) {
               logger.debug("Could not preview a model's storage views", {
                  packageName: this.owner.packageName(),
                  error: errMessage(error),
               });
            }
         }
      }
      const verdicts = this.serveVerdicts(views);
      try {
         return await this.withCutoverPaths(verdicts);
      } catch (error) {
         logger.warn("Could not preview the v1 cutover's serve paths", {
            packageName: this.owner.packageName(),
            error: errMessage(error),
         });
         return verdicts;
      }
   }

   /**
    * The v1 cutover's serve path on each verdict (6e, plan question 15): the
    * served copy compiled over bindings declared from the plan's output
    * columns, `storage=` ones as {@link previewServeBindings} declares them
    * and colocated ones as the cutover binds them, names the sources it
    * re-roots; a rollup is the rollup leg's. Nothing is built or read.
    */
   private async withCutoverPaths(
      verdicts: ServeVerdict[],
   ): Promise<ServeVerdict[]> {
      const plan = this.buildPlan ?? this.servingPlanWhileUnavailable;
      const sources = plan?.sources ?? {};
      const eligible = this.colocatedSourceEligibility?.eligibleEntityIds;
      const viewBuilds = this.persistedViewBuildIDs();
      const entries: FreshnessManifest = {};
      for (const source of Object.values(sources)) {
         if (source.annotationFields?.storage?.trim()) continue;
         entries[source.sourceEntityId] = {
            tableName: `preview_${source.sourceEntityId}`,
            connectionName: source.connectionName,
         };
      }
      // A v1 `storage=` table stores the persisted query's own output (before
      // an extend block's `rename:` or `except:`, nests included), as the
      // bind will read it; a `#(materialize)` plan's columns are its own.
      const bindings = [
         ...this.previewServeBindings().map((b) => {
            const sourceID = b.sourceIDs?.[0];
            if (!sourceID || this.materializePlans?.[sourceID]) return b;
            const schema = v1StoredColumnsOf(this.declarationOf(sourceID));
            return schema?.length ? { ...b, schema } : b;
         }),
         ...deriveColocatedServeBindings({
            entries,
            planSources: sources,
            materializePlans: this.materializePlans,
            eligible: (sourceID) =>
               !!eligible?.has(sources[sourceID]?.sourceEntityId ?? ""),
            v1Schema: (sourceID) =>
               v1StoredColumnsOf(this.declarationOf(sourceID)),
            notV1: (sourceID) => viewBuilds.has(sourceID),
         }).map((b) => previewNativeSchema(b, sources)),
      ];
      const rerooted = new Set<string>();
      const throughBase = new Set<string>();
      const gateDeclined = new Set<string>();
      const problems = new Map<string, string>();
      for (const model of this.owner.models().values()) {
         const preview = await model.previewServedCopy?.(bindings);
         for (const id of preview?.rerooted ?? []) rerooted.add(id);
         for (const id of preview?.throughBase ?? []) throughBase.add(id);
         for (const id of preview?.gateDeclined ?? []) gateDeclined.add(id);
         for (const p of preview?.problems ?? []) {
            if (p.sourceID && !problems.has(p.sourceID)) {
               problems.set(p.sourceID, p.reason);
            }
         }
      }
      return verdicts.map((v) => {
         if (v.origin === "preaggregate") {
            return { ...v, cutoverPath: "rollup" };
         }
         // A row-level gate declared on a base the re-root replaces cannot
         // be grafted on the served copy: its queries are answered live.
         if (gateDeclined.has(v.sourceID)) {
            return {
               ...v,
               cutoverPath: "live",
               cutoverReason: "inherited_gate",
            };
         }
         if (rerooted.has(v.sourceID)) {
            return { ...v, cutoverPath: "served_copy" };
         }
         // An extension of a re-rooted source, left as written: it reads
         // that source's table by name.
         if (throughBase.has(v.sourceID) && !problems.has(v.sourceID)) {
            return {
               ...v,
               cutoverPath: "served_copy",
               cutoverReason: "through_base",
            };
         }
         return {
            ...v,
            cutoverPath: "live",
            cutoverReason:
               problems.get(v.sourceID) ?? v.reason ?? "not_rerooted",
         };
      });
   }

   /**
    * One serve binding per planned `storage=` source, as its first build would
    * bind it: declared from the plan's output columns at the DuckDB types the
    * build stores them as (see stored_column_types.ts). The table it names is
    * never read; a preview only compiles.
    */
   previewServeBindings(): ServeBinding[] {
      const out: ServeBinding[] = [];
      for (const [sourceID, source] of Object.entries(
         this.buildPlan?.sources ?? {},
      )) {
         const destinationName = source.annotationFields?.storage?.trim();
         if (!destinationName || source.origin === "preaggregate") continue;
         const schema = (source.columns ?? []).flatMap((column) => {
            const type = PREVIEW_STORED_TYPES[String(column.type ?? "")];
            return column.name && type ? [{ name: column.name, type }] : [];
         });
         if (schema.length === 0) continue;
         out.push({
            sourceName: source.name,
            sourceIDs: [sourceID],
            origin: "persist",
            destinationName,
            virtualHandle: source.sourceEntityId,
            tablePath: storageTablePath(
               destinationName,
               `preview_${source.sourceEntityId}`,
               this.serveDestinationSchema?.(destinationName),
            ),
            schema,
         });
      }
      return out;
   }

   /**
    * The publish warnings a serve verdict carries, from one verdict pass:
    * `unreachable` (`materialization-unreachable`: nothing on the published
    * surface reads the source, so its table is built for no query;
    * ms2data/service#6277), and the deployment facts (P13): a `storage=`
    * destination the environment does not configure
    * (`materialization-destination-missing`), or a colocated source on a
    * connection that cannot create tables
    * (`materialization-connection-cannot-hold-table`). Unservable, not
    * malformed: the package loads, the source serves live, and a deployment
    * warning clears once the deployment changes, with no reload. The other
    * live verdicts already carry a warning of their own (storage mode,
    * refusals, rollups over a stored base).
    */
   verdictWarnings(): ApiPackageWarning[] {
      // serveVerdicts degrades to none on a throw, so a package read never fails here.
      return this.serveVerdicts().flatMap((v) => {
         const code = v.reason ? VERDICT_WARNING_CODES[v.reason] : undefined;
         if (!code) return [];
         return [
            {
               model: v.modelPath ?? "",
               subject: v.sourceName,
               code,
               severity: "warn" as const,
               message: v.message + " " + (v.fixes ?? []).join(" "),
            },
         ];
      });
   }

   /** Memo for {@link entryPointIndex}, keyed on the models map it read. */
   private entryPointMemo:
      | { models: Map<string, Model>; index: EntryPointIndex | undefined }
      | undefined;

   /**
    * The package's entry points (what callers may query) and what each
    * reads, or undefined when that cannot be told. Memoized on the models
    * map, which a load or reload replaces wholesale along with the surface it
    * curates, so the status poll does no walk of its own.
    */
   private entryPointIndex(
      models: Map<string, Model>,
   ): EntryPointIndex | undefined {
      if (this.entryPointMemo?.models === models) {
         return this.entryPointMemo.index;
      }
      let index: EntryPointIndex | undefined;
      try {
         index = this.buildEntryPointIndex(models);
      } catch (error) {
         logger.debug("Could not read which sources the entry points reach", {
            packageName: this.owner.packageName(),
            error: errMessage(error),
         });
         index = undefined;
      }
      this.entryPointMemo = { models, index };
      return index;
   }

   /**
    * Each entry point a caller may query (a published source or named query,
    * or every model's when the package curates nothing), the sourceIDs it
    * reads, and whether it carries a row-level gate, the question routing
    * asks of a query's entry point. Undefined when a model has no compiled
    * definition: a false `unreachable` or `row_level_gate` is a false alarm,
    * so unknown stays quiet.
    */
   private buildEntryPointIndex(
      models: Map<string, Model>,
   ): EntryPointIndex | undefined {
      const surface = this.owner.publishedSurface?.();
      const entries =
         surface ??
         [...models].map(([modelPath, model]) => ({
            modelPath,
            names: (model.getSources?.() ?? []).flatMap((source) =>
               source.name ? [source.name] : [],
            ),
            queries: (model.getQueries?.() ?? []).flatMap((query) =>
               query.name ? [query.name] : [],
            ),
         }));
      const roots: EntryPointIndex["roots"] = [];
      for (const { modelPath, names, queries } of entries) {
         const model = models.get(modelPath);
         const modelDef = model?.getModelDef?.();
         if (!model || !modelDef) return undefined;
         const gatedSources = new Set(
            (model.getSources?.(true) ?? []).flatMap((source) =>
               source.name &&
               ((source.authorize?.length ?? 0) > 0 ||
                  (source.accessFilter?.length ?? 0) > 0)
                  ? [source.name]
                  : [],
            ),
         );
         const sourceRoot = (name: string, gated: boolean): void => {
            const entry = modelDef.contents?.[name];
            if (!entry || !isSourceDef(entry)) return;
            roots.push({
               gated,
               ids: reachableSourceIDs(
                  structsReachedFrom([entry], modelDef),
                  modelDef,
               ),
            });
         };
         for (const name of names) sourceRoot(name, gatedSources.has(name));
         // A published named query reads its source, and whatever its
         // pipeline names; it is gated as that source is.
         for (const name of queries ?? []) {
            const query = modelDef.contents?.[name] as
               | { type?: string; structRef?: unknown }
               | undefined;
            if (query?.type !== "query") continue;
            const ref = query.structRef;
            const base =
               typeof ref === "string"
                  ? modelDef.contents?.[ref]
                  : (ref as SourceDef | undefined);
            const ids = reachableSourceIDs([query], modelDef);
            if (base && isSourceDef(base)) {
               for (const id of reachableSourceIDs(
                  structsReachedFrom([base], modelDef),
                  modelDef,
               )) {
                  ids.add(id);
               }
            }
            roots.push({
               gated: typeof ref === "string" && gatedSources.has(ref),
               ids,
            });
         }
      }
      const reached =
         surface === undefined
            ? undefined
            : new Set(roots.flatMap((root) => [...root.ids]));
      return { roots, reached };
   }

   /**
    * The Malloy Persistence policy's REJECTION rules: a non-empty result fails a
    * publish and disarms the scheduler (warn-only at load). Advisory findings go
    * to materializationConfigWarnings. Cross-package scope rules are not
    * checked: the build plan is intra-package only.
    */
   persistencePolicyWarnings(): string[] {
      const warnings: string[] = [];
      const sources = this.buildPlan?.sources
         ? Object.values(this.buildPlan.sources)
         : [];
      const materialization = this.owner.metadata().materialization;
      const packageSchedule = materialization?.schedule ?? null;
      const packageFreshness = materialization?.freshness ?? null;
      const scope = this.owner.metadata().scope ?? "package";

      // Rule 1: per-source sharing/schedule are retired.
      for (const source of sources) {
         const fields = source.annotationFields ?? {};
         if (fields.sharing !== undefined) {
            warnings.push(
               `#@ persist source "${source.name}" declares sharing=... which is ` +
                  `no longer supported: scope is a single package-level mode. Set ` +
                  `"materialization": { "scope": "version" | "package" } in ` +
                  `${PACKAGE_MANIFEST_NAME} instead.`,
            );
         }
         if (fields.schedule !== undefined) {
            warnings.push(
               `#@ persist source "${source.name}" declares schedule=... which is ` +
                  `no longer supported: a schedule is package-root-only. Declare ` +
                  `"materialization.schedule" at the ${PACKAGE_MANIFEST_NAME} root ` +
                  `(requires "scope": "version") instead.`,
            );
         }
      }

      // Rule 2: a package-level cron is legal only with scope: version.
      if (packageSchedule && scope !== "version") {
         warnings.push(
            `materialization.schedule (cron) in ${PACKAGE_MANIFEST_NAME} requires ` +
               `"scope": "version": a package-scoped lineage is reused across ` +
               `versions, so a single per-version cadence is meaningless. Set ` +
               `"scope": "version", or remove the schedule.`,
         );
      }

      // Rule 3: schedule and freshness are mutually exclusive.
      if (packageSchedule) {
         const freshnessDeclared =
            !!(
               packageFreshness &&
               (packageFreshness.window || packageFreshness.fallback)
            ) || sources.some((s) => s.freshness != null);
         if (freshnessDeclared) {
            warnings.push(
               `materialization.schedule and freshness are mutually exclusive in ` +
                  `${PACKAGE_MANIFEST_NAME}: declare either a schedule (power tier) ` +
                  `or freshness (objective tier), never both.`,
            );
         }
      }

      // Rule 4: a valid 5-field cron, here so publish, PATCH, load and the
      // scheduler all apply one rule and a bad cron cannot silently never arm.
      if (packageSchedule && !new CronEvaluator().isValid(packageSchedule)) {
         warnings.push(
            `materialization.schedule in ${PACKAGE_MANIFEST_NAME} is not a valid ` +
               `5-field UNIX cron: ${JSON.stringify(packageSchedule)}. Use ` +
               `"minute hour day-of-month month day-of-week" (no L/W/#/? ` +
               `extensions).`,
         );
      }

      return warnings;
   }

   /**
    * The publish refusal for `#@` lines that do not parse, and for malformed
    * materialization v2 declarations (an unknown key on `#(materialize)` or
    * `#(preaggregate)`, whose key sets are closed), one per line, or "" when
    * none. Publish only: a load warns (the finding rides
    * `Package.warnings`), so a package already published keeps loading and its
    * schedule keeps running, and a metadata PATCH is not refused for it. Kept
    * out of {@link persistencePolicyWarnings}, which the scheduler and the
    * metadata PATCH both read.
    */
   formatUnparseableAnnotations(): string {
      return this.annotationFindings
         .filter(
            (finding) =>
               finding.code === "persist-unparseable" ||
               (finding.tier === "malformed" &&
                  MATERIALIZE_V2_ROUTES.has(finding.route)),
         )
         .map(
            (finding) =>
               `${finding.model ? `${finding.model}: ` : ""}${finding.message}`,
         )
         .join("\n");
   }

   /** The {@link persistencePolicyWarnings}, one per line, or "" if valid. */
   formatInvalidPersistencePolicy(): string {
      return this.persistencePolicyWarnings().join("\n");
   }

   /**
    * REJECTION messages for incremental-refresh declarations (rules in
    * incremental_policy). Unlike {@link persistencePolicyWarnings}, a non-empty
    * result also fails the LOAD.
    */
   incrementalPolicyWarnings(): string[] {
      return incrementalPolicyRejections(this.incrementalPolicySources);
   }

   /** The {@link incrementalPolicyWarnings}, one per line, or "" if valid. */
   formatInvalidIncrementalPolicy(): string {
      return this.incrementalPolicyWarnings().join("\n");
   }

   /**
    * REJECTION messages for `#@ preaggregate` declarations that cannot take
    * effect (rules in preaggregation_validation), strict at publish AND load:
    * pre-aggregation is invisible when it works, so an ignored declaration
    * would show correct numbers and no acceleration, with no one told.
    *
    * The access-modifier pair only warns: refusing would stop already-published
    * packages loading, and an inherited annotation on a hiding source yields
    * advice the author cannot follow. Warning is safe because the planner
    * itself skips hidden fields (planSourcePreaggregation), so nothing is built.
    */
   formatInvalidPreaggregatePolicy(): string {
      return this.preaggregateMessages(
         (code) => !PREAGG_WARN_ONLY.has(code),
      ).join("\n");
   }

   /** The access-modifier violations, surfaced at load as warnings. */
   preaggregateAccessWarnings(): ApiPackageWarning[] {
      return this.preaggregateFindings((code) =>
         PREAGG_WARN_ONLY.has(code),
      ).map(({ modelPath, violation }) => ({
         model: modelPath,
         subject: violation.fieldName ?? violation.sourceName,
         message: violation.message,
         code: "preaggregate-hidden-field",
         severity: "warn" as const,
      }));
   }

   private preaggregateMessages(
      keep: (code: PreaggregateViolationCode) => boolean,
   ): string[] {
      return this.preaggregateFindings(keep).map(
         // Source names repeat across models; name the file.
         ({ modelPath, violation }) => `${modelPath}: ${violation.message}`,
      );
   }

   /** Violations kept by `keep`, paired with their model, unformatted for both consumers. */
   private preaggregateFindings(
      keep: (code: PreaggregateViolationCode) => boolean,
   ): { modelPath: string; violation: PreaggregateViolation }[] {
      const findings: {
         modelPath: string;
         violation: PreaggregateViolation;
      }[] = [];
      for (const [modelPath, model] of this.owner.models()) {
         for (const violation of model.preaggregateViolations()) {
            if (keep(violation.code)) findings.push({ modelPath, violation });
         }
      }
      return findings;
   }

   /**
    * Within-package persist-target COLLISIONS: distinct sources (by
    * `sourceEntityId`) resolving to one physical table in one destination,
    * where each build would overwrite the other. Imports of the same source are
    * not flagged. The destination comes from the DECLARED annotation, so the
    * storage kill switch never hides a collision.
    *
    * A publish refusal unless `PERSIST_COLLISION_ENFORCE=false` opts out, and
    * a warning at load either way. Either way none of them is served
    * from the shared table (`sharedTables` in eligibility_collect.ts): it
    * holds whichever build ran last, so each answers live. Cross-version and
    * cross-package collisions need the host's global view.
    */
   persistenceCollisionWarnings(): string[] {
      const sources = this.buildPlan?.sources
         ? Object.values(this.buildPlan.sources)
         : [];
      const targets = new Map<
         string,
         { name: string; destination: string; sources: Map<string, string> }
      >();
      for (const source of sources) {
         // The one definition of a collision, shared with the serve refusal.
         const {
            key,
            name: physicalName,
            destination,
         } = persistTableTarget(
            source.annotationFields ?? {},
            source.name,
            source.connectionName,
         );
         const bucket = targets.get(key) ?? {
            name: physicalName,
            destination,
            sources: new Map<string, string>(),
         };
         // Keyed by sourceEntityId so identical-content imports collapse to one.
         bucket.sources.set(source.sourceEntityId, source.name);
         targets.set(key, bucket);
      }
      const warnings: string[] = [];
      for (const {
         name,
         destination,
         sources: colliding,
      } of targets.values()) {
         if (colliding.size < 2) continue;
         const names = [...colliding.values()].sort();
         warnings.push(
            `#@ persist sources ${names
               .map((n) => `"${n}"`)
               .join(", ")} all resolve to the same materialized table ` +
               `"${name}" in destination "${destination}". Distinct sources ` +
               `must not share a physical target — the second build would ` +
               `overwrite the first, and a serve binding or GC drop for one ` +
               `would affect the other, so none is served from it and queries ` +
               `on them are answered live. Give each a distinct #@ persist name=.`,
         );
      }
      return warnings;
   }

   /** Collisions as a publish rejection; "" when `PERSIST_COLLISION_ENFORCE=false`. */
   formatPersistenceCollisionRejections(): string {
      if (!getPersistCollisionEnforce()) return "";
      const collisions = this.persistenceCollisionWarnings();
      if (collisions.length === 0) return "";
      // The operator's opt-out, named once, after the author's fix.
      return [
         ...collisions,
         "A deployment that must publish such a package anyway can set " +
            "PERSIST_COLLISION_ENFORCE=false, which warns instead and serves " +
            "the colliding sources live.",
      ].join("\n");
   }
}

/** See PackageMaterialization's entry-point index. */
interface EntryPointIndex {
   /** Each entry point: the sourceIDs it reads, and whether a row-level gate sits on it. */
   roots: { gated: boolean; ids: Set<string> }[];
   /** Every sourceID an entry point reads; undefined when the package curates nothing. */
   reached: Set<string> | undefined;
}

/**
 * The DuckDB type a preview declares a stored column as, by its Malloy type:
 * what the build stores (stored_column_types.ts casts string, boolean and date
 * to these; numbers and timestamps already arrive as one). A column of any
 * other type is left off, and a view reading it previews as live.
 */
const PREVIEW_STORED_TYPES: Readonly<Record<string, string>> = {
   string: "VARCHAR",
   number: "DOUBLE",
   boolean: "BOOLEAN",
   date: "DATE",
   timestamp: "TIMESTAMP",
   timestamptz: "TIMESTAMP WITH TIME ZONE",
};

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

/**
 * A caller-safe sentence for why a served copy leaves a source unserved. The
 * compiler's own message stays in the logs: it can name the destination alias.
 */
/**
 * The compiler's own reasons in a compile error, as `: <reason>; <reason>.`:
 * each diagnostic's message without its file and source excerpt, so a package
 * warning names why without the server's paths. A reason that names a
 * served-copy alias (a join refused across connections) is left out: the
 * alias stays in the server log. `.` when there are none.
 */
function compilerReasons(detail: string | undefined): string {
   const reasons = [...(detail ?? "").matchAll(/^line \d+: (.+)$/gm)]
      .map((m) => m[1].trim())
      .filter((reason) => !namesServedCopyAlias(reason));
   return reasons.length > 0 ? `: ${reasons.join("; ")}.` : ".";
}

function servedCopyProblemMessage(
   problem: ServedCopyProblem,
   inlinedButKept = false,
): string {
   const { reason, join } = problem;
   switch (reason) {
      case "compile_failed":
         return "This model's served copy does not compile, so its stored sources serve through the serve shape or live.";
      case "cross_connection_unbuilt":
         return "This model joins sources from different connections; its served copy compiles once both sides are built into one destination, and serves live until then.";
      case "column_types":
         return "This source's stored column types differ from the types the model declares, or one cannot be declared on its table, so the served copy leaves it to the serve shape.";
      case "no_declaration":
         return "This source's declaration was not found in the compiled model, so the served copy leaves it to the serve shape.";
      case "range_not_found":
      case "definition_not_parsed":
         return "This source's declaration could not be re-rooted on its table, so the served copy leaves it to the serve shape.";
      case "pair_join_not_found":
         return "This source inlines a join_many that is not declared on its own in its own trailing `extend` (a source it is built from declares it, or its `join_many:` statement declares other joins too), so its pair table cannot be read with the join as a record and the served copy leaves it as written.";
      case "pair_not_compiled":
         return "This source's pair table (an inlined join_many) could not be read as the source with its join as a record (its text reads a measure or view of the joined source through the join, which the record does not carry), so the served copy leaves it as written.";
      case "reroot_not_compiled":
         return (
            "This source's re-root on its stored table does not compile in the served copy (the server log has the compiler's message)." +
            " The served copy leaves it as written, so it answers live, and the model's other stored sources still serve. This is a Publisher bug; please report it."
         );
      case "read_terms_not_kept":
         return "This source filters by a given in a place its stored table cannot keep, or is filtered and reached here only through a join, so the served copy leaves it as written rather than answer every caller alike.";
      case "fields_not_carried":
         return "This source inherits fields (a measure, view or join declared on the source it is built from) that its stored table does not carry, so the served copy leaves it as written.";
      case "cross_connection_join":
         return (
            `This source is stored in a destination its join${join ? ` \`${join}\`` : ""} cannot reach: ` +
            "the joined source reads another connection. Queries that use the join are answered live; " +
            "the others read the stored table. To serve them too, " +
            (inlinedButKept
               ? "fix what keeps the inlined join from being answered from its stored columns (the warning on " +
                 "this join says what), materialize the joined source into the same destination, or keep this " +
                 "source colocated."
               : "inline the join (inline=), materialize the joined source into the same destination, or keep " +
                 "this source colocated.")
         );
      case "inline_join_kept":
         return (
            `This source inlines its join${join ? ` \`${join}\`` : ""} (inline=), but its served copy does not compile ` +
            "with the join answered from the stored columns, so the join is kept as written and every query " +
            "through it reads the joined source through the join" +
            compilerReasons(problem.detail) +
            " A definition that reads the joined source past one hop (a join of the joined source), or reads " +
            "one of its measures or views, is the usual cause."
         );
      case "classification_not_kept":
         return (
            "This source's served copy reaches fields its compile of record does not (a join left out " +
            "across connections, answered once both sides are built), and the load's classification " +
            "checks flag them there, so its queries are not answered from the stored tables: " +
            [...(problem.leaks ?? []), ...(problem.coverage ?? [])].join(" ")
         );
   }
}

/** The colocated bindings' identity, to push only when they change. */
function colocatedSignature(bindings: readonly ServeBinding[]): string {
   return bindings
      .map(
         (b) =>
            `${b.sourceIDs?.join(",")}@${b.destinationName}/${b.virtualHandle}/${b.tablePath}/${b.freshAsOf ?? ""}/${b.freshnessWindowSeconds ?? ""}/${b.freshnessFallback ?? ""}`,
      )
      .sort()
      .join("|");
}

/**
 * A dry run's colocated binding with the warehouse types a build would
 * capture (`ManifestEntry.nativeSchema`), so it predicts the cast a big-integer
 * column is read through: each column as the dialect's own type for a
 * `bigint` number. A preview has no table to read; the real build's capture
 * can only be as wide or wider, and a type Malloy does not read as `bigint`
 * answers that column live, which no dry run can foresee.
 */
function previewNativeSchema(
   binding: ServeBinding,
   sources: Readonly<Record<string, { dialect?: string }>>,
): ServeBinding {
   const dialect = sources[binding.sourceIDs?.[0] ?? ""]?.dialect;
   if (!binding.colocated || !dialect) return binding;
   let type: string;
   try {
      type = getDialect(dialect).malloyTypeToSQLType({
         type: "number",
         numberType: "bigint",
      });
   } catch {
      return binding;
   }
   return {
      ...binding,
      nativeSchema: binding.schema.map((c) => ({ name: c.name, type })),
   };
}
