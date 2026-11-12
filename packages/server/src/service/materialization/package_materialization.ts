// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   isSourceDef,
   type MalloyConfig,
   type SourceDef,
} from "@malloydata/malloy";
import {
   findingSeverity,
   type PackageAnnotationFinding,
} from "../annotation_reader";
import { components } from "../../api";
import {
   getPersistCollisionEnforce,
   getPersistStorageMode,
} from "../../config";
import { PACKAGE_MANIFEST_NAME } from "../../constants";
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
import { STORED_COLUMN_TYPE_MISMATCH_CODE } from "./stored_column_types";
import {
   computeServeVerdicts,
   MATERIALIZATION_UNREACHABLE_CODE,
   type ServeVerdict,
   sourceIDsReachedFrom,
   type ViewServeVerdict,
} from "./serve/serve_verdict";
import { storageTablePath } from "./run/storage_table_path";
import { structsReachedFrom } from "../governance/ir/source_graph";
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
 * A package's materialization state: its build plan and which sources may be
 * served from a materialized table, the manifest it is bound to (both tiers),
 * the serve routing it pushes onto its models, and the persistence-policy
 * findings a publish and a load judge it by. Held by `Package`, which
 * delegates to it only what callers outside this module use.
 */
export class PackageMaterialization {
   /** The environment's storage destinations for a serve-shape compile; see {@link setServeDestinationConfig}. */
   private serveDestinationConfig?: () => MalloyConfig;
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
   /** The build plan could not be derived at load (see applyLoadBuildPlan). */
   private buildPlanFailed = false;
   /** The plan re-derivation in flight after a connection edit, if any. */
   private buildPlanRefresh: Promise<void> | undefined;
   // Keyed on the plan it came from, so a reload recomputes it.
   private preaggregateEntityIdCache:
      | { plan: BuildPlan | null; ids: ReadonlySet<string> }
      | undefined;
   // Keyed on the plan, the bound entries and the live verdicts it came from.
   private tableCatalogCache:
      | {
           plan: BuildPlan | null;
           bound: FreshnessManifest | undefined;
           liveVerdicts: ReadonlyMap<string, LiveTableVerdict> | undefined;
           catalog: MaterializedTableCatalog;
        }
      | undefined;
   // Keyed on every fact a verdict reads that is not the clock.
   private liveVerdictCache:
      | {
           key: readonly unknown[];
           byTable: ReadonlyMap<string, LiveTableVerdict> | undefined;
        }
      | undefined;
   // `#@ persist` sources Malloy's build walk did not plan, which
   // would silently serve live. Warned, and refused at build.
   private droppedPersistSources: { name: string; modelPath: string }[] = [];
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
         } = workerPlan
            ? workerPlan.outcome
            : await computePackageBuildPlan(this.owner.buildPlanPackage);
         this.buildPlan = plan;
         this.buildPlanFailed = false;
         this.droppedPersistSources = droppedPersistSources;
         this.rollupsOverStoredBase = rollupsOverStoredBase ?? [];
         // `?? []`: an outcome a worker built before this field existed.
         this.annotationFindings = annotationFindings ?? [];
         this.sourceEligibility = sourceEligibility;
         this.colocatedSourceEligibility = colocatedSourceEligibility;
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
         // Nothing from the previous load describes this one.
         this.rollupsOverStoredBase = [];
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
         });
      }
      const bound: Record<string, BoundTable> = {};
      for (const [sourceEntityId, entry] of Object.entries(
         this.freshnessEntries ?? {},
      )) {
         bound[sourceEntityId] = {
            tableName: entry.tableName,
            ...(entry.dataAsOf ? { dataAsOf: entry.dataAsOf } : {}),
         };
      }
      const catalog: MaterializedTableCatalog = {
         planned,
         bound,
         ...(liveVerdicts ? { liveVerdicts } : {}),
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
    * (each replaced wholesale) and costs a query only a key compare.
    */
   private liveVerdictsByTable():
      | ReadonlyMap<string, LiveTableVerdict>
      | undefined {
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
      ];
      const cached = this.liveVerdictCache;
      if (cached && cached.key.every((part, i) => part === key[i])) {
         return cached.byTable;
      }
      let byTable: Map<string, LiveTableVerdict> | undefined;
      if (this.buildPlan) {
         const sources = this.buildPlan.sources ?? {};
         const perTable = new Map<string, ServeVerdict[]>();
         for (const verdict of this.serveVerdicts()) {
            const entityId = sources[verdict.sourceID]?.sourceEntityId;
            if (!entityId) continue;
            const key = liveVerdictKey(verdict.tier, entityId);
            const list = perTable.get(key) ?? [];
            list.push(verdict);
            perTable.set(key, list);
         }
         byTable = new Map();
         for (const [tableKey, verdicts] of perTable) {
            const live = verdicts.every(
               (v) => v.verdict === "live" && v.reason && v.reason !== "stale",
            );
            if (!live) continue;
            const first = verdicts[0];
            const plan = sources[first.sourceID];
            byTable.set(tableKey, {
               reason: first.reason as string,
               sourceName:
                  plan?.preaggregate?.baseSourceName ?? first.sourceName,
            });
         }
      }
      this.liveVerdictCache = { key, byTable };
      return byTable;
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
      for (const [sourceEntityId, entry] of Object.entries(entries)) {
         if (eligibleEntityIds?.has(sourceEntityId)) {
            allowed[sourceEntityId] = entry;
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
    * A published version also binds only the tables its own definition built.
    * Storage bindings are keyed by source NAME, and the versions of a
    * `scope: package` package share its tables under unchanged names, so an
    * entry whose `sourceEntityId` none of this version's same-named sources
    * has was built by another version's definition, and that source serves
    * live. (The colocated tier is keyed by content address already.)
    */
   bindStorageServeBindings(entries: Record<string, ManifestEntry>): void {
      const derived = deriveServeBindings(
         this.owner.versionId() === undefined
            ? entries
            : this.ownEntries(entries),
         groupAliasesByName(Object.values(this.buildPlan?.sources ?? {})),
         this.serveDestinationSchema,
         planSourceIds(this.buildPlan?.sources ?? {}),
      );
      const eligibility = this.sourceEligibility;
      // Keyed by NAME: a rollup passes only because `collectSourceEligibility`
      // also records the synthesized rollups under their generated names.
      // Scoping it to authored sources would silently switch off every storage
      // rollup.
      const eligible = new Set(eligibility?.eligible ?? []);
      const allowed = derived.filter((binding) => {
         if (eligible.has(binding.sourceName)) return true;
         const reason =
            eligibility === undefined
               ? "the package build plan could not be computed, so no source was examined for eligibility"
               : (eligibility.refused[binding.sourceName] ??
                 "the source is not in the package build plan, so it was never examined (a `#@ persist` on a non-query-shaped source is dropped)");
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
         return false;
      });
      this.storageServeBindings = allowed;
      this.pushStorageServeBindingsToModels();
   }

   /**
    * The entries this version's own sources built: same name, same content
    * address. See {@link bindStorageServeBindings}.
    */
   private ownEntries(
      entries: Record<string, ManifestEntry>,
   ): Record<string, ManifestEntry> {
      const own = new Set(
         Object.values(this.buildPlan?.sources ?? {}).map(
            (source) => `${source.name}\u0000${source.sourceEntityId}`,
         ),
      );
      const kept: Record<string, ManifestEntry> = {};
      for (const [key, entry] of Object.entries(entries)) {
         if (own.has(`${entry.sourceName}\u0000${entry.sourceEntityId}`)) {
            kept[key] = entry;
            continue;
         }
         logger.info(
            "Serving a source live: its stored table was built from another version's definition",
            {
               packageName: this.owner.packageName(),
               versionId: this.owner.versionId(),
               sourceName: entry.sourceName,
            },
         );
      }
      return kept;
   }

   /**
    * Compile each model's pre-aggregation companion so the serve path can route
    * to a rollup. Re-run after a manifest bind, since the companion compiles
    * against the manifest. A no-op for a model with no `#@ preaggregate`.
    */
   async pushPreaggregateServeModels(): Promise<void> {
      for (const model of this.owner.models().values()) {
         await model.buildPreaggregateServeModel(
            this.owner.packagePath(),
            this.owner.malloyConfig(),
            this.buildManifestEntries,
         );
      }
   }

   /** Push the current storage serve bindings onto every loaded model. */
   pushStorageServeBindingsToModels(): void {
      for (const model of this.owner.models().values()) {
         model.setServeBindings(this.storageServeBindings);
      }
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
    * Re-derive the build plan from the connections as they are now. Every
    * content address folds in its connection's digest, and the digest reads
    * connection config (host, database, project, role, the setup SQL a
    * statement timeout is applied through, …), so a connection edit can move
    * the address of every source on it. The plan computed at load would keep
    * handing out the old one: a host would echo ledger entries recorded under
    * it, they would pass create-time validation against that same stale plan,
    * and the run's own compile, made with the new connection, would refuse
    * them as "measured under a different definition" (ms2data/service#6145).
    *
    * Serialized: an edit landing while a refresh runs re-derives again after
    * it. Best-effort like the load: a failure marks the plan unavailable.
    */
   refreshBuildPlan(): Promise<void> {
      const previous = this.buildPlanRefresh ?? Promise.resolve();
      const run = previous.then(async () => {
         await this.applyLoadBuildPlan(undefined);
      });
      const tracked = run.finally(() => {
         if (this.buildPlanRefresh === tracked)
            this.buildPlanRefresh = undefined;
      });
      this.buildPlanRefresh = tracked;
      return tracked;
   }

   /** Resolves once no plan re-derivation is in flight (see refreshBuildPlan). */
   whenBuildPlanCurrent(): Promise<void> {
      return this.buildPlanRefresh ?? Promise.resolve();
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
            `base's stored table instead.`,
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
    * Warnings for the `#@ persist` sources the compile-time gates refused,
    * carrying the gate's message verbatim so there is one wording to maintain.
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
         const warning: ApiPackageWarning = { message: source.message };
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
      const servedStorageNames = new Set<string>();
      for (const model of models.values()) {
         for (const name of model.servedStorageSourceNames?.() ?? []) {
            servedStorageNames.add(name);
         }
      }
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
         refusedAtBind: (_sourceID, source, tier) =>
            this.refusedAtBind(source, tier),
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
         views,
      });
   }

   /**
    * Why the bind refuses a planned source, as {@link bindStorageServeBindings}
    * and {@link bindColocatedServeManifest} decide it, or undefined when it
    * keeps it. Storage requires a positive eligibility, as the bind does.
    * Colocated rollups need not appear in the colocated record, so only an
    * authored source missing from it, or one it names as refused, counts.
    */
   private refusedAtBind(
      source: { name: string; sourceEntityId: string; origin?: string },
      tier: "colocated" | "storage",
   ): string | undefined {
      if (tier === "storage") {
         const eligibility = this.sourceEligibility;
         if (!eligibility || eligibility.eligible.includes(source.name)) {
            return undefined;
         }
         return (
            eligibility.refused[source.name] ??
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
      return this.serveVerdicts(views);
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
    * Warnings for materialized sources whose verdict is `unreachable`: nothing
    * on the published surface reads them, so their tables are built for no
    * query (ms2data/service#6277). The other live verdicts already carry a
    * warning of their own (storage mode, refusals, rollups over a stored base).
    */
   unreachableMaterializationWarnings(): ApiPackageWarning[] {
      // serveVerdicts degrades to none on a throw, so a package read never fails here.
      return this.serveVerdicts()
         .filter((v) => v.reason === "unreachable")
         .map((v) => ({
            model: v.modelPath ?? "",
            subject: v.sourceName,
            code: MATERIALIZATION_UNREACHABLE_CODE,
            severity: "warn" as const,
            message: v.message + " " + (v.fixes ?? []).join(" "),
         }));
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
               ids: sourceIDsReachedFrom(structsReachedFrom([entry], modelDef)),
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
            const ids = sourceIDsReachedFrom([query]);
            if (base && isSourceDef(base)) {
               for (const id of sourceIDsReachedFrom(
                  structsReachedFrom([base], modelDef),
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
    * The publish refusal for `#@` lines that do not parse, one per line, or ""
    * when none. Publish only: a load warns (the finding rides
    * `Package.warnings`), so a package already published keeps loading and its
    * schedule keeps running, and a metadata PATCH is not refused for it. Kept
    * out of {@link persistencePolicyWarnings}, which the scheduler and the
    * metadata PATCH both read.
    */
   formatUnparseableAnnotations(): string {
      return this.annotationFindings
         .filter((finding) => finding.code === "persist-unparseable")
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
    * Warn-only unless `PERSIST_COLLISION_ENFORCE` is set, so packages published
    * before this check still republish. Cross-version and cross-package
    * collisions need the host's global view.
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
         const fields = source.annotationFields ?? {};
         const physicalName = (fields.name || source.name).trim();
         const storage = fields.storage?.trim();
         const destination = storage ? storage : source.connectionName;
         // Verbatim key: names differing only by case or quoting may still fold
         // to one table on some destinations. Normalizing is dialect-dependent,
         // so the host's naming is the backstop.
         const key = `${destination} ${physicalName}`;
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
               `would affect the other. Give each a distinct #@ persist name=.`,
         );
      }
      return warnings;
   }

   /** Collisions as a publish rejection; "" unless `PERSIST_COLLISION_ENFORCE` is set. */
   formatPersistenceCollisionRejections(): string {
      if (!getPersistCollisionEnforce()) return "";
      return this.persistenceCollisionWarnings().join("\n");
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
