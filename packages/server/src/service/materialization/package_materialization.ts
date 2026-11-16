// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The package facade: one package version's materialization, composing
 * plan/, run/ and serve/ for `Package` and `Environment`, and the module's
 * one door for them. It holds the version's bindings and snapshot itself, and
 * delegates the rest to three collaborators:
 *
 * | Concern | Held by |
 * | --- | --- |
 * | The plan: load, stale, refresh, when current | {@link PlanLifecycle} (`plan_lifecycle.ts`) |
 * | Serve verdicts, the preview, the path report's catalog | {@link ServeVerdicts} (`serve_verdicts.ts`) |
 * | Package warnings and publish rejections | `package_warnings.ts` |
 * | Bind a manifest, swap the snapshot, push it to the models | this facade |
 *
 * Effects: logs, and the bind metrics.
 */

import type { MalloyConfig } from "@malloydata/malloy";
import { components } from "../../api";
import type { ChainBuildRequest, ChainBuildText } from "./domain/chain_build";
import {
   getMaterializeEnabled,
   getPersistCollisionEnforce,
   getPersistStorageMode,
} from "../../config";
import { logger } from "../../logger";
import {
   recordSourceBound,
   recordSourceDeclined,
} from "../../materialization_metrics";
import type { LoadPackageOutcome } from "../../package_load/package_load_pool";
import type { Model } from "../model";
import type { PackageModelPolicy } from "../model_policy";
import type { BuildPlanPackage } from "./plan/build_plan";
import {
   collisionGateLines,
   incrementalGateLines,
   invalidFindingLines,
   planCollisions,
} from "./plan/load_gate_text";
import {
   incrementalPolicyAdvisories,
   incrementalPolicyRejections,
} from "./plan/incremental_policy";
import {
   type Binding,
   type BindingKind,
   makeSnapshot,
   pin,
   type Snapshot,
} from "./domain/binding";
import {
   bindColocatedEntries,
   bindStorageEntries,
   deriveColocatedServeBindings,
   kindFromPlan,
   v1StoredColumnsOf,
} from "./serve/bind";
import type { ServeVerdict, ViewServeVerdict } from "./serve/serve_verdict";
import * as warnings from "./package_warnings";
import { PlanLifecycle } from "./plan_lifecycle";
import { type DeploymentFacts, ServeVerdicts } from "./serve_verdicts";
import { SourceMap } from "./text/source_map";
import type {
   BuildManifest,
   BuildPlan,
   FreshnessManifest,
   ManifestEntry,
} from "./domain/types";

type ApiPackage = components["schemas"]["Package"];
type ApiPackageWarning = NonNullable<ApiPackage["warnings"]>[number];

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

/** The facade as `Package.materialization()` hands it out: without the plan's state. */
export type PackageMaterializationView = Omit<PackageMaterialization, "plan">;

export class PackageMaterialization {
   /**
    * The plan, and everything compiled beside it. Read by the facade's
    * collaborators; `Package`'s test hooks set its eligibility through it,
    * and `Package.materialization()` leaves it out.
    */
   readonly plan: PlanLifecycle;
   /** The serve verdicts over the plan and the bindings here. */
   readonly verdicts: ServeVerdicts;
   /** The environment's storage destinations for a serve-shape compile. */
   private serveDestinationConfig?: () => MalloyConfig;
   /** The environment's deployment facts, read live. */
   private deploymentFacts?: DeploymentFacts;
   /** A destination's schema by name, resolved live. */
   private serveDestinationSchema?: (
      destinationName: string,
   ) => string | undefined;
   /**
    * The colocated manifest entries bound now, freshness fields and key
    * checks included: the snapshot's `entries`. Undefined when serving live.
    */
   private colocatedEntries: FreshnessManifest | undefined;
   /**
    * The same entries as table names only, the manifest models hydrate with
    * and /compile reads. Surfaced on /status so the control plane can confirm
    * a worker bound it.
    */
   private buildManifestEntries: BuildManifest["entries"] | undefined;
   /** The `storage=` bindings a host's or a run's manifest made. */
   private storageBindings: readonly Binding[] = [];
   /** {@link snapshot}, keyed on every fact it is made from. */
   private snapshotMemo:
      | { key: readonly unknown[]; snapshot: Snapshot; signature: string }
      | undefined;
   /** The bindings the models last compiled their served copies over. */
   private publishedBindings: readonly Binding[] | undefined;
   private manifestBindingStatus: "unbound" | "bound" | "live_fallback" =
      "unbound";
   private manifestEntryCount = 0;
   private boundManifestUri: string | null = null;
   private preaggregateEntityIdCache:
      | { plan: BuildPlan | null; ids: ReadonlySet<string> }
      | undefined;

   constructor(private readonly owner: PackageMaterializationOwner) {
      this.plan = new PlanLifecycle(owner, () => this.publish());
      this.verdicts = new ServeVerdicts({
         plan: this.plan,
         packageName: () => owner.packageName(),
         models: () => owner.models(),
         publishedSurface: owner.publishedSurface
            ? () => owner.publishedSurface?.()
            : undefined,
         snapshot: () => this.snapshot(),
         storageBindings: () => this.storageBindings,
         colocatedEntries: () => this.colocatedEntries,
         deploymentFacts: () => this.deploymentFacts,
         destinationSchema: (name) => this.serveDestinationSchema?.(name),
         declarationOf: this.declarationLookup,
         persistedViewBuildIDs: () => this.persistedViewBuildIDs(),
      });
   }

   // ---- The plan (PlanLifecycle) ------------------------------------------

   getBuildPlan(): BuildPlan | null {
      return this.plan.buildPlan;
   }

   /** Whether the build plan could not be derived, so `getBuildPlan` says nothing. */
   isBuildPlanUnavailable(): boolean {
      return this.plan.failed;
   }

   /** See {@link PlanLifecycle.load}. */
   applyLoadBuildPlan(
      workerPlan: LoadPackageOutcome["buildPlan"],
   ): Promise<number | undefined> {
      return this.plan.load(workerPlan);
   }

   /** See {@link PlanLifecycle.markStale}. */
   markBuildPlanStale(changedConnections: Iterable<string> = []): void {
      this.plan.markStale(changedConnections);
   }

   lastBuildPlanRefreshFailedTransiently(): boolean {
      return this.plan.lastRefreshFailedTransiently();
   }

   isBuildPlanStale(): boolean {
      return this.plan.isStale();
   }

   /** See {@link PlanLifecycle.refresh}; publishes the snapshot after. */
   refreshBuildPlan(
      derive: () => Promise<LoadPackageOutcome["buildPlan"]>,
      options: { retryFailed?: boolean; isStale?: () => boolean } = {},
   ): Promise<boolean> {
      return this.plan.refresh(derive, options);
   }

   /** See {@link PlanLifecycle.whenCurrent}. */
   whenBuildPlanCurrent(timeoutMs?: number): Promise<void> {
      return this.plan.whenCurrent(timeoutMs);
   }

   /** See {@link PlanLifecycle.connectionNames}. */
   planConnectionNames(): ReadonlySet<string> | null {
      return this.plan.connectionNames();
   }

   // ---- Bindings and the snapshot -----------------------------------------

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
         storageServeBindings: this.storageBindings.map((b) => ({
            origin: b.kind === "rollup" ? "preaggregate" : "persist",
            sourceName: b.sourceName,
            storageDestinationName: b.destinationName,
            tablePath: b.tablePath,
         })),
      };
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

   /** Whether `storage=` serve bindings exist, so a refresh whose entries vanished still clears them. */
   hasStorageServeBindings(): boolean {
      return this.storageBindings.length > 0;
   }

   /**
    * The colocated entries a query may read at `now` (fresh, un-gated, or
    * stale under `stale_ok`), as table names: what /compile reads. Undefined
    * when nothing is bound.
    */
   getFreshBuildManifest(
      now: number = Date.now(),
   ): BuildManifest["entries"] | undefined {
      if (!this.colocatedEntries) return undefined;
      return pin(this.snapshot(), new Date(now)).compilerManifest.entries;
   }

   /** Point every model's policy at this package's snapshot; idempotent. */
   wireServingResolvers(): void {
      this.owner.modelPolicy().setServingResolvers({
         snapshot: () => this.snapshot(),
         preaggregateEntityIds: () => this.getPreaggregateEntityIds(),
         materializedTables: () => this.verdicts.tableCatalog(),
      });
   }

   /**
    * Every table this package serves now, as one immutable snapshot: the
    * `storage=` bindings, each kind read off the current plan, the colocated
    * tables the served copy reads, and the colocated entries. Memoized on
    * every fact it is made from, and the bindings array is kept while they
    * do not change, so caches keyed by it survive an unrelated rebind.
    */
   snapshot(): Snapshot {
      const plan = this.plan;
      const key = [
         this.storageBindings,
         this.colocatedEntries,
         plan.buildPlan,
         plan.servingPlan(),
         plan.materializePlans,
         plan.colocatedSourceEligibility,
         this.owner.models(),
         getMaterializeEnabled(),
      ];
      const memo = this.snapshotMemo;
      if (memo && memo.key.every((part, i) => part === key[i])) {
         return memo.snapshot;
      }
      const kindOf = this.bindingKinds();
      const bindings = [
         ...this.storageBindings.map((b) => {
            const kind = kindOf(b);
            return kind === b.kind ? b : { ...b, kind };
         }),
         ...this.colocatedServeBindings(),
      ];
      const signature = JSON.stringify(bindings);
      const snapshot = makeSnapshot(
         memo && memo.signature === signature
            ? memo.snapshot.bindings
            : Object.freeze(bindings),
         this.colocatedEntries ?? {},
      );
      this.snapshotMemo = { key, snapshot, signature };
      return snapshot;
   }

   /**
    * The host port's `chainBuildText` for this package version: the text a
    * chained `storage=` build of the request's downstream compiles from,
    * written by the served copy of a model that declares it, the downstream's
    * own file first. Declines when no model of the package declares it.
    */
   chainBuildText(request: ChainBuildRequest): ChainBuildText {
      const declaringURL = request.downstream.sourceID
         .split("@")
         .slice(1)
         .join("@");
      const models = [...this.owner.models()].sort(
         ([a], [b]) =>
            Number(declaringURL.endsWith(`/${b}`)) -
            Number(declaringURL.endsWith(`/${a}`)),
      );
      for (const [, model] of models) {
         const text = model.chainBuildText(request);
         if (text !== undefined) return text;
      }
      return { declined: "no model of this package declares the downstream" };
   }

   /**
    * The `sourceEntityId`s pre-aggregation synthesized, read off the plan's
    * `origin` (never the rollup naming convention, which an author's source
    * could share). Memoized on plan identity; the plan is replaced wholesale.
    */
   getPreaggregateEntityIds(): ReadonlySet<string> {
      const plan = this.plan.buildPlan;
      if (this.preaggregateEntityIdCache?.plan === plan) {
         return this.preaggregateEntityIdCache.ids;
      }
      const ids = new Set<string>();
      for (const source of Object.values(plan?.sources ?? {})) {
         if (source.origin === "preaggregate" && source.sourceEntityId) {
            ids.add(source.sourceEntityId);
         }
      }
      this.preaggregateEntityIdCache = { plan, ids };
      return ids;
   }

   /**
    * Record that `uri`'s manifest is bound. Asserted rather than derived from
    * the entry count (a bind may carry no colocated entries); called after both
    * tiers apply.
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

   /**
    * Bind a manifest, either half or both, and swap the snapshot every model
    * reads by one reference: a query already routing keeps the one it
    * pinned.
    *
    * - `colocated`: the colocated entries, filtered POSITIVELY against the
    *   colocated eligibility (a miss is dropped, not fatal) and quoted for
    *   their dialects; an empty map reverts that tier to live. With
    *   `colocatedAsGiven`, recorded as given: a reload that recompiled every
    *   model against them, which relies on the per-query `#(authorize)`
    *   graft, not on this filter.
    * - `storage`: the `storage=` entries a host vouches for, minus any this
    *   package's eligibility gate refuses and any built from another
    *   definition of their source. The host knows WHICH table backs a source,
    *   not WHETHER a frozen table may serve it: only a compile knows that.
    */
   bind(update: {
      colocated?: FreshnessManifest;
      colocatedAsGiven?: boolean;
      storage?: Record<string, ManifestEntry>;
      /** Models a reload just installed: publish to them even when the bindings did not move. */
      models?: "installed";
   }): void {
      if (update.colocated) {
         this.adoptColocated(
            update.colocatedAsGiven
               ? update.colocated
               : this.admitColocated(update.colocated),
         );
      }
      if (update.storage) {
         this.storageBindings = this.admitStorage(update.storage);
      }
      this.publish(update.models);
   }

   /** The colocated entries bound now, by identity: a bind replaces them wholesale. */
   boundColocatedEntries(): FreshnessManifest | undefined {
      return this.colocatedEntries;
   }

   /**
    * Drop the rollup key checks (known limitation 29) bound for these content
    * addresses: a run is about to write their tables, so what a check found
    * over the rows before no longer describes them. The routing itself is
    * kept; the join-to-rollup branch reads the table as unknown until a run
    * binds a new check.
    */
   forgetKeyUniqueness(sourceEntityIds: Iterable<string>): void {
      const ids = new Set(sourceEntityIds);
      // A stored table's own primary key check, and a stored rollup's key
      // check, on its storage binding.
      const checked = (b: Binding) =>
         (b.primaryKeys || b.rollupKey) && ids.has(b.virtualHandle);
      if (this.storageBindings.some(checked)) {
         this.storageBindings = this.storageBindings.map((b) => {
            if (!checked(b)) return b;
            const {
               primaryKeys: _forgotten,
               rollupKey: _forgottenKey,
               ...rest
            } = b;
            return rest;
         });
      }
      const entries = this.colocatedEntries;
      let next: FreshnessManifest | undefined;
      for (const id of ids) {
         const entry = entries?.[id];
         if (!entry?.keyUniqueness) continue;
         next ??= { ...entries };
         const { keyUniqueness: _forgotten, ...rest } = entry;
         next[id] = rest;
      }
      // A colocated pair table's leaf key check rides on its binding
      // (`primaryKeys`, known limitation 40): the snapshot drops it too.
      if (next) this.colocatedEntries = next;
      this.publish();
   }

   /** Store the colocated entries; an empty map reverts to live (unbound). */
   private adoptColocated(entries: FreshnessManifest): void {
      const count = Object.keys(entries).length;
      this.colocatedEntries = count > 0 ? entries : undefined;
      this.buildManifestEntries =
         count > 0 ? toTableNameManifest(entries) : undefined;
      this.manifestEntryCount = count;
      this.manifestBindingStatus = count > 0 ? "bound" : "unbound";
      if (count === 0) {
         this.boundManifestUri = null;
      }
   }

   /** The colocated entries the bind admits, logging and counting the rest. */
   private admitColocated(entries: FreshnessManifest): FreshnessManifest {
      const eligibility = this.plan.colocatedSourceEligibility;
      const dialects = new Map<string, string>();
      for (const source of Object.values(this.plan.buildPlan?.sources ?? {})) {
         if (source.sourceEntityId && source.dialect) {
            dialects.set(source.sourceEntityId, source.dialect);
         }
      }
      const admitted = bindColocatedEntries({ entries, eligibility, dialects });
      for (const _entry of Object.keys(admitted.entries)) recordSourceBound();
      for (const decline of admitted.declined) {
         recordSourceDeclined(decline.reason);
         logger.warn(
            "Refusing a colocated serve binding: the source is not eligible to be served from a materialized table",
            {
               packageName: this.owner.packageName(),
               sourceEntityId: decline.sourceEntityId,
               reason: decline.detail,
            },
         );
      }
      if (eligibility === undefined && admitted.declined.length > 0) {
         // The whole default tier just reverted to live, not one source.
         logger.warn(
            "Colocated serve bindings dropped package-wide: the build plan never computed, so no source could be examined for eligibility",
            {
               packageName: this.owner.packageName(),
               droppedCount: admitted.declined.length,
            },
         );
      }
      return admitted.entries;
   }

   /** The `storage=` bindings the bind admits, logging the rest. */
   private admitStorage(entries: Record<string, ManifestEntry>): Binding[] {
      const admitted = bindStorageEntries({
         entries,
         plan: this.plan.servingPlan(),
         destinationSchema: this.serveDestinationSchema,
         eligibility: this.plan.sourceEligibility,
         kindOf: this.bindingKinds(),
      });
      // Once per stored table: a source's alias names share its binding's
      // virtual handle.
      for (const _table of new Set(
         admitted.bindings.map((b) => b.virtualHandle),
      ))
         recordSourceBound();
      for (const sourceName of admitted.otherDefinition) {
         // Its own table is missing: the one there is another definition's.
         recordSourceDeclined("missing_table");
         logger.info(
            "Serving a source live: its stored table was built from another definition",
            {
               packageName: this.owner.packageName(),
               ...(this.owner.versionId() !== undefined
                  ? { versionId: this.owner.versionId() }
                  : {}),
               sourceName,
            },
         );
      }
      for (const decline of admitted.declined) {
         recordSourceDeclined(decline.reason);
         logger.warn(
            "Refusing a storage serve binding: the source is not eligible to be served from a materialized table",
            {
               packageName: this.owner.packageName(),
               sourceName: decline.sourceName,
               // A generated rollup name means nothing to an operator alone.
               ...(decline.rollup ? { kind: "pre-aggregation rollup" } : {}),
               reason: decline.detail,
            },
         );
      }
      return admitted.bindings;
   }

   /** A binding's kind, read off the current plan (`kindFromPlan`). */
   private bindingKinds(): (b: Binding) => BindingKind {
      const plan = {
         materializeSourceIDs: new Set(Object.keys(this.plan.materializePlans)),
         viewBuildSourceIDs: this.persistedViewBuildIDs(),
      };
      return (b) => kindFromPlan(b, plan);
   }

   /**
    * A declaration's compiled definition, from the package model that holds it
    * as a node of its own (the model that declares or imports it). A model
    * that reaches a source only through a join holds the join, not the
    * declaration, so its served copy finds the declaration here. One function
    * for the package's life: the served-copy context is compared by identity.
    */
   private readonly declarationLookup = (sourceID: string): unknown => {
      for (const model of this.owner.models().values()) {
         const def = SourceMap.of(model.getModelDef?.()).node(sourceID)?.def;
         if (def) return def;
      }
      return undefined;
   };

   /**
    * The served copy's bindings for the colocated `#(materialize)` and v1
    * `#@ persist` tables the bound build manifest holds, admitted by the
    * colocated tier's positive eligibility, keyed by content address.
    */
   private colocatedServeBindings(): Binding[] {
      const sources = this.plan.servingPlan()?.sources ?? {};
      const eligible = this.plan.colocatedSourceEligibility?.eligibleEntityIds;
      const viewBuilds = this.persistedViewBuildIDs();
      return deriveColocatedServeBindings({
         entries: this.colocatedEntries,
         planSources: sources,
         materializePlans: this.plan.materializePlans,
         eligible: (sourceID) =>
            !!eligible?.has(sources[sourceID]?.sourceEntityId ?? ""),
         v1Schema: (sourceID) =>
            v1StoredColumnsOf(this.declarationLookup(sourceID)),
         notV1: (sourceID) => viewBuilds.has(sourceID),
      });
   }

   /** Every model's persisted views' generated build sources, by sourceID. */
   private persistedViewBuildIDs(): Set<string> {
      const ids = new Set<string>();
      for (const model of this.owner.models().values()) {
         for (const b of model.persistedViewBuilds?.() ?? []) {
            ids.add(b.buildSourceID);
         }
      }
      return ids;
   }

   /**
    * Recompute the snapshot and, when its bindings changed (or for models a
    * reload just installed), hand every model its served copy's context and
    * have it compile its served copy over the snapshot now, so a copy that
    * does not compile is a package warning before any query. Never throws.
    * Models read the snapshot itself per query, through their policy
    * ({@link wireServingResolvers}).
    */
   private publish(models?: "installed"): void {
      const { bindings } = this.snapshot();
      if (bindings === this.publishedBindings && !models) return;
      this.publishedBindings = bindings;
      for (const model of this.owner.models().values()) {
         this.setServedCopyContextOn(model);
         void model.prepareServedCopy();
      }
   }

   /**
    * Hand every loaded model the package directory, connections and plans its
    * served copy, rollup and persisted-view legs compile with, before any
    * binding exists. A colocated rollup is read through the build manifest,
    * which a bind sets without pushing bindings, so its leg needs this from
    * load.
    */
   pushServedCopyContextToModels(): void {
      for (const model of this.owner.models().values()) {
         this.setServedCopyContextOn(model);
      }
   }

   private setServedCopyContextOn(model: Model): void {
      model.setServedCopyContext(
         this.owner.packagePath(),
         this.owner.malloyConfig(),
         this.plan.materializePlans,
         this.declarationLookup,
      );
   }

   /**
    * How a serve binding names a destination's schema, so it reads the table
    * where the build wrote it. Absent, every destination is `main`.
    */
   setServeDestinationSchema(
      resolver: (destinationName: string) => string | undefined,
   ): void {
      this.serveDestinationSchema = resolver;
   }

   /**
    * Give the package the environment's deployment facts its publish warnings
    * read (P13), read live on every package read, so a destination configured
    * after the load clears its warning with no reload.
    */
   setDeploymentFacts(facts: DeploymentFacts): void {
      this.deploymentFacts = facts;
   }

   /**
    * Set the connections stored tables are read through: the environment's
    * storage destinations, kept apart from the package's own MalloyConfig so
    * no author model can reach a destination. Absent ⇒ queries serve live.
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

   /** Drop every model's compiled serving legs, when its destinations are replaced. */
   invalidateCompiledLegs(): void {
      for (const model of this.owner.models().values()) {
         model.invalidateCompiledLegs();
      }
   }

   // ---- Verdicts (ServeVerdicts) ------------------------------------------

   /** See {@link ServeVerdicts.verdicts}. */
   serveVerdicts(
      views?: ReadonlyMap<string, ViewServeVerdict[]>,
   ): ServeVerdict[] {
      return this.verdicts.verdicts(views);
   }

   /** See {@link ServeVerdicts.preview}. */
   previewServeVerdicts(): Promise<ServeVerdict[]> {
      return this.verdicts.preview();
   }

   // ---- Warnings and publish rejections (package_warnings.ts) -------------

   annotationFindingWarnings(): ApiPackageWarning[] {
      return warnings.annotationFindingWarnings(this.plan.annotationFindings);
   }

   repeatedJoinOneKeyWarnings(): ApiPackageWarning[] {
      return warnings.repeatedJoinOneKeyWarnings(
         this.plan.buildPlan,
         this.colocatedEntries,
         this.storageBindings,
      );
   }

   persistRefusalWarnings(): ApiPackageWarning[] {
      return warnings.persistRefusalWarnings(
         this.plan.buildPlan,
         this.plan.refusedRollups,
         this.plan.materializePlans,
      );
   }

   storageWarnings(): ApiPackageWarning[] {
      return warnings.storageWarnings(
         this.plan.buildPlan,
         getPersistStorageMode(),
      );
   }

   servedCopyWarnings(): ApiPackageWarning[] {
      return warnings.servedCopyWarnings(this.owner.models());
   }

   storedColumnTypeWarnings(): ApiPackageWarning[] {
      return warnings.storedColumnTypeWarnings(this.owner.models());
   }

   nativeSchemaWarnings(): ApiPackageWarning[] {
      return warnings.nativeSchemaWarnings(
         this.snapshot().bindings,
         this.owner.models(),
         this.plan.materializePlans,
      );
   }

   /** The publish warnings a serve verdict carries, from one verdict pass. */
   verdictWarnings(): ApiPackageWarning[] {
      return warnings.verdictWarnings(this.serveVerdicts());
   }

   /** Legal-but-probably-unintended incremental declarations, as warnings. */
   incrementalPolicyAdvisories(): ApiPackageWarning[] {
      return incrementalPolicyAdvisories(this.plan.incrementalPolicySources);
   }

   preaggregateAccessWarnings(): ApiPackageWarning[] {
      return warnings.preaggregateAccessWarnings(this.owner.models());
   }

   /** See `persistencePolicyWarnings` in package_warnings.ts. */
   persistencePolicyWarnings(): string[] {
      return warnings.persistencePolicyWarnings(
         this.plan.buildPlan,
         this.owner.metadata(),
      );
   }

   /** The {@link persistencePolicyWarnings}, one per line, or "" if valid. */
   formatInvalidPersistencePolicy(): string {
      return this.persistencePolicyWarnings().join("\n");
   }

   /**
    * Every invalid materialization annotation, one per line with its code, or
    * "" when there is none: every `malformed` finding, and two sources an
    * author's names put on one table (unless `PERSIST_COLLISION_ENFORCE=false`).
    * Each fails the publish AND the load, on every path: an invalid annotation
    * is the author's to fix, never something to serve around. A valid
    * annotation Publisher cannot build is a `*-not-built` warning instead.
    */
   formatInvalidAnnotations(): string {
      return [
         ...invalidFindingLines(this.plan.annotationFindings),
         ...(getPersistCollisionEnforce()
            ? collisionGateLines(this.persistenceCollisions())
            : []),
      ].join("\n");
   }

   /**
    * The materialization half of the load gate's text, or "" when the package
    * is valid: invalid annotations and collisions, invalid incremental
    * declarations, invalid `#@ preaggregate` ones. The same text a run over
    * the files and a re-derivation read (plan/load_gate_text.ts).
    */
   formatLoadGateText(): string {
      return [
         this.formatInvalidAnnotations(),
         this.formatInvalidIncrementalPolicy(),
         this.formatInvalidPreaggregatePolicy(),
      ]
         .filter(Boolean)
         .join("\n");
   }

   /**
    * REJECTION messages for incremental-refresh declarations. Unlike
    * {@link persistencePolicyWarnings}, a non-empty result also fails the LOAD.
    */
   incrementalPolicyWarnings(): string[] {
      return incrementalPolicyRejections(this.plan.incrementalPolicySources);
   }

   /** The {@link incrementalPolicyWarnings}, one per line with its route's code, or "". */
   formatInvalidIncrementalPolicy(): string {
      return incrementalGateLines(this.plan.incrementalPolicySources).join(
         "\n",
      );
   }

   /** See `formatInvalidPreaggregatePolicy` in package_warnings.ts. */
   formatInvalidPreaggregatePolicy(): string {
      return warnings.formatInvalidPreaggregatePolicy(this.owner.models());
   }

   /**
    * Within-package persist-target COLLISIONS: distinct sources resolving to
    * one physical table in one destination, where each build would overwrite
    * the other. A publish refusal unless `PERSIST_COLLISION_ENFORCE=false`,
    * and a warning at load either way; none is served from the shared table.
    */
   persistenceCollisionWarnings(): string[] {
      return this.persistenceCollisions().map((c) => c.message);
   }

   /** The collisions as refusals, under `PERSIST_COLLISION_ENFORCE=false`. */
   persistenceCollisionRefusals(): ApiPackageWarning[] {
      return this.persistenceCollisions().map(({ message, code, subject }) => ({
         message,
         code,
         subject,
         severity: "warn" as const,
      }));
   }

   private persistenceCollisions(): ReturnType<typeof planCollisions> {
      if (!this.plan.buildPlan) return [];
      return planCollisions(
         this.plan.buildPlan,
         this.plan.materializePlans,
         this.plan.versionOwnsTables(),
      );
   }
}
