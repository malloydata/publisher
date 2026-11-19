// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The package facade: one package version's materialization, composing
 * plan/, run/ and serve/ for `Package` and `Environment`, and the module's
 * one door for them. It holds the version's bindings, its snapshot and
 * whether a host owns it, and delegates the rest to three collaborators:
 *
 * | Concern | Held by |
 * | --- | --- |
 * | The plan: load, stale, refresh, when current | {@link PlanLifecycle} (`plan_lifecycle.ts`) |
 * | Serve verdicts, the preview, the path report's catalog | {@link ServeVerdicts} (`serve_verdicts.ts`) |
 * | Package warnings and publish rejections | `package_warnings.ts` |
 * | Bind a manifest, swap the snapshot, push it to the models | this facade |
 *
 * Its public surface is wide, not narrow: sixty-two members, most of
 * them one accessor or one warning list each, in these groups: the package's
 * identity and files; the plan's lifecycle; the binding (bind, mark bound,
 * live, failed or host-owned, the manifest it holds); what it pushes to the
 * models (serving context, destinations, deployment facts); the serve
 * verdicts; and one method per package-warning family plus the load gate's
 * text, which `Package` composes in its own order. `facade_surface.spec.ts`
 * lists every public name, so the surface only shrinks.
 *
 * Effects: logs, and the bind metrics.
 */

import type { Connection, MalloyConfig } from "@malloydata/malloy";
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
   recordStoredColumnTypeMismatch,
} from "../../materialization_metrics";
import type { LoadPackageOutcome } from "../../package_load/package_load_pool";
import type { Model } from "../model";
import type { PackageModelPolicy } from "../model_policy";
import type { BuildPlanPackage } from "./plan/build_plan";
import {
   collisionGateLines,
   invalidFindingLines,
   loadGateText,
   nearMissGateLines,
   planCollisions,
   preaggregateGateLines,
} from "./plan/load_gate_text";
import { incrementalPolicyAdvisories } from "./plan/incremental_policy";
import type { PersistNameProblem } from "./domain/package_plan";
import {
   type Binding,
   type BindingKind,
   makeSnapshot,
   pin,
   type Snapshot,
} from "./domain/binding";
import { observedDatabaseColumns } from "./run/database_column_read";
import {
   applyObservedColumns,
   type DriftDeclaration,
   bindDatabaseEntries,
   bindStorageEntries,
   deriveDatabaseServeBindings,
   type ObservedDrift,
   type UnreadTable,
   kindFromPlan,
   v1StoredColumnsOf,
} from "./serve/bind";
import type { ServeVerdict, ViewServeVerdict } from "./serve/serve_verdict";
import {
   type BoundTableTypeFinding,
   boundTableTypeFindings,
   type DatabaseDriftFinding,
   databaseDriftFindings,
} from "./serve/storage_bindings";
import { STORED_COLUMN_TYPE_MISMATCH_CODE } from "./stored_column_types";
import { tablePathParts } from "../quoting";
import * as warnings from "./package_warnings";
import { PlanLifecycle } from "./plan_lifecycle";
import { type DeploymentFacts, ServeVerdicts } from "./serve_verdicts";
import { SourceMap } from "./text/source_map";
import type { KeptTable } from "./domain/types";
import type { LateRows } from "./domain/types";
import { isRollupOrigin, originOfTable } from "./domain/table_plans";
import { queryTimezoneOf } from "../governance/ir/compiled_model";
import type {
   BuildManifest,
   BuildPlan,
   FreshnessManifest,
   ManifestEntry,
   ObservedColumns,
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

/** A bound table's path as a person reads it: its parts, unquoted. */
function tableNameOf(name: string | undefined): { tableName?: string } {
   if (!name) return {};
   return { tableName: (tablePathParts(name, "") ?? [name]).join(".") };
}

/** Tries to read a table a bind left unread, before polls alone retry it. */
const UNREAD_RETRY_ATTEMPTS = 10;
/** The first retry's delay; each next one doubles, up to {@link UNREAD_RETRY_MAX_MS}. */
const UNREAD_RETRY_BASE_MS = 1_000;
const UNREAD_RETRY_MAX_MS = 60_000;

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
   /** One of the package's connections, by name, for the bind's column read. */
   connection(connectionName: string): Promise<Connection>;
   /** The package, for a main-thread build-plan compute. */
   buildPlanPackage: BuildPlanPackage;
   /**
    * The sources callers may query, by the model that publishes them, or
    * undefined when every source is queryable.
    */
   publishedSurface():
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
    * The database manifest entries bound now, freshness fields and key
    * checks included: the snapshot's `entries`. Undefined when serving live.
    */
   private databaseEntries: FreshnessManifest | undefined;
   /**
    * The tables the newest auto-run lists as kept after a rename
    * (Reclaimer.reclaimRenamedTables), set by that run.
    */
   private keptTables: readonly KeptTable[] = [];
   /**
    * The same entries as table names only, the manifest models hydrate with
    * and /compile reads. Surfaced on /status so the control plane can confirm
    * a worker bound it.
    */
   private buildManifestEntries: BuildManifest["entries"] | undefined;
   /**
    * What the bind read of each database table's columns from its warehouse
    * ({@link readDatabaseColumns}), for the entries bound with it: one read
    * per table per bind, never per query. Undefined for a bind that read
    * none (a hand-made bind), whose tables are read as their build wrote them.
    */
   private observedColumns: ReadonlyMap<string, ObservedColumns> | undefined;
   /** What the last snapshot found of the observation: drift, and tables not read. */
   /** The pending re-read of the tables the last read left unread. */
   /** Set by {@link dispose}: the package is released. */
   private disposed = false;
   private unreadRetry:
      | { timer: ReturnType<typeof setTimeout>; attempt: number }
      | undefined;
   private observedOutcome: {
      drifted: ObservedDrift[];
      unread: UnreadTable[];
   } = { drifted: [], unread: [] };
   /** The `storage=` bindings a host's or a run's manifest made. */
   private storageBindings: readonly Binding[] = [];
   /**
    * The `lateRows` the bound `storage=` entries carry, by content address:
    * an incremental table's late-row check, which outlives the binding's own
    * shape. The database entries carry theirs on {@link databaseEntries}.
    */
   private storageLateRows: ReadonlyMap<string, LateRows> = new Map();
   /** {@link snapshot}, keyed on every fact it is made from. */
   private snapshotMemo:
      | { key: readonly unknown[]; snapshot: Snapshot; signature: string }
      | undefined;
   /** The bindings the models last compiled their served copies over. */
   private publishedBindings: readonly Binding[] | undefined;
   private manifestBindingStatus: "unbound" | "bound" | "bind_failed" =
      "unbound";
   private manifestEntryCount = 0;
   private boundManifestUri: string | null = null;
   /**
    * A host owns this version's binding: a manifest it named bound, or its
    * instructed run committed (see Environment.isHostOwned). Never cleared:
    * a later clear leaves the version live, not back on this server's store.
    */
   private hostOwned = false;
   /**
    * The declined bindings already warned of under the plan they were
    * judged by ({@link logDecline}); a new plan warns afresh.
    */
   private warnedDeclines: { plan: unknown; keys: Set<string> } = {
      plan: undefined,
      keys: new Set(),
   };
   /**
    * The type findings of the bound `storage=` tables a model's own
    * `#@ persist` check does not make ({@link boundTableTypeFindings}), as of
    * the last publish.
    */
   private boundTypeFindings: readonly BoundTableTypeFinding[] = [];
   private preaggregateEntityIdCache:
      | { plan: BuildPlan | null; ids: ReadonlySet<string> }
      | undefined;

   constructor(private readonly owner: PackageMaterializationOwner) {
      this.plan = new PlanLifecycle(owner, () => this.publish());
      this.verdicts = new ServeVerdicts({
         plan: this.plan,
         packageName: () => owner.packageName(),
         models: () => owner.models(),
         publishedSurface: () => owner.publishedSurface(),
         snapshot: () => this.snapshot(),
         storageBindings: () => this.storageBindings,
         databaseEntries: () => this.databaseEntries,
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
      manifestBindingStatus: "unbound" | "bound" | "bind_failed";
      manifestEntryCount: number;
      boundManifestUri: string | null;
      storageServeBindings: NonNullable<ApiPackage["storageServeBindings"]>;
   } {
      return {
         manifestBindingStatus: this.manifestBindingStatus,
         manifestEntryCount: this.manifestEntryCount,
         boundManifestUri: this.boundManifestUri,
         storageServeBindings: this.storageBindings.map((b) => ({
            origin: originOfTable(b.kind, b.sourceIDs?.[0] ?? ""),
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
    * Whether a non-empty database manifest is bound. A pure-storage refresh may
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
    * The database entries a query may read at `now` (fresh, un-gated, or
    * stale under `stale_ok`), as table names: what /compile reads. Undefined
    * when nothing is bound.
    */
   getFreshBuildManifest(
      now: number = Date.now(),
   ): BuildManifest["entries"] | undefined {
      if (!this.databaseEntries) return undefined;
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
    * `storage=` bindings, each kind read off the current plan, the database
    * tables the served copy reads, and the database entries. Memoized on
    * every fact it is made from, and the bindings array is kept while they
    * do not change, so caches keyed by it survive an unrelated rebind.
    */
   snapshot(): Snapshot {
      const plan = this.plan;
      const key = [
         this.storageBindings,
         this.databaseEntries,
         this.observedColumns,
         plan.buildPlan,
         plan.servingPlan(),
         plan.materializePlans,
         plan.databaseSourceEligibility,
         plan.asOfBuild,
         this.owner.models(),
         getMaterializeEnabled(),
      ];
      const memo = this.snapshotMemo;
      if (memo && memo.key.every((part, i) => part === key[i])) {
         return memo.snapshot;
      }
      const kindOf = this.bindingKinds();
      // Each database table held to the columns its warehouse holds now.
      const observed = applyObservedColumns({
         entries: this.databaseEntries ?? {},
         bindings: this.databaseServeBindings(),
         observed: this.observedColumns,
         declared: (id) => this.driftDeclaration(id),
      });
      this.observedOutcome = {
         drifted: observed.drifted,
         unread: observed.unread,
      };
      const asOfBuild = this.asOfBuildBinding();
      const bindings = [
         ...this.storageBindings.map((b) => {
            const kind = kindOf(b);
            return kind === b.kind ? b : { ...b, kind };
         }),
         ...observed.bindings,
      ].map((b) => (asOfBuild(b) ? { ...b, asOfBuild: true } : b));
      const signature = JSON.stringify(bindings);
      const snapshot = makeSnapshot(
         memo && memo.signature === signature
            ? memo.snapshot.bindings
            : Object.freeze(bindings),
         this.observedColumns ? observed.entries : (this.databaseEntries ?? {}),
      );
      this.snapshotMemo = { key, snapshot, signature };
      return snapshot;
   }

   /**
    * Whether a binding reads a table stored as of its build: one of its
    * sources, or its content address, is one the plan marks `asOfBuild`.
    * Read admission answers an access decision over it live.
    */
   private asOfBuildBinding(): (b: Binding) => boolean {
      const ids = new Set(Object.keys(this.plan.asOfBuild));
      if (ids.size === 0) return () => false;
      const sources = this.plan.buildPlan?.sources ?? {};
      const addresses = new Set(
         [...ids].flatMap((id) => {
            const address = sources[id]?.sourceEntityId;
            return address ? [address] : [];
         }),
      );
      return (b) =>
         addresses.has(b.virtualHandle) ||
         (b.sourceIDs ?? []).some((id) => ids.has(id));
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
         if (isRollupOrigin(source.origin) && source.sourceEntityId) {
            ids.add(source.sourceEntityId);
         }
      }
      this.preaggregateEntityIdCache = { plan, ids };
      return ids;
   }

   /**
    * Record that `uri`'s manifest is bound. Asserted rather than derived from
    * the entry count (a bind may carry no database entries); called after both
    * tiers apply.
    */
   markManifestBound(uri: string): void {
      this.boundManifestUri = uri;
      this.manifestBindingStatus = "bound";
   }

   /**
    * Record that no manifest is bound and the package serves live: a cleared
    * `manifestLocation` with nothing taking its place. The status otherwise
    * keeps naming a manifest that bound nothing (an empty one) or did not
    * bind (a failed fetch). Binds nothing; a caller with tables bound drops
    * them too.
    */
   markServingLive(): void {
      this.boundManifestUri = null;
      this.manifestBindingStatus = "unbound";
   }

   /** Record that a host owns this version's binding ({@link hostOwned}). */
   markHostOwned(): void {
      this.hostOwned = true;
   }

   /** Whether a host owns this version's binding ({@link hostOwned}). */
   isHostOwned(): boolean {
      return this.hostOwned;
   }

   /**
    * Mark that a configured `manifestLocation` could not be fetched/bound, so
    * the package is serving live despite intending to be materialized-routed.
    */
   markManifestBindFailed(): void {
      this.manifestBindingStatus = "bind_failed";
   }

   /**
    * Bind a manifest, either half or both, and swap the snapshot every model
    * reads by one reference: a query already routing keeps the one it
    * pinned.
    *
    * - `database`: the database entries, filtered POSITIVELY against the
    *   database eligibility (a miss is dropped, not fatal) and quoted for
    *   their dialects; an empty map reverts that tier to live. With
    *   `databaseAsGiven`, recorded as given: a reload that recompiled every
    *   model against them, which relies on the per-query `#(authorize)`
    *   graft, not on this filter.
    * - `storage`: the `storage=` entries a host vouches for, minus any this
    *   package's eligibility gate refuses and any built from another
    *   definition of their source. The host knows WHICH table backs a source,
    *   not WHETHER a frozen table may serve it: only a compile knows that.
    */
   bind(update: {
      database?: FreshnessManifest;
      databaseAsGiven?: boolean;
      /**
       * What {@link readDatabaseColumns} read of `database`'s tables: each
       * table is then read as its warehouse holds it now. Every server bind
       * passes it; absent, the tables are read as their builds wrote them.
       */
      observed?: ReadonlyMap<string, ObservedColumns>;
      storage?: Record<string, ManifestEntry>;
      /** Models a reload just installed: publish to them even when the bindings did not move. */
      models?: "installed";
   }): void {
      if (update.database) {
         this.adoptDatabase(
            update.databaseAsGiven
               ? update.database
               : this.admitDatabase(update.database),
         );
         this.observedColumns = update.observed;
         this.cancelUnreadRetry();
      }
      if (update.storage) {
         this.storageBindings = this.admitStorage(update.storage);
         this.storageLateRows = new Map(
            Object.entries(update.storage).flatMap(([id, entry]) =>
               entry.lateRows ? [[id, entry.lateRows]] : [],
            ),
         );
      }
      this.publish(update.models);
      if (update.database && update.observed) {
         this.logObservedOutcome();
         this.scheduleUnreadRetry(1);
      }
   }

   /**
    * Whether the bound database tables include one the bind could not read
    * (it answers live): a poll that finds the manifest unchanged binds again
    * rather than skip, so the table is read once it can be.
    */
   hasUnreadDatabaseTables(): boolean {
      return this.knownUnread().length > 0;
   }

   /**
    * The unread tables this package reads: an entry whose address its plan
    * or a served binding names. A manifest entry for a table no source here
    * reads (another definition's) is neither warned of nor rebound for.
    */
   private knownUnread(): UnreadTable[] {
      this.snapshot();
      // The bindings before the observation leaves the unread ones out.
      const known = new Set<string>(
         this.databaseServeBindings().map((b) => b.virtualHandle),
      );
      for (const source of Object.values(
         (this.plan.servingPlan() ?? this.plan.buildPlan)?.sources ?? {},
      )) {
         if (source.sourceEntityId) known.add(source.sourceEntityId);
      }
      return this.observedOutcome.unread.filter((u) =>
         known.has(u.sourceEntityId),
      );
   }

   /**
    * The package is released: cancel the unread tables' retry, and schedule
    * none after it, so no timer outlives its package.
    */
   dispose(): void {
      this.disposed = true;
      this.cancelUnreadRetry();
   }

   private cancelUnreadRetry(): void {
      if (this.unreadRetry) clearTimeout(this.unreadRetry.timer);
      this.unreadRetry = undefined;
   }

   /**
    * Read again, with backoff, the tables this bind left unread (a read that
    * failed or timed out), so a transient failure does not keep a table live
    * until something else binds. Stops when every table is read, when a bind
    * replaces these entries, or after {@link UNREAD_RETRY_ATTEMPTS} tries.
    */
   private scheduleUnreadRetry(attempt: number): void {
      this.cancelUnreadRetry();
      if (this.disposed) return;
      const observed = this.observedColumns;
      const entries = this.databaseEntries;
      if (!observed || !entries || attempt > UNREAD_RETRY_ATTEMPTS) return;
      // Only tables a source here reads: another definition's never is.
      const unread = this.knownUnread()
         .map((u) => u.sourceEntityId)
         .filter((id) => {
            const o = observed.get(id);
            return (
               o !== undefined && "unread" in o && Object.hasOwn(entries, id)
            );
         });
      if (unread.length === 0) return;
      const delay = Math.min(
         UNREAD_RETRY_BASE_MS * 2 ** (attempt - 1),
         UNREAD_RETRY_MAX_MS,
      );
      const timer = setTimeout(() => {
         void this.retryUnread(entries, observed, unread, attempt);
      }, delay);
      (timer as { unref?: () => void }).unref?.();
      this.unreadRetry = { timer, attempt };
   }

   private async retryUnread(
      entries: FreshnessManifest,
      observed: ReadonlyMap<string, ObservedColumns>,
      unread: string[],
      attempt: number,
   ): Promise<void> {
      const current = () =>
         !this.disposed &&
         this.databaseEntries === entries &&
         this.observedColumns === observed;
      if (!current()) return;
      const reread = await this.readDatabaseColumns(
         Object.fromEntries(unread.map((id) => [id, entries[id]])),
      );
      // A bind while the read ran replaced what it was reading for.
      if (!current()) return;
      const next = new Map(observed);
      for (const [id, o] of reread) next.set(id, o);
      this.observedColumns = next;
      this.publish();
      // Only what this retry read: the rest was logged and counted already.
      this.logObservedOutcome(new Set(unread));
      this.scheduleUnreadRetry(attempt + 1);
   }

   /**
    * Read each database table's columns from its warehouse, for a bind of
    * `entries` ({@link bind}'s `observed`): one schema read per table, on
    * the connection its entry names (else its plan source's). A table whose
    * read fails is answered live at that bind, with a warning.
    */
   async readDatabaseColumns(
      entries: FreshnessManifest,
   ): Promise<ReadonlyMap<string, ObservedColumns>> {
      const planConnections = new Map<string, string>();
      for (const source of Object.values(this.plan.buildPlan?.sources ?? {})) {
         if (source.sourceEntityId && source.connectionName) {
            planConnections.set(source.sourceEntityId, source.connectionName);
         }
      }
      return observedDatabaseColumns({
         entries,
         connectionNameOf: (id) =>
            entries[id]?.connectionName ?? planConnections.get(id),
         connection: (name) => this.owner.connection(name),
      });
   }

   /**
    * Log and count what the bind's column read found: drifted tables, each
    * column of them the package warning reports counted as a `storage=`
    * table's is, and tables it could not read. `only`: the tables a retry
    * read again, so a table is counted and logged once per read of it.
    */
   private logObservedOutcome(only?: ReadonlySet<string>): void {
      const read = (id: string) => only === undefined || only.has(id);
      for (const finding of this.databaseDriftFindings()) {
         if (!read(finding.sourceEntityId)) continue;
         for (const m of finding.mismatches) {
            recordStoredColumnTypeMismatch("serve", m.declaredType);
         }
      }
      for (const drift of this.observedOutcome.drifted) {
         if (!read(drift.sourceEntityId)) continue;
         const asDouble = drift.columns.filter((c) => c.storedAsDouble);
         if (asDouble.length > 0) {
            logger.warn(
               "A DuckLake table holds a 128-bit integer column as a DOUBLE, so it is never read as exact: each such column is answered live where live keeps more digits",
               {
                  code: warnings.DATABASE_COLUMN_TYPE_MISMATCH_CODE,
                  packageName: this.owner.packageName(),
                  sourceEntityId: drift.sourceEntityId,
                  columns: asDouble,
               },
            );
            if (asDouble.length === drift.columns.length) continue;
         }
         const reported = drift.columns.filter(
            (c) => !c.storedAsDouble && !c.castOnly,
         );
         if (reported.length === 0) continue;
         logger.warn(
            "A database table's column types changed outside a build; each changed column is read through a faithful cast or answered live",
            {
               code: warnings.DATABASE_COLUMN_TYPE_MISMATCH_CODE,
               packageName: this.owner.packageName(),
               sourceEntityId: drift.sourceEntityId,
               columns: reported,
            },
         );
      }
      for (const miss of this.observedOutcome.unread) {
         if (!read(miss.sourceEntityId)) continue;
         logger.warn(
            "Could not read a database table's column types at bind; its sources answer live, and it is read again",
            {
               packageName: this.owner.packageName(),
               sourceEntityId: miss.sourceEntityId,
               reason: miss.reason,
               ...(miss.detail !== undefined ? { error: miss.detail } : {}),
            },
         );
      }
   }

   /** The database entries bound now, by identity: a bind replaces them wholesale. */
   boundDatabaseEntries(): FreshnessManifest | undefined {
      return this.databaseEntries;
   }

   /**
    * Drop the rollup key checks bound for these content
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
      const entries = this.databaseEntries;
      let next: FreshnessManifest | undefined;
      for (const id of ids) {
         const entry = entries?.[id];
         if (!entry?.keyUniqueness) continue;
         next ??= { ...entries };
         const { keyUniqueness: _forgotten, ...rest } = entry;
         next[id] = rest;
      }
      // A database pair table's leaf key check rides on its binding
      // (`primaryKeys`): the snapshot drops it too.
      if (next) this.databaseEntries = next;
      this.publish();
   }

   /** Store the database entries; an empty map reverts to live (unbound). */
   private adoptDatabase(entries: FreshnessManifest): void {
      const count = Object.keys(entries).length;
      this.databaseEntries = count > 0 ? entries : undefined;
      this.buildManifestEntries =
         count > 0 ? toTableNameManifest(entries) : undefined;
      this.manifestEntryCount = count;
      this.manifestBindingStatus = count > 0 ? "bound" : "unbound";
      if (count === 0) {
         this.boundManifestUri = null;
      }
   }

   /** The database entries the bind admits, logging and counting the rest. */
   private admitDatabase(entries: FreshnessManifest): FreshnessManifest {
      const eligibility = this.plan.databaseSourceEligibility;
      const dialects = new Map<string, string>();
      for (const source of Object.values(this.plan.buildPlan?.sources ?? {})) {
         if (source.sourceEntityId && source.dialect) {
            dialects.set(source.sourceEntityId, source.dialect);
         }
      }
      const admitted = bindDatabaseEntries({ entries, eligibility, dialects });
      for (const _entry of Object.keys(admitted.entries)) recordSourceBound();
      for (const decline of admitted.declined) {
         recordSourceDeclined(decline.reason);
         this.logDecline(
            `database\u0000${decline.sourceEntityId}\u0000${decline.reason}`,
            "Refusing a database serve binding: the source is not eligible to be served from a materialized table",
            {
               packageName: this.owner.packageName(),
               sourceEntityId: decline.sourceEntityId,
               // The decline's code: its text can name a host's table.
               reason: decline.reason,
            },
         );
      }
      if (eligibility === undefined && admitted.declined.length > 0) {
         // The whole default tier just reverted to live, not one source.
         logger.warn(
            "Database serve bindings dropped package-wide: the build plan never computed, so no source could be examined for eligibility",
            {
               packageName: this.owner.packageName(),
               droppedCount: admitted.declined.length,
            },
         );
      }
      return admitted.entries;
   }

   /**
    * Log a declined binding: warned the first time under a plan, then at
    * debug, since a rebind of an unchanged manifest (a poll) declines the
    * same entries again. A new plan (a reload, a refresh) warns again, so a
    * decline that comes back after a fix is seen.
    */
   private logDecline(
      key: string,
      message: string,
      fields: Record<string, unknown>,
   ): void {
      const plan = this.plan.buildPlan;
      if (this.warnedDeclines.plan !== plan) {
         this.warnedDeclines = { plan, keys: new Set() };
      }
      if (this.warnedDeclines.keys.has(key)) {
         logger.debug(message, fields);
         return;
      }
      this.warnedDeclines.keys.add(key);
      logger.warn(message, fields);
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
         this.logDecline(
            `storage\u0000${decline.sourceName}\u0000${decline.reason}`,
            "Refusing a storage serve binding: the source is not eligible to be served from a materialized table",
            {
               packageName: this.owner.packageName(),
               sourceName: decline.sourceName,
               // A generated rollup name means nothing to an operator alone.
               ...(decline.rollup ? { kind: "pre-aggregation rollup" } : {}),
               // The decline's code: its text can name a host's table.
               reason: decline.reason,
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
   /**
    * A rollup's base, by its name in the model the plan entry came from, as
    * the rollup leg reads the base's zone.
    */
   private baseDeclaration(source: {
      modelPath?: string;
      preaggregate?: { baseSourceName?: string } | null;
   }): unknown {
      const name = source.preaggregate?.baseSourceName;
      const model = source.modelPath
         ? this.owner.models().get(source.modelPath)
         : undefined;
      if (!name || !model) return undefined;
      const map = SourceMap.of(model.getModelDef());
      const id = map.sourceIDOf(name);
      return id === undefined ? undefined : map.node(id)?.def;
   }

   private readonly declarationLookup = (sourceID: string): unknown => {
      for (const model of this.owner.models().values()) {
         const def = SourceMap.of(model.getModelDef()).node(sourceID)?.def;
         if (def) return def;
      }
      return undefined;
   };

   /**
    * The served copy's bindings for the database `#(materialize)` and v1
    * `#@ persist` tables the bound build manifest holds, admitted by the
    * database tier's positive eligibility, keyed by content address.
    */
   private databaseServeBindings(): Binding[] {
      const sources = this.plan.servingPlan()?.sources ?? {};
      const eligible = this.plan.databaseSourceEligibility?.eligibleEntityIds;
      const viewBuilds = this.persistedViewBuildIDs();
      return deriveDatabaseServeBindings({
         entries: this.databaseEntries,
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
         for (const b of model.persistedViewBuilds() ?? []) {
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
      this.bindTypeFindings(bindings);
   }

   /**
    * Read the bound tables' type findings once per publish, counting and
    * logging each table's mismatched columns there, as a model's own are.
    */
   private bindTypeFindings(bindings: readonly Binding[]): void {
      this.boundTypeFindings = boundTableTypeFindings(
         bindings,
         this.plan.buildPlan,
         {
            materializePlans: this.plan.materializePlans,
            queryTimezone: (sourceID, source) =>
               queryTimezoneOf(
                  source.preaggregate
                     ? this.baseDeclaration(source)
                     : this.declarationLookup(sourceID),
               ),
         },
      );
      for (const finding of this.boundTypeFindings) {
         for (const m of finding.mismatches) {
            recordStoredColumnTypeMismatch("serve", m.declaredType);
         }
         logger.warn(
            "Stored column types do not match the declared Malloy types",
            {
               code: STORED_COLUMN_TYPE_MISMATCH_CODE,
               packageName: this.owner.packageName(),
               source: finding.subject,
               kind: finding.binding.kind,
               columns: finding.mismatches,
            },
         );
      }
   }

   /**
    * Hand every loaded model the package directory, connections and plans its
    * served copy, rollup and persisted-view legs compile with, before any
    * binding exists. A database rollup is read through the build manifest,
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
    * read, read live on every package read, so a destination configured
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
      return this.withAsOfBuild(this.verdicts.verdicts(views));
   }

   /** See {@link ServeVerdicts.preview}. */
   async previewServeVerdicts(): Promise<ServeVerdict[]> {
      return this.withAsOfBuild(await this.verdicts.preview());
   }

   /**
    * A verdict that serves a table stored as of its build says so in its
    * message: its answers are as of its last build, not of each query.
    */
   private withAsOfBuild(verdicts: ServeVerdict[]): ServeVerdict[] {
      const asOfBuild = this.plan.asOfBuild;
      return verdicts.map((v) =>
         v.verdict === "serves" && asOfBuild[v.sourceID]
            ? {
                 ...v,
                 message:
                    `${v.message} Its answers are as of its last build, ` +
                    `since its relation reads the current time; it is ` +
                    `rebuilt on every run.`,
              }
            : v,
      );
   }

   // ---- Warnings and publish rejections (package_warnings.ts) -------------

   annotationFindingWarnings(): ApiPackageWarning[] {
      return warnings.annotationFindingWarnings(this.plan.annotationFindings);
   }

   /** The refusal warnings a planner's finding made (see annotationFindingWarnings). */
   findingRefusalWarnings(): ApiPackageWarning[] {
      return this.persistRefusalWarnings(
         (id) => id in this.plan.refusalNotices,
      );
   }

   /** The tables the newest auto-run lists as kept after a rename. */
   setKeptTables(tables: readonly KeptTable[]): void {
      this.keptTables = tables;
   }

   keptTableWarnings(): ApiPackageWarning[] {
      return warnings.keptTableWarnings(this.plan.buildPlan, this.keptTables);
   }

   /**
    * The `lateRows` the bound manifest entries report (database and
    * `storage=`), by content address: what the warning names, and what an
    * incremental delta carries until a rebuild.
    */
   boundLateRows(): ReadonlyMap<string, LateRows> {
      const bound = new Map(this.storageLateRows);
      for (const [id, entry] of Object.entries(this.databaseEntries ?? {})) {
         if (entry.lateRows) bound.set(id, entry.lateRows);
      }
      return bound;
   }

   /** See package_warnings.ts, `lateRowWarnings`. */
   lateRowWarnings(): ApiPackageWarning[] {
      return warnings.lateRowWarnings(
         this.plan.buildPlan,
         this.boundLateRows(),
      );
   }

   repeatedJoinOneKeyWarnings(): ApiPackageWarning[] {
      return warnings.repeatedJoinOneKeyWarnings(
         this.plan.buildPlan,
         this.databaseEntries,
         this.storageBindings,
      );
   }

   persistRefusalWarnings(
      only?: (sourceID: string) => boolean,
   ): ApiPackageWarning[] {
      return warnings.persistRefusalWarnings(
         this.plan.buildPlan,
         this.plan.refusedRollups,
         this.plan.materializePlans,
         this.plan.refusalNotices,
         only,
      );
   }

   /** See package_warnings.ts, `asOfBuildWarnings`. */
   asOfBuildWarnings(): ApiPackageWarning[] {
      return warnings.asOfBuildWarnings(
         this.plan.asOfBuild,
         this.plan.persistedViews,
      );
   }

   storageWarnings(): ApiPackageWarning[] {
      return warnings.storageWarnings(
         this.plan.buildPlan,
         getPersistStorageMode(),
      );
   }

   servedCopyWarnings(): ApiPackageWarning[] {
      const materialized = this.plan.materializePlans;
      return warnings.servedCopyWarnings(this.owner.models(), (sourceID) =>
         Object.hasOwn(materialized, sourceID),
      );
   }

   storedColumnTypeWarnings(): ApiPackageWarning[] {
      return warnings.storedColumnTypeWarnings(
         this.owner.models(),
         this.boundTypeFindings,
         this.databaseDriftFindings(),
      );
   }

   /**
    * The database tables whose columns the last bind found moved outside a
    * build, as the `storage-column-type-mismatch` warning reports them.
    */
   private databaseDriftFindings(): DatabaseDriftFinding[] {
      this.snapshot();
      const { drifted } = this.observedOutcome;
      if (drifted.length === 0) return [];
      return databaseDriftFindings(drifted, this.plan.buildPlan, {
         queryTimezone: this.driftQueryTimezone,
      });
   }

   /** The time zone a moved column of `source`'s table is read in. */
   private readonly driftQueryTimezone = (
      sourceID: string,
      source: NonNullable<NonNullable<BuildPlan>["sources"]>[string],
   ): string | undefined =>
      queryTimezoneOf(
         source.preaggregate
            ? this.baseDeclaration(source)
            : this.declarationLookup(sourceID),
      );

   /**
    * What a substituted database table's moved columns must read as, by its
    * content address: its build source's declared columns and reader's time
    * zone, the inputs {@link databaseDriftFindings} reads, so the bind and
    * the warning decide each column alike.
    */
   private driftDeclaration(
      sourceEntityId: string,
   ): DriftDeclaration | undefined {
      const found = Object.entries(this.plan.buildPlan?.sources ?? {}).find(
         ([, s]) => s.sourceEntityId === sourceEntityId,
      );
      if (!found) return undefined;
      const [sourceID, source] = found;
      const queryTimezone = this.driftQueryTimezone(sourceID, source);
      return {
         columns: source.columns ?? [],
         connectionName: source.connectionName,
         ...(queryTimezone ? { queryTimezone } : {}),
      };
   }

   /**
    * A warning per bound database table the bind could not read, while it
    * is unread (`database-table-unread`).
    */
   databaseTableUnreadWarnings(): ApiPackageWarning[] {
      return warnings.databaseTableUnreadWarnings(
         this.knownUnread().map((u) => ({
            ...u,
            ...tableNameOf(this.databaseEntries?.[u.sourceEntityId]?.tableName),
         })),
         this.plan.servingPlan() ?? this.plan.buildPlan,
         this.unreadRetry !== undefined,
      );
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
    * The load gate's text, or "" when the package is valid: the one text
    * (plan/load_gate_text.ts) over this load's plan, its models' own lines
    * and the raw text's `name=` problems, the same a run over the files, a
    * re-derivation and a package compile read off the plan they derive.
    */
   loadGateText(nameProblems: readonly PersistNameProblem[]): string {
      const models = [...this.owner.models()];
      return loadGateText(
         {
            plan: this.plan.buildPlan,
            annotationFindings: this.plan.annotationFindings,
            incrementalDeclarations: this.plan.incrementalDeclarations,
            materializePlans: this.plan.materializePlans,
            persistedViews: this.plan.persistedViews,
            modelGateLines: {
               preaggregate: models.flatMap(([modelPath, model]) =>
                  preaggregateGateLines(
                     modelPath,
                     model.preaggregateViolations(),
                  ),
               ),
               nearMiss: this.nearMissLines(),
               nameProblems: [...nameProblems],
            },
         },
         {
            enforceCollisions: getPersistCollisionEnforce(),
            versionScoped: this.plan.versionOwnsTables(),
         },
      );
   }

   /** Each model's misspelled materialization routes, one line each. */
   nearMissLines(): string[] {
      return [...this.owner.models()].flatMap(([modelPath, model]) =>
         nearMissGateLines(modelPath, model.annotationNearMisses?.() ?? []),
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
         reason: "table_name_collision",
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
