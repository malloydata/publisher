// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A package's serve verdicts, as the facade's plan and bindings stand now:
 * whether each planned source is served and why not, the same answer before
 * anything is built (a preview), the tables the path report treats as live,
 * and the entry-point index both read.
 *
 * Effects: logs a verdict that cannot be computed, and reports none.
 */

import { isRollupOrigin, originOf } from "./domain/table_plans";
import { isSourceDef, type SourceDef } from "@malloydata/malloy";
import { gateSwitches, getPersistStorageMode } from "../../config";
import { logger } from "../../logger";
import { errorCodeOf } from "../../error_code";
import type { Model } from "../model";
import { collectEntryPointGates } from "../gate_classification";
import { structsReachedFrom } from "../governance/ir/source_graph";
import { keepsGatesWhenReadExact, mayAttempt } from "./domain/admission";
import type { GateSwitches, TableRead } from "./domain/gate_policy";
import { type Binding, pin, type Snapshot } from "./domain/binding";
import { storageTablePath } from "./domain/storage_table_path";
import type { FreshnessManifest } from "./domain/types";
import type { PlanLifecycle } from "./plan_lifecycle";
import type { Tier } from "./domain/vocabulary";

type BuildPlan = PlanLifecycle["buildPlan"];
import {
   deriveDatabaseServeBindings,
   keptTableAddresses,
   ungatedTableAddresses,
   v1StoredColumnsOf,
} from "./serve/bind";
import {
   type LiveTableVerdict,
   liveVerdictKey,
   type MaterializedTableCatalog,
   type PlannedTable,
} from "./serve/query_path";
import {
   computeServeVerdicts,
   type ContractVerdict,
   type PersistedResultOf,
   type NotServedIn,
   type ServedCopyOutcome,
   type ServeVerdict,
   type ViewServeVerdict,
} from "./serve/serve_verdict";
import type { ServedCopyProblem } from "./serve/served_copy_types";
import { byActionability } from "./domain/source_decline";
import { previewNativeSchema } from "./stored_column_types";
import { reachableSourceIDs, SourceMap } from "./text/source_map";
import type { ColumnContract } from "./domain/column_contract";
import {
   entryPointGateExprs,
   gateColumnsExactOf,
   gateReads,
} from "./serve/gate_reads";
import { ownLevelNotes, type AnnotationsDef } from "../annotations";
import { routeOfNote } from "../annotation_reader";

/** Whether a compiled source's own statement carries `#(materialize)`. */
function declaresMaterialize(def: unknown): boolean {
   const annotations = (def as { annotations?: AnnotationsDef } | undefined)
      ?.annotations;
   return ownLevelNotes(annotations).some(
      (n) => routeOfNote(n.text) === "materialize",
   );
}

/**
 * What the environment answers about where a package's tables can be written
 *, read live. `undefined` from either means "not known", which never
 * warns.
 */
export interface DeploymentFacts {
   /** The configured storage destinations, or undefined when the list is not authoritative. */
   destinationNames(): readonly string[] | undefined;
   /** Whether a connection's configured type can create a table. */
   connectionCanHoldTable(connectionName: string): boolean | undefined;
   /**
    * The `data_inlining_row_limit` above zero that the DuckLake catalog a
    * destination or connection names was found recording at its last attach.
    */
   dataInliningLimit?(name: string): number | undefined;
}

/** What the verdicts read of the facade that holds them. */
interface ServeVerdictsHost {
   readonly plan: PlanLifecycle;
   packageName(): string;
   models(): Map<string, Model>;
   publishedSurface():
      | { modelPath: string; names: string[]; queries?: string[] }[]
      | undefined;
   snapshot(): Snapshot;
   storageBindings(): readonly Binding[];
   databaseEntries(): FreshnessManifest | undefined;
   /** The bound tables the bind held to their contracts and left out, by address. */
   contractVerdicts(): ReadonlyMap<string, ContractVerdict>;
   deploymentFacts(): DeploymentFacts | undefined;
   destinationSchema(destinationName: string): string | undefined;
   declarationOf(sourceID: string): unknown;
   persistedViewBuildIDs(): Set<string>;
}

/** The live serve verdicts the path report reads, by table and by source. */
interface LiveVerdicts {
   byTable: ReadonlyMap<string, LiveTableVerdict>;
   bySource: ReadonlyMap<string, LiveTableVerdict>;
}

/** Each entry point a caller may query, and what it reads. */
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

export class ServeVerdicts {
   /**
    * Whether a refused source is declared with `#(materialize)`, by sourceID,
    * read once per plan (the plan is replaced wholesale, and with it the
    * models it was planned from).
    */
   private declaredMaterializeMemo:
      | { plan: unknown; models: unknown; ids: Map<string, boolean> }
      | undefined;
   private tableCatalogCache:
      | {
           plan: unknown;
           bound: FreshnessManifest | undefined;
           liveVerdicts: LiveVerdicts | undefined;
           catalog: MaterializedTableCatalog;
        }
      | undefined;
   private liveVerdictCache:
      | { key: readonly unknown[]; verdicts: LiveVerdicts | undefined }
      | undefined;
   private entryPointMemo:
      | { models: Map<string, Model>; index: EntryPointIndex | undefined }
      | undefined;
   /**
    * The plan whose verdicts last failed to compute: a status poll asks again
    * and again, so each plan's failure is warned once and then logged at
    * debug.
    */
   private verdictFailureWarnedFor: object | undefined;

   constructor(private readonly host: ServeVerdictsHost) {}

   /**
    * Each materialized source's serve verdict, read from the plan and the
    * binding as they stand now. `views` adds per-view verdicts (a preview).
    * Reporting only: one that cannot be computed is logged and none is
    * returned, so the dry run, package read or status poll never fails here.
    */
   verdicts(views?: ReadonlyMap<string, ViewServeVerdict[]>): ServeVerdict[] {
      const plan = this.host.plan.buildPlan;
      if (!plan) return [];
      try {
         return this.compute(views);
      } catch (error) {
         const fields = {
            packageName: this.host.packageName(),
            code: errorCodeOf(error),
         };
         if (this.verdictFailureWarnedFor === plan) {
            logger.debug("Could not compute serve verdicts", fields);
         } else {
            this.verdictFailureWarnedFor = plan;
            logger.warn("Could not compute serve verdicts", fields);
         }
         return [];
      }
   }

   /**
    * The package's materialized tables as the path report reads them: every
    * planned source by `sourceID`, and the database tables bound now.
    * Memoized on the plan, the bound entries and the live verdicts.
    */
   tableCatalog(): MaterializedTableCatalog {
      const { plan } = this.host;
      const liveVerdicts = this.liveVerdictsByTable();
      const bound = this.host.databaseEntries();
      const cached = this.tableCatalogCache;
      if (
         cached &&
         cached.plan === plan.buildPlan &&
         cached.bound === bound &&
         cached.liveVerdicts === liveVerdicts
      ) {
         return cached.catalog;
      }
      const planned = new Map<string, PlannedTable>();
      for (const [sourceID, source] of Object.entries(
         plan.buildPlan?.sources ?? {},
      )) {
         if (!source.sourceEntityId) continue;
         const compilerKey = plan.compilerKeys[source.sourceEntityId];
         planned.set(sourceID, {
            sourceID,
            name: source.name,
            sourceEntityId: source.sourceEntityId,
            ...(compilerKey ? { compilerKey } : {}),
            origin: originOf(source.origin),
            ...(source.modelPath ? { modelPath: source.modelPath } : {}),
            ...(isRollupOrigin(source.origin)
               ? { columns: declaredColumnsOfPlan(source.columns) }
               : {}),
         });
      }
      const catalog: MaterializedTableCatalog = {
         planned,
         bound: bound ?? {},
         ...(liveVerdicts
            ? {
                 liveVerdicts: liveVerdicts.byTable,
                 sourceVerdicts: liveVerdicts.bySource,
              }
            : {}),
      };
      this.tableCatalogCache = {
         plan: plan.buildPlan,
         bound,
         liveVerdicts,
         catalog,
      };
      return catalog;
   }

   /**
    * {@link verdicts} with each stored source's named views compiled against
    * a compile-only served copy over bindings declared from the plan's output
    * columns, before anything is built. Costs one served-copy compile per
    * model and one compile per view, so it runs only where a caller asks.
    */
   async preview(): Promise<ServeVerdict[]> {
      const { packageName } = this.host;
      let bindings: Binding[];
      try {
         bindings = this.previewStorageBindings();
      } catch (error) {
         logger.warn("Could not declare the preview's serve bindings", {
            packageName: packageName(),
            code: errorCodeOf(error),
         });
         bindings = [];
      }
      const views = new Map<string, ViewServeVerdict[]>();
      if (bindings.length > 0) {
         for (const model of this.host.models().values()) {
            try {
               const found = await model.previewStorageViews(bindings);
               // Keyed by sourceID: a model that imports the source reports
               // the same views as the one that declares it.
               for (const [sourceID, verdicts] of found) {
                  if (!views.has(sourceID)) views.set(sourceID, verdicts);
               }
            } catch (error) {
               logger.debug("Could not preview a model's storage views", {
                  packageName: packageName(),
                  code: errorCodeOf(error),
               });
            }
         }
      }
      const verdicts = this.verdicts(views);
      try {
         return await this.withCutoverPaths(verdicts);
      } catch (error) {
         logger.warn("Could not preview the v1 cutover's serve paths", {
            packageName: packageName(),
            code: errorCodeOf(error),
         });
         return verdicts;
      }
   }

   private compute(
      views?: ReadonlyMap<string, ViewServeVerdict[]>,
   ): ServeVerdict[] {
      const host = this.host;
      const plan = host.plan;
      const buildPlan = plan.buildPlan;
      const models = host.models();
      const index = this.entryPointIndex(models);
      const persistedViews = new Map<string, PersistedResultOf>();
      const servedStorageNames = new Set<string>();
      for (const model of models.values()) {
         for (const v of model.persistedViewBuilds() ?? []) {
            persistedViews.set(v.buildSourceID, v);
         }
         for (const name of model.servedStorageSourceNames?.() ?? []) {
            servedStorageNames.add(name);
         }
      }
      const servedCopy = servedCopyState(models);
      const materializeIDs = new Set(Object.keys(plan.materializePlans));
      const storageBindings = host.storageBindings();
      // A table not bound yet is judged by the bind's own rule, so the
      // verdict says what it will say once the table is bound.
      const keptTables = keptTableAddresses(
         plan.sourceEligibility?.gateKept,
         (id) => buildPlan?.sources?.[id]?.sourceEntityId,
      );
      // A `storage=` table's own gate, read off the bound bindings the router
      // reads, or, not bound yet, by the bind's own rule.
      const ownGateKept = (sourceID: string): boolean => {
         const bound = storageBindings.filter((b) =>
            b.sourceIDs?.includes(sourceID),
         );
         if (bound.length > 0) {
            return bound.some((b) => b.ownGateKept === true);
         }
         return keptTables.has(
            buildPlan?.sources?.[sourceID]?.sourceEntityId ?? "",
         );
      };
      // A `storage=` table that holds only ungated rows (the ungated-rows
      // rule), read the same way: off the bound bindings, or, not bound yet, by
      // the bind's own rule.
      const ungatedTables = ungatedTableAddresses(
         plan.sourceEligibility?.ungated,
         buildPlan?.sources ?? {},
      );
      const holdsOnlyUngatedRows = (sourceID: string): boolean => {
         const bound = storageBindings.filter((b) =>
            b.sourceIDs?.includes(sourceID),
         );
         if (bound.length > 0) {
            return bound.every((b) => b.holdsOnlyUngatedRows === true);
         }
         return (
            (plan.sourceEligibility?.ungated ?? []).includes(sourceID) &&
            ungatedTables.has(
               buildPlan?.sources?.[sourceID]?.sourceEntityId ?? "",
            )
         );
      };
      // The table a source's answer reads past a gate, as the router
      // judges it, on the verdict's tier.
      const tableRead = (sourceID: string, tier: Tier): TableRead =>
         tier === "storage"
            ? {
                 tier,
                 ownGateKept: ownGateKept(sourceID),
                 holdsOnlyUngatedRows: holdsOnlyUngatedRows(sourceID),
                 ...storageGateRead(
                    sourceID,
                    storageBindings,
                    host.snapshot().contracts,
                    buildPlan,
                    models,
                 ),
              }
            : { tier };
      const switches = gateSwitches();
      // Pinned now: freshness is read once, as a query reads it.
      const pinned = pin(host.snapshot(), new Date());
      const reads = (
         ids: ReadonlySet<string>,
         sourceID: string | undefined,
         sourceName: string,
      ) =>
         sourceID !== undefined
            ? ids.has(sourceID)
            : [...ids].some((id) => id.startsWith(`${sourceName}@`));
      return computeServeVerdicts({
         plan: buildPlan,
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
         // The router's own question, asked of the source's own table: a
         // rollup's leg past a gate, with its variant proven at plan; the
         // served copy over a table it keeps the gate on.
         keepsGate: (sourceID, tier) =>
            verdictKeepsGate(
               isRollupOrigin(buildPlan?.sources?.[sourceID]?.origin)
                  ? {
                       rollup: true,
                       keptAtPlan: plan.rollupsKeepingGates.has(sourceID),
                       table: tableRead(sourceID, tier),
                    }
                  : { rollup: false, table: tableRead(sourceID, tier) },
               switches,
            ),
         refusedAtBind: (sourceID, source, tier) =>
            this.refusedAtBind(sourceID, source, tier),
         boundEntityIds: new Set(Object.keys(host.databaseEntries() ?? {})),
         freshEntityIds: new Set(Object.keys(pinned.compilerManifest.entries)),
         contractVerdicts: host.contractVerdicts(),
         boundStorageNames: new Set(storageBindings.map((b) => b.sourceName)),
         freshStorageIds: new Set(
            pinned.fresh
               .filter((b) => b.tier === "storage")
               .flatMap((b) => b.sourceIDs ?? []),
         ),
         boundStorageIds: new Set(
            storageBindings.flatMap((b) => b.sourceIDs ?? []),
         ),
         servedStorageNames,
         freshStorageNames: new Set(
            pinned.fresh
               .filter((b) => b.tier === "storage")
               .map((b) => b.sourceName),
         ),
         materialize: (sourceID) => {
            if (!materializeIDs.has(sourceID)) return undefined;
            return servedCopy.on ? "serves" : "off";
         },
         servedCopyOnDatabaseOnly: (sourceID) =>
            servedCopy.on && !servedCopy.uncompiled
               ? servedCopy.databaseOnly.has(sourceID)
               : undefined,
         // Judged per source: a model whose copy has yet to compile for the
         // first time blanks the judgement only of the sources it holds.
         servedCopyOutcome: (sourceID) =>
            servedCopy.on && !servedCopy.uncompiledHolds(sourceID)
               ? servedCopyOutcomeOf(
                    sourceID,
                    buildPlan?.sources?.[sourceID]?.modelPath,
                    servedCopy.compiled,
                    (id) => buildPlan?.sources?.[id]?.sourceEntityId,
                 )
               : undefined,
         persistedViewOf: (sourceID) => persistedViews.get(sourceID),
         declaredMaterialize: (sourceID) =>
            this.declaredMaterialize(buildPlan, sourceID),
         views,
         destinationConfigured: (name) => this.destinationConfigured(name),
         connectionCanHoldTable: (name) =>
            host.deploymentFacts()?.connectionCanHoldTable(name),
      });
   }

   /** {@link declaredMaterializeMemo}'s read. */
   private declaredMaterialize(plan: unknown, sourceID: string): boolean {
      const models = this.host.models();
      let memo = this.declaredMaterializeMemo;
      if (!memo || memo.plan !== plan || memo.models !== models) {
         memo = { plan, models, ids: new Map() };
         this.declaredMaterializeMemo = memo;
      }
      let declared = memo.ids.get(sourceID);
      if (declared === undefined) {
         declared = declaresMaterialize(this.host.declarationOf(sourceID));
         memo.ids.set(sourceID, declared);
      }
      return declared;
   }

   /**
    * The tables no query can be answered from, for the path report: keyed by
    * tier and `sourceEntityId` ({@link liveVerdictKey}), a table only when
    * every planned source sharing it on that tier is `live`. Staleness is left
    * out (the report decides it per query), and every other reason reads no
    * clock, so this is memoized on the facts the verdicts read.
    */
   private liveVerdictsByTable(): LiveVerdicts | undefined {
      const host = this.host;
      const plan = host.plan;
      const models = host.models();
      const key = [
         plan.buildPlan,
         host.databaseEntries(),
         host.contractVerdicts(),
         host.storageBindings(),
         plan.sourceEligibility,
         plan.databaseSourceEligibility,
         getPersistStorageMode(),
         models,
         // Read live, not replaced wholesale, so they key the memo by what
         // they answer for this plan.
         this.deploymentSignature(),
         // Each model's served copy compiles after the bind and is dropped on
         // invalidation; a `#(materialize)` verdict reads it.
         servedCopyState(models).signature,
      ];
      const cached = this.liveVerdictCache;
      if (cached && cached.key.every((part, i) => part === key[i])) {
         return cached.verdicts;
      }
      let verdicts: LiveVerdicts | undefined;
      if (plan.buildPlan) {
         const sources = plan.buildPlan.sources ?? {};
         const perTable = new Map<string, ServeVerdict[]>();
         const bySource = new Map<string, LiveTableVerdict>();
         // A `#@ persist` source the served copy leaves is reported per
         // query by the leg's own decline code, which says more than
         // `served_copy`; only a `#(materialize)` one is re-read here.
         const liveFor = (v: ServeVerdict) =>
            v.verdict === "live" &&
            !!v.reason &&
            v.reason !== "stale" &&
            (v.reason !== "served_copy" || v.origin === "materialize");
         for (const verdict of this.verdicts()) {
            if (liveFor(verdict) && !bySource.has(verdict.sourceID)) {
               bySource.set(verdict.sourceID, {
                  reason: verdict.reason as string,
                  sourceName: verdict.sourceName,
               });
            }
            const entityId = sources[verdict.sourceID]?.sourceEntityId;
            if (!entityId) continue;
            const tableKey = liveVerdictKey(verdict.tier, entityId);
            const list = perTable.get(tableKey) ?? [];
            list.push(verdict);
            perTable.set(tableKey, list);
         }
         const byTable = new Map<string, LiveTableVerdict>();
         for (const [tableKey, tableVerdicts] of perTable) {
            if (!tableVerdicts.every(liveFor)) continue;
            const first = tableVerdicts[0];
            byTable.set(tableKey, {
               reason: first.reason as string,
               sourceName:
                  sources[first.sourceID]?.preaggregate?.baseSourceName ??
                  first.sourceName,
            });
         }
         verdicts = { byTable, bySource };
      }
      this.liveVerdictCache = { key, verdicts };
      return verdicts;
   }

   private destinationConfigured(name: string): boolean | undefined {
      const names = this.host.deploymentFacts()?.destinationNames();
      return names === undefined ? undefined : names.includes(name);
   }

   /** What the deployment facts answer for this plan's sources, as a memo key. */
   private deploymentSignature(): string {
      const facts = this.host.deploymentFacts();
      if (!facts) return "";
      const parts: string[] = [];
      for (const source of Object.values(
         this.host.plan.buildPlan?.sources ?? {},
      )) {
         const destination = source.annotationFields?.storage?.trim();
         parts.push(
            destination
               ? `d:${destination}=${this.destinationConfigured(destination)}`
               : `c:${source.connectionName}=${facts.connectionCanHoldTable(source.connectionName)}`,
         );
      }
      return parts.join("|");
   }

   /**
    * Why the bind refuses a planned source, as the bind decides it, or
    * undefined when it keeps it. Storage requires a positive eligibility, as
    * the bind does. Database rollups need not appear in the database
    * record, so only an authored source missing from it, or one it names as
    * refused, counts.
    */
   private refusedAtBind(
      sourceID: string,
      source: { name: string; sourceEntityId: string; origin?: string },
      tier: "database" | "storage",
   ): string | undefined {
      const { plan } = this.host;
      const never = "the source was never examined for eligibility";
      if (tier === "storage") {
         const eligibility = plan.sourceEligibility;
         if (!eligibility || eligibility.eligible.includes(sourceID)) {
            return undefined;
         }
         return eligibility.refused[sourceID] ?? never;
      }
      const eligibility = plan.databaseSourceEligibility;
      if (!eligibility) return undefined;
      const refused = eligibility.refused[source.sourceEntityId];
      if (refused !== undefined) return refused;
      if (
         !isRollupOrigin(source.origin) &&
         !eligibility.eligibleEntityIds.has(source.sourceEntityId)
      ) {
         return never;
      }
      return undefined;
   }

   /**
    * Each verdict's serve path for v1 and v2 declarations: the served copy
    * compiled over bindings declared from the plan's output columns names the
    * sources it re-roots; a rollup is the rollup leg's. Nothing is built or
    * read.
    */
   private async withCutoverPaths(
      verdicts: ServeVerdict[],
   ): Promise<ServeVerdict[]> {
      const host = this.host;
      const plan = host.plan;
      const sources = plan.servingPlan()?.sources ?? {};
      const eligible = plan.databaseSourceEligibility?.eligibleEntityIds;
      const viewBuilds = host.persistedViewBuildIDs();
      const entries: FreshnessManifest = {};
      for (const source of Object.values(sources)) {
         if (source.annotationFields?.storage?.trim()) continue;
         entries[source.sourceEntityId] = {
            tableName: `preview_${source.sourceEntityId}`,
            connectionName: source.connectionName,
         };
      }
      const v1Schema = (sourceID: string) =>
         v1StoredColumnsOf(host.declarationOf(sourceID));
      // A v1 `storage=` table stores the persisted query's own output (before
      // an extend block's `rename:` or `except:`, nests included), as the
      // bind will read it; a `#(materialize)` plan's columns are its own.
      const bindings = [
         ...this.previewStorageBindings().map((b) => {
            const sourceID = b.sourceIDs?.[0];
            if (!sourceID || plan.materializePlans[sourceID]) return b;
            const schema = v1Schema(sourceID);
            return schema?.length ? { ...b, schema } : b;
         }),
         ...deriveDatabaseServeBindings({
            entries,
            planSources: sources,
            materializePlans: plan.materializePlans,
            eligible: (sourceID) =>
               !!eligible?.has(sources[sourceID]?.sourceEntityId ?? ""),
            v1Schema,
            notV1: (sourceID) => viewBuilds.has(sourceID),
         }).map((b) => previewNativeSchema(b, sources)),
      ];
      const rerooted = new Set<string>();
      const throughBase = new Set<string>();
      const gateDeclined = new Set<string>();
      const problems = new Map<string, string>();
      for (const model of host.models().values()) {
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
         const path = servePathOf(v, {
            rerooted,
            throughBase,
            gateDeclined,
            problems,
         });
         return {
            ...v,
            servePath: path.path,
            ...(path.reason !== undefined
               ? { servePathReason: path.reason }
               : {}),
            // Deprecated names of the same two fields, emitted for one release.
            cutoverPath: path.path,
            ...(path.reason !== undefined
               ? { cutoverReason: path.reason }
               : {}),
         };
      });
   }

   /**
    * One serve binding per planned `storage=` source, as its first build would
    * bind it: declared from the plan's output columns at the DuckDB types the
    * build stores them as. The table it names is never read; a preview only
    * compiles.
    */
   private previewStorageBindings(): Binding[] {
      const host = this.host;
      const plan = host.plan;
      const out: Binding[] = [];
      for (const [sourceID, source] of Object.entries(
         plan.buildPlan?.sources ?? {},
      )) {
         const destinationName = source.annotationFields?.storage?.trim();
         if (!destinationName || isRollupOrigin(source.origin)) continue;
         const schema = (source.columns ?? []).flatMap((column) => {
            const type = PREVIEW_STORED_TYPES[String(column.type ?? "")];
            return column.name && type ? [{ name: column.name, type }] : [];
         });
         if (schema.length === 0) continue;
         out.push({
            kind: plan.materializePlans[sourceID] ? "materialize" : "persist",
            tier: "storage",
            sourceName: source.name,
            sourceIDs: [sourceID],
            destinationName,
            virtualHandle: source.sourceEntityId,
            tablePath: storageTablePath(
               destinationName,
               `preview_${source.sourceEntityId}`,
               host.destinationSchema(destinationName),
            ),
            schema,
         });
      }
      return out;
   }

   /**
    * The package's entry points and what each reads, or undefined when that
    * cannot be told. Memoized on the models map, which a load or reload
    * replaces wholesale along with the surface it curates.
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
            packageName: this.host.packageName(),
            code: errorCodeOf(error),
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
      const surface = this.host.publishedSurface();
      const entries =
         surface ??
         [...models].map(([modelPath, model]) => ({
            modelPath,
            names: (model.getSources() ?? []).flatMap((source) =>
               source.name ? [source.name] : [],
            ),
            queries: (model.getQueries?.() ?? []).flatMap((query) =>
               query.name ? [query.name] : [],
            ),
         }));
      const roots: EntryPointIndex["roots"] = [];
      for (const { modelPath, names, queries } of entries) {
         const model = models.get(modelPath);
         const modelDef = model?.getModelDef();
         // An empty or markdown-only notebook has no definition and names
         // nothing, so it reaches nothing: it must not unknow the rest. A
         // model that failed to compile is unknown, not empty, and stays so.
         if (
            model &&
            !modelDef &&
            !names.length &&
            !queries?.length &&
            model.getCompilationError() === undefined
         ) {
            continue;
         }
         if (!model || !modelDef) return undefined;
         // Gated as the router's veto finds it: the governor's walk of the
         // entry point's own and inherited gates (`collectEntryPointGates`).
         const gated = (struct: SourceDef): boolean =>
            collectEntryPointGates(struct, modelDef, new Set(), true).length >
            0;
         for (const name of names) {
            const entry = modelDef.contents?.[name];
            if (!entry || !isSourceDef(entry)) continue;
            roots.push({
               gated: gated(entry),
               ids: reachableSourceIDs(
                  structsReachedFrom([entry], modelDef),
                  modelDef,
               ),
            });
         }
         // A published named query reads its source, and whatever its
         // pipeline names; it is gated as its run target is.
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
            const source = base && isSourceDef(base) ? base : undefined;
            if (source) {
               for (const id of reachableSourceIDs(
                  structsReachedFrom([source], modelDef),
                  modelDef,
               )) {
                  ids.add(id);
               }
            }
            roots.push({ gated: !!source && gated(source), ids });
         }
      }
      const reached =
         surface === undefined
            ? undefined
            : new Set(roots.flatMap((root) => [...root.ids]));
      return { roots, reached };
   }
}

/**
 * What the models' served copies answer from storage, by author `sourceID`,
 * as each one's last committed compile left them: `on` when any model's
 * served copy is enabled, `uncompiled` when one has yet to compile for the
 * first time, and a `signature` that changes whenever either does.
 * `uncompiledHolds` says whether such a model holds a source, so its
 * judgement waits for that model's first compile.
 */
function servedCopyState(models: ReadonlyMap<string, Model>): {
   on: boolean;
   uncompiled: boolean;
   ids: ReadonlySet<string>;
   /** Re-rooted on a database table by some model, on a storage table by none. */
   databaseOnly: ReadonlySet<string>;
   /** Each model whose served copy has compiled, and what it left. */
   compiled: readonly CompiledServedCopy[];
   uncompiledHolds: (sourceID: string) => boolean;
   signature: string;
} {
   let on = false;
   let uncompiled = false;
   const ids = new Set<string>();
   const onDatabase = new Set<string>();
   const onStorage = new Set<string>();
   const compiled: CompiledServedCopy[] = [];
   const parts: string[] = [];
   const uncompiledModels: Model[] = [];
   for (const [modelPath, model] of models) {
      const reread = model.servedCopySourceIDs?.();
      if (reread === undefined) continue;
      on = true;
      if (reread === "uncompiled") {
         uncompiled = true;
         uncompiledModels.push(model);
         continue;
      }
      const databaseOnly = model.servedCopyDatabaseOnlyIDs?.() ?? new Set();
      for (const id of reread) {
         ids.add(id);
         (databaseOnly.has(id) ? onDatabase : onStorage).add(id);
      }
      const problems = model.servedCopyProblems?.() ?? [];
      const throughBase = model.servedCopyThroughBaseIDs?.() ?? new Set();
      compiled.push({
         modelPath,
         modelDef: model.getModelDef?.(),
         rerooted: reread,
         throughBase,
         problems,
      });
      parts.push(
         `${modelPath}=${problems.map((p) => `${p.reason}/${p.sourceID ?? ""}`).join(";")}` +
            `/${[...throughBase].sort().join(";")}`,
      );
   }
   // A source another model re-rooted on its storage table is not database-only.
   const databaseOnly = new Set(
      [...onDatabase].filter((id) => !onStorage.has(id)),
   );
   const signature = on
      ? `${uncompiled ? "uncompiled" : "compiled"}:${[...ids].sort().join(",")}:${[...databaseOnly].sort().join(",")}:${parts.join("|")}`
      : "off";
   // Holds as `servedCopyOutcomeOf` reads it: the model declares or
   // imports the source.
   const uncompiledHolds = (sourceID: string) =>
      uncompiledModels.some(
         (model) => sourceMapOf(model).node(sourceID) !== undefined,
      );
   return {
      on,
      uncompiled,
      ids,
      databaseOnly,
      compiled,
      uncompiledHolds,
      signature,
   };
}

/**
 * A model's source map, read once per model: its IR never changes, and
 * `/status` reads the verdicts on every poll.
 */
const sourceMaps = new WeakMap<Model, SourceMap>();
function sourceMapOf(model: Model): SourceMap {
   let map = sourceMaps.get(model);
   if (!map) {
      map = SourceMap.of(model.getModelDef?.());
      sourceMaps.set(model, map);
   }
   return map;
}

/** One model's compiled served copy, as the verdicts read it. */
interface CompiledServedCopy {
   modelPath: string;
   modelDef: unknown;
   /** The sources it re-rooted on a stored table. */
   rerooted: ReadonlySet<string>;
   /** The sources it left as written, read through a base it re-rooted. */
   throughBase: ReadonlySet<string>;
   /** What its `served-copy-not-served` warnings are read from. */
   problems: readonly ServedCopyProblem[];
}

/**
 * How the compiled served copies left one source they read, the same for a
 * `#(materialize)` source and a `#@ persist` one. A model reads its table
 * when its copy re-roots it, or reads it through a re-rooted base whose
 * table it shares (an extension inheriting `#@ persist`, one content
 * address); an extension with a table of its own read through its base does
 * not read that table. Every model that holds it (its compiled model
 * declares or imports it, or a warning names it) and does not read it is
 * listed, with every `served-copy-not-served` reason it gives for the source
 * (its own, and its whole copy's), most actionable first. When no model
 * holds it and none reads it, the declaring model is listed, `not_rerooted`.
 */
function servedCopyOutcomeOf(
   sourceID: string,
   declaredIn: string | undefined,
   compiled: readonly CompiledServedCopy[],
   entityOf: (sourceID: string) => string | undefined,
): ServedCopyOutcome {
   const entity = entityOf(sourceID);
   let serves = false;
   const notServedIn: NotServedIn[] = [];
   for (const copy of compiled) {
      const sharesRerootedTable =
         copy.throughBase.has(sourceID) &&
         entity !== undefined &&
         [...copy.rerooted].some((id) => entityOf(id) === entity);
      if (copy.rerooted.has(sourceID) || sharesRerootedTable) {
         serves = true;
         continue;
      }
      const own = copy.problems.filter(
         (p) => p.sourceID === sourceID && !p.servedFromDatabase,
      );
      const holds =
         own.length > 0 ||
         SourceMap.of(copy.modelDef).node(sourceID) !== undefined;
      if (!holds) continue;
      const whole = copy.problems.filter((p) => p.sourceID === undefined);
      const reasons = byActionability([...own, ...whole].map((p) => p.reason));
      notServedIn.push({
         model: copy.modelPath,
         reasons: reasons.length > 0 ? reasons : ["not_rerooted"],
      });
   }
   if (!serves && notServedIn.length === 0 && declaredIn !== undefined) {
      notServedIn.push({ model: declaredIn, reasons: ["not_rerooted"] });
   }
   notServedIn.sort((a, b) => a.model.localeCompare(b.model));
   return { serves, notServedIn };
}

/** A plan source's wire columns with both a name and a type. */
function declaredColumnsOfPlan(
   columns: readonly { name?: string; type?: string }[] | undefined,
): { name: string; type: string }[] {
   return (columns ?? []).flatMap((c) =>
      c.name && c.type ? [{ name: c.name, type: c.type }] : [],
   );
}

/**
 * On a dry run, the path that will answer queries on a source (`servePath`)
 * and, when that is not its own table, why (`servePathReason`).
 */
function servePathOf(
   v: ServeVerdict,
   preview: {
      rerooted: ReadonlySet<string>;
      throughBase: ReadonlySet<string>;
      gateDeclined: ReadonlySet<string>;
      problems: ReadonlyMap<string, string>;
   },
): { path: "served_copy" | "rollup" | "live"; reason?: string } {
   // A refused declaration has no table to serve from, a rollup's included.
   if (v.verdict === "live" && v.reason === "refused") {
      return { path: "live", reason: "refused" };
   }
   if (v.origin === "rollup") return { path: "rollup" };
   // A row-level gate declared on a base the re-root replaces cannot be
   // grafted on the served copy: its queries are answered live.
   if (preview.gateDeclined.has(v.sourceID)) {
      return { path: "live", reason: "inherited_gate" };
   }
   if (preview.rerooted.has(v.sourceID)) return { path: "served_copy" };
   // An extension of a re-rooted source, left as written: it reads that
   // source's table by name.
   if (
      preview.throughBase.has(v.sourceID) &&
      !preview.problems.has(v.sourceID)
   ) {
      return { path: "served_copy", reason: "through_base" };
   }
   return {
      path: "live",
      reason: preview.problems.get(v.sourceID) ?? v.reason ?? "not_rerooted",
   };
}

/**
 * The stored types of the columns a bound `storage=` table's own gate
 * reads, and whether its column contract records each exact (`gateExact`),
 * for the policy's exactness rule, as the router reads each query's. A
 * table not bound yet has none to read, and is judged once it is bound.
 *
 * A rollup's gates are its base's, read off the base's def in the rollup's
 * model (the file that declares the base), with whether they read no column
 * at all (locks only), as the router marks a read.
 */
function storageGateRead(
   sourceID: string,
   storageBindings: readonly Binding[],
   contracts: ReadonlyMap<string, ColumnContract> | undefined,
   buildPlan: BuildPlan | undefined,
   models: Map<string, Model>,
): Pick<
   TableRead,
   "gateColumnTypes" | "gateColumnsExactRecorded" | "gatesReadNoColumn"
> {
   const bound = storageBindings.filter((b) => b.sourceIDs?.includes(sourceID));
   if (bound.length === 0) return { gateColumnTypes: [] };
   const found = gatedDefOf(sourceID, buildPlan, models);
   if (found === undefined) return { gateColumnTypes: undefined };
   const exprs = entryPointGateExprs(found.def, found.modelDef);
   const reads =
      exprs === undefined
         ? undefined
         : // Its own `where:` terms were applied by the build.
           gateReads(found.def, exprs, found.modelDef, new Set([sourceID]));
   const { types, exact } = gateColumnsExactOf(
      reads,
      bound.map((b) => contracts?.get(b.virtualHandle)),
   );
   const readsNoColumn =
      reads !== undefined &&
      !reads.unresolved &&
      reads.columns.length === 0 &&
      reads.names.size === 0;
   const listed = !types.some((t) => t === undefined);
   return {
      gateColumnTypes: listed ? types.flatMap((t) => t ?? []) : undefined,
      ...(listed
         ? { gateColumnsExactRecorded: exact.every((e) => e === true) }
         : {}),
      ...(readsNoColumn ? { gatesReadNoColumn: true } : {}),
   };
}

/**
 * The def whose entry-point gates a stored source's table is read past: a
 * rollup's base, in the rollup's model; any other source's own.
 */
function gatedDefOf(
   sourceID: string,
   buildPlan: BuildPlan | undefined,
   models: Map<string, Model>,
): { def: unknown; modelDef: unknown } | undefined {
   const planned = buildPlan?.sources?.[sourceID];
   if (isRollupOrigin(planned?.origin)) {
      const base = planned?.preaggregate?.baseSourceName;
      const modelDef = planned?.modelPath
         ? models.get(planned.modelPath)?.getModelDef()
         : undefined;
      const contents = (
         modelDef as { contents?: Record<string, unknown> } | undefined
      )?.contents;
      const def = base !== undefined ? contents?.[base] : undefined;
      return def === undefined ? undefined : { def, modelDef };
   }
   for (const model of models.values()) {
      const modelDef = model.getModelDef();
      const def = modelDef
         ? SourceMap.of(modelDef).node(sourceID)?.def
         : undefined;
      if (def !== undefined) return { def, modelDef };
   }
   return undefined;
}

/**
 * Whether a source's own table keeps its entry point's gates, as the serve
 * verdict reports it: the router's own question, asked before any query. A
 * rollup is asked of its leg past a gate over its own table, with its variant
 * proven at plan (`keptAtPlan`); any other table of the served copy's leg
 * over that table, read exactly.
 */
export function verdictKeepsGate(
   source:
      | { rollup: true; keptAtPlan: boolean; table: TableRead }
      | { rollup: false; table: TableRead },
   switches: GateSwitches,
): boolean {
   if (source.rollup) {
      // The variant proven at plan, and the rollup's own table read past
      // the gate as the router judges it (its tier, marks, and the stored
      // types of the columns the gate reads).
      return (
         mayAttempt("rollup", switches) &&
         source.keptAtPlan &&
         keepsGatesWhenReadExact("rollup", [source.table], switches)
      );
   }
   return keepsGatesWhenReadExact("served", [source.table], switches);
}
