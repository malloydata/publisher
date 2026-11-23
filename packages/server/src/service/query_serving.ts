// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Routing one query over the serving legs, and the reports that read how it
 * was served.
 *
 * The query pipeline (`model_serving.ts`) makes the legs for its pin
 * (`makeLegs`, serve/leg.ts) and routes over them with {@link variantsOf}:
 * each leg becomes one router candidate, and this is where read admission
 * is asked (before an attempt, by leg kind; after it, over the bindings the
 * answer read: `serve/read_admission.ts`), where a `LegDecline` is read for
 * the path report, and where a leg's skip is logged. A leg does neither. {@link answerOf} turns the
 * router's decision into the {@link Answer} every reader derives from, and
 * {@link queryPathFor} assembles the path report from it.
 *
 * Adding a leg needs nothing here: a leg module, its `makeLegs` entry and its
 * decline rows, and the router, read admission and the reports read it
 * through the answer.
 */

import type { ModelDef, QueryMaterializer } from "@malloydata/malloy";
import { gateSwitches, getPersistStorageMode } from "../config";
import {
   type CollationProbe,
   lookupProbe,
   observeAdmission,
   ReadAdmission,
   reportQueryWalk,
} from "./materialization/serve/read_admission";
import {
   recordQueryCatalogRead,
   recordQueryProbeCompile,
   recordQueryWalk,
} from "../materialization_metrics";
import { logger } from "../logger";
import { SourceMap, sourceChain } from "./materialization/text/source_map";
import {
   type Binding,
   isDatabase,
   type Pinned,
   type Snapshot,
} from "./materialization/domain/binding";
import type { ServedCopyServe } from "./materialization/serve/served_copy_serve";
import {
   explanationOfDecline,
   isDeclineOf,
   LegDecline,
} from "./materialization/domain/leg_decline";
import {
   type ApiQueryPath,
   buildQueryPath,
   type MaterializedTableCatalog,
   queryShapeOf,
   type StorageTable,
} from "./materialization/serve/query_path";
import type { BuildManifest } from "./materialization/domain/types";
import type { RollupVariantServe } from "./materialization/serve/rollup_variants";
import type { PersistedViewServe } from "./materialization/serve/view_variants";
import {
   type Answer,
   type Leg,
   type LegAnswer,
   type LegContext,
   type LegKind,
   LIVE_ANSWER,
   type QueryFacts,
} from "./materialization/serve/leg";
import { storageServes } from "./materialization/domain/placement";
import { originOfTable } from "./materialization/domain/table_plans";
import {
   readPreparedQuery,
   resolveStructRefSource,
} from "./governance/ir/prepared_query";
import type {
   RouteDecision,
   Variant,
   VariantSkipExplanation,
} from "./query_router";

/**
 * The decline vocabularies each leg's codes are in. The query leg declines
 * in its own (`query_*`) and, through the caller-text checks it shares with
 * the view leg, in the view leg's (`view_*`).
 */
const DECLINING_LEGS = {
   view: ["view"],
   query: ["query", "view"],
   rollup: ["rollup"],
   served: ["served_copy"],
} as const satisfies Record<LegKind, readonly string[]>;

/** Whether `err` is a decline of the leg `kind`, in any of its vocabularies. */
function isLegDecline(kind: LegKind, err: unknown): err is LegDecline {
   return DECLINING_LEGS[kind].some((leg) => isDeclineOf(err, leg));
}

/** What each leg's skip is logged as, at debug. */
const SKIP_MESSAGE: Record<LegKind, string> = {
   view: "the persisted view does not answer this query",
   query: "the persisted named query does not answer this query",
   rollup: "no #(rollup) rollup answers this query",
   served: "served copy cannot answer this query",
};

/** Each leg's `STALE` decline. */
const STALE_DECLINE = {
   served: "served_copy_stale",
   rollup: "rollup_stale",
   view: "view_stale",
   query: "query_stale",
} as const satisfies Record<LegKind, LegDecline["code"]>;

/**
 * A leg's skip, at debug: it repeats on every query of a dashboard, and the
 * path report and `publisher_query_route_total` already say why.
 */
function logSkip(kind: LegKind, modelPath: string, err: unknown): void {
   if (!logger.isDebugEnabled()) return;
   logger.debug(SKIP_MESSAGE[kind], {
      modelPath,
      code: isLegDecline(kind, err) ? err.code : "error",
      error: err instanceof Error ? err.message : String(err),
   });
}

/**
 * The router's candidates for one query: one per leg, in the legs' order.
 * Read admission (`serve/read_admission.ts`, deciding by the pure
 * `admitRead`), its switches read once for the query, decides whether a leg
 * may be tried past an entry-point gate (R3: the leg compiles the author's
 * own text, gates included, so `govern()` grafts and proves the gate on it),
 * and whether the answer it produced is used: before the gate, by the row
 * scope and the caller's own filters; past it, by every rule.
 */
export function variantsOf(
   legs: readonly Leg[],
   facts: QueryFacts,
   pinned: Pinned,
   args: { modelPath: string; probe?: CollationProbe },
): Variant<LegAnswer>[] {
   const admission = new ReadAdmission(
      facts,
      gateSwitches(),
      args.probe ?? lookupProbe,
      pinned.snapshot,
   );
   return legs.map((leg) => ({
      kind: leg.kind,
      applicable: leg.applies(facts, pinned),
      keepsContracts: admission.mayAttempt(leg.kind),
      keepsContractsFor: async (answer: LegAnswer) =>
         (await admission.admit(leg.kind, answer, true)).used,
      attempt: async () => {
         const answer = await leg.attempt(facts, pinned);
         // A table a failed reload found built from a changed upstream
         // (Snapshot.withdrawn) is not read: the query answers as live does.
         const withdrawn = pinned.snapshot.withdrawn;
         if (
            withdrawn &&
            (answer.tablesRead ?? []).some((b) =>
               withdrawn.has(b.virtualHandle),
            )
         ) {
            throw new LegDecline(
               STALE_DECLINE[leg.kind],
               "a table this answer reads was built from an upstream table that changed since its build",
            );
         }
         const outcome = await admission.admit(leg.kind, answer, false);
         if (!outcome.used && "decline" in outcome) throw outcome.decline;
         if (leg.kind === "served") {
            logger.debug("Serving query from the served copy (virtual)", {
               modelPath: args.modelPath,
               tables: (answer.tablesRead ?? []).map((b) => b.sourceName),
            });
         }
         return { runnable: answer.runnable, extra: answer };
      },
      explain: (err): VariantSkipExplanation | undefined =>
         isLegDecline(leg.kind, err) ? explanationOfDecline(err) : undefined,
      onSkip: (err) => logSkip(leg.kind, args.modelPath, err),
   }));
}

// What read admission reports, counted and logged here, outside its layer.
observeAdmission({
   catalogRead: recordQueryCatalogRead,
   probeCompile: recordQueryProbeCompile,
   queryWalk: recordQueryWalk,
});

/** The answer the router's decision describes, before anything runs. */
export function answerOf(route: RouteDecision<LegAnswer>): Answer {
   if (route.served !== "variant") {
      return Object.freeze({ ...LIVE_ANSWER, considered: route.considered });
   }
   return Object.freeze({
      leg: route.kind,
      considered: route.considered,
      answer: route.extra,
      pastEntryPointGate: route.pastEntryPointGate === true,
      runFailed: false,
      databaseRead: new Set<string>(),
   });
}

/**
 * The build manifest a run is handed, wrapped to record which entries Malloy
 * looked up and found: the database tables it substituted. Malloy looks a
 * persisted source up by its key in exactly one place (`persistedTableFor`,
 * `buildManifest.entries[buildId]`), so a hit is a table the SQL reads, with no
 * reading of the SQL text.
 *
 * Malloy also ENUMERATES the entries before it compiles (it checks every
 * entry's table path), and an enumeration reads each value too. Those reads
 * are not lookups: recording them reported every bound table as having
 * answered, including for a `#@ -persist` reader that recomputes and for a
 * source Malloy never substitutes.
 *
 * The two are told apart by which object they read, not by the order of the
 * reads. Every read of `manifest.entries` is handed a fresh view of the same
 * entries, and a view's reads are lookups only while nothing has enumerated
 * that view. Every enumeration (`Object.entries`, `Object.keys`,
 * `Object.values`, spread, `for…in`, `JSON.stringify`, `Object.assign`) asks
 * for the object's own keys first ([[OwnPropertyKeys]], the `ownKeys` trap),
 * and a keyed lookup never does, whether it reads the value alone or checks
 * `hasOwn` first (Malloy's `safeRecordGet`). So this holds for any enumeration
 * and any lookup form, in any order, sync or async. (The order heuristic this
 * replaced, "a read right after its own key's descriptor is an enumeration",
 * missed `JSON.stringify`, which describes every key before reading any, and
 * would have dropped every real lookup had Malloy guarded it with `hasOwn`, as
 * its own `safeRecordGet` does.) It assumes only that Malloy reaches
 * `entries` afresh for a lookup, as `persistedTableFor` does, rather than
 * enumerating and then looking up through one held reference, or re-reading
 * `entries` for each value of an enumeration (`for (k in m.entries)
 * m.entries[k]`). `query_serving_malloy.spec.ts` runs a real compile against
 * the pinned Malloy and fails loudly if either assumption stops holding.
 */
export function recordingManifest(
   manifest: BuildManifest | undefined,
   read: Set<string>,
): BuildManifest | undefined {
   if (!manifest) return undefined;
   const target = manifest.entries;
   const view = (): BuildManifest["entries"] => {
      let enumerated = false;
      return new Proxy(target, {
         ownKeys(t) {
            enumerated = true;
            return Reflect.ownKeys(t);
         },
         get(t, key, receiver) {
            if (
               !enumerated &&
               typeof key === "string" &&
               Object.hasOwn(t, key)
            ) {
               read.add(key);
            }
            return Reflect.get(t, key, receiver);
         },
      });
   };
   const wrapped = { ...manifest } as BuildManifest;
   Object.defineProperty(wrapped, "entries", {
      get: view,
      enumerable: true,
      configurable: true,
   });
   return wrapped;
}

/** What serving reads from the model that owns the query. */
export interface QueryServingHost {
   readonly modelPath: string;
   /** The model's own `#@ persist` `storage=` tables in a snapshot, for the path report. */
   storageTables(snapshot: Snapshot): readonly Binding[];
   /** The served copy on `virtual` (materialization v2), the `served` candidate. */
   readonly servedCopy: ServedCopyServe;
   modelDef(): ModelDef | undefined;
   materializedTables(): MaterializedTableCatalog | undefined;
   /** The discovery surface's names, or undefined when everything is listed. */
   publishedNames(): Set<string> | undefined;
   admitsSourceByName(name: string): boolean;
   /** Every source's `sourceID` chain in the model. */
   sourceChains(): ReadonlyMap<string, readonly string[]>;
   /** The `#(rollup)` rollup leg (materialization v2, phase 4). */
   readonly rollupVariants: RollupVariantServe;
   /** The `#@ persist` view leg (materialization v2, phase 4, P14). */
   readonly persistedViews: PersistedViewServe;
   /** The `#@ persist` named-query leg. */
   readonly persistedQueries: NonNullable<LegContext["persistedQueries"]>;
}

/**
 * The path report for one answered query, or undefined when it cannot be
 * assembled (logged at debug). Reads the author's compile, the
 * {@link Answer} and the query's pin; changes nothing.
 */
export async function queryPathFor(
   host: QueryServingHost,
   answer: Answer,
   args: {
      /** The author's own compile: its names, and no grafted gate filter. */
      runnable: QueryMaterializer;
      sourceName: string | undefined;
      viewName: string | undefined;
      /** The query's pinned tables: what it could read, at its instant. */
      pinned: Pinned;
      includeHidden: boolean;
      /** The text the router saw, to name the persisted view it calls. */
      queryString?: string;
   },
): Promise<ApiQueryPath | undefined> {
   try {
      const prepared = await readPreparedQuery(args.runnable);
      const modelDef = prepared.modelDef ?? host.modelDef();
      const struct = resolveStructRefSource<{ as?: string; name?: string }>(
         prepared.structRef,
         modelDef,
      );
      const sourceName =
         args.sourceName ??
         (typeof prepared.structRef === "string"
            ? prepared.structRef
            : (struct?.as ?? ""));
      const shape = queryShapeOf(prepared.query, struct, {
         sourceName,
         viewName: args.viewName,
      });
      const { pinned } = args;
      // The database tables bound when the query was pinned.
      const planned = host.materializedTables();
      const catalog = planned && {
         ...planned,
         bound: pinned.snapshot.entries,
      };
      const variantRollups = host.rollupVariants.hasRollups()
         ? host.rollupVariants.rollups()
         : [];
      // The persisted view or named query the query calls, if any: its table
      // is the first candidate (R1), whatever the router decided about it.
      const routed = answer.leg === "live" ? undefined : answer.answer;
      const viewTable =
         routed?.viewTable ??
         (args.queryString !== undefined
            ? (host.persistedViews.reportedTable(args.queryString, pinned) ??
              host.persistedQueries.reportedTable(args.queryString, pinned))
            : undefined);
      // The `#(materialize)` tables the served copy reads at this pin.
      const servedBound = host.servedCopy.materializeBindings(pinned, "bound");
      // Nothing materialized for this model: the answer is live and there is
      // nothing to name, so skip the chain walks and the catalog reads.
      if (
         (catalog?.planned.size ?? 0) === 0 &&
         host.storageTables(pinned.snapshot).length === 0 &&
         servedBound.length === 0 &&
         variantRollups.length === 0 &&
         viewTable === undefined &&
         answer.considered.length === 0
      ) {
         return {
            query: shape,
            servedBy: { kind: "live" },
            considered: [],
            notConsidered: [],
            warnings: [],
         };
      }
      const toTable = (b: Binding): StorageTable => ({
         sourceName: b.sourceName,
         sourceIDs: b.sourceIDs,
         origin: originOfTable(b.kind, b.sourceIDs?.[0] ?? ""),
         tablePath: b.tablePath,
         freshAsOf: b.freshAsOf,
         ...(isDatabase(b) ? { database: true as const } : {}),
      });
      const storageTables = host.storageTables(pinned.snapshot);
      const published = args.includeHidden ? undefined : host.publishedNames();
      return buildQueryPath({
         shape,
         modelPath: host.modelPath,
         chain: queriedChain(host, struct, modelDef),
         catalog,
         freshDatabase: pinned.hasEntries
            ? new Set(Object.keys(pinned.compilerManifest.entries))
            : undefined,
         // The `storage=` tables, the `#(materialize)` tables the served copy
         // reads and the rollup tables, so a table on the chain is named
         // whichever candidate answered.
         storage: {
            all: [
               ...storageTables,
               ...servedBound,
               ...host.rollupVariants.rollupBindings(pinned, "bound"),
            ].map(toTable),
            fresh: [
               ...storageTables.filter((b) => pinned.serves(b)),
               ...host.servedCopy.materializeBindings(pinned, "fresh"),
               ...host.rollupVariants.rollupBindings(pinned, "fresh"),
            ].map(toTable),
         },
         rollups: variantRollups.map((r) => ({ ...r, variant: true })),
         // Every `storage=` source carries a package warning when the tier
         // is off or write-only (see storageWarnings).
         storageWarned: !storageServes(getPersistStorageMode()),
         considered: answer.considered,
         storageTablesRead: routed?.tablesRead?.map(toTable),
         ...(answer.leg === "rollup"
            ? {
                 rollupTablesRead: answer.answer.shapeBindings
                    .filter((b) => b.kind !== "rollup")
                    .map(toTable),
              }
            : {}),
         ...(answer.leg === "served" ? { via: "served_copy" as const } : {}),
         servedCopyTables: new Set(servedBound.map((b) => b.tablePath)),
         servedCopyDetails: servedCopyDetailsOf(
            servedBound,
            host.servedCopy.problems(),
         ),
         runFailed: answer.leg !== "live" && answer.runFailed,
         databaseRead: answer.databaseRead,
         ...(viewTable ? { view: viewTable } : {}),
         ...(routed?.rollupMember ? { rollupMember: routed.rollupMember } : {}),
         ...(variantRollups.length > 0
            ? {
                 joinedRollupBases: host.rollupVariants.joinedRollupBaseIDs(
                    struct,
                    prepared,
                 ),
              }
            : {}),
         chains: host.sourceChains(),
         // The discovery surface: a name the caller cannot list is never
         // named, unless the caller may see hidden sources.
         ...(args.includeHidden
            ? {}
            : {
                 isVisibleSource: (name: string) =>
                    (published === undefined || published.has(name)) &&
                    host.admitsSourceByName(name),
              }),
      });
   } catch (err) {
      logger.debug("Path report unavailable", {
         modelPath: host.modelPath,
         error: err instanceof Error ? err.message : String(err),
      });
      return undefined;
   }
}

/**
 * Each served-copy table's `column_types` detail, by table path: the columns
 * its source's problem names, for the path report's `detail`.
 */
function servedCopyDetailsOf(
   bindings: readonly Binding[],
   problems: ReturnType<ServedCopyServe["problems"]>,
): ReadonlyMap<string, string> {
   const out = new Map<string, string>();
   for (const p of problems) {
      if (p.reason !== "column_types" || !p.detail || !p.sourceID) continue;
      for (const b of bindings) {
         if (b.sourceIDs?.includes(p.sourceID)) out.set(b.tablePath, p.detail);
      }
   }
   return out;
}

/**
 * The queried source's chain for the path report: the chain the model
 * recorded for a source it declares ({@link QueryServingHost.sourceChains},
 * read once per compile); a source it does not declare (one only the
 * query's compile holds) is walked, and counted.
 */
export function queriedChain(
   host: Pick<QueryServingHost, "modelDef" | "sourceChains">,
   struct: unknown,
   queryModelDef: unknown,
): string[] {
   const id = (struct as { sourceID?: unknown } | undefined)?.sourceID;
   const recorded =
      typeof id === "string" ? host.sourceChains().get(id) : undefined;
   if (recorded) return [...recorded];
   reportQueryWalk("path_chain");
   return chainOfQuerySource(struct, queryModelDef, host.modelDef());
}

/**
 * The queried source's chain ({@link sourceChain}), walked on the model's own
 * definition when it declares the source: the query's compile may not carry
 * the model's imports, and an extension of a table-backed source declared in
 * another file finds its base only through them (P2-41). Otherwise on the
 * query's.
 */
function chainOfQuerySource(
   struct: unknown,
   queryModelDef: unknown,
   modelDef: unknown,
): string[] {
   const id = (struct as { sourceID?: unknown } | undefined)?.sourceID;
   const own =
      typeof id === "string" && modelDef !== queryModelDef
         ? SourceMap.of(modelDef).node(id)?.def
         : undefined;
   return own ? sourceChain(own, modelDef) : sourceChain(struct, queryModelDef);
}
