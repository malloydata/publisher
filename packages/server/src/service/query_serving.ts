// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Routing one query over the serving legs, and the reports that read how it
 * was served.
 *
 * The query pipeline (`model_serving.ts`) makes the legs for its pin
 * (`makeLegs`, serve/leg.ts) and routes over them with {@link variantsOf}:
 * each leg becomes one router candidate, and this is where the gate policy is
 * asked (before an attempt, by leg kind; after it, over the bindings the
 * answer read), where a `LegDecline` is read for the path report, and where a
 * leg's skip is logged. A leg does neither. {@link answerOf} turns the
 * router's decision into the {@link Answer} every reader derives from, and
 * {@link queryPathFor} assembles the path report from it.
 *
 * Adding a leg needs nothing here: a leg module, its `makeLegs` entry and its
 * decline rows, and the router, the gate policy and the reports read it
 * through the answer.
 */

import type { ModelDef, QueryMaterializer } from "@malloydata/malloy";
import { gateSwitches, getPersistStorageMode } from "../config";
import {
   answerKeepsGates,
   GATE_LEGS,
   gateColumnsExact,
   legMayPassGate,
   SERVING_GATE_CONTEXT,
   type TableRead,
} from "./materialization/domain/gate_policy";
import {
   type GateReads,
   gateColumnTypesOf,
   gateReadsOfQuery,
   queryFilterReadsOfQuery,
   resultReadsOfQuery,
   storedTableCollationRefusal,
   storedTypeIsString,
   stringGateCollationRefusal,
} from "./materialization/serve/gate_reads";
import { rowFilterOnComposite } from "./composite_row_filter";
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
import { originOfTable } from "./materialization/domain/table_plans";
import { extractRunQueryCall, extractRunViewCall } from "./query_text";
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
 * The gate policy (`domain/gate_policy.ts`) decides, its switches read once
 * for the query, whether a leg may be tried past an entry-point gate (R3:
 * the leg compiles the author's own text, gates included, so `govern()`
 * grafts and proves the gate on it) and whether the answer it produced keeps
 * the gates given the tables it read.
 */
export function variantsOf(
   legs: readonly Leg[],
   facts: QueryFacts,
   pinned: Pinned,
   args: { modelPath: string },
): Variant<LegAnswer>[] {
   const switches = gateSwitches();
   // What the entry point's gates and row scope read, from the author's
   // compile: read once per set of stored sources an answer read (a `where:`
   // inside one was applied by its build), and only when needed.
   const reads = new Map<string, Promise<GateReads | undefined>>();
   const gateReadsFor: GateReadsFor = (tables) => {
      const stored = storedSourceIDs(tables);
      const key = [...stored].sort().join("\n");
      let r = reads.get(key);
      if (r === undefined) {
         r = gateReadsOfQuery(facts.authorRunnable, stored);
         reads.set(key, r);
      }
      return r;
   };
   let composite: Promise<boolean> | undefined;
   const compositeOnce = () =>
      (composite ??= entryRowFilterOnComposite(facts.authorRunnable));
   const givens = facts.givens as Record<string, unknown> | undefined;
   return legs.map((leg) => {
      // A leg with no row in the gate policy (the query leg) is never tried
      // past an entry-point gate: the policy fails closed.
      const gateLeg = (GATE_LEGS as readonly LegKind[]).includes(leg.kind)
         ? (leg.kind as (typeof GATE_LEGS)[number])
         : undefined;
      return {
         kind: leg.kind,
         applicable: leg.applies(facts, pinned),
         keepsContracts:
            gateLeg !== undefined &&
            legMayPassGate(gateLeg, switches, SERVING_GATE_CONTEXT),
         keepsContractsFor: async (answer: LegAnswer) => {
            if (gateLeg === undefined) return false;
            // Live refuses every caller of a source whose row filter was
            // declared on a composite: no stored answer is kept for it.
            if (await compositeOnce()) return false;
            const read = answer.gateTablesRead ?? answer.tablesRead ?? [];
            const tables = await withGateColumnTypes(read, gateReadsFor);
            if (
               (await storedComparisonRefusal(
                  answer,
                  tables,
                  facts.authorRunnable,
                  () => gateReadsFor(read),
                  givens,
               )) !== undefined
            ) {
               return false;
            }
            return answerKeepsGates(
               gateLeg,
               tables,
               switches,
               SERVING_GATE_CONTEXT,
            );
         },
         attempt: async () => {
            const answer = await leg.attempt(facts, pinned);
            if (leg.kind === "served" || leg.kind === "rollup") {
               await assertRowScopeExact(
                  answer,
                  gateReadsFor,
                  facts.authorRunnable,
                  leg.kind,
                  givens,
               );
            }
            await assertQueryFiltersCompareAsLive(
               answer,
               leg.kind,
               facts,
               givens,
            );
            await assertResultOrdersAsLive(answer, leg.kind, facts, givens);
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
      };
   });
}

/** What the gates and row scope read, given the tables an answer read. */
type GateReadsFor = (
   tables: readonly TableRead[],
) => Promise<GateReads | undefined>;

/** The sources (by `sourceID`) whose stored tables `tables` are. */
function storedSourceIDs(tables: readonly TableRead[]): Set<string> {
   const ids = new Set<string>();
   for (const t of tables) {
      for (const id of (t as Partial<Pick<Binding, "sourceIDs">>).sourceIDs ??
         []) {
         ids.add(id);
      }
   }
   return ids;
}

/**
 * Each `storage=` table an answer read, with the stored types of the columns
 * the entry point's gates read (`gateColumnTypes`), for the gate policy's
 * exactness rule: a gate column stored as a float, which an exact number
 * rounds to, admits a neighbour's rows. A database table is built in the
 * source's own types and is left as it is.
 */
async function withGateColumnTypes(
   tables: readonly TableRead[],
   gateReadsFor: GateReadsFor,
): Promise<readonly TableRead[]> {
   const retyped = (t: TableRead) =>
      (t as Partial<Pick<Binding, "retypedColumns">>).retypedColumns ?? [];
   const storage = tables.filter((t) => t.tier === "storage");
   if (storage.length === 0 && !tables.some((t) => retyped(t).length > 0)) {
      return tables;
   }
   const reads = await gateReadsFor(tables);
   // Judged together: a gate column held by none of the storage tables the
   // answer read leaves every one of them unproven.
   const types = gateColumnTypesOf(
      reads,
      storage.map((t) => (t as Partial<Pick<Binding, "schema">>).schema),
   );
   const typeOf = new Map(storage.map((t, i) => [t, types[i]]));
   const held = new Set((reads?.columns ?? []).flat());
   // Locks only: the gates name no field at all, so no stored row can change
   // their outcome (the gate policy's `gatesReadNoColumn`).
   const readsNoColumn =
      reads !== undefined &&
      !reads.unresolved &&
      reads.columns.length === 0 &&
      reads.names.size === 0;
   return tables.map((t) => {
      if (t.tier === "storage") {
         return {
            ...t,
            gateColumnTypes: typeOf.get(t),
            ...(readsNoColumn ? { gatesReadNoColumn: true } : {}),
         };
      }
      // A database table is built in the source's own types, so only a
      // column the bind found moved since then (altered in the warehouse, or
      // a 128-bit integer its DuckLake catalog holds as a DOUBLE) is judged,
      // by the type it holds now. Unreadable gates judge every such column.
      const moved = retyped(t);
      if (moved.length === 0) return t;
      return {
         ...t,
         gateColumnTypes: (reads === undefined || reads.unresolved
            ? moved
            : moved.filter((c) => held.has(c.name))
         ).map((c) => c.type),
      };
   });
}

/**
 * A served answer over a source whose rows a `where:` scopes (its own, one it
 * extends, a composite member's, a query-shaped source's base's or stage's,
 * gated or not) is kept only when the columns that scope reads are stored
 * exactly, by the gate policy's own rule (`gateColumnsExact`): the policy
 * itself is asked only past an entry-point gate, and a scope on a column
 * stored as a float admits a neighbour's rows. Every term counts, not only one
 * that reads a given: a scope written as a literal merges neighbours too.
 * Fails closed on a source whose reads cannot be listed.
 */
async function assertRowScopeExact(
   answer: LegAnswer,
   gateReadsFor: GateReadsFor,
   authorRunnable: QueryMaterializer,
   leg: "served" | "rollup",
   givens: Record<string, unknown> | undefined,
): Promise<void> {
   const tables = answer.gateTablesRead ?? answer.tablesRead ?? [];
   const judged = await withGateColumnTypes(tables, gateReadsFor);
   const reads = await gateReadsFor(tables);
   // Unreadable reads name no scope to judge: database tables none of whose
   // columns moved are served as before; anything else fails closed below.
   if (reads === undefined && judged === tables) return;
   // A gated entry point's scope is judged by the gate policy, past the gate
   // (`keepsContractsFor`), with its gate's columns: here, only an ungated one.
   if (reads !== undefined && (!reads.scoped || reads.gated)) return;
   // The tables the scope was judged on, so the path report names each one
   // skipped for it (a database `#@ persist` table included).
   const tableIds = tables.flatMap((t) => {
      const handle = (t as { virtualHandle?: unknown }).virtualHandle;
      return typeof handle === "string" ? [handle] : [];
   });
   const decline = (why: string) =>
      leg === "served"
         ? new LegDecline("served_copy_not_served", why, { tableIds })
         : new LegDecline("rollup_row_level_gate", why, {
              shapeCheck: true,
              tableIds,
           });
   // The exactness rule is the served copy's; the rollup leg judges its own
   // stored columns (`storedRollupReads`), so it is asked only the
   // comparison rules below.
   // Database tables none of whose columns moved (`judged === tables`) hold
   // the source's types, so only their comparison is judged: a collation
   // altered after the build changes no type.
   if (
      leg === "served" &&
      judged !== tables &&
      !judged.every(gateColumnsExact)
   ) {
      throw decline(
         "the query's source scopes its rows by a column a stored table does not hold exactly",
      );
   }
   if (
      (await storedComparisonRefusal(
         answer,
         judged,
         authorRunnable,
         () => gateReadsFor(tables),
         givens,
      )) !== undefined
   ) {
      throw decline(
         "the query's source scopes its rows by a column a stored table does not compare as its source does",
      );
   }
}

/**
 * The caller's own query filters (`where:`, `having:`, a filtered measure's,
 * a view's or a nest's) are applied to the stored rows an answer reads, so
 * each string comparison they make must compare there as live compares it,
 * by the same rules as the source's scope (`serve/gate_collation.ts`): the
 * stored column's collation identity against its source's on a Postgres
 * database table, the definition and the instance on DuckDB, and the source
 * side's own rule on a `storage=` table. A filter that compares no string
 * is not judged. Fails closed (the answer is declined, live answers) when
 * the query's filters cannot be read.
 *
 * Which filters a leg applies to stored rows: every one on the served copy
 * and a rollup (a rollup's build applied each measure it stores, so a
 * measure's own filter is not one), and only the later stages' on a
 * persisted view or named query, whose own stages its build applied.
 */
async function assertQueryFiltersCompareAsLive(
   answer: LegAnswer,
   leg: LegKind,
   facts: QueryFacts,
   givens: Record<string, unknown> | undefined,
): Promise<void> {
   let tables: readonly TableRead[];
   let fromStage = 0;
   if (leg === "view" || leg === "query") {
      const later = (
         leg === "view"
            ? extractRunViewCall(facts.queryString)
            : extractRunQueryCall(facts.queryString)
      )?.laterStages;
      // An exact call's result was computed by its build, as live's is.
      if (later === 0) return;
      const pipeline = (
         await readPreparedQuery(facts.authorRunnable).catch(() => undefined)
      )?.pipeline;
      // Unread: every stage is judged, which only declines more.
      if (later !== undefined && Array.isArray(pipeline)) {
         fromStage = pipeline.length - later;
      }
      tables = answer.tablesRead ?? [];
      if (answer.resultTableRead) tables = [...tables, answer.resultTableRead];
   } else {
      tables = answer.gateTablesRead ?? answer.tablesRead ?? [];
   }
   // A served copy or rollup that reads no stored table applies nothing to
   // one; a persisted result's later stages always read its table.
   if (tables.length === 0 && leg !== "view" && leg !== "query") return;
   const reads = await queryFilterReadsOfQuery(
      facts.authorRunnable,
      storedSourceIDs(tables),
      { fromStage, namedMeasures: leg !== "rollup" },
   );
   if (reads !== undefined && !reads.unresolved && reads.comparesNoString) {
      return;
   }
   const why =
      reads === undefined
         ? "the query's filters unread"
         : tables.length === 0
           ? "the stored result's table unread"
           : await storedComparisonRefusal(
                answer,
                tables,
                facts.authorRunnable,
                async () => reads,
                givens,
             );
   if (why === undefined) return;
   const tableIds = tables.flatMap((t) => {
      const handle = (t as { virtualHandle?: unknown }).virtualHandle;
      return typeof handle === "string" ? [handle] : [];
   });
   const message = `the query filters on a column a stored table does not compare as its source does: ${why}`;
   switch (leg) {
      case "served":
         throw new LegDecline("served_copy_not_served", message, { tableIds });
      case "rollup":
         throw new LegDecline("rollup_row_level_gate", message, {
            shapeCheck: true,
            tableIds,
         });
      case "view":
         throw new LegDecline("view_missing_field", message);
      case "query":
         throw new LegDecline("query_missing_field", message);
   }
}

/**
 * A `storage=` table compares strings byte-exact, so a result read from one
 * is kept only where the query's result does not depend on comparing a
 * string the source compares otherwise (`serve/result_reads.ts`): an
 * ordering, a range comparison, a `min`/`max` of a string needs the source
 * column's collation to be binary (a Postgres `C`, `POSIX` or builtin
 * collation; a DuckDB instance that names none), and a grouping, a distinct
 * count, an equality or a join on a string needs it to be at least
 * deterministic, where equality is byte equality. Judged by the source side
 * of the query filters' rule (`stringGateCollationRefusal`): the source
 * column's collation, probed once per compiled model. The stored side (a
 * DuckLake table holds no collated column) is not read again per answer.
 *
 * Only a `storage=` table is judged: a database table sits in its source's
 * warehouse and compares as its source does. Only a source whose dialect's
 * collation is read (Postgres per column, DuckDB per instance): a dialect
 * read as binary needs nothing, and any other is left as it was. Which
 * stages count: every one on the served copy and a rollup; on a persisted
 * view or named query, the later stages, and the ordering its read applies
 * again to its own last stage's stored rows. Fails closed: a result that
 * cannot be read answers live.
 */
async function assertResultOrdersAsLive(
   answer: LegAnswer,
   leg: LegKind,
   facts: QueryFacts,
   givens: Record<string, unknown> | undefined,
): Promise<void> {
   const persisted = leg === "view" || leg === "query";
   const read = persisted
      ? (answer.tablesRead ?? [])
      : (answer.gateTablesRead ?? answer.tablesRead ?? []);
   const storage = read.filter((t) => t.tier === "storage");
   if (storage.length === 0) return;
   let fromStage = 0;
   let orderOnly: number | undefined;
   if (persisted) {
      const later = (
         leg === "view"
            ? extractRunViewCall(facts.queryString)
            : extractRunQueryCall(facts.queryString)
      )?.laterStages;
      const pipeline = (
         await readPreparedQuery(facts.authorRunnable).catch(() => undefined)
      )?.pipeline;
      // Unread: every stage is judged, which only declines more.
      if (later !== undefined && Array.isArray(pipeline)) {
         orderOnly = pipeline.length - later - 1;
         fromStage = orderOnly + 1;
      }
   }
   const reads = await resultReadsOfQuery(
      facts.authorRunnable,
      storedSourceIDs(read),
      { fromStage, ...(orderOnly !== undefined ? { orderOnly } : {}) },
   );
   const collation = reads?.stringCollation;
   if (
      reads !== undefined &&
      collation !== "column" &&
      collation !== "instance"
   ) {
      return;
   }
   let why: string | undefined;
   if (reads === undefined) {
      why = "the query's result unread";
   } else {
      for (const [what, r] of [
         ["orders", reads.ordered],
         ["groups or compares", reads.grouped],
      ] as const) {
         const refused = await stringGateCollationRefusal({
            authorRunnable: facts.authorRunnable,
            reads: r,
            stringStored: true,
            ...(givens ? { givens } : {}),
         });
         if (refused !== undefined) {
            why = `the result ${what} a string a stored table does not compare as its source does: ${refused}`;
            break;
         }
      }
   }
   if (why === undefined) return;
   const tableIds = storage.flatMap((t) => {
      const handle = (t as { virtualHandle?: unknown }).virtualHandle;
      return typeof handle === "string" ? [handle] : [];
   });
   switch (leg) {
      case "served":
         throw new LegDecline("served_copy_not_served", why, { tableIds });
      case "rollup":
         throw new LegDecline("rollup_row_level_gate", why, {
            shapeCheck: true,
            tableIds,
         });
      case "view":
         throw new LegDecline("view_missing_field", why);
      case "query":
         throw new LegDecline("query_missing_field", why);
   }
}

/**
 * Whether the query's entry point reads a row filter declared on a composite
 * source ({@link rowFilterOnComposite}); a compile that cannot be read
 * reads true.
 */
async function entryRowFilterOnComposite(
   runnable: QueryMaterializer,
): Promise<boolean> {
   try {
      const prepared = await readPreparedQuery(runnable);
      const struct = resolveStructRefSource(
         prepared.structRef as never,
         prepared.modelDef as never,
      );
      return rowFilterOnComposite(struct, prepared.modelDef);
   } catch {
      return true;
   }
}

/** A stored type that carries a zone: compared in the reading session's zone. */
const ZONED_STORED_TYPE = /\b(TIMESTAMP\s+WITH\s+TIME\s+ZONE|TIMESTAMPTZ)\b/i;

/**
 * Why a stored table an answer read does not compare the columns the entry
 * point's gates and scopes read as the source compares them, or undefined
 * when it does (the comparison rules, beside the exactness rule):
 *
 * - a column stored with a zone (`TIMESTAMP WITH TIME ZONE`) is compared in
 *   the destination session's zone, not the source's;
 * - a string column is compared byte-exact on a `storage=` table, which
 *   matches the source only under its collation's terms, and every table
 *   read is judged by its own collation as well (`serve/gate_collation.ts`).
 */
async function storedComparisonRefusal(
   answer: LegAnswer,
   tables: readonly TableRead[],
   authorRunnable: QueryMaterializer,
   readsOf: () => Promise<GateReads | undefined>,
   givens: Record<string, unknown> | undefined,
): Promise<string | undefined> {
   const stored = tables.filter((t) => t.tier === "storage");
   const types = stored.flatMap((t) => t.gateColumnTypes ?? []);
   if (types.some((type) => ZONED_STORED_TYPE.test(type))) {
      return "a gate column stored with a zone";
   }
   const reads = await readsOf();
   if (stored.length > 0) {
      const source = await stringGateCollationRefusal({
         authorRunnable,
         reads,
         stringStored: types.some(storedTypeIsString),
         ...(givens ? { givens } : {}),
      });
      if (source !== undefined) return source;
   }
   // The stored side: every table read, however it was admitted.
   return storedTableCollationRefusal({
      tables: tables as readonly (TableRead & {
         virtualHandle?: string;
         tablePath?: string;
         destinationName?: string;
      })[],
      answerRunnable: answer.runnable,
      virtualMap: answer.virtualMap,
      authorRunnable,
      reads,
      ...(givens ? { givens } : {}),
   });
}

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
      // Nothing materialized for this model: the answer is live and there is
      // nothing to name, so skip the chain walks and the catalog reads.
      if (
         (catalog?.planned.size ?? 0) === 0 &&
         host.storageTables(pinned.snapshot).length === 0 &&
         host.servedCopy.materializeBindings(pinned, "bound").length === 0 &&
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
         chain: chainOfQuerySource(struct, modelDef, host.modelDef()),
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
               ...host.servedCopy.materializeBindings(pinned, "bound"),
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
         storageWarned: getPersistStorageMode() !== "on",
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
         servedCopyTables: new Set(
            host.servedCopy
               .materializeBindings(pinned, "bound")
               .map((b) => b.tablePath),
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
