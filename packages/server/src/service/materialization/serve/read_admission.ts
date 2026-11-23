// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The admission shell: for one query, gathers what `admitRead`
 * (`domain/admission.ts`) decides a stored answer by, and turns its verdict
 * into the router's terms. The decision itself is pure; this module reads
 * the author's compile for the gates', the row scope's, the query filters'
 * and the result's reads, the bind's stored types, and the collation probes through
 * the {@link CollationProbe} port, each only when the decision needs it.
 *
 * One {@link ReadAdmission} per query: what it reads of the author's compile
 * is read once per set of stored sources an answer read, and each answer's
 * gathered facts, the probes' answers among them, are kept with the answer,
 * so asking about one answer twice (before the gate and past it) runs no
 * probe twice.
 */

import type { QueryMaterializer } from "@malloydata/malloy";
import {
   preparedQueryView,
   queryGivenUsageIds,
   readPreparedQuery,
   resolveStructRefSource,
   sqlTargetRef,
} from "../../governance/ir/prepared_query";
import { readsAGiven } from "../../governance/ir/compiled_model";
import { parseAuthorizeAnnotation } from "../../authorize";
import { rowFilterOnComposite } from "../../composite_row_filter";
import { extractRunQueryCall, extractRunViewCall } from "../../query_text";
import {
   admitRead,
   type AdmissionVerdict,
   type Comparison,
   type ComparisonProbes,
   mayAttempt,
   type ProbeRequest,
   type QueryFilterFacts,
   type ReadFacts,
   type ResultOrderFacts,
} from "../domain/admission";
import type { Binding } from "../domain/binding";
import type { GateSwitches, TableRead } from "../domain/gate_policy";
import { LegDecline } from "../domain/leg_decline";
import {
   gateColumnTypesOf,
   gateColumnUpstreamTypesOf,
   gateReadsOfQuery,
   queryFilterReadsOfQuery,
} from "./gate_reads";
import type {
   CollationProbe,
   GateReads,
   ProbedTable,
} from "./gate_reads_types";
import type { LegAnswer, LegKind, QueryFacts } from "./leg";
import { type ResultReads, resultReadsOfQuery } from "./result_reads";

// The port, and its implementation for the query pipeline to hand in.
export type { CollationProbe } from "./gate_reads_types";
export { collationProbe } from "./gate_collation";
export { lookupProbe } from "./admission_lookup";
export { observeAdmission, shadowProbe } from "./admission_parity";

/** What admission decided about one answer, in the router's terms. */
type AdmissionOutcome =
   | { readonly used: true }
   /** Past an entry-point gate, the answer does not keep the gates. */
   | { readonly used: false; readonly gateNotKept: true }
   /** The answer is declined as the leg's own decline (`does_not_answer`). */
   | { readonly used: false; readonly decline: LegDecline };

/** The facts gathered about one answer, and the reads they came from. */
interface Gathered {
   readonly tables: readonly TableRead[];
   readonly facts: ReadFacts;
   readonly scopeReads: () => Promise<GateReads | undefined>;
   readonly filterReads?: GateReads;
   readonly filterTables: readonly TableRead[];
   /** What the result orders and groups strings by, when it is judged. */
   readonly resultReads?: ResultReads;
   /** The stored tables the result is read from. */
   readonly resultTables: readonly TableRead[];
   /**
    * The stored side's warehouse reads for this answer, shared by every
    * comparison's stored probe, so the result's adds none the gates' or the
    * filters' already made.
    */
   readonly storedReads: Map<string, Promise<unknown>>;
   /** The probes' answers so far, kept across this answer's decisions. */
   probes: NonNullable<ReadFacts["probes"]>;
}

/** Read admission for one query. */
export class ReadAdmission {
   private readonly reads = new Map<string, Promise<GateReads | undefined>>();
   private composite: Promise<boolean> | undefined;
   private access: Promise<boolean> | undefined;
   private readonly gathered = new WeakMap<LegAnswer, Promise<Gathered>>();
   private readonly givens: Record<string, unknown> | undefined;

   constructor(
      private readonly query: QueryFacts,
      private readonly switches: GateSwitches,
      private readonly probe: CollationProbe,
      /** The bind the query was pinned to (its serving snapshot). */
      private readonly bind?: object,
   ) {
      this.givens = query.givens as Record<string, unknown> | undefined;
   }

   /** Whether `leg` may be tried past an entry-point gate at all. */
   mayAttempt(leg: LegKind): boolean {
      return mayAttempt(leg, this.switches);
   }

   /**
    * Whether the answer `leg` produced is used: before the gate (the row
    * scope, the query filters and the result only) or past it (every rule).
    */
   async admit(
      leg: LegKind,
      answer: LegAnswer,
      pastEntryPointGate: boolean,
   ): Promise<AdmissionOutcome> {
      const { verdict, filterTables, resultTables, tables } = await this.decide(
         leg,
         answer,
         pastEntryPointGate,
      );
      if (verdict.admit === true) return { used: true };
      if (verdict.reason === "row_level_gate") {
         return { used: false, gateNotKept: true };
      }
      const declined =
         verdict.reason === "query_filter_comparison"
            ? filterTables
            : verdict.reason === "result_order_comparison"
              ? resultTables
              : tables;
      return {
         used: false,
         decline: legDecline(leg, verdict.detail, tableIdsOf(declined)),
      };
   }

   /**
    * `admitRead`'s verdict on the answer, with the facts it was decided on:
    * gathered from the author's compile and the answer, and the probes'
    * answers it asked for, in the order it asked.
    */
   async decide(
      leg: LegKind,
      answer: LegAnswer,
      pastEntryPointGate: boolean,
   ): Promise<{
      verdict: Exclude<AdmissionVerdict, { admit: "needs_probe" }>;
      facts: ReadFacts;
      tables: readonly TableRead[];
      filterTables: readonly TableRead[];
      resultTables: readonly TableRead[];
   }> {
      const g = await this.gather(leg, answer);
      let facts: ReadFacts = {
         ...g.facts,
         pastEntryPointGate,
         compositeRowFilter: pastEntryPointGate && (await this.compositeOnce()),
         probes: g.probes,
      };
      for (;;) {
         const verdict = admitRead(leg, g.tables, facts, this.switches);
         if (verdict.admit !== "needs_probe") {
            return {
               verdict,
               facts,
               tables: g.tables,
               filterTables: g.filterTables,
               resultTables: g.resultTables,
            };
         }
         facts = await this.withProbe(facts, verdict.probe, g, answer);
         g.probes = facts.probes ?? {};
      }
   }

   /**
    * What the gates and row scope read, given the tables an answer read and
    * the sources its compile re-rooted on their tables (see
    * `gateReadsOfQuery`).
    */
   private gateReadsFor(
      tables: readonly TableRead[],
      onTables: ReadonlySet<string> = new Set(),
   ): Promise<GateReads | undefined> {
      const stored = storedSourceIDs(tables);
      const key = [[...stored].sort(), [...onTables].sort()].join("\n\n");
      let r = this.reads.get(key);
      if (r === undefined) {
         r = gateReadsOfQuery(this.query.authorRunnable, stored, onTables);
         this.reads.set(key, r);
      }
      return r;
   }

   /** See {@link queryReadsAccess}; read once per query. */
   private readsAccessOnce(): Promise<boolean> {
      return (this.access ??= queryReadsAccess(this.query.authorRunnable));
   }

   private compositeOnce(): Promise<boolean> {
      return (this.composite ??= entryRowFilterOnComposite(
         this.query.authorRunnable,
      ));
   }

   private gather(leg: LegKind, answer: LegAnswer): Promise<Gathered> {
      let g = this.gathered.get(answer);
      if (g === undefined) {
         g = this.gatherNow(leg, answer);
         this.gathered.set(answer, g);
      }
      return g;
   }

   private async gatherNow(leg: LegKind, answer: LegAnswer): Promise<Gathered> {
      const read = answer.gateTablesRead ?? answer.tablesRead ?? [];
      // The served copy reads each source it re-rooted from its table, so a
      // join to one never re-applies that source's own query.
      const rerooted = leg === "served" ? answer.rerootedSourceIDs : undefined;
      const scopeReads = () => this.gateReadsFor(read, rerooted);
      // Only the served copy and a rollup re-apply a row scope, and only
      // past a gate is the scope's reads needed otherwise; both read through
      // the one memo.
      const judged = await withGateColumnTypes(read, scopeReads);
      const reads =
         leg === "served" || leg === "rollup" ? await scopeReads() : undefined;
      const filters = await this.queryFilters(leg, answer);
      const result = await this.resultOrder(leg, answer);
      const readsAsOfBuild = [
         ...read,
         ...(answer.tablesRead ?? []),
         ...(answer.resultTableRead ? [answer.resultTableRead] : []),
      ].some((t) => (t as { asOfBuild?: unknown }).asOfBuild === true);
      return {
         tables: judged,
         scopeReads,
         ...(filters.reads ? { filterReads: filters.reads } : {}),
         filterTables: filters.facts.applies ? filters.facts.tables : [],
         ...(result.reads ? { resultReads: result.reads } : {}),
         resultTables: result.tables,
         storedReads: new Map(),
         probes: {},
         facts: {
            pastEntryPointGate: false,
            compositeRowFilter: false,
            reads:
               reads === undefined
                  ? undefined
                  : { scoped: reads.scoped, gated: reads.gated },
            columnsListed: judged !== read,
            queryFilters: filters.facts,
            resultOrder: result.facts,
            ...(readsAsOfBuild
               ? {
                    readsAsOfBuild,
                    readsAccess: await this.readsAccessOnce(),
                 }
               : {}),
         },
      };
   }

   /**
    * The caller's own filters an answer applies to stored rows: every one on
    * the served copy and a rollup (a rollup's build applied each measure it
    * stores, so a measure's own filter is not one), and only the later
    * stages' on a persisted view or named query, whose own stages its build
    * applied.
    */
   private async queryFilters(
      leg: LegKind,
      answer: LegAnswer,
   ): Promise<{ facts: QueryFilterFacts; reads?: GateReads }> {
      const { queryString, authorRunnable } = this.query;
      let tables: readonly TableRead[];
      let fromStage = 0;
      if (leg === "view" || leg === "query") {
         const later = (
            leg === "view"
               ? extractRunViewCall(queryString)
               : extractRunQueryCall(queryString)
         )?.laterStages;
         // An exact call's result was computed by its build, as live's is.
         if (later === 0) return { facts: { applies: false } };
         const pipeline = (
            await readPreparedQuery(authorRunnable).catch(() => undefined)
         )?.pipeline;
         // Unread: every stage is judged, which only declines more.
         if (later !== undefined && Array.isArray(pipeline)) {
            fromStage = pipeline.length - later;
         }
         tables = answer.tablesRead ?? [];
         if (answer.resultTableRead)
            tables = [...tables, answer.resultTableRead];
      } else {
         tables = answer.gateTablesRead ?? answer.tablesRead ?? [];
         // A served copy or rollup that reads no stored table applies
         // nothing to one.
         if (tables.length === 0) return { facts: { applies: false } };
      }
      const reads = await queryFilterReadsOfQuery(
         authorRunnable,
         storedSourceIDs(tables),
         { fromStage, namedMeasures: leg !== "rollup" },
      );
      const what =
         reads === undefined
            ? "unread"
            : !reads.unresolved && reads.comparesNoString
              ? "no_string"
              : "string";
      return {
         facts: { applies: true, reads: what, tables },
         ...(reads ? { reads } : {}),
      };
   }

   /**
    * What the query's result orders and groups strings by, over the stored
    * tables the answer reads, when the source's dialect's collation is read
    * (Postgres per column, DuckDB per instance, BigQuery per project). A `storage=` table compares
    * bytes, so the source's collation is judged against that; a database
    * table sits in its source's warehouse, so its stored columns are judged
    * against the source's (an `ALTER` after the build changes them). Which stages count: every one on the served copy and a
    * rollup; on a persisted view or named query, the later stages, and the
    * ordering its read applies again to its own last stage's stored rows.
    */
   private async resultOrder(
      leg: LegKind,
      answer: LegAnswer,
   ): Promise<{
      facts: ResultOrderFacts;
      reads?: ResultReads;
      tables: readonly TableRead[];
   }> {
      const { queryString, authorRunnable } = this.query;
      const persisted = leg === "view" || leg === "query";
      // A database view's or query's own table is its result's table.
      const read: readonly TableRead[] = persisted
         ? [
              ...(answer.tablesRead ?? []),
              ...(answer.resultTableRead ? [answer.resultTableRead] : []),
           ]
         : (answer.gateTablesRead ?? answer.tablesRead ?? []);
      // A stored table, of either tier: what a result is judged on, as the
      // `storage=`-only rule before it was (a read naming no tier is not one).
      const tables = read.filter(
         (t) => t.tier === "storage" || t.tier === "database",
      );
      if (tables.length === 0) return { facts: { applies: false }, tables };
      let fromStage = 0;
      let orderOnly: number | undefined;
      if (persisted) {
         const later = (
            leg === "view"
               ? extractRunViewCall(queryString)
               : extractRunQueryCall(queryString)
         )?.laterStages;
         const pipeline = (
            await readPreparedQuery(authorRunnable).catch(() => undefined)
         )?.pipeline;
         // Unread: every stage is judged, which only declines more.
         if (later !== undefined && Array.isArray(pipeline)) {
            orderOnly = pipeline.length - later - 1;
            fromStage = orderOnly + 1;
         }
      }
      // The sources whose tables the answer reads, by ID, for telling a
      // read of a live source from one of a stored table: a database
      // rollup's table names its base only through the answer.
      const storedIDs = storedSourceIDs(read);
      const storedSources = new Set(storedIDs);
      if (answer.rollupBaseSourceID !== undefined) {
         storedSources.add(answer.rollupBaseSourceID);
      }
      const reads = await resultReadsOfQuery(authorRunnable, storedIDs, {
         fromStage,
         ...(orderOnly !== undefined ? { orderOnly } : {}),
         storedSources,
      });
      if (reads === undefined) {
         return { facts: { applies: true, reads: "unread", tables }, tables };
      }
      const collation = reads.stringCollation;
      if (
         collation !== "column" &&
         collation !== "instance" &&
         collation !== "dataset"
      ) {
         return { facts: { applies: false }, tables };
      }
      return { facts: { applies: true, reads: "read", tables }, reads, tables };
   }

   /** `facts` with the answer to `request`, from the probe port. */
   private async withProbe(
      facts: ReadFacts,
      request: ProbeRequest,
      g: Gathered,
      answer: LegAnswer,
   ): Promise<ReadFacts> {
      const reads =
         request.comparison === "scope"
            ? await g.scopeReads()
            : request.comparison === "filters"
              ? g.filterReads
              : request.comparison === "result_ordered"
                ? g.resultReads?.ordered
                : g.resultReads?.grouped;
      const tables =
         request.comparison === "scope"
            ? g.tables
            : request.comparison === "filters"
              ? g.filterTables
              : g.resultTables.filter((t) => t.tier === "database");
      const givens = {
         ...(this.givens ? { givens: this.givens } : {}),
         ...(this.bind ? { bind: this.bind } : {}),
      };
      const refusal =
         request.side === "source"
            ? await this.probe.sourceRefusal({
                 authorRunnable: this.query.authorRunnable,
                 reads,
                 ...givens,
              })
            : await this.probe.storedRefusal({
                 tables: tables as readonly ProbedTable[],
                 answerRunnable: answer.runnable,
                 virtualMap: answer.virtualMap,
                 authorRunnable: this.query.authorRunnable,
                 reads,
                 storedReads: g.storedReads,
                 ...(request.comparison === "result_ordered" ||
                 request.comparison === "result_grouped"
                    ? { unlistedPasses: true }
                    : {}),
                 ...givens,
              });
      const probes: Partial<Record<Comparison, ComparisonProbes>> = {
         ...facts.probes,
      };
      probes[request.comparison] = {
         ...probes[request.comparison],
         [request.side]: { refusal },
      };
      return { ...facts, probes };
   }
}

/** The leg's own decline for an answer admission declined. */
function legDecline(
   leg: LegKind,
   message: string,
   tableIds: string[],
): LegDecline {
   switch (leg) {
      case "served":
         return new LegDecline("served_copy_not_served", message, { tableIds });
      case "rollup":
         return new LegDecline("rollup_row_level_gate", message, {
            shapeCheck: true,
            tableIds,
         });
      case "view":
         return new LegDecline("view_missing_field", message);
      case "query":
         return new LegDecline("query_missing_field", message);
   }
}

/**
 * Whether a query is an access decision: it reads a given (Malloy's own
 * given-usage summary of the query, or a `{node: "given"}` anywhere in it),
 * or the source it runs carries an
 * `#(authorize)` or `#(access_filter)` gate anywhere beneath it. Fails
 * closed: a query that cannot be read counts.
 */
async function queryReadsAccess(runnable: QueryMaterializer): Promise<boolean> {
   try {
      const prepared = await runnable.getPreparedQuery();
      const usage = queryGivenUsageIds(prepared);
      if (usage === undefined || usage.length > 0) return true;
      const view = preparedQueryView(prepared);
      if (readsAGiven(view.query)) return true;
      const source = resolveStructRefSource<unknown>(
         sqlTargetRef(view),
         view.modelDef,
      );
      return carriesAGate(source) || carriesAGate(view.query);
   } catch {
      return true;
   }
}

/** Max IR depth {@link carriesAGate} walks; past it, a gate is assumed. */
const MAX_GATE_WALK_DEPTH = 200;

/**
 * Whether a slice of IR carries an `#(authorize)` or `#(access_filter)`
 * note anywhere (the plan's gate walk, `referencesAuthorize`, read with the
 * same parser). Fails closed.
 */
function carriesAGate(
   node: unknown,
   seen: WeakSet<object> = new WeakSet(),
   depth = 0,
): boolean {
   if (depth > MAX_GATE_WALK_DEPTH) return true;
   if (node === null || typeof node !== "object") return false;
   if (seen.has(node)) return false;
   seen.add(node);
   if (Array.isArray(node)) {
      return node.some((item) => carriesAGate(item, seen, depth + 1));
   }
   const record = node as Record<string, unknown>;
   for (const key of ["blockNotes", "notes"]) {
      const notes = record[key];
      if (!Array.isArray(notes)) continue;
      for (const note of notes) {
         const text =
            typeof note === "string"
               ? note
               : (note as { text?: unknown } | null)?.text;
         if (typeof text !== "string") continue;
         try {
            if (parseAuthorizeAnnotation(text) !== null) return true;
         } catch {
            return true;
         }
      }
   }
   return Object.values(record).some((v) => carriesAGate(v, seen, depth + 1));
}

/** The tables' virtual handles, so the path report names each one declined. */
function tableIdsOf(tables: readonly TableRead[]): string[] {
   return tables.flatMap((t) => {
      const handle = (t as { virtualHandle?: unknown }).virtualHandle;
      return typeof handle === "string" ? [handle] : [];
   });
}

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
 * source's own types and is left as it is. Returns `tables` itself when no
 * table's columns are listed.
 */
async function withGateColumnTypes(
   tables: readonly TableRead[],
   readsOf: () => Promise<GateReads | undefined>,
): Promise<readonly TableRead[]> {
   const retyped = (t: TableRead) =>
      (t as Partial<Pick<Binding, "retypedColumns">>).retypedColumns ?? [];
   const storage = tables.filter((t) => t.tier === "storage");
   if (storage.length === 0 && !tables.some((t) => retyped(t).length > 0)) {
      return tables;
   }
   const reads = await readsOf();
   // Judged together: a gate column held by none of the storage tables the
   // answer read leaves every one of them unproven.
   const types = gateColumnTypesOf(
      reads,
      storage.map((t) => (t as Partial<Pick<Binding, "schema">>).schema),
   );
   const typeOf = new Map(storage.map((t, i) => [t, types[i]]));
   // What each stored gate column copies, for a float the policy may read as
   // exact (`gateColumnUpstreamTypes`).
   const upstreamTypes = gateColumnUpstreamTypesOf(
      reads,
      storage.map((t) => (t as Partial<Pick<Binding, "schema">>).schema),
      storage.map(
         (t) =>
            (t as Partial<Pick<Binding, "upstreamSchemas">>).upstreamSchemas,
      ),
   );
   const upstreamTypeOf = new Map(storage.map((t, i) => [t, upstreamTypes[i]]));
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
         const upstream = upstreamTypeOf.get(t);
         return {
            ...t,
            gateColumnTypes: typeOf.get(t),
            ...(upstream ? { gateColumnUpstreamTypes: upstream } : {}),
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
