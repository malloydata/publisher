// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The path report (materialization v2, D1): what answered a query and why.
 *
 * Built after the query runs, from three things Publisher already has:
 *
 * - the router's decision ({@link routeQuery}'s `considered`), which says how
 *   each candidate variant (the `storage=` serve shape, the pre-aggregation
 *   companion) fared;
 * - the build-manifest entries the run that answered actually read, which say
 *   which colocated table answered: a colocated `#@ persist` table (or rollup)
 *   Malloy substituted through the manifest, a tier that never reaches the
 *   router. Read from Malloy's own lookup, never from the SQL text;
 * - the package's catalog of materialized tables (its build plan, bound
 *   manifest and `storage=` bindings), read against the queried source's
 *   chain, with the serve verdict (D2) of every table no query can be answered
 *   from, so a skip says why the table will not serve rather than only that it
 *   did not.
 *
 * Reporting only: nothing here changes which table answers, and a report that
 * cannot be assembled is left off the result rather than failing the query.
 *
 * Tables are named by `tableId`, the source's content address
 * (`sourceEntityId`), which the build plan, the manifest and every build
 * instruction already key on.
 *
 * Pure apart from the IR reads: no I/O, so it is unit-tested on fixtures and
 * on real compiled queries.
 */

import type { components } from "../../../api";
import type { ConsideredVariant } from "../../query_router";
import type { KeyUniqueness } from "../types";
import { forEachFieldPath } from "../../governance/ir/array_query";
import { declineOfCode } from "./leg_decline";

export type ApiQueryPath = components["schemas"]["QueryPath"];
type ApiQueryShape = components["schemas"]["QueryShape"];
type ApiCandidate = components["schemas"]["QueryPathCandidate"];
type ApiNearby = components["schemas"]["QueryPathNearby"];
type ApiServer = components["schemas"]["QueryPathServer"];
type SkipReason = components["schemas"]["SkipReason"];
type ApiWarning = components["schemas"]["QueryPathWarning"];

/** At most this many nearby tables are reported, nearest first. */
export const MAX_NOT_CONSIDERED = 10;

/** One source the package's build plan materializes. */
export interface PlannedTable {
   /** `<declared name>@<file URL>`; never sent on the wire (it is a file path). */
   sourceID: string;
   name: string;
   /** The content address: the table's `tableId` in the path report. */
   sourceEntityId: string;
   origin: "persist" | "preaggregate";
   /**
    * The model the plan entry came from. A rollup has no stable `sourceID` on
    * the model side, so it is matched to its plan by name within its model.
    */
   modelPath?: string;
}

/** A colocated table the package has bound, from its manifest. */
export interface BoundTable {
   tableName: string;
   dataAsOf?: string;
   /**
    * A rollup's key check, as the run that wrote or carried the table found
    * it (known limitation 29); absent when unknown.
    */
   keyUniqueness?: KeyUniqueness;
}

/**
 * A table no query can be answered from, by its serve verdict: every planned
 * source sharing its `sourceEntityId` on its tier has a `live` verdict for a
 * reason other than staleness (which the report decides per query, from the
 * freshness window it reads now).
 */
export interface LiveTableVerdict {
   /** The verdict's reason: `storage_mode`, `unreachable`, `row_level_gate`, `refused`, `shadowed`. Open. */
   reason: string;
   /** The source a query names to reach it: a rollup's base, else its own. */
   sourceName: string;
}

/**
 * The package's materialized tables, as the path report reads them. Read per
 * query from the package, so a rebind or a new plan is seen at once.
 */
export interface MaterializedTableCatalog {
   /** Every planned source, keyed by `sourceID`. */
   planned: ReadonlyMap<string, PlannedTable>;
   /** The colocated tables bound now, keyed by `sourceEntityId`. */
   bound: Readonly<Record<string, BoundTable>>;
   /**
    * The tables whose serve verdict says no query is answered from them,
    * keyed by {@link liveVerdictKey} (tier and `sourceEntityId`: a `storage=`
    * table and its colocated twin share the id but not the verdict). Absent
    * when the verdicts are not known.
    */
   liveVerdicts?: ReadonlyMap<string, LiveTableVerdict>;
   /**
    * Each planned source's own serve verdict when it is `live` for a reason
    * other than staleness, keyed by `sourceID`. A source can be live while the
    * table it shares serves another source (a gated extension the eligibility
    * gate refused, beside its base), which {@link liveVerdicts} cannot say.
    * Absent when the verdicts are not known.
    */
   sourceVerdicts?: ReadonlyMap<string, LiveTableVerdict>;
}

/** The key of {@link MaterializedTableCatalog.liveVerdicts}. */
export function liveVerdictKey(
   tier: "storage" | "colocated",
   sourceEntityId: string,
): string {
   return `${tier}:${sourceEntityId}`;
}

/** A `storage=` serve binding, as the report reads it. */
export interface StorageTable {
   /** The source it materializes, or a rollup's synthesized name. */
   sourceName: string;
   sourceIDs?: readonly string[];
   origin?: "persist" | "preaggregate";
   tablePath: string;
   freshAsOf?: string;
   /**
    * A colocated `#(materialize)` table, which only the served copy reads, on
    * the source's own connection: reported on the colocated tier, decided by
    * the served copy as a `storage=` table is.
    */
   colocated?: true;
}

/** A rollup this model's pre-aggregation plans. */
interface ModelRollup {
   rollupSourceName: string;
   baseSourceName: string;
   baseSourceID?: string;
   /**
    * A `#(preaggregate)` rollup, which the router's rollup leg answers on
    * either tier (its `storage=` table too), never the serve shape.
    */
   variant?: boolean;
}

/** One table the report can name, with what is known about it now. */
interface TableState {
   /** Internal identity, for de-duplication; never sent. */
   key: string;
   /**
    * The content address. Absent for a storage binding the plan does not know
    * (a plan that failed to compute): such a table is never named on the wire,
    * since its only other name is a physical storage path.
    */
   tableId?: string;
   kind: "materialized" | "rollup" | "view";
   tier: "storage" | "colocated";
   /** Built and bound, but withheld by its freshness window. */
   stale: boolean;
   /** Not bound to the package at all. */
   unbuilt: boolean;
   /**
    * A `storage=` table the serve path is not routing to: the storage tier is
    * off or write-only, which the package reports as a warning.
    */
   warned?: boolean;
   dataAsOf?: string;
   /** For a colocated table: the name the SQL substitutes. */
   tableName?: string;
   /**
    * For a table a stored variant reads through a binding (a `storage=`
    * table, or a colocated `#(materialize)` table the served copy reads):
    * the binding.
    */
   binding?: StorageTable;
   /** For a rollup: its synthesized source name (the composite member). */
   rollupName?: string;
   /** A `#(preaggregate)` rollup: the rollup leg decides it on either tier. */
   variant?: boolean;
}

/** What happened to the query, as the model saw it. */
export interface QueryPathInput {
   shape: ApiQueryShape;
   /** The model the query ran in, to match a rollup to its plan. */
   modelPath?: string;
   /** The queried source's `sourceID`s, nearest first (see `sourceChain` in plan/source_map). */
   chain: readonly string[];
   catalog: MaterializedTableCatalog | undefined;
   /**
    * The colocated tables inside their freshness window now (the per-query
    * build manifest's keys). Undefined treats every bound table as fresh.
    */
   freshColocated?: ReadonlySet<string>;
   /** This model's `storage=` bindings, and the fresh subset of them. */
   storage: { all: readonly StorageTable[]; fresh: readonly StorageTable[] };
   /**
    * The storage tier is not routed (`PERSIST_STORAGE_MODE` off or write-only),
    * and the package warns about every `storage=` source for it. Its tables are
    * then reported `WARNED`.
    */
   storageWarned?: boolean;
   rollups: readonly ModelRollup[];
   /**
    * The bases of the rollups the query could read through one of its
    * source's joins (A7, join-to-rollup): named as the query's own rollups.
    */
   joinedRollupBases?: ReadonlySet<string>;
   /** The router's decision; empty when no candidate existed. */
   considered: readonly ConsideredVariant[];
   /** For a storage answer: the bindings whose tables the routed query read. */
   storageTablesRead?: readonly StorageTable[];
   /**
    * For a rollup answer read through a join in a storage destination (known
    * limitation 26): the query source's stored tables it read beside the
    * rollup's, each named as used.
    */
   rollupTablesRead?: readonly StorageTable[];
   /** The tables only the served copy reads (`#(materialize)`): its decision is theirs. */
   servedCopyTables?: ReadonlySet<string>;
   /** For a storage answer: `served_copy` when the served copy on `virtual` answered. */
   via?: "served_copy";
   /**
    * The routed storage read failed at run time and the query ran live. Its
    * error is logged, never reported: it can carry storage paths.
    */
   runFailed?: boolean;
   /**
    * The manifest keys (`sourceEntityId`s) the run that answered read, from
    * Malloy's manifest lookup: the colocated tables it substituted.
    */
   colocatedRead?: ReadonlySet<string>;
   /**
    * For a query the pre-aggregation companion answered: the composite member
    * its compile resolved to (a rollup's name, or the base's). A composite
    * looks every member up in the manifest, so the manifest's reads cannot
    * say which rollup answered; the compiled query can.
    */
   rollupMember?: string;
   /**
    * The table of the persisted view the query calls (`#@ persist` on a view,
    * P14), when it calls one: tried before every other table (R1).
    */
   view?: {
      tableId?: string;
      tier: "storage" | "colocated";
      built: boolean;
      stale: boolean;
      dataAsOf?: string;
   };
   /** The `sourceID` chain of every source in the model, for `notConsidered`. */
   chains: ReadonlyMap<string, readonly string[]>;
   /**
    * Whether the caller may see a source by this name: the discovery surface
    * (export curation and the package's query boundary), or everything when
    * the caller may see hidden sources. Absent admits every name.
    */
   isVisibleSource?: (sourceName: string) => boolean;
}

/**
 * Assemble the path report. Order in `considered` follows the order Publisher
 * tries the tiers: `storage=` tables, then colocated rollups, then colocated
 * `#@ persist` tables nearest first.
 */
export function buildQueryPath(input: QueryPathInput): ApiQueryPath {
   const onChain = new Set(input.chain);
   // One decision per kind; where two candidates share a kind (the v1 and
   // `#(preaggregate)` rollup legs), the one that answered.
   const decided = new Map<ConsideredVariant["kind"], ConsideredVariant>();
   for (const c of input.considered) {
      if (!decided.has(c.kind) || c.outcome === "used") decided.set(c.kind, c);
   }
   // The served copy on `virtual` reads the same stored tables as the serve
   // shape; when it answered, its decision is the storage tier's.
   const servedCopyAnswered = input.via === "served_copy";
   // Otherwise the serve shape's decision, or, when only the served copy was
   // tried (a `#(materialize)` table), the served copy's. A served copy that
   // declined a query through a join it leaves out across connections says
   // why the table did not answer better than the serve shape's compile
   // failure behind it, unless the serve shape answered.
   const servedDecision = decided.get("served");
   const shapeDecision = decided.get("storage");
   const storageDecision =
      servedCopyAnswered ||
      (servedDecision?.code === "served_copy_cross_connection" &&
         shapeDecision?.outcome !== "used")
         ? servedDecision
         : (shapeDecision ?? servedDecision);
   const rollupDecision = decided.get("rollup");
   // Computed once per report.
   const freshStorage = new Set(input.storage.fresh.map((b) => b.tablePath));
   const tables = tablesFor(input, onChain, freshStorage, !!storageDecision);
   const considered: ApiCandidate[] = [];
   // Each skipped candidate's tier, which the wire does not carry, so a serve
   // verdict is read for the table that was skipped, not its twin.
   const tierOf = new Map<ApiCandidate, TableState["tier"]>();
   let servedBy: ApiServer = { kind: "live" };
   const markUsed = (t: TableState) => {
      if (t.tableId) {
         considered.push({ tableId: t.tableId, kind: t.kind, outcome: "used" });
      }
      servedBy = {
         kind: t.kind,
         ...(t.tableId ? { tableId: t.tableId } : {}),
         ...(t.dataAsOf ? { dataAsOf: t.dataAsOf } : {}),
      };
   };
   const skip = (
      t: TableState,
      reason: SkipReason,
      detail?: string,
      code?: string,
   ) => {
      if (!t.tableId) return;
      const candidate: ApiCandidate = {
         tableId: t.tableId,
         kind: t.kind,
         outcome: "skipped",
         reason,
         ...(code ? { code } : {}),
         ...(detail ? { detail } : {}),
      };
      tierOf.set(candidate, t.tier);
      considered.push(candidate);
   };

   // A persisted view's table, first (R1). When it answered, nothing after
   // it was tried, so nothing after it is named.
   const viewDecision = decided.get("view");
   if (input.view) {
      const t: TableState = {
         key: `persisted-view/${input.view.tableId ?? ""}`,
         ...(input.view.tableId ? { tableId: input.view.tableId } : {}),
         kind: "view",
         tier: input.view.tier,
         stale: input.view.stale,
         unbuilt: !input.view.built,
         ...(input.view.dataAsOf ? { dataAsOf: input.view.dataAsOf } : {}),
      };
      if (viewDecision?.outcome === "used") {
         if (input.runFailed) {
            skip(t, "RUN_FAILED");
         } else {
            markUsed(t);
            return {
               query: input.shape,
               servedBy,
               considered,
               notConsidered: nearbyTables(
                  input,
                  onChain,
                  new Set([t.tableId]),
               ),
               warnings: [],
            };
         }
      } else if (!input.view.built) {
         skip(t, "NOT_BUILT");
      } else if (input.view.stale) {
         skip(t, "STALE");
      } else if (viewDecision?.reason === "row_level_gate") {
         skip(t, "CONTRACT");
      } else if (viewDecision && viewDecision.reason !== "not_applicable") {
         skip(t, skipReasonOf(viewDecision), viewDecision.callerDetail);
      }
   }

   // `storage=`: tried only when the tier was a candidate at all (mode on,
   // bindings and a destination present).
   // A colocated `#(materialize)` table is the served copy's, decided here
   // with the storage tier's tables.
   const storageTables = tables.filter((t) => t.binding && !t.variant);
   let storageAnswered = false;
   if (storageDecision) {
      const read = new Set(
         (input.storageTablesRead ?? []).map((b) => b.tablePath),
      );
      for (const t of storageTables) {
         if (t.stale) {
            skip(t, "STALE");
         } else if (storageDecision.reason === "row_level_gate") {
            skip(t, "CONTRACT");
         } else if (storageDecision.outcome === "used") {
            if (!t.binding || !read.has(t.binding.tablePath)) continue;
            if (input.runFailed) {
               skip(t, "RUN_FAILED");
            } else {
               markUsed(t);
               storageAnswered = true;
            }
         } else {
            // A table only the served copy reads skipped for the served
            // copy's reason, whichever candidate the tier reported.
            const served = decided.get("served");
            const decision =
               served && input.servedCopyTables?.has(t.binding?.tablePath ?? "")
                  ? served
                  : storageDecision;
            skip(
               t,
               skipReasonOf(decision),
               decision.callerDetail,
               declineOfCode(decision.code)?.warnedCode,
            );
         }
      }
      // The shape answered from a table the chain walk did not place (an IR
      // shape it does not follow): still name it, so `servedBy` never says
      // `live` for a storage answer.
      // The queried source's own table when it is among them (nearest on
      // its chain first), not whichever the served copy read first: a fact
      // read through a persisted join is not served from the join's table.
      const tablesRead = input.storageTablesRead ?? [];
      const first =
         input.chain
            .map((id) => tablesRead.find((b) => b.sourceIDs?.includes(id)))
            .find((b) => b !== undefined) ?? tablesRead[0];
      const placed =
         storageAnswered ||
         considered.some(
            (c) => c.outcome === "used" || c.reason === "RUN_FAILED",
         );
      if (storageDecision.outcome === "used" && first && !placed) {
         const t = stateOfBinding(input, first, freshStorage);
         if (input.runFailed) {
            skip(t, "RUN_FAILED");
         } else {
            markUsed(t);
            storageAnswered = true;
         }
      }
   } else {
      // Not routed at all (the tier is off or write-only): every storage table
      // carries the package's warning, and the query reads its own warehouse.
      for (const t of storageTables) if (t.warned) skip(t, "WARNED");
   }

   // Colocated rollups, through the pre-aggregation companion.
   const colocatedRollups = tables.filter(
      (t) =>
         (t.tier === "colocated" || t.variant) &&
         t.kind === "rollup" &&
         !t.unbuilt,
   );
   let rollupAnswered = false;
   if (rollupDecision && !storageAnswered) {
      const used =
         rollupDecision.outcome === "used"
            ? colocatedRollups.find(
                 (t) => !t.stale && t.rollupName === input.rollupMember,
              )
            : undefined;
      for (const t of colocatedRollups) {
         if (t.stale) {
            skip(t, "STALE");
         } else if (rollupDecision.reason === "row_level_gate") {
            skip(t, "CONTRACT");
         } else if (rollupDecision.reason === "not_applicable") {
            continue;
         } else if (rollupDecision.outcome === "used") {
            if (t === used) {
               markUsed(t);
               rollupAnswered = true;
            } else if (!used && input.rollupMember !== undefined) {
               // The companion compiled, but Malloy's composite chose the base.
               skip(t, "GRAIN_TOO_COARSE");
            }
         } else {
            skip(t, skipReasonOf(rollupDecision), rollupDecision.callerDetail);
         }
      }
   }

   // A join leg answered in a storage destination reads the query source's
   // stored table there as well as the rollup's: named as used beside it,
   // while the rollup stays the table that answered.
   if (rollupAnswered) {
      for (const b of input.rollupTablesRead ?? []) {
         const t = stateOfBinding(input, b, freshStorage);
         if (
            t.tableId &&
            !considered.some(
               (c) => c.tableId === t.tableId && c.outcome === "used",
            )
         ) {
            considered.push({
               tableId: t.tableId,
               kind: t.kind,
               outcome: "used",
            });
         }
      }
   }

   // Colocated `#@ persist`: Malloy substitutes the bound table into whatever
   // runnable answered, unless a stored variant replaced the source.
   if (!storageAnswered && !rollupAnswered) {
      let answered = false;
      for (const t of tables) {
         if (
            t.tier !== "colocated" ||
            t.kind !== "materialized" ||
            t.unbuilt ||
            t.binding
         )
            continue;
         if (t.stale) {
            skip(t, "STALE");
         } else if (!answered && readByRun(input, t)) {
            markUsed(t);
            answered = true;
         }
      }
   }

   for (const t of tables) if (t.unbuilt) skip(t, "NOT_BUILT");

   return {
      query: input.shape,
      servedBy:
         servedCopyAnswered && servedBy.kind === "materialized"
            ? { ...servedBy, via: "served_copy" }
            : servedBy,
      considered,
      notConsidered: nearbyTables(
         input,
         onChain,
         new Set(tables.map((t) => t.tableId)),
      ),
      warnings: applyServeVerdicts(considered, tierOf, input),
   };
}

/**
 * Whether a serve verdict explains a skip better than the query did. Only
 * skips where the router never tried the table: `NOT_BUILT` and `WARNED`
 * (nothing to try), and `CONTRACT` when the verdict is the same gate or a
 * refusal (a gated extension the eligibility gate refused at build). A skip
 * the router reached by trying the table (`MISSING_FIELD`, `GRAIN_TOO_COARSE`,
 * `COUNT_SEMANTICS`, …) is this query's own reason, and a caller who can reach
 * a table no published entry point reaches is seeing exactly that; `STALE` and
 * `RUN_FAILED` are facts only the query knows.
 */
function verdictExplains(reason: SkipReason | undefined, verdict: string) {
   if (reason === "NOT_BUILT" || reason === "WARNED") return true;
   return (
      reason === "CONTRACT" &&
      (verdict === "row_level_gate" || verdict === "refused")
   );
}

/**
 * Re-read each skipped table against its serve verdict: a table no query can
 * be answered from is reported `WARNED`, with the verdict's reason as `code`,
 * in place of what this query alone showed when the verdict explains it
 * ({@link verdictExplains}): `NOT_BUILT` for a source the bind refuses,
 * `CONTRACT` for a rollup every entry point gates or a gated source the build
 * refused. A table that answered stays
 * `used`. Returns one warning per such table whose source the caller may see.
 */
function applyServeVerdicts(
   considered: ApiCandidate[],
   tierOf: ReadonlyMap<ApiCandidate, TableState["tier"]>,
   input: Pick<QueryPathInput, "catalog" | "isVisibleSource" | "chain">,
): ApiWarning[] {
   const verdicts = input.catalog?.liveVerdicts ?? new Map();
   const warnings: ApiWarning[] = [];
   const own = ownSourceVerdict(input);
   if (verdicts.size === 0 && !own) return warnings;
   // A table that answered (its colocated copy, beside a storage copy that is
   // not routed) is no table that "answers no query": its skipped twin keeps
   // the code, and no warning says otherwise.
   const used = new Set(
      considered.filter((c) => c.outcome === "used").map((c) => c.tableId),
   );
   const warned = new Set<string>();
   for (const candidate of considered) {
      if (candidate.outcome !== "skipped") continue;
      const tier = tierOf.get(candidate);
      if (!tier) continue;
      const verdict: LiveTableVerdict | undefined = verdicts.get(
         liveVerdictKey(tier, candidate.tableId),
      );
      if (!verdict || !verdictExplains(candidate.reason, verdict.reason)) {
         // The table serves another source; the queried source's own verdict
         // may still explain why this query could not use it.
         if (own && ownVerdictExplains(candidate, own)) {
            candidate.reason = "WARNED";
            candidate.code = own.verdict.reason;
            delete candidate.detail;
            // As below: a table whose other-tier copy answered gets no warning.
            if (
               !used.has(candidate.tableId) &&
               !warned.has(candidate.tableId)
            ) {
               warned.add(candidate.tableId);
               // The caller queried this source, so naming it reveals nothing.
               warnings.push({
                  code: own.verdict.reason,
                  tableId: candidate.tableId,
                  sourceName: own.verdict.sourceName,
                  message: verdictWarningMessage(own.verdict.reason),
               });
            }
         }
         continue;
      }
      candidate.reason = "WARNED";
      candidate.code = verdict.reason;
      delete candidate.detail;
      if (used.has(candidate.tableId) || warned.has(candidate.tableId)) {
         continue;
      }
      warned.add(candidate.tableId);
      // Named like `notConsidered`: never a source the caller cannot list.
      if (input.isVisibleSource && !input.isVisibleSource(verdict.sourceName)) {
         continue;
      }
      warnings.push({
         code: verdict.reason,
         tableId: candidate.tableId,
         sourceName: verdict.sourceName,
         message: verdictWarningMessage(verdict.reason),
      });
   }
   return warnings;
}

/** The queried source's own `live` verdict, with the table it would have. */
function ownSourceVerdict(
   input: Pick<QueryPathInput, "catalog" | "chain">,
): { verdict: LiveTableVerdict; ownTableId?: string } | undefined {
   const sourceID = input.chain[0];
   if (!sourceID) return undefined;
   const verdict = input.catalog?.sourceVerdicts?.get(sourceID);
   if (!verdict) return undefined;
   if (verdict.reason !== "refused" && verdict.reason !== "row_level_gate") {
      return undefined;
   }
   const ownTableId = input.catalog?.planned.get(sourceID)?.sourceEntityId;
   return { verdict, ...(ownTableId ? { ownTableId } : {}) };
}

/**
 * Whether the queried source's own verdict explains a skip: `CONTRACT` on any
 * table (the gate that refused the source kept the query off it), and
 * `NOT_BUILT` only on the source's own table, which the refusal left unbuilt.
 * A skip the router reached by trying the table keeps its own reason.
 */
function ownVerdictExplains(
   candidate: ApiCandidate,
   own: { ownTableId?: string },
): boolean {
   if (candidate.reason === "CONTRACT") return true;
   return (
      candidate.reason === "NOT_BUILT" &&
      own.ownTableId !== undefined &&
      candidate.tableId === own.ownTableId
   );
}

/**
 * A caller-safe sentence for a verdict reason. Never the package warning's own
 * text, which can carry a refusal's detail; that stays on the package's
 * warnings and its status.
 */
function verdictWarningMessage(reason: string): string {
   switch (reason) {
      case "storage_mode":
         return "Built into storage, but the server's storage mode routes no query to it, so its queries are answered live.";
      case "unreachable":
         return "No source the package publishes reaches this table, so no query is answered from it.";
      case "row_level_gate":
         return "Every published entry point over this table carries a row-level gate it cannot keep yet, so its queries are answered live.";
      case "refused":
         return "The package refuses to serve this table, so its queries are answered live; the package's warnings say why.";
      case "shadowed":
         return "Its storage table is bound to a different source of the same name, so its queries are answered live.";
      case "served_copy":
         return "Only the served copy reads this table, and it does not answer from it here, so its queries are answered live.";
      default:
         return "This table carries a publish warning and serves live; the package's warnings say why.";
   }
}

/**
 * The reason for a variant the router skipped because it did not answer.
 * Anything unexplained is a compile failure against the stored form: almost
 * always a field or refinement the stored form does not carry.
 */
function skipReasonOf(decision: ConsideredVariant): SkipReason {
   return declineOfCode(decision.code)?.reason ?? "MISSING_FIELD";
}

/** Whether the run that answered read this colocated table from the manifest. */
function readByRun(input: QueryPathInput, t: TableState): boolean {
   return !!t.tableId && !!input.colocatedRead?.has(t.tableId);
}

/**
 * The tables that could answer a query on the chain, nearest first: a persist
 * table whose source is on the chain, and a rollup whose base is.
 */
function tablesFor(
   input: QueryPathInput,
   onChain: ReadonlySet<string>,
   fresh: ReadonlySet<string>,
   /** The router tried the `storage=` tier for this query. */
   storageCandidate: boolean,
): TableState[] {
   const out: TableState[] = [];
   const seen = new Set<string>();
   const add = (t: TableState) => {
      if (seen.has(t.key)) return;
      seen.add(t.key);
      out.push(t);
   };
   const planned = input.catalog?.planned;
   // A storage table, then (unless the storage tier answers for it) the
   // source's own colocated table, which Malloy substitutes when the tier is
   // not routed. An unbuilt colocated twin of a storage table is not reported:
   // the source was never meant to build there.
   const addSource = (
      binding: StorageTable | undefined,
      plan: PlannedTable | undefined,
      kind: TableState["kind"],
      rollupName?: string,
      variant?: boolean,
   ) => {
      const named = {
         ...(rollupName ? { rollupName } : {}),
         ...(variant ? { variant } : {}),
      };
      // A `#(preaggregate)` table is the rollup leg's on either tier.
      if (variant && binding) {
         add({ ...storageState(binding, plan, kind, fresh), ...named });
         return;
      }
      if (binding) {
         if (storageCandidate) {
            add({ ...storageState(binding, plan, kind, fresh), ...named });
            return;
         }
         // A colocated `#(materialize)` table the served copy was not tried
         // for: reported as its colocated twin below, never warned.
         if (input.storageWarned && !binding.colocated) {
            add({
               ...storageState(binding, plan, kind, fresh),
               ...named,
               warned: true,
            });
         }
      }
      if (!plan) return;
      const colocated = { ...colocatedState(plan, kind, input), ...named };
      if (!binding || !colocated.unbuilt) add(colocated);
   };

   // Persist tables, in chain order.
   for (const sourceID of input.chain) {
      const plan = planned?.get(sourceID);
      const binding = input.storage.all.find(
         (b) => b.origin !== "preaggregate" && b.sourceIDs?.includes(sourceID),
      );
      addSource(
         binding,
         plan?.origin === "persist" ? plan : undefined,
         "materialized",
      );
   }

   // Rollups of a source on the chain.
   for (const rollup of input.rollups) {
      const baseOnChain = rollup.baseSourceID
         ? onChain.has(rollup.baseSourceID) ||
           (rollup.variant === true &&
              !!input.joinedRollupBases?.has(rollup.baseSourceID))
         : input.chain.some((id) => nameOf(id) === rollup.baseSourceName);
      if (!baseOnChain) continue;
      const binding = input.storage.all.find(
         (b) =>
            b.origin === "preaggregate" &&
            b.sourceName === rollup.rollupSourceName,
      );
      addSource(
         binding,
         planForRollup(input, rollup.rollupSourceName),
         "rollup",
         rollup.rollupSourceName,
         rollup.variant,
      );
   }
   // Storage first, then colocated; chain order within each.
   return [
      ...out.filter((t) => t.tier === "storage"),
      ...out.filter((t) => t.tier === "colocated"),
   ];
}

/**
 * The plan of a rollup this model synthesized, by its name within this model.
 * A rollup's synthesized name is unique only per model, so a name planned in
 * several models is matched to this model's entry. Several entries that share
 * one content address are one table: every model that imports an annotated
 * source (an `index.malloy` that re-exports it) synthesizes its own companion
 * entry for the same rollup. Only entries naming different tables, none of
 * them this model's, are ambiguous, and those name nothing rather than another
 * model's table.
 */
function planForRollup(
   input: Pick<QueryPathInput, "catalog" | "modelPath">,
   rollupSourceName: string,
): PlannedTable | undefined {
   const named = [...(input.catalog?.planned.values() ?? [])].filter(
      (p) => p.origin === "preaggregate" && p.name === rollupSourceName,
   );
   if (named.length <= 1) return named[0];
   const own = named.filter((p) => p.modelPath === input.modelPath);
   if (own.length === 1) return own[0];
   const addresses = new Set(named.map((p) => p.sourceEntityId));
   return addresses.size === 1 ? named[0] : undefined;
}

/** A storage binding's state, named through the plan where it knows it. */
function stateOfBinding(
   input: QueryPathInput,
   binding: StorageTable,
   fresh: ReadonlySet<string>,
): TableState {
   const rollup = binding.origin === "preaggregate";
   const plan = rollup
      ? planForRollup(input, binding.sourceName)
      : binding.sourceIDs
           ?.map((id) => input.catalog?.planned.get(id))
           .find((p) => p !== undefined);
   return storageState(
      binding,
      plan,
      rollup ? "rollup" : "materialized",
      fresh,
   );
}

function storageState(
   binding: StorageTable,
   plan: PlannedTable | undefined,
   kind: TableState["kind"],
   fresh: ReadonlySet<string>,
): TableState {
   return {
      key: `${binding.colocated ? "colocated" : "storage"}:${binding.tablePath}`,
      ...(plan ? { tableId: plan.sourceEntityId } : {}),
      kind,
      tier: binding.colocated ? "colocated" : "storage",
      stale: !fresh.has(binding.tablePath),
      unbuilt: false,
      ...(binding.freshAsOf ? { dataAsOf: binding.freshAsOf } : {}),
      binding,
   };
}

function colocatedState(
   plan: PlannedTable,
   kind: TableState["kind"],
   input: Pick<QueryPathInput, "catalog" | "freshColocated">,
): TableState {
   const bound = input.catalog?.bound[plan.sourceEntityId];
   return {
      key: `colocated:${plan.sourceEntityId}`,
      tableId: plan.sourceEntityId,
      kind,
      tier: "colocated",
      // Bound, but left out of this query's build manifest by its window.
      stale:
         !!bound &&
         !!input.freshColocated &&
         !input.freshColocated.has(plan.sourceEntityId),
      unbuilt: !bound,
      ...(bound?.dataAsOf ? { dataAsOf: bound.dataAsOf } : {}),
      ...(bound ? { tableName: bound.tableName } : {}),
   };
}

/**
 * Materialized tables near the queried source that the query cannot use:
 * `below`, on a source that extends the queried one; `sibling`, on a source
 * sharing an ancestor with it, off its chain. Nearest first, at most
 * {@link MAX_NOT_CONSIDERED}.
 */
function nearbyTables(
   input: QueryPathInput,
   onChain: ReadonlySet<string>,
   /** Tables already on the chain: a source that shares one is no neighbour. */
   chainTables: ReadonlySet<string | undefined>,
): ApiNearby[] {
   const queried = input.chain[0];
   if (!queried || !input.catalog) return [];
   const builtStorage = new Set(
      input.storage.all.flatMap((b) =>
         b.origin === "preaggregate"
            ? [b.sourceName]
            : [...(b.sourceIDs ?? [])],
      ),
   );
   const found: (ApiNearby & { distance: number })[] = [];
   const consider = (
      tableSourceID: string,
      table: PlannedTable,
      kind: ApiNearby["kind"],
      sourceName: string,
   ) => {
      if (onChain.has(tableSourceID)) return;
      if (chainTables.has(table.sourceEntityId)) return;
      // A source hidden from this caller is never named, not even as a
      // neighbour: the report would disclose that it exists.
      if (input.isVisibleSource && !input.isVisibleSource(sourceName)) return;
      const chain = input.chains.get(tableSourceID);
      if (!chain) return;
      let relationship: ApiNearby["relationship"] | undefined;
      let distance = Infinity;
      const below = chain.indexOf(queried);
      if (below > 0) {
         relationship = "below";
         distance = below;
      } else {
         for (const [i, id] of chain.entries()) {
            const j = input.chain.indexOf(id);
            if (j >= 0 && i + j < distance) {
               relationship = "sibling";
               distance = i + j;
            }
         }
      }
      if (!relationship) return;
      found.push({
         tableId: table.sourceEntityId,
         kind,
         sourceName,
         relationship,
         built:
            !!input.catalog?.bound[table.sourceEntityId] ||
            builtStorage.has(
               table.origin === "preaggregate" ? table.name : table.sourceID,
            ),
         distance,
      });
   };
   for (const p of input.catalog.planned.values()) {
      if (p.origin === "persist") {
         consider(p.sourceID, p, "materialized", p.name);
      }
   }
   for (const rollup of input.rollups) {
      const plan = planForRollup(input, rollup.rollupSourceName);
      const base =
         rollup.baseSourceID ??
         [...input.chains.keys()].find(
            (id) => nameOf(id) === rollup.baseSourceName,
         );
      if (plan && base) consider(base, plan, "rollup", rollup.baseSourceName);
   }
   // Sources with one content address share one table: name it once, by its
   // nearest source.
   const byTable = new Map<string, ApiNearby & { distance: number }>();
   for (const f of found.sort(nearestFirst)) {
      if (!byTable.has(f.tableId)) byTable.set(f.tableId, f);
   }
   return [...byTable.values()]
      .sort(nearestFirst)
      .slice(0, MAX_NOT_CONSIDERED)
      .map(({ distance: _distance, ...nearby }) => nearby);
}

/** Nearest first, then by source name, so the order is stable. */
function nearestFirst(
   a: { distance: number; sourceName: string },
   b: { distance: number; sourceName: string },
): number {
   return (
      a.distance - b.distance ||
      (a.sourceName < b.sourceName ? -1 : a.sourceName > b.sourceName ? 1 : 0)
   );
}

/** A `sourceID`'s declared name (`name@<url>`). */
function nameOf(sourceID: string): string {
   const at = sourceID.lastIndexOf("@");
   return at > 0 ? sourceID.slice(0, at) : sourceID;
}

// --- Reading the compiled IR ---------------------------------------------

/** The IR fragments read below; everything else is walked generically. */
interface IrSource {
   type?: string;
   as?: string;
   name?: string;
   sourceID?: string;
   referenceID?: string;
   extends?: string;
   fields?: readonly IrField[];
   query?: { structRef?: unknown };
}

interface IrField {
   type?: string;
   name?: string;
   as?: string;
   join?: string;
   expressionType?: string;
   fields?: readonly IrField[];
   path?: readonly string[];
   e?: unknown;
   pipeline?: readonly IrStage[];
}

interface IrStage {
   type?: string;
   queryFields?: readonly IrField[];
   filterList?: readonly { e?: unknown }[];
   queryTimezone?: unknown;
}

/**
 * The query's shape from its compiled first stage: what it groups by (or
 * selects), filters on and aggregates, as dotted paths from the source. A
 * field the query defines inline lists the fields it reads.
 */
export function queryShapeOf(
   query: unknown,
   struct: unknown,
   names: { sourceName: string; viewName?: string },
): ApiQueryShape {
   const grouped = new Set<string>();
   const filtered = new Set<string>();
   const measures = new Set<string>();
   const source = struct as IrSource | undefined;
   const stage = (query as { pipeline?: readonly IrStage[] } | undefined)
      ?.pipeline?.[0];
   // Named views already followed, so a view nesting itself cannot loop.
   const followed = new Set<IrField>();
   /**
    * One stage, read against `root` (the source, or the join a view nested
    * through a join path lives on), with `prefix` prepended to every path so
    * each is dotted from the queried source.
    */
   const visitStage = (
      s: IrStage | undefined,
      root: { fields?: readonly IrField[] } | undefined,
      prefix: readonly string[],
   ) => {
      const at = (path: readonly string[]) => [...prefix, ...path].join(".");
      for (const f of s?.queryFields ?? []) {
         if (f.type === "fieldref" && f.path) {
            const def = resolvePath(root, f.path);
            if (def?.type === "turtle") {
               // A nested named view (`nest: by_category`): its own groups,
               // filters and measures are part of what the query reads. (Malloy
               // refuses a view through a join today; the prefix keeps paths
               // dotted from the queried source if that changes.)
               if (followed.has(def)) continue;
               followed.add(def);
               const owner = f.path.slice(0, -1);
               visitStage(def.pipeline?.[0], resolveStruct(root, owner), [
                  ...prefix,
                  ...owner,
               ]);
               continue;
            }
            (isAggregate(def) ? measures : grouped).add(at(f.path));
         } else if (f.type === "turtle") {
            visitStage(f.pipeline?.[0], root, prefix);
         } else if (f.e !== undefined) {
            const into = isAggregate(f) ? measures : grouped;
            for (const path of fieldPathsIn(f.e)) into.add(at(path.split(".")));
         }
      }
      for (const filter of s?.filterList ?? []) {
         for (const path of fieldPathsIn(filter.e)) {
            filtered.add(at(path.split(".")));
         }
      }
   };
   visitStage(stage, source, []);
   const tz = stage?.queryTimezone;
   return {
      sourceName: names.sourceName,
      ...(names.viewName ? { viewName: names.viewName } : {}),
      groupedBy: [...grouped],
      filteredBy: [...filtered],
      measures: [...measures],
      ...(typeof tz === "string" && tz !== "" ? { timeZone: tz } : {}),
   };
}

function isAggregate(field: IrField | undefined): boolean {
   const t = field?.expressionType;
   return t === "aggregate" || t === "ungrouped_aggregate";
}

/** The struct a path of joins leads to (the root itself for an empty path). */
function resolveStruct(
   root: { fields?: readonly IrField[] } | undefined,
   path: readonly string[],
): { fields?: readonly IrField[] } | undefined {
   return path.length === 0 ? root : resolvePath(root, path);
}

/** The field a dotted path names, through the joins on the way. */
function resolvePath(
   source: { fields?: readonly IrField[] } | undefined,
   path: readonly string[],
): IrField | undefined {
   let fields = source?.fields;
   let found: IrField | undefined;
   for (const name of path) {
      found = fields?.find((f) => (f.as ?? f.name) === name);
      if (!found) return undefined;
      fields = found.fields;
   }
   return found;
}

/**
 * Every `{ node: "field", path }` reference in an expression, as a dotted
 * path; one inside an array query is re-rooted under its array.
 */
function fieldPathsIn(e: unknown): string[] {
   const out: string[] = [];
   forEachFieldPath(
      e,
      (path) => {
         const dotted = path.join(".");
         if (!out.includes(dotted)) out.push(dotted);
      },
      // A function's per-dialect templates, not this call's arguments.
      { skipKeys: FIELD_PATH_SKIPPED_KEYS },
   );
   return out;
}

const FIELD_PATH_SKIPPED_KEYS = new Set(["overload", "at"]);
