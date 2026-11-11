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
 * - the SQL that actually ran, which says which stored table answered: a
 *   variant's table, or a colocated `#@ persist` table Malloy substituted
 *   through the build manifest (a tier that never reaches the router);
 * - the package's catalog of materialized tables (its build plan, bound
 *   manifest and `storage=` bindings), read against the queried source's
 *   chain.
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

export type ApiQueryPath = components["schemas"]["QueryPath"];
export type ApiQueryShape = components["schemas"]["QueryShape"];
type ApiCandidate = components["schemas"]["QueryPathCandidate"];
type ApiNearby = components["schemas"]["QueryPathNearby"];
type ApiServer = components["schemas"]["QueryPathServer"];
type SkipReason = components["schemas"]["SkipReason"];

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
}

/** A colocated table the package has bound, from its manifest. */
export interface BoundTable {
   tableName: string;
   dataAsOf?: string;
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
}

/** A `storage=` serve binding, as the report reads it. */
export interface StorageTable {
   /** The source it materializes, or a rollup's synthesized name. */
   sourceName: string;
   sourceIDs?: readonly string[];
   origin?: "persist" | "preaggregate";
   tablePath: string;
   freshAsOf?: string;
}

/** A rollup this model's pre-aggregation plans. */
export interface ModelRollup {
   rollupSourceName: string;
   baseSourceName: string;
   baseSourceID?: string;
}

/** One table the report can name, with what is known about it now. */
interface TableState {
   tableId: string;
   kind: "materialized" | "rollup";
   tier: "storage" | "colocated";
   /** Built and bound, but withheld by its freshness window. */
   stale: boolean;
   /** Not bound to the package at all. */
   unbuilt: boolean;
   dataAsOf?: string;
   /** For a colocated table: the name the SQL substitutes. */
   tableName?: string;
   /** For a storage table: the binding. */
   binding?: StorageTable;
}

/** What happened to the query, as the model saw it. */
export interface QueryPathInput {
   shape: ApiQueryShape;
   /** The queried source's `sourceID`s, nearest first (see {@link sourceChainOf}). */
   chain: readonly string[];
   catalog: MaterializedTableCatalog | undefined;
   /**
    * The colocated tables inside their freshness window now (the per-query
    * build manifest's keys). Undefined treats every bound table as fresh.
    */
   freshColocated?: ReadonlySet<string>;
   /** This model's `storage=` bindings, and the fresh subset of them. */
   storage: { all: readonly StorageTable[]; fresh: readonly StorageTable[] };
   rollups: readonly ModelRollup[];
   /** The router's decision; empty when no candidate existed. */
   considered: readonly ConsideredVariant[];
   /** For a storage answer: the bindings whose tables the routed query read. */
   storageTablesRead?: readonly StorageTable[];
   /** The routed storage read failed at run time and the query ran live. */
   runFailed?: { detail: string };
   /** The SQL that answered (the live retry's, after a run failure). */
   sql: string | undefined;
   /** The `sourceID` chain of every source in the model, for `notConsidered`. */
   chains: ReadonlyMap<string, readonly string[]>;
}

/**
 * Assemble the path report. Order in `considered` follows the order Publisher
 * tries the tiers: `storage=` tables, then colocated rollups, then colocated
 * `#@ persist` tables nearest first.
 */
export function buildQueryPath(input: QueryPathInput): ApiQueryPath {
   const onChain = new Set(input.chain);
   const tables = tablesFor(input, onChain);
   const decided = new Map(input.considered.map((c) => [c.kind, c]));
   const storageDecision = decided.get("storage");
   const rollupDecision = decided.get("rollup");
   const considered: ApiCandidate[] = [];
   let servedBy: ApiServer = { kind: "live" };
   const markUsed = (t: TableState) => {
      considered.push({ tableId: t.tableId, kind: t.kind, outcome: "used" });
      servedBy = {
         kind: t.kind,
         tableId: t.tableId,
         ...(t.dataAsOf ? { dataAsOf: t.dataAsOf } : {}),
      };
   };
   const skip = (t: TableState, reason: SkipReason, detail?: string) =>
      considered.push({
         tableId: t.tableId,
         kind: t.kind,
         outcome: "skipped",
         reason,
         ...(detail ? { detail } : {}),
      });

   // `storage=`: tried only when the tier was a candidate at all (mode on,
   // bindings and a destination present).
   const storageTables = tables.filter((t) => t.tier === "storage");
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
               skip(t, "RUN_FAILED", input.runFailed.detail);
            } else {
               markUsed(t);
               storageAnswered = true;
            }
         } else {
            skip(t, skipReasonOf(storageDecision), storageDecision.detail);
         }
      }
      // The shape answered from a table the chain walk did not place (an IR
      // shape it does not follow): still name it, so `servedBy` never says
      // `live` for a storage answer.
      const first = input.storageTablesRead?.[0];
      const placed = considered.some(
         (c) => c.outcome === "used" || c.reason === "RUN_FAILED",
      );
      if (storageDecision.outcome === "used" && first && !placed) {
         const t = stateOfBinding(input, first);
         if (input.runFailed) {
            skip(t, "RUN_FAILED", input.runFailed.detail);
         } else {
            markUsed(t);
            storageAnswered = true;
         }
      }
   }

   // Colocated rollups, through the pre-aggregation companion.
   const colocatedRollups = tables.filter(
      (t) => t.tier === "colocated" && t.kind === "rollup" && !t.unbuilt,
   );
   let rollupAnswered = false;
   if (rollupDecision && !storageAnswered) {
      const used =
         rollupDecision.outcome === "used"
            ? colocatedRollups.find(
                 (t) => !t.stale && sqlReads(input.sql, t.tableName),
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
            } else if (!used) {
               // The companion compiled, but Malloy's composite chose the base.
               skip(t, "GRAIN_TOO_COARSE");
            }
         } else {
            skip(t, skipReasonOf(rollupDecision), rollupDecision.detail);
         }
      }
   }

   // Colocated `#@ persist`: Malloy substitutes the bound table into whatever
   // runnable answered, unless a stored variant replaced the source.
   if (!storageAnswered && !rollupAnswered) {
      let answered = false;
      for (const t of tables) {
         if (t.tier !== "colocated" || t.kind !== "materialized" || t.unbuilt)
            continue;
         if (t.stale) {
            skip(t, "STALE");
         } else if (!answered && sqlReads(input.sql, t.tableName)) {
            markUsed(t);
            answered = true;
         }
      }
   }

   for (const t of tables) if (t.unbuilt) skip(t, "NOT_BUILT");

   return {
      query: input.shape,
      servedBy,
      considered,
      notConsidered: nearbyTables(
         input,
         onChain,
         new Set(tables.map((t) => t.tableId)),
      ),
   };
}

/** The reason for a variant the router skipped because it did not answer. */
function skipReasonOf(decision: ConsideredVariant): SkipReason {
   // A rollup refused for an aggregate it would compute over its own rows.
   if (decision.errorName === "RollupCoverageError") {
      return decision.detail?.includes("reaches past")
         ? "MISSING_FIELD"
         : "COUNT_SEMANTICS";
   }
   // A compile failure against the stored form: almost always a field or
   // refinement the stored form does not carry.
   return "MISSING_FIELD";
}

/** Whether `sql` reads the table named `tableName` (as a whole identifier). */
function sqlReads(sql: string | undefined, tableName: string | undefined) {
   if (!sql || !tableName) return false;
   let from = 0;
   for (;;) {
      const at = sql.indexOf(tableName, from);
      if (at < 0) return false;
      const before = sql[at - 1];
      const after = sql[at + tableName.length];
      if (!isIdentifierChar(before) && !isIdentifierChar(after)) return true;
      from = at + 1;
   }
}

function isIdentifierChar(c: string | undefined): boolean {
   return c !== undefined && /[A-Za-z0-9_]/.test(c);
}

/**
 * The tables that could answer a query on the chain, nearest first: a persist
 * table whose source is on the chain, and a rollup whose base is.
 */
function tablesFor(
   input: QueryPathInput,
   onChain: ReadonlySet<string>,
): TableState[] {
   const out: TableState[] = [];
   const seen = new Set<string>();
   const add = (t: TableState) => {
      if (seen.has(t.tableId)) return;
      seen.add(t.tableId);
      out.push(t);
   };
   const fresh = new Set(input.storage.fresh.map((b) => b.tablePath));
   const planned = input.catalog?.planned;
   const plannedByName = new Map<string, PlannedTable>();
   for (const p of planned?.values() ?? []) plannedByName.set(p.name, p);

   // Persist tables, in chain order.
   for (const sourceID of input.chain) {
      const plan = planned?.get(sourceID);
      const binding = input.storage.all.find(
         (b) => b.origin !== "preaggregate" && b.sourceIDs?.includes(sourceID),
      );
      if (binding) {
         add(storageState(binding, plan, "materialized", fresh));
         continue;
      }
      if (plan && plan.origin === "persist") {
         add(colocatedState(plan, "materialized", input));
      }
   }

   // Rollups of a source on the chain.
   for (const rollup of input.rollups) {
      const baseOnChain = rollup.baseSourceID
         ? onChain.has(rollup.baseSourceID)
         : input.chain.some((id) => nameOf(id) === rollup.baseSourceName);
      if (!baseOnChain) continue;
      const plan = plannedByName.get(rollup.rollupSourceName);
      const binding = input.storage.all.find(
         (b) =>
            b.origin === "preaggregate" &&
            b.sourceName === rollup.rollupSourceName,
      );
      if (binding) add(storageState(binding, plan, "rollup", fresh));
      else if (plan) add(colocatedState(plan, "rollup", input));
   }
   // Storage first, then colocated; chain order within each.
   return [
      ...out.filter((t) => t.tier === "storage"),
      ...out.filter((t) => t.tier === "colocated"),
   ];
}

/** A storage binding's state, named through the plan where it knows it. */
function stateOfBinding(
   input: QueryPathInput,
   binding: StorageTable,
): TableState {
   const planned = [...(input.catalog?.planned.values() ?? [])];
   const rollup = binding.origin === "preaggregate";
   const plan = rollup
      ? planned.find((p) => p.name === binding.sourceName)
      : planned.find((p) => binding.sourceIDs?.includes(p.sourceID));
   return storageState(
      binding,
      plan,
      rollup ? "rollup" : "materialized",
      new Set(input.storage.fresh.map((b) => b.tablePath)),
   );
}

function storageState(
   binding: StorageTable,
   plan: PlannedTable | undefined,
   kind: TableState["kind"],
   fresh: ReadonlySet<string>,
): TableState {
   return {
      // A binding the plan does not know (a plan that failed to compute) is
      // still named, by its table.
      tableId: plan?.sourceEntityId ?? binding.tablePath,
      kind,
      tier: "storage",
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
   chainTables: ReadonlySet<string>,
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
   const plannedByName = new Map<string, PlannedTable>();
   for (const p of input.catalog.planned.values()) {
      plannedByName.set(p.name, p);
      if (p.origin === "persist") {
         consider(p.sourceID, p, "materialized", p.name);
      }
   }
   for (const rollup of input.rollups) {
      const plan = plannedByName.get(rollup.rollupSourceName);
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

interface IrModel {
   contents?: Record<string, unknown>;
   sourceRegistry?: Record<
      string,
      { entry?: { type?: string; name?: string } & IrSource }
   >;
}

/**
 * The `sourceID`s a source derives from, itself first: through `extends` (an
 * `extend`), `referenceID` (a rename) and a query source's base. Persistence is
 * inherited along exactly these hops, so a table on any of them can answer a
 * query on the source.
 */
export function sourceChainOf(struct: unknown, modelDef: unknown): string[] {
   const model = modelDef as IrModel | undefined;
   const out: string[] = [];
   const seen = new Set<unknown>();
   const byId = (id: string): IrSource | undefined => {
      const entry = model?.sourceRegistry?.[id]?.entry;
      if (!entry) return undefined;
      if (entry.type === "source_registry_reference" && entry.name) {
         return model?.contents?.[entry.name] as IrSource | undefined;
      }
      return entry;
   };
   let current = struct as IrSource | undefined;
   for (let hops = 0; current && hops < 64; hops++) {
      if (seen.has(current)) break;
      seen.add(current);
      const id = current.sourceID;
      if (typeof id === "string" && !out.includes(id)) out.push(id);
      const next: IrSource | undefined =
         (typeof current.extends === "string"
            ? byId(current.extends)
            : undefined) ??
         (typeof current.referenceID === "string" && current.referenceID !== id
            ? byId(current.referenceID)
            : undefined) ??
         querySourceBase(current, model);
      // A hop that resolves to nothing still names the source it points at.
      if (!next) {
         for (const ref of [current.extends, current.referenceID]) {
            if (typeof ref === "string" && !out.includes(ref)) out.push(ref);
         }
      }
      current = next;
   }
   return out;
}

function querySourceBase(
   struct: IrSource,
   model: IrModel | undefined,
): IrSource | undefined {
   if (struct.type !== "query_source") return undefined;
   const ref = struct.query?.structRef;
   if (typeof ref !== "string") {
      return ref && typeof ref === "object" ? (ref as IrSource) : undefined;
   }
   // A name in the model, or a `sourceID` in its registry.
   const byName = model?.contents?.[ref] as IrSource | undefined;
   if (byName && typeof byName === "object") return byName;
   const entry = model?.sourceRegistry?.[ref]?.entry;
   if (entry?.type === "source_registry_reference" && entry.name) {
      return model?.contents?.[entry.name] as IrSource | undefined;
   }
   return entry;
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
   const visitStage = (s: IrStage | undefined) => {
      for (const f of s?.queryFields ?? []) {
         if (f.type === "fieldref" && f.path) {
            const def = resolvePath(source, f.path);
            if (def?.type === "turtle") continue;
            (isAggregate(def) ? measures : grouped).add(f.path.join("."));
         } else if (f.type === "turtle") {
            visitStage(f.pipeline?.[0]);
         } else if (f.e !== undefined) {
            const into = isAggregate(f) ? measures : grouped;
            for (const path of fieldPathsIn(f.e)) into.add(path);
         }
      }
      for (const filter of s?.filterList ?? []) {
         for (const path of fieldPathsIn(filter.e)) filtered.add(path);
      }
   };
   visitStage(stage);
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

/** The field a dotted path names, through the joins on the way. */
function resolvePath(
   source: IrSource | undefined,
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

/** Every `{ node: "field", path }` reference in an expression, as a dotted path. */
function fieldPathsIn(e: unknown): string[] {
   const out: string[] = [];
   const walk = (n: unknown) => {
      if (!n || typeof n !== "object") return;
      if (Array.isArray(n)) {
         for (const item of n) walk(item);
         return;
      }
      const node = n as { node?: unknown; path?: unknown };
      if (node.node === "field" && Array.isArray(node.path)) {
         const path = (node.path as unknown[]).join(".");
         if (!out.includes(path)) out.push(path);
         return;
      }
      for (const [key, value] of Object.entries(n)) {
         // A function's per-dialect templates, not this call's arguments.
         if (key === "overload" || key === "at") continue;
         walk(value);
      }
   };
   walk(e);
   return out;
}
