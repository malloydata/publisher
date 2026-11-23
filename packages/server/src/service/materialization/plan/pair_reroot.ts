// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A `#(materialize)` pair table (an inlined join_many) read back as the
 * source it stores, for a persisted view or named query built over it: the
 * join put back. A pair table holds one row per row of live's LEFT JOIN
 * through the join, so reading it as the source's rows would count pairs,
 * sum a parent's value once per child and count a childless parent's
 * null-extended row. Instead the source is re-rooted as a join over that one
 * table:
 *
 *     <build source> -> { where: __parent_row; select: <the source's columns> }
 *     extend {
 *       primary_key: <parent key>
 *       join_many: items is <build source> -> {
 *         where: items__item_id is not null
 *         select: __pair_parent_key is <parent key>, item_id is items__item_id, …
 *       } extend { primary_key: item_id } on items.__pair_parent_key = <parent key>
 *     }
 *
 * The parent is the rows the parent-row flag marks (one per parent,
 * childless ones included); each level of the path is a join_many to the
 * rows that hold that level, keyed by the level's key and renamed back to
 * the joined source's field names, nested one join per level, a level above
 * the leaf filtered on its row flag so it holds one row per row of that
 * level. Malloy's symmetric aggregates then answer the fan-out as live does:
 * a childless parent's empty row contributes nothing, as in live's left
 * join, and an aggregate written in the view is no special case. A table of
 * several paths (blocks) re-roots each block's path as its own join, every
 * read filtered to that block's rows.
 *
 * It relies on each level's key being unique and never null on that level's
 * rows: the build checks every level above the leaf before it writes, and
 * records the leaf's check in the table's contract ({@link pairLeafContractKeys}),
 * which a build over the table requires to have passed.
 *
 * What the joined source declares beyond its stored dimensions (its
 * measures, views and the fields it computes at read) is not on the join: a
 * view that reads it does not compile over the re-root.
 *
 * Pure: plan in, Malloy text out.
 */

import { identifier } from "../../malloy_text";
import type {
   MaterializeColumn,
   MaterializePair,
   MaterializePlan,
} from "../domain/table_plans";
import { pairKeyColumns, pairLevelRowColumn } from "./build_file_generator";

/** The field each level of the re-root carries its parent level's key in. */
const PAIR_PARENT_KEY_FIELD = "__pair_parent_key";

/** The pairs (paths) a plan stores: one, or one per block. */
function pairsOf(plan: MaterializePlan): readonly MaterializePair[] {
   return plan.pairBlocks?.blocks ?? (plan.pair ? [plan.pair] : []);
}

/**
 * The first join of every path the pair table stores: the join_manys on the
 * source itself that the re-root declares in place of the author's.
 */
export function pairRerootJoins(plan: MaterializePlan): string[] {
   return pairsOf(plan).map((p) => p.join);
}

/**
 * The contract keys (`ColumnContract.keys`) of the leaf checks a build over
 * the pair table needs passed (unique and never null): one per path, as
 * `run/inline_uniqueness.ts` records them (`items.options.option_id`).
 */
export function pairLeafContractKeys(plan: MaterializePlan): string[] {
   return pairsOf(plan).map((p) => {
      const names = [p.join, ...(p.hops ?? []).map((h) => h.join)];
      const hops = p.hops ?? [];
      const key = hops.length > 0 ? hops[hops.length - 1].key : p.childKey;
      return `${names.join(".")}.${key}`;
   });
}

/**
 * The source expression a pair table re-roots its source on (see the module
 * comment), in place of the bare build source a plain table re-roots on.
 * Undefined when the plan stores no pair, or a level's key is not a stored
 * column.
 */
export function pairRerootBase(plan: MaterializePlan): string | undefined {
   const pairs = pairsOf(plan);
   if (pairs.length === 0) return undefined;
   const mat = identifier(plan.buildSourceName);
   const firstJoins = new Set(pairs.map((p) => p.join));
   const lane = plan.pairBlocks?.laneColumn;
   const laneTerm = (k: number) =>
      lane === undefined ? [] : [`${identifier(lane)} = ${k}`];
   const own = plan.columns.filter(
      (c) => c.join === undefined || !firstJoins.has(c.join),
   );
   if (own.length === 0) return undefined;
   const parentKey = pairs[0].parentKey;
   const parentWhere = [identifier(pairs[0].parentRowColumn), ...laneTerm(0)];
   const joins: string[] = [];
   for (const [k, pair] of pairs.entries()) {
      const join = levelJoin(plan, pair, 1, parentKey, laneTerm(k), mat);
      if (join === undefined) return undefined;
      joins.push(join);
   }
   // malloy-text: every name through identifier
   return (
      `${mat} -> {\n  where: ${parentWhere.join(" and ")}\n` +
      `  select: ${own.map((c) => identifier(c.column)).join(", ")}\n` +
      `} extend {\n  primary_key: ${identifier(parentKey)}\n${joins.join("\n")}\n}`
   );
}

/**
 * The join_many to level `level` of `pair`'s path (1 for the first join),
 * with every deeper level nested in it, joined on `parentField` (the field
 * the level above names its key by). Undefined when a level's key is not
 * stored.
 */
function levelJoin(
   plan: MaterializePlan,
   pair: MaterializePair,
   level: number,
   parentField: string,
   laneTerms: readonly string[],
   mat: string,
): string | undefined {
   const names = [pair.join, ...(pair.hops ?? []).map((h) => h.join)];
   const keyFields = [pair.childKey, ...(pair.hops ?? []).map((h) => h.key)];
   const keyColumns = pairKeyColumns(pair);
   const depth = names.length;
   const columns = plan.columns.filter(
      (c) => c.join === pair.join && pathOf(c).length === level,
   );
   const keyField = keyFields[level - 1];
   const keyColumn = keyColumns[level];
   if (!columns.some((c) => c.column === keyColumn)) return undefined;
   const where = [
      ...(level < depth ? [identifier(pairLevelRowColumn(level))] : []),
      `${identifier(keyColumn)} is not null`,
      ...laneTerms,
   ];
   const select = [
      `${identifier(PAIR_PARENT_KEY_FIELD)} is ${identifier(keyColumns[level - 1])}`,
      ...columns.map(
         (c) => `${identifier(c.dimension)} is ${identifier(c.column)}`,
      ),
   ];
   const inner =
      level < depth
         ? levelJoin(plan, pair, level + 1, keyField, laneTerms, mat)
         : "";
   if (inner === undefined) return undefined;
   const pad = "  ".repeat(level);
   const name = identifier(names[level - 1]);
   // malloy-text: every name through identifier
   return (
      `${pad}join_many: ${name} is ${mat} -> {\n` +
      `${pad}  where: ${where.join(" and ")}\n` +
      `${pad}  select: ${select.join(", ")}\n` +
      `${pad}} extend {\n${pad}  primary_key: ${identifier(keyField)}` +
      `${inner ? `\n${inner}` : ""}\n` +
      `${pad}} on ${name}.${identifier(PAIR_PARENT_KEY_FIELD)} = ${identifier(parentField)}`
   );
}

/** The joins from the source down to a joined column's level. */
function pathOf(c: MaterializeColumn): readonly string[] {
   return c.path ?? (c.join !== undefined ? [c.join] : []);
}
