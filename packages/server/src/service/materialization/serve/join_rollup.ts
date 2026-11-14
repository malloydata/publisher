// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Join-to-rollup (materialization v2, phase 4, A7): the rollup leg's second
 * form. A query on a source that reads a joined source's measures
 * (`flights -> { group_by: carrier; aggregate: segments.seg_miles }`) can be
 * answered from the JOINED source's rollup, read at its grain: the variant
 * declares the query's source as the author's, with its join re-pointed at
 * the rollup on the join's key,
 *
 *     source: flights is flights__author include { *; except: segments } extend {
 *       join_one: segments is <segments' rollup at flight_id> on segments.flight_id = id
 *     }
 *
 * so every other field, join, access modifier and annotation is the author's,
 * and `segments.seg_miles` reads the rollup's merged partial. A join_one when
 * the grain is the key alone (one rollup row per parent), else a join_many.
 * (`include { *; except: … }` keeps each field's access modifier, probed on
 * Malloy 0.0.436; `public: *` would publish internal fields.)
 *
 * The rollup's rows are the joined rows grouped at the key, so the leg reads
 * them only through the joined source's own stored measures (sum, min, max
 * and their expressions; counts below). A query that computes an aggregate
 * of its own over the join (`segments.count()`, `sum(segments.x)`, or a
 * measure of the source that does) would count the rollup's rows instead
 * (`COUNT_SEMANTICS`), and one that reads a joined field the grain does not
 * keep does not compile against it (`GRAIN_TOO_COARSE` / `MISSING_FIELD`).
 *
 * **Unmatched parents (malloydata/malloy#1762).** Live, a parent with no
 * joined row meets one row of NULLs. Since malloydata/malloy#3208 (Malloy
 * 0.0.438), a `count()`, `sum` or `avg` taken at a joined source skips the
 * rows where that source's key is NULL, so the joined source's own counts
 * and sums agree with its rollup, which holds no such row, whatever their
 * operand. Two aggregates still read that row ({@link unmatchedRowProblem}):
 * a `min` or `max`, which Malloy does not guard, whose operand is not NULL
 * there (a constant, `??`, a computed value); and a sum or count taken at a
 * join of the joined source that can match the row of NULLs (keyed on a
 * computed dimension such as `coalesce(kind, 'p')`), whose own key is then
 * not NULL. The leg declines both (`COUNT_SEMANTICS`).
 *
 * A rollup in a `storage=` destination is joined there, so only for a query
 * source the served copy reads from the same destination: the variant is
 * then compiled over the served copy's files (known limitation 26,
 * `RollupVariantServe.answerJoinInDestination`); any other query source
 * reads its own connection, and Malloy joins within one (`CROSS_CONNECTION`,
 * malloydata/malloy#3032).
 *
 * Fan-out grains and join-level measures (A6, A2, row 27): a rollup row is
 * one group of the joined rows, so a partial re-summed across groups that
 * share a joined row counts it twice. {@link checkJoinCoverage} holds each
 * query to the base leg's rule, level by level, over `<join>.<field>`: a
 * fan-out grain dimension, and the other grain dimensions of a join-level
 * sum or count, grouped or fixed wherever the query aggregates through the
 * join. A join-level operand (`kinds.weight` in `kinds.weight.sum()`) is
 * NULL on an unmatched parent's row when its join is keyed on a column of
 * the joined source, so it is read like the joined source's own column.
 *
 * Gates: a gated entry point skips every rollup (`CONTRACT`) as for the base
 * leg; a joined source that declares a gate of its own is declined
 * (`CONTRACT`), since the rollup carries none.
 */

import type { SourceJoin } from "../plan/source_map";
import type { RollupVariantPlan } from "../plan/rollup_plan";
import type { MeasureExpr } from "../plan/rollup_measure_grammar";
import type {
   QueryLevelFacts,
   QueryLevelField,
} from "../../governance/ir/prepared_query";

/** One join of the query's source that a rollup of its joined source can answer through. */
export interface JoinRollupLeg {
   join: SourceJoin & { keyColumn: string; baseKey: string; sourceID: string };
   plan: RollupVariantPlan;
   /** `one` when the rollup holds one row per key (its grain is the key alone). */
   relationship: "one" | "many";
}

/** Why a join cannot take the leg at all, whatever the query: a skip code and a caller-safe detail. */
interface JoinRollupRefusal {
   code:
      | "rollup_count_semantics"
      | "rollup_grain_too_coarse"
      | "rollup_contract";
   callerDetail: string;
}

/**
 * The legs a join of the query's source offers: the join's rollups, each with
 * why it cannot be used, if it cannot. `sourceFilterReads` holds the joins
 * the source's own `where:` reads; `parentPrimaryKey` is the query source's
 * declared primary key, if it has one.
 */
export function joinRollupLegs(
   joins: readonly SourceJoin[],
   plansByBase: ReadonlyMap<string, readonly RollupVariantPlan[]>,
   sourceFilterReads: ReadonlySet<string>,
   parentPrimaryKey: string | undefined,
): { leg: JoinRollupLeg; refusal?: JoinRollupRefusal }[] {
   const out: { leg: JoinRollupLeg; refusal?: JoinRollupRefusal }[] = [];
   for (const join of joins) {
      if (join.relationship === "cross") continue;
      // A join defined in place (`segments extend { … }`) has no sourceID:
      // its rows are not the declared source's.
      if (join.sourceID === undefined) continue;
      const plans = plansByBase.get(join.sourceID);
      if (!plans || plans.length === 0) continue;
      for (const plan of plans) {
         const leg = {
            join: join as JoinRollupLeg["join"],
            plan,
            relationship:
               plan.grainDimensions.length === 1 &&
               plan.grainDimensions[0] === join.keyColumn
                  ? ("one" as const)
                  : ("many" as const),
         };
         out.push({
            leg,
            ...(leg.relationship === "many" || join.relationship === "many"
               ? keyRepeatRefusal(join, parentPrimaryKey)
               : {}),
            // The condition's own refusal, when it has one, is the reason given.
            ...refusalOf(join, plan, joins, sourceFilterReads),
         });
      }
   }
   return out;
}

/**
 * Why a join read as a join_many (declared one, or re-pointed as one at a
 * grain finer than its key) cannot take the leg: its key on the query's side
 * is not that source's primary key. A join_many is read as if each joined row
 * met one parent row, so live sums a joined row once per parent row that
 * meets it, and a declared join_one deduplicates at its joined row; a rollup
 * row met by several parent rows sharing a key answers neither way. Keyed on
 * the primary key, each rollup row meets one parent row.
 */
function keyRepeatRefusal(
   join: SourceJoin,
   parentPrimaryKey: string | undefined,
): { refusal?: JoinRollupRefusal } {
   if (join.baseKey === undefined || join.baseKey === parentPrimaryKey) {
      return {};
   }
   return {
      refusal: {
         code: "rollup_count_semantics",
         callerDetail: `\`${join.name}\` is keyed on \`${join.baseKey}\`, which is not the query source's primary key, so a row of its rollup could meet more than one of the source's rows`,
      },
   };
}

function refusalOf(
   join: SourceJoin,
   plan: RollupVariantPlan,
   joins: readonly SourceJoin[],
   sourceFilterReads: ReadonlySet<string>,
): { refusal?: JoinRollupRefusal } {
   const refuse = (
      code: JoinRollupRefusal["code"],
      callerDetail: string,
   ): { refusal: JoinRollupRefusal } => ({ refusal: { code, callerDetail } });
   const name = join.name;
   if (plan.baseGated) {
      return refuse(
         "rollup_contract",
         `\`${name}\` joins a source with a gate of its own, which its rollup does not carry`,
      );
   }
   if (
      join.keyColumn === undefined ||
      join.baseKey === undefined ||
      !join.condition ||
      join.condition.nested ||
      [...join.condition.joins].some((j) => j !== name)
   ) {
      return refuse(
         "rollup_count_semantics",
         `\`${name}\` is not joined on one field of each side, so its rollup's rows cannot stand in for its rows`,
      );
   }
   if (!plan.grainDimensions.includes(join.keyColumn)) {
      return refuse(
         "rollup_grain_too_coarse",
         `the rollup of \`${name}\` does not keep its join key \`${join.keyColumn}\``,
      );
   }
   if (sourceFilterReads.has(name)) {
      return refuse(
         "rollup_count_semantics",
         `the source's own where: reads \`${name}\`, whose rows the rollup does not keep`,
      );
   }
   if (
      joins.some(
         (j) =>
            j.name !== name &&
            (!j.condition || j.condition.joins.has(name) || j.condition.nested),
      )
   ) {
      return refuse(
         "rollup_count_semantics",
         `another join's condition reads \`${name}\`, whose rows the rollup does not keep`,
      );
   }
   // A6 and A2 across the join are judged per query (checkJoinCoverage),
   // except a join key that is itself a fan-out dimension: a joined row would
   // then meet the parent rows of more than one key.
   if (plan.fanOutDimensions.includes(join.keyColumn)) {
      return refuse(
         "rollup_grain_too_coarse",
         `the rollup of \`${name}\` keeps its join key \`${join.keyColumn}\` through a join_many of its own`,
      );
   }
   return {};
}

/**
 * Why the stored measures `read` (by name) cannot be read through the leg's
 * join, because Malloy still aggregates the row of NULLs a LEFT JOIN writes
 * for an unmatched parent, or undefined. Since malloydata/malloy#3208 a
 * `count()` or `sum` at the joined source skips that row, so only two leaves
 * can read it: a sum or count at a join of the joined source, which skips the
 * row only when that join meets no row there (`joinMeetsNoRow`: keyed on a
 * column of the joined source, NULL on the row); and a `min` or `max`, which
 * Malloy does not guard, whose operand is not NULL there. `isColumn` says
 * whether a path of the joined source is NULL on that row: its own column
 * (`["miles"]`), or a column of one of its joins that meets no row
 * (`["lines", "qty"]`, a join-level operand).
 */
export function unmatchedRowProblem(
   leg: JoinRollupLeg,
   read: ReadonlySet<string>,
   isColumn: (path: readonly string[]) => boolean,
   joinMeetsNoRow: (name: string) => boolean,
): string | undefined {
   for (const m of leg.plan.rollupMeasures) {
      if (!read.has(m.name)) continue;
      for (const { leaf } of m.partials) {
         if (leaf.aggregate === "min" || leaf.aggregate === "max") {
            const operand = bareOperand(leaf.expr);
            if (operand === undefined || !isColumn(operand)) {
               return `\`${leg.join.name}.${m.name}\` takes a ${leaf.aggregate} of a value that is not NULL on a parent with no rows, which Malloy reads live (malloydata/malloy#1762)`;
            }
            continue;
         }
         const at = leaf.joinPath[0];
         if (at !== undefined && !joinMeetsNoRow(at)) {
            return `\`${leg.join.name}.${m.name}\` aggregates at \`${at}\`, which is not keyed on a column of \`${leg.join.name}\` and so matches a row for a parent with no rows live (malloydata/malloy#1762)`;
         }
      }
   }
   return undefined;
}

/**
 * Why the stored measures `read` (by name) cannot be read through the leg's
 * join while the joined source's own primary key is not known unique and
 * never NULL, or undefined. Since malloydata/malloy#3208 live takes a
 * `count()`, `sum` or `avg` at the joined source over the rows whose key is
 * not NULL, and where the rows fan out, once per distinct key; its rollup,
 * built at the joined source's own grain, counts and sums every row. A
 * join keyed on that primary key (`joinedKey` the leg's key column) meets no
 * row whose key is NULL; a `min` or `max` reads every row live too; and a
 * partial taken at a join of the joined source tests that join's key, which
 * the rollup's build tests the same way. `joinedKeyUnique` is what the
 * rollup's build found of the key: true, false (repeated or NULL), or
 * undefined (not checked).
 */
export function joinedKeyProblem(
   leg: JoinRollupLeg,
   read: ReadonlySet<string>,
   joinedKey: string | undefined,
   joinedKeyUnique: boolean | undefined,
): string | undefined {
   if (
      joinedKey === undefined ||
      joinedKey === leg.join.keyColumn ||
      joinedKeyUnique === true
   ) {
      return undefined;
   }
   for (const m of leg.plan.rollupMeasures) {
      if (!read.has(m.name)) continue;
      for (const { leaf } of m.partials) {
         if (leaf.aggregate === "min" || leaf.aggregate === "max") continue;
         if (leaf.joinPath.length > 0) continue;
         return joinedKeyUnique === false
            ? `\`${leg.join.name}\` holds a value of its primary key \`${joinedKey}\` more than once or NULL, and live counts and sums \`${leg.join.name}.${m.name}\` once per non-NULL key where its rollup reads every row`
            : `\`${leg.join.name}\`'s primary key \`${joinedKey}\` was not checked unique and never NULL when its rollup was built, and live counts and sums \`${leg.join.name}.${m.name}\` once per non-NULL key where its rollup reads every row`;
      }
   }
   return undefined;
}

/**
 * Why the leg cannot read a join read as a join_many while the query
 * source's primary key is not known unique and never NULL, or undefined.
 * `parentKeyUnique` is what the rollup's build found of that key: true,
 * false (repeated or NULL), or undefined (not checked). A rollup row is met
 * by every parent row sharing its key: with the key repeated, the variant
 * sums it once per parent row where live sums each joined row once, and with
 * a NULL key the variant's `COUNT(1)` counts a parent live's
 * `COUNT(DISTINCT key)` skips.
 */
export function parentKeyProblem(
   leg: JoinRollupLeg,
   parentKeyUnique: boolean | undefined,
): string | undefined {
   if (leg.relationship === "one" && leg.join.relationship === "one") {
      return undefined;
   }
   if (parentKeyUnique === true) return undefined;
   const key = leg.join.baseKey;
   return parentKeyUnique === false
      ? `the query source holds a value of its primary key \`${key}\` more than once or NULL, so a row of the rollup of \`${leg.join.name}\` would be read once per row that shares it`
      : `the query source's primary key \`${key}\` was not checked unique and never NULL when the rollup of \`${leg.join.name}\` was built, so a row of it could be read once per row that shares it`;
}

/**
 * The one field path a `x.sum()`, `sum(x)`, `j.x.max()` leaf aggregates,
 * filters aside: one name, or a join and one name.
 */
function bareOperand(expr: MeasureExpr): string[] | undefined {
   let e = expr;
   while (e.kind === "filtered" || e.kind === "paren") e = e.expr;
   if (
      e.kind === "method" &&
      e.args.length === 0 &&
      (e.path.length === 1 || e.path.length === 2)
   ) {
      return e.path;
   }
   if (e.kind === "call" && e.args.length === 1) {
      const arg = e.args[0];
      if (
         arg?.kind === "path" &&
         (arg.path.length === 1 || arg.path.length === 2)
      ) {
         return arg.path;
      }
   }
   return undefined;
}

/**
 * A6 and A2 through the join, level by level: the base leg's
 * `checkCoverage` rule, read on the caller's compiled query, where the
 * rollup's fields are `<join>.<field>`. A rollup row is one group of the
 * joined rows, matched to each parent row with its key, so a measure partial
 * re-summed across groups that share a joined row would count that row
 * twice. Each level that aggregates anything through the join must group or
 * fix (`where: <join>.d = <value>`, there or above) every fan-out grain
 * dimension of the rollup (A6), and every grain dimension a join-level sum
 * or count it reads merges across (A2). Grouping or fixing the parent's own
 * key field fixes the join's key column: every rollup row a parent row meets
 * carries that key. A level that only reads the parent's own fields, or
 * another join's, aggregates nothing through the join: the parent's
 * aggregates are taken over its own rows whatever the join matches. Anything
 * the check cannot place (an expression over the join's measures, a
 * `having:`, a field it cannot trace) is held to every measure's dimensions;
 * an ungrouped aggregate (`all`, `exclude`) declines.
 *
 * `parentReads` says how a parent field reads the join
 * (`sourceFieldJoinReads`). Returns why the leg cannot answer, or undefined.
 */
export function checkJoinCoverage(
   leg: JoinRollupLeg,
   levels: QueryLevelFacts,
   parentReads: (name: string) => "none" | "scalar" | "aggregate",
): JoinRollupRefusal | undefined {
   const { plan, join } = leg;
   const name = join.name;
   const fanOut = new Set(plan.fanOutDimensions);
   const measureFanOut = new Map(
      plan.rollupMeasures.map((m) => [m.name, m.fanOutDimensions]),
   );
   const anyMeasureFanOut = new Set(
      plan.rollupMeasures.flatMap((m) => m.fanOutDimensions),
   );
   if (fanOut.size === 0 && anyMeasureFanOut.size === 0) return undefined;
   const grain = new Set(plan.grainDimensions);
   const all = new Set([...fanOut, ...anyMeasureFanOut]);
   const refuse = (callerDetail: string): JoinRollupRefusal => ({
      code: "rollup_grain_too_coarse",
      callerDetail,
   });
   /** Whether a field the query defines reads anything through the join. */
   const definedReadsJoin = (f: QueryLevelField): boolean =>
      f.reads === undefined ||
      f.reads.some((p) =>
         p.length > 1 ? p[0] === name : parentReads(p[0] as string) !== "none",
      );
   const visit = (
      level: QueryLevelFacts,
      fixedAbove: ReadonlySet<string>,
   ): JoinRollupRefusal | undefined => {
      if (level.kind !== "reduce") {
         return {
            code: "rollup_count_semantics",
            callerDetail:
               "a stage that is not a reduction reads the joined rows, which the rollup does not keep",
         };
      }
      const required = new Set<string>();
      let aggregates = false;
      const any = () => {
         aggregates = true;
         for (const d of anyMeasureFanOut) required.add(d);
      };
      if (level.havings.length > 0) any();
      const grouped = new Set<string>();
      let keyFixed = false;
      for (const f of level.fields) {
         if (f.isNest) continue;
         if (f.ungrouped) {
            return refuse(
               `\`${f.name}\` re-sums across a fan-out dimension of the rollup of \`${name}\``,
            );
         }
         const ref = f.ref;
         if (ref !== undefined) {
            if (ref[0] === name && ref.length === 2) {
               const field = ref[1] as string;
               const named = measureFanOut.get(field);
               if (named !== undefined) {
                  aggregates = true;
                  for (const d of named) required.add(d);
               } else if (grain.has(field)) grouped.add(field);
               else any();
            } else if (ref[0] === name) any();
            else if (ref.length === 1) {
               if (ref[0] === join.baseKey) keyFixed = true;
               if (parentReads(ref[0] as string) === "aggregate") any();
            }
            // A field of another join reads that join's source alone.
            continue;
         }
         if (
            f.expressionType !== "scalar" &&
            f.expressionType !== "scalar_analytic" &&
            definedReadsJoin(f)
         ) {
            any();
         } else if (f.computesAggregate && definedReadsJoin(f)) any();
      }
      const fixed = new Set(fixedAbove);
      for (const p of level.fixedPathsByWhere) {
         if (p.length === 2 && p[0] === name) fixed.add(p[1] as string);
         if (p.length === 1 && p[0] === join.baseKey) keyFixed = true;
      }
      for (const d of grouped) fixed.add(d);
      if (keyFixed) fixed.add(join.keyColumn);
      for (const d of [...fixed]) if (!all.has(d)) fixed.delete(d);
      if (aggregates) {
         for (const d of fanOut) required.add(d);
         const loose = [...required].find((d) => !fixed.has(d));
         if (loose !== undefined) {
            return refuse(
               fanOut.has(loose)
                  ? `the query does not group by or fix \`${name}.${loose}\`, a fan-out dimension of its rollup's grain, so its measures would be counted once per value`
                  : `the query does not group by or fix \`${name}.${loose}\`, so a measure taken through a join of \`${name}\` would count each joined row once per value of it`,
            );
         }
      }
      for (const f of level.fields) {
         if (!f.isNest) continue;
         if (!f.nest) {
            return {
               code: "rollup_count_semantics",
               callerDetail: `the nest \`${f.name}\` could not be read`,
            };
         }
         const inner = visit(f.nest, fixed);
         if (inner) return inner;
      }
      return undefined;
   };
   return visit(levels, new Set());
}
