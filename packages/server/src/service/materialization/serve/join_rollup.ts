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
 * joined row meets one row of NULLs. Until malloydata/malloy#3208 (merged,
 * not yet released), Malloy aggregated that row as a row of the joined
 * source: a join_many's `count()` counts it as one child, and a sum, min or
 * max whose operand is not NULL there (a constant, `??`, a computed value)
 * reads it. The rollup holds no such row, so for those measures it would
 * disagree with live. {@link malloyCountsChildlessParentsAsZero} says which
 * Malloy is pinned; until it is true the leg declines (`COUNT_SEMANTICS`) a
 * count through a join_many and any partial whose operand is not a bare
 * column of the joined source. Through a join_one, a count already skips the
 * unmatched row (it counts the joined key), so it is served.
 *
 * Gates: a gated entry point skips every rollup (`CONTRACT`) as for the base
 * leg; a joined source that declares a gate of its own is declined
 * (`CONTRACT`), since the rollup carries none.
 */

import { createRequire } from "module";
import type { SourceJoin } from "../plan/source_map";
import type { RollupVariantPlan } from "../plan/rollup_plan";
import type { MeasureExpr } from "../plan/rollup_measure_grammar";

/** The first Malloy release carrying malloydata/malloy#3208. */
export const MALLOY_SKIPS_UNMATCHED_JOIN_ROWS_SINCE = "0.0.438";

function installedMalloyVersion(): string | undefined {
   try {
      return (
         createRequire(import.meta.url)("@malloydata/malloy/package.json") as {
            version?: string;
         }
      ).version;
   } catch {
      return undefined;
   }
}

/** `a >= b` for dotted numeric versions; false when either is unreadable. */
export function versionAtLeast(a: string | undefined, b: string): boolean {
   const parse = (v: string) => {
      const core = v.split(/[-+]/)[0] ?? "";
      const parts = core.split(".").map((p) => Number(p));
      return parts.every((p) => Number.isInteger(p) && p >= 0)
         ? parts
         : undefined;
   };
   const x = a === undefined ? undefined : parse(a);
   const y = parse(b);
   if (!x || !y) return false;
   for (let i = 0; i < Math.max(x.length, y.length); i++) {
      const d = (x[i] ?? 0) - (y[i] ?? 0);
      if (d !== 0) return d > 0;
   }
   return true;
}

const SKIPS_UNMATCHED = versionAtLeast(
   installedMalloyVersion(),
   MALLOY_SKIPS_UNMATCHED_JOIN_ROWS_SINCE,
);

/**
 * Whether the pinned Malloy aggregates a joined source over its own rows only
 * (malloydata/malloy#3208, fixing #1762): a childless parent counts as zero
 * children through a join_many, and the row of NULLs a LEFT JOIN writes for
 * an unmatched parent never reaches a sum, min or max. The one switch the
 * join-to-rollup leg's count and operand rules read; it turns on by itself
 * when Publisher's Malloy pin reaches the release that carries the fix, and
 * `join_rollup_semantics.component.spec.ts` checks it against what the
 * pinned Malloy actually answers.
 */
export function malloyCountsChildlessParentsAsZero(): boolean {
   return SKIPS_UNMATCHED;
}

/** One join of the query's source that a rollup of its joined source can answer through. */
export interface JoinRollupLeg {
   join: SourceJoin & { keyColumn: string; baseKey: string; sourceID: string };
   plan: RollupVariantPlan;
   /** `one` when the rollup holds one row per key (its grain is the key alone). */
   relationship: "one" | "many";
}

/** Why a join cannot take the leg at all, whatever the query: a skip code and a caller-safe detail. */
export interface JoinRollupRefusal {
   code:
      | "rollup_count_semantics"
      | "rollup_grain_too_coarse"
      | "rollup_contract";
   callerDetail: string;
}

/**
 * The legs a join of the query's source offers: the join's rollups, each with
 * why it cannot be used, if it cannot. `filterReadsJoin` is whether the
 * source's own `where:` reads the join; `otherConditionsRead` the joins whose
 * conditions read another join.
 */
export function joinRollupLegs(
   joins: readonly SourceJoin[],
   plansByBase: ReadonlyMap<string, readonly RollupVariantPlan[]>,
   sourceFilterReads: ReadonlySet<string>,
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
         out.push({ leg, ...refusalOf(join, plan, joins, sourceFilterReads) });
      }
   }
   return out;
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
   // A6 and A2 across the join: a rollup row is matched to each parent row,
   // and a measure partial re-summed across a fan-out grain dimension (or a
   // join-level one across a grain dimension other than its key) would count
   // a joined row twice. Not served through a join yet.
   if (
      plan.fanOutDimensions.length > 0 ||
      plan.rollupMeasures.some((m) => m.fanOutDimensions.length > 0)
   ) {
      return refuse(
         "rollup_grain_too_coarse",
         `the rollup of \`${name}\` has a grain dimension its measures fan out across`,
      );
   }
   return {};
}

/**
 * Why the stored measures `read` (by name) cannot be read through the leg's
 * join while unmatched parents still reach an aggregate
 * ({@link malloyCountsChildlessParentsAsZero} false), or undefined. `isColumn`
 * says whether a field of the joined source is a bare column (NULL on the row
 * a LEFT JOIN writes for an unmatched parent).
 */
export function unmatchedRowProblem(
   leg: JoinRollupLeg,
   read: ReadonlySet<string>,
   isColumn: (field: string) => boolean,
   skipsUnmatched = malloyCountsChildlessParentsAsZero(),
): string | undefined {
   if (skipsUnmatched) return undefined;
   for (const m of leg.plan.rollupMeasures) {
      if (!read.has(m.name)) continue;
      for (const { leaf } of m.partials) {
         if (leaf.aggregate === "count") {
            // A join_one's count counts the joined key, NULL when unmatched.
            if (leg.join.relationship === "many") {
               return `\`${leg.join.name}.${m.name}\` counts through a join_many, where Malloy counts a parent with no rows as one (malloydata/malloy#1762)`;
            }
            continue;
         }
         const operand = bareOperand(leaf.expr);
         if (operand === undefined || !isColumn(operand)) {
            return `\`${leg.join.name}.${m.name}\` aggregates a value that is not NULL on a parent with no rows, which Malloy aggregates live (malloydata/malloy#1762)`;
         }
      }
   }
   return undefined;
}

/** The one column a `x.sum()`, `sum(x)` leaf aggregates, filters aside. */
function bareOperand(expr: MeasureExpr): string | undefined {
   let e = expr;
   while (e.kind === "filtered" || e.kind === "paren") e = e.expr;
   if (e.kind === "method" && e.args.length === 0 && e.path.length === 1) {
      return e.path[0];
   }
   if (e.kind === "call" && e.args.length === 1) {
      const arg = e.args[0];
      if (arg?.kind === "path" && arg.path.length === 1) return arg.path[0];
   }
   return undefined;
}
