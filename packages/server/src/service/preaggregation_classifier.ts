// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Additivity classification for `#@ preaggregate` measures: can this measure be
 * computed at a coarse grain, stored, and then correctly re-aggregated to a
 * coarser one?
 *
 * The hazard: a rollup stores `count()` per grain, and a coarser query that
 * re-ran `count()` over those rows would return THE NUMBER OF ROLLUP ROWS — a
 * plausible wrong number. So a measure is pre-aggregated only when its merge is
 * positively proven, and `count` merges with `sum`, never with `count`.
 *
 * ## Fail closed
 *
 * Anything not positively classified is non-additive. The measure must be ONE
 * aggregate at the root of its expression, drawn from `sum`, `count`, `min`,
 * `max`, either bare or with a filter written directly on it
 * (`amount.sum() { where: … }`). `avg` is not decomposed into sum/count.
 *
 * ## Why a filtered aggregate is additive
 *
 * The rollup computes each partial by the measure's NAME against the base
 * (`total__partial is total`, see preaggregation_synthesis.ts), so the filter
 * rides into the rollup build, and a row-level filter commutes with
 * partitioned re-aggregation for every merge handed out here. For groups with
 * no matching rows, `min`/`max` store NULL (ignored by their merges); `sum`
 * stores 0 (the compiled SQL wraps it in COALESCE) and `count` stores 0 (COUNT
 * never returns NULL). That 0 is safe only because it is the identity for
 * `SUM`, the merge both use, and direct computation yields the same 0; a
 * future merge for which 0 is not the identity cannot rely on this.
 *
 * Only that one shape is accepted. A filter under a wrapper
 * (`coalesce(x.sum() { where: … }, 0)`), a chained refinement
 * (`x.sum() { where: a } { where: b }`, which the compiler NESTS; the comma form
 * is one filterList and is accepted), or a non-scalar condition is refused. The
 * chain is a known false negative, kept as conscious narrowness.
 *
 * ## This is a publish gate, so a false negative is an outage
 *
 * A `#@ preaggregate` measure that lands non-additive is REFUSED at publish
 * (and fails the package load), so every non-additive result carries a
 * `detail` naming the measure and a fix: it becomes a 400 a human acts on.
 * Widening what counts as additive is safe; narrowing it breaks deployed
 * packages.
 *
 * ## Two traps in the pinned IR, both verified by probe
 *
 *  1. **`isAsymmetricExpr` is not the signal it looks like.** It returns true
 *     for `sum`, `avg`, `count`, `distinct` — it is about join fan-out, not
 *     re-aggregation. Using it to filter would reject `sum` and `count`, the two
 *     cases that matter most, and accept `min`/`max` for the wrong reason. It is
 *     not used here.
 *  2. **`ungroupings` is `[]`, not `undefined`, on measures containing any
 *     function call.** `stddev(x)` and `coalesce(x.sum(), 0)` both carry an
 *     empty `ungroupings` array while containing no `all()`/`exclude()` at all.
 *     It must be length-checked; a truthiness check silently rejects every
 *     measure that mentions a function.
 *
 * The IR facts are pinned by real-compile tests in
 * `preaggregation_classifier.spec.ts`, so a Malloy bump that changes the IR
 * fails there rather than quietly changing what a rollup stores.
 */

import type { Expr, ExpressionType } from "@malloydata/malloy";

/** The aggregate functions whose partials can be merged. */
export type AdditiveAggregate = "sum" | "count" | "min" | "max";

/** The aggregate applied to the STORED partial column to merge it. */
export type ReaggregateFunction = "sum" | "min" | "max";

/**
 * How each additive aggregate merges. `count: "sum"` is the point of this
 * module (see the header).
 */
const REAGGREGATE: Record<AdditiveAggregate, ReaggregateFunction> = {
   sum: "sum",
   count: "sum",
   min: "min",
   max: "max",
};

function isAdditiveAggregate(fn: string): fn is AdditiveAggregate {
   return fn === "sum" || fn === "count" || fn === "min" || fn === "max";
}

/**
 * Why a measure cannot be pre-aggregated. Machine-readable so callers can group
 * and count them; the paired `detail` is what a human reads.
 */
export type NonAdditiveReason =
   | "not_an_aggregate"
   | "no_expression"
   | "unsupported_aggregate"
   | "no_aggregate_found"
   | "multiple_aggregates"
   | "aggregate_not_at_root"
   | "filtered_aggregate"
   | "ungrouped_aggregate"
   | "requires_group_by"
   | "analytic";

type AdditivityResult =
   | {
        additive: true;
        /** The aggregate the measure computes; stored as the partial column. */
        aggregate: AdditiveAggregate;
        /** The aggregate the rollup member applies to that stored column. */
        reaggregate: ReaggregateFunction;
     }
   | { additive: false; reason: NonAdditiveReason; detail: string };

/**
 * The subset of a measure's `FieldDef` this classifier reads. A real compiled
 * `FieldDef` is assignable to it, so callers pass IR straight through while
 * tests can build a minimal node to pin one branch.
 */
export interface ClassifiableMeasure {
   name: string;
   as?: string;
   e?: Expr;
   expressionType?: ExpressionType;
   requiresGroupBy?: unknown[];
   ungroupings?: unknown[];
}

/** What one pass over the expression tree found. */
interface Findings {
   aggregates: { function: string }[];
   /** How many `filteredExpr` nodes the tree holds, wherever they sit. */
   filtered: number;
   ungrouped: boolean;
   analytic: boolean;
}

/**
 * Walk the WHOLE expression, not just the root. A hazard can sit anywhere: the
 * filter in `coalesce(x.sum() { where: … }, 0)` is three levels down, and a
 * measure whose root looks additive can still contain a second aggregate.
 *
 * Traversal follows the two structural shapes the IR uses — `e` (a single child)
 * and `kids` (named children, each a node or an array). `'e' in node` is how the
 * compiler's own `exprHasE` works, so `e` can be present-but-undefined; the
 * guard below is load-bearing rather than defensive.
 */
function walk(node: Expr, found: Findings): void {
   switch (node.node) {
      case "aggregate":
         found.aggregates.push({ function: node.function });
         break;
      case "filteredExpr":
         // Counted rather than flagged: acceptance below requires exactly one,
         // at the root. The walk still descends into the filter's own
         // conditions, so a hazard hiding inside one is found like any other.
         found.filtered += 1;
         break;
      case "all":
      case "exclude":
         found.ungrouped = true;
         break;
      case "function_call":
         // An aggregate function call (`stddev`) is not an `aggregate` node and
         // is caught by "no aggregate found". An ANALYTIC one is called out
         // separately because the fix differs.
         if (
            node.expressionType === "scalar_analytic" ||
            node.expressionType === "aggregate_analytic"
         ) {
            found.analytic = true;
         }
         break;
      default:
         break;
   }

   const anyNode = node as { e?: Expr; kids?: Record<string, Expr | Expr[]> };
   if ("e" in anyNode && anyNode.e !== undefined) walk(anyNode.e, found);
   if ("kids" in anyNode && anyNode.kids !== undefined) {
      for (const kid of Object.values(anyNode.kids)) {
         if (Array.isArray(kid)) {
            for (const one of kid) walk(one, found);
         } else if (kid !== undefined) {
            walk(kid, found);
         }
      }
   }
}

function nonAdditive(
   reason: NonAdditiveReason,
   detail: string,
): AdditivityResult {
   return { additive: false, reason, detail };
}

/**
 * Classify one measure. Pure: same IR in, same answer out, no compilation and no
 * I/O, so it is safe to call at publish, at load, and while planning a build.
 *
 * The checks run most-specific-first so the `detail` a human sees names the
 * actual problem rather than a downstream symptom of it — `all(x.sum())` fails
 * as an ungrouping, not as "the aggregate is not at the root", even though both
 * are true.
 */
export function classifyMeasureAdditivity(
   measure: ClassifiableMeasure,
): AdditivityResult {
   const name = measure.as ?? measure.name;

   // An ungrouping changes what the aggregate is computed OVER, so its value at
   // the rollup's grain is not a partial of the query's answer. The compiler
   // types the whole measure `ungrouped_aggregate`, which is a cheaper and more
   // reliable signal than finding the node.
   if (measure.expressionType === "ungrouped_aggregate") {
      return nonAdditive(
         "ungrouped_aggregate",
         `Measure \`${name}\` uses \`all()\` or \`exclude()\`, whose value depends on the query's grouping, so it cannot be re-aggregated from a stored partial. Remove \`#@ preaggregate\` from it.`,
      );
   }
   if (
      measure.expressionType === "scalar_analytic" ||
      measure.expressionType === "aggregate_analytic"
   ) {
      return nonAdditive(
         "analytic",
         `Measure \`${name}\` is a window function, which is computed across result rows rather than within a group, so it cannot be pre-aggregated. Remove \`#@ preaggregate\` from it.`,
      );
   }
   if (measure.expressionType !== "aggregate") {
      return nonAdditive(
         "not_an_aggregate",
         `\`${name}\` is not an aggregate measure (it is \`${measure.expressionType ?? "untyped"}\`), and only measures can be pre-aggregated. Remove \`#@ preaggregate\` from it.`,
      );
   }

   // See the header: `[]` is the common case, so length is the only safe test.
   if ((measure.requiresGroupBy?.length ?? 0) > 0) {
      return nonAdditive(
         "requires_group_by",
         `Measure \`${name}\` declares \`grouped_by\`, which constrains the grain a query may use it at; combining that with a rollup's own grain is not supported in this version. Remove \`#@ preaggregate\` from it.`,
      );
   }
   if ((measure.ungroupings?.length ?? 0) > 0) {
      return nonAdditive(
         "ungrouped_aggregate",
         `Measure \`${name}\` contains an ungrouping (\`all()\` or \`exclude()\`), so it cannot be re-aggregated from a stored partial. Remove \`#@ preaggregate\` from it.`,
      );
   }

   if (measure.e === undefined) {
      return nonAdditive(
         "no_expression",
         `Measure \`${name}\` has no expression the publisher can inspect, so it cannot be proven safe to pre-aggregate. Remove \`#@ preaggregate\` from it.`,
      );
   }

   const found: Findings = {
      aggregates: [],
      filtered: 0,
      ungrouped: false,
      analytic: false,
   };
   walk(measure.e, found);

   if (found.ungrouped) {
      return nonAdditive(
         "ungrouped_aggregate",
         `Measure \`${name}\` contains an ungrouping (\`all()\` or \`exclude()\`), so it cannot be re-aggregated from a stored partial. Remove \`#@ preaggregate\` from it.`,
      );
   }
   if (found.analytic) {
      return nonAdditive(
         "analytic",
         `Measure \`${name}\` contains a window function, which is computed across result rows rather than within a group, so it cannot be pre-aggregated. Remove \`#@ preaggregate\` from it.`,
      );
   }
   // The one accepted filter shape (see the header): ONE `filteredExpr`, at the
   // root, directly on the aggregate, every condition scalar. Checked before
   // the aggregate counts so a nonconforming filter is reported AS a filter
   // problem (`m_arith { where: … }` would otherwise read as "no aggregate").
   //
   // INVARIANT below this block: read `aggregateRoot`, never `measure.e`; on
   // the filtered shape `measure.e` is the filteredExpr wrapper.
   let aggregateRoot = measure.e;
   if (found.filtered > 0) {
      const root = measure.e;
      if (
         found.filtered !== 1 ||
         root.node !== "filteredExpr" ||
         root.kids.e.node !== "aggregate" ||
         !root.kids.filterList.every((c) => c.expressionType === "scalar")
      ) {
         return nonAdditive(
            "filtered_aggregate",
            `Measure \`${name}\` applies a filter this version cannot pre-aggregate: a filter is supported only when written directly on the measure's single aggregate, as \`amount.sum() { where: … }\` (several conditions go in ONE \`where:\`, comma-separated, not chained). If the filter sits under a wrapper or refines a derived measure, pre-aggregate the bare filtered aggregate as its own measure and do the rest in a view, or remove \`#@ preaggregate\` from it.`,
         );
      }
      aggregateRoot = root.kids.e;
   }

   // Two shapes land here and the message serves both: an aggregate FUNCTION
   // not modelled (`stddev(x)` is a `function_call`), and the common case of a
   // measure derived from other measures (`total_margin / total_sales`), whose
   // references are field nodes.
   if (found.aggregates.length === 0) {
      return nonAdditive(
         "no_aggregate_found",
         `Measure \`${name}\` does not compute a single plain aggregate, and only \`sum\`, \`count\`, \`min\` and \`max\` can be re-aggregated from a stored partial. If it is derived from other measures, pre-aggregate those instead and compute this one in a view.`,
      );
   }
   if (found.aggregates.length > 1) {
      return nonAdditive(
         "multiple_aggregates",
         `Measure \`${name}\` combines ${found.aggregates.length} aggregates, and a ratio or difference of aggregates cannot be re-aggregated from a single stored column. Pre-aggregate each aggregate as its own measure and combine them in a view.`,
      );
   }

   // The one aggregate must BE the expression (once a conforming root filter is
   // set aside). If anything wraps it — `* 2`, `coalesce(…)`, a cast — then
   // merging stored partials with a single aggregate is not equivalent to the
   // measure; rewriting the wrapper around the merge is not supported.
   if (aggregateRoot.node !== "aggregate") {
      return nonAdditive(
         "aggregate_not_at_root",
         `Measure \`${name}\` wraps its aggregate in a further expression, so re-aggregating a stored partial would not reproduce it. Pre-aggregate the bare aggregate as its own measure and do the rest in a view.`,
      );
   }

   const fn = aggregateRoot.function;
   if (!isAdditiveAggregate(fn)) {
      const hint =
         fn === "avg"
            ? " Pre-aggregate a `sum` and a `count` instead, and divide them in a view."
            : fn === "distinct"
              ? " A distinct count cannot be merged from per-group partials."
              : "";
      return nonAdditive(
         "unsupported_aggregate",
         `Measure \`${name}\` uses \`${fn}\`, which cannot be re-aggregated from a stored partial; only \`sum\`, \`count\`, \`min\` and \`max\` can.${hint}`,
      );
   }

   return { additive: true, aggregate: fn, reaggregate: REAGGREGATE[fn] };
}
