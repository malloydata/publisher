/**
 * Additivity classification for `#@ preaggregate` measures: can this measure be
 * computed at a coarse grain, stored, and then correctly re-aggregated to a
 * coarser one?
 *
 * This module exists because of one specific hazard. A rollup stores
 * `count()` per grain; a query at a coarser grain that re-ran `count()` over
 * those rollup rows would return THE NUMBER OF ROLLUP ROWS. It compiles, it
 * runs, and it is wrong — a plausible wrong number, which is the single failure
 * mode pre-aggregation must never produce. So a measure is pre-aggregated only
 * when this module can positively prove how to merge its stored partial, and
 * `count` merges with `sum`, never with `count`.
 *
 * ## Fail closed
 *
 * Anything not positively classified is non-additive. The classifier is
 * deliberately narrow: the measure must be ONE bare aggregate, at the root of
 * its expression, drawn from `sum`, `count`, `min`, `max`. `avg` is excluded in
 * v1 — decomposing it into sum/count is a later phase, not something to sneak in
 * here.
 *
 * ## This is a publish gate, so a false negative is an outage
 *
 * A measure carrying `#@ preaggregate` that lands non-additive is REFUSED at
 * publish (and fails the package load), rather than being silently omitted from
 * the rollup. That is why every non-additive result carries a `detail` naming
 * the measure and a fix: it becomes the text of a 400 a human has to act on.
 * Widening what counts as additive is safe; narrowing it breaks deployed
 * packages.
 *
 * ## Two traps in the pinned IR (0.0.427), both verified by probe
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
 * The IR coupling here is the price of the feature (preaggregation.md §4.4), so
 * the module is kept small and isolated and its facts are pinned by real-compile
 * tests in `preaggregation_classifier.spec.ts`. A pin bump that changes the IR
 * should fail there rather than quietly change what a rollup stores.
 */

import type { Expr, ExpressionType } from "@malloydata/malloy";

/** The aggregate functions whose partials can be merged. */
export type AdditiveAggregate = "sum" | "count" | "min" | "max";

/** The aggregate applied to the STORED partial column to merge it. */
export type ReaggregateFunction = "sum" | "min" | "max";

/**
 * How each additive aggregate merges. `count: "sum"` is the whole point of this
 * module — see the header. Re-reading a stored count with `count` is the bug
 * this table exists to make impossible to write by accident.
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

export type AdditivityResult =
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
   filtered: boolean;
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
         found.filtered = true;
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
      filtered: false,
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
   // A filtered aggregate is excluded even though its partial would often merge
   // correctly: the rollup would silently store the FILTERED value under the
   // measure's name, and a query that expects the unfiltered one gets a
   // plausible wrong number. Correct support means storing both, which is not v1.
   if (found.filtered) {
      return nonAdditive(
         "filtered_aggregate",
         `Measure \`${name}\` applies a filter to its aggregate, which this version cannot pre-aggregate. Define the aggregate as its own measure and apply the filter in a view, or remove \`#@ preaggregate\` from it.`,
      );
   }

   // Two quite different shapes land here, and the message has to serve both: an
   // aggregate FUNCTION this module does not model (`stddev(x)` is a
   // `function_call`, not an `aggregate` node), and a measure derived from other
   // measures (`total_margin / total_sales`), whose references are field nodes
   // rather than aggregates. The second is very common — a real model in
   // `examples/storefront` has four — and telling its author there is "no plain
   // aggregate" sends them looking at the wrong thing, since the measures it
   // divides plainly are aggregates.
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

   // The one aggregate must BE the expression. If anything wraps it — `* 2`,
   // `coalesce(…)`, a cast — then merging stored partials with a single
   // aggregate is not equivalent to the measure, and rewriting the wrapper
   // around the merge is not v1.
   if (measure.e.node !== "aggregate") {
      return nonAdditive(
         "aggregate_not_at_root",
         `Measure \`${name}\` wraps its aggregate in a further expression, so re-aggregating a stored partial would not reproduce it. Pre-aggregate the bare aggregate as its own measure and do the rest in a view.`,
      );
   }

   const fn = measure.e.function;
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
