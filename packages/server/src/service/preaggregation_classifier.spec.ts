// Real-compiler tests for the additivity classifier. Every measure below is
// COMPILED by the pinned @malloydata/malloy and classified from the IR it
// actually emits — not from a hand-built node — because the whole risk this
// module carries is that the IR does not look the way we assume. A pin bump that
// reshapes a measure's expression must fail here.
//
// The corpus is deliberately lopsided: four additive cases and a long tail of
// things that must all land non-additive. That is the handoff's "surprising
// expression" corpus, and it is the half that protects against a plausible wrong
// number.
import type { FixedConnectionMap } from "@malloydata/malloy";
import { beforeAll, describe, expect, it } from "bun:test";
import {
   classifyMeasureAdditivity,
   type AdditiveAggregate,
   type ClassifiableMeasure,
   type NonAdditiveReason,
   type ReaggregateFunction,
} from "./preaggregation_classifier";
import {
   duckdbTestConnections,
   loadTestModel,
} from "./incremental_test_harness";

/**
 * Measures compiled once and looked up by name. A join is present so a
 * through-a-join aggregate can be classified on real IR (it carries a
 * `structPath`, which must NOT disqualify it — see the `sum` case).
 */
const MODEL = `##! experimental.persistence
source: items is duckdb.sql("""SELECT 1 AS order_id, 5 AS price""")

source: s is duckdb.sql("""
  SELECT 1 AS order_id, 10 AS amount, 'A' AS category, DATE '2024-01-01' AS d
""") extend {
  join_many: items on order_id = items.order_id

  // --- must be additive ---
  measure: m_sum is amount.sum()
  measure: m_count is count()
  measure: m_min is amount.min()
  measure: m_max is amount.max()
  measure: m_sum_joined is items.price.sum()
  measure: m_filtered is amount.sum() { where: category = 'A' }
  measure: m_filtered_count is count() { where: category = 'A' }
  measure: m_filtered_joined is items.price.sum() { where: category = 'A' }

  // --- must all be non-additive ---
  measure: m_avg is amount.avg()
  measure: m_distinct is count(amount)
  measure: m_filtered_avg is amount.avg() { where: category = 'A' }
  measure: m_arith is amount.sum() * 2
  measure: m_filtered_arith is m_arith { where: category = 'A' }
  measure: m_ratio is amount.sum() / count()
  measure: m_all is all(amount.sum())
  measure: m_stddev is stddev(amount)
  measure: m_coalesce is coalesce(amount.sum(), 0)
  measure: m_cast is amount.sum()::string
  measure: m_nested_filter is coalesce(amount.sum() { where: category = 'A' }, 0)
  measure: m_diff is amount.max() - amount.min()
  measure: m_ratio_of_measures is m_sum / m_count

  dimension: dim_scalar is amount * 2
}
`;

let lookup: (name: string) => ClassifiableMeasure;

beforeAll(async () => {
   const { connections }: { connections: FixedConnectionMap } =
      duckdbTestConnections();
   const compiled = await loadTestModel(connections, MODEL).getModel();
   // eslint-disable-next-line @typescript-eslint/no-explicit-any
   const source = (compiled as any)._modelDef.contents["s"];
   lookup = (name: string) => {
      const field = source.fields.find(
         (f: { name: string; as?: string }) => (f.as ?? f.name) === name,
      );
      if (!field) throw new Error(`no field ${name} in the compiled source`);
      return field as ClassifiableMeasure;
   };
});

describe("additivity classifier: the additive cases", () => {
   // The table from the handoff, with the merge function each one implies. The
   // count row is the one that matters: it merges with SUM.
   const cases: [string, AdditiveAggregate, ReaggregateFunction][] = [
      ["m_sum", "sum", "sum"],
      ["m_count", "count", "sum"],
      ["m_min", "min", "min"],
      ["m_max", "max", "max"],
      // Filtered on the aggregate directly: the measure MEANS the filtered
      // value, its partial is computed from the measure by name, and a
      // row-level filter commutes with partitioned re-aggregation. The filtered
      // count is the one to watch: its partial counts MATCHING rows and still
      // merges with sum.
      ["m_filtered", "sum", "sum"],
      ["m_filtered_count", "count", "sum"],
   ];

   for (const [name, aggregate, reaggregate] of cases) {
      it(`${name}: ${aggregate} stores a partial merged with ${reaggregate}`, () => {
         const result = classifyMeasureAdditivity(lookup(name));
         expect(result).toEqual({ additive: true, aggregate, reaggregate });
      });
   }

   it("count merges with sum, never with count", () => {
      // Stated as its own test because it is the entire reason this module
      // exists: re-running count() over rollup rows returns the ROLLUP ROW
      // COUNT, which compiles, runs, and is wrong.
      const result = classifyMeasureAdditivity(lookup("m_count"));
      expect(result).toMatchObject({ additive: true, reaggregate: "sum" });
      expect(result).not.toMatchObject({ reaggregate: "count" });
   });

   it("a filtered aggregate through a join is still additive", () => {
      // The two accepted wrinkles compose: the filter sits at the root and the
      // aggregate inside it carries a structPath. Neither disqualifies alone,
      // so together they must not either.
      expect(classifyMeasureAdditivity(lookup("m_filtered_joined"))).toEqual({
         additive: true,
         aggregate: "sum",
         reaggregate: "sum",
      });
   });

   it("an aggregate through a join is still additive", () => {
      // `items.price.sum()` carries a structPath. Rejecting it would exclude
      // most real models; fan-out is the rollup's own compile to handle, and
      // summing correct per-group partials stays correct.
      expect(classifyMeasureAdditivity(lookup("m_sum_joined"))).toEqual({
         additive: true,
         aggregate: "sum",
         reaggregate: "sum",
      });
   });
});

describe("additivity classifier: the surprising-expression corpus", () => {
   // Every one of these must be non-additive. The reason is asserted too, since
   // it becomes the text of a publish-time 400 someone has to act on.
   const cases: [string, NonAdditiveReason][] = [
      ["m_avg", "unsupported_aggregate"],
      ["m_distinct", "unsupported_aggregate"],
      // A filter does not launder an unmergeable aggregate: the shape is
      // accepted, then the function inside it is judged on its own.
      ["m_filtered_avg", "unsupported_aggregate"],
      ["m_arith", "aggregate_not_at_root"],
      // A filter on a WRAPPED aggregate is still refused: the additivity
      // argument covers a filter applied directly to the one aggregate, and
      // nothing else.
      ["m_filtered_arith", "filtered_aggregate"],
      ["m_ratio", "multiple_aggregates"],
      ["m_all", "ungrouped_aggregate"],
      ["m_stddev", "no_aggregate_found"],
      ["m_coalesce", "aggregate_not_at_root"],
      ["m_cast", "aggregate_not_at_root"],
      ["m_nested_filter", "filtered_aggregate"],
      ["m_diff", "multiple_aggregates"],
      // A ratio of NAMED measures, which is the common real-world shape — four of
      // them in examples/storefront. Its references compile to field nodes, not
      // aggregate nodes, so it lands as "no aggregate found" rather than
      // "multiple aggregates" despite looking like the latter.
      ["m_ratio_of_measures", "no_aggregate_found"],
      ["dim_scalar", "not_an_aggregate"],
   ];

   for (const [name, reason] of cases) {
      it(`${name} is non-additive (${reason})`, () => {
         const result = classifyMeasureAdditivity(lookup(name));
         expect(result.additive).toBe(false);
         if (result.additive) return;
         expect(result.reason).toBe(reason);
      });
   }

   it("every non-additive detail names the measure and says what to do", () => {
      // The detail is user-facing error text, not a log line, so it is asserted
      // as a contract rather than left to drift.
      for (const [name] of cases) {
         const result = classifyMeasureAdditivity(lookup(name));
         expect(result.additive).toBe(false);
         if (result.additive) continue;
         expect(result.detail).toContain(`\`${name}\``);
         expect(result.detail.length).toBeGreaterThan(40);
         expect(result.detail).toMatch(/\.$/);
      }
   });

   it("a ratio of measures is told to pre-aggregate its parts instead", () => {
      // The message matters more than usual here: the author is looking at two
      // measures that plainly ARE aggregates, so "no plain aggregate" alone
      // would send them the wrong way.
      const result = classifyMeasureAdditivity(lookup("m_ratio_of_measures"));
      expect(result.additive).toBe(false);
      if (result.additive) return;
      expect(result.detail).toContain("derived from other measures");
      expect(result.detail).toContain("view");
   });

   it("avg is told to decompose, and it is NOT quietly decomposed here", () => {
      // avg decomposition is a later phase. The classifier's job is to refuse it
      // and say what to do instead.
      const result = classifyMeasureAdditivity(lookup("m_avg"));
      expect(result.additive).toBe(false);
      if (result.additive) return;
      expect(result.detail).toContain("sum");
      expect(result.detail).toContain("count");
   });
});

describe("additivity classifier: fails closed on IR it cannot read", () => {
   // These are hand-built because they represent IR the current compiler does
   // not produce from a source measure — an unreachable branch today is exactly
   // the branch a pin bump makes reachable.
   it("a measure with no expression is non-additive", () => {
      expect(
         classifyMeasureAdditivity({ name: "m", expressionType: "aggregate" }),
      ).toMatchObject({ additive: false, reason: "no_expression" });
   });

   it("an untyped measure is non-additive", () => {
      expect(classifyMeasureAdditivity({ name: "m" })).toMatchObject({
         additive: false,
         reason: "not_an_aggregate",
      });
   });

   it("an analytic measure is non-additive", () => {
      expect(
         classifyMeasureAdditivity({
            name: "m",
            expressionType: "aggregate_analytic",
            e: { node: "aggregate", function: "sum", e: { node: "" } },
         }),
      ).toMatchObject({ additive: false, reason: "analytic" });
   });

   it("a grouped_by measure is non-additive", () => {
      expect(
         classifyMeasureAdditivity({
            name: "m",
            expressionType: "aggregate",
            requiresGroupBy: [{ path: ["category"] }],
            e: { node: "aggregate", function: "sum", e: { node: "" } },
         }),
      ).toMatchObject({ additive: false, reason: "requires_group_by" });
   });

   it("an unknown future aggregate function is non-additive, not assumed", () => {
      expect(
         classifyMeasureAdditivity({
            name: "m",
            expressionType: "aggregate",
            e: {
               node: "aggregate",
               // A function outside the union on purpose: the point is that an
               // aggregate this classifier has never heard of is refused rather
               // than assumed additive.
               // eslint-disable-next-line @typescript-eslint/no-explicit-any
               function: "median" as any,
               e: { node: "" },
            },
         }),
      ).toMatchObject({ additive: false, reason: "unsupported_aggregate" });
   });

   it("a SECOND filter anywhere in the tree is refused", () => {
      // The second filteredExpr here hides inside the aggregate's own argument,
      // where the root checks cannot see it: the root IS a filteredExpr, its
      // child IS an aggregate, its conditions ARE scalar. Only the exactly-one
      // count refuses it — remove that guard and this shape sails through as
      // conforming. IR we have not seen from source (the compiler merges
      // refinements into one filterList), which is exactly why it is pinned on
      // a built node: a pin bump that starts emitting it must land here, be
      // proven additive, and only then be accepted.
      expect(
         classifyMeasureAdditivity({
            name: "m",
            expressionType: "aggregate",
            e: {
               node: "filteredExpr",
               kids: {
                  e: {
                     node: "aggregate",
                     function: "sum",
                     e: {
                        node: "filteredExpr",
                        kids: { e: { node: "" }, filterList: [] },
                     },
                  },
                  filterList: [],
               },
               // eslint-disable-next-line @typescript-eslint/no-explicit-any
            } as any,
         }),
      ).toMatchObject({ additive: false, reason: "filtered_aggregate" });
   });

   it("a NON-SCALAR filter condition is refused", () => {
      // A condition the compiler types as anything but scalar is not a
      // row-level filter, and row-level is the whole additivity argument: an
      // aggregate or analytic condition selects rows by a value that depends
      // on grouping, so its partial is not a partial of the query's answer.
      // Unreachable from source today (the language rejects them), so pinned
      // on a built node.
      expect(
         classifyMeasureAdditivity({
            name: "m",
            expressionType: "aggregate",
            e: {
               node: "filteredExpr",
               kids: {
                  e: { node: "aggregate", function: "sum", e: { node: "" } },
                  filterList: [
                     { node: "filterCondition", expressionType: "aggregate" },
                  ],
               },
               // eslint-disable-next-line @typescript-eslint/no-explicit-any
            } as any,
         }),
      ).toMatchObject({ additive: false, reason: "filtered_aggregate" });
   });

   it("a hand-built CONFORMING filtered aggregate is additive", () => {
      // The acceptance itself, on a minimal node, so the four conditions above
      // are each provably load-bearing: flip any one and exactly one of these
      // three tests changes its answer.
      expect(
         classifyMeasureAdditivity({
            name: "m",
            expressionType: "aggregate",
            e: {
               node: "filteredExpr",
               kids: {
                  e: { node: "aggregate", function: "sum", e: { node: "" } },
                  filterList: [
                     { node: "filterCondition", expressionType: "scalar" },
                  ],
               },
               // eslint-disable-next-line @typescript-eslint/no-explicit-any
            } as any,
         }),
      ).toMatchObject({ additive: true, aggregate: "sum", reaggregate: "sum" });
   });

   it("an empty ungroupings array does not disqualify a measure", () => {
      // The trap: `ungroupings` is `[]` (not undefined) on any measure that
      // mentions a function, so a truthiness check would reject valid measures.
      expect(
         classifyMeasureAdditivity({
            name: "m",
            expressionType: "aggregate",
            ungroupings: [],
            requiresGroupBy: [],
            e: { node: "aggregate", function: "sum", e: { node: "" } },
         }),
      ).toMatchObject({ additive: true });
   });
});
