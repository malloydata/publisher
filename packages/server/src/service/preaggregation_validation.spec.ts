// Real-compile tests for `#@ preaggregate` publish validation.
//
// Every model below COMPILES CLEANLY. That is the point of the file: Malloy has
// no opinion about a `#@` annotation it does not own, so all of these are legal
// Malloy that would silently do nothing, and the only thing standing between an
// author and a rollup they think exists is this validator.
import type { FixedConnectionMap } from "@malloydata/malloy";
import { beforeAll, describe, expect, it } from "bun:test";
import {
   validateModelPreaggregation,
   validateSourcePreaggregation,
   type PreaggregateViolationCode,
} from "./preaggregation_validation";
import {
   duckdbTestConnections,
   loadTestModel,
} from "./incremental_test_harness";

let connections: FixedConnectionMap;

beforeAll(() => {
   ({ connections } = duckdbTestConnections());
});

const COLUMNS = `SELECT 1 AS order_id, 10 AS amount, 'A' AS category,
       TIMESTAMP '2024-01-01 00:00:00' AS order_time, DATE '2024-01-01' AS order_date`;

/** Compile a model and return its `contents` map. */
async function compileContents(body: string): Promise<Record<string, unknown>> {
   const text = `##! experimental { persistence composite_sources }
source: other is duckdb.sql("""SELECT 1 AS order_id, 2 AS qty""")
source: s is duckdb.sql("""${COLUMNS}""") extend {
${body}
}
`;
   const compiled = await loadTestModel(connections, text).getModel();
   // eslint-disable-next-line @typescript-eslint/no-explicit-any
   return (compiled as any)._modelDef.contents;
}

/** Validate just source `s` from a compiled extend body. */
async function validate(body: string) {
   const contents = await compileContents(body);
   return validateSourcePreaggregation(
      "s",
      contents["s"] as Parameters<typeof validateSourcePreaggregation>[1],
   );
}

/** The codes reported for a body, in order. */
async function codesFor(body: string): Promise<PreaggregateViolationCode[]> {
   return (await validate(body)).map((v) => v.code);
}

const GOOD_MEASURE = `  #@ preaggregate grain="category"
  measure: revenue is amount.sum()`;

describe("a measure may be rolled up at several grains", () => {
   // Each `#@ preaggregate` line is its own rollup. Not a convenience: a rollup at
   // the combined grain covers the same queries, but a combined grain has roughly
   // the product of its dimensions' cardinalities, so it can approach the base
   // table and save nothing where either grain alone is small.
   it("two annotations on one measure are both accepted", async () => {
      expect(
         await codesFor(`  dimension: order_day is order_time.day
  #@ preaggregate grain="category"
  #@ preaggregate grain="order_day"
  measure: revenue is amount.sum()`),
      ).toEqual([]);
   });

   it("the combined grain is valid too — it is a third choice, not a rival", async () => {
      expect(
         await codesFor(`  dimension: order_day is order_time.day
  #@ preaggregate grain="category, order_day"
  measure: revenue is amount.sum()`),
      ).toEqual([]);
   });

   it("each grain is validated independently, so a good one cannot vouch for a bad one", async () => {
      const violations = await validate(`  #@ preaggregate grain="category"
  #@ preaggregate grain="nope"
  measure: revenue is amount.sum()`);
      expect(violations.map((v) => v.code)).toEqual([
         "unknown_grain_dimension",
      ]);
      expect(violations[0].message).toContain("`nope`");
   });

   it("every broken grain is reported, not just the first", async () => {
      expect(
         await codesFor(`  #@ preaggregate grain="nope"
  #@ preaggregate grain="also_nope"
  measure: revenue is amount.sum()`),
      ).toEqual(["unknown_grain_dimension", "unknown_grain_dimension"]);
   });

   it("a non-additive measure is still refused once, not once per grain", async () => {
      // The measure is the problem, not the grains, so repeating the complaint per
      // grain would just make it harder to read.
      expect(
         await codesFor(`  #@ preaggregate grain="category"
  #@ preaggregate grain="order_id"
  measure: avg_amount is amount.avg()`),
      ).toEqual(["non_additive_measure"]);
   });

   it("two DIFFERENT measures may each carry their own annotations", async () => {
      expect(
         await codesFor(`  #@ preaggregate grain="category"
  measure: revenue is amount.sum()
  #@ preaggregate grain="category"
  measure: order_count is count()`),
      ).toEqual([]);
   });
});

describe("valid declarations produce no violations", () => {
   it("a measure with a grain of a real dimension passes", async () => {
      expect(await codesFor(GOOD_MEASURE)).toEqual([]);
   });

   it("a raw column is a usable grain dimension", async () => {
      // Columns off the source carry no expressionType at all, so treating
      // "absent" as unknown would reject the most ordinary grain there is.
      expect(
         await codesFor(`  #@ preaggregate grain="order_id"
  measure: revenue is amount.sum()`),
      ).toEqual([]);
   });

   it("a declared truncation DIMENSION is a usable grain", async () => {
      // The supported way to pre-aggregate by time: name the truncation as a
      // dimension on the source, then put that name in the grain. See
      // preaggregation_grain_semantics.spec.ts for why the inline form is not.
      expect(
         await codesFor(`  dimension: order_day is order_time.day
  #@ preaggregate grain="order_day"
  measure: revenue is amount.sum()`),
      ).toEqual([]);
   });

   it("an unannotated model produces nothing", async () => {
      expect(
         await codesFor(`  measure: revenue is amount.sum()
  dimension: cat is category`),
      ).toEqual([]);
   });

   it("a negated annotation produces nothing", async () => {
      // Turning the feature off for a measure must not fail the package.
      expect(
         await codesFor(`  #@ preaggregate grain="category"
  #@ -preaggregate
  measure: revenue is amount.sum()`),
      ).toEqual([]);
   });
});

describe("misplaced annotations are refused", () => {
   // The user-visible gap this module exists for: each of these compiles, and
   // each would otherwise be read by nothing.
   it("on a dimension", async () => {
      const violations = await validate(`  #@ preaggregate grain="category"
  dimension: cat is category`);
      expect(violations.map((v) => v.code)).toEqual(["misplaced_on_dimension"]);
      expect(violations[0].message).toContain("`cat`");
      expect(violations[0].message).toContain("dimension, not a measure");
   });

   it("on a view", async () => {
      const violations = await validate(`  #@ preaggregate grain="category"
  view: by_cat is { group_by: category }`);
      expect(violations.map((v) => v.code)).toEqual(["misplaced_on_view"]);
      expect(violations[0].message).toContain("`by_cat`");
   });

   it("on a join", async () => {
      const violations = await validate(`  #@ preaggregate grain="category"
  join_one: other_j is other on order_id = other_j.order_id`);
      // Only the misplacement: a `join_one` is a permitted thing for a
      // pre-aggregated source to have, so nothing else is wrong here.
      expect(violations.map((v) => v.code)).toEqual(["misplaced_on_join"]);
      expect(violations[0].message).toContain("`other_j`");
   });

   it("on a source", async () => {
      const text = `##! experimental.persistence
#@ preaggregate grain="category"
source: s is duckdb.sql("""${COLUMNS}""") extend {
  measure: revenue is amount.sum()
}
`;
      const compiled = await loadTestModel(connections, text).getModel();
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const contents = (compiled as any)._modelDef.contents;
      const violations = validateModelPreaggregation(contents);
      expect(violations.map((v) => v.code)).toEqual(["misplaced_on_source"]);
      expect(violations[0].message).toContain("`s`");
      expect(violations[0].fieldName).toBeUndefined();
   });

   it("a dimension is told where the annotation actually goes", async () => {
      // A rejection is only useful if it says what to do instead.
      const violations = await validate(`  #@ preaggregate grain="category"
  dimension: cat is category`);
      expect(violations[0].message).toMatch(/grain/);
      expect(violations[0].message).toContain("annotate the measure");
   });
});

describe("unusable declarations on a measure are refused", () => {
   it("no grain", async () => {
      const violations = await validate(`  #@ preaggregate
  measure: revenue is amount.sum()`);
      expect(violations.map((v) => v.code)).toEqual(["missing_grain"]);
      expect(violations[0].message).toContain("`revenue`");
   });

   it("empty grain", async () => {
      expect(
         await codesFor(`  #@ preaggregate grain=""
  measure: revenue is amount.sum()`),
      ).toEqual(["empty_grain"]);
   });

   it("a non-additive measure", async () => {
      const violations = await validate(`  #@ preaggregate grain="category"
  measure: avg_amount is amount.avg()`);
      expect(violations.map((v) => v.code)).toEqual(["non_additive_measure"]);
      expect(violations[0].message).toContain("`avg_amount`");
   });

   it("a non-additive measure ALSO gets its grain checked", async () => {
      // Otherwise fixing the measure reveals a second failure on the next
      // publish, which is a bad loop to put someone in.
      expect(
         await codesFor(`  #@ preaggregate grain="nope"
  measure: avg_amount is amount.avg()`),
      ).toEqual(["non_additive_measure", "unknown_grain_dimension"]);
   });
});

describe("grain dimensions must resolve", () => {
   it("a dimension that does not exist", async () => {
      const violations = await validate(`  #@ preaggregate grain="not_a_field"
  measure: revenue is amount.sum()`);
      expect(violations.map((v) => v.code)).toEqual([
         "unknown_grain_dimension",
      ]);
      expect(violations[0].message).toContain("`not_a_field`");
      expect(violations[0].message).toContain("`s`");
   });

   it("a grain naming a measure", async () => {
      const violations = await validate(`  #@ preaggregate grain="revenue"
  measure: revenue is amount.sum()`);
      expect(violations.map((v) => v.code)).toEqual([
         "grain_dimension_is_measure",
      ]);
      expect(violations[0].message).toContain("not the values it aggregates");
   });

   it("a grain naming a view", async () => {
      expect(
         await codesFor(`  #@ preaggregate grain="by_cat"
  measure: revenue is amount.sum()
  view: by_cat is { group_by: category }`),
      ).toEqual(["grain_dimension_is_view"]);
   });

   it("a grain reaching through a join, even a permitted join_one", async () => {
      // The grain rule is independent of the source-level fan-out gate, and it
      // holds for the §3e reason rather than the fan-out one: a query writing
      // `other_j.qty` references `other_j`, which the rollup member does not have,
      // so no rollup could serve it however the stored column were named.
      const violations = await validate(`  #@ preaggregate grain="other_j.qty"
  measure: revenue is amount.sum()
  join_one: other_j is other on order_id = other_j.order_id`);
      expect(violations.map((v) => v.code)).toEqual([
         "grain_path_through_join",
      ]);
      expect(violations[0].message).toContain("`other_j`");
   });

   it("every bad dimension in one grain is reported", async () => {
      expect(
         await codesFor(`  #@ preaggregate grain="nope_one, nope_two, category"
  measure: revenue is amount.sum()`),
      ).toEqual(["unknown_grain_dimension", "unknown_grain_dimension"]);
   });
});

describe("an inline time truncation is refused, however well-formed", () => {
   // preaggregation_grain_semantics.spec.ts establishes why: stored under the
   // base's own name a truncated column answers untruncated queries with wrong
   // rows, and stored under any other name nothing ever references it. Neither is
   // shippable, so the grain must name a dimension the source declares.

   it("a perfectly valid truncation is still refused, and the message says what to add", async () => {
      const violations =
         await validate(`  #@ preaggregate grain="order_time.day"
  measure: revenue is amount.sum()`);
      expect(violations.map((v) => v.code)).toEqual([
         "grain_truncation_expression",
      ]);
      // The fix has to be copy-pasteable, or an author reading it has to rederive
      // the thing we just worked out.
      expect(violations[0].message).toContain(
         "dimension: order_time_day is order_time.day",
      );
      expect(violations[0].message).toContain('grain="order_time_day"');
   });

   it("a truncation on a non-temporal dimension gets its own message", async () => {
      const violations = await validate(`  #@ preaggregate grain="category.day"
  measure: revenue is amount.sum()`);
      expect(violations.map((v) => v.code)).toEqual([
         "truncation_on_non_temporal",
      ]);
      expect(violations[0].message).toContain("string");
      // Suggesting `dimension: category_day is category.day` would be nonsense.
      expect(violations[0].message).not.toContain("dimension:");
   });

   it("a multi-part path is refused rather than guessed at", async () => {
      expect(
         await codesFor(`  #@ preaggregate grain="order_time.day.week"
  measure: revenue is amount.sum()`),
      ).toEqual(["grain_truncation_expression"]);
   });

   it("the unit is not inspected, because the form is refused before it matters", async () => {
      // A nonsense unit and a valid one land in the same place. Asserted so a
      // later change that reintroduces unit checking has to face this rule again.
      expect(
         await codesFor(`  #@ preaggregate grain="order_time.fortnight"
  measure: revenue is amount.sum()`),
      ).toEqual(["grain_truncation_expression"]);
      expect(
         await codesFor(`  #@ preaggregate grain="order_date.hour"
  measure: revenue is amount.sum()`),
      ).toEqual(["grain_truncation_expression"]);
   });
});

describe("v1 scope restriction: fan-out joins only", () => {
   // The rule exists because a rollup summing a measure computed over duplicated
   // rows returns a wrong total. That is a property of fan-out, so the gate is
   // scoped to fan-out: refusing `join_one` too would have excluded every
   // candidate in `examples/storefront`.

   it("a join_one on the base source is PERMITTED", async () => {
      expect(
         await codesFor(`  #@ preaggregate grain="category"
  measure: revenue is amount.sum()
  join_one: other_j is other on order_id = other_j.order_id`),
      ).toEqual([]);
   });

   it("a measure aggregating THROUGH a join_one is permitted too", async () => {
      // The classifier already calls this additive, and with the gate narrowed the
      // two rules finally agree. Synthesis needs nothing extra: the rollup computes
      // the base's own measure by name, so the join comes along with it.
      expect(
         await codesFor(`  #@ preaggregate grain="category"
  measure: total_qty is other_j.qty.sum()
  join_one: other_j is other on order_id = other_j.order_id`),
      ).toEqual([]);
   });

   it("a join_many is refused, reported once for the source", async () => {
      const violations = await validate(`  #@ preaggregate grain="category"
  measure: revenue is amount.sum()
  #@ preaggregate grain="category"
  measure: units is amount.max()
  join_many: other_j is other on order_id = other_j.order_id`);
      // Two annotated measures, but one source-level finding.
      expect(violations.map((v) => v.code)).toEqual([
         "base_source_has_fanout_join",
      ]);
      expect(violations[0].fieldName).toBeUndefined();
      expect(violations[0].message).toContain("`other_j`");
   });

   it("a join_cross is refused", async () => {
      expect(
         await codesFor(`  #@ preaggregate grain="category"
  measure: revenue is amount.sum()
  join_cross: other_j is other`),
      ).toEqual(["base_source_has_fanout_join"]);
   });

   it("a fan-out join is refused even alongside a harmless join_one", async () => {
      // The predicate has to name the offender, not just notice that joins exist.
      const violations = await validate(`  #@ preaggregate grain="category"
  measure: revenue is amount.sum()
  join_one: safe_j is other on order_id = safe_j.order_id
  join_many: risky_j is other on order_id = risky_j.order_id`);
      expect(violations.map((v) => v.code)).toEqual([
         "base_source_has_fanout_join",
      ]);
      expect(violations[0].message).toContain("`risky_j`");
      expect(violations[0].message).not.toContain("`safe_j`");
   });

   it("fan-out joins on a source nobody annotated are fine", async () => {
      // The restriction must not punish a model that never asked for the feature.
      expect(
         await codesFor(`  measure: revenue is amount.sum()
  join_many: other_j is other on order_id = other_j.order_id`),
      ).toEqual([]);
   });
});

describe("every message names its target and offers a fix", () => {
   const bodies = [
      `  #@ preaggregate grain="category"\n  dimension: cat is category`,
      `  #@ preaggregate grain="category"\n  view: by_cat is { group_by: category }`,
      `  #@ preaggregate\n  measure: revenue is amount.sum()`,
      `  #@ preaggregate grain=""\n  measure: revenue is amount.sum()`,
      `  #@ preaggregate grain="category"\n  measure: avg_amount is amount.avg()`,
      `  #@ preaggregate grain="not_a_field"\n  measure: revenue is amount.sum()`,
      `  #@ preaggregate grain="revenue"\n  measure: revenue is amount.sum()`,
      `  #@ preaggregate grain="category.day"\n  measure: revenue is amount.sum()`,
      `  #@ preaggregate grain="order_date.hour"\n  measure: revenue is amount.sum()`,
   ];

   it("is a full sentence, mentions preaggregate or a grain, and ends cleanly", async () => {
      for (const body of bodies) {
         const violations = await validate(body);
         expect(violations.length).toBeGreaterThan(0);
         for (const violation of violations) {
            expect(violation.message.length).toBeGreaterThan(50);
            expect(violation.message).toMatch(/\.$/);
            expect(violation.message.toLowerCase()).toMatch(
               /preaggregate|grain|measure/,
            );
            expect(violation.sourceName).toBe("s");
         }
      }
   });
});
