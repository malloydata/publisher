// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Tests for the v1 `#@ preaggregate` planner: the plans a source's lines
// describe. (The composite text it once emitted is retired with the v1
// cutover, 6g; a v1 rollup is the rollup leg's.)
import type { FixedConnectionMap } from "@malloydata/malloy";
import { beforeAll, describe, expect, it } from "bun:test";
import {
   duckdbTestConnections,
   loadTestModel,
} from "../../../test_helpers/incremental_test_harness";
import {
   planSourcePreaggregation,
   rollupSourceName,
   type RollupPlan,
} from "./preaggregation_synthesis";
import type { ValidatableSource } from "./preaggregation_validation";

let connections: FixedConnectionMap;

beforeAll(() => {
   ({ connections } = duckdbTestConnections());
});

const ROWS = `SELECT * FROM (VALUES
    (10, DATE '2024-01-01', 'A'),
    (20, DATE '2024-01-01', 'B'),
    (30, DATE '2024-01-02', 'A')
  ) AS t(amount, order_date, category)`;

/** An author model with `body` inside `extend {}` on source `orders`. */
function authorModel(body: string): string {
   return `##! experimental { persistence composite_sources }
source: orders is duckdb.sql("""${ROWS}""") extend {
${body}
}
`;
}

/** Compile an author model and return source `orders` from its contents. */
async function compileOrders(body: string): Promise<ValidatableSource> {
   const compiled = await loadTestModel(
      connections,
      authorModel(body),
   ).getModel();
   // eslint-disable-next-line @typescript-eslint/no-explicit-any
   return (compiled as any)._modelDef.contents["orders"] as ValidatableSource;
}

async function planFor(body: string): Promise<RollupPlan[]> {
   return planSourcePreaggregation("orders", await compileOrders(body));
}

describe("the generated name is a single unqualified identifier", () => {
   // The name is also the rollup's physical table name — the build self-assigns
   // it, there being no `name=` — so a consumer reading a dotted name as
   // schema-qualified would refuse it or address the wrong table. Pinned here
   // rather than left to the publish gate that refuses dotted grains, so the
   // property belongs to the name instead of to a gate elsewhere agreeing to hold.
   it("has no dot, for a legal grain or an illegal one", () => {
      expect(rollupSourceName("orders", ["category"])).toBe(
         "orders__preagg__category__edb2cd3b",
      );
      expect(rollupSourceName("orders", ["category", "d"])).not.toContain(".");
      // Refused at publish and fatal at load, so it cannot reach a build plan —
      // asserted anyway, because "cannot reach" is a claim about other code.
      expect(rollupSourceName("orders", ["d.month"])).not.toContain(".");
   });

   it("a folded dot still does not collide with a real underscore", () => {
      // The digest is over the RAW dimensions, so folding cannot merge two grains
      // that differ only where the dot was.
      expect(rollupSourceName("orders", ["a.b"])).not.toBe(
         rollupSourceName("orders", ["a_b"]),
      );
   });

   it("is stable across everything but the base and the grain", () => {
      // Not the connection, and not the destination: neither is an input. The
      // CONTENT address (sourceEntityId) folds the connection digest; the name
      // does not, so it is stable per (base, grain) alone.
      expect(rollupSourceName("orders", ["category"])).toBe(
         rollupSourceName("orders", ["category"]),
      );
   });
});

describe("the planner never plans what the source hides", () => {
   // These two skips ARE the access control, not a tidy agreement with the
   // validator. A rollup stores its grain and each measure's partial, and the
   // serve path rebinds that table under the base's name with the stored columns
   // declared — nothing on that path consults the author's model, so a field
   // hidden on the source is not hidden on the rollup.
   //
   // Publish and load refuse these too, but a refusal is a message. What
   // guarantees nothing is BUILT, and therefore that nothing can be served, is
   // that no plan exists — which is what these assert. The measure case matters
   // most in the sequence that produced it: a measure is public, the rollup
   // builds, the author then marks it private. The table already exists at that
   // point, and the serve leg matches on the PLAN, so the plan going away is what
   // takes the table out of service.
   const withAccess = async (body: string, include: string) => {
      const text = `##! experimental { persistence composite_sources access_modifiers }
source: orders is duckdb.sql("""${ROWS}""") extend {
${body}
} include {
${include}
}
`;
      const compiled = await loadTestModel(connections, text).getModel();
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const contents = (compiled as any)._modelDef.contents;
      return planSourcePreaggregation(
         "orders",
         contents["orders"] as ValidatableSource,
      );
   };

   it("plans nothing for a hidden MEASURE", async () => {
      expect(
         await withAccess(
            `  measure:\n    #@ preaggregate grain="category"\n    total is sum(amount)`,
            "  public: category, amount, order_date\n  private: total",
         ),
      ).toEqual([]);
   });

   it("plans nothing for a hidden GRAIN DIMENSION", async () => {
      // The asymmetric half: a measure is a field the loop already visits, while
      // a grain dimension is a STRING that has to be resolved back to its field
      // before its modifier can be read. Missing this left the grain case guarded
      // only by the load gate, where the measure case was guarded three ways.
      expect(
         await withAccess(
            `  measure:\n    #@ preaggregate grain="category"\n    total is sum(amount)`,
            "  public: total, amount, order_date\n  private: category",
         ),
      ).toEqual([]);
   });

   it("plans normally when everything the rollup touches is public", async () => {
      const plans = await withAccess(
         `  measure:\n    #@ preaggregate grain="category"\n    total is sum(amount)`,
         "  public: *",
      );
      expect(plans).toHaveLength(1);
      expect(plans[0].grainDimensions).toEqual(["category"]);
   });

   it("a grain naming NO field is left alone, which is not the same as hidden", async () => {
      // The validator refuses an unknown grain dimension; the planner has always
      // kept it. Absence must not start reading as hidden, or this skip would
      // silently change what an unknown grain does.
      const plans = await withAccess(
         `  measure:\n    #@ preaggregate grain="nonexistent"\n    total is sum(amount)`,
         "  public: *",
      );
      expect(plans).toHaveLength(1);
   });
});

describe("the plan groups by grain, not by measure", () => {
   it("two measures at one grain share one rollup", async () => {
      const plans = await planFor(`  #@ preaggregate grain="category"
  measure: total is amount.sum()
  #@ preaggregate grain="category"
  measure: order_count is count()`);
      expect(plans).toHaveLength(1);
      expect(plans[0].grainDimensions).toEqual(["category"]);
      expect(plans[0].measures.map((m) => m.name)).toEqual([
         "order_count",
         "total",
      ]);
   });

   it("count's partial merges with SUM, not with count", async () => {
      // The classifier owns this rule; asserted here because the emitter is where
      // getting it wrong would produce a plausible wrong number rather than an
      // error, and this is the last place it can be caught by reading.
      const plans = await planFor(`  #@ preaggregate grain="category"
  measure: order_count is count()
  #@ preaggregate grain="category"
  measure: biggest is amount.max()`);
      const byName = Object.fromEntries(
         plans[0].measures.map((m) => [m.name, m.reaggregate]),
      );
      expect(byName).toEqual({ order_count: "sum", biggest: "max" });
   });

   it("different grains produce different rollups", async () => {
      const plans = await planFor(`  #@ preaggregate grain="category"
  measure: total is amount.sum()
  #@ preaggregate grain="order_date"
  measure: daily is amount.sum()`);
      expect(plans).toHaveLength(2);
      expect(plans.map((p) => p.grainDimensions)).toEqual(
         // Sorted by rollup name, which is derived from the grain.
         expect.arrayContaining([["category"], ["order_date"]]),
      );
   });

   it("a measure that would be REFUSED at publish is skipped, not emitted", async () => {
      // The planner trusts the validator: an unusable declaration produces no
      // rollup rather than a second error, so the two cannot disagree about what
      // is buildable. Here everything is refusable — so there is nothing to build.
      expect(
         await planFor(`  #@ preaggregate grain="category"
  measure: avg_amount is amount.avg()
  #@ preaggregate
  measure: total is amount.sum()
  #@ preaggregate grain="nonexistent"
  measure: other is amount.sum()`),
      ).toEqual([
         // `other`'s grain does not resolve, but that is the validator's call and
         // it fails the publish; the planner only drops what it cannot express.
         expect.objectContaining({ grainDimensions: ["nonexistent"] }),
      ]);
   });

   it("a negated measure is not planned", async () => {
      expect(
         await planFor(`  #@ -preaggregate
  measure: total is amount.sum()`),
      ).toEqual([]);
   });
});
