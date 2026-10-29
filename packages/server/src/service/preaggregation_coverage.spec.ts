// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// The coverage check over real compiled IR, for every query shape that can put
// an aggregate over a rollup's rows. Hand-written composites rather than a
// synthesized package, so each case reads as the query alone; the colocated
// and storage serve paths over a real package are preaggregation_seams.spec.ts
// and the hammer `preaggregation` suite.
import type { FixedConnectionMap } from "@malloydata/malloy";
import { beforeAll, describe, expect, it } from "bun:test";
import {
   duckdbTestConnections,
   loadTestModel,
} from "../test_helpers/incremental_test_harness";
import { rollupCoverageViolation } from "./preaggregation_coverage";

let connections: FixedConnectionMap;

beforeAll(() => {
   ({ connections } = duckdbTestConnections());
});

// The colocated shape synthesis emits: the rollup first, the base last.
const COLOCATED = `##! experimental { persistence composite_sources }
source: base is duckdb.sql("""
  SELECT * FROM (VALUES (10, 'A', 1), (20, 'A', 1), (30, 'B', 2)) AS t(amount, category, qty)
""") extend {
  measure: total is amount.sum()
}
#@ persist
source: r is base -> {
  group_by: category, qty
  aggregate: total__partial is total
} extend {
  measure: total is total__partial.sum()
}
source: orders is compose(r, base)
`;

// The storage shape: a virtual member, and no base member.
const VIRTUAL = `##! experimental { virtual_source composite_sources }
type: r__shape is { category::string, qty::number, total__partial::number }
source: r is duckdb.virtual('h')::r__shape extend {
  measure: total is total__partial.sum()
}
source: orders is compose(r)
`;

const ROLLUPS = new Map([["r", new Set(["total"])]]);

async function prepare(model: string, query: string) {
   return (
      await loadTestModel(connections, model)
         .loadRestrictedQuery(query)
         .getPreparedQuery()
   )._query as { compositeResolvedSourceDef?: { as?: string } };
}

async function violation(model: string, query: string) {
   return rollupCoverageViolation(await prepare(model, query), ROLLUPS);
}

/** The member the composite resolver chose, so an allowed case is not vacuous. */
async function resolvedMember(model: string, query: string) {
   return (await prepare(model, query)).compositeResolvedSourceDef?.as;
}

describe("an aggregate over the rollup's own rows refuses it", () => {
   const REFUSED: [string, string][] = [
      ["a bare count()", "run: orders -> { aggregate: n is count() }"],
      [
         "a sum over a grain dimension",
         "run: orders -> { aggregate: s is qty.sum() }",
      ],
      [
         "an average over a grain dimension",
         "run: orders -> { group_by: category; aggregate: a is qty.avg() }",
      ],
      // Correct from a rollup, refused anyway: the rule is one line.
      [
         "a distinct count of a grain dimension",
         "run: orders -> { aggregate: d is count(category) }",
      ],
      [
         "a max over a grain dimension",
         "run: orders -> { aggregate: m is qty.max() }",
      ],
      [
         "a dialect aggregate function",
         "run: orders -> { aggregate: s is stddev(qty) }",
      ],
      [
         "an aggregate function wrapped in a scalar one",
         "run: orders -> { aggregate: l is length(string_agg(category)) }",
      ],
      [
         "count() combined with a merged measure",
         "run: orders -> { aggregate: avg_total is total / count() }",
      ],
      [
         "count() inside a nest",
         "run: orders -> { group_by: category; nest: n is { aggregate: c is count() } }",
      ],
      [
         "count() in a having: clause",
         "run: orders -> { group_by: category; aggregate: total; having: count() > 1 }",
      ],
      [
         "count() inside an ungrouped aggregate",
         "run: orders -> { group_by: category; aggregate: a is all(count()) }",
      ],
      [
         "count() defined by an inline extend",
         "run: orders extend { measure: n is count() } -> { aggregate: n }",
      ],
      [
         "count() reached through a measure an inline extend derives",
         "run: orders extend { measure: n is count(), m is n * 2 } -> { aggregate: m }",
      ],
      // Refused only because Malloy expands a named view into the query's IR.
      [
         "count() inside a nested named view",
         "run: orders extend { view: v is { aggregate: c is count() } } -> { group_by: category; nest: v }",
      ],
      [
         "count() inside a named view run directly",
         "run: orders extend { view: v is { aggregate: c is count() } } -> v",
      ],
      [
         "count() added by refining a named view",
         "run: orders extend { view: v is { group_by: category } } -> v + { aggregate: c is count() }",
      ],
      [
         "count() inside a calculate:",
         "run: orders -> { group_by: category; aggregate: total; calculate: p is lag(count()) }",
      ],
   ];

   for (const [label, query] of REFUSED) {
      it(label, async () => {
         expect(await violation(COLOCATED, query)).toStartWith("r: aggregate");
      });
   }

   // Not an aggregate at all: a stage that reads rows, of which a rollup has
   // one per grain group.
   const ROW_STAGES: [string, string][] = [
      ["a select: stage", "run: orders -> { select: category, qty }"],
      ["an index: stage", "run: orders -> { index: category }"],
      [
         "a select: inside a nest",
         "run: orders -> { group_by: category; nest: n is { select: qty } }",
      ],
   ];

   it("a field reached through a join the query adds", async () => {
      const query =
         "run: orders extend { join_one: b is base on b.qty = qty } -> { group_by: category; aggregate: bt is b.total }";
      expect(await resolvedMember(COLOCATED, query)).toBe("r");
      expect(await violation(COLOCATED, query)).toStartWith(
         "r: reference b.total",
      );
   });

   for (const [label, query] of ROW_STAGES) {
      it(label, async () => {
         expect(await resolvedMember(COLOCATED, query)).toBe("r");
         expect(await violation(COLOCATED, query)).toMatch(
            /^r: (a select|an index): stage over the rollup/,
         );
      });
   }

   it("over a virtual member, as the storage tier serves it", async () => {
      // The member name has to be readable off a virtual source too, or the
      // storage path would never be checked.
      expect(
         await violation(VIRTUAL, "run: orders -> { aggregate: n is count() }"),
      ).toStartWith("r: aggregate");
   });
});

describe("what a rollup can answer still routes", () => {
   const ALLOWED: [string, string][] = [
      [
         "a merged measure",
         "run: orders -> { group_by: category; aggregate: total }",
      ],
      ["no aggregate at all", "run: orders -> { group_by: category, qty }"],
      [
         "an expression over a merged measure",
         "run: orders -> { aggregate: half is total / 2 }",
      ],
      [
         "a merged measure filtered on a grain dimension",
         "run: orders -> { aggregate: a is total { where: category = 'A' } }",
      ],
      [
         "an ungrouped merged measure",
         "run: orders -> { group_by: category; aggregate: total, share is total / all(total) }",
      ],
      [
         "a window function over a merged measure",
         "run: orders -> { group_by: category; aggregate: total; calculate: prev is lag(total) }",
      ],
      [
         "a merged measure inside a nest",
         "run: orders -> { group_by: category; nest: n is { group_by: qty; aggregate: total } }",
      ],
      [
         "a measure an inline extend derives from a merged measure",
         "run: orders extend { measure: h is total / 2; dimension: q2 is qty * 2 } -> { group_by: q2; aggregate: h }",
      ],
      [
         // A later stage reads the first stage's output, where count() counts
         // its rows, which is what the author asked for.
         "count() in a later pipeline stage",
         "run: orders -> { group_by: category; aggregate: total } -> { aggregate: n is count() }",
      ],
   ];

   for (const [label, query] of ALLOWED) {
      it(label, async () => {
         expect(await resolvedMember(COLOCATED, query)).toBe("r");
         expect(await violation(COLOCATED, query)).toBeUndefined();
      });
   }

   it("over a virtual member", async () => {
      expect(
         await violation(
            VIRTUAL,
            "run: orders -> { group_by: category; aggregate: total }",
         ),
      ).toBeUndefined();
   });
});

describe("a query the resolver did not send to a rollup", () => {
   it("is not checked when the base member answered", async () => {
      // `amount` is not in the rollup, so the base answers and count() is
      // a real row count.
      const query =
         "run: orders -> { group_by: amount; aggregate: n is count() }";
      expect(await resolvedMember(COLOCATED, query)).toBe("base");
      expect(await violation(COLOCATED, query)).toBeUndefined();
   });

   it("is not checked when no composite was involved", async () => {
      expect(
         await violation(COLOCATED, "run: base -> { aggregate: n is count() }"),
      ).toBeUndefined();
   });
});
