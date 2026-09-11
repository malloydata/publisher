// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// SPIKE — does pre-aggregation work over VIRTUAL sources, the way it was shown
// to work over ordinary persist sources?
//
// preaggregation_compose_spike.spec.ts proved the colocated mechanism: a
// `#@ persist` rollup as a compose() member, bound to its built table by the
// stock manifest substitution. Serving a rollup from the managed store is a
// different substrate and re-opens every one of those questions, because a
// stored rollup is reached as a VIRTUAL source — its fields come from a declared
// `type:` block rather than from schema discovery, and its table is resolved
// per call through `virtualMap` rather than through the build manifest. Neither
// property held in the colocated spike, so nothing it proved carries over on its
// own.
//
// Four things have to hold or part of the design has no mechanism:
//
//  1. A virtual source can be a compose() member at all. The composite resolver
//     has to read a member's fields to decide coverage, and a virtual source's
//     fields are declared rather than discovered. THIS IS THE LOAD-BEARING ONE:
//     if it fails, serving several grains on one base has no mechanism, because
//     two rollups both want to be `orders` and one name cannot rebind to two
//     shapes. The feature then becomes single-grain rebinding plus a hard
//     multi-grain refusal, rather than a deferred phase.
//  2. A merged measure declared on a virtual base resolves and routes — the
//     `total_revenue is total_revenue__partial.sum()` shape synthesis emits.
//  3. Subset-grain routing still holds over virtual members: a query grouping
//     by a subset of a member's grain is served by it.
//  4. `virtualMap` resolution fires for the member the composite CHOSE, not
//     merely for a virtual source queried directly.
//
// Method follows the colocated spike, for the same reason: ASSERT ON GENERATED
// SQL, not on values. A correct number proves nothing about routing, because the
// live path returns the same answer by design — that is the whole feature, and
// also why a value assertion cannot tell routing from non-routing. Values are
// asserted only where the question is specifically "does the re-aggregated
// answer equal the live one" (the count trap, test 6).
//
// In-memory DuckDB throughout, with real tables standing in for lake tables. The
// store is a DuckDB attach in production, so a DuckDB table reached through
// `virtualMap` is the same shape the serve path resolves.
//
// WHAT THIS DOES NOT PROVE: nothing about the build, the manifest, freshness, or
// the control plane. The rollup tables here are created by hand rather than by a
// build, and the bindings are constructed rather than derived from a manifest.
// Those seams are covered elsewhere; this file is only about whether the
// compiler does what the serve design assumes.
import type { DuckDBConnection } from "@malloydata/db-duckdb";
import type { FixedConnectionMap } from "@malloydata/malloy";
import { beforeAll, describe, expect, it } from "bun:test";
import {
   duckdbTestConnections,
   loadTestModel,
} from "./incremental_test_harness";
import {
   buildVirtualMap,
   type ServeBinding,
} from "./materialization_serve_transform";

let connections: FixedConnectionMap;
let duckdb: DuckDBConnection;

/** Four rows over two days and two categories; 260 total, 4 orders. */
const BASE_ROWS = `SELECT * FROM (VALUES
    (10, DATE '2024-01-01', 'A'),
    (20, DATE '2024-01-01', 'B'),
    (30, DATE '2024-01-02', 'A'),
    (200, DATE '2024-01-02', 'B')
  ) AS t(amount, order_date, category)`;

// The two lake tables, one per grain on ONE base — the multi-grain case, which
// is the case the composite exists for.
const TABLE_CAT = "lake_orders_rollup_category";
const TABLE_CAT_DAY = "lake_orders_rollup_category_day";
const HANDLE_CAT = "h_cat";
const HANDLE_CAT_DAY = "h_cat_day";

beforeAll(async () => {
   ({ duckdb, connections } = duckdbTestConnections());
   // Built by hand rather than by a build: this file is about the serve
   // compile, and a real build is what the colocated spike already covers.
   // Column names are synthesis's (`<measure>__partial`), because the merged
   // measure the shape declares has to reference exactly those.
   await duckdb.runSQL(
      `CREATE OR REPLACE TABLE ${TABLE_CAT} AS
       SELECT category,
              sum(amount) AS total_revenue__partial,
              count(*)    AS order_count__partial
       FROM (${BASE_ROWS}) GROUP BY category`,
   );
   await duckdb.runSQL(
      `CREATE OR REPLACE TABLE ${TABLE_CAT_DAY} AS
       SELECT category, order_date,
              sum(amount) AS total_revenue__partial,
              count(*)    AS order_count__partial
       FROM (${BASE_ROWS}) GROUP BY category, order_date`,
   );
});

/**
 * The bindings a manifest would produce for the two rollups. Built here rather
 * than through `deriveServeBindings` because that function reads a manifest,
 * which this file deliberately does not involve — but the FIELDS are the ones it
 * produces, so the `virtualMap` contract exercised below is the production one.
 */
function bindings(): ServeBinding[] {
   return [
      {
         sourceName: "r_cat",
         destinationName: "duckdb",
         virtualHandle: HANDLE_CAT,
         tablePath: TABLE_CAT,
         schema: [
            { name: "category", type: "VARCHAR" },
            { name: "total_revenue__partial", type: "HUGEINT" },
            { name: "order_count__partial", type: "BIGINT" },
         ],
      },
      {
         sourceName: "r_cat_day",
         destinationName: "duckdb",
         virtualHandle: HANDLE_CAT_DAY,
         tablePath: TABLE_CAT_DAY,
         schema: [
            { name: "category", type: "VARCHAR" },
            { name: "order_date", type: "DATE" },
            { name: "total_revenue__partial", type: "HUGEINT" },
            { name: "order_count__partial", type: "BIGINT" },
         ],
      },
   ];
}

/**
 * The lake-only composite the serve path would synthesize: each rollup as a
 * virtual source carrying its merged measures, and the author's base name
 * re-exposed as a compose() over just those.
 *
 * NO base member, deliberately — that is the design: the base lives on
 * the source warehouse and compose() is single-connection, so an uncovered query
 * must fail to compile here and fall through to the next rung rather than being
 * answered by a base member. Test 5 is that property.
 *
 * `members` is a parameter so a single-member composite — the phase-1 shape, and
 * the one grain case — is expressible in the same harness.
 */
function compositeModel(members: string): string {
   return `##! experimental { virtual_source composite_sources }
type: r_cat__shape is {
  category::string,
  total_revenue__partial::number,
  order_count__partial::number
}
source: r_cat is duckdb.virtual('${HANDLE_CAT}')::r_cat__shape extend {
  measure: total_revenue is total_revenue__partial.sum()
  measure: order_count is order_count__partial.sum()
}

type: r_cat_day__shape is {
  category::string,
  order_date::date,
  total_revenue__partial::number,
  order_count__partial::number
}
source: r_cat_day is duckdb.virtual('${HANDLE_CAT_DAY}')::r_cat_day__shape extend {
  measure: total_revenue is total_revenue__partial.sum()
  measure: order_count is order_count__partial.sum()
}

source: orders is compose(${members})
`;
}

/** The live warehouse model, for the value comparisons. */
const LIVE_MODEL = `source: orders is duckdb.sql("""${BASE_ROWS}""") extend {
  measure: total_revenue is amount.sum()
  measure: order_count is count()
}
`;

const BOTH_MEMBERS = compositeModel("r_cat, r_cat_day");

/**
 * A table name as it appears in generated SQL — quoted.
 *
 * Not cosmetic: `lake_orders_rollup_category` is a string PREFIX of
 * `lake_orders_rollup_category_day`, so a bare substring check can never assert
 * that the narrower table is ABSENT. Matching the quoted identifier is both
 * exact and closer to what the compiler emits.
 */
const q = (table: string): string => `"${table}"`;

/** SQL for a query against the composite, with virtualMap threaded as production does. */
async function sqlFor(model: string, query: string): Promise<string> {
   return loadTestModel(connections, model)
      .loadQuery(query)
      .getSQL({ virtualMap: buildVirtualMap(bindings()) });
}

/** Run a query against the composite and return its rows. */
async function rowsFor(
   model: string,
   query: string,
): Promise<Record<string, unknown>[]> {
   const result = await loadTestModel(connections, model)
      .loadQuery(query)
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      .run({ virtualMap: buildVirtualMap(bindings()) } as any);
   return result.data.toObject() as unknown as Record<string, unknown>[];
}

/** Run the same question against the live warehouse model. */
async function liveRows(query: string): Promise<Record<string, unknown>[]> {
   const result = await loadTestModel(connections, LIVE_MODEL)
      .loadQuery(query)
      .run();
   return result.data.toObject() as unknown as Record<string, unknown>[];
}

// Covered by BOTH members: category is the whole grain of one and a subset of
// the other's.
const BY_CATEGORY =
   "run: orders -> { group_by: category; aggregate: total_revenue }";
// Covered only by the (category, order_date) member.
const BY_DAY =
   "run: orders -> { group_by: order_date; aggregate: total_revenue }";
// `amount` is a base column no rollup carries, so no member covers this.
const UNCOVERED =
   "run: orders -> { group_by: category; aggregate: total_revenue; where: amount > 100 }";
// The count trap: re-aggregating a stored count() partial rather than counting
// rollup rows.
const COUNT_BY_CATEGORY =
   "run: orders -> { group_by: category; aggregate: order_count }";
/**
 * A COARSER truncation of the stored time dimension — the documented promise
 * that one `order_day` rollup serves day, month, quarter and year queries
 * (docs/preaggregation.md, "one `order_day` rollup serves day, month, quarter
 * and year queries", and the "does not route" section's `order_day.month`).
 */
const byTruncation = (unit: string) =>
   `run: orders -> { group_by: p is order_date.${unit}; aggregate: total_revenue }`;

describe("spike: pre-aggregation over virtual sources", () => {
   it("1. THE LOAD-BEARING ONE: a virtual source is a usable compose() member", async () => {
      // Compiling at all is the claim: the resolver has to read a member's
      // declared fields to decide coverage, and a virtual source declares rather
      // than discovers them. If this throws, multi-grain has no mechanism.
      const sql = await sqlFor(BOTH_MEMBERS, BY_CATEGORY);
      expect(sql).toBeTruthy();
   });

   it("2. the merged measure resolves and routes to a stored partial", async () => {
      const sql = await sqlFor(BOTH_MEMBERS, BY_CATEGORY);
      // The stored partial column, not a re-derivation from a base column: the
      // shape declares no `amount`, so seeing the partial is what proves the
      // merged measure is what ran.
      expect(sql).toContain("total_revenue__partial");
      expect(sql).not.toContain("amount");
   });

   it("3. subset-grain routing holds: a member covers a query at a coarser grain", async () => {
      // `order_date` is carried only by the (category, order_date) member, so
      // this pins that the resolver picks by coverage rather than by position.
      const sql = await sqlFor(BOTH_MEMBERS, BY_DAY);
      expect(sql).toContain(q(TABLE_CAT_DAY));
      expect(sql).not.toContain(q(TABLE_CAT));
   });

   it("4. virtualMap resolution fires for the member the composite CHOSE", async () => {
      // Querying a virtual source directly is already covered
      // (materialization_serve_transform.spec.ts). The open question is whether
      // the handle of a member selected INSIDE a composite still resolves, so
      // the assertion is that a physical table name reached the SQL at all.
      const sql = await sqlFor(BOTH_MEMBERS, BY_DAY);
      expect(sql).toContain(q(TABLE_CAT_DAY));
      // And the handle itself must NOT survive into the SQL: an unresolved
      // handle would mean the map never fired and the query would fail at run.
      expect(sql).not.toContain(HANDLE_CAT_DAY);
   });

   it("5. an uncovered query fails to compile — the fallback signal the ladder needs", async () => {
      // The lake-only composite is deliberately NOT total. The serve ladder
      // depends on this failing, so that an uncovered query falls through to the
      // next rung instead of being answered wrongly here.
      await expect(sqlFor(BOTH_MEMBERS, UNCOVERED)).rejects.toThrow();
   });

   it("6. the count trap stays closed over virtual members", async () => {
      // Emitting the measure's own count() over rollup rows would count ROLLUP
      // ROWS. It compiles, it runs, and it is wrong — so the routed value is
      // diffed against the live answer explicitly rather than merely inspected.
      const routed = await rowsFor(BOTH_MEMBERS, COUNT_BY_CATEGORY);
      const live = await liveRows(COUNT_BY_CATEGORY);
      const byCategory = (rows: Record<string, unknown>[]) =>
         Object.fromEntries(
            rows.map((r) => [String(r.category), Number(r.order_count)]),
         );
      expect(byCategory(routed)).toEqual(byCategory(live));
   });

   it("7. a single-member composite works — the phase-1 shape", async () => {
      // One grain on a base is the common case and the first phase's target.
      // Pinned here so the member-count branch is known to be a branch rather
      // than a separate mechanism.
      const sql = await sqlFor(compositeModel("r_cat"), BY_CATEGORY);
      expect(sql).toContain(q(TABLE_CAT));
   });

   it("9. THE DOCUMENTED PROMISE: a coarser truncation of a stored time dimension routes", async () => {
      // docs/preaggregation.md commits to this in two places: "one `order_day`
      // rollup serves day, month, quarter and year queries", and the
      // "does not route" section, which tells an author to group by "the declared
      // dimension (`order_day`) or a coarser truncation of it (`order_day.month`)".
      //
      // Subset-GRAIN routing (test 3) does not imply it. On a virtual source the
      // stored dimension arrives as a `type:`-declared field (`order_date::date`)
      // rather than a discovered column, and whether a truncation still resolves
      // against a declaration is a separate question about the type: block.
      for (const unit of ["month", "quarter", "year"]) {
         const sql = await sqlFor(BOTH_MEMBERS, byTruncation(unit));
         expect(sql).toContain(q(TABLE_CAT_DAY));
      }
   });

   it("10. and the truncated answer equals the live one", async () => {
      // Summing daily partials into months is only correct because the merge
      // commutes; asserting the SQL alone would not catch a rollup that routed
      // and then re-aggregated wrongly.
      const routed = await rowsFor(BOTH_MEMBERS, byTruncation("month"));
      const live = await liveRows(byTruncation("month"));
      const byPeriod = (rows: Record<string, unknown>[]) =>
         Object.fromEntries(
            rows.map((r) => [String(r.p), Number(r.total_revenue)]),
         );
      expect(byPeriod(routed)).toEqual(byPeriod(live));
   });

   it("8. THE PREMISE THE ORDERING FIX RESTS ON: selection follows declaration order", async () => {
      // Both members cover a group-by on category, and the resolver takes the
      // first one declared — narrow-first picks narrow, wide-first picks wide.
      //
      // This file hand-writes its compose() member list, so it cannot test
      // `compareRollupBreadth`; that rule lives in synthesis and is pinned in
      // preaggregation_synthesis.spec.ts. What it CAN pin is the compiler
      // behaviour that rule exploits — that member order decides selection, and
      // that the compiler applies no size preference of its own.
      //
      // Which makes this the load-bearing assumption behind the fix for the
      // member-ordering defect rather than a curiosity: emitting the coarsest
      // member first only works because the resolver honours the order it is
      // given. If the compiler ever became size-aware, or stopped being
      // first-wins, the synthesis-side ordering would be either redundant or
      // actively wrong, and this is what would say so.
      const chosen = async (members: string) =>
         (await sqlFor(compositeModel(members), BY_CATEGORY)).includes(
            q(TABLE_CAT_DAY),
         )
            ? "wide"
            : "narrow";
      expect({
         narrowFirst: await chosen("r_cat, r_cat_day"),
         wideFirst: await chosen("r_cat_day, r_cat"),
      }).toEqual({ narrowFirst: "narrow", wideFirst: "wide" });
   });
});
