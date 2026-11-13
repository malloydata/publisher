// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4 for `#(preaggregate)` (materialization v2, phase 4): with
 * PUBLISHER_MATERIALIZE on, a query a rollup reproduces is answered from the
 * rollup's table, colocated or in a storage destination, and equals live; a
 * query it cannot reproduce is answered live with the path report's reason.
 * One scenario per rule: A2 (qualifying measures, join-level aggregates), A3
 * (a rollup over a `storage=` base, #6293), A5 (givens, time zones), A6
 * (fan-out grains).
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   buildScenario,
   DESTINATION,
   disposeScenarios,
   rowDifference,
   type R4Fixture,
   type R4Run,
} from "./harness";

const seedSql = [
   `CREATE TABLE orders_raw AS SELECT * FROM (VALUES
      (1, 10.0, 'A', 'east', 'AA', TIMESTAMP '2026-01-01 08:00:00'),
      (2, 20.0, 'A', 'west', 'BB', TIMESTAMP '2026-01-02 09:00:00'),
      (3, 30.0, 'B', 'east', 'AA', TIMESTAMP '2026-02-01 10:00:00'),
      (4,  5.0, 'B', 'east', 'CC', TIMESTAMP '2026-02-03 11:00:00'),
      (5, 12.5, 'C', 'west', 'AA', TIMESTAMP '2026-03-04 23:30:00')
    ) AS t(id, amount, category, region, carrier, order_time)`,
   `CREATE TABLE carriers_raw AS SELECT * FROM (VALUES
      ('AA', 5), ('BB', 7), ('CC', 11)
    ) AS t(code, fleet)`,
   // Order 3 has no lines; order 1 has two kinds.
   `CREATE TABLE lines_raw AS SELECT * FROM (VALUES
      (1, 'p', 2.0), (1, 'q', 3.0), (2, 'p', 4.0), (4, 'q', 1.0), (5, 'p', 6.0)
    ) AS t(order_id, kind, qty)`,
];

/** `#(preaggregate)` lines, colocated or into the scenario's destination. */
function model(placement: string): string {
   return `##! experimental { givens sql_functions }
given: MIN_AMOUNT :: number is 0

source: carriers is duckdb.table('carriers_raw') extend { primary_key: code }
source: lines is duckdb.table('lines_raw')

source: orders is duckdb.table('orders_raw') extend {
  primary_key: id
  join_one: carriers with carrier
  join_many: lines on lines.order_id = id
  dimension:
    order_day is order_time.day
    line_kind is lines.kind
  #(preaggregate) grain=["category"]${placement}
  #(preaggregate) grain=["category", "order_day"]${placement}
  measure: total is amount.sum()
  #(preaggregate) grain=["category"]${placement}
  measure: order_count is count()
  #(preaggregate) grain=["category"]${placement}
  measure: biggest is amount.max()
  #(preaggregate) grain=["category"]${placement}
  measure: east_total is amount.sum() { where: region = 'east' }
  #(preaggregate) grain=["category"]${placement}
  measure: per_order is coalesce(amount.sum() / count(), 0)
  #(preaggregate) grain=["carrier"]${placement}
  measure: fleet is carriers.fleet.sum()
  #(preaggregate) grain=["carrier", "category"]${placement}
  measure: fleet_by_category is carriers.fleet.sum()
  #(preaggregate) grain=["category", "line_kind"]${placement}
  measure: line_total is amount.sum()
  #(preaggregate) grain=["carrier", "category"]${placement}
  measure: top_fleet is carriers.fleet.max()
  #(preaggregate) grain=["carrier", "category"]${placement}
  measure: low_fleet is carriers.fleet.min()
  view: by_category is { group_by: category; aggregate: total }
  # bar_chart
  view: by_day is {
    group_by: order_day
    aggregate: total
    order_by: order_day desc
  }
  view: categories_by_day is { group_by: category; nest: by_day }
  view: by_region is { group_by: region; aggregate: total }
  view: counted is { group_by: category; aggregate: c is count() }
  view: east_by_sql is {
    where: sql_boolean("\${TABLE}.region = 'east'")
    group_by: category
    aggregate: total
  }
}

// Extensions of orders (row 22): what each adds decides whether orders's
// rollups answer for it.
source: orders_ext is orders extend {
  dimension:
    cat_lower is lower(category)
    cat_tag is concat(cat_lower, '!')
    region_lower is lower(region)
  measure: double_total is total * 2
  view: by_lower is { group_by: cat_lower; aggregate: total }
}
source: orders_alias is orders
source: orders_east is orders extend { where: region = 'east' }
source: orders_west_coast is orders extend { timezone: 'America/Los_Angeles' }
// Each binds a name the base's stored fields read to another field: the
// rollup stores them as the base defines them.
source: orders_region_rebound is orders extend {
  rename: region_was is region
  dimension: region is 'east'
}
source: orders_amount_rebound is orders extend {
  rename: amount_was is amount
  dimension: amount is 1.0
}
source: orders_raw_sql is orders extend {
  dimension: region_sql is sql_string("lower(\${TABLE}.region)")
}
`;
}

/** A served run read the rollup and answered as live did. */
function expectServedFromRollup(run: R4Run): void {
   expect(run.servedBy?.kind).toBe("rollup");
   expect(rowDifference(run.live, run.served)).toBeUndefined();
}

/** A run the rollup could not answer: live answered, equal to live. */
function expectLive(run: R4Run): void {
   expect(run.servedBy?.kind).not.toBe("rollup");
   expect(rowDifference(run.live, run.served)).toBeUndefined();
}

const SERVED = [
   ["a stored sum", "run: orders -> { group_by: category; aggregate: total }"],
   [
      "a count, merged as a sum",
      "run: orders -> { group_by: category; aggregate: order_count }",
   ],
   ["a max", "run: orders -> { group_by: category; aggregate: biggest }"],
   [
      "a filtered measure (A2)",
      "run: orders -> { group_by: category; aggregate: east_total }",
   ],
   [
      "an expression of aggregates (A2)",
      "run: orders -> { group_by: category; aggregate: per_order }",
   ],
   [
      "a grand total, a subset of the grain",
      "run: orders -> { aggregate: total, order_count }",
   ],
   [
      "a ratio written in the query over stored measures",
      "run: orders -> { group_by: category; aggregate: r is total / order_count }",
   ],
   [
      "a count and a sum over no rows (0, as live, not null)",
      "run: orders -> { where: category = 'Z'; aggregate: order_count, total }",
   ],
   [
      "a filter on a grain dimension",
      "run: orders -> { where: category = 'A'; aggregate: total }",
   ],
   [
      "a coarser truncation of a stored day",
      "run: orders -> { group_by: m is order_day.month; aggregate: total }",
   ],
   [
      "a having: on a stored measure",
      "run: orders -> { group_by: category; aggregate: total; having: total > 15 }",
   ],
   [
      "a nest over the grain",
      "run: orders -> { group_by: category; nest: d is { group_by: order_day; aggregate: total } }",
   ],
   [
      "a join-level aggregate keyed in the grain (A2)",
      "run: orders -> { group_by: carrier; aggregate: fleet }",
   ],
   [
      "a join-level aggregate with every other grain dimension grouped (A2)",
      "run: orders -> { group_by: category; aggregate: fleet_by_category }",
   ],
   [
      "a join-level aggregate at the full grain (A2)",
      "run: orders -> { group_by: carrier, category; aggregate: fleet_by_category }",
   ],
   [
      "a fan-out dimension grouped (A6)",
      "run: orders -> { group_by: category, line_kind; aggregate: line_total }",
   ],
   [
      "a fan-out dimension fixed in a conjunct (A6)",
      "run: orders -> { where: line_kind = 'p' and category = 'A'; aggregate: line_total }",
   ],
   [
      "a fan-out dimension fixed to one value (A6)",
      "run: orders -> { where: line_kind = 'p'; group_by: category; aggregate: line_total }",
   ],
   [
      "a calculate: over a stored measure",
      "run: orders -> { group_by: category; aggregate: total; calculate: prev is lag(total); order_by: category }",
   ],
   [
      "a window function over the result",
      "run: orders -> { group_by: category; aggregate: total; calculate: r is rank() }",
   ],
   [
      "a calculate: over a fan-out measure with its dimension grouped (A6)",
      "run: orders -> { group_by: category, line_kind; calculate: prev is lag(line_total); order_by: category, line_kind }",
   ],
   [
      "a join-level max and min keyed in the grain (row 23)",
      "run: orders -> { group_by: category; aggregate: top_fleet, low_fleet }",
   ],
   [
      "a join-level max across its other grain dimension (row 23: max is the same over repeats)",
      "run: orders -> { aggregate: top_fleet, low_fleet }",
   ],
   [
      "a join-level max grouped by its key alone (row 23)",
      "run: orders -> { group_by: carrier; aggregate: top_fleet }",
   ],
   ["a named view (row 11)", "run: orders -> by_category"],
   [
      "a named view with its own ordering and notes (row 11)",
      "run: orders -> by_day",
   ],
   [
      "a named view that nests a named view (row 11)",
      "run: orders -> categories_by_day",
   ],
   [
      "a named view refined (row 11)",
      "run: orders -> by_category + { where: category = 'A' }",
   ],
   [
      "a query that starts from a named view (row 11)",
      "run: orders -> by_category -> { group_by: big is total > 20; aggregate: n is count() }",
   ],
   [
      "a named view nested in the query (row 11)",
      "run: orders -> { group_by: category; nest: by_day }",
   ],
   [
      "an extension's dimension over the grain (row 22)",
      "run: orders_ext -> { group_by: cat_lower; aggregate: total }",
   ],
   [
      "an extension's dimension over its own dimension (row 22)",
      "run: orders_ext -> { group_by: cat_tag; aggregate: total, order_count }",
   ],
   [
      "a filter on an extension's dimension (row 22)",
      "run: orders_ext -> { where: cat_lower = 'a'; aggregate: total }",
   ],
   [
      "a grain dimension and stored measure through an extension (row 22)",
      "run: orders_ext -> { group_by: category; aggregate: total }",
   ],
   ["an extension's own view (rows 11 and 22)", "run: orders_ext -> by_lower"],
   [
      "the base's view through an extension (rows 11 and 22)",
      "run: orders_ext -> by_category",
   ],
   [
      "a rename of the base (row 22)",
      "run: orders_alias -> { group_by: category; aggregate: total }",
   ],
] as const;

const LIVE = [
   [
      "a dimension outside the grain",
      "run: orders -> { group_by: region; aggregate: total }",
      "GRAIN_TOO_COARSE",
   ],
   [
      "an unstored measure-like aggregate written in the query",
      "run: orders -> { group_by: category; aggregate: c is count() }",
      "COUNT_SEMANTICS",
   ],
   [
      "a having: that applies an aggregate function to the rollup's rows",
      "run: orders -> { group_by: order_day; aggregate: total; having: count_approx(category) > 0 }",
      "COUNT_SEMANTICS",
   ],
   [
      "a having: that counts rows",
      "run: orders -> { group_by: category; aggregate: total; having: count() > 1 }",
      "COUNT_SEMANTICS",
   ],
   [
      "a join-level aggregate across a grain dimension other than its key (A2)",
      "run: orders -> { aggregate: fleet_by_category }",
      "GRAIN_TOO_COARSE",
   ],
   [
      "a join-level aggregate grouped by its key alone (A2)",
      "run: orders -> { group_by: carrier; aggregate: fleet_by_category }",
      "GRAIN_TOO_COARSE",
   ],
   [
      "a join-level aggregate in an expression across the grain (A2)",
      "run: orders -> { aggregate: f is fleet_by_category * 2 }",
      "GRAIN_TOO_COARSE",
   ],
   [
      "a fan-out dimension dropped (A6)",
      "run: orders -> { group_by: category; aggregate: line_total }",
      "GRAIN_TOO_COARSE",
   ],
   [
      "a fan-out dimension fixed in one branch of an or (A6)",
      "run: orders -> { where: line_kind = 'p' or category = 'A'; group_by: category; aggregate: line_total }",
      "GRAIN_TOO_COARSE",
   ],
   [
      "a fan-out dimension fixed to two values (A6)",
      "run: orders -> { where: line_kind = 'p' | 'q'; group_by: category; aggregate: line_total }",
      "GRAIN_TOO_COARSE",
   ],
   [
      "a select: stage, which reads rows",
      "run: orders -> { select: category }",
      "COUNT_SEMANTICS",
   ],
   [
      "a named view that groups outside the grain (row 11)",
      "run: orders -> by_region",
      "GRAIN_TOO_COARSE",
   ],
   [
      "a named view that counts rows itself (row 11)",
      "run: orders -> counted",
      "COUNT_SEMANTICS",
   ],
   [
      "an extension's dimension over a field outside the grain (row 22)",
      "run: orders_ext -> { group_by: region_lower; aggregate: total }",
      "GRAIN_TOO_COARSE",
   ],
   [
      "an extension's own measure (row 22)",
      "run: orders_ext -> { aggregate: double_total }",
      "MISSING_FIELD",
   ],
   [
      "an extension that filters its rows (row 22)",
      "run: orders_east -> { group_by: category; aggregate: total }",
      "GRAIN_TOO_COARSE",
   ],
   [
      "an extension that binds a name a stored filtered measure reads to another field (row 22)",
      "run: orders_region_rebound -> { group_by: category; aggregate: east_total }",
      "MISSING_FIELD",
   ],
   [
      "an extension that binds a name a stored sum reads to another field (row 22)",
      "run: orders_amount_rebound -> { group_by: category; aggregate: total }",
      "MISSING_FIELD",
   ],
   [
      "an extension's dimension written in raw SQL (row 22)",
      "run: orders_raw_sql -> { group_by: region_sql; aggregate: total }",
      "GRAIN_TOO_COARSE",
   ],
   [
      "a named view that filters in raw SQL (row 11)",
      "run: orders -> east_by_sql",
      "MISSING_FIELD",
   ],
   [
      "an extension in its own time zone (row 22)",
      "run: orders_west_coast -> { group_by: order_day; aggregate: total }",
      "TIME_ZONE",
   ],
   [
      "a query in its own time zone (A5)",
      "run: orders -> { timezone: 'America/Los_Angeles'; group_by: order_day; aggregate: total }",
      "TIME_ZONE",
   ],
   [
      "a nest in its own time zone (A5)",
      "run: orders -> { group_by: category; nest: n is { timezone: 'America/Los_Angeles'; group_by: order_day; aggregate: total } }",
      "TIME_ZONE",
   ],
   [
      "a calculate: over a fan-out measure the query does not aggregate (A6)",
      "run: orders -> { group_by: category; calculate: prev is lag(line_total); order_by: category }",
      "GRAIN_TOO_COARSE",
   ],
   [
      "a calculate: over a join-level measure across its other grain dimension (A2)",
      "run: orders -> { group_by: carrier; calculate: prev is lag(fleet_by_category); order_by: carrier }",
      "GRAIN_TOO_COARSE",
   ],
] as const;

function scenarioSuite(label: string, placement: string): void {
   describe(`R4: #(preaggregate) rollups ${label} answer equal to live`, () => {
      let restore: () => void;
      let fixture: R4Fixture;
      beforeAll(async () => {
         restore = installSettings({
            env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
         }).restore;
         fixture = await buildScenario({
            files: { "m.malloy": model(placement) },
            seedSql,
            preaggregate: true,
         });
      });
      afterAll(async () => {
         restore();
         await disposeScenarios();
      });

      for (const [name, malloy] of SERVED) {
         it(`serves ${name}`, async () => {
            expectServedFromRollup(await fixture.run({ name, malloy }));
         });
      }
      for (const [name, malloy, reason] of LIVE) {
         it(`answers ${name} live (${reason})`, async () => {
            const run = await fixture.run({ name, malloy });
            expectLive(run);
            fixture.setBindings(fixture.bindings);
            const { path } = await fixture.runAsIs(malloy);
            expect(
               path?.considered
                  .filter((c) => c.kind === "rollup")
                  .map((c) => c.reason),
            ).toContain(reason);
         });
      }

      it("keeps the rollups' tables off the serve shape and the served copy", () => {
         // Only the rollup leg reads a rollup's table: the serve shape would
         // compile (and fail) every query against it.
         fixture.setBindings(fixture.bindings);
         const internals = fixture.model as unknown as {
            storageServe: { serveBindings: unknown[] };
            servedCopy: { hasBindings(): boolean };
         };
         // A storage= rollup is bound; a colocated one has no binding.
         expect(fixture.bindings.length > 0).toBe(placement !== "");
         expect(internals.storageServe.serveBindings).toEqual([]);
         expect(internals.servedCopy.hasBindings()).toBe(false);
      });

      it(`reports servedFrom ${placement ? "storage" : "null"} for a rollup answer`, async () => {
         const run = await fixture.run({
            name: "servedFrom",
            malloy: "run: orders -> { group_by: category; aggregate: total }",
         });
         expectServedFromRollup(run);
         // `storage` means a table in a `storage=` destination answered.
         expect(run.servedFrom).toBe(placement ? "storage" : null);
      });

      it("routes a query that supplies a given (A5)", async () => {
         expectServedFromRollup(
            await fixture.run({
               name: "given",
               malloy:
                  "run: orders -> { group_by: category; aggregate: total; having: total > $MIN_AMOUNT }",
               givens: { MIN_AMOUNT: 10 },
            }),
         );
      });
   });
}

scenarioSuite("built colocated", "");
scenarioSuite("built into a storage destination", ` storage="${DESTINATION}"`);

describe("R4: a #(preaggregate) rollup over a storage= base (#6293)", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
         files: {
            "m.malloy": `##! experimental.persistence
#@ persist storage="${DESTINATION}"
source: daily is duckdb.table('orders_raw') -> {
  group_by: category, region, d is order_time.day
  aggregate: amount_sum is amount.sum()
} extend {
  #(preaggregate) grain=["category"] storage="${DESTINATION}"
  measure: total is amount_sum.sum()
}
`,
         },
         seedSql,
         preaggregate: true,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   it("builds the rollup and serves it, equal to live", async () => {
      expectServedFromRollup(
         await fixture.run({
            name: "rollup over a stored base",
            malloy: "run: daily -> { group_by: category; aggregate: total }",
         }),
      );
   });

   it("still serves the base from its own stored table", async () => {
      const run = await fixture.run({
         name: "the base",
         malloy: "run: daily -> { group_by: region; aggregate: total }",
      });
      expect(run.servedFrom).toBe("storage");
      expect(rowDifference(run.live, run.served)).toBeUndefined();
   });
});

describe("R4: a #(preaggregate) rollup and the model's givens", () => {
   // The given is declared in a file the model imports whole, so the model's
   // `contents` carries it with no location: a query reading $MIN_AMOUNT
   // compiles against the rollup only because the variant imports the model's
   // givens from the file declaring each (author_imports.ts).
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
         files: {
            "m.malloy": `##! experimental.givens
import "givens.malloy"

source: orders is duckdb.table('orders_raw') extend {
  #(preaggregate) grain=["category"]
  measure: total is amount.sum()
}
`,
            "givens.malloy": `##! experimental.givens
given: MIN_AMOUNT :: number is 0
`,
         },
         seedSql,
         preaggregate: true,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   it("serves a query that supplies a given from the rollup, equal to live", async () => {
      const run = await fixture.run({
         name: "supplied",
         malloy:
            "run: orders -> { group_by: category; aggregate: total; having: total > $MIN_AMOUNT }",
         givens: { MIN_AMOUNT: 20 },
      });
      expectServedFromRollup(run);
      // The given took effect: category C (12.5) is below it.
      expect(run.served.map((r) => r.category).sort()).toEqual(["A", "B"]);
   });

   it("serves a query that reads a given at its default from the rollup", async () => {
      expectServedFromRollup(
         await fixture.run({
            name: "default",
            malloy:
               "run: orders -> { group_by: category; aggregate: total; having: total > $MIN_AMOUNT }",
         }),
      );
   });
});

describe("R4: a #(preaggregate) rollup over a source whose rows read a given", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
         files: {
            "m.malloy": `##! experimental.givens
given: REGION :: string is 'east'

source: scoped is duckdb.table('orders_raw') extend {
  where: region = $REGION
  #(preaggregate) grain=["category"]
  measure: scoped_total is amount.sum()
}
`,
         },
         seedSql,
         preaggregate: true,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   it("leaves out a rollup whose rows read a given, and answers live", async () => {
      expect(
         Object.keys(fixture.rollupTables).filter((n) =>
            n.startsWith("scoped__rollup__"),
         ),
      ).toEqual([]);
      const run = await fixture.run({
         name: "scoped",
         malloy:
            "run: scoped -> { group_by: category; aggregate: scoped_total }",
         givens: { REGION: "west" },
      });
      expectLive(run);
      expect(run.live.map((r) => r.category).sort()).toEqual(["A", "C"]);
   });
});

describe("R4: a #(preaggregate) rollup under a gated entry point", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
         files: {
            "m.malloy": `##! experimental.givens
given: REGION :: string is 'east'
#(access_filter) region = $REGION
source: orders is duckdb.table('orders_raw') extend {
  #(preaggregate) grain=["category", "region"]
  measure: total is amount.sum()
}
source: open is duckdb.table('orders_raw') extend {
  #(preaggregate) grain=["category"]
  measure: total is amount.sum()
}
// An ungated base, read through a gated extension.
source: base is duckdb.table('orders_raw') extend {
  #(preaggregate) grain=["category", "region"]
  measure: base_total is amount.sum()
}
#(access_filter) region = $REGION
source: mine is base extend {}
`,
         },
         seedSql,
         preaggregate: true,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   it("answers live, as the gate requires, and says CONTRACT", async () => {
      const malloy = "run: orders -> { group_by: category; aggregate: total }";
      const run = await fixture.run({
         name: "gated",
         malloy,
         givens: { REGION: "east" },
      });
      expectLive(run);
      // The gate held: only the east rows are counted.
      expect(run.live).toEqual(
         expect.arrayContaining([
            expect.objectContaining({ category: "A", total: 10 }),
         ]),
      );
   });

   it("builds no rollup over the gated source, though its grain keeps the gate's field (row 21)", () => {
      const built = Object.keys(fixture.rollupTables);
      expect(built.filter((n) => n.startsWith("orders__rollup__"))).toEqual([]);
      // The ungated sources' rollups are still built.
      expect(built.some((n) => n.startsWith("open__rollup__"))).toBe(true);
      expect(built.some((n) => n.startsWith("base__rollup__"))).toBe(true);
   });

   it("answers a gated extension of an ungated base live, with CONTRACT (row 22)", async () => {
      const malloy =
         "run: mine -> { group_by: category; aggregate: base_total }";
      const run = await fixture.run({
         name: "gated extension",
         malloy,
         givens: { REGION: "east" },
      });
      expectLive(run);
      expect(run.live).toEqual(
         expect.arrayContaining([
            expect.objectContaining({ category: "A", base_total: 10 }),
         ]),
      );
      // The base itself, ungated, reads its rollup.
      expectServedFromRollup(
         await fixture.run({
            name: "the ungated base",
            malloy:
               "run: base -> { group_by: category; aggregate: base_total }",
         }),
      );
   });
});

describe("R4: a #(preaggregate) rollup under #(authorize)", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
         files: {
            "m.malloy": `##! experimental.givens
given: ROLE :: string is 'viewer'
#(authorize) 'admin' = $ROLE
source: orders is duckdb.table('orders_raw') extend {
  #(preaggregate) grain=["category"]
  measure: total is amount.sum()
}
source: open is duckdb.table('orders_raw') extend {
  #(preaggregate) grain=["category"]
  measure: total is amount.sum()
}
`,
         },
         seedSql,
         preaggregate: true,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   it("still refuses a caller the gate does not admit", async () => {
      await expect(
         fixture.run({
            name: "denied",
            malloy: "run: orders -> { group_by: category; aggregate: total }",
            givens: { ROLE: "viewer" },
         }),
      ).rejects.toThrow();
   });

   it("builds no rollup over the gated source (row 21)", () => {
      const built = Object.keys(fixture.rollupTables);
      expect(built.filter((n) => n.startsWith("orders__rollup__"))).toEqual([]);
      expect(built.some((n) => n.startsWith("open__rollup__"))).toBe(true);
   });

   it("answers an admitted caller live, never from the rollup (CONTRACT)", async () => {
      expectLive(
         await fixture.run({
            name: "admitted",
            malloy: "run: orders -> { group_by: category; aggregate: total }",
            givens: { ROLE: "admin" },
         }),
      );
   });
});

describe("R4: a #(preaggregate) rollup over a rolling window (S25b)", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   const query = "run: recent -> { group_by: category; aggregate: total }";
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
         files: {
            "m.malloy": `source: recent is duckdb.table('orders_raw') extend {
  where: order_time > now - 40 days
  #(preaggregate) grain=["category"]
  measure: total is amount.sum()
}
source: orders is duckdb.table('orders_raw') extend {
  dimension: is_recent is order_time > now - 40 days
  #(preaggregate) grain=["category", "is_recent"]
  measure: total is amount.sum()
  #(preaggregate) grain=["category"]
  measure: recent_total is amount.sum() { where: order_time > now - 40 days }
  #(preaggregate) grain=["category"]
  measure: all_total is amount.sum()
}
`,
         },
         seedSql,
         preaggregate: true,
         // Inside the window: January's orders and February's.
         clock: "2026-02-05 00:00:00",
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   it("answers live at the build's clock and after it moves, equal to live", async () => {
      expectLive(await fixture.run({ name: "at build", malloy: query }));
      // Now January has left the window: a rollup built at the first clock
      // would still count it.
      fixture.setClock("2026-03-20 00:00:00");
      const moved = await fixture.run({ name: "moved", malloy: query });
      expectLive(moved);
      expect(moved.live).not.toContainEqual(
         expect.objectContaining({ category: "A" }),
      );
   });

   it("answers a grain dimension or a measure that reads now live", async () => {
      fixture.setClock("2026-03-20 00:00:00");
      for (const malloy of [
         "run: orders -> { group_by: category, is_recent; aggregate: total }",
         "run: orders -> { group_by: category; aggregate: recent_total }",
      ]) {
         expectLive(await fixture.run({ name: malloy, malloy }));
      }
      // A measure that reads no clock still serves from its rollup.
      expectServedFromRollup(
         await fixture.run({
            name: "no clock",
            malloy:
               "run: orders -> { group_by: category; aggregate: all_total }",
         }),
      );
   });
});

describe("R4: a join-level max and min over a rollup, with NULLs and empty groups (row 23)", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   // Category C's only carrier matches no carrier row, D's carrier is NULL,
   // and carrier BB's fleet is NULL: each live max or min there is NULL.
   const seed = [
      `CREATE TABLE orders_raw AS SELECT * FROM (VALUES
         (1, 10.0, 'A', 'AA'), (2, 20.0, 'A', 'BB'), (3, 30.0, 'B', 'AA'),
         (4, 5.0, 'B', 'CC'), (5, 12.5, 'C', 'ZZ'), (6, 1.0, 'D', NULL)
       ) AS t(id, amount, category, carrier)`,
      `CREATE TABLE carriers_raw AS SELECT * FROM (VALUES
         ('AA', 5), ('BB', NULL), ('CC', 11)
       ) AS t(code, fleet)`,
   ];
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
         files: {
            "m.malloy": `source: carriers is duckdb.table('carriers_raw') extend { primary_key: code }
source: orders is duckdb.table('orders_raw') extend {
  primary_key: id
  join_one: carriers with carrier
  #(preaggregate) grain=["carrier", "category"]
  measure: top_fleet is carriers.fleet.max()
  #(preaggregate) grain=["carrier", "category"]
  measure: low_fleet is carriers.fleet.min()
  #(preaggregate) grain=["carrier", "category"]
  measure: spread is coalesce(carriers.fleet.max() - carriers.fleet.min(), -1)
}
`,
         },
         seedSql: seed,
         preaggregate: true,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   it("serves each group's max and min, NULL where live is NULL", async () => {
      const run = await fixture.run({
         name: "by category",
         malloy:
            "run: orders -> { group_by: category; aggregate: top_fleet, low_fleet, spread; order_by: category }",
      });
      expectServedFromRollup(run);
      expect(run.served).toEqual([
         { category: "A", top_fleet: 5, low_fleet: 5, spread: 0 },
         { category: "B", top_fleet: 11, low_fleet: 5, spread: 6 },
         { category: "C", top_fleet: null, low_fleet: null, spread: -1 },
         { category: "D", top_fleet: null, low_fleet: null, spread: -1 },
      ]);
   });

   it("serves a grand total, a key grouped, and a NULL key, equal to live", async () => {
      for (const malloy of [
         "run: orders -> { aggregate: top_fleet, low_fleet, spread }",
         "run: orders -> { group_by: carrier; aggregate: top_fleet, low_fleet }",
         "run: orders -> { where: carrier = null; aggregate: top_fleet }",
      ]) {
         expectServedFromRollup(await fixture.run({ name: malloy, malloy }));
      }
   });

   it("serves an empty group as live does: NULL, not zero", async () => {
      const run = await fixture.run({
         name: "empty",
         malloy:
            "run: orders -> { where: category = 'Z'; aggregate: top_fleet, low_fleet, spread }",
      });
      expectServedFromRollup(run);
      expect(run.served).toEqual([
         { top_fleet: null, low_fleet: null, spread: -1 },
      ]);
   });
});

describe("R4: a persisted view of the name answers before a rollup (R1, row 11)", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
         files: {
            "m.malloy": `##! experimental.persistence
source: orders is duckdb.table('orders_raw') extend {
  #(preaggregate) grain=["category"]
  measure: total is amount.sum()
  #@ persist storage="${DESTINATION}"
  view: by_category is { group_by: category; aggregate: total }
  view: by_category_too is { group_by: category; aggregate: total }
}
`,
         },
         seedSql,
         preaggregate: true,
         persistedViews: true,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   it("answers the persisted view's call from its own table", async () => {
      const run = await fixture.run({
         name: "persisted",
         malloy: "run: orders -> by_category",
      });
      expect(run.servedBy?.kind).toBe("view");
      expect(rowDifference(run.live, run.served)).toBeUndefined();
   });

   it("answers a view that is not persisted from the rollup", async () => {
      expectServedFromRollup(
         await fixture.run({
            name: "not persisted",
            malloy: "run: orders -> by_category_too",
         }),
      );
   });
});

describe("R4: a named view whose file changed since the model compiled (row 11)", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
         files: {
            "m.malloy": `source: orders is duckdb.table('orders_raw') extend {
  #(preaggregate) grain=["category"]
  measure: total is amount.sum()
  #(preaggregate) grain=["category"]
  measure: east_ is amount.sum() { where: region = 'east' }
  view: by_category is { group_by: category; aggregate: total }
}
`,
         },
         seedSql,
         preaggregate: true,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   it("answers live, never the view as the file now reads", async () => {
      // An edit not yet reloaded: the served model still runs \`total\`.
      const file = path.join(fixture.packagePath, "m.malloy");
      fs.writeFileSync(
         file,
         fs
            .readFileSync(file, "utf8")
            .replace(
               "group_by: category; aggregate: total }",
               "group_by: category; aggregate: east_ }",
            ),
      );
      const malloy = "run: orders -> by_category";
      const run = await fixture.run({ name: "stale view", malloy });
      expectLive(run);
      expect(run.live).toEqual(
         expect.arrayContaining([
            expect.objectContaining({ category: "A", total: 30 }),
         ]),
      );
      fixture.setBindings(fixture.bindings);
      const { path: report } = await fixture.runAsIs(malloy);
      expect(
         report?.considered
            .filter((c) => c.kind === "rollup")
            .map((c) => c.reason),
      ).toContain("MISSING_FIELD");
   });
});
