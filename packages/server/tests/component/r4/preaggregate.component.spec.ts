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
   return `##! experimental.givens
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
  view: by_category is { group_by: category; aggregate: total }
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
      "a named view, which the rollup does not carry",
      "run: orders -> by_category",
      "MISSING_FIELD",
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
