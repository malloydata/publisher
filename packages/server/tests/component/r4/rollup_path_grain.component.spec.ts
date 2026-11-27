// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4 for #2048: a `#(rollup)` grain that names a one-hop join path
 * (`grain=["account.region"]`), and a grain dimension that is exactly one
 * (`region is account.region`), each answered against live and, where it
 * must be, from the rollup. The base fixture is the issue's repro.
 *
 *  - through a join_one: the path, an alias of it, a filter on it, a
 *    coarser query and a zoned query are served; another field of the join
 *    is not;
 *  - a joined dimension the author annotated is not returned from the
 *    rollup as a column of its own (its record field cannot carry the
 *    notes), but a field over it is;
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { strictRowDifference } from "../../../src/test_helpers/conformance/answer_difference";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   buildScenario,
   DESTINATION,
   disposeScenarios,
   rowDifference,
   type R4Fixture,
   type R4Run,
} from "./harness";

const PLACEMENTS = [
   ["database", ""],
   ["storage=", ` storage="${DESTINATION}"`],
] as const;

const SETTINGS = {
   env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
};

function expectRollup(run: R4Run): void {
   expect(rowDifference(run.live, run.served)).toBeUndefined();
   expect(strictRowDifference(run.live, run.served)).toBeUndefined();
   expect(run.servedBy?.kind).toBe("rollup");
}

function expectLive(run: R4Run): void {
   expect(rowDifference(run.live, run.served)).toBeUndefined();
   expect(run.servedBy?.kind).toBe("live");
}

/** The issue's repro, with `subs` declared by `body`. */
function reproModel(
   body: string,
   options: { accountsDoc?: boolean; accountRows?: string } = {},
): string {
   return `##! experimental { persistence }
source: accounts is duckdb.sql("""select * from (values ${options.accountRows ?? "(1,'EU','SE'),(2,'EU','DE'),(3,'NA','US')"}) t(account_id, region, country)""") extend {
  primary_key: account_id${options.accountsDoc ? "\n  #(doc) The sales region the account belongs to.\n  dimension: sales_region is region" : ""}
}
source: subs is duckdb.sql("""select * from (values (10,1,DATE '2026-01-01',100.0),(11,1,DATE '2026-02-01',120.0),(12,2,DATE '2026-01-01',50.0),(13,3,DATE '2026-02-01',80.0),(14,3,DATE '2026-02-01',20.0),(15,7,DATE '2026-01-01',5.0)) t(sub_id, account_id, period, amount)""") extend {
  primary_key: sub_id
  join_one: account is accounts with account_id
${body}
}
`;
}

const JOIN_ONE_CASES = [
   ["the path", "run: subs -> { group_by: account.region; aggregate: mrr }"],
   [
      "the path under another name",
      "run: subs -> { group_by: r is account.region; aggregate: mrr }",
   ],
   [
      "a filter on the path",
      "run: subs -> { where: account.region = 'EU'; aggregate: mrr }",
   ],
   [
      "the path and the other grain dimension",
      "run: subs -> { group_by: account.region, period_month; aggregate: mrr, n }",
   ],
   [
      "a coarser query that drops the path",
      "run: subs -> { group_by: period_month; aggregate: mrr }",
   ],
   [
      "a query in its own time zone over the path",
      "run: subs -> { timezone: 'America/New_York'; group_by: account.region; aggregate: mrr }",
   ],
   [
      "a nest by the path",
      "run: subs -> { group_by: period_month; aggregate: mrr; nest: by_region is { group_by: account.region; aggregate: mrr } }",
   ],
] as const;

const JOIN_ONE_LIVE = [
   [
      "another field of the join",
      "run: subs -> { group_by: account.country; aggregate: mrr }",
   ],
   [
      "an aggregate through the join",
      "run: subs -> { group_by: account.region; aggregate: c is account.count() }",
   ],
] as const;

for (const [label, placement] of PLACEMENTS)
   describe(`R4 #2048: a path grain through a join_one [${label}]`, () => {
      let restore: () => void;
      let fixture: R4Fixture;
      beforeAll(async () => {
         restore = installSettings(SETTINGS).restore;
         fixture = await buildScenario({
            files: {
               "m.malloy": reproModel(`  dimension: period_month is period.month
  #(rollup) grain=["account.region", "period_month"]${placement}
  measure: mrr is amount.sum()
  #(rollup) grain=["account.region", "period_month"]${placement}
  measure: n is count()`),
            },
            seedSql: [],
            preaggregate: true,
         });
      });
      afterAll(async () => {
         restore();
         await disposeScenarios();
      });
      for (const [name, malloy] of JOIN_ONE_CASES) {
         it(`${name}: served from the rollup, equal to live`, async () => {
            expectRollup(await fixture.run({ name, malloy }));
         });
      }
      for (const [name, malloy] of JOIN_ONE_LIVE) {
         it(`${name}: answered live`, async () => {
            expectLive(await fixture.run({ name, malloy }));
         });
      }
   });

describe("R4 #2048: a grain dimension that is exactly a path serves the path", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings(SETTINGS).restore;
      fixture = await buildScenario({
         files: {
            "m.malloy": reproModel(`  dimension:
    region is account.region
    period_month is period.month
  #(rollup) grain=["region", "period_month"]
  measure: mrr is amount.sum()
  #(rollup) grain=["region", "period_month"]
  measure: n is count()`),
         },
         seedSql: [],
         preaggregate: true,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });
   for (const [name, malloy] of [
      ...JOIN_ONE_CASES,
      ["the dimension", "run: subs -> { group_by: region; aggregate: mrr }"],
      [
         "a filter on the dimension",
         "run: subs -> { where: region = 'EU'; aggregate: mrr }",
      ],
   ] as const) {
      it(`${name}: served from the rollup, equal to live`, async () => {
         expectRollup(await fixture.run({ name, malloy }));
      });
   }
   for (const [name, malloy] of JOIN_ONE_LIVE) {
      it(`${name}: answered live`, async () => {
         expectLive(await fixture.run({ name, malloy }));
      });
   }
});

describe("R4 #2048: an annotated joined dimension", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings(SETTINGS).restore;
      fixture = await buildScenario({
         files: {
            "m.malloy": reproModel(
               `  dimension: period_month is period.month
  #(rollup) grain=["account.sales_region", "period_month"]
  measure: mrr is amount.sum()`,
               { accountsDoc: true },
            ),
         },
         seedSql: [],
         preaggregate: true,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });
   it("returned as its own column: answered live, since the record cannot carry its notes", async () => {
      expectLive(
         await fixture.run({
            name: "annotated column",
            malloy:
               "run: subs -> { group_by: account.sales_region; aggregate: mrr }",
         }),
      );
   });
   for (const [name, malloy] of [
      [
         "under another name",
         "run: subs -> { group_by: r is account.sales_region; aggregate: mrr }",
      ],
      [
         "as a filter",
         "run: subs -> { where: account.sales_region = 'EU'; aggregate: mrr }",
      ],
   ] as const) {
      it(`${name}: served from the rollup, equal to live`, async () => {
         expectRollup(await fixture.run({ name, malloy }));
      });
   }
});

/**
 * A join_one whose key repeats in the data inflates the build's rows, so a
 * coarser query that does not read the join live re-aggregates inflated
 * partials (340 against live's 220 for February). Pinned failing until the
 * maintainer picks the fix (#2048 thread); the declared-dimension
 * workaround has the same defect on main.
 */
describe("R4 #2048: a repeated join_one key", () => {
   for (const [label, body] of [
      [
         "path grain",
         `  dimension: period_month is period.month
  #(rollup) grain=["account.region", "period_month"]
  measure: mrr is amount.sum()`,
      ],
      [
         "declared dimension",
         `  dimension:
    region is account.region
    period_month is period.month
  #(rollup) grain=["region", "period_month"]
  measure: mrr is amount.sum()`,
      ],
   ] as const) {
      it.todo(`[${label}] a coarser query answers live`, async () => {
         const restore = installSettings(SETTINGS).restore;
         try {
            const fixture = await buildScenario({
               files: {
                  "m.malloy": reproModel(body, {
                     accountRows: "(1,'EU','SE'),(1,'EU','DE'),(3,'NA','US')",
                  }),
               },
               seedSql: [],
               preaggregate: true,
               rollupKeyChecks: true,
            });
            expectLive(
               await fixture.run({
                  name: "coarser",
                  malloy:
                     "run: subs -> { group_by: period_month; aggregate: mrr }",
               }),
            );
         } finally {
            restore();
            await disposeScenarios();
         }
      });
   }
});
