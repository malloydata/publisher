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
 *  - through a join_many: a query that groups by the path is served, with
 *    the symmetric-aggregate partials live computes, a base row with several
 *    joined rows sharing a value and one with none (the `null` group)
 *    included; one that drops the path or only filters on it answers live.
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
      "a coarser query that filters through the join",
      "run: subs -> { where: account.region != 'XX'; group_by: period_month; aggregate: mrr }",
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
      // B (#2048): it does not read `account`, which the build joined, so a
      // repeated key would have inflated it.
      "a coarser query that reads nothing through the join",
      "run: subs -> { group_by: period_month; aggregate: mrr }",
   ],
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
 * Sub 10 has items of two skus; sub 11 has two items of one sku (several
 * joined rows sharing a value); sub 12 has none (the `null` group); item 99
 * has no sub.
 */
const ITEMS_MODEL = `##! experimental { persistence }
source: items is duckdb.sql("""select * from (values (1,10,'A',2),(2,10,'B',1),(3,11,'A',4),(4,11,'A',1),(5,13,'B',3),(6,14,'C',1),(7,99,'A',8)) t(item_id, sub_id, sku, qty)""") extend {
  primary_key: item_id
}
source: subs is duckdb.sql("""select * from (values (10,1,DATE '2026-01-01',100.0),(11,1,DATE '2026-02-01',120.0),(12,2,DATE '2026-01-01',50.0),(13,3,DATE '2026-02-01',80.0),(14,3,DATE '2026-02-01',20.0)) t(sub_id, account_id, period, amount)""") extend {
  primary_key: sub_id
  join_many: items on items.sub_id = sub_id
  dimension: period_month is period.month
  #(rollup) grain=["items.sku", "period_month"]
  measure: mrr is amount.sum()
  #(rollup) grain=["items.sku", "period_month"]
  measure: n is count()
  #(rollup) grain=["items.sku", "period_month"]
  measure: top is amount.max()
}
`;

for (const [label, placement] of PLACEMENTS)
   describe(`R4 #2048: a path grain through a join_many [${label}]`, () => {
      let restore: () => void;
      let fixture: R4Fixture;
      beforeAll(async () => {
         restore = installSettings(SETTINGS).restore;
         fixture = await buildScenario({
            files: {
               "m.malloy": ITEMS_MODEL.replaceAll(
                  '"period_month"]\n',
                  `"period_month"]${placement}\n`,
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
      for (const [name, malloy] of [
         [
            "grouped by the path",
            "run: subs -> { group_by: items.sku; aggregate: mrr, n, top }",
         ],
         [
            "grouped by the path and the other grain dimension",
            "run: subs -> { group_by: items.sku, period_month; aggregate: mrr, n }",
         ],
         [
            "grouped by the path, filtered on it",
            "run: subs -> { where: items.sku = 'A'; group_by: items.sku; aggregate: mrr, n }",
         ],
         [
            "a nest under a grouping by the path",
            "run: subs -> { group_by: items.sku; aggregate: mrr; nest: by_month is { group_by: period_month; aggregate: mrr } }",
         ],
      ] as const) {
         it(`${name}: served from the rollup, equal to live`, async () => {
            expectRollup(await fixture.run({ name, malloy }));
         });
      }
      for (const [name, malloy] of [
         [
            "a query that drops the path",
            "run: subs -> { group_by: period_month; aggregate: mrr, n }",
         ],
         [
            "a filter on the path without grouping by it",
            "run: subs -> { where: items.sku = 'A'; aggregate: mrr }",
         ],
         [
            // A field the query defines is not read as the path it reads: an
            // expression over it could merge two values' groups.
            "grouped by the path under another name",
            "run: subs -> { group_by: s is items.sku; aggregate: mrr }",
         ],
         ["a total with no grouping", "run: subs -> { aggregate: mrr }"],
      ] as const) {
         it(`${name}: answered live`, async () => {
            expectLive(await fixture.run({ name, malloy }));
         });
      }
      it("holds the null group live computes for a base row with no joined rows", async () => {
         const run = await fixture.run({
            name: "null group",
            malloy: "run: subs -> { group_by: items.sku; aggregate: mrr, n }",
         });
         expectRollup(run);
         expect(run.served).toContainEqual({ sku: null, mrr: 50, n: 1 });
         expect(run.served).toContainEqual({ sku: "A", mrr: 220, n: 2 });
      });
   });

/**
 * A join_one whose key repeats in the data (account 1 twice) repeats the
 * build's rows, so a coarser query that does not read the join live would
 * re-aggregate inflated partials (340 against live's 220 for February,
 * 255 against 155 for January). The
 * rollup serves only a query that reads the join, which live joins too, so
 * every answer below equals live.
 */
for (const [label, body, byRegion] of [
   [
      "path grain",
      `  dimension: period_month is period.month
  #(rollup) grain=["account.region", "period_month"]
  measure: mrr is amount.sum()`,
      "run: subs -> { group_by: account.region; aggregate: mrr }",
   ],
   [
      "declared dimension",
      `  dimension:
    region is account.region
    period_month is period.month
  #(rollup) grain=["region", "period_month"]
  measure: mrr is amount.sum()`,
      "run: subs -> { group_by: region; aggregate: mrr }",
   ],
] as const) {
   describe(`R4 #2048: a repeated join_one key [${label}]`, () => {
      let restore: () => void;
      let fixture: R4Fixture;
      beforeAll(async () => {
         restore = installSettings(SETTINGS).restore;
         fixture = await buildScenario({
            files: {
               "m.malloy": reproModel(body, {
                  accountRows: "(1,'EU','SE'),(1,'EU','DE'),(3,'NA','US')",
               }),
            },
            seedSql: [],
            preaggregate: true,
            rollupKeyChecks: true,
         });
      });
      afterAll(async () => {
         restore();
         await disposeScenarios();
      });
      it("a coarser query that does not read the join answers live, with live's totals", async () => {
         const run = await fixture.run({
            name: "coarser",
            malloy: "run: subs -> { group_by: period_month; aggregate: mrr }",
         });
         expectLive(run);
         // Live's totals: January 100 + 50 + 5 (sub 15 meets no account), February
         // 120 + 80 + 20; the rollup's partials hold account 1's rows twice.
         expect(run.served.map((r) => r.mrr).sort()).toEqual([155, 220]);
      });
      it("a query grouped through the join is served, equal to live", async () => {
         expectRollup(
            await fixture.run({ name: "by region", malloy: byRegion }),
         );
      });
   });
}
