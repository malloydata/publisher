// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   Runtime,
} from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it, test } from "bun:test";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   buildScenario,
   rowDifference,
   freshDuckDB,
   type R4Fixture,
   type R4Query,
   type Row,
} from "./harness";

let restoreSettings: () => void;
beforeAll(() => {
   restoreSettings = installSettings({
      env: { PERSIST_STORAGE_MODE: "on" },
   }).restore;
});
afterAll(() => restoreSettings());

/** Served must equal live, and must have read a stored table. */
async function expectServedMatchesLive(fixture: R4Fixture, query: R4Query) {
   const run = await fixture.run(query);
   if (query.expectLive) expect(run.servedFrom).not.toBe("storage");
   else expect(run.servedFrom).toBe("storage");
   expect(rowDifference(run.live, run.served)).toBeUndefined();
   return run;
}

describe("R4: a persisted rollup and its extension", () => {
   let fixture: R4Fixture;
   beforeAll(async () => {
      fixture = await buildScenario({
         files: {
            "m.malloy": `##! experimental.persistence
source: orders is duckdb.table('orders') extend { primary_key: id }

#@ persist name="daily" storage="lake"
source: daily is orders -> {
  group_by: order_date, region
  aggregate: total is amount.sum(), order_count is count()
}

source: daily_ext is daily extend {
  dimension: big_day is total > 100
  measure: grand_total is total.sum()
}
`,
         },
         seedSql: [
            `CREATE TABLE orders AS SELECT * FROM (VALUES
               (1, DATE '2026-01-01', 'east', 100.5),
               (2, DATE '2026-01-01', 'west', 50.25),
               (3, DATE '2026-01-02', 'east', 200.0),
               (4, DATE '2026-01-02', 'east', 25.125),
               (5, DATE '2026-01-03', 'west', 300.0)
             ) AS t(id, order_date, region, amount)`,
         ],
      });
   });

   it("binds the persisted source", () => {
      expect(fixture.bindings.map((b) => b.sourceName)).toContain("daily");
   });

   it("matches live grouped by a stored dimension", async () => {
      await expectServedMatchesLive(fixture, {
         name: "by date",
         malloy:
            "run: daily -> { group_by: order_date; aggregate: t is total.sum() }",
      });
   });

   it("matches live under a filter", async () => {
      await expectServedMatchesLive(fixture, {
         name: "east only",
         malloy:
            "run: daily -> { where: region = 'east'; aggregate: t is total.sum(), n is order_count.sum() }",
      });
   });

   it("matches live through the extension's dimension and measure", async () => {
      await expectServedMatchesLive(fixture, {
         name: "big days",
         malloy:
            "run: daily_ext -> { group_by: big_day; aggregate: grand_total }",
      });
   });
});

describe("R4: child counts through a join_many with childless parents", () => {
   // Flights 5 and 6 have no segments: the case malloydata/malloy#1762 is about.
   const seedSql = [
      `CREATE TABLE flights AS SELECT * FROM (VALUES
         (1, 'AA'), (2, 'AA'), (3, 'UA'), (4, 'UA'), (5, 'AA'), (6, 'UA')
       ) AS t(id, carrier)`,
      `CREATE TABLE segments AS SELECT * FROM (VALUES
         (10, 1, 100), (11, 1, 200), (12, 2, 300), (13, 3, 400), (14, 4, 500)
       ) AS t(seg_id, flight_id, miles)`,
   ];
   const model = `##! experimental.persistence
#@ persist name="segs" storage="lake"
source: segs is duckdb.table('segments') -> { select: * } extend {
  primary_key: seg_id
  measure: seg_count is count()
}

#@ persist name="flight_rows" storage="lake"
source: flights is duckdb.table('flights') -> { select: * } extend {
  primary_key: id
  join_many: segs on segs.flight_id = id
}
`;
   const byCarrier: R4Query = {
      name: "segments per carrier",
      malloy:
         "run: flights -> { group_by: carrier; aggregate: seg_count is segs.count(), miles is segs.miles.sum() }",
      truthSql: `SELECT f.carrier, count(s.seg_id) AS seg_count, sum(s.miles) AS miles
                 FROM flights f LEFT JOIN segments s ON s.flight_id = f.id GROUP BY 1`,
   };
   let fixture: R4Fixture;
   beforeAll(async () => {
      fixture = await buildScenario({ files: { "m.malloy": model }, seedSql });
   });

   const pick =
      (...columns: string[]) =>
      (rows: Row[]) =>
         rows.map((r) => Object.fromEntries(columns.map((c) => [c, r[c]])));
   const miles = pick("carrier", "miles");
   const counts = pick("carrier", "seg_count");

   it("serves the sum from storage, equal to live and to the truth", async () => {
      const run = await fixture.run(byCarrier);
      expect(run.servedFrom).toBe("storage");
      expect(rowDifference(miles(run.live), miles(run.served))).toBeUndefined();
      expect(
         rowDifference(miles(run.truth ?? []), miles(run.served)),
      ).toBeUndefined();
   });

   it("serves the right child count from storage", async () => {
      const run = await fixture.run(byCarrier);
      expect(
         rowDifference(counts(run.truth ?? []), counts(run.served)),
      ).toBeUndefined();
   });

   // Live counts each childless flight as one segment: Malloy compiles the
   // leafiest join_many count as COUNT(1) over the LEFT JOIN
   // (malloydata/malloy#1762). The storage serve shape does not, so served and
   // live disagree, and served is right. Both tests pass once Publisher's
   // Malloy pin carries malloydata/malloy#3207; then drop `.failing`.
   test.failing(
      "live child count matches the truth (malloydata/malloy#1762)",
      async () => {
         const run = await fixture.run(byCarrier);
         expect(
            rowDifference(counts(run.truth ?? []), counts(run.live)),
         ).toBeUndefined();
      },
   );

   test.failing(
      "served child count equals live (malloydata/malloy#1762)",
      async () => {
         const run = await fixture.run(byCarrier);
         expect(
            rowDifference(counts(run.live), counts(run.served)),
         ).toBeUndefined();
      },
   );
});

// ms2data/service#6982, the phase 0 probe: two sources from different origin
// connections, both stored in one destination, joined. Malloy refuses the join
// as authored (malloydata/malloy#3032); over the destination it is a join on
// one connection. This checks the second half by hand, before phase 3 builds
// the served copy that does it.
describe("R4 probe: a join across origin connections, over their destination", () => {
   const ROOT = "file:///r4probe/";
   const origin = (name: string): DuckDBConnection => freshDuckDB(name);

   async function compile(
      connections: Map<string, DuckDBConnection>,
      model: string,
      query: string,
   ) {
      const runtime = new Runtime({
         urlReader: new InMemoryURLReader(
            new Map([[`${ROOT}m.malloy`, model]]),
         ),
         connections: new FixedConnectionMap(
            connections,
            [...connections.keys()][0],
         ),
      });
      return runtime
         .loadModel(new URL(`${ROOT}m.malloy`), {
            importBaseURL: new URL(ROOT),
         })
         .loadQuery(query);
   }

   const accounts = `SELECT * FROM (VALUES (1, 'acme'), (2, 'globex'), (3, 'initech')) AS t(id, account_name)`;
   const usage = `SELECT * FROM (VALUES (1, 5), (1, 7), (2, 1)) AS t(account_id, events)`;
   const query =
      "run: account_usage -> { group_by: account_name; aggregate: total_events is u.events.sum(), usage_rows is u.count() }";

   it("is refused as authored, across two connections", async () => {
      const crm = origin("crm");
      const product = origin("product");
      await crm.runSQL(`CREATE TABLE accounts AS ${accounts}`);
      await product.runSQL(`CREATE TABLE usage AS ${usage}`);
      const authored = `
source: accounts is crm.table('accounts') extend { primary_key: id }
source: usage is product.table('usage')
source: account_usage is accounts extend { join_many: u is usage on u.account_id = id }
`;
      const q = await compile(
         new Map([
            ["crm", crm],
            ["product", product],
         ]),
         authored,
         query,
      );
      await expect(q.getSQL()).rejects.toThrow(/connection/i);
   });

   it("runs over the destination and matches a hand-computed answer", async () => {
      // Both sides stored in the destination, as their builds would write them.
      const lake = origin("lake");
      await lake.runSQL(`CREATE TABLE accounts_s AS ${accounts}`);
      await lake.runSQL(`CREATE TABLE usage_s AS ${usage}`);
      const servedCopy = `
source: accounts is lake.table('accounts_s') extend { primary_key: id }
source: usage is lake.table('usage_s')
source: account_usage is accounts extend { join_many: u is usage on u.account_id = id }
`;
      const q = await compile(new Map([["lake", lake]]), servedCopy, query);
      const rows = (await q.run()).data.toObject() as Row[];
      // initech has no usage: Malloy's sum of no rows is 0. Its count reads 1
      // until malloy#3207 ships, so only the sum is compared here; the count
      // case is above.
      expect(
         rowDifference(
            [
               { account_name: "acme", total_events: 12 },
               { account_name: "globex", total_events: 1 },
               { account_name: "initech", total_events: 0 },
            ],
            rows.map((r) => ({
               account_name: r.account_name,
               total_events: r.total_events,
            })),
         ),
      ).toBeUndefined();
   });
});
