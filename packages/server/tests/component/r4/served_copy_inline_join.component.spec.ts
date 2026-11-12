// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4 for an inlined join_one answered from its stored columns: the served
 * copy replaces `inline=["carriers"]`'s join with a record of the stored
 * `carriers__<field>` columns under the join's name, so `carriers.<field>`
 * reads the table with no join. `carriers` is not materialized, so on the
 * storage tier the join would cross connections (question 11): before this,
 * every query through it was answered live. What the record cannot answer (a
 * measure, view or join of the joined source; an aggregate at the join's
 * grain) is declined and answered live. Every answer equals live.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   buildScenario,
   disposeScenarios,
   rowDifference,
   type R4Fixture,
} from "./harness";

const seedSql = [
   `CREATE TABLE alliances AS SELECT * FROM (VALUES
      ('one', 'oneworld'), ('star', 'Star Alliance')
    ) AS t(id, alliance_name)`,
   `CREATE TABLE carriers AS SELECT * FROM (VALUES
      ('AA', 'American', 'east', 10.0, DATE '2001-03-15', 'one'),
      ('UA', 'United', 'west', 20.0, DATE '1999-07-02', 'star'),
      ('DL', 'Delta', 'east', 30.0, DATE '2004-11-30', NULL)
    ) AS t(code, nickname, region, fee, since, alliance_id)`,
   // 'XX' matches no carrier, and flight 7 has none: their joined fields
   // are null, as live's LEFT JOIN reads them.
   `CREATE TABLE flights AS SELECT * FROM (VALUES
      (1, 'AA', 100.0), (2, 'AA', 250.0), (3, 'UA', 300.0),
      (4, 'DL', 50.0), (5, 'XX', 75.0), (6, 'UA', 120.0), (7, NULL, 10.0)
    ) AS t(id, carrier, distance)`,
];

// `carriers` is not materialized: it stays on the origin connection while
// `flights_wide` is stored in the destination.
const model = `source: alliances is duckdb.table('alliances') extend {
  primary_key: id
}

source: carriers is duckdb.table('carriers') extend {
  primary_key: code
  join_one: alliance is alliances with alliance_id
  dimension:
    loud_name is upper(nickname)
    since_month is since.month
  measure:
    carrier_count is count()
    total_fee is fee.sum()
  view: by_region is { group_by: region; aggregate: carrier_count }
}

#(materialize) storage="lake" inline=["carriers"]
source: flights_wide is duckdb.table('flights') extend {
  primary_key: id
  join_one: carriers with carrier
  dimension: carrier_region is carriers.region
  measure:
    flight_count is count()
    total_distance is distance.sum()
    carriers_flown is count(carriers.code)
    fee_per_flight is sum(carriers.fee)
    fee_of_carriers is carriers.fee.sum()
  view: by_carrier is {
    group_by: carriers.nickname
    aggregate: flight_count, total_distance
  }
}
`;

describe("R4: an inlined join_one served from its stored columns", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
         files: { "m.malloy": model },
         seedSql,
         materialize: true,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   const served = [
      [
         "a joined dimension",
         "run: flights_wide -> { group_by: carriers.nickname; aggregate: flight_count, total_distance }",
      ],
      [
         "a dimension through the join, stored as the source's own",
         "run: flights_wide -> { group_by: carrier_region; aggregate: flight_count }",
      ],
      [
         "a joined source's computed dimension",
         "run: flights_wide -> { group_by: carriers.loud_name; aggregate: total_distance }",
      ],
      [
         "a joined source's timeframe",
         "run: flights_wide -> { group_by: carriers.since_month; aggregate: flight_count }",
      ],
      [
         "a filter on a joined field",
         "run: flights_wide -> { where: carriers.region = 'east'; aggregate: flight_count, total_distance }",
      ],
      [
         "unmatched rows, whose joined fields are null",
         "run: flights_wide -> { where: carriers.code = null; group_by: id, carriers.nickname, carriers.fee }",
      ],
      [
         "a row-grain aggregate of a joined field",
         "run: flights_wide -> { group_by: carrier_region; aggregate: fee_per_flight, carriers_flown }",
      ],
      [
         "a distinct count of a joined key",
         "run: flights_wide -> { aggregate: c is count(carriers.code) }",
      ],
      ["a view through the join", "run: flights_wide -> by_carrier"],
      [
         "a nest through the join",
         "run: flights_wide -> { group_by: carriers.region; nest: by_carrier }",
      ],
   ] as const;
   for (const [name, malloy] of served) {
      it(`serves ${name} from the table`, async () => {
         const run = await fixture.run({ name, malloy });
         expect(run.servedVia).toBe("served_copy");
         expect(rowDifference(run.live, run.served)).toBeUndefined();
      });
   }

   it("leaves no join to cross connections, so warns of none", async () => {
      const problems = await fixture.servedCopyProblems();
      expect(problems).toEqual([]);
   });

   it("reports the table as answering in the path report", async () => {
      fixture.setBindings(fixture.bindings);
      const { path } = await fixture.runAsIs(
         "run: flights_wide -> { group_by: carriers.nickname; aggregate: flight_count }",
      );
      expect(path?.servedBy).toMatchObject({
         kind: "materialized",
         via: "served_copy",
      });
   });

   // Each equals live because the served copy declines it and live answers.
   const declined = [
      [
         "a measure of the joined source",
         "run: flights_wide -> { group_by: carrier_region; aggregate: carriers.carrier_count, carriers.total_fee }",
         "MISSING_FIELD",
      ],
      [
         "an aggregate at the join's grain",
         "run: flights_wide -> { group_by: carrier_region; aggregate: f is carriers.fee.sum() }",
         "COUNT_SEMANTICS",
      ],
      [
         "a count at the join's grain",
         "run: flights_wide -> { aggregate: c is carriers.count() }",
         "COUNT_SEMANTICS",
      ],
      [
         "an average at the join's grain",
         "run: flights_wide -> { aggregate: a is carriers.fee.avg() }",
         "COUNT_SEMANTICS",
      ],
      [
         "the source's own measure at the join's grain",
         "run: flights_wide -> { group_by: carrier_region; aggregate: fee_of_carriers }",
         "COUNT_SEMANTICS",
      ],
      [
         "a join-grain aggregate in a nest",
         "run: flights_wide -> { group_by: id; nest: n is { aggregate: c is carriers.count() } }",
         "COUNT_SEMANTICS",
      ],
      [
         "a query in another time zone, over joined fields the build computed",
         "run: flights_wide -> { timezone: 'America/New_York'; group_by: carriers.since_month; aggregate: flight_count }",
         "TIME_ZONE",
      ],
      [
         "a join of the joined source",
         "run: flights_wide -> { group_by: carriers.alliance.alliance_name; aggregate: flight_count }",
         "MISSING_FIELD",
      ],
   ] as const;
   for (const [name, malloy, reason] of declined) {
      it(`answers ${name} live (${reason})`, async () => {
         const run = await fixture.run({ name, malloy });
         expect(run.servedVia).toBeUndefined();
         expect(rowDifference(run.live, run.served)).toBeUndefined();
         fixture.setBindings(fixture.bindings);
         const { path } = await fixture.runAsIs(malloy);
         expect(path?.considered?.map((c) => c.reason)).toContain(reason);
      });
   }
});

// The source's own text reads the joined source past what the table stores
// (a join of the joined source), so the record cannot stand in for the join:
// the served copy keeps the join as written, which here crosses connections,
// so the queries through it are answered live and the rest read the table.
const keptModel = model.replace(
   "    fee_of_carriers is carriers.fee.sum()",
   "    fee_of_carriers is carriers.fee.sum()\n" +
      "    alliances_flown is count(carriers.alliance.alliance_name)",
);

describe("R4: an inlined join the record cannot stand in for is kept as written", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
         files: { "m.malloy": keptModel },
         seedSql,
         materialize: true,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   it("answers a query through the join live, and the rest from the table", async () => {
      const through = await fixture.run({
         name: "through",
         malloy:
            "run: flights_wide -> { group_by: carriers.nickname; aggregate: flight_count }",
      });
      expect(through.servedVia).toBeUndefined();
      expect(rowDifference(through.live, through.served)).toBeUndefined();
      const not = await fixture.run({
         name: "not through",
         malloy:
            "run: flights_wide -> { group_by: carrier_region; aggregate: flight_count }",
      });
      expect(not.servedVia).toBe("served_copy");
      expect(rowDifference(not.live, not.served)).toBeUndefined();
      const problems = await fixture.servedCopyProblems();
      expect(problems.map((p) => [p.reason, p.join])).toEqual([
         ["cross_connection_join", "carriers"],
      ]);
   });
});
