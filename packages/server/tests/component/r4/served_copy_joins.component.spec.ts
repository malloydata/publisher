// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4 for joins through the served copy (materialization v2, phase 3 PR 4):
 * reference joins (the default), an inlined join_one, a reference join the
 * re-root moves across connections (question 11), and a served-copy answer
 * retried live. Every answer equals live, or, where live cannot answer, a
 * hand-computed truth.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { useMaterializationOn } from "../../../src/test_helpers/settings_env";
import {
   buildScenario,
   disposeScenarios,
   rowDifference,
   type R4Fixture,
} from "./harness";

const seedSql = [
   `CREATE TABLE carriers AS SELECT * FROM (VALUES
      ('AA', 'American', 'east'),
      ('UA', 'United', 'west'),
      ('DL', 'Delta', 'east')
    ) AS t(code, nickname, region)`,
   `CREATE TABLE flights AS SELECT * FROM (VALUES
      (1, 'AA', 100.0), (2, 'AA', 250.0), (3, 'UA', 300.0),
      (4, 'DL', 50.0), (5, 'XX', 75.0), (6, 'UA', 120.0)
    ) AS t(id, carrier, distance)`,
   `CREATE TABLE segments AS SELECT * FROM (VALUES
      (1, 1, 40.0), (2, 1, 60.0), (3, 2, 250.0), (4, 3, 100.0), (5, 3, 200.0)
    ) AS t(seg_id, flight_id, miles)`,
];

const model = `#(materialize) storage="lake"
source: carriers is duckdb.table('carriers') extend {
  primary_key: code
  dimension: loud_name is upper(nickname)
}

source: segments is duckdb.table('segments') extend {
  primary_key: seg_id
  measure: seg_miles is miles.sum()
}

#(materialize) storage="lake"
source: flights is duckdb.table('flights') extend {
  primary_key: id
  join_one: carriers with carrier
  join_many: segments on segments.flight_id = id
  dimension:
    long_haul is distance > 200
    carrier_label is concat(carrier, ' / ', carriers.nickname)
  measure:
    flight_count is count()
    total_distance is distance.sum()
  view: by_carrier is {
    group_by: carriers.nickname
    aggregate: flight_count, total_distance
  }
}

#(materialize) storage="lake" inline=["carriers"]
source: flights_wide is duckdb.table('flights') extend {
  primary_key: id
  join_one: carriers with carrier
  dimension: carrier_region is carriers.region
  measure: flight_count is count()
}
`;

describe("R4: joins through the served copy answer equal to live", () => {
   let fixture: R4Fixture;
   useMaterializationOn();
   beforeAll(async () => {
      fixture = await buildScenario({
         files: { "m.malloy": model },
         seedSql,
         materialize: true,
      });
   });
   afterAll(async () => {
      await disposeScenarios();
   });

   const served = [
      [
         "a reference join_one to a materialized source",
         "run: flights -> { group_by: carriers.nickname; aggregate: flight_count, total_distance }",
      ],
      ["a view through the reference join", "run: flights -> by_carrier"],
      [
         "a dimension through a reference join, computed at read",
         "run: flights -> { group_by: carrier_label; aggregate: flight_count }",
      ],
      [
         "a joined source's computed dimension",
         "run: flights -> { group_by: carriers.loud_name; aggregate: total_distance }",
      ],
      [
         "a dimension through an inlined join_one",
         "run: flights_wide -> { group_by: carrier_region; aggregate: flight_count }",
      ],
      [
         "the inlined join itself",
         "run: flights_wide -> { group_by: carriers.nickname; aggregate: flight_count }",
      ],
   ] as const;
   for (const [name, malloy] of served) {
      it(name, async () => {
         const run = await fixture.run({ name, malloy });
         expect(run.servedVia).toBe("served_copy");
         expect(rowDifference(run.live, run.served)).toBeUndefined();
      });
   }

   // `segments` is not materialized, so it stays on the origin connection
   // while `flights` moves to the destination (question 11).
   it("leaves out a reference join the re-root moves across connections, and answers its queries live", async () => {
      const malloy =
         "run: flights -> { group_by: long_haul; aggregate: flight_count, segments.seg_miles }";
      const run = await fixture.run({ name: "join_many", malloy });
      expect(run.servedVia).toBeUndefined();
      expect(rowDifference(run.live, run.served)).toBeUndefined();
   });

   it("still serves the table for a query that does not use the left-out join", async () => {
      const run = await fixture.run({
         name: "no join",
         malloy:
            "run: flights -> { group_by: long_haul; aggregate: flight_count }",
      });
      expect(run.servedVia).toBe("served_copy");
      expect(rowDifference(run.live, run.served)).toBeUndefined();
   });

   it("names the left-out join in a package warning", async () => {
      const problems = await fixture.servedCopyProblems();
      expect(problems.map((p) => [p.sourceName, p.reason, p.join])).toEqual([
         ["flights", "cross_connection_join", "segments"],
      ]);
   });
});

// The same through a v1 `#@ persist storage=` source, whose table the path
// report places on the query's chain: the skipped table carries
// `CROSS_CONNECTION`.
const persistModel = `##! experimental { persistence }
source: segments is duckdb.table('segments') extend {
  primary_key: seg_id
  measure: seg_miles is miles.sum()
}

#@ persist name="fl" storage="lake"
source: fl is duckdb.table('flights') -> { select: * } extend {
  primary_key: id
  join_many: segments on segments.flight_id = id
  measure: flight_count is count()
}
`;

describe("R4: a reference join across connections in the path report", () => {
   let fixture: R4Fixture;
   useMaterializationOn();
   beforeAll(async () => {
      fixture = await buildScenario({
         files: { "m.malloy": persistModel },
         seedSql,
      });
   });
   afterAll(async () => {
      await disposeScenarios();
   });

   it("skips the table with reason CROSS_CONNECTION for a query through the join", async () => {
      const malloy =
         "run: fl -> { group_by: carrier; aggregate: flight_count, segments.seg_miles }";
      const run = await fixture.run({ name: "through", malloy });
      expect(run.servedVia).toBeUndefined();
      expect(rowDifference(run.live, run.served)).toBeUndefined();
      fixture.setBindings(fixture.bindings);
      const { path } = await fixture.runAsIs(malloy);
      expect(path?.considered?.map((c) => c.reason)).toContain(
         "CROSS_CONNECTION",
      );
   });

   it("serves a query that does not use the join from the table", async () => {
      const run = await fixture.run({
         name: "not through",
         malloy: "run: fl -> { group_by: carrier; aggregate: flight_count }",
      });
      expect(run.servedVia).toBe("served_copy");
      expect(rowDifference(run.live, run.served)).toBeUndefined();
   });
});

// The origin connections of the live retry below.
const crossOrigin = {
   crm: [
      `CREATE TABLE accounts AS SELECT * FROM (VALUES (1, 'acme'), (2, 'globex'), (3, 'initech')) AS t(id, account_name)`,
   ],
   product: [
      `CREATE TABLE usage AS SELECT * FROM (VALUES (1, 5), (1, 7), (2, 1), (9, 4)) AS t(account_id, events)`,
   ],
};

// A served-copy answer that fails at run time is retried live
// (freshnessFallback=live), and the retry is governed again with live's
// #(carries) ceiling. Live was already read before the served copy ran
// whenever it compiles, so the retry's read refuses nothing new; this pins
// that it refuses nothing clean either.
describe("R4: a served-copy answer retried live", () => {
   useMaterializationOn();
   afterAll(async () => {
      await disposeScenarios();
   });

   const liveFallback = (fixture: R4Fixture) =>
      fixture.setBindings(
         fixture.bindings.map((b) => ({
            ...b,
            freshnessFallback: "live" as const,
         })),
      );

   it("answers a clean query live, and refuses one past the ceiling", async () => {
      const fixture = await buildScenario({
         files: {
            "m.malloy": `##! experimental.access_modifiers
#(materialize) storage="lake"
source: accounts is crm.table('accounts') include {
  #(classification) "none"
  public: id
  #(classification) "phi"
  public: account_name
} extend { primary_key: id }

#(carries) ["none"]
source: capped is accounts extend {
  #(classification) "none"
  measure: n is count()
}
`,
         },
         seedSql: [],
         origins: crossOrigin,
         materialize: true,
      });
      liveFallback(fixture);
      const clean = "run: capped -> { group_by: id; aggregate: n }";
      expect((await fixture.runAsIs(clean)).servedFrom).toBe("storage");
      // The stored table goes, so the served copy fails at run time.
      await fixture.storedSQL((table) => `DROP TABLE ${table("accounts")}`);
      const retried = await fixture.runAsIs(clean);
      expect(retried.servedFrom).toBe("live_fallback");
      expect(
         rowDifference(
            [
               { id: 1, n: 1 },
               { id: 2, n: 1 },
               { id: 3, n: 1 },
            ],
            retried.rows,
         ),
      ).toBeUndefined();
      await expect(
         fixture.runAsIs("run: capped -> { group_by: account_name }"),
      ).rejects.toThrow(/carries \[none\]; this query reads account_name/);
   });
});

// A curating index that exports a source whose join the served copy leaves
// out. Before the join is left out, the importer cannot export what its
// import failed to define (`export-name-not-defined`); that follows from the
// refusal, so the join is left out and the export compiles.
const exportedFiles = {
   "index.malloy": `##! experimental { persistence }
import "a.malloy"
export { fl, carriers_copy }
`,
   "a.malloy": `##! experimental { persistence }
source: segments is duckdb.table('segments') extend {
  primary_key: seg_id
  measure: seg_miles is miles.sum()
}

#@ persist name="fl" storage="lake"
source: fl is duckdb.table('flights') -> { select: * } extend {
  primary_key: id
  join_many: segments on segments.flight_id = id
  measure: flight_count is count()
}

#@ persist name="carriers_copy" storage="lake"
source: carriers_copy is duckdb.table('carriers') -> { select: * } extend {
  measure: carrier_count is count()
}
`,
};

describe("R4: an index that exports a source whose join is left out", () => {
   let fixture: R4Fixture;
   useMaterializationOn();
   beforeAll(async () => {
      fixture = await buildScenario({ files: exportedFiles, seedSql });
   });
   afterAll(async () => {
      await disposeScenarios();
   });

   it("serves the other exported source from its table", async () => {
      const run = await fixture.run({
         name: "other",
         malloy:
            "run: carriers_copy -> { group_by: region; aggregate: carrier_count }",
      });
      expect(run.servedVia).toBe("served_copy");
      expect(rowDifference(run.live, run.served)).toBeUndefined();
   });

   it("serves the exported source from its table for a query off the join", async () => {
      const run = await fixture.run({
         name: "not through",
         malloy: "run: fl -> { group_by: carrier; aggregate: flight_count }",
      });
      expect(run.servedVia).toBe("served_copy");
      expect(rowDifference(run.live, run.served)).toBeUndefined();
   });

   it("answers a query through the left-out join live, equal to live", async () => {
      const run = await fixture.run({
         name: "through",
         malloy:
            "run: fl -> { group_by: carrier; aggregate: flight_count, segments.seg_miles }",
      });
      expect(run.servedVia).toBeUndefined();
      expect(rowDifference(run.live, run.served)).toBeUndefined();
   });

   it("names the left-out join, and the copy compiles", async () => {
      const problems = await fixture.servedCopyProblems();
      expect(problems.map((p) => [p.sourceName, p.reason, p.join])).toEqual([
         ["fl", "cross_connection_join", "segments"],
      ]);
   });
});
