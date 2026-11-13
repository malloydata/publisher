// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4 for an inlined join_one declared in a kept ancestor's block: an
 * extension table (P10) whose `inline=` names a join its parent declares. The
 * extension's re-root keeps its ancestors' blocks, so the join's definition is
 * in the kept text, not the extension's own: the served copy takes it out of
 * the kept blocks and puts the record of stored columns in its place, as it
 * does for a join in the source's own text. Every answer equals live.
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
   `CREATE TABLE carriers AS SELECT * FROM (VALUES
      ('AA', 'American', 'east', 10.0),
      ('UA', 'United', 'west', 20.0),
      ('DL', 'Delta', 'east', 30.0)
    ) AS t(code, nickname, region, fee)`,
   `CREATE TABLE flights AS SELECT * FROM (VALUES
      (1, 'AA', 100.0), (2, 'AA', 250.0), (3, 'UA', 300.0),
      (4, 'DL', 50.0), (5, 'XX', 75.0), (6, 'UA', 120.0), (7, NULL, 10.0)
    ) AS t(id, carrier, distance)`,
];

/** `parentInline`: the `inline=` on the parent's own `#(materialize)`. */
function model(storage: string, parentInline: string): string {
   return `source: carriers is duckdb.table('carriers') extend {
  primary_key: code
  dimension: loud_name is upper(nickname)
  measure: carrier_count is count()
}

#(materialize) ${storage}${parentInline}name="flights_t"
source: flights is duckdb.table('flights') extend {
  primary_key: id
  join_one: carriers with carrier
  dimension: carrier_region is carriers.region
  measure:
    flight_count is count()
    total_distance is distance.sum()
    fee_per_flight is sum(carriers.fee)
  view: by_carrier is {
    group_by: carriers.nickname
    aggregate: flight_count, total_distance
  }
}

#(materialize) ${storage}inline=["carriers"] name="long_t"
source: long_flights is flights extend {
  where: distance > 60
  dimension: shout is upper(carriers.nickname)
}
`;
}

const QUERIES = [
   [
      "a joined dimension",
      "run: long_flights -> { group_by: carriers.nickname; aggregate: flight_count, total_distance }",
   ],
   [
      "the extension's own dimension over the join",
      "run: long_flights -> { group_by: shout; aggregate: flight_count }",
   ],
   [
      "the ancestor's dimension over the join",
      "run: long_flights -> { group_by: carrier_region; aggregate: fee_per_flight }",
   ],
   [
      "a joined source's computed dimension",
      "run: long_flights -> { group_by: carriers.loud_name; aggregate: total_distance }",
   ],
   ["the ancestor's view through the join", "run: long_flights -> by_carrier"],
] as const;

// `storage=` only: a colocated `#(materialize)` is not built
// (`COLOCATED_NOT_SERVED`, #1666), so there is no colocated extension table.
for (const [label, storage, parentInline, problems] of [
   [
      "storage=, the parent inlining it too",
      'storage="lake" ',
      'inline=["carriers"] ',
      [],
   ],
   [
      "storage=, the parent not inlining it",
      'storage="lake" ',
      "",
      [["flights", "cross_connection_join", "carriers"]],
   ],
] as const) {
   // Without the parent's `inline=`, the parent's own copy keeps its join as
   // written, which crosses connections; the extension's copy does not.
   describe(`R4: an inlined join declared in a kept ancestor's block (${label})`, () => {
      let restore: () => void;
      let fixture: R4Fixture;
      beforeAll(async () => {
         restore = installSettings({
            env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
         }).restore;
         fixture = await buildScenario({
            files: { "m.malloy": model(storage, parentInline) },
            seedSql,
            materialize: true,
         });
      });
      afterAll(async () => {
         restore();
         await disposeScenarios();
      });

      it("replaces the ancestor's join with the record: nothing kept as written", async () => {
         const found = await fixture.servedCopyProblems();
         expect(found.map((p) => [p.sourceName, p.reason, p.join])).toEqual(
            problems.map((p) => [...p]),
         );
      });

      // The stored-chain build cannot read the joined source over the
      // parent's stored table (the join's target is not in it), so the
      // extension builds over the raw chain, which is correct.
      it("builds the extension over the raw chain", () => {
         expect(fixture.storedBuildSQL.long_flights).toBeUndefined();
         expect(fixture.tableIds.long_flights).toBeDefined();
      });

      for (const [name, malloy] of QUERIES) {
         it(`serves ${name} from the extension's table`, async () => {
            const run = await fixture.run({ name, malloy });
            expect(rowDifference(run.live, run.served)).toBeUndefined();
            expect(run.servedVia).toBe("served_copy");
            fixture.setBindings(fixture.bindings);
            const { path } = await fixture.runAsIs(malloy);
            expect(path?.servedBy).toMatchObject({
               kind: "materialized",
               tableId: fixture.tableIds.long_flights,
               via: "served_copy",
            });
         });
      }
   });
}
