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
   colocatedFiles,
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

/**
 * `parentInline`: the `inline=` on the parent's own `#(materialize)`.
 * `joinAccess`: the access label on the parent's join (row 24 with #1661's
 * follow-up), `""` for a public one.
 */
function model(storage: string, parentInline: string, joinAccess = ""): string {
   const flag = joinAccess ? "##! experimental { access_modifiers }\n" : "";
   return `${flag}source: carriers is duckdb.table('carriers') extend {
  primary_key: code
  dimension: loud_name is upper(nickname)
  measure: carrier_count is count()
}

#(materialize) ${storage}${parentInline}name="flights_t"
source: flights is duckdb.table('flights') extend {
  primary_key: id
  ${joinAccess}join_one: carriers with carrier
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

// Colocated too (phase 3 pull request 6): the tables sit in the source's own
// warehouse, so the parent's join kept as written stays on one connection
// and nothing is left out. `chained` (`storage=` only, null colocated): the
// extension builds in the destination over its parent's stored table, the
// parent's inlined join read from its stored columns as a record (#1661's
// follow-up). Without the parent's `inline=`, its table holds no joined
// column, so the extension builds over the raw chain.
for (const [label, storage, parentInline, problems, chained] of [
   [
      "colocated, the parent inlining it too",
      "",
      'inline=["carriers"] ',
      [],
      null,
   ],
   ["colocated, the parent not inlining it", "", "", [], null],
   [
      "storage=, the parent inlining it too",
      'storage="lake" ',
      'inline=["carriers"] ',
      [],
      true,
   ],
   [
      "storage=, the parent not inlining it",
      'storage="lake" ',
      "",
      [["flights", "cross_connection_join", "carriers"]],
      false,
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
         const files = { "m.malloy": model(storage, parentInline) };
         fixture = await buildScenario({
            files: storage ? files : colocatedFiles(files),
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

      if (chained === true) {
         it("builds the extension over the parent's stored table, the join read from its stored columns", () => {
            expect(fixture.buildReads.long_flights).toEqual(["flights"]);
            const sql = fixture.storedBuildSQL.long_flights;
            expect(sql).toContain(fixture.physicalTables.flights);
            expect(sql).toContain("carriers__nickname");
            // Nothing from the warehouse: neither raw table is read.
            expect(sql).not.toMatch(/\bcarriers\b(?!__)/);
            expect(sql).not.toMatch(/\bflights\b/);
            expect(fixture.tableIds.long_flights).toBeDefined();
         });
      } else if (chained === false) {
         // The parent's table holds no joined column, so the stored-chain
         // build cannot read the join over it (its target is not in the
         // destination): the extension builds over the raw chain.
         it("builds the extension over the raw chain", () => {
            expect(fixture.storedBuildSQL.long_flights).toBeUndefined();
            expect(fixture.tableIds.long_flights).toBeDefined();
         });
      }

      it("builds the extension's stored rows exactly as the raw chain would", async () => {
         const stored = await fixture.storedSQL(
            (t) =>
               `SELECT id, shout, carriers__nickname FROM ${t("long_flights")} ORDER BY id`,
         );
         const raw = await fixture.storedSQL(
            () =>
               "SELECT f.id, upper(c.nickname) AS shout, c.nickname AS carriers__nickname " +
               "FROM flights f LEFT JOIN carriers c ON f.carrier = c.code " +
               "WHERE f.distance > 60 ORDER BY f.id",
         );
         expect(rowDifference(raw, stored)).toBeUndefined();
         expect(stored.length).toBe(5);
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

/**
 * The same chain with the parent's join `internal` (row 24 with #1661's
 * follow-up): the parent's table stores the join's columns, the extension
 * builds over that table reading them through the record, and the served
 * record keeps the join `internal`, so a caller reads `carriers.<field>`
 * exactly where live lets it (through its own `extend`, never a plain
 * `group_by:`), while the author's fields and view over the join answer from
 * storage.
 */
describe("R4: an internal inlined join declared in a kept ancestor's block", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
         files: {
            "m.malloy": model(
               'storage="lake" ',
               'inline=["carriers"] ',
               "internal ",
            ),
         },
         seedSql,
         materialize: true,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   it("re-roots both sources with no problem, and chains the extension on its parent's table", async () => {
      expect(await fixture.servedCopyProblems()).toEqual([]);
      expect(fixture.buildReads.long_flights).toEqual(["flights"]);
      const sql = fixture.storedBuildSQL.long_flights;
      expect(sql).toContain(fixture.physicalTables.flights);
      expect(sql).not.toMatch(/\bcarriers\b(?!__)/);
   });

   for (const [name, malloy] of [
      [
         "the extension's own dimension over the join",
         "run: long_flights -> { group_by: shout; aggregate: flight_count }",
      ],
      [
         "the ancestor's dimension over the join",
         "run: long_flights -> { group_by: carrier_region; aggregate: total_distance }",
      ],
      [
         "the ancestor's view through the join",
         "run: long_flights -> by_carrier",
      ],
      [
         "a caller's own extend reading the internal join",
         "run: long_flights extend { dimension: nick is carriers.nickname } -> { group_by: nick; aggregate: flight_count }",
      ],
   ] as const) {
      it(`serves ${name} from storage with live's rows`, async () => {
         const run = await fixture.run({ name, malloy });
         expect(run.live.length).toBeGreaterThan(0);
         expect(rowDifference(run.live, run.served)).toBeUndefined();
         expect(run.servedVia).toBe("served_copy");
      });
   }

   for (const malloy of [
      "run: long_flights -> { group_by: carriers.nickname }",
      "run: flights -> { group_by: carriers.nickname }",
      "run: long_flights -> { select: carriers }",
      "run: long_flights -> { group_by: carriers__nickname__stored }",
      "run: long_flights extend { dimension: x is carriers__nickname__stored } -> { group_by: x }",
   ]) {
      it(`refuses ${malloy}, as live does`, async () => {
         fixture.setBindings([]);
         let live: unknown;
         try {
            await fixture.runAsIs(malloy);
         } catch (e) {
            live = e;
         }
         fixture.setBindings(fixture.bindings);
         let served: unknown;
         try {
            await fixture.runAsIs(malloy);
         } catch (e) {
            served = e;
         }
         expect(live).toBeDefined();
         expect(served).toBeDefined();
         expect(String(served)).not.toMatch(/American|United|Delta/);
      });
   }
});
