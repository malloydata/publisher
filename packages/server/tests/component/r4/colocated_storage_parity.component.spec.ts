// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4: a colocated table and a `storage=` one give the same answer, served the
 * same way, for shapes where they used to differ by accident. Every
 * shape runs in both tiers, compares rows (and field types where they matter)
 * to live, and asserts what served it:
 *
 * 1. A `#(materialize)` source with a zoned timestamp (and a UUID) column is
 *    served from its table, the timestamp typed as live types it: a colocated
 *    table is bound with the columns its build captured that the plan cannot
 *    spell, as a `storage=` one is with every column, where the planned
 *    columns alone left the zoned timestamp out and sent the whole source
 *    live.
 * 2. Two pair tables (an inlined join_many) whose sources compile to the same
 *    SQL are each served as their own source: colocated, they share one table
 *    at one content address, which the pair check once read as a read
 *    through a join.
 * 3. A rollup a join_one reads beside the parent's own aggregates is served
 *    once its build checked the key unique, in a destination as colocated; a
 *    `storage=` rollup's key check was never planned.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   buildScenario,
   DESTINATION,
   disposeScenarios,
   expectPairServedVia,
   rowDifference,
   type R4Fixture,
   type R4Run,
} from "./harness";

/** Each tier: its placement on a declaration, and its `name=` when colocated. */
const TIERS = [
   {
      label: "storage=",
      place: (_name: string) => ` storage="${DESTINATION}"`,
      servedFrom: "storage",
   },
   {
      label: "colocated",
      place: (name: string) => ` name="${name}"`,
      servedFrom: null,
   },
] as const;

/** Field types, without what a stored table cannot carry (KL 12). */
function comparableFields(fields: { name: string; type: unknown }[]) {
   return JSON.parse(
      JSON.stringify(fields, (key, value) =>
         key === "subtype" || key === "annotations" ? undefined : value,
      ),
   );
}

/** A run the served copy answered, as live did, typed as live types it. */
function expectServedCopy(run: R4Run): void {
   expect(run.live.length).toBeGreaterThan(0);
   expect(rowDifference(run.live, run.served)).toBeUndefined();
   expect(comparableFields(run.servedFields)).toEqual(
      comparableFields(run.liveFields),
   );
   expect(run.servedVia).toBe("served_copy");
}

/** A run a rollup answered, as live did. */
function expectServedFromRollup(run: R4Run, servedFrom: string | null): void {
   expect(run.live.length).toBeGreaterThan(0);
   expect(rowDifference(run.live, run.served)).toBeUndefined();
   expect(run.servedBy?.kind).toBe("rollup");
   expect(run.servedFrom).toBe(servedFrom);
}

/** Why each rollup a query considered was not read. */
async function rollupReasons(
   fixture: R4Fixture,
   malloy: string,
): Promise<(string | undefined)[] | undefined> {
   fixture.setBindings(fixture.bindings);
   const { path } = await fixture.runAsIs(malloy);
   return path?.considered
      .filter((c) => c.kind === "rollup")
      .map((c) => c.reason);
}

for (const tier of TIERS) {
   describe(`R4 parity (${tier.label}): a #(materialize) table with a zoned timestamp and a UUID`, () => {
      let restore: () => void;
      let fixture: R4Fixture;
      beforeAll(async () => {
         restore = installSettings({
            env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
         }).restore;
         fixture = await buildScenario({
            files: {
               "m.malloy": `#(materialize)${tier.place("parity_zoned")}
source: s is duckdb.table('ex') extend { primary_key: id }
`,
            },
            seedSql: [
               `CREATE TABLE ex AS SELECT * FROM (VALUES
                  (1, 'a', 1.5, '11111111-2222-3333-4444-555555555555'::UUID, TIMESTAMPTZ '2024-01-02 03:04:05+00'),
                  (2, 'b', 2.5, NULL, NULL),
                  (3, 'a', 3.5, '31111111-2222-3333-4444-555555555555'::UUID, TIMESTAMPTZ '2024-06-30 23:00:00+00')
                ) AS t(id, g, amount, x, seen)`,
            ],
            materialize: true,
         });
      });
      afterAll(async () => {
         restore();
         await disposeScenarios();
      });

      it("binds the source with no problem", async () => {
         expect(await fixture.servedCopyProblems()).toEqual([]);
      });

      it.each([
         "run: s -> { group_by: g; aggregate: n is count(), total is amount.sum(); order_by: g }",
         "run: s -> { select: id, seen; order_by: id }",
         "run: s -> { timezone: 'America/Los_Angeles'; group_by: d is seen.day; aggregate: n is count(); order_by: d }",
      ])("serves, typed and equal to live: %s", async (malloy) => {
         expectServedCopy(await fixture.run({ name: malloy, malloy }));
      });

      it("answers a query reading the UUID live, equal to live", async () => {
         const malloy = "run: s -> { select: id, x; order_by: id }";
         const run = await fixture.run({
            name: malloy,
            malloy,
            expectLive: true,
         });
         expect(run.live.length).toBe(3);
         expect(rowDifference(run.live, run.served)).toBeUndefined();
         expect(run.servedVia).toBeUndefined();
      });
   });

   describe(`R4 parity (${tier.label}): two pair tables whose sources compile to the same SQL`, () => {
      let restore: () => void;
      let fixture: R4Fixture;
      const pairSource = (
         name: string,
      ) => `#(materialize)${tier.place(`parity_${name}`)} inline=["segs"]
source: ${name} is duckdb.table('flights') extend {
  primary_key: id
  join_many: segs on segs.flight_id = id
  measure:
    flight_count is count()
    seg_w is segs.w.sum()
    seg_max is segs.w.max()
}`;
      beforeAll(async () => {
         restore = installSettings({
            env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
         }).restore;
         fixture = await buildScenario({
            files: {
               "m.malloy": `source: segs is duckdb.table('segs') extend { primary_key: seg_id }

${pairSource("pairs_a")}

${pairSource("pairs_b")}
`,
            },
            seedSql: [
               `CREATE TABLE flights AS SELECT * FROM (VALUES
                  (1, 'AA', 100), (2, 'AA', 200), (3, 'BB', 300), (4, 'BB', 50)
                ) AS t(id, carrier, distance)`,
               `CREATE TABLE segs AS SELECT * FROM (VALUES
                  (10, 1, 2.0), (11, 1, 3.0), (12, 2, 4.0), (13, 3, 1.0), (14, 3, 6.0)
                ) AS t(seg_id, flight_id, w)`,
            ],
            materialize: true,
         });
      });
      afterAll(async () => {
         restore();
         await disposeScenarios();
      });

      for (const source of ["pairs_a", "pairs_b"]) {
         it(`serves ${source}'s child measures from its pair table, equal to live`, async () => {
            const run = await fixture.run({
               name: source,
               malloy: `run: ${source} -> { group_by: carrier; aggregate: seg_w, seg_max; order_by: carrier }`,
            });
            expectPairServedVia(run, []);
            expect(rowDifference(run.live, run.served)).toBeUndefined();
         });
      }

      it("serves parent and child measures together, equal to live", async () => {
         const run = await fixture.run({
            name: "mixed",
            malloy:
               "run: pairs_b -> { group_by: carrier; aggregate: flight_count, seg_w; order_by: carrier }",
         });
         expectPairServedVia(run, []);
         expect(rowDifference(run.live, run.served)).toBeUndefined();
      });
   });

   describe(`R4 parity (${tier.label}): a rollup read beside a join_one, its key checked at build`, () => {
      const place = tier.label === "storage=" ? tier.place("") : "";
      // The query source is read in the rollup's destination for a
      // `storage=` rollup (#1704), so it is stored there too.
      const model = `source: crew is duckdb.table('crew_raw') extend {
  primary_key: flight_id
  #(preaggregate) grain=["flight_id"]${place}
  measure: crew_size is size.sum()
}
${tier.label === "storage=" ? `#(materialize)${place}\n` : ""}source: flights is duckdb.table('flights_raw') extend {
  primary_key: id
  join_one: crew with id
  measure: flight_count is count()
  measure: total_distance is distance.sum()
}
`;
      const flights = `CREATE TABLE flights_raw AS SELECT * FROM (VALUES
         (1, 'AA', 100), (2, 'AA', 200), (3, 'BB', 300), (4, 'BB', 50), (5, 'CC', 70)
       ) AS t(id, carrier, distance)`;
      // At most one per flight: flights 2 and 4 have none.
      const unique = `CREATE TABLE crew_raw AS SELECT * FROM (VALUES
         (1, 3), (3, 2), (5, 4)
       ) AS t(flight_id, size)`;
      // Flight 3 held twice: the join_one is not unique in the data.
      const repeated = `CREATE TABLE crew_raw AS SELECT * FROM (VALUES
         (1, 3), (3, 2), (3, 5), (5, 4)
       ) AS t(flight_id, size)`;
      const malloy =
         "run: flights -> { group_by: carrier; aggregate: flight_count, total_distance, crew.crew_size; order_by: carrier }";
      let restore: () => void;
      let checked: R4Fixture;
      let broken: R4Fixture;
      let unchecked: R4Fixture;
      beforeAll(async () => {
         restore = installSettings({
            env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
         }).restore;
         const scenario = (crew: string, rollupKeyChecks = true) =>
            buildScenario({
               files: { "m.malloy": model },
               seedSql: [flights, crew],
               materialize: tier.label === "storage=",
               preaggregate: true,
               rollupKeyChecks,
            });
         checked = await scenario(unique);
         broken = await scenario(repeated);
         unchecked = await scenario(unique, false);
      });
      afterAll(async () => {
         restore();
         await disposeScenarios();
      });

      it("serves the parent's own aggregates beside the join_one from the rollup, equal to live", async () => {
         expectServedFromRollup(
            await checked.run({ name: "checked", malloy }),
            tier.servedFrom,
         );
      });

      it("answers live, equal to live, when the build found the key repeated", async () => {
         const run = await broken.run({ name: "repeated", malloy });
         expect(rowDifference(run.live, run.served)).toBeUndefined();
         expect(run.servedBy?.kind).not.toBe("rollup");
         expect(await rollupReasons(broken, malloy)).toContain(
            "COUNT_SEMANTICS",
         );
      });

      it("answers live, equal to live, when no build checked the key", async () => {
         const run = await unchecked.run({ name: "unchecked", malloy });
         expect(rowDifference(run.live, run.served)).toBeUndefined();
         expect(run.servedBy?.kind).not.toBe("rollup");
         expect(await rollupReasons(unchecked, malloy)).toContain(
            "COUNT_SEMANTICS",
         );
      });
   });

}
