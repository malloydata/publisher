// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4: a database table and a `storage=` one give the same answer, served the
 * same way, for shapes where they used to differ by accident. Every
 * shape runs in both tiers, compares rows (and field types where they matter)
 * to live, and asserts what served it:
 *
 * 1. A `#(materialize)` source with a zoned timestamp (and a UUID) column is
 *    served from its table, the timestamp typed as live types it: a database
 *    table is bound with the columns its build captured that the plan cannot
 *    spell, as a `storage=` one is with every column, where the planned
 *    columns alone left the zoned timestamp out and sent the whole source
 *    live.
 * 2. Two pair tables (an inlined join_many) whose sources compile to the same
 *    SQL are each served as their own source: database, they share one table
 *    at one content address, which the pair check once read as a read
 *    through a join.
 * 3. A rollup a join_one reads beside the parent's own aggregates is served
 *    once its build checked the key unique, in a destination as database; a
 *    `storage=` rollup's key check was never planned.
 * 4. A rollup over a source in its own time zone is built only where the join
 *    leg reads it: joined from a source in the same zone, which for a
 *    `storage=` rollup must be stored in its destination. Otherwise it is
 *    refused at plan (`preaggregate-time-zone`), in both tiers.
 * 5. An incremental pair table refreshes in place in DuckDB only. A database
 *    one in another warehouse is planned as a full rebuild with the package
 *    warning `materialize-pair-full-refresh`, not rebuilt silently; it serves
 *    as live does. On Postgres when `PUBLISHER_TEST_POSTGRES_URL` names one.
 */

import { PostgresConnection } from "@malloydata/db-postgres";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { useMaterializationOn } from "../../../src/test_helpers/settings_env";
import {
   buildScenario,
   DESTINATION,
   disposeScenarios,
   expectPairServedVia,
   rowDifference,
   type R4Fixture,
   type R4Run,
   itLeavesNoServedCopyProblem,
} from "./harness";

const POSTGRES_URL = process.env.PUBLISHER_TEST_POSTGRES_URL;
const PG_SCHEMA = "r4_parity";

/** Each tier: its placement on a declaration, and its `name=` when database. */
const TIERS = [
   {
      label: "storage=",
      place: (_name: string) => ` storage="${DESTINATION}"`,
      servedFrom: "storage",
   },
   {
      label: "database",
      place: (name: string) => ` name="${name}"`,
      servedFrom: "database",
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
      let fixture: R4Fixture;
      useMaterializationOn();
      beforeAll(async () => {
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
         await disposeScenarios();
      });

      itLeavesNoServedCopyProblem(() => fixture);

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
      useMaterializationOn();
      beforeAll(async () => {
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
  #(rollup) grain=["flight_id"]${place}
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
      let checked: R4Fixture;
      let broken: R4Fixture;
      let unchecked: R4Fixture;
      useMaterializationOn();
      beforeAll(async () => {
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

   describe(`R4 parity (${tier.label}): a rollup over a source in its own time zone`, () => {
      const storage = tier.label === "storage=";
      const place = storage ? tier.place("") : "";
      const zoned = (
         name: string,
      ) => `source: ${name} is duckdb.table('segs_raw') extend {
  timezone: 'America/Los_Angeles'
  primary_key: seg_id
  #(rollup) grain=["flight_id"]${place}
  measure: seg_miles is miles.sum()
}`;
      // `read_segs` is joined from a source in its zone the join leg reads
      // it for: on its own connection beside a database rollup, stored in
      // the destination beside a `storage=` one. `unread_segs` is joined only
      // from one the leg never reads it for: in another zone beside a
      // database rollup; in its zone, but read on its own connection,
      // beside a `storage=` one.
      const model = `${zoned("read_segs")}
${zoned("unread_segs")}
${storage ? `#(materialize)${place}\n` : ""}source: flights_la is duckdb.table('flights_raw') extend {
  timezone: 'America/Los_Angeles'
  primary_key: id
  join_many: read_segs on read_segs.flight_id = id
}
source: flights_unread is duckdb.table('flights_raw') extend {
  ${storage ? "timezone: 'America/Los_Angeles'" : "timezone: 'America/New_York'"}
  primary_key: id
  join_many: unread_segs on unread_segs.flight_id = id
}
`;
      let fixture: R4Fixture;
      useMaterializationOn();
      beforeAll(async () => {
         fixture = await buildScenario({
            files: { "m.malloy": model },
            seedSql: [
               `CREATE TABLE flights_raw AS SELECT * FROM (VALUES
                  (1, 'AA', 100), (2, 'AA', 200), (3, 'BB', 300)
                ) AS t(id, carrier, distance)`,
               `CREATE TABLE segs_raw AS SELECT * FROM (VALUES
                  (10, 1, 2.0), (11, 1, 3.0), (12, 2, 4.0), (13, 3, 1.0)
                ) AS t(seg_id, flight_id, miles)`,
            ],
            materialize: storage,
            preaggregate: true,
         });
      });
      afterAll(async () => {
         await disposeScenarios();
      });

      it("builds the rollup a join from the same zone reads, and serves it, equal to live", async () => {
         expectServedFromRollup(
            await fixture.run({
               name: "read",
               malloy:
                  "run: flights_la -> { group_by: carrier; aggregate: read_segs.seg_miles; order_by: carrier }",
            }),
            tier.servedFrom,
         );
      });

      it("refuses at plan the rollup no join reads, and answers it live, equal to live", async () => {
         expect(fixture.rollupRefusals).toContainEqual({
            reason: "time_zone",
            sourceName: "unread_segs",
         });
         expect(
            fixture.rollupRefusals.filter((r) => r.sourceName === "read_segs"),
         ).toEqual([]);
         const run = await fixture.run({
            name: "unread",
            malloy:
               "run: flights_unread -> { group_by: carrier; aggregate: unread_segs.seg_miles; order_by: carrier }",
            expectLive: true,
         });
         expect(run.live.length).toBeGreaterThan(0);
         expect(rowDifference(run.live, run.served)).toBeUndefined();
         expect(run.servedBy?.kind).not.toBe("rollup");
      });
   });
}

/**
 * Fix 5: an incremental pair table. In a `storage=` destination (DuckDB) its
 * plan keeps the incremental keys and the package says nothing; database in
 * a Postgres warehouse it is planned as a full rebuild and the package says
 * so. Both serve as live does.
 */
const PAIR_SEED = (schema: string) => [
   `CREATE TABLE ${schema}flights AS SELECT * FROM (VALUES
      (1, 'AA', 100, DATE '2026-11-01'), (2, 'AA', 200, DATE '2026-11-02'),
      (3, 'BB', 300, DATE '2026-11-03'), (4, 'BB', 50, DATE '2026-11-04')
    ) AS t(id, carrier, distance, updated_on)`,
   `CREATE TABLE ${schema}segs AS SELECT * FROM (VALUES
      (10, 1, 2.0, DATE '2026-11-01'), (11, 1, 3.0, DATE '2026-11-02'),
      (12, 2, 4.0, DATE '2026-11-03'), (13, 3, 1.0, DATE '2026-11-04')
    ) AS t(seg_id, flight_id, w, loaded_on)`,
];
const pairModel = (w: string, schema: string, place: string) =>
   `source: segs is ${w}.table('${schema}segs') extend { primary_key: seg_id }

#(materialize)${place} inline=["segs"] refresh="incremental" watermark=["updated_on", "segs.loaded_on"]
source: flights is ${w}.table('${schema}flights') extend {
  primary_key: id
  join_many: segs on segs.flight_id = id
  measure: seg_w is segs.w.sum()
}
`;
const PAIR_TIERS = [
   {
      label: "storage=, from DuckDB",
      incremental: true,
      scenario: async () => ({
         files: {
            "m.malloy": pairModel("duckdb", "", ` storage="${DESTINATION}"`),
         },
         seedSql: PAIR_SEED(""),
         materialize: true,
      }),
   },
   ...(POSTGRES_URL
      ? [
           {
              label: "database, in Postgres",
              incremental: false,
              scenario: async () => {
                 const pg = new PostgresConnection({
                    name: "pg",
                    connectionString: POSTGRES_URL,
                 });
                 await pg.runSQL(`DROP SCHEMA IF EXISTS ${PG_SCHEMA} CASCADE`);
                 await pg.runSQL(`CREATE SCHEMA ${PG_SCHEMA}`);
                 return {
                    files: {
                       "m.malloy": pairModel(
                          "pg",
                          `${PG_SCHEMA}.`,
                          ` name="parity_pairs"`,
                       ),
                    },
                    seedSql: PAIR_SEED(`${PG_SCHEMA}.`),
                    materialize: true,
                    warehouse: {
                       name: "pg",
                       connection: pg,
                       schema: PG_SCHEMA,
                    },
                    cleanup: async () => {
                       await pg.runSQL(
                          `DROP SCHEMA IF EXISTS ${PG_SCHEMA} CASCADE`,
                       );
                       await pg.close();
                    },
                 };
              },
           },
        ]
      : []),
];

for (const tier of PAIR_TIERS) {
   describe(`R4 parity (${tier.label}): an incremental pair table`, () => {
      let fixture: R4Fixture;
      let cleanup: (() => Promise<void>) | undefined;
      useMaterializationOn();
      beforeAll(async () => {
         const scenario = await tier.scenario();
         cleanup = (scenario as { cleanup?: () => Promise<void> }).cleanup;
         fixture = await buildScenario(scenario);
      });
      afterAll(async () => {
         await disposeScenarios();
         await cleanup?.();
      });

      it(
         tier.incremental
            ? "plans it incremental, with no warning"
            : "plans it as a full rebuild, saying why (materialize-pair-full-refresh)",
         () => {
            const plan = fixture.materializePlans.flights;
            expect(plan).toBeDefined();
            if (tier.incremental) {
               expect(plan.pairFullRefresh).toBeUndefined();
               expect(plan.persist.refresh).toBe("incremental");
            } else {
               expect(plan.pairFullRefresh).toMatch(
                  /in its source's warehouse \(dialect postgres\)/,
               );
               expect(plan.persist.refresh).toBeUndefined();
            }
         },
      );

      it("serves child measures from the pair table, equal to live", async () => {
         const run = await fixture.run({
            name: "pair",
            malloy:
               "run: flights -> { group_by: carrier; aggregate: seg_w; order_by: carrier }",
         });
         expectPairServedVia(run, []);
         expect(rowDifference(run.live, run.served)).toBeUndefined();
      });
   });
}
