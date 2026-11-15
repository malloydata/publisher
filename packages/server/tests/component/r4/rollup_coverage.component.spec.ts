// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4 for rollup cases the other specs do not reach, each answered against
 * live and, where it must be, from the rollup:
 *
 *  - join-to-rollup through a v1 `#@ preaggregate`, colocated and in a
 *    `storage=` destination beside the joining source;
 *  - a rollup on a zoned source read through a join from a source in the
 *    same zone, the one reader a zoned rollup has (v1 and v2);
 *  - the column types a rollup stores (v1 and v2): a BIGINT partial past
 *    2^53, a TIMESTAMPTZ grain, a DECIMAL partial and grain; all exact,
 *    colocated and in a `storage=` destination.
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

/** Field types with each number's subtype left out. */
function withoutSubtype(fields: { name: string; type: unknown }[]): unknown {
   return JSON.parse(
      JSON.stringify(fields, (key, value) =>
         key === "subtype" ? undefined : value,
      ),
   );
}

const SETTINGS = {
   env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
};

/** Served from a rollup, with live's rows by both comparators. */
function expectRollup(run: R4Run): void {
   expect(rowDifference(run.live, run.served)).toBeUndefined();
   expect(strictRowDifference(run.live, run.served)).toBeUndefined();
   expect(run.servedBy?.kind).toBe("rollup");
}

const FLIGHTS_SEED = [
   `CREATE TABLE flights_raw AS SELECT * FROM (VALUES
      (1, 'AA', TIMESTAMP '2024-03-15 02:30:00'),
      (2, 'AA', TIMESTAMP '2024-03-15 04:10:00'),
      (3, 'BB', TIMESTAMP '2024-07-20 23:45:00'),
      (4, 'CC', TIMESTAMP '2024-11-03 05:30:00')
    ) AS t(id, carrier, departed)`,
   // Flight 4 has no segments; segment 15 has no miles; 17 has no flight.
   `CREATE TABLE segs_raw AS SELECT * FROM (VALUES
      (10, 1, 2.0), (11, 1, 3.0), (12, 2, 4.0), (13, 3, 1.5),
      (15, 3, NULL), (17, 9, 50.0)
    ) AS t(seg_id, flight_id, miles)`,
];

for (const [label, placement] of [
   ["colocated", ""],
   ["storage=", ` storage="${DESTINATION}"`],
] as const) {
   describe(`R4: join-to-rollup through a v1 #@ preaggregate [${label}]`, () => {
      let restore: () => void;
      let fixture: R4Fixture;
      beforeAll(async () => {
         restore = installSettings(SETTINGS).restore;
         fixture = await buildScenario({
            files: {
               "m.malloy": `##! experimental { persistence }
source: segments is duckdb.table('segs_raw') extend {
  primary_key: seg_id
  #@ preaggregate grain="flight_id"${placement}
  measure: seg_miles is miles.sum()
  #@ preaggregate grain="flight_id"${placement}
  measure: seg_count is count()
}
${
   placement
      ? `#@ persist storage="${DESTINATION}"
source: flights is duckdb.table('flights_raw') -> { select: * } extend {`
      : `source: flights is duckdb.table('flights_raw') extend {`
}
  primary_key: id
  join_many: segments on segments.flight_id = id
}
`,
            },
            seedSql: FLIGHTS_SEED,
            preaggregate: true,
         });
      });
      afterAll(async () => {
         restore();
         await disposeScenarios();
      });
      for (const [name, malloy] of [
         [
            "a sum through a join_many, by a parent dimension",
            "run: flights -> { group_by: carrier; aggregate: segments.seg_miles }",
         ],
         [
            "a count through a join_many",
            "run: flights -> { group_by: carrier; aggregate: segments.seg_count }",
         ],
      ] as const) {
         it(`${name}: served from the rollup, equal to live`, async () => {
            expectRollup(await fixture.run({ name, malloy }));
         });
      }
   });
}

for (const [label, route] of [
   ["v2 #(preaggregate)", `#(preaggregate) grain=["flight_id"]`],
   ["v1 #@ preaggregate", `#@ preaggregate grain="flight_id"`],
] as const) {
   describe(`R4: a ${label} rollup on a zoned source, read through a same-zone join`, () => {
      let restore: () => void;
      let fixture: R4Fixture;
      beforeAll(async () => {
         restore = installSettings(SETTINGS).restore;
         fixture = await buildScenario({
            files: {
               "m.malloy": `##! experimental { persistence }
source: segments_ny is duckdb.table('segs_raw') extend {
  primary_key: seg_id
  timezone: 'America/New_York'
  ${route}
  measure: seg_miles is miles.sum()
}
source: flights_ny is duckdb.table('flights_raw') extend {
  primary_key: id
  timezone: 'America/New_York'
  join_many: segments_ny on segments_ny.flight_id = id
}
`,
            },
            seedSql: FLIGHTS_SEED,
            preaggregate: true,
         });
      });
      afterAll(async () => {
         restore();
         await disposeScenarios();
      });
      it("a sum by a parent dimension: served from the rollup, equal to live", async () => {
         expectRollup(
            await fixture.run({
               name: "by carrier",
               malloy:
                  "run: flights_ny -> { group_by: carrier; aggregate: segments_ny.seg_miles }",
            }),
         );
      });
      it("a sum by a zoned truncation of the parent: served from the rollup, equal to live", async () => {
         expectRollup(
            await fixture.run({
               name: "by departure day",
               malloy:
                  "run: flights_ny -> { group_by: d is departed.day; aggregate: segments_ny.seg_miles; order_by: d }",
            }),
         );
      });
      it("the zoned source on its own: live (TIME_ZONE), equal to live", async () => {
         const malloy =
            "run: segments_ny -> { group_by: flight_id; aggregate: seg_miles }";
         const run = await fixture.run({
            name: "the rolled-up source",
            malloy,
         });
         expect(rowDifference(run.live, run.served)).toBeUndefined();
         expect(run.servedBy?.kind).toBe("live");
         fixture.setBindings(fixture.bindings);
         const { path } = await fixture.runAsIs(malloy);
         expect(
            (path?.considered ?? []).map((c) => [c.kind, c.reason]),
         ).toEqual([["rollup", "TIME_ZONE"]]);
      });
   });
}

const TYPED_SEED = [
   `CREATE TABLE typed AS SELECT * FROM (VALUES
      (CAST('9007199254740993' AS BIGINT), TIMESTAMPTZ '2024-01-01 10:00:00+00', 'a', CAST(1.25 AS DECIMAL(10,2))),
      (CAST('9007199254740991' AS BIGINT), TIMESTAMPTZ '2024-01-01 10:00:00+00', 'a', CAST(1.25 AS DECIMAL(10,2))),
      (CAST('-9007199254740993' AS BIGINT), TIMESTAMPTZ '2024-06-30 23:30:00+00', 'b', CAST(2.50 AS DECIMAL(10,2)))
    ) AS t(big, tstz, g, dec)`,
];

const ROUTES = {
   v2: (grain: string, placement: string) =>
      `#(preaggregate) grain=["${grain}"]${placement}`,
   v1: (grain: string, placement: string) =>
      `#@ preaggregate grain="${grain}"${placement}`,
};

for (const [route, annotate] of Object.entries(ROUTES)) {
   for (const [label, placement] of [
      ["colocated", ""],
      ["storage=", ` storage="${DESTINATION}"`],
   ] as const) {
      describe(`R4: the column types a ${route} rollup stores [${label}]`, () => {
         let restore: () => void;
         let fixture: R4Fixture;
         beforeAll(async () => {
            restore = installSettings(SETTINGS).restore;
            fixture = await buildScenario({
               files: {
                  "m.malloy": `##! experimental { persistence }
source: typed is duckdb.table('typed') extend {
  ${annotate("g", placement)}
  measure: big_sum is big.sum()
  ${annotate("tstz", placement)}
  measure: n_by_tstz is count()
  ${annotate("g", placement)}
  measure: dec_sum is dec.sum()
  ${annotate("dec", placement)}
  measure: n_by_dec is count()
}
`,
               },
               seedSql: TYPED_SEED,
               preaggregate: true,
            });
         });
         afterAll(async () => {
            restore();
            await disposeScenarios();
         });
         for (const [name, malloy] of [
            [
               "a BIGINT sum past 2^53",
               "run: typed -> { group_by: g; aggregate: big_sum; order_by: g }",
            ],
            [
               "a TIMESTAMPTZ grain",
               "run: typed -> { group_by: tstz; aggregate: n_by_tstz; order_by: tstz }",
            ],
            [
               "a DECIMAL sum",
               "run: typed -> { group_by: g; aggregate: dec_sum; order_by: g }",
            ],
            [
               "a DECIMAL grain",
               "run: typed -> { group_by: dec; aggregate: n_by_dec; order_by: dec }",
            ],
         ] as const) {
            it(`${name}: served from the rollup, rows and types equal to live`, async () => {
               const run = await fixture.run({ name, malloy });
               expectRollup(run);
               // A stored number loses its integer/decimal subtype (known
               // limitation 12), so subtypes are not compared.
               expect(withoutSubtype(run.servedFields)).toEqual(
                  withoutSubtype(run.liveFields),
               );
            });
         }
      });
   }
}
