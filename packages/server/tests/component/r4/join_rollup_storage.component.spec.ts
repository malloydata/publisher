// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4 for join-to-rollup in a storage destination (materialization v2, A7,
 * known limitation 26): a joined source's `#(preaggregate) storage=` rollup is
 * read through the join when the query's source is stored in the same
 * destination (a `#(materialize) storage=` table, or a `#@ persist storage=`
 * source) and served through the served copy. The variant declares the query
 * source as the served copy does, over its stored table, with the join
 * re-pointed at the rollup, so the whole query runs on the destination; every
 * answer equals live. A query source not stored there, or stored but stale,
 * still declines with `CROSS_CONNECTION`, and the leg's own declines keep
 * their reasons.
 *
 * The fixture keeps parents with no joined rows (flight 4), a joined row
 * whose measure is NULL (flight 5), and a joined row with no parent
 * (segment 17), where a rollup and a join disagree.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   buildScenario,
   DESTINATION,
   disposeScenarios,
   rowDifference,
   type R4Fixture,
   type R4Run,
} from "./harness";

const seedSql = [
   `CREATE TABLE flights_raw AS SELECT * FROM (VALUES
      (1, 'AA', 100, TIMESTAMP '2026-11-09 10:00:00'),
      (2, 'AA', 200, TIMESTAMP '2026-10-01 10:00:00'),
      (3, 'BB', 300, TIMESTAMP '2026-11-01 10:00:00'),
      (4, 'CC', 400, TIMESTAMP '2026-11-08 10:00:00'),
      (5, 'BB', 500, TIMESTAMP '2026-10-05 10:00:00')
    ) AS t(id, carrier, distance, dep_ts)`,
   // Flight 4 has no segments; flight 5's one segment has no miles; segment
   // 17 belongs to no flight.
   `CREATE TABLE segs_raw AS SELECT * FROM (VALUES
      (10, 1, 'p', 2.0), (11, 1, 'q', 3.0), (12, 2, 'p', 4.0), (13, 2, 'p', 1.5),
      (14, 3, 'q', 1.0), (15, 5, 'p', NULL), (16, 3, 'p', 7.0), (17, 9, 'p', 50.0)
    ) AS t(seg_id, flight_id, kind, miles)`,
   // At most one per flight: flights 2 and 4 have none.
   `CREATE TABLE crew_raw AS SELECT * FROM (VALUES
      (1, 3, 40.0), (3, 2, 35.0), (5, 4, NULL)
    ) AS t(flight_id, size, duty_hours)`,
];

const S = ` storage="${DESTINATION}"`;

/** The joined sources, with their rollups in the destination. */
const children = `source: segments is duckdb.table('segs_raw') extend {
  primary_key: seg_id
  #(preaggregate) grain=["flight_id"]${S}
  #(preaggregate) grain=["flight_id", "kind"]${S}
  measure: seg_miles is miles.sum()
  #(preaggregate) grain=["flight_id"]${S}
  #(preaggregate) grain=["flight_id", "kind"]${S}
  measure: longest is miles.max()
  #(preaggregate) grain=["flight_id"]${S}
  measure: shortest is miles.min()
  #(preaggregate) grain=["flight_id"]${S}
  measure: p_miles is miles.sum() { where: kind = 'p' }
  #(preaggregate) grain=["flight_id"]${S}
  measure: spread is longest - shortest
  #(preaggregate) grain=["flight_id"]${S}
  measure: seg_count is count()
}

source: crew is duckdb.table('crew_raw') extend {
  primary_key: flight_id
  #(preaggregate) grain=["flight_id"]${S}
  measure: crew_size is size.sum()
  #(preaggregate) grain=["flight_id"]${S}
  measure: crew_count is count()
  #(preaggregate) grain=["flight_id"]${S}
  measure: most_hours is duty_hours.max()
}
`;

/** The query source's own fields, the same however it is stored. */
const flightFields = `  primary_key: id
  join_many: segments on segments.flight_id = id
  join_one: crew with id
  internal dimension: departed is dep_ts
  dimension: recent is departed >= @2026-11-01
  measure: flight_count is count()
  measure: total_distance is distance.sum()
  measure: total_seg_miles is segments.seg_miles
  view: by_carrier is { group_by: carrier; aggregate: segments.seg_miles }`;

const materializeModel = `##! experimental.access_modifiers
${children}
#(materialize)${S}
source: flights is duckdb.table('flights_raw') extend {
${flightFields}
}

source: flights_live is duckdb.table('flights_raw') extend {
  primary_key: id
  join_many: segments on segments.flight_id = id
}
`;

const persistModel = `##! experimental { persistence access_modifiers }
${children}
#@ persist name="flights_p"${S}
source: flights is duckdb.table('flights_raw') -> { select: * } extend {
${flightFields}
}
`;

/** A served run read a rollup and answered as live did. */
function expectServedFromRollup(run: R4Run): void {
   expect(run.servedBy?.kind).toBe("rollup");
   expect(run.servedFrom).toBe("storage");
   expect(run.live.length).toBeGreaterThan(0);
   expect(rowDifference(run.live, run.served)).toBeUndefined();
}

/** Answered without the rollup, equal to live. */
function expectNotFromRollup(run: R4Run): void {
   expect(run.servedBy?.kind).not.toBe("rollup");
   expect(rowDifference(run.live, run.served)).toBeUndefined();
}

async function rollupReasons(
   fixture: R4Fixture,
   malloy: string,
   givens?: Record<string, string>,
): Promise<(string | undefined)[] | undefined> {
   fixture.setBindings(fixture.bindings);
   const { path } = await fixture.runAsIs(malloy, givens);
   return path?.considered
      .filter((c) => c.kind === "rollup")
      .map((c) => c.reason);
}

const SERVED = [
   [
      "a sum through a join_many, by a parent dimension",
      "run: flights -> { group_by: carrier; aggregate: segments.seg_miles }",
   ],
   [
      "a max and a min through a join_many",
      "run: flights -> { group_by: carrier; aggregate: segments.longest, segments.shortest }",
   ],
   [
      "a filtered measure",
      "run: flights -> { group_by: carrier; aggregate: segments.p_miles }",
   ],
   [
      "a measure of measures",
      "run: flights -> { group_by: carrier; aggregate: segments.spread }",
   ],
   [
      "an expression written over joined measures",
      "run: flights -> { group_by: carrier; aggregate: r is segments.seg_miles / segments.longest }",
   ],
   [
      "the parent's own aggregates beside a joined one",
      "run: flights -> { group_by: carrier; aggregate: flight_count, total_distance, segments.seg_miles }",
   ],
   [
      "a grand total (childless and NULL-mile parents included)",
      "run: flights -> { aggregate: segments.seg_miles, segments.longest, flight_count }",
   ],
   [
      "a parent with no joined rows on its own (0 and null, as live)",
      "run: flights -> { where: id = 4; aggregate: segments.seg_miles, segments.longest }",
   ],
   [
      "grouping by the parent's key",
      "run: flights -> { group_by: id; aggregate: segments.seg_miles; order_by: id }",
   ],
   [
      "grouping by a joined grain dimension",
      "run: flights -> { group_by: segments.kind; aggregate: segments.seg_miles, flight_count }",
   ],
   [
      "a filter on a joined grain dimension",
      "run: flights -> { where: segments.kind = 'q'; group_by: carrier; aggregate: segments.seg_miles }",
   ],
   [
      "a parent dimension that reads an internal field",
      "run: flights -> { group_by: recent; aggregate: segments.seg_miles }",
   ],
   [
      "a having: on a joined measure",
      "run: flights -> { group_by: carrier; aggregate: segments.seg_miles; having: segments.seg_miles > 5 }",
   ],
   [
      "a nest",
      "run: flights -> { group_by: carrier; nest: k is { group_by: segments.kind; aggregate: segments.seg_miles } }",
   ],
   [
      "a calculate: over a joined measure",
      "run: flights -> { group_by: carrier; aggregate: segments.seg_miles; calculate: prev is lag(segments.seg_miles); order_by: carrier }",
   ],
   [
      "a sum and a max through a join_one",
      "run: flights -> { group_by: carrier; aggregate: crew.crew_size, crew.most_hours }",
   ],
   [
      "a count through a join_one (unmatched parents count zero live too)",
      "run: flights -> { group_by: carrier; aggregate: crew.crew_count }",
   ],
] as const;

const DECLINED = [
   [
      "a definition of the source that reads the join (the served copy leaves it out across connections)",
      "run: flights -> { group_by: carrier; aggregate: total_seg_miles }",
      "CROSS_CONNECTION",
   ],
   [
      "a named view that reads the join (left out with it)",
      "run: flights -> by_carrier",
      "CROSS_CONNECTION",
   ],
   [
      "a joined dimension outside the grain",
      "run: flights -> { group_by: segments.seg_id; aggregate: segments.seg_miles }",
      "GRAIN_TOO_COARSE",
   ],
   [
      "a count written over the join",
      "run: flights -> { group_by: carrier; aggregate: c is segments.count() }",
      "COUNT_SEMANTICS",
   ],
   [
      "a query in its own time zone",
      "run: flights -> { timezone: 'America/Los_Angeles'; group_by: carrier; aggregate: segments.seg_miles }",
      "TIME_ZONE",
   ],
   [
      "the parent's own aggregates beside a join_one",
      "run: flights -> { group_by: carrier; aggregate: flight_count, crew.crew_size }",
      "COUNT_SEMANTICS",
   ],
] as const;

for (const [kind, model, materialize] of [
   ["#(materialize) storage=", materializeModel, true],
   ["#@ persist storage=", persistModel, false],
] as const) {
   describe(`R4: join-to-rollup in a storage destination, query source stored by ${kind}`, () => {
      let restore: () => void;
      let fixture: R4Fixture;
      beforeAll(async () => {
         restore = installSettings({
            env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
         }).restore;
         fixture = await buildScenario({
            files: { "m.malloy": model },
            seedSql,
            materialize,
            preaggregate: true,
         });
      });
      afterAll(async () => {
         restore();
         await disposeScenarios();
      });

      for (const [name, malloy] of SERVED) {
         it(`serves ${name}`, async () => {
            expectServedFromRollup(await fixture.run({ name, malloy }));
         });
      }

      for (const [name, malloy, reason] of DECLINED) {
         it(`answers ${name} without the rollup (${reason})`, async () => {
            expectNotFromRollup(await fixture.run({ name, malloy }));
            expect(await rollupReasons(fixture, malloy)).toContain(reason);
         });
      }

      it("serves a count through a join_many", async () => {
         const malloy =
            "run: flights -> { group_by: carrier; aggregate: segments.seg_count }";
         expectServedFromRollup(await fixture.run({ name: "count", malloy }));
      });

      it("refuses an internal field to the caller, as live does", async () => {
         fixture.setBindings(fixture.bindings);
         await expect(
            fixture.runAsIs(
               "run: flights -> { group_by: departed; aggregate: segments.seg_miles }",
            ),
         ).rejects.toThrow();
         fixture.setBindings([]);
         await expect(
            fixture.runAsIs(
               "run: flights -> { group_by: departed; aggregate: segments.seg_miles }",
            ),
         ).rejects.toThrow();
      });

      it("reads both tables in the destination, and reports the rollup as the table that answered", async () => {
         fixture.setBindings(fixture.bindings);
         const { path, servedFrom } = await fixture.runAsIs(
            "run: flights -> { group_by: carrier; aggregate: segments.seg_miles }",
         );
         expect(servedFrom).toBe("storage");
         expect(path?.servedBy.kind).toBe("rollup");
         // The query source's table was read too, so the report names it.
         expect(path?.considered).toContainEqual({
            tableId: fixture.tableIds.flights,
            kind: "materialized",
            outcome: "used",
         });
      });

      it("declines while the query source's table is stale (CROSS_CONNECTION)", async () => {
         const malloy =
            "run: flights -> { group_by: carrier; aggregate: segments.seg_miles }";
         fixture.setBindings(
            fixture.bindings.map((b) =>
               b.origin === "preaggregate"
                  ? b
                  : {
                       ...b,
                       freshAsOf: "2020-01-01T00:00:00.000Z",
                       freshnessWindowSeconds: 60,
                       freshnessFallback: "live" as const,
                    },
            ),
         );
         const { path, rows } = await fixture.runAsIs(malloy);
         expect(path?.servedBy.kind).toBe("live");
         expect(
            path?.considered
               .filter((c) => c.kind === "rollup")
               .map((c) => c.reason),
         ).toContain("CROSS_CONNECTION");
         fixture.setBindings([]);
         const live = await fixture.runAsIs(malloy);
         expect(rowDifference(live.rows, rows)).toBeUndefined();
      });

      it("declines while the rollup's table is stale", async () => {
         const malloy =
            "run: flights -> { group_by: carrier; aggregate: segments.seg_miles }";
         fixture.setBindings(
            fixture.bindings.map((b) =>
               b.origin === "preaggregate"
                  ? {
                       ...b,
                       freshAsOf: "2020-01-01T00:00:00.000Z",
                       freshnessWindowSeconds: 60,
                       freshnessFallback: "live" as const,
                    }
                  : b,
            ),
         );
         const { path, rows } = await fixture.runAsIs(malloy);
         expect(path?.servedBy.kind).not.toBe("rollup");
         fixture.setBindings([]);
         const live = await fixture.runAsIs(malloy);
         expect(rowDifference(live.rows, rows)).toBeUndefined();
      });

      if (materialize) {
         it("still declines a query source not stored in the destination (CROSS_CONNECTION)", async () => {
            const malloy =
               "run: flights_live -> { group_by: carrier; aggregate: segments.seg_miles }";
            expectNotFromRollup(
               await fixture.run({ name: "unstored", malloy }),
            );
            expect(await rollupReasons(fixture, malloy)).toContain(
               "CROSS_CONNECTION",
            );
         });
      }
   });
}

describe("R4: join-to-rollup in a storage destination, the joined source stored there too", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
         files: {
            "m.malloy": `#(materialize)${S}
source: segments is duckdb.table('segs_raw') extend {
  primary_key: seg_id
  #(preaggregate) grain=["flight_id"]${S}
  measure: seg_miles is miles.sum()
  #(preaggregate) grain=["flight_id"]${S}
  measure: longest is miles.max()
}

#(materialize)${S}
source: flights is duckdb.table('flights_raw') extend {
  primary_key: id
  join_many: segments on segments.flight_id = id
  measure: flight_count is count()
  measure: total_seg_miles is segments.seg_miles
  view: by_carrier is { group_by: carrier; aggregate: segments.seg_miles }
}
`,
         },
         seedSql,
         materialize: true,
         preaggregate: true,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   // The served copy keeps the join (both sides are in the destination), so
   // the variant replaces it, and the source's definitions over it are kept.
   for (const [name, malloy] of [
      [
         "a sum and a max through the join",
         "run: flights -> { group_by: carrier; aggregate: segments.seg_miles, segments.longest, flight_count }",
      ],
      [
         "a parent measure that reads a joined measure",
         "run: flights -> { group_by: carrier; aggregate: total_seg_miles }",
      ],
      ["a named view over the join", "run: flights -> by_carrier"],
   ] as const) {
      it(`serves ${name} from the rollup`, async () => {
         expectServedFromRollup(await fixture.run({ name, malloy }));
      });
   }
});

describe("R4: join-to-rollup in a storage destination re-applies the query source's read terms", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   const B = "2026-11-10 00:00:00";
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
         files: {
            "m.malloy": `##! experimental.givens
given: CARRIER :: string is 'AA'
${children}
#(materialize)${S}
source: by_caller is duckdb.table('flights_raw') extend {
  primary_key: id
  where: carrier = $CARRIER
  join_many: segments on segments.flight_id = id
  measure: flight_count is count()
}

#(materialize)${S}
source: last30 is duckdb.table('flights_raw') extend {
  primary_key: id
  where: dep_ts >= now - 30 days
  join_many: segments on segments.flight_id = id
  measure: flight_count is count()
}
`,
         },
         seedSql,
         materialize: true,
         preaggregate: true,
         clock: B,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   it("applies a given-reading where: per caller", async () => {
      const malloy =
         "run: by_caller -> { group_by: carrier; aggregate: segments.seg_miles, flight_count }";
      const runs: R4Run[] = [];
      for (const CARRIER of ["AA", "BB"]) {
         const run = await fixture.run({
            name: CARRIER,
            malloy,
            givens: { CARRIER },
         });
         expectServedFromRollup(run);
         runs.push(run);
      }
      expect(runs[0].live).not.toEqual(runs[1].live);
   });

   for (const at of [B, "2026-11-25 00:00:00", "2026-12-05 00:00:00"]) {
      it(`applies a rolling window at read, at ${at}`, async () => {
         fixture.setClock(at);
         const run = await fixture.run({
            name: at,
            malloy:
               "run: last30 -> { group_by: carrier; aggregate: segments.seg_miles, flight_count }",
         });
         expectServedFromRollup(run);
      });
   }
});

describe("R4: join-to-rollup in a storage destination on a gated source", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: {
            PERSIST_STORAGE_MODE: "on",
            PUBLISHER_MATERIALIZE: "true",
            PUBLISHER_MATERIALIZE_GATED_SOURCES: "true",
         },
      }).restore;
      fixture = await buildScenario({
         files: {
            "m.malloy": `##! experimental.givens
given: CARRIER :: string is 'AA'
${children}
#(materialize)${S}
#(access_filter) carrier = $CARRIER
source: flights is duckdb.table('flights_raw') extend {
  primary_key: id
  join_many: segments on segments.flight_id = id
}

#(access_filter) kind = 'p'
source: p_segs is duckdb.table('segs_raw') extend {
  primary_key: seg_id
  #(preaggregate) grain=["flight_id", "kind"]${S}
  measure: p_seg_miles is miles.sum()
}

#(materialize)${S}
source: open_flights is duckdb.table('flights_raw') extend {
  primary_key: id
  join_many: p_segs on p_segs.flight_id = id
}
`,
         },
         seedSql,
         materialize: true,
         preaggregate: true,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   it("answers a gated entry point without the rollup, with its gate held (CONTRACT)", async () => {
      const malloy =
         "run: flights -> { group_by: carrier; aggregate: segments.seg_miles }";
      const run = await fixture.run({
         name: "gated",
         malloy,
         givens: { CARRIER: "AA" },
      });
      expectNotFromRollup(run);
      expect(run.served.map((r) => r.carrier)).toEqual(["AA"]);
      expect(await rollupReasons(fixture, malloy, { CARRIER: "AA" })).toContain(
         "CONTRACT",
      );
   });

   it("builds no storage= rollup over a gated joined source, which stays behind its review switch", async () => {
      // The rollup over `p_segs` is not built (a `storage=` rollup over a
      // gated base waits for the managed tier's review), so a query through
      // the join reads no rollup.
      expect(
         fixture.bindings.some((b) =>
            (b.sourceIDs ?? []).some((id) => id.includes("p_segs")),
         ),
      ).toBe(false);
      const malloy =
         "run: open_flights -> { group_by: carrier; aggregate: p_segs.p_seg_miles }";
      expectNotFromRollup(await fixture.run({ name: "gated child", malloy }));
      fixture.setBindings(fixture.bindings);
      const { path } = await fixture.runAsIs(malloy);
      expect(path?.servedBy.kind).not.toBe("rollup");
      expect(
         path?.considered.filter(
            (c) => c.kind === "rollup" && c.outcome === "used",
         ),
      ).toEqual([]);
   });
});

describe("R4: join-to-rollup in a storage destination keeps the served copy's own rules", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
         files: {
            "m.malloy": `##! experimental.persistence
${children}
source: carriers is duckdb.table('carriers_raw') extend {
  primary_key: code
  #(doc) The carrier's name, shouted.
  dimension: loud_name is upper(nickname)
}

#(materialize)${S} inline=["carriers"]
source: flights is duckdb.table('flights_raw') extend {
  primary_key: id
  join_one: carriers with carrier
  join_many: segments on segments.flight_id = id
  measure: flight_count is count()
}

#@ -persist
source: flights_fresh is flights extend {
  dimension: doubled is distance * 2
}

#(materialize)${S} inline=["segments"]
source: flights_pairs is duckdb.table('flights_raw') extend {
  primary_key: id
  join_many: segments on segments.flight_id = id
  join_one: crew with id
  measure: flight_count is count()
}
`,
         },
         seedSql: [
            ...seedSql,
            // Two carriers each: no flight's carrier row is unique to it.
            `CREATE TABLE carriers_raw AS SELECT * FROM (VALUES
               ('AA', 'American'), ('BB', 'Bee'), ('CC', 'Sea')
             ) AS t(code, nickname)`,
         ],
         materialize: true,
         preaggregate: true,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   it("serves through a source whose join_one it stores inline", async () => {
      expectServedFromRollup(
         await fixture.run({
            name: "inline",
            malloy:
               "run: flights -> { group_by: carriers.nickname; aggregate: segments.seg_miles }",
         }),
      );
   });

   for (const [name, malloy, reason] of [
      [
         "an aggregate at the grain of a join stored inline",
         "run: flights -> { group_by: carrier; aggregate: n is carriers.count(), segments.seg_miles }",
         "COUNT_SEMANTICS",
      ],
      [
         "a query that returns an annotated field of a join stored inline",
         "run: flights -> { group_by: carriers.loud_name; aggregate: segments.seg_miles }",
         "MISSING_FIELD",
      ],
      [
         "a reader that opts out with #@ -persist",
         "run: flights_fresh -> { group_by: carrier; aggregate: segments.seg_miles }",
         "CROSS_CONNECTION",
      ],
      [
         "a source stored as a pair table (one row per child)",
         "run: flights_pairs -> { group_by: carrier; aggregate: crew.crew_size }",
         "COUNT_SEMANTICS",
      ],
   ] as const) {
      it(`answers ${name} without the rollup (${reason})`, async () => {
         expectNotFromRollup(await fixture.run({ name, malloy }));
         expect(await rollupReasons(fixture, malloy)).toContain(reason);
      });
   }
});

describe("R4: join-to-rollup in a storage destination, over a source with its own time zone", () => {
   // A rollup over a zoned source is planned when a source in the same zone
   // joins it (preaggregate-time-zone's exception), in a storage= destination
   // too: the join leg reads it when the joining source is stored there.
   const zonedModel = `source: segs_la is duckdb.table('segs_raw') extend {
  timezone: 'America/Los_Angeles'
  primary_key: seg_id
  #(preaggregate) grain=["flight_id"]${S}
  measure: seg_miles is miles.sum()
}
#(materialize)${S}
source: flights_la is duckdb.table('flights_raw') extend {
  timezone: 'America/Los_Angeles'
  primary_key: id
  join_many: segs_la on segs_la.flight_id = id
}
`;
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
         files: { "m.malloy": zonedModel },
         seedSql,
         materialize: true,
         preaggregate: true,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   it("builds the rollup and reads it through the join from the same zone", async () => {
      expectServedFromRollup(
         await fixture.run({
            name: "zoned join",
            malloy:
               "run: flights_la -> { group_by: carrier; aggregate: segs_la.seg_miles }",
         }),
      );
   });
});

/**
 * A join read as a join_many takes the leg only when the query source's
 * primary key holds in the rows the leg reads: here, the query source's
 * stored table in the destination, checked at its own build. A key the data
 * repeats or leaves NULL there, or one no build checked, is answered without
 * the rollup (`COUNT_SEMANTICS`).
 */
describe("R4: join-to-rollup in a storage destination, over a stored query source whose key the data breaks", () => {
   let restore: () => void;
   let repeated: R4Fixture;
   let nulled: R4Fixture;
   let persisted: R4Fixture;
   let unchecked: R4Fixture;
   const flightsOf = (rows: string) =>
      `CREATE TABLE flights_raw AS SELECT * FROM (VALUES ${rows}) AS t(id, carrier, distance, dep_ts)`;
   // Flight 1 held twice.
   const REPEATED = flightsOf(`(1, 'AA', 100, TIMESTAMP '2026-11-09 10:00:00'),
      (1, 'BB', 150, TIMESTAMP '2026-11-09 11:00:00'),
      (2, 'AA', 200, TIMESTAMP '2026-10-01 10:00:00'),
      (3, 'BB', 300, TIMESTAMP '2026-11-01 10:00:00')`);
   // One flight with no id.
   const NULLED = flightsOf(`(1, 'AA', 100, TIMESTAMP '2026-11-09 10:00:00'),
      (2, 'AA', 200, TIMESTAMP '2026-10-01 10:00:00'),
      (NULL, 'BB', 350, TIMESTAMP '2026-11-02 10:00:00'),
      (3, 'BB', 300, TIMESTAMP '2026-11-01 10:00:00')`);
   const seedWith = (flights: string) => [flights, ...seedSql.slice(1)];
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      repeated = await buildScenario({
         files: { "m.malloy": materializeModel },
         seedSql: seedWith(REPEATED),
         preaggregate: true,
         materialize: true,
      });
      nulled = await buildScenario({
         files: { "m.malloy": materializeModel },
         seedSql: seedWith(NULLED),
         preaggregate: true,
         materialize: true,
      });
      persisted = await buildScenario({
         files: { "m.malloy": persistModel },
         seedSql: seedWith(REPEATED),
         preaggregate: true,
      });
      // As for a table a host's manifest binds, or a build whose check
      // could not run: unknown.
      unchecked = await buildScenario({
         files: { "m.malloy": materializeModel },
         seedSql,
         preaggregate: true,
         materialize: true,
         rollupKeyChecks: false,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   const details = async (fixture: R4Fixture, malloy: string) => {
      fixture.setBindings(fixture.bindings);
      const { path } = await fixture.runAsIs(malloy);
      return (path?.considered ?? [])
         .filter((c) => c.kind === "rollup")
         .map((c) => ({
            reason: c.reason,
            detail: (c as { detail?: string }).detail ?? "",
         }));
   };

   for (const [name, malloy] of [
      [
         "a sum through a join_many, by a parent dimension",
         "run: flights -> { group_by: carrier; aggregate: segments.seg_miles }",
      ],
      [
         "the parent's count beside a joined sum",
         "run: flights -> { aggregate: flight_count, segments.seg_miles }",
      ],
      [
         "grouping by the parent's key",
         "run: flights -> { group_by: id; aggregate: segments.seg_miles; order_by: id }",
      ],
   ] as const) {
      for (const [which, get, says] of [
         ["repeated", () => repeated, "more than once or NULL"],
         ["NULL", () => nulled, "more than once or NULL"],
         [
            "repeated, stored by #@ persist",
            () => persisted,
            "more than once or NULL",
         ],
         ["never checked", () => unchecked, "was not checked"],
      ] as const) {
         it(`answers ${name} without the rollup, its key ${which} (COUNT_SEMANTICS)`, async () => {
            // Rows first: from the rollup, the repeated and NULL cases
            // differ from live.
            const fixture = get();
            expectNotFromRollup(await fixture.run({ name, malloy }));
            const considered = await details(fixture, malloy);
            expect(considered.map((c) => c.reason)).toContain(
               "COUNT_SEMANTICS",
            );
            expect(considered.some((c) => c.detail.includes(says))).toBe(true);
         });
      }
   }
});

/**
 * The joined source's own primary key NULL in the data (see the colocated
 * case in join_rollup.component.spec.ts): live counts and sums `segments`
 * over the rows whose `seg_id` is not NULL, the rollup in the destination
 * over every row. The rollup's build checks the key on the base's
 * connection, and a count or sum through the join is answered live.
 */
describe("R4: join-to-rollup (A7) in a storage destination, the joined source's primary key NULL", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
         files: {
            "m.malloy": `##! experimental.access_modifiers
source: segments is duckdb.table('segs_raw') extend {
  primary_key: seg_id
  #(preaggregate) grain=["flight_id"]${S}
  measure: seg_miles is miles.sum()
  #(preaggregate) grain=["flight_id"]${S}
  measure: seg_count is count()
  #(preaggregate) grain=["flight_id"]${S}
  measure: longest is miles.max()
}
#(materialize)${S}
source: flights is duckdb.table('flights_raw') extend {
  primary_key: id
  join_many: segments on segments.flight_id = id
}
`,
         },
         seedSql: [
            seedSql[0]!,
            `CREATE TABLE segs_raw AS SELECT * FROM (VALUES
               (10, 1, 'p', 2.0), (11, 1, 'q', 3.0), (NULL, 1, 'p', 9.0),
               (12, 2, 'p', 4.0), (14, 3, 'q', 1.0), (16, 3, 'p', 7.0)
             ) AS t(seg_id, flight_id, kind, miles)`,
         ],
         materialize: true,
         preaggregate: true,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   for (const malloy of [
      "run: flights -> { group_by: carrier; aggregate: segments.seg_miles }",
      "run: flights -> { group_by: carrier; aggregate: segments.seg_count }",
   ]) {
      it(`answers ${malloy} live (COUNT_SEMANTICS)`, async () => {
         expectNotFromRollup(await fixture.run({ name: malloy, malloy }));
         expect(await rollupReasons(fixture, malloy)).toContain(
            "COUNT_SEMANTICS",
         );
      });
   }

   it("serves a max, which live reads every row for", async () => {
      expectServedFromRollup(
         await fixture.run({
            name: "max",
            malloy:
               "run: flights -> { group_by: carrier; aggregate: segments.longest }",
         }),
      );
   });
});
