// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4 for join-to-rollup (materialization v2, phase 4, A7): with
 * PUBLISHER_MATERIALIZE on, a query on a source that reads a joined source's
 * measures is answered from the joined source's `#(preaggregate)` rollup,
 * read at its grain through the re-pointed join, and equals live; a query
 * the rollup cannot reproduce is answered live with the path report's
 * reason. The fixture keeps parents with no joined rows (flight 4), a joined
 * row whose measure column is NULL (flight 5), and a joined row with no
 * parent (segment 17), since those are where a rollup and a join disagree.
 *
 * Counts through a join_many wait for malloydata/malloy#3208: until the
 * pinned Malloy carries it (`malloyCountsChildlessParentsAsZero`), live
 * counts a childless parent as one child, and those cases assert the
 * decline; after it, they assert the rollup answers.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { malloyCountsChildlessParentsAsZero } from "../../../src/service/materialization/serve/join_rollup";
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
      (1, 'AA', 100), (2, 'AA', 200), (3, 'BB', 300), (4, 'CC', 400), (5, 'BB', 500)
    ) AS t(id, carrier, distance)`,
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

/** The joined sources' rollups, colocated or in the scenario's destination. */
function model(placement: string): string {
   return `source: segments is duckdb.table('segs_raw') extend {
  primary_key: seg_id
  #(preaggregate) grain=["flight_id"]${placement}
  #(preaggregate) grain=["flight_id", "kind"]${placement}
  measure: seg_miles is miles.sum()
  #(preaggregate) grain=["flight_id"]${placement}
  #(preaggregate) grain=["flight_id", "kind"]${placement}
  measure: longest is miles.max()
  #(preaggregate) grain=["flight_id"]${placement}
  measure: shortest is miles.min()
  #(preaggregate) grain=["flight_id"]${placement}
  measure: p_miles is miles.sum() { where: kind = 'p' }
  #(preaggregate) grain=["flight_id"]${placement}
  measure: spread is longest - shortest
  #(preaggregate) grain=["flight_id"]${placement}
  measure: seg_count is count()
  #(preaggregate) grain=["flight_id"]${placement}
  measure: miles_per_seg is miles.sum() / count()
  #(preaggregate) grain=["flight_id"]${placement}
  measure: padded_miles is sum(miles + 1)
}

source: crew is duckdb.table('crew_raw') extend {
  primary_key: flight_id
  #(preaggregate) grain=["flight_id"]${placement}
  measure: crew_size is size.sum()
  #(preaggregate) grain=["flight_id"]${placement}
  measure: crew_count is count()
  #(preaggregate) grain=["flight_id"]${placement}
  measure: most_hours is duty_hours.max()
}

source: flights is duckdb.table('flights_raw') extend {
  primary_key: id
  join_many: segments on segments.flight_id = id
  join_one: crew with id
  measure: flight_count is count()
  measure: total_distance is distance.sum()
  measure: total_seg_miles is segments.seg_miles
  measure: written_count is segments.count()
  dimension: seg_kind is segments.kind
  view: by_carrier is { group_by: carrier; aggregate: segments.seg_miles }
}

source: p_flights is flights extend { where: segments.kind = 'p' }
`;
}

/** A served run read a rollup and answered as live did. */
function expectServedFromRollup(run: R4Run): void {
   expect(run.servedBy?.kind).toBe("rollup");
   expect(rowDifference(run.live, run.served)).toBeUndefined();
}

/** Live answered, equal to live. */
function expectLive(run: R4Run): void {
   expect(run.servedBy?.kind).not.toBe("rollup");
   expect(rowDifference(run.live, run.served)).toBeUndefined();
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
      "a parent measure that reads a joined measure",
      "run: flights -> { group_by: carrier; aggregate: total_seg_miles }",
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
      "grouping by a parent dimension that reads a joined grain dimension",
      "run: flights -> { group_by: seg_kind; aggregate: segments.longest }",
   ],
   [
      "a filter on a joined grain dimension",
      "run: flights -> { where: segments.kind = 'q'; group_by: carrier; aggregate: segments.seg_miles }",
   ],
   [
      "a filter on a parent dimension",
      "run: flights -> { where: carrier = 'AA'; aggregate: segments.seg_miles }",
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
      "a sum through a join_one",
      "run: flights -> { group_by: carrier; aggregate: crew.crew_size, crew.most_hours }",
   ],
   [
      "a count through a join_one (unmatched parents count zero live too)",
      "run: flights -> { group_by: carrier; aggregate: crew.crew_count }",
   ],
] as const;

const LIVE = [
   [
      "a joined dimension outside the grain",
      "run: flights -> { group_by: segments.seg_id; aggregate: segments.seg_miles }",
      "GRAIN_TOO_COARSE",
   ],
   [
      "a joined measure the rollup does not store",
      "run: flights -> { aggregate: m is segments.miles.sum() }",
      "MISSING_FIELD",
   ],
   [
      "a count written over the join",
      "run: flights -> { group_by: carrier; aggregate: c is segments.count() }",
      "COUNT_SEMANTICS",
   ],
   [
      "a distinct count written over a joined grain dimension",
      "run: flights -> { group_by: carrier; aggregate: c is count(segments.flight_id), segments.seg_miles }",
      "COUNT_SEMANTICS",
   ],
   [
      "a parent measure that counts through the join",
      "run: flights -> { group_by: carrier; aggregate: written_count, segments.seg_miles }",
      "COUNT_SEMANTICS",
   ],
   [
      "a select: of joined rows",
      "run: flights -> { select: id, segments.kind }",
      "COUNT_SEMANTICS",
   ],
   [
      "a query in its own time zone",
      "run: flights -> { timezone: 'America/Los_Angeles'; group_by: carrier; aggregate: segments.seg_miles }",
      "TIME_ZONE",
   ],
   [
      "a nest in its own time zone",
      "run: flights -> { group_by: carrier; nest: n is { timezone: 'America/Los_Angeles'; group_by: segments.kind; aggregate: segments.seg_miles } }",
      "TIME_ZONE",
   ],
   [
      "a source whose own where: reads the join",
      "run: p_flights -> { group_by: carrier; aggregate: segments.seg_miles }",
      "COUNT_SEMANTICS",
   ],
] as const;

/** Count-shaped cases: declined until malloydata/malloy#3208 is pinned, served after. */
const COUNTS = [
   [
      "a count through a join_many",
      "run: flights -> { group_by: carrier; aggregate: segments.seg_count }",
   ],
   [
      "a ratio over a count through a join_many",
      "run: flights -> { group_by: carrier; aggregate: segments.miles_per_seg }",
   ],
   [
      "a sum whose operand is not NULL on a childless parent",
      "run: flights -> { group_by: carrier; aggregate: segments.padded_miles }",
   ],
] as const;

describe("R4: join-to-rollup (A7), a joined source's colocated rollup, equal to live", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
         files: { "m.malloy": model("") },
         seedSql,
         preaggregate: true,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   const reasonOf = async (malloy: string) => {
      fixture.setBindings(fixture.bindings);
      const { path } = await fixture.runAsIs(malloy);
      return path?.considered
         .filter((c) => c.kind === "rollup")
         .map((c) => c.reason);
   };

   for (const [name, malloy] of SERVED) {
      it(`serves ${name}`, async () => {
         expectServedFromRollup(await fixture.run({ name, malloy }));
      });
   }
   for (const [name, malloy, reason] of LIVE) {
      it(`answers ${name} live (${reason})`, async () => {
         expectLive(await fixture.run({ name, malloy }));
         expect(await reasonOf(malloy)).toContain(reason);
      });
   }

   const skipsUnmatched = malloyCountsChildlessParentsAsZero();
   for (const [name, malloy] of COUNTS) {
      it.if(!skipsUnmatched)(
         `answers ${name} live until malloydata/malloy#3208 (COUNT_SEMANTICS)`,
         async () => {
            expectLive(await fixture.run({ name, malloy }));
            expect(await reasonOf(malloy)).toContain("COUNT_SEMANTICS");
         },
      );
      it.if(skipsUnmatched)(
         `serves ${name} once malloydata/malloy#3208 is pinned`,
         async () => {
            expectServedFromRollup(await fixture.run({ name, malloy }));
         },
      );
   }

   it("the count switch says what the pinned Malloy answers for a childless parent", async () => {
      // Flight 4 has no segments: one child live before #3208, none after.
      fixture.setBindings([]);
      const { rows } = await fixture.runAsIs(
         "run: flights -> { where: id = 4; aggregate: segments.seg_count }",
      );
      expect(Number(rows[0]?.seg_count)).toBe(skipsUnmatched ? 0 : 1);
   });

   it("reports the joined source's rollup as the table that answered", async () => {
      fixture.setBindings(fixture.bindings);
      const { path } = await fixture.runAsIs(
         "run: flights -> { group_by: carrier; aggregate: segments.seg_miles }",
      );
      expect(path?.servedBy.kind).toBe("rollup");
      expect(path?.considered.find((c) => c.outcome === "used")?.kind).toBe(
         "rollup",
      );
   });

   it("answers a query that reads nothing through the join live", async () => {
      const run = await fixture.run({
         name: "parent only",
         malloy:
            "run: flights -> { group_by: carrier; aggregate: flight_count }",
      });
      expectLive(run);
   });

   it("names no joined source's rollup for a query that reads no join", async () => {
      fixture.setBindings(fixture.bindings);
      const { path } = await fixture.runAsIs(
         "run: flights -> { group_by: carrier; aggregate: flight_count }",
      );
      expect(path?.considered.filter((c) => c.kind === "rollup")).toEqual([]);
   });

   it("serves a named view of the source: the view is the author's, kept", async () => {
      expectServedFromRollup(
         await fixture.run({
            name: "view",
            malloy: "run: flights -> by_carrier",
         }),
      );
   });
});

describe("R4: join-to-rollup (A7) and the wrong-answer shapes the base leg declines", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
         files: {
            "m.malloy": `source: lines is duckdb.table('lines_raw') extend { primary_key: line_id }
source: segments is duckdb.table('segs_raw') extend {
  primary_key: seg_id
  join_many: lines on lines.seg_id = seg_id
  dimension: line_kind is lines.kind
  #(preaggregate) grain=["flight_id", "line_kind"]
  measure: seg_miles is miles.sum()
}
source: flights is duckdb.table('flights_raw') extend {
  primary_key: id
  join_many: segments on segments.flight_id = id
}
`,
         },
         seedSql: [
            ...seedSql,
            `CREATE TABLE lines_raw AS SELECT * FROM (VALUES
               (1, 10, 'a'), (2, 10, 'b'), (3, 12, 'a')
             ) AS t(line_id, seg_id, kind)`,
         ],
         preaggregate: true,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   for (const [name, malloy] of [
      [
         "a measure across a fan-out grain dimension (A6)",
         "run: flights -> { group_by: carrier; aggregate: segments.seg_miles }",
      ],
      [
         "a calculate: over a fan-out measure the query does not aggregate (A6)",
         "run: flights -> { group_by: carrier; calculate: prev is lag(segments.seg_miles); order_by: carrier }",
      ],
      [
         "a fan-out measure with its dimension grouped (A6, not served through a join yet)",
         "run: flights -> { group_by: segments.line_kind; aggregate: segments.seg_miles }",
      ],
   ] as const) {
      it(`answers ${name} live (GRAIN_TOO_COARSE)`, async () => {
         expectLive(await fixture.run({ name, malloy }));
         fixture.setBindings(fixture.bindings);
         const { path } = await fixture.runAsIs(malloy);
         expect(
            path?.considered
               .filter((c) => c.kind === "rollup")
               .map((c) => c.reason),
         ).toContain("GRAIN_TOO_COARSE");
      });
   }
});

describe("R4: join-to-rollup (A7) declines a joined source's rollup in a storage destination", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
         files: { "m.malloy": model(` storage="${DESTINATION}"`) },
         seedSql,
         preaggregate: true,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   it("answers live, saying the rollup is across connections (CROSS_CONNECTION)", async () => {
      const malloy =
         "run: flights -> { group_by: carrier; aggregate: segments.seg_miles }";
      expectLive(await fixture.run({ name: "storage", malloy }));
      fixture.setBindings(fixture.bindings);
      const { path } = await fixture.runAsIs(malloy);
      expect(
         path?.considered
            .filter((c) => c.kind === "rollup")
            .map((c) => c.reason),
      ).toContain("CROSS_CONNECTION");
   });

   it("still serves the joined source's own queries from its storage rollup", async () => {
      expectServedFromRollup(
         await fixture.run({
            name: "child",
            malloy:
               "run: segments -> { group_by: flight_id; aggregate: seg_miles }",
         }),
      );
   });
});

describe("R4: join-to-rollup (A7) on a gated source", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
         files: {
            "m.malloy": `##! experimental.givens
given: CARRIER :: string is 'AA'
source: segments is duckdb.table('segs_raw') extend {
  primary_key: seg_id
  #(preaggregate) grain=["flight_id"]
  measure: seg_miles is miles.sum()
}
#(access_filter) carrier = $CARRIER
source: flights is duckdb.table('flights_raw') extend {
  primary_key: id
  join_many: segments on segments.flight_id = id
}
`,
         },
         seedSql,
         preaggregate: true,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   it("answers a gated entry point live, with its gate held (CONTRACT)", async () => {
      const malloy =
         "run: flights -> { group_by: carrier; aggregate: segments.seg_miles }";
      const run = await fixture.run({ name: "gated", malloy });
      expectLive(run);
      expect(run.served.map((r) => r.carrier)).toEqual(["AA"]);
      fixture.setBindings(fixture.bindings);
      const { path } = await fixture.runAsIs(malloy);
      expect(
         path?.considered
            .filter((c) => c.kind === "rollup")
            .map((c) => c.reason),
      ).toContain("CONTRACT");
   });
});

describe("R4: join-to-rollup (A7) where live and a rollup disagree by construction", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
         files: {
            "m.malloy": `source: segments is duckdb.table('segs_ts_raw') extend {
  primary_key: seg_id
  dimension: seg_day is seg_ts.day
  #(preaggregate) grain=["flight_id", "seg_day"]
  measure: seg_miles is miles.sum()
}
source: segs_la is duckdb.table('segs_ts_raw') extend {
  timezone: 'America/Los_Angeles'
  primary_key: seg_id
  dimension: seg_day is seg_ts.day
  #(preaggregate) grain=["flight_id", "seg_day"]
  measure: seg_miles is miles.sum()
}
source: crew_dup is duckdb.table('crew_dup_raw') extend {
  #(preaggregate) grain=["flight_id"]
  measure: crew_size is size.sum()
}
source: flights is duckdb.table('flights_raw') extend {
  primary_key: id
  join_many: segments on segments.flight_id = id
  join_many: segs_la on segs_la.flight_id = id
  join_one: crew_dup on crew_dup.flight_id = id
  measure: flight_count is count()
  measure: total_distance is distance.sum()
}
source: flights_la is flights extend { timezone: 'America/Los_Angeles' }
`,
         },
         seedSql: [
            ...seedSql,
            // Segments 11 and 13 fall on another day in Los Angeles than in UTC.
            `CREATE TABLE segs_ts_raw AS SELECT * FROM (VALUES
               (10, 1, 2.0, TIMESTAMP '2024-01-01 12:00:00'),
               (11, 1, 3.0, TIMESTAMP '2024-01-02 03:00:00'),
               (12, 2, 4.0, TIMESTAMP '2024-01-02 12:00:00'),
               (13, 3, 1.5, TIMESTAMP '2024-01-03 05:00:00')
             ) AS t(seg_id, flight_id, miles, seg_ts)`,
            // Declared join_one, but flight 1 has two crew rows.
            `CREATE TABLE crew_dup_raw AS SELECT * FROM (VALUES
               (1, 3), (1, 4), (3, 2), (5, 7)
             ) AS t(flight_id, size)`,
         ],
         preaggregate: true,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   const reasonOf = async (malloy: string) => {
      fixture.setBindings(fixture.bindings);
      const { path } = await fixture.runAsIs(malloy);
      return path?.considered
         .filter((c) => c.kind === "rollup")
         .map((c) => c.reason);
   };

   for (const [name, malloy, reason] of [
      [
         "a joined source read in the query source's time zone, not its own",
         "run: flights -> { group_by: segs_la.seg_day; aggregate: segs_la.seg_miles }",
         "TIME_ZONE",
      ],
      [
         "a query source in its own time zone, through a joined source in the model's",
         "run: flights_la -> { group_by: segments.seg_day; aggregate: segments.seg_miles }",
         "TIME_ZONE",
      ],
      [
         "the parent's own aggregates beside a join_one the data does not keep unique",
         "run: flights -> { group_by: carrier; aggregate: flight_count, total_distance, crew_dup.crew_size }",
         "COUNT_SEMANTICS",
      ],
      [
         "a having: on a parent aggregate beside a non-unique join_one",
         "run: flights -> { group_by: carrier; aggregate: crew_dup.crew_size; having: flight_count > 1 }",
         "COUNT_SEMANTICS",
      ],
   ] as const) {
      it(`answers ${name} live (${reason})`, async () => {
         expectLive(await fixture.run({ name, malloy }));
         expect(await reasonOf(malloy)).toContain(reason);
      });
   }

   for (const [name, malloy] of [
      [
         "a joined source in the query source's time zone",
         "run: flights -> { group_by: segments.seg_day; aggregate: segments.seg_miles }",
      ],
      [
         "a non-unique join_one's own measure, by parent dimensions",
         "run: flights -> { group_by: carrier, id; aggregate: crew_dup.crew_size }",
      ],
      // A source with its own time zone keeps its rollup only for this read
      // (#1686's decision 1): a join from a source in the same zone.
      [
         "a joined source with its own time zone, from a query source in the same zone",
         "run: flights_la -> { group_by: segs_la.seg_day; aggregate: segs_la.seg_miles }",
      ],
   ] as const) {
      it(`serves ${name}`, async () => {
         expectServedFromRollup(await fixture.run({ name, malloy }));
      });
   }
});
