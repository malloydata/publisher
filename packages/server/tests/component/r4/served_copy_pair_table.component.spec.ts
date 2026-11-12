// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4 for an inlined join_many (materialization v2, phase 4, P5): the pair
 * table, served through its parent-row flag (the served source) and its
 * child-row flag (the join's rows, joined back with the author's condition).
 * Every answer equals live row for row: parent-level and child-level measures,
 * both together, filters on either side, nests, childless parents, fan-out up
 * to 5 and past it, and live's count of a childless parent through a
 * join_many (malloydata/malloy#1762), which the served copy reproduces because
 * its query is live's own LEFT JOIN.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   buildScenario,
   disposeScenarios,
   rowDifference,
   type R4Fixture,
} from "./harness";

// Flights 1–12 with 0 to 12 segments each: four childless (4, 5, 9, 11), fan-out
// up to 5 on most, and 12 on flight 7. Segment 99 pairs with no flight.
const fanOut: Record<number, number> = {
   1: 2,
   2: 1,
   3: 2,
   4: 0,
   5: 0,
   6: 5,
   7: 12,
   8: 3,
   9: 0,
   10: 1,
   11: 0,
   12: 4,
};
const segmentRows: string[] = [];
let segId = 1;
for (const [flight, n] of Object.entries(fanOut)) {
   for (let i = 0; i < n; i++) {
      segmentRows.push(`(${segId}, ${flight}, ${(segId * 37) % 500}.0)`);
      segId++;
   }
}
segmentRows.push("(99, 99, 10.0)");

const seedSql = [
   `CREATE TABLE flights AS SELECT * FROM (VALUES
      (1, 'AA', 100.0), (2, 'AA', 250.0), (3, 'UA', 300.0), (4, 'DL', 50.0),
      (5, 'XX', 75.0), (6, 'UA', 120.0), (7, 'DL', 900.0), (8, 'AA', 410.0),
      (9, 'UA', 60.0), (10, 'DL', 220.0), (11, 'AA', 130.0), (12, 'XX', 330.0)
    ) AS t(id, carrier, distance)`,
   `CREATE TABLE segments AS SELECT * FROM (VALUES ${segmentRows.join(", ")}) AS t(seg_id, flight_id, miles)`,
   `CREATE TABLE notes AS SELECT * FROM (VALUES
      (1, 'AA', 'gate'), (2, 'AA', 'crew'), (3, 'UA', 'gate'), (4, 'ZZ', 'none')
    ) AS t(note_id, carrier, topic)`,
];

const model = `source: segments is duckdb.table('segments') extend {
  primary_key: seg_id
  dimension: long_leg is miles > 200
  measure:
    seg_miles is miles.sum()
    seg_count is count()
    longest is miles.max()
}

source: notes is duckdb.table('notes') extend {
  primary_key: note_id
  measure: note_count is count()
}

#(materialize) storage="lake" inline=["segments"]
source: flights is duckdb.table('flights') extend {
  primary_key: id
  join_many: segments on segments.flight_id = id
  dimension:
    long_haul is distance > 200
    leg_miles is segments.miles
  measure:
    flight_count is count()
    total_distance is distance.sum()
    avg_distance is distance.avg()
  view: by_carrier is {
    group_by: carrier
    aggregate: flight_count, total_distance, segments.seg_miles
  }
}

#(materialize) storage="lake" inline=["notes"]
source: flights_notes is duckdb.table('flights') extend {
  primary_key: id
  join_many: notes on notes.carrier = carrier
  measure:
    flight_count is count()
    total_distance is distance.sum()
}

#(materialize) storage="lake" inline=["segments"]
source: flights_unkeyed is duckdb.table('flights') extend {
  join_many: segments on segments.flight_id = id
  measure: flight_count is count()
}
`;

describe("R4: an inlined join_many, served from its pair table, answers equal to live", () => {
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

   it("stores one parent row per flight, childless flights included", async () => {
      const rows = await fixture.storedSQL(
         (t) =>
            `SELECT count(*) AS n, count(*) FILTER (WHERE __parent_row) AS parents, count(*) FILTER (WHERE __child_row) AS children FROM ${t("flights")}`,
      );
      // 30 paired segments, one row each for the 4 childless flights.
      expect(rows.map((r) => [r.n, r.parents, r.children].map(Number))).toEqual(
         [[34, 12, 30]],
      );
   });

   const served = [
      [
         "parent-level measures only",
         "run: flights -> { group_by: carrier; aggregate: flight_count, total_distance, avg_distance }",
      ],
      [
         "a parent-level filter",
         "run: flights -> { where: long_haul; group_by: carrier; aggregate: flight_count, total_distance }",
      ],
      [
         "child-level measures",
         "run: flights -> { group_by: carrier; aggregate: segments.seg_miles, segments.longest }",
      ],
      [
         "live's count of a childless parent through a join_many (malloydata/malloy#1762)",
         "run: flights -> { group_by: carrier; aggregate: segments.seg_count, n is count(segments.seg_id) }",
      ],
      [
         "parent and child measures together",
         "run: flights -> { group_by: carrier; aggregate: flight_count, total_distance, segments.seg_miles, segments.seg_count }",
      ],
      ["the source's own view", "run: flights -> by_carrier"],
      [
         "grouped by a child dimension the pair table stores",
         "run: flights -> { group_by: segments.long_leg; aggregate: flight_count, total_distance, segments.seg_miles }",
      ],
      [
         "grouped by a dimension through the join, computed at read",
         "run: flights -> { group_by: leg_miles; aggregate: flight_count }",
      ],
      [
         "a filter on the child side",
         "run: flights -> { where: segments.miles > 150; group_by: carrier; aggregate: flight_count, total_distance, segments.seg_miles }",
      ],
      [
         "childless parents found through the join",
         "run: flights -> { where: segments.seg_id = null; group_by: carrier; aggregate: flight_count }",
      ],
      [
         "filtered measures on both sides",
         "run: flights -> { group_by: carrier; aggregate: d is total_distance { where: segments.long_leg }, c is segments.seg_count { where: long_haul } }",
      ],
      [
         "a nest of parent and child aggregates",
         "run: flights -> { group_by: carrier; aggregate: flight_count; nest: legs is { group_by: segments.long_leg; aggregate: total_distance, segments.seg_miles } }",
      ],
      [
         "one row per pair",
         "run: flights -> { group_by: id, segments.seg_id, segments.miles }",
      ],
      [
         "a fan-out past 5 (flight 7, 12 segments)",
         "run: flights -> { where: id = 7 | 6; group_by: id; aggregate: total_distance, segments.seg_miles, segments.seg_count }",
      ],
      [
         "a many-to-many join_many (each note pairs with several flights)",
         "run: flights_notes -> { group_by: carrier; aggregate: flight_count, total_distance, notes.note_count }",
      ],
      [
         "a many-to-many join_many, grouped by the child",
         "run: flights_notes -> { group_by: notes.topic; aggregate: flight_count, total_distance, notes.note_count }",
      ],
   ] as const;
   for (const [name, malloy] of served) {
      it(name, async () => {
         const run = await fixture.run({ name, malloy });
         expect(run.servedVia).toBe("served_copy");
         expect(run.live.length).toBeGreaterThan(0);
         expect(rowDifference(run.live, run.served)).toBeUndefined();
      });
   }

   it("reproduces live's count of a childless parent, not the true count", async () => {
      const run = await fixture.run({
         name: "count",
         malloy:
            "run: flights -> { where: id = 4 | 6; group_by: id; aggregate: segments.seg_count }",
      });
      expect(run.servedVia).toBe("served_copy");
      // Live counts childless flight 4 as one segment; so does the served copy.
      expect(
         run.served
            .map((r) => [Number(r.id), Number(r.seg_count)])
            .sort((a, b) => a[0] - b[0]),
      ).toEqual([
         [4, 1],
         [6, 5],
      ]);
      expect(rowDifference(run.live, run.served)).toBeUndefined();
   });

   it("leaves no problem on the served copy", async () => {
      expect(await fixture.servedCopyProblems()).toEqual([]);
   });

   it("answers a refused inline= (no primary key) live", async () => {
      expect(fixture.bindings.map((b) => b.sourceName).sort()).toEqual([
         "flights",
         "flights_notes",
      ]);
      const run = await fixture.run({
         name: "unkeyed",
         malloy:
            "run: flights_unkeyed -> { group_by: carrier; aggregate: flight_count, segments.seg_miles }",
         expectLive: true,
      });
      expect(run.servedVia).toBeUndefined();
      expect(rowDifference(run.live, run.served)).toBeUndefined();
   });
});

// The load's classification checks run on the served copy's own compile, so
// the joined source's fields must keep their classes through the pair table:
// each is written on the field the child rows are renamed back to.
const classified = `##! experimental.access_modifiers
source: segments is duckdb.table('segments') include {
  #(classification) "none"
  public: seg_id, flight_id, miles
} extend {
  primary_key: seg_id
  #(classification) "none"
  measure: seg_miles is miles.sum()
}

#(carries) ["none"]
#(materialize) storage="lake" inline=["segments"]
source: flights is duckdb.table('flights') include {
  #(classification) "none"
  public: id, carrier, distance
} extend {
  primary_key: id
  join_many: segments on segments.flight_id = id
  #(classification) "none"
  measure: flight_count is count()
}
`;

describe("R4: a pair table keeps the joined source's classifications", () => {
   let restore: () => void;
   beforeAll(() => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   it("serves a classified source through its pair table", async () => {
      const fixture = await buildScenario({
         files: { "m.malloy": classified },
         seedSql,
         materialize: true,
      });
      expect(await fixture.servedCopyProblems()).toEqual([]);
      const run = await fixture.run({
         name: "classified",
         malloy:
            "run: flights -> { group_by: carrier; aggregate: flight_count, segments.seg_miles }",
      });
      expect(run.servedVia).toBe("served_copy");
      expect(rowDifference(run.live, run.served)).toBeUndefined();
   });
});
