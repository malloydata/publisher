// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4 for an inlined join_many (materialization v2, phase 4, P5): the pair
 * table, served flat. The served source reads the table itself, with no join:
 * the join's name is a record of the child's columns, a measure over the
 * source's own fields aggregates the rows the parent-row flag marks, and an
 * aggregate through the join reads every row, which are live's LEFT JOIN rows.
 * Every accepted shape equals live row for row: parent-level and child-level
 * measures, both together, ratios and measures that read measures, filtered
 * measures, nests, filters on either side, childless parents, fan-out up to 5
 * and past it, a many-to-many join_many. Since Malloy 0.0.438
 * (malloydata/malloy#3208) live counts a childless parent through a
 * join_many as none, which the flat table's null-extended row cannot
 * reproduce, so every count through the join is answered live. Every shape
 * the flat table cannot reproduce is answered live, and equals live there too.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { mkdtempSync, rmSync } from "fs";
import { tmpdir } from "os";
import path from "path";
import { SingleConnectionRuntime } from "@malloydata/malloy";
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   rerootDeclaration,
   typeDeclaration,
   type PairReroot,
} from "../../../src/service/materialization/serve/served_copy";
import { flatPairQueryRefusal } from "../../../src/service/governance/ir/pair_flat_query";
import {
   buildScenario,
   disposeScenarios,
   rowDifference,
   type R4Fixture,
   type Row,
   expectPairServedVia,
} from "./harness";

/** Measures that count through the inlined path (see {@link expectPairServedVia}). */
const PATH_COUNTS = ["seg_n", "long_legs", "note_n", "by_carrier", "by_leg"];

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

const flightsDeclaration = `flights is duckdb.table('flights') extend {
  primary_key: id
  join_many: segments on segments.flight_id = id
  dimension:
    long_haul is distance > 200
    leg_miles is segments.miles
  measure:
    flight_count is count()
    total_distance is distance.sum()
    avg_distance is distance.avg()
    longest_flight is distance.max()
    carrier_count is count(carrier)
    distance_per_flight is total_distance / flight_count
    long_flights is flight_count { where: long_haul }
    seg_total is segments.miles.sum()
    seg_n is segments.count()
    seg_longest is segments.miles.max()
    seg_avg is segments.miles.avg()
    seg_ids is count(segments.seg_id)
    long_legs is segments.count() { where: segments.long_leg }
    miles_per_flight is seg_total / flight_count
    mixed_inline is segments.miles.sum() / count()
  view: by_carrier is {
    group_by: carrier
    aggregate: flight_count, total_distance, seg_total, seg_n
  }
}`;

const model = `source: segments is duckdb.table('segments') extend {
  primary_key: seg_id
  dimension: long_leg is miles > 200
  measure:
    seg_miles is miles.sum()
    seg_count is count()
}

source: notes is duckdb.table('notes') extend {
  primary_key: note_id
}

#(materialize) storage="lake" inline=["segments"]
source: ${flightsDeclaration}

#(materialize) storage="lake" inline=["notes"]
source: flights_notes is duckdb.table('flights') extend {
  primary_key: id
  join_many: notes on notes.carrier = carrier
  measure:
    flight_count is count()
    total_distance is distance.sum()
    note_n is notes.count()
}

#(materialize) storage="lake" inline=["segments"]
source: flights_unkeyed is duckdb.table('flights') extend {
  join_many: segments on segments.flight_id = id
  measure: flight_count is count()
}

#(materialize) storage="lake"
source: carriers is duckdb.table('notes') extend {
  primary_key: note_id
  join_many: flights on flights.carrier = carrier
}
`;

describe("R4: an inlined join_many, served flat from its pair table, answers equal to live", () => {
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

   it("stores one row per row of live's LEFT JOIN, and one parent row per flight", async () => {
      const rows = await fixture.storedSQL(
         (t) =>
            `SELECT count(*) AS n, count(*) FILTER (WHERE __parent_row) AS parents FROM ${t("flights")}`,
      );
      // 30 paired segments, one row each for the 4 childless flights.
      expect(rows.map((r) => [r.n, r.parents].map(Number))).toEqual([[34, 12]]);
   });

   const served = [
      [
         "parent-level measures beside a child one",
         "run: flights -> { group_by: carrier; aggregate: flight_count, total_distance, avg_distance, seg_n }",
      ],
      [
         "non-additive parent measures: avg, max, a distinct count",
         "run: flights -> { group_by: long_haul; aggregate: avg_distance, longest_flight, carrier_count, seg_n }",
      ],
      [
         "a ratio of measures, and a filtered measure that reads a measure",
         "run: flights -> { group_by: carrier; aggregate: distance_per_flight, long_flights, seg_total }",
      ],
      [
         "a parent-level filter",
         "run: flights -> { where: long_haul; group_by: carrier; aggregate: flight_count, total_distance, seg_n }",
      ],
      [
         "child-level measures",
         "run: flights -> { group_by: carrier; aggregate: seg_total, seg_longest, seg_avg, seg_ids }",
      ],
      [
         "child-level aggregates written in the query",
         "run: flights -> { group_by: carrier; aggregate: m is segments.miles.sum(), n is segments.count(), a is segments.miles.avg(), d is count(segments.seg_id) }",
      ],
      [
         "a childless parent's count through a join_many (none; answered live)",
         "run: flights -> { group_by: id; aggregate: seg_n, n is segments.count() }",
      ],
      [
         "parent and child measures together, and a ratio across them",
         "run: flights -> { group_by: carrier; aggregate: flight_count, total_distance, seg_total, seg_n, miles_per_flight }",
      ],
      ["the source's own view", "run: flights -> by_carrier"],
      [
         "child measures grouped by a child dimension the pair table stores",
         "run: flights -> { group_by: segments.long_leg; aggregate: seg_total, seg_n }",
      ],
      [
         "grouped by a dimension through the join, computed at read",
         "run: flights -> { group_by: leg_miles; aggregate: seg_n }",
      ],
      [
         "a filter on the child side, child measures",
         "run: flights -> { where: segments.miles > 150; group_by: carrier; aggregate: seg_total, seg_n }",
      ],
      [
         "filtered child measures, filtered by either side",
         "run: flights -> { group_by: carrier; aggregate: long_legs, c is seg_n { where: long_haul }, m is seg_total { where: segments.long_leg } }",
      ],
      [
         "a nest of child aggregates under parent ones",
         "run: flights -> { group_by: carrier; aggregate: flight_count, total_distance; nest: legs is { group_by: segments.long_leg; aggregate: seg_total, seg_n; order_by: long_leg } }",
      ],
      [
         "a nest of parent aggregates by a parent dimension",
         "run: flights -> { group_by: carrier; aggregate: seg_n; nest: haul is { group_by: long_haul; aggregate: flight_count, total_distance; order_by: long_haul } }",
      ],
      [
         "one row per pair",
         "run: flights -> { group_by: id, segments.seg_id, segments.miles }",
      ],
      [
         "a fan-out past 5 (flight 7, 12 segments)",
         "run: flights -> { where: id = 7 | 6; group_by: id; aggregate: total_distance, seg_total, seg_n }",
      ],
      [
         "a having on a parent measure",
         "run: flights -> { group_by: carrier; aggregate: flight_count, seg_total; having: flight_count > 2 }",
      ],
      [
         "a many-to-many join_many (each note pairs with several flights)",
         "run: flights_notes -> { group_by: carrier; aggregate: flight_count, total_distance, note_n }",
      ],
      [
         "a many-to-many join_many, child grouped",
         "run: flights_notes -> { group_by: notes.topic; aggregate: note_n }",
      ],
   ] as const;
   for (const [name, malloy] of served) {
      it(name, async () => {
         const run = await fixture.run({ name, malloy });
         expectPairServedVia(run, PATH_COUNTS);
         expect(run.live.length).toBeGreaterThan(0);
         expect(rowDifference(run.live, run.served)).toBeUndefined();
      });
   }

   it("answers a childless parent's count (none, since malloydata/malloy#3208) live", async () => {
      const run = await fixture.run({
         name: "count",
         malloy:
            "run: flights -> { where: id = 4 | 6; group_by: id; aggregate: seg_n }",
      });
      expectPairServedVia(run, PATH_COUNTS);
      // Live counts childless flight 4 as no segments; the pair table's row
      // for it would count one, so the count is answered live.
      expect(run.servedVia).toBeUndefined();
      expect(
         run.live
            .map((r) => [Number(r.id), Number(r.seg_n)])
            .sort((a, b) => a[0] - b[0]),
      ).toEqual([
         [4, 0],
         [6, 5],
      ]);
      expect(rowDifference(run.live, run.served)).toBeUndefined();
   });

   // Each answered live, equal to live: the flat table cannot reproduce it.
   const declined = [
      [
         "a parent aggregate written in the query",
         "run: flights -> { group_by: carrier; aggregate: c is count(), d is distance.sum(), seg_n }",
         "COUNT_SEMANTICS",
      ],
      [
         "a parent measure grouped by a child dimension",
         "run: flights -> { group_by: segments.long_leg; aggregate: flight_count, total_distance }",
         "COUNT_SEMANTICS",
      ],
      [
         "a parent measure under a child filter",
         "run: flights -> { where: segments.miles > 150; group_by: carrier; aggregate: flight_count }",
         "COUNT_SEMANTICS",
      ],
      [
         "a parent measure filtered by a child field",
         "run: flights -> { group_by: carrier; aggregate: d is total_distance { where: segments.long_leg } }",
         "COUNT_SEMANTICS",
      ],
      [
         "childless parents found through the join, counted at the parent",
         "run: flights -> { where: segments.seg_id = null; group_by: carrier; aggregate: flight_count }",
         "COUNT_SEMANTICS",
      ],
      [
         "a measure mixing both sides in one expression",
         "run: flights -> { group_by: carrier; aggregate: mixed_inline }",
         "COUNT_SEMANTICS",
      ],
      [
         "a parent measure in a nest grouped by a child dimension",
         "run: flights -> { group_by: carrier; nest: legs is { group_by: segments.long_leg; aggregate: flight_count; order_by: long_leg } }",
         "COUNT_SEMANTICS",
      ],
      [
         "a window function",
         "run: flights -> { group_by: carrier; aggregate: flight_count, seg_n; calculate: r is rank() }",
         "COUNT_SEMANTICS",
      ],
      [
         "an ungrouped aggregate",
         "run: flights -> { group_by: carrier; aggregate: seg_n, t is all(seg_n) }",
         "COUNT_SEMANTICS",
      ],
      [
         "a select: over the source",
         "run: flights -> { select: id, segments.miles }",
         "COUNT_SEMANTICS",
      ],
      [
         "the joined source's own measure, which the record cannot carry",
         "run: flights -> { group_by: carrier; aggregate: segments.seg_miles }",
         "MISSING_FIELD",
      ],
      [
         "a join to the pair source from another source",
         "run: carriers -> { group_by: carrier; aggregate: flights.flight_count }",
         "COUNT_SEMANTICS",
      ],
   ] as const;
   for (const [name, malloy, reason] of declined) {
      it(`answers live: ${name}`, async () => {
         const run = await fixture.run({ name, malloy, expectLive: true });
         expect(run.servedVia).toBeUndefined();
         expect(rowDifference(run.live, run.served)).toBeUndefined();
         fixture.setBindings(fixture.bindings);
         const { path } = await fixture.runAsIs(malloy);
         expect(path?.considered?.map((c) => c.reason)).toContain(reason);
      });
   }

   // A query that reads nothing through the inlined join is exact over the
   // pair table under the parent-row flag, but costs a scan of every pair row
   // where live scans the parents: the pair table is passed over for it.
   const parentOnly = [
      [
         "parent-level measures only",
         "run: flights -> { group_by: carrier; aggregate: flight_count, total_distance, avg_distance }",
      ],
      [
         "non-additive parent measures only",
         "run: flights -> { group_by: long_haul; aggregate: avg_distance, longest_flight, carrier_count }",
      ],
      [
         "a ratio and a filtered measure over parent fields",
         "run: flights -> { where: long_haul; group_by: carrier; aggregate: distance_per_flight, long_flights }",
      ],
      [
         "a parent aggregate written in the query",
         "run: flights -> { group_by: carrier; aggregate: c is count(), d is distance.sum() }",
      ],
      ["parent dimensions only", "run: flights -> { group_by: carrier }"],
      ["a select: of parent fields", "run: flights -> { select: id, carrier }"],
      [
         "a many-to-many source, parent measures only",
         "run: flights_notes -> { group_by: carrier; aggregate: flight_count, total_distance }",
      ],
   ] as const;
   for (const [name, malloy] of parentOnly) {
      it(`passes over the pair table, reading no child: ${name}`, async () => {
         const run = await fixture.run({ name, malloy, expectLive: true });
         expect(run.servedVia).toBeUndefined();
         expect(run.live.length).toBeGreaterThan(0);
         expect(rowDifference(run.live, run.served)).toBeUndefined();
         fixture.setBindings(fixture.bindings);
         const { path } = await fixture.runAsIs(malloy);
         const skipped = path?.considered?.find((c) => c.outcome === "skipped");
         expect(skipped?.reason).toBe("COUNT_SEMANTICS");
         expect(skipped?.detail).toMatch(/reads no child field/);
      });
   }

   it("leaves no problem on the served copy", async () => {
      expect(await fixture.servedCopyProblems()).toEqual([]);
   });

   it("answers a refused inline= (no primary key) live", async () => {
      expect(fixture.bindings.map((b) => b.sourceName).sort()).toEqual([
         "carriers",
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

   // Mutation: the same served source with its parent measures left as the
   // author wrote them (no parent-row filter) reads each flight once per
   // segment. The answer is wrong, and the query check refuses it.
   it("a parent measure without the parent-row filter answers wrong, and is refused", async () => {
      const malloy =
         "run: flights -> { group_by: carrier; aggregate: flight_count, total_distance }";
      const live = (await fixture.run({ name: "mutation", malloy })).live;
      const dir = mkdtempSync(path.join(tmpdir(), "pair-mutation-"));
      try {
         const file = path.join(dir, "pair.parquet");
         await fixture.storedSQL(
            (t) => `COPY (SELECT * FROM ${t("flights")}) TO '${file}'`,
         );
         const duckdb = new DuckDBConnection("duckdb", ":memory:");
         await duckdb.runSQL(`CREATE TABLE pair AS SELECT * FROM '${file}'`);
         const described = (await duckdb.runSQL("DESCRIBE pair")).rows as Row[];
         const types = new Map(
            described.map((r) => [
               String(r.column_name),
               String(r.column_type),
            ]),
         );
         const own = ["id", "carrier", "distance", "long_haul"].map((name) => ({
            name,
            type: types.get(name) as string,
         }));
         const pair = (
            parentMeasures: PairReroot["parentMeasures"],
         ): PairReroot => ({
            join: "segments",
            parentRowColumn: { name: "__parent_row", type: "BOOLEAN" },
            parentRowField: "__parent_row__pair",
            childColumns: ["seg_id", "flight_id", "miles", "long_leg"].map(
               (name) => ({
                  name,
                  column: `segments__${name}`,
                  internalName: `segments__${name}__pair`,
                  type: types.get(`segments__${name}`) as string,
               }),
            ),
            parentMeasures,
            childStoredDimensions: ["long_leg"],
         });
         const servedText = (p: PairReroot) =>
            `##! experimental { virtual_source }
${typeDeclaration({
   typeName: "flights__served",
   columns: [
      ...own,
      p.parentRowColumn,
      ...p.childColumns.map((c) => ({ name: c.column, type: c.type })),
   ],
})}
source: ${rerootDeclaration(flightsDeclaration, {
               sourceName: "flights",
               connectionAlias: "duckdb",
               virtualHandle: "pair",
               typeName: "flights__served",
               columns: own,
               storedDimensions: ["long_haul"],
               pair: p,
            })}`;
         const runtime = new SingleConnectionRuntime({ connection: duckdb });
         const virtualMap = new Map([["duckdb", new Map([["pair", "pair"]])]]);
         const answer = async (p: PairReroot) => {
            const query = runtime.loadModel(servedText(p)).loadQuery(malloy);
            const sql = await query.getSQL({ virtualMap } as never);
            const prepared = (await query.getPreparedQuery()) as unknown as {
               _query: { pipeline?: unknown; structRef?: unknown };
               _modelDef: { contents: Record<string, unknown> };
            };
            const refusal = flatPairQueryRefusal(
               prepared._query,
               prepared._modelDef.contents.flights as never,
               {
                  join: "segments",
                  parentRowField: "__parent_row__pair",
               },
            );
            // Raw DuckDB returns a DOUBLE sum as text; Malloy's run, a number.
            const rows = ((await duckdb.runSQL(sql)).rows as Row[]).map((r) =>
               Object.fromEntries(
                  Object.entries(r).map(([k, v]) => [
                     k,
                     typeof v === "string" && /^-?[\d.]+$/.test(v)
                        ? Number(v)
                        : v,
                  ]),
               ),
            );
            return { rows, refusal };
         };
         const flagged = await answer(
            pair(
               ["flight_count", "total_distance"].map((name) => ({
                  name,
                  internalName: `${name}__pair`,
               })),
            ),
         );
         expect(rowDifference(live, flagged.rows)).toBeUndefined();
         expect(flagged.refusal).toBeUndefined();
         const mutated = await answer(pair([]));
         expect(rowDifference(live, mutated.rows)).toBeDefined();
         expect(mutated.refusal).toMatch(/not one of its measures/);
         await duckdb.close();
      } finally {
         rmSync(dir, { recursive: true, force: true });
      }
   });
});

// The load's classification checks run on the served copy's own compile, so
// the joined source's fields must keep their classes through the pair table:
// each is written on its stored column, which the record's field reads.
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

// The same joined source through an inlined join_one (each flight id names
// at most one segment), for the other leg of the same check.
#(carries) ["none"]
#(materialize) storage="lake" inline=["segments"]
source: flights_one is duckdb.table('flights') include {
  #(classification) "none"
  public: id, carrier, distance
} extend {
  primary_key: id
  join_one: segments with id
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
            "run: flights -> { group_by: carrier; aggregate: flight_count, m is segments.miles.sum() }",
      });
      expectPairServedVia(run, PATH_COUNTS);
      expect(rowDifference(run.live, run.served)).toBeUndefined();
      // A classified child field as an output: the record cannot carry its
      // annotations onto the result, so live answers it, with them.
      const out = await fixture.run({
         name: "annotated output",
         malloy:
            "run: flights -> { group_by: segments.miles; aggregate: n is segments.count() }",
         expectLive: true,
      });
      expect(out.servedVia).toBeUndefined();
      expect(rowDifference(out.live, out.served)).toBeUndefined();
      fixture.setBindings(fixture.bindings);
      const { path } = await fixture.runAsIs(
         "run: flights -> { group_by: segments.miles; aggregate: n is segments.count() }",
      );
      expect(path?.considered?.map((c) => c.reason)).toContain("MISSING_FIELD");
   });

   // One check declines an annotated joined field returned as a column,
   // whichever record of stored columns answers the join: the pair table's
   // (an inlined join_many, one row per pair) and an inlined join_one's.
   const legs = [
      [
         "an inlined join_many, one row per pair",
         "run: flights -> { group_by: id, segments.seg_id, segments.miles }",
      ],
      [
         "an inlined join_one",
         "run: flights_one -> { group_by: id, segments.seg_id, segments.miles }",
      ],
   ] as const;
   for (const [name, malloy] of legs) {
      it(`answers an annotated joined field as a column live (MISSING_FIELD): ${name}`, async () => {
         const fixture = await buildScenario({
            files: { "m.malloy": classified },
            seedSql,
            materialize: true,
         });
         expect(await fixture.servedCopyProblems()).toEqual([]);
         const run = await fixture.run({ name, malloy, expectLive: true });
         expect(run.servedVia).toBeUndefined();
         expect(run.live.length).toBeGreaterThan(0);
         expect(rowDifference(run.live, run.served)).toBeUndefined();
         fixture.setBindings(fixture.bindings);
         const { path } = await fixture.runAsIs(malloy);
         const reasons = path?.considered?.map((c) => c.reason);
         expect(reasons).toContain("MISSING_FIELD");
         expect(reasons).not.toContain("COUNT_SEMANTICS");
      });
   }
});

// What live attaches to a joined field returned as a column, beyond its own
// notes: notes inherited from the joined source's base, a render tag, and a
// class inferred from what the field reads (no note of its own). The pair
// table's record must keep each, or the query must be answered live.
const inherited = `##! experimental.access_modifiers
source: seg_base is duckdb.table('segments') include {
  #(classification) "none"
  public: seg_id, flight_id
  #(classification) "sensitive"
  public: miles
} extend {
  primary_key: seg_id
  #(doc) "Twice the leg's miles."
  dimension: doubled is miles * 2
  dimension: tripled is miles * 3
  # label="Leg"
  dimension: leg is seg_id
}

source: segments is seg_base extend {
  measure: seg_n is count()
}

#(carries) ["none", "sensitive"]
#(materialize) storage="lake" inline=["segments"]
source: flights is duckdb.table('flights') include {
  #(classification) "none"
  public: id, carrier, distance
} extend {
  primary_key: id
  join_many: segments on segments.flight_id = id
  #(classification) "none"
  measure: flight_count is count()
  view: by_leg is { group_by: segments.leg; aggregate: n is segments.count() }
}

#(carries) ["none", "sensitive"]
#(materialize) storage="lake" inline=["segments"]
source: flights_one is duckdb.table('flights') include {
  #(classification) "none"
  public: id, carrier, distance
} extend {
  primary_key: id
  join_one: segments with id
  #(classification) "none"
  measure: flight_count is count()
}
`;

describe("R4: an inherited, render-tagged or inferred-class joined field", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
         files: { "m.malloy": inherited },
         seedSql,
         materialize: true,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   it("loads the served copy with no problems", async () => {
      expect(await fixture.servedCopyProblems()).toEqual([]);
   });

   const live = [
      [
         "a doc note inherited from the joined source's base",
         "run: flights -> { group_by: segments.doubled; aggregate: n is segments.count() }",
      ],
      [
         "a render tag (# label)",
         "run: flights -> { group_by: segments.leg; aggregate: n is segments.count() }",
      ],
      [
         "the field in a nest",
         "run: flights -> { group_by: carrier; nest: legs is { group_by: segments.leg; aggregate: n is segments.count(); order_by: leg } }",
      ],
      ["a view read by name", "run: flights -> by_leg"],
      [
         "the field in the first stage of two",
         "run: flights -> { group_by: segments.leg; aggregate: n is segments.count() } -> { group_by: leg; aggregate: t is n.sum() }",
      ],
   ] as const;
   for (const [name, malloy] of live) {
      it(`answers it live (MISSING_FIELD): ${name}`, async () => {
         const run = await fixture.run({ name, malloy, expectLive: true });
         expect(run.servedVia).toBeUndefined();
         expect(run.live.length).toBeGreaterThan(0);
         expect(rowDifference(run.live, run.served)).toBeUndefined();
         fixture.setBindings(fixture.bindings);
         const { path } = await fixture.runAsIs(malloy);
         const reasons = path?.considered?.map((c) => c.reason);
         expect(reasons).toContain("MISSING_FIELD");
         expect(reasons).not.toContain("COUNT_SEMANTICS");
      });
   }

   it("serves an inferred-class field (no note of its own), keeping its class on the stored column", async () => {
      // Each leg's own exact aggregate: a child count on the pair table, a
      // parent measure on the join_one's record.
      for (const [source, aggregate] of [
         ["flights", "segments.count()"],
         ["flights_one", "flight_count"],
      ]) {
         const malloy = `run: ${source} -> { group_by: segments.tripled; aggregate: n is ${aggregate} }`;
         const run = await fixture.run({ name: source, malloy });
         expectPairServedVia(run, PATH_COUNTS);
         expect(rowDifference(run.live, run.served)).toBeUndefined();
      }
   });

   it("serves a query that reads the annotated fields only in aggregates and filters", async () => {
      const run = await fixture.run({
         name: "aggregate and filter",
         malloy:
            "run: flights -> { where: segments.leg > 2; group_by: carrier; aggregate: m is segments.doubled.sum() }",
      });
      expectPairServedVia(run, PATH_COUNTS);
      expect(rowDifference(run.live, run.served)).toBeUndefined();
   });
});

// A row-level gate is grafted on the served copy after the query is checked,
// so the check reads the entry point's gates itself: one that filters on a
// child field would drop a parent's flagged row while its other rows stay.
const gatedSeed = [
   `CREATE TABLE flights AS SELECT * FROM (VALUES
      (1, 'AA', 100.0), (2, 'AA', 250.0), (3, 'UA', 300.0), (4, 'DL', 50.0), (8, 'AA', 410.0)
    ) AS t(id, carrier, distance)`,
   // Flight 1's first segment (its flagged row) is west, its second east.
   `CREATE TABLE segs AS SELECT * FROM (VALUES
      (1, 1, 'west', 5.0), (2, 1, 'east', 7.0), (3, 2, 'west', 11.0),
      (4, 3, 'east', 13.0), (5, 8, 'east', 1.0)
    ) AS t(seg_id, flight_id, region, w)`,
   // Flight 1 has two red notes, flight 2 one red and one blue, flight 3 one
   // blue: a gate on the notes keeps flight 1's rows twice over a join_many.
   `CREATE TABLE notes AS SELECT * FROM (VALUES
      (1, 1, 'red'), (2, 1, 'red'), (3, 2, 'red'), (4, 2, 'blue'), (5, 3, 'blue'),
      (6, 8, 'red')
    ) AS t(note_id, flight_id, color)`,
];

const gatedModel = `##! experimental.givens
given: R :: string
given: C :: string
given: K :: string

source: segs is duckdb.table('segs') extend { primary_key: seg_id }

#(access_filter) segs.region = $R
#(materialize) storage="lake" inline=["segs"]
source: by_region is duckdb.table('flights') extend {
  primary_key: id
  join_many: segs on segs.flight_id = id
  measure:
    flight_count is count()
    total_distance is distance.sum()
    seg_n is segs.count()
    seg_w is segs.w.sum()
}

#(access_filter) carrier = $C
#(materialize) storage="lake" inline=["segs"]
source: by_carrier is duckdb.table('flights') extend {
  primary_key: id
  join_many: segs on segs.flight_id = id
  measure:
    flight_count is count()
    total_distance is distance.sum()
    seg_n is segs.count()
    seg_w is segs.w.sum()
}

// A join_many the pair table does not inline, to a source stored in the same
// destination, so the served copy keeps that join over the pair rows.
#(materialize) storage="lake"
source: notes_stored is duckdb.table('notes') extend { primary_key: note_id }

#(access_filter) ns.color = $K
#(materialize) storage="lake" inline=["segs"]
source: by_note is duckdb.table('flights') extend {
  primary_key: id
  join_many: segs on segs.flight_id = id
  join_many: ns is notes_stored on ns.flight_id = id
  measure:
    flight_count is count()
    total_distance is distance.sum()
    seg_n is segs.count()
    seg_w is segs.w.sum()
}

// The same joins, gated on the parent: the join_many is there, the gate does
// not read it.
#(access_filter) carrier = $C
#(materialize) storage="lake" inline=["segs"]
source: by_carrier_noted is duckdb.table('flights') extend {
  primary_key: id
  join_many: segs on segs.flight_id = id
  join_many: ns is notes_stored on ns.flight_id = id
  measure:
    flight_count is count()
    total_distance is distance.sum()
    seg_n is segs.count()
    seg_w is segs.w.sum()
}
`;

describe("R4: a row-level gate on a pair source", () => {
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
         files: { "m.malloy": gatedModel },
         seedSql: gatedSeed,
         materialize: true,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   const mixed = (source: string) =>
      `run: ${source} -> { group_by: carrier; aggregate: flight_count, total_distance, seg_n, seg_w }`;

   it("answers parent measures live under a gate that reads the child", async () => {
      const malloy = mixed("by_region");
      const run = await fixture.run({
         name: "child gate",
         malloy,
         givens: { R: "east" },
      });
      expect(run.servedVia).toBeUndefined();
      // Live keeps flight 1 (an east segment); its flagged row is west.
      expect(run.live.find((r) => r.carrier === "AA")?.flight_count).toBe(2);
      expect(rowDifference(run.live, run.served)).toBeUndefined();
   });

   it("serves child measures under a gate that reads the child", async () => {
      const run = await fixture.run({
         name: "child gate, child measures",
         malloy:
            "run: by_region -> { group_by: carrier; aggregate: seg_n, seg_w }",
         givens: { R: "east" },
      });
      expectPairServedVia(run, PATH_COUNTS);
      expect(rowDifference(run.live, run.served)).toBeUndefined();
   });

   it("serves parent and child measures under a gate on the parent", async () => {
      const run = await fixture.run({
         name: "parent gate",
         malloy: mixed("by_carrier"),
         givens: { C: "AA" },
      });
      expectPairServedVia(run, PATH_COUNTS);
      expect(run.served.map((r) => r.carrier)).toEqual(["AA"]);
      expect(rowDifference(run.live, run.served)).toBeUndefined();
   });

   it("leaves no problem on the served copy", async () => {
      expect(await fixture.servedCopyProblems()).toEqual([]);
   });

   // A gate through a join_many the pair table does not inline joins every
   // pair row to each of its parent's matching rows: child aggregates over
   // the flat rows count a segment once per matching note, so it is live.
   it("answers child measures live under a gate through a join_many it does not inline", async () => {
      const malloy =
         "run: by_note -> { group_by: carrier; aggregate: seg_n, seg_w }";
      const run = await fixture.run({
         name: "fan-out gate",
         malloy,
         givens: { K: "red" },
         expectLive: true,
      });
      expect(run.live.length).toBeGreaterThan(0);
      expect(rowDifference(run.live, run.served)).toBeUndefined();
      expect(run.servedVia).toBeUndefined();
      fixture.setBindings(fixture.bindings);
      const { path } = await fixture.runAsIs(malloy, { K: "red" });
      const skipped = path?.considered?.find((c) => c.outcome === "skipped");
      expect(skipped?.reason).toBe("COUNT_SEMANTICS");
      expect(skipped?.detail).toMatch(/fans out the pair table's rows/);
   });

   it("serves a source beside a join_many it does not inline, under a gate on the parent", async () => {
      const run = await fixture.run({
         name: "parent gate beside a join_many",
         malloy: mixed("by_carrier_noted"),
         givens: { C: "AA" },
      });
      expectPairServedVia(run, PATH_COUNTS);
      expect(rowDifference(run.live, run.served)).toBeUndefined();
   });
});

// A pair source the served copy cannot read with its join as a record is left
// as written, with a warning saying why, and the model's other tables serve.
const unreadable = `source: segments is duckdb.table('segments') extend {
  primary_key: seg_id
  measure: seg_miles is miles.sum()
}

#(materialize) storage="lake" inline=["segments"]
source: reads_child_measure is duckdb.table('flights') extend {
  primary_key: id
  join_many: segments on segments.flight_id = id
  measure:
    flight_count is count()
    via_child is segments.seg_miles
}

#(materialize) storage="lake" inline=["segments"]
source: shared_statement is duckdb.table('flights') extend {
  primary_key: id
  join_many:
    segments on segments.flight_id = id
    notes is duckdb.table('notes') on notes.carrier = carrier
  measure: seg_n is segments.count()
}

#(materialize) storage="lake"
source: plain is duckdb.table('flights') extend {
  primary_key: id
  measure: flight_count is count()
}
`;

describe("R4: a pair table the served copy cannot read flat", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
         files: { "m.malloy": unreadable },
         seedSql,
         materialize: true,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   it("warns for each, by its own reason, and serves the other tables", async () => {
      const problems = await fixture.servedCopyProblems();
      expect(problems.map((p) => [p.sourceName, p.reason]).sort()).toEqual([
         ["reads_child_measure", "pair_not_compiled"],
         ["shared_statement", "pair_join_not_found"],
      ]);
      const plain = await fixture.run({
         name: "plain",
         malloy: "run: plain -> { group_by: carrier; aggregate: flight_count }",
      });
      expect(plain.servedVia).toBe("served_copy");
      expect(rowDifference(plain.live, plain.served)).toBeUndefined();
      for (const malloy of [
         "run: reads_child_measure -> { group_by: carrier; aggregate: flight_count, via_child }",
         "run: shared_statement -> { group_by: carrier; aggregate: seg_n }",
      ]) {
         const run = await fixture.run({
            name: malloy,
            malloy,
            expectLive: true,
         });
         expect(run.servedVia).toBeUndefined();
         expect(rowDifference(run.live, run.served)).toBeUndefined();
      }
   });
});

// A join_many reached through a join_one fans out the pair rows as much as one
// on the source: a query that filters or groups through it is answered live.
// Both joins are to sources stored in the same destination, so the served copy
// keeps them over the pair rows.
const hubSeed = [
   ...gatedSeed.filter((sql) => !/TABLE notes /.test(sql)),
   `CREATE TABLE hubs AS SELECT * FROM (VALUES ('AA'), ('UA'), ('DL')) AS t(code)`,
   // Hub AA has two gates of one name, UA one, DL none.
   `CREATE TABLE gates AS SELECT * FROM (VALUES
      (1, 'AA', 'A1'), (2, 'AA', 'A1'), (3, 'UA', 'A1')
    ) AS t(gate_id, code, gate)`,
];

const hubModel = `source: segs is duckdb.table('segs') extend { primary_key: seg_id }

#(materialize) storage="lake"
source: gates_stored is duckdb.table('gates') extend { primary_key: gate_id }

#(materialize) storage="lake"
source: hubs_stored is duckdb.table('hubs') extend {
  primary_key: code
  join_many: gs is gates_stored on gs.code = code
}

#(materialize) storage="lake" inline=["segs"]
source: by_hub is duckdb.table('flights') extend {
  primary_key: id
  join_many: segs on segs.flight_id = id
  join_one: hub is hubs_stored on hub.code = carrier
  dimension: hub_gate is hub.gs.gate
  measure:
    flight_count is count()
    seg_n is segs.count()
    seg_w is segs.w.sum()
}
`;

describe("R4: a one-hop pair table beside a join_one whose source joins many", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
         files: { "m.malloy": hubModel },
         seedSql: hubSeed,
         materialize: true,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   it("leaves no problem on the served copy", async () => {
      expect(await fixture.servedCopyProblems()).toEqual([]);
   });

   const cases = [
      [
         "a filter through it",
         "run: by_hub -> { where: hub.gs.gate = 'A1'; aggregate: seg_n, seg_w }",
         false,
      ],
      [
         "a group-by through it",
         "run: by_hub -> { group_by: hub.gs.gate; aggregate: seg_n, seg_w }",
         false,
      ],
      [
         "a group-by on a dimension through it",
         "run: by_hub -> { group_by: hub_gate; aggregate: seg_n }",
         false,
      ],
      [
         "a filtered measure reading through it",
         "run: by_hub -> { aggregate: r is seg_n { where: hub.gs.gate = 'A1' } }",
         false,
      ],
      [
         "the join_one's own field",
         "run: by_hub -> { group_by: hub.code; aggregate: flight_count, seg_n }",
         true,
      ],
   ] as const;
   for (const [name, malloy, servedHere] of cases) {
      it(`${servedHere ? "serves" : "answers live"}: ${name}`, async () => {
         const run = await fixture.run({
            name,
            malloy,
            ...(servedHere ? {} : { expectLive: true }),
         });
         expect(run.live.length).toBeGreaterThan(0);
         expect(rowDifference(run.live, run.served)).toBeUndefined();
         expectPairServedVia(run, PATH_COUNTS, servedHere);
      });
   }
});

// A one-hop pair table's `__parent_row` marks one row per distinct parent key,
// so two parents of one key, or two null keys, share one flagged row where
// live aggregates each: the build checks the parent key first and is refused,
// so the source answers live, equal to live.
const keyedModel = `source: segs is duckdb.table('segs') extend { primary_key: seg_id }

#(materialize) storage="lake" inline=["segs"]
source: flights is duckdb.table('flights') extend {
  primary_key: id
  join_many: segs on segs.flight_id = id
  measure:
    flight_count is count()
    total_distance is distance.sum()
    seg_n is segs.count()
    seg_w is segs.w.sum()
}
`;

const parentKeySeeds = {
   "null parent keys": `CREATE TABLE flights AS SELECT * FROM (VALUES
      (1, 'AA', 100.0), (2, 'AA', 250.0), (NULL, 'UA', 30.0), (NULL, 'UA', 40.0), (8, 'DL', 410.0)
    ) AS t(id, carrier, distance)`,
   "a parent key held twice": `CREATE TABLE flights AS SELECT * FROM (VALUES
      (1, 'AA', 100.0), (2, 'AA', 250.0), (3, 'UA', 30.0), (3, 'UA', 40.0), (8, 'DL', 410.0)
    ) AS t(id, carrier, distance)`,
};

for (const [label, flights] of Object.entries(parentKeySeeds)) {
   describe(`R4: a one-hop pair table over ${label} is not built, and answers live`, () => {
      let restore: () => void;
      let fixture: R4Fixture;
      beforeAll(async () => {
         restore = installSettings({
            env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
         }).restore;
         fixture = await buildScenario({
            files: { "m.malloy": keyedModel },
            seedSql: [
               flights,
               ...gatedSeed.filter((sql) => /TABLE segs /.test(sql)),
            ],
            materialize: true,
            checkKeys: true,
         });
      });
      afterAll(async () => {
         restore();
         await disposeScenarios();
      });

      // Each would differ from live over the table: the flag counts the
      // shared key once.
      for (const malloy of [
         "run: flights -> { group_by: carrier; aggregate: flight_count, total_distance }",
         "run: flights -> { group_by: carrier; aggregate: flight_count, total_distance, seg_n, seg_w }",
      ]) {
         it(`answers live, equal to live: ${malloy}`, async () => {
            const run = await fixture.run({
               name: malloy,
               malloy,
               expectLive: true,
            });
            expect(rowDifference(run.live, run.served)).toBeUndefined();
            expect(run.servedVia).toBeUndefined();
         });
      }

      it("refuses the build, naming the key", () => {
         expect(Object.keys(fixture.checkFailures)).toEqual(["flights"]);
         expect(fixture.checkFailures.flights).toMatch(
            /keys one row flag on 'id' of 'flights' itself/,
         );
      });
   });
}

// The key check counts the rows the build stores, not the rows the source's
// own `where:` keeps at read. An upper-bound rolling window (#1687) is left
// out of the build and re-applied at read, so a key held twice across the
// cut-off is one row live and two in the table: the flag can land on the row
// the window drops, and the parent goes missing from parent measures. The
// row past the cut-off comes first, so DuckDB's unordered row_number flags it.
describe("R4: a one-hop pair table whose key repeats only past an upper-bound window is not built", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
         files: {
            "m.malloy": `source: segs is duckdb.table('segs') extend { primary_key: seg_id }

#(materialize) storage="lake" inline=["segs"]
source: flights is duckdb.table('flights') extend {
  primary_key: id
  where: ts < now - 30 days
  join_many: segs on segs.flight_id = id
  measure:
    flight_count is count()
    total_distance is distance.sum()
    seg_n is segs.count()
}
`,
         },
         seedSql: [
            `CREATE TABLE flights AS SELECT * FROM (VALUES
      (3, 'UA', 40.0, TIMESTAMP '2999-01-01'), (1, 'AA', 100.0, TIMESTAMP '2020-01-01'),
      (2, 'AA', 250.0, TIMESTAMP '2020-01-02'), (3, 'UA', 30.0, TIMESTAMP '2020-01-03'),
      (8, 'DL', 410.0, TIMESTAMP '2020-01-04')
    ) AS t(id, carrier, distance, ts)`,
            ...gatedSeed.filter((sql) => /TABLE segs /.test(sql)),
         ],
         materialize: true,
         checkKeys: true,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   it("answers live, equal to live: a parent measure beside the join", async () => {
      const malloy =
         "run: flights -> { group_by: carrier; aggregate: flight_count, total_distance, seg_n }";
      const run = await fixture.run({ name: malloy, malloy, expectLive: true });
      expect(rowDifference(run.live, run.served)).toBeUndefined();
      expect(run.servedVia).toBeUndefined();
   });

   it("refuses the build, naming the key", () => {
      expect(fixture.checkFailures.flights).toMatch(
         /keys one row flag on 'id' of 'flights' itself/,
      );
   });
});
