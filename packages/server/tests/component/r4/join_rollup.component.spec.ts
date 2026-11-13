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
 * Since malloydata/malloy#3208 (Malloy 0.0.438) live counts a childless
 * parent as no children and sums nothing for it, so counts and sums through
 * a join_many are served. A `min` or `max` of a value that is not NULL on the
 * unmatched row, and an aggregate at a join of the joined source keyed on a
 * computed dimension, still read that row live and are declined.
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
  #(preaggregate) grain=["flight_id"]${placement}
  measure: floor_longest is max(coalesce(miles, 0))
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
   // Rows first: a guard that lets a wrong answer through shows here.
   expect(rowDifference(run.live, run.served)).toBeUndefined();
   expect(run.servedBy?.kind).not.toBe("rollup");
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
   // Known limitation 29: `crew` holds each flight at most once, and its
   // rollup's build checked it, so live's rows through the join_one are the
   // variant's rows and the parent's own aggregates read them as live does.
   [
      "the parent's own aggregates beside a join_one checked unique",
      "run: flights -> { group_by: carrier; aggregate: flight_count, total_distance, crew.crew_size }",
   ],
   [
      "a having: on a parent aggregate beside a join_one checked unique",
      "run: flights -> { group_by: carrier; aggregate: crew.crew_size; having: flight_count > 1 }",
   ],
   [
      "a grand total of parent and joined aggregates through a join_one checked unique",
      "run: flights -> { aggregate: flight_count, crew.crew_size, crew.most_hours, crew.crew_count }",
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
   [
      "a max of a value that is not NULL on a childless parent",
      "run: flights -> { group_by: carrier; aggregate: segments.floor_longest }",
      "COUNT_SEMANTICS",
   ],
] as const;

/** Count-shaped cases, served since malloydata/malloy#3208 skips a childless parent's row. */
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

   for (const [name, malloy] of COUNTS) {
      it(`serves ${name}`, async () => {
         expectServedFromRollup(await fixture.run({ name, malloy }));
      });
   }

   it("live counts a childless parent as no children (malloydata/malloy#3208)", async () => {
      // Flight 4 has no segments: one child live before 0.0.438.
      fixture.setBindings([]);
      const { rows } = await fixture.runAsIs(
         "run: flights -> { where: id = 4; aggregate: segments.seg_count, segments.padded_miles }",
      );
      expect(Number(rows[0]?.seg_count)).toBe(0);
      expect(Number(rows[0]?.padded_miles)).toBe(0);
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

/**
 * Row 27: a joined source's rollup with a fan-out grain dimension (A6) or a
 * join-level measure (A2), read through the join under the base leg's rule.
 * Segment 10 has two line kinds (the fan-out), segments 11, 13 and 15 have no
 * lines, segment 14's one line has no kind and segment 16's no qty; flight 4
 * has no segments. Kinds `p` and `q` are each reached from several flights,
 * so a join-level sum re-summed across flights counts a kind twice.
 */
const fanOutSeedSql = [
   ...seedSql,
   `CREATE TABLE lines_raw AS SELECT * FROM (VALUES
      (1, 10, 'a', 1.0), (2, 10, 'b', 2.0), (3, 12, 'a', 3.0),
      (4, 14, NULL, 4.0), (5, 16, 'a', NULL), (6, 99, 'a', 9.0)
    ) AS t(line_id, seg_id, kind, qty)`,
   `CREATE TABLE kinds_raw AS SELECT * FROM (VALUES
      ('p', 10.0), ('q', 3.0), ('z', 100.0)
    ) AS t(kind, weight)`,
   // Flight 1's crew holds two certificates, flight 5's one without a kind.
   `CREATE TABLE certs_raw AS SELECT * FROM (VALUES
      (1, 1, 'pilot'), (2, 1, 'medic'), (3, 3, 'pilot'), (4, 5, NULL)
    ) AS t(cert_id, flight_id, kind)`,
];

const fanOutModel = `source: lines is duckdb.table('lines_raw') extend { primary_key: line_id }
source: kinds is duckdb.table('kinds_raw') extend { primary_key: kind }
source: certs is duckdb.table('certs_raw') extend { primary_key: cert_id }
source: segments is duckdb.table('segs_raw') extend {
  primary_key: seg_id
  join_many: lines on lines.seg_id = seg_id
  join_one: kinds with kind
  join_one: kinds_or_p is kinds with kind_or_p
  dimension:
    line_kind is lines.kind
    kind_or_p is coalesce(kind, 'p')
  #(preaggregate) grain=["flight_id", "line_kind"]
  measure: seg_miles is miles.sum()
  #(preaggregate) grain=["flight_id", "line_kind"]
  measure: longest is miles.max()
  #(preaggregate) grain=["flight_id", "kind"]
  measure: kind_weight is kinds.weight.sum()
  #(preaggregate) grain=["flight_id", "kind"]
  measure: heaviest is kinds.weight.max()
  #(preaggregate) grain=["flight_id", "kind"]
  measure: kind_count is kinds.count()
  #(preaggregate) grain=["flight_id", "seg_id"]
  measure: line_qty is lines.qty.sum()
  #(preaggregate) grain=["flight_id", "kind_or_p"]
  measure: p_weight is kinds_or_p.weight.sum()
  #(preaggregate) grain=["flight_id", "kind_or_p"]
  measure: p_kind_count is kinds_or_p.count()
}
source: crew is duckdb.table('crew_raw') extend {
  primary_key: flight_id
  join_many: certs on certs.flight_id = flight_id
  dimension: cert_kind is certs.kind
  #(preaggregate) grain=["flight_id", "cert_kind"]
  measure: crew_size is size.sum()
}
source: flights is duckdb.table('flights_raw') extend {
  primary_key: id
  join_many: segments on segments.flight_id = id
  join_one: crew with id
  measure: flight_count is count()
  measure: total_seg_miles is segments.seg_miles
  dimension: seg_line_kind is segments.line_kind
  view: miles_by_line_kind is { group_by: segments.line_kind; aggregate: segments.seg_miles }
  view: a_miles is miles_by_line_kind + { where: segments.line_kind = 'a' }
  view: a_miles_by_carrier is a_miles + { group_by: carrier }
  view: miles_by_carrier is { group_by: carrier; aggregate: segments.seg_miles }
  view: a_miles_flat is miles_by_carrier + { where: segments.line_kind = 'a' }
}
`;

const FAN_OUT_SERVED = [
   [
      "a fan-out dimension grouped, its NULL group included (A6)",
      "run: flights -> { group_by: segments.line_kind; aggregate: segments.seg_miles }",
   ],
   [
      "a fan-out dimension fixed to one value (A6)",
      "run: flights -> { where: segments.line_kind = 'a'; group_by: carrier; aggregate: segments.seg_miles }",
   ],
   [
      "a fan-out dimension fixed to NULL, with the parent's count (A6)",
      "run: flights -> { where: segments.line_kind = null; group_by: carrier; aggregate: segments.seg_miles, flight_count }",
   ],
   [
      "a fan-out dimension fixed in a conjunct (A6)",
      "run: flights -> { where: segments.line_kind = 'a' and carrier = 'AA'; aggregate: segments.seg_miles, segments.longest }",
   ],
   [
      "a fan-out dimension grouped beside a parent dimension, with the parent's count (A6)",
      "run: flights -> { group_by: carrier, segments.line_kind; aggregate: segments.seg_miles, segments.longest, flight_count }",
   ],
   [
      "a fan-out dimension grouped in a nest, the outer level reading no joined measure (A6)",
      "run: flights -> { group_by: carrier; nest: k is { group_by: segments.line_kind; aggregate: segments.seg_miles } }",
   ],
   [
      "a fan-out dimension grouped in a nest, the outer level aggregating the parent's own rows (A6)",
      "run: flights -> { group_by: carrier; aggregate: flight_count; nest: k is { group_by: segments.line_kind; aggregate: segments.seg_miles } }",
   ],
   [
      "a fan-out dimension grouped above a nest that aggregates (A6)",
      "run: flights -> { group_by: segments.line_kind; nest: c is { group_by: carrier; aggregate: segments.seg_miles } }",
   ],
   [
      "a fan-out dimension fixed above a nest that aggregates (A6)",
      "run: flights -> { where: segments.line_kind = 'a'; group_by: carrier; nest: n is { group_by: id; aggregate: segments.seg_miles } }",
   ],
   [
      "a calculate: over a fan-out measure with its dimension grouped (A6)",
      "run: flights -> { group_by: segments.line_kind; aggregate: segments.seg_miles; calculate: prev is lag(segments.seg_miles); order_by: line_kind }",
   ],
   [
      "a having: with the fan-out dimension grouped (A6)",
      "run: flights -> { group_by: segments.line_kind; aggregate: segments.seg_miles; having: segments.seg_miles > 3 }",
   ],
   [
      "a named view that groups the fan-out dimension (A6, row 11)",
      "run: flights -> miles_by_line_kind",
   ],
   [
      "a refinement view that fixes the fan-out dimension (A6, row 11)",
      "run: flights -> a_miles",
   ],
   [
      "a refinement of a refinement (A6, row 11)",
      "run: flights -> a_miles_by_carrier",
   ],
   [
      "a refinement that fixes the fan-out dimension of a view that drops it (A6, row 11)",
      "run: flights -> a_miles_flat",
   ],
   [
      "a join-level sum with its other grain dimension grouped (A2)",
      "run: flights -> { group_by: segments.flight_id; aggregate: segments.kind_weight }",
   ],
   [
      "a join-level sum with the parent's key grouped, which fixes the join's key (A2)",
      "run: flights -> { group_by: id; aggregate: segments.kind_weight; order_by: id }",
   ],
   [
      "a join-level sum with the parent's key fixed (A2)",
      "run: flights -> { where: id = 1; aggregate: segments.kind_weight }",
   ],
   [
      "a join-level sum of a join_many's column, a NULL qty and childless parents included (A2)",
      "run: flights -> { group_by: id; aggregate: segments.line_qty; order_by: id }",
   ],
   [
      "a join-level max across the join's key (row 23 through the join)",
      "run: flights -> { group_by: carrier; aggregate: segments.heaviest }",
   ],
   [
      "a join-level max as a grand total (row 23 through the join)",
      "run: flights -> { aggregate: segments.heaviest, flight_count }",
   ],
   [
      "a declared join_one read at a finer grain, its fan-out dimension grouped (A6)",
      "run: flights -> { group_by: crew.cert_kind; aggregate: crew.crew_size }",
   ],
   [
      "a declared join_one read at a finer grain, its fan-out dimension fixed (A6)",
      "run: flights -> { where: crew.cert_kind = 'pilot'; group_by: carrier; aggregate: crew.crew_size }",
   ],
] as const;

const FAN_OUT_LIVE = [
   [
      "a measure across a fan-out grain dimension (A6)",
      "run: flights -> { group_by: carrier; aggregate: segments.seg_miles }",
      "GRAIN_TOO_COARSE",
   ],
   [
      "a calculate: over a fan-out measure the query does not aggregate (A6)",
      "run: flights -> { group_by: carrier; calculate: prev is lag(segments.seg_miles); order_by: carrier }",
      "GRAIN_TOO_COARSE",
   ],
   [
      "a fan-out dimension fixed to two values (A6)",
      "run: flights -> { where: segments.line_kind = 'a' | 'b'; group_by: carrier; aggregate: segments.seg_miles }",
      "GRAIN_TOO_COARSE",
   ],
   [
      "a fan-out dimension fixed in one branch of an or (A6)",
      "run: flights -> { where: segments.line_kind = 'a' or carrier = 'AA'; group_by: carrier; aggregate: segments.seg_miles }",
      "GRAIN_TOO_COARSE",
   ],
   [
      "a fan-out measure aggregated above the nest that groups its dimension (A6)",
      "run: flights -> { group_by: carrier; aggregate: segments.seg_miles; nest: k is { group_by: segments.line_kind; aggregate: segments.seg_miles } }",
      "GRAIN_TOO_COARSE",
   ],
   [
      "a fan-out measure in a nest that neither groups nor fixes its dimension (A6)",
      "run: flights -> { group_by: carrier; nest: n is { group_by: id; aggregate: segments.seg_miles } }",
      "GRAIN_TOO_COARSE",
   ],
   [
      "a parent measure that reads a fan-out measure, its dimension dropped (A6)",
      "run: flights -> { group_by: carrier; aggregate: total_seg_miles }",
      "GRAIN_TOO_COARSE",
   ],
   [
      "an expression over a fan-out measure, its dimension dropped (A6)",
      "run: flights -> { group_by: carrier; aggregate: m is segments.seg_miles * 2 }",
      "GRAIN_TOO_COARSE",
   ],
   [
      "a parent dimension over the fan-out dimension, which the check does not trace (A6)",
      "run: flights -> { group_by: seg_line_kind; aggregate: segments.seg_miles }",
      "GRAIN_TOO_COARSE",
   ],
   [
      "a having: on a fan-out measure, its dimension dropped (A6)",
      "run: flights -> { group_by: carrier; aggregate: flight_count; having: segments.seg_miles > 11 }",
      "GRAIN_TOO_COARSE",
   ],
   [
      "an ungrouped fan-out measure (A6)",
      "run: flights -> { group_by: segments.line_kind; aggregate: segments.seg_miles, t is all(segments.seg_miles) }",
      "GRAIN_TOO_COARSE",
   ],
   [
      "a named view that drops the fan-out dimension (A6, row 11)",
      "run: flights -> miles_by_carrier",
      "GRAIN_TOO_COARSE",
   ],
   [
      "a join-level sum across the flights that share a kind (A2)",
      "run: flights -> { group_by: carrier; aggregate: segments.kind_weight }",
      "GRAIN_TOO_COARSE",
   ],
   [
      "a join-level sum grouped by its key alone (A2)",
      "run: flights -> { group_by: segments.kind; aggregate: segments.kind_weight }",
      "GRAIN_TOO_COARSE",
   ],
   [
      "a join-level sum in an expression across the flights (A2)",
      "run: flights -> { aggregate: w is segments.kind_weight * 2 }",
      "GRAIN_TOO_COARSE",
   ],
   [
      "a declared join_one read at a finer grain, its fan-out dimension dropped (A6)",
      "run: flights -> { group_by: carrier; aggregate: crew.crew_size }",
      "GRAIN_TOO_COARSE",
   ],
   [
      "a join-level sum through a join keyed on a computed dimension, which matches a row for a childless parent",
      "run: flights -> { group_by: id; aggregate: segments.p_weight; order_by: id }",
      "COUNT_SEMANTICS",
   ],
   [
      "a join-level count through a join keyed on a computed dimension, which matches a row for a childless parent",
      "run: flights -> { group_by: id; aggregate: segments.p_kind_count; order_by: id }",
      "COUNT_SEMANTICS",
   ],
] as const;

describe("R4: join-to-rollup (A7) with fan-out grain dimensions and join-level measures (row 27)", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
         files: { "m.malloy": fanOutModel },
         seedSql: fanOutSeedSql,
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

   for (const [name, malloy] of FAN_OUT_SERVED) {
      it(`serves ${name}`, async () => {
         expectServedFromRollup(await fixture.run({ name, malloy }));
      });
   }
   for (const [name, malloy, reason] of FAN_OUT_LIVE) {
      it(`answers ${name} live (${reason})`, async () => {
         const run = await fixture.run({ name, malloy });
         // Rows first: a guard that lets a wrong answer through shows here.
         expect(rowDifference(run.live, run.served)).toBeUndefined();
         expect(run.servedBy?.kind).not.toBe("rollup");
         expect(await reasonOf(malloy)).toContain(reason);
      });
   }

   it("serves a join-level count with its other grain dimension fixed", async () => {
      const malloy =
         "run: flights -> { group_by: id; aggregate: segments.kind_count; order_by: id }";
      expectServedFromRollup(await fixture.run({ name: "count", malloy }));
   });
});

/**
 * A join read as a join_many (declared one, or a declared join_one re-pointed
 * as one at a grain finer than its key) assumes each joined row meets one
 * parent row. Keyed on a field the query source does not keep unique (not
 * its primary key), a rollup row meets several parent rows: a fact table's
 * join_one to a dimension read at a finer grain, or a join_many from a source
 * without a primary key. Live deduplicates or repeats at the joined row; the
 * rollup row cannot, so the leg declines.
 */
describe("R4: join-to-rollup (A7) through a join keyed on a field the query source repeats", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
         files: {
            "m.malloy": `source: certs is duckdb.table('certs_raw') extend { primary_key: cert_id }
source: crew is duckdb.table('crew_raw') extend {
  primary_key: flight_id
  join_many: certs on certs.flight_id = flight_id
  dimension: cert_kind is certs.kind
  #(preaggregate) grain=["flight_id"]
  measure: crew_size is size.sum()
  #(preaggregate) grain=["flight_id", "cert_kind"]
  measure: crew_by_cert is size.sum()
  #(preaggregate) grain=["flight_id", "duty_hours"]
  measure: crew_by_hours is size.sum()
}
source: segments is duckdb.table('segs_raw') extend {
  primary_key: seg_id
  join_one: crew with flight_id
  #(preaggregate) grain=["flight_id"]
  measure: seg_miles is miles.sum()
}
source: legs is duckdb.table('legs_raw') extend {
  join_many: segments on segments.flight_id = id
  measure: leg_count is count()
}
`,
         },
         seedSql: [
            ...seedSql,
            `CREATE TABLE certs_raw AS SELECT * FROM (VALUES
               (1, 1, 'pilot'), (2, 1, 'medic'), (3, 3, 'pilot'), (4, 5, NULL)
             ) AS t(cert_id, flight_id, kind)`,
            // Flight 1 flown as two legs; no primary key.
            `CREATE TABLE legs_raw AS SELECT * FROM (VALUES
               (1, 'AA'), (1, 'BB'), (2, 'AA'), (4, 'CC')
             ) AS t(id, carrier)`,
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
         "a fact's join_one to a dimension read at a finer grain",
         "run: segments -> { aggregate: crew.crew_by_hours }",
      ],
      [
         "a fact's join_one read at a finer grain, its fan-out dimension grouped",
         "run: segments -> { group_by: crew.cert_kind; aggregate: crew.crew_by_cert }",
      ],
      [
         "a fact's join_one read at a finer grain, its fan-out dimension fixed",
         "run: segments -> { where: crew.cert_kind = 'pilot'; aggregate: crew.crew_by_cert }",
      ],
      [
         "a join_many from a source without a primary key",
         "run: legs -> { aggregate: segments.seg_miles, leg_count }",
      ],
      [
         "a join_many from a source without a primary key, by its key",
         "run: legs -> { group_by: id; aggregate: segments.seg_miles }",
      ],
   ] as const) {
      it(`answers ${name} live`, async () => {
         // Rows first: served from the rollup, these differ from live.
         const run = await fixture.run({ name, malloy });
         expect(rowDifference(run.live, run.served)).toBeUndefined();
         expect(run.servedBy?.kind).not.toBe("rollup");
      });
   }

   it("serves a fact's join_one read at its key, many facts to one key", async () => {
      expectServedFromRollup(
         await fixture.run({
            name: "join_one at its key",
            malloy:
               "run: segments -> { group_by: kind; aggregate: crew.crew_size }",
         }),
      );
   });
});

describe("R4: join-to-rollup (A7) declines a joined source's rollup in a storage destination the query source is not stored in", () => {
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
      // Known limitation 29: another join_one the query reads, its key found
      // repeated by its rollup's build. Re-pointing `segments` changes how
      // Malloy aggregates over the rows `crew_dup` repeats (15.5 live against
      // 10.5 served, before the check).
      [
         "a joined measure beside a join_one whose key the data repeats",
         "run: flights -> { group_by: carrier; aggregate: segments.seg_miles, crew_dup.crew_size }",
         "COUNT_SEMANTICS",
      ],
      [
         "a joined measure grouped by a join_one whose key the data repeats",
         "run: flights -> { group_by: crew_dup.size; aggregate: segments.seg_miles }",
         "COUNT_SEMANTICS",
      ],
      [
         "a joined measure filtered on a join_one whose key the data repeats",
         "run: flights -> { where: crew_dup.size > 2; aggregate: segments.seg_miles }",
         "COUNT_SEMANTICS",
      ],
   ] as const) {
      it(`answers ${name} live (${reason})`, async () => {
         expectLive(await fixture.run({ name, malloy }));
         expect(await reasonOf(malloy)).toContain(reason);
      });
   }

   it("says the key was found repeated, not merely unchecked", async () => {
      fixture.setBindings(fixture.bindings);
      for (const malloy of [
         "run: flights -> { group_by: carrier; aggregate: flight_count, crew_dup.crew_size }",
         "run: flights -> { group_by: carrier; aggregate: segments.seg_miles, crew_dup.crew_size }",
      ]) {
         const { path } = await fixture.runAsIs(malloy);
         const details = (path?.considered ?? [])
            .filter((c) => c.kind === "rollup")
            .map((c) => (c as { detail?: string }).detail ?? "");
         expect(details.some((d) => d.includes("more than once"))).toBe(true);
      }
   });

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

/** The fixture's crew, a declared join_one, with flight 1 held twice. */
const CREW_DUP_MODEL = `source: segments is duckdb.table('segs_raw') extend {
  primary_key: seg_id
  #(preaggregate) grain=["flight_id"]
  measure: seg_miles is miles.sum()
}
source: crew_dup is duckdb.table('crew_dup_raw') extend {
  #(preaggregate) grain=["flight_id"]
  measure: crew_size is size.sum()
}
source: flights is duckdb.table('flights_raw') extend {
  primary_key: id
  join_many: segments on segments.flight_id = id
  join_one: crew_dup on crew_dup.flight_id = id
  measure: flight_count is count()
}
`;
const CREW_DUP_SEED = [
   ...seedSql,
   `CREATE TABLE crew_dup_raw AS SELECT * FROM (VALUES
      (1, 3), (1, 4), (3, 2), (5, 7)
    ) AS t(flight_id, size)`,
];

describe("R4: join-to-rollup (A7) where no build checked the join_one's key (known limitation 29)", () => {
   let restore: () => void;
   let unique: R4Fixture;
   let repeated: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      // As for a table a host's manifest binds, or a build whose check
      // could not run: unknown.
      unique = await buildScenario({
         files: { "m.malloy": model("") },
         seedSql,
         preaggregate: true,
         rollupKeyChecks: false,
      });
      repeated = await buildScenario({
         files: { "m.malloy": CREW_DUP_MODEL },
         seedSql: CREW_DUP_SEED,
         preaggregate: true,
         rollupKeyChecks: false,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   const reasonsOf = async (fixture: R4Fixture, malloy: string) => {
      fixture.setBindings(fixture.bindings);
      const { path } = await fixture.runAsIs(malloy);
      return (path?.considered ?? []).filter((c) => c.kind === "rollup");
   };

   it("answers the parent's own aggregates beside a join_one live, as before the check (COUNT_SEMANTICS)", async () => {
      const malloy =
         "run: flights -> { group_by: carrier; aggregate: flight_count, total_distance, crew.crew_size }";
      expectLive(await unique.run({ name: "unchecked", malloy }));
      const considered = await reasonsOf(unique, malloy);
      expect(considered.map((c) => c.reason)).toContain("COUNT_SEMANTICS");
      expect(
         considered.some((c) =>
            ((c as { detail?: string }).detail ?? "").includes(
               "not checked unique",
            ),
         ),
      ).toBe(true);
   });

   it("answers a join_many whose query source's key was not checked live, fail closed (COUNT_SEMANTICS)", async () => {
      const malloy =
         "run: flights -> { group_by: carrier; aggregate: segments.seg_miles }";
      expectLive(await unique.run({ name: "unchecked parent key", malloy }));
      const considered = await reasonsOf(unique, malloy);
      expect(considered.map((c) => c.reason)).toContain("COUNT_SEMANTICS");
      expect(
         considered.some((c) =>
            ((c as { detail?: string }).detail ?? "").includes(
               "was not checked unique and never NULL",
            ),
         ),
      ).toBe(true);
   });

   it("still serves a join_one's own measures, which live sums once per joined row too", async () => {
      expectServedFromRollup(
         await repeated.run({
            name: "own measure",
            malloy:
               "run: flights -> { group_by: carrier, id; aggregate: crew_dup.crew_size }",
         }),
      );
   });

   it("answers a parent aggregate beside the unchecked join_one live (COUNT_SEMANTICS)", async () => {
      const malloy =
         "run: flights -> { group_by: carrier; aggregate: flight_count, crew_dup.crew_size }";
      expectLive(await repeated.run({ name: "unchecked repeat", malloy }));
      expect(
         (await reasonsOf(repeated, malloy)).map((c) => c.reason),
      ).toContain("COUNT_SEMANTICS");
   });
});

describe("R4: join-to-rollup (A7) and a key check that no longer describes the table (known limitation 29)", () => {
   let restore: () => void;
   let stale: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      // A pass bound over a table whose rows now repeat the key: what a
      // check over an earlier generation would say. A run never binds one
      // (a rebuild checks again, and a run forgets the bound check of every
      // table it is about to write: rollup_key_check.spec.ts); this is why.
      stale = await buildScenario({
         files: { "m.malloy": CREW_DUP_MODEL },
         seedSql: CREW_DUP_SEED,
         preaggregate: true,
         rollupKeyChecks: "unique",
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   it("would serve an answer live does not give, so only a current check is bound", async () => {
      const run = await stale.run({
         name: "stale pass",
         malloy:
            "run: flights -> { group_by: carrier; aggregate: flight_count, crew_dup.crew_size }",
      });
      expect(run.servedBy?.kind).toBe("rollup");
      expect(rowDifference(run.live, run.served)).toBeDefined();
   });
});

/**
 * A join read as a join_many takes the leg only on the query source's
 * declared primary key, and a declared key the data breaks breaks the leg: a
 * rollup row met by two parent rows sharing a key is summed twice, where live
 * sums each joined row once; a parent with a NULL key is counted by the
 * variant's `COUNT(1)`, where live's `COUNT(DISTINCT id)` skips it. Served
 * only when the rollup's build found the query source's key unique and never
 * NULL.
 */
const PARENT_KEY_MODEL = `source: certs is duckdb.table('certs_raw') extend {
  primary_key: cert_id
  measure: cert_count is count()
  measure: cert_rows is sum(1)
}
source: segments is duckdb.table('segs_raw') extend {
  primary_key: seg_id
  #(preaggregate) grain=["flight_id"]
  #(preaggregate) grain=["flight_id", "kind"]
  measure: seg_miles is miles.sum()
  #(preaggregate) grain=["flight_id"]
  measure: key_miles is miles.sum()
}
source: flights is duckdb.table('flights_raw') extend {
  primary_key: id
  join_many: segments on segments.flight_id = id
  join_many: certs on certs.flight_id = id
  measure: flight_count is count()
  measure: written_cert_count is certs.count()
}
source: flights_dup is duckdb.table('flights_dup_raw') extend {
  primary_key: id
  join_many: segments on segments.flight_id = id
  measure: flight_count is count()
}
source: flights_null is duckdb.table('flights_null_raw') extend {
  primary_key: id
  join_many: segments on segments.flight_id = id
  measure: flight_count is count()
}
`;
const PARENT_KEY_SEED = [
   ...seedSql,
   `CREATE TABLE certs_raw AS SELECT * FROM (VALUES
      (1, 1, 'pilot'), (2, 1, 'medic'), (3, 3, 'pilot'), (4, 5, NULL)
    ) AS t(cert_id, flight_id, kind)`,
   // Flight 1 held twice.
   `CREATE TABLE flights_dup_raw AS SELECT * FROM (VALUES
      (1, 'AA', 100), (1, 'BB', 150), (2, 'AA', 200), (3, 'BB', 300), (4, 'CC', 400)
    ) AS t(id, carrier, distance)`,
   // One flight with no id.
   `CREATE TABLE flights_null_raw AS SELECT * FROM (VALUES
      (1, 'AA', 100), (2, 'AA', 200), (3, 'BB', 300), (NULL, 'BB', 350),
      (4, 'CC', 400), (5, 'BB', 500)
    ) AS t(id, carrier, distance)`,
];

describe("R4: join-to-rollup (A7) through a join_many whose parent key the data breaks", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
         files: { "m.malloy": PARENT_KEY_MODEL },
         seedSql: PARENT_KEY_SEED,
         preaggregate: true,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   const reasonsOf = async (malloy: string) => {
      fixture.setBindings(fixture.bindings);
      const { path } = await fixture.runAsIs(malloy);
      return (path?.considered ?? []).filter((c) => c.kind === "rollup");
   };

   for (const [name, malloy, says] of [
      [
         "a sum through a join_many, by a parent key the data repeats",
         "run: flights_dup -> { group_by: id; aggregate: segments.seg_miles; order_by: id }",
         "more than once",
      ],
      [
         "a grand total through a join_many whose parent key the data repeats",
         "run: flights_dup -> { aggregate: segments.seg_miles, flight_count }",
         "more than once",
      ],
      [
         "a fan-out read through a join_many whose parent key the data repeats",
         "run: flights_dup -> { group_by: segments.kind; aggregate: segments.seg_miles }",
         "more than once",
      ],
      [
         "the parent's count beside a join_many, with a NULL parent key",
         "run: flights_null -> { aggregate: flight_count, segments.seg_miles }",
         "more than once",
      ],
      [
         "the parent's count by a parent dimension, with a NULL parent key",
         "run: flights_null -> { group_by: carrier; aggregate: flight_count, segments.seg_miles }",
         "more than once",
      ],
   ] as const) {
      it(`answers ${name} live (COUNT_SEMANTICS)`, async () => {
         // Rows first: served from the rollup, these differ from live.
         const run = await fixture.run({ name, malloy });
         expect(rowDifference(run.live, run.served)).toBeUndefined();
         expect(run.servedBy?.kind).not.toBe("rollup");
         const considered = await reasonsOf(malloy);
         expect(considered.map((c) => c.reason)).toContain("COUNT_SEMANTICS");
         expect(
            considered.some((c) =>
               ((c as { detail?: string }).detail ?? "").includes(says),
            ),
         ).toBe(true);
      });
   }

   it("serves the same queries over a parent key the data keeps unique", async () => {
      for (const malloy of [
         "run: flights -> { group_by: id; aggregate: segments.seg_miles; order_by: id }",
         "run: flights -> { aggregate: segments.seg_miles, flight_count }",
         "run: flights -> { group_by: segments.kind; aggregate: segments.seg_miles }",
      ]) {
         expectServedFromRollup(await fixture.run({ name: malloy, malloy }));
      }
   });

   // A count through another join_many beside a join_many the leg re-points
   // as a join_one (`key_miles` is stored only at the key, so that leg is the
   // only one): with the fan-out gone, Malloy before 0.0.438 wrote the count
   // as COUNT(1) and counted the row a LEFT JOIN writes for a parent with no
   // certs as one cert (6 against live's 4). Since malloydata/malloy#3208 a
   // joined source's count reads its key, so these are served.
   for (const [name, malloy] of [
      [
         "a count through another join_many",
         "run: flights -> { aggregate: segments.key_miles, c is certs.count() }",
      ],
      [
         "another join_many's count measure, by a parent dimension",
         "run: flights -> { group_by: carrier; aggregate: segments.key_miles, certs.cert_count }",
      ],
      [
         "a parent measure that counts through another join_many",
         "run: flights -> { aggregate: segments.key_miles, written_cert_count }",
      ],
      [
         "a sum of a constant through another join_many",
         "run: flights -> { aggregate: segments.key_miles, certs.cert_rows }",
      ],
   ] as const) {
      it(`serves ${name} beside a re-pointed join`, async () => {
         expectServedFromRollup(await fixture.run({ name, malloy }));
      });
   }

   for (const [name, malloy] of [
      [
         "another join_many's max beside a re-pointed join",
         "run: flights -> { aggregate: segments.key_miles, m is certs.cert_id.max() }",
      ],
      [
         "another join_many's grouped dimension beside a re-pointed join",
         "run: flights -> { group_by: certs.kind; aggregate: segments.key_miles }",
      ],
   ] as const) {
      it(`serves ${name}`, async () => {
         expectServedFromRollup(await fixture.run({ name, malloy }));
      });
   }
});

// The query source's rows depend on a given: a primary key check compiled
// once at plan time reads the default caller's rows (flight 3 is held once
// among AA's), while another caller's query reads its own (BB's hold flight
// 3 twice), so the check cannot stand for every caller and is not run.
const GIVEN_KEY_MODEL = `##! experimental.givens
given: CARRIER :: string is 'AA'

source: segments is duckdb.table('segs_raw') extend {
  primary_key: seg_id
  #(preaggregate) grain=["flight_id"]
  measure: seg_miles is miles.sum()
}
source: flights_g is duckdb.table('flights_g_raw') extend {
  primary_key: id
  where: carrier = $CARRIER
  join_many: segments on segments.flight_id = id
  measure: flight_count is count()
}
`;
const GIVEN_KEY_SEED = [
   ...seedSql,
   `CREATE TABLE flights_g_raw AS SELECT * FROM (VALUES
      (1, 'AA', 100), (2, 'AA', 200), (3, 'BB', 300), (3, 'BB', 350), (5, 'BB', 500)
    ) AS t(id, carrier, distance)`,
];

describe("R4: join-to-rollup (A7) through a join_many on the key of a query source whose rows depend on a given", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
         files: { "m.malloy": GIVEN_KEY_MODEL },
         seedSql: GIVEN_KEY_SEED,
         preaggregate: true,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   for (const malloy of [
      "run: flights_g -> { group_by: id; aggregate: segments.seg_miles; order_by: id }",
      "run: flights_g -> { aggregate: segments.seg_miles, flight_count }",
   ]) {
      it(`answers ${malloy} live for every caller (COUNT_SEMANTICS)`, async () => {
         for (const givens of [{ CARRIER: "AA" }, { CARRIER: "BB" }]) {
            // Rows first: served from the rollup, BB's differ from live.
            const run = await fixture.run({ name: malloy, malloy, givens });
            expect(rowDifference(run.live, run.served)).toBeUndefined();
            expect(run.servedBy?.kind).not.toBe("rollup");
            fixture.setBindings(fixture.bindings);
            const { path } = await fixture.runAsIs(malloy, givens);
            const considered = (path?.considered ?? []).filter(
               (c) => c.kind === "rollup",
            );
            expect(considered.map((c) => c.reason)).toContain(
               "COUNT_SEMANTICS",
            );
            expect(
               considered.some((c) =>
                  ((c as { detail?: string }).detail ?? "").includes(
                     "was not checked unique",
                  ),
               ),
            ).toBe(true);
         }
      });
   }
});

/**
 * The joined source's own primary key, NULL or repeated in the data. Since
 * malloydata/malloy#3208 live takes a count, sum or average at `segments`
 * over the rows whose `seg_id` is not NULL, and once per distinct `seg_id`
 * where the rows fan out; the rollup, built at `segments`' own grain, reads
 * every row. Each rollup's build checks the key, and a count, sum or average
 * through the join is answered live when the check found it NULL or
 * repeated. Flight 1 has a segment with no `seg_id` (9 miles), flight 2 one
 * `seg_id` twice; a `min` or `max` reads every row live too, so is served.
 */
const NULL_CHILD_KEY_SEED = [
   `CREATE TABLE flights_raw AS SELECT * FROM (VALUES
      (1, 'AA', 100), (2, 'AA', 200), (3, 'BB', 300), (4, 'CC', 400)
    ) AS t(id, carrier, distance)`,
   `CREATE TABLE segs_raw AS SELECT * FROM (VALUES
      (10, 1, 'p', 2.0), (11, 1, 'q', 3.0), (NULL, 1, 'p', 9.0), (12, 2, 'p', 4.0),
      (12, 2, 'p', 1.5), (14, 3, 'q', 1.0), (16, 3, 'p', 7.0)
    ) AS t(seg_id, flight_id, kind, miles)`,
   `CREATE TABLE certs_raw AS SELECT * FROM (VALUES
      (1, 1), (2, 1), (3, 2), (4, 3)
    ) AS t(cert_id, flight_id)`,
];

function nullChildKeyModel(placement: string): string {
   return `source: certs is duckdb.table('certs_raw') extend { primary_key: cert_id }
source: segments is duckdb.table('segs_raw') extend {
  primary_key: seg_id
  #(preaggregate) grain=["flight_id"]${placement}
  measure: seg_miles is miles.sum()
  #(preaggregate) grain=["flight_id"]${placement}
  measure: seg_count is count()
  #(preaggregate) grain=["flight_id"]${placement}
  measure: padded_miles is sum(miles + 1)
  #(preaggregate) grain=["flight_id"]${placement}
  measure: longest is miles.max()
}
source: flights is duckdb.table('flights_raw') extend {
  primary_key: id
  join_many: segments on segments.flight_id = id
  join_many: certs on certs.flight_id = id
}
`;
}

const NULL_CHILD_KEY_LIVE = [
   [
      "a sum of a column",
      "run: flights -> { group_by: carrier; aggregate: segments.seg_miles }",
   ],
   [
      "a count",
      "run: flights -> { group_by: carrier; aggregate: segments.seg_count }",
   ],
   [
      "a sum of a computed value",
      "run: flights -> { group_by: carrier; aggregate: segments.padded_miles }",
   ],
   [
      "a count in a nest",
      "run: flights -> { group_by: carrier; nest: n is { group_by: id; aggregate: segments.seg_count } }",
   ],
   [
      "a sum and a count beside another join_many",
      "run: flights -> { group_by: carrier; aggregate: segments.seg_miles, segments.seg_count, cert_n is certs.count() }",
   ],
] as const;

describe("R4: join-to-rollup (A7) through a join to a source whose own primary key the data holds NULL or repeated", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
         files: { "m.malloy": nullChildKeyModel("") },
         seedSql: NULL_CHILD_KEY_SEED,
         preaggregate: true,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   for (const [name, malloy] of NULL_CHILD_KEY_LIVE) {
      it(`answers ${name} live (COUNT_SEMANTICS)`, async () => {
         // Rows first: served from the rollup, flight 1's NULL-keyed segment
         // is counted and summed where live skips it.
         expectLive(await fixture.run({ name, malloy }));
         fixture.setBindings(fixture.bindings);
         const { path } = await fixture.runAsIs(malloy);
         const considered = (path?.considered ?? []).filter(
            (c) => c.kind === "rollup",
         );
         expect(considered.map((c) => c.reason)).toContain("COUNT_SEMANTICS");
         expect(
            considered.some((c) =>
               ((c as { detail?: string }).detail ?? "").includes(
                  "primary key `seg_id` more than once or NULL",
               ),
            ),
         ).toBe(true);
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
