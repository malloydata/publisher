// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4 for an incremental pair table (materialization v2, known limitation 9):
 * an inlined join_many whose `#(materialize)` declares
 * `refresh="incremental"` with a watermark on both sides,
 * `watermark=["updated_at", "segments.loaded_at"]`. Each refresh replaces
 * every row of each parent whose watermark moved, by the parent key, so the
 * parent-row flag stays exactly one row per parent.
 *
 * The table is built in full at b, then refreshed in place twice by the
 * production delta planner and apply (planIncrementalStep, applyIncrementalStep),
 * each refresh after the source tables change: parents gaining and losing
 * children, a childless parent gaining its first, a parent losing its last,
 * late-arriving children of old parents, restated parents and children, new
 * parents with and without children, and an orphan child. After each, the
 * stored table equals a full rebuild at the same instant (every row, the
 * watermark included; the flag one row per parent, as the build's window
 * leaves which row unspecified), and every served answer equals live,
 * parent-only queries (which skip the pair table) included. A rolling window
 * deletes whole parents, and its table equals a rebuild within the window.
 *
 * Mutation checks: without the delete by parent key the parent-row check
 * fails the delta, which rolls back and is rebuilt in full; without the check
 * too, the table counts a parent twice and a served parent measure is wrong.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   applyIncrementalStep,
   type IncrementalRunContext,
} from "../../../src/service/materialization/run/incremental_build";
import {
   canonicalBoundValue,
   planIncrementalStep,
   warehouseDeltaTarget,
   type IncrementalLineage,
   type IncrementalStep,
   type WatermarkBound,
} from "../../../src/service/materialization/run/incremental_apply";
import {
   buildScenario,
   DESTINATION,
   disposeScenarios,
   rowDifference,
   type R4Fixture,
   type Row,
   expectPairServedVia,
} from "./harness";

/** Measures that count through the inlined path (see {@link expectPairServedVia}). */
const PATH_COUNTS: string[] = [];

const B = "2026-11-10 00:00:00"; // the full build
const R1 = "2026-11-11 00:00:00"; // first refresh
const R2 = "2026-11-12 00:00:00"; // second refresh

// Flights 1–12, 0 to 12 segments each; 4, 5, 9 and 11 childless. Flights 1–4
// were last updated in September, outside the 30-day window at b.
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
const updated = (id: number) =>
   id <= 4
      ? `2026-09-${String(10 + id).padStart(2, "0")} 00:00:00`
      : `2026-10-${String(10 + id).padStart(2, "0")} 00:00:00`;
const segmentRows: string[] = [];
let segId = 1;
for (const [flight, n] of Object.entries(fanOut)) {
   for (let i = 0; i < n; i++) {
      segmentRows.push(
         `(${segId}, ${flight}, ${(segId * 37) % 500}.0, TIMESTAMP '${updated(Number(flight))}' + INTERVAL ${i} HOUR)`,
      );
      segId++;
   }
}
segmentRows.push("(99, 99, 10.0, TIMESTAMP '2026-10-01 00:00:00')");

const seedSql = [
   `CREATE TABLE flights AS SELECT id, carrier, distance::DOUBLE AS distance, updated_at FROM (VALUES
      ${Array.from({ length: 12 }, (_, i) => i + 1)
         .map(
            (id) =>
               `(${id}, '${["AA", "UA", "DL"][id % 3]}', ${id * 75}.0, TIMESTAMP '${updated(id)}')`,
         )
         .join(", ")}
    ) AS t(id, carrier, distance, updated_at)`,
   `CREATE TABLE segments AS SELECT seg_id, flight_id, miles::DOUBLE AS miles, loaded_at FROM (VALUES ${segmentRows.join(", ")}) AS t(seg_id, flight_id, miles, loaded_at)`,
];

const body = `
  join_many: segments on segments.flight_id = id
  dimension: long_haul is distance > 400
  measure:
    flight_count is count()
    total_distance is distance.sum()
    seg_total is segments.miles.sum()
    seg_n is segments.count()
    long_legs is segments.count() { where: segments.long_leg }
    miles_per_flight is seg_total / flight_count
`;

const model = `source: segments is duckdb.table('segments') extend {
  primary_key: seg_id
  dimension: long_leg is miles > 200
}

#(materialize) storage="lake" inline=["segments"] refresh="incremental" watermark=["updated_at", "segments.loaded_at"]
source: flights is duckdb.table('flights') extend {
  primary_key: id${body}}

#(materialize) storage="lake" inline=["segments"] refresh="incremental" watermark=["updated_at", "segments.loaded_at"]
source: recent is duckdb.table('flights') extend {
  primary_key: id
  where: updated_at >= now - 30 days${body}}
`;

/** A refresh's instant as the run's `now`. */
const at = (ts: string) => new Date(`${ts.replace(" ", "T")}Z`);

const WINDOW = {
   field: "updated_at",
   op: ">=" as const,
   offset: { sign: -1 as const, amount: 30, unit: "day" as const },
};

/** The lineage a refresh of `source`'s pair table runs under. */
function lineageOf(fixture: R4Fixture, source: string): IncrementalLineage {
   return {
      physicalTableName: `${DESTINATION}.${fixture.physicalTables[source]}`,
      connectionName: "duckdb",
      sourceEntityId: fixture.tableIds[source],
      watermarkName: "__pair_watermark",
      watermarkType: "timestamp",
      mergeKeys: ["id"],
      scopeColumns: [],
      strategy: "merge",
      pair: { parentKey: "id", parentRow: "__parent_row" },
      ...(source === "recent" ? { rollingWindow: [WINDOW] } : {}),
   };
}

/** A run context whose ledger records each advance, in memory. */
function contextAt(
   now: Date,
   advanced: WatermarkBound[],
): IncrementalRunContext {
   return {
      environmentId: "r4",
      packageName: "r4",
      materializationId: "m",
      forceRefresh: false,
      now,
      declarations: {},
      ledger: {
         getIncrementalLedgerEntry: async () => null,
         upsertIncrementalLedgerEntry: async (e: {
            coveredThroughValue: string;
            coveredThroughType: string;
         }) => {
            advanced.push({
               malloyType: e.coveredThroughType,
               value: e.coveredThroughValue,
            });
         },
         deleteIncrementalLedgerEntry: async () => {},
      } as unknown as IncrementalRunContext["ledger"],
   };
}

/**
 * The refresh helpers over one scenario: plan and apply a refresh of a
 * source's pair table from its recorded boundary, as a build does, and
 * compare the stored table with a full rebuild. `fixture` is set once the
 * scenario is built.
 */
function pairRefresher() {
   const state = {} as { fixture: R4Fixture };
   // Each table's boundary, as the ledger records it.
   const covered: Record<string, WatermarkBound> = {};

   const runner = (sql: string) =>
      state.fixture.duckdb.runSQL(sql, { rowLimit: 100_000 }) as Promise<{
         rows: unknown[];
      }>;
   const rows = async (sql: string) => (await runner(sql)).rows as Row[];
   const table = (source: string) =>
      `${DESTINATION}.${state.fixture.physicalTables[source]}`;

   /** Plan one refresh of `source` at `now` from its recorded boundary. */
   async function plan(
      source: string,
      now: string,
   ): Promise<{
      step: IncrementalStep;
      lineage: IncrementalLineage;
      target: ReturnType<typeof warehouseDeltaTarget>;
   }> {
      state.fixture.setClock(now);
      const lineage = lineageOf(state.fixture, source);
      const target = warehouseDeltaTarget({
         dialect: "duckdb",
         runner,
         quotedTablePath: table(source),
         lineage,
         sourceSQL: state.fixture.builtSQL[source],
      });
      const columns = (await rows(`DESCRIBE ${table(source)}`)).map((r) =>
         String(r.column_name),
      );
      const step = await planIncrementalStep({
         target,
         lineage,
         ledgerEntry: {
            sourceEntityId: lineage.sourceEntityId,
            coveredThroughValue: covered[source].value,
            coveredThroughType: covered[source].malloyType,
            watermarkDimension: lineage.watermarkName,
            mergeKeyDimensions: lineage.mergeKeys,
            derivedStrategy: lineage.strategy,
            physicalTableName: lineage.physicalTableName,
            connectionName: lineage.connectionName,
            storageDestinationName: undefined,
         },
         forceRefresh: false,
         now: at(now),
         columns,
      });
      return { step, lineage, target };
   }

   /** Refresh `source` in place at `now`, as a build does, and record it. */
   async function refresh(
      source: string,
      now: string,
      mutate?: (statements: string[]) => string[],
   ) {
      const { step, lineage, target } = await plan(source, now);
      expect(step.mode).toBe("delta");
      const advanced: WatermarkBound[] = [];
      const applied = await applyIncrementalStep({
         context: contextAt(at(now), advanced),
         lineage,
         step:
            step.mode === "delta" && mutate
               ? { ...step, statements: mutate(step.statements) }
               : step,
         target,
         sourceName: source,
      });
      if (advanced.length > 0) covered[source] = advanced[0];
      return applied;
   }

   /**
    * The stored table against a full rebuild at the same clock: every row
    * but the flag the same, and the flag on exactly one row per parent.
    * `where` narrows both to the rows a read keeps (a rolling window's).
    */
   async function expectEqualsRebuild(source: string, where = "true") {
      const rebuilt = `${DESTINATION}.rebuilt_${source}`;
      await runner(
         `CREATE OR REPLACE TABLE ${rebuilt} AS ${state.fixture.builtSQL[source]}`,
      );
      const side = (t: string) =>
         `SELECT * EXCLUDE (__parent_row) FROM ${t} WHERE ${where}`;
      for (const [a, b] of [
         [table(source), rebuilt],
         [rebuilt, table(source)],
      ]) {
         const extra = await rows(
            `SELECT * FROM (${side(a)} EXCEPT ALL ${side(b)}) ORDER BY ALL`,
         );
         expect({ in: a, extra }).toEqual({ in: a, extra: [] });
      }
      const flags = await rows(
         `SELECT id FROM ${table(source)} GROUP BY id
           HAVING count(*) FILTER (WHERE __parent_row) <> 1`,
      );
      expect(flags).toEqual([]);
   }

   return {
      state,
      covered,
      runner,
      rows,
      table,
      refresh,
      expectEqualsRebuild,
   };
}

describe("R4: an incremental pair table refreshed in place equals a full rebuild, and serves live's answers", () => {
   let restore: () => void;
   const { state, covered, runner, rows, table, refresh, expectEqualsRebuild } =
      pairRefresher();
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      state.fixture = await buildScenario({
         files: { "m.malloy": model },
         seedSql,
         materialize: true,
         clock: B,
      });
      // A full build of a time watermark records the run's clock.
      for (const source of ["flights", "recent"]) {
         covered[source] = canonicalBoundValue("timestamp", at(B));
      }
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   const served = [
      [
         "parent measures beside child ones",
         "run: flights -> { group_by: carrier; aggregate: flight_count, total_distance, seg_total, seg_n }",
      ],
      [
         "child measures grouped by a child dimension",
         "run: flights -> { group_by: segments.long_leg; aggregate: seg_total, seg_n }",
      ],
      [
         "a filtered child measure and a ratio across both sides",
         "run: flights -> { group_by: carrier; aggregate: long_legs, miles_per_flight }",
      ],
      [
         "per flight, childless flights' count of none included (answered live, malloydata/malloy#3208)",
         "run: flights -> { group_by: id; aggregate: seg_n, seg_total }",
      ],
      [
         "one row per pair",
         "run: flights -> { group_by: id, segments.seg_id, segments.miles }",
      ],
      [
         "a nest of child aggregates under parent ones",
         "run: flights -> { group_by: carrier; aggregate: flight_count; nest: legs is { group_by: segments.long_leg; aggregate: seg_n; order_by: long_leg } }",
      ],
      [
         "the rolling-window source, parent and child",
         "run: recent -> { group_by: carrier; aggregate: flight_count, seg_total, seg_n }",
      ],
   ] as const;
   const parentOnly = [
      [
         "parent-only measures",
         "run: flights -> { group_by: carrier; aggregate: flight_count, total_distance }",
      ],
      [
         "parent-only measures over the rolling window",
         "run: recent -> { group_by: long_haul; aggregate: flight_count, total_distance }",
      ],
   ] as const;

   /** Every query served from the table (or skipping it) equals live. */
   async function expectServedEqualsLive(now: string) {
      state.fixture.setClock(now);
      for (const [name, malloy] of served) {
         const run = await state.fixture.run({ name, malloy });
         expectPairServedVia(run, PATH_COUNTS);
         expect(run.live.length).toBeGreaterThan(0);
         expect({ name, diff: rowDifference(run.live, run.served) }).toEqual({
            name,
            diff: undefined,
         });
      }
      for (const [name, malloy] of parentOnly) {
         state.fixture.setBindings(state.fixture.bindings);
         const run = await state.fixture.runAsIs(malloy);
         expect(
            (run.path?.considered ?? []).map((c) => [c.outcome, c.reason]),
         ).toEqual([["skipped", "COUNT_SEMANTICS"]]);
         const both = await state.fixture.run({
            name,
            malloy,
            expectLive: true,
         });
         expect(rowDifference(both.live, both.served)).toBeUndefined();
      }
   }

   it("builds in full at b: equal to a rebuild, serving live's answers", async () => {
      await expectEqualsRebuild("flights");
      await expectEqualsRebuild("recent");
      await expectServedEqualsLive(B);
   });

   it("refreshes in place after children arrive, leave, and parents change", async () => {
      const t = (h: number) =>
         `TIMESTAMP '2026-11-10 ${String(h).padStart(2, "0")}:00:00'`;
      for (const sql of [
         // A late-arriving child of a flight last updated in September.
         `INSERT INTO segments VALUES (100, 2, 333.0, ${t(1)})`,
         // A childless flight gains its first child.
         `INSERT INTO segments VALUES (101, 4, 50.0, ${t(2)}), (102, 5, 250.0, ${t(2)})`,
         // A flight loses two of its five children; it is restamped.
         `DELETE FROM segments WHERE seg_id IN (SELECT seg_id FROM segments WHERE flight_id = 6 ORDER BY seg_id LIMIT 2)`,
         `UPDATE flights SET updated_at = ${t(3)} WHERE id = 6`,
         // A flight loses its last child, and is restamped.
         `DELETE FROM segments WHERE flight_id = 8`,
         `UPDATE flights SET updated_at = ${t(4)} WHERE id = 8`,
         // A child restated in place.
         `UPDATE segments SET miles = miles + 1000, loaded_at = ${t(5)} WHERE seg_id = (SELECT min(seg_id) FROM segments WHERE flight_id = 7)`,
         // A flight restated: a new carrier and distance.
         `UPDATE flights SET carrier = 'ZZ', distance = 999.0, updated_at = ${t(6)} WHERE id = 3`,
         // New flights, one with children and one without, and an orphan.
         `INSERT INTO flights VALUES (13, 'AA', 640.0, ${t(7)}), (14, 'UA', 80.0, ${t(7)})`,
         `INSERT INTO segments VALUES (103, 13, 210.0, ${t(8)}), (104, 13, 90.0, ${t(8)}), (105, 98, 5.0, ${t(8)})`,
      ]) {
         await runner(sql);
      }
      for (const source of ["flights", "recent"]) {
         const before = await rows(
            `SELECT count(*) AS n FROM ${table(source)}`,
         );
         const outcome = await refresh(source, R1);
         expect(outcome.applied).toBe(true);
         expect(outcome.refresh).toBe("delta");
         expect(covered[source].value).toBe(
            canonicalBoundValue("timestamp", at(R1)).value,
         );
         // In place: not rewritten, only the parents that moved.
         expect(Number(before[0].n)).toBeGreaterThan(0);
      }
      await expectEqualsRebuild("flights");
      // The rolling window keeps up to a day past its cut-off, which reads trim.
      await expectEqualsRebuild(
         "recent",
         `updated_at >= TIMESTAMP '${R1}' - INTERVAL 30 DAY`,
      );
      await expectServedEqualsLive(R1);
   });

   it("refreshes again: a parent gains a second child, another loses one, a childless one is restated", async () => {
      const t = (h: number) =>
         `TIMESTAMP '2026-11-11 ${String(h).padStart(2, "0")}:00:00'`;
      for (const sql of [
         `INSERT INTO segments VALUES (106, 4, 75.0, ${t(1)})`,
         `DELETE FROM segments WHERE seg_id = 104`,
         `UPDATE flights SET updated_at = ${t(2)} WHERE id = 13`,
         `UPDATE flights SET distance = 120.0, updated_at = ${t(3)} WHERE id = 9`,
         // A late child of a flight that has left the rolling window.
         `INSERT INTO segments VALUES (107, 1, 480.0, ${t(4)})`,
      ]) {
         await runner(sql);
      }
      for (const source of ["flights", "recent"]) {
         expect((await refresh(source, R2)).refresh).toBe("delta");
      }
      await expectEqualsRebuild("flights");
      await expectEqualsRebuild(
         "recent",
         `updated_at >= TIMESTAMP '${R2}' - INTERVAL 30 DAY`,
      );
      // The window's delete removed whole parents: none left below its
      // cut-off less the margin, and each kept parent flagged once.
      const below = await rows(
         `SELECT count(*) AS n FROM ${table("recent")}
           WHERE updated_at < TIMESTAMP '${R2}' - INTERVAL 31 DAY`,
      );
      expect(Number(below[0].n)).toBe(0);
      await expectServedEqualsLive(R2);
   });

   it("leaves the table as it is when nothing moved", async () => {
      const before = await rows(
         `SELECT * FROM ${table("flights")} ORDER BY ALL`,
      );
      expect((await refresh("flights", "2026-11-12 06:00:00")).refresh).toBe(
         "delta",
      );
      expect(
         await rows(`SELECT * FROM ${table("flights")} ORDER BY ALL`),
      ).toEqual(before);
   });

   it("mutation: without the delete by parent key, the check rolls the delta back and the table is rebuilt in full", async () => {
      await runner(
         `INSERT INTO segments VALUES (108, 10, 300.0, TIMESTAMP '2026-11-12 07:00:00')`,
      );
      const before = await rows(
         `SELECT * FROM ${table("flights")} ORDER BY ALL`,
      );
      const outcome = await refresh("flights", "2026-11-12 12:00:00", (s) =>
         s.filter((x) => !x.startsWith("DELETE FROM")),
      );
      // Seeded: the caller rebuilds, and the boundary is not advanced.
      expect(outcome).toEqual({ applied: false });
      expect(
         await rows(`SELECT * FROM ${table("flights")} ORDER BY ALL`),
      ).toEqual(before);
      // The session is still usable after the rollback.
      expect(
         Number(
            (await rows(`SELECT count(*) AS n FROM ${table("flights")}`))[0].n,
         ),
      ).toBe(before.length);
   });

   it("mutation: without the delete and the check, a parent is counted twice", async () => {
      const outcome = await refresh("flights", "2026-11-12 12:00:00", (s) =>
         s.filter((x) => !x.startsWith("DELETE FROM") && !x.includes("error(")),
      );
      expect(outcome.refresh).toBe("delta");
      const twice = await rows(
         `SELECT id FROM ${table("flights")} GROUP BY id
           HAVING count(*) FILTER (WHERE __parent_row) > 1`,
      );
      expect(twice.map((r) => Number(r.id))).toEqual([10]);
      const run = await state.fixture.run({
         name: "parent measures beside child ones",
         malloy:
            "run: flights -> { group_by: carrier; aggregate: flight_count, total_distance, seg_total }",
      });
      expect(rowDifference(run.live, run.served)).toBeDefined();
   });
});

describe("R4: an incremental pair table's edges: a watermark ahead of the clock, NULL parent keys, the watermark column", () => {
   let restore: () => void;
   const { state, covered, runner, refresh, expectEqualsRebuild } =
      pairRefresher();
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      state.fixture = await buildScenario({
         files: { "m.malloy": model },
         seedSql,
         materialize: true,
         clock: B,
      });
      covered.flights = canonicalBoundValue("timestamp", at(B));
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   async function expectServedEqualsLive(now: string) {
      state.fixture.setClock(now);
      const run = await state.fixture.run({
         name: "parent and child measures per flight",
         malloy:
            "run: flights -> { group_by: id, carrier; aggregate: total_distance, seg_n, seg_total }",
      });
      expectPairServedVia(run, PATH_COUNTS);
      expect(rowDifference(run.live, run.served)).toBeUndefined();
   }

   it("does not expose the watermark column on the served source", async () => {
      state.fixture.setBindings(state.fixture.bindings);
      await expect(
         state.fixture.runAsIs(
            "run: flights -> { group_by: __pair_watermark; aggregate: seg_n }",
         ),
      ).rejects.toThrow(/__pair_watermark/);
   });

   it("a child whose watermark is ahead of the clock does not hold back its parent's later changes", async () => {
      // A child stamped in the future: its parent's watermark is that value
      // until the clock reaches it.
      await runner(
         `INSERT INTO segments VALUES (200, 10, 1.0, TIMESTAMP '2027-01-01 00:00:00')`,
      );
      expect((await refresh("flights", R1)).refresh).toBe("delta");
      await expectEqualsRebuild("flights");
      // The parent restated: a range that stopped at the clock would never
      // read it again, since its watermark stays in 2027.
      await runner(
         `UPDATE flights SET distance = 5555.0, updated_at = TIMESTAMP '2026-11-11 05:00:00' WHERE id = 10`,
      );
      expect((await refresh("flights", R2)).refresh).toBe("delta");
      await expectEqualsRebuild("flights");
      await expectServedEqualsLive(R2);
   });

   it("replaces the parents with a NULL key together, as the build's window groups them", async () => {
      await runner(
         `INSERT INTO flights VALUES (NULL, 'AA', 100.0, TIMESTAMP '2026-11-12 01:00:00'), (NULL, 'UA', 200.0, TIMESTAMP '2026-11-12 01:00:00')`,
      );
      expect((await refresh("flights", "2026-11-12 02:00:00")).refresh).toBe(
         "delta",
      );
      await expectEqualsRebuild("flights");
      await runner(
         `INSERT INTO flights VALUES (NULL, 'DL', 300.0, TIMESTAMP '2026-11-12 03:00:00')`,
      );
      expect((await refresh("flights", "2026-11-12 04:00:00")).refresh).toBe(
         "delta",
      );
      await expectEqualsRebuild("flights");
   });
});

// A path of two join_manys: flights → segments → stops. Each level's row
// flag partitions on the keys down to it, so replacing whole parents keeps
// every flag exact; the delta's check reads each level's flag over its keys.
// Flights 1–6; flight 3 childless; segment 13 has no stop.
const pathSeedSql = [
   `CREATE TABLE flights AS SELECT id, carrier, distance::DOUBLE AS distance, updated_at FROM (VALUES
      (1, 'AA', 100.0, TIMESTAMP '2026-10-01 00:00:00'),
      (2, 'UA', 200.0, TIMESTAMP '2026-10-02 00:00:00'),
      (3, 'DL', 300.0, TIMESTAMP '2026-10-03 00:00:00'),
      (4, 'AA', 400.0, TIMESTAMP '2026-10-04 00:00:00'),
      (5, 'UA', 500.0, TIMESTAMP '2026-10-05 00:00:00'),
      (6, 'DL', 600.0, TIMESTAMP '2026-10-06 00:00:00')
    ) AS t(id, carrier, distance, updated_at)`,
   `CREATE TABLE segments AS SELECT seg_id, flight_id, miles::DOUBLE AS miles, loaded_at FROM (VALUES
      (11, 1, 10.0, TIMESTAMP '2026-10-01 01:00:00'),
      (12, 1, 20.0, TIMESTAMP '2026-10-01 02:00:00'),
      (13, 2, 30.0, TIMESTAMP '2026-10-02 01:00:00'),
      (14, 4, 40.0, TIMESTAMP '2026-10-04 01:00:00'),
      (15, 5, 50.0, TIMESTAMP '2026-10-05 01:00:00'),
      (16, 6, 60.0, TIMESTAMP '2026-10-06 01:00:00')
    ) AS t(seg_id, flight_id, miles, loaded_at)`,
   `CREATE TABLE stops AS SELECT stop_id, seg_id, mins::DOUBLE AS mins, loaded_at FROM (VALUES
      (101, 11, 1.0, TIMESTAMP '2026-10-01 03:00:00'),
      (102, 11, 2.0, TIMESTAMP '2026-10-01 04:00:00'),
      (103, 12, 3.0, TIMESTAMP '2026-10-01 05:00:00'),
      (104, 14, 4.0, TIMESTAMP '2026-10-04 02:00:00'),
      (105, 15, 5.0, TIMESTAMP '2026-10-05 02:00:00'),
      (106, 16, 6.0, TIMESTAMP '2026-10-06 02:00:00')
    ) AS t(stop_id, seg_id, mins, loaded_at)`,
];

const pathModel = `source: stops is duckdb.table('stops') extend { primary_key: stop_id }
source: segments is duckdb.table('segments') extend {
  primary_key: seg_id
  join_many: stops on stops.seg_id = seg_id
}

#(materialize) storage="lake" inline=["segments.stops"] refresh="incremental" watermark=["updated_at", "segments.loaded_at", "segments.stops.loaded_at"]
source: flights is duckdb.table('flights') extend {
  primary_key: id
  join_many: segments on segments.flight_id = id
  measure:
    flight_count is count()
    total_distance is distance.sum()
    seg_n is segments.count()
    seg_total is segments.miles.sum()
    stop_n is segments.stops.count()
    stop_total is segments.stops.mins.sum()
}
`;

const PATH_FLAGS = [
   { flag: "__parent_row", keys: ["id"] },
   { flag: "__child_row", keys: ["id", "segments__seg_id"] },
];

describe("R4: an incremental pair table of a path of join_manys refreshes in place, replacing whole parents", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   let covered: WatermarkBound;
   const runner = (sql: string) =>
      fixture.duckdb.runSQL(sql, { rowLimit: 100_000 }) as Promise<{
         rows: unknown[];
      }>;
   const rows = async (sql: string) => (await runner(sql)).rows as Row[];
   const table = () => `${DESTINATION}.${fixture.physicalTables.flights}`;
   const lineage = (): IncrementalLineage => ({
      physicalTableName: table(),
      connectionName: "duckdb",
      sourceEntityId: fixture.tableIds.flights,
      watermarkName: "__pair_watermark",
      watermarkType: "timestamp",
      mergeKeys: ["id"],
      scopeColumns: [],
      strategy: "merge",
      pair: {
         parentKey: "id",
         parentRow: "__parent_row",
         checks: PATH_FLAGS,
      },
   });

   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
         files: { "m.malloy": pathModel },
         seedSql: pathSeedSql,
         materialize: true,
         clock: B,
      });
      covered = canonicalBoundValue("timestamp", at(B));
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   async function refresh(
      now: string,
      mutate?: (statements: string[]) => string[],
   ) {
      fixture.setClock(now);
      const l = lineage();
      const target = warehouseDeltaTarget({
         dialect: "duckdb",
         runner,
         quotedTablePath: table(),
         lineage: l,
         sourceSQL: fixture.builtSQL.flights,
      });
      const columns = (await rows(`DESCRIBE ${table()}`)).map((r) =>
         String(r.column_name),
      );
      const step = await planIncrementalStep({
         target,
         lineage: l,
         ledgerEntry: {
            sourceEntityId: l.sourceEntityId,
            coveredThroughValue: covered.value,
            coveredThroughType: covered.malloyType,
            watermarkDimension: l.watermarkName,
            mergeKeyDimensions: l.mergeKeys,
            derivedStrategy: l.strategy,
            physicalTableName: l.physicalTableName,
            connectionName: l.connectionName,
            storageDestinationName: undefined,
         },
         forceRefresh: false,
         now: at(now),
         columns,
      });
      expect(step.mode).toBe("delta");
      const advanced: WatermarkBound[] = [];
      const applied = await applyIncrementalStep({
         context: contextAt(at(now), advanced),
         lineage: l,
         step:
            step.mode === "delta" && mutate
               ? { ...step, statements: mutate(step.statements) }
               : step,
         target,
         sourceName: "flights",
      });
      if (advanced.length > 0) covered = advanced[0];
      return applied;
   }

   /** Every row but the flags as a rebuild's, and each flag once per its keys. */
   async function expectEqualsRebuild() {
      const rebuilt = `${DESTINATION}.rebuilt_path_flights`;
      await runner(
         `CREATE OR REPLACE TABLE ${rebuilt} AS ${fixture.builtSQL.flights}`,
      );
      const flags = PATH_FLAGS.map((f) => f.flag).join(", ");
      const side = (t: string) => `SELECT * EXCLUDE (${flags}) FROM ${t}`;
      for (const [a, b] of [
         [table(), rebuilt],
         [rebuilt, table()],
      ]) {
         const extra = await rows(
            `SELECT * FROM (${side(a)} EXCEPT ALL ${side(b)}) ORDER BY ALL`,
         );
         expect({ in: a, extra }).toEqual({ in: a, extra: [] });
      }
      for (const { flag, keys } of PATH_FLAGS) {
         const wrong = await rows(
            `SELECT ${keys.join(", ")} FROM ${table()} GROUP BY ${keys.join(", ")}
              HAVING count(*) FILTER (WHERE ${flag}) <> 1`,
         );
         expect({ flag, wrong }).toEqual({ flag, wrong: [] });
      }
   }

   const served = [
      "run: flights -> { group_by: carrier; aggregate: flight_count, total_distance, stop_n, stop_total }",
      "run: flights -> { group_by: carrier; aggregate: flight_count, seg_n, seg_total }",
      "run: flights -> { group_by: id; aggregate: seg_total, stop_n, stop_total }",
      "run: flights -> { group_by: segments.seg_id; aggregate: stop_n, stop_total }",
   ];
   async function expectServedEqualsLive(now: string) {
      fixture.setClock(now);
      for (const malloy of served) {
         const run = await fixture.run({ name: malloy, malloy });
         expectPairServedVia(run, PATH_COUNTS);
         expect({ malloy, diff: rowDifference(run.live, run.served) }).toEqual({
            malloy,
            diff: undefined,
         });
      }
   }

   it("builds in full at b: equal to a rebuild, serving live's answers", async () => {
      await expectEqualsRebuild();
      await expectServedEqualsLive(B);
   });

   it("refreshes in place after rows arrive, leave and change at every level", async () => {
      const t = (h: number) =>
         `TIMESTAMP '2026-11-10 ${String(h).padStart(2, "0")}:00:00'`;
      for (const sql of [
         // A stop added to a stored segment of a stored flight.
         `INSERT INTO stops VALUES (107, 13, 7.0, ${t(1)})`,
         // A segment with no stop added to a stored flight.
         `INSERT INTO segments VALUES (17, 4, 70.0, ${t(2)})`,
         // A childless flight gains a segment and a stop under it.
         `INSERT INTO segments VALUES (18, 3, 80.0, ${t(3)})`,
         `INSERT INTO stops VALUES (108, 18, 8.0, ${t(3)})`,
         // A stop restated in place.
         `UPDATE stops SET mins = 99.0, loaded_at = ${t(4)} WHERE stop_id = 105`,
         // A segment loses its stops and is restamped.
         `DELETE FROM stops WHERE seg_id = 11`,
         `UPDATE segments SET loaded_at = ${t(5)} WHERE seg_id = 11`,
         // A flight restated, and a new flight with no segment.
         `UPDATE flights SET carrier = 'ZZ', updated_at = ${t(6)} WHERE id = 6`,
         `INSERT INTO flights VALUES (7, 'AA', 700.0, ${t(7)})`,
      ]) {
         await runner(sql);
      }
      const outcome = await refresh(R1);
      expect(outcome).toEqual(
         expect.objectContaining({ applied: true, refresh: "delta" }),
      );
      await expectEqualsRebuild();
      await expectServedEqualsLive(R1);
   });

   it("mutation: without the delete, the checks roll the delta back", async () => {
      await runner(
         `INSERT INTO stops VALUES (109, 16, 9.0, TIMESTAMP '2026-11-11 01:00:00')`,
      );
      const before = await rows(`SELECT * FROM ${table()} ORDER BY ALL`);
      const outcome = await refresh("2026-11-11 12:00:00", (s) =>
         s.filter((x) => !x.startsWith("DELETE FROM")),
      );
      expect(outcome).toEqual({ applied: false });
      expect(await rows(`SELECT * FROM ${table()} ORDER BY ALL`)).toEqual(
         before,
      );
   });

   it("mutation: the level check alone catches a segment flagged twice", async () => {
      // Without the delete and the parent's check, only the level check is
      // left: it fails the delta, which rolls back.
      const before = await rows(`SELECT * FROM ${table()} ORDER BY ALL`);
      const outcome = await refresh("2026-11-11 12:00:00", (s) =>
         s.filter(
            (x) =>
               !x.startsWith("DELETE FROM") &&
               !(x.includes("error(") && x.includes('GROUP BY "id" ')),
         ),
      );
      expect(outcome).toEqual({ applied: false });
      expect(await rows(`SELECT * FROM ${table()} ORDER BY ALL`)).toEqual(
         before,
      );
      await refresh(R2);
      await expectEqualsRebuild();
      await expectServedEqualsLive(R2);
   });
});

// A table of two blocks: the path segments.stops, and crew. A parent's rows
// in both blocks carry one watermark, the greatest over all of them, so a
// delta replaces a parent in every block at once.
const blockSeedSql = [
   ...pathSeedSql,
   `CREATE TABLE crew AS SELECT crew_id, flight_id, duty::DOUBLE AS duty, loaded_at FROM (VALUES
      (201, 1, 5.0, TIMESTAMP '2026-10-01 06:00:00'),
      (202, 1, 6.0, TIMESTAMP '2026-10-01 07:00:00'),
      (203, 3, 7.0, TIMESTAMP '2026-10-03 01:00:00'),
      (204, 5, 8.0, TIMESTAMP '2026-10-05 03:00:00')
    ) AS t(crew_id, flight_id, duty, loaded_at)`,
];

const blockModel = `source: stops is duckdb.table('stops') extend { primary_key: stop_id }
source: segments is duckdb.table('segments') extend {
  primary_key: seg_id
  join_many: stops on stops.seg_id = seg_id
}
source: crew is duckdb.table('crew') extend { primary_key: crew_id }

#(materialize) storage="lake" inline=["segments.stops", "crew"] refresh="incremental" watermark=["updated_at", "segments.loaded_at", "segments.stops.loaded_at", "crew.loaded_at"]
source: flights is duckdb.table('flights') extend {
  primary_key: id
  join_many: segments on segments.flight_id = id
  join_many: crew on crew.flight_id = id
  measure:
    flight_count is count()
    total_distance is distance.sum()
    seg_n is segments.count()
    seg_total is segments.miles.sum()
    stop_n is segments.stops.count()
    stop_total is segments.stops.mins.sum()
    crew_n is crew.count()
    crew_duty is crew.duty.sum()
}
`;

const BLOCK_FLAGS = [
   { flag: "__parent_row", keys: ["id"], lane: 0 },
   { flag: "__child_row", keys: ["id", "segments__seg_id"], lane: 0 },
   { flag: "__parent_row", keys: ["id"], lane: 1 },
   { flag: "__child_row", keys: ["id", "crew__crew_id"], lane: 1 },
];

describe("R4: an incremental pair table of several blocks refreshes in place, replacing each parent in every block", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   let covered: WatermarkBound;
   const runner = (sql: string) =>
      fixture.duckdb.runSQL(sql, { rowLimit: 100_000 }) as Promise<{
         rows: unknown[];
      }>;
   const rows = async (sql: string) => (await runner(sql)).rows as Row[];
   const table = () => `${DESTINATION}.${fixture.physicalTables.flights}`;
   const lineage = (): IncrementalLineage => ({
      physicalTableName: table(),
      connectionName: "duckdb",
      sourceEntityId: fixture.tableIds.flights,
      watermarkName: "__pair_watermark",
      watermarkType: "timestamp",
      mergeKeys: ["id"],
      scopeColumns: [],
      strategy: "merge",
      pair: {
         parentKey: "id",
         parentRow: "__parent_row",
         checks: BLOCK_FLAGS,
         laneColumn: "__pair_lane",
         lanes: 2,
      },
   });

   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
         files: { "m.malloy": blockModel },
         seedSql: blockSeedSql,
         materialize: true,
         clock: B,
      });
      covered = canonicalBoundValue("timestamp", at(B));
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   async function refresh(
      now: string,
      mutate?: (statements: string[]) => string[],
   ) {
      fixture.setClock(now);
      const l = lineage();
      const target = warehouseDeltaTarget({
         dialect: "duckdb",
         runner,
         quotedTablePath: table(),
         lineage: l,
         sourceSQL: fixture.builtSQL.flights,
      });
      const columns = (await rows(`DESCRIBE ${table()}`)).map((r) =>
         String(r.column_name),
      );
      const step = await planIncrementalStep({
         target,
         lineage: l,
         ledgerEntry: {
            sourceEntityId: l.sourceEntityId,
            coveredThroughValue: covered.value,
            coveredThroughType: covered.malloyType,
            watermarkDimension: l.watermarkName,
            mergeKeyDimensions: l.mergeKeys,
            derivedStrategy: l.strategy,
            physicalTableName: l.physicalTableName,
            connectionName: l.connectionName,
            storageDestinationName: undefined,
         },
         forceRefresh: false,
         now: at(now),
         columns,
      });
      expect(step.mode).toBe("delta");
      const advanced: WatermarkBound[] = [];
      const applied = await applyIncrementalStep({
         context: contextAt(at(now), advanced),
         lineage: l,
         step:
            step.mode === "delta" && mutate
               ? { ...step, statements: mutate(step.statements) }
               : step,
         target,
         sourceName: "flights",
      });
      if (advanced.length > 0) covered = advanced[0];
      return applied;
   }

   async function expectEqualsRebuild() {
      const rebuilt = `${DESTINATION}.rebuilt_block_flights`;
      await runner(
         `CREATE OR REPLACE TABLE ${rebuilt} AS ${fixture.builtSQL.flights}`,
      );
      const side = (t: string) =>
         `SELECT * EXCLUDE (__parent_row, __child_row) FROM ${t}`;
      for (const [a, b] of [
         [table(), rebuilt],
         [rebuilt, table()],
      ]) {
         const extra = await rows(
            `SELECT * FROM (${side(a)} EXCEPT ALL ${side(b)}) ORDER BY ALL`,
         );
         expect({ in: a, extra }).toEqual({ in: a, extra: [] });
      }
      for (const { flag, keys, lane } of BLOCK_FLAGS) {
         const wrong = await rows(
            `SELECT ${keys.join(", ")} FROM ${table()} WHERE __pair_lane = ${lane}
              GROUP BY ${keys.join(", ")} HAVING count(*) FILTER (WHERE ${flag}) <> 1`,
         );
         expect({ flag, lane, wrong }).toEqual({ flag, lane, wrong: [] });
      }
   }

   const served = [
      "run: flights -> { group_by: carrier; aggregate: flight_count, stop_n, stop_total }",
      "run: flights -> { group_by: carrier; aggregate: flight_count, seg_n, seg_total }",
      "run: flights -> { group_by: carrier; aggregate: flight_count, total_distance, crew_n, crew_duty }",
      "run: flights -> { group_by: id; aggregate: crew_n, crew_duty }",
      "run: flights -> { group_by: id; aggregate: seg_total, stop_n }",
   ];
   async function expectServedEqualsLive(now: string) {
      fixture.setClock(now);
      for (const malloy of served) {
         const run = await fixture.run({ name: malloy, malloy });
         expectPairServedVia(run, PATH_COUNTS);
         expect({ malloy, diff: rowDifference(run.live, run.served) }).toEqual({
            malloy,
            diff: undefined,
         });
      }
   }

   it("builds in full at b: equal to a rebuild, serving live's answers", async () => {
      await expectEqualsRebuild();
      await expectServedEqualsLive(B);
   });

   it("exposes neither watermark column on the served source", async () => {
      fixture.setBindings(fixture.bindings);
      expect(
         (await rows(`DESCRIBE ${table()}`)).map((r) => r.column_name),
      ).toEqual(
         expect.arrayContaining(["__pair_watermark", "__pair_watermark_block"]),
      );
      for (const column of ["__pair_watermark", "__pair_watermark_block"]) {
         await expect(
            fixture.runAsIs(
               `run: flights -> { group_by: ${column}; aggregate: flight_count }`,
            ),
         ).rejects.toThrow(new RegExp(column));
      }
   });

   it("refreshes in place after rows change in each block", async () => {
      const t = (h: number) =>
         `TIMESTAMP '2026-11-10 ${String(h).padStart(2, "0")}:00:00'`;
      for (const sql of [
         // Only the crew block's rows of flight 2 change: its path rows must
         // be written again with it.
         `INSERT INTO crew VALUES (205, 2, 9.0, ${t(1)})`,
         // Only the path block's rows of flight 1 change.
         `INSERT INTO stops VALUES (107, 12, 7.0, ${t(2)})`,
         // A crew row leaves flight 1's other block; it is restamped.
         `DELETE FROM crew WHERE crew_id = 202`,
         `UPDATE flights SET updated_at = ${t(3)} WHERE id = 1`,
         // A new flight with a crew row and no segment.
         `INSERT INTO flights VALUES (7, 'AA', 700.0, ${t(4)})`,
         `INSERT INTO crew VALUES (206, 7, 1.5, ${t(4)})`,
         // A segment with no stop added to the childless flight 3.
         `INSERT INTO segments VALUES (17, 3, 70.0, ${t(5)})`,
      ]) {
         await runner(sql);
      }
      const outcome = await refresh(R1);
      expect(outcome).toEqual(
         expect.objectContaining({ applied: true, refresh: "delta" }),
      );
      await expectEqualsRebuild();
      await expectServedEqualsLive(R1);
   });

   it("mutation: a delta holding one block's rows of a parent, the other block's dropped, rolls back", async () => {
      await runner(
         `INSERT INTO crew VALUES (208, 6, 3.0, TIMESTAMP '2026-11-11 00:30:00')`,
      );
      const before = await rows(`SELECT * FROM ${table()} ORDER BY ALL`);
      // As a per-block watermark would stage it: flight 6's crew rows only.
      const outcome = await refresh("2026-11-11 06:00:00", (s) =>
         s.map((x) =>
            x.startsWith("CREATE OR REPLACE TEMP TABLE")
               ? x.replace(
                    / AS (.*)$/s,
                    " AS SELECT * FROM ($1) AS __d WHERE __pair_lane = 1",
                 )
               : x,
         ),
      );
      expect(outcome).toEqual({ applied: false });
      expect(await rows(`SELECT * FROM ${table()} ORDER BY ALL`)).toEqual(
         before,
      );
   });

   it("mutation: without the delete, the per-block checks roll the delta back", async () => {
      await runner(
         `INSERT INTO crew VALUES (207, 4, 2.0, TIMESTAMP '2026-11-11 01:00:00')`,
      );
      const before = await rows(`SELECT * FROM ${table()} ORDER BY ALL`);
      const outcome = await refresh("2026-11-11 12:00:00", (s) =>
         s.filter((x) => !x.startsWith("DELETE FROM")),
      );
      expect(outcome).toEqual({ applied: false });
      expect(await rows(`SELECT * FROM ${table()} ORDER BY ALL`)).toEqual(
         before,
      );
      await refresh(R2);
      await expectEqualsRebuild();
      await expectServedEqualsLive(R2);
   });
});
