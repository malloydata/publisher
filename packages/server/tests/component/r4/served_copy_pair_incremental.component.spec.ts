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
} from "./harness";

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

describe("R4: an incremental pair table refreshed in place equals a full rebuild, and serves live's answers", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   // Each table's boundary, as the ledger records it.
   const covered: Record<string, WatermarkBound> = {};

   const runner = (sql: string) =>
      fixture.duckdb.runSQL(sql, { rowLimit: 100_000 }) as Promise<{
         rows: unknown[];
      }>;
   const rows = async (sql: string) => (await runner(sql)).rows as Row[];
   const table = (source: string) =>
      `${DESTINATION}.${fixture.physicalTables[source]}`;

   /** Plan one refresh of `source` at `now` from its recorded boundary. */
   async function plan(
      source: string,
      now: string,
   ): Promise<{
      step: IncrementalStep;
      lineage: IncrementalLineage;
      target: ReturnType<typeof warehouseDeltaTarget>;
   }> {
      fixture.setClock(now);
      const lineage = lineageOf(fixture, source);
      const target = warehouseDeltaTarget({
         dialect: "duckdb",
         runner,
         quotedTablePath: table(source),
         lineage,
         sourceSQL: fixture.builtSQL[source],
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
         `CREATE OR REPLACE TABLE ${rebuilt} AS ${fixture.builtSQL[source]}`,
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

   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
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
         "per flight, childless flights' live count of one included (malloydata/malloy#1762)",
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
      fixture.setClock(now);
      for (const [name, malloy] of served) {
         const run = await fixture.run({ name, malloy });
         expect({ name, via: run.servedVia }).toEqual({
            name,
            via: "served_copy",
         });
         expect(run.live.length).toBeGreaterThan(0);
         expect({ name, diff: rowDifference(run.live, run.served) }).toEqual({
            name,
            diff: undefined,
         });
      }
      for (const [name, malloy] of parentOnly) {
         fixture.setBindings(fixture.bindings);
         const run = await fixture.runAsIs(malloy);
         expect(
            (run.path?.considered ?? []).map((c) => [c.outcome, c.reason]),
         ).toEqual([["skipped", "COUNT_SEMANTICS"]]);
         const both = await fixture.run({ name, malloy, expectLive: true });
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
      const run = await fixture.run({
         name: "parent measures beside child ones",
         malloy:
            "run: flights -> { group_by: carrier; aggregate: flight_count, total_distance, seg_n }",
      });
      expect(rowDifference(run.live, run.served)).toBeDefined();
   });
});
