// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Where a delta starts (F-23), `lookback=`, and the late-row check.
 *
 * A date or timestamp delta starts at the newest watermark the stored table
 * holds at or below the recorded boundary, less `lookback=`, never at the
 * recorded boundary itself (the previous run's clock). The pure arithmetic is
 * checked here directly; the probe and the check's SQL run on DuckDB, so the
 * text is proven to execute, not only to read right.
 */

import type { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { duckdbTestConnections } from "../../../test_helpers/incremental_test_harness";
import {
   boundMinusSeconds,
   canonicalBoundValue,
   compareBounds,
   countLateRows,
   deltaLowerBound,
   lateRowsSql,
   lateRowsWindowStart,
   planIncrementalStep,
   probeStoredWatermark,
   type DeltaTarget,
   type IncrementalLineage,
   type RecordedBoundary,
   type SqlRunner,
   type WatermarkBound,
   warehouseDeltaTarget,
} from "./incremental_apply";
import { durationSeconds } from "../domain/freshness";
import { watermarkTextSql } from "./warehouse_sql";

const ts = (value: string): WatermarkBound => ({
   malloyType: "timestamp",
   value,
});
const date = (value: string): WatermarkBound => ({ malloyType: "date", value });

describe("deltaLowerBound", () => {
   it("starts at the newest stored watermark, below the recorded boundary", () => {
      // The recorded boundary is the previous run's start; the table's newest
      // row is two hours older. A row stamped between them must be read.
      expect(
         deltaLowerBound({
            recorded: ts("2026-11-18T12:00:00"),
            stored: ts("2026-11-18 10:00:00"),
         }),
      ).toEqual(ts("2026-11-18 10:00:00"));
   });

   it("keeps the recorded boundary when nothing is stored at or below it", () => {
      // Every stored watermark NULL, or past the clock when the table was
      // seeded: the probe finds nothing, and today's boundary stands.
      expect(
         deltaLowerBound({
            recorded: ts("2026-11-18T12:00:00"),
            stored: undefined,
         }),
      ).toEqual(ts("2026-11-18T12:00:00"));
   });

   it("never leads the recorded boundary", () => {
      expect(
         deltaLowerBound({
            recorded: ts("2026-11-18T12:00:00"),
            stored: ts("2026-11-18 13:00:00"),
         }),
      ).toEqual(ts("2026-11-18T12:00:00"));
   });

   it("moves back by lookback=, from the stored watermark", () => {
      expect(
         deltaLowerBound({
            recorded: ts("2026-11-18T12:00:00"),
            stored: ts("2026-11-18 10:00:00"),
            lookbackSeconds: 2 * 86_400,
         }),
      ).toEqual(ts("2026-11-16T10:00:00"));
   });

   it("moves a date back by whole days, rounding a part day up", () => {
      expect(
         deltaLowerBound({
            recorded: date("2026-11-18"),
            stored: date("2026-11-17"),
            lookbackSeconds: 12 * 3600,
         }),
      ).toEqual(date("2026-11-16"));
      expect(
         deltaLowerBound({
            recorded: date("2026-03-01"),
            stored: date("2026-03-01"),
            lookbackSeconds: 86_400,
         }),
      ).toEqual(date("2026-02-28"));
   });
});

describe("boundMinusSeconds", () => {
   it("crosses a month, a year and a leap day in UTC", () => {
      expect(boundMinusSeconds(ts("2026-01-01T00:30:00"), 3600)).toEqual(
         ts("2025-12-31T23:30:00"),
      );
      expect(boundMinusSeconds(date("2028-03-01"), 86_400)).toEqual(
         date("2028-02-29"),
      );
   });
});

describe("lateRowsWindowStart", () => {
   it("mirrors the delta's own width below its start", () => {
      expect(
         lateRowsWindowStart(
            ts("2026-11-18 10:00:00"),
            ts("2026-11-18T12:00:00"),
         ),
      ).toEqual(ts("2026-11-18T08:00:00"));
      expect(
         lateRowsWindowStart(date("2026-11-16"), date("2026-11-18")),
      ).toEqual(date("2026-11-14"));
      expect(
         lateRowsWindowStart(
            { malloyType: "number", value: "10" },
            { malloyType: "number", value: "13" },
         ),
      ).toEqual({ malloyType: "number", value: "7" });
   });

   it("has no window for a string watermark or an empty range", () => {
      expect(
         lateRowsWindowStart(
            { malloyType: "string", value: "a" },
            { malloyType: "string", value: "b" },
         ),
      ).toBeUndefined();
      expect(
         lateRowsWindowStart(
            ts("2026-11-18T12:00:00"),
            ts("2026-11-18T12:00:00"),
         ),
      ).toBeUndefined();
   });
});

describe("a stored timestamp read back as text", () => {
   it("reads a zero offset as UTC and refuses any other", () => {
      for (const text of [
         "2026-11-18 10:00:00+00",
         "2026-11-18 10:00:00.537+00:00",
         "2026-11-18T10:00:00+0000",
         "2026-11-18 10:00:00",
      ]) {
         expect(canonicalBoundValue("timestamp", text)).toEqual(
            ts("2026-11-18 10:00:00"),
         );
      }
      expect(() =>
         canonicalBoundValue("timestamp", "2026-11-18 10:00:00+01:00"),
      ).toThrow();
   });

   it("compares with the run clock's spelling", () => {
      // `T` sorts after a space: compared as raw text, 23:00 on the same day
      // would read as EARLIER than 01:00.
      expect(
         compareBounds(ts("2026-11-18 23:00:00"), ts("2026-11-18T01:00:00")),
      ).toBeGreaterThan(0);
   });
});

describe("durationSeconds", () => {
   it("reads a window's letters and the units' words", () => {
      expect(durationSeconds("2 days")).toBe(172_800);
      expect(durationSeconds("1 day")).toBe(86_400);
      expect(durationSeconds("36h")).toBe(129_600);
      expect(durationSeconds("90 minutes")).toBe(5400);
      expect(durationSeconds("1 week")).toBe(604_800);
      expect(durationSeconds("45 seconds")).toBe(45);
   });

   it("refuses anything else", () => {
      for (const text of [
         "",
         "2",
         "0 days",
         "-1 day",
         "1.5 days",
         "2 months",
         "1 year",
         "2 Days",
         "2 days ago",
      ]) {
         expect(durationSeconds(text)).toBeUndefined();
      }
   });
});

// ─── On DuckDB: the probe and the check run as written ─────────────────────

let duckdb: DuckDBConnection;
// Names of this file's own: in-process `:memory:` DuckDB connections can share
// one database with other specs.
const ID = Math.random().toString(36).slice(2, 8);
const STORED = `f23_stored_${ID}`;
const SRC = `f23_src_${ID}`;
const TBL = `f23_tbl_${ID}`;
const run: SqlRunner = async (sql) => {
   const result = await duckdb.runSQL(sql);
   return { rows: result.rows as unknown[] };
};

beforeAll(async () => {
   ({ duckdb } = duckdbTestConnections());
   await duckdb.runSQL("SET TimeZone='UTC'");
   await duckdb.runSQL(`CREATE TABLE ${STORED} AS SELECT * FROM (VALUES
      (1, TIMESTAMP '2026-11-18 08:00:00', TIMESTAMPTZ '2026-11-18 08:00:00+00', DATE '2026-11-16'),
      (2, TIMESTAMP '2026-11-18 10:00:00.750', TIMESTAMPTZ '2026-11-18 10:00:00+00', DATE '2026-11-17'),
      (3, TIMESTAMP '2026-11-18 13:00:00', TIMESTAMPTZ '2026-11-18 13:00:00+00', DATE '2026-11-19'),
      (4, NULL, NULL, NULL)
   ) AS t(id, ts, tstz, d)`);
});

afterAll(async () => {
   await duckdb.close();
});

describe("probeStoredWatermark, on DuckDB", () => {
   const probe = (column: string, atOrBelow: WatermarkBound) =>
      probeStoredWatermark({
         runner: run,
         dialect: "duckdb",
         quotedTablePath: STORED,
         column,
         atOrBelow,
      });

   it("reads the newest watermark at or below the boundary, in whole seconds", async () => {
      // 13:00 is past the boundary (a seeded row stamped ahead of the clock),
      // so 10:00:00.750 is the newest the boundary covers, read down to 10:00:00.
      expect(await probe("ts", ts("2026-11-18T12:00:00"))).toEqual({
         bound: ts("2026-11-18 10:00:00"),
      });
      expect(await probe("tstz", ts("2026-11-18T12:00:00"))).toEqual({
         bound: ts("2026-11-18 10:00:00"),
      });
      expect(await probe("d", date("2026-11-18"))).toEqual({
         bound: date("2026-11-17"),
      });
   });

   it("finds nothing when every watermark is above the boundary or NULL", async () => {
      expect(await probe("ts", ts("2026-11-01T00:00:00"))).toEqual({});
   });

   it("spells the text read for each incremental dialect", () => {
      expect(watermarkTextSql("postgres", "timestamp", "MAX(x)")).toBe(
         "CAST(MAX(x) AS VARCHAR)",
      );
      expect(watermarkTextSql("standardsql", "date", "MAX(x)")).toBe(
         "CAST(MAX(x) AS STRING)",
      );
      expect(watermarkTextSql("snowflake", "timestamp", "MAX(x)")).toBe(
         "TO_CHAR(CONVERT_TIMEZONE('UTC', MAX(x))::TIMESTAMP_NTZ, 'YYYY-MM-DD HH24:MI:SS')",
      );
   });
});

describe("the late-row check, on DuckDB", () => {
   beforeAll(async () => {
      // The source, and the table an earlier refresh stored from it: id 6
      // (stamped 09:00) and the restated id 2 landed after it was written,
      // both below the next delta's start (10:00).
      await duckdb.runSQL(`CREATE TABLE ${SRC} AS SELECT * FROM (VALUES
         (1, 'a', TIMESTAMP '2026-11-18 08:00:00'),
         (2, 'b', TIMESTAMP '2026-11-18 09:30:00'),
         (5, NULL, TIMESTAMP '2026-11-18 09:45:00'),
         (6, 'c', TIMESTAMP '2026-11-18 09:00:00'),
         (7, 'd', TIMESTAMP '2026-11-18 10:30:00')
      ) AS t(id, region, loaded_at)`);
      await duckdb.runSQL(`CREATE TABLE ${TBL} AS SELECT * FROM (VALUES
         (1, 'a', TIMESTAMP '2026-11-18 08:00:00'),
         (2, 'b', TIMESTAMP '2026-11-18 08:30:00'),
         (5, NULL, TIMESTAMP '2026-11-18 09:45:00')
      ) AS t(id, region, loaded_at)`);
   });

   const lineage = (mergeKeys: string[]): IncrementalLineage => ({
      physicalTableName: TBL,
      connectionName: "duckdb",
      sourceEntityId: "sid",
      watermarkName: "loaded_at",
      watermarkType: "timestamp",
      mergeKeys,
      scopeColumns: [],
      strategy: mergeKeys.length > 0 ? "merge" : "range_replace",
   });
   const target = (l: IncrementalLineage): DeltaTarget =>
      warehouseDeltaTarget({
         dialect: "duckdb",
         runner: run,
         quotedTablePath: TBL,
         lineage: l,
         sourceSQL: `SELECT * FROM ${SRC}`,
      });

   it("counts source rows whose keys and watermark the table lacks", async () => {
      // Window [08:00, 10:00): id 6 is missing, id 2's restatement too, and
      // id 5's NULL-keyed region still matches its stored copy.
      const found = await countLateRows({
         target: target(lineage(["id", "region"])),
         lineage: lineage(["id", "region"]),
         start: ts("2026-11-18 10:00:00"),
         end: ts("2026-11-18T12:00:00"),
      });
      expect(found).toEqual({
         lateRows: {
            count: 2,
            from: "2026-11-18T08:00:00",
            to: "2026-11-18T10:00:00",
         },
      });
   });

   it("counts the source's rows less the table's without a merge key", async () => {
      const found = await countLateRows({
         target: target(lineage([])),
         lineage: lineage([]),
         start: ts("2026-11-18 10:00:00"),
         end: ts("2026-11-18T12:00:00"),
      });
      // Four source rows in the window, three stored.
      expect(found.lateRows?.count).toBe(1);
   });

   it("reports nothing when the window holds no missing row", async () => {
      const found = await countLateRows({
         target: target(lineage(["id"])),
         lineage: lineage(["id"]),
         start: ts("2026-11-18 08:00:00"),
         end: ts("2026-11-18T08:30:00"),
      });
      expect(found).toEqual({});
   });

   it("is skipped for a pair table, and on a target that cannot read the window", async () => {
      const pair = {
         ...lineage(["id"]),
         pair: { parentKey: "id", parentRow: "__parent_row" },
      };
      expect(
         await countLateRows({
            target: target(pair),
            lineage: pair,
            start: ts("2026-11-18 10:00:00"),
            end: ts("2026-11-18T12:00:00"),
         }),
      ).toEqual({});
      const { windowRows: _none, ...blind } = target(lineage(["id"]));
      expect(
         await countLateRows({
            target: blind,
            lineage: lineage(["id"]),
            start: ts("2026-11-18 10:00:00"),
            end: ts("2026-11-18T12:00:00"),
         }),
      ).toEqual({});
   });

   it("returns a failure rather than throwing", async () => {
      const failing: DeltaTarget = {
         ...target(lineage(["id"])),
         runner: async () => {
            throw new Error("permission denied");
         },
      };
      const found = await countLateRows({
         target: failing,
         lineage: lineage(["id"]),
         start: ts("2026-11-18 10:00:00"),
         end: ts("2026-11-18T12:00:00"),
      });
      expect(found.error).toContain("permission denied");
   });

   it("joins on the watermark's equality, which BigQuery requires of an outer join", () => {
      const sql = lateRowsSql({
         dialect: "standardsql",
         quotedTablePath: "`ds.tbl`",
         windowSQL: "SELECT id, loaded_at FROM src",
         watermark: "loaded_at",
         matchColumns: ["id"],
         from: ts("2026-11-18T08:00:00"),
         to: ts("2026-11-18 10:00:00"),
      });
      expect(sql).toContain("ON __t.`loaded_at` = __s.`loaded_at` AND");
      expect(sql).not.toContain("NOT EXISTS");
   });
});

describe("planIncrementalStep starts a time-watermark delta at the stored boundary", () => {
   const LINEAGE: IncrementalLineage = {
      physicalTableName: "events",
      connectionName: "wh",
      sourceEntityId: "sid",
      watermarkName: "loaded_at",
      watermarkType: "timestamp",
      mergeKeys: ["id"],
      scopeColumns: [],
      strategy: "merge",
   };
   const LEDGER: RecordedBoundary = {
      sourceEntityId: "sid",
      coveredThroughValue: "2026-11-18T12:00:00",
      coveredThroughType: "timestamp",
      watermarkDimension: "loaded_at",
      mergeKeyDimensions: ["id"],
      derivedStrategy: "merge",
      physicalTableName: "events",
      connectionName: "wh",
   };

   function fake(stored: unknown): { runner: SqlRunner; seen: string[] } {
      const seen: string[] = [];
      return {
         seen,
         runner: async (sql) => {
            seen.push(sql);
            if (/information_schema/i.test(sql)) {
               return {
                  rows: ["id", "loaded_at", "amt"].map((c) => ({
                     column_name: c,
                  })),
               };
            }
            if (sql.includes("LIMIT 1")) return { rows: [{ present: 1 }] };
            if (sql.includes("CAST(MAX(")) {
               if (stored instanceof Error) throw stored;
               return { rows: [{ watermark_max: stored }] };
            }
            throw new Error(`unexpected probe: ${sql}`);
         },
      };
   }

   async function plan(stored: unknown, lineage = LINEAGE) {
      const { runner, seen } = fake(stored);
      const step = await planIncrementalStep({
         target: warehouseDeltaTarget({
            dialect: "postgres",
            runner,
            quotedTablePath: `"events"`,
            lineage,
            sourceSQL: "SELECT * FROM base",
         }),
         lineage,
         ledgerEntry: LEDGER,
         forceRefresh: false,
         now: new Date("2026-11-18T13:00:00Z"),
         columns: ["id", "loaded_at", "amt"],
         postgresVersionNum: 160000,
      });
      return { step, seen };
   }

   it("reads from the newest stored row, and records the run's clock", async () => {
      const { step, seen } = await plan("2026-11-18 10:00:00+00");
      expect(step).toMatchObject({
         mode: "delta",
         start: ts("2026-11-18 10:00:00"),
         coveredThrough: ts("2026-11-18T13:00:00"),
      });
      if (step.mode !== "delta") return;
      expect(step.statements[0]).toContain(
         `"loaded_at" >= TIMESTAMP '2026-11-18 10:00:00'`,
      );
      // One aggregate over the stored column, capped at the boundary.
      expect(seen.find((s) => s.includes("CAST(MAX("))).toContain(
         `WHERE "loaded_at" <= TIMESTAMP '2026-11-18 12:00:00'`,
      );
   });

   it("reads lookback= further back", async () => {
      const { step } = await plan("2026-11-18 10:00:00", {
         ...LINEAGE,
         lookbackSeconds: 3600,
      });
      expect(step).toMatchObject({
         mode: "delta",
         start: ts("2026-11-18T09:00:00"),
      });
   });

   it("keeps the recorded boundary when nothing stored is at or below it", async () => {
      const { step } = await plan(null);
      expect(step).toMatchObject({
         mode: "delta",
         start: ts("2026-11-18T12:00:00"),
      });
   });

   it("rebuilds when the stored boundary cannot be read", async () => {
      const { step } = await plan(new Error("relation does not exist"));
      expect(step).toMatchObject({
         mode: "seed",
         reasonCode: "table_unreadable",
      });
   });

   it("leaves a number watermark's boundary where the source's frontier put it", async () => {
      const numbered: IncrementalLineage = {
         ...LINEAGE,
         watermarkType: "number",
      };
      const seen: string[] = [];
      const step = await planIncrementalStep({
         target: {
            ...warehouseDeltaTarget({
               dialect: "postgres",
               runner: async (sql) => {
                  seen.push(sql);
                  if (/information_schema/i.test(sql)) {
                     return {
                        rows: ["id", "loaded_at", "amt"].map((c) => ({
                           column_name: c,
                        })),
                     };
                  }
                  if (sql.includes("LIMIT 1"))
                     return { rows: [{ present: 1 }] };
                  return { rows: [{ watermark_max: 9 }] };
               },
               quotedTablePath: `"events"`,
               lineage: numbered,
               sourceSQL: "SELECT * FROM base",
            }),
         },
         lineage: numbered,
         ledgerEntry: {
            ...LEDGER,
            coveredThroughType: "number",
            coveredThroughValue: "5",
         },
         forceRefresh: false,
         now: new Date("2026-11-18T13:00:00Z"),
         columns: ["id", "loaded_at", "amt"],
         postgresVersionNum: 160000,
      });
      expect(step).toMatchObject({
         mode: "delta",
         start: { malloyType: "number", value: "5" },
      });
      expect(seen.some((s) => s.includes("CAST(MAX("))).toBe(false);
   });
});
