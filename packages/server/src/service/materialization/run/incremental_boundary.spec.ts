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
import { afterAll, beforeAll, describe, expect, it, spyOn } from "bun:test";
import { logger } from "../../../logger";
import { duckdbTestConnections } from "../../../test_helpers/incremental_test_harness";
import {
   boundMinusSeconds,
   canonicalBoundValue,
   compareBounds,
   countLateRows,
   deltaLowerBound,
   lateRowsSql,
   LATE_ROWS_WINDOW_CAP_SECONDS,
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
import type { BuildPlan } from "../domain/types";
import { lateRowWarnings } from "../package_warnings";
import { sessionRunner } from "./incremental_storage";
import { watermarkTextSql, withStatementTimeout } from "./warehouse_sql";

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

   it("caps a time window at seven days, or the lookback when longer", () => {
      // A source quiet for a month: its newest stored row is 30 days old.
      expect(
         lateRowsWindowStart(
            ts("2026-10-19 12:00:00"),
            ts("2026-11-18T12:00:00"),
         ),
      ).toEqual(ts("2026-10-12T12:00:00"));
      expect(
         lateRowsWindowStart(
            ts("2026-10-19 12:00:00"),
            ts("2026-11-18T12:00:00"),
            10 * 86_400,
         ),
      ).toEqual(ts("2026-10-09T12:00:00"));
      expect(LATE_ROWS_WINDOW_CAP_SECONDS).toBe(7 * 86_400);
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
      // An explicit format: a cast would follow the session's DateStyle.
      expect(watermarkTextSql("postgres", "timestamp", "MAX(x)")).toBe(
         "to_char(MAX(x), 'YYYY-MM-DD HH24:MI:SSOF')",
      );
      expect(withStatementTimeout("postgres", "SELECT 1", 60_000)).toBe(
         "SET LOCAL statement_timeout = 60000; SELECT 1",
      );
      expect(withStatementTimeout("standardsql", "SELECT 1", 60_000)).toBe(
         "SELECT 1",
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

   it("carries what an earlier delta found, and counts only above it", async () => {
      // Delta 1 found id 6 and id 2's restatement in [08:00, 10:00); delta 3,
      // with the start unmoved, re-checks nothing below 10:00 and still
      // reports both, so the warning stays while they are missing.
      const carried = {
         count: 2,
         from: "2026-11-18T08:00:00",
         to: "2026-11-18T10:00:00",
      };
      expect(
         await countLateRows({
            target: target(lineage(["id", "region"])),
            lineage: lineage(["id", "region"]),
            start: ts("2026-11-18 10:00:00"),
            end: ts("2026-11-18T14:00:00"),
            carried,
         }),
      ).toEqual({ lateRows: carried });
      // A later delta that starts higher checks [10:00, 11:00) only: id 7 at
      // 10:30 is missing too, added to the two carried.
      expect(
         await countLateRows({
            target: target(lineage(["id", "region"])),
            lineage: lineage(["id", "region"]),
            start: ts("2026-11-18 11:00:00"),
            end: ts("2026-11-18T14:00:00"),
            carried,
         }),
      ).toEqual({
         lateRows: {
            count: 3,
            from: "2026-11-18T08:00:00",
            to: "2026-11-18T11:00:00",
         },
      });
   });

   it("gives up after its timeout, keeping what was carried", async () => {
      const carried = {
         count: 1,
         from: "2026-11-18T08:00:00",
         to: "2026-11-18T09:00:00",
      };
      const slow: DeltaTarget = {
         ...target(lineage(["id"])),
         // Never answers until it is cancelled, as a slow read would.
         runner: (_sql, options) =>
            new Promise((_resolve, reject) =>
               options?.abortSignal?.addEventListener("abort", () =>
                  reject(new Error("cancelled")),
               ),
            ),
      };
      const found = await countLateRows({
         target: slow,
         lineage: lineage(["id"]),
         start: ts("2026-11-18 10:00:00"),
         end: ts("2026-11-18T12:00:00"),
         carried,
         timeoutMs: 20,
      });
      expect(found.lateRows).toEqual(carried);
      expect(found.error).toContain("longer than 20 ms");
   });

   it("frees a storage= session when it times out: the statement is interrupted", async () => {
      // A DuckDB session behind the storage runner, reading a window slow
      // enough to outlast the timeout many times over.
      const slow: DeltaTarget = {
         ...warehouseDeltaTarget({
            dialect: "duckdb",
            runner: sessionRunner(duckdb),
            quotedTablePath: TBL,
            lineage: lineage(["id"]),
            sourceSQL:
               `SELECT s.id, s.region, s.loaded_at FROM ${SRC} AS s, ` +
               `range(400000000) AS r(n) WHERE r.n % 7 = 0`,
         }),
      };
      const started = Date.now();
      const found = await countLateRows({
         target: slow,
         lineage: lineage(["id"]),
         start: ts("2026-11-18 10:00:00"),
         end: ts("2026-11-18T12:00:00"),
         timeoutMs: 200,
      });
      expect(found.error).toContain("longer than 200 ms");
      expect(Date.now() - started).toBeLessThan(5_000);
      // The session is free at once for the build's next statement.
      const next = Date.now();
      await duckdb.runSQL("SELECT 1 AS one");
      expect(Date.now() - next).toBeLessThan(1_000);
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
            if (/ <= (DATE|TIMESTAMP) '/.test(sql)) {
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
      expect(seen.find((s) => / <= (DATE|TIMESTAMP) '/.test(s))).toContain(
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

   it("rebuilds rather than start an hour off when the session is not UTC", async () => {
      // A Postgres session in America/New_York prints the fall-back hour's
      // two instants with their offsets; any offset but zero is refused.
      const { step } = await plan("2026-11-01 01:30:00-04");
      expect(step).toMatchObject({
         mode: "seed",
         reasonCode: "table_unreadable",
      });
   });

   it("says why when the stored watermark does not read as UTC ISO-8601", async () => {
      // A setupSQL that moves the session's zone, or an infinite value.
      const warn = spyOn(logger, "warn");
      try {
         const { step } = await plan("infinity");
         expect(step).toMatchObject({
            mode: "seed",
            reasonCode: "table_unreadable",
         });
         expect(
            warn.mock.calls.some((c) =>
               String(c[0]).includes("not a UTC ISO-8601"),
            ),
         ).toBe(true);
      } finally {
         warn.mockRestore();
      }
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
      expect(seen.some((s) => / <= (DATE|TIMESTAMP) '/.test(s))).toBe(false);
   });
});

describe("the late-row warning", () => {
   const plan = (origin: string, annotationFields: Record<string, string>) =>
      ({
         graphs: [],
         sources: {
            s: {
               name: "events",
               sourceID: "events@m.malloy",
               sourceEntityId: "sid",
               modelPath: "m.malloy",
               origin,
               annotationFields,
            },
         },
      }) as unknown as BuildPlan;
   const late = new Map([
      [
         "sid",
         { count: 2, from: "2026-11-18T08:00:00", to: "2026-11-18T10:00:00" },
      ],
   ]);

   it("asks for a merge key with the lookback when the source has none", () => {
      const [w] = lateRowWarnings(
         plan("persist", { refresh: "incremental" }),
         late,
      );
      expect(w.code).toBe("persist-incremental-late-rows");
      expect(w.message).toContain("2 rows");
      expect(w.message).toContain("declare a merge_key=");
      expect(w.message).toContain("#@ persist");
   });

   it("names #(materialize), and the lookback to widen when there is one", () => {
      const [w] = lateRowWarnings(
         plan("materialize", { merge_key: "id", lookback: "1 hour" }),
         late,
      );
      expect(w.message).toContain('widen its lookback= (now "1 hour")');
      expect(w.message).toContain("#(materialize)");
   });
});
