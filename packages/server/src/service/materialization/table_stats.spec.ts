// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { computeTableStats, type RecordedRun } from "./table_stats";
import type { BuildPlan, ManifestEntry } from "./types";

const plan = {
   graphs: [],
   sources: {
      "daily@m": {
         name: "daily",
         sourceID: "daily@m",
         connectionName: "duckdb",
         sourceEntityId: "t_daily",
         sql: "SELECT 1",
         columns: [
            { name: "day", type: "date" },
            { name: "total", type: "number" },
         ],
      },
      "agg@m": {
         name: "agg",
         sourceID: "agg@m",
         connectionName: "duckdb",
         origin: "preaggregate",
         sourceEntityId: "t_agg",
         sql: "SELECT 1",
         columns: [{ name: "n", type: "number" }],
      },
   },
} as unknown as BuildPlan;

const entry = (over: Partial<ManifestEntry>): ManifestEntry =>
   ({
      sourceEntityId: "t_daily",
      physicalTableName: "daily_x",
      ...over,
   }) as ManifestEntry;

const SINCE = new Date("2026-11-04T00:00:00Z");
const UNTIL = new Date("2026-11-11T00:00:00Z");

describe("computeTableStats", () => {
   it("takes the newest build of each table, and counts every build and failure", () => {
      const runs: RecordedRun[] = [
         {
            id: "r3",
            status: "MANIFEST_FILE_READY",
            completedAt: new Date("2026-11-10T00:00:00Z"),
            manifest: {
               entries: {
                  t_daily: entry({
                     rowCount: 40,
                     buildDurationMs: 300,
                     queryCostBytes: 1024,
                     storageDestinationName: "lake",
                     schema: [
                        { name: "day", type: "DATE" },
                        { name: "total", type: "DOUBLE" },
                     ],
                  }),
               },
               failures: { t_agg: { sourceEntityId: "t_agg", reason: "x" } },
            },
         },
         {
            id: "r2",
            status: "MANIFEST_FILE_READY",
            completedAt: new Date("2026-11-09T00:00:00Z"),
            manifest: {
               entries: {
                  t_daily: entry({ rowCount: 30, buildDurationMs: 100 }),
                  // A failure mirrored into `entries` is still a failure.
                  t_agg: entry({ sourceEntityId: "t_agg", error: "boom" }),
               },
            },
         },
         { id: "r1", status: "FAILED", completedAt: null, manifest: null },
      ];
      const stats = computeTableStats({
         plan,
         runs,
         since: SINCE,
         until: UNTIL,
         use: { unavailable: "store off" },
      });
      expect(stats.runsRead).toBe(3);
      expect(stats.workload).toEqual({
         available: false,
         unavailableReason: "store off",
      });
      const [agg, daily] = stats.tables;
      expect(daily).toEqual({
         tableId: "t_daily",
         sourceName: "daily",
         sourceID: "daily@m",
         origin: "persist",
         builds: { succeeded: 2, failed: 0, meanDurationMs: 200 },
         lastBuild: {
            materializationId: "r3",
            builtAt: "2026-11-10T00:00:00.000Z",
            rowCount: 40,
            buildDurationMs: 300,
            scannedBytes: 1024,
            storageDestinationName: "lake",
         },
         columns: [
            { name: "day", type: "DATE" },
            { name: "total", type: "DOUBLE" },
         ],
         columnsFrom: "stored",
      });
      // Never built: no lastBuild, the declared columns, and both failures.
      expect(agg).toEqual({
         tableId: "t_agg",
         sourceName: "agg",
         sourceID: "agg@m",
         origin: "preaggregate",
         builds: { succeeded: 0, failed: 2 },
         columns: [{ name: "n", type: "number" }],
         columnsFrom: "declared",
      });
   });

   it("reports use per table when the workload log is on, zero for a table no query considered", () => {
      const stats = computeTableStats({
         plan,
         runs: [],
         since: SINCE,
         until: UNTIL,
         use: {
            answered: 9,
            reported: 7,
            tables: {
               t_daily: { considered: 5, hits: 3, skips: { STALE: 2 } },
               t_gone: { considered: 1, hits: 1, skips: {} },
            },
         },
      });
      expect(stats.window).toEqual({
         since: "2026-11-04T00:00:00.000Z",
         until: "2026-11-11T00:00:00.000Z",
      });
      expect(stats.workload).toEqual({
         available: true,
         answeredQueries: 9,
         reportedQueries: 7,
      });
      expect(stats.tables.map((t) => [t.tableId, t.use])).toEqual([
         ["t_agg", { considered: 0, hits: 0, skips: {} }],
         ["t_daily", { considered: 5, hits: 3, skips: { STALE: 2 } }],
      ]);
   });

   it("answers with no tables for a package with no plan", () => {
      const stats = computeTableStats({
         plan: null,
         runs: [],
         since: SINCE,
         until: UNTIL,
         use: { unavailable: "off" },
      });
      expect(stats.tables).toEqual([]);
   });
});
