// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it, spyOn } from "bun:test";
import { logger } from "../logger";
import { DuckDBConnection } from "../storage/duckdb/DuckDBConnection";
import { initializeSchema } from "../storage/duckdb/schema";
import { emitAccessRecord, type AccessRecord } from "./access_record";
import {
   setWorkloadLogSink,
   storedRecordOf,
   WorkloadLogStore,
} from "./workload_log_store";

const record = (over: Partial<AccessRecord> = {}): AccessRecord => ({
   event: "access",
   environment: "env",
   package: "pkg",
   modelPath: "m.malloy",
   adHoc: true,
   source: "orders",
   outcome: "ok",
   status: 200,
   durationMs: 12,
   rowCount: 3,
   servedFrom: "storage",
   shape: {
      key: "0123456789abcdef",
      source: "orders",
      kind: "reduce",
      stages: 1,
      dimensions: ["region"],
      measures: ["total"],
      filteredDimensions: [],
      filters: [],
      complete: true,
   },
   ...over,
});

describe("WorkloadLogStore", () => {
   let db: DuckDBConnection;
   let ready: boolean;
   let clock: Date;
   const storage = {
      isInitialized: () => ready,
      getDuckDbConnection: () => db,
   };
   const store = (over: { retentionDays?: number; maxRows?: number } = {}) =>
      new WorkloadLogStore(storage, {
         retentionDays: over.retentionDays ?? 30,
         maxRows: over.maxRows ?? 1000,
         now: () => clock,
      });

   beforeEach(async () => {
      db = new DuckDBConnection(":memory:");
      await db.initialize();
      await initializeSchema(db, false);
      ready = true;
      clock = new Date("2026-11-11T12:00:00Z");
   });

   afterEach(async () => {
      setWorkloadLogSink(undefined);
      await db.close();
   });

   it("stores each record with the columns the log is read by", async () => {
      const s = store();
      s.add(record({ scannedBytes: 2048 }));
      await s.flush();
      const [row] = await s.rows();
      expect(row).toMatchObject({
         recordedAt: "2026-11-11T12:00:00.000Z",
         environment: "env",
         package: "pkg",
         modelPath: "m.malloy",
         source: "orders",
         shapeKey: "0123456789abcdef",
         servedFrom: "storage",
         outcome: "ok",
         durationMs: 12,
         rowCount: 3,
         scannedBytes: 2048,
      });
      expect(row.record.shape?.dimensions).toEqual(["region"]);
   });

   it("drops the values of #(secure) givens, and keeps their names", async () => {
      const s = store();
      s.add(
         record({
            givenNames: ["ROLES"],
            secureGivens: { ROLES: ["clinician"] },
         }),
      );
      await s.flush();
      const [row] = await s.rows();
      expect(row.record.secureGivens).toBeUndefined();
      expect(row.record.givenNames).toEqual(["ROLES"]);
      expect(
         storedRecordOf(record({ secureGivens: { A: "b" } })),
      ).not.toHaveProperty("secureGivens");
   });

   it("deletes what is older than the retention on each flush", async () => {
      const s = store({ retentionDays: 7 });
      s.add(record({ source: "old" }));
      await s.flush();
      clock = new Date("2026-11-20T12:00:00Z");
      s.add(record({ source: "new" }));
      await s.flush();
      expect((await s.rows()).map((r) => r.source)).toEqual(["new"]);
   });

   it("keeps at most maxRows, dropping the oldest", async () => {
      const s = store({ maxRows: 3 });
      for (let i = 0; i < 5; i++) {
         clock = new Date(Date.UTC(2026, 10, 11, 12, 0, i));
         s.add(record({ source: `s${i}` }));
      }
      await s.flush();
      expect((await s.rows()).map((r) => r.source)).toEqual(["s4", "s3", "s2"]);
   });

   it("holds records until storage opens, then writes them", async () => {
      ready = false;
      const s = store();
      s.add(record());
      await s.flush();
      ready = true;
      expect(await s.rows()).toHaveLength(0);
      await s.flush();
      expect(await s.rows()).toHaveLength(1);
   });

   it("never throws from add or flush, and logs a batch it could not write", async () => {
      const warn = spyOn(logger, "warn").mockImplementation(() => logger);
      try {
         const s = store();
         await db.run("DROP TABLE workload_log");
         s.add(record());
         await s.flush();
         expect(warn).toHaveBeenCalledWith(
            "Could not write the workload log batch; dropped it",
            expect.objectContaining({ records: 1 }),
         );
      } finally {
         warn.mockRestore();
      }
   });

   it("receives every emitted access record once installed as the sink", async () => {
      const info = spyOn(logger, "info").mockImplementation(() => logger);
      try {
         const s = store();
         setWorkloadLogSink(s);
         emitAccessRecord(record({ source: "a" }));
         emitAccessRecord(
            record({ source: "b", outcome: "denied_lock", status: 403 }),
         );
         setWorkloadLogSink(undefined);
         emitAccessRecord(record({ source: "not stored" }));
         await s.flush();
         expect((await s.rows()).map((r) => [r.source, r.outcome])).toEqual([
            ["b", "denied_lock"],
            ["a", "ok"],
         ]);
      } finally {
         info.mockRestore();
      }
   });
});
