// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it, spyOn } from "bun:test";
import { logger } from "../logger";
import { DuckDBConnection } from "../storage/duckdb/DuckDBConnection";
import { initializeSchema } from "../storage/duckdb/schema";
import { emitAccessRecord, type AccessRecord } from "./access_record";
import { settings } from "../settings";
import {
   redactErrorLiterals,
   setWorkloadLogSink,
   startWorkloadLogFromSettings,
   stopWorkloadLogSink,
   storedRecordOf,
   workloadLogSink,
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
   const store = (
      over: {
         retentionDays?: number;
         maxRows?: number;
         maxBuffered?: number;
         maxHeld?: number;
         pruneIntervalMs?: number;
      } = {},
   ) =>
      new WorkloadLogStore(storage, {
         retentionDays: over.retentionDays ?? 30,
         maxRows: over.maxRows ?? 1000,
         maxBuffered: over.maxBuffered,
         maxHeld: over.maxHeld,
         pruneIntervalMs: over.pruneIntervalMs,
         now: () => clock,
      });

   /** Count the statements a transaction runs, by their first word. */
   function countTransactionStatements(): {
      inserts: () => number;
      restore: () => void;
   } {
      let inserts = 0;
      const original = db.transaction.bind(db);
      const spy = spyOn(db, "transaction").mockImplementation(((
         fn: (tx: {
            run: (q: string, p?: unknown[]) => Promise<void>;
         }) => Promise<unknown>,
      ) =>
         original((tx) =>
            fn({
               ...tx,
               run: (q: string, p?: unknown[]) => {
                  if (q.trimStart().startsWith("INSERT")) inserts++;
                  return tx.run(q, p);
               },
            } as typeof tx),
         )) as typeof db.transaction);
      return { inserts: () => inserts, restore: () => spy.mockRestore() };
   }

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

   it("writes a flush's batch in multi-row INSERTs, not one statement per record", async () => {
      const counter = countTransactionStatements();
      try {
         const s = store({ maxBuffered: 10_000 });
         for (let i = 0; i < 600; i++) s.add(record({ source: `s${i}` }));
         await s.flush();
         expect(await s.rows(1000)).toHaveLength(600);
         // 250 rows per statement.
         expect(counter.inserts()).toBe(3);
      } finally {
         counter.restore();
      }
   });

   it("prunes at most once per prune interval, not on every flush", async () => {
      const all = spyOn(db, "all");
      try {
         const s = store({ pruneIntervalMs: 60_000 });
         const counts = () =>
            all.mock.calls.filter(([q]) => String(q).includes("count(*)"))
               .length;
         s.add(record());
         await s.flush();
         expect(counts()).toBe(1);
         clock = new Date(clock.getTime() + 2_000);
         s.add(record());
         await s.flush();
         expect(counts()).toBe(1);
         clock = new Date(clock.getTime() + 60_000);
         s.add(record());
         await s.flush();
         expect(counts()).toBe(2);
      } finally {
         all.mockRestore();
      }
   });

   it("on stop, writes records that arrived while a write was in flight", async () => {
      const s = store();
      s.add(record({ source: "first" }));
      const inFlight = s.flush();
      s.add(record({ source: "during" }));
      await s.stop();
      await inFlight;
      expect((await s.rows()).map((r) => r.source).sort()).toEqual([
         "during",
         "first",
      ]);
   });

   it("a flush called during a write also writes what was buffered when it was called", async () => {
      const s = store();
      s.add(record({ source: "first" }));
      const inFlight = s.flush();
      s.add(record({ source: "during" }));
      await s.flush();
      await inFlight;
      expect(await s.rows()).toHaveLength(2);
   });

   it("holds at most maxHeld records under load, dropping the oldest", async () => {
      ready = false;
      const s = store({ maxBuffered: 5, maxHeld: 5 });
      for (let i = 0; i < 8; i++) s.add(record({ source: `s${i}` }));
      ready = true;
      await s.stop();
      expect((await s.rows()).map((r) => r.source).sort()).toEqual([
         "s3",
         "s4",
         "s5",
         "s6",
         "s7",
      ]);
   });

   it("redacts the literals an error quotes back, and keeps its words", async () => {
      const s = store();
      s.add(
         record({
            outcome: "error",
            status: 500,
            error: `Conversion Error: invalid input "acme" for tenant = 'acme' LINE 1: WHERE x > 42`,
         }),
      );
      await s.flush();
      const [row] = await s.rows();
      expect(row.record.error).toBe(
         "Conversion Error: invalid input ? for tenant = ? LINE ?: WHERE x > ?",
      );
      expect(redactErrorLiterals("name = 'o''brien'")).toBe("name = ?");
   });

   it("counts each table's hits and skips by reason, from the path reports in the window", async () => {
      const s = store();
      const served = (tableId: string) => ({
         servedBy: { kind: "materialized", tableId },
         considered: [
            { tableId, kind: "materialized", outcome: "used" },
            {
               tableId: "t_far",
               kind: "materialized",
               outcome: "skipped",
               reason: "MISSING_FIELD",
            },
         ],
      });
      s.add(record({ path: served("t_daily") }));
      s.add(record({ path: served("t_daily") }));
      s.add(
         record({
            path: {
               servedBy: { kind: "live" },
               considered: [
                  {
                     tableId: "t_daily",
                     kind: "materialized",
                     outcome: "skipped",
                     reason: "STALE",
                  },
               ],
            },
         }),
      );
      // Answered but from before the path report: counted only in `answered`.
      s.add(record());
      // Refused, another package, and before the window: not counted.
      s.add(record({ outcome: "denied_lock", path: served("t_daily") }));
      s.add(record({ package: "other", path: served("t_daily") }));
      await s.flush();
      clock = new Date("2026-11-12T12:00:00Z");
      s.add(record({ path: served("t_new") }));
      await s.flush();

      const use = await s.tableUse({
         environment: "env",
         package: "pkg",
         since: new Date("2026-11-11T00:00:00Z"),
      });
      expect(use.answered).toBe(5);
      expect(use.reported).toBe(4);
      expect(use.tables).toEqual({
         t_daily: { considered: 3, hits: 2, skips: { STALE: 1 } },
         t_far: { considered: 3, hits: 0, skips: { MISSING_FIELD: 3 } },
         t_new: { considered: 1, hits: 1, skips: {} },
      });

      const windowed = await s.tableUse({
         environment: "env",
         package: "pkg",
         since: new Date("2026-11-12T00:00:00Z"),
      });
      expect(windowed.answered).toBe(1);
      expect(Object.keys(windowed.tables).sort()).toEqual(["t_far", "t_new"]);
   });

   it("stopWorkloadLogSink writes what is buffered and removes the sink", async () => {
      const s = store();
      setWorkloadLogSink(s);
      s.add(record());
      await stopWorkloadLogSink();
      expect(workloadLogSink()).toBeUndefined();
      expect(await s.rows()).toHaveLength(1);
   });

   it("a rebuild with the store off removes the sink an earlier build installed", async () => {
      const original = settings.get("workloadLog.store");
      const get = spyOn(settings, "get");
      try {
         get.mockImplementation(((key: string) =>
            key === "workloadLog.store"
               ? "on"
               : key === "workloadLog.retentionDays"
                 ? 30
                 : key === "workloadLog.maxRows"
                   ? 1000
                   : original) as typeof settings.get);
         const info = spyOn(logger, "info").mockImplementation(() => logger);
         const first = startWorkloadLogFromSettings(storage);
         info.mockRestore();
         expect(workloadLogSink()).toBe(first);
         get.mockImplementation(((key: string) =>
            key === "workloadLog.store"
               ? "off"
               : original) as typeof settings.get);
         expect(startWorkloadLogFromSettings(storage)).toBeUndefined();
         expect(workloadLogSink()).toBeUndefined();
      } finally {
         get.mockRestore();
      }
   });
});
