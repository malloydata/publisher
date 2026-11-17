// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * P5-10 (F-22): cancelling a run stops the statement it is reading with.
 *
 * - A `storage=` build's DuckDB session is interrupted when the run's signal
 *   aborts (`interruptOnAbort`), so a long read stops at DuckDB's next poll
 *   rather than running to completion.
 * - An incremental delta's script carries the signal, and its rollback does
 *   not; a delta whose run is already cancelled never starts.
 *
 * A read DuckDB makes through BigQuery's extension is not cancelled in
 * BigQuery by an interrupt (docs/materialization.md); nothing here reaches a
 * warehouse.
 */

import { describe, expect, it } from "bun:test";
import { BuildCancelledError } from "../domain/build_cancelled";
import { applyDeltaScript } from "./incremental_apply";
import {
   createIsolatedBuildSession,
   interruptOnAbort,
} from "./materialization_build_session";

describe("P5-10: a cancelled run stops its read", () => {
   it("interrupts a storage= build's session mid-statement", async () => {
      const { session, dispose } = createIsolatedBuildSession("cancel_probe");
      try {
         await session.runSQL("SELECT 1");
         const run = new AbortController();
         const stop = interruptOnAbort(session, run.signal);
         const started = Date.now();
         const read = session.runSQL(
            "SELECT count(*) FROM range(1000000000000) a WHERE a.range % 7 = 3",
         );
         setTimeout(() => run.abort(), 200);
         await expect(read).rejects.toThrow(/interrupt/i);
         expect(Date.now() - started).toBeLessThan(10_000);
         stop();
         // The session is usable again once the interrupted statement ends.
         await expect(session.runSQL("SELECT 1 AS one")).resolves.toBeDefined();
      } finally {
         await dispose();
      }
   }, 30_000);

   it("interrupts nothing without a signal, or once released", async () => {
      const { session, dispose } = createIsolatedBuildSession("cancel_none");
      try {
         interruptOnAbort(session, undefined)();
         const run = new AbortController();
         interruptOnAbort(session, run.signal)();
         run.abort();
         await expect(session.runSQL("SELECT 1 AS one")).resolves.toBeDefined();
      } finally {
         await dispose();
      }
   });

   it("gives a delta's script the signal and its rollback none", async () => {
      const run = new AbortController();
      const calls: { sql: string; signal?: AbortSignal }[] = [];
      const runner = async (
         sql: string,
         options?: { abortSignal?: AbortSignal },
      ) => {
         calls.push({ sql, signal: options?.abortSignal });
         if (sql.includes("INSERT")) throw new Error("cancelled by the driver");
         return { rows: [] };
      };
      await expect(
         applyDeltaScript(
            runner,
            "duckdb",
            ["DELETE FROM t WHERE w >= 1", "INSERT INTO t SELECT 1"],
            run.signal,
         ),
      ).rejects.toThrow("cancelled by the driver");
      expect(calls.map((c) => c.signal)).toEqual([run.signal, undefined]);
      expect(calls[1].sql).toMatch(/ROLLBACK/i);
   });

   it("never starts a delta whose run is already cancelled", async () => {
      const run = new AbortController();
      run.abort();
      const calls: string[] = [];
      await expect(
         applyDeltaScript(
            async (sql) => {
               calls.push(sql);
               return { rows: [] };
            },
            "duckdb",
            ["INSERT INTO t SELECT 1"],
            run.signal,
         ),
      ).rejects.toBeInstanceOf(BuildCancelledError);
      expect(calls).toEqual([]);
   });
});
