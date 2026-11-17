// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { streamSqlWithBudget } from "../stream_helpers";
import { installDuckDBStatementGate } from "./duckdb_statement_gate";

installDuckDBStatementGate();

const ROWS = 5000;
const ALL = { rowLimit: 100_000 };
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function drain(
   connection: DuckDBConnection,
   sql: string,
   onRow?: (n: number) => Promise<void>,
): Promise<number> {
   let n = 0;
   for await (const _row of connection.runSQLStream(sql, ALL)) {
      n++;
      await onRow?.(n);
   }
   return n;
}

describe("concurrent statements on one DuckDB connection", () => {
   const dir = fs.mkdtempSync(path.join(os.tmpdir(), "statement-gate-"));
   let connection: DuckDBConnection;

   beforeAll(async () => {
      connection = new DuckDBConnection({
         name: "duckdb",
         databasePath: ":memory:",
         workingDirectory: dir,
      });
      await connection.runSQL(
         `CREATE TABLE t AS SELECT i AS id FROM range(${ROWS}) r(i)`,
      );
   });
   afterAll(async () => {
      await connection.close();
      fs.rmSync(dir, { recursive: true, force: true });
   });

   it("returns every row of each of several parallel streams", async () => {
      const shapes: [string, number][] = [
         ["SELECT count(*) AS n FROM t", 1],
         ["SELECT * FROM t LIMIT 13", 13],
         ["SELECT * FROM t", ROWS],
      ];
      for (let round = 0; round < 3; round++) {
         const counts = await Promise.all(
            Array.from({ length: 8 }, (_, i) =>
               drain(connection, shapes[i % shapes.length][0]),
            ),
         );
         expect(counts).toEqual(
            Array.from({ length: 8 }, (_, i) => shapes[i % shapes.length][1]),
         );
      }
   });

   it("does not truncate a slow stream when another statement arrives", async () => {
      const slow = drain(connection, "SELECT * FROM t", async (n) => {
         if (n % 100 === 0) await sleep(2);
      });
      await sleep(5);
      const other = connection.runSQL("SELECT 1 AS one", ALL);
      expect(await slow).toBe(ROWS);
      expect((await other).rows).toEqual([{ one: 1 }]);
   });

   it("frees the connection when a consumer stops early", async () => {
      for await (const _row of connection.runSQLStream(
         "SELECT * FROM t",
         ALL,
      )) {
         break;
      }
      expect(await drain(connection, "SELECT * FROM t")).toBe(ROWS);
   });

   it("lets a stream run the statements it splits off itself", async () => {
      const sql = [
         "CREATE TEMP TABLE gate_scratch AS SELECT 7 AS v",
         "SELECT v FROM gate_scratch",
      ].join("\n-- hack: split on this\n");
      expect(await drain(connection, sql)).toBe(1);
   });

   /** A stream that holds the gate until `release()`, so waiters' timing is not a guess. */
   async function holdGate(): Promise<{
      release: () => void;
      done: Promise<number>;
   }> {
      let release!: () => void;
      const parked = new Promise<void>((r) => (release = r));
      let started!: () => void;
      const first = new Promise<void>((r) => (started = r));
      const done = drain(connection, "SELECT * FROM t", async (n) => {
         if (n === 1) {
            started();
            await parked;
         }
      });
      await first;
      return { release, done };
   }

   it("rejects a stream whose signal fires while it waits at the gate, before the holder finishes", async () => {
      const holder = await holdGate();
      try {
         // The holder is parked until the finally below, so any settle before
         // then came from the waiter's own signal, not from the gate freeing.
         const outcome = await Promise.race([
            streamSqlWithBudget(
               connection,
               "SELECT * FROM t",
               { ...ALL, abortSignal: AbortSignal.timeout(30) },
               { maxRows: 0, maxBytes: 0 },
            ).then(
               () => "resolved",
               () => "rejected",
            ),
            sleep(1500).then(() => "still waiting"),
         ]);
         expect(outcome).toBe("rejected");
      } finally {
         holder.release();
      }
      expect(await holder.done).toBe(ROWS);
      // The abandoned waiter handed its turn on rather than keeping the gate.
      expect(await drain(connection, "SELECT * FROM t")).toBe(ROWS);
   });

   it("does not run a statement whose signal fired while it waited for the gate", async () => {
      const holder = await holdGate();
      const ac = new AbortController();
      const sql =
         "CREATE TEMP TABLE side AS SELECT 1 AS v\n-- hack: split on this\nSELECT 1";
      const waiter = (async () => {
         for await (const _row of connection.runSQLStream(sql, {
            ...ALL,
            abortSignal: ac.signal,
         })) {
            // drained
         }
      })();
      await sleep(20);
      ac.abort();
      await waiter;
      holder.release();
      await holder.done;
      const { rows } = await connection.runSQL(
         "SELECT count(*) AS n FROM duckdb_tables() WHERE table_name = 'side'",
         ALL,
      );
      expect(Number(rows[0].n)).toBe(0);
   });

   it("does not run a statement whose signal fired before it was called", async () => {
      const sql =
         "CREATE TEMP TABLE side_pre AS SELECT 1 AS v\n-- hack: split on this\nSELECT 1";
      for await (const _row of connection.runSQLStream(sql, {
         ...ALL,
         abortSignal: AbortSignal.abort(),
      })) {
         // drained
      }
      const { rows } = await connection.runSQL(
         "SELECT count(*) AS n FROM duckdb_tables() WHERE table_name = 'side_pre'",
         ALL,
      );
      expect(Number(rows[0].n)).toBe(0);
   });

   it("releases the gate when a stream throws", async () => {
      await expect(
         drain(connection, "SELECT * FROM no_such_table"),
      ).rejects.toThrow();
      expect(await drain(connection, "SELECT * FROM t")).toBe(ROWS);
   });

   it("serves a first statement and a first stream issued together", async () => {
      const fresh = new DuckDBConnection({
         name: "duckdb",
         databasePath: ":memory:",
         workingDirectory: fs.mkdtempSync(path.join(dir, "fresh-")),
      });
      try {
         const [stream, run] = await Promise.all([
            drain(fresh, "SELECT * FROM range(3000)"),
            fresh.runSQL("SELECT 1 AS one", ALL),
         ]);
         expect(stream).toBe(3000);
         expect(run.rows).toEqual([{ one: 1 }]);
      } finally {
         await fresh.close();
      }
   });
});
