// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
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
