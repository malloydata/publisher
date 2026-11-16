// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { SingleConnectionRuntime } from "@malloydata/malloy";
import { describe, expect, it } from "bun:test";
import { execFileSync } from "child_process";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import {
   dbDuckDBHardCloseInternalsMatch,
   hardCloseOwnedDuckDBConnections,
   hardCloseOutcome,
   hardCloseWhenIdle,
   installDuckDBHardClose,
   ownDuckDBConnection,
} from "./duckdb_hard_close";

installDuckDBHardClose();

/** This process's OS thread count, or undefined where it cannot be read. */
function threadCount(): number | undefined {
   if (process.platform === "linux") {
      const status = fs.readFileSync("/proc/self/status", "utf8");
      return Number(/^Threads:\s+(\d+)/m.exec(status)?.[1]);
   }
   if (process.platform === "darwin") {
      const out = execFileSync("ps", ["-M", "-p", String(process.pid)]);
      return out.toString().trim().split("\n").length - 1;
   }
   return undefined;
}

/**
 * A connection on an instance of its own (a unique working directory is part
 * of db-duckdb's share key), with a pool large enough that a leaked instance
 * shows: four threads, three of them workers.
 */
function ownInstance(): { connection: DuckDBConnection; dir: string } {
   const dir = fs.mkdtempSync(path.join(os.tmpdir(), "hard-close-"));
   const connection = new DuckDBConnection({
      name: "duckdb",
      databasePath: ":memory:",
      workingDirectory: dir,
      threads: 4,
   });
   return { connection, dir };
}

/** Open, use and retire `cycles` instances; the thread growth across them. */
async function churn(cycles: number, hardClose: boolean): Promise<number> {
   const before = threadCount()!;
   for (let i = 0; i < cycles; i++) {
      const { connection, dir } = ownInstance();
      const owner = {};
      ownDuckDBConnection(owner, connection);
      // Parallel enough to start every worker in the pool.
      await connection.runSQL(
         "SELECT count(*) FROM range(2000000) t(x) WHERE x % 7 = 0",
      );
      await connection.close();
      if (hardClose) hardCloseOwnedDuckDBConnections(owner);
      fs.rmSync(dir, { recursive: true, force: true });
   }
   return threadCount()! - before;
}

describe("DuckDB hard close", () => {
   it("finds db-duckdb's connection classes exactly as audited", () => {
      // The in-flight count proves a handle idle only while runDuckDBQuery and
      // runSQLStream are the sole native paths. A db-duckdb upgrade that adds
      // or removes a member turns the hard close off and fails here: audit the
      // new members for native use, then update the lists in
      // duckdb_hard_close.ts.
      expect(dbDuckDBHardCloseInternalsMatch()).toBe(true);
   });

   it.skipIf(threadCount() === undefined)(
      "frees each retired instance's threads at once, where a refcount close leaves them to the GC",
      async () => {
         const cycles = 20;
         // No GC is forced: a refcount close keeps every pool alive until one
         // happens to run, a hard close never needs one.
         const hard = await churn(cycles, true);
         expect(hard).toBeLessThan(4);
         // The mutation check, run in-place: the same loop without the hard
         // close grows by the instances it leaves (three workers each). If this
         // stops growing, the test above proves nothing.
         const soft = await churn(cycles, false);
         expect(soft).toBeGreaterThan(cycles);
      },
   );

   it("waits for a query in flight, which completes, and then disconnects", async () => {
      const { connection, dir } = ownInstance();
      try {
         await connection.runSQL("SELECT 1");
         const query = connection.runSQL(
            "SELECT count(*) AS n FROM range(300000000) t(x) WHERE x % 7 = 0",
         );
         // Let the query reach native code before retiring the connection.
         await new Promise((resolve) => setTimeout(resolve, 50));
         await connection.close();
         hardCloseWhenIdle(connection);
         expect(hardCloseOutcome(connection)).toBe("deferred");

         const result = await query;
         expect(Number(result.rows[0].n)).toBe(42857143);
         expect(hardCloseOutcome(connection)).toBe("disconnected");
      } finally {
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });

   it("waits for a stream being read, and disconnects when its reader returns", async () => {
      const { connection, dir } = ownInstance();
      try {
         const stream = connection.runSQLStream(
            "SELECT x FROM range(100000) t(x)",
            { rowLimit: 100000 },
         );
         const first = await stream.next();
         expect(first.done).toBe(false);
         await connection.close();
         hardCloseWhenIdle(connection);
         expect(hardCloseOutcome(connection)).toBe("deferred");

         // The reader goes on after the close, and stops early.
         const second = await stream.next();
         expect(second.done).toBe(false);
         await stream.return?.(undefined);
         expect(hardCloseOutcome(connection)).toBe("disconnected");
      } finally {
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });

   it("leaves a model compiled before the close failing cleanly after it", async () => {
      const { connection, dir } = ownInstance();
      try {
         const model = new SingleConnectionRuntime({
            connection,
         } as never).loadModel(
            'source: s is duckdb.sql("SELECT 1 AS n") extend { measure: c is count() }',
         );
         const query = model.loadQuery("run: s -> { aggregate: c }");
         expect((await query.run()).data.toObject()).toEqual([{ c: 1 }]);
         await connection.close();
         hardCloseWhenIdle(connection);
         expect(hardCloseOutcome(connection)).toBe("disconnected");

         // The compiled query, and a fresh compile against the same model,
         // both refuse with an error rather than reaching the freed database.
         await expect(query.run()).rejects.toThrow(/closed|not open/i);
         await expect(
            model.loadQuery("run: s -> { group_by: n }").run(),
         ).rejects.toThrow();
      } finally {
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });

   it("never disconnects a connection that is still open", async () => {
      const { connection, dir } = ownInstance();
      try {
         await connection.runSQL("SELECT 1");
         hardCloseWhenIdle(connection);
         expect(hardCloseOutcome(connection)).toBe("skipped");
         expect((await connection.runSQL("SELECT 2 AS n")).rows).toEqual([
            { n: 2 },
         ]);
         await connection.close();
         hardCloseWhenIdle(connection);
         expect(hardCloseOutcome(connection)).toBe("disconnected");
      } finally {
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });
});
