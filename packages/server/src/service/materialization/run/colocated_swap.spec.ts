// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { beforeAll, describe, expect, it } from "bun:test";
import { isTableNotFound, swapIntoPlace } from "./colocated_build_strategy";

/**
 * The colocated swap never leaves the served table missing for good: whatever
 * statement fails, the physical name ends up holding the previous table or the
 * new one, and a table is only dropped once nothing served depends on it.
 */

const PHYSICAL = "orders";
const STAGING = "orders_s";
const BACKUP = "orders_o";

/**
 * A warehouse of named tables, each holding a label, that runs the
 * rename-aside swap's statements and fails the ones `fails` picks (counting
 * from 0) or matches. A missing table fails the way warehouses say so.
 */
function fakeWarehouse(
   tables: Record<string, string>,
   fails: (sql: string, index: number) => boolean = () => false,
) {
   const statements: string[] = [];
   const unquote = (name: string) => name.replace(/"/g, "");
   const notFound = (name: string) =>
      new Error(`Object '${name}' does not exist or not authorized.`);
   const connection = {
      runSQL: async (sql: string) => {
         const index = statements.length;
         statements.push(sql);
         if (fails(sql, index)) throw new Error(`boom: ${sql}`);
         let m = /^SELECT 1 FROM (\S+) WHERE 1 = 0$/.exec(sql);
         if (m) {
            if (!(unquote(m[1]) in tables)) throw notFound(m[1]);
            return { rows: [] };
         }
         m = /^DROP TABLE IF EXISTS (\S+)$/.exec(sql);
         if (m) {
            delete tables[unquote(m[1])];
            return { rows: [] };
         }
         m = /^ALTER TABLE (\S+) RENAME TO (\S+)$/.exec(sql);
         if (m) {
            const [from, to] = [unquote(m[1]), unquote(m[2])];
            if (!(from in tables)) throw notFound(m[1]);
            if (to in tables) throw new Error(`Object '${to}' already exists.`);
            tables[to] = tables[from];
            delete tables[from];
            return { rows: [] };
         }
         throw new Error(`unexpected statement: ${sql}`);
      },
   };
   return { connection, statements, tables };
}

function swap(
   connection: { runSQL: (sql: string) => Promise<unknown> },
   dialect = "snowflake",
   names = { physical: PHYSICAL, staging: STAGING, backup: BACKUP },
) {
   return swapIntoPlace({
      connection: connection as never,
      runOptions: {},
      dialect,
      physicalTableName: names.physical,
      stagingTableName: names.staging,
      backupTableName: names.backup,
      connectionName: "warehouse",
   });
}

describe("swapIntoPlace, renaming aside (no transactional DDL)", () => {
   it("moves the served table aside, renames the new one in, then drops the old, with no probe", async () => {
      const w = fakeWarehouse({ [PHYSICAL]: "old", [STAGING]: "new" });
      await swap(w.connection);
      expect(w.tables).toEqual({ [PHYSICAL]: "new" });
      expect(w.statements).toEqual([
         'ALTER TABLE "orders" RENAME TO "orders_o"',
         'ALTER TABLE "orders_s" RENAME TO "orders"',
         'DROP TABLE IF EXISTS "orders_o"',
      ]);
   });

   it("on a first build, reads the not-found move as nothing served and renames the new table in", async () => {
      const w = fakeWarehouse({ [STAGING]: "new" });
      await swap(w.connection);
      expect(w.tables).toEqual({ [PHYSICAL]: "new" });
      expect(w.statements).toEqual([
         'ALTER TABLE "orders" RENAME TO "orders_o"',
         'ALTER TABLE "orders_s" RENAME TO "orders"',
         'DROP TABLE IF EXISTS "orders_o"',
      ]);
   });

   it("drops a stale backup left beside a served table once a probe shows the served table, then swaps", async () => {
      const w = fakeWarehouse({
         [PHYSICAL]: "old",
         [STAGING]: "new",
         [BACKUP]: "older",
      });
      await swap(w.connection);
      expect(w.tables).toEqual({ [PHYSICAL]: "new" });
      expect(w.statements).toEqual([
         'ALTER TABLE "orders" RENAME TO "orders_o"',
         'SELECT 1 FROM "orders" WHERE 1 = 0',
         'DROP TABLE IF EXISTS "orders_o"',
         'ALTER TABLE "orders" RENAME TO "orders_o"',
         'ALTER TABLE "orders_s" RENAME TO "orders"',
         'DROP TABLE IF EXISTS "orders_o"',
      ]);
   });

   it("after a crash between the two renames, puts the new table in and drops the stranded copy", async () => {
      const w = fakeWarehouse({ [STAGING]: "new", [BACKUP]: "old" });
      await swap(w.connection);
      expect(w.tables).toEqual({ [PHYSICAL]: "new" });
   });

   it("after a crash between the two renames, restores the stranded copy when the new one cannot go in", async () => {
      const w = fakeWarehouse(
         { [STAGING]: "new", [BACKUP]: "old" },
         (sql) => sql === 'ALTER TABLE "orders_s" RENAME TO "orders"',
      );
      await expect(swap(w.connection)).rejects.toThrow("boom");
      expect(w.tables).toEqual({ [PHYSICAL]: "old" });
   });

   it("aborts without renaming when the move aside fails other than as not-found and the probe fails too", async () => {
      // A permissions, network or timeout error says nothing about whether the
      // served table is there, so the backup name is not touched either.
      const w = fakeWarehouse(
         { [PHYSICAL]: "old", [STAGING]: "new", [BACKUP]: "older" },
         (sql) => !sql.startsWith("DROP"),
      );
      await expect(swap(w.connection)).rejects.toThrow(
         'boom: ALTER TABLE "orders" RENAME TO "orders_o"',
      );
      expect(w.tables).toEqual({ [PHYSICAL]: "old", [BACKUP]: "older" });
      expect(w.statements).toEqual([
         'ALTER TABLE "orders" RENAME TO "orders_o"',
         'SELECT 1 FROM "orders" WHERE 1 = 0',
         'DROP TABLE IF EXISTS "orders_s"',
      ]);
   });

   it("leaves the served table when the retried move aside fails", async () => {
      const w = fakeWarehouse(
         { [PHYSICAL]: "old", [STAGING]: "new" },
         (sql) => sql === 'ALTER TABLE "orders" RENAME TO "orders_o"',
      );
      await expect(swap(w.connection)).rejects.toThrow("boom");
      expect(w.tables).toEqual({ [PHYSICAL]: "old" });
   });

   it("reads a not-found from a served table it cannot see as absent, and still leaves it", async () => {
      // Snowflake says "does not exist or not authorized" for both. Read as
      // absent, the rename onto the occupied name fails, and nothing is lost.
      const w = fakeWarehouse({ [PHYSICAL]: "old", [STAGING]: "new" });
      const connection = {
         runSQL: async (sql: string) => {
            if (sql === 'ALTER TABLE "orders" RENAME TO "orders_o"') {
               throw new Error(
                  "Object 'orders' does not exist or not authorized.",
               );
            }
            return w.connection.runSQL(sql);
         },
      };
      await expect(swap(connection)).rejects.toThrow("already exists");
      expect(w.tables).toEqual({ [PHYSICAL]: "old" });
   });

   // Each statement of a rebuild over a served table, failing in turn.
   const steps = [
      // The move aside fails: the served table is there, so it is retried.
      { index: 0, survivor: "new", error: undefined },
      // The new table cannot go in: the old one is renamed back.
      { index: 1, survivor: "old", error: "boom" },
      // Only the old copy's drop failed: the new table is served.
      { index: 2, survivor: "new", error: undefined },
   ] as const;
   for (const { index, survivor, error } of steps) {
      it(`statement ${index} failing leaves the physical table holding the ${survivor} copy`, async () => {
         const w = fakeWarehouse(
            { [PHYSICAL]: "old", [STAGING]: "new" },
            (_sql, i) => i === index,
         );
         if (error) {
            await expect(swap(w.connection)).rejects.toThrow(error);
         } else {
            await swap(w.connection);
         }
         expect(w.tables[PHYSICAL]).toBe(survivor);
         // A failed swap gives back its staging table once the served table
         // is intact.
         if (error) expect(STAGING in w.tables).toBe(false);
      });
   }

   it("drops nothing when neither the new table nor the old one can be put back", async () => {
      const w = fakeWarehouse(
         { [PHYSICAL]: "old", [STAGING]: "new" },
         (sql) =>
            sql.startsWith('ALTER TABLE "orders_') && sql.endsWith('"orders"'),
      );
      await expect(swap(w.connection)).rejects.toThrow("boom");
      // Both copies are kept, under their own names, for a person to put back.
      expect(w.tables).toEqual({ [BACKUP]: "old", [STAGING]: "new" });
   });
});

/** A connection that records each call and fails the ones `fails` picks. */
function recordingConnection(fails: (sql: string) => boolean) {
   const calls: string[] = [];
   return {
      calls,
      connection: {
         runSQL: async (sql: string) => {
            calls.push(sql);
            if (fails(sql)) throw new Error(`boom: ${sql.split("\n")[0]}`);
            return { rows: [] };
         },
      },
   };
}

describe("swapIntoPlace, in one transaction (transactional DDL)", () => {
   it("sends Postgres's drop and rename as one call, an implicit transaction", async () => {
      const r = recordingConnection(() => false);
      await swap(r.connection, "postgres");
      expect(r.calls).toEqual([
         'DROP TABLE IF EXISTS "orders";\nALTER TABLE "orders_s" RENAME TO "orders";',
      ]);
   });

   it("on a Postgres failure (a dependent view refusing the drop, say), drops staging and sends no ROLLBACK", async () => {
      // The failed implicit transaction is already rolled back and ended; a
      // ROLLBACK would reach some other pooled session.
      const r = recordingConnection((sql) =>
         sql.startsWith('DROP TABLE IF EXISTS "orders";'),
      );
      await expect(swap(r.connection, "postgres")).rejects.toThrow("boom");
      expect(r.calls).toEqual([
         'DROP TABLE IF EXISTS "orders";\nALTER TABLE "orders_s" RENAME TO "orders";',
         'DROP TABLE IF EXISTS "orders_s"',
      ]);
   });

   it("brackets DuckDB's swap in BEGIN/COMMIT and rolls back before dropping staging", async () => {
      const r = recordingConnection((sql) => sql.startsWith("BEGIN"));
      await expect(swap(r.connection, "duckdb")).rejects.toThrow("boom");
      expect(r.calls).toEqual([
         'BEGIN TRANSACTION;\nDROP TABLE IF EXISTS "orders";\nALTER TABLE "orders_s" RENAME TO "orders";\nCOMMIT;',
         "ROLLBACK",
         'DROP TABLE IF EXISTS "orders_s"',
      ]);
   });

   it("still drops staging and raises the swap's own error when the ROLLBACK fails", async () => {
      const r = recordingConnection(
         (sql) => sql.startsWith("BEGIN") || sql === "ROLLBACK",
      );
      await expect(swap(r.connection, "duckdb")).rejects.toThrow(
         "boom: BEGIN TRANSACTION;",
      );
      expect(r.calls.at(-1)).toBe('DROP TABLE IF EXISTS "orders_s"');
   });

   describe("on a real DuckDB", () => {
      let duckdb: DuckDBConnection;
      // One `:memory:` database is shared by every connection in a process,
      // so each test takes fresh table names.
      let n = 0;
      beforeAll(() => {
         duckdb = new DuckDBConnection("duckdb", ":memory:");
      });

      const fresh = () => {
         const physical = `swap_${n++}`;
         return {
            physical,
            staging: `${physical}_s`,
            backup: `${physical}_o`,
         };
      };
      const label = async (table: string) => {
         const result = await duckdb.runSQL(`SELECT label FROM ${table}`);
         return (result.rows[0] as { label: string }).label;
      };
      const tables = async (prefix: string) => {
         const result = await duckdb.runSQL(
            `SELECT table_name FROM information_schema.tables WHERE table_name LIKE '${prefix}%' ORDER BY 1`,
         );
         return result.rows.map(
            (r) => (r as { table_name: string }).table_name,
         );
      };

      it("replaces the served table and leaves no staging or backup", async () => {
         const t = fresh();
         await duckdb.runSQL(
            `CREATE TABLE ${t.physical} AS SELECT 'old' AS label`,
         );
         await duckdb.runSQL(
            `CREATE TABLE ${t.staging} AS SELECT 'new' AS label`,
         );
         await swap(duckdb, "duckdb", t);
         expect(await label(t.physical)).toBe("new");
         expect(await tables(t.physical)).toEqual([t.physical]);
      });

      it("puts a first build in place", async () => {
         const t = fresh();
         await duckdb.runSQL(
            `CREATE TABLE ${t.staging} AS SELECT 'new' AS label`,
         );
         await swap(duckdb, "duckdb", t);
         expect(await label(t.physical)).toBe("new");
      });

      it("a failing rename rolls back the drop: the served table is intact", async () => {
         const t = fresh();
         await duckdb.runSQL(
            `CREATE TABLE ${t.physical} AS SELECT 'old' AS label`,
         );
         // No staging table, so the rename after the drop fails.
         await expect(swap(duckdb, "duckdb", t)).rejects.toThrow();
         expect(await label(t.physical)).toBe("old");
         // The session is out of the failed transaction: the next swap works.
         await duckdb.runSQL(
            `CREATE TABLE ${t.staging} AS SELECT 'new' AS label`,
         );
         await swap(duckdb, "duckdb", t);
         expect(await label(t.physical)).toBe("new");
      });

      it("a failing drop rolls back: the served object is intact and staging is dropped", async () => {
         const t = fresh();
         // A view under the physical name makes DROP TABLE fail, the stand-in
         // for a Postgres view that depends on the served table.
         await duckdb.runSQL(
            `CREATE VIEW ${t.physical} AS SELECT 'old' AS label`,
         );
         await duckdb.runSQL(
            `CREATE TABLE ${t.staging} AS SELECT 'new' AS label`,
         );
         await expect(swap(duckdb, "duckdb", t)).rejects.toThrow();
         expect(await label(t.physical)).toBe("old");
         expect(await tables(t.staging)).toEqual([]);
      });
   });
});

describe("isTableNotFound", () => {
   it.each([
      ['relation "orders" does not exist', true],
      ["Catalog Error: Table with name orders does not exist!", true],
      [
         "SQL compilation error: Object 'ORDERS' does not exist or not authorized.",
         true,
      ],
      [
         "Not found: Table my-project:ds.orders was not found in location US",
         true,
      ],
      ["Table 'db.orders' doesn't exist", true],
      ["Table 'hive.ds.orders' does not exist", true],
      [
         "[TABLE_OR_VIEW_NOT_FOUND] The table or view `orders` cannot be found.",
         true,
      ],
      ["Invalid object name 'orders'.", true],
      ["permission denied for table orders", false],
      ["connect ETIMEDOUT 10.0.0.1:5432", false],
      ["Object 'ORDERS_O' already exists.", false],
   ])("%s -> %p", (message, expected) => {
      expect(isTableNotFound(new Error(message))).toBe(expected);
   });

   it("reads the driver's error code", () => {
      expect(
         isTableNotFound(Object.assign(new Error("x"), { code: "42P01" })),
      ).toBe(true);
      expect(
         isTableNotFound(Object.assign(new Error("x"), { code: 1146 })),
      ).toBe(true);
      expect(
         isTableNotFound(Object.assign(new Error("x"), { code: "42501" })),
      ).toBe(false);
   });
});
