// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { swapIntoPlace } from "./colocated_build_strategy";

/**
 * The colocated swap never leaves the served table missing for good: whatever
 * statement fails, the physical name ends up holding the previous table or the
 * new one, and a table is only dropped once nothing served depends on it.
 */

const PHYSICAL = "orders";
const STAGING = "orders_s";
const BACKUP = "orders_o";

/**
 * A warehouse of named tables, each holding a label, that runs the swap's
 * statements and fails the ones `fails` picks (counting from 0) or matches.
 */
function fakeWarehouse(
   tables: Record<string, string>,
   fails: (sql: string, index: number) => boolean = () => false,
) {
   const statements: string[] = [];
   const unquote = (name: string) => name.replace(/"/g, "");
   const connection = {
      runSQL: async (sql: string) => {
         const index = statements.length;
         statements.push(sql);
         if (fails(sql, index)) throw new Error(`boom: ${sql}`);
         let m = /^SELECT 1 FROM (\S+) WHERE 1 = 0$/.exec(sql);
         if (m) {
            if (!(unquote(m[1]) in tables)) throw new Error("no such table");
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
            if (!(from in tables)) throw new Error("no such table");
            if (to in tables) throw new Error("already exists");
            tables[to] = tables[from];
            delete tables[from];
            return { rows: [] };
         }
         throw new Error(`unexpected statement: ${sql}`);
      },
   };
   return { connection, statements, tables };
}

function swap(connection: ReturnType<typeof fakeWarehouse>["connection"]) {
   return swapIntoPlace({
      connection: connection as never,
      runOptions: {},
      dialect: "duckdb",
      physicalTableName: PHYSICAL,
      stagingTableName: STAGING,
      backupTableName: BACKUP,
      connectionName: "warehouse",
   });
}

describe("swapIntoPlace", () => {
   it("moves the served table aside, renames the new one in, then drops the old", async () => {
      const w = fakeWarehouse({ [PHYSICAL]: "old", [STAGING]: "new" });
      await swap(w.connection);
      expect(w.tables).toEqual({ [PHYSICAL]: "new" });
      expect(w.statements).toEqual([
         'SELECT 1 FROM "orders" WHERE 1 = 0',
         'DROP TABLE IF EXISTS "orders_o"',
         'ALTER TABLE "orders" RENAME TO "orders_o"',
         'ALTER TABLE "orders_s" RENAME TO "orders"',
         'DROP TABLE IF EXISTS "orders_o"',
      ]);
   });

   it("on a first build, with nothing served, renames the new table in", async () => {
      const w = fakeWarehouse({ [STAGING]: "new" });
      await swap(w.connection);
      expect(w.tables).toEqual({ [PHYSICAL]: "new" });
      // Nothing was moved aside, so nothing is dropped.
      expect(w.statements.filter((s) => s.startsWith("DROP"))).toEqual([]);
   });

   it("drops a stale backup left beside a served table, then swaps", async () => {
      const w = fakeWarehouse({
         [PHYSICAL]: "old",
         [STAGING]: "new",
         [BACKUP]: "older",
      });
      await swap(w.connection);
      expect(w.tables).toEqual({ [PHYSICAL]: "new" });
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

   // Each statement of a rebuild over a served table, failing in turn.
   const steps = [
      // A failed probe reads as "no table": the swap then cannot rename onto
      // the name the served table still holds, and leaves it.
      { index: 0, survivor: "old", error: "already exists" },
      { index: 1, survivor: "old", error: "boom" },
      { index: 2, survivor: "old", error: "boom" },
      // The new table cannot go in: the old one is renamed back.
      { index: 3, survivor: "old", error: "boom" },
      // Only the old copy's drop failed: the new table is served.
      { index: 4, survivor: "new", error: undefined },
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
