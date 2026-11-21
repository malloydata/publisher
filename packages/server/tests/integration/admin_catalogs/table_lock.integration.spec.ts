// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The admission a table load goes through (a per-table advisory lock in the
 * catalog database, and the process's load slot) against a real Postgres. Same
 * environment as the admin integration spec; skipped without it.
 */
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";
import { loadSlots } from "../../../src/query_concurrency";
import {
   CatalogAdmin,
   CatalogAdminError,
} from "../../../src/service/catalog_admin";
import { SUPERUSER, superuser } from "./superuser";

const DB = `table_lock_${process.pid}`;

async function refusal(p: Promise<unknown>): Promise<CatalogAdminError> {
   const error = await p.then(
      () => undefined,
      (e) => e,
   );
   expect(error).toBeInstanceOf(CatalogAdminError);
   return error as CatalogAdminError;
}

describe.skipIf(!SUPERUSER)("table load admission", () => {
   const root = mkdtempSync(join(tmpdir(), "table-lock-"));
   let admin: CatalogAdmin;

   beforeAll(async () => {
      await superuser(`DROP DATABASE IF EXISTS ${DB} WITH (FORCE)`);
      await superuser(`CREATE DATABASE ${DB}`);
      admin = new CatalogAdmin({
         catalogDatabase: `${SUPERUSER} dbname=${DB}`,
      });
   });

   afterAll(async () => {
      rmSync(root, { recursive: true, force: true });
      await superuser(`DROP DATABASE IF EXISTS ${DB} WITH (FORCE)`);
   });

   it("refuses a second lock on the same table while the first is held", async () => {
      await admin.withTableLock("c", "s", "t", async () => {
         const error = await refusal(
            admin.withTableLock("c", "s", "t", async () => "never"),
         );
         expect([error.status, error.reason]).toEqual([503, "LOAD_BUSY"]);
         // The key is folded as the engine folds it.
         expect(
            (
               await refusal(
                  admin.withTableLock("C", "S", "T", async () => "never"),
               )
            ).reason,
         ).toBe("LOAD_BUSY");
      });
   });

   it("does not block a different table", async () => {
      const result = await admin.withTableLock("c", "s", "t", () =>
         admin.withTableLock("c", "s", "other", async () => "ran"),
      );
      expect(result).toBe("ran");
   });

   it("releases the lock when the callback throws", async () => {
      await expect(
         admin.withTableLock("c", "s", "t", async () => {
            throw new Error("load failed");
         }),
      ).rejects.toThrow("load failed");
      expect(
         await admin.withTableLock("c", "s", "t", async () => "again"),
      ).toBe("again");
   });

   it("answers reads of the catalog database on the lock's own connection", async () => {
      const rows = await admin.withTableLock("c", "s", "t", ({ pg }) =>
         pg("SELECT current_database() AS db"),
      );
      expect(rows).toEqual([{ db: DB }]);
   });

   it("fails assertLockHeld once the lock's backend is gone", async () => {
      await admin.withTableLock(
         "c",
         "s",
         "t",
         async ({ pg, assertLockHeld }) => {
            await assertLockHeld();
            const [{ pid }] = await pg("SELECT pg_backend_pid() AS pid");
            await superuser(`SELECT pg_terminate_backend(${Number(pid)})`);
            const error = await refusal(assertLockHeld());
            expect([error.status, error.reason]).toEqual([503, "LOAD_BUSY"]);
         },
      );
      // The dead connection's lock went with it.
      expect(await admin.withTableLock("c", "s", "t", async () => "free")).toBe(
         "free",
      );
   });

   it("refuses a PUT load while the process's load slot is held", async () => {
      mkdirSync(join(root, "ds"), { recursive: true });
      writeFileSync(join(root, "ds", "x.csv"), "id\n1\n");
      await admin.createCatalog({
         name: "ds",
         dataPath: join(root, "ds"),
         encrypted: false,
         schemas: ["s"],
      });
      const request = {
         source: { uri: join(root, "ds", "x.csv"), format: "csv" },
      };
      const slot = loadSlots.tryAcquire()!;
      try {
         const error = await refusal(
            admin.putTable("ds", "s", "t", request, false),
         );
         expect([error.status, error.reason]).toEqual([503, "LOAD_BUSY"]);
      } finally {
         slot.release();
      }
      const { created } = await admin.putTable("ds", "s", "t", request, false);
      expect(created).toBe(true);
      expect(loadSlots.active).toBe(0);
   });
});
