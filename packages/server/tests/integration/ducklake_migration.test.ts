/// <reference types="bun-types" />

// Characterization tests for DuckLake's AUTOMATIC_MIGRATION on attach.
//
// The baked engine (duckdb@1.5.3) cannot attach a 0.3-line catalog as-is; it
// requires AUTOMATIC_MIGRATION to upgrade the catalog to the "1.0" format
// first. Before Publisher decides whether to ever pass that flag on a user's
// behalf, these tests pin down what migration actually does to a real catalog,
// because the behavior is not free of footguns:
//
//   1. Migration is one-way and in-place. It rewrites the user's catalog file
//      to the "1.0" format; an older DuckDB engine (<1.5) can no longer open
//      it afterward.
//   2. Migration is data-path sensitive. If the DATA_PATH passed at attach does
//      not byte-match the path recorded in the catalog, the attach refuses
//      unless OVERRIDE_DATA_PATH is also set -- and this guards plain attach
//      too, not just migration. Publisher computes absolute data paths, while
//      catalogs created elsewhere may record relative ones, so this mismatch
//      is a live hazard.
//   3. OVERRIDE_DATA_PATH only suppresses the mismatch error; it does NOT
//      persist the passed path into the catalog. The recorded data_path is
//      left as-is, so a migrated catalog must subsequently be attached with
//      either the originally recorded DATA_PATH or no DATA_PATH at all.
//
// The fixture under tests/fixtures/ducklake_v03_catalog is a genuine 0.3
// catalog (created with the duckdb 1.4.4 CLI) with a relative recorded data
// path, so these run in CI without needing an old engine present. Each test
// copies the fixture to a temp dir and mutates only the copy.

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import fs from "fs/promises";
import os from "os";
import path from "path";

const FIXTURE_DIR = path.join(
   process.cwd(),
   "tests/fixtures/ducklake_v03_catalog",
);

interface MetadataRow {
   value: string;
}

describe("DuckLake AUTOMATIC_MIGRATION characterization", () => {
   let dir: string;
   let metaPath: string;
   let dataPath: string;
   const openConnections: DuckDBConnection[] = [];

   const connect = (name: string): DuckDBConnection => {
      const conn = new DuckDBConnection(name);
      openConnections.push(conn);
      return conn;
   };

   beforeEach(async () => {
      dir = await fs.mkdtemp(path.join(os.tmpdir(), "ducklake-migration-"));
      await fs.cp(FIXTURE_DIR, dir, { recursive: true });
      metaPath = path.join(dir, "catalog.ducklake");
      dataPath = path.join(dir, "data");
   });

   afterEach(async () => {
      for (const conn of openConnections) {
         try {
            await conn.close();
         } catch {
            // already closed / never opened
         }
      }
      openConnections.length = 0;
      await fs.rm(dir, { recursive: true, force: true });
   });

   const recordedValue = async (key: string): Promise<string> => {
      const conn = connect(`reader_${key}`);
      await conn.runSQL(`ATTACH '${metaPath}' AS meta`);
      const res = await conn.runSQL(
         `SELECT value FROM meta.ducklake_metadata WHERE key = '${key}'`,
      );
      const value = String((res.rows[0] as MetadataRow).value);
      await conn.close();
      return value;
   };

   it("the fixture is a genuine 0.3-line catalog", async () => {
      expect(await recordedValue("version")).toBe("0.3");
      // Relative recorded data path is what makes the path-mismatch case below
      // reproducible against an absolute attach path.
      expect(await recordedValue("data_path")).toBe("data/");
   });

   it("a plain attach of the 0.3 catalog is rejected by the engine", async () => {
      const conn = connect("plain_attach");
      await conn.runSQL("LOAD ducklake");
      let attachFailed = false;
      try {
         await conn.runSQL(
            `ATTACH 'ducklake:${metaPath}' AS dl (DATA_PATH '${dataPath}')`,
         );
      } catch {
         attachFailed = true;
      }
      expect(attachFailed).toBe(true);
   });

   it("AUTOMATIC_MIGRATION refuses when the attach data path differs from the recorded one", async () => {
      // dataPath here is absolute; the catalog recorded the relative "data/".
      // Migration must not silently proceed on a data-path mismatch.
      const conn = connect("migrate_path_mismatch");
      await conn.runSQL("LOAD ducklake");
      let migrateFailed = false;
      let message = "";
      try {
         await conn.runSQL(
            `ATTACH 'ducklake:${metaPath}' AS dl (DATA_PATH '${dataPath}', AUTOMATIC_MIGRATION TRUE)`,
         );
      } catch (e) {
         migrateFailed = true;
         message = String(e);
      }
      expect(migrateFailed).toBe(true);
      expect(message).toContain("does not match existing data path");

      // The refused attempt must not have upgraded the catalog.
      expect(await recordedValue("version")).toBe("0.3");
   });

   it("AUTOMATIC_MIGRATION + OVERRIDE_DATA_PATH migrates in place and preserves data, but leaves the recorded path unchanged", async () => {
      const conn = connect("migrate_override");
      await conn.runSQL("LOAD ducklake");
      await conn.runSQL(
         `ATTACH 'ducklake:${metaPath}' AS dl (DATA_PATH '${dataPath}', AUTOMATIC_MIGRATION TRUE, OVERRIDE_DATA_PATH TRUE)`,
      );
      const rows = await conn.runSQL(
         "SELECT id, label FROM dl.items ORDER BY id",
      );
      expect(rows.rows).toEqual([
         { id: 1, label: "alpha" },
         { id: 2, label: "beta" },
         { id: 3, label: "gamma" },
      ]);
      await conn.close();

      // In-place upgrade: the catalog file is now 1.0. OVERRIDE_DATA_PATH only
      // suppressed the mismatch error for this attach; it did NOT persist the
      // absolute path, so data_path is still the original relative value.
      expect(await recordedValue("version")).toBe("1.0");
      expect(await recordedValue("data_path")).toBe("data/");
   });

   it("a migrated catalog re-attaches without the migration flag, using the recorded data path", async () => {
      const migrate = connect("migrate_then_reattach");
      await migrate.runSQL("LOAD ducklake");
      await migrate.runSQL(
         `ATTACH 'ducklake:${metaPath}' AS dl (DATA_PATH '${dataPath}', AUTOMATIC_MIGRATION TRUE, OVERRIDE_DATA_PATH TRUE)`,
      );
      await migrate.close();

      // The catalog is now 1.0, so no migration flag is needed. But the
      // recorded data path is still relative, so a plain attach must match it
      // (or omit DATA_PATH entirely). Passing the absolute path again would
      // re-trigger the mismatch error.
      const reattach = connect("plain_reattach");
      await reattach.runSQL("LOAD ducklake");
      await reattach.runSQL(`ATTACH 'ducklake:${metaPath}' AS dl`);
      const rows = await reattach.runSQL("SELECT count(*) AS n FROM dl.items");
      expect(Number((rows.rows[0] as { n: number }).n)).toBe(3);
   });

   it("re-attaching a migrated catalog with a mismatched absolute path still fails", async () => {
      const migrate = connect("migrate_for_mismatch");
      await migrate.runSQL("LOAD ducklake");
      await migrate.runSQL(
         `ATTACH 'ducklake:${metaPath}' AS dl (DATA_PATH '${dataPath}', AUTOMATIC_MIGRATION TRUE, OVERRIDE_DATA_PATH TRUE)`,
      );
      await migrate.close();

      const reattach = connect("mismatch_reattach");
      await reattach.runSQL("LOAD ducklake");
      let failed = false;
      let message = "";
      try {
         await reattach.runSQL(
            `ATTACH 'ducklake:${metaPath}' AS dl (DATA_PATH '${dataPath}')`,
         );
      } catch (e) {
         failed = true;
         message = String(e);
      }
      expect(failed).toBe(true);
      expect(message).toContain("does not match existing data path");
   });
});
