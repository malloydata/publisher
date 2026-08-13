/// <reference types="bun-types" />

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { DuckDBConnection } from "../../../src/storage/duckdb/DuckDBConnection";
import { MaterializationRepository } from "../../../src/storage/duckdb/MaterializationRepository";
import { initializeSchema } from "../../../src/storage/duckdb/schema";

const TEST_DB_DIR = path.join(os.tmpdir(), "duckdb-column-reconcile-tests");

// These tests cover the case CI otherwise never reaches: an UPGRADE. Every other
// suite starts from no `publisher.db` at all, so the CREATE TABLE pass always
// runs with the current DDL and a column missing from an older store cannot
// surface. Here the older store is seeded first, on the same connection the
// assertions run against (see legacy_schema_migration.test.ts for why we do not
// close and reopen the file mid-test).

/**
 * The `materializations` shape from before 1673e281 (2026-06-19): no `manifest`
 * column. Seeded verbatim rather than derived, so it keeps describing the store
 * that shipped even as the current DDL moves on.
 */
async function seedPreManifestSchema(db: DuckDBConnection): Promise<void> {
   await db.run(`
      CREATE TABLE environments (
         id VARCHAR PRIMARY KEY,
         name VARCHAR NOT NULL UNIQUE,
         path VARCHAR NOT NULL,
         description VARCHAR,
         metadata JSON,
         created_at TIMESTAMP NOT NULL,
         updated_at TIMESTAMP NOT NULL
      )
   `);
   await db.run(`
      CREATE TABLE materializations (
         id VARCHAR PRIMARY KEY,
         environment_id VARCHAR NOT NULL,
         package_name VARCHAR NOT NULL,
         status VARCHAR NOT NULL,
         active_key VARCHAR,
         started_at TIMESTAMP,
         completed_at TIMESTAMP,
         error TEXT,
         metadata JSON,
         created_at TIMESTAMP NOT NULL,
         updated_at TIMESTAMP NOT NULL,
         FOREIGN KEY (environment_id) REFERENCES environments(id)
      )
   `);
   await db.run(
      `INSERT INTO environments VALUES ('env-1', 'env-one', '/e', NULL, NULL,
         TIMESTAMP '2026-06-01 00:00:00', TIMESTAMP '2026-06-01 00:00:00')`,
   );
}

async function columnNames(
   db: DuckDBConnection,
   table: string,
): Promise<string[]> {
   const rows = await db.all<{ column_name: string }>(
      "SELECT column_name FROM duckdb_columns() WHERE schema_name = 'main' AND table_name = ?",
      [table],
   );
   return rows.map((row) => row.column_name);
}

describe("DuckDB declared-column reconcile", () => {
   beforeEach(async () => {
      await fs.mkdir(TEST_DB_DIR, { recursive: true });
   });

   afterEach(async () => {
      try {
         await fs.rm(TEST_DB_DIR, { recursive: true, force: true });
      } catch {
         // ignore
      }
   });

   it("adds `manifest` to a store that predates it, so creating a materialization works", async () => {
      const db = new DuckDBConnection(
         path.join(TEST_DB_DIR, "premanifest.duckdb"),
      );
      await db.initialize();
      await seedPreManifestSchema(db);

      await initializeSchema(db);

      expect(await columnNames(db, "materializations")).toContain("manifest");

      // The exact call that returned 500 on such a store: the repository names
      // `manifest` in its INSERT, so before the reconcile this threw a binder
      // error rather than returning a row.
      const repo = new MaterializationRepository(db);
      const created = await repo.create("env-1", "pkg-a", "PENDING", {
         trigger: "manual",
      });
      expect(created.environmentId).toBe("env-1");
      expect(created.packageName).toBe("pkg-a");

      await db.close();
   });

   it("preserves existing rows, leaving the added column NULL", async () => {
      const db = new DuckDBConnection(path.join(TEST_DB_DIR, "rows.duckdb"));
      await db.initialize();
      await seedPreManifestSchema(db);
      await db.run(
         `INSERT INTO materializations (id, environment_id, package_name, status,
            active_key, metadata, created_at, updated_at)
          VALUES ('m-old', 'env-1', 'pkg-a', 'MANIFEST_FILE_READY', NULL, NULL,
            TIMESTAMP '2026-06-02 00:00:00', TIMESTAMP '2026-06-02 00:00:00')`,
      );

      await initializeSchema(db);

      const rows = await db.all<{ id: string; manifest: unknown }>(
         "SELECT id, manifest FROM materializations",
      );
      expect(rows).toHaveLength(1);
      expect(rows[0].id).toBe("m-old");
      expect(rows[0].manifest).toBeNull();

      await db.close();
   });

   it("leaves columns and tables the current build no longer declares in place", async () => {
      const db = new DuckDBConnection(path.join(TEST_DB_DIR, "relics.duckdb"));
      await db.initialize();
      await seedPreManifestSchema(db);
      // `build_plan` was added and removed within four days in June 2026, and
      // `build_manifests` was dropped by the same commit that added `manifest`.
      // Both are still on stores in the wild; reconciling must not take them.
      await db.run("ALTER TABLE materializations ADD COLUMN build_plan JSON");
      await db.run(
         `CREATE TABLE build_manifests (
            id VARCHAR PRIMARY KEY,
            environment_id VARCHAR NOT NULL,
            created_at TIMESTAMP NOT NULL
         )`,
      );

      await initializeSchema(db);

      const columns = await columnNames(db, "materializations");
      expect(columns).toContain("manifest");
      expect(columns).toContain("build_plan");
      const relicTable = await db.all<{ name: string }>(
         "SELECT name FROM sqlite_master WHERE type='table' AND name='build_manifests'",
      );
      expect(relicTable).toHaveLength(1);

      await db.close();
   });

   it("is idempotent: a second boot against a reconciled store changes nothing", async () => {
      const db = new DuckDBConnection(
         path.join(TEST_DB_DIR, "idempotent.duckdb"),
      );
      await db.initialize();
      await seedPreManifestSchema(db);

      await initializeSchema(db);
      const afterFirst = await columnNames(db, "materializations");
      await initializeSchema(db);
      const afterSecond = await columnNames(db, "materializations");

      expect(afterSecond).toEqual(afterFirst);

      await db.close();
   });

   it("matches a store this build created, so a fresh boot needs no reconcile", async () => {
      const db = new DuckDBConnection(path.join(TEST_DB_DIR, "fresh.duckdb"));
      await db.initialize();
      await initializeSchema(db);

      // Same comparison the reconcile makes, asserted directly: every table the
      // DDL declares is on disk with exactly the columns declared for it. This
      // is what makes the reconcile a no-op on a fresh store rather than a
      // silent source of ALTERs.
      const mirror = new DuckDBConnection(":memory:");
      await mirror.initialize();
      await initializeSchema(mirror);
      const shapeQuery = `SELECT table_name, column_name, data_type FROM duckdb_columns()
         WHERE schema_name = 'main' ORDER BY table_name, column_name`;
      expect(await db.all(shapeQuery)).toEqual(await mirror.all(shapeQuery));

      await mirror.close();
      await db.close();
   });
});
