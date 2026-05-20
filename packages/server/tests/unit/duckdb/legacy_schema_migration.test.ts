/// <reference types="bun-types" />

// TODO: Remove this during projects cleanup

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { DuckDBConnection } from "../../../src/storage/duckdb/DuckDBConnection";
import { initializeSchema } from "../../../src/storage/duckdb/schema";

const TEST_DB_DIR = path.join(os.tmpdir(), "duckdb-legacy-migration-tests");

// Seed a pre-rename schema on an *already-open* connection. We deliberately
// avoid opening, closing, and reopening the same DuckDB file within a test:
// on Windows runners the second `duckdb.Database(path)` call sometimes fails
// with `Invalid Error` because the OS hasn't released the file handle yet.
// Sharing one connection between seed and assertion sidesteps that entirely.
async function seedLegacySchema(db: DuckDBConnection): Promise<void> {
   // Seed a pre-rename schema: parent table named `projects` and child
   // tables with `project_id` foreign-key columns. Mirrors what an existing
   // installation looked like before the projects→environments rename.
   await db.run(`
      CREATE TABLE projects (
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
      CREATE TABLE packages (
         id VARCHAR PRIMARY KEY,
         project_id VARCHAR NOT NULL,
         name VARCHAR NOT NULL,
         description VARCHAR,
         manifest_path VARCHAR NOT NULL,
         metadata JSON,
         created_at TIMESTAMP NOT NULL,
         updated_at TIMESTAMP NOT NULL,
         FOREIGN KEY (project_id) REFERENCES projects(id)
      )
   `);
   await db.run(`
      CREATE TABLE connections (
         id VARCHAR PRIMARY KEY,
         project_id VARCHAR NOT NULL,
         name VARCHAR NOT NULL,
         type VARCHAR NOT NULL,
         config JSON NOT NULL,
         created_at TIMESTAMP NOT NULL,
         updated_at TIMESTAMP NOT NULL,
         FOREIGN KEY (project_id) REFERENCES projects(id)
      )
   `);
   await db.run(`
      CREATE TABLE materializations (
         id VARCHAR PRIMARY KEY,
         project_id VARCHAR NOT NULL,
         package_name VARCHAR NOT NULL,
         status VARCHAR NOT NULL,
         active_key VARCHAR,
         started_at TIMESTAMP,
         completed_at TIMESTAMP,
         error TEXT,
         metadata JSON,
         created_at TIMESTAMP NOT NULL,
         updated_at TIMESTAMP NOT NULL,
         FOREIGN KEY (project_id) REFERENCES projects(id)
      )
   `);
   await db.run(`
      CREATE TABLE build_manifests (
         id VARCHAR PRIMARY KEY,
         project_id VARCHAR NOT NULL,
         package_name VARCHAR NOT NULL,
         build_id VARCHAR NOT NULL,
         table_name VARCHAR NOT NULL,
         source_name VARCHAR NOT NULL,
         connection_name VARCHAR NOT NULL,
         created_at TIMESTAMP NOT NULL,
         updated_at TIMESTAMP NOT NULL,
         FOREIGN KEY (project_id) REFERENCES projects(id)
      )
   `);

   await db.run(
      `INSERT INTO projects VALUES ('p1', 'proj-one', '/p1', 'd1', NULL,
         TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 00:00:00')`,
   );
   await db.run(
      `INSERT INTO packages VALUES ('pkg1', 'p1', 'pkg-one', NULL, '/m', NULL,
         TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 00:00:00')`,
   );
}

describe("DuckDB legacy projects schema cleanup", () => {
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

   it("drops legacy projects schema and creates the new environments schema cleanly", async () => {
      const dbPath = path.join(TEST_DB_DIR, "legacy.duckdb");
      const db = new DuckDBConnection(dbPath);
      await db.initialize();

      // Seed the legacy schema on the same connection, then run the
      // production schema-init path. This mirrors a server upgrade
      // (legacy data on disk, new code starting up) without forcing a
      // close+reopen, which is unreliable on Windows runners.
      await seedLegacySchema(db);
      await initializeSchema(db);

      // Legacy parent table is gone.
      const legacyProjects = await db.all<{ name: string }>(
         "SELECT name FROM sqlite_master WHERE type='table' AND name='projects'",
      );
      expect(legacyProjects.length).toBe(0);

      // New `environments` table exists and is empty (legacy data dropped).
      const envs = await db.all<{ id: string }>("SELECT id FROM environments");
      expect(envs.length).toBe(0);

      // Child tables are queryable by `environment_id` (the new column),
      // proving they were recreated with the new schema rather than left on
      // the old `project_id` column.
      const pkgs = await db.all<{ id: string }>(
         "SELECT id FROM packages WHERE environment_id = ?",
         ["p1"],
      );
      expect(pkgs.length).toBe(0);
      const conns = await db.all<{ id: string }>(
         "SELECT id FROM connections WHERE environment_id = ?",
         ["p1"],
      );
      expect(conns.length).toBe(0);
      const mats = await db.all<{ id: string }>(
         "SELECT id FROM materializations WHERE environment_id = ?",
         ["p1"],
      );
      expect(mats.length).toBe(0);
      const manifests = await db.all<{ id: string }>(
         "SELECT id FROM build_manifests WHERE environment_id = ?",
         ["p1"],
      );
      expect(manifests.length).toBe(0);

      await db.close();
   });

   it("is idempotent: running initializeSchema twice on a migrated DB is a no-op", async () => {
      const dbPath = path.join(TEST_DB_DIR, "legacy_idempotent.duckdb");
      const db = new DuckDBConnection(dbPath);
      await db.initialize();

      await seedLegacySchema(db);
      await initializeSchema(db);
      // Second call should hit the early-return path (isInitialized() === true).
      await initializeSchema(db);

      const envs = await db.all<{ id: string }>("SELECT id FROM environments");
      expect(envs.length).toBe(0);

      await db.close();
   });

   it("creates a fresh schema unchanged when no legacy projects table is present", async () => {
      const dbPath = path.join(TEST_DB_DIR, "fresh.duckdb");
      const db = new DuckDBConnection(dbPath);
      await db.initialize();
      await initializeSchema(db);

      const envs = await db.all<{ id: string }>("SELECT id FROM environments");
      expect(envs.length).toBe(0);

      const legacy = await db.all<{ name: string }>(
         "SELECT name FROM sqlite_master WHERE type='table' AND name='projects'",
      );
      expect(legacy.length).toBe(0);

      await db.close();
   });
});
