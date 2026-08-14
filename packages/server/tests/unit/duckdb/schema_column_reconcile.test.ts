/// <reference types="bun-types" />

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { DuckDBConnection } from "../../../src/storage/duckdb/DuckDBConnection";
import { MaterializationRepository } from "../../../src/storage/duckdb/MaterializationRepository";
import {
   type ColumnShape,
   constrainedColumnsOf,
   initializeSchema,
   planColumnReconcile,
} from "../../../src/storage/duckdb/schema";

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

/**
 * The full definition of a table's columns, not just their names — type,
 * nullability and default. Comparing all three is what makes the upgrade
 * assertions a tripwire: a reconciled store must be indistinguishable from one
 * this build created, so the day a declared column carries something the ALTER
 * cannot express, the comparison fails instead of a divergent store shipping.
 */
async function fullShape(
   db: DuckDBConnection,
   table: string,
): Promise<unknown[]> {
   return db.all(
      `SELECT column_name, data_type, is_nullable, column_default
       FROM duckdb_columns()
       WHERE schema_name = 'main' AND database_name = current_database()
         AND table_name = ?
       ORDER BY column_name`,
      [table],
   );
}

/**
 * The table's constraints, the half `fullShape` cannot see. A column definition
 * can match perfectly while the table enforces different rules, which is the
 * divergence that fails silently rather than loudly.
 */
async function tableConstraints(
   db: DuckDBConnection,
   table: string,
): Promise<unknown[]> {
   return db.all(
      `SELECT constraint_type, constraint_column_names
       FROM duckdb_constraints()
       WHERE schema_name = 'main' AND database_name = current_database()
         AND table_name = ?
       ORDER BY constraint_type, constraint_column_names`,
      [table],
   );
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

      // The upgraded table is now indistinguishable from a freshly created one,
      // down to nullability and default — not merely "has a column of that name".
      const mirror = new DuckDBConnection(":memory:");
      await mirror.initialize();
      await initializeSchema(mirror);
      expect(await fullShape(db, "materializations")).toEqual(
         await fullShape(mirror, "materializations"),
      );
      expect(await tableConstraints(db, "materializations")).toEqual(
         await tableConstraints(mirror, "materializations"),
      );

      await mirror.close();
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

   it("refuses to add a declared NOT NULL column rather than adding it unconstrained", async () => {
      const db = new DuckDBConnection(path.join(TEST_DB_DIR, "notnull.duckdb"));
      await db.initialize();
      await seedPreManifestSchema(db);
      // `themes.payload` is declared JSON NOT NULL. A store holding the table
      // without it cannot be repaired by ALTER, and adding a nullable stand-in
      // would leave the store disagreeing with its own declaration.
      await db.run(`
         CREATE TABLE themes (
            id VARCHAR PRIMARY KEY,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL
         )
      `);

      await initializeSchema(db);

      expect(await columnNames(db, "themes")).not.toContain("payload");
      // The reconcile continues past the refusal: the unrelated column it CAN
      // add still lands in the same boot. Note this exercises the PLANNER's
      // refusal, not the per-column try/catch around the ALTER — a refused
      // column never becomes an add, so it never reaches that catch. The catch
      // is a backstop for DuckDB rejecting an ALTER the screen approved, which
      // nothing here constructs.
      expect(await columnNames(db, "materializations")).toContain("manifest");

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

// Today's declared schema has no defaulted, UNIQUE or CHECK column, so no test
// driving the real DDL can reach the branches that handle them — and those are
// precisely the branches the next column added to `schema.ts` will land in.
// Drive the decision function directly with synthetic shapes instead.
describe("planColumnReconcile", () => {
   const col = (
      table: string,
      name: string,
      type: string,
      overrides: Partial<ColumnShape> = {},
   ): ColumnShape => ({
      table_name: table,
      column_name: name,
      data_type: type,
      is_nullable: true,
      column_default: null,
      ...overrides,
   });

   const existingTable = [col("t", "id", "VARCHAR")];

   it("carries a declared DEFAULT onto the added column", () => {
      const plan = planColumnReconcile(
         [
            ...existingTable,
            col("t", "flag", "BOOLEAN", {
               column_default: "CAST('t' AS BOOLEAN)",
            }),
         ],
         existingTable,
         new Map(),
         new Map(),
      );
      expect(plan.adds).toHaveLength(1);
      expect(plan.adds[0].sql).toBe(
         `ALTER TABLE "t" ADD COLUMN "flag" BOOLEAN DEFAULT CAST('t' AS BOOLEAN)`,
      );
      expect(plan.needsMigration).toEqual([]);
   });

   it("never emits IF NOT EXISTS, which DuckDB 1.5.0-1.5.3 turns into a silent overwrite", () => {
      // duckdb/duckdb#23209: `ADD COLUMN IF NOT EXISTS ... DEFAULT` against a
      // column that already exists re-applies the default to every row rather
      // than doing nothing. Reproduced on the pinned engine for BOOLEAN and
      // TIMESTAMP. The plan's own absence check is the guard, so the clause is
      // pure downside — pinned here because re-adding it looks like a harmless
      // safety improvement.
      const plan = planColumnReconcile(
         [
            ...existingTable,
            col("t", "plain", "VARCHAR"),
            col("t", "stamped", "TIMESTAMP", { column_default: "now()" }),
         ],
         existingTable,
         new Map(),
         new Map(),
      );
      expect(plan.adds).toHaveLength(2);
      for (const add of plan.adds) {
         expect(add.sql).not.toContain("IF NOT EXISTS");
      }
   });

   it("refuses a UNIQUE column, which reports as nullable and would otherwise be added bare", () => {
      const plan = planColumnReconcile(
         [...existingTable, col("t", "slug", "VARCHAR")],
         existingTable,
         new Map([["t.slug", ["UNIQUE"]]]),
         new Map(),
      );
      expect(plan.adds).toEqual([]);
      expect(plan.needsMigration[0]).toContain("t.slug");
      expect(plan.needsMigration[0]).toContain("UNIQUE");
   });

   it("reports a nullability or default change on a column already present", () => {
      const plan = planColumnReconcile(
         [
            col("t", "id", "VARCHAR", { is_nullable: false }),
            col("t", "n", "INTEGER", { column_default: "1" }),
         ],
         [col("t", "id", "VARCHAR"), col("t", "n", "INTEGER")],
         new Map(),
         new Map(),
      );
      expect(plan.adds).toEqual([]);
      expect(plan.needsMigration).toHaveLength(2);
      expect(plan.needsMigration[0]).toContain("nullability");
      expect(plan.needsMigration[1]).toContain("default");
   });

   it("reports a constraint the store lacks, which no ALTER can add back", () => {
      // A table-level UNIQUE added to an existing table's DDL: the store keeps
      // accepting duplicate rows a fresh store would reject, and nothing fails.
      const plan = planColumnReconcile(
         existingTable,
         existingTable,
         new Map([["t.id", ["PRIMARY KEY", "UNIQUE"]]]),
         new Map([["t.id", ["PRIMARY KEY"]]]),
      );
      expect(plan.adds).toEqual([]);
      expect(plan.needsMigration).toHaveLength(1);
      expect(plan.needsMigration[0]).toContain(
         "constraints on disk PRIMARY KEY",
      );
      expect(plan.needsMigration[0]).toContain("declared PRIMARY KEY + UNIQUE");
   });

   it("does not report NOT NULL twice: nullability covers it, constraints ignore it", () => {
      const plan = planColumnReconcile(
         [col("t", "id", "VARCHAR", { is_nullable: false })],
         [col("t", "id", "VARCHAR", { is_nullable: false })],
         new Map([["t.id", ["NOT NULL"]]]),
         new Map(),
      );
      expect(plan.needsMigration).toEqual([]);
   });

   it("never plans anything for a table absent from the store", () => {
      // Just created at the declared shape by the CREATE TABLE pass.
      const plan = planColumnReconcile(
         [col("fresh", "a", "VARCHAR")],
         [],
         new Map(),
         new Map(),
      );
      expect(plan.adds).toEqual([]);
      expect(plan.needsMigration).toEqual([]);
   });

   it("reports undeclared columns and tables without planning a drop", () => {
      const plan = planColumnReconcile(
         existingTable,
         [
            ...existingTable,
            col("t", "relic", "JSON"),
            col("gone", "x", "VARCHAR"),
         ],
         new Map(),
         new Map(),
      );
      expect(plan.adds).toEqual([]);
      expect(plan.undeclared).toEqual(["t.relic", "gone (whole table)"]);
   });

   it("maps every constraint kind onto the columns it names", () => {
      const constrained = constrainedColumnsOf([
         {
            table_name: "t",
            constraint_type: "PRIMARY KEY",
            constraint_column_names: ["a", "b"],
         },
         {
            table_name: "t",
            constraint_type: "NOT NULL",
            constraint_column_names: ["a"],
         },
         {
            table_name: "t",
            constraint_type: "CHECK",
            constraint_column_names: ["c"],
         },
      ]);
      expect(constrained.get("t.a")).toEqual(["PRIMARY KEY", "NOT NULL"]);
      expect(constrained.get("t.b")).toEqual(["PRIMARY KEY"]);
      expect(constrained.get("t.c")).toEqual(["CHECK"]);
   });
});
