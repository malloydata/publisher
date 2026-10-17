// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, describe, expect, it } from "bun:test";
import { StorageManager } from "./StorageManager";
import { ResourceRepository } from "./DatabaseInterface";
import { DuckDBConnection } from "./duckdb/DuckDBConnection";
import { initializeSchema } from "./duckdb/schema";

/**
 * Characterizes the persistence layer end to end against a real in-memory
 * DuckDB: what each repository writes, how a row maps back, the id format, and
 * the cascades the repository runs on delete.
 */

const ID_FORMAT = /^\d{13}-[0-9a-z]{1,9}$/;

const managers: StorageManager[] = [];
const connections: DuckDBConnection[] = [];

afterEach(async () => {
   while (managers.length) await managers.pop()!.close();
   while (connections.length) await connections.pop()!.close();
});

async function freshStore(): Promise<{
   sm: StorageManager;
   repo: ResourceRepository;
   db: DuckDBConnection;
}> {
   const sm = new StorageManager({
      type: "duckdb",
      duckdb: { path: ":memory:" },
   });
   managers.push(sm);
   await sm.initialize();
   return { sm, repo: sm.getRepository(), db: sm.getDuckDbConnection() };
}

async function scratchDb(): Promise<DuckDBConnection> {
   const db = new DuckDBConnection(":memory:");
   connections.push(db);
   await db.initialize();
   return db;
}

async function tableNames(db: DuckDBConnection): Promise<string[]> {
   const rows = await db.all<{ name: string }>(
      "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name",
   );
   return rows.map((r) => r.name);
}

async function columnNames(
   db: DuckDBConnection,
   table: string,
): Promise<string[]> {
   const rows = await db.all<{ name: string }>(`PRAGMA table_info('${table}')`);
   return rows.map((r) => r.name);
}

describe("StorageManager", () => {
   it("throws until initialized, then serves a repository and connection", async () => {
      const sm = new StorageManager({
         type: "duckdb",
         duckdb: { path: ":memory:" },
      });
      managers.push(sm);
      expect(sm.isInitialized()).toBe(false);
      expect(() => sm.getRepository()).toThrow(
         "Storage not initialized. Call initialize() first.",
      );
      expect(() => sm.getDuckDbConnection()).toThrow("Storage not initialized");

      await sm.initialize();
      expect(sm.isInitialized()).toBe(true);
      expect(await sm.getRepository().listEnvironments()).toEqual([]);
      expect(sm.getDuckDbConnection()).toBeInstanceOf(DuckDBConnection);

      await sm.close();
      expect(sm.isInitialized()).toBe(false);
      expect(() => sm.getRepository()).toThrow("Storage not initialized");
   });

   it("rejects a storage type it does not implement", async () => {
      const sm = new StorageManager({ type: "sqlite" } as never);
      await expect(sm.initialize()).rejects.toThrow(
         "Unsupported storage type: sqlite",
      );
   });
});

describe("EnvironmentRepository (through the repository)", () => {
   it("creates, reads, lists by name and maps absent optionals", async () => {
      const { repo } = await freshStore();
      const b = await repo.createEnvironment({ name: "b", path: "/b" });
      const a = await repo.createEnvironment({
         name: "a",
         path: "/a",
         description: "first",
         metadata: { team: "data" },
      });

      expect(a.id).toMatch(ID_FORMAT);
      expect(a.createdAt).toBeInstanceOf(Date);
      expect(a.updatedAt).toEqual(a.createdAt);

      const byName = await repo.getEnvironmentByName("a");
      expect(byName).toEqual({ ...a });
      const byId = await repo.getEnvironmentById(b.id);
      // A NULL description reads back as null; absent metadata as undefined.
      expect(byId).toEqual({
         id: b.id,
         name: "b",
         path: "/b",
         description: null as unknown as undefined,
         metadata: undefined,
         createdAt: b.createdAt,
         updatedAt: b.updatedAt,
      });
      expect((await repo.listEnvironments()).map((e) => e.name)).toEqual([
         "a",
         "b",
      ]);
      expect(await repo.getEnvironmentByName("nope")).toBeNull();
      expect(await repo.getEnvironmentById("nope")).toBeNull();
   });

   it("returns the existing row when a name is created twice", async () => {
      const { repo } = await freshStore();
      const first = await repo.createEnvironment({ name: "e", path: "/one" });
      const second = await repo.createEnvironment({ name: "e", path: "/two" });
      expect(second.id).toBe(first.id);
      expect(second.path).toBe("/one");
      expect(await repo.listEnvironments()).toHaveLength(1);
   });

   it("updates only the fields given and bumps updatedAt", async () => {
      const { repo } = await freshStore();
      const env = await repo.createEnvironment({ name: "e", path: "/p" });
      await new Promise((r) => setTimeout(r, 5));
      const updated = await repo.updateEnvironment(env.id, {
         name: "e",
         path: "/q",
         description: "d",
         metadata: { k: 1 },
      });
      expect(updated).toMatchObject({
         id: env.id,
         name: "e",
         path: "/q",
         description: "d",
         metadata: { k: 1 },
      });
      expect(updated.createdAt).toEqual(env.createdAt);
      expect(updated.updatedAt.getTime()).toBeGreaterThan(
         env.updatedAt.getTime(),
      );
      await expect(repo.updateEnvironment("missing", {})).rejects.toThrow(
         "Environment with id missing not found",
      );
   });
});

describe("PackageRepository (through the repository)", () => {
   it("creates, reads, lists, updates and deletes", async () => {
      const { repo } = await freshStore();
      const env = await repo.createEnvironment({ name: "e", path: "/e" });
      const other = await repo.createEnvironment({ name: "o", path: "/o" });
      const z = await repo.createPackage({
         environmentId: env.id,
         name: "z",
         manifestPath: "/e/z",
      });
      const a = await repo.createPackage({
         environmentId: env.id,
         name: "a",
         description: "pkg",
         manifestPath: "/e/a",
         metadata: { v: 1 },
      });
      await repo.createPackage({
         environmentId: other.id,
         name: "elsewhere",
         manifestPath: "/o/x",
      });

      expect(a.id).toMatch(ID_FORMAT);
      expect(await repo.getPackageById(a.id)).toEqual(a);
      expect(await repo.getPackageByName(env.id, "z")).toEqual({
         ...z,
         description: null as unknown as undefined,
         metadata: undefined,
      });
      expect(await repo.getPackageByName(other.id, "z")).toBeNull();
      expect((await repo.listPackages(env.id)).map((p) => p.name)).toEqual([
         "a",
         "z",
      ]);

      const updated = await repo.updatePackage(z.id, {
         description: "now",
         manifestPath: "/e/z2",
         metadata: { w: 2 },
      });
      expect(updated).toMatchObject({
         name: "z",
         description: "now",
         manifestPath: "/e/z2",
         metadata: { w: 2 },
      });
      await expect(repo.updatePackage("missing", {})).rejects.toThrow(
         "Package with id missing not found",
      );

      await repo.deletePackage(z.id);
      expect(await repo.getPackageById(z.id)).toBeNull();
   });

   it("deletes a package's materializations and ledger rows with it", async () => {
      const { repo } = await freshStore();
      const env = await repo.createEnvironment({ name: "e", path: "/e" });
      const pkg = await repo.createPackage({
         environmentId: env.id,
         name: "p",
         manifestPath: "/e/p",
      });
      const m = await repo.createMaterialization(env.id, "p", "FAILED");
      const kept = await repo.createMaterialization(env.id, "q", "FAILED");
      const table = { connectionName: "c", physicalTableName: "t" };
      await repo.upsertIncrementalLedgerEntry({
         environmentId: env.id,
         packageName: "p",
         sourceEntityId: "s",
         coveredThroughValue: "1",
         coveredThroughType: "number",
         watermarkDimension: "w",
         mergeKeyDimensions: [],
         derivedStrategy: "range_replace",
         advancedByMaterializationId: null,
         ...table,
      });

      await repo.deletePackage(pkg.id);
      expect(await repo.getMaterializationById(m.id)).toBeNull();
      expect(await repo.getMaterializationById(kept.id)).not.toBeNull();
      expect(await repo.getIncrementalLedgerEntry(env.id, table)).toBeNull();
   });
});

describe("ConnectionRepository (through the repository)", () => {
   it("creates, reads, lists, updates and deletes", async () => {
      const { repo } = await freshStore();
      const env = await repo.createEnvironment({ name: "e", path: "/e" });
      const pg = await repo.createConnection({
         environmentId: env.id,
         name: "pg",
         type: "postgres",
         config: { host: "h", port: 5432 },
      });
      await repo.createConnection({
         environmentId: env.id,
         name: "bq",
         type: "bigquery",
         config: {},
      });

      expect(pg.id).toMatch(ID_FORMAT);
      expect(await repo.getConnectionById(pg.id)).toEqual(pg);
      expect(await repo.getConnectionByName(env.id, "pg")).toEqual(pg);
      expect(await repo.getConnectionByName("other", "pg")).toBeNull();
      expect((await repo.listConnections(env.id)).map((c) => c.name)).toEqual([
         "bq",
         "pg",
      ]);

      const updated = await repo.updateConnection(pg.id, {
         name: "pg2",
         type: "mysql",
         config: { host: "h2" },
      });
      expect(updated).toMatchObject({
         name: "pg2",
         type: "mysql",
         config: { host: "h2" },
      });
      await expect(repo.updateConnection("missing", {})).rejects.toThrow(
         "Connection with id missing not found",
      );

      await repo.deleteConnection(pg.id);
      expect(await repo.getConnectionById(pg.id)).toBeNull();
   });
});

describe("deleteEnvironment", () => {
   it("removes every row the environment owns and nothing of another's", async () => {
      const { repo } = await freshStore();
      const ids = [];
      for (const name of ["gone", "kept"]) {
         const env = await repo.createEnvironment({ name, path: `/${name}` });
         ids.push(env.id);
         await repo.createPackage({
            environmentId: env.id,
            name: "p",
            manifestPath: "/m",
         });
         await repo.createConnection({
            environmentId: env.id,
            name: "c",
            type: "duckdb",
            config: {},
         });
         await repo.upsertStorageDestination({
            environmentId: env.id,
            name: "d",
            type: "ducklake",
            config: {},
         });
         await repo.createMaterialization(env.id, "p", "FAILED");
      }
      const [gone, kept] = ids;

      await repo.deleteEnvironment(gone);
      expect(await repo.getEnvironmentById(gone)).toBeNull();
      expect(await repo.listPackages(gone)).toEqual([]);
      expect(await repo.listConnections(gone)).toEqual([]);
      expect(await repo.listStorageDestinations(gone)).toEqual([]);
      expect(await repo.listMaterializations(gone, "p")).toEqual([]);

      expect(await repo.listPackages(kept)).toHaveLength(1);
      expect(await repo.listConnections(kept)).toHaveLength(1);
      expect(await repo.listStorageDestinations(kept)).toHaveLength(1);
      expect(await repo.listMaterializations(kept, "p")).toHaveLength(1);
   });
});

describe("generated ids", () => {
   it("use `${Date.now()}-${base36}` for every id-bearing entity", async () => {
      const { repo } = await freshStore();
      const before = Date.now();
      const env = await repo.createEnvironment({ name: "e", path: "/e" });
      const created = [
         env.id,
         (
            await repo.createPackage({
               environmentId: env.id,
               name: "p",
               manifestPath: "/m",
            })
         ).id,
         (
            await repo.createConnection({
               environmentId: env.id,
               name: "c",
               type: "duckdb",
               config: {},
            })
         ).id,
         (
            await repo.upsertStorageDestination({
               environmentId: env.id,
               name: "d",
               type: "ducklake",
               config: {},
            })
         ).id,
         (await repo.createMaterialization(env.id, "p")).id,
      ];
      for (const id of created) {
         expect(id).toMatch(ID_FORMAT);
         const millis = Number(id.split("-")[0]);
         expect(millis).toBeGreaterThanOrEqual(before);
         expect(millis).toBeLessThanOrEqual(Date.now());
      }
   });
});

describe("initializeSchema", () => {
   it("drops a legacy `projects` schema before creating the current one", async () => {
      const db = await scratchDb();
      await db.run("CREATE TABLE projects (id VARCHAR PRIMARY KEY)");
      await db.run(
         "CREATE TABLE packages (id VARCHAR PRIMARY KEY, project_id VARCHAR)",
      );
      await initializeSchema(db);
      const tables = await tableNames(db);
      expect(tables).not.toContain("projects");
      expect(tables).toContain("environments");
      expect(await columnNames(db, "packages")).toContain("environment_id");
      expect(await columnNames(db, "packages")).not.toContain("project_id");
   });

   it("keeps rows across a normal reboot and drops them with force", async () => {
      const db = await scratchDb();
      await initializeSchema(db);
      await db.run(
         `INSERT INTO environments (id, name, path, created_at, updated_at)
          VALUES ('1', 'e', '/e', now(), now())`,
      );
      await initializeSchema(db);
      expect(await db.all("SELECT * FROM environments")).toHaveLength(1);
      await initializeSchema(db, true);
      expect(await db.all("SELECT * FROM environments")).toHaveLength(0);
   });

   it("adds a declared column an older store lacks, keeping its rows", async () => {
      const db = await scratchDb();
      await initializeSchema(db);
      await db.run("DROP TABLE materializations");
      await db.run(
         `CREATE TABLE materializations (
            id VARCHAR PRIMARY KEY, environment_id VARCHAR NOT NULL,
            package_name VARCHAR NOT NULL, status VARCHAR NOT NULL,
            active_key VARCHAR, started_at TIMESTAMP, completed_at TIMESTAMP,
            error TEXT, metadata JSON,
            created_at TIMESTAMP NOT NULL, updated_at TIMESTAMP NOT NULL,
            FOREIGN KEY (environment_id) REFERENCES environments(id))`,
      );
      await db.run(
         `INSERT INTO environments (id, name, path, created_at, updated_at)
          VALUES ('e', 'e', '/e', now(), now())`,
      );
      await db.run(
         `INSERT INTO materializations (id, environment_id, package_name, status, created_at, updated_at)
          VALUES ('m', 'e', 'p', 'FAILED', now(), now())`,
      );
      await initializeSchema(db);
      expect(await columnNames(db, "materializations")).toContain("manifest");
      expect(await db.all("SELECT * FROM materializations")).toHaveLength(1);
   });

   it("leaves an undeclared table in place", async () => {
      const db = await scratchDb();
      await initializeSchema(db);
      await db.run("CREATE TABLE scratch_extra (x INTEGER)");
      await initializeSchema(db);
      expect(await tableNames(db)).toContain("scratch_extra");
   });
});
