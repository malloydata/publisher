/// <reference types="bun-types" />

// Round-trip tests for the DuckDB storage repositories against a REAL
// DuckDBConnection (no mocks). The repositories were untested at the storage
// layer -- existing service specs mock the repository methods -- so these run
// their actual SQL through the connection migrated to @duckdb/node-api. They
// guard the migration-sensitive behaviors: parameterized writes, JSON-column
// round-trips (config/metadata via JSON.stringify -> JSON.parse), TIMESTAMP ->
// Date mapping, and foreign-key relationships between the tables.

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { DuckDBConnection } from "../../../src/storage/duckdb/DuckDBConnection";
import { initializeSchema } from "../../../src/storage/duckdb/schema";
import { EnvironmentRepository } from "../../../src/storage/duckdb/EnvironmentRepository";
import { PackageRepository } from "../../../src/storage/duckdb/PackageRepository";
import { ConnectionRepository } from "../../../src/storage/duckdb/ConnectionRepository";

describe("DuckDB storage repositories (real connection)", () => {
   let dbDir: string;
   let db: DuckDBConnection;
   let environments: EnvironmentRepository;
   let packages: PackageRepository;
   let connections: ConnectionRepository;

   beforeEach(async () => {
      dbDir = await fs.mkdtemp(path.join(os.tmpdir(), "duckdb-repos-"));
      db = new DuckDBConnection(path.join(dbDir, "test.db"));
      await db.initialize();
      await initializeSchema(db);
      environments = new EnvironmentRepository(db);
      packages = new PackageRepository(db);
      connections = new ConnectionRepository(db);
   });

   afterEach(async () => {
      try {
         await db.close();
      } catch {
         // ignore
      }
      await fs.rm(dbDir, { recursive: true, force: true });
   });

   describe("EnvironmentRepository", () => {
      it("creates and reads back an environment with metadata", async () => {
         const created = await environments.createEnvironment({
            name: "env-a",
            path: "/tmp/env-a",
            description: "first",
            metadata: { team: "data", tier: 2 },
         });

         expect(created.id).toBeTruthy();
         expect(created.createdAt).toBeInstanceOf(Date);

         const byId = await environments.getEnvironmentById(created.id);
         expect(byId).not.toBeNull();
         expect(byId!.name).toBe("env-a");
         expect(byId!.description).toBe("first");
         // metadata survives the JSON round-trip (stringify on write, parse on read)
         expect(byId!.metadata).toEqual({ team: "data", tier: 2 });
         // TIMESTAMP columns come back as Date
         expect(byId!.createdAt).toBeInstanceOf(Date);
         expect(byId!.updatedAt).toBeInstanceOf(Date);
      });

      it("looks up an environment by name", async () => {
         await environments.createEnvironment({
            name: "env-b",
            path: "/tmp/b",
         });
         const byName = await environments.getEnvironmentByName("env-b");
         expect(byName?.name).toBe("env-b");
         expect(await environments.getEnvironmentByName("nope")).toBeNull();
      });

      it("lists, updates, and deletes environments", async () => {
         const e = await environments.createEnvironment({
            name: "env-c",
            path: "/tmp/c",
         });

         expect(
            (await environments.listEnvironments()).map((x) => x.name),
         ).toContain("env-c");

         const updated = await environments.updateEnvironment(e.id, {
            description: "updated",
            metadata: { changed: true },
         });
         expect(updated.description).toBe("updated");
         expect(updated.metadata).toEqual({ changed: true });

         await environments.deleteEnvironment(e.id);
         expect(await environments.getEnvironmentById(e.id)).toBeNull();
      });
   });

   describe("PackageRepository", () => {
      it("creates a package under an environment and reads it back", async () => {
         const env = await environments.createEnvironment({
            name: "env-pkg",
            path: "/tmp/env-pkg",
         });

         const pkg = await packages.createPackage({
            environmentId: env.id,
            name: "pkg-1",
            manifestPath: "/tmp/env-pkg/pkg-1/publisher.json",
            metadata: { source: "test" },
         });

         const byId = await packages.getPackageById(pkg.id);
         expect(byId?.name).toBe("pkg-1");
         expect(byId?.manifestPath).toBe("/tmp/env-pkg/pkg-1/publisher.json");
         expect(byId?.metadata).toEqual({ source: "test" });

         const byName = await packages.getPackageByName(env.id, "pkg-1");
         expect(byName?.id).toBe(pkg.id);

         const list = await packages.listPackages(env.id);
         expect(list.map((p) => p.name)).toEqual(["pkg-1"]);

         await packages.deletePackage(pkg.id);
         expect(await packages.getPackageById(pkg.id)).toBeNull();
      });
   });

   describe("ConnectionRepository", () => {
      it("round-trips a connection's JSON config", async () => {
         const env = await environments.createEnvironment({
            name: "env-conn",
            path: "/tmp/env-conn",
         });

         const config = {
            account: "acct",
            warehouse: "wh",
            nested: { a: 1, b: ["x", "y"] },
         };
         const created = await connections.createConnection({
            environmentId: env.id,
            name: "snow",
            type: "snowflake",
            config,
         });

         const byId = await connections.getConnectionById(created.id);
         expect(byId).not.toBeNull();
         expect(byId!.type).toBe("snowflake");
         // The config column is stored as JSON and must deserialize identically.
         expect(byId!.config).toEqual(config);

         const byName = await connections.getConnectionByName(env.id, "snow");
         expect(byName?.id).toBe(created.id);
      });

      it("lists and deletes connections, including by environment", async () => {
         const env = await environments.createEnvironment({
            name: "env-conn2",
            path: "/tmp/env-conn2",
         });
         await connections.createConnection({
            environmentId: env.id,
            name: "c1",
            type: "postgres",
            config: { host: "h" },
         });
         await connections.createConnection({
            environmentId: env.id,
            name: "c2",
            type: "duckdb",
            config: {},
         });

         expect(
            (await connections.listConnections(env.id)).map((c) => c.name),
         ).toEqual(["c1", "c2"]);

         await connections.deleteConnectionsByEnvironmentId(env.id);
         expect(await connections.listConnections(env.id)).toEqual([]);
      });

      it("updates a connection's config", async () => {
         const env = await environments.createEnvironment({
            name: "env-conn3",
            path: "/tmp/env-conn3",
         });
         const c = await connections.createConnection({
            environmentId: env.id,
            name: "c",
            type: "postgres",
            config: { host: "old" },
         });

         const updated = await connections.updateConnection(c.id, {
            config: { host: "new", port: 5432 },
         });
         expect(updated.config).toEqual({ host: "new", port: 5432 });

         const reread = await connections.getConnectionById(c.id);
         expect(reread!.config).toEqual({ host: "new", port: 5432 });
      });
   });
});
