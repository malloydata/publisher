/// <reference types="bun-types" />

// Integration test for the DuckDB storage stack end-to-end:
//   StorageManager -> ResourceRepository -> DuckDBConnection (@duckdb/node-api)
//   -> a real on-disk publisher.db
//
// Unlike the repository unit tests (which construct a single repo against a bare
// connection) this drives the same path the server uses -- StorageManager.initialize()
// wires the schema + repositories over the migrated connection, and getRepository()
// is what the service layer calls. It also reopens a second StorageManager against
// the same file to prove data persists across a restart, which is the real
// end-to-end guarantee for the migration.

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { StorageManager } from "../../../src/storage/StorageManager";

describe("DuckDB storage stack (integration)", () => {
   let dbDir: string;
   let dbPath: string;
   let sm: StorageManager;

   beforeEach(async () => {
      dbDir = await fs.mkdtemp(path.join(os.tmpdir(), "duckdb-storage-int-"));
      dbPath = path.join(dbDir, "publisher.db");
      sm = new StorageManager({ type: "duckdb", duckdb: { path: dbPath } });
      await sm.initialize();
   });

   afterEach(async () => {
      try {
         await sm.close();
      } catch {
         // ignore
      }
      await fs.rm(dbDir, { recursive: true, force: true });
   });

   it("initializes the schema and reports ready", () => {
      expect(sm.isInitialized()).toBe(true);
   });

   it("creates an environment with a package and connection through the repository", async () => {
      const repo = sm.getRepository();

      const env = await repo.createEnvironment({
         name: "prod",
         path: "/srv/prod",
         metadata: { owner: "platform" },
      });
      expect(env.id).toBeTruthy();

      const pkg = await repo.createPackage({
         environmentId: env.id,
         name: "sales",
         manifestPath: "/srv/prod/sales/publisher.json",
      });

      const conn = await repo.createConnection({
         environmentId: env.id,
         name: "warehouse",
         type: "snowflake",
         config: { account: "acct", warehouse: "wh" },
      });

      // Read everything back through the same repository.
      expect((await repo.getEnvironmentByName("prod"))?.id).toBe(env.id);
      expect((await repo.listPackages(env.id)).map((p) => p.name)).toEqual([
         "sales",
      ]);
      const conns = await repo.listConnections(env.id);
      expect(conns.map((c) => c.name)).toEqual(["warehouse"]);
      expect(conns[0].config).toEqual({ account: "acct", warehouse: "wh" });
      expect(pkg.environmentId).toBe(env.id);
   });

   it("persists data across a StorageManager restart (reopen the same file)", async () => {
      const repo = sm.getRepository();
      const env = await repo.createEnvironment({
         name: "persisted",
         path: "/srv/persisted",
         metadata: { keep: true },
      });
      await repo.createConnection({
         environmentId: env.id,
         name: "pg",
         type: "postgres",
         config: { host: "db.internal", port: 5432 },
      });

      // Close the manager (releases the file), then open a fresh one on the
      // same path -- the data must still be there.
      await sm.close();

      const reopened = new StorageManager({
         type: "duckdb",
         duckdb: { path: dbPath },
      });
      await reopened.initialize();
      try {
         const repo2 = reopened.getRepository();
         const env2 = await repo2.getEnvironmentByName("persisted");
         expect(env2).not.toBeNull();
         expect(env2!.metadata).toEqual({ keep: true });
         expect(env2!.createdAt).toBeInstanceOf(Date);

         const conns = await repo2.listConnections(env2!.id);
         expect(conns.map((c) => c.name)).toEqual(["pg"]);
         expect(conns[0].config).toEqual({ host: "db.internal", port: 5432 });
      } finally {
         await reopened.close();
      }

      // Point afterEach's close() at the already-closed original without error.
      sm = reopened;
   });

   it("deleteEnvironment also removes the environment's connections", async () => {
      const repo = sm.getRepository();
      const env = await repo.createEnvironment({
         name: "to-delete",
         path: "/srv/del",
      });
      await repo.createConnection({
         environmentId: env.id,
         name: "c1",
         type: "duckdb",
         config: {},
      });

      await repo.deleteEnvironment(env.id);

      expect(await repo.getEnvironmentById(env.id)).toBeNull();
      expect(await repo.listConnections(env.id)).toEqual([]);
   });
});
