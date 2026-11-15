// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { PostgresConnection } from "@malloydata/db-postgres";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { Environment } from "../../src/service/environment";
import type { EnvironmentStore } from "../../src/service/environment_store";
import { MaterializationService } from "../../src/service/materialization/run/materialization_service";
import { PACKAGE_STALE_MARKER } from "../../src/service/materialization/run/materialization_run_support";
import type {
   Materialization,
   ResourceRepository,
} from "../../src/storage/DatabaseInterface";
import { DuckDBConnection } from "../../src/storage/duckdb/DuckDBConnection";
import { createDuckDBRepository } from "../../src/storage/duckdb/DuckDBRepository";
import { initializeSchema } from "../../src/storage/duckdb/schema";
import { installSettings } from "../../src/test_helpers/settings_env";
import { writePackageFiles } from "../../src/test_helpers/temp_package";

/**
 * A chained `storage=` build's text is the package's files rewritten at the
 * source ranges of the model the package serves. A package that serves a
 * model older than its files (a reload that did not load) must not stack:
 * those ranges describe other text, so the rewrite could re-root or blank the
 * wrong declaration. The downstream is recomputed, as the run's plan (compiled
 * from the files) defines it, and the entry says why.
 *
 * The package is made stale the way a save makes it: one file changes the
 * downstream's definition and still compiles, another fails a load-time check
 * the build plan does not run (a misspelled gate), so the reload fails and the
 * old model keeps serving.
 *
 * Opt-in: set PUBLISHER_TEST_POSTGRES_URL, for the DuckLake destination's
 * catalog (the only destination type a configuration may declare).
 */

const ENV = "e1";
const ENV_ID = "env-1";
const PKG = "pkg";

/** Opt-in: a Postgres server the test may create and drop a database on. */
const url = process.env.PUBLISHER_TEST_POSTGRES_URL;

const RAW = `##! experimental.persistence
source: raw is warehouse.sql("""
  SELECT * FROM (VALUES ('a', 1.0), ('a', 2.0), ('b', 4.0)) AS t(cat, amt)
""")

#@ persist storage="lake"
source: daily is raw -> { group_by: cat; aggregate: total is amt.sum() }
`;

const DOWNSTREAM = `#@ persist storage="lake"
source: summed is daily -> { group_by: cat; aggregate: grand is total.sum() }
`;

describe.skipIf(!url)("a chained storage= build of a stale package", () => {
   const token = Math.random().toString(36).slice(2, 8);
   const catalog = `stale_chain_lake_${token}`;
   const warehouse = `stale_chain_wh_${token}`;
   let admin: PostgresConnection;
   let root: string;
   let envPath: string;
   let db: DuckDBConnection;
   let repo: ResourceRepository;
   let env: Environment;
   let service: MaterializationService;
   let restore: () => void;

   beforeEach(async () => {
      restore = installSettings({
         env: {
            PERSIST_STORAGE_MODE: "on",
         },
      }).restore;
      root = fs.realpathSync(
         fs.mkdtempSync(path.join(os.tmpdir(), "stale-chain-")),
      );
      envPath = path.join(root, "env");
      fs.mkdirSync(envPath);
      writePackageFiles(
         path.join(envPath, PKG),
         {
            "m.malloy": `${RAW}\n${DOWNSTREAM}`,
            "other.malloy": "source: other is warehouse.sql('SELECT 1 AS n')\n",
         },
         { manifest: { name: PKG, description: "fixture" } },
      );
      db = new DuckDBConnection(":memory:");
      await db.initialize();
      await initializeSchema(db);
      await db.run(
         "INSERT INTO environments (id, name, path, created_at, updated_at) VALUES (?, ?, ?, now(), now())",
         [ENV_ID, ENV, envPath],
      );
      repo = createDuckDBRepository(db);
      admin = new PostgresConnection({ name: "admin", connectionString: url! });
      await admin.runSQL(`CREATE DATABASE ${catalog}`);
      await admin.runSQL(`CREATE DATABASE ${warehouse}`);
      const parsed = new URL(url!);
      const pg = {
         host: parsed.hostname,
         port: Number(parsed.port || 5432),
         userName: decodeURIComponent(parsed.username),
         password: decodeURIComponent(parsed.password),
      };
      const bucket = path.join(root, "bucket");
      fs.mkdirSync(bucket);
      env = await Environment.create(
         ENV,
         envPath,
         [
            {
               name: "warehouse",
               type: "postgres",
               postgresConnection: { ...pg, databaseName: warehouse },
            } as never,
         ],
         [
            {
               name: "lake",
               type: "ducklake",
               ducklakeConnection: {
                  catalog: {
                     postgresConnection: { ...pg, databaseName: catalog },
                  },
                  storage: { bucketUrl: bucket },
               },
            } as never,
         ],
      );
      await env.addPackage(PKG);
      service = new MaterializationService({
         storageManager: { getRepository: () => repo },
         getEnvironment: async () => env,
         getLoadedEnvironments: () => [env],
      } as unknown as EnvironmentStore);
   });

   afterEach(async () => {
      await env?.closeAllConnections().catch(() => undefined);
      await db.close();
      for (const name of [catalog, warehouse]) {
         await admin
            ?.runSQL(`DROP DATABASE IF EXISTS ${name} WITH (FORCE)`)
            .catch(() => undefined);
      }
      await admin?.close().catch(() => undefined);
      fs.rmSync(root, { recursive: true, force: true });
      restore();
   });

   async function build(forceRefresh = false): Promise<Materialization> {
      const created = await service.createMaterialization(ENV, PKG, {
         forceRefresh,
      });
      const deadline = Date.now() + 60_000;
      for (;;) {
         const m = await repo.getMaterializationById(created.id);
         if (
            m &&
            ["MANIFEST_FILE_READY", "FAILED", "CANCELLED"].includes(m.status)
         ) {
            const running = (
               service as unknown as {
                  runningAbortControllers: Map<string, unknown>;
               }
            ).runningAbortControllers;
            while (running.has(created.id)) {
               await new Promise((resolve) => setTimeout(resolve, 10));
            }
            return (await repo.getMaterializationById(created.id)) ?? m;
         }
         if (Date.now() > deadline) throw new Error("run never settled");
         await new Promise((resolve) => setTimeout(resolve, 20));
      }
   }

   type Entry = {
      sourceName?: string;
      physicalTableName: string;
      rowCount?: number;
      upstreamReuse?: string;
      upstreamRecomputeReason?: string;
   };
   const entryOf = (m: Materialization, name: string): Entry => {
      const entries = (m.manifest as { entries: Record<string, Entry> })
         .entries;
      const hit = Object.values(entries).find((e) => e.sourceName === name);
      if (!hit) throw new Error(`no entry for ${name}`);
      return hit;
   };

   it("recomputes the downstream, with a coded reason, rather than rewrite its files at the served model's ranges", async () => {
      const first = await build();
      expect([first.status, first.error]).toEqual([
         "MANIFEST_FILE_READY",
         null,
      ]);
      expect(entryOf(first, "summed").upstreamReuse).toBe("reused");

      // The downstream's file now filters, and still compiles; another file
      // does not, so the reload fails and the package keeps serving the
      // model it compiled before.
      const pkgDir = path.join(envPath, PKG);
      fs.writeFileSync(
         path.join(pkgDir, "m.malloy"),
         `${RAW}\n${DOWNSTREAM.replace(
            "daily -> {",
            "daily -> { where: cat = 'a';",
         )}`,
      );
      fs.writeFileSync(
         path.join(pkgDir, "other.malloy"),
         "#(authorise) 1 = 1\nsource: other is warehouse.sql('SELECT 1 AS n')\n",
      );
      await expect(env.getPackage(PKG as never, true)).rejects.toThrow();
      expect(env.getStaleCompileErrors().has(PKG)).toBe(true);

      const second = await build(true);
      expect([second.status, second.error]).toEqual([
         "MANIFEST_FILE_READY",
         null,
      ]);
      const summed = entryOf(second, "summed");
      expect(summed.upstreamReuse).toBe("recomputed");
      expect(summed.upstreamRecomputeReason).toContain(PACKAGE_STALE_MARKER);
      // The run plans from the files, so the recompute holds their
      // definition: one category.
      expect(summed.rowCount).toBe(1);
   });
});
