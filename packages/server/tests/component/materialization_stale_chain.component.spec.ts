// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { PostgresConnection } from "@malloydata/db-postgres";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import { BuildPlanUnavailableError } from "../../src/errors";
import { Environment } from "../../src/service/environment";
import { MaterializationService } from "../../src/service/materialization/run/materialization_service";
import { PACKAGE_STALE_MARKER } from "../../src/service/materialization/run/build_upstreams";
import type {
   Materialization,
   ResourceRepository,
} from "../../src/storage/DatabaseInterface";
import { DuckDBConnection } from "../../src/storage/duckdb/DuckDBConnection";
import { installSettings } from "../../src/test_helpers/settings_env";
import { writePackageFiles } from "../../src/test_helpers/temp_package";
import { openTempStorage } from "../../src/test_helpers/temp_storage";
import { serviceOverEnvironment } from "../../src/test_helpers/materialization_service_harness";

/**
 * A chained `storage=` build's text is the package's files rewritten at the
 * source ranges of the model the package serves. A package that serves a
 * model older than its files (a reload that did not load) must not stack:
 * those ranges describe other text, so the rewrite could re-root or blank the
 * wrong declaration. The downstream is refused, never recomputed from raw,
 * and the entry says why.
 *
 * A package already stale is not run at all (a 409 at creation, #1786), so
 * the refusal is reached by a package that goes stale while its run is
 * past those checks: the save and the failed reload land just before the
 * build (a test hook).
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
      ({ root, envPath, db, repo } = await openTempStorage("stale-chain-", {
         id: ENV_ID,
         name: ENV,
      }));
      writePackageFiles(
         path.join(envPath, PKG),
         {
            "m.malloy": `${RAW}\n${DOWNSTREAM}`,
            "other.malloy": "source: other is warehouse.sql('SELECT 1 AS n')\n",
         },
         { manifest: { name: PKG, description: "fixture" } },
      );
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
      service = serviceOverEnvironment(
         () => repo,
         () => env,
      );
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

   /**
    * Make the package stale the way a save makes it: one file changes the
    * downstream's definition and still compiles, another fails a load-time
    * check the build plan does not run (a misspelled gate), so the reload
    * fails and the old model keeps serving.
    */
   async function makeStale(): Promise<void> {
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
   }

   it("refuses, with a 409, to create a run of a package that is already stale", async () => {
      const first = await build();
      expect([first.status, first.error]).toEqual([
         "MANIFEST_FILE_READY",
         null,
      ]);
      await makeStale();
      const refused = await service
         .createMaterialization(ENV, PKG, { forceRefresh: true })
         .then(
            () => undefined,
            (err: unknown) => err,
         );
      expect(refused).toBeInstanceOf(BuildPlanUnavailableError);
   });

   // A package that goes stale after a run's guards (at its creation, and
   // once its plan is compiled) and before its chained build: the chained
   // build is not written from the files, which no longer describe the model
   // the run planned from, and the downstream is never recomputed from raw
   // instead: it is refused, with the coded reason.
   it("refuses the downstream, with a coded reason, when the package goes stale mid-run, rather than rewrite its files at the served model's ranges or recompute it", async () => {
      const first = await build();
      expect([first.status, first.error]).toEqual([
         "MANIFEST_FILE_READY",
         null,
      ]);
      expect(entryOf(first, "summed").upstreamReuse).toBe("reused");

      // The test hook: the save, and the reload it fails, land as the run
      // starts building, after both of its stale checks passed.
      const hooked = service.collaborators;
      const executeInstructedBuild = hooked.executeInstructedBuild;
      hooked.executeInstructedBuild = async (...args) => {
         await makeStale();
         return executeInstructedBuild(...args);
      };
      let second: Materialization;
      try {
         second = await build(true);
      } finally {
         hooked.executeInstructedBuild = executeInstructedBuild;
      }
      expect([second.status, second.error]).toEqual([
         "MANIFEST_FILE_READY",
         null,
      ]);
      const summed = entryOf(second, "summed") as Entry & { error?: string };
      expect(summed.error ?? "").toContain(PACKAGE_STALE_MARKER);
      expect(summed.error ?? "").toContain(
         "a downstream is never built by recomputing its upstreams from raw",
      );
   });
});
