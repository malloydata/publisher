// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * F-42: deleting or unloading a package aborts its running build first,
 * through a real Environment and MaterializationService on DuckDB. Before,
 * the delete answered while the run kept building the package's remaining
 * sources. Now the run is aborted before the package goes, and the delete
 * returns once it has settled CANCELLED.
 *
 * Then, over a warehouse that outlives the package (an environment DuckDB
 * connection on a file, where the package sandbox is in memory), what the
 * retire leaves: no table the cancelled run wrote, the one whose write
 * landed after the abort included, and every table a committed run of the
 * package names. Republishing the package answers equal to live, reading no
 * table the cancelled run left behind.
 */

import {
   afterEach,
   beforeEach,
   describe,
   expect,
   it,
   setDefaultTimeout,
   spyOn,
} from "bun:test";
import * as path from "path";
import { logger } from "../../src/logger";
import { unrecordedAudit } from "../../src/service/access_record";
import { parsePackageName } from "../../src/service/identity";
import { Environment } from "../../src/service/environment";
import { BuildCancelledError } from "../../src/service/materialization/domain/build_cancelled";
import {
   MaterializationService,
   type RunCollaboratorOverrides,
} from "../../src/service/materialization/run/materialization_service";
import type { ResourceRepository } from "../../src/storage/DatabaseInterface";
import { DuckDBConnection } from "../../src/storage/duckdb/DuckDBConnection";
import { __setPackageLoadPoolForTests } from "../../src/package_load/package_load_pool";
import { installSettings } from "../../src/test_helpers/settings_env";
import { writePackageFiles } from "../../src/test_helpers/temp_package";
import {
   disposeTempStorage,
   openTempStorage,
} from "../../src/test_helpers/temp_storage";
import { serviceOverEnvironment } from "../../src/test_helpers/materialization_service_harness";

setDefaultTimeout(180_000);

const ENV = "e1";
const PKG = "pkg";

// `afast` builds first (name order); each `slow` CTAS takes a few seconds,
// and DuckDB ignores the abort, so one already sent runs to its end.
const modelOn = (connection: string) => {
   const slow = `${connection}.sql("""SELECT a.range % 10 AS k, count(*) AS n FROM range(300000000) a GROUP BY 1""")`;
   return `##! experimental.persistence
#@ persist
source: afast is ${connection}.sql("""SELECT 1 AS k UNION ALL SELECT 2""")

#@ persist
source: slow1 is ${slow}

#@ persist
source: slow2 is ${slow}

#@ persist
source: slow3 is ${slow}

#@ -persist
source: afast__live is afast extend {}
`;
};
const MODEL = modelOn("duckdb");

type Retire = "deletePackage" | "unloadPackage";

/**
 * Start a run of the package and retire it once `afast` is built, while the
 * run is on `slow1`. Returns once the retire has, with the run's id and the
 * sources whose builds finished after the retire began.
 */
async function retireMidBuild(
   env: Environment,
   service: MaterializationService,
   retire: Retire,
   options: { forceRefresh?: boolean } = {},
): Promise<{ runId: string; builtAfterRetire: string[] }> {
   let created: { id: string } | undefined;
   let retired: Promise<void> | undefined;
   const builtAfterRetire: string[] = [];
   const info = logger.info.bind(logger);
   const spy = spyOn(logger, "info").mockImplementation(((
      ...args: unknown[]
   ) => {
      const [message, meta] = args as [
         string,
         { sourceName?: string } | undefined,
      ];
      if (message === "materialization.build.finished" && retired) {
         builtAfterRetire.push(meta?.sourceName ?? "");
      }
      if (
         message === "materialization.build.finished" &&
         meta?.sourceName === "afast" &&
         created &&
         !retired
      ) {
         retired = env[retire](PKG);
      }
      return (info as (...a: unknown[]) => unknown)(...args);
   }) as never);
   try {
      created = await service.createMaterialization(ENV, PKG, options);
      const deadline = Date.now() + 150_000;
      while (!retired) {
         if (Date.now() > deadline) throw new Error("afast never built");
         await new Promise((r) => setTimeout(r, 20));
      }
      await retired;
      return { runId: created.id, builtAfterRetire };
   } finally {
      spy.mockRestore();
   }
}

describe("F-42: retiring a package aborts its running build", () => {
   let root: string;
   let db: DuckDBConnection;
   let repo: ResourceRepository;
   let env: Environment;
   let service: MaterializationService;
   let restore: (() => void) | undefined;

   beforeEach(async () => {
      let envPath: string;
      ({ root, envPath, db, repo } = await openTempStorage("mz-unload-", {
         id: "env-1",
         name: ENV,
      }));
      restore = installSettings({ env: {} }).restore;
      await __setPackageLoadPoolForTests(null);
      writePackageFiles(
         path.join(envPath, PKG),
         { "m.malloy": MODEL },
         { manifest: { name: PKG, description: "fixture" } },
      );
      env = await Environment.create(ENV, envPath, []);
      await env.addPackage(PKG);
      service = serviceOverEnvironment(
         () => repo,
         () => env,
      );
      // As main_server wires it, through the store.
      env.setPackageRetiringHook((packageName) =>
         service.abortPackageRuns(ENV, packageName),
      );
   });

   afterEach(async () => {
      await env?.closeAllConnections().catch(() => undefined);
      await disposeTempStorage({ db, root });
      restore?.();
      await __setPackageLoadPoolForTests(null);
   });

   for (const retire of ["deletePackage", "unloadPackage"] as const) {
      it(`${retire} returns once the run has settled CANCELLED`, async () => {
         const { runId, builtAfterRetire } = await retireMidBuild(
            env,
            service,
            retire,
         );
         // Settled before the retire returned, and cancelled, not run on.
         const running = (
            service as unknown as {
               runningAbortControllers: Map<string, unknown>;
            }
         ).runningAbortControllers;
         expect(running.has(runId)).toBe(false);
         const m = await repo.getMaterializationById(runId);
         expect(m?.status).toBe("CANCELLED");
         // At most the source in flight at the abort finished after it.
         expect(builtAfterRetire.length).toBeLessThanOrEqual(1);
      });
   }
});

describe("F-42: what retiring a package mid-build leaves in its warehouse", () => {
   let root: string;
   let envPath: string;
   let db: DuckDBConnection;
   let repo: ResourceRepository;
   let env: Environment;
   let service: MaterializationService;
   let restore: (() => void) | undefined;
   const writeFiles = () =>
      writePackageFiles(
         path.join(envPath, PKG),
         { "m.malloy": modelOn("warehouse") },
         { manifest: { name: PKG, description: "fixture" } },
      );

   beforeEach(async () => {
      ({ root, envPath, db, repo } = await openTempStorage("mz-unload-wh-", {
         id: "env-1",
         name: ENV,
      }));
      // The warehouse attaches a file in the environment directory, so its
      // tables outlive the package and can be listed after it goes.
      restore = installSettings({
         env: { PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true" },
      }).restore;
      await __setPackageLoadPoolForTests(null);
      writeFiles();
      env = await Environment.create(ENV, envPath, [
         {
            name: "warehouse",
            type: "duckdb",
            duckdbConnection: {
               setupSQL: `ATTACH '${path.join(envPath, "wh.duckdb")}' AS wh; USE wh`,
            },
         } as never,
      ]);
      await env.addPackage(PKG);
      service = serviceOverEnvironment(
         () => repo,
         () => env,
      );
      env.setPackageRetiringHook((packageName) =>
         service.abortPackageRuns(ENV, packageName),
      );
   });

   afterEach(async () => {
      await env?.closeAllConnections().catch(() => undefined);
      await disposeTempStorage({ db, root });
      restore?.();
      await __setPackageLoadPoolForTests(null);
   });

   /**
    * Run `sql` on the warehouse, through the environment's own connection,
    * which has its file attached and in use. A second instance over the file
    * is refused on Windows, which locks a DuckDB file to the instance that
    * opened it.
    */
   async function onWarehouseFile(
      sql: string,
   ): Promise<Record<string, unknown>[]> {
      const warehouse = await env.getMalloyConnection("warehouse");
      return (await warehouse.runSQL(sql)).rows as Record<string, unknown>[];
   }

   /** The package's tables in the warehouse, by name. */
   async function packageTables(): Promise<string[]> {
      return (
         await onWarehouseFile(
            "SELECT table_name FROM information_schema.tables ORDER BY 1",
         )
      )
         .map((r) => String(r.table_name))
         .filter((t) => t.startsWith(`${PKG}__`));
   }

   /** One query's rows and which table answered it. */
   async function ask(
      query: string,
   ): Promise<{ rows: unknown; servedBy?: string }> {
      const pkg = await env.getPackage(parsePackageName(PKG), false);
      const r = await pkg.getModel("m.malloy")!.runQuery(unrecordedAudit(), {
         query,
      });
      return {
         rows: (r as unknown as { compactResult: unknown }).compactResult,
         servedBy: r.path?.servedBy?.kind ?? "live",
      };
   }

   const exact = (rows: unknown) =>
      (rows as Record<string, unknown>[]).map((r) =>
         Object.fromEntries(Object.entries(r).map(([k, v]) => [k, String(v)])),
      );

   /** A service over `env` with `collaborators`, wired to its retire. */
   function rewire(collaborators: RunCollaboratorOverrides): void {
      service = serviceOverEnvironment(
         () => repo,
         () => env,
         collaborators,
      );
      env.setPackageRetiringHook((packageName) =>
         service.abortPackageRuns(ENV, packageName),
      );
   }

   /** Run the package's `sourceNames` and wait until the run settles. */
   async function commitRun(sourceNames: string[]): Promise<void> {
      const created = await service.createMaterialization(ENV, PKG, {
         sourceNames,
      });
      const running = (
         service as unknown as {
            runningAbortControllers: Map<string, unknown>;
         }
      ).runningAbortControllers;
      const deadline = Date.now() + 120_000;
      while (running.has(created.id)) {
         if (Date.now() > deadline) throw new Error("run never settled");
         await new Promise((r) => setTimeout(r, 20));
      }
      expect((await repo.getMaterializationById(created.id))?.status).toBe(
         "MANIFEST_FILE_READY",
      );
   }

   for (const retire of ["deletePackage", "unloadPackage"] as const) {
      // F-42: the table `afast` was built into before the retire is dropped
      // before the retire returns, so no table outlives the package.
      it(`${retire} leaves no table of the package in its warehouse`, async () => {
         const { runId } = await retireMidBuild(env, service, retire);
         expect((await repo.getMaterializationById(runId))?.status).toBe(
            "CANCELLED",
         );
         expect(await packageTables()).toEqual([]);
      });

      // The race: a build whose write lands after the abort (a driver that
      // ignores the signal, or a write that commits before its interrupt
      // reaches it) and then answers cancelled. Its table is in no record of
      // the run, and is dropped all the same.
      it(`${retire} drops the table of the build in flight when its write lands after the abort`, async () => {
         let retired: Promise<void> | undefined;
         rewire((real) => ({
            buildOneSource: async (options) => {
               if (options.persistSource.name !== "afast" || retired) {
                  return real.buildOneSource(options);
               }
               retired = env[retire](PKG);
               const ignoringAbort = { ...options };
               delete ignoringAbort.signal;
               await real.buildOneSource(ignoringAbort);
               throw new BuildCancelledError();
            },
         }));
         const created = await service.createMaterialization(ENV, PKG, {});
         const deadline = Date.now() + 150_000;
         while (!retired) {
            if (Date.now() > deadline) throw new Error("afast never started");
            await new Promise((r) => setTimeout(r, 20));
         }
         await retired;
         expect((await repo.getMaterializationById(created.id))?.status).toBe(
            "CANCELLED",
         );
         expect(await packageTables()).toEqual([]);
      });

      // A run the retire reaches only after it committed, and whose next
      // step then fails: its own record names its tables, and serves them.
      it(`${retire} keeps the tables of a run that committed before the abort reached it`, async () => {
         let retired: Promise<void> | undefined;
         rewire((real) => ({
            commitManifest: async (...args) => {
               const served = await real.commitManifest(...args);
               retired = env[retire](PKG);
               void served;
               throw new Error("injected: a step after the commit failed");
            },
         }));
         const created = await service.createMaterialization(ENV, PKG, {
            sourceNames: ["afast"],
         });
         const deadline = Date.now() + 120_000;
         while (!retired) {
            if (Date.now() > deadline) throw new Error("never committed");
            await new Promise((r) => setTimeout(r, 20));
         }
         await retired;
         expect((await repo.getMaterializationById(created.id))?.status).toBe(
            "MANIFEST_FILE_READY",
         );
         expect(await packageTables()).toHaveLength(1);
      });

      // A table a committed run names is that run's: the retire keeps it,
      // as deleting a package keeps every committed run's tables. After an
      // unload, the package answers equal to live (live, as it does after
      // any cancelled rebuild of a table it served).
      it(`${retire} keeps the table a committed run names, though the cancelled run rebuilt it`, async () => {
         await commitRun(["afast"]);
         const before = await packageTables();
         expect(before).toHaveLength(1);
         const { runId } = await retireMidBuild(env, service, retire, {
            forceRefresh: true,
         });
         expect((await repo.getMaterializationById(runId))?.status).toBe(
            "CANCELLED",
         );
         expect(await packageTables()).toEqual(before);
         if (retire === "deletePackage") return;
         await env.addPackage(PKG);
         const body = "{ group_by: k; order_by: k }";
         const live = await ask(`run: afast__live -> ${body}`);
         const served = await ask(`run: afast -> ${body}`);
         expect(exact(served.rows)).toEqual(exact(live.rows));
      });

      it(`after ${retire}, the republished package answers live, equal to live, reading no table the run left`, async () => {
         const { runId } = await retireMidBuild(env, service, retire);
         expect((await repo.getMaterializationById(runId))?.status).toBe(
            "CANCELLED",
         );
         // Whatever the run left, tampered, so a read of it is told apart.
         const left = await packageTables();
         for (const table of left) {
            await onWarehouseFile(`UPDATE ${table} SET k = k + 1000`);
         }
         if (retire === "deletePackage") writeFiles();
         await env.addPackage(PKG);
         for (const source of ["afast", "slow1"]) {
            const body = "{ group_by: k; order_by: k }";
            const live = await ask(
               `run: ${source === "afast" ? "afast__live" : source} -> ${body}`,
            );
            const served = await ask(`run: ${source} -> ${body}`);
            expect(live.servedBy).toBe("live");
            expect(served.servedBy).toBe("live");
            expect(exact(served.rows)).toEqual(exact(live.rows));
            expect(exact(served.rows).some((r) => Number(r.k) >= 1000)).toBe(
               false,
            );
         }
      });
   }
});
