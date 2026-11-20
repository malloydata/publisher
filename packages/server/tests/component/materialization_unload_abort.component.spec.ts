// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * F-42: deleting or unloading a package aborts its running build first,
 * through a real Environment and MaterializationService on DuckDB. Before,
 * the delete answered while the run kept building the package's remaining
 * sources. Now the run is aborted before the package goes, and the delete
 * returns once it has settled CANCELLED.
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
import { Environment } from "../../src/service/environment";
import { MaterializationService } from "../../src/service/materialization/run/materialization_service";
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
const SLOW = `duckdb.sql("""SELECT a.range % 10 AS k, count(*) AS n FROM range(300000000) a GROUP BY 1""")`;
const MODEL = `##! experimental.persistence
#@ persist
source: afast is duckdb.sql("""SELECT 1 AS k""")

#@ persist
source: slow1 is ${SLOW}

#@ persist
source: slow2 is ${SLOW}

#@ persist
source: slow3 is ${SLOW}
`;

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
            // Once `afast` is built, the run is on `slow1`: retire it there.
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
            created = await service.createMaterialization(ENV, PKG, {});
            const deadline = Date.now() + 150_000;
            while (!retired) {
               if (Date.now() > deadline) throw new Error("afast never built");
               await new Promise((r) => setTimeout(r, 20));
            }
            await retired;
            // Settled before the retire returned, and cancelled, not run on.
            const running = (
               service as unknown as {
                  runningAbortControllers: Map<string, unknown>;
               }
            ).runningAbortControllers;
            expect(running.has(created.id)).toBe(false);
            const m = await repo.getMaterializationById(created.id);
            expect(m?.status).toBe("CANCELLED");
            // At most the source in flight at the abort finished after it.
            expect(builtAfterRetire.length).toBeLessThanOrEqual(1);
         } finally {
            spy.mockRestore();
         }
      });
   }
});
