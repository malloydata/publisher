// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * P5-10 (F-22): a run stopped while its last source builds settles
 * CANCELLED, through a real Environment and MaterializationService on DuckDB.
 * DuckDB ignores the abort signal, so the slow CTAS finishes; the build stops
 * before its swap, and the run must not then commit the sources it built
 * before as a served, partial manifest.
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

// `afast` builds first (name order); `slow`'s CTAS takes a few seconds.
const MODEL = `##! experimental.persistence
#@ persist
source: afast is duckdb.sql("""SELECT 1 AS k""")

#@ persist
source: slow is duckdb.sql("""SELECT a.range % 10 AS k, count(*) AS n FROM range(400000000) a GROUP BY 1""")
`;

describe("P5-10: a run stopped during its last source", () => {
   let root: string;
   let db: DuckDBConnection;
   let repo: ResourceRepository;
   let env: Environment;
   let service: MaterializationService;
   let restore: (() => void) | undefined;

   beforeEach(async () => {
      let envPath: string;
      ({ root, envPath, db, repo } = await openTempStorage("mz-cancel-", {
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
   });

   afterEach(async () => {
      await env?.closeAllConnections().catch(() => undefined);
      await disposeTempStorage({ db, root });
      restore?.();
      await __setPackageLoadPoolForTests(null);
   });

   it("settles CANCELLED, not a served partial manifest", async () => {
      let created: { id: string } | undefined;
      let stopped: Promise<unknown> | undefined;
      const info = logger.info.bind(logger);
      const spy = spyOn(logger, "info").mockImplementation(((
         ...args: unknown[]
      ) => {
         const [message, meta] = args as [
            string,
            { sourceName?: string } | undefined,
         ];
         // Once `afast` is built, the run is on `slow`: stop it there.
         if (
            message === "materialization.build.finished" &&
            meta?.sourceName === "afast" &&
            created &&
            !stopped
         ) {
            stopped = service.stopMaterialization(ENV, PKG, created.id);
         }
         return (info as (...a: unknown[]) => unknown)(...args);
      }) as never);
      try {
         created = await service.createMaterialization(ENV, PKG, {});
         const deadline = Date.now() + 150_000;
         let status = "";
         for (;;) {
            const m = await repo.getMaterializationById(created.id);
            status = m?.status ?? "";
            if (
               ["MANIFEST_FILE_READY", "FAILED", "CANCELLED"].includes(status)
            ) {
               break;
            }
            if (Date.now() > deadline) throw new Error("run never settled");
            await new Promise((r) => setTimeout(r, 50));
         }
         expect(stopped).toBeDefined();
         await stopped;
         const running = (
            service as unknown as {
               runningAbortControllers: Map<string, unknown>;
            }
         ).runningAbortControllers;
         while (running.has(created.id)) {
            await new Promise((r) => setTimeout(r, 10));
         }
         const m = await repo.getMaterializationById(created.id);
         expect(m?.status).toBe("CANCELLED");
         // The run's own cancel, not a failure it raced.
         expect(
            (m?.metadata as { errorAtCancel?: string } | undefined)
               ?.errorAtCancel,
         ).toBeUndefined();
      } finally {
         spy.mockRestore();
      }
   });
});
