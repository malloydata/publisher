// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A table built in the package's own `duckdb` sandbox, then lost: the sandbox
 * is in memory, so a restart empties it while the run record survives
 * (#1419). The next run rebuilds the table instead of carrying it.
 *
 * Before the fix skip-if-unchanged trusted the run record: every later run
 * reported the source reused (`sourcesReused: 1`), the table was never
 * rebuilt, and its queries answered live. A table at an exact `name=` was
 * already rebuilt (its ownership recheck finds nothing to own), so the source
 * here has none. The test drops the table rather than restarting the server;
 * reuse is decided from the run record either way.
 */

import { DuckDBConnection as MalloyDuckDBConnection } from "@malloydata/db-duckdb";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as path from "path";
import { Environment } from "../../src/service/environment";
import { parsePackageName } from "../../src/service/identity";
import { MaterializationService } from "../../src/service/materialization/run/materialization_service";
import type {
   Materialization,
   ResourceRepository,
} from "../../src/storage/DatabaseInterface";
import { DuckDBConnection } from "../../src/storage/duckdb/DuckDBConnection";
import { __setPackageLoadPoolForTests } from "../../src/package_load/package_load_pool";
import { installSettings } from "../../src/test_helpers/settings_env";
import { writePackageFiles } from "../../src/test_helpers/temp_package";
import {
   openTempStorage,
   disposeTempStorage,
} from "../../src/test_helpers/temp_storage";
import { serviceOverEnvironment } from "../../src/test_helpers/materialization_service_harness";

const ENV = "e1";
const ENV_ID = "env-1";
const PKG = "pkg";

const FILES = {
   "m.malloy": `#(materialize)
source: daily is duckdb.sql("""
  SELECT * FROM (VALUES (1, 'a', 10.0), (2, 'b', 20.0)) AS t(id, cat, amt)
""") extend {
  measure: total is amt.sum()
}
`,
};

describe("a table in the package's duckdb sandbox, lost after it was built", () => {
   let root: string;
   let envPath: string;
   let db: DuckDBConnection;
   let repo: ResourceRepository;
   let env: Environment;
   let service: MaterializationService;
   let restore: () => void;

   beforeEach(async () => {
      restore = installSettings({
         env: { PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      await __setPackageLoadPoolForTests(null);
      ({ root, envPath, db, repo } = await openTempStorage("sandbox-lost-", {
         id: ENV_ID,
         name: ENV,
      }));
      writePackageFiles(path.join(envPath, PKG), FILES, {
         manifest: { name: PKG, description: "fixture" },
      });
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
      restore();
      await __setPackageLoadPoolForTests(null);
   });

   async function build(): Promise<Materialization> {
      const created = await service.createMaterialization(ENV, PKG, {});
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

   async function sandbox(): Promise<MalloyDuckDBConnection> {
      const pkg = await env.getPackage(parsePackageName(PKG), false);
      return (await pkg.getMalloyConnection(
         "duckdb",
      )) as MalloyDuckDBConnection;
   }

   const tableOf = (m: Materialization) =>
      Object.values(
         (
            m.manifest as {
               entries: Record<string, { physicalTableName: string }>;
            }
         ).entries,
      )[0].physicalTableName;

   async function exists(table: string): Promise<boolean> {
      const rows = (
         await (
            await sandbox()
         ).runSQL(
            `SELECT 1 FROM information_schema.tables WHERE table_name = '${table}'`,
         )
      ).rows;
      return rows.length > 0;
   }

   it("is carried while it exists, and rebuilt once it is gone", async () => {
      const first = await build();
      expect([first.status, first.error]).toEqual([
         "MANIFEST_FILE_READY",
         null,
      ]);
      expect(first.metadata).toMatchObject({ sourcesBuilt: 1 });
      const table = tableOf(first);
      expect(await exists(table)).toBe(true);

      // Unchanged and present: carried, not rebuilt.
      const second = await build();
      expect(second.metadata).toMatchObject({
         sourcesBuilt: 0,
         sourcesReused: 1,
      });

      // What a restart does to the in-memory sandbox.
      await (await sandbox()).runSQL(`DROP TABLE "${table}"`);
      expect(await exists(table)).toBe(false);

      const third = await build();
      expect([third.status, third.error]).toEqual([
         "MANIFEST_FILE_READY",
         null,
      ]);
      expect(third.metadata).toMatchObject({
         sourcesBuilt: 1,
         sourcesReused: 0,
      });
      expect(await exists(tableOf(third))).toBe(true);
   });
});
