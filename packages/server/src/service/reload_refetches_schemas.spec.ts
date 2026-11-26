// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * An explicit refresh refetches the columns of the tables a package reads,
 * and a watch-mode reload keeps the cached ones.
 *
 * A table's schema is cached twice: in the package-load worker
 * (schema_cache.ts) and in the environment connection's own Malloy cache on
 * the main thread. Both outlive a load, so without a refresh a warehouse
 * column change is never seen until a restart. These run a real Environment
 * with a file-backed DuckDB connection, and change the table's columns
 * between loads with ALTER TABLE.
 */
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import type { DuckDBConnection } from "@malloydata/db-duckdb";
import { PackageController } from "../controller/package.controller";
import { registerReloadPackageTool } from "../mcp/tools/reload_package_tool";
import type { EnvironmentStore } from "./environment_store";
import { Environment } from "./environment";
import { parsePackageName } from "./identity";
import type { Package } from "./package";
import {
   usePackageLoadPool,
   writePackageFiles,
} from "../test_helpers/temp_package";
import { testRetrieval } from "../test_helpers/test_retrieval";
import { installSettings } from "../test_helpers/settings_env";
import {
   disposeTempStorage,
   openTempStorage,
} from "../test_helpers/temp_storage";

const MODELS = {
   // Two models read the same table and the same SQL, so one load asks for
   // each more than once.
   "one.malloy": `source: from_table is warehouse.table('t')
source: from_sql is warehouse.sql("select * from t")
`,
   "two.malloy": `source: also_table is warehouse.table('t')
`,
};

function columnsOf(pkg: Package, modelPath: string, source: string): string[] {
   const def = pkg.getModel(modelPath)?.getModelDef()?.contents[source] as
      | { fields?: { name: string }[] }
      | undefined;
   return (def?.fields ?? []).map((f) => f.name);
}

/** Every source's columns, so a test sees the table and the SQL agree. */
function served(pkg: Package): string[][] {
   return [
      columnsOf(pkg, "one.malloy", "from_table"),
      columnsOf(pkg, "one.malloy", "from_sql"),
      columnsOf(pkg, "two.malloy", "also_table"),
   ];
}

const all = (columns: string[]) => [columns, columns, columns];

const WAREHOUSE = {
   name: "warehouse",
   type: "duckdb",
   // An environment DuckDB connection needs setupSQL or an attachment.
   duckdbConnection: { setupSQL: "SELECT 1" },
} as never;

describe("an explicit refresh refetches table schemas", () => {
   usePackageLoadPool();
   let rootDir: string;
   let envPath: string;
   let env: Environment;
   let warehouse: DuckDBConnection;
   let restoreSettings: () => void;

   const pkgName = parsePackageName("pkg");
   const current = () => env.getPackage(pkgName, false);

   beforeEach(async () => {
      restoreSettings = installSettings({
         env: { PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true" },
      }).restore;
      rootDir = fs.mkdtempSync(path.join(os.tmpdir(), "publisher-refetch-"));
      envPath = path.join(rootDir, "env");
      fs.mkdirSync(envPath, { recursive: true });
      env = await Environment.create("testEnv", envPath, [WAREHOUSE]);
      warehouse = (await env.getMalloyConnection(
         "warehouse",
      )) as unknown as DuckDBConnection;
      await warehouse.runSQL("CREATE TABLE t AS SELECT 1 AS a");
      writePackageFiles(path.join(envPath, "pkg"), MODELS);
   });

   afterEach(async () => {
      await env?.closeAllConnections().catch(() => {});
      restoreSettings();
      fs.rmSync(rootDir, { recursive: true, force: true });
   });

   function controller(): PackageController {
      const store = {
         getEnvironment: async () => env,
      } as unknown as EnvironmentStore;
      return new PackageController(store, testRetrieval(store));
   }

   it("a REST reload sees a new column, and a watch-mode reload does not", async () => {
      await env.addPackage("pkg");
      expect(served(await current())).toEqual(all(["a"]));

      await warehouse.runSQL("ALTER TABLE t ADD COLUMN b INTEGER");
      // The watcher's reload: the cached columns stay.
      await env.getPackage(pkgName, true);
      expect(served(await current())).toEqual(all(["a"]));

      // `GET …/packages/pkg?reload=true`.
      await controller().getPackage("testEnv", "pkg", true);
      expect(served(await current())).toEqual(all(["a", "b"]));

      // What the refresh fetched is cached again: a later watch-mode reload
      // keeps the new columns.
      await env.getPackage(pkgName, true);
      expect(served(await current())).toEqual(all(["a", "b"]));

      await warehouse.runSQL("ALTER TABLE t DROP COLUMN a");
      await controller().getPackage("testEnv", "pkg", true);
      expect(served(await current())).toEqual(all(["b"]));
   });

   it("a reload after the table is dropped fails, and the watch-mode reload after it does too", async () => {
      await env.addPackage("pkg");
      await warehouse.runSQL("DROP TABLE t");

      await expect(
         controller().getPackage("testEnv", "pkg", true),
      ).rejects.toThrow();
      // The failed refetch left no cached columns behind, so a watch-mode
      // reload does not compile against the dropped table's old ones.
      await expect(env.getPackage(pkgName, true)).rejects.toThrow();
      // The last good model still serves, as after any failed reload.
      expect(served(await current())).toEqual(all(["a"]));
   });

   it("MCP reload_package sees a changed column", async () => {
      await env.addPackage("pkg");
      await warehouse.runSQL("ALTER TABLE t ADD COLUMN b INTEGER");

      type Handler = (
         params: Record<string, unknown>,
      ) => Promise<{ isError?: boolean }>;
      let handler: Handler | undefined;
      registerReloadPackageTool(
         {
            registerTool: (_name: string, _config: unknown, h: Handler) => {
               handler = h;
            },
         } as never,
         controller(),
      );
      const result = await handler!({
         environmentName: "testEnv",
         packageName: "pkg",
      });
      expect(result.isError).toBeFalsy();
      expect(served(await current())).toEqual(all(["a", "b"]));
   });

   it("a publish sees a changed column, from a location and in place", async () => {
      await env.addPackage("pkg");
      await warehouse.runSQL("ALTER TABLE t ADD COLUMN b INTEGER");

      // A publish from a location (and a PATCH that reinstalls, and a
      // reload of a package installed from one) loads a staged tree.
      const source = path.join(rootDir, "source");
      writePackageFiles(source, MODELS);
      await env.installPackage("pkg", (stagingPath) =>
         fs.promises.cp(source, stagingPath, { recursive: true }),
      );
      expect(served(await current())).toEqual(all(["a", "b"]));

      // A publish of a directory already on disk (`POST …/packages` with no
      // location).
      await warehouse.runSQL("ALTER TABLE t ADD COLUMN c INTEGER");
      await env.unloadPackage("pkg");
      await env.addPackage("pkg", { refreshSchemas: true });
      expect(served(await current())).toEqual(all(["a", "b", "c"]));
   });

   it("refreshes a table another package reads too", async () => {
      writePackageFiles(path.join(envPath, "other"), MODELS, {
         name: "other",
      });
      await env.addPackage("pkg");
      await env.addPackage("other");
      await warehouse.runSQL("ALTER TABLE t ADD COLUMN b INTEGER");

      await controller().getPackage("testEnv", "pkg", true);
      // The other package's served model is not recompiled, but its next
      // load, even a watch-mode one, reads the refetched columns.
      const other = parsePackageName("other");
      expect(served(await env.getPackage(other, false))).toEqual(all(["a"]));
      await env.getPackage(other, true);
      expect(served(await env.getPackage(other, false))).toEqual(
         all(["a", "b"]),
      );
   });
});

describe("publishing a version refetches table schemas", () => {
   usePackageLoadPool();
   const ENV_ID = "env-1";
   let storage: Awaited<ReturnType<typeof openTempStorage>>;
   let env: Environment;
   let restoreSettings: () => void;

   beforeEach(async () => {
      restoreSettings = installSettings({
         env: { PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true" },
      }).restore;
      storage = await openTempStorage("publisher-refetch-versions-", {
         id: ENV_ID,
         name: "testEnv",
      });
      const repo = storage.repo;
      env = await Environment.create("testEnv", storage.envPath, [WAREHOUSE]);
      env.bindVersions(repo, ENV_ID, {
         downloaderFor: (_packageName, location) => (stagingPath) =>
            fs.promises.cp(location, stagingPath, { recursive: true }),
      });
   });

   afterEach(async () => {
      await env?.closeAllConnections().catch(() => {});
      restoreSettings();
      await disposeTempStorage(storage);
   });

   async function publish(version: string, refreshSchemas: boolean) {
      const location = path.join(storage.root, "sources", version);
      writePackageFiles(location, MODELS, {
         manifest: { name: "pkg", version },
      });
      const published = await env
         .getVersionService()!
         .publish(
            "pkg",
            (stagingPath) =>
               fs.promises.cp(location, stagingPath, { recursive: true }),
            {
               sourceLocation: location,
               promotion: "on-publish",
               ...(refreshSchemas ? { refreshSchemas } : {}),
            },
         );
      return published.loaded;
   }

   it("a request's publish sees a changed column; a configured package's boot publish does not", async () => {
      const warehouse = (await env.getMalloyConnection(
         "warehouse",
      )) as unknown as DuckDBConnection;
      await warehouse.runSQL("CREATE TABLE t AS SELECT 1 AS a");
      expect(served(await publish("1.0.0", false))).toEqual(all(["a"]));

      await warehouse.runSQL("ALTER TABLE t ADD COLUMN b INTEGER");
      // As the boot publish of a configured package: the cache is kept.
      expect(served(await publish("1.0.1", false))).toEqual(all(["a"]));
      // As `POST …/packages` publishes (PackageController.publishVersion).
      expect(served(await publish("1.0.2", true))).toEqual(all(["a", "b"]));
   });
});
