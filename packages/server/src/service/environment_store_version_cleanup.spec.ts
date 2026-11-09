// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { PackageController } from "../controller/package.controller";
import { EnvironmentStore } from "./environment_store";
import { installSettings } from "../test_helpers/settings_env";
import { writePackageFiles } from "../test_helpers/temp_package";
import { parseEnvironmentName, parsePackageName, VersionRef } from "./identity";
import { startedStore } from "../test_helpers/started_store";
import { testRetrieval } from "../test_helpers/test_retrieval";

/**
 * The startup cleanup of version folders, as a restart runs it: an
 * environment reloaded from the database binds its version registry, which
 * hands VersionStore.cleanup the folders the registry rows own, keyed by each
 * row's folder name (`dir_name`), before anything of it loads.
 *
 *  - A version with build metadata lives in a folder whose name is not its
 *    version (`1.0.0+b` in `1.0.0_b`); its folder is kept.
 *  - A version folder no row owns (a placement whose publish never
 *    committed) is removed.
 *
 * Each published source is deleted before the restart, so a folder the
 * cleanup removed by mistake could not be fetched back again unnoticed.
 */

const ENV = "org";
const PKG = "sales";

let restoreSettings: (() => void) | undefined;

let root: string;
let store: EnvironmentStore | undefined;

beforeEach(() => {
   root = fs.realpathSync(
      fs.mkdtempSync(path.join(os.tmpdir(), "version-startup-cleanup-")),
   );
   fs.mkdirSync(path.join(root, "server"));
});

afterEach(async () => {
   await closeStore();
   restoreSettings?.();
   restoreSettings = undefined;
   fs.rmSync(root, { recursive: true, force: true });
});

/** Start a store as a boot does, with no variables set. */
async function openStore(): Promise<EnvironmentStore> {
   restoreSettings?.();
   restoreSettings = installSettings({
      env: {},
      cwd: path.join(root, "server"),
   }).restore;
   store = startedStore(new EnvironmentStore(path.join(root, "server")));
   await store.finishedInitialization;
   return store;
}

async function closeStore() {
   if (!store) return;
   for (const environment of store.getLoadedEnvironments()) {
      await environment.closeAllConnections();
   }
   const internals = store as unknown as {
      storageManager: { close(): Promise<void> };
   };
   await internals.storageManager.close();
   store = undefined;
}

/** A package source declaring `version`, to publish from. */
function source(version: string): string {
   const dir = path.join(root, "sources", version);
   writePackageFiles(
      dir,
      {
         "model.malloy": `source: numbers is duckdb.sql("SELECT 1 AS answer")\n`,
      },
      { manifest: { name: PKG, version } },
   );
   return dir;
}

describe("startup cleanup of version folders", () => {
   it("keeps a build-metadata version's folder and removes a folder no version owns", async () => {
      const first = await openStore();
      await first.addEnvironment({ name: ENV, connections: [] } as never);
      const packages = new PackageController(first, testRetrieval(first));
      await packages.addPackage(ENV, {
         name: PKG,
         location: source("1.0.0+b"),
      });
      await packages.addPackage(ENV, { name: PKG, location: source("2.0.0") });
      const envPath = (
         await first.getEnvironment(parseEnvironmentName(ENV))
      ).getEnvironmentPath();
      const packagePath = path.join(envPath, PKG);
      expect(fs.readdirSync(packagePath).sort()).toEqual(["1.0.0_b", "2.0.0"]);
      const buildFolder = fs.statSync(path.join(packagePath, "1.0.0_b")).ino;
      await closeStore();

      // Left behind by publishes that never committed: one plain, one with
      // build metadata. Neither has a row.
      for (const orphan of ["3.0.0", "1.0.0_c"]) {
         fs.mkdirSync(path.join(packagePath, orphan));
         fs.writeFileSync(
            path.join(packagePath, orphan, "publisher.json"),
            JSON.stringify({ name: PKG }),
         );
      }
      fs.rmSync(path.join(root, "sources"), { recursive: true, force: true });

      // The restart: the environment comes back from the database.
      const restarted = await openStore();
      expect(fs.readdirSync(packagePath).sort()).toEqual(["1.0.0_b", "2.0.0"]);
      // The same folder, not one fetched again.
      expect(fs.statSync(path.join(packagePath, "1.0.0_b")).ino).toBe(
         buildFolder,
      );
      const environment = await restarted.getEnvironment(
         parseEnvironmentName(ENV),
      );
      const served = await environment.getPackage(
         parsePackageName(PKG),
         false,
         {
            versionId: VersionRef.parse("1.0.0+b"),
         },
      );
      expect(served.getPackageMetadata().versionId).toBe("1.0.0+b");
      expect(served.getPackagePath()).toBe(path.join(packagePath, "1.0.0_b"));
   });
});
