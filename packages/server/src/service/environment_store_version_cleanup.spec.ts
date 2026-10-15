// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { PackageController } from "../controller/package.controller";
import { EnvironmentStore } from "./environment_store";

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

const ENV_VARS = [
   "PUBLISHER_PACKAGE_VERSIONING",
   "PUBLISHER_VERSION_PROMOTION",
   "INITIALIZE_STORAGE",
   "PUBLISHER_CONFIG_PATH",
   "PUBLISHER_USE_BUNDLED_DEFAULT",
] as const;
const savedEnv = new Map<string, string | undefined>();

let root: string;
let store: EnvironmentStore | undefined;

beforeEach(() => {
   for (const name of ENV_VARS) {
      savedEnv.set(name, process.env[name]);
      delete process.env[name];
   }
   process.env.PUBLISHER_PACKAGE_VERSIONING = "on";
   root = fs.realpathSync(
      fs.mkdtempSync(path.join(os.tmpdir(), "version-startup-cleanup-")),
   );
   fs.mkdirSync(path.join(root, "server"));
});

afterEach(async () => {
   await closeStore();
   for (const [name, value] of savedEnv) {
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
   }
   fs.rmSync(root, { recursive: true, force: true });
});

async function openStore(): Promise<EnvironmentStore> {
   store = new EnvironmentStore(path.join(root, "server"));
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
   fs.mkdirSync(dir, { recursive: true });
   fs.writeFileSync(
      path.join(dir, "publisher.json"),
      JSON.stringify({ name: PKG, version }),
   );
   fs.writeFileSync(
      path.join(dir, "model.malloy"),
      `source: numbers is duckdb.sql("SELECT 1 AS answer")\n`,
   );
   return dir;
}

describe("startup cleanup of version folders", () => {
   it("keeps a build-metadata version's folder and removes a folder no version owns", async () => {
      const first = await openStore();
      await first.addEnvironment({ name: ENV, connections: [] } as never);
      const packages = new PackageController(first);
      await packages.addPackage(ENV, {
         name: PKG,
         location: source("1.0.0+b"),
      });
      await packages.addPackage(ENV, { name: PKG, location: source("2.0.0") });
      const envPath = (await first.getEnvironment(ENV)).getEnvironmentPath();
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
      const environment = await restarted.getEnvironment(ENV);
      const served = await environment.getPackage(PKG, false, {
         versionId: "1.0.0+b",
      });
      expect(served.getPackageMetadata().versionId).toBe("1.0.0+b");
      expect(served.getPackagePath()).toBe(path.join(packagePath, "1.0.0_b"));
   });
});
