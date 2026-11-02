// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Lives under `tests/unit/` (not `src/`) on purpose: it builds a real
// EnvironmentStore over the real StorageManager, which sibling specs under
// `src/` replace with `mock.module(...)` (see environment_store_version_cleanup.spec.ts).

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { EnvironmentStore } from "../../../src/service/environment_store";

/**
 * With package versioning on, a package in publisher.config.json loads as a
 * publish of its location: the version its publisher.json declares, by the
 * same rules as `POST .../packages`. The config is read on a first boot (and
 * on `--init`); a later boot restores what was published. A watched
 * environment, and a server with versioning off, load configured packages as
 * before, with no versions.
 */

const ENV = "org";

const ENV_VARS = [
   "PUBLISHER_PACKAGE_VERSIONING",
   "PUBLISHER_VERSION_PROMOTION",
   "PUBLISHER_WATCH",
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
   root = fs.realpathSync(
      fs.mkdtempSync(path.join(os.tmpdir(), "config-versions-")),
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

/** A package source; `version` undefined writes a publisher.json without one. */
function source(name: string, version?: string): string {
   const dir = path.join(root, "sources", name);
   fs.mkdirSync(dir, { recursive: true });
   fs.writeFileSync(
      path.join(dir, "publisher.json"),
      JSON.stringify(version ? { name, version } : { name }),
   );
   fs.writeFileSync(
      path.join(dir, "model.malloy"),
      `source: numbers is duckdb.sql("SELECT 1 AS answer")\n`,
   );
   return dir;
}

/** publisher.config.json in the server root, naming these packages. */
function configure(packages: { name: string; location: string }[]) {
   fs.writeFileSync(
      path.join(root, "server", "publisher.config.json"),
      JSON.stringify({
         frozenConfig: false,
         environments: [{ name: ENV, packages, connections: [] }],
      }),
   );
}

async function versionsOf(s: EnvironmentStore, packageName: string) {
   const environment = await s.getEnvironment(ENV, false);
   const versions = environment.getVersionService();
   if (!versions) return { latest: null, ids: [] as string[] };
   const listed = await versions.listVersions(packageName);
   return {
      latest: listed.latest,
      ids: listed.versions.map((v) => v.versionId),
   };
}

describe("configured packages with package versioning on", () => {
   it("publishes each as the version its publisher.json declares, and restores it on a later boot", async () => {
      process.env.PUBLISHER_PACKAGE_VERSIONING = "on";
      configure([{ name: "sales", location: source("sales", "1.2.0") }]);

      const first = await openStore();
      expect(await versionsOf(first, "sales")).toEqual({
         latest: "1.2.0",
         ids: ["1.2.0"],
      });
      const environment = await first.getEnvironment(ENV, false);
      const served = await environment.getPackage("sales", false);
      expect(served.getVersionId()).toBe("1.2.0");
      await closeStore();

      // A later boot reads the database, not the config: the version is
      // restored, not published again.
      const second = await openStore();
      expect(await versionsOf(second, "sales")).toEqual({
         latest: "1.2.0",
         ids: ["1.2.0"],
      });
   });

   it("records a package whose publisher.json declares no version as its load error, and loads the others", async () => {
      process.env.PUBLISHER_PACKAGE_VERSIONING = "on";
      configure([
         { name: "plain", location: source("plain") },
         { name: "sales", location: source("sales", "1.0.0") },
      ]);

      const s = await openStore();
      expect((await versionsOf(s, "sales")).ids).toEqual(["1.0.0"]);
      const environment = await s.getEnvironment(ENV, false);
      const failed = environment.getFailedPackages();
      expect(failed.has("plain")).toBe(true);
      expect(String(failed.get("plain"))).toContain("version");
   });

   it("under explicit promotion publishes with no latest, as a publish does", async () => {
      process.env.PUBLISHER_PACKAGE_VERSIONING = "on";
      process.env.PUBLISHER_VERSION_PROMOTION = "explicit";
      configure([{ name: "sales", location: source("sales", "1.0.0") }]);

      const s = await openStore();
      expect(await versionsOf(s, "sales")).toEqual({
         latest: null,
         ids: ["1.0.0"],
      });
   });

   it("loads a watched environment's packages in place, with no versions", async () => {
      process.env.PUBLISHER_PACKAGE_VERSIONING = "on";
      process.env.PUBLISHER_WATCH = ENV;
      configure([{ name: "sales", location: source("sales", "1.0.0") }]);

      const s = await openStore();
      expect((await versionsOf(s, "sales")).ids).toEqual([]);
      const environment = await s.getEnvironment(ENV, false);
      const served = await environment.getPackage("sales", false);
      expect(served.getVersionId()).toBeUndefined();
   });
});

describe("configured packages with package versioning off", () => {
   it("loads them as before, with no versions, whatever their publisher.json says", async () => {
      configure([{ name: "sales", location: source("sales", "1.0.0") }]);

      const s = await openStore();
      expect((await versionsOf(s, "sales")).ids).toEqual([]);
      const environment = await s.getEnvironment(ENV, false);
      const served = await environment.getPackage("sales", false);
      expect(served.getVersionId()).toBeUndefined();
   });
});
