// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { EnvironmentStore } from "./environment_store";
import { installSettings } from "../test_helpers/settings_env";
import { writePackageFiles } from "../test_helpers/temp_package";
import { parseEnvironmentName, parsePackageName } from "./identity";
import { startedStore } from "../test_helpers/started_store";

/**
 * A package in publisher.config.json loads as a publish of its location: the
 * version its publisher.json declares, by the same rules as
 * `POST .../packages`. The config is read on a first boot (and on `--init`);
 * a later boot restores what was published. An environment created at
 * runtime publishes the packages its body names the same way. A watched
 * environment loads its packages in place, with no versions.
 */

const ENV = "org";

/** The variables a case sets; the store starts with them and no others. */
let env: Record<string, string>;
let restoreSettings: (() => void) | undefined;

let root: string;
let store: EnvironmentStore | undefined;

beforeEach(() => {
   env = {};
   root = fs.realpathSync(
      fs.mkdtempSync(path.join(os.tmpdir(), "config-versions-")),
   );
   fs.mkdirSync(path.join(root, "server"));
});

afterEach(async () => {
   await closeStore();
   restoreSettings?.();
   restoreSettings = undefined;
   fs.rmSync(root, { recursive: true, force: true });
});

/** Start a store as a boot does: settings read from `env` and the config, then the store. */
async function openStore(): Promise<EnvironmentStore> {
   restoreSettings?.();
   restoreSettings = installSettings({
      env,
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

/** A package source; `version` undefined writes a publisher.json without one. */
function source(name: string, version?: string): string {
   const dir = path.join(root, "sources", name);
   writePackageFiles(
      dir,
      {
         "model.malloy": `source: numbers is duckdb.sql("SELECT 1 AS answer")\n`,
      },
      { manifest: version ? { name, version } : { name } },
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
   const environment = await s.getEnvironment(parseEnvironmentName(ENV), false);
   const versions = environment.getVersionService();
   if (!versions) return { latest: null, ids: [] as string[] };
   const listed = await versions.listVersions(packageName);
   return {
      latest: listed.latest,
      ids: listed.versions.map((v) => v.versionId),
   };
}

describe("configured packages", () => {
   it("publishes each as the version its publisher.json declares, and restores it on a later boot", async () => {
      configure([{ name: "sales", location: source("sales", "1.2.0") }]);

      const first = await openStore();
      expect(await versionsOf(first, "sales")).toEqual({
         latest: "1.2.0",
         ids: ["1.2.0"],
      });
      const environment = await first.getEnvironment(
         parseEnvironmentName(ENV),
         false,
      );
      const served = await environment.getPackage(
         parsePackageName("sales"),
         false,
      );
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

   it("publishes a package whose location is relative to the config, as the bundled examples' config names them, and restores it from there", async () => {
      source("sales", "1.2.0");
      configure([{ name: "sales", location: "../sources/sales" }]);

      const first = await openStore();
      expect(await versionsOf(first, "sales")).toEqual({
         latest: "1.2.0",
         ids: ["1.2.0"],
      });
      const environment = await first.getEnvironment(
         parseEnvironmentName(ENV),
         false,
      );
      expect(environment.getFailedPackages().has("sales")).toBe(false);
      await closeStore();

      // A version whose tree is gone is fetched again from its recorded
      // source location, so that location has to name the config's folder:
      // left relative, the fetch would stage nothing.
      fs.rmSync(path.join(root, "server", "publisher_data", ENV, "sales"), {
         recursive: true,
         force: true,
      });
      const second = await openStore();
      const restored = await (
         await second.getEnvironment(parseEnvironmentName(ENV), false)
      ).getPackage(parsePackageName("sales"), false);
      expect(restored.getVersionId()).toBe("1.2.0");
   });

   it("records a package whose publisher.json declares no version as its load error, and loads the others", async () => {
      configure([
         { name: "plain", location: source("plain") },
         { name: "sales", location: source("sales", "1.0.0") },
      ]);

      const s = await openStore();
      expect((await versionsOf(s, "sales")).ids).toEqual(["1.0.0"]);
      const environment = await s.getEnvironment(
         parseEnvironmentName(ENV),
         false,
      );
      const failed = environment.getFailedPackages();
      expect(failed.has("plain")).toBe(true);
      expect(String(failed.get("plain"))).toContain("version");
   });

   it("under explicit promotion publishes with no latest, as a publish does", async () => {
      env.PUBLISHER_VERSION_PROMOTION = "explicit";
      configure([{ name: "sales", location: source("sales", "1.0.0") }]);

      const s = await openStore();
      expect(await versionsOf(s, "sales")).toEqual({
         latest: null,
         ids: ["1.0.0"],
      });
   });

   it("loads a watched environment's packages in place, with no versions", async () => {
      env.PUBLISHER_WATCH = ENV;
      configure([{ name: "sales", location: source("sales", "1.0.0") }]);

      const s = await openStore();
      expect((await versionsOf(s, "sales")).ids).toEqual([]);
      const environment = await s.getEnvironment(
         parseEnvironmentName(ENV),
         false,
      );
      const served = await environment.getPackage(
         parsePackageName("sales"),
         false,
      );
      expect(served.getVersionId()).toBeUndefined();
   });
});

describe("an environment created at runtime", () => {
   it("publishes each package its body names as a version, recording one with no version as its load error", async () => {
      // No config: the environment comes only from the request body, as
      // `POST /environments` sends it.
      const s = await openStore();
      await s.addEnvironment({
         name: ENV,
         packages: [
            { name: "plain", location: source("plain") },
            { name: "sales", location: source("sales", "1.0.0") },
         ],
         connections: [],
      } as never);

      expect(await versionsOf(s, "sales")).toEqual({
         latest: "1.0.0",
         ids: ["1.0.0"],
      });
      const environment = await s.getEnvironment(
         parseEnvironmentName(ENV),
         false,
      );
      const served = await environment.getPackage(
         parsePackageName("sales"),
         false,
      );
      expect(served.getVersionId()).toBe("1.0.0");
      const failed = environment.getFailedPackages();
      expect(failed.has("plain")).toBe(true);
      expect(String(failed.get("plain"))).toContain("version");
   });
});
