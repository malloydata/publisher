// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { EnvironmentStore } from "./environment_store";
import { installSettings } from "../test_helpers/settings_env";
import { writePackageFiles } from "../test_helpers/temp_package";
import { parseEnvironmentName, parsePackageName, VersionRef } from "./identity";
import { startedStore } from "../test_helpers/started_store";

/**
 * With package versioning on, a package in publisher.config.json loads as a
 * publish of its location: the version its publisher.json declares, by the
 * same rules as `POST .../packages`. The config is read on a first boot (and
 * on `--init`); a later boot restores what was published. A watched
 * environment, and a server with versioning off, load configured packages as
 * before, with no versions.
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

describe("configured packages with package versioning on", () => {
   it("publishes each as the version its publisher.json declares, and restores it on a later boot", async () => {
      env.PUBLISHER_PACKAGE_VERSIONING = "on";
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
      env.PUBLISHER_PACKAGE_VERSIONING = "on";
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
      env.PUBLISHER_PACKAGE_VERSIONING = "on";
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
      env.PUBLISHER_PACKAGE_VERSIONING = "on";
      env.PUBLISHER_VERSION_PROMOTION = "explicit";
      configure([{ name: "sales", location: source("sales", "1.0.0") }]);

      const s = await openStore();
      expect(await versionsOf(s, "sales")).toEqual({
         latest: null,
         ids: ["1.0.0"],
      });
   });

   it("loads a watched environment's packages in place, with no versions", async () => {
      env.PUBLISHER_PACKAGE_VERSIONING = "on";
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

describe("configured packages with package versioning off", () => {
   it("loads them as before, with no versions, whatever their publisher.json says", async () => {
      env.PUBLISHER_PACKAGE_VERSIONING = "off";
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

/**
 * A configured package whose boot-time publish is refused places nothing, so
 * it used to have no files at all: compiling it, building it and reading it all
 * answered "manifest does not exist", and a failure that clears on its own
 * could never recover until `--init`. Its next access publishes it again.
 */
describe("a configured package whose boot publish was refused", () => {
   function writeModel(dir: string, text: string) {
      fs.writeFileSync(path.join(dir, "model.malloy"), text);
   }

   it("answers its compile error when read again, not a missing manifest", async () => {
      env.PUBLISHER_PACKAGE_VERSIONING = "on";
      const dir = source("broken", "1.0.0");
      writeModel(dir, `source: numbers is duckdb.table('no_such_table_xyz')\n`);
      configure([{ name: "broken", location: dir }]);

      const s = await openStore();
      expect(await versionsOf(s, "broken")).toEqual({ latest: null, ids: [] });
      const environment = await s.getEnvironment(
         parseEnvironmentName(ENV),
         false,
      );
      const read = environment.getPackage(parsePackageName("broken"), false);
      await expect(read).rejects.toThrow(/no_such_table_xyz/);
      await expect(read).rejects.not.toThrow(/manifest/i);

      const compiled = environment.compileSource(
         "broken",
         "model.malloy",
         "",
         false,
         undefined,
         "file",
      );
      await expect(compiled).rejects.toThrow(/no_such_table_xyz/);
   });

   it("is published as its version once the cause clears, with no restart", async () => {
      env.PUBLISHER_PACKAGE_VERSIONING = "on";
      const dir = source("late", "1.0.0");
      writeModel(dir, `source: numbers is duckdb.table('late_table_xyz')\n`);
      configure([{ name: "late", location: dir }]);

      const s = await openStore();
      expect(await versionsOf(s, "late")).toEqual({ latest: null, ids: [] });
      const environment = await s.getEnvironment(
         parseEnvironmentName(ENV),
         false,
      );
      await expect(
         environment.getPackage(parsePackageName("late"), false),
      ).rejects.toThrow(/late_table_xyz/);

      // What the model needed now exists: here, the model no longer reads the
      // missing table, as a warehouse table created after boot would.
      writeModel(dir, `source: numbers is duckdb.sql("SELECT 1 AS answer")\n`);
      const served = await environment.getPackage(
         parsePackageName("late"),
         false,
      );
      expect(served.getVersionId()).toBe("1.0.0");
      expect(await versionsOf(s, "late")).toEqual({
         latest: "1.0.0",
         ids: ["1.0.0"],
      });
      expect(environment.getFailedPackages().has("late")).toBe(false);
   });
});

/**
 * A retry copies the package and compiles it, from unauthenticated routes, so
 * it is shared, backed off and gated to the default version, and a delete
 * waits for one in flight rather than being undone by it.
 */
describe("retrying a refused configured package's publish", () => {
   async function environmentWithRetry(
      publish: () => Promise<unknown>,
      fingerprint?: () => Promise<string | undefined>,
   ) {
      env.PUBLISHER_PACKAGE_VERSIONING = "on";
      configure([{ name: "ok", location: source("ok", "1.0.0") }]);
      const s = await openStore();
      const environment = await s.getEnvironment(
         parseEnvironmentName(ENV),
         false,
      );
      environment.setConfiguredPublishRetry("ghost", publish, fingerprint);
      return environment;
   }
   const ghost = parsePackageName("ghost");

   it("runs one publish for callers that arrive together, and answers each its error", async () => {
      let calls = 0;
      const environment = await environmentWithRetry(async () => {
         calls++;
         await new Promise((resolve) => setTimeout(resolve, 50));
         throw new Error("still broken: ghost_table");
      });
      const results = await Promise.allSettled([
         environment.getPackage(ghost, false),
         environment.getPackage(ghost, false),
         environment.publishRefusedConfiguredPackage("ghost"),
      ]);
      expect(calls).toBe(1);
      for (const result of results) {
         expect(result.status).toBe("rejected");
         expect(String((result as PromiseRejectedResult).reason)).toContain(
            "ghost_table",
         );
      }
   });

   it("answers the last failure without publishing again until the backoff passes", async () => {
      let calls = 0;
      const environment = await environmentWithRetry(async () => {
         calls++;
         throw new Error(`failure ${calls}`);
      });
      await expect(environment.getPackage(ghost, false)).rejects.toThrow(
         "failure 1",
      );
      await expect(environment.getPackage(ghost, false)).rejects.toThrow(
         "failure 1",
      );
      expect(calls).toBe(1);
      // The /status record is the retry's, as a load failure.
      expect(environment.getFailedPackages().get("ghost")).toContain(
         "failure 1",
      );
      const realNow = Date.now;
      try {
         Date.now = () => realNow() + 3_000;
         await expect(environment.getPackage(ghost, false)).rejects.toThrow(
            "failure 2",
         );
      } finally {
         Date.now = realNow;
      }
      expect(calls).toBe(2);
   });

   it("publishes again at once when the package's files changed", async () => {
      let calls = 0;
      let files = "a";
      const environment = await environmentWithRetry(
         async () => {
            calls++;
            throw new Error("broken");
         },
         async () => files,
      );
      await expect(environment.getPackage(ghost, false)).rejects.toThrow();
      await expect(environment.getPackage(ghost, false)).rejects.toThrow();
      expect(calls).toBe(1);
      files = "b";
      await expect(environment.getPackage(ghost, false)).rejects.toThrow();
      expect(calls).toBe(2);
   });

   it("publishes nothing for a request naming a version", async () => {
      let calls = 0;
      const environment = await environmentWithRetry(async () => {
         calls++;
         throw new Error("broken");
      });
      await expect(
         environment.getPackage(ghost, false, {
            versionId: VersionRef.of("9.9.9"),
         }),
      ).rejects.toThrow();
      await environment.publishRefusedConfiguredPackage("ghost", "9.9.9");
      expect(calls).toBe(0);
      // The materialization path, with the default resolution, does publish.
      await expect(
         environment.publishRefusedConfiguredPackage("ghost"),
      ).rejects.toThrow("broken");
      expect(calls).toBe(1);
   });

   it("answers a refused remote package's boot error on every read, and never publishes it", async () => {
      env.PUBLISHER_PACKAGE_VERSIONING = "on";
      configure([{ name: "ok", location: source("ok", "1.0.0") }]);
      const s = await openStore();
      const environment = await s.getEnvironment(
         parseEnvironmentName(ENV),
         false,
      );
      const bootError = new Error("remote boot refusal: ghost_table");
      environment.setConfiguredPublishRefused("ghost", bootError);
      const realNow = Date.now;
      try {
         for (const offset of [0, 3_600_000]) {
            Date.now = () => realNow() + offset;
            await expect(environment.getPackage(ghost, false)).rejects.toBe(
               bootError,
            );
            await expect(
               environment.publishRefusedConfiguredPackage("ghost"),
            ).rejects.toBe(bootError);
         }
      } finally {
         Date.now = realNow;
      }
      // A request naming a version still answers 404, not the boot error.
      await expect(
         environment.getPackage(ghost, false, {
            versionId: VersionRef.of("9.9.9"),
         }),
      ).rejects.not.toBe(bootError);
      // A delete forgets it.
      await environment.deletePackage(ghost).catch(() => {});
      await expect(environment.getPackage(ghost, false)).rejects.not.toBe(
         bootError,
      );
   });

   it("lets a delete land after a retry in flight, and starts none after it", async () => {
      const events: string[] = [];
      let release!: () => void;
      const gate = new Promise<void>((resolve) => (release = resolve));
      let calls = 0;
      let started!: () => void;
      const publishStarted = new Promise<void>(
         (resolve) => (started = resolve),
      );
      const environment = await environmentWithRetry(async () => {
         calls++;
         events.push("publish start");
         started();
         await gate;
         events.push("publish end");
         throw new Error("broken");
      });
      const read = environment.getPackage(ghost, false).catch(() => {});
      // Wait for the retry to be in flight, not a fixed time: on a slow runner
      // 10ms was not enough, the delete landed first, and (correctly) no retry
      // ran at all.
      await publishStarted;
      const deleted = environment
         .deletePackage(ghost)
         .then(() => events.push("delete done"));
      await new Promise((resolve) => setTimeout(resolve, 10));
      release();
      await Promise.all([read, deleted]);
      expect(events).toEqual(["publish start", "publish end", "delete done"]);
      await expect(environment.getPackage(ghost, false)).rejects.toThrow();
      expect(calls).toBe(1);
   });
});
