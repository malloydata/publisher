// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import type { Environment } from "./environment";
import { EnvironmentStore } from "./environment_store";
import { installSettings } from "../test_helpers/settings_env";
import type { VersionService } from "./versions/version_service";

/**
 * A publish of a version is a load in flight from its download until it
 * settles, as an install is: `/status?includeLoading=true` reports it as
 * loading, never as a failed load, and once its tree names the version, as a
 * version entry an orchestrator can match by its `versionId`. Under explicit
 * promotion a read that took the package for one not loaded and loaded it
 * itself found a version with no `latest`, and recorded that as a failure.
 */

const ENV = "org";

let restoreSettings: () => void;

let root: string;
let store: EnvironmentStore | undefined;

beforeEach(() => {
   root = fs.realpathSync(
      fs.mkdtempSync(path.join(os.tmpdir(), "status-during-publish-")),
   );
   fs.mkdirSync(path.join(root, "server"));
   // The environment needs one package to load from the config.
   fs.writeFileSync(
      path.join(root, "server", "publisher.config.json"),
      JSON.stringify({
         frozenConfig: false,
         environments: [
            {
               name: ENV,
               packages: [
                  { name: "anchor", location: source("anchor", "1.0.0") },
               ],
               connections: [],
            },
         ],
      }),
   );
   // Versioning on, explicit promotion, and no other variables.
   restoreSettings = installSettings({
      env: {
         PUBLISHER_PACKAGE_VERSIONING: "on",
         PUBLISHER_VERSION_PROMOTION: "explicit",
      },
      cwd: path.join(root, "server"),
   }).restore;
});

afterEach(async () => {
   if (store) {
      for (const environment of store.getLoadedEnvironments()) {
         await environment.closeAllConnections();
      }
      const internals = store as unknown as {
         storageManager: { close(): Promise<void> };
      };
      await internals.storageManager.close();
      store = undefined;
   }
   restoreSettings();
   fs.rmSync(root, { recursive: true, force: true });
});

function source(name: string, version: string): string {
   const dir = path.join(root, "sources", name);
   fs.mkdirSync(dir, { recursive: true });
   fs.writeFileSync(
      path.join(dir, "publisher.json"),
      JSON.stringify({ name, version }),
   );
   fs.writeFileSync(
      path.join(dir, "model.malloy"),
      `source: numbers is duckdb.sql("SELECT 1 AS answer")\n`,
   );
   return dir;
}

async function open(): Promise<{
   environment: Environment;
   versions: VersionService<unknown>;
}> {
   store = new EnvironmentStore(path.join(root, "server"));
   await store.finishedInitialization;
   const environment = await store.getEnvironment(ENV, false);
   const versions = environment.getVersionService();
   if (!versions) throw new Error("versioning is not bound");
   return {
      environment,
      versions: versions as unknown as VersionService<unknown>,
   };
}

/** A promise and the function that settles it. */
function gate(): { wait: Promise<void>; open: () => void } {
   let open!: () => void;
   const wait = new Promise<void>((resolve) => {
      open = resolve;
   });
   return { wait, open };
}

/** Publish `sales` 1.0.0, with `beforeDownload` run inside the download. */
function publishSales(
   versions: VersionService<unknown>,
   beforeDownload: () => Promise<void> = async () => {},
) {
   const dir = source("sales", "1.0.0");
   return versions.publish(
      "sales",
      async (staging) => {
         await beforeDownload();
         await fs.promises.cp(dir, staging, { recursive: true });
      },
      { sourceLocation: dir, promotion: "explicit" },
   );
}

/** Hold the registry commit of every publish until `held.open()`. */
function holdCommits(versions: VersionService<unknown>) {
   const registry = (
      versions as unknown as {
         registry: { commitPublish: (...args: unknown[]) => Promise<unknown> };
      }
   ).registry;
   const original = registry.commitPublish.bind(registry);
   const held = gate();
   const reached = gate();
   registry.commitPublish = async (...args: unknown[]) => {
      reached.open();
      await held.wait;
      return original(...args);
   };
   return { held, reached };
}

type StatusPackage = {
   name?: string;
   versionId?: string | null;
   loaded?: boolean;
   status?: { serving?: boolean; loading?: boolean; loadingSince?: string };
};

async function salesEntries(includeLoading: boolean) {
   const status = await store!.getStatus({ includeLoading });
   const environment = status.environments.find((e) => e.name === ENV);
   const packages = (environment?.packages ?? []) as StatusPackage[];
   return {
      entries: packages.filter((p) => p.name === "sales"),
      loadErrors: status.loadErrors,
   };
}

describe("/status while a version is published", () => {
   it("never records a failed load for a package whose version finished compiling but is not yet committed", async () => {
      const { environment, versions } = await open();

      // Hold the version's compile, so a read can take the package for a load
      // in flight.
      const compile = gate();
      const internals = environment as unknown as {
         loadVersionPackage: (...args: unknown[]) => Promise<unknown>;
      };
      const loadVersionPackage = internals.loadVersionPackage.bind(environment);
      const compiling = gate();
      internals.loadVersionPackage = async (...args: unknown[]) => {
         compiling.open();
         await compile.wait;
         return loadVersionPackage(...args);
      };
      const { held, reached } = holdCommits(versions);

      const published = publishSales(versions);
      await compiling.wait;

      // The read asks whether the package has versions while it compiles (it
      // has none yet), and gets the answer only once the compile is done and
      // the publish waits on its commit: the moment the bug saw.
      const answer = gate();
      const asked = gate();
      const isVersioned = versions.isVersioned.bind(versions);
      versions.isVersioned = async (packageName: string) => {
         const result = await isVersioned(packageName);
         if (packageName === "sales") {
            asked.open();
            await answer.wait;
         }
         return result;
      };
      const read = store!.getStatus({ includeLoading: true });
      await asked.wait;
      compile.open();
      await reached.wait;
      answer.open();
      // Without the publish held as a load, the read now loads the package
      // itself and waits on the publish's lock; let the publish finish.
      await new Promise((resolve) => setTimeout(resolve, 50));
      held.open();
      await read;
      await published;

      expect(environment.getFailedPackages().has("sales")).toBe(false);
      const after = await salesEntries(true);
      expect(after.loadErrors).toBeUndefined();
   });

   it("never records a failed load for a package whose download fails while a read lists it", async () => {
      const { environment, versions } = await open();
      const downloading = gate();
      const fail = gate();
      const published = publishSales(versions, async () => {
         downloading.open();
         await fail.wait;
         throw new Error("download failed");
      });
      await downloading.wait;

      // The read takes the package for a load in flight, and gets its answer
      // to whether it has versions only after the download has failed.
      const answer = gate();
      const asked = gate();
      const isVersioned = versions.isVersioned.bind(versions);
      versions.isVersioned = async (packageName: string) => {
         const result = await isVersioned(packageName);
         if (packageName === "sales") {
            asked.open();
            await answer.wait;
         }
         return result;
      };
      const read = store!.getStatus({ includeLoading: true });
      await asked.wait;
      fail.open();
      await expect(published).rejects.toThrow("download failed");
      answer.open();
      await read;

      expect(environment.getFailedPackages().has("sales")).toBe(false);
      const after = await salesEntries(true);
      expect(after.entries).toHaveLength(0);
      expect(after.loadErrors).toBeUndefined();
   });

   it("lists the package as loading while its download runs", async () => {
      const { versions } = await open();
      const downloading = gate();
      const download = gate();
      const published = publishSales(versions, async () => {
         downloading.open();
         await download.wait;
      });
      await downloading.wait;

      const loading = await salesEntries(true);
      expect(loading.entries).toHaveLength(1);
      expect(loading.entries[0].status?.loading).toBe(true);
      expect(loading.entries[0].status?.serving).toBe(false);
      // A plain read lists only what can serve.
      expect((await salesEntries(false)).entries).toHaveLength(0);

      download.open();
      await published;
   });

   it("lists a version waiting on its commit as loading, by its versionId, and as served once committed", async () => {
      const { versions } = await open();
      const { held, reached } = holdCommits(versions);
      const published = publishSales(versions);
      await reached.wait;

      const loading = await salesEntries(true);
      expect(loading.entries).toHaveLength(1);
      expect(loading.entries[0]).toMatchObject({
         versionId: "1.0.0",
         loaded: false,
         status: { serving: false, loading: true },
      });
      expect(loading.entries[0].status?.loadingSince).toBeString();
      expect(loading.loadErrors).toBeUndefined();
      expect((await salesEntries(false)).entries).toHaveLength(0);

      held.open();
      await published;

      const served = await salesEntries(true);
      expect(served.entries).toHaveLength(1);
      expect(served.entries[0]).toMatchObject({
         versionId: "1.0.0",
         loaded: true,
         status: { serving: true, loading: false },
      });
      expect(served.loadErrors).toBeUndefined();
      expect(versions.publishingVersions()).toEqual([]);
   });
});
