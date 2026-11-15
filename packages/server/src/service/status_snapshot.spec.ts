// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import * as os from "os";
import sinon from "sinon";
import type { Environment } from "./environment";
import { EnvironmentStore } from "./environment_store";
import { Package } from "./package";
import { installSettings } from "../test_helpers/settings_env";
import { writePackageFiles } from "../test_helpers/temp_package";
import { parseEnvironmentName, parsePackageName } from "./identity";
import { startedStore } from "../test_helpers/started_store";
import type { VersionService } from "./versions/version_service";

/**
 * The correctness proof for the `/status` snapshot (ms2data/service#7795).
 *
 * `/status` used to list every package on every poll: it built each copy's
 * full metadata, read every version row from the database, and loaded a
 * registered package that was not resident. It now answers from a snapshot
 * the environment keeps current as it changes. Across a sequence of loads,
 * reloads, binds, PATCHes, failed loads, stale reloads, unloads, version
 * publishes, promotions, archives and deletes, with loads held open
 * mid-flight, the snapshot's answer must equal, field for field, what the old
 * computation (`computeStatusFromListings`) gives at the same moment, with
 * inline fields on and off, and no poll may build a copy's metadata itself.
 */

const GOOD = `source: numbers is duckdb.sql("SELECT 1 AS answer")\n`;
const BROKEN = `source: numbers is duckdb.sql("SELECT 1 AS answer") extend { dimension: x is nope }\n`;

let root: string;
let store: EnvironmentStore | undefined;
let restoreSettings: (() => void) | undefined;

afterEach(async () => {
   sinon.restore();
   if (store) {
      for (const environment of store.getLoadedEnvironments()) {
         await environment.closeAllConnections();
      }
      await (
         store as unknown as { storageManager: { close(): Promise<void> } }
      ).storageManager.close();
      store = undefined;
   }
   restoreSettings?.();
   restoreSettings = undefined;
   if (root) fs.rmSync(root, { recursive: true, force: true });
});

function source(name: string, version?: string, model = GOOD): string {
   const dir = path.join(root, "sources", `${name}-${version ?? "dev"}`);
   fs.rmSync(dir, { recursive: true, force: true });
   writePackageFiles(
      dir,
      { "model.malloy": model },
      { manifest: { name, description: `${name} package`, version } },
   );
   return dir;
}

function manifestFile(): string {
   const file = path.join(root, "manifest.json");
   fs.writeFileSync(
      file,
      JSON.stringify({
         builtAt: new Date().toISOString(),
         strict: false,
         entries: {},
      }),
   );
   return file;
}

async function boot(
   environments: Array<{ name: string; packages: string[] }>,
   env: Record<string, string>,
): Promise<EnvironmentStore> {
   root = fs.realpathSync(
      fs.mkdtempSync(path.join(os.tmpdir(), "status-snapshot-")),
   );
   const serverRoot = path.join(root, "server");
   fs.mkdirSync(serverRoot);
   fs.writeFileSync(
      path.join(serverRoot, "publisher.config.json"),
      JSON.stringify({
         frozenConfig: false,
         environments: environments.map(({ name, packages }) => ({
            name,
            packages: packages.map((pkg) => ({
               name: pkg,
               location: source(pkg, "1.0.0"),
            })),
            connections: [],
         })),
      }),
   );
   restoreSettings = installSettings({ env, cwd: serverRoot }).restore;
   store = startedStore(new EnvironmentStore(serverRoot));
   await store.finishedInitialization;
   return store;
}

async function environmentOf(name: string): Promise<Environment> {
   return store!.getEnvironment(parseEnvironmentName(name), false);
}

/** The body without what differs between any two calls (the clock). */
function comparable(body: unknown): unknown {
   const parsed = JSON.parse(JSON.stringify(body)) as {
      timestamp?: number;
      environments: Array<{ name?: string }>;
   };
   delete parsed.timestamp;
   // The old computation pushes environments as their listings settle.
   parsed.environments.sort((a, b) =>
      (a.name ?? "").localeCompare(b.name ?? ""),
   );
   return parsed;
}

function misses(): number {
   return store!
      .getLoadedEnvironments()
      .reduce((sum, e) => sum + e.statusSnapshotMissesForTesting(), 0);
}

/** Wait out the loads a step left running in the background. */
async function settleLoads(): Promise<void> {
   for (const environment of store!.getLoadedEnvironments()) {
      const body = environment.describeForStatus({ includeLoading: true });
      await Promise.all(
         (body.packages ?? []).map((p) =>
            environment.awaitPackageLoads(p.name ?? ""),
         ),
      );
   }
}

/**
 * The snapshot answers what the old computation answers, building nothing.
 * `inFlight`: the step holds a load open on purpose, so none is waited for.
 */
async function expectSnapshotEqualsFresh(
   step: string,
   inFlight = false,
): Promise<void> {
   if (!inFlight) await settleLoads();
   for (const includeLoading of [false, true]) {
      const before = misses();
      const snapshot = await store!.getStatus({ includeLoading });
      expect({ step, includeLoading, misses: misses() - before }).toEqual({
         step,
         includeLoading,
         misses: 0,
      });
      const fresh = await store!.computeStatusFromListings({ includeLoading });
      expect({ step, includeLoading, body: comparable(snapshot) }).toEqual({
         step,
         includeLoading,
         body: comparable(fresh),
      });
   }
}

/** Hold every `Package.create` open until `release`. */
function holdCompile(): { entered: Promise<void>; release: () => void } {
   let entered!: () => void;
   const enteredP = new Promise<void>((r) => (entered = r));
   let release!: () => void;
   const gate = new Promise<void>((r) => (release = r));
   const original = Package.create.bind(Package);
   const stub = sinon
      .stub(Package, "create")
      .callsFake(async (...args: Parameters<typeof Package.create>) => {
         entered();
         await gate;
         return original(...args);
      });
   return {
      entered: enteredP,
      release: () => {
         stub.restore();
         release();
      },
   };
}

function copyFrom(dir: string) {
   return (staging: string) =>
      fs.promises.cp(dir, staging, { recursive: true });
}

for (const inline of ["on", "off"]) {
   describe(`/status snapshot, inline fields ${inline}`, () => {
      it("equals the listing computation through unversioned loads, binds, failures, stale reloads and unloads", async () => {
         await boot(
            [
               { name: "alpha", packages: ["a", "b"] },
               { name: "beta", packages: ["c"] },
            ],
            {
               PUBLISHER_PACKAGE_VERSIONING: "off",
               PUBLISHER_PACKAGE_INLINE_FIELDS: inline,
            },
         );
         const alpha = await environmentOf("alpha");
         const beta = await environmentOf("beta");
         await expectSnapshotEqualsFresh("boot");

         // A reload that fails to compile: `a` keeps serving, stale.
         fs.writeFileSync(
            path.join(alpha.getEnvironmentPath(), "a", "model.malloy"),
            BROKEN,
         );
         await expect(
            alpha.getPackage(parsePackageName("a"), true),
         ).rejects.toThrow();
         await expectSnapshotEqualsFresh("stale reload");

         fs.writeFileSync(
            path.join(alpha.getEnvironmentPath(), "a", "model.malloy"),
            GOOD,
         );
         await alpha.getPackage(parsePackageName("a"), true);
         await expectSnapshotEqualsFresh("fixed reload");

         // An install bound to a build manifest.
         const manifest = manifestFile();
         await alpha.installPackage("d", copyFrom(source("d")), undefined, {
            update: { manifestLocation: manifest },
         });
         await expectSnapshotEqualsFresh("install with a manifest bind");

         await alpha.updatePackage("d", {
            name: "d",
            description: "patched",
         });
         await expectSnapshotEqualsFresh("metadata PATCH");

         await alpha.updatePackage("d", { name: "d", manifestLocation: null });
         await expectSnapshotEqualsFresh("binding reverted to live");

         await alpha.updatePackage("d", {
            name: "d",
            manifestLocation: manifest,
         });
         await expectSnapshotEqualsFresh("rebound by PATCH");

         await alpha.unloadPackage("b");
         await expectSnapshotEqualsFresh("unload");

         // A first install that fails to load.
         await expect(
            alpha.installPackage("e", copyFrom(source("e", undefined, BROKEN))),
         ).rejects.toThrow();
         await expectSnapshotEqualsFresh("failed install");

         // A reload held mid-compile: the previous copy serves, loading.
         const reload = holdCompile();
         const reloading = alpha.getPackage(parsePackageName("a"), true);
         await reload.entered;
         await expectSnapshotEqualsFresh("reload in flight", true);
         reload.release();
         await reloading;
         await expectSnapshotEqualsFresh("reload settled");

         // A first install held mid-compile: listed only under includeLoading.
         const install = holdCompile();
         const installing = beta.installPackage("f", copyFrom(source("f")));
         await install.entered;
         await expectSnapshotEqualsFresh("first install in flight", true);
         install.release();
         await installing;
         await expectSnapshotEqualsFresh("first install settled");

         await beta.deletePackage("c");
         await expectSnapshotEqualsFresh("delete");

         await store!.deleteEnvironment("beta");
         await expectSnapshotEqualsFresh("environment deleted");
      });

      it("equals the listing computation through version publishes, promotions, archives and deletes", async () => {
         await boot([{ name: "org", packages: ["anchor"] }], {
            PUBLISHER_PACKAGE_VERSIONING: "on",
            PUBLISHER_VERSION_PROMOTION: "explicit",
            PUBLISHER_PACKAGE_INLINE_FIELDS: inline,
         });
         const org = await environmentOf("org");
         const versions = org.getVersionService() as VersionService<Package>;
         await expectSnapshotEqualsFresh("boot");

         const publish = (version: string, manifestLocation?: string) =>
            versions.publish("sales", copyFrom(source("sales", version)), {
               sourceLocation: source("sales", version),
               promotion: "explicit",
               ...(manifestLocation ? { manifestLocation } : {}),
            });

         await publish("1.0.0");
         await expectSnapshotEqualsFresh("first version, no latest");

         await versions.setLatest("sales", "1.0.0");
         await expectSnapshotEqualsFresh("made latest");

         await publish("1.1.0", manifestFile());
         await expectSnapshotEqualsFresh("second version bound");

         await versions.setLatest("sales", "1.1.0");
         await expectSnapshotEqualsFresh("latest moved");

         await versions.setArchiveStatus("sales", "1.0.0", "archive");
         await expectSnapshotEqualsFresh("archived");

         await versions.setArchiveStatus("sales", "1.0.0", "unarchive");
         await expectSnapshotEqualsFresh("unarchived, not loaded");

         await versions.getLoaded("sales", "1.0.0");
         await expectSnapshotEqualsFresh("loaded again by a read");

         // A publish held mid-compile: listed by its version under
         // includeLoading.
         const compile = holdCompile();
         const publishing = publish("1.2.0");
         await compile.entered;
         await expectSnapshotEqualsFresh("publish in flight", true);
         compile.release();
         await publishing;
         await expectSnapshotEqualsFresh("publish settled");

         await versions.deleteVersion("sales", "1.0.0");
         await expectSnapshotEqualsFresh("version deleted");

         await org.deletePackage("sales", {
            removeRecords: () =>
               store!.deletePackageFromDatabase("org", "sales"),
         });
         await expectSnapshotEqualsFresh("versioned package deleted");
      });
   });
}

describe("/status snapshot reads", () => {
   it("never loads a registered package that is not resident, and reads no version row", async () => {
      await boot([{ name: "org", packages: ["anchor"] }], {
         PUBLISHER_PACKAGE_VERSIONING: "on",
      });
      const org = await environmentOf("org");
      org.registerConfiguredPackage("later");
      const getPackage = sinon.spy(org, "getPackage");
      const versions = org.getVersionService() as VersionService<Package>;
      const listAllVersions = sinon.spy(versions, "listAllVersions");
      const versionRecords = sinon.spy(versions, "versionRecords");
      const isVersioned = sinon.spy(versions, "isVersioned");

      const status = await store!.getStatus({ includeLoading: true });

      expect(getPackage.called).toBe(false);
      expect(listAllVersions.called).toBe(false);
      expect(versionRecords.called).toBe(false);
      expect(isVersioned.called).toBe(false);
      const names = status.environments[0].packages?.map((p) => p.name);
      expect(names).toContain("anchor");
      expect(names).not.toContain("later");
   });
});
