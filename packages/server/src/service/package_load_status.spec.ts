// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs/promises";
import * as os from "os";
import * as path from "path";
import sinon from "sinon";

import { PackageNotFoundError, ServiceUnavailableError } from "../errors";
import { Environment } from "./environment";
import { Package } from "./package";
import type { PackageMemoryGovernor } from "./package_memory_governor";

/**
 * `Package.status` and the listing rules behind it.
 *
 * A reinstall swaps a new compiled copy in beside the one still serving, so
 * for the whole compile the package is both serving and loading. The listing
 * used to answer that window by omitting the package, which an orchestrator
 * reading /status could only take as the package having left this server;
 * it would unload the replica and place the package elsewhere, and the
 * reinstall in flight here then failed for nothing. These tests pin the two
 * facts the API now reports separately, `serving` and `loading`, through each
 * path that loads a package, and the two install behaviours that ride on the
 * same change: admission control before the download, and the metadata that
 * comes with an install landing under the install's own lock hold.
 *
 * Run against a real `Environment` and a real `Package.create` over temp dirs,
 * the way package_reload_safety.spec.ts does. `Package.create` is stubbed only
 * to hold the compile open, so the window these tests are about can be
 * observed rather than raced.
 */
describe("Package.status: serving and loading", () => {
   let rootDir: string;
   let envPath: string;

   const MODEL = `source: ones is duckdb.sql("SELECT 1 as x")\n`;

   function deferred(): { promise: Promise<void>; resolve: () => void } {
      let resolve!: () => void;
      const promise = new Promise<void>((r) => {
         resolve = r;
      });
      return { promise, resolve };
   }

   async function writePackageDir(
      dir: string,
      description = "fixture",
   ): Promise<void> {
      await fs.mkdir(dir, { recursive: true });
      await fs.writeFile(
         path.join(dir, "publisher.json"),
         JSON.stringify({ name: "pkg", description }),
      );
      await fs.writeFile(path.join(dir, "model.malloy"), MODEL);
   }

   async function copyDir(src: string, dst: string): Promise<void> {
      await fs.mkdir(dst, { recursive: true });
      await fs.cp(src, dst, { recursive: true });
   }

   /** Hold every `Package.create` open until `release` is called. */
   function holdCompile(): {
      entered: Promise<void>;
      release: () => void;
   } {
      const entered = deferred();
      const gate = deferred();
      const original = Package.create.bind(Package);
      sinon
         .stub(Package, "create")
         .callsFake(async (...args: Parameters<typeof Package.create>) => {
            entered.resolve();
            await gate.promise;
            return original(...args);
         });
      return { entered: entered.promise, release: gate.resolve };
   }

   beforeEach(async () => {
      rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "publisher-status-"));
      envPath = path.join(rootDir, "env");
      await fs.mkdir(envPath, { recursive: true });
   });

   afterEach(async () => {
      sinon.restore();
      await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
   });

   it("keeps a package listed, serving and loading, while it is reinstalled", async () => {
      const env = await Environment.create("testEnv", envPath, []);
      const fixture = path.join(rootDir, "fixture");
      await writePackageDir(fixture);
      await env.installPackage("pkg", (stagingPath) =>
         copyDir(fixture, stagingPath),
      );
      const before = (await env.listPackages()).find((p) => p.name === "pkg");
      expect(before?.status).toEqual({ serving: true, loading: false });

      const compile = holdCompile();
      const reinstall = env.installPackage("pkg", (stagingPath) =>
         copyDir(fixture, stagingPath),
      );
      await compile.entered;

      // The previous copy is still what answers queries, and the reinstall is
      // compiling under the lock. Both facts are on the listing; the package
      // was not dropped from it.
      const during = (await env.listPackages()).find((p) => p.name === "pkg");
      expect(during).toBeDefined();
      expect(during?.status?.serving).toBe(true);
      expect(during?.status?.loading).toBe(true);
      expect(during?.status?.loadingSince).toBeString();
      expect(env.describePackageStatus("pkg").loading).toBe(true);

      compile.release();
      await reinstall;

      const after = (await env.listPackages()).find((p) => p.name === "pkg");
      expect(after?.status).toEqual({ serving: true, loading: false });
   });

   it("reports a reload in place as loading while the previous copy serves", async () => {
      const env = await Environment.create("testEnv", envPath, []);
      await writePackageDir(path.join(envPath, "pkg"));
      await env.addPackage("pkg");

      const compile = holdCompile();
      const reload = env.getPackage("pkg", true);
      await compile.entered;

      const during = (await env.listPackages()).find((p) => p.name === "pkg");
      expect(during?.status).toMatchObject({ serving: true, loading: true });

      compile.release();
      await reload;
      expect(env.describePackageStatus("pkg")).toEqual({
         serving: true,
         loading: false,
      });
   });

   it("lists a first load as loading and not serving, on the status listing only", async () => {
      const env = await Environment.create("testEnv", envPath, []);
      const fixture = path.join(rootDir, "fixture");
      await writePackageDir(fixture);

      const download = deferred();
      const install = env.installPackage("pkg", async (stagingPath) => {
         await download.promise;
         await copyDir(fixture, stagingPath);
      });
      // installPackage marks the load before it starts the download, so there
      // is nothing to wait for here: the download is what is being held.

      // Nothing compiled yet, so the default listing (what the discovery tools
      // and the database sync read) leaves it out...
      expect((await env.listPackages()).map((p) => p.name)).not.toContain(
         "pkg",
      );
      // ...while the status listing names it, with the two facts an
      // orchestrator needs to tell its own dispatched load from an absence.
      const onStatus = (await env.listPackages({ includeLoading: true })).find(
         (p) => p.name === "pkg",
      );
      expect(onStatus).toBeDefined();
      expect(onStatus?.status?.serving).toBe(false);
      expect(onStatus?.status?.loading).toBe(true);
      expect(onStatus?.status?.loadingSince).toBeString();

      // The environment resource and the default status report describe the
      // environment without it: a listed package has always meant one that
      // can serve here. Only `includeLoading` adds it.
      expect(
         (await env.serialize()).packages?.map((p) => p.name),
      ).not.toContain("pkg");
      expect(
         (await env.serialize({ includeLoading: true })).packages?.map(
            (p) => p.name,
         ),
      ).toContain("pkg");

      download.resolve();
      await install;

      for (const listing of [
         await env.listPackages(),
         await env.listPackages({ includeLoading: true }),
      ]) {
         const pkg = listing.find((p) => p.name === "pkg");
         expect(pkg?.status).toEqual({ serving: true, loading: false });
      }
   });

   it("refuses an install under memory back-pressure before downloading", async () => {
      const env = await Environment.create("testEnv", envPath, []);
      const fixture = path.join(rootDir, "fixture");
      await writePackageDir(fixture);
      await env.installPackage("pkg", (stagingPath) =>
         copyDir(fixture, stagingPath),
      );

      env.setMemoryGovernor({
         isBackpressured: () => true,
      } as unknown as PackageMemoryGovernor);
      const downloader = sinon.stub().resolves(undefined);

      await expect(
         env.installPackage("pkg", downloader),
      ).rejects.toBeInstanceOf(ServiceUnavailableError);
      // Refused at the door: no download was started, and the copy that was
      // serving is untouched and still listed as such.
      expect(downloader.called).toBe(false);
      const still = (await env.listPackages()).find((p) => p.name === "pkg");
      expect(still?.status).toEqual({ serving: true, loading: false });
   });

   it("applies the metadata sent with an install under the same lock hold, ahead of a queued delete", async () => {
      const env = await Environment.create("testEnv", envPath, []);
      const fixture = path.join(rootDir, "fixture");
      await writePackageDir(fixture);
      await env.installPackage("pkg", (stagingPath) =>
         copyDir(fixture, stagingPath),
      );

      const compile = holdCompile();
      const reinstall = env.installPackage(
         "pkg",
         (stagingPath) => copyDir(fixture, stagingPath),
         undefined,
         { update: { name: "pkg", description: "installed together" } },
      );
      await compile.entered;
      // A delete arrives while the swap is compiling. It queues on the package
      // lock behind the install, and must not run between the swap and the
      // metadata update: that gap is where the update used to find no package
      // and answer 404 for an install that had completed.
      const deletion = env.deletePackage("pkg");
      compile.release();

      const installed = await reinstall;
      expect(installed.getPackageMetadata().description).toBe(
         "installed together",
      );
      await deletion;
      expect((await env.listPackages()).map((p) => p.name)).not.toContain(
         "pkg",
      );
   });

   it("binds and persists a manifestLocation sent with an install", async () => {
      const env = await Environment.create("testEnv", envPath, []);
      const fixture = path.join(rootDir, "fixture");
      await writePackageDir(fixture);
      const manifestFile = path.join(rootDir, "manifest.json");
      await fs.writeFile(
         manifestFile,
         JSON.stringify({
            builtAt: new Date().toISOString(),
            strict: false,
            entries: {},
         }),
      );

      const installed = await env.installPackage(
         "pkg",
         (stagingPath) => copyDir(fixture, stagingPath),
         undefined,
         { update: { manifestLocation: manifestFile } },
      );

      expect(installed.getPackageMetadata().manifestLocation).toBe(
         manifestFile,
      );
      // Written through to publisher.json, so an in-place reload binds it again
      // without being told.
      const onDisk = JSON.parse(
         await fs.readFile(
            path.join(envPath, "pkg", "publisher.json"),
            "utf-8",
         ),
      );
      expect(onDisk.manifestLocation).toBe(manifestFile);
      expect(onDisk.description).toBe("fixture");
   });

   it("keeps description, resource and location through a PATCH that omits them", async () => {
      const env = await Environment.create("testEnv", envPath, []);
      const fixture = path.join(rootDir, "fixture");
      await writePackageDir(fixture, "first");
      await env.installPackage(
         "pkg",
         (stagingPath) => copyDir(fixture, stagingPath),
         undefined,
         {
            update: {
               resource: "/api/v0/environments/testEnv/packages/pkg",
               location: "gs://bucket/pkg.zip",
            },
         },
      );

      // A metadata PATCH that names only the manifest: everything it omits
      // must survive.
      const after = await env.updatePackage("pkg", {
         name: "pkg",
         manifestLocation: null,
      });

      expect(after.description).toBe("first");
      expect(after.resource).toBe("/api/v0/environments/testEnv/packages/pkg");
      expect(after.location).toBe("gs://bucket/pkg.zip");
      const onDisk = JSON.parse(
         await fs.readFile(
            path.join(envPath, "pkg", "publisher.json"),
            "utf-8",
         ),
      );
      expect(onDisk.description).toBe("first");
   });

   it("a metadata PATCH during a first install's download waits for the install and lands on it", async () => {
      // The download runs before the install takes the package lock, so a
      // PATCH in that window finds no resident copy and a free lock. It is
      // meant for the copy being installed; answering 404 instead would read
      // to an orchestrator as the package having gone, which is the very
      // signal this status work exists to stop sending.
      const env = await Environment.create("testEnv", envPath, []);
      const fixture = path.join(rootDir, "fixture");
      await writePackageDir(fixture);
      const location = "gs://bucket/pkg___1.0.0.zip";

      const download = deferred();
      const install = env.installPackage(
         "pkg",
         async (stagingPath) => {
            await download.promise;
            await copyDir(fixture, stagingPath);
         },
         undefined,
         { update: { location } },
      );
      expect(env.describePackageStatus("pkg").serving).toBe(false);

      let patchSettled = false;
      const patch = env
         .updatePackage("pkg", { name: "pkg", description: "patched" })
         .finally(() => {
            patchSettled = true;
         });
      // Nothing to apply it to yet, so it is pending, not a 404.
      await new Promise((resolve) => setTimeout(resolve, 20));
      expect(patchSettled).toBe(false);

      download.resolve();
      await install;
      const after = await patch;
      expect(after.description).toBe("patched");
      expect(after.location).toBe(location);
      expect(env.describePackageStatus("pkg")).toEqual({
         serving: true,
         loading: false,
      });
   });

   it("a metadata PATCH during a first install that then fails answers 404", async () => {
      const env = await Environment.create("testEnv", envPath, []);
      const download = deferred();
      const install = env.installPackage("pkg", async () => {
         await download.promise;
         throw new Error("download failed");
      });
      const patchOutcome = env.updatePackage("pkg", { name: "pkg" }).then(
         () => undefined,
         (err: unknown) => err,
      );
      download.resolve();
      await expect(install).rejects.toThrow("download failed");
      expect(await patchOutcome).toBeInstanceOf(PackageNotFoundError);
   });

   it("a PATCH sending null for a field leaves it alone, in memory and on disk", async () => {
      // A client that serializes unset fields as null must not blank them:
      // null counts as not mentioned, the rule `scope` already follows, and
      // publisher.json gets no `description: null` either.
      const env = await Environment.create("testEnv", envPath, []);
      const fixture = path.join(rootDir, "fixture");
      await writePackageDir(fixture, "first");
      await env.installPackage(
         "pkg",
         (stagingPath) => copyDir(fixture, stagingPath),
         undefined,
         {
            update: {
               resource: "/api/v0/environments/testEnv/packages/pkg",
               location: "gs://bucket/pkg.zip",
            },
         },
      );

      const after = await env.updatePackage("pkg", {
         name: "pkg",
         description: null as unknown as string,
         resource: null as unknown as string,
         location: null as unknown as string,
         manifestLocation: null,
         queryableSources: null as unknown as "declared",
      });

      expect(after.name).toBe("pkg");
      expect(after.queryableSources).toBe("declared");
      expect(after.description).toBe("first");
      expect(after.resource).toBe("/api/v0/environments/testEnv/packages/pkg");
      expect(after.location).toBe("gs://bucket/pkg.zip");
      const onDisk = JSON.parse(
         await fs.readFile(
            path.join(envPath, "pkg", "publisher.json"),
            "utf-8",
         ),
      );
      expect(onDisk.description).toBe("first");
      const record = JSON.parse(
         await fs.readFile(
            path.join(envPath, "pkg", ".publisher-install.json"),
            "utf-8",
         ),
      );
      expect(record.location).toBe("gs://bucket/pkg.zip");

      // An empty string is a value, and clears.
      const cleared = await env.updatePackage("pkg", {
         name: "pkg",
         description: "",
      });
      expect(cleared.description).toBe("");
   });

   it("a same-location PATCH is never refused by the memory governor", async () => {
      // The governor gates allocations of a new compiled copy. A metadata
      // update allocates none, so back-pressure that refuses an install must
      // let the orchestrator's rebind through, or a throttled replica could
      // never pick up a new manifest.
      const env = await Environment.create("testEnv", envPath, []);
      const fixture = path.join(rootDir, "fixture");
      await writePackageDir(fixture);
      await env.installPackage("pkg", (stagingPath) =>
         copyDir(fixture, stagingPath),
      );
      env.setMemoryGovernor({
         isBackpressured: () => true,
      } as unknown as PackageMemoryGovernor);

      await expect(
         env.installPackage("pkg", sinon.stub().resolves(undefined)),
      ).rejects.toBeInstanceOf(ServiceUnavailableError);
      const updated = await env.updatePackage("pkg", {
         name: "pkg",
         description: "rebound under back-pressure",
      });
      expect(updated.description).toBe("rebound under back-pressure");
   });

   it("clears the loading marker after an install is rolled back", async () => {
      const env = await Environment.create("testEnv", envPath, []);
      const fixture = path.join(rootDir, "fixture");
      await writePackageDir(fixture);

      // A first install whose validation rejects the tree: the swap is rolled
      // back, nothing is resident, and nothing is loading.
      await expect(
         env.installPackage(
            "pkg",
            (stagingPath) => copyDir(fixture, stagingPath),
            () => "rejected by validation",
         ),
      ).rejects.toThrow("rejected by validation");
      expect(env.describePackageStatus("pkg")).toEqual({
         serving: false,
         loading: false,
      });
      expect(
         (await env.listPackages({ includeLoading: true })).map((p) => p.name),
      ).not.toContain("pkg");

      // The same rollback over a serving copy leaves that copy serving.
      await env.installPackage("pkg", (stagingPath) =>
         copyDir(fixture, stagingPath),
      );
      await expect(
         env.installPackage(
            "pkg",
            (stagingPath) => copyDir(fixture, stagingPath),
            () => "rejected by validation",
         ),
      ).rejects.toThrow("rejected by validation");
      expect(env.describePackageStatus("pkg")).toEqual({
         serving: true,
         loading: false,
      });
   });

   it("a waiting PATCH is released when the last load in flight finishes, not the first", async () => {
      // Loads of one package started independently share the loading entry.
      // Released on the first to finish, a PATCH would find nothing resident
      // and answer 404 while the other is still downloading.
      const env = await Environment.create("testEnv", envPath, []);
      const fixture = path.join(rootDir, "fixture");
      await writePackageDir(fixture);
      const location = "gs://bucket/pkg___1.0.0.zip";

      const firstDownload = deferred();
      const first = env.installPackage(
         "pkg",
         async () => {
            await firstDownload.promise;
            throw new Error("first download failed");
         },
         undefined,
         { update: { location } },
      );
      const secondDownload = deferred();
      const second = env.installPackage(
         "pkg",
         async (stagingPath) => {
            await secondDownload.promise;
            await copyDir(fixture, stagingPath);
         },
         undefined,
         { update: { location } },
      );

      let patchSettled = false;
      const patch = env
         .updatePackage("pkg", { name: "pkg", description: "patched" })
         .finally(() => {
            patchSettled = true;
         });

      firstDownload.resolve();
      await expect(first).rejects.toThrow("first download failed");
      await new Promise((resolve) => setTimeout(resolve, 20));
      expect(patchSettled).toBe(false);
      expect(env.describePackageStatus("pkg").loading).toBe(true);

      secondDownload.resolve();
      await second;
      const after = await patch;
      expect(after.description).toBe("patched");
   });

   it("remembers where a package was installed from, across a reload", async () => {
      // The reinstall decision compares a PATCH's `location` with the one the
      // package was installed from. That value has to survive the install
      // itself, an in-place reload, and (through publisher.json) a restart,
      // or the first rebind after any of them is a full reinstall again.
      const env = await Environment.create("testEnv", envPath, []);
      const fixture = path.join(rootDir, "fixture");
      await writePackageDir(fixture);
      const location = "gs://bucket/pkg___1.0.0.zip";

      const download = deferred();
      const install = env.installPackage(
         "pkg",
         async (stagingPath) => {
            await download.promise;
            await copyDir(fixture, stagingPath);
         },
         undefined,
         { update: { location } },
      );
      expect(env.describePackageStatus("pkg").loading).toBe(true);
      download.resolve();
      const installed = await install;
      expect(env.describePackageStatus("pkg").loading).toBe(false);

      expect(installed.getPackageMetadata().location).toBe(location);
      // Recorded in the server's own file, not in the package's manifest: the
      // manifest is the author's, and a reload fetches from this value.
      const record = JSON.parse(
         await fs.readFile(
            path.join(envPath, "pkg", ".publisher-install.json"),
            "utf-8",
         ),
      );
      expect(record.location).toBe(location);
      const manifest = JSON.parse(
         await fs.readFile(
            path.join(envPath, "pkg", "publisher.json"),
            "utf-8",
         ),
      );
      expect(manifest.location).toBeUndefined();

      // An in-place reload rebuilds the metadata from disk.
      const reloaded = await env.getPackage("pkg", true);
      expect(reloaded.getPackageMetadata().location).toBe(location);
   });

   it("ignores a location an author wrote into publisher.json", async () => {
      // A reload re-fetches from the recorded location, mounting a local path
      // in place. Read from the package's own manifest, that would let a
      // package author name any path on the server; only the server's record
      // counts.
      const env = await Environment.create("testEnv", envPath, []);
      const dir = path.join(envPath, "pkg");
      await fs.mkdir(dir, { recursive: true });
      await fs.writeFile(
         path.join(dir, "publisher.json"),
         JSON.stringify({ name: "pkg", location: "/etc" }),
      );
      await fs.writeFile(path.join(dir, "model.malloy"), MODEL);

      const added = await env.addPackage("pkg");
      expect(added?.getPackageMetadata().location).toBeUndefined();
   });

   it("a metadata PATCH never changes where a package was installed from", async () => {
      // Only an install records a location. A PATCH naming a different one is
      // a reinstall, decided by the controller; if a PATCH could write the
      // location directly, a failed reinstall would leave the old content
      // claiming the new location and the package would never upgrade.
      const env = await Environment.create("testEnv", envPath, []);
      const fixture = path.join(rootDir, "fixture");
      await writePackageDir(fixture);
      const first = "gs://bucket/pkg___1.0.0.zip";
      await env.installPackage(
         "pkg",
         (stagingPath) => copyDir(fixture, stagingPath),
         undefined,
         { update: { location: first } },
      );

      const after = await env.updatePackage("pkg", {
         name: "pkg",
         location: "gs://bucket/pkg___1.0.1.zip",
         description: "patched",
      });
      expect(after.description).toBe("patched");
      expect(after.location).toBe(first);
      const record = JSON.parse(
         await fs.readFile(
            path.join(envPath, "pkg", ".publisher-install.json"),
            "utf-8",
         ),
      );
      expect(record.location).toBe(first);
   });

   it("a metadata PATCH during a reinstall waits for the swap and lands on the new copy", async () => {
      // During a reinstall the previous copy is resident and the lock is free
      // while the download runs. Applied then, the PATCH would land on the
      // copy about to be replaced: lost at the swap, or kept by a rollback.
      const env = await Environment.create("testEnv", envPath, []);
      const fixture = path.join(rootDir, "fixture");
      await writePackageDir(fixture, "first");
      const second = path.join(rootDir, "fixture2");
      await writePackageDir(second, "second");
      await env.installPackage(
         "pkg",
         (stagingPath) => copyDir(fixture, stagingPath),
         undefined,
         { update: { location: "gs://bucket/pkg___1.0.0.zip" } },
      );

      const download = deferred();
      const reinstall = env.installPackage(
         "pkg",
         async (stagingPath) => {
            await download.promise;
            await copyDir(second, stagingPath);
         },
         undefined,
         { update: { location: "gs://bucket/pkg___1.0.1.zip" } },
      );
      let patchSettled = false;
      const patch = env
         .updatePackage("pkg", { name: "pkg", resource: "/r" })
         .finally(() => {
            patchSettled = true;
         });
      await new Promise((resolve) => setTimeout(resolve, 20));
      expect(patchSettled).toBe(false);
      expect(env.peekPackage("pkg")?.getPackageMetadata().description).toBe(
         "first",
      );

      download.resolve();
      await reinstall;
      const after = await patch;
      expect(after.description).toBe("second");
      expect(after.resource).toBe("/r");
      expect(after.location).toBe("gs://bucket/pkg___1.0.1.zip");
   });

   it("clearing a manifest that was never bound does not recompile the package", async () => {
      // A null manifestLocation from a client that serializes unset fields
      // has nothing to revert on a package serving live; recompiling every
      // model to get back to where it already is costs a full load.
      const env = await Environment.create("testEnv", envPath, []);
      const fixture = path.join(rootDir, "fixture");
      await writePackageDir(fixture);
      await env.installPackage("pkg", (stagingPath) =>
         copyDir(fixture, stagingPath),
      );
      const resident = env.peekPackage("pkg");
      expect(resident).toBeDefined();
      const reload = sinon.spy(resident!, "reloadAllModels");

      await env.updatePackage("pkg", { name: "pkg", manifestLocation: null });
      expect(reload.called).toBe(false);
   });

   it("applies a metadata PATCH that arrives during a first install once the install lands", async () => {
      // The orchestrator's drift check can PATCH a package it sees loading
      // but not yet serving. With the same location that PATCH is a metadata
      // update, so it queues on the package lock behind the install and lands
      // on the installed copy, rather than starting a second install.
      const env = await Environment.create("testEnv", envPath, []);
      const fixture = path.join(rootDir, "fixture");
      await writePackageDir(fixture);

      const compile = holdCompile();
      const install = env.installPackage("pkg", (stagingPath) =>
         copyDir(fixture, stagingPath),
      );
      await compile.entered;
      const update = env.updatePackage("pkg", {
         name: "pkg",
         description: "from the drift check",
      });
      compile.release();

      await install;
      const after = await update;
      expect(after.description).toBe("from the drift check");
      expect(env.describePackageStatus("pkg")).toEqual({
         serving: true,
         loading: false,
      });
   });
});
