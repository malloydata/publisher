// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs/promises";
import * as os from "os";
import * as path from "path";
import sinon from "sinon";

import { ServiceUnavailableError } from "../errors";
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
      await writePackageDir(path.join(envPath, "pkg"), "first");
      await env.addPackage("pkg");
      await env.updatePackage("pkg", {
         name: "pkg",
         resource: "/api/v0/environments/testEnv/packages/pkg",
         location: "gs://bucket/pkg.zip",
      });

      // The orchestrator's rebind PATCH: a name and a manifest, nothing else.
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
});
