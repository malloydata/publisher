// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs/promises";
import * as os from "os";
import * as path from "path";
import sinon from "sinon";

import { Environment } from "./environment";
import { Package } from "./package";
import { writePackageFiles } from "../test_helpers/temp_package";
import { parsePackageName } from "./identity";

/**
 * `Package.status` and the listing rules behind it.
 *
 * A reload compiles a new copy beside the one still serving, so for the whole
 * compile the package is both serving and loading. The listing used to answer
 * that window by omitting the package, which an orchestrator reading /status
 * could only take as the package having left this server; it would unload the
 * replica and place the package elsewhere. These tests pin the two facts the
 * API now reports separately, `serving` and `loading`, through the paths that
 * load a package in place, and that only the server, never package content,
 * says where a package came from. (A published version's load is covered in
 * environment_versions.spec.ts.)
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
   const BROKEN_MODEL = `source: broken is @@@ not valid malloy !!!\n`;

   function deferred(): { promise: Promise<void>; resolve: () => void } {
      let resolve!: () => void;
      const promise = new Promise<void>((r) => {
         resolve = r;
      });
      return { promise, resolve };
   }

   async function writePackageDir(dir: string, model = MODEL): Promise<void> {
      writePackageFiles(
         dir,
         {
            "model.malloy": model,
         },
         { manifest: { name: "pkg", description: "fixture" } },
      );
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

   it("reports a reload in place as loading while the previous copy serves", async () => {
      const env = await Environment.create("testEnv", envPath, []);
      await writePackageDir(path.join(envPath, "pkg"));
      await env.addPackage("pkg");

      const compile = holdCompile();
      const reload = env.getPackage(parsePackageName("pkg"), true);
      await compile.entered;

      const during = (await env.listPackages()).find((p) => p.name === "pkg");
      expect(during?.status).toMatchObject({ serving: true, loading: true });
      expect(during?.status?.loadingSince).toBeString();
      expect(env.describePackageStatus("pkg").loading).toBe(true);

      compile.release();
      await reload;
      expect(env.describePackageStatus("pkg")).toEqual({
         serving: true,
         loading: false,
      });
   });

   it("lists a first load as loading and not serving, on the status listing only", async () => {
      const env = await Environment.create("testEnv", envPath, []);
      await writePackageDir(path.join(envPath, "pkg"));

      const compile = holdCompile();
      const add = env.addPackage("pkg");
      // addPackage marks the load before it compiles, so once the compile is
      // entered the package is loading and nothing is resident.
      await compile.entered;

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

      compile.release();
      await add;

      for (const listing of [
         await env.listPackages(),
         await env.listPackages({ includeLoading: true }),
      ]) {
         const pkg = listing.find((p) => p.name === "pkg");
         expect(pkg?.status).toEqual({ serving: true, loading: false });
      }
   });

   it("clears the loading marker after a load fails", async () => {
      // A marker left behind would report a load in flight forever, which an
      // orchestrator reads as its own dispatched load still running.
      const env = await Environment.create("testEnv", envPath, []);
      const pkgDir = path.join(envPath, "pkg");
      await writePackageDir(pkgDir, BROKEN_MODEL);

      // A first load that fails: nothing is resident, and nothing is loading.
      await expect(env.addPackage("pkg")).rejects.toThrow();
      expect(env.describePackageStatus("pkg")).toEqual({
         serving: false,
         loading: false,
      });

      // A reload that fails over a serving copy leaves that copy serving.
      await writePackageDir(pkgDir);
      await env.addPackage("pkg");
      await fs.writeFile(path.join(pkgDir, "model.malloy"), BROKEN_MODEL);
      await expect(
         env.getPackage(parsePackageName("pkg"), true),
      ).rejects.toThrow();
      expect(env.describePackageStatus("pkg")).toEqual({
         serving: true,
         loading: false,
      });
   });

   it("ignores a location an author wrote into publisher.json", async () => {
      // A package's location is where the server fetched it from, which only
      // the server knows (a published version's comes from its registry row).
      // Read from the package's own manifest, it would let a package author
      // claim any path on the server.
      const env = await Environment.create("testEnv", envPath, []);
      const dir = path.join(envPath, "pkg");
      writePackageFiles(
         dir,
         {
            "model.malloy": MODEL,
         },
         { manifest: { name: "pkg", location: "/etc" } },
      );

      const added = await env.addPackage("pkg");
      expect(added?.getPackageMetadata().location).toBeUndefined();
   });
});
