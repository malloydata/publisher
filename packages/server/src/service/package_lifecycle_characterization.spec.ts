// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs/promises";
import * as os from "os";
import * as path from "path";

import { writePackageFiles } from "../test_helpers/temp_package";
import { Environment } from "./environment";
import { PackageStatus } from "./package_lifecycle";
import { parsePackageName } from "./identity";

/**
 * Characterization of a package's lifecycle as Environment's public read
 * surface reports it: what /status, get_status, the readiness line and the
 * `publisher_packages{state}` gauge read (getFailedPackages,
 * getStaleCompileErrors, getServingPackageCount, getPackageStatus,
 * getLoadedPackages). Pinned before the PackageLifecycle refactor so that the
 * refactor is held to today's observable behavior, including its overlaps: a
 * registered package can carry a failure record, a stale package keeps
 * serving, and failure records outlive a registration.
 */
describe("package lifecycle (characterization)", () => {
   let rootDir: string;
   let envPath: string;

   const GOOD_MODEL = `source: ones is duckdb.sql("SELECT 1 as x")\n`;
   const BROKEN_MODEL = `source: broken is @@@ not valid malloy !!!\n`;

   function writePackageDir(name: string, model: string): string {
      const dir = path.join(envPath, name);
      writePackageFiles(
         dir,
         { "model.malloy": model },
         { manifest: { name, description: "lifecycle fixture" } },
      );
      return dir;
   }

   /** Register `name` the way boot seeds a configured package. */
   function seed(env: Environment, name: string): void {
      env.registerConfiguredPackage(name);
   }

   beforeEach(async () => {
      rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "publisher-life-"));
      envPath = path.join(rootDir, "env");
      await fs.mkdir(envPath, { recursive: true });
   });

   afterEach(async () => {
      await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
   });

   it("a configured package is registered SERVING before any load, and serving once listed", async () => {
      const env = await Environment.create("e", envPath, []);
      writePackageDir("pkg", GOOD_MODEL);
      seed(env, "pkg");

      expect(env.getPackageStatus("pkg")?.status).toBe(PackageStatus.SERVING);
      expect(env.getLoadedPackages()).toHaveLength(0);
      expect(env.getServingPackageCount()).toBe(1);
      expect(env.hasLoadedPackage("pkg")).toBe(false);
      const state = await env.describeServingState("pkg");
      expect(state.status).toEqual({ serving: false, loading: false });

      const listed = await env.listPackages();
      expect(listed.map((p) => p.name)).toEqual(["pkg"]);
      expect(env.getLoadedPackages()).toHaveLength(1);
      expect(env.getPackageStatus("pkg")?.status).toBe(PackageStatus.SERVING);
   });

   it("a configured package that fails its lazy load is dropped and recorded as failed", async () => {
      const env = await Environment.create("e", envPath, []);
      seed(env, "missing");
      expect(env.getServingPackageCount()).toBe(1);

      expect(await env.listPackages()).toEqual([]);

      expect(env.getPackageStatus("missing")).toBeUndefined();
      expect([...env.getFailedPackages().keys()]).toEqual(["missing"]);
      expect(env.getServingPackageCount()).toBe(0);
      expect(env.getStaleCompileErrors().size).toBe(0);
      await expect(env.describeServingState("missing")).rejects.toThrow(
         "not found",
      );
   });

   it("merges failure records: load failures first, a mount error's message wins", async () => {
      const env = await Environment.create("e", envPath, []);
      // A mount error recorded before any load failure still lists after it.
      env.setPackageMountError("mount-only", "location does not exist");
      seed(env, "both");
      seed(env, "load-only");
      await env.listPackages();
      env.setPackageMountError("both", "both: never mounted");

      const failed = env.getFailedPackages();
      const keys = [...failed.keys()];
      // Both load failures precede the mount-only entry.
      expect(keys.slice(0, 2).sort()).toEqual(["both", "load-only"]);
      expect(keys[2]).toBe("mount-only");
      expect(failed.get("both")).toBe("both: never mounted");
      expect(failed.get("mount-only")).toBe("location does not exist");
      expect(failed.get("load-only")).toContain("load-only");
   });

   it("a mount error on a seeded package is excluded from the serving count", async () => {
      const env = await Environment.create("e", envPath, []);
      env.setPackageMountError("bad", "location does not exist");
      seed(env, "good");
      seed(env, "bad");
      expect(env.getServingPackageCount()).toBe(1);
      expect(env.getPackageStatus("bad")?.status).toBe(PackageStatus.SERVING);
      expect([...env.getFailedPackages()]).toEqual([
         ["bad", "location does not exist"],
      ]);
   });

   it("records a runtime add failure only for an unregistered name, capped oldest first", async () => {
      const env = await Environment.create("e", envPath, []);
      seed(env, "registered");
      env.recordPackageAddFailure("registered", "ignored");
      expect(env.getFailedPackages().size).toBe(0);

      env.setPackageMountError("boot", "boot mount error");
      for (let i = 0; i < 101; i++) {
         env.recordPackageAddFailure(`add-${i}`, `failure ${i}`);
      }
      // Re-recording an already recorded name updates it in place.
      env.recordPackageAddFailure("add-50", "again");
      const failed = env.getFailedPackages();
      expect(failed.has("add-0")).toBe(false);
      expect(failed.has("add-1")).toBe(true);
      expect(failed.has("add-100")).toBe(true);
      expect(failed.get("add-50")).toBe("again");
      // Boot mount errors are never evicted.
      expect(failed.get("boot")).toBe("boot mount error");
      expect(failed.size).toBe(101);
      expect([...failed.keys()][0]).toBe("boot");
   });

   it("a failed reload keeps serving and reports stale, a later success clears it", async () => {
      const env = await Environment.create("e", envPath, []);
      const dir = writePackageDir("pkg", GOOD_MODEL);
      await env.addPackage("pkg");
      const registeredAt = env.getPackageStatus("pkg")?.loadTimestamp;
      expect(registeredAt).toBeNumber();

      await fs.writeFile(path.join(dir, "model.malloy"), BROKEN_MODEL);
      await expect(
         env.getPackage(parsePackageName("pkg"), true),
      ).rejects.toThrow();

      expect(env.getPackageStatus("pkg")).toEqual({
         name: "pkg",
         loadTimestamp: registeredAt!,
         status: PackageStatus.SERVING,
      });
      const stale = env.getStaleCompileErrors().get("pkg");
      expect(stale?.message).toBeTruthy();
      expect(new Date(stale!.failedAt).toISOString()).toBe(stale!.failedAt);
      expect(env.getFailedPackages().size).toBe(0);
      expect(env.getServingPackageCount()).toBe(1);
      expect(env.getLoadedPackages()).toHaveLength(1);

      // A second failure replaces the record.
      await expect(
         env.getPackage(parsePackageName("pkg"), true),
      ).rejects.toThrow();
      expect(env.getStaleCompileErrors().size).toBe(1);

      await fs.writeFile(path.join(dir, "model.malloy"), GOOD_MODEL);
      await env.getPackage(parsePackageName("pkg"), true);
      expect(env.getStaleCompileErrors().size).toBe(0);
      expect(env.getPackageStatus("pkg")?.loadTimestamp).toBe(registeredAt!);
   });

   it("a first load of an unregistered name that fails leaves no status and no record", async () => {
      const env = await Environment.create("e", envPath, []);
      writePackageDir("broken", BROKEN_MODEL);
      await expect(env.addPackage("broken")).rejects.toThrow();
      expect(env.getPackageStatus("broken")).toBeUndefined();
      expect(env.getFailedPackages().size).toBe(0);
      await expect(
         env.getPackage(parsePackageName("broken")),
      ).rejects.toThrow();
      expect(env.getPackageStatus("broken")).toBeUndefined();
      expect(env.getFailedPackages().size).toBe(0);
   });

   it("a successful load clears a recorded failure of any kind", async () => {
      const env = await Environment.create("e", envPath, []);
      env.recordPackageAddFailure("pkg", "earlier add failed");
      expect(env.getFailedPackages().size).toBe(1);
      writePackageDir("pkg", GOOD_MODEL);
      await env.addPackage("pkg");
      expect(env.getFailedPackages().size).toBe(0);
      expect(env.getPackageStatus("pkg")?.status).toBe(PackageStatus.SERVING);
   });

   it("unloading clears registration and every failure record", async () => {
      const env = await Environment.create("e", envPath, []);
      const dir = writePackageDir("pkg", GOOD_MODEL);
      await env.addPackage("pkg");
      await fs.writeFile(path.join(dir, "model.malloy"), BROKEN_MODEL);
      await expect(
         env.getPackage(parsePackageName("pkg"), true),
      ).rejects.toThrow();
      expect(env.getStaleCompileErrors().size).toBe(1);

      await env.unloadPackage("pkg");
      expect(env.getPackageStatus("pkg")).toBeUndefined();
      expect(env.getStaleCompileErrors().size).toBe(0);
      expect(env.getLoadedPackages()).toHaveLength(0);
   });

   it("deleting a failed package forgets its failure", async () => {
      const env = await Environment.create("e", envPath, []);
      seed(env, "missing");
      await env.listPackages();
      expect(env.getFailedPackages().size).toBe(1);
      await env.deletePackage("missing");
      expect(env.getFailedPackages().size).toBe(0);
      expect(env.getPackageStatus("missing")).toBeUndefined();
   });

   it("deleting a configured package that was never loaded removes it and its folder", async () => {
      const env = await Environment.create("e", envPath, []);
      const dir = writePackageDir("pkg", GOOD_MODEL);
      seed(env, "pkg");
      let recordsRemoved = false;
      await env.deletePackage("pkg", {
         removeRecords: async () => {
            recordsRemoved = true;
         },
      });
      expect(recordsRemoved).toBe(true);
      expect(env.getPackageStatus("pkg")).toBeUndefined();
      expect(await env.listPackages()).toEqual([]);
      expect(env.getLoadedPackages()).toHaveLength(0);
      await expect(fs.stat(dir)).rejects.toThrow();
   });

   it("deleting a package whose lazy load failed keeps its folder", async () => {
      const env = await Environment.create("e", envPath, []);
      const dir = writePackageDir("pkg", BROKEN_MODEL);
      seed(env, "pkg");
      await env.listPackages();
      expect(env.getFailedPackages().size).toBe(1);
      await env.deletePackage("pkg");
      expect(env.getFailedPackages().size).toBe(0);
      expect(env.getPackageStatus("pkg")).toBeUndefined();
      expect((await fs.stat(dir)).isDirectory()).toBe(true);
   });

   it("deleting a serving package removes it", async () => {
      const env = await Environment.create("e", envPath, []);
      writePackageDir("pkg", GOOD_MODEL);
      await env.addPackage("pkg");
      await env.deletePackage("pkg");
      expect(env.getPackageStatus("pkg")).toBeUndefined();
      expect(env.getLoadedPackages()).toHaveLength(0);
      expect(await env.listPackages()).toEqual([]);
   });

   it("closing connections drops registrations but keeps failure records", async () => {
      const env = await Environment.create("e", envPath, []);
      writePackageDir("pkg", GOOD_MODEL);
      await env.addPackage("pkg");
      env.setPackageMountError("gone", "location does not exist");
      await env.closeAllConnections();
      expect(env.getPackageStatus("pkg")).toBeUndefined();
      expect(env.getServingPackageCount()).toBe(0);
      expect([...env.getFailedPackages().keys()]).toEqual(["gone"]);
   });
});
