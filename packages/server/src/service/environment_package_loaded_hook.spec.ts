// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { Environment } from "./environment";
import type { Package } from "./package";

// The package-loaded hook fires at the three places a package enters the
// environment's map: a lazy load from disk (what boot does for each package),
// addPackage, and installPackage (publish, update and reload-by-reinstall).
// A plain reload through getPackage(name, true) is the same site as the lazy
// load. Each firing is one load, so a caller that only enqueues work gets
// exactly one entry per load.

const MODEL = 'source: s is duckdb.sql("select 1 as n")\n';

async function writePackage(dir: string, name: string): Promise<void> {
   await fs.mkdir(dir, { recursive: true });
   await fs.writeFile(path.join(dir, "publisher.json"), `{"name":"${name}"}`);
   await fs.writeFile(path.join(dir, "model.malloy"), MODEL);
}

describe("Environment package-loaded hook", () => {
   let rootDir: string;
   let envPath: string;
   let env: Environment;
   let loaded: Package[];

   beforeEach(async () => {
      rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "publisher-hook-"));
      envPath = path.join(rootDir, "env");
      await fs.mkdir(envPath, { recursive: true });
      env = await Environment.create("hookEnv", envPath, []);
      loaded = [];
      env.setPackageLoadedHook((pkg) => {
         loaded.push(pkg);
      });
   });

   afterEach(async () => {
      await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
   });

   it("fires once when a package is first loaded from disk, and not on a cached lookup", async () => {
      await writePackage(path.join(envPath, "boot"), "boot");
      const pkg = await env.getPackage("boot", false);
      expect(loaded).toEqual([pkg]);
      await env.getPackage("boot", false);
      expect(loaded.length).toBe(1);
   });

   it("fires again, with the new instance, on a reload", async () => {
      await writePackage(path.join(envPath, "again"), "again");
      const first = await env.getPackage("again", false);
      const second = await env.getPackage("again", true);
      expect(second).not.toBe(first);
      expect(loaded).toEqual([first, second]);
   });

   it("fires once for addPackage", async () => {
      await writePackage(path.join(envPath, "added"), "added");
      const added = await env.addPackage("added");
      expect(added).toBeDefined();
      expect(loaded).toEqual([added as Package]);
   });

   it("fires once for installPackage", async () => {
      const installed = await env.installPackage(
         "installed",
         async (stagingPath) => {
            await writePackage(stagingPath, "installed");
         },
      );
      expect(loaded.length).toBe(1);
      expect(loaded[0].getPackageName()).toBe("installed");
      expect(installed).toBeDefined();
   });

   it("does not fail the load when the hook throws", async () => {
      env.setPackageLoadedHook(() => {
         throw new Error("hook exploded");
      });
      await writePackage(path.join(envPath, "sturdy"), "sturdy");
      const pkg = await env.getPackage("sturdy", false);
      expect(pkg.getPackageName()).toBe("sturdy");
      // Served: a second lookup returns the same instance.
      expect(await env.getPackage("sturdy", false)).toBe(pkg);
   });

   it("fires nothing for a package that fails to load", async () => {
      await fs.mkdir(path.join(envPath, "broken"), { recursive: true });
      await fs.writeFile(
         path.join(envPath, "broken", "publisher.json"),
         '{"name":"broken"}',
      );
      await fs.writeFile(
         path.join(envPath, "broken", "model.malloy"),
         "source: s is duckdb.sql(\n",
      );
      await expect(env.getPackage("broken", false)).rejects.toThrow();
      expect(loaded).toEqual([]);
   });
});
