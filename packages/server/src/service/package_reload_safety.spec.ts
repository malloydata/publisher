import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs/promises";
import * as os from "os";
import * as path from "path";

import { Environment, PackageStatus } from "./environment";

/**
 * Regression tests for RELOAD data loss.
 *
 * `Package.create`'s failure cleanup was written for a fresh install, where the
 * half-built tree is Publisher's to remove. A reload runs the same code against
 * a directory that already exists and is already serving, so a model that
 * failed to compile deleted the user's package directory and evicted the
 * last-good compiled model, taking the package offline until it was
 * re-provisioned. That is reachable from the reload endpoint and the
 * malloy_reloadPackage MCP tool by any caller that saves a broken model and
 * reloads, which is the ordinary authoring mistake.
 *
 * Directory cleanup is now opt-in and only `installPackage` asks for it, so
 * these two guard the reload path. Run against a real `Environment` and a real
 * `Package.create` over temp dirs, the same way package_rollback.spec.ts does.
 */
describe("failed reload does not destroy a serving package", () => {
   let rootDir: string;
   let envPath: string;

   const GOOD_MODEL = `source: ones is duckdb.sql("SELECT 1 as x")\n`;
   const BROKEN_MODEL = `source: broken is @@@ not valid malloy !!!\n`;

   async function writePackageDir(dir: string, model: string): Promise<void> {
      await fs.mkdir(dir, { recursive: true });
      await fs.writeFile(
         path.join(dir, "publisher.json"),
         JSON.stringify({ name: "pkg", description: "reload fixture" }),
      );
      await fs.writeFile(path.join(dir, "model.malloy"), model);
   }

   async function copyDir(src: string, dst: string): Promise<void> {
      await fs.mkdir(dst, { recursive: true });
      await fs.cp(src, dst, { recursive: true });
   }

   beforeEach(async () => {
      rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "publisher-reload-"));
      envPath = path.join(rootDir, "env");
      await fs.mkdir(envPath, { recursive: true });
   });

   afterEach(async () => {
      await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
   });

   it("keeps the package directory on disk when the reloaded model does not compile", async () => {
      const env = await Environment.create("testEnv", envPath, []);
      const pkgDir = path.join(envPath, "pkg");
      await writePackageDir(pkgDir, GOOD_MODEL);
      await env.addPackage("pkg");

      // The ordinary authoring mistake: save something that does not compile,
      // then reload.
      await fs.writeFile(path.join(pkgDir, "model.malloy"), BROKEN_MODEL);
      await expect(env.getPackage("pkg", true)).rejects.toThrow();

      // The directory and the file the user was editing both survive.
      const modelText = await fs.readFile(
         path.join(pkgDir, "model.malloy"),
         "utf-8",
      );
      expect(modelText).toBe(BROKEN_MODEL);
   });

   it("keeps serving the last good model after a failed reload", async () => {
      const env = await Environment.create("testEnv", envPath, []);
      const pkgDir = path.join(envPath, "pkg");
      await writePackageDir(pkgDir, GOOD_MODEL);
      await env.addPackage("pkg");

      const servingBefore = await env.getPackage("pkg", false);

      await fs.writeFile(path.join(pkgDir, "model.malloy"), BROKEN_MODEL);
      await expect(env.getPackage("pkg", true)).rejects.toThrow();

      // The package is still loaded and answerable: a failed reload reports the
      // compile error, it does not take the package down. Assert the exact
      // state, not just that something is there: a status stranded at LOADING
      // would satisfy toBeDefined() while listPackages skips it, so the package
      // would answer getPackage and be invisible to listings and discovery.
      const stillServing = await env.getPackage("pkg", false);
      expect(stillServing).toBe(servingBefore);
      expect(env.getPackageStatus("pkg")?.status).toBe(PackageStatus.SERVING);
      expect((await env.listPackages()).map((p) => p.name)).toContain("pkg");
   });

   it("keeps a rolled-back reinstall listed and serving", async () => {
      // The other reload path. A package with an install location reloads via
      // installPackage, whose rollback restores the previous tree but used to
      // drop the status while `packages` kept the old package: it answered
      // getPackage but vanished from listPackages and discovery until a
      // restart. Both maps must agree that it is still serving.
      const env = await Environment.create("testEnv", envPath, []);
      const goodFixture = path.join(rootDir, "good");
      const brokenFixture = path.join(rootDir, "broken");
      await writePackageDir(goodFixture, GOOD_MODEL);
      await writePackageDir(brokenFixture, BROKEN_MODEL);

      await env.installPackage("pkg", (stagingPath) =>
         copyDir(goodFixture, stagingPath),
      );
      const servingBefore = await env.getPackage("pkg", false);

      await expect(
         env.installPackage("pkg", (stagingPath) =>
            copyDir(brokenFixture, stagingPath),
         ),
      ).rejects.toThrow();

      expect(env.getPackageStatus("pkg")?.status).toBe(PackageStatus.SERVING);
      expect((await env.listPackages()).map((p) => p.name)).toContain("pkg");
      expect(await env.getPackage("pkg", false)).toBe(servingBefore);
      // The on-disk half of the same claim: the rejected tree is gone and the
      // user's is back. Without this, nothing pins "your files are left alone".
      expect(
         await fs.readFile(path.join(envPath, "pkg", "model.malloy"), "utf-8"),
      ).toBe(GOOD_MODEL);
   });

   it("does not advertise a package as serving when the rollback could not restore it", async () => {
      // Keeping the last-good package served is only honest when the rollback
      // actually put its tree back. If nothing was restored, the cached package
      // no longer matches disk, and claiming SERVING would advertise a package
      // over a canonical path that is missing or still holds rejected content.
      const env = await Environment.create("testEnv", envPath, []);
      const pkgDir = path.join(envPath, "pkg");
      await writePackageDir(pkgDir, GOOD_MODEL);
      await env.addPackage("pkg");
      expect(env.getPackageStatus("pkg")?.status).toBe(PackageStatus.SERVING);

      // The tree disappears from under Publisher, so the reinstall has nothing
      // to retire and the rollback has nothing to restore.
      await fs.rm(pkgDir, { recursive: true, force: true });

      const brokenFixture = path.join(rootDir, "broken-nofallback");
      await writePackageDir(brokenFixture, BROKEN_MODEL);
      await expect(
         env.installPackage("pkg", (stagingPath) =>
            copyDir(brokenFixture, stagingPath),
         ),
      ).rejects.toThrow();

      // Eviction is the honest outcome here, and both maps must agree on it.
      expect(env.getPackageStatus("pkg")).toBeUndefined();
      expect((await env.listPackages()).map((p) => p.name)).not.toContain(
         "pkg",
      );
   });

   it("recovers on the next reload once the model compiles again", async () => {
      const env = await Environment.create("testEnv", envPath, []);
      const pkgDir = path.join(envPath, "pkg");
      await writePackageDir(pkgDir, GOOD_MODEL);
      await env.addPackage("pkg");

      await fs.writeFile(path.join(pkgDir, "model.malloy"), BROKEN_MODEL);
      await expect(env.getPackage("pkg", true)).rejects.toThrow();

      // Because nothing was deleted, fixing the model and reloading is enough.
      await fs.writeFile(path.join(pkgDir, "model.malloy"), GOOD_MODEL);
      const reloaded = await env.getPackage("pkg", true);
      expect(reloaded).toBeDefined();
   });
});
