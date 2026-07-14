import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs/promises";
import * as os from "os";
import * as path from "path";

import { Environment } from "./environment";

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

      await fs.writeFile(path.join(pkgDir, "model.malloy"), BROKEN_MODEL);
      await expect(env.getPackage("pkg", true)).rejects.toThrow();

      // The package is still loaded and answerable: a failed reload reports the
      // compile error, it does not take the package down.
      const stillServing = await env.getPackage("pkg", false);
      expect(stillServing).toBeDefined();
      expect(env.getPackageStatus("pkg")).toBeDefined();
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
