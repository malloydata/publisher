import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs/promises";
import * as os from "os";
import * as path from "path";

import { BadRequestError } from "../errors";
import { Environment } from "./environment";

/**
 * Regression tests for explores-validation ROLLBACK, guarding against the two
 * data-loss bugs a code review found:
 *
 *  1. No-location create rollback must NOT delete a pre-existing user
 *     directory. The controller now `unloadPackage`s (evict from memory, keep
 *     files) instead of `deletePackage` (which renames + rm's the tree).
 *
 *  2. A location update/reinstall with invalid explores must roll back to the
 *     PREVIOUS tree rather than swapping the rejected one in and 400-ing after.
 *     Validation now happens inside `installPackage`'s swap window.
 *
 * Both run against a real `Environment` + real `Package.create` over temp dirs.
 */
describe("explores-validation rollback (real Environment)", () => {
   let rootDir: string;
   let envPath: string;

   const GOOD_MODEL = `source: ones is duckdb.sql("SELECT 1 as x")\n`;
   const BAD_MODEL = `source: twos is duckdb.sql("SELECT 2 as y")\n`;

   async function writePackageDir(
      dir: string,
      manifest: Record<string, unknown>,
      model: string,
   ): Promise<void> {
      await fs.mkdir(dir, { recursive: true });
      await fs.writeFile(
         path.join(dir, "publisher.json"),
         JSON.stringify({
            name: "pkg",
            description: "rollback fixture",
            ...manifest,
         }),
      );
      await fs.writeFile(path.join(dir, "model.malloy"), model);
   }

   async function copyDir(src: string, dst: string): Promise<void> {
      await fs.mkdir(dst, { recursive: true });
      await fs.cp(src, dst, { recursive: true });
   }

   beforeEach(async () => {
      rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "publisher-rollback-"));
      envPath = path.join(rootDir, "env");
      await fs.mkdir(envPath, { recursive: true });
   });

   afterEach(async () => {
      await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
   });

   it("Fix 1: unloadPackage evicts from memory but keeps the on-disk directory", async () => {
      // Mimics the no-location create path: a pre-existing package directory is
      // registered, then rolled back. unloadPackage must not touch the files.
      const env = await Environment.create("testEnv", envPath, []);
      const pkgDir = path.join(envPath, "pkg");
      await writePackageDir(pkgDir, {}, GOOD_MODEL);

      await env.addPackage("pkg");
      expect(env.getPackageStatus("pkg")).toBeDefined();

      await env.unloadPackage("pkg");

      // Evicted from memory...
      expect(env.getPackageStatus("pkg")).toBeUndefined();
      // ...but the user's files are untouched.
      const modelText = await fs.readFile(
         path.join(pkgDir, "model.malloy"),
         "utf-8",
      );
      expect(modelText).toBe(GOOD_MODEL);
   });

   it("Fix 2: a rejected location reinstall restores the previous tree", async () => {
      const env = await Environment.create("testEnv", envPath, []);

      const goodFixture = path.join(rootDir, "good");
      const badFixture = path.join(rootDir, "bad");
      await writePackageDir(goodFixture, {}, GOOD_MODEL);
      await writePackageDir(
         badFixture,
         { explores: ["missing.malloy"] },
         BAD_MODEL,
      );

      // First install: the good tree is served.
      await env.installPackage("pkg", (stagingPath) =>
         copyDir(goodFixture, stagingPath),
      );
      expect(await env.getModelFileText("pkg", "model.malloy")).toBe(
         GOOD_MODEL,
      );

      // Reinstall with an invalid-explores tree, validated inside the swap.
      await expect(
         env.installPackage(
            "pkg",
            (stagingPath) => copyDir(badFixture, stagingPath),
            (pkg) => pkg.formatInvalidExplores(),
         ),
      ).rejects.toBeInstanceOf(BadRequestError);

      // The previous (good) tree is still the one on disk — the rejected tree
      // was rolled back, not swapped in.
      expect(await env.getModelFileText("pkg", "model.malloy")).toBe(
         GOOD_MODEL,
      );
   });

   it("a valid location reinstall still succeeds (no false rollback)", async () => {
      const env = await Environment.create("testEnv", envPath, []);
      const goodFixture = path.join(rootDir, "good");
      const nextFixture = path.join(rootDir, "next");
      await writePackageDir(goodFixture, {}, GOOD_MODEL);
      await writePackageDir(
         nextFixture,
         { explores: ["model.malloy"] },
         BAD_MODEL,
      );

      await env.installPackage("pkg", (stagingPath) =>
         copyDir(goodFixture, stagingPath),
      );
      await env.installPackage(
         "pkg",
         (stagingPath) => copyDir(nextFixture, stagingPath),
         (pkg) => pkg.formatInvalidExplores(),
      );
      expect(await env.getModelFileText("pkg", "model.malloy")).toBe(BAD_MODEL);
   });

   it("updatePackage normalizes a ./-prefixed body explores (no false 400, persists normalized)", async () => {
      // API-body explores must go through the same normalization the worker
      // applies to on-disk explores, so `./model.malloy` validates and persists
      // as `model.malloy` rather than being rejected with a misleading 404.
      const env = await Environment.create("testEnv", envPath, []);
      const pkgDir = path.join(envPath, "pkg");
      await writePackageDir(pkgDir, {}, GOOD_MODEL);
      await env.addPackage("pkg");

      const updated = await env.updatePackage("pkg", {
         name: "pkg",
         explores: ["./model.malloy"],
      });

      // Accepted (no BadRequestError) and stored in normalized form...
      expect(updated.explores).toEqual(["model.malloy"]);
      // ...both in memory and on disk.
      const manifest = JSON.parse(
         await fs.readFile(path.join(pkgDir, "publisher.json"), "utf-8"),
      );
      expect(manifest.explores).toEqual(["model.malloy"]);
   });

   it("compileSource rejects a notebook path; a model path still compiles", async () => {
      // /compile appends source to the target MODEL's content for context; a
      // notebook isn't a valid target and must be rejected up front (not left to
      // a confusing downstream parse error).
      const env = await Environment.create("testEnv", envPath, []);
      const pkgDir = path.join(envPath, "pkg");
      await writePackageDir(pkgDir, {}, GOOD_MODEL);
      await fs.writeFile(
         path.join(pkgDir, "report.malloynb"),
         `>>>markdown\n# Report\n`,
      );
      await env.addPackage("pkg");

      await expect(
         env.compileSource("pkg", "report.malloynb", "run: ones"),
      ).rejects.toBeInstanceOf(BadRequestError);

      // The .malloy model still compiles ad-hoc source (no regression).
      const { problems } = await env.compileSource(
         "pkg",
         "model.malloy",
         "run: ones -> { select: x }",
      );
      expect(problems.some((p) => p.severity === "error")).toBe(false);
   });
});
