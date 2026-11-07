// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs/promises";
import * as os from "os";
import * as path from "path";

import { BadRequestError } from "../errors";
import { Environment } from "./environment";

/**
 * Regression test for publish-validation ROLLBACK, guarding against a
 * data-loss bug a code review found: a no-location create rollback must NOT
 * delete a pre-existing user directory. The controller `unloadPackage`s
 * (evict from memory, keep files) instead of `deletePackage` (which renames +
 * rm's the tree).
 *
 * It runs against a real `Environment` + real `Package.create` over temp dirs.
 */
describe("publish-validation rollback (real Environment)", () => {
   let rootDir: string;
   let envPath: string;

   const GOOD_MODEL = `source: ones is duckdb.sql("SELECT 1 as x")\n`;

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

   // DEPRECATED FEATURE COVERAGE — delete together with
   // `.malloynb` notebooks (see RELEASE_NOTES deprecations).
   describe("deprecated: .malloynb notebooks", () => {
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
});
