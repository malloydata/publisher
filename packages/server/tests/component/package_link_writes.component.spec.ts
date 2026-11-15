// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Writes into a package never go through a link a package author planted.
 * A publisher.json swapped for a link to another writable JSON file (another
 * package's manifest, the server's config) must not let a PATCH rewrite it,
 * and a report write onto a planted link answers the same whatever the link
 * leads to. The package here is installed with no recorded location, so a
 * PATCH writes the manifest and a report write is allowed.
 */

import { afterAll, describe, expect, it } from "bun:test";
import fs from "fs";
import os from "os";
import path from "path";
import { BadRequestError, InvalidModelPathError } from "../../src/errors";
import { Environment } from "../../src/service/environment";
import { writePackageFiles } from "../../src/test_helpers/temp_package";

const roots: string[] = [];
afterAll(() => {
   for (const root of roots) fs.rmSync(root, { recursive: true, force: true });
});

async function installed(): Promise<{
   env: Environment;
   served: string;
   root: string;
}> {
   const root = fs.mkdtempSync(path.join(os.tmpdir(), "manifest-link-"));
   roots.push(root);
   const envPath = path.join(root, "env");
   fs.mkdirSync(envPath, { recursive: true });
   const env = await Environment.create("manifestEnv", envPath, []);
   await env.installPackage("pkg", async (staging) => {
      writePackageFiles(staging, {
         "ok.malloy": 'source: n is duckdb.sql("SELECT 1 AS answer")\n',
      });
   });
   return { env, served: path.join(envPath, "pkg"), root };
}

describe("a PATCH of a package whose publisher.json is a link", () => {
   it("refuses, and leaves the file the link leads to as it was", async () => {
      const { env, served, root } = await installed();
      const victim = path.join(root, "victim.json");
      const before = JSON.stringify({
         name: "victim",
         description: "unchanged",
      });
      fs.writeFileSync(victim, before);
      fs.rmSync(path.join(served, "publisher.json"));
      fs.symlinkSync(victim, path.join(served, "publisher.json"));

      let error: unknown;
      try {
         await env.updatePackage("pkg", { description: "overwritten" });
      } catch (e) {
         error = e;
      }
      expect(error).toBeInstanceOf(BadRequestError);
      expect((error as Error).message).not.toContain(victim);
      expect(fs.readFileSync(victim, "utf8")).toBe(before);
      expect(
         fs.lstatSync(path.join(served, "publisher.json")).isSymbolicLink(),
      ).toBe(true);
   });

   it("still writes a manifest that is a plain file, by replacing it", async () => {
      const { env, served } = await installed();
      await env.updatePackage("pkg", { description: "written" });
      const manifest = JSON.parse(
         fs.readFileSync(path.join(served, "publisher.json"), "utf8"),
      ) as { description?: string };
      expect(manifest.description).toBe("written");
      // No temporary file is left beside it.
      expect(fs.readdirSync(served).filter((f) => f.endsWith(".tmp"))).toEqual(
         [],
      );
   });
});

describe("a report write onto a link planted in the package", () => {
   it("is refused the same way whether the link's target exists or not", async () => {
      const { env, served, root } = await installed();
      const there = path.join(root, "rpt_a.malloy");
      const text = "## artifact { layout=linear }\n";
      fs.writeFileSync(there, text);
      const answers: string[] = [];
      for (const [name, target] of [
         ["one.malloy", there],
         ["two.malloy", path.join(root, "rpt_b.malloy")],
      ]) {
         fs.symlinkSync(target, path.join(served, name));
         let error: unknown;
         try {
            await env.writeModelFileTransactional(
               "pkg",
               name,
               '## artifact { layout=linear }\nimport "ok.malloy"\n',
               () => {},
               async () => undefined,
            );
         } catch (e) {
            error = e;
         }
         expect(error).toBeInstanceOf(InvalidModelPathError);
         answers.push((error as Error).message.replaceAll(name, "<f>"));
      }
      expect(answers[0]).toBe(answers[1]);
      expect(fs.readFileSync(there, "utf8")).toBe(text);
      expect(fs.existsSync(path.join(root, "rpt_b.malloy"))).toBe(false);
   });
});
