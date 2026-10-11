// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

/**
 * The report write path over real HTTP (`docs/reports-design.md` §7): create
 * anywhere (at the root, and nested in a directory that does not exist yet),
 * switch a report's layout, and refuse to replace a model, an untagged file, a
 * `.malloynb` or `index.malloy`, or to write under `public/`.
 *
 * Runs against a copy of `reports-anywhere`, since these tests write to it.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import fsSync from "fs";
import fs from "fs/promises";
import path from "path";
import {
   fixture,
   hashOf,
   startPackageEnv,
   type PackageEnv,
} from "../../harness/reports";

const PKG = "reports-anywhere";

const grid = (title: string, importPath: string) =>
   `## artifact { layout=grid title="${title}" tiles=["orders -> by_brand"] }\n` +
   `import { orders } from "${importPath}"\n`;

const BROKEN_TAG =
   `## artifact { layout=grid title="Broken" tiles: ["orders -> by_brand"] }\n` +
   `import { orders } from "orders.malloy"\n`;

describe("PUT model source: reports anywhere (E2E)", () => {
   let env: PackageEnv;
   const put = (modelPath: string, body: unknown) => env.put(modelPath, body);
   const served = (modelPath: string) => env.served(modelPath);
   const reportsByPath = async () =>
      Object.fromEntries(
         (
            await env.getJson<{ path: string; layout: string }[]>(
               env.api("/reports"),
            )
         ).map((r) => [r.path, r.layout]),
      );

   beforeAll(async () => {
      env = await startPackageEnv(
         "report-write-env",
         { [PKG]: fixture(PKG) },
         {
            prepare: async (dir) => {
               await fs.writeFile(
                  path.join(dir, "legacy.malloynb"),
                  ">>>markdown\n# Legacy\n",
               );
               // A report whose own tag does not parse (`tiles:` is not tag
               // syntax): it compiles, so discovery does not classify it, yet
               // it is the author's report to fix.
               await fs.writeFile(
                  path.join(dir, "broken_tag.malloy"),
                  BROKEN_TAG,
               );
            },
         },
      );
   }, 300_000);

   afterAll(() => env?.stop());

   it("creates a report two directories deep, making the directories", async () => {
      const res = await put("teams/east/weekly.malloy", {
         source: grid("Weekly", "../../orders.malloy"),
      });
      expect(res.status).toBe(201);
      expect((await reportsByPath())["teams/east/weekly.malloy"]).toBe("grid");
      expect(fsSync.existsSync(served("teams/east/weekly.malloy"))).toBe(true);
   });

   it("creates no directory for a write it refuses", async () => {
      // A tag only inside a block comment is no tag, as the compiler reads it,
      // so the write is refused before anything touches disk.
      const res = await put("rolled/back/x.malloy", {
         source:
            "/*\n## artifact { layout=grid }\n*/\nsource: s is duckdb.sql('select 1 as x')\n",
      });
      expect(res.status).toBe(400);
      expect(fsSync.existsSync(served("rolled"))).toBe(false);
   });

   it("switches a report's layout with one property, and keeps it served", async () => {
      const before = grid("Root", "orders.malloy");
      const after = before.replace("layout=grid", "layout=linear");
      expect((await put("new_root.malloy", { source: before })).status).toBe(
         201,
      );
      expect((await reportsByPath())["new_root.malloy"]).toBe("grid");
      const res = await put("new_root.malloy", {
         source: after,
         expectedHash: hashOf(before),
      });
      expect(res.status).toBe(200);
      expect((await reportsByPath())["new_root.malloy"]).toBe("linear");
   });

   it("writes a report whose basename another report has", async () => {
      const res = await put("teams/summary.malloy", {
         source: grid("Team summary", "../orders.malloy"),
      });
      expect(res.status).toBe(201);
      const reports = await reportsByPath();
      expect(reports["teams/summary.malloy"]).toBe("grid");
      expect(reports["sales/summary.malloy"]).toBe("grid");
   });

   it.each([
      ["a model", "orders.malloy"],
      ["an untagged file", "shared/helpers.malloy"],
      ["the root index.malloy", "index.malloy"],
      ["a .malloynb", "legacy.malloynb"],
      ["a file in public/", "public/hidden_report.malloy"],
   ])("refuses to replace %s", async (_name, modelPath) => {
      const before = fsSync.readFileSync(served(modelPath), "utf8");
      const res = await put(modelPath, {
         source: grid("Clobber", "orders.malloy"),
         expectedHash: hashOf(before),
      });
      expect(res.status).toBe(400);
      expect(fsSync.readFileSync(served(modelPath), "utf8")).toBe(before);
   });

   it("replaces a report whose own artifact tag does not parse, so the fix can be saved", async () => {
      expect((await reportsByPath())["broken_tag.malloy"]).toBeUndefined();
      const res = await put("broken_tag.malloy", {
         source: grid("Fixed", "orders.malloy"),
         expectedHash: hashOf(BROKEN_TAG),
      });
      expect(res.status).toBe(200);
      expect((await reportsByPath())["broken_tag.malloy"]).toBe("grid");
   });
});
