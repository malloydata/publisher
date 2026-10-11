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
import { createHash } from "crypto";
import fsSync from "fs";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";

const FIXTURE = path.resolve(__dirname, "../../fixtures/reports-anywhere");
const ENV_NAME = "report-write-env";
const PKG = "reports-anywhere";

const hashOf = (text: string) =>
   createHash("sha256").update(text, "utf8").digest("hex");

const grid = (title: string, importPath: string) =>
   `## artifact { layout=grid title="${title}" tiles=["orders -> by_brand"] }\n` +
   `import { orders } from "${importPath}"\n`;

const BROKEN_TAG =
   `## artifact { layout=grid title="Broken" tiles: ["orders -> by_brand"] }\n` +
   `import { orders } from "orders.malloy"\n`;

describe("PUT model source: reports anywhere (E2E)", () => {
   let env: (RestE2EEnv & { stop(): Promise<void> }) | null = null;
   let baseUrl: string;
   let location: string;

   const api = (sub = "") =>
      `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${PKG}${sub}`;
   const put = (modelPath: string, body: unknown) =>
      fetch(api(`/models/${modelPath}`), {
         method: "PUT",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify(body),
      });
   const reportsByPath = async () =>
      Object.fromEntries(
         (
            (await (await fetch(api("/reports"))).json()) as {
               path: string;
               layout: string;
            }[]
         ).map((r) => [r.path, r.layout]),
      );
   /** The package's own copy of a file: where writes land. */
   const served = (modelPath: string) =>
      path.join(
         path.resolve(__dirname, "../../../publisher_data"),
         ENV_NAME,
         PKG,
         modelPath,
      );

   beforeAll(async () => {
      env = await startRestE2E();
      baseUrl = env.baseUrl;
      location = await fs.mkdtemp(
         path.join(os.tmpdir(), "publisher-report-write-"),
      );
      await fs.cp(FIXTURE, location, { recursive: true });
      await fs.writeFile(
         path.join(location, "legacy.malloynb"),
         ">>>markdown\n# Legacy\n",
      );
      // A report whose own tag does not parse (`tiles:` is not tag syntax):
      // it compiles, so discovery does not classify it, yet it is the
      // author's report to fix.
      await fs.writeFile(path.join(location, "broken_tag.malloy"), BROKEN_TAG);
      const created = await fetch(`${baseUrl}/api/v0/environments`, {
         method: "POST",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({
            name: ENV_NAME,
            packages: [{ name: PKG, location }],
            connections: [],
         }),
      });
      if (!created.ok) {
         throw new Error(
            `Failed to create test environment (${created.status}): ${await created.text()}`,
         );
      }
   }, 300_000);

   afterAll(async () => {
      if (env) {
         await fetch(`${baseUrl}/api/v0/environments/${ENV_NAME}`, {
            method: "DELETE",
         });
         await env.stop();
      }
      if (location) await fs.rm(location, { recursive: true, force: true });
   });

   it("creates a report at the package root", async () => {
      const res = await put("new_root.malloy", {
         source: grid("Root", "orders.malloy"),
      });
      expect(res.status).toBe(201);
      expect((await reportsByPath())["new_root.malloy"]).toBe("grid");
   });

   it("creates a report two directories deep, making the directories", async () => {
      const res = await put("teams/east/weekly.malloy", {
         source: grid("Weekly", "../../orders.malloy"),
      });
      expect(res.status).toBe(201);
      expect((await reportsByPath())["teams/east/weekly.malloy"]).toBe("grid");
      expect(fsSync.existsSync(served("teams/east/weekly.malloy"))).toBe(true);
   });

   it("takes back the directories a create made when the reload refuses it", async () => {
      // Tagged text that compiles as a file but only inside a block comment:
      // the reloaded package does not serve it, so the write is rolled back.
      const res = await put("rolled/back/x.malloy", {
         source:
            "/*\n## artifact { layout=grid }\n*/\nsource: s is duckdb.sql('select 1 as x')\n",
      });
      expect(res.status).toBe(500);
      expect(fsSync.existsSync(served("rolled"))).toBe(false);
   });

   it("switches a report's layout with one property, and keeps it served", async () => {
      const before = grid("Root", "orders.malloy");
      const after = before.replace("layout=grid", "layout=linear");
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

   it.each([
      ["a model", "orders.malloy", "orders.malloy"],
      ["an untagged file", "shared/helpers.malloy", "../orders.malloy"],
   ])(
      "answers a create onto %s with 409, so a create tries the next name",
      async (_name, modelPath, importPath) => {
         // A create-only write (no expectedHash) onto any existing file is a
         // conflict: the path is taken. createDocument retries on 409 only.
         const before = fsSync.readFileSync(served(modelPath), "utf8");
         const source =
            modelPath === "orders.malloy"
               ? // Self-contained: importing itself would not compile.
                 `## artifact { layout=grid tiles=["s -> v"] }\n` +
                 `source: s is duckdb.sql("select 1 as x") extend { view: v is { select: x } }\n`
               : grid("Clobber", importPath);
         const res = await put(modelPath, { source });
         expect(res.status).toBe(409);
         expect(fsSync.readFileSync(served(modelPath), "utf8")).toBe(before);
      },
   );

   it("replaces a report whose own artifact tag does not parse, so the fix can be saved", async () => {
      expect((await reportsByPath())["broken_tag.malloy"]).toBeUndefined();
      const res = await put("broken_tag.malloy", {
         source: grid("Fixed", "orders.malloy"),
         expectedHash: hashOf(BROKEN_TAG),
      });
      expect(res.status).toBe(200);
      expect((await reportsByPath())["broken_tag.malloy"]).toBe("grid");
   });

   it("refuses to create a report under public/", async () => {
      const res = await put("public/new.malloy", {
         source: grid("Public", "../orders.malloy"),
      });
      expect(res.status).toBe(400);
      expect(fsSync.existsSync(served("public/new.malloy"))).toBe(false);
   });

   it("refuses untagged text anywhere", async () => {
      const res = await put("teams/plain.malloy", {
         source: "source: s is duckdb.sql('select 1 as x')\n",
      });
      expect(res.status).toBe(400);
      expect(fsSync.existsSync(served("teams/plain.malloy"))).toBe(false);
   });
});
