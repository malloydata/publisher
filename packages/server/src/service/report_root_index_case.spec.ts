// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A root `index.malloy` is never a report, whatever its letter case: the write
 * path refuses `Index.malloy` (on a case-insensitive file system it IS the
 * surface), so discovery must not serve one either, or it is a report that
 * can be opened but never saved. Which file decides the SURFACE is a separate
 * rule, and stays exact: a root `Index.malloy` publishes nothing
 * (`package_manifest.ts`).
 */
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { Environment } from "./environment";
import { isReportEligiblePath, reportWritePathProblem } from "./report";

const PACKAGE = "report-root-index-case";

describe("a tagged root index file in another letter case", () => {
   let rootDir: string;
   let env: Environment;

   beforeEach(async () => {
      rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "publisher-index-"));
      const envPath = path.join(rootDir, "env");
      await fs.mkdir(envPath, { recursive: true });
      env = await Environment.create("testEnv", envPath, []);
   });

   afterEach(async () => {
      await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
   });

   it("is no report path, as it is no writable one", () => {
      for (const p of ["Index.malloy", "INDEX.malloy", "./Index.malloy"]) {
         expect(isReportEligiblePath(p)).toBe(false);
         expect(reportWritePathProblem(p)).toBeDefined();
      }
      expect(isReportEligiblePath("sales/Index.malloy")).toBe(true);
   });

   it("is not served, and its tag is reported as ignored", async () => {
      await env.installPackage(PACKAGE, async (stagingPath) => {
         await fs.mkdir(stagingPath, { recursive: true });
         await fs.writeFile(
            path.join(stagingPath, "publisher.json"),
            JSON.stringify({ name: PACKAGE, version: "1.0.0" }),
         );
         await fs.writeFile(
            path.join(stagingPath, "base.malloy"),
            `source: s is duckdb.sql("select 1 as x") extend {\n` +
               `   view: v is { group_by: x }\n` +
               `}\n`,
         );
         await fs.writeFile(
            path.join(stagingPath, "Index.malloy"),
            `## artifact { layout=grid tiles=["s -> v"] }\nimport "base.malloy"\n`,
         );
      });
      const pkg = await env.getPackage(PACKAGE, false);

      expect(pkg.isServedReport("Index.malloy")).toBe(false);
      expect(pkg.listReports().map((r) => r.path)).toEqual([]);
      expect(
         (pkg.getPackageMetadata().warnings ?? []).filter(
            (w) => w.code === "index-is-not-a-report",
         ),
      ).toEqual([
         expect.objectContaining({ model: "Index.malloy", severity: "error" }),
      ]);
   });
});
