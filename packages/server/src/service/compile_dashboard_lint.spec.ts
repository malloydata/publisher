// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What `compile_model` at scope "package" reports about dashboards.
 *
 * The gap this pins: every file in `tests/fixtures/dashboards-lint` compiles
 * cleanly. A tile naming a view that does not exist, a `# dashboard { columns }`
 * that is not a number, a `# drill` pointing at nothing -- the Malloy compiler
 * has no opinion about any of them, so before this the authoring tool said
 * "success" right up until the page was served. That is why the fixture is
 * reused rather than a new one written: it is the exact set of defects the
 * compiler cannot see, already curated, and already pinned on the load path by
 * `tests/integration/dashboards`. If a case is removed from it, the two suites
 * disagree and that is worth noticing.
 */
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { fileURLToPath } from "url";
import { Environment } from "./environment";

const FIXTURE = path.join(
   path.dirname(fileURLToPath(import.meta.url)),
   "..",
   "..",
   "tests",
   "fixtures",
   "dashboards-lint",
);

describe("compile_model, package scope: dashboard and render-tag findings", () => {
   let rootDir: string;
   let env: Environment;

   beforeEach(async () => {
      rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "publisher-dashlint-"));
      const envPath = path.join(rootDir, "env");
      await fs.mkdir(envPath, { recursive: true });
      env = await Environment.create("testEnv", envPath, []);
      await env.installPackage("dashboards-lint", async (stagingPath) => {
         await fs.cp(FIXTURE, stagingPath, { recursive: true });
      });
   });

   afterEach(async () => {
      await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
   });

   const compilePackage = () =>
      env.compileSource(
         "dashboards-lint",
         "orders.malloy",
         undefined,
         false,
         undefined,
         "package",
      );

   const messages = async (): Promise<string[]> =>
      (await compilePackage()).problems.map((p) => p.message);

   it("reports a tile naming a view the source does not have", async () => {
      expect((await messages()).join("\n")).toContain("missing_view");
   });

   it("reports a tile naming a source the file does not import", async () => {
      expect((await messages()).join("\n")).toContain("ghost");
   });

   it("reports a grid width that is not a positive integer", async () => {
      expect((await messages()).join("\n")).toContain("columns");
   });

   it("reports a drill pointing at no dashboard in the package", async () => {
      expect((await messages()).join("\n")).toContain("no_such_dashboard");
   });

   it("does not report a drill that resolves, even to an oddly named file", async () => {
      // `v1.2` is served despite its name, so its drill is not dangling. This
      // is the one the naming advisory must not leak into.
      const joined = (await messages()).join("\n");
      expect(joined).not.toContain('"v1.2" is not a dashboard');
   });

   it("reports a suggest naming a query nothing defines", async () => {
      expect((await messages()).join("\n")).toContain("nowhere");
   });

   /**
    * The contract that decides whether an agent can use this in a loop: these
    * findings never fail a package load, so compile must not call them errors.
    * If it did, an edit the server would serve happily would look rejected.
    */
   it("reports them as warnings, so a clean compile still reads as success", async () => {
      const { problems } = await compilePackage();
      const dashboardFindings = problems.filter(
         (p) =>
            p.message.includes("missing_view") ||
            p.message.includes("no_such_dashboard"),
      );

      expect(dashboardFindings.length).toBeGreaterThan(0);
      for (const finding of dashboardFindings) {
         expect(finding.severity).toBe("warn");
      }
   });

   it("keeps the load-time severity in the message rather than dropping it", async () => {
      const joined = (await messages()).join("\n");
      expect(joined).toContain("reported as an error at package load");
   });

   it("attributes a file-level finding to the file it is in", async () => {
      const { problems } = await compilePackage();
      const tile = problems.find((p) => p.message.includes("missing_view"));

      expect(tile?.model).toBe("dashboards/broken.malloy");
   });

   /**
    * The compiler diagnostics are what the caller asked for; the lint is
    * additional. A lint that swallowed them, or that threw, would be worse than
    * not running at all.
    */
   it("still returns compiler diagnostics for a real compile error", async () => {
      const { problems } = await env.compileSource(
         "dashboards-lint",
         "orders.malloy",
         "source: bad is nonexistent_connection.table('nope')",
         false,
         undefined,
         "package",
      );

      expect(problems.some((p) => p.severity === "error")).toBe(true);
   });
});
