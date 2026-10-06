// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What `compile_model` at scope "package" reports about dashboards.
 *
 * Every defect in `tests/fixtures/dashboards-lint` compiles cleanly: a tile
 * naming a view that does not exist, a `# dashboard { columns }` that is not a
 * number, a `# drill` pointing at nothing. Only the package's load-time checks
 * see them. The fixture is reused because it is that exact set, already pinned
 * on the load path by `tests/integration/dashboards`; if a case is removed
 * from it, the two suites disagree.
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
      expect((await messages()).join("\n")).toContain(
         'tile "ghost -> x" does not resolve',
      );
   });

   it("reports a grid width that is not a positive integer", async () => {
      expect((await messages()).join("\n")).toContain(
         "must be a positive integer",
      );
   });

   it("reports a drill pointing at no dashboard in the package", async () => {
      expect((await messages()).join("\n")).toContain("no_such_dashboard");
   });

   it("does not report a drill that resolves, even to an oddly named file", async () => {
      // `v1.2` is served despite its name, so its drill is not dangling. This
      // is the one the naming advisory must not leak into.
      const joined = (await messages()).join("\n");
      expect(joined).not.toContain('targets "v1.2"');
   });

   it("reports a suggest naming a query nothing defines", async () => {
      expect((await messages()).join("\n")).toContain("nowhere");
   });

   /**
    * The contract that decides whether an agent can use this in a loop: these
    * findings never fail a package load, so compile must not call them errors.
    * If it did, an edit the server would serve happily would look rejected.
    */
   it("reports the dashboard lint as warnings, under its own code", async () => {
      const { problems } = await compilePackage();
      const lint = problems.filter(
         (p) => (p as { code?: string }).code === "dashboard-lint",
      );

      expect(lint.map((p) => p.message).join("\n")).toContain("missing_view");
      expect(lint.map((p) => p.message).join("\n")).toContain(
         "no_such_dashboard",
      );
      for (const finding of lint) expect(finding.severity).toBe("warn");
   });

   /**
    * The notebook lint already reports an unparsed `## artifact` on a
    * dashboard file as an error, with a line. The dashboard lint says the same
    * thing, and the load path keeps only one; so does compile.
    */
   it("reports an unparsed dashboard tag once, as the notebook lint's error", async () => {
      const { problems } = await compilePackage();
      const unparsed = problems.filter(
         (p) =>
            p.model === "dashboards/malformed.malloy" &&
            p.message.includes("does not parse"),
      );

      expect(unparsed).toHaveLength(1);
      expect((unparsed[0] as { code?: string }).code).toBe(
         "notebook-artifact-unparsed",
      );
      expect(unparsed[0].severity).toBe("error");
   });

   it("leaves a package-wide finding unattributed rather than pinned on a file", async () => {
      const { problems } = await compilePackage();
      const drill = problems.find((p) =>
         p.message.includes("no_such_dashboard"),
      );

      expect(drill).toBeDefined();
      expect("model" in (drill as object)).toBe(false);
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
    * additional. It must neither swallow them nor be dropped because of them.
    */
   it("returns compiler errors and the dashboard lint side by side", async () => {
      const { problems } = await env.compileSource(
         "dashboards-lint",
         "dashboards/broken.malloy",
         (await fs.readFile(
            path.join(FIXTURE, "dashboards", "broken.malloy"),
            "utf8",
         )) + "\nsource: bad is nonexistent_connection.table('nope')\n",
         false,
         undefined,
         "package",
      );

      expect(
         problems.some(
            (p) =>
               p.severity === "error" &&
               p.model === "dashboards/broken.malloy" &&
               (p as { code?: string }).code !== "notebook-artifact-unparsed",
         ),
      ).toBe(true);
      expect(problems.map((p) => p.message).join("\n")).toContain(
         "no_such_dashboard",
      );
   });
});

/**
 * The findings that depend on how the package is SERVED rather than on what
 * compiles: a package whose `index.malloy` curates the surface refuses a tile
 * reading a source that file does not export. The compiler cannot see it, and
 * it needs Package state, which is why the dry run loads the outcome into a
 * scratch package instead of re-deriving the surface.
 */
describe("compile_model, package scope: curation findings", () => {
   let rootDir: string;
   let env: Environment;

   const install = async (fixture: string) => {
      rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "publisher-curate-"));
      const envPath = path.join(rootDir, "env");
      await fs.mkdir(envPath, { recursive: true });
      env = await Environment.create("testEnv", envPath, []);
      await env.installPackage(fixture, async (stagingPath) => {
         await fs.cp(path.join(FIXTURE, "..", fixture), stagingPath, {
            recursive: true,
         });
      });
   };

   afterEach(async () => {
      await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
   });

   const compilePackage = (
      fixture: string,
      replacement?: { modelPath: string; source: string },
   ) =>
      env.compileSource(
         fixture,
         replacement?.modelPath ?? "index.malloy",
         replacement?.source,
         false,
         undefined,
         "package",
      );

   const refusals = <T extends { message: string }>(problems: T[]) =>
      problems.filter((p) => p.message.includes("won't load"));

   it("reports a tile reading a source the package's surface does not export", async () => {
      await install("dashboards-convention");
      const { problems } = await compilePackage("dashboards-convention");
      const refused = problems.filter((p) =>
         p.message.includes("orders_staging"),
      );

      expect(refused.length).toBeGreaterThan(0);
      for (const finding of refused) expect(finding.severity).toBe("warn");
      expect(refused.map((p) => p.model)).toContain("dashboards/tiles.malloy");
   });

   it("reports only the intended findings for a well-formed convention package", async () => {
      await install("dashboards-convention");
      const { problems } = await compilePackage("dashboards-convention");

      // The fixture's artifact tags are well formed, so the only refusals are
      // the three the surface withholds. A malformed tag shows up here as an
      // "Unknown render tag" on the dashboard.
      expect(refusals(problems)).toHaveLength(3);
      expect(
         problems.some((p) => p.message.includes("Unknown render tag")),
      ).toBe(false);
   });

   /**
    * The edit is what gets judged, not the saved file. A new dashboard that
    * declares its own source on top of an exported one may read it; judged
    * against the disk, where the file does not exist yet, its source was
    * unknown and the tile was refused with a fix that could not work.
    */
   it("judges a what-if dashboard by its replacement text, not the saved file", async () => {
      await install("dashboards-convention");
      const { problems } = await compilePackage("dashboards-convention", {
         modelPath: "dashboards/new.malloy",
         source: [
            'import "../orders.malloy"',
            "source: b is orders extend {}",
            '## artifact { title="New" tiles=["b -> by_status"] }',
            "",
         ].join("\n"),
      });

      expect(
         refusals(problems).filter((p) => p.model === "dashboards/new.malloy"),
      ).toEqual([]);
      // The three on the saved files are still reported.
      expect(refusals(problems)).toHaveLength(3);
   });

   it("still refuses a what-if dashboard that re-bases onto a hidden source", async () => {
      await install("dashboards-convention");
      const { problems } = await compilePackage("dashboards-convention", {
         modelPath: "dashboards/new.malloy",
         source: [
            'import "../orders.malloy"',
            "source: b is orders_staging extend {}",
            '## artifact { title="New" tiles=["b -> by_flag"] }',
            "",
         ].join("\n"),
      });

      const mine = refusals(problems).filter(
         (p) => p.model === "dashboards/new.malloy",
      );
      expect(mine).toHaveLength(1);
      expect(mine[0].message).toContain("reads orders_staging");
   });

   it("reports nothing of the kind for a package with no curated surface", async () => {
      await install("dashboards-lint");
      const { problems } = await compilePackage("dashboards-lint");

      expect(
         problems.filter((p) => p.message.includes("doesn't export")),
      ).toEqual([]);
   });
});
