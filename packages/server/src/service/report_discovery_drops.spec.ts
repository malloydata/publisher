// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The load path's handling of a report it cannot read, pinned on a real
 * package: a report dropped because reading it threw, one dropped because it
 * produced no facts and no error, a grid report that does not compile (listed
 * anyway, carrying its error), and a lint that stops early. Each is a way a
 * report could vanish with nothing said, which is what these lock out.
 */
import { afterEach, beforeEach, describe, expect, it, spyOn } from "bun:test";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { logger } from "../logger";
import type { components } from "../api";
import * as dashboard from "./dashboard";
import { Environment } from "./environment";
import { Model } from "./model";
import { Package } from "./package";
import { getDashboard } from "./legacy_report_views";

type ApiPackageWarning = NonNullable<
   components["schemas"]["Package"]["warnings"]
>[number];

/** Whether `logger.warn` logged `message` for `modelPath`. */
const logged = (
   warn: { mock: { calls: unknown[][] } },
   message: string,
   modelPath: string,
) =>
   warn.mock.calls.some(
      ([text, meta]) =>
         text === message &&
         (meta as { modelPath?: string } | undefined)?.modelPath === modelPath,
   );

const BASE = `source: s is duckdb.sql("select 1 as x") extend {
   view: v is { group_by: x }
}
`;

const FILES: Record<string, string> = {
   "publisher.json": JSON.stringify({ name: "drops", version: "1.0.0" }),
   "base.malloy": BASE,
   "grid.malloy": `## artifact { layout=grid tiles=["s -> v"] }\nimport "base.malloy"\n`,
   "story.malloy": `## artifact { layout=linear }\nimport "base.malloy"\nrun: s -> v\n`,
};

/** A grid report that does not compile. A first install refuses a package with one, so it arrives by a reload, which keeps a failed model (`reloadAllModels`). */
const BROKEN = `## artifact { layout=grid tiles=["s -> v"] }\nsource: t is nowhere.table('x')\n`;

const DROPPED = (modelPath: string) =>
   `"${modelPath}" carries an artifact tag but could not be read, so it is not served and there is no report at that path. The cause is in the server log. Reload the package to try again.`;

describe("report discovery: what it does with a report it cannot read", () => {
   let rootDir: string;
   let env: Environment;
   const spies: { mockRestore(): void }[] = [];

   beforeEach(async () => {
      rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "publisher-drops-"));
      const envPath = path.join(rootDir, "env");
      await fs.mkdir(envPath, { recursive: true });
      env = await Environment.create("testEnv", envPath, []);
   });

   afterEach(async () => {
      for (const spy of spies.splice(0)) spy.mockRestore();
      await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
   });

   const install = async (): Promise<Package> => {
      await env.installPackage("drops", async (stagingPath) => {
         await fs.mkdir(stagingPath, { recursive: true });
         for (const [name, text] of Object.entries(FILES))
            await fs.writeFile(path.join(stagingPath, name), text);
      });
      return env.getPackage("drops", false);
   };

   const warningsOf = (pkg: Package): ApiPackageWarning[] =>
      (pkg.getPackageMetadata().warnings ?? []) as ApiPackageWarning[];

   const listed = (pkg: Package) =>
      pkg.listReports().map((report) => [report.path, report.layout]);

   it("serves every readable report, so the cases below are drops, not absences", async () => {
      const pkg = await install();
      expect(listed(pkg)).toEqual([
         ["grid.malloy", "grid"],
         ["story.malloy", "linear"],
      ]);
      expect(
         warningsOf(pkg).filter((w) =>
            w.message?.includes("could not be read"),
         ),
      ).toEqual([]);
   });

   it("lists a grid report that does not compile, carrying its compile error", async () => {
      const pkg = await install();
      await fs.writeFile(
         path.join(rootDir, "env", "drops", "broken.malloy"),
         BROKEN,
      );
      await pkg.reloadAllModels({});
      expect(listed(pkg)).toEqual([
         ["broken.malloy", "grid"],
         ["grid.malloy", "grid"],
         ["story.malloy", "linear"],
      ]);
      const [entry] = pkg
         .listReports()
         .filter((report) => report.path === "broken.malloy");
      expect(entry).toMatchObject({
         path: "broken.malloy",
         name: "broken",
         layout: "grid",
         title: "broken",
         editable: true,
      });
      expect(entry.error).toContain("nowhere");
      // The stub manifest the deprecated route serves for it.
      expect(await getDashboard(pkg, "env", "broken")).toMatchObject({
         name: "broken",
         layout: "grid",
         kind: "dashboard",
         title: "broken",
         autorun: true,
         path: "broken.malloy",
         givens: [],
         tiles: undefined,
      });
      expect((await getDashboard(pkg, "env", "broken"))?.error).toContain(
         "nowhere",
      );
      const report = await pkg.getReport("broken.malloy");
      expect(report?.error).toContain("nowhere");
      expect(report?.tiles).toBeUndefined();
   });

   it("drops a grid report whose manifest cannot be built, and says so", async () => {
      const real = dashboard.buildDashboardManifest;
      spies.push(
         spyOn(dashboard, "buildDashboardManifest").mockImplementation(
            (facts, options) => {
               if (facts.modelPath === "grid.malloy")
                  throw new Error("simulated manifest failure");
               return real(facts, options);
            },
         ),
      );
      const warn = spyOn(logger, "warn");
      spies.push(warn);
      const pkg = await install();

      expect(listed(pkg)).toEqual([["story.malloy", "linear"]]);
      expect(warningsOf(pkg)).toContainEqual({
         model: "grid.malloy",
         subject: "grid",
         message: DROPPED("grid.malloy"),
         severity: "error",
      });
      expect(logged(warn, "Report discovery failed", "grid.malloy")).toBe(true);
   });

   for (const modelPath of ["grid.malloy", "story.malloy"]) {
      it(`drops ${modelPath} when it produced no facts and no compile error, and says so`, async () => {
         const real = Model.prototype.getCompiledDashboardModelFacts;
         spies.push(
            spyOn(
               Model.prototype,
               "getCompiledDashboardModelFacts",
            ).mockImplementation(function (this: Model) {
               if (this.getPath() === modelPath)
                  return Promise.resolve(undefined);
               return real.call(this);
            }),
         );
         const warn = spyOn(logger, "warn");
         spies.push(warn);
         const pkg = await install();

         expect(listed(pkg).map(([p]) => p)).not.toContain(modelPath);
         expect(warningsOf(pkg)).toContainEqual({
            model: modelPath,
            subject: modelPath.replace(/\.malloy$/, ""),
            message: DROPPED(modelPath),
            severity: "error",
         });
         expect(
            logged(
               warn,
               "Report file produced no facts and no error",
               modelPath,
            ),
         ).toBe(true);
      });
   }

   it("says the lint stopped early when a check throws, keeping the drops", async () => {
      const real = dashboard.buildDashboardManifest;
      spies.push(
         spyOn(dashboard, "buildDashboardManifest").mockImplementation(
            (facts, options) => {
               if (facts.modelPath === "grid.malloy")
                  throw new Error("simulated manifest failure");
               return real(facts, options);
            },
         ),
      );
      const proto = Package.prototype as unknown as {
         unsupportedComponentWarnings: () => Promise<ApiPackageWarning[]>;
      };
      spies.push(
         spyOn(proto, "unsupportedComponentWarnings").mockRejectedValue(
            new Error("simulated lint failure"),
         ),
      );
      const pkg = await install();
      const warnings = warningsOf(pkg);

      expect(
         warnings.filter((w) =>
            w.message?.startsWith("Report lint stopped early"),
         ),
      ).toEqual([
         {
            message:
               "Report lint stopped early, so this list is incomplete: an " +
               "unknown subset of the tile, report, drill, given, curation " +
               "and unsupported-component checks did not run. Treat a missing " +
               "finding of any of those kinds as unknown rather than clean. " +
               "Reload the package to run the lint again. The reports " +
               "themselves are unaffected, because they are discovered " +
               'before the lint runs. An operator can find the cause in the server log under "Report lint failed".',
            severity: "warn",
         },
      ]);
      // A drop is pushed before any check that can throw, so it survives.
      expect(warnings).toContainEqual({
         model: "grid.malloy",
         subject: "grid",
         message: DROPPED("grid.malloy"),
         severity: "error",
      });
   });
});
