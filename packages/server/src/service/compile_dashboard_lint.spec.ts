// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What `compile_model` at scope "package" reports about dashboards, pinned on
 * the fixtures the load path already uses.
 *
 * Every defect in `tests/fixtures/dashboards-lint` compiles cleanly: a tile
 * naming a view that does not exist, a `# dashboard { columns }` that is not a
 * number, a `# drill` pointing at nothing. Only the package's load-time checks
 * see them. The fixture is reused because it is that exact set, already pinned
 * on the load path by `tests/integration/dashboards`; if a case is removed
 * from it, the two suites disagree.
 *
 * `compile_scopes.spec.ts` pins that package scope reports what a real reload
 * reports. This file pins the findings' exact text on the fixtures, and what
 * the compile does when a check cannot run.
 */
import { afterEach, beforeEach, describe, expect, it, spyOn } from "bun:test";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { fileURLToPath } from "url";
import { logger } from "../logger";
import { Environment } from "./environment";
import { Model } from "./model";
import { Package } from "./package";
import { resetNotebookMetricsForTest } from "../notebook_metrics";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "../test_helpers/metrics_harness";

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

   it("leaves a package-wide finding unattributed rather than pinned on a file", async () => {
      const { problems } = await compilePackage();
      const drill = problems.find((p) =>
         p.message.includes("no_such_dashboard"),
      );

      expect(drill).toBeDefined();
      expect("model" in (drill as object)).toBe(false);
   });

   it("attributes a file-level finding to the file it is in", async () => {
      const { problems } = await compilePackage();
      const tile = problems.find((p) => p.message.includes("missing_view"));

      expect(tile?.model).toBe("dashboards/broken.malloy");
   });

   /**
    * A what-if dashboard that does not compile is still a dashboard, judged
    * by its replacement text: a drill to it is not dangling. Read from disk,
    * where the file does not exist, it was not registered and the drill was.
    */
   it("registers a what-if dashboard that does not compile, so a drill to it resolves", async () => {
      const { problems } = await env.compileSource(
         "dashboards-lint",
         "dashboards/no_such_dashboard.malloy",
         [
            '## artifact { title="Now exists" }',
            "source: broken is nonexistent_connection.table('nope')",
            "",
         ].join("\n"),
         false,
         undefined,
         "package",
      );
      const joined = problems.map((p) => p.message).join("\n");

      expect(joined).not.toContain('targets "no_such_dashboard"');
      // The control: the other dangling drill is still reported.
      expect(joined).toContain('targets "ghost"');
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
 * it needs Package state, which is why the compile loads the outcome into a
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

   const spies: { mockRestore(): void }[] = [];

   afterEach(async () => {
      for (const spy of spies.splice(0)) spy.mockRestore();
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

   const REFUSAL_FIX =
      "which index.malloy doesn't export, so it won't load. Fix: add " +
      "orders_staging to the export { ... } in index.malloy.";

   // The three findings the saved fixture carries: two tiles on `tiles` and
   // the single-query dashboard `hidden`, each reading orders_staging. They
   // keep the severity the load gives them, which is error.
   const SAVED_REFUSALS = [
      {
         model: "dashboards/hidden.malloy",
         severity: "error",
         message: `hidden: Dashboard hidden reads orders_staging, ${REFUSAL_FIX}`,
      },
      {
         model: "dashboards/tiles.malloy",
         severity: "error",
         message:
            "tiles: Tile orders_staging -> by_flag on dashboard tiles reads " +
            `orders_staging, ${REFUSAL_FIX}`,
      },
      {
         model: "dashboards/tiles.malloy",
         severity: "error",
         message:
            "tiles: Tile staged -> by_flag on dashboard tiles reads " +
            `orders_staging, ${REFUSAL_FIX}`,
      },
   ];

   const summarize = <
      T extends { model?: string; severity?: string; message: string },
   >(
      problems: T[],
   ) =>
      problems.map((p) => ({
         model: p.model,
         severity: p.severity,
         message: p.message,
      }));

   it("reports exactly the surface refusals for a well-formed convention package", async () => {
      await install("dashboards-convention");
      const { problems } = await compilePackage("dashboards-convention");

      // The fixture's artifact tags are well formed, so these are the only
      // findings of any kind. A malformed tag would add an "Unknown render
      // tag" on the dashboard.
      expect(summarize(problems)).toEqual(SAVED_REFUSALS);
   });

   it("reports a render-tag finding from the what-if text, under its own code", async () => {
      await install("dashboards-convention");
      const saved = await fs.readFile(
         path.join(FIXTURE, "..", "dashboards-convention", "orders.malloy"),
         "utf8",
      );
      const { problems } = await compilePackage("dashboards-convention", {
         modelPath: "orders.malloy",
         source: saved.replace(
            "  view: by_status is {",
            "  # no_such_tag\n  view: by_status is {",
         ),
      });

      expect(
         problems.filter(
            (p) =>
               p.model === "orders.malloy" &&
               (p as { code?: string }).code === "render-tag",
         ),
      ).toEqual([
         {
            code: "render-tag",
            severity: "warn",
            model: "orders.malloy",
            message:
               "orders -> by_status: Unknown render tag 'no_such_tag' on field 'root'",
         } as (typeof problems)[number],
      ]);
   });

   /**
    * A check that throws must not read as a clean package, and must not take
    * the compiler's own answer down with it. The compile still returns, and
    * says the dashboard findings are unknown.
    */
   it("says the findings are unknown when the dashboard checks throw", async () => {
      await install("dashboards-convention");
      const proto = Package.prototype as unknown as {
         discoverDashboards: () => Promise<void>;
      };
      spies.push(
         spyOn(proto, "discoverDashboards").mockRejectedValue(
            new Error("simulated lint failure"),
         ),
      );
      const result = await compilePackage("dashboards-convention");

      expect(result.problems).toEqual([
         {
            code: "dashboard-lint",
            severity: "warn",
            message:
               "The dashboard checks did not run, so their findings are " +
               "unknown rather than clean. Reload the package to see them. " +
               'The cause is in the server log under "Dashboard lint failed ' +
               'during compile".',
         } as (typeof result.problems)[number],
      ]);
   });

   it("costs a model that will not hydrate only its own findings", async () => {
      await install("dashboards-convention");
      const real = Model.fromSerialized.bind(Model);
      spies.push(
         spyOn(Model, "fromSerialized").mockImplementation(
            (...args: Parameters<typeof Model.fromSerialized>) => {
               if (args[3].modelPath === "dashboards/hidden.malloy") {
                  throw new Error("simulated hydration failure");
               }
               return real(...args);
            },
         ),
      );
      const { problems } = await compilePackage("dashboards-convention");

      expect(
         problems.filter(
            (p) =>
               p.model === "dashboards/hidden.malloy" &&
               (p as { code?: string }).code === "render-tag",
         ),
      ).toEqual([
         {
            code: "render-tag",
            severity: "error",
            model: "dashboards/hidden.malloy",
            message:
               "The load-time checks could not read this model (simulated " +
               "hydration failure), so its render-tag and dashboard findings are unknown " +
               "rather than clean.",
         } as (typeof problems)[number],
      ]);
      // The other files' findings survive.
      expect(
         summarize(
            problems.filter((p) => p.model === "dashboards/tiles.malloy"),
         ),
      ).toEqual(SAVED_REFUSALS.slice(1));
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
      expect(summarize(refusals(problems))).toEqual(SAVED_REFUSALS);
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

/**
 * The compile runs the load's discovery over a package nothing serves. The
 * notebook discovery counter means "a served package was discovered", so a
 * compile, which agents call in a loop, must leave it alone.
 */
describe("compile_model, package scope: telemetry", () => {
   let rootDir: string;
   let harness: MetricsHarness;

   beforeEach(async () => {
      harness = await startMetricsHarness();
      resetNotebookMetricsForTest();
   });

   afterEach(async () => {
      resetNotebookMetricsForTest();
      await harness.shutdown();
      await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
   });

   it("does not count a package-scope compile as a notebook discovery", async () => {
      rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "publisher-nbmetric-"));
      const envPath = path.join(rootDir, "env");
      await fs.mkdir(envPath, { recursive: true });
      const env = await Environment.create("testEnv", envPath, []);
      await env.installPackage("notebooks-malloyyo", async (stagingPath) => {
         await fs.cp(
            path.join(FIXTURE, "..", "notebooks-malloyyo"),
            stagingPath,
            {
               recursive: true,
            },
         );
      });
      const count = () =>
         harness.collectCounter("publisher_notebook_discovery_total", {
            format: "malloy",
         });
      const afterLoad = await count();
      // The control: loading the package did count, so the harness sees it.
      expect(afterLoad).toBeGreaterThan(0);

      await env.compileSource(
         "notebooks-malloyyo",
         "index.malloy",
         undefined,
         false,
         undefined,
         "package",
      );

      expect(await count()).toBe(afterLoad);
   });
});

/**
 * Hydration logs a model's `#(authorize)` warnings, once per hydration. A
 * package-scope compile hydrates every model on every call, so it asks for
 * them not to be logged again.
 */
describe("Model.fromSerialized: authorize warning log", () => {
   const serialized = {
      modelPath: "m.malloy",
      modelType: "model",
      authorizeWarnings: ["gate on x is not expressible"],
   } as unknown as Parameters<typeof Model.fromSerialized>[3];

   const hydrate = (skipAuthorizeWarningLog?: boolean): string[] => {
      const warn = spyOn(logger, "warn");
      try {
         Model.fromSerialized("pkg", "/unused", {} as never, serialized, {
            skipAuthorizeWarningLog,
         });
         return warn.mock.calls.map(([message]) => String(message));
      } finally {
         warn.mockRestore();
      }
   };

   it("logs the warnings by default", () => {
      expect(hydrate()).toEqual(["gate on x is not expressible"]);
   });

   it("does not log them when asked not to", () => {
      expect(hydrate(true)).toEqual([]);
   });
});
