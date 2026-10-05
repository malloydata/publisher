// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Notebooks written as a tile layout (`## artifact { kind=notebook tiles=[…] }`)
 * and the kind-from-tag rule, driven through `Package.create` on the
 * package-load worker pool, the way a real load runs.
 */
import {
   afterAll,
   afterEach,
   beforeAll,
   beforeEach,
   describe,
   expect,
   it,
} from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import {
   PackageLoadPool,
   __setPackageLoadPoolForTests,
} from "../package_load/package_load_pool";
import { Package } from "./package";

const ORIGINAL_ENV = process.env.PACKAGE_LOAD_WORKERS;

const SOURCE =
   `source: base is duckdb.sql("select 1 as id, 'x' as label") extend {\n` +
   `   measure: n is count()\n` +
   `   view: by_label is { group_by: label  aggregate: n }\n` +
   `}\n`;

const LAYOUT =
   `## artifact { kind=notebook title="Tour" tiles=[intro { kind=text }, "base -> by_label"] }\n` +
   SOURCE +
   `\n##|(markdown) intro\n# Welcome\n\nRead **this** first.\n|##\n`;

describe("notebooks written as tiles (worker path)", () => {
   let tempDir: string;
   let pool: PackageLoadPool;

   beforeAll(async () => {
      process.env.PACKAGE_LOAD_WORKERS = "1";
      pool = new PackageLoadPool(1);
      await __setPackageLoadPoolForTests(pool);
   });

   afterAll(async () => {
      await __setPackageLoadPoolForTests(null);
      if (ORIGINAL_ENV === undefined) delete process.env.PACKAGE_LOAD_WORKERS;
      else process.env.PACKAGE_LOAD_WORKERS = ORIGINAL_ENV;
   });

   beforeEach(() => {
      tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "publisher-nb-layout-"));
      fs.mkdirSync(path.join(tempDir, "notebooks"));
      fs.mkdirSync(path.join(tempDir, "dashboards"));
      fs.writeFileSync(
         path.join(tempDir, "publisher.json"),
         JSON.stringify({ name: "pkg" }),
      );
   });

   afterEach(() => {
      fs.rmSync(tempDir, { recursive: true, force: true });
   });

   const write = (rel: string, text: string) =>
      fs.writeFileSync(path.join(tempDir, rel), text);

   async function withPackage(run: (pkg: Package) => Promise<void>) {
      const { MalloyConfig, FixedConnectionMap } = await import(
         "@malloydata/malloy"
      );
      const { DuckDBConnection } = await import("@malloydata/db-duckdb");
      const duckdb = new DuckDBConnection("duckdb", ":memory:");
      const malloyConfig = new MalloyConfig({ connections: {} });
      malloyConfig.wrapConnections(
         () => new FixedConnectionMap(new Map([["duckdb", duckdb]]), "duckdb"),
      );
      try {
         await run(await Package.create("env", "pkg", tempDir, malloyConfig));
      } finally {
         await duckdb.close();
      }
   }

   const warningsOf = (pkg: Package, model: string) =>
      (pkg.getPackageMetadata().warnings ?? []).filter(
         (warning) => warning.model === model,
      );

   it("serves the layout on the notebook GET: text tile body, one column, kind notebook", async () => {
      write("notebooks/tour.malloy", LAYOUT);
      await withPackage(async (pkg) => {
         expect(pkg.isServedNotebook("notebooks/tour.malloy")).toBe(true);
         const raw = await pkg.getModel("notebooks/tour.malloy")!.getNotebook();
         expect(raw.dashboard).toMatchObject({
            kind: "notebook",
            path: "notebooks/tour.malloy",
            title: "Tour",
            dashboardColumns: 1,
            tiles: [
               {
                  kind: "text",
                  name: "intro",
                  markdown: "# Welcome\n\nRead **this** first.",
               },
               { kind: "query", query: "base -> by_label" },
            ],
         });
      });
   });

   it("serves the same layout when the artifact tag is a multi-line ##| block", async () => {
      write(
         "notebooks/tour.malloy",
         LAYOUT.replace(
            /^## artifact .*\n/,
            `##| artifact { kind=notebook title="Tour"\n  tiles=[\n    intro { kind=text },\n    "base -> by_label"\n  ]\n}\n|##\n`,
         ),
      );
      await withPackage(async (pkg) => {
         expect(pkg.isServedNotebook("notebooks/tour.malloy")).toBe(true);
         const raw = await pkg.getModel("notebooks/tour.malloy")!.getNotebook();
         expect(raw.dashboard).toMatchObject({
            kind: "notebook",
            title: "Tour",
            tiles: [
               { kind: "text", name: "intro" },
               { kind: "query", query: "base -> by_label" },
            ],
         });
         expect(warningsOf(pkg, "notebooks/tour.malloy")).toEqual([]);
      });
   });

   it("synthesizes cells from the tiles, in tile order, after the file's definitions", async () => {
      write("notebooks/tour.malloy", LAYOUT);
      await withPackage(async (pkg) => {
         const raw = await pkg.getModel("notebooks/tour.malloy")!.getNotebook();
         expect(
            raw.notebookCells?.map((cell) => [cell.kind, cell.text]),
         ).toEqual([
            ["definition", SOURCE.trimEnd()],
            ["markdown", "# Welcome\n\nRead **this** first."],
            ["query", "run: base -> by_label"],
         ]);
      });
   });

   it("runs a synthesized query cell through the cell endpoint", async () => {
      write("notebooks/tour.malloy", LAYOUT);
      await withPackage(async (pkg) => {
         const model = pkg.getModel("notebooks/tour.malloy")!;
         const result = await model.executeNotebookCell(2);
         expect(result).toMatchObject({ type: "code", kind: "query" });
         expect(JSON.parse(result.result ?? "{}")).toBeTruthy();
      });
   });

   it("is a notebook, not a dashboard: unlisted as one, and a notebook listing carries it", async () => {
      write("notebooks/tour.malloy", LAYOUT);
      await withPackage(async (pkg) => {
         expect(pkg.listDashboards()).toEqual([]);
         expect(pkg.getDashboard("tour")).toBeUndefined();
         expect((await pkg.listNotebooks()).map((n) => n.path)).toEqual([
            "notebooks/tour.malloy",
         ]);
      });
   });

   it("keeps a notebook written as cells free of a layout", async () => {
      write(
         "notebooks/cells.malloy",
         `## artifact { kind=notebook }\n${SOURCE}run: base -> by_label\n`,
      );
      await withPackage(async (pkg) => {
         const raw = await pkg
            .getModel("notebooks/cells.malloy")!
            .getNotebook();
         expect(raw.dashboard).toBeUndefined();
         expect(raw.notebookCells?.map((cell) => cell.kind)).toEqual([
            "definition",
            "query",
         ]);
      });
   });

   it("serves a notebook with no tiles yet as a layout", async () => {
      write(
         "notebooks/empty.malloy",
         `## artifact { kind=notebook tiles=[] }\n${SOURCE}`,
      );
      await withPackage(async (pkg) => {
         const raw = await pkg
            .getModel("notebooks/empty.malloy")!
            .getNotebook();
         expect(raw.dashboard?.tiles).toEqual([]);
      });
   });

   it("lints the layout like a dashboard: an unresolved tile is an error, a stray run: is reported", async () => {
      write(
         "notebooks/tour.malloy",
         LAYOUT.replace("base -> by_label", "base -> missing") +
            "run: base -> by_label\n",
      );
      await withPackage(async (pkg) => {
         const messages = warningsOf(pkg, "notebooks/tour.malloy").map(
            (warning) => warning.message,
         );
         expect(messages).toEqual(
            expect.arrayContaining([
               expect.stringContaining(
                  'tile "base -> missing" does not resolve',
               ),
               expect.stringContaining("a `run:` is never shown"),
            ]),
         );
      });
   });

   it("serves the kind the tag names, whichever folder holds the file", async () => {
      write(
         "dashboards/in_dashboards.malloy",
         LAYOUT.replace("Tour", "Misfiled"),
      );
      write(
         "notebooks/in_notebooks.malloy",
         `## artifact { kind=dashboard tiles=["base -> by_label"] }\n${SOURCE}`,
      );
      await withPackage(async (pkg) => {
         expect(pkg.isServedNotebook("dashboards/in_dashboards.malloy")).toBe(
            true,
         );
         expect(pkg.isServedNotebook("notebooks/in_notebooks.malloy")).toBe(
            false,
         );
         expect(pkg.listDashboards().map((d) => d.path)).toEqual([
            "notebooks/in_notebooks.malloy",
         ]);
         expect(pkg.getDashboard("in_notebooks")).toMatchObject({
            kind: "dashboard",
         });
         const raw = await pkg
            .getModel("dashboards/in_dashboards.malloy")!
            .getNotebook();
         expect(raw.dashboard?.kind).toBe("notebook");
      });
   });

   it("serves one dashboard per name and reports the file that lost it", async () => {
      const dashboard = `## artifact { tiles=["base -> by_label"] }\n${SOURCE}`;
      write("dashboards/same.malloy", dashboard);
      write(
         "notebooks/same.malloy",
         dashboard.replace("{ tiles", "{ kind=dashboard tiles"),
      );
      await withPackage(async (pkg) => {
         expect(pkg.listDashboards().map((d) => d.path)).toEqual([
            "dashboards/same.malloy",
         ]);
         expect(
            warningsOf(pkg, "notebooks/same.malloy").map((w) => w.message),
         ).toEqual(
            expect.arrayContaining([
               expect.stringContaining("already holds the dashboard name"),
            ]),
         );
      });
   });
});
