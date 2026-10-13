// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A linear `.malloy` report's cells once discovery has attached them: its
 * definitions, then its tiles, the reader's refusal as the report's error, and
 * the two load paths agreeing. A file written as `run:` cells, the retired
 * format, is no report at all. Driven over a copy of
 * `tests/fixtures/notebooks-malloyyo/`, whose `notebooks/` holds one report
 * written as tiles (`layout.malloy`) and many written as cells.
 */
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { ModelCompilationError } from "../errors";
import { resetNotebookMetricsForTest } from "../notebook_metrics";
import { resetReportMetricsForTest } from "../report_metrics";
import {
   PackageLoadPool,
   __setPackageLoadPoolForTests,
} from "../package_load/package_load_pool";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "../test_helpers/metrics_harness";
import { Model } from "./model";
import { Package } from "./package";
import { listNotebooks } from "./legacy_report_views";

const FIXTURE_DIR = path.resolve(
   __dirname,
   "../../tests/fixtures/notebooks-malloyyo",
);
const ORIGINAL_ENV = process.env.PACKAGE_LOAD_WORKERS;

const LAYOUT = "notebooks/layout.malloy";
const SETTINGS = "notebooks/settings.malloy";
const REFUSED = "notebooks/refused_tiles.malloy";

/** The rows of a cell result, as plain objects keyed by column. */
function rowsOf(result: string | undefined): Record<string, unknown>[] {
   const parsed = JSON.parse(result ?? "{}") as {
      data?: {
         array_value?: {
            record_value?: { number_value?: number; string_value?: string }[];
         }[];
      };
      schema?: { fields?: { name: string }[] };
   };
   const names = (parsed.schema?.fields ?? []).map((f) => f.name);
   return (parsed.data?.array_value ?? []).map((row) =>
      Object.fromEntries(
         (row.record_value ?? []).map((cell, i) => [
            names[i],
            cell.number_value ?? cell.string_value,
         ]),
      ),
   );
}

describe("served linear report cells", () => {
   let tempDir: string;
   let pool: PackageLoadPool;
   let harness: MetricsHarness;
   let pkg: Package;
   /** The fixture files written as `run:` cells. */
   let cellFiles: string[];

   beforeAll(async () => {
      harness = await startMetricsHarness();
      resetNotebookMetricsForTest();
      resetReportMetricsForTest();
      process.env.PACKAGE_LOAD_WORKERS = "1";
      pool = new PackageLoadPool(1);
      await __setPackageLoadPoolForTests(pool);
      tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "publisher-nb-cells-"));
      fs.cpSync(FIXTURE_DIR, tempDir, { recursive: true });
      cellFiles = fs
         .readdirSync(path.join(tempDir, "notebooks"))
         .map((file) => `notebooks/${file}`)
         .filter((p) => p !== LAYOUT);
      fs.writeFileSync(
         path.join(tempDir, SETTINGS),
         "##! experimental.givens\n" +
            "## artifact { layout=linear autorun=false givens { REGION=f'EU' } tiles=[\"orders -> kpis\"] }\n" +
            'import "../models/orders.malloy"\n' +
            "given: REGION :: filter<string> is f''\n",
      );
      // A tiled report whose definitions the reader cannot hold as cells.
      fs.writeFileSync(
         path.join(tempDir, REFUSED),
         "##! experimental { givens compilerTestExperimentParse compilerTestExperimentTranslate }\n" +
            '## artifact { layout=linear title="Refused" tiles=["orders -> kpis"] }\n' +
            'import "../models/orders.malloy"\n' +
            "\n" +
            ';;[ "a compiler test statement no notebook cell can hold" ]\n',
      );
      const { MalloyConfig } = await import("@malloydata/malloy");
      pkg = await Package.create(
         "env",
         "notebooks-malloyyo",
         tempDir,
         new MalloyConfig({ connections: {} }),
      );
   });

   afterAll(async () => {
      await __setPackageLoadPoolForTests(null);
      if (ORIGINAL_ENV === undefined) delete process.env.PACKAGE_LOAD_WORKERS;
      else process.env.PACKAGE_LOAD_WORKERS = ORIGINAL_ENV;
      fs.rmSync(tempDir, { recursive: true, force: true });
      resetNotebookMetricsForTest();
      resetReportMetricsForTest();
      await harness.shutdown();
   });

   const model = (modelPath: string) => {
      const found = pkg.getModel(modelPath);
      if (!found) throw new Error(`${modelPath} is not in the package`);
      return found;
   };

   it("serves a report's definitions as cells, then its tiles in tile order", async () => {
      const raw = await model(LAYOUT).getNotebook();
      expect(raw.format).toBe("malloy");
      expect(raw.notebookCells?.map((cell) => [cell.type, cell.kind])).toEqual([
         ["code", "definition"],
         ["code", "definition"],
         ["markdown", "markdown"],
         ["code", "query"],
         ["code", "query"],
      ]);
      expect(raw.notebookCells?.[2].text).toContain("## How to read this page");
      expect(raw.notebookCells?.[3].text).toBe("run: orders_tiles -> headline");
      expect(raw.dashboard?.layout).toBe("linear");
   });

   it("reads autorun and starting givens off the artifact tag", async () => {
      const raw = await model(SETTINGS).getNotebook();
      expect(raw.autorun).toBe(false);
      expect(raw.startingGivens).toEqual({ REGION: "EU" });
   });

   it("runs a tile cell, and answers a definition or markdown cell with its text and kind, and no result", async () => {
      const nb = model(LAYOUT);
      const query = await nb.executeNotebookCell(4);
      expect(query.kind).toBe("query");
      expect(rowsOf(query.result).length).toBeGreaterThan(0);
      const definition = await nb.executeNotebookCell(0);
      expect(definition).toMatchObject({ type: "code", kind: "definition" });
      expect(definition.result).toBeUndefined();
      const markdown = await nb.executeNotebookCell(2);
      expect(markdown).toMatchObject({ type: "markdown", kind: "markdown" });
      expect(markdown.result).toBeUndefined();
   });

   it("attaches identical cells on the worker path and on Model.create", async () => {
      const { MalloyConfig } = await import("@malloydata/malloy");
      const { DuckDBConnection } = await import("@malloydata/db-duckdb");
      const duckdb = new DuckDBConnection("duckdb", ":memory:", tempDir);
      const config = new MalloyConfig({ connections: {} });
      const { FixedConnectionMap } = await import("@malloydata/malloy");
      config.wrapConnections(
         () => new FixedConnectionMap(new Map([["duckdb", duckdb]]), "duckdb"),
      );
      try {
         for (const modelPath of [LAYOUT, SETTINGS]) {
            const inProcess = await Model.create(
               "notebooks-malloyyo",
               tempDir,
               modelPath,
               config,
            );
            inProcess.setQueryBoundary({
               mode: "all",
               exploresDeclared: false,
               isQueryEntryPoint: true,
               layout: "linear",
            });
            expect(
               inProcess.attachServedNotebookCells(
                  inProcess.getCompiledSourceText()!,
               ),
            ).toBe("ok");
            const fromWorker = await model(modelPath).getNotebook();
            const local = await inProcess.getNotebook();
            // Each compile mints fresh drill reference ids; nothing else may differ.
            const stable = (cells: unknown) =>
               JSON.stringify(cells).replace(
                  /reference_id = \\\\\\"[0-9a-f-]+\\\\\\"/g,
                  "reference_id",
               );
            expect(stable(local.notebookCells)).toContain("reference_id");
            expect(stable(local.notebookCells)).toEqual(
               stable(fromWorker.notebookCells),
            );
            expect(local.annotations).toEqual(fromWorker.annotations);
            const queryAt = (fromWorker.notebookCells ?? []).findIndex(
               (cell) => cell.kind === "query",
            );
            const givens =
               modelPath === SETTINGS ? { REGION: "EU" } : undefined;
            const a = await inProcess.executeNotebookCell(
               queryAt,
               undefined,
               undefined,
               givens,
            );
            const b = await model(modelPath).executeNotebookCell(
               queryAt,
               undefined,
               undefined,
               givens,
            );
            expect(rowsOf(a.result)).toEqual(rowsOf(b.result));
            expect(rowsOf(a.result).length).toBeGreaterThan(0);
         }
      } finally {
         await duckdb.close();
      }
   });

   describe("a file written as `run:` cells", () => {
      it("is no report, and still serves as a model", async () => {
         expect(cellFiles.length).toBeGreaterThan(5);
         const reports = pkg.listReports().map((report) => report.path);
         for (const modelPath of cellFiles) {
            expect({ modelPath, served: reports.includes(modelPath) }).toEqual({
               modelPath,
               served: false,
            });
            expect(pkg.isServedReport(modelPath)).toBe(false);
            expect(model(modelPath).isNotebook()).toBe(false);
         }
         const compiled = await model(
            "notebooks/revenue_review.malloy",
         ).getModel();
         expect(compiled.sources?.map((source) => source.name)).toContain(
            "orders",
         );
      });

      it("is one error finding per file, naming the fix", () => {
         const findings = (pkg.getPackageMetadata().warnings ?? []).filter(
            (warning) => warning.code === "report-cells-removed",
         );
         expect(findings.map((f) => f.model).sort()).toEqual(
            [...cellFiles].sort(),
         );
         for (const finding of findings) {
            expect(finding.severity).toBe("error");
            expect(finding.message).toContain(
               "Fix: list its tiles in `tiles=[…]`",
            );
         }
      });

      it("is counted as an invalid file and a finding, never as a served report", async () => {
         expect(
            await harness.collectCounter(
               "publisher_report_lint_findings_total",
               { code: "report-cells-removed", severity: "error" },
            ),
         ).toBe(cellFiles.length);
         expect(
            await harness.collectCounter("publisher_reports_discovered_total", {
               layout: "linear",
            }),
         ).toBe(
            pkg.listReports().filter((report) => report.layout === "linear")
               .length,
         );
      });
   });

   describe("a report the reader refuses", () => {
      it("lists with the reader's error, naming the line", async () => {
         const listed = (await listNotebooks(pkg, "env")).find(
            (nb) => nb.path === REFUSED,
         );
         expect(listed?.error).toContain("Line 5:");
      });

      it("fails the notebook GET and a cell run the way a notebook that did not compile does", async () => {
         const nb = model(REFUSED);
         await expect(nb.getNotebook()).rejects.toBeInstanceOf(
            ModelCompilationError,
         );
         await expect(nb.executeNotebookCell(0)).rejects.toBeInstanceOf(
            ModelCompilationError,
         );
      });

      it("is a package finding of severity error, counted at discovery", async () => {
         expect(pkg.getPackageMetadata().warnings).toContainEqual(
            expect.objectContaining({
               model: REFUSED,
               severity: "error",
               code: "notebook-cells-unreadable",
            }),
         );
         expect(
            await harness.collectCounter(
               "publisher_report_lint_findings_total",
               { code: "notebook-cells-unreadable", severity: "error" },
            ),
         ).toBe(1);
      });
   });

   it("counts a cell run by format, kind and outcome", async () => {
      await model(LAYOUT).executeNotebookCell(3);
      expect(
         await harness.collectCounter(
            "publisher_notebook_cell_executions_total",
            { format: "malloy", kind: "query", outcome: "ok" },
         ),
      ).toBeGreaterThan(0);
      await expect(model(REFUSED).executeNotebookCell(0)).rejects.toThrow();
      expect(
         await harness.collectCounter(
            "publisher_notebook_cell_executions_total",
            { format: "malloy", kind: "none", outcome: "error" },
         ),
      ).toBeGreaterThan(0);
      expect(
         await harness.collectCounter(
            "publisher_notebook_cell_executions_total",
            { format: "malloy", kind: "code" },
         ),
      ).toBe(0);
   });
});
