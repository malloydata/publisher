// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A served notebook's cells once discovery has attached them: the notebook GET,
 * running a cell, the reader's refusal as the notebook's error, and the two load
 * paths agreeing. Driven over a copy of `tests/fixtures/notebooks-malloyyo/`.
 */
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { ModelCompilationError } from "../errors";
import { resetNotebookMetricsForTest } from "../notebook_metrics";
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

const FIXTURE_DIR = path.resolve(
   __dirname,
   "../../tests/fixtures/notebooks-malloyyo",
);
const ORIGINAL_ENV = process.env.PACKAGE_LOAD_WORKERS;

const SETTINGS = "notebooks/settings.malloy";
const REFUSED = "notebooks/refused.malloy";

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

describe("served notebook cells", () => {
   let tempDir: string;
   let pool: PackageLoadPool;
   let harness: MetricsHarness;
   let pkg: Package;

   beforeAll(async () => {
      harness = await startMetricsHarness();
      resetNotebookMetricsForTest();
      process.env.PACKAGE_LOAD_WORKERS = "1";
      pool = new PackageLoadPool(1);
      await __setPackageLoadPoolForTests(pool);
      tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "publisher-nb-cells-"));
      fs.cpSync(FIXTURE_DIR, tempDir, { recursive: true });
      fs.writeFileSync(
         path.join(tempDir, SETTINGS),
         "##! experimental.givens\n" +
            "## artifact { kind=notebook autorun=false givens { REGION=f'EU' } }\n" +
            'import "../models/orders.malloy"\n' +
            "given: REGION :: filter<string> is f''\n" +
            "run: orders -> kpis + { where: region ~ $REGION }\n",
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
      await harness.shutdown();
   });

   const model = (modelPath: string) => {
      const found = pkg.getModel(modelPath);
      if (!found) throw new Error(`${modelPath} is not in the package`);
      return found;
   };

   it("serves a notebook's cells with their kinds, and its header notes as annotations", async () => {
      const raw = await model("notebooks/revenue_review.malloy").getNotebook();
      expect(raw.format).toBe("malloy");
      expect(raw.notebookCells?.map((cell) => [cell.type, cell.kind])).toEqual([
         ["code", "definition"],
         ["markdown", "markdown"],
         ["code", "definition"],
         ["markdown", "markdown"],
         ["code", "query"],
      ]);
      expect(raw.notebookCells?.[1].text).toBe(
         "# Where revenue came from\nProse in **markdown**, any length.",
      );
      expect(raw.annotations).toEqual([
         "##! experimental.givens\n",
         '## artifact { kind=notebook title="Revenue review" }\n',
      ]);
      // The query cell describes the run it maps to, not some other one.
      expect(
         JSON.parse(
            raw.notebookCells?.[4].queryInfo ?? "{}",
         ).schema?.fields?.map((f: { name: string }) => f.name),
      ).toEqual(["order_month", "total_amount"]);
   });

   it("gives each of several query cells the schema of its own run", async () => {
      const raw = await model("notebooks/tagged_runs.malloy").getNotebook();
      const columns = (index: number) =>
         (
            JSON.parse(raw.notebookCells?.[index].queryInfo ?? "{}") as {
               schema?: { fields?: { name: string }[] };
            }
         ).schema?.fields?.map((f) => f.name);
      expect(columns(1)).toEqual(["order_month", "total_amount"]);
      expect(columns(2)).toEqual(["order_count", "total_amount"]);
   });

   it("reads autorun and starting givens off the artifact tag", async () => {
      const raw = await model(SETTINGS).getNotebook();
      expect(raw.autorun).toBe(false);
      expect(raw.startingGivens).toEqual({ REGION: "EU" });
   });

   it("runs a query cell with the request's givens bound", async () => {
      const nb = model("notebooks/revenue_review.malloy");
      const result = await nb.executeNotebookCell(4, undefined, undefined, {
         REGION: "US",
      });
      expect(result.kind).toBe("query");
      expect(rowsOf(result.result).map((row) => row.total_amount)).toEqual([
         50, 200, 100,
      ]);
   });

   it("runs a cell over the notebook's own named query", async () => {
      const result = await model(
         "notebooks/named_runs.malloy",
      ).executeNotebookCell(2);
      expect(rowsOf(result.result)).toEqual([
         { order_count: 6, total_amount: 1550 },
      ]);
   });

   it("answers a definition or markdown cell with its text and kind, and no result", async () => {
      const nb = model("notebooks/revenue_review.malloy");
      const definition = await nb.executeNotebookCell(2);
      expect(definition).toMatchObject({ type: "code", kind: "definition" });
      expect(definition.result).toBeUndefined();
      const markdown = await nb.executeNotebookCell(1);
      expect(markdown).toMatchObject({ type: "markdown", kind: "markdown" });
      expect(markdown.result).toBeUndefined();
   });

   it("grafts a served notebook's cell against the whole compiled model", async () => {
      const nb = model("notebooks/tagged_runs.malloy") as unknown as {
         runnableNotebookCells: { runnable?: unknown }[];
         resolveNotebookCellGraftScope(
            index: number,
            runnable: unknown,
         ): Promise<{
            graftScope?: { cacheScope: string };
            usesOwnScope: boolean;
         }>;
      };
      // Cell 1 is the first query cell, which has no earlier code cell with a materializer.
      for (const index of [1, 2]) {
         const resolved = await nb.resolveNotebookCellGraftScope(
            index,
            nb.runnableNotebookCells[index].runnable,
         );
         expect(resolved.usesOwnScope).toBe(false);
         expect(resolved.graftScope?.cacheScope).toBe("model");
      }
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
         const served = fs
            .readdirSync(path.join(tempDir, "notebooks"))
            .map((file) => `notebooks/${file}`)
            .filter((p) => p !== REFUSED);
         expect(served.length).toBeGreaterThan(5);
         let normalized = 0;
         for (const modelPath of served) {
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
               notebook: true,
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
            if (stable(local.notebookCells).includes("reference_id"))
               normalized++;
            expect(stable(local.notebookCells)).toEqual(
               stable(fromWorker.notebookCells),
            );
            expect(local.annotations).toEqual(fromWorker.annotations);
            const queryAt = (fromWorker.notebookCells ?? []).findIndex(
               (cell) => cell.kind === "query",
            );
            if (queryAt >= 0) {
               const givens =
                  modelPath === SETTINGS ||
                  modelPath.endsWith("revenue_review.malloy")
                     ? { REGION: "EU" }
                     : undefined;
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
         }
         // The id pattern above really matched, so the comparison was not vacuous about queryInfo.
         expect(normalized).toBeGreaterThan(0);
      } finally {
         await duckdb.close();
      }
   });

   describe("a notebook the reader refuses", () => {
      it("lists with the reader's error, naming the line", async () => {
         const listed = (await pkg.listNotebooks()).find(
            (nb) => nb.path === REFUSED,
         );
         expect(listed?.error).toContain("Line 7:");
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

      it("still serves the file as a model", async () => {
         const compiled = await model(REFUSED).getModel();
         expect(compiled.sources?.map((source) => source.name)).toContain(
            "orders",
         );
      });

      it("is a package finding of severity error", () => {
         expect(pkg.getPackageMetadata().warnings).toContainEqual(
            expect.objectContaining({ model: REFUSED, severity: "error" }),
         );
      });
   });

   it("counts each served notebook at discovery by outcome", async () => {
      const served = fs.readdirSync(path.join(tempDir, "notebooks")).length;
      expect(
         await harness.collectCounter("publisher_notebook_discovery_total", {
            format: "malloy",
            outcome: "refused",
         }),
      ).toBe(1);
      expect(
         await harness.collectCounter("publisher_notebook_discovery_total", {
            format: "malloy",
            outcome: "ok",
         }),
      ).toBe(served - 1);
   });

   it("counts a cell run by format, kind and outcome", async () => {
      await model("notebooks/tagged_runs.malloy").executeNotebookCell(2);
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
