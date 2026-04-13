import { DuckDBConnection } from "@malloydata/db-duckdb";
import type { Connection } from "@malloydata/malloy";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import fs from "fs/promises";
import path from "path";
import { Model } from "./model";

// Shared fixture: a model with a parameterized source over inline data.
// The source filters rows where value >= min_val, so the number of
// matching rows depends on the parameter value passed at query time.
const MALLOY_MODEL = [
   "##! experimental.parameters",
   "",
   "source: items(min_val::number) is duckdb.sql(",
   '   """SELECT * FROM (VALUES (1,10),(2,50),(3,100),(4,200)) AS t(id,value)"""',
   ") extend {",
   "   where: value >= min_val",
   "   measure: row_count is count()",
   "   measure: total_value is sum(value)",
   "   view: summary is { aggregate: row_count, total_value }",
   "}",
].join("\n");

const MALLOY_NOTEBOOK = [
   ">>>malloy",
   'import "model.malloy"',
   "",
   ">>>markdown",
   "# Test Notebook",
   "",
   ">>>malloy",
   "run: items -> { aggregate: row_count }",
].join("\n");

function makeTmpDir() {
   return path.join(
      __dirname,
      "__test_params_" + process.pid + "_" + Date.now(),
   );
}

async function setupFixture(tmpDir: string) {
   await fs.mkdir(tmpDir, { recursive: true });
   await fs.writeFile(path.join(tmpDir, "model.malloy"), MALLOY_MODEL);
   await fs.writeFile(path.join(tmpDir, "notebook.malloynb"), MALLOY_NOTEBOOK);
   const duckdb = new DuckDBConnection("duckdb", ":memory:", tmpDir);
   return new Map<string, Connection>([["duckdb", duckdb]]);
}

// -----------------------------------------------------------------------
// Notebook loading with parameterized sources
// -----------------------------------------------------------------------

describe("notebook with parameterized source (integration)", () => {
   const tmpDir = makeTmpDir();
   let connections: Map<string, Connection>;

   beforeEach(async () => {
      connections = await setupFixture(tmpDir);
   });

   afterEach(async () => {
      await fs.rm(tmpDir, { recursive: true, force: true }).catch(() => {});
   });

   it(
      "should load the notebook without throwing",
      async () => {
         const model = await Model.create(
            "test-pkg",
            tmpDir,
            "notebook.malloynb",
            connections,
         );

         expect(model).toBeInstanceOf(Model);
         expect(model.getType()).toBe("notebook");
      },
      { timeout: 30000 },
   );

   it(
      "should expose notebook cells with queryInfo from stub compilation",
      async () => {
         const model = await Model.create(
            "test-pkg",
            tmpDir,
            "notebook.malloynb",
            connections,
         );

         const notebook = await model.getNotebook();
         expect(notebook.notebookCells).toBeDefined();
         expect(notebook.notebookCells!.length).toBeGreaterThanOrEqual(2);

         // The parameterized cell should have queryInfo from the
         // stub-compiled query — proves structural validation worked.
         const paramCell = notebook.notebookCells!.find(
            (c) => c.type === "code" && c.text?.includes("items"),
         );
         expect(paramCell).toBeDefined();
         expect(paramCell!.queryInfo).toBeDefined();
      },
      { timeout: 30000 },
   );

   it(
      "should expose sourceInfos with parameter metadata",
      async () => {
         const model = await Model.create(
            "test-pkg",
            tmpDir,
            "notebook.malloynb",
            connections,
         );

         const sourceInfos = model.getSourceInfos();
         expect(sourceInfos).toBeDefined();

         const paramSource = sourceInfos!.find((s) => s.name === "items");
         expect(paramSource).toBeDefined();
         expect(paramSource!.parameters).toBeDefined();
         expect(paramSource!.parameters!.length).toBe(1);
         expect(paramSource!.parameters![0].name).toBe("min_val");
      },
      { timeout: 30000 },
   );

   it(
      "should throw when executing the parameterized cell without sourceParameters",
      async () => {
         const model = await Model.create(
            "test-pkg",
            tmpDir,
            "notebook.malloynb",
            connections,
         );

         const notebook = await model.getNotebook();
         const codeCellIndex = notebook.notebookCells!.findIndex(
            (c) => c.type === "code" && c.text?.includes("items"),
         );
         expect(codeCellIndex).toBeGreaterThanOrEqual(0);

         await expect(model.executeNotebookCell(codeCellIndex)).rejects.toThrow(
            /parameterized source/,
         );
      },
      { timeout: 30000 },
   );

   it(
      "should execute the parameterized cell when sourceParameters are provided",
      async () => {
         const model = await Model.create(
            "test-pkg",
            tmpDir,
            "notebook.malloynb",
            connections,
         );

         const notebook = await model.getNotebook();
         const codeCellIndex = notebook.notebookCells!.findIndex(
            (c) => c.type === "code" && c.text?.includes("items"),
         );

         const result = await model.executeNotebookCell(codeCellIndex, {
            min_val: 50,
         });

         expect(result.type).toBe("code");
         expect(result.result).toBeDefined();
      },
      { timeout: 30000 },
   );

   it(
      "should also load the standalone model file successfully",
      async () => {
         const model = await Model.create(
            "test-pkg",
            tmpDir,
            "model.malloy",
            connections,
         );

         expect(model).toBeInstanceOf(Model);
         expect(model.getType()).toBe("model");
         expect(model.getSourceInfos()).toBeDefined();
      },
      { timeout: 30000 },
   );
});

// -----------------------------------------------------------------------
// getQueryResults with source parameters (integration)
// -----------------------------------------------------------------------

describe("getQueryResults with source parameters (integration)", () => {
   const tmpDir = makeTmpDir();
   let connections: Map<string, Connection>;
   let model: Model;

   beforeEach(async () => {
      connections = await setupFixture(tmpDir);
      model = await Model.create(
         "test-pkg",
         tmpDir,
         "model.malloy",
         connections,
      );
   });

   afterEach(async () => {
      await fs.rm(tmpDir, { recursive: true, force: true }).catch(() => {});
   });

   // -- Named query (sourceName + queryName) path -------------------------

   it(
      "named query: min_val=0 returns all 4 rows",
      async () => {
         const { compactResult } = await model.getQueryResults(
            "items",
            "summary",
            undefined,
            { min_val: 0 },
         );
         expect(compactResult).toHaveLength(1);
         expect(compactResult[0]["row_count"]).toBe(4);
         expect(compactResult[0]["total_value"]).toBe(360);
      },
      { timeout: 30000 },
   );

   it(
      "named query: min_val=100 returns only rows with value >= 100",
      async () => {
         const { compactResult } = await model.getQueryResults(
            "items",
            "summary",
            undefined,
            { min_val: 100 },
         );
         expect(compactResult).toHaveLength(1);
         expect(compactResult[0]["row_count"]).toBe(2);
         expect(compactResult[0]["total_value"]).toBe(300);
      },
      { timeout: 30000 },
   );

   it(
      "named query: min_val=999 returns 0 rows",
      async () => {
         const { compactResult } = await model.getQueryResults(
            "items",
            "summary",
            undefined,
            { min_val: 999 },
         );
         expect(compactResult).toHaveLength(1);
         expect(compactResult[0]["row_count"]).toBe(0);
      },
      { timeout: 30000 },
   );

   // -- Ad-hoc query (raw query string) path ------------------------------

   it(
      "ad-hoc query: min_val=50 filters correctly",
      async () => {
         const { compactResult } = await model.getQueryResults(
            undefined,
            undefined,
            "run: items -> { aggregate: row_count, total_value }",
            { min_val: 50 },
         );
         expect(compactResult).toHaveLength(1);
         expect(compactResult[0]["row_count"]).toBe(3);
         expect(compactResult[0]["total_value"]).toBe(350);
      },
      { timeout: 30000 },
   );

   it(
      "ad-hoc query: min_val=200 returns single matching row",
      async () => {
         const { compactResult } = await model.getQueryResults(
            undefined,
            undefined,
            "run: items -> { aggregate: row_count, total_value }",
            { min_val: 200 },
         );
         expect(compactResult).toHaveLength(1);
         expect(compactResult[0]["row_count"]).toBe(1);
         expect(compactResult[0]["total_value"]).toBe(200);
      },
      { timeout: 30000 },
   );

   // -- Error cases -------------------------------------------------------

   it(
      "should throw when required param is missing",
      async () => {
         await expect(
            model.getQueryResults("items", "summary", undefined, {}),
         ).rejects.toThrow(/required/);
      },
      { timeout: 30000 },
   );

   it(
      "should throw when no sourceParameters provided at all",
      async () => {
         await expect(
            model.getQueryResults("items", "summary"),
         ).rejects.toThrow(/required/);
      },
      { timeout: 30000 },
   );
});
