import { DuckDBConnection } from "@malloydata/db-duckdb";
import type { Connection } from "@malloydata/malloy";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import fs from "fs/promises";
import path from "path";
import { Model } from "./model";

/**
 * Integration test: loading a notebook that uses a parameterized source
 * without providing the required parameter values.  The publisher should
 * compile the notebook with stub values at load time (validating
 * structure) and require real sourceParameters at execution time.
 */
describe("notebook with parameterized source (integration)", () => {
   const tmpDir = path.join(__dirname, "__test_notebook_params_" + process.pid);
   let connections: Map<string, Connection>;

   beforeEach(async () => {
      await fs.mkdir(tmpDir, { recursive: true });

      await fs.writeFile(
         path.join(tmpDir, "model.malloy"),
         [
            "##! experimental.parameters",
            "",
            "source: param_source(min_val::number) is duckdb.sql(",
            '   """SELECT 1 as id, 100 as value"""',
            ") extend {",
            "   where: value >= min_val",
            "   measure: row_count is count()",
            "}",
         ].join("\n"),
      );

      await fs.writeFile(
         path.join(tmpDir, "notebook.malloynb"),
         [
            ">>>malloy",
            'import "model.malloy"',
            "",
            ">>>markdown",
            "# Test Notebook",
            "",
            ">>>malloy",
            "run: param_source -> { aggregate: row_count }",
         ].join("\n"),
      );

      const duckdb = new DuckDBConnection("duckdb", ":memory:", tmpDir);
      connections = new Map<string, Connection>([["duckdb", duckdb]]);
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
            (c) => c.type === "code" && c.text?.includes("param_source"),
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

         const paramSource = sourceInfos!.find(
            (s) => s.name === "param_source",
         );
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
            (c) => c.type === "code" && c.text?.includes("param_source"),
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
            (c) => c.type === "code" && c.text?.includes("param_source"),
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
