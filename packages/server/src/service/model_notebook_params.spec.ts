import { DuckDBConnection } from "@malloydata/db-duckdb";
import type { Connection } from "@malloydata/malloy";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import fs from "fs/promises";
import path from "path";
import { Model } from "./model";

/**
 * Integration test: loading a notebook that uses a parameterized source
 * without providing the required parameter values.  The publisher should
 * load the notebook successfully (deferring the compilation error to
 * execution time) rather than failing the entire load.
 */
describe("notebook with parameterized source (integration)", () => {
   const tmpDir = path.join(
      import.meta.dir,
      "__test_notebook_params_" + process.pid,
   );
   let connections: Map<string, Connection>;

   beforeEach(async () => {
      await fs.mkdir(tmpDir, { recursive: true });

      // Model file: declares a parameterized source with a required param
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

      // Notebook file: imports the model and runs a query WITHOUT
      // providing the required parameter — this cell should fail to
      // compile, but the notebook should still load.
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
      "should expose notebook cells from the loaded model",
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
      },
      { timeout: 30000 },
   );

   it(
      "should report an error when executing the parameterized cell without sourceParameters",
      async () => {
         const model = await Model.create(
            "test-pkg",
            tmpDir,
            "notebook.malloynb",
            connections,
         );

         const notebook = await model.getNotebook();
         // Find the code cell that runs the parameterized query
         const codeCellIndex = notebook.notebookCells!.findIndex(
            (c) => c.type === "code" && c.text.includes("param_source"),
         );
         expect(codeCellIndex).toBeGreaterThanOrEqual(0);

         await expect(
            model.executeNotebookCell(codeCellIndex),
         ).rejects.toThrow();
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
