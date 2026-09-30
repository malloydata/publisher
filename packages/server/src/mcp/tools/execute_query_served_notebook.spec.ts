// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * MCP `execute_query` against a served notebook on a package with a surface,
 * through the real tool handler and a real loaded Package. A served notebook
 * stays a model, so the tool accepts it; what it may run is held to the surface,
 * and it admits none of its own named queries by name.
 */
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import {
   PackageLoadPool,
   __setPackageLoadPoolForTests,
} from "../../package_load/package_load_pool";
import type { EnvironmentStore } from "../../service/environment_store";
import { Package } from "../../service/package";
import { registerExecuteQueryTool } from "./execute_query_tool";

const FIXTURE_DIR = path.resolve(
   __dirname,
   "../../../tests/fixtures/notebooks-malloyyo-surface",
);
const NOTEBOOK = "notebooks/cells.malloy";
const ORIGINAL_ENV = process.env.PACKAGE_LOAD_WORKERS;

type Handler = (params: Record<string, unknown>) => Promise<{
   isError?: boolean;
   content: Array<{ resource?: { text: string } }>;
}>;

describe("execute_query on a served notebook", () => {
   let tempDir: string;
   let handler: Handler;

   beforeAll(async () => {
      process.env.PACKAGE_LOAD_WORKERS = "1";
      await __setPackageLoadPoolForTests(new PackageLoadPool(1));
      tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "publisher-mcp-nb-"));
      fs.cpSync(FIXTURE_DIR, tempDir, { recursive: true });
      const { MalloyConfig } = await import("@malloydata/malloy");
      const pkg = await Package.create(
         "env",
         "notebooks-malloyyo-surface",
         tempDir,
         new MalloyConfig({ connections: {} }),
      );
      const store: Partial<EnvironmentStore> = {
         getEnvironment: async () =>
            ({
               assertCanAdmitQuery: () => undefined,
               getApiConnection: () => {
                  throw new Error("no connection config in this test");
               },
               getPackage: async () => pkg,
            }) as never,
      };
      const fakeServer = {
         tool: (_name: string, _desc: string, _shape: unknown, h: Handler) => {
            handler = h;
         },
      };
      registerExecuteQueryTool(fakeServer as never, store as EnvironmentStore);
   });

   afterAll(async () => {
      await __setPackageLoadPoolForTests(null);
      if (ORIGINAL_ENV === undefined) delete process.env.PACKAGE_LOAD_WORKERS;
      else process.env.PACKAGE_LOAD_WORKERS = ORIGINAL_ENV;
      fs.rmSync(tempDir, { recursive: true, force: true });
   });

   const call = (args: Record<string, unknown>) =>
      handler({
         environmentName: "env",
         packageName: "notebooks-malloyyo-surface",
         modelPath: NOTEBOOK,
         ...args,
      });
   const payload = (result: Awaited<ReturnType<Handler>>) =>
      JSON.parse(result.content[0].resource!.text);

   it("runs ad-hoc Malloy over a curated source", async () => {
      const result = await call({
         query: "run: orders -> { aggregate: c is count() }",
      });
      expect(result.isError).not.toBe(true);
      expect(payload(result).rows).toEqual([{ c: 6 }]);
   });

   it("does not admit the notebook's own named query by name", async () => {
      const result = await call({ queryName: "own_cells_query" });
      expect(result.isError).toBe(true);
      expect(payload(result).error).toContain("Resource not found");
   });

   it("refuses ad-hoc Malloy over a source the surface hides", async () => {
      const result = await call({
         query: "run: hidden -> { aggregate: c is count() }",
      });
      expect(result.isError).toBe(true);
      expect(payload(result).error).toContain("Resource not found");
   });
});
