// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * MCP `execute_query` addressed to the wrong file of a package. A report's own
 * named query or source asked of index.malloy is refused with the file that
 * declares it, through the real tool handler and a real loaded Package. A name
 * no file declares, and a name only a gated file declares, keep the plain
 * not-found answer.
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

const ORIGINAL_ENV = process.env.PACKAGE_LOAD_WORKERS;

type Handler = (params: Record<string, unknown>) => Promise<{
   isError?: boolean;
   content: Array<{ resource?: { text: string } }>;
}>;

describe("execute_query on a name another file declares", () => {
   let tempDir: string;
   let handler: Handler;
   let duckdb: { close: () => Promise<void> };

   beforeAll(async () => {
      process.env.PACKAGE_LOAD_WORKERS = "1";
      await __setPackageLoadPoolForTests(new PackageLoadPool(1));
      tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "publisher-mcp-de-"));
      fs.writeFileSync(
         path.join(tempDir, "publisher.json"),
         JSON.stringify({ name: "pkg", description: "test package" }),
      );
      fs.writeFileSync(
         path.join(tempDir, "index.malloy"),
         `source: order_items is duckdb.sql("""
  select 'Shoes' as category, 1 as region
  union all select 'Hats', 2
""") extend {
  measure: c is count()
  view: by_category is { group_by: category; aggregate: c; order_by: category }
}`,
      );
      // The builder's form (a named query) and the old form (a tiles source).
      fs.writeFileSync(
         path.join(tempDir, "overview.malloy"),
         `## artifact { layout=grid title="Overview" tiles=["revenue_by_category", "order_items_tiles -> t"] }
import "index.malloy"
query: revenue_by_category is order_items -> by_category + { where: region = 1 }
source: order_items_tiles is order_items extend { view: t is by_category }`,
      );
      // A served report like overview.malloy, but gated.
      fs.writeFileSync(
         path.join(tempDir, "locked.malloy"),
         `## artifact { layout=grid title="Locked" tiles=["locked_only -> by_category"] }
import "index.malloy"

#(authorize) false
source: locked_only is order_items extend {}`,
      );
      // Declared, but in a file the package neither lists nor serves.
      fs.writeFileSync(
         path.join(tempDir, "scratch.malloy"),
         `import "index.malloy"
query: scratch_only is order_items -> by_category`,
      );

      const { MalloyConfig, FixedConnectionMap } = await import(
         "@malloydata/malloy"
      );
      const { DuckDBConnection } = await import("@malloydata/db-duckdb");
      const connection = new DuckDBConnection("duckdb", ":memory:");
      duckdb = connection;
      const malloyConfig = new MalloyConfig({ connections: {} });
      malloyConfig.wrapConnections(
         () =>
            new FixedConnectionMap(new Map([["duckdb", connection]]), "duckdb"),
      );
      const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);
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
      await duckdb.close();
      await __setPackageLoadPoolForTests(null);
      if (ORIGINAL_ENV === undefined) delete process.env.PACKAGE_LOAD_WORKERS;
      else process.env.PACKAGE_LOAD_WORKERS = ORIGINAL_ENV;
      fs.rmSync(tempDir, { recursive: true, force: true });
   });

   const call = (args: Record<string, unknown>) =>
      handler({
         environmentName: "env",
         packageName: "pkg",
         modelPath: "index.malloy",
         ...args,
      });
   const errorOf = async (args: Record<string, unknown>) => {
      const result = await call(args);
      expect(result.isError).toBe(true);
      return JSON.parse(result.content[0].resource!.text) as {
         error: string;
         suggestions: string[];
      };
   };

   it("names the report that declares a named query", async () => {
      const expected = `'revenue_by_category' is not defined in index.malloy. It is a query in overview.malloy; run it with modelPath "overview.malloy".`;
      for (const args of [
         { query: "run: revenue_by_category" },
         { queryName: "revenue_by_category" },
      ]) {
         const { error, suggestions } = await errorOf(args);
         expect(error).toBe(expected);
         expect(suggestions.join(" ")).not.toContain("reload_package");
      }
   });

   it("names the report that declares a source", async () => {
      const expected = `'order_items_tiles' is not defined in index.malloy. It is a source in overview.malloy; run it with modelPath "overview.malloy".`;
      expect(
         (await errorOf({ query: "run: order_items_tiles -> t" })).error,
      ).toBe(expected);
      expect(
         (await errorOf({ sourceName: "order_items_tiles", queryName: "t" }))
            .error,
      ).toBe(expected);
   });

   it("runs the same query once addressed to that file", async () => {
      const result = await call({
         modelPath: "overview.malloy",
         query: "run: revenue_by_category",
      });
      expect(result.isError).not.toBe(true);
      expect(JSON.parse(result.content[0].resource!.text).rows).toEqual([
         { category: "Shoes", c: 1 },
      ]);
   });

   it("keeps the plain not-found answer for a name no file declares", async () => {
      const { error } = await errorOf({ query: "run: nowhere_at_all" });
      expect(error).toBe("Resource not found: env/pkg/index.malloy");
   });

   it("never points at a file the package does not list or serve", async () => {
      const { error } = await errorOf({ query: "run: scratch_only" });
      expect(error).toBe("Resource not found: env/pkg/index.malloy");
   });

   it("never points at a gated file", async () => {
      const { error } = await errorOf({
         query: "run: locked_only -> { select: * }",
      });
      expect(error).toBe("Resource not found: env/pkg/index.malloy");
   });
});
