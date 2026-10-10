// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A control suggesting over a joined dimension (`dimension="products.category"`)
 * must lint clean, and a bad path must still be flagged. Drives `Package.create`
 * through the package-load worker pool, the path the compiled model takes in
 * production.
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

const MODEL = `##! experimental.givens

source: products is duckdb.sql("select 1 as id, 'toys' as category") extend {
  join_one: maker is duckdb.sql("select 1 as id, 'acme' as name") on id = maker.id
}

source: order_items is duckdb.sql("select 1 as product_id, 5 as qty") extend {
  join_one: products on product_id = products.id
  measure: total_qty is qty.sum()
}
`;

const dashboard = (dimension: string) => `##! experimental.givens
import { order_items } from '../model.malloy'

# control=select suggest { source=order_items dimension="${dimension}" }
given: CATEGORY :: filter<string> is f''

# artifact { title="By category" }
query: by_category is order_items -> {
   where: products.category ~ $CATEGORY
   group_by: products.category
   aggregate: total_qty
}
`;

describe("dashboard lint: suggest over a joined dimension", () => {
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
      tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "publisher-joinlint-"));
      fs.writeFileSync(
         path.join(tempDir, "publisher.json"),
         JSON.stringify({ name: "pkg", description: "test package" }),
      );
      fs.writeFileSync(path.join(tempDir, "model.malloy"), MODEL);
      fs.mkdirSync(path.join(tempDir, "dashboards"));
   });

   afterEach(() => {
      fs.rmSync(tempDir, { recursive: true, force: true });
   });

   async function suggestFindings(dimension: string): Promise<string[]> {
      fs.writeFileSync(
         path.join(tempDir, "dashboards", "by_category.malloy"),
         dashboard(dimension),
      );
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
         const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);
         return (pkg.getPackageMetadata().warnings ?? [])
            .filter((w) => w.severity === "error")
            .map((w) => w.message ?? "");
      } finally {
         await duckdb.close();
      }
   }

   it("accepts a one-level joined dimension", async () => {
      expect(await suggestFindings("products.category")).toEqual([]);
   });

   it("still flags an unknown field on a known join", async () => {
      expect(await suggestFindings("products.nope")).toEqual([
         expect.stringContaining('has no field "products.nope"'),
      ]);
   });

   it("still flags an unknown join", async () => {
      expect(await suggestFindings("ghost.category")).toEqual([
         expect.stringContaining('has no field "ghost.category"'),
      ]);
   });

   it("still flags a two-level path", async () => {
      expect(await suggestFindings("products.maker.name")).toEqual([
         expect.stringContaining('has no field "products.maker.name"'),
      ]);
   });
});
