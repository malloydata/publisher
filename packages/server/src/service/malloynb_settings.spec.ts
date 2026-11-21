// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A `.malloynb`'s own `##` settings, `## autorun=false` and a `## givens { … }`
 * starting value, reach the reports read, driven through `Package.create` on
 * the package-load worker pool.
 */
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import {
   PackageLoadPool,
   __setPackageLoadPoolForTests,
} from "../package_load/package_load_pool";
import { Package } from "./package";

const NOTEBOOK = `>>>markdown
# Settings
>>>malloy
##! experimental.givens
## autorun=false
## givens { REGION="EU" }
given: REGION :: string is "US"
source: orders is duckdb.sql("select 1 as id, 'EU' as region") extend {
   measure: c is count()
}
>>>malloy
run: orders -> { aggregate: c }
`;

describe("a .malloynb's own settings on the reports read", () => {
   let pool: PackageLoadPool;
   let tempDir: string;

   beforeAll(async () => {
      pool = new PackageLoadPool(1);
      await __setPackageLoadPoolForTests(pool);
      tempDir = fs.mkdtempSync(
         path.join(os.tmpdir(), "publisher-nb-settings-"),
      );
      fs.writeFileSync(
         path.join(tempDir, "publisher.json"),
         JSON.stringify({ name: "pkg" }),
      );
      fs.writeFileSync(path.join(tempDir, "settings.malloynb"), NOTEBOOK);
   });

   afterAll(async () => {
      await __setPackageLoadPoolForTests(null);
      fs.rmSync(tempDir, { recursive: true, force: true });
   });

   it("reads autorun and the starting givens off the notebook's own ## tags", async () => {
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
         const report = await pkg.getReport("settings.malloynb");
         expect(report?.notebook?.format).toBe("malloynb");
         expect(report?.notebook?.autorun).toBe(false);
         expect(report?.notebook?.startingGivens).toEqual({ REGION: "EU" });
      } finally {
         await duckdb.close();
      }
   });
});
