/**
 * Integration test: Entry-Point Visibility.
 *
 * Drives `Package.create` through the package-load worker pool (same harness as
 * package_worker_path.spec.ts) and verifies, end-to-end, the listing rule:
 *
 *   - `entryPoints` in publisher.json hides non-entry .malloy models from
 *     `listModels()` while they still compile for import/join resolution.
 *   - Within an entry point, only the re-export closure (`export { … }`) is
 *     listed as sources — imported helpers stay out.
 *   - A query that joins through a hidden module still resolves.
 *   - Notebooks are always listed regardless of `entryPoints`.
 *   - Absent/empty `entryPoints` → every model is listed (backward compatible).
 *   - A bogus `entryPoints` path fails the load loudly with an actionable error.
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

describe("Entry-Point Visibility via worker pool", () => {
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
      tempDir = fs.mkdtempSync(
         path.join(os.tmpdir(), "publisher-entrypoints-"),
      );
   });

   afterEach(() => {
      if (tempDir) {
         try {
            fs.rmSync(tempDir, { recursive: true, force: true });
         } catch {
            /* already gone */
         }
         tempDir = "";
      }
   });

   async function makeMalloyConfig(): Promise<{
      malloyConfig: import("@malloydata/malloy").MalloyConfig;
      duckdb: { close: () => Promise<void> };
   }> {
      const { MalloyConfig, FixedConnectionMap } = await import(
         "@malloydata/malloy"
      );
      const { DuckDBConnection } = await import("@malloydata/db-duckdb");
      const duckdb = new DuckDBConnection("duckdb", ":memory:");
      const connections = new FixedConnectionMap(
         new Map([["duckdb", duckdb]]),
         "duckdb",
      );
      const malloyConfig = new MalloyConfig({ connections: {} });
      malloyConfig.wrapConnections(() => connections);
      return { malloyConfig, duckdb };
   }

   function writeManifest(extra: Record<string, unknown> = {}): void {
      fs.writeFileSync(
         path.join(tempDir, "publisher.json"),
         JSON.stringify({ name: "pkg", description: "test package", ...extra }),
      );
   }

   // A base module (never an entry point) plus a curated index that imports it,
   // joins through it, and re-exports only `customers`. `helper` is a second
   // local source the index does NOT export — used to prove within-file curation.
   function writeLayeredModels(): void {
      fs.writeFileSync(
         path.join(tempDir, "base.malloy"),
         `source: base_source is duckdb.sql("select 1 as id, 'x' as label")`,
      );
      fs.writeFileSync(
         path.join(tempDir, "index.malloy"),
         `import "base.malloy"
source: helper is duckdb.sql("select 1 as id")
source: customers is duckdb.sql("select 1 as id, 100 as amt") extend {
  join_one: b is base_source on id = b.id
  measure: total is amt.sum()
  view: v is { aggregate: total }
}
export { customers }`,
      );
      fs.writeFileSync(
         path.join(tempDir, "report.malloynb"),
         `>>>markdown\n# Sales Report\nA report that is always public.`,
      );
   }

   async function listedModelPaths(pkg: Package): Promise<string[]> {
      return (await pkg.listModels()).map((m) => m.path as string).sort();
   }

   it("hides non-entry models, curates exports, keeps join-through, lists notebooks", async () => {
      writeManifest({ entryPoints: ["index.malloy"] });
      writeLayeredModels();

      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);

         // Listing surface: only the entry point, not base.malloy.
         expect(await listedModelPaths(pkg)).toEqual(["index.malloy"]);

         // Within-file curation: only the re-exported `customers`, not the
         // un-exported `helper`, and not the imported `base_source`.
         const apiModel = (await pkg.getModel("index.malloy")!.getModel()) as {
            sources?: { name?: string }[];
         };
         expect((apiModel.sources ?? []).map((s) => s.name).sort()).toEqual([
            "customers",
         ]);

         // The hidden base module still resolves: index.malloy compiled (above)
         // and a query that joins through base_source executes.
         const { result } = await pkg
            .getModel("index.malloy")!
            .getQueryResults("customers", "v", undefined);
         expect(result.data).toBeDefined();

         // Notebooks are always public regardless of entryPoints.
         expect((await pkg.listNotebooks()).map((n) => n.path)).toEqual([
            "report.malloynb",
         ]);
      } finally {
         await duckdb.close();
      }
   });

   it("keeps enforcing a hidden source's authorize gate (curation ≠ access)", async () => {
      // base_source is hidden from index.malloy's listing (not re-exported),
      // but it carries its own #(authorize) gate. Curation must not drop that
      // gate: getAuthorize reads the COMPLETE source list, not the curated view.
      writeManifest({ entryPoints: ["index.malloy"] });
      fs.writeFileSync(
         path.join(tempDir, "base.malloy"),
         `#(authorize) "1 = 1"
source: base_source is duckdb.sql("select 1 as id")`,
      );
      fs.writeFileSync(
         path.join(tempDir, "index.malloy"),
         `import "base.malloy"
source: customers is duckdb.sql("select 1 as id, 100 as amt") extend {
  measure: total is amt.sum()
}
export { customers }`,
      );

      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);
         const model = pkg.getModel("index.malloy")!;

         // Discovery: base_source is hidden, customers is listed.
         const apiModel = (await model.getModel()) as {
            sources?: { name?: string }[];
         };
         expect((apiModel.sources ?? []).map((s) => s.name).sort()).toEqual([
            "customers",
         ]);

         // Enforcement: the hidden source's gate is still in force.
         expect(model.getAuthorize("base_source")).toEqual(["1 = 1"]);
      } finally {
         await duckdb.close();
      }
   });

   it("lists every model when entryPoints is absent (backward compatible)", async () => {
      writeManifest();
      writeLayeredModels();

      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);
         expect(await listedModelPaths(pkg)).toEqual([
            "base.malloy",
            "index.malloy",
         ]);
      } finally {
         await duckdb.close();
      }
   });

   it("fails the load with an actionable error on an unknown entryPoints path", async () => {
      writeManifest({ entryPoints: ["does-not-exist.malloy"] });
      writeLayeredModels();

      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         await expect(
            Package.create("env", "pkg", tempDir, malloyConfig),
         ).rejects.toThrow(/does-not-exist\.malloy.*not found/s);
      } finally {
         await duckdb.close();
      }
   });
});
