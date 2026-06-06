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
 *   - Within-file curation reflects `export {}` for ANY model (consistent with
 *     Malloy's export-aware modelInfo), independent of entryPoints.
 *   - A bogus `entryPoints` path is fail-safe at load (warn + hide, no throw);
 *     the publish path rejects it (covered at the controller layer).
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

   it("curates sources by export{} even without entryPoints (consistent with modelInfo)", async () => {
      // No entryPoints declared, but the model has an export{}. Discovery must
      // reflect the export closure (matching Malloy's modelInfo/sourceInfos that
      // the app renders), not the publisher-internal full source list.
      writeManifest(); // no entryPoints
      fs.writeFileSync(
         path.join(tempDir, "model.malloy"),
         `source: public_orders is duckdb.sql("select 1 as id")
source: internal_scratch is duckdb.sql("select 1 as id")
export { public_orders }`,
      );

      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);
         const api = (await pkg.getModel("model.malloy")!.getModel()) as {
            sources?: { name?: string }[];
            modelInfo?: string;
         };
         const sourceNames = (api.sources ?? []).map((s) => s.name).sort();
         expect(sourceNames).toEqual(["public_orders"]); // internal_scratch hidden
         // sources[] agrees with Malloy's modelInfo (the app's source of truth).
         const mi = JSON.parse(api.modelInfo ?? "{}");
         const infoNames = (mi.entries ?? [])
            .filter((e: { kind: string }) => e.kind === "source")
            .map((e: { name: string }) => e.name)
            .sort();
         expect(sourceNames).toEqual(infoNames);
      } finally {
         await duckdb.close();
      }
   });

   it("load is fail-safe on an unknown entryPoints path: warns, hides, does not throw", async () => {
      // Policy: strict at publish (rejected in package.controller), fail-safe at
      // load. An unresolvable entry must NOT fall back to listing everything —
      // that would expose the sources entryPoints exists to hide. It hides.
      writeManifest({ entryPoints: ["does-not-exist.malloy"] });
      writeLayeredModels();

      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);
         // Did not throw; fail-safe → no models listed (not all of them).
         expect(await listedModelPaths(pkg)).toEqual([]);
         // The reason is surfaced (logged at load; used to reject at publish).
         expect(pkg.formatInvalidEntryPoints()).toMatch(
            /does-not-exist\.malloy.*not found/s,
         );
         // …and on the package metadata the API/UI consume.
         const warnings = pkg.getPackageMetadata().entryPointsWarnings ?? [];
         expect(warnings.length).toBe(1);
         expect(warnings[0]).toMatch(/does-not-exist\.malloy.*not found/s);
      } finally {
         await duckdb.close();
      }
   });

   it("flags a notebook listed as an entry point; valid entries still resolve", async () => {
      writeManifest({ entryPoints: ["index.malloy", "report.malloynb"] });
      writeLayeredModels();

      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);
         // The valid entry still lists; the notebook entry is flagged invalid.
         expect(await listedModelPaths(pkg)).toEqual(["index.malloy"]);
         expect(pkg.getInvalidEntryPoints().map((p) => p.entry)).toEqual([
            "report.malloynb",
         ]);
         expect(pkg.formatInvalidEntryPoints()).toMatch(
            /report\.malloynb.*notebooks are always public/s,
         );
      } finally {
         await duckdb.close();
      }
   });
});
