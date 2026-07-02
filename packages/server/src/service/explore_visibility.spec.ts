/**
 * Integration test: Explore Visibility.
 *
 * Drives `Package.create` through the package-load worker pool (same harness as
 * package_worker_path.spec.ts) and verifies, end-to-end, the listing rule:
 *
 *   - `explores` in publisher.json hides non-entry .malloy models from
 *     `listModels()` while they still compile for import/join resolution.
 *   - Within an explore, only the re-export closure (`export { … }`) is
 *     listed as sources — imported helpers stay out.
 *   - A query that joins through a hidden module still resolves.
 *   - Notebooks are always listed regardless of `explores`.
 *   - Absent/empty `explores` → every model is listed with the full source set
 *     (backward compatible; no within-file `export {}` curation).
 *   - Within-file curation applies only when `explores` is declared.
 *   - A bogus `explores` path is fail-safe at load (warn + hide, no throw);
 *     the publish path rejects it (package.controller.spec.ts).
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

describe("Explore Visibility via worker pool", () => {
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
      tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "publisher-explores-"));
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

   // A base module (never an explore) plus a curated index that imports it,
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
      writeManifest({ explores: ["index.malloy"] });
      writeLayeredModels();

      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);

         // Listing surface: only the explore, not base.malloy.
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

         // Notebooks are always public regardless of explores.
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
      writeManifest({ explores: ["index.malloy"] });
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

   it("lists every model and full sources when explores is absent (backward compatible)", async () => {
      writeManifest();
      writeLayeredModels();

      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);
         expect(await listedModelPaths(pkg)).toEqual([
            "base.malloy",
            "index.malloy",
         ]);

         // No `explores` ⇒ export{} curation off; non-exported `helper` listed.
         const apiModel = (await pkg.getModel("index.malloy")!.getModel()) as {
            sources?: { name?: string }[];
         };
         const names = (apiModel.sources ?? []).map((s) => s.name).sort();
         expect(names).toContain("customers");
         expect(names).toContain("helper");
      } finally {
         await duckdb.close();
      }
   });

   it("does not curate by export{} without explores; curates once explores is declared", async () => {
      writeManifest(); // no explores
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
         let sourceNames = (api.sources ?? []).map((s) => s.name).sort();
         expect(sourceNames).toEqual(["internal_scratch", "public_orders"]);

         // Opt in via explores ⇒ export closure only, aligned with modelInfo.
         pkg.setPackageMetadata({
            ...pkg.getPackageMetadata(),
            explores: ["model.malloy"],
         });
         const curated = (await pkg.getModel("model.malloy")!.getModel()) as {
            sources?: { name?: string }[];
            modelInfo?: string;
         };
         sourceNames = (curated.sources ?? []).map((s) => s.name).sort();
         expect(sourceNames).toEqual(["public_orders"]);
         const mi = JSON.parse(curated.modelInfo ?? "{}");
         const infoNames = (mi.entries ?? [])
            .filter((e: { kind: string }) => e.kind === "source")
            .map((e: { name: string }) => e.name)
            .sort();
         expect(sourceNames).toEqual(infoNames);
      } finally {
         await duckdb.close();
      }
   });

   it("import-only barrel lists imported sources without explores; empty once explores lists it", async () => {
      writeManifest(); // no explores
      fs.writeFileSync(
         path.join(tempDir, "base.malloy"),
         `source: base_source is duckdb.sql("select 1 as id, 100 as amt") extend {
  measure: total is amt.sum()
  view: v is { aggregate: total }
}`,
      );
      fs.writeFileSync(
         path.join(tempDir, "consumer.malloy"),
         `import "base.malloy"
run: base_source -> v`,
      );

      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);
         const api = (await pkg.getModel("consumer.malloy")!.getModel()) as {
            sources?: { name?: string }[];
         };
         expect((api.sources ?? []).map((s) => s.name).sort()).toEqual([
            "base_source",
         ]);
         expect(pkg.emptyDiscoveryWarnings()).toEqual([]);

         pkg.setPackageMetadata({
            ...pkg.getPackageMetadata(),
            explores: ["consumer.malloy"],
            queryableSources: "all",
         });
         const curated = (await pkg
            .getModel("consumer.malloy")!
            .getModel()) as {
            sources?: { name?: string }[];
            modelInfo?: string;
         };
         const sourceNames = (curated.sources ?? []).map((s) => s.name).sort();
         expect(sourceNames).toEqual([]);
         const mi = JSON.parse(curated.modelInfo ?? "{}");
         const infoNames = (mi.entries ?? [])
            .filter((e: { kind: string }) => e.kind === "source")
            .map((e: { name: string }) => e.name)
            .sort();
         expect(sourceNames).toEqual(infoNames);

         // Soft migration: queryableSources "all" keeps hidden files queryable.
         const { result } = await pkg
            .getModel("base.malloy")!
            .getQueryResults("base_source", "v", undefined);
         expect(result.data).toBeDefined();
      } finally {
         await duckdb.close();
      }
   });

   it("direct getModel on a hidden file succeeds and uses package-wide curation", async () => {
      writeManifest({ explores: ["index.malloy"] });
      fs.writeFileSync(
         path.join(tempDir, "base.malloy"),
         `source: pub is duckdb.sql("select 1 as id")
source: hidden is duckdb.sql("select 2 as id")
export { pub }`,
      );
      fs.writeFileSync(
         path.join(tempDir, "index.malloy"),
         `import "base.malloy"
source: customers is duckdb.sql("select 1 as id")
export { customers }`,
      );

      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);
         expect(await listedModelPaths(pkg)).toEqual(["index.malloy"]);

         const hidden = (await pkg.getModel("base.malloy")!.getModel()) as {
            sources?: { name?: string }[];
         };
         expect((hidden.sources ?? []).map((s) => s.name)).toEqual(["pub"]);
      } finally {
         await duckdb.close();
      }
   });

   it("warns for a LISTED import-only model (blank page), not for a hidden one", async () => {
      writeManifest({ explores: ["consumer.malloy"] });
      fs.writeFileSync(
         path.join(tempDir, "base.malloy"),
         `source: base_source is duckdb.sql("select 1 as id")`,
      );
      fs.writeFileSync(
         path.join(tempDir, "consumer.malloy"),
         `import "base.malloy"\nrun: base_source -> { group_by: id }`,
      );

      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);
         const warnings = pkg.emptyDiscoveryWarnings();
         expect(warnings.length).toBe(1);
         expect(warnings[0]).toBe(
            `Model "consumer.malloy" is listed but exposes nothing: it only ` +
               `imports other files and re-exports none of their sources. ` +
               `Add e.g. 'export { source_name }' to surface sources on ` +
               `this model.`,
         );
         expect(pkg.formatInvalidExplores()).toBe("");

         fs.writeFileSync(
            path.join(tempDir, "consumer.malloy"),
            `import "base.malloy"\nexport { base_source }`,
         );
         await pkg.reloadAllModels({});
         expect(pkg.emptyDiscoveryWarnings()).toEqual([]);

         fs.writeFileSync(
            path.join(tempDir, "consumer.malloy"),
            `import "base.malloy"\nrun: base_source -> { group_by: id }`,
         );
         writeManifest({ explores: ["base.malloy"] });
         await pkg.reloadAllModels({});
         expect(pkg.emptyDiscoveryWarnings()).toEqual([]);
      } finally {
         await duckdb.close();
      }
   });

   it("load is fail-safe on an unknown explores path: warns, hides, does not throw", async () => {
      writeManifest({ explores: ["does-not-exist.malloy"] });
      writeLayeredModels();

      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);
         expect(await listedModelPaths(pkg)).toEqual([]);
         expect(pkg.formatInvalidExplores()).toMatch(
            /does-not-exist\.malloy.*not found/s,
         );
         const warnings = pkg.getPackageMetadata().exploresWarnings ?? [];
         expect(warnings.length).toBe(1);
         expect(warnings[0]).toBe(
            `Invalid explores entry 'does-not-exist.malloy' in ` +
               `publisher.json: file not found in the package. Fix: list a ` +
               `.malloy file relative to the package root ` +
               `(e.g. "index.malloy").`,
         );
      } finally {
         await duckdb.close();
      }
   });

   it("flags a notebook listed as an explore; valid entries still resolve", async () => {
      writeManifest({ explores: ["index.malloy", "report.malloynb"] });
      writeLayeredModels();

      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);
         expect(await listedModelPaths(pkg)).toEqual(["index.malloy"]);
         expect(pkg.getInvalidExplores().map((p) => p.entry)).toEqual([
            "report.malloynb",
         ]);
         expect(pkg.formatInvalidExplores()).toMatch(
            /report\.malloynb.*notebooks are always public/s,
         );
      } finally {
         await duckdb.close();
      }
   });
});
